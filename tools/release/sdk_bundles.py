#!/usr/bin/env python3
"""Fail-closed membership checks for native SDK release archives."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve().parent
_CORE_TOOLS = _HERE.parents[1] / "core" / "tools"
if str(_CORE_TOOLS) not in sys.path:
    sys.path.insert(0, str(_CORE_TOOLS))

from core_abi import read_core_abi  # noqa: E402


# Release target: Ubuntu 22.04 / ROS 2 Humble, glibc 2.35.
MAX_GLIBC_VERSION = (2, 35)

# The release contract names the asset
# `aorta-cpp-sdk-<release>-abi<N>-ubuntu22.04-<arch>.tar.zst`: the distro token
# is part of the contract because the bundle's glibc baseline is 22.04's, and a
# consumer must be able to tell which userspace an asset was built for. `<N>`
# is the Core ABI generation, `AORTA_ABI_VERSION` in core/src/ffi/c_api/mod.rs.
CPP_RELEASE_TRIPLETS = ("ubuntu22.04-aarch64", "ubuntu22.04-x86_64")


def core_shared_object_name(core_abi: int) -> str:
    """Linux Core library name and DT_SONAME for an ABI generation."""
    return f"libaorta_core.so.{core_abi}"


def expected_release_asset_names(
    version: str,
    core_abi: int,
    triplets: tuple[str, ...] = CPP_RELEASE_TRIPLETS,
) -> set[str]:
    archives = {
        f"aorta-cpp-sdk-{version}-abi{core_abi}-{triplet}.tar.zst"
        for triplet in triplets
    }
    return archives | {f"{name}.sha256" for name in archives}


def verify_release_assets(
    directory: Path,
    version: str,
    core_abi: int,
    triplets: tuple[str, ...] = CPP_RELEASE_TRIPLETS,
) -> None:
    expected = expected_release_asset_names(version, core_abi, triplets)
    actual = {path.name for path in directory.iterdir() if path.is_file()}
    verify_exact_members(actual, expected)

    for checksum_name in sorted(name for name in expected if name.endswith(".sha256")):
        archive_name = checksum_name.removesuffix(".sha256")
        digest = hashlib.sha256((directory / archive_name).read_bytes()).hexdigest()
        checksum = (directory / checksum_name).read_text(encoding="utf-8")
        if checksum != f"{digest}  {archive_name}\n":
            raise ValueError(f"invalid checksum file: {checksum_name}")


def verify_exact_members(actual: set[str], expected: set[str]) -> None:
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        raise ValueError("; ".join(details))


def _read_expected(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


_ELF_MAGIC = b"\x7fELF"

_TEXT_MEMBERS = (
    "include/flatbuffers/base.h",
    "PROVENANCE.json",
    "share/cmake/Aorta/AortaConfig.cmake",
    "share/cmake/Aorta/AortaConfigVersion.cmake",
    "share/cmake/FlatBuffers/FlatBuffersConfig.cmake",
)


def _extract_archive(
    path: Path, zstd_path: Optional[Path]
) -> tuple[
    set[str],
    dict,
    dict[str, str],
    dict[str, str],
    dict[str, bytes],
    dict[str, tuple[str, int]],
]:
    zstd = str(zstd_path) if zstd_path is not None else shutil.which("zstd")
    if zstd is None:
        raise RuntimeError("zstd executable is required for membership tests")

    with tempfile.TemporaryDirectory() as tmp:
        tar_path = Path(tmp) / "bundle.tar"
        if zstd_path is not None:
            subprocess.run([zstd, str(path), str(tar_path)], check=True)
        else:
            with tar_path.open("wb") as output:
                subprocess.run(
                    [zstd, "-q", "-d", "-c", str(path)],
                    check=True,
                    stdout=output,
                )
        with tarfile.open(tar_path, "r:") as archive:
            entries = archive.getmembers()
            names = [entry.name for entry in entries]
            if len(names) != len(set(names)):
                raise ValueError("archive contains duplicate members")
            if any(not entry.isfile() and not entry.issym() for entry in entries):
                raise ValueError("archive may contain regular files and declared symlinks only")
            symlinks = {
                entry.name: entry.linkname for entry in entries if entry.issym()
            }
            manifest = json.load(archive.extractfile("MANIFEST.json"))
            texts = {
                member: archive.extractfile(member).read().decode("utf-8")
                for member in _TEXT_MEMBERS
                if member in names
            }
            libraries = {}
            digests = {}
            for entry in entries:
                if not entry.isfile() or entry.name == "MANIFEST.json":
                    continue
                payload = archive.extractfile(entry).read()
                digests[entry.name] = (hashlib.sha256(payload).hexdigest(), len(payload))
                if entry.name.startswith("lib/"):
                    libraries[entry.name] = payload
    return set(names), manifest, symlinks, texts, libraries, digests


def _verify_elf_soname(core_elf: bytes, core_abi: int, readelf_path: Optional[Path]) -> None:
    readelf = str(readelf_path) if readelf_path is not None else shutil.which("readelf")
    if readelf is None:
        raise RuntimeError("readelf is required to verify the packaged ELF SONAME")
    soname = core_shared_object_name(core_abi)
    with tempfile.TemporaryDirectory() as tmp:
        library = Path(tmp) / soname
        library.write_bytes(core_elf)
        dynamic = subprocess.check_output([readelf, "-d", str(library)], text=True)
    if f"Library soname: [{soname}]" not in dynamic:
        raise ValueError(f"packaged Core ELF DT_SONAME is not {soname}")


_ADVERTISED_ABI_PATTERNS = {
    "PROVENANCE.json": re.compile(r'"core_abi":\s*(\d+)'),
    "share/cmake/Aorta/AortaConfig.cmake": re.compile(
        r'set\(Aorta_VERSION "(\d+)\.\d+\.\d+"\)'
        r"|set\(Aorta_CORE_ABI_VERSION (\d+)\)"
        r"|libaorta_core\.so\.(\d+)"
    ),
    "share/cmake/Aorta/AortaConfigVersion.cmake": re.compile(
        r'set\(PACKAGE_VERSION "(\d+)\.\d+\.\d+"\)'
        r"|PACKAGE_FIND_VERSION_MAJOR EQUAL (\d+)"
    ),
}


def advertised_core_abis(
    actual: set[str], symlinks: dict[str, str], texts: dict[str, str]
) -> dict[str, set[int]]:
    """Every Core ABI number the bundle advertises, keyed by where it says so.

    Covers the versioned Linux member name, the development symlink target,
    `PROVENANCE.json`, and the CMake package version / `Aorta_CORE_ABI_VERSION`
    / library path in the installed package config.
    """
    advertised: dict[str, set[int]] = {}
    member_pattern = re.compile(r"^lib/libaorta_core\.so\.(\d+)$")
    for member in sorted(actual):
        match = member_pattern.match(member)
        if match:
            advertised.setdefault("lib/libaorta_core.so.<N> member", set()).add(int(match.group(1)))
    for link, target in sorted(symlinks.items()):
        match = re.fullmatch(r"libaorta_core\.so\.(\d+)", target)
        if match:
            advertised.setdefault(f"{link} symlink target", set()).add(int(match.group(1)))
    for member, pattern in _ADVERTISED_ABI_PATTERNS.items():
        text = texts.get(member)
        if text is None:
            continue
        values = {
            int(group)
            for match in pattern.finditer(text)
            for group in match.groups()
            if group is not None
        }
        if values:
            advertised[member] = values
    return advertised


def verify_advertised_core_abi(
    core_abi: int, actual: set[str], symlinks: dict[str, str], texts: dict[str, str]
) -> None:
    """Every ABI number the bundle advertises must be Core's own.

    Past failure: `AORTA_ABI_VERSION` had moved to 12 while the
    bundle's config, member names, SONAME and provenance all still said 10.
    Installed next to a genuine ABI-10 Core, the dynamic loader would have
    picked an incompatible library carrying the same SONAME. The number now
    reaches every one of those places through core/tools/core_abi.py, and
    this gate re-parses the constant independently and compares.
    """
    advertised = advertised_core_abis(actual, symlinks, texts)
    for required in ("PROVENANCE.json", "share/cmake/Aorta/AortaConfig.cmake",
                     "share/cmake/Aorta/AortaConfigVersion.cmake"):
        if required not in advertised:
            raise ValueError(f"{required} does not advertise a Core ABI")
    mismatched = {
        where: sorted(values) for where, values in advertised.items() if values != {core_abi}
    }
    if mismatched:
        raise ValueError(
            f"bundle advertises a Core ABI other than {core_abi} (AORTA_ABI_VERSION): "
            + "; ".join(f"{where} says {values}" for where, values in sorted(mismatched.items()))
        )


def _verify_internal_manifest(
    actual: set[str],
    manifest: dict,
    digests: dict[str, tuple[str, int]],
    symlinks: dict[str, str],
) -> None:
    """`MANIFEST.json` must describe the archive it travels in, member by member.

    The bundle contract includes SHA-256 checksums. The out-of-band
    `<archive>.tar.zst.sha256` sidecar only proves the download arrived intact;
    per-member digests are what lets a consumer check an unpacked tree, so the
    gate recomputes every one of them rather than trusting the name list.
    """
    if manifest.get("format_version") != 2:
        raise ValueError("archive MANIFEST.json must use format_version 2")

    members = manifest.get("members", [])
    if not isinstance(members, list):
        raise ValueError("archive MANIFEST.json members must be a list")
    described = {}
    for entry in members:
        if not isinstance(entry, dict) or "path" not in entry:
            raise ValueError(f"invalid MANIFEST.json member: {entry!r}")
        if entry["path"] in described:
            raise ValueError(f"duplicate MANIFEST.json member: {entry['path']}")
        described[entry["path"]] = entry

    verify_exact_members(set(described), actual - {"MANIFEST.json"})

    mismatched = []
    for path, entry in sorted(described.items()):
        if path in symlinks:
            if entry.get("symlink") != symlinks[path]:
                mismatched.append(f"{path} (symlink target)")
            continue
        digest, size = digests[path]
        if entry.get("sha256") != digest:
            mismatched.append(f"{path} (sha256)")
        elif entry.get("size") != size:
            mismatched.append(f"{path} (size)")
    if mismatched:
        raise ValueError(
            "MANIFEST.json does not describe the archive: " + ", ".join(mismatched)
        )


def iter_ar_members(archive: bytes):
    """Yield `(name, payload)` for every member of a System V `ar` archive."""
    if not archive.startswith(b"!<arch>\n"):
        raise ValueError("static archive is not a System V ar archive")
    offset = len(b"!<arch>\n")
    long_names = b""
    while offset + 60 <= len(archive):
        header = archive[offset : offset + 60]
        if header[58:60] != b"`\n":
            raise ValueError("corrupt ar member header")
        raw_name = header[0:16].decode("ascii", "replace").rstrip()
        size = int(header[48:58].decode("ascii").strip())
        payload = archive[offset + 60 : offset + 60 + size]
        offset += 60 + size + (size % 2)

        if raw_name == "//":
            long_names = payload
            continue
        if raw_name in ("/", "/SYM64/", "__.SYMDEF", "__.SYMDEF SORTED"):
            continue
        if raw_name.startswith("/") and raw_name[1:].isdigit():
            start = int(raw_name[1:])
            end = long_names.find(b"/\n", start)
            if end == -1:
                end = long_names.find(b"/", start)
            raw_name = long_names[start:end].decode("ascii", "replace")
        yield raw_name.rstrip("/"), payload


def _elf_sections(payload: bytes) -> list[dict]:
    """Section table of an ELF file, or `[]` when the payload is not ELF."""
    if not payload.startswith(_ELF_MAGIC):
        return []
    is_64 = payload[4] == 2
    order = "little" if payload[5] == 1 else "big"

    def read(offset: int, width: int) -> int:
        return int.from_bytes(payload[offset : offset + width], order)

    if is_64:
        shoff = read(0x28, 8)
        shentsize, shnum, shstrndx = read(0x3A, 2), read(0x3C, 2), read(0x3E, 2)
        offset_field, link_field, width = 0x18, 0x28, 8
    else:
        shoff = read(0x20, 4)
        shentsize, shnum, shstrndx = read(0x2E, 2), read(0x30, 2), read(0x32, 2)
        offset_field, link_field, width = 0x10, 0x18, 4
    if shoff == 0 or shnum == 0:
        return []

    raw = []
    for index in range(shnum):
        header = shoff + index * shentsize
        raw.append(
            {
                "name_offset": read(header, 4),
                "type": read(header + 4, 4),
                "offset": read(header + offset_field, width),
                "size": read(header + offset_field + width, width),
                "link": read(header + link_field, 4),
            }
        )

    strtab_offset = raw[shstrndx]["offset"]
    for section in raw:
        start = strtab_offset + section["name_offset"]
        end = payload.find(b"\x00", start)
        section["name"] = payload[start:end].decode("ascii", "replace")
        section["order"] = order
        section["is_64"] = is_64
    return raw


def elf_section_names(payload: bytes) -> list[str]:
    """Section names of an ELF object, or `[]` when the payload is not ELF."""
    return [section["name"] for section in _elf_sections(payload)]


def _cstring(payload: bytes, offset: int) -> str:
    end = payload.find(b"\x00", offset)
    return payload[offset:end].decode("ascii", "replace")


def elf_required_glibc_versions(payload: bytes) -> set[tuple[int, ...]]:
    """`GLIBC_x.y` versions an ELF binary requires, from `.gnu.version_r`."""
    sections = _elf_sections(payload)
    verneed = next((s for s in sections if s["name"] == ".gnu.version_r"), None)
    if verneed is None:
        return set()
    order, is_64 = verneed["order"], verneed["is_64"]
    strtab = sections[verneed["link"]]["offset"]

    def read(offset: int, width: int) -> int:
        return int.from_bytes(payload[offset : offset + width], order)

    versions: set[tuple[int, ...]] = set()
    entry = verneed["offset"]
    while True:
        aux_count = read(entry + 2, 2)  # Elf{32,64}_Verneed.vn_cnt
        aux_offset = entry + read(entry + 8, 4)  # .vn_aux
        aux = aux_offset
        for _ in range(aux_count):
            name = _cstring(payload, strtab + read(aux + 8, 4))
            if name.startswith("GLIBC_"):
                try:
                    versions.add(tuple(int(part) for part in name[6:].split(".")))
                except ValueError:
                    pass
            next_aux = read(aux + 12, 4)
            if next_aux == 0:
                break
            aux += next_aux
        next_entry = read(entry + 12, 4)
        if next_entry == 0:
            break
        entry += next_entry
    _ = is_64
    return versions


def elf_undefined_symbols(payload: bytes) -> set[str]:
    """Undefined symbol names of an ELF object (`.symtab`, relocatable input)."""
    sections = _elf_sections(payload)
    symtab = next((s for s in sections if s["name"] == ".symtab"), None)
    if symtab is None:
        return set()
    order, is_64 = symtab["order"], symtab["is_64"]
    strtab = sections[symtab["link"]]["offset"]
    entry_size = 24 if is_64 else 16
    shndx_field = 6 if is_64 else 14
    names = set()
    for offset in range(symtab["offset"], symtab["offset"] + symtab["size"], entry_size):
        shndx = int.from_bytes(payload[offset + shndx_field : offset + shndx_field + 2], order)
        if shndx != 0:  # SHN_UNDEF
            continue
        name_offset = int.from_bytes(payload[offset : offset + 4], order)
        if name_offset:
            names.add(_cstring(payload, strtab + name_offset))
    return names


def _format_glibc(version: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in version)


def verify_glibc_baseline(libraries: dict[str, bytes]) -> None:
    """Every shipped Linux binary must run on the documented glibc baseline.

    The documented target is Ubuntu 22.04 (glibc 2.35), but the release
    jobs used to run on 24.04 hosts, so `libaorta_cpp.so` picked up
    `__isoc23_strtol@GLIBC_2.38` and could not be linked or loaded there at all.
    The shared objects are checked through their `.gnu.version_r` requirements;
    relocatable objects inside the static archive carry no symbol versions yet,
    so they are checked for the `__isoc23_*` family glibc 2.38 introduced --
    their presence is a direct tell of a too-new build host.
    """
    for member in sorted(libraries):
        payload = libraries[member]
        too_new = sorted(
            version
            for version in elf_required_glibc_versions(payload)
            if version > MAX_GLIBC_VERSION
        )
        if too_new:
            raise ValueError(
                f"{member} requires glibc "
                + ", ".join(_format_glibc(version) for version in too_new)
                + f" (baseline is {_format_glibc(MAX_GLIBC_VERSION)})"
            )
        if not member.endswith(".a"):
            continue
        offenders = sorted(
            {
                symbol
                for _, object_payload in iter_ar_members(payload)
                for symbol in elf_undefined_symbols(object_payload)
                if symbol.startswith("__isoc23_")
            }
        )
        if offenders:
            raise ValueError(
                f"{member} references post-baseline glibc symbols: "
                + ", ".join(offenders)
            )


def verify_static_archive_is_portable(archive: bytes) -> None:
    """Reject a released `libaorta_cpp.a` that carries GCC LTO bytecode.

    GNU ld's LTO plugin claims `.gnu.lto_*` sections in preference to any
    machine code in the same object, so an archive built with `-flto` — fat
    objects included — can only be linked by the exact GCC major version that
    produced it. A redistributable archive must be toolchain-portable.
    """
    members = list(iter_ar_members(archive))
    if not members:
        raise ValueError("released static archive has no members")
    offenders = sorted(
        name
        for name, payload in members
        if any(
            section.startswith(".gnu.lto_") for section in elf_section_names(payload)
        )
    )
    if offenders:
        raise ValueError(
            "released static archive carries LTO bytecode: " + ", ".join(offenders)
        )


def _flatbuffers_header_version(base_header: str) -> str:
    values = []
    for field in ("MAJOR", "MINOR", "REVISION"):
        match = re.search(
            rf"^#define\s+FLATBUFFERS_VERSION_{field}\s+(\d+)\s*$",
            base_header,
            re.MULTILINE,
        )
        if match is None:
            raise ValueError(f"vendored base.h declares no FLATBUFFERS_VERSION_{field}")
        values.append(match.group(1))
    return ".".join(values)


def _verify_vendored_flatbuffers(texts: dict[str, str]) -> None:
    """The vendored package config must advertise the vendored headers' version.

    The bundle's generated headers `static_assert` on an exact FlatBuffers
    version, so a config that claims a different one would hand the consumer a
    package that cannot compile the archive it shipped in.
    """
    base_header = texts.get("include/flatbuffers/base.h")
    config = texts.get("share/cmake/FlatBuffers/FlatBuffersConfig.cmake")
    if base_header is None or config is None:
        raise ValueError("archive is missing the vendored FlatBuffers runtime")
    version = _flatbuffers_header_version(base_header)
    if f'set(FlatBuffers_VERSION "{version}")' not in config:
        raise ValueError(
            f"vendored FlatBuffersConfig.cmake does not advertise headers version {version}"
        )


def verify_archive(
    archive_path: Path,
    expected_path: Path,
    zstd_path: Optional[Path],
    core_abi: int,
    readelf_path: Optional[Path] = None,
) -> None:
    actual, manifest, symlinks, texts, libraries, digests = _extract_archive(
        archive_path, zstd_path
    )
    core_so = core_shared_object_name(core_abi)
    core_elf = libraries.get(f"lib/{core_so}")
    static_archive = libraries.get("lib/libaorta_cpp.a")
    verify_exact_members(actual, _read_expected(expected_path))
    _verify_internal_manifest(actual, manifest, digests, symlinks)

    allowed_prefixes = (
        "include/",
        "lib/",
        "share/cmake/Aorta/",
        "share/cmake/FlatBuffers/",
    )
    allowed_root_files = {"MANIFEST.json", "PROVENANCE.json", "LICENSE",
        "NOTICE",
        "THIRD_PARTY_NOTICES.md",
        "FLATBUFFERS-LICENSE", "THIRD_PARTY_COMPONENTS.md"}
    unexpected = {
        name
        for name in actual
        if name not in allowed_root_files and not name.startswith(allowed_prefixes)
    }
    if unexpected:
        raise ValueError("unexpected SDK member classes: " + ", ".join(sorted(unexpected)))

    required = {
        "include/aorta.h",
        "include/aorta/aorta.h",
        "include/aorta/contextstore.h",
        "include/aorta/typed_publish.h",
        # `aorta/aorta.h` includes this one; a non-recursive header glob used to
        # drop it and produce a bundle whose public header would not compile.
        "include/aorta/detail/reentrancy_check.h",
        "LICENSE",
        "NOTICE",
        "THIRD_PARTY_NOTICES.md",
        "FLATBUFFERS-LICENSE",
        "THIRD_PARTY_COMPONENTS.md",
        "lib/libaorta_cpp.a",
        "share/cmake/Aorta/AortaConfig.cmake",
        "share/cmake/Aorta/AortaConfigVersion.cmake",
        "share/cmake/Aorta/AortaSchemaTargets.cmake",
        "share/cmake/FlatBuffers/FlatBuffersConfig.cmake",
        "share/cmake/FlatBuffers/FlatBuffersConfigVersion.cmake",
        "include/flatbuffers/flatbuffers.h",
        "include/flatbuffers/base.h",
        "include/aorta_base_generated.h",
        "include/action_common_generated.h",
        "PROVENANCE.json",
    }
    missing = required - actual
    if missing:
        raise ValueError("missing SDK contract members: " + ", ".join(sorted(missing)))
    _verify_vendored_flatbuffers(texts)
    verify_advertised_core_abi(core_abi, actual, symlinks, texts)
    verify_glibc_baseline(libraries)
    if static_archive is not None:
        verify_static_archive_is_portable(static_archive)
    if not ({"lib/libaorta_cpp.so", "lib/libaorta_cpp.dylib", "lib/aorta_cpp.dll"} & actual):
        raise ValueError("missing shared aorta_cpp library")
    if not ({
        f"lib/{core_so}",
        "lib/libaorta_core.dylib",
        "lib/aorta_core.dll",
    } & actual):
        raise ValueError(f"missing ABI-{core_abi} shared Core library")
    if core_elf is not None:
        expected_symlinks = {"lib/libaorta_core.so": core_so}
        if symlinks != expected_symlinks:
            raise ValueError(f"unexpected Core development symlink: {symlinks}")
        _verify_elf_soname(core_elf, core_abi, readelf_path)
    elif symlinks:
        raise ValueError(f"unexpected SDK symlinks: {symlinks}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--expected-members", type=Path)
    parser.add_argument("--zstd", type=Path)
    parser.add_argument("--readelf", type=Path)
    parser.add_argument("--release-assets-dir", type=Path)
    parser.add_argument("--release-version")
    parser.add_argument(
        "--release-triplet",
        action="append",
        choices=CPP_RELEASE_TRIPLETS,
    )
    args = parser.parse_args()

    # Parsed from core/src/ffi/c_api/mod.rs on every run: the gate never takes
    # the ABI it checks against from the artifacts it is checking.
    core_abi = read_core_abi()

    if args.release_assets_dir is not None:
        if args.release_version is None:
            parser.error("--release-assets-dir requires --release-version")
        triplets = tuple(args.release_triplet or CPP_RELEASE_TRIPLETS)
        verify_release_assets(
            args.release_assets_dir,
            args.release_version,
            core_abi,
            triplets,
        )
        return 0

    if args.archive is None or args.expected_members is None:
        parser.error("archive verification requires --archive, --expected-members, ")
    verify_archive(
        args.archive,
        args.expected_members,
        args.zstd,
        core_abi,
        args.readelf,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
