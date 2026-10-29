import json
import tempfile
import unittest
from pathlib import Path

from tools.release.sdk_bundles import (
    _verify_internal_manifest,
    advertised_core_abis,
    elf_required_glibc_versions,
    elf_section_names,
    elf_undefined_symbols,
    expected_release_asset_names,
    iter_ar_members,
    read_core_abi,
    verify_advertised_core_abi,
    verify_exact_members,
    verify_glibc_baseline,
    verify_static_archive_is_portable,
)

# What core/src/ffi/c_api/mod.rs declares today; the fixtures below are built
# from it so no test restates the number.
CORE_ABI = read_core_abi()
CORE_SO = f"libaorta_core.so.{CORE_ABI}"


def _elf_object(section_names, undefined_symbols=()):
    """Minimal little-endian ELF64 relocatable with the given sections/symbols."""
    symbol_names = list(undefined_symbols)
    names = [""] + list(section_names)
    if symbol_names:
        names += [".symtab", ".strtab"]
    names += [".shstrtab"]

    shstrtab = b"\x00".join(name.encode() for name in names) + b"\x00"
    name_offsets = {}
    cursor = 0
    for name in names:
        name_offsets[name] = cursor
        cursor += len(name) + 1

    strtab = b"\x00"
    symbol_offsets = {}
    for symbol in symbol_names:
        symbol_offsets[symbol] = len(strtab)
        strtab += symbol.encode() + b"\x00"

    symtab = bytearray(24)  # reserved null symbol
    for symbol in symbol_names:
        entry = bytearray(24)
        entry[0:4] = symbol_offsets[symbol].to_bytes(4, "little")
        entry[4] = 0x10  # STB_GLOBAL | STT_NOTYPE
        entry[6:8] = (0).to_bytes(2, "little")  # SHN_UNDEF
        symtab += entry

    header_size = 64
    entry_size = 64
    count = len(names)
    payload_offset = header_size + entry_size * count

    blobs = {".shstrtab": shstrtab}
    if symbol_names:
        blobs[".symtab"] = bytes(symtab)
        blobs[".strtab"] = strtab

    offsets = {}
    cursor = payload_offset
    for name in names:
        if name in blobs:
            offsets[name] = cursor
            cursor += len(blobs[name])

    header = bytearray(header_size)
    header[0:4] = b"\x7fELF"
    header[4] = 2  # ELFCLASS64
    header[5] = 1  # little endian
    header[0x28:0x30] = header_size.to_bytes(8, "little")  # e_shoff
    header[0x3A:0x3C] = entry_size.to_bytes(2, "little")
    header[0x3C:0x3E] = count.to_bytes(2, "little")
    header[0x3E:0x40] = names.index(".shstrtab").to_bytes(2, "little")

    sections = bytearray()
    for name in names:
        section = bytearray(entry_size)
        section[0:4] = name_offsets[name].to_bytes(4, "little")
        if name == ".symtab":
            section[4:8] = (2).to_bytes(4, "little")  # SHT_SYMTAB
            section[0x28:0x2C] = names.index(".strtab").to_bytes(4, "little")
        if name in blobs:
            section[0x18:0x20] = offsets[name].to_bytes(8, "little")
            section[0x20:0x28] = len(blobs[name]).to_bytes(8, "little")
        else:
            # Section names still have to resolve, so point at the string table.
            section[0x18:0x20] = offsets[".shstrtab"].to_bytes(8, "little")
        sections += section

    body = b"".join(blobs[name] for name in names if name in blobs)
    return bytes(header) + bytes(sections) + body


def _ar_archive(members):
    out = bytearray(b"!<arch>\n")
    for name, payload in members:
        out += f"{name + '/':<16}".encode()
        out += b"0" * 12 + b"0" * 6 + b"0" * 6 + b"100644  "
        out += f"{len(payload):<10}".encode() + b"`\n"
        out += payload
        if len(payload) % 2:
            out += b"\n"
    return bytes(out)


class ManifestMembershipTest(unittest.TestCase):
    def test_rejects_missing_member(self):
        with self.assertRaisesRegex(ValueError, "missing: include/aorta.h"):
            verify_exact_members(
                actual={"MANIFEST.json"},
                expected={"MANIFEST.json", "include/aorta.h"},
            )

    def test_rejects_unexpected_member(self):
        with self.assertRaisesRegex(ValueError, "unexpected: private.fbs"):
            verify_exact_members(
                actual={"MANIFEST.json", "private.fbs"},
                expected={"MANIFEST.json"},
            )

    def test_release_asset_names_carry_the_distro_and_abi_tokens(self):
        self.assertEqual(
            expected_release_asset_names("2026.8.9", core_abi=12),
            {
                "aorta-cpp-sdk-2026.8.9-abi12-ubuntu22.04-aarch64.tar.zst",
                "aorta-cpp-sdk-2026.8.9-abi12-ubuntu22.04-aarch64.tar.zst.sha256",
                "aorta-cpp-sdk-2026.8.9-abi12-ubuntu22.04-x86_64.tar.zst",
                "aorta-cpp-sdk-2026.8.9-abi12-ubuntu22.04-x86_64.tar.zst.sha256",
            },
        )


def _bundle_texts(core_abi):
    """The members through which a bundle advertises its Core ABI."""
    return {
        "PROVENANCE.json": json.dumps({"aorta_commit": "", "core_abi": core_abi}) + "\n",
        "share/cmake/Aorta/AortaConfig.cmake": (
            f'set(Aorta_VERSION "{core_abi}.0.0")\n'
            f"set(Aorta_CORE_ABI_VERSION {core_abi})\n"
            f'set(_AORTA_CORE_SHARED "${{_AORTA_PREFIX}}/lib/libaorta_core.so.{core_abi}")\n'
        ),
        "share/cmake/Aorta/AortaConfigVersion.cmake": (
            f'set(PACKAGE_VERSION "{core_abi}.0.0")\n'
            f"if(PACKAGE_FIND_VERSION_MAJOR EQUAL {core_abi})\n"
        ),
    }


class CoreAbiConsistencyTest(unittest.TestCase):
    """Regression guard: a bundle stamped with an ABI
    other than `AORTA_ABI_VERSION` must be rejected wherever it says so."""

    def _members(self, core_abi):
        actual = {"MANIFEST.json", f"lib/libaorta_core.so.{core_abi}", "lib/libaorta_core.so"}
        symlinks = {"lib/libaorta_core.so": f"libaorta_core.so.{core_abi}"}
        return actual, symlinks, _bundle_texts(core_abi)

    def test_reads_every_advertised_location(self):
        actual, symlinks, texts = self._members(12)
        self.assertEqual(
            advertised_core_abis(actual, symlinks, texts),
            {
                "lib/libaorta_core.so.<N> member": {12},
                "lib/libaorta_core.so symlink target": {12},
                "PROVENANCE.json": {12},
                "share/cmake/Aorta/AortaConfig.cmake": {12},
                "share/cmake/Aorta/AortaConfigVersion.cmake": {12},
            },
        )

    def test_accepts_a_bundle_advertising_the_core_abi(self):
        actual, symlinks, texts = self._members(CORE_ABI)
        verify_advertised_core_abi(CORE_ABI, actual, symlinks, texts)

    def test_rejects_a_bundle_advertising_a_different_abi(self):
        # Exactly the shipped defect: Core at N, every bundle surface at N-2.
        stale = CORE_ABI - 2
        actual, symlinks, texts = self._members(stale)
        with self.assertRaises(ValueError) as caught:
            verify_advertised_core_abi(CORE_ABI, actual, symlinks, texts)
        message = str(caught.exception)
        self.assertIn(f"other than {CORE_ABI} (AORTA_ABI_VERSION)", message)
        for where in (
            "PROVENANCE.json",
            "lib/libaorta_core.so symlink target",
            "lib/libaorta_core.so.<N> member",
            "share/cmake/Aorta/AortaConfig.cmake",
            "share/cmake/Aorta/AortaConfigVersion.cmake",
        ):
            self.assertIn(f"{where} says [{stale}]", message)

    def test_rejects_one_stale_surface(self):
        # A partial bump (SONAME moved, package config not) is still a lie.
        actual, symlinks, texts = self._members(CORE_ABI)
        texts["share/cmake/Aorta/AortaConfig.cmake"] = _bundle_texts(CORE_ABI - 2)[
            "share/cmake/Aorta/AortaConfig.cmake"
        ]
        with self.assertRaisesRegex(ValueError, r"AortaConfig\.cmake says"):
            verify_advertised_core_abi(CORE_ABI, actual, symlinks, texts)

    def test_rejects_a_bundle_that_advertises_nothing(self):
        actual, symlinks, texts = self._members(CORE_ABI)
        del texts["PROVENANCE.json"]
        with self.assertRaisesRegex(ValueError, "PROVENANCE.json does not advertise"):
            verify_advertised_core_abi(CORE_ABI, actual, symlinks, texts)

    def test_expected_asset_names_follow_the_core_abi(self):
        for name in expected_release_asset_names("1.2.3", CORE_ABI):
            self.assertIn(f"-abi{CORE_ABI}-", name)


class InternalManifestTest(unittest.TestCase):
    ACTUAL = {"MANIFEST.json", f"lib/{CORE_SO}", "lib/libaorta_core.so"}
    DIGESTS = {f"lib/{CORE_SO}": ("a" * 64, 12)}
    SYMLINKS = {"lib/libaorta_core.so": CORE_SO}
    MANIFEST = {
        "format_version": 2,
        "members": [
            {"path": f"lib/{CORE_SO}", "sha256": "a" * 64, "size": 12},
            {"path": "lib/libaorta_core.so", "symlink": CORE_SO},
        ],
    }

    def _verify(self, manifest):
        _verify_internal_manifest(self.ACTUAL, manifest, self.DIGESTS, self.SYMLINKS)

    def test_accepts_a_manifest_that_describes_the_archive(self):
        self._verify(self.MANIFEST)

    def test_rejects_format_version_1(self):
        manifest = dict(self.MANIFEST, format_version=1)
        with self.assertRaisesRegex(ValueError, "format_version 2"):
            self._verify(manifest)

    def test_rejects_wrong_digest(self):
        members = [dict(self.MANIFEST["members"][0], sha256="b" * 64)]
        members.append(self.MANIFEST["members"][1])
        with self.assertRaisesRegex(ValueError, rf"{CORE_SO} \(sha256\)"):
            self._verify(dict(self.MANIFEST, members=members))

    def test_rejects_wrong_size(self):
        members = [dict(self.MANIFEST["members"][0], size=13)]
        members.append(self.MANIFEST["members"][1])
        with self.assertRaisesRegex(ValueError, rf"{CORE_SO} \(size\)"):
            self._verify(dict(self.MANIFEST, members=members))

    def test_rejects_wrong_symlink_target(self):
        members = [self.MANIFEST["members"][0], {"path": "lib/libaorta_core.so", "symlink": "elsewhere"}]
        with self.assertRaisesRegex(ValueError, r"libaorta_core.so \(symlink target\)"):
            self._verify(dict(self.MANIFEST, members=members))

    def test_rejects_duplicate_member(self):
        members = list(self.MANIFEST["members"]) + [self.MANIFEST["members"][0]]
        with self.assertRaisesRegex(ValueError, "duplicate MANIFEST.json member"):
            self._verify(dict(self.MANIFEST, members=members))


class GlibcBaselineTest(unittest.TestCase):
    """Fixtures model release archives that were actually published."""

    def test_plain_object_archive_passes(self):
        archive = _ar_archive([("common.o", _elf_object([".text"]))])
        verify_glibc_baseline({"lib/libaorta_cpp.a": archive})

    def test_reads_undefined_symbols(self):
        payload = _elf_object([".text"], undefined_symbols=["__isoc23_strtol", "memcpy"])
        self.assertEqual(
            elf_undefined_symbols(payload), {"__isoc23_strtol", "memcpy"}
        )

    def test_rejects_post_baseline_symbol_in_static_archive(self):
        # `__isoc23_strtol` is exactly what previously published archives
        # referenced: glibc 2.38 introduced it, 22.04 cannot resolve it.
        archive = _ar_archive(
            [("common.o", _elf_object([".text"], undefined_symbols=["__isoc23_strtol"]))]
        )
        with self.assertRaisesRegex(ValueError, "__isoc23_strtol"):
            verify_glibc_baseline({"lib/libaorta_cpp.a": archive})

    def test_non_elf_payload_requires_nothing(self):
        self.assertEqual(elf_required_glibc_versions(b"not an elf"), set())
        self.assertEqual(elf_undefined_symbols(b"not an elf"), set())


class StaticArchivePortabilityTest(unittest.TestCase):
    def test_reads_ar_members(self):
        archive = _ar_archive([("a.o", b"1234"), ("b.o", b"567")])
        self.assertEqual(
            [name for name, _ in iter_ar_members(archive)], ["a.o", "b.o"]
        )

    def test_reads_elf_section_names(self):
        self.assertIn(".text", elf_section_names(_elf_object([".text"])))

    def test_non_elf_member_has_no_sections(self):
        self.assertEqual(elf_section_names(b"not an elf"), [])

    def test_accepts_plain_objects(self):
        archive = _ar_archive([("common.o", _elf_object([".text", ".data"]))])
        verify_static_archive_is_portable(archive)

    def test_rejects_lto_bytecode(self):
        archive = _ar_archive(
            [
                ("common.o", _elf_object([".text", ".gnu.lto_.odr_types.2fb3"])),
                ("node.o", _elf_object([".text"])),
            ]
        )
        with self.assertRaisesRegex(ValueError, "carries LTO bytecode: common.o"):
            verify_static_archive_is_portable(archive)

    def test_rejects_empty_archive(self):
        with self.assertRaisesRegex(ValueError, "has no members"):
            verify_static_archive_is_portable(b"!<arch>\n")


if __name__ == "__main__":
    unittest.main()
