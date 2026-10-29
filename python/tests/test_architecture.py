"""Architecture guard: the Python SDK must go through the L2 C-ABI only.

In the fixed layer model (L1 Rust Core -> L2 C-ABI -> L3 SDKs), the C++ and
Python SDKs bind only through the L2 C-ABI: the Python SDK MUST bind via the
C-ABI and must NOT depend on any Rust crate (no PyO3/maturin). The repo's Rust
``aorta-lint`` covers ``rust/`` + ``tools/`` but not ``python/`` (it is a
cargo-only workflow), so this layer rule is enforced here, in the test suite CI
already runs (Python SDK design proposal §2.1).
"""

from __future__ import annotations

import json
import os
import re

_PYTHON_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SKIP_DIRS = {".venv-dev", "__pycache__", "tests", ".pytest_cache"}
_TEXT_SUFFIXES = (".py", ".bazel", ".bzl", ".toml", ".txt", ".cfg", ".ini")


def _iter_source_files():
    for root, dirs, files in os.walk(_PYTHON_ROOT):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in files:
            yield os.path.join(root, name)


def test_no_cargo_manifest_under_python():
    cargo = [f for f in _iter_source_files() if os.path.basename(f) == "Cargo.toml"]
    assert cargo == [], f"Python SDK must not carry a Rust crate manifest: {cargo}"


def test_no_native_binding_framework_references():
    # No PyO3 / maturin anywhere in the Python SDK sources or build files: those
    # would mean a direct aorta_core crate dependency, violating the layer model.
    pattern = re.compile(r"py[o]3|maturin", re.IGNORECASE)
    offenders = []
    for path in _iter_source_files():
        if not path.endswith(_TEXT_SUFFIXES):
            continue
        try:
            text = open(path, "r", encoding="utf-8").read()
        except (UnicodeDecodeError, OSError):  # pragma: no cover - no binaries here
            continue
        if pattern.search(text):
            offenders.append(path)
    assert offenders == [], f"forbidden native-binding references: {offenders}"


# ── Python packaging guard: SDK vs generated-msgs namespace collisions ────────
# Keep SDK-local package/module structural checks and the base-schema census.
# External product/custom ownership is verified against the actual composed
# wheel RECORDs in //message/tools:python_wheel_composition_test. Release trees
# omit generated ancestor initializers; the legacy development-tree rule still
# reserves SDK subpackages explicitly. This local census is not a product gate.

_REPO_ROOT = os.path.dirname(_PYTHON_ROOT)
_MESSAGE_ROOT = os.path.join(_REPO_ROOT, "message")
_BAZEL_BIN = os.path.join(_REPO_ROOT, "bazel-bin")
_REGISTRY_MANIFEST = os.path.join(
    _BAZEL_BIN, "message", "schemas_registry_manifest.json"
)
_RULES_BZL = os.path.join(_MESSAGE_ROOT, "tools", "flatbuffers_rules.bzl")
_AORTA_NS_RE = re.compile(r"^\s*namespace\s+aorta\.([A-Za-z0-9_]+)", re.MULTILINE)
_INCLUDE_RE = re.compile(r'^\s*include\s+"([^"]+)"', re.MULTILINE)


def _registry_entries():
    """Every schema registry entry from the current Bazel registry."""
    with open(_REGISTRY_MANIFEST, "r", encoding="utf-8") as handle:
        entries = json.load(handle)["schema_definitions"]
    assert entries, "registry manifest carries no schema_definitions"
    return entries


def _dep_repo_rel(dep, pkg):
    """A `dep_fbs` label -> its repo-relative canonical path.

    `":x.fbs"` is package-local; `"//message/schemas/<d>/<f>:x.fbs"` is absolute.
    """
    if dep.startswith(":"):
        return os.path.join(pkg[len("//") :], dep[1:])
    label, _, name = dep.partition(":")
    return os.path.join(label[len("//") :], name)


def _resolve_canonical(repo_rel):
    """Absolute path of a canonical `message/schemas/...` slot, or None.

    Retained packages own their slot with a tracked file; a MIGRATED package's
    slot is owned by the external root's copy output under `bazel-bin`. Both
    are the same bytes the build compiles, so either is a valid read.
    """
    for base in (_REPO_ROOT, _BAZEL_BIN):
        candidate = os.path.join(base, repo_rel)
        if os.path.isfile(candidate):
            return candidate
    return None


def _include_slot(inc, owner_slot):
    """An `include "..."` -> its repo-relative canonical slot, or None.

    flatc searches the including schema's OWN directory as well as the
    `message/` root, and both spellings are in use (`"schemas/system/
    aorta_base.fbs"` across packages, a bare `"Pose.fbs"` inside
    foxglove_schema). Resolving only against `message/` — as this walk used to
    — silently dropped every same-directory include.
    """
    for repo_rel in (
        os.path.join(os.path.dirname(owner_slot), inc),
        os.path.join("message", inc),
    ):
        repo_rel = os.path.normpath(repo_rel)
        if _resolve_canonical(repo_rel) is not None:
            return repo_rel
    return None


def _schema_namespace_roots():
    """`aorta.<root>` namespace roots flatc can emit into the aorta_msgs wheel.

    flatc generates modules for a schema's includes too (that is where the
    `aorta/sys/` header modules come from), so the walk follows BOTH the
    registry's declared `dep_fbs` edges and the file's own `include` lines —
    two independently-derived views of the same graph, so a one-sided drift
    cannot shrink the census.
    """
    by_slot = {
        os.path.join(entry["pkg"][len("//") :], entry["root_fbs"]): entry
        for entry in _registry_entries()
    }
    queue = [slot for slot, entry in by_slot.items() if entry["python"]]
    assert queue, "no python-enabled registry entries — manifest format changed?"

    roots = set()
    seen = set()
    unresolved = []
    while queue:
        slot = os.path.normpath(queue.pop())
        if slot in seen:
            continue
        seen.add(slot)
        path = _resolve_canonical(slot)
        if path is None:
            unresolved.append(slot)
            continue
        text = open(path, "r", encoding="utf-8").read()
        roots.update(_AORTA_NS_RE.findall(text))
        entry = by_slot.get(slot)
        if entry is not None:
            for dep in entry["dep_fbs"]:
                queue.append(_dep_repo_rel(dep, entry["pkg"]))
        for inc in _INCLUDE_RE.findall(text):
            resolved = _include_slot(inc, slot)
            if resolved is None:
                unresolved.append(f"{slot} includes {inc!r}")
                continue
            queue.append(resolved)

    # Fail CLOSED. A missing canonical slot means the census covered less than
    # the registry declares, which is precisely how this guard can report green
    # while the wheel it protects is unguarded.
    assert not unresolved, (
        "these registry-declared schemas resolve to no canonical .fbs, so the "
        "namespace census is incomplete: "
        + str(sorted(unresolved))
        + ". Migrated families live in an external schema module and materialize "
        "into bazel-bin — run `bazel build //message:schemas` first."
    )
    return roots


def _sdk_toplevel_names():
    sdk = os.path.join(_PYTHON_ROOT, "aorta")
    names = set()
    for entry in os.listdir(sdk):
        full = os.path.join(sdk, entry)
        if os.path.isdir(full) and entry != "__pycache__":
            names.add(entry)
        elif entry.endswith(".py") and entry != "__init__.py":
            names.add(entry[: -len(".py")])
    return names


def _declared_sdk_owned_subpackages():
    text = open(_RULES_BZL, "r", encoding="utf-8").read()
    block = re.search(r"_SDK_OWNED_SUBPACKAGES\s*=\s*\[(.*?)\]", text, re.DOTALL)
    assert block is not None, "_SDK_OWNED_SUBPACKAGES not found in flatbuffers_rules.bzl"
    return set(re.findall(r'"([^"]+)"', block.group(1)))


def test_colliding_sdk_names_are_packages_not_flat_modules():
    collisions = _schema_namespace_roots() & _sdk_toplevel_names()
    flat = [
        name
        for name in sorted(collisions)
        if not os.path.isfile(os.path.join(_PYTHON_ROOT, "aorta", name, "__init__.py"))
    ]
    assert flat == [], (
        "these SDK modules share a name with a schema namespace root and MUST be "
        "packages (aorta/<name>/__init__.py), or the generated msgs tree will "
        f"shadow them in a shared site-packages: {flat}"
    )


def test_colliding_sdk_names_are_declared_in_the_msgs_tree_rule():
    collisions = _schema_namespace_roots() & _sdk_toplevel_names()
    declared = _declared_sdk_owned_subpackages()
    missing = sorted(collisions - declared)
    assert missing == [], (
        "add these to _SDK_OWNED_SUBPACKAGES in message/tools/flatbuffers_rules.bzl "
        "so the aorta_msgs tree drops the generated __init__.py: " + str(missing)
    )
    # The declared set may name namespace roots that only exist in an external
    # schema module built with the same tree rule (e.g. `capture`), so the
    # census cannot decide staleness; it can and does require that every
    # declared name is a real SDK package, or the msgs tree would drop an
    # `__init__.py` that nothing replaces.
    not_packages = sorted(
        name
        for name in declared
        if not os.path.isfile(os.path.join(_PYTHON_ROOT, "aorta", name, "__init__.py"))
    )
    assert not_packages == [], (
        "_SDK_OWNED_SUBPACKAGES lists names that are not SDK packages "
        "(aorta/<name>/__init__.py): " + str(not_packages)
    )
