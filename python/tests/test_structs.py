"""Tests for aorta._ffi.structs — layout lock + drift vs core's abi_assert.rs."""

from __future__ import annotations

import ctypes
import os
import re

import pytest

from aorta._ffi import loader, structs
from aorta._ffi.loader import AbiGuardError

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_ABI_ASSERT = os.path.join(_REPO_ROOT, "core", "src", "ffi", "abi_assert.rs")
_CPP_LOCK = os.path.join(_REPO_ROOT, "cpp", "test", "abi_layout_test.cc")


def _parse_abi_assert_sizes() -> dict:
    if not os.path.exists(_ABI_ASSERT):  # pragma: no cover - present in-repo
        pytest.skip("abi_assert.rs not available (non-source build)")
    text = open(_ABI_ASSERT, "r", encoding="utf-8").read()
    pattern = re.compile(r"size_of::<(\w+)>\(\)\s*==\s*(\d+)")
    return {name: int(size) for name, size in pattern.findall(text)}


def _parse_cpp_offset_lock() -> dict:
    """Parse cpp/test/abi_layout_test.cc into {type: {field: offset}}."""
    if not os.path.exists(_CPP_LOCK):  # pragma: no cover - present in-repo
        pytest.skip("abi_layout_test.cc not available (non-source build)")
    text = open(_CPP_LOCK, "r", encoding="utf-8").read()
    pattern = re.compile(r"offsetof\((\w+),\s*(\w+)\)\s*==\s*(\d+)")
    offsets: dict = {}
    for type_name, field, off in pattern.findall(text):
        offsets.setdefault(type_name, {})[field] = int(off)
    return offsets


# A few fields are named differently in the cbindgen C header than in this
# binding's ctypes mirrors; map mirror-name -> C++/cbindgen-name for the cross-check.
_CPP_FIELD_ALIAS = {
    "aorta_string_view_t": {"ptr": "data"},
    "aorta_bytes_slice_t": {"ptr": "data"},
    "aorta_owned_bytes_slice_t": {"ptr": "data"},
    "aorta_owned_bytes_t": {"owner": "internal"},
}


def test_check_layouts_clean():
    assert structs.check_layouts() == []


def test_pub_options_v11_layout():
    assert ctypes.sizeof(structs.AortaPubOptions) == 40
    assert structs.AortaPubOptions.schema_root_type.offset == 32


@pytest.mark.parametrize("rust_name,ctype", structs.MIRRORS)
def test_sizeof_matches_expected(rust_name, ctype):
    assert ctypes.sizeof(ctype) == structs.EXPECTED_SIZES[rust_name]


@pytest.mark.parametrize("rust_name", list(structs.EXPECTED_SIZES))
def test_no_drift_vs_abi_assert(rust_name):
    abi_sizes = _parse_abi_assert_sizes()
    assert rust_name in abi_sizes, f"{rust_name} not asserted in abi_assert.rs"
    assert abi_sizes[rust_name] == structs.EXPECTED_SIZES[rust_name]


def test_every_mirror_has_expected_size_entry():
    assert {name for name, _ in structs.MIRRORS} == set(structs.EXPECTED_SIZES)


@pytest.mark.parametrize("rust_name", list(structs.EXPECTED_OFFSETS))
def test_mirror_field_offsets_match_expected(rust_name):
    ctype = dict(structs.MIRRORS)[rust_name]
    assert structs.field_offsets(ctype) == structs.EXPECTED_OFFSETS[rust_name]


@pytest.mark.parametrize("rust_name", list(structs.EXPECTED_OFFSETS))
def test_no_field_offset_drift_vs_cpp_lock(rust_name):
    # Every mirrored struct MUST have a per-field offsetof lock in the C++ test,
    # and every field's offset must match (by name, via the alias map): this
    # is what catches a same-size reorder, not an ordered offset list.
    cpp_offsets = _parse_cpp_offset_lock()
    assert rust_name in cpp_offsets, f"{rust_name} lacks an offsetof lock in abi_layout_test.cc"
    alias = _CPP_FIELD_ALIAS.get(rust_name, {})
    for field, off in structs.EXPECTED_OFFSETS[rust_name].items():
        cpp_field = alias.get(field, field)
        assert cpp_offsets[rust_name].get(cpp_field) == off, (
            f"{rust_name}.{field} (C++ {cpp_field}) = "
            f"{cpp_offsets[rust_name].get(cpp_field)}, expected {off}"
        )


def test_offset_map_catches_same_size_reorder():
    # A name->offset map distinguishes a same-size field swap that an ordered
    # offset list [0, 8, 16] cannot.
    class _Reordered(ctypes.Structure):
        _fields_ = [
            ("slice_count", ctypes.c_size_t),
            ("slices", ctypes.c_void_p),
            ("total_len", ctypes.c_size_t),
        ]

    offsets = structs.field_offsets(_Reordered)
    assert sorted(offsets.values()) == [0, 8, 16]  # ordered list is unchanged...
    assert offsets != structs.EXPECTED_OFFSETS["aorta_bytes_view_t"]  # ...but the map is


def test_check_layouts_detects_size_mismatch(monkeypatch):
    monkeypatch.setitem(structs.EXPECTED_SIZES, "aorta_bytes_view_t", 999)
    mismatches = structs.check_layouts()
    assert any("aorta_bytes_view_t" in m and "expected 999" in m for m in mismatches)


def test_check_layouts_detects_offset_mismatch(monkeypatch):
    bad = dict(structs.EXPECTED_OFFSETS["aorta_msg_context_t"])
    bad["priority"] = 999
    monkeypatch.setitem(structs.EXPECTED_OFFSETS, "aorta_msg_context_t", bad)
    mismatches = structs.check_layouts()
    assert any("aorta_msg_context_t" in m and "field offsets" in m for m in mismatches)


def test_guard_rejects_layout_drift(lib, monkeypatch):
    monkeypatch.setattr(
        structs, "check_layouts", lambda: ["aorta_bytes_view_t: 999 != 24"]
    )
    with pytest.raises(AbiGuardError, match="layout drift"):
        loader._run_abi_guard(lib)


def test_nested_owned_bytes_embeds_view():
    # AortaOwnedBytes embeds AortaBytesView (24) + owner ptr (8) = 32.
    assert structs.AortaOwnedBytes.view.size == ctypes.sizeof(structs.AortaBytesView)
    assert ctypes.sizeof(structs.AortaOwnedBytes) == 32


# ── Action entry points take a BORROWED bytes view ───────────────────────────
# Six action C-ABI functions declare `*const aorta_bytes_view_t` on the Rust
# side. The Python bindings previously declared POINTER(AortaOwnedBytesView)
# for them — an ABI type mismatch that was non-crashing only because the
# 24-byte outer struct layouts coincide and the callers always passed exactly
# one slice (the owned slice stride is 32B vs borrowed 16B, so any multi-slice
# view would have been read corrupted). Lock both sides.

_ACTION_BORROWED_VIEW_FNS = {
    "aorta_action_client_send_goal": "action_client.rs",
    "aorta_action_client_cancel": "action_client.rs",
    "aorta_action_client_get_result": "action_client.rs",
    "aorta_action_client_get_result_with_timeout": "action_client.rs",
    "aorta_action_context_publish_feedback": "action_server.rs",
    "aorta_action_context_succeed": "action_server.rs",
}


def _rust_ffi_source(fname: str) -> str:
    path = os.path.join(_REPO_ROOT, "core", "src", "ffi", "c_api", fname)
    if not os.path.exists(path):  # pragma: no cover - present in-repo
        pytest.skip(f"{fname} not available (non-source build)")
    return open(path, "r", encoding="utf-8").read()


def test_rust_action_entry_points_declare_borrowed_views():
    for fn_name, fname in _ACTION_BORROWED_VIEW_FNS.items():
        text = _rust_ffi_source(fname)
        match = re.search(
            r"pub unsafe extern \"C\" fn " + re.escape(fn_name) + r"\((.*?)\)",
            text,
            re.DOTALL,
        )
        assert match, f"{fn_name} not found in {fname}"
        params = match.group(1)
        assert "*const aorta_bytes_view_t" in params, (
            f"{fn_name} no longer takes a borrowed bytes view — "
            "update the Python argtypes AND this lock together"
        )
        assert "owned_bytes_view" not in params.split("aorta_owned_bytes_t")[0], (
            f"{fn_name} request argument must be borrowed, not owned"
        )


def test_python_action_argtypes_use_borrowed_views(lib):
    from aorta._ffi import bindings

    configured = bindings.configure(lib)
    borrowed_ptr = ctypes.POINTER(structs.AortaBytesView)
    for fn_name in _ACTION_BORROWED_VIEW_FNS:
        fn = getattr(configured, fn_name)
        view_args = [
            t
            for t in fn.argtypes
            if getattr(t, "_type_", None) in (structs.AortaBytesView, structs.AortaOwnedBytesView)
        ]
        assert view_args, f"{fn_name} has no bytes-view argtype at all"
        for t in view_args:
            assert t is borrowed_ptr, (
                f"{fn_name} must declare POINTER(AortaBytesView) (borrowed) to "
                f"match the Rust C-ABI, got {t}"
            )
