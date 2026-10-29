"""Tests for aorta._ffi.loader — discovery, load, and the runtime ABI guard."""

from __future__ import annotations

import ctypes

import pytest

from aorta._ffi import loader
from aorta._ffi.loader import AbiGuardError, AortaLoadError


class _FakeFn:
    def __init__(self, impl=None, ret=0):
        self.argtypes = None
        self.restype = None
        self._impl = impl
        self._ret = ret

    def __call__(self, *args):
        if self._impl is not None:
            return self._impl(*args)
        return self._ret


class _FakeLib:
    """Minimal stand-in for a ctypes.CDLL with a configurable symbol set."""

    def __init__(
        self, symbols, now_ns_impl=None, now_ns_ret=0, abi_version=None
    ):
        if abi_version is None:
            abi_version = loader.EXPECTED_ABI_VERSION
        for sym in symbols:
            setattr(self, sym, _FakeFn())
        if "aorta_abi_version" in symbols:
            self.aorta_abi_version = _FakeFn(ret=abi_version)
        if "aorta_now_ns" in symbols:
            self.aorta_now_ns = _FakeFn(impl=now_ns_impl, ret=now_ns_ret)


@pytest.fixture(autouse=True)
def _reset_loader_cache():
    saved = loader._lib
    loader._lib = None
    yield
    loader._lib = None
    loader._lib = saved


# --- dynamic filenames + candidate discovery ------------------------------

def test_dynamic_lib_filenames_darwin(monkeypatch):
    monkeypatch.setattr(loader.sys, "platform", "darwin")
    assert loader._dynamic_lib_filenames() == ["libaorta_core.dylib"]


def test_dynamic_lib_filenames_linux(monkeypatch):
    monkeypatch.setattr(loader.sys, "platform", "linux")
    assert loader._dynamic_lib_filenames() == ["libaorta_core.so"]


def test_package_relative_candidates_nonempty():
    candidates = loader._package_relative_candidates()
    assert candidates
    assert all(c.endswith((".dylib", ".so")) for c in candidates)


def test_candidate_paths_honours_env_first(monkeypatch):
    monkeypatch.setenv("AORTA_CORE_FFI_LIB", "/tmp/explicit/libaorta_core.dylib")
    candidates = loader._candidate_paths()
    assert candidates[0] == "/tmp/explicit/libaorta_core.dylib"


def test_candidate_paths_without_env(monkeypatch):
    monkeypatch.delenv("AORTA_CORE_FFI_LIB", raising=False)
    monkeypatch.setattr(loader.ctypes.util, "find_library", lambda name: None)
    candidates = loader._candidate_paths()
    # only the package-relative candidates remain
    assert candidates == loader._package_relative_candidates()


def test_candidate_paths_appends_find_library(monkeypatch):
    monkeypatch.delenv("AORTA_CORE_FFI_LIB", raising=False)
    monkeypatch.setattr(
        loader.ctypes.util, "find_library", lambda name: "/sys/lib/" + name
    )
    candidates = loader._candidate_paths()
    assert "/sys/lib/aorta_core" in candidates


# --- runtime ABI guard ----------------------------------------------------

def test_expected_abi_version_is_thirteen():
    assert loader.EXPECTED_ABI_VERSION == 13


def test_guard_rejects_missing_symbol():
    incomplete = list(loader._REQUIRED_SYMBOLS)[:-1]
    fake = _FakeLib(incomplete)
    with pytest.raises(AbiGuardError, match="missing expected"):
        loader._run_abi_guard(fake)


def test_guard_rejects_abi_version_mismatch():
    fake = _FakeLib(loader._REQUIRED_SYMBOLS, abi_version=999)
    with pytest.raises(AbiGuardError, match="ABI version 999"):
        loader._run_abi_guard(fake)


def test_guard_rejects_now_ns_error_rc():
    fake = _FakeLib(loader._REQUIRED_SYMBOLS, now_ns_ret=-3)
    with pytest.raises(AbiGuardError, match="rc=-3"):
        loader._run_abi_guard(fake)


def test_guard_accepts_zero_timestamp():
    # system_time_ns can legitimately return 0 (clock error / sim time); the
    # guard must NOT reject it — only rc matters.
    fake = _FakeLib(loader._REQUIRED_SYMBOLS, now_ns_ret=0)
    assert loader._run_abi_guard(fake) is None


def test_guard_passes_on_real_library(lib):
    # The real cdylib loaded via the fixture already passed the guard; a direct
    # re-run must also pass.
    assert loader._run_abi_guard(lib) is None
    out = ctypes.c_int64(0)
    assert lib.aorta_now_ns(ctypes.byref(out)) == 0


# --- load_library ---------------------------------------------------------

def test_load_library_real_and_cached(cdylib_path):
    first = loader.load_library()
    second = loader.load_library()
    assert isinstance(first, ctypes.CDLL)
    assert first is second  # cached


def test_load_library_force_reload(cdylib_path):
    first = loader.load_library()
    reloaded = loader.load_library(force_reload=True)
    assert isinstance(reloaded, ctypes.CDLL)
    assert reloaded is loader._lib


def test_load_library_no_candidates(monkeypatch):
    monkeypatch.setattr(loader, "_candidate_paths", list)
    with pytest.raises(AortaLoadError, match="could not locate"):
        loader.load_library()


def test_load_library_all_candidates_fail(monkeypatch):
    monkeypatch.setattr(
        loader, "_candidate_paths", lambda: ["/no/such/libaorta_core.dylib"]
    )
    with pytest.raises(AortaLoadError, match="failed to load"):
        loader.load_library()


def test_library_helper_loads(cdylib_path):
    assert loader.library() is loader.load_library()
