"""Shared loader for the Aorta L2 C-ABI shared library (``libaorta_core``).

The Aorta Python SDK is a *pure-Python* ctypes binding over the Bazel-built
``rust_shared_library`` cdylib of the L2 C-ABI (see
``spec/Aorta_Python_SDK_Design_Proposal.md`` §2/§4). This module owns the one
true library-discovery + load path, plus a **runtime ABI guard** that runs once
on first load so the SDK never executes against a wrong / drifted library.

Discovery order (single source of truth — Python SDK design proposal §4.2/§9.0):

1. ``AORTA_CORE_FFI_LIB`` — explicit override (absolute path to the cdylib).
2. The bundled, package-relative cdylib shipped inside the wheel.
3. ``ctypes.util.find_library`` for the well-known names.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
import threading
from typing import List, Optional

__all__ = ["load_library", "library", "AortaLoadError", "AbiGuardError"]


class AortaLoadError(RuntimeError):
    """Raised when the Aorta C-ABI shared library cannot be located or loaded."""


class AbiGuardError(RuntimeError):
    """Raised when the loaded library fails the runtime ABI guard.

    A failure here means the cdylib is the wrong build, is missing expected
    symbols, or its struct layout diverges from this binding — running against
    it would be undefined behaviour, so we refuse at import time.
    """


# C-ABI version this build of the SDK was written against. Must equal
# ``aorta_abi_version()`` exported by the loaded ``libaorta_core``; a mismatch
# means a stale / wrong-arch library and is rejected before any other call.
# Bump in lockstep with core's ``AORTA_ABI_VERSION`` on any C-ABI break.
# 4→5: core added the route-readiness externs
# ``aorta_{publisher,client}_matching_status`` / ``_wait_for_matching``.
# 5→6: core added the raw-capture + schema-resolver surface
# (``aorta_capture_*`` / ``aorta_schema_resolver_*``).
# 6→7: QoS & buffering redesign — first layout-breaking bump. ``aorta_qos_t``
# reshaped 24→12 B, removed ``aorta_*_new_with_express`` /
# ``aorta_qos_preset_express|congestion`` symbols. ``aorta_sub_options_t``
# stays layout-frozen at 72 B (Python-mirrored structs never grow); the
# buffering knobs ride the new 16-byte
# ``aorta_sub_buffering_t`` behind the new ``aorta_subscriber_new_buffered``.
# A stale v6 cdylib must be rejected here (its ``aorta_qos_t`` layout no longer
# matches the ctypes mirrors in ``structs.py`` and it lacks the new symbols).
# 7→8: Aorta Flow — ONE release bump (the branch's interim
# per-step generations were collapsed to a single bump before release):
# arbiter spine, lane-facade hooks, task accounting, priority wiring,
# ``aorta_executor_post_resume_after``, and the
# arbiter/scope/readiness/action-ctx surface; see core ``mod.rs`` ABI
# ledger. No Python-visible layout changes. NOTE: Python ships NO Flow
# bindings yet — this pin only asserts the loaded core is the generation
# the ctypes mirrors were written against, not that Flow is reachable
# from Python.
# 8→9: SerialDomain — one additive symbol,
# ``aorta_executor_stop_flag_ptr`` (the C++ SDK stop-bridge arming edge;
# see core ``mod.rs`` ABI ledger). No Python-visible layout changes and
# no new Python bindings; the pin tracks the generation in lockstep.
# 10→11: stacked generation — C++ reentrancy guardrails (one additive
# symbol, ``aorta_reentrant_context``; no Python-visible layout change)
# plus ``aorta_pub_options_t.schema_root_type`` appended at offset 32;
# that struct grows from 32 B to 40 B.
# 11→12: generic UUIDv7 added ``aorta_uuid_v7`` and the Python binding.
# The pin tracks the Core generation in lockstep.
EXPECTED_ABI_VERSION = 13

# The minimum set of symbols the guard requires to be resolvable. This is not
# the full surface (later phases bind more); it is the floor that proves we
# loaded a real, current Aorta core. Keep alphabetised.
_REQUIRED_SYMBOLS = (
    "aorta_abi_version",
    "aorta_client_call",
    "aorta_ctx_is_key_declared",
    "aorta_last_error",
    "aorta_log_init_from_env",
    "aorta_node_new",
    "aorta_now_ns",
    "aorta_publisher_publish",
    "aorta_service_request_reply_error",
    "aorta_subscriber_new_buffered",
    "aorta_timer_is_running",
    "aorta_uuid_v7",
)

_SHARED_LIB_NAMES = ("aorta_core", "aorta_core_ffi", "aorta")

_lib: Optional[ctypes.CDLL] = None
_load_lock = threading.Lock()


def _dynamic_lib_filenames() -> List[str]:
    """Platform-specific cdylib filenames for ``aorta_core``."""
    if sys.platform == "darwin":
        return ["libaorta_core.dylib"]
    if sys.platform == "win32":  # pragma: no cover - not a supported target
        return ["aorta_core.dll"]
    return ["libaorta_core.so"]


def _package_relative_candidates() -> List[str]:
    """Paths where a bundled cdylib may live relative to this package."""
    here = os.path.dirname(os.path.abspath(__file__))
    # Inside the wheel the cdylib is bundled next to ``_ffi`` (this dir) and/or
    # at the package root (``python/aorta``); search both.
    roots = [here, os.path.dirname(here)]
    candidates: List[str] = []
    for root in roots:
        for name in _dynamic_lib_filenames():
            candidates.append(os.path.join(root, name))
    return candidates


def _candidate_paths() -> List[str]:
    candidates: List[str] = []

    env_path = os.environ.get("AORTA_CORE_FFI_LIB")
    if env_path:
        candidates.append(env_path)

    candidates.extend(_package_relative_candidates())

    for name in _SHARED_LIB_NAMES:
        found = ctypes.util.find_library(name)
        if found:
            candidates.append(found)

    return candidates


def _try_load(path: str) -> ctypes.CDLL:
    return ctypes.CDLL(path)


def _run_abi_guard(lib: ctypes.CDLL) -> None:
    """Verify the loaded library is a live, current Aorta core.

    1. Every required symbol resolves.
    2. ``aorta_abi_version()`` matches the SDK's compiled-in expected version —
       this is the authoritative stale/wrong-library check (a symbol-presence
       probe alone cannot catch "right names, wrong layout").
    3. A cheap live call (``aorta_now_ns``) returns OK (the value is not range
       checked — ``system_time_ns`` may legitimately be 0, e.g. a clock error
       or simulation time; only the result code matters).
    4. Every mirrored ``#[repr(C)]`` struct's ``ctypes.sizeof`` matches the baked
       layout baseline (``aorta._ffi.structs``).
    """
    missing = []
    for symbol in _REQUIRED_SYMBOLS:
        if not hasattr(lib, symbol):
            missing.append(symbol)
    if missing:
        raise AbiGuardError(
            "loaded Aorta library is missing expected C-ABI symbols: "
            + ", ".join(missing)
        )

    lib.aorta_abi_version.argtypes = []
    lib.aorta_abi_version.restype = ctypes.c_uint32
    version = lib.aorta_abi_version()
    if version != EXPECTED_ABI_VERSION:
        raise AbiGuardError(
            f"libaorta_core ABI version {version} != expected "
            f"{EXPECTED_ABI_VERSION}; the loaded library is incompatible with "
            "this SDK (stale or wrong-arch build)"
        )

    lib.aorta_now_ns.argtypes = [ctypes.POINTER(ctypes.c_int64)]
    lib.aorta_now_ns.restype = ctypes.c_int32
    out = ctypes.c_int64(0)
    rc = lib.aorta_now_ns(ctypes.byref(out))
    if rc != 0:
        raise AbiGuardError(f"aorta_now_ns probe failed with rc={rc}")

    from . import structs

    layout_mismatches = structs.check_layouts()
    if layout_mismatches:
        raise AbiGuardError(
            "ctypes struct layout drift vs the C-ABI: "
            + "; ".join(layout_mismatches)
        )


def load_library(*, force_reload: bool = False) -> ctypes.CDLL:
    """Locate, load, ABI-guard, and cache the Aorta C-ABI shared library.

    Idempotent: the library is loaded and guarded once, then cached. Pass
    ``force_reload=True`` only in tests that deliberately re-exercise discovery.
    """
    global _lib
    if _lib is not None and not force_reload:
        return _lib

    with _load_lock:
        if _lib is not None and not force_reload:  # pragma: no cover - race re-check
            # Another thread won the load race between the outer check and the
            # lock acquisition; return its result. Not deterministically testable.
            return _lib

        candidates = _candidate_paths()
        if not candidates:
            raise AortaLoadError(
                "could not locate the Aorta C-ABI library; set AORTA_CORE_FFI_LIB "
                "to the path of libaorta_core.{so,dylib}"
            )

        last_error: Optional[BaseException] = None
        loaded: Optional[ctypes.CDLL] = None
        for path in candidates:
            try:
                loaded = _try_load(path)
                break
            except OSError as err:
                last_error = err

        if loaded is None:
            raise AortaLoadError(
                "failed to load the Aorta C-ABI library from candidates "
                f"{candidates!r}: {last_error}"
            )

        _run_abi_guard(loaded)
        _lib = loaded
        return _lib


def library() -> ctypes.CDLL:
    """Return the loaded, ABI-guarded library, loading it on first use."""
    return load_library()
