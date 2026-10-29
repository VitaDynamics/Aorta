"""Raw capture + schema-resolver wrappers, thin over the core
C-ABI via ctypes (no native-extension build).

``CaptureSubscriber`` is a recorder/observer snoop that yields the concrete key
+ raw payload + attachment metadata; ``SchemaResolver`` resolves a captured
message's schema lazily. Both take an :class:`aorta.Node` and use the canonical
loader (ABI-guarded) and shared ``_ffi.structs`` views.

Lifetime contract: the CFUNCTYPE trampoline and the encoded selector patterns are
held on the subscriber for its whole lifetime (else the GC frees the trampoline →
use-after-free in Rust). ``close()`` / ``__del__`` frees the FFI handle.
"""

from __future__ import annotations

# Path-extending for the same reason ``aorta`` and ``aorta.action`` are (see
# ``aorta/__init__.py``): the generated FlatBuffers modules for ``namespace
# aorta.capture.*`` schemas live in a DIFFERENT ``sys.path`` root than this SDK
# package whenever the two are separate Bazel targets (``//python:aorta_py`` plus
# ``//message/schemas/topic/capture:capture_lifecycle_py_lib``). Extending
# ``aorta.__path__`` alone is not enough — once this regular package is selected
# for ``aorta.capture``, its own ``__path__`` bounds the search for
# ``aorta.capture.CaptureLifecycle`` & friends. Co-installed wheels happen to
# work without this only because pip merges both trees into one physical
# directory.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

import ctypes
from dataclasses import dataclass
from enum import IntEnum
from typing import Callable, List, Optional, Tuple

from .. import errors
from .._ffi import loader, structs

# Re-export so `except capture.AortaError` keeps working; the SDK-wide typed
# hierarchy is the single source of truth. A previous local, incompatible
# AortaError here discarded aorta_last_error() diagnostics and broke
# `except errors.NotFoundError` for NOT_FOUND.
AortaError = errors.AortaError

AORTA_OK = 0
AORTA_ERR_NOT_FOUND = -2


class CaptureSelectorKind(IntEnum):
    TOPIC = 0
    CONTEXT = 1
    TELEMETRY = 2
    RPC_RECORD = 3
    RAW_KEYEXPR = 4


class CaptureKind(IntEnum):
    LIVE = 0
    SNAPSHOT = 1


# --- ctypes layout (reuses the shared views; matches core/src/ffi/c_api/capture.rs) ---


class _TraceParent(ctypes.Structure):
    _fields_ = [
        ("trace_id", ctypes.c_uint8 * 16),
        ("span_id", ctypes.c_uint64),
        ("flags", ctypes.c_uint8),
    ]


class _CapturedSample(ctypes.Structure):
    _fields_ = [
        ("key", structs.AortaStringView),
        ("key_suffix", structs.AortaStringView),
        ("payload", structs.AortaBytesView),
        ("has_schema_hash", ctypes.c_bool),
        ("schema_hash", structs.AortaSchemaHashView),
        ("has_traceparent", ctypes.c_bool),
        ("traceparent", _TraceParent),
        ("has_publish_time", ctypes.c_bool),
        ("publish_time_ns", ctypes.c_uint64),
        ("recv_time_ns", ctypes.c_uint64),
        ("kind", ctypes.c_int32),
    ]


class _CaptureSelector(ctypes.Structure):
    _fields_ = [("kind", ctypes.c_int32), ("pattern", ctypes.c_char_p)]


class _ResolverOpts(ctypes.Structure):
    _fields_ = [
        ("cache_max_entries", ctypes.c_size_t),
        ("by_hash_timeout_ms", ctypes.c_uint32),
        ("by_key_timeout_ms", ctypes.c_uint32),
    ]


class _ResolvedSchema(ctypes.Structure):
    _fields_ = [
        ("schema_hash", structs.AortaSchemaHashView),
        ("root_type_name", structs.AortaStringView),
        ("bfbs", structs.AortaBytesView),
        ("internal", ctypes.c_void_p),
    ]


_CAPTURE_CB = ctypes.CFUNCTYPE(
    None, ctypes.POINTER(_CapturedSample), ctypes.c_bool, ctypes.c_int32, ctypes.c_void_p
)

_configured = False


def _lib() -> ctypes.CDLL:
    """Return the ABI-guarded core library with the capture surface configured.

    ``_configure`` is idempotent (it only re-sets argtypes), so the worst a race
    on first use does is set them twice — no lock needed.
    """
    lib = loader.library()
    global _configured
    if not _configured:
        _configure(lib)
        _configured = True
    return lib


def _configure(lib: ctypes.CDLL) -> None:
    u64 = ctypes.c_uint64
    i32 = ctypes.c_int32
    p_u64 = ctypes.POINTER(u64)
    lib.aorta_capture_subscriber_new.argtypes = [
        u64, ctypes.POINTER(_CaptureSelector), ctypes.c_size_t, _CAPTURE_CB,
        ctypes.c_void_p, p_u64,
    ]
    lib.aorta_capture_subscriber_new.restype = i32
    lib.aorta_capture_subscriber_free.argtypes = [u64]
    lib.aorta_capture_subscriber_free.restype = i32
    lib.aorta_schema_resolver_new.argtypes = [u64, ctypes.POINTER(_ResolverOpts), p_u64]
    lib.aorta_schema_resolver_new.restype = i32
    lib.aorta_schema_resolver_resolve_by_hash.argtypes = [
        u64, structs.AortaSchemaHashView, ctypes.POINTER(_ResolvedSchema),
    ]
    lib.aorta_schema_resolver_resolve_by_hash.restype = i32
    lib.aorta_schema_resolver_resolve_by_key.argtypes = [
        u64, ctypes.c_char_p, ctypes.POINTER(_ResolvedSchema),
    ]
    lib.aorta_schema_resolver_resolve_by_key.restype = i32
    lib.aorta_resolved_schema_free.argtypes = [ctypes.POINTER(_ResolvedSchema)]
    lib.aorta_resolved_schema_free.restype = i32
    lib.aorta_schema_resolver_clear_key_cache.argtypes = [u64]
    lib.aorta_schema_resolver_clear_key_cache.restype = i32
    lib.aorta_schema_resolver_free.argtypes = [u64]
    lib.aorta_schema_resolver_free.restype = i32


def _read_str(view: structs.AortaStringView) -> str:
    if not view.ptr or view.len == 0:
        return ""
    return ctypes.string_at(view.ptr, view.len).decode("utf-8", "replace")


def _read_hash(view: structs.AortaSchemaHashView) -> Optional[str]:
    if not view.hex or view.len == 0:
        return None
    return ctypes.string_at(view.hex, view.len).decode("ascii", "replace")


def _read_bytes(view: structs.AortaBytesView) -> bytes:
    if view.slice_count == 0 or not view.slices:
        return b""
    slice0 = ctypes.cast(view.slices, ctypes.POINTER(structs.AortaBytesSlice))[0]
    if not slice0.ptr or slice0.len == 0:
        return b""
    return ctypes.string_at(slice0.ptr, slice0.len)


@dataclass
class CapturedSample:
    """An owned copy of one captured message (safe to retain)."""

    key: str
    key_suffix: str
    payload: bytes
    schema_hash_hex: Optional[str]
    publish_time_ns: Optional[int]
    recv_time_ns: int
    kind: CaptureKind


@dataclass
class ResolvedSchema:
    schema_hash_hex: str
    root_type_name: str
    bfbs: bytes


# A selector is a (kind, pattern) pair.
CaptureSelector = Tuple[CaptureSelectorKind, str]


class CaptureSubscriber:
    """Raw capture subscription over an :class:`aorta.Node`. Close (or GC) to stop."""

    def __init__(self, node, selectors: List[CaptureSelector],
                 on_sample: Callable[[CapturedSample], None]) -> None:
        self._lib = _lib()
        self._on_sample = on_sample
        self._handle = 0

        def _trampoline(sample_ptr, is_snapshot_done, _done_reason, _user_data):
            # Never let a Python exception unwind across the FFI boundary.
            try:
                if is_snapshot_done or not sample_ptr:  # pragma: no cover - live sub never gets SnapshotDone/NULL
                    return
                s = sample_ptr.contents
                self._on_sample(
                    CapturedSample(
                        key=_read_str(s.key),
                        key_suffix=_read_str(s.key_suffix),
                        payload=_read_bytes(s.payload),
                        schema_hash_hex=_read_hash(s.schema_hash) if s.has_schema_hash else None,
                        publish_time_ns=s.publish_time_ns if s.has_publish_time else None,
                        recv_time_ns=s.recv_time_ns,
                        kind=CaptureKind(s.kind),
                    )
                )
            except Exception:  # noqa: BLE001  # pragma: no cover - must not cross the C boundary
                pass

        # Keep the trampoline AND the encoded patterns alive for the lifetime.
        self._cb = _CAPTURE_CB(_trampoline)
        self._patterns = [pattern.encode("utf-8") for _, pattern in selectors]
        arr = (_CaptureSelector * len(selectors))()
        for i, (kind, _) in enumerate(selectors):
            arr[i].kind = int(kind)
            arr[i].pattern = self._patterns[i]

        out = ctypes.c_uint64(0)
        rc = self._lib.aorta_capture_subscriber_new(
            node._handle, arr, len(selectors), self._cb, None, ctypes.byref(out)
        )
        if rc != AORTA_OK:  # pragma: no cover - valid selectors on a live node do not fail
            errors.check_rc(self._lib, rc, "aorta_capture_subscriber_new")
        self._handle = out.value

    def close(self) -> None:
        if self._handle:
            self._lib.aorta_capture_subscriber_free(self._handle)
            self._handle = 0

    def __enter__(self) -> "CaptureSubscriber":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class SchemaResolver:
    """Resolve a captured message's schema (hash/key → bfbs). Take an aorta.Node."""

    def __init__(self, node, *, cache_max_entries: int = 256,
                 by_hash_timeout_ms: int = 2000, by_key_timeout_ms: int = 2000) -> None:
        self._lib = _lib()
        self._handle = 0
        opts = _ResolverOpts(cache_max_entries, by_hash_timeout_ms, by_key_timeout_ms)
        out = ctypes.c_uint64(0)
        rc = self._lib.aorta_schema_resolver_new(node._handle, ctypes.byref(opts), ctypes.byref(out))
        if rc != AORTA_OK:  # pragma: no cover - resolver creation on a live node does not fail
            errors.check_rc(self._lib, rc, "aorta_schema_resolver_new")
        self._handle = out.value

    def _take(self, out: _ResolvedSchema) -> ResolvedSchema:
        try:
            return ResolvedSchema(
                schema_hash_hex=_read_hash(out.schema_hash) or "",
                root_type_name=_read_str(out.root_type_name),
                bfbs=_read_bytes(out.bfbs),
            )
        finally:
            self._lib.aorta_resolved_schema_free(ctypes.byref(out))

    def resolve_by_hash(self, hash_hex: str) -> ResolvedSchema:
        raw = hash_hex.encode("ascii")
        buf = ctypes.create_string_buffer(raw, len(raw))  # kept alive across the call
        view = structs.AortaSchemaHashView(ctypes.cast(buf, ctypes.c_void_p), len(raw))
        out = _ResolvedSchema()
        rc = self._lib.aorta_schema_resolver_resolve_by_hash(self._handle, view, ctypes.byref(out))
        if rc != AORTA_OK:
            errors.check_rc(self._lib, rc, "resolve_by_hash")
        return self._take(out)

    def resolve_by_key(self, key_suffix: str) -> ResolvedSchema:
        out = _ResolvedSchema()
        rc = self._lib.aorta_schema_resolver_resolve_by_key(
            self._handle, key_suffix.encode("utf-8"), ctypes.byref(out)
        )
        if rc != AORTA_OK:
            errors.check_rc(self._lib, rc, "resolve_by_key")
        return self._take(out)

    def clear_key_cache(self) -> None:
        """Drop the mutable key→schema cache (e.g. after a re-registration)."""
        rc = self._lib.aorta_schema_resolver_clear_key_cache(self._handle)
        if rc != AORTA_OK:  # pragma: no cover - clear on a valid handle does not fail
            errors.check_rc(self._lib, rc, "clear_key_cache")

    def close(self) -> None:
        if self._handle:
            self._lib.aorta_schema_resolver_free(self._handle)
            self._handle = 0

    def __del__(self) -> None:
        self.close()
