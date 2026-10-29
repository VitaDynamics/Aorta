"""ContextStore — declarative shared key/value state with watches.

Mirrors the C++ ``ContextStore`` (cpp/include/aorta/contextstore.h) and the Rust
``ContextStore``. ``set_*`` accept an optional ``expires_at_ns`` (C++ surface,
default 0 = no expiry). ``get_*`` return a :class:`ContextRead` (the typed value
plus metadata) or ``None`` when the key is absent. ``watch`` callbacks fire on a
Zenoh worker thread through the same trampoline discipline as subscribers
(Python SDK design proposal §5.1).

The C-ABI ``aorta_ctx_read_t`` carries owned strings the caller must release via
``aorta_ctx_read_free``; this module copies the values out into a Python
:class:`ContextRead` and frees the FFI read in a ``finally`` so callers never see
raw pointers.
"""

from __future__ import annotations

import ctypes
import itertools
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, List, Optional

from . import errors, logging as _log
from ._ffi import bindings, loader, structs

# Watch callback: (change: ContextChange) -> None
WatchCallback = Callable[["ContextChange"], None]

_SNAPSHOT_DONE_NONE = 0xFF  # AORTA_CTX_SNAPSHOT_DONE_NONE sentinel


class ContextKind(IntEnum):
    """Mirror of the ``AORTA_CTX_KIND_*`` value-type tags."""

    NONE = 0
    BOOL = 1
    I64 = 2
    U64 = 3
    F64 = 4
    STRING = 5


class ContextChangeKind(IntEnum):
    """Mirror of ``AORTA_CTX_CHANGE_*``."""

    LIVE = 0
    SNAPSHOT = 1
    SNAPSHOT_DONE = 2


class SnapshotDoneReason(IntEnum):
    """Mirror of ``AORTA_CTX_SNAPSHOT_DONE_*`` (the ``NONE`` sentinel maps to ``None``)."""

    QUERY_DRAINED = 0
    DEADLINE_ELAPSED = 1


@dataclass(frozen=True)
class ContextRead:
    """A ``get_*`` result: the typed ``value`` plus provenance metadata."""

    value: Any
    value_kind: ContextKind
    is_fresh: bool
    stamp_ns: int
    expires_at_ns: int
    source_node: str
    source_instance_id: str
    sequence: int


@dataclass(frozen=True)
class ContextChange:
    """A watch event. ``value`` is decoded per ``value_kind`` (``None`` for a
    SnapshotDone marker). ``snapshot_done_reason`` is set only for SnapshotDone."""

    kind: ContextChangeKind
    snapshot_done_reason: Optional[SnapshotDoneReason]
    key: str
    value_kind: ContextKind
    value: Any
    stamp_ns: int
    expires_at_ns: int
    source_node: str
    source_instance_id: str
    sequence: int

    def is_snapshot_done(self) -> bool:
        return self.kind == ContextChangeKind.SNAPSHOT_DONE


@dataclass(frozen=True)
class OwnerLivelinessEvent:
    """An owner-liveliness event (mirrors ``OwnerLivelinessEvent``): an owning
    ContextStore became alive (``alive`` True, token declared at its first
    ``declare_owned_prefix``) or went away (``alive`` False, token withdrawn by
    shutdown / store drop / process death). ``source_instance_id`` is the same
    id carried on every context event and query reply."""

    node_name: str
    source_instance_id: str
    alive: bool


# Owner-liveliness watch callback: (event: OwnerLivelinessEvent) -> None
OwnerLivelinessCallback = Callable[["OwnerLivelinessEvent"], None]


@dataclass
class WatchSnapshotOptions:
    """Options for ``watch_with_initial_snapshot`` (C++ ``WatchSnapshotOptions``)."""

    snapshot_deadline_ms: int = 1000


# ── watch trampoline (process-global, token registry) ─────────────────────────
_watch_handlers: dict = {}
_watch_handlers_lock = threading.Lock()
_watch_token_counter = itertools.count(1)


def _register_watch(callback: WatchCallback) -> int:
    token = next(_watch_token_counter)
    with _watch_handlers_lock:
        _watch_handlers[token] = callback
    return token


def _unregister_watch(token: int) -> None:
    with _watch_handlers_lock:
        _watch_handlers.pop(token, None)


def _cstr_to_str(ptr) -> str:
    """Copy an owned ``*mut c_char`` (or NULL) into a Python str."""
    if not ptr:
        return ""
    return ctypes.cast(ptr, ctypes.c_char_p).value.decode("utf-8", "replace")


def _decode_value(value_kind: int, *, b, i, u, f, s):
    """Pick the value field matching ``value_kind`` (``s`` is a pre-decoded str)."""
    if value_kind == ContextKind.BOOL:
        return bool(b)
    if value_kind == ContextKind.I64:
        return int(i)
    if value_kind == ContextKind.U64:
        return int(u)
    if value_kind == ContextKind.F64:
        return float(f)
    if value_kind == ContextKind.STRING:
        return s
    return None  # ContextKind.NONE


def change_from_struct(c: "structs.AortaCtxChange") -> ContextChange:
    """Build a :class:`ContextChange` from the borrowed FFI struct (copies views).

    Split out so the decode path is unit-testable on the main thread.
    """
    reason = (
        None
        if c.snapshot_done_reason == _SNAPSHOT_DONE_NONE
        else SnapshotDoneReason(c.snapshot_done_reason)
    )
    value = _decode_value(
        c.value_kind,
        b=c.bool_value,
        i=c.i64_value,
        u=c.u64_value,
        f=c.f64_value,
        s=bindings.str_from_view(c.string_value),
    )
    return ContextChange(
        kind=ContextChangeKind(c.kind),
        snapshot_done_reason=reason,
        key=bindings.str_from_view(c.key),
        value_kind=ContextKind(c.value_kind),
        value=value,
        stamp_ns=int(c.stamp_ns),
        expires_at_ns=int(c.expires_at_ns),
        source_node=bindings.str_from_view(c.source_node),
        source_instance_id=bindings.str_from_view(c.source_instance_id),
        sequence=int(c.sequence),
    )


def _watch_dispatch(change_ptr, user_data) -> None:
    # Foreign worker thread, GIL held by ctypes. Must never let an exception
    # propagate into Rust (extern "C" is nounwind -> abort).
    callback = _watch_handlers.get(int(user_data)) if user_data else None
    if callback is None or not change_ptr:
        return
    try:
        callback(change_from_struct(change_ptr.contents))
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        try:
            _log.error("context watch callback raised: %r", exc, scope="aorta_sdk")
        except BaseException:  # pragma: no cover - logging must never re-raise
            pass


_WATCH_TRAMPOLINE = bindings.CTX_WATCH_CB(_watch_dispatch)


def _owner_liveliness_dispatch(event_ptr, user_data) -> None:
    # Foreign worker thread, GIL held by ctypes. Must never let an exception
    # propagate into Rust (extern "C" is nounwind -> abort).
    callback = _watch_handlers.get(int(user_data)) if user_data else None
    if callback is None or not event_ptr:
        return
    try:
        event = event_ptr.contents
        callback(
            OwnerLivelinessEvent(
                node_name=bindings.str_from_view(event.node_name),
                source_instance_id=bindings.str_from_view(event.source_instance_id),
                alive=bool(event.alive),
            )
        )
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        try:
            _log.error(
                "ctx owner-liveliness watch callback raised: %r", exc, scope="aorta_sdk"
            )
        except BaseException:  # pragma: no cover - logging must never re-raise
            pass


_OWNER_LIVELINESS_TRAMPOLINE = bindings.CTX_OWNER_LIVELINESS_CB(
    _owner_liveliness_dispatch
)


class WatchHandle:
    """A live watch. Call :meth:`shutdown` (or use as a context manager) to stop.

    Do NOT call ``shutdown`` from within this watch's own callback (it would
    deadlock on the Zenoh undeclare) — parity with the C++/Rust contract.
    """

    def __init__(self, lib: ctypes.CDLL, handle: int, token: int) -> None:
        self._lib = lib
        self._handle = handle
        self._token = token
        self._closed = False

    def shutdown(self) -> None:
        """Stop the watch (awaits the undeclare; no further callbacks after).
        Idempotent."""
        if self._closed:
            return
        rc = self._lib.aorta_ctx_watch_free(self._handle)
        if rc != 0:
            # On REENTRANT the FFI re-inserted the watch and may
            # still deliver callbacks — keep the token registered and
            # stay open so a retry from a non-runtime thread works.
            errors.check_rc(self._lib, rc, "aorta_ctx_watch_free")
        self._closed = True
        _unregister_watch(self._token)

    close = shutdown

    def __enter__(self) -> "WatchHandle":
        return self

    def __exit__(self, *_exc) -> None:
        self.shutdown()


class OwnerLivelinessWatchHandle:
    """A live owner-liveliness watch. Call :meth:`shutdown` (or use as a
    context manager) to stop. Do NOT call ``shutdown`` from within this
    watch's own callback (it would deadlock on the Zenoh undeclare) — parity
    with the C++/Rust contract."""

    def __init__(self, lib: ctypes.CDLL, handle: int, token: int) -> None:
        self._lib = lib
        self._handle = handle
        self._token = token
        self._closed = False

    def shutdown(self) -> None:
        """Stop the watch (awaits the undeclare; no further callbacks after).
        Idempotent."""
        if self._closed:
            return
        rc = self._lib.aorta_ctx_owner_liveliness_watch_free(self._handle)
        if rc != 0:
            # On REENTRANT the FFI re-inserted the watch and may
            # still deliver callbacks — keep the token registered and
            # stay open so a retry from a non-runtime thread works.
            errors.check_rc(self._lib, rc, "aorta_ctx_owner_liveliness_watch_free")
        self._closed = True
        _unregister_watch(self._token)

    close = shutdown

    def __enter__(self) -> "OwnerLivelinessWatchHandle":
        return self

    def __exit__(self, *_exc) -> None:
        self.shutdown()


class ContextStore:
    """Declarative shared key/value state. Created via ``Node.create_context_store``."""

    def __init__(self, node) -> None:
        self._lib = bindings.configure(loader.library())
        self._closed = False
        self._watches: List[WatchHandle] = []
        handle = bindings.aorta_ctx_t(0)
        rc = self._lib.aorta_ctx_new(node._handle, ctypes.byref(handle))
        errors.check_rc(self._lib, rc, "aorta_ctx_new")
        self._handle = handle.value

    # ── prefixes ─────────────────────────────────────────────────────────────
    def declare_owned_prefix(self, prefix: str) -> None:
        """Declare ownership of ``prefix`` (must precede ``set_*`` under it)."""
        self._ensure_open()
        rc = self._lib.aorta_ctx_declare_owned_prefix(self._handle, prefix.encode("utf-8"))
        errors.check_rc(self._lib, rc, "aorta_ctx_declare_owned_prefix")

    def is_key_declared(self, relative_key: str) -> bool:
        """Whether ``relative_key`` falls under a declared prefix (Rust parity)."""
        self._ensure_open()
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_ctx_is_key_declared(
            self._handle, relative_key.encode("utf-8"), ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_ctx_is_key_declared")
        return bool(out.value)

    def declared_prefixes(self) -> List[str]:
        """The prefixes this store has declared, in declaration order (Rust parity)."""
        self._ensure_open()
        buf = ctypes.create_string_buffer(4096)
        out_len = ctypes.c_size_t(0)
        rc = self._lib.aorta_ctx_declared_prefixes(
            self._handle, buf, len(buf), ctypes.byref(out_len)
        )
        errors.check_rc(self._lib, rc, "aorta_ctx_declared_prefixes")
        while out_len.value > len(buf):
            # The buffer was too small; out_len carries the full length. Resize to
            # exactly that and retry. A loop (not a single retry) covers the rare
            # case where the prefix set grows between calls.
            buf = ctypes.create_string_buffer(out_len.value)
            rc = self._lib.aorta_ctx_declared_prefixes(
                self._handle, buf, len(buf), ctypes.byref(out_len)
            )
            errors.check_rc(self._lib, rc, "aorta_ctx_declared_prefixes")
        raw = buf.raw[: out_len.value].decode("utf-8", "replace")
        return [p for p in raw.split("\n") if p]

    # ── setters (optional expiry) ────────────────────────────────────────────
    def set_bool(self, key: str, value: bool, *, expires_at_ns: int = 0) -> None:
        self._set("aorta_ctx_set_bool", key, bool(value), expires_at_ns)

    def set_i64(self, key: str, value: int, *, expires_at_ns: int = 0) -> None:
        self._set("aorta_ctx_set_i64", key, int(value), expires_at_ns)

    def set_u64(self, key: str, value: int, *, expires_at_ns: int = 0) -> None:
        self._set("aorta_ctx_set_u64", key, int(value), expires_at_ns)

    def set_f64(self, key: str, value: float, *, expires_at_ns: int = 0) -> None:
        self._set("aorta_ctx_set_f64", key, float(value), expires_at_ns)

    def set_string(self, key: str, value: str, *, expires_at_ns: int = 0) -> None:
        self._set("aorta_ctx_set_string", key, value.encode("utf-8"), expires_at_ns)

    def _set(self, fn_name: str, key: str, value, expires_at_ns: int) -> None:
        self._ensure_open()
        fn = getattr(self._lib, fn_name)
        rc = fn(self._handle, key.encode("utf-8"), value, int(expires_at_ns))
        errors.check_rc(self._lib, rc, fn_name)

    # ── getters (None on missing key) ────────────────────────────────────────
    def get_bool(self, key: str) -> Optional[ContextRead]:
        return self._get("aorta_ctx_get_bool", key, lambda r: bool(r.bool_value))

    def get_i64(self, key: str) -> Optional[ContextRead]:
        return self._get("aorta_ctx_get_i64", key, lambda r: int(r.i64_value))

    def get_u64(self, key: str) -> Optional[ContextRead]:
        return self._get("aorta_ctx_get_u64", key, lambda r: int(r.u64_value))

    def get_f64(self, key: str) -> Optional[ContextRead]:
        return self._get("aorta_ctx_get_f64", key, lambda r: float(r.f64_value))

    def get_string(self, key: str) -> Optional[ContextRead]:
        return self._get("aorta_ctx_get_string", key, lambda r: _cstr_to_str(r.string_value))

    def _get(self, fn_name: str, key: str, extract) -> Optional[ContextRead]:
        self._ensure_open()
        read = structs.AortaCtxRead()
        fn = getattr(self._lib, fn_name)
        rc = fn(self._handle, key.encode("utf-8"), ctypes.byref(read))
        try:
            if rc == errors.ResultCode.NOT_FOUND:
                return None
            errors.check_rc(self._lib, rc, fn_name)
            # Extract the value (and owned strings) BEFORE read_free releases them.
            value = extract(read)
            return ContextRead(
                value=value,
                value_kind=ContextKind(read.value_kind),
                is_fresh=bool(read.is_fresh),
                stamp_ns=int(read.stamp_ns),
                expires_at_ns=int(read.expires_at_ns),
                source_node=_cstr_to_str(read.source_node),
                source_instance_id=_cstr_to_str(read.source_instance_id),
                sequence=int(read.sequence),
            )
        finally:
            self._lib.aorta_ctx_read_free(ctypes.byref(read))

    # ── watch ────────────────────────────────────────────────────────────────
    def watch(self, prefix: str, callback: WatchCallback) -> WatchHandle:
        """Watch ``prefix`` for live changes; ``callback(ContextChange)`` runs on
        a worker thread (exceptions are logged and swallowed)."""
        return self._watch("aorta_ctx_watch", prefix, callback, None)

    def watch_with_initial_snapshot(
        self,
        prefix: str,
        callback: WatchCallback,
        *,
        options: Optional[WatchSnapshotOptions] = None,
    ) -> WatchHandle:
        """Like :meth:`watch`, but first replays a snapshot of current values
        (terminated by a SnapshotDone change), then streams live changes."""
        deadline = (options or WatchSnapshotOptions()).snapshot_deadline_ms
        return self._watch("aorta_ctx_watch_with_snapshot", prefix, callback, deadline)

    def _watch(self, fn_name, prefix, callback, deadline_ms) -> WatchHandle:
        self._ensure_open()
        token = _register_watch(callback)
        handle = bindings.aorta_ctx_watch_t(0)
        fn = getattr(self._lib, fn_name)
        if deadline_ms is None:
            rc = fn(
                self._handle, prefix.encode("utf-8"), _WATCH_TRAMPOLINE,
                ctypes.c_void_p(token), ctypes.byref(handle),
            )
        else:
            rc = fn(
                self._handle, prefix.encode("utf-8"), int(deadline_ms),
                _WATCH_TRAMPOLINE, ctypes.c_void_p(token), ctypes.byref(handle),
            )
        if rc != 0:
            _unregister_watch(token)
            errors.check_rc(self._lib, rc, fn_name)
        watch = WatchHandle(self._lib, handle.value, token)
        self._watches.append(watch)
        return watch

    def watch_ctx_owner_liveliness(
        self, callback: OwnerLivelinessCallback
    ) -> OwnerLivelinessWatchHandle:
        """Watch the liveliness of ContextStore owners in this store's group:
        current owners are replayed as ``alive`` True events, then live
        join/leave events follow. ``callback(OwnerLivelinessEvent)`` runs
        on a worker thread (exceptions are logged and swallowed)."""
        self._ensure_open()
        token = _register_watch(callback)
        handle = bindings.aorta_ctx_owner_liveliness_watch_t(0)
        rc = self._lib.aorta_ctx_owner_liveliness_watch(
            self._handle, _OWNER_LIVELINESS_TRAMPOLINE, ctypes.c_void_p(token),
            ctypes.byref(handle),
        )
        if rc != 0:
            _unregister_watch(token)
            errors.check_rc(self._lib, rc, "aorta_ctx_owner_liveliness_watch")
        watch = OwnerLivelinessWatchHandle(self._lib, handle.value, token)
        self._watches.append(watch)
        return watch

    # ── lifecycle ────────────────────────────────────────────────────────────
    def shutdown(self) -> None:
        """Deterministically undeclare all prefixes/watches (Rust/C++ ``Shutdown``)."""
        self._ensure_open()
        rc = self._lib.aorta_ctx_shutdown(self._handle)
        errors.check_rc(self._lib, rc, "aorta_ctx_shutdown")

    def close(self) -> None:
        """Free the store (and any still-open watches). Idempotent."""
        if self._closed:
            return
        self._closed = True
        for watch in self._watches:
            try:
                watch.shutdown()
            except errors.AortaError:  # pragma: no cover - best-effort teardown
                pass
        self._watches.clear()
        self._lib.aorta_ctx_free(self._handle)

    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.AortaError("operation on a closed ContextStore")

    def __enter__(self) -> "ContextStore":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
