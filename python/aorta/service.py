"""RPC service (server) wrapper + the service callback trampoline.

Like the subscriber trampoline (Python SDK design proposal §5.1), service
request callbacks fire on Tokio/Zenoh worker threads. The same discipline
applies: ONE process-global ``SERVICE_CB`` trampoline, a token→handler registry
(never pass a PyObject pointer to Rust), GIL acquired by ctypes on entry, and a
try/except that must never let a Python exception cross the C boundary.

A service request differs from a sub message: it carries a **handle that must be
replied to or released**, else the caller's query hangs until timeout. So:
- the handler is invoked as ``handler(request_bytes, req: ServiceRequest)``;
- if the handler raises, the trampoline releases the request (fail fast);
- a ``ServiceRequest`` that is GC'd without reply releases itself via a
  ``weakref.finalize`` guard (parity with the C++ ``ServiceRequest`` destructor),
  so the deferred-reply pattern (retain ``req``, reply later) still works.
"""

from __future__ import annotations

import ctypes
import itertools
import threading
import weakref
from typing import Any, Callable, Optional

import flatbuffers

from . import errors, logging as _log
from ._ffi import bindings, loader, structs
from .header import build_aorta_header
from .options import ServiceOptions, ServiceStats, build_service_options
from .publisher import _include_publisher_node

# Raw service handler: (request: bytes, req: ServiceRequest) -> None
ServiceHandler = Callable[[bytes, "ServiceRequest"], None]
# fill(builder, aorta_header_offset) -> root_table_offset  (as in publish_typed)
TypedFill = Callable[["flatbuffers.Builder", int], int]

_handlers: dict = {}
_handlers_lock = threading.Lock()
_token_counter = itertools.count(1)


def _register(handler: ServiceHandler) -> int:
    token = next(_token_counter)
    with _handlers_lock:
        _handlers[token] = handler
    return token


def _unregister(token: int) -> None:
    with _handlers_lock:
        _handlers.pop(token, None)


def _finalize_request(lib, handle: int) -> None:
    """Release an un-replied request when its wrapper is GC'd (parity with the
    C++ ``ServiceRequest`` destructor's defensive ``Release()``). Best-effort and
    silent — it may run during interpreter shutdown."""
    try:
        lib.aorta_service_request_release(handle)
    except BaseException:  # pragma: no cover - shutdown best-effort
        pass


class ServiceRequest:
    """An in-flight request handle. Reply exactly once, or release it.

    ``reply``/``reply_error`` **consume** the handle on every outcome (success or
    error) — matching the C-ABI contract — so they must not be called twice and
    you must not ``release`` afterwards. Mirrors the C++ ``ServiceRequest``.
    """

    def __init__(self, lib: ctypes.CDLL, handle: int) -> None:
        self._lib = lib
        self._handle = handle
        # Armed safety net; detached the moment we reply/release explicitly.
        self._finalizer = weakref.finalize(self, _finalize_request, lib, handle)

    def is_valid(self) -> bool:
        """True until the request has been replied to or released."""
        return self._handle != 0

    def reply(self, data: bytes = b"") -> None:
        """Reply with ``data`` (bytes/bytearray/memoryview; empty = empty reply)."""
        if not self._handle:
            raise errors.AlreadyRepliedError("service request already replied/released")
        view, _keep = bindings.owned_view_from_bytes(data)
        rc = self._lib.aorta_service_request_reply(self._handle, ctypes.byref(view))
        # Consumed on every outcome — zero + detach BEFORE check_rc (which may
        # raise), so a send failure never double-frees via the finalizer.
        self._handle = 0
        self._finalizer.detach()
        errors.check_rc(self._lib, rc, "aorta_service_request_reply")

    def reply_error(self, message: str) -> None:
        """Reply with an error (a Zenoh query error reply, not an OK payload).

        Backed by ``aorta_service_request_reply_error`` (added for the Python SDK
        Phase 2 — it backs the Rust ``ServiceRequest::reply_error``; the C++ SDK
        has no raw-request error reply).
        """
        if not self._handle:
            raise errors.AlreadyRepliedError("service request already replied/released")
        rc = self._lib.aorta_service_request_reply_error(
            self._handle, message.encode("utf-8")
        )
        self._handle = 0
        self._finalizer.detach()
        errors.check_rc(self._lib, rc, "aorta_service_request_reply_error")

    def release(self) -> None:
        """Drop the request without replying (idempotent)."""
        if not self._handle:
            return
        self._lib.aorta_service_request_release(self._handle)
        self._handle = 0
        self._finalizer.detach()


class TypedServiceResponder:
    """Typed reply side handed to a ``create_service_typed_view`` handler.

    ``reply(fill)`` injects a byte-identical AortaHeader (per-service monotonic
    sequence + node name, like ``publish_typed``) and lets the caller build the
    response body around it. ``reply_bytes`` / ``reply_error`` bypass the typed
    build. Mirrors the C++ ``TypedServiceResponder``.
    """

    def __init__(self, request: ServiceRequest, stamper: Callable[[], tuple]) -> None:
        self._request = request
        self._stamper = stamper

    def is_valid(self) -> bool:
        return self._request.is_valid()

    def raw(self) -> ServiceRequest:
        """The underlying raw :class:`ServiceRequest` (C++ ``Raw()``)."""
        return self._request

    def reply(self, fill: TypedFill, *, initial_size: int = 256) -> None:
        seq, stamp_ns, node = self._stamper()
        builder = flatbuffers.Builder(initial_size)
        header_off = build_aorta_header(
            builder, sequence=seq, publish_stamp_ns=stamp_ns, publisher_node=node
        )
        root_off = fill(builder, header_off)
        builder.Finish(root_off)
        self._request.reply(bytes(builder.Output()))

    def reply_bytes(self, data: bytes) -> None:
        self._request.reply(data)

    def reply_error(self, message: str) -> None:
        self._request.reply_error(message)


def make_header_stamper(lib: ctypes.CDLL, node_name: str) -> Callable[[], tuple]:
    """Build a per-service header stamper: ``() -> (seq, stamp_ns, node|None)``.

    Owns a monotonic sequence (thread-safe) — one per service, shared by every
    responder (parity with the C++ ``shared_ptr<atomic<uint64_t>>``). The node
    name is omitted when ``AORTA_HEADER_INCLUDE_PUBLISHER_NODE`` is falsey.
    """
    lock = threading.Lock()
    counter = itertools.count(0)
    include_node = _include_publisher_node()

    def stamp() -> tuple:
        with lock:
            seq = next(counter)
        out = ctypes.c_int64(0)
        lib.aorta_now_ns(ctypes.byref(out))
        return seq, out.value, (node_name if include_node else None)

    return stamp


def typed_service_decode_dispatch(root_decoder, handler, stamper, payload, req) -> None:
    """Decode ``payload``; on success call ``handler(decoded, responder)``.

    A decode failure sends a real error reply (so the caller's ``call`` fails
    fast rather than receiving a garbage/empty OK) and is logged — parity with
    the C++ typed-view auto-error path, but with a true error reply. Split out so
    the decode branch is unit-testable on the main thread.
    """
    responder = TypedServiceResponder(req, stamper)
    try:
        decoded = root_decoder(payload)
    except Exception as exc:  # noqa: BLE001 - decode failure -> error reply
        _log.error("typed service decode failed: %r", exc, scope="aorta_sdk")
        responder.reply_error("request payload decode failed")
        return
    handler(decoded, responder)


def _handle_failed_request(req: "ServiceRequest") -> None:
    """A handler failed before completing — fail the caller IMMEDIATELY instead of
    making it wait out ``ClientOptions.timeout_ms``: ``release()`` only DROPS the
    request (no Zenoh reply is sent), so send an error reply when the handler
    hasn't answered yet, else just release (idempotent). Runs on the worker
    thread, so it must never re-raise — hence the release fallback.

    Module-level (not inlined in the trampoline) so both branches are unit-testable
    without a live FFI handle.
    """
    try:
        if req.is_valid():
            req.reply_error("service handler raised an exception")
        else:
            req.release()
    except BaseException:  # pragma: no cover - reply_error on a valid req won't raise
        req.release()


def _service_dispatch(view_ptr, request_handle, user_data) -> None:
    # Foreign worker thread, GIL held by ctypes. Must never let an exception
    # propagate into Rust (extern "C" is nounwind -> abort).
    lib = loader.library()
    req = ServiceRequest(lib, int(request_handle))
    handler = _handlers.get(int(user_data)) if user_data else None
    if handler is None:
        # Service was closed (token gone) — release so the query fails fast
        # instead of hanging until timeout.
        req.release()
        return
    try:
        payload = bindings.bytes_from_view(view_ptr.contents) if view_ptr else b""
        handler(payload, req)
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        try:
            _log.error("service callback raised: %r", exc, scope="aorta_sdk")
        except BaseException:  # pragma: no cover - logging must never re-raise
            pass
        _handle_failed_request(req)


# One process-global trampoline, kept alive for the process lifetime.
_TRAMPOLINE = bindings.SERVICE_CB(_service_dispatch)


class Service:
    """An RPC service (server). Created via ``Node.create_service*``."""

    def __init__(
        self,
        node,
        service_name: str,
        handler: ServiceHandler,
        *,
        options: Optional[ServiceOptions] = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        self._name = service_name
        self._closed = False
        self._token = _register(handler)
        opts_struct, self._opts_keep = build_service_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        handle = bindings.aorta_service_t(0)
        rc = self._lib.aorta_service_new_with_options(
            node._handle,
            service_name.encode("utf-8"),
            opts_arg,
            _TRAMPOLINE,
            ctypes.c_void_p(self._token),
            ctypes.byref(handle),
        )
        if rc != 0:
            _unregister(self._token)
            errors.check_rc(self._lib, rc, "aorta_service_new_with_options")
        self._handle = handle.value

    def name(self) -> str:
        return self._name

    def stats(self) -> ServiceStats:
        """Real FFI service counters (``aorta_service_stats_t``)."""
        out = structs.AortaServiceStats()
        rc = self._lib.aorta_service_get_stats(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_service_get_stats")
        return ServiceStats._from_ffi(out)

    def close(self) -> None:
        """Stop the service (bounded in-flight drain) and drop the handler token."""
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_service_free(self._handle)
        _unregister(self._token)

    def __enter__(self) -> "Service":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
