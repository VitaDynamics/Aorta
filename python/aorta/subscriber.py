"""Subscriber wrapper + the callback trampoline (the #1 crash-safety hazard).

Subscriber callbacks fire on Tokio/Zenoh worker threads, not the Python thread.
The discipline (Python SDK design proposal §5.1):

1. The ctypes ``CFUNCTYPE`` trampoline acquires the GIL on entry, so user code
   is GIL-safe.
2. A Python exception must NEVER cross the C boundary — the trampoline wraps the
   user handler in try/except, logs, and swallows.
3. ``user_data`` is a stable integer **token**; the real handler is looked up in
   a process-global registry (never pass a movable PyObject pointer to Rust).
4. The single module-global trampoline is kept alive for the process lifetime.
"""

from __future__ import annotations

import ctypes
import itertools
import threading
from typing import Callable, Dict

from . import errors, logging as _log
from ._ffi import bindings, loader, structs
from .context import MessageContext, context_from_ptr
from .options import SubscriberOptions, build_sub_buffering, build_subscriber_options
from .qos import QoS

# Subscriber message handler: (payload: bytes, ctx: MessageContext) -> None
MessageHandler = Callable[[bytes, MessageContext], None]

_handlers: Dict[int, MessageHandler] = {}
_handlers_lock = threading.Lock()
_token_counter = itertools.count(1)


def _register(handler: MessageHandler) -> int:
    token = next(_token_counter)
    with _handlers_lock:
        _handlers[token] = handler
    return token


def _unregister(token: int) -> None:
    with _handlers_lock:
        _handlers.pop(token, None)


def _dispatch(view_ptr, _hash_ptr, ctx_ptr, user_data) -> None:
    # Runs on a foreign worker thread with the GIL held by ctypes. Must never
    # let an exception propagate into Rust (extern "C" is nounwind -> abort).
    # _hash_ptr (schema hash view) is ignored, matching the C++ SDK's
    # `(void)schema` — Core's FFI passes an empty view to all SDKs.
    handler = _handlers.get(int(user_data)) if user_data else None
    if handler is None:
        return
    try:
        payload = bindings.bytes_from_view(view_ptr.contents) if view_ptr else b""
        ctx = context_from_ptr(ctx_ptr)
        handler(payload, ctx)
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        try:
            _log.error("subscriber callback raised: %r", exc, scope="aorta_sdk")
        except BaseException:  # pragma: no cover - logging must never re-raise here
            pass


# One process-global trampoline, kept alive forever (GC must not free it while
# Rust holds the pointer).
_TRAMPOLINE = bindings.ON_MSG(_dispatch)


def typed_decode_dispatch(root_decoder, handler, payload, ctx) -> None:
    """Decode ``payload`` via ``root_decoder`` then call ``handler(decoded, ctx)``.

    A decode failure is logged and the message skipped (parity with the C++
    typed-view auto-error path). Used by ``Node.create_subscriber_typed`` via
    ``functools.partial`` so the decode path is unit-testable on the main thread
    (the live path runs on a foreign worker thread that coverage cannot trace).
    """
    try:
        decoded = root_decoder(payload)
    except Exception as exc:  # noqa: BLE001 - decode failure -> skip
        _log.error("typed subscriber decode failed: %r", exc)
        return
    handler(decoded, ctx)


class Subscriber:
    """A raw byte subscriber. Created via ``Node.create_subscriber``.

    The handler is invoked as ``handler(payload: bytes, ctx: MessageContext)``
    on a worker thread; exceptions are logged and swallowed.
    """

    def __init__(
        self,
        node,
        topic: str,
        handler: MessageHandler,
        *,
        qos: "QoS" = None,
        options: "SubscriberOptions" = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        self._topic = topic
        self._closed = False
        self._token = _register(handler)
        # aorta_sub_options_t is layout-frozen (Python mirrors it); the
        # Tier A receive_depth / delivery knobs ride the aorta_sub_buffering_t
        # companion of aorta_subscriber_new_buffered. None pointers reproduce
        # today's behavior.
        opts_struct, self._opts_keep = build_subscriber_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        buf_struct = build_sub_buffering(options)
        self._buf_keep = buf_struct  # keep alive across the synchronous new call
        buf_arg = ctypes.byref(buf_struct) if buf_struct is not None else None
        self._qos_keep = None
        if qos is None:
            qos_arg = None
        else:
            qstruct = qos._to_ffi()  # carries express + congestion_control
            self._qos_keep = qstruct  # keep alive across the synchronous new call
            qos_arg = ctypes.cast(ctypes.byref(qstruct), ctypes.c_void_p)
        handle = bindings.aorta_sub_t(0)
        rc = self._lib.aorta_subscriber_new_buffered(
            node._handle,
            topic.encode("utf-8"),
            qos_arg,  # qos (None -> Core defaults)
            opts_arg,
            buf_arg,  # buffering (None -> today's behavior)
            _TRAMPOLINE,
            ctypes.c_void_p(self._token),
            ctypes.byref(handle),
        )
        if rc != 0:
            _unregister(self._token)
            errors.check_rc(self._lib, rc, "aorta_subscriber_new_buffered")
        self._handle = handle.value

    def dropped_count(self) -> int:
        """Number of events shed by this subscriber's Tier A ``receive_depth``
        gate (drop-newest). 0 when uncapped (the default).
        """
        return int(self._lib.aorta_sub_dropped_count(self._handle))

    def topic(self) -> str:
        return self._topic

    def runtime_stats(self):
        """Snapshot the 7 runtime drop/reorder counters (Rust ``Subscriber::runtime_stats``).

        Bridged via the ``aorta_subscriber_runtime_stats`` C-ABI fn.
        Returns a :class:`~aorta.options.SubscriberRuntimeStats`.
        """
        from .options import SubscriberRuntimeStats

        out = structs.AortaSubscriberRuntimeStats()
        rc = self._lib.aorta_subscriber_runtime_stats(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_subscriber_runtime_stats")
        return SubscriberRuntimeStats._from_ffi(out)

    def close(self) -> None:
        """Undeclare the subscriber, then drop the handler token.

        The C-ABI free does a *bounded* in-flight drain on the normal path, but
        from a callback/async context it spawns the undeclare and returns
        immediately — so an in-flight callback MAY still run after ``close()``.
        That is safe: a late dispatch finds no token in the global registry and
        no-ops (the process-global trampoline + registry outlive any single
        subscriber, so there is no use-after-free). Do not call ``close()`` from
        within this subscriber's own callback.
        """
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_subscriber_free(self._handle)
        _unregister(self._token)

    def __enter__(self) -> "Subscriber":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
