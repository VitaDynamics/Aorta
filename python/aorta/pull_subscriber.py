"""Pull-mode (Tier B-pull) subscriber.

A :class:`PullSubscriber` owns a Zenoh-native keep-last ring (declared via
``aorta_subscriber_new_buffered`` with ``delivery_mode = INLINE_PULL`` in the
``aorta_sub_buffering_t`` companion). Instead of a
callback on a worker thread, the application drains samples by calling
:meth:`recv` / :meth:`try_recv` from its own thread — the rclpy
``wait_for_message`` idiom, latest-wins, with structural per-topic isolation.

``INLINE_PUSH`` is intentionally not offered in Python: a slow GIL callback on
the Zenoh RX pool would be a node-wide footgun.
"""

from __future__ import annotations

import ctypes
import dataclasses
from dataclasses import dataclass
from typing import Optional

from . import errors
from ._ffi import bindings, loader, structs
from .context import MessageContext, context_from_ptr
from .errors import ResultCode
from .options import (
    DeliveryMode,
    ReceiveOverflow,
    SubscriberOptions,
    build_sub_buffering,
    build_subscriber_options,
)
from .qos import QoS


@dataclass(frozen=True)
class PullSample:
    """A sample drained from a pull subscriber: raw ``payload`` bytes plus the
    opt-in :class:`~aorta.context.MessageContext` (populated per the subscriber's
    ``include_*`` options).
    """

    payload: bytes
    context: MessageContext


class PullSubscriber:
    """A pull-mode subscriber. Created via ``Node.create_subscriber_pull``.

    Drain it with :meth:`recv` (blocking / with timeout) or :meth:`try_recv`
    (non-blocking) from a dedicated consumer thread.
    """

    def __init__(
        self,
        node,
        topic: str,
        depth: int,
        *,
        qos: Optional[QoS] = None,
        options: Optional[SubscriberOptions] = None,
    ) -> None:
        if depth < 1:
            raise errors.InvalidArgumentError(
                "create_subscriber_pull requires depth >= 1"
            )
        self._lib = bindings.configure(loader.library())
        self._topic = topic
        self._closed = False

        # Copy the caller's options before forcing pull-mode fields — mutating
        # the caller-owned object in place would carry delivery=INLINE_PULL /
        # DROP_OLDEST / this depth into a later callback subscription (which Core
        # rejects) and silently change their intended receive_depth.
        base = options if options is not None else SubscriberOptions()
        opts = dataclasses.replace(
            base,
            delivery=DeliveryMode.INLINE_PULL,
            receive_overflow=ReceiveOverflow.DROP_OLDEST,
            receive_depth=int(depth),
        )
        opts_struct, self._opts_keep = build_subscriber_options(opts)
        # aorta_sub_options_t is layout-frozen (Python mirrors it); the
        # pull ring's depth/mode ride the aorta_sub_buffering_t companion.
        buf_struct = build_sub_buffering(opts)
        self._buf_keep = buf_struct

        self._qos_keep = None
        if qos is None:
            qos_arg = None
        else:
            qstruct = qos._to_ffi()  # carries express + congestion_control
            self._qos_keep = qstruct
            qos_arg = ctypes.cast(ctypes.byref(qstruct), ctypes.c_void_p)

        handle = bindings.aorta_sub_t(0)
        # Pull mode takes no callback. ctypes won't accept a bare ``None`` for a
        # ``CFUNCTYPE`` argtype, so pass a typed NULL function pointer; Core sees
        # a null ``cb`` and the InlinePull branch ignores it.
        null_cb = ctypes.cast(None, bindings.ON_MSG)
        rc = self._lib.aorta_subscriber_new_buffered(
            node._handle,
            topic.encode("utf-8"),
            qos_arg,
            ctypes.byref(opts_struct),
            ctypes.byref(buf_struct),
            null_cb,  # no callback for pull
            None,  # no user_data
            ctypes.byref(handle),
        )
        errors.check_rc(self._lib, rc, "aorta_subscriber_new_buffered")
        self._handle = handle.value

    def topic(self) -> str:
        return self._topic

    def recv(self, timeout: Optional[float] = None) -> Optional[PullSample]:
        """Pull the next sample.

        ``timeout`` is in seconds: ``None`` blocks until a sample arrives, ``0``
        is non-blocking, and a positive value waits up to that long. A negative
        value raises ``ValueError`` — at the C-ABI a negative ``timeout_ns``
        means block-forever, so silently mapping it to non-blocking here would
        invert the convention a C/C++ porter expects; use ``None`` to block.
        Returns the :class:`PullSample`, or ``None`` if the timeout elapsed
        with no sample. Raises on a disconnected subscriber.
        """
        if timeout is None:
            timeout_ns = -1
        elif timeout < 0:
            raise ValueError(
                "timeout must be None (block), 0 (non-blocking), or positive "
                "seconds; got a negative value (the C-ABI's negative "
                "timeout_ns means block-forever — pass None for that)"
            )
        elif timeout == 0:
            timeout_ns = 0
        else:
            timeout_ns = int(timeout * 1_000_000_000)
        return self._recv_ns(timeout_ns)

    def try_recv(self) -> Optional[PullSample]:
        """Non-blocking pull. ``None`` means the ring is currently empty."""
        return self._recv_ns(0)

    def _recv_ns(self, timeout_ns: int) -> Optional[PullSample]:
        sample = structs.AortaPullSample()
        rc = self._lib.aorta_sub_recv(self._handle, ctypes.byref(sample), timeout_ns)
        if rc == int(ResultCode.TIMEOUT):
            return None  # no sample within the deadline / empty ring
        errors.check_rc(self._lib, rc, "aorta_sub_recv")
        try:
            # Copy the payload + context out before releasing the backing.
            # ``context_from_ptr`` dereferences ``.contents``, so pass a real
            # POINTER (not ``byref``, whose CArgObject has no ``.contents``).
            payload = bindings.bytes_from_view(sample.payload)
            context = context_from_ptr(ctypes.pointer(sample.context))
            return PullSample(payload=payload, context=context)
        finally:
            self._lib.aorta_pull_sample_free(ctypes.byref(sample))

    def dropped_count(self) -> int:
        """Always 0 for pull subscribers (drops happen natively inside the
        ring); present for API symmetry with :class:`~aorta.subscriber.Subscriber`.
        """
        return int(self._lib.aorta_sub_dropped_count(self._handle))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_subscriber_free(self._handle)

    def __enter__(self) -> "PullSubscriber":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
