"""Publisher wrapper. Raw byte publish; typed publish_typed lands with codegen wiring."""

from __future__ import annotations

import ctypes
import os
import threading
from typing import Callable

import flatbuffers

from . import errors
from ._ffi import bindings, loader, structs
from .header import build_aorta_header
from .options import PublisherStats
from .qos import QoS
from .schema import SchemaAttachPolicy

# fill(builder, aorta_header_offset) -> root_table_offset
TypedFill = Callable[["flatbuffers.Builder", int], int]

_ENV_INCLUDE_PUBLISHER_NODE = "AORTA_HEADER_INCLUDE_PUBLISHER_NODE"
_FALSEY = {"0", "false", "no", "off"}


def _include_publisher_node() -> bool:
    """Whether to write publisher_node into the AortaHeader (parity with the
    Rust/C++ ``AORTA_HEADER_INCLUDE_PUBLISHER_NODE`` env; default: include)."""
    raw = os.environ.get(_ENV_INCLUDE_PUBLISHER_NODE)
    if raw is None:
        return True
    return raw.strip().lower() not in _FALSEY


class Publisher:
    """A raw byte publisher. Created via ``Node.create_publisher``.

    ``publish_bytes`` is synchronous fire-and-forget (no await), mirroring the
    Rust/C++ contract; it sends the bytes verbatim. ``publish_typed`` injects a
    byte-identical AortaHeader (with a per-publisher monotonic sequence) and lets
    the caller build the message body around it.
    """

    def __init__(
        self,
        node,
        topic: str,
        *,
        qos: "QoS" = None,
        schema_bfbs=None,
        schema_root_type: str | None = None,
        schema_attach_policy: SchemaAttachPolicy = SchemaAttachPolicy.INHERIT,
        max_message_size: int = 0,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        self._topic = topic
        self._node_name = node._name
        self._sequence = 0
        self._seq_lock = threading.Lock()
        self._include_node = _include_publisher_node()
        self._closed = False
        # Build aorta_pub_options_t only when a schema / non-default policy /
        # size override is requested; keep the bfbs buffer + options struct
        # alive across the (synchronous) publisher_new call.
        self._schema_buf = None
        self._schema_root_type_buf = None
        self._opts = None
        opts_arg = None
        # congestion_control now rides aorta_qos_t (carried by qos._to_ffi); the
        # SDK exposes no per-publisher override, so the aorta_pub_options_t
        # override byte is left zero-initialized = AORTA_PUB_CONGESTION_INHERIT
        # and Core resolves it against qos.congestion_control. Build the options
        # struct only for a schema / non-default attach policy / size override.
        if (
            schema_bfbs is not None
            or schema_root_type is not None
            or schema_attach_policy != SchemaAttachPolicy.INHERIT
            or max_message_size
        ):
            opts = structs.AortaPubOptions()
            opts.schema_attach_policy = int(schema_attach_policy)
            if schema_bfbs is not None:
                raw = bytes(schema_bfbs)
                self._schema_buf = ctypes.create_string_buffer(raw, len(raw))
                opts.schema_bfbs = ctypes.cast(self._schema_buf, ctypes.c_void_p)
                opts.schema_bfbs_len = len(raw)
            if schema_root_type is not None:
                self._schema_root_type_buf = ctypes.create_string_buffer(
                    schema_root_type.encode("utf-8")
                )
                opts.schema_root_type = ctypes.cast(
                    self._schema_root_type_buf, ctypes.c_char_p
                )
            opts.max_message_size = max_message_size
            self._opts = opts
            opts_arg = ctypes.cast(ctypes.byref(opts), ctypes.c_void_p)
        # QoS (priority/reliability/congestion_control + express) is opt-in;
        # the no-QoS path stays byte-identical (NULL qos -> core defaults).
        self._qos_keep = None
        handle = bindings.aorta_pub_t(0)
        if qos is None:
            qos_arg = None
        else:
            qstruct = qos._to_ffi()  # carries express + congestion_control
            self._qos_keep = qstruct  # keep alive across the synchronous new call
            qos_arg = ctypes.cast(ctypes.byref(qstruct), ctypes.c_void_p)
        rc = self._lib.aorta_publisher_new(
            node._handle, topic.encode("utf-8"), qos_arg, opts_arg,
            ctypes.byref(handle),
        )
        errors.check_rc(self._lib, rc, "aorta_publisher_new")
        self._handle = handle.value

    def topic(self) -> str:
        return self._topic

    def stats(self) -> PublisherStats:
        """Real FFI publisher counters (``aorta_publisher_stats_t``) — parity
        with Rust ``Publisher::stats`` / C++ ``Publisher::GetStats``."""
        out = structs.AortaPublisherStats()
        rc = self._lib.aorta_publisher_get_stats(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_publisher_get_stats")
        return PublisherStats._from_ffi(out)

    def matching_status(self) -> bool:
        """One-shot readiness check: is there at least one matching subscriber
        for this publisher's key right now? Reads local routing tables (no
        network round-trip). Parity with Rust ``Publisher::matching_status`` /
        C++ ``Publisher::MatchingStatus``.
        """
        if self._closed:
            raise errors.AortaError("matching_status on a closed Publisher")
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_publisher_matching_status(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_publisher_matching_status")
        return bool(out.value)

    def wait_for_matching(self, timeout_ms: int) -> bool:
        """Wait up to ``timeout_ms`` for a matching subscriber to appear; returns
        the final state (``False`` on timeout, which is not an error). Use to gate
        the first publish on a fresh client. Parity with Rust
        ``Publisher::wait_for_matching`` / C++ ``Publisher::WaitForMatching``.
        """
        if self._closed:
            raise errors.AortaError("wait_for_matching on a closed Publisher")
        # Validate BEFORE marshaling: timeout_ms is a c_uint64, so a negative
        # (or >= 2^64) value would wrap to a huge ms count and hang the gate
        # ~forever. Parity with the ClientOptions.timeout_ms validation.
        ms = int(timeout_ms)
        if not 0 <= ms <= 0xFFFFFFFFFFFFFFFF:
            raise errors.InvalidArgumentError(
                f"wait_for_matching timeout_ms out of range [0, 2^64): {timeout_ms!r}"
            )
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_publisher_wait_for_matching(
            self._handle, ms, ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_publisher_wait_for_matching")
        return bool(out.value)

    def publish_bytes(self, data) -> None:
        """Publish raw bytes (accepts bytes/bytearray/memoryview)."""
        if self._closed:
            raise errors.AortaError("publish on a closed Publisher")
        buf = data if isinstance(data, bytes) else bytes(data)
        rc = self._lib.aorta_publisher_publish(self._handle, buf, len(buf))
        errors.check_rc(self._lib, rc, "aorta_publisher_publish")

    def publish_typed(self, fill: TypedFill, *, initial_size: int = 256) -> None:
        """Build a message with an injected AortaHeader and publish it.

        ``fill(builder, header_offset)`` builds the message body (embedding the
        AortaHeader at the schema's ``aorta_header`` field) and returns the root
        table offset. The header carries a per-publisher monotonic ``sequence``
        (from 0) and this node's name; ``publish_stamp_ns`` defaults to now.
        Synchronous fire-and-forget, like ``publish_bytes``.
        """
        if self._closed:
            raise errors.AortaError("publish on a closed Publisher")
        builder = flatbuffers.Builder(initial_size)
        # Atomic fetch-add so concurrent publishers don't reuse a sequence
        # (parity with the Rust/C++ atomic counter).
        with self._seq_lock:
            seq = self._sequence
            self._sequence += 1
        # Timestamp from the Aorta/Core clock (honors simulation time / overrides),
        # NOT the Python wall clock, matching Rust/C++ typed publish.
        stamp = ctypes.c_int64(0)
        rc = self._lib.aorta_now_ns(ctypes.byref(stamp))
        errors.check_rc(self._lib, rc, "aorta_now_ns")
        publisher_node = self._node_name if self._include_node else None
        header_off = build_aorta_header(
            builder,
            sequence=seq,
            publish_stamp_ns=stamp.value,
            publisher_node=publisher_node,
        )
        root_off = fill(builder, header_off)
        builder.Finish(root_off)
        self.publish_bytes(bytes(builder.Output()))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_publisher_free(self._handle)

    def __enter__(self) -> "Publisher":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
