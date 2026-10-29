"""RPC client (synchronous) wrapper + typed client.

``Client.call(request)`` blocks until the reply arrives or the construction-time
timeout fires, then returns the response bytes (or raises the mapped error).
Mirrors the C++ ``Client``/``TypedClient`` and the Rust ``Client``.

Documented gap: the C-ABI ``aorta_client_call`` takes no per-call
timeout, so the Rust-only ``Client::call_with_timeout`` is not bridged — the
timeout is fixed at construction via ``ClientOptions.timeout_ms`` (which is also
exactly the C++ ``Client`` contract).
"""

from __future__ import annotations

import ctypes
from typing import Any, Callable, Optional

import flatbuffers

from . import errors
from ._ffi import bindings, loader, structs
from .header import build_aorta_header
from .options import ClientOptions, ClientStats, build_client_options
from .service import TypedFill, make_header_stamper

RootDecoder = Callable[[bytes], Any]


class TypedClientResponse:
    """Typed wrapper over a raw response. Mirrors the C++ ``TypedClientResponse``.

    ``payload()`` returns the raw response bytes; ``decode()`` runs the response
    schema's root decoder over them.
    """

    def __init__(self, payload: bytes, decoder: RootDecoder) -> None:
        self._payload = payload
        self._decoder = decoder

    def payload(self) -> bytes:
        return self._payload

    def decode(self) -> Any:
        return self._decoder(self._payload)


class Client:
    """A synchronous RPC client. Created via ``Node.create_client*``."""

    def __init__(
        self,
        node,
        service_name: str,
        *,
        options: Optional[ClientOptions] = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        self._service_name = service_name
        self._closed = False
        opts_struct, self._opts_keep = build_client_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        handle = bindings.aorta_client_t(0)
        rc = self._lib.aorta_client_new_with_options(
            node._handle, service_name.encode("utf-8"), opts_arg, ctypes.byref(handle)
        )
        errors.check_rc(self._lib, rc, "aorta_client_new_with_options")
        self._handle = handle.value

    def service_name(self) -> str:
        return self._service_name

    def call(self, request: bytes) -> bytes:
        """Blocking RPC call; returns the response bytes.

        Raises :class:`~aorta.errors.TimeoutError` on timeout and the mapped
        error otherwise. ``request`` must be non-empty (Core rejects an empty
        request with ``INVALID_ARGUMENT``).
        """
        if self._closed:
            raise errors.AortaError("call on a closed Client")
        view, _keep = bindings.owned_view_from_bytes(request)
        resp = structs.AortaOwnedBytes()
        rc = self._lib.aorta_client_call(
            self._handle, ctypes.byref(view), ctypes.byref(resp)
        )
        try:
            errors.check_rc(self._lib, rc, "aorta_client_call")
            return bindings.bytes_from_view(resp.view)
        finally:
            # Idempotent + safe on a zeroed struct (the error path leaves resp
            # zero-initialized): always release the owned response buffer.
            self._lib.aorta_owned_bytes_free(ctypes.byref(resp))

    def matching_status(self) -> bool:
        """One-shot readiness check: is there at least one matching service
        provider (queryable) for this client's service right now? Reads local
        routing tables (no network round-trip). Parity with Rust/C++ ``Client``.
        """
        if self._closed:
            raise errors.AortaError("matching_status on a closed Client")
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_client_matching_status(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_client_matching_status")
        return bool(out.value)

    def wait_for_matching(self, timeout_ms: int) -> bool:
        """Wait up to ``timeout_ms`` for a matching service provider to appear;
        returns the final state (``False`` on timeout, which is not an error).
        Use to gate the first call on a fresh client. Parity with Rust/C++.
        """
        if self._closed:
            raise errors.AortaError("wait_for_matching on a closed Client")
        # Validate BEFORE marshaling: timeout_ms is a c_uint64, so a negative
        # (or >= 2^64) value would wrap to a huge ms count and hang the gate
        # ~forever. Parity with the ClientOptions.timeout_ms validation.
        ms = int(timeout_ms)
        if not 0 <= ms <= 0xFFFFFFFFFFFFFFFF:
            raise errors.InvalidArgumentError(
                f"wait_for_matching timeout_ms out of range [0, 2^64): {timeout_ms!r}"
            )
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_client_wait_for_matching(
            self._handle, ms, ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_client_wait_for_matching")
        return bool(out.value)

    def stats(self) -> ClientStats:
        """Real FFI client counters (``aorta_client_stats_t``)."""
        out = structs.AortaClientStats()
        rc = self._lib.aorta_client_get_stats(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_client_get_stats")
        return ClientStats._from_ffi(out)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_client_free(self._handle)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


class TypedClient:
    """Typed RPC client. ``call(fill)`` injects an AortaHeader into the request
    (per-client monotonic sequence + node name) and decodes the response.
    Mirrors the C++ ``TypedClient``.
    """

    def __init__(self, node, client: Client, response_decoder: RootDecoder) -> None:
        self._client = client
        self._stamper = make_header_stamper(client._lib, node._name)
        self._decoder = response_decoder

    def call(self, fill: TypedFill, *, initial_size: int = 256) -> TypedClientResponse:
        seq, stamp_ns, node = self._stamper()
        builder = flatbuffers.Builder(initial_size)
        header_off = build_aorta_header(
            builder, sequence=seq, publish_stamp_ns=stamp_ns, publisher_node=node
        )
        root_off = fill(builder, header_off)
        builder.Finish(root_off)
        response = self._client.call(bytes(builder.Output()))
        return TypedClientResponse(response, self._decoder)

    def raw(self) -> Client:
        """The underlying raw :class:`Client` (C++ ``Raw()``)."""
        return self._client

    def service_name(self) -> str:
        return self._client.service_name()

    def stats(self) -> ClientStats:
        return self._client.stats()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TypedClient":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
