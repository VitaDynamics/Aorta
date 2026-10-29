"""ExecutionGroup — bind primitives to a shared callback group.

``node.create_execution_group(policy)`` mints a node-level callback group
(SERIALIZED → MutuallyExclusive, CONCURRENT → Reentrant). Stamp it onto a
primitive's options via :meth:`timer_options` / :meth:`service_options` so those
callbacks share the group's scheduling discipline (e.g. SERIALIZED guarantees
they never run concurrently with each other). Mirrors the C++ ``ExecutionGroup``
(``Bind`` overloads) and the Rust ``ExecutionGroup`` (per-type stampers).

Stamps ``timer_options`` / ``service_options`` / ``subscriber_options`` (the last
added in Phase 5 alongside ``SubscriberOptions``).
"""

from __future__ import annotations

import ctypes
import dataclasses
from typing import Optional

from . import errors
from ._ffi import bindings, loader
from .options import ExecutionPolicy, ServiceOptions, SubscriberOptions, TimerOptions


class ExecutionGroup:
    """A shared callback group. Created via ``Node.create_execution_group``."""

    def __init__(self, node, policy: ExecutionPolicy) -> None:
        self._lib = bindings.configure(loader.library())
        self._node = node
        self._policy = policy
        self._closed = False
        out = bindings.aorta_callback_group_handle_t(0)
        rc = self._lib.aorta_node_create_callback_group(
            node._handle, int(policy.to_callback_group()), ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_node_create_callback_group")
        self._handle = out.value

    def handle(self) -> int:
        """The underlying callback group handle."""
        return self._handle

    def policy(self) -> ExecutionPolicy:
        return self._policy

    def timer_options(self, options: Optional[TimerOptions] = None) -> TimerOptions:
        """Return a copy of ``options`` (or defaults) bound to this group."""
        self._ensure_open()
        base = options if options is not None else TimerOptions()
        # Stamp BOTH the handle and the callback_group enum: Core falls back to
        # the enum when the handle is unavailable, so the enum must reflect the
        # group's policy (else a CONCURRENT group could degrade to mutually-
        # exclusive). Parity with C++/Rust which stamp both.
        return dataclasses.replace(
            base,
            callback_group=self._policy.to_callback_group(),
            callback_group_handle=self._handle,
        )

    def service_options(self, options: Optional[ServiceOptions] = None) -> ServiceOptions:
        """Return a copy of ``options`` (or defaults) bound to this group."""
        self._ensure_open()
        base = options if options is not None else ServiceOptions()
        return dataclasses.replace(
            base,
            callback_group=self._policy.to_callback_group(),
            callback_group_handle=self._handle,
        )

    def subscriber_options(self, options: Optional[SubscriberOptions] = None) -> SubscriberOptions:
        """Return a copy of ``options`` (or defaults) bound to this group."""
        self._ensure_open()
        base = options if options is not None else SubscriberOptions()
        return dataclasses.replace(
            base,
            callback_group=self._policy.to_callback_group(),
            callback_group_handle=self._handle,
        )

    def close(self) -> None:
        """Free the callback group. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_node_free_callback_group(self._node._handle, self._handle)
        self._handle = 0  # never stamp a freed handle into new options

    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.AortaError("operation on a closed ExecutionGroup")

    def __enter__(self) -> "ExecutionGroup":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
