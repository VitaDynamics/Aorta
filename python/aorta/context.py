"""MessageContext — opt-in per-message metadata delivered to subscriber callbacks."""

from __future__ import annotations

from dataclasses import dataclass

from ._ffi import bindings, structs


@dataclass(frozen=True)
class MessageContext:
    """Per-message metadata. Fields are populated only when the corresponding
    SubscriberOptions flags are enabled; unset string fields are ``""`` and
    unset numeric fields are ``0`` (mirrors the C-ABI ``aorta_msg_context_t``).

    Note: the on_msg schema-hash view is intentionally NOT surfaced — Core's FFI
    passes an empty view and the C++ SDK discards it (``(void)schema``); exposing
    it would be a Python-only field beyond Rust/C++ parity.
    """

    topic: str
    receive_stamp_ns: int
    priority: int
    publisher_node: str
    sequence: int


_EMPTY = MessageContext("", 0, 0, "", 0)


def context_from_ptr(ctx_ptr) -> MessageContext:
    """Build a MessageContext from a borrowed ``aorta_msg_context_t*`` (or null)."""
    if not ctx_ptr:
        return _EMPTY
    ctx: structs.AortaMsgContext = ctx_ptr.contents
    return MessageContext(
        topic=bindings.str_from_view(ctx.topic),
        receive_stamp_ns=int(ctx.receive_stamp_ns),
        priority=int(ctx.priority),
        publisher_node=bindings.str_from_view(ctx.publisher_node),
        sequence=int(ctx.sequence),
    )
