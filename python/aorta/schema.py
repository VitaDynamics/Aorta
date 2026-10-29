"""Schema attach policy + the SchemaMeta protocol used by typed publishers.

A generated ``*_schema_meta.py`` module (produced by
``gen_schema_meta.py --lang=python``) satisfies :class:`SchemaMeta`: it exposes
``SCHEMA_BFBS``, ``SCHEMA_HASH_HEX``, ``ROOT_TYPE_NAME``, ``FIXED_SIZE``, and
``HAS_AORTA_HEADER``.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Optional

try:  # Protocol is stdlib 3.8+, but guard for type-only usage
    from typing import Protocol, runtime_checkable
except ImportError:  # pragma: no cover - 3.10+ always has it
    Protocol = object  # type: ignore

    def runtime_checkable(cls):  # type: ignore
        return cls


class SchemaAttachPolicy(IntEnum):
    """Mirror of ``aorta_schema_attach_policy_t`` (core/src/ffi/c_api/mod.rs)."""

    INHERIT = 0
    ALWAYS = 1
    NEVER = 2


@runtime_checkable
class SchemaMeta(Protocol):
    """Structural type for a generated schema-metadata module."""

    SCHEMA_BFBS: bytes
    SCHEMA_HASH_HEX: str
    ROOT_TYPE_NAME: str
    FIXED_SIZE: Optional[int]
    HAS_AORTA_HEADER: bool
