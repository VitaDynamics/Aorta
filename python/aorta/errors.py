"""Aorta SDK exception hierarchy and C-ABI result-code mapping.

Every non-zero ``aorta_result_t`` (``i32``) is turned into a typed exception
carrying the numeric ``code`` and the thread-local error ``message`` retrieved
from ``aorta_last_error`` on the *same* thread (it is invalidated by the next
FFI call). The mapping lives here, in exactly one place — DRY, never forked per
call site (Python SDK design proposal §5.2).

Note: several distinct *core* errors collapse onto shared FFI codes in
``map_aorta_error`` (core/src/ffi/c_api/mod.rs): e.g. ShmUnavailable/QuotaExceeded
→ BUSY(-4), CapacityExceeded/NotSupported → INVALID_ARGUMENT(-1), Transport →
INTERNAL(-5). The Python hierarchy keys off the 15 distinct FFI codes; the
original granularity survives in the message string.
"""

from __future__ import annotations

import builtins
import ctypes
from enum import IntEnum
from typing import Dict, Optional, Type

__all__ = [
    "ResultCode",
    "AortaError",
    "InvalidArgumentError",
    "NotFoundError",
    "TimeoutError",
    "BusyError",
    "InternalError",
    "SchemaConflictError",
    "ConcurrentConflictError",
    "AlreadyRepliedError",
    "PrefixNotDeclaredError",
    "DeclarationNotReadyError",
    "KindMismatchError",
    "InstanceIdCollisionError",
    "ReentrantError",
    "CancelledError",
    "BufferTooSmallError",
    "last_error",
    "check_rc",
    "exception_for_code",
]


class ResultCode(IntEnum):
    """Mirror of the ``aorta_result_t`` constants (core/src/ffi/c_api/mod.rs).

    Codes ``-6`` and ``-9`` are intentionally unused in core.
    """

    OK = 0
    INVALID_ARGUMENT = -1
    NOT_FOUND = -2
    TIMEOUT = -3
    BUSY = -4
    INTERNAL = -5
    SCHEMA_CONFLICT = -7
    CONCURRENT_CONFLICT = -8
    ALREADY_REPLIED = -10
    PREFIX_NOT_DECLARED = -11
    DECLARATION_NOT_READY = -12
    KIND_MISMATCH = -13
    INSTANCE_ID_COLLISION = -14
    REENTRANT = -15
    CANCELLED = -16
    BUFFER_TOO_SMALL = -17


class AortaError(RuntimeError):
    """Base class for all Aorta SDK errors.

    Attributes:
        code: the numeric ``aorta_result_t`` value (``None`` for SDK-internal
            errors that do not originate from a C-ABI return code).
        message: the human-readable detail (from ``aorta_last_error`` when
            available, else a synthesised description).
    """

    code: Optional[int] = None

    def __init__(self, message: str = "", *, code: Optional[int] = None) -> None:
        self.message = message
        if code is not None:
            self.code = code
        super().__init__(self._render())

    def _render(self) -> str:
        if self.code is None:
            return self.message
        name = _CODE_NAMES.get(self.code, str(self.code))
        return f"[{name} ({self.code})] {self.message}".rstrip()


class InvalidArgumentError(AortaError):
    code = ResultCode.INVALID_ARGUMENT


class NotFoundError(AortaError):
    code = ResultCode.NOT_FOUND


class TimeoutError(AortaError, builtins.TimeoutError):  # noqa: A001 - intentional Aorta-scoped Timeout
    """Also subclasses the builtin ``TimeoutError`` so a plain
    ``except TimeoutError:`` catches Aorta RPC/action timeouts too."""

    code = ResultCode.TIMEOUT


class BusyError(AortaError):
    code = ResultCode.BUSY


class InternalError(AortaError):
    code = ResultCode.INTERNAL


class SchemaConflictError(AortaError):
    code = ResultCode.SCHEMA_CONFLICT


class ConcurrentConflictError(AortaError):
    code = ResultCode.CONCURRENT_CONFLICT


class AlreadyRepliedError(AortaError):
    code = ResultCode.ALREADY_REPLIED


class PrefixNotDeclaredError(AortaError):
    code = ResultCode.PREFIX_NOT_DECLARED


class DeclarationNotReadyError(AortaError):
    code = ResultCode.DECLARATION_NOT_READY


class KindMismatchError(AortaError):
    code = ResultCode.KIND_MISMATCH


class InstanceIdCollisionError(AortaError):
    code = ResultCode.INSTANCE_ID_COLLISION


class ReentrantError(AortaError):
    code = ResultCode.REENTRANT


class CancelledError(AortaError):
    """Flow cancellation (-16): the operation was refused or stopped because
    its scope was cancelled / its budget expired. Python ships no Flow
    bindings yet, so today this only surfaces via ambient-scope-gated core
    paths; mapped here for completeness (review 2026-07-12)."""

    code = ResultCode.CANCELLED


class BufferTooSmallError(AortaError):
    code = ResultCode.BUFFER_TOO_SMALL


_CODE_TO_EXC: Dict[int, Type[AortaError]] = {
    ResultCode.INVALID_ARGUMENT: InvalidArgumentError,
    ResultCode.NOT_FOUND: NotFoundError,
    ResultCode.TIMEOUT: TimeoutError,
    ResultCode.BUSY: BusyError,
    ResultCode.INTERNAL: InternalError,
    ResultCode.SCHEMA_CONFLICT: SchemaConflictError,
    ResultCode.CONCURRENT_CONFLICT: ConcurrentConflictError,
    ResultCode.ALREADY_REPLIED: AlreadyRepliedError,
    ResultCode.PREFIX_NOT_DECLARED: PrefixNotDeclaredError,
    ResultCode.DECLARATION_NOT_READY: DeclarationNotReadyError,
    ResultCode.KIND_MISMATCH: KindMismatchError,
    ResultCode.INSTANCE_ID_COLLISION: InstanceIdCollisionError,
    ResultCode.REENTRANT: ReentrantError,
    ResultCode.CANCELLED: CancelledError,
    ResultCode.BUFFER_TOO_SMALL: BufferTooSmallError,
}

_CODE_NAMES: Dict[int, str] = {int(rc): rc.name for rc in ResultCode}


def exception_for_code(code: int, message: str) -> AortaError:
    """Construct the typed exception for an ``aorta_result_t`` code.

    Unknown negative codes fall back to the base :class:`AortaError` so future
    core additions never silently degrade to a wrong type.
    """
    exc_type = _CODE_TO_EXC.get(code, AortaError)
    return exc_type(message, code=code)


def last_error(lib: ctypes.CDLL) -> Optional[str]:
    """Return the current thread's last Aorta error message, if any.

    ``aorta_last_error`` is thread-local and invalidated by the next FFI call,
    so this must be read on the same thread immediately after a failing call.
    """
    if getattr(lib.aorta_last_error, "restype", None) is not ctypes.c_char_p:
        lib.aorta_last_error.argtypes = []
        lib.aorta_last_error.restype = ctypes.c_char_p
    raw = lib.aorta_last_error()
    if raw is None:
        return None
    return raw.decode("utf-8", errors="replace")


def check_rc(lib: ctypes.CDLL, rc: int, op: str) -> None:
    """Raise the mapped exception when ``rc`` is a non-OK ``aorta_result_t``.

    No-op on ``AORTA_OK`` (0). On error, retrieves the thread-local message and
    raises the typed exception. ``op`` is prepended for context when no
    thread-local message is available.
    """
    if rc == ResultCode.OK:
        return
    detail = last_error(lib)
    message = detail if detail else f"{op} failed"
    raise exception_for_code(rc, message)
