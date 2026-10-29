"""Aorta unified logging — Python surface over the C-ABI ``aorta_log_*`` FFI.

Folded from the legacy top-level ``python/logging.py`` into the ``aorta`` package:
living under ``aorta.logging`` instead of a top-level
``logging`` module avoids shadowing the standard library's ``logging``. It now
binds through the shared loader (``aorta._ffi.loader``) instead of carrying its
own ``dlopen`` — one library, one ABI guard (DRY).

Core remains the canonical log sink; this module is a thin, level-guarded shim.
"""

from __future__ import annotations

import ctypes
import inspect
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional, Tuple

from . import errors
from ._ffi import loader

__all__ = [
    "LogFormat",
    "LogLevel",
    "LogConfig",
    "TRACE",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "configure_logging",
    "init_from_env_once",
    "set_default_context",
    "flush_logging",
    "is_enabled",
    "write_log",
    "write_log_at",
    "log",
    "trace",
    "debug",
    "info",
    "warning",
    "warn",
    "error",
    "if_",
    "every_n",
]


class LogFormat(IntEnum):
    TEXT = 0
    JSON = 1


class LogLevel(IntEnum):
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4


TRACE = LogLevel.TRACE
DEBUG = LogLevel.DEBUG
INFO = LogLevel.INFO
WARN = LogLevel.WARN
ERROR = LogLevel.ERROR


@dataclass
class LogConfig:
    filter: Optional[str] = None
    file_path: Optional[str] = None
    format: LogFormat = LogFormat.TEXT
    stderr_enabled: bool = True


class _AortaLogConfigC(ctypes.Structure):
    _fields_ = [
        ("filter", ctypes.c_char_p),
        ("file_path", ctypes.c_char_p),
        ("format", ctypes.c_int),
        ("stderr_enabled", ctypes.c_uint8),
    ]


_bound = False
_bind_lock = threading.Lock()
_every_n_lock = threading.Lock()
_every_n_counter: Dict[Tuple[int, str, str, int, int], int] = {}


def _lib() -> ctypes.CDLL:
    """Return the shared, ABI-guarded library with ``aorta_log_*`` bound once."""
    global _bound
    lib = loader.library()
    if _bound:
        return lib
    with _bind_lock:
        if _bound:  # pragma: no cover - race re-check
            return lib
        lib.aorta_log_init_from_env.argtypes = []
        lib.aorta_log_init_from_env.restype = ctypes.c_int32
        lib.aorta_log_configure.argtypes = [ctypes.POINTER(_AortaLogConfigC)]
        lib.aorta_log_configure.restype = ctypes.c_int32
        lib.aorta_log_flush.argtypes = []
        lib.aorta_log_flush.restype = ctypes.c_int32
        lib.aorta_log_write_at.argtypes = [
            ctypes.c_int32,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_uint32,
            ctypes.c_char_p,
        ]
        lib.aorta_log_write_at.restype = ctypes.c_int32
        lib.aorta_log_set_context.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.aorta_log_set_context.restype = ctypes.c_int32
        lib.aorta_log_enabled.argtypes = [ctypes.c_int32, ctypes.c_char_p]
        lib.aorta_log_enabled.restype = ctypes.c_uint8
        _bound = True
        return lib


def _check(rc: int, op: str) -> None:
    if rc != 0:
        raise errors.AortaError(f"{op} failed", code=rc)


def _format_message(message: str, *args: object) -> str:
    if not args:
        return message
    return message % args


def _caller_info(depth: int = 2) -> Tuple[str, int]:
    frame = inspect.currentframe()
    for _ in range(depth):
        if frame is None:  # pragma: no cover - defensive; frames present in CPython
            return ("", 0)
        frame = frame.f_back
    if frame is None:  # pragma: no cover - defensive
        return ("", 0)
    return (frame.f_code.co_filename, frame.f_lineno)


def init_from_env_once() -> None:
    lib = _lib()
    _check(lib.aorta_log_init_from_env(), "aorta_log_init_from_env")


def configure_logging(config: LogConfig) -> None:
    lib = _lib()
    ffi = _AortaLogConfigC(
        filter=config.filter.encode("utf-8") if config.filter else None,
        file_path=config.file_path.encode("utf-8") if config.file_path else None,
        format=int(config.format),
        stderr_enabled=1 if config.stderr_enabled else 0,
    )
    _check(lib.aorta_log_configure(ctypes.byref(ffi)), "aorta_log_configure")


def set_default_context(node: str, group: str) -> None:
    lib = _lib()
    _check(
        lib.aorta_log_set_context(node.encode("utf-8"), group.encode("utf-8")),
        "aorta_log_set_context",
    )


def flush_logging() -> None:
    lib = _lib()
    _check(lib.aorta_log_flush(), "aorta_log_flush")


def is_enabled(level: LogLevel, scope: str = "aorta_sdk") -> bool:
    lib = _lib()
    return bool(lib.aorta_log_enabled(int(level), scope.encode("utf-8")))


def write_log_at(level: LogLevel, scope: str, file: str, line: int, message: str) -> None:
    lib = _lib()
    _check(
        lib.aorta_log_write_at(
            int(level),
            scope.encode("utf-8"),
            file.encode("utf-8"),
            int(line),
            message.encode("utf-8"),
        ),
        "aorta_log_write_at",
    )


def write_log(level: LogLevel, scope: str, message: str) -> None:
    file, line = _caller_info(depth=2)
    write_log_at(level, scope, file, line, message)


def log(
    level: LogLevel,
    message: str,
    *args: object,
    scope: str = "aorta_sdk",
    _caller_depth: int = 2,
) -> None:
    if not is_enabled(level, scope):
        return
    file, line = _caller_info(depth=_caller_depth)
    write_log_at(level, scope, file, line, _format_message(message, *args))


def trace(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.TRACE, message, *args, scope=scope, _caller_depth=3)


def debug(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.DEBUG, message, *args, scope=scope, _caller_depth=3)


def info(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.INFO, message, *args, scope=scope, _caller_depth=3)


def warning(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.WARN, message, *args, scope=scope, _caller_depth=3)


def warn(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.WARN, message, *args, scope=scope, _caller_depth=3)


def error(message: str, *args: object, scope: str = "aorta_sdk") -> None:
    log(LogLevel.ERROR, message, *args, scope=scope, _caller_depth=3)


def if_(
    level: LogLevel,
    condition: bool,
    message: str,
    *args: object,
    scope: str = "aorta_sdk",
) -> None:
    if condition:
        log(level, message, *args, scope=scope, _caller_depth=3)


def every_n(
    level: LogLevel,
    n: int,
    message: str,
    *args: object,
    scope: str = "aorta_sdk",
) -> None:
    if n <= 0:
        log(level, message, *args, scope=scope, _caller_depth=3)
        return

    file, line = _caller_info(depth=2)
    key = (int(level), scope, file, line, n)
    with _every_n_lock:
        count = _every_n_counter.get(key, 0) + 1
        _every_n_counter[key] = count
    if ((count - 1) % n) != 0:
        return
    if not is_enabled(level, scope):
        return
    write_log_at(level, scope, file, line, _format_message(message, *args))
