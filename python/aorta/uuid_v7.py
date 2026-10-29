"""Business-agnostic UUIDv7 generation."""

from __future__ import annotations

import ctypes

from . import errors
from ._ffi import bindings, loader


def new_uuid_v7() -> str:
    """Return one canonical lowercase hyphenated UUIDv7 string."""
    lib = bindings.configure(loader.library())
    output = ctypes.create_string_buffer(37)
    rc = lib.aorta_uuid_v7(output, len(output))
    errors.check_rc(lib, rc, "aorta_uuid_v7")
    return output.value.decode("ascii")
