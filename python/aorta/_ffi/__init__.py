"""Private FFI layer: loader, ctypes struct mirrors, and signature bindings.

Nothing in ``aorta._ffi`` is public API. ctypes ``Structure`` mirrors and raw
``CDLL`` handles must not leak into the typed public surface (Python SDK design
proposal §4.3).
"""

from __future__ import annotations

from .loader import AbiGuardError, AortaLoadError, library, load_library

__all__ = ["load_library", "library", "AortaLoadError", "AbiGuardError"]
