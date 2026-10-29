"""Pytest fixtures + dev-time discovery of the built Aorta cdylib.

Under Bazel the cdylib is a data dependency; under plain ``pytest`` (dev loop)
we locate the Bazel-built artifact and export ``AORTA_CORE_FFI_LIB`` so the
loader finds it. The ``aorta`` package is imported from ``python/`` (the repo's
existing Python source root).
"""

from __future__ import annotations

import os
import sys

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_PYTHON_ROOT = os.path.join(_REPO_ROOT, "python")
if _PYTHON_ROOT not in sys.path:
    sys.path.insert(0, _PYTHON_ROOT)


def _dylib_name() -> str:
    if sys.platform == "darwin":
        return "libaorta_core.dylib"
    return "libaorta_core.so"


def _discover_cdylib() -> str | None:
    if os.environ.get("AORTA_CORE_FFI_LIB"):
        return os.environ["AORTA_CORE_FFI_LIB"]
    candidate = os.path.join(_REPO_ROOT, "bazel-bin", "core", _dylib_name())
    if os.path.exists(candidate):
        return candidate
    return None


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    path = _discover_cdylib()
    if path and not os.environ.get("AORTA_CORE_FFI_LIB"):
        os.environ["AORTA_CORE_FFI_LIB"] = path


@pytest.fixture(scope="session")
def cdylib_path() -> str:
    path = _discover_cdylib()
    if not path:
        pytest.skip(
            "libaorta_core not built; run `bazel build //core:aorta_core_cdylib`"
        )
    return path


@pytest.fixture()
def lib(cdylib_path: str):  # noqa: ARG001 - ensures the artifact exists
    from aorta._ffi import loader

    return loader.load_library()
