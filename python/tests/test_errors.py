"""Tests for aorta.errors — result codes, exception mapping, last_error, check_rc."""

from __future__ import annotations

import ctypes

import pytest

from aorta import errors
from aorta.errors import (
    AlreadyRepliedError,
    AortaError,
    BusyError,
    BufferTooSmallError,
    CancelledError,
    ConcurrentConflictError,
    DeclarationNotReadyError,
    InstanceIdCollisionError,
    InternalError,
    InvalidArgumentError,
    KindMismatchError,
    NotFoundError,
    PrefixNotDeclaredError,
    ReentrantError,
    ResultCode,
    SchemaConflictError,
    TimeoutError,
    check_rc,
    exception_for_code,
    last_error,
)


class _FakeLastErrorFn:
    def __init__(self, value):
        self.argtypes = None
        self.restype = None
        self._value = value

    def __call__(self):
        return self._value


class _FakeLib:
    def __init__(self, last_error_value):
        self.aorta_last_error = _FakeLastErrorFn(last_error_value)


def test_result_code_values():
    assert ResultCode.OK == 0
    assert ResultCode.INVALID_ARGUMENT == -1
    assert ResultCode.REENTRANT == -15
    assert ResultCode.CANCELLED == -16
    assert ResultCode.BUFFER_TOO_SMALL == -17
    # -6 and -9 are intentionally absent
    present = {int(c) for c in ResultCode}
    assert -6 not in present
    assert -9 not in present


@pytest.mark.parametrize(
    "code,exc",
    [
        (-1, InvalidArgumentError),
        (-2, NotFoundError),
        (-3, TimeoutError),
        (-4, BusyError),
        (-5, InternalError),
        (-7, SchemaConflictError),
        (-8, ConcurrentConflictError),
        (-10, AlreadyRepliedError),
        (-11, PrefixNotDeclaredError),
        (-12, DeclarationNotReadyError),
        (-13, KindMismatchError),
        (-14, InstanceIdCollisionError),
        (-15, ReentrantError),
        (-16, CancelledError),
        (-17, BufferTooSmallError),
    ],
)
def test_exception_for_code_maps_all_15(code, exc):
    err = exception_for_code(code, "boom")
    assert isinstance(err, exc)
    assert err.code == code
    assert err.message == "boom"


def test_every_result_code_has_a_distinct_exception():
    # All 15 non-OK codes map to a distinct, code-correct exception subclass.
    mapped = {
        int(rc): type(exception_for_code(int(rc), ""))
        for rc in ResultCode
        if rc != ResultCode.OK
    }
    assert len(mapped) == 15
    for code, exc in mapped.items():
        assert exc is not AortaError, f"code {code} fell back to base AortaError"
        assert exc.code == code


def test_exception_for_code_unknown_falls_back_to_base():
    err = exception_for_code(-999, "mystery")
    assert type(err) is AortaError
    assert err.code == -999


def test_error_render_with_code():
    err = InvalidArgumentError("bad name")
    assert "INVALID_ARGUMENT" in str(err)
    assert "(-1)" in str(err)
    assert "bad name" in str(err)


def test_error_render_without_code():
    err = AortaError("sdk-internal", code=None)
    assert str(err) == "sdk-internal"


def test_error_render_unknown_code_uses_number():
    err = AortaError("x", code=-999)
    assert "-999" in str(err)


def test_last_error_returns_decoded_message():
    fake = _FakeLib(b"something went wrong")
    assert last_error(fake) == "something went wrong"


def test_last_error_none_when_null():
    fake = _FakeLib(None)
    assert last_error(fake) is None


def test_last_error_sets_signature_once():
    fake = _FakeLib(b"msg")
    # first call configures restype, second call hits the already-configured path
    assert last_error(fake) == "msg"
    fake.aorta_last_error.restype = ctypes.c_char_p
    assert last_error(fake) == "msg"


def test_check_rc_ok_is_noop():
    fake = _FakeLib(b"unused")
    assert check_rc(fake, 0, "op") is None


def test_check_rc_raises_mapped_with_message():
    fake = _FakeLib(b"node handle not found")
    with pytest.raises(NotFoundError, match="node handle not found") as exc_info:
        check_rc(fake, -2, "aorta_node_now")
    assert exc_info.value.code == -2


def test_check_rc_uses_op_when_no_message():
    fake = _FakeLib(None)
    with pytest.raises(BusyError, match="aorta_client_call failed"):
        check_rc(fake, -4, "aorta_client_call")


def test_check_rc_real_ffi_error_path(lib):
    # End-to-end against the real cdylib: a genuine failing FFI call must set
    # the thread-local error, and check_rc must map it to the right exception
    # with the real message read immediately on the same thread.
    lib.aorta_now_ns.argtypes = [ctypes.POINTER(ctypes.c_int64)]
    lib.aorta_now_ns.restype = ctypes.c_int32
    rc = lib.aorta_now_ns(None)  # NULL out pointer → INVALID_ARGUMENT
    assert rc == ResultCode.INVALID_ARGUMENT
    with pytest.raises(InvalidArgumentError, match="out_ns is null"):
        check_rc(lib, rc, "aorta_now_ns")


def test_timeout_error_is_builtin_timeout_error():
    # A bare `except TimeoutError:` must catch Aorta
    # RPC/action timeouts.
    err = errors.TimeoutError("rpc timed out")
    assert isinstance(err, TimeoutError)
    assert isinstance(err, errors.AortaError)
    with pytest.raises(TimeoutError):
        raise errors.TimeoutError("deadline elapsed")


def test_capture_reuses_sdk_error_hierarchy():
    # Capture must not define a parallel AortaError.
    from aorta import capture

    assert capture.AortaError is errors.AortaError
