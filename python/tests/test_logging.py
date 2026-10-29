"""Tests for aorta.logging — the folded, shared-loader logging shim."""

from __future__ import annotations

import pytest

from aorta import errors, logging as alog


@pytest.fixture(autouse=True)
def _ensure_lib(lib):  # noqa: ARG001 - guarantees the cdylib is loaded/bound
    return lib


# --- enums / dataclass / config struct ------------------------------------

def test_levels_and_formats():
    assert alog.LogLevel.TRACE == 0
    assert alog.LogLevel.ERROR == 4
    assert alog.LogFormat.TEXT == 0
    assert alog.LogFormat.JSON == 1
    assert alog.INFO is alog.LogLevel.INFO


def test_logconfig_defaults():
    cfg = alog.LogConfig()
    assert cfg.filter is None
    assert cfg.file_path is None
    assert cfg.format == alog.LogFormat.TEXT
    assert cfg.stderr_enabled is True


def test_lib_binds_and_is_cached():
    # Force the one-time bind path to run even if another test (e.g. an action
    # handler-exception log) already triggered it earlier in the session.
    # Re-binding argtypes is idempotent.
    alog._bound = False
    first = alog._lib()
    second = alog._lib()
    assert first is second
    assert alog._bound is True


# --- helpers --------------------------------------------------------------

def test_check_ok_is_noop():
    assert alog._check(0, "op") is None


def test_check_raises_aorta_error_with_code():
    with pytest.raises(errors.AortaError) as exc_info:
        alog._check(-5, "aorta_log_flush")
    assert exc_info.value.code == -5


def test_format_message_without_args():
    assert alog._format_message("plain") == "plain"


def test_format_message_with_args():
    assert alog._format_message("x=%d y=%s", 1, "a") == "x=1 y=a"


def test_caller_info_reports_this_file():
    file, line = alog._caller_info(depth=1)
    assert file.endswith("test_logging.py")
    assert line > 0


# --- FFI-backed happy paths (real cdylib; rc==0) --------------------------

def test_configure_quietly():
    alog.configure_logging(alog.LogConfig(stderr_enabled=False, filter="aorta_sdk=trace"))


def test_init_from_env_once():
    alog.init_from_env_once()


def test_set_default_context():
    alog.set_default_context("test_node", "test_group")


def test_flush_logging():
    alog.flush_logging()


def test_is_enabled_returns_bool():
    assert isinstance(alog.is_enabled(alog.LogLevel.ERROR), bool)


def test_write_log_at_and_write_log():
    alog.write_log_at(alog.LogLevel.INFO, "aorta_sdk", __file__, 1, "direct write_at")
    alog.write_log(alog.LogLevel.INFO, "aorta_sdk", "direct write")


# --- log()/level helpers with deterministic enable state ------------------

def test_log_writes_when_enabled(monkeypatch):
    calls = []
    monkeypatch.setattr(alog, "is_enabled", lambda level, scope: True)
    monkeypatch.setattr(
        alog, "write_log_at", lambda *a: calls.append(a)
    )
    alog.log(alog.LogLevel.INFO, "hello %s", "world")
    assert calls and calls[0][4] == "hello world"


def test_log_skips_when_disabled(monkeypatch):
    calls = []
    monkeypatch.setattr(alog, "is_enabled", lambda level, scope: False)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: calls.append(a))
    alog.log(alog.LogLevel.TRACE, "nope")
    assert calls == []


@pytest.mark.parametrize(
    "fn,level",
    [
        (alog.trace, alog.LogLevel.TRACE),
        (alog.debug, alog.LogLevel.DEBUG),
        (alog.info, alog.LogLevel.INFO),
        (alog.warning, alog.LogLevel.WARN),
        (alog.warn, alog.LogLevel.WARN),
        (alog.error, alog.LogLevel.ERROR),
    ],
)
def test_level_helpers_write_when_enabled(monkeypatch, fn, level):
    captured = []
    monkeypatch.setattr(alog, "is_enabled", lambda lvl, scope: True)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: captured.append(a))
    fn("msg %d", 7)
    assert captured and captured[0][0] == int(level)
    assert captured[0][4] == "msg 7"


def test_if_true_and_false(monkeypatch):
    captured = []
    monkeypatch.setattr(alog, "is_enabled", lambda lvl, scope: True)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: captured.append(a))
    alog.if_(alog.LogLevel.INFO, False, "skipped")
    assert captured == []
    alog.if_(alog.LogLevel.INFO, True, "kept")
    assert len(captured) == 1


def test_every_n_zero_logs_each_time(monkeypatch):
    captured = []
    monkeypatch.setattr(alog, "is_enabled", lambda lvl, scope: True)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: captured.append(a))
    alog.every_n(alog.LogLevel.INFO, 0, "always")
    assert len(captured) == 1


def test_every_n_modulo_throttles(monkeypatch):
    captured = []
    monkeypatch.setattr(alog, "is_enabled", lambda lvl, scope: True)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: captured.append(a))
    # Fresh call site: 1st (count=1 → emit), 2nd (count=2 → skip), 3rd (emit)
    for _ in range(3):
        alog.every_n(alog.LogLevel.INFO, 2, "throttled")
    assert len(captured) == 2


def test_every_n_respects_disabled(monkeypatch):
    captured = []
    monkeypatch.setattr(alog, "is_enabled", lambda lvl, scope: False)
    monkeypatch.setattr(alog, "write_log_at", lambda *a: captured.append(a))
    alog.every_n(alog.LogLevel.INFO, 1, "disabled")
    assert captured == []
