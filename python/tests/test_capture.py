"""Tests for the capture + schema-resolver Python wrappers.

The network tests depend on the ``lib`` fixture (conftest), which skips when the
core cdylib isn't built (`bazel build //core:aorta_core_cdylib`). The pure-helper
test runs without a library.
"""

from __future__ import annotations

import ctypes
import pathlib
import threading
import time
import types

import pytest

import aorta
from aorta import capture as cap
from aorta._ffi import structs
from aorta.capture import AORTA_ERR_NOT_FOUND, AortaError

_DEMO_BFBS = (pathlib.Path(__file__).parent / "_fixtures" / "demo_chat.bfbs").read_bytes()


def test_read_helpers_handle_empty_views():
    # Empty/null views decode to empty values (no library needed).
    assert cap._read_str(structs.AortaStringView()) == ""
    assert cap._read_hash(structs.AortaSchemaHashView()) is None
    assert cap._read_bytes(structs.AortaBytesView()) == b""
    # A non-empty bytes-view whose single slice is null also decodes to empty.
    empty_slice = structs.AortaBytesSlice()
    view = structs.AortaBytesView()
    view.slices = ctypes.cast(ctypes.byref(empty_slice), ctypes.c_void_p)
    view.slice_count = 1
    assert cap._read_bytes(view) == b""


def test_capture_receives_live_publish(lib):  # noqa: ARG001 - fixture ensures lib
    node = aorta.Node("py_cap_sub", group="default")
    try:
        received = []
        lock = threading.Lock()

        def on_sample(sample: aorta.CapturedSample) -> None:
            with lock:
                received.append(sample)

        # `with` exercises __enter__/__exit__ (closes the subscriber on exit).
        with aorta.CaptureSubscriber(
            node, [(aorta.CaptureSelectorKind.TOPIC, "pycap/chatter")], on_sample
        ):
            time.sleep(0.3)  # let routing settle
            pub = node.create_publisher("pycap/chatter")
            pub.publish_bytes(b"py-hello")

            deadline = time.time() + 3.0
            while time.time() < deadline:
                with lock:
                    if received:
                        break
                time.sleep(0.025)

        with lock:
            assert received, "capture should receive the live publish"
            sample = received[0]

        assert sample.key_suffix == "pub/pycap/chatter"
        assert sample.key.endswith("/pub/pycap/chatter")
        assert sample.payload == b"py-hello"
        assert sample.kind == aorta.CaptureKind.LIVE
        # Default attach policy is "never" → no schema hash on the wire.
        assert sample.schema_hash_hex is None
    finally:
        node.close()


def test_capture_subscriber_lifecycle(lib):  # noqa: ARG001
    import gc

    node = aorta.Node("py_cap_life", group="default")
    try:
        sub = aorta.CaptureSubscriber(
            node, [(aorta.CaptureSelectorKind.TOPIC, "life/x")], lambda s: None
        )
        sub.close()
        sub.close()  # idempotent: handle already 0
        del sub
        gc.collect()  # break the trampoline<->self cycle so __del__ runs
    finally:
        node.close()


def test_resolver_resolves_registered_schema(lib):  # noqa: ARG001
    node = aorta.Node("py_res_ok", group="default")
    try:
        schema_meta = types.SimpleNamespace(SCHEMA_BFBS=_DEMO_BFBS)
        pub = node.create_publisher_typed(schema_meta, "resok/chatter")
        time.sleep(0.3)  # let the schema_by_key queryable settle

        resolver = aorta.SchemaResolver(node, by_key_timeout_ms=400, by_hash_timeout_ms=400)
        deadline = time.time() + 3.0
        by_key = None
        while time.time() < deadline:
            try:
                by_key = resolver.resolve_by_key("pub/resok/chatter")
                break
            except AortaError:
                time.sleep(0.05)
        assert by_key is not None, "registered schema should resolve by key"
        assert by_key.bfbs == _DEMO_BFBS
        assert by_key.schema_hash_hex
        assert by_key.root_type_name

        # Same schema, by hash.
        by_hash = resolver.resolve_by_hash(by_key.schema_hash_hex)
        assert by_hash.bfbs == _DEMO_BFBS
        assert by_hash.schema_hash_hex == by_key.schema_hash_hex

        resolver.close()
        pub.close()
    finally:
        node.close()


def test_resolver_reports_not_found(lib):  # noqa: ARG001
    node = aorta.Node("py_res", group="default")
    try:
        resolver = aorta.SchemaResolver(node, by_key_timeout_ms=200)
        with pytest.raises(AortaError) as exc:
            resolver.resolve_by_key("pub/does/not/exist")
        assert exc.value.code == AORTA_ERR_NOT_FOUND
        # NOT_FOUND maps to the SDK-wide typed
        # exception (previously a local incompatible AortaError class).
        from aorta import errors as _errors

        assert isinstance(exc.value, _errors.NotFoundError)
        # clear_key_cache is a no-op on an empty cache but must succeed.
        resolver.clear_key_cache()
        resolver.close()
    finally:
        node.close()


def test_resolver_rejects_bad_hash(lib):  # noqa: ARG001
    node = aorta.Node("py_res2", group="default")
    try:
        resolver = aorta.SchemaResolver(node, by_hash_timeout_ms=200)
        with pytest.raises(AortaError):
            resolver.resolve_by_hash("not-a-valid-hash")
        resolver.close()
    finally:
        node.close()
