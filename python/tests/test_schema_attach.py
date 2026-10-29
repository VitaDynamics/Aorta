"""Schema-attach tests: a typed publisher wires its schema BFBS into
aorta_pub_options_t and Core registers it (the schema_by_key queryable). Note
the schema hash is NOT delivered to subscriber callbacks — Core passes an empty
view to all SDKs (C++ does ``(void)schema``); cross-language wire compatibility
is proven by the test_scripts interop suite, not by a per-message hash here."""

from __future__ import annotations

import hashlib
import os
import time
import types

import flatbuffers
import pytest

import aorta
from aorta.publisher import Publisher

_BFBS = os.path.join(os.path.dirname(__file__), "_fixtures", "demo_chat.bfbs")


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _schema_meta():
    raw = open(_BFBS, "rb").read()
    return types.SimpleNamespace(
        SCHEMA_BFBS=raw,
        SCHEMA_HASH_HEX=hashlib.sha256(raw).hexdigest(),
        ROOT_TYPE_NAME="demo.Chat",
        FIXED_SIZE=None,
        HAS_AORTA_HEADER=True,
    )


def _minimal_fill(builder, header_off):
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(0, header_off, 0)
    return builder.EndObject()


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_schema_attach", group="py_test")
    yield n
    n.close()


def test_schema_meta_fixture_is_valid():
    meta = _schema_meta()
    assert len(meta.SCHEMA_HASH_HEX) == 64
    assert meta.SCHEMA_HASH_HEX == hashlib.sha256(meta.SCHEMA_BFBS).hexdigest()
    assert isinstance(meta, aorta.SchemaMeta)


def test_typed_publisher_attaches_schema(node):
    # The schema BFBS is wired into the publisher options and Core accepts it
    # (registers the schema_by_key queryable); typed messages still deliver.
    # Cross-language wire compatibility is proven by the test_scripts interop
    # suite (the schema hash is not delivered per-message to subscribers).
    meta = _schema_meta()
    received = []
    topic = "/py_test/schema/attach"
    node.create_subscriber(topic, lambda p, c: received.append(p))
    time.sleep(0.4)
    pub = node.create_publisher_typed(
        meta, topic, schema_attach_policy=aorta.SchemaAttachPolicy.ALWAYS
    )
    assert pub._opts.schema_bfbs_len == len(meta.SCHEMA_BFBS)
    assert pub._opts.schema_attach_policy == int(aorta.SchemaAttachPolicy.ALWAYS)
    assert pub._opts.schema_root_type == b"demo.Chat"
    pub.publish_typed(_minimal_fill)
    assert _poll_until(lambda: len(received) >= 1), "no message delivered"


def test_publisher_options_policy_only_without_schema(node):
    # Options built from a non-default policy with no bfbs (exercises the
    # schema_bfbs-None branch of the options builder).
    pub = Publisher(
        node,
        "/py_test/schema/policyonly",
        schema_attach_policy=aorta.SchemaAttachPolicy.NEVER,
    )
    assert pub._opts is not None and pub._schema_buf is None
    pub.close()


def test_publisher_options_max_message_size(node):
    pub = Publisher(node, "/py_test/schema/maxsz", max_message_size=4096)
    assert pub._opts is not None
    pub.close()


def test_publisher_options_manual_schema_root_type(node):
    pub = Publisher(
        node,
        "/py_test/schema/manualroot",
        schema_bfbs=b"bfbs",
        schema_root_type="demo.ManualRoot",
    )
    assert pub._opts.schema_root_type == b"demo.ManualRoot"
    assert pub._schema_root_type_buf is not None
    pub.close()
