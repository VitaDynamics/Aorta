"""Tests for `gen_schema_meta.py --lang=python` (schema-meta codegen).

Validates that the generated module round-trips the .bfbs byte-for-byte and that
SCHEMA_HASH_HEX is sha256(SCHEMA_BFBS) — the property that makes the Python
schema hash byte-identical to the Rust/C++ hash for the same `:stem_bfbs`
artifact (the cross-language wire-compat gate). Uses a real .fbs for root/header
parsing and synthetic bfbs bytes for the byte-flow (the tool hashes whatever
bytes it is given; the real artifact is fed by the Bazel rule).
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys

import pytest

# gen_schema_meta.py computes every emitted
# value from the compiled bfbs via schema_reflection — it (a) requires the
# bazel-generated `reflection` bindings, which this venv-run pytest suite
# does not have, and (b) PARSES the bfbs, so this file's synthetic
# `bytes(range(256)) * 3` payload is structurally rejected instead of being
# hashed blindly. The byte-flow properties this suite asserted (SCHEMA_BFBS
# round-trip, SCHEMA_HASH_HEX == sha256(bfbs), ROOT_TYPE_NAME/HAS_AORTA_HEADER
# shape) are now covered against a REAL bfbs by
# //message/tools:gen_schema_meta_reflection_test (PythonMetaByteFlowTest).
pytestmark = pytest.mark.skip(
    reason="gen_schema_meta requires bazel-generated reflection "
    "bindings and a real bfbs; coverage moved to "
    "//message/tools:gen_schema_meta_reflection_test"
)

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_GEN = os.path.join(_REPO_ROOT, "message", "tools", "gen_schema_meta.py")
_FBS = os.path.join(
    _REPO_ROOT, "message", "schemas", "topic", "example", "demo_chat.fbs"
)


def _load_module(path: str):
    spec = importlib.util.spec_from_file_location("_gen_meta", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def generated_meta(tmp_path):
    if not (os.path.exists(_GEN) and os.path.exists(_FBS)):  # pragma: no cover
        pytest.skip("schema codegen tool / demo schema not present")
    bfbs = bytes(range(256)) * 3
    bfbs_path = tmp_path / "demo.bfbs"
    bfbs_path.write_bytes(bfbs)
    out_path = tmp_path / "demo_chat_schema_meta.py"
    rc = subprocess.run(
        [
            sys.executable,
            _GEN,
            "--lang=python",
            "--root",
            _FBS,
            "--bfbs",
            str(bfbs_path),
            "--out",
            str(out_path),
        ],
        check=False,
    ).returncode
    assert rc == 0
    return bfbs, _load_module(str(out_path))


def test_bfbs_round_trips_byte_identical(generated_meta):
    bfbs, module = generated_meta
    assert module.SCHEMA_BFBS == bfbs


def test_hash_is_sha256_of_bfbs(generated_meta):
    bfbs, module = generated_meta
    assert module.SCHEMA_HASH_HEX == hashlib.sha256(bfbs).hexdigest()
    assert len(module.SCHEMA_HASH_HEX) == 64
    assert module.SCHEMA_HASH_HEX == module.SCHEMA_HASH_HEX.lower()


def test_root_type_and_header_fields(generated_meta):
    _, module = generated_meta
    assert module.ROOT_TYPE_NAME == "demo.Chat"  # namespace.RootType
    assert isinstance(module.HAS_AORTA_HEADER, bool)
    assert module.FIXED_SIZE is None or isinstance(module.FIXED_SIZE, int)
