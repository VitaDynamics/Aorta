#!/usr/bin/env python3
"""One bfbs is the only schema identity.

For every schema of the EDU external root this asserts

    C++ SchemaMeta<T>::SchemaHashHex()
      == Rust <T>Schema::schema_hash().to_hex()
      == Python SCHEMA_HASH_HEX
      == sha256(<stem>.bfbs)

so a language-specific drift (a second bfbs, a stale schema-meta, a wrapper
built from a different file) cannot pass unnoticed. All four legs come from the
ONE `<stem>_bfbs` artifact: the Rust crate embeds it with
`include_bytes!`, the C++/Python schema-meta generators SHA-256 the same bytes,
and the test additionally reads the artifact itself.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import unittest
from pathlib import Path

import edu_demo_job_schema_meta as job_meta
import edu_demo_service_schema_meta as service_meta
import edu_demo_text_schema_meta as text_meta

_PACKAGE = "message/tools/tests/external_root_edu/schemas"
_PY_META = {
    "edu_demo_job": job_meta,
    "edu_demo_service": service_meta,
    "edu_demo_text": text_meta,
}


def _runfile(relative: str) -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace / relative
        if candidate.exists():
            return candidate
    candidate = Path(relative)
    if candidate.exists():
        return candidate
    raise FileNotFoundError(relative)


def _probe_hashes(probe_name: str) -> dict[str, str]:
    probe = _runfile(f"{_PACKAGE}/{probe_name}")
    output = subprocess.run(
        [str(probe)], check=True, capture_output=True, text=True
    ).stdout
    hashes = {}
    for line in output.splitlines():
        stem, _, value = line.partition(" ")
        if stem:
            hashes[stem] = value.strip()
    return hashes


class SchemaHashParityTest(unittest.TestCase):
    def test_cpp_rust_python_and_bfbs_agree(self):
        cpp_hashes = _probe_hashes("cpp_hash_probe")
        rust_hashes = _probe_hashes("rust_hash_probe")
        self.assertEqual(sorted(_PY_META), sorted(cpp_hashes))
        self.assertEqual(sorted(_PY_META), sorted(rust_hashes))
        for stem, meta in sorted(_PY_META.items()):
            with self.subTest(stem=stem):
                bfbs = _runfile(f"{_PACKAGE}/{stem}.bfbs").read_bytes()
                digest = hashlib.sha256(bfbs).hexdigest()
                self.assertEqual(64, len(digest))
                self.assertEqual(digest, meta.SCHEMA_HASH_HEX)
                self.assertEqual(digest, cpp_hashes[stem])
                self.assertEqual(digest, rust_hashes[stem])
                # The bfbs the Python meta embeds is the SAME artifact, not a
                # re-compilation that merely hashes the same.
                self.assertEqual(bfbs, meta.SCHEMA_BFBS)


if __name__ == "__main__":
    unittest.main()
