#!/usr/bin/env python3
"""Unit tests for gen_schema_meta.py's reflection-grounded metadata.

gen_schema_meta.py must compute schema metadata from the compiled `.bfbs`
(via `schema_reflection.load_bfbs`) instead of the regex text parsers, whose
defects these tests pin (written red-first):

1. `--src` carries DIRECT dep edges only, so a struct declared in a
   GRANDCHILD include was invisible to `compute_struct_size` -> silently
   wrong FIXED_SIZE (None) for a struct root composed of grandchild structs.
2. A single `NAMESPACE_RE.search` took the FIRST namespace statement only,
   so a mid-file `namespace` change forward-declared and specialized later
   types under the WRONG C++ namespace (compile-wrong output).
3. `has_aorta_header_field` matched the field NAME only, so a root field
   named `aorta_header` of any non-`aorta.sys.AortaHeader` type still set
   HAS_AORTA_HEADER=true.

(`--src` itself — never read for anything once every computation moved
over to `--bfbs` — is now deleted from
the tool's CLI entirely. The historical narrative above is kept as-is; the
test bodies below no longer pass `--src`.)

Fixture notes (message/tools/tests/schema_meta_fixtures/):

- The pinned flatc REJECTS `root_type <struct>` ("root type must be a
  table"), so no flatc-produced bfbs can carry a struct root. The
  FIXED_SIZE test therefore synthesizes its struct-rooted bfbs from the
  flatc-COMPILED grandchild_root.bfbs (table root Probe) by re-rooting it
  onto `fixmeta.Wrap` via the reflection object API — every struct
  `bytesize` in that buffer is still flatc's own layout computation, so
  the ground truth is NOT hand-derived. (This mirrors
  schema_reflection_test.py's synthetic-Pack precedent for shapes the
  flatc CLI cannot emit.)
- Everything else (midns, header_wrong_type, header_right_type,
  enums_only) is real flatc `--binary --schema --bfbs-builtins` output
  compiled by BUILD-time genrules.

This test also carries the byte-flow properties previously covered by
python/tests/test_schema_meta_codegen.py (SCHEMA_BFBS round-trip +
SCHEMA_HASH_HEX = sha256(bfbs)): that pytest ran the tool as a bare
subprocess with synthetic non-bfbs bytes, which the bfbs-based tool structurally
rejects (the bfbs is now parsed, and the tool needs the bazel-generated
reflection bindings), so the coverage moved here (PythonMetaByteFlowTest).
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

import flatbuffers

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from reflection.Schema import Schema, SchemaT  # noqa: E402

import gen_schema_meta  # noqa: E402
import schema_reflection as sr  # noqa: E402

FIXTURE_DIR = "message/tools/tests/schema_meta_fixtures"


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message" / "schemas" / "registry.bzl").exists():
            return candidate
    # Fallback for running directly with `python3` outside `bazel test`
    # (needs `reflection`/`flatbuffers` on PYTHONPATH some other way, plus
    # the fixture bfbs built separately).
    return THIS_DIR.parent.parent


def _fixture(name: str) -> str:
    return str(_repo_root() / FIXTURE_DIR / name)


def _run_tool(argv: list) -> tuple:
    """Run gen_schema_meta.main() in-process with the EXACT argv shape the
    schema-meta genrules use, capturing stderr. Returns (rc, stderr_text)."""
    old_argv = sys.argv
    stderr = io.StringIO()
    sys.argv = ["gen_schema_meta.py"] + list(argv)
    try:
        with contextlib.redirect_stderr(stderr):
            try:
                rc = gen_schema_meta.main()
            except SystemExit as exc:  # argparse-style exits
                rc = exc.code if isinstance(exc.code, int) else 1
    finally:
        sys.argv = old_argv
    return rc, stderr.getvalue()


def _struct_rooted_bfbs(tmpdir: Path) -> Path:
    """Synthesize the struct-rooted bfbs for structroot_root.fbs: load the
    flatc-compiled grandchild_root.bfbs (table root `Probe`), point its
    root_table at the `fixmeta.Wrap` STRUCT object, and re-pack. All
    Object.bytesize values (including Wrap's, computed across the
    grandchild include chain) remain flatc's own — see module docstring."""
    with open(_fixture("grandchild_root.bfbs"), "rb") as f:
        compiled = f.read()
    schema_t = SchemaT.InitFromObj(Schema.GetRootAs(compiled, 0))
    wrap = [o for o in schema_t.objects if o.name == b"fixmeta.Wrap" or o.name == "fixmeta.Wrap"]
    assert len(wrap) == 1, "fixture drift: expected exactly one fixmeta.Wrap object"
    schema_t.rootTable = wrap[0]
    builder = flatbuffers.Builder(4096)
    builder.Finish(schema_t.Pack(builder))
    out = tmpdir / "structroot_root.bfbs"
    out.write_bytes(bytes(builder.Output()))
    return out


class FixedSizeFromReflectionTest(unittest.TestCase):
    """Defect 1: FIXED_SIZE must come from flatc's own
    Object.bytesize (grandchild-complete by construction), not from a regex
    walk over root + DIRECT --src files only (historical -- see module
    docstring; `--src` no longer exists, so there is nothing
    left to vary here: the bfbs alone, which already carries the full
    transitive closure, is the only input)."""

    def test_struct_root_fixed_size_is_flatc_bytesize(self) -> None:
        # flatc ground truth from the COMPILED probe: Wrap is composed of
        # Mid (direct include) which is composed of Leaf (grandchild
        # include).
        loaded = sr.load_bfbs_file(_fixture("grandchild_root.bfbs"))
        flatc_size = loaded.structs["fixmeta.Wrap"].bytesize
        self.assertEqual(flatc_size, 32)  # int+long chain, align 8 (see fixtures)

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            bfbs = _struct_rooted_bfbs(tmpdir)
            out = tmpdir / "structroot_schema_meta.rs"
            rc, err = _run_tool(
                [
                    "--root", _fixture("structroot_root.fbs"),
                    "--bfbs", str(bfbs),
                    "--out", str(out),
                ]
            )
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            self.assertEqual(
                out.read_text(encoding="utf-8"),
                "pub const AORTA_FIXED_SIZE: Option<usize> = Some({});\n".format(flatc_size),
            )

    def test_table_root_fixed_size_is_none(self) -> None:
        # A table root has no fixed size — the unchanged production case
        # (every flatc-compilable schema, since flatc rejects struct roots).
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "grandchild_root_schema_meta.rs"
            rc, err = _run_tool(
                [
                    "--root", _fixture("grandchild_root.fbs"),
                    "--bfbs", _fixture("grandchild_root.bfbs"),
                    "--out", str(out),
                ]
            )
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            self.assertEqual(
                out.read_text(encoding="utf-8"),
                "pub const AORTA_FIXED_SIZE: Option<usize> = None;\n",
            )

    def test_rust_lang_requires_bfbs(self) -> None:
        # --bfbs is REQUIRED for ALL languages (the rust path previously ran
        # regex-only with no ground truth at all).
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "no_bfbs_schema_meta.rs"
            rc, err = _run_tool(
                [
                    "--root", _fixture("grandchild_root.fbs"),
                    "--out", str(out),
                ]
            )
            self.assertNotEqual(rc, 0)
            self.assertIn("--bfbs", err)


class MidFileNamespaceCppTest(unittest.TestCase):
    """Defect 3: every local type must be forward-declared
    and specialized under ITS OWN namespace, not the file's first one."""

    def _generate(self) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "midns_schema_meta.h"
            rc, err = _run_tool(
                [
                    "--lang=cpp",
                    "--root", _fixture("midns.fbs"),
                    "--bfbs", _fixture("midns.bfbs"),
                    "--out", str(out),
                ]
            )
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            return out.read_text(encoding="utf-8")

    def test_forward_decls_use_each_types_own_namespace(self) -> None:
        text = self._generate()
        # Root type Msg lives in fixmeta.beta (declared AFTER the mid-file
        # namespace change) — the old emitter put it under fixmeta.alpha.
        self.assertIn(
            "namespace fixmeta {\nnamespace beta {\nstruct Msg;\n"
            "}  // namespace beta\n}  // namespace fixmeta\n",
            text,
        )
        self.assertIn(
            "namespace fixmeta {\nnamespace alpha {\nstruct Inner;\n"
            "}  // namespace alpha\n}  // namespace fixmeta\n",
            text,
        )
        # Canonical order: root first, then the rest sorted by qualified name.
        self.assertLess(text.index("struct Msg;"), text.index("struct Inner;"))

    def test_specializations_use_each_types_own_namespace(self) -> None:
        text = self._generate()
        self.assertIn("struct SchemaMeta<fixmeta::beta::Msg>", text)
        self.assertIn("struct SchemaMeta<fixmeta::alpha::Inner>", text)
        # The old single-first-namespace emitter produced exactly these
        # wrong names:
        self.assertNotIn("fixmeta::alpha::Msg", text)
        self.assertNotIn("fixmeta::beta::Inner", text)

    def test_root_type_names_are_fully_qualified_per_type(self) -> None:
        text = self._generate()
        self.assertIn('return "fixmeta.beta.Msg";', text)
        self.assertIn('return "fixmeta.alpha.Inner";', text)
        self.assertNotIn('"fixmeta.alpha.Msg"', text)
        self.assertNotIn('"fixmeta.beta.Inner"', text)


class HasAortaHeaderTightenedTest(unittest.TestCase):
    """Defect 4: HAS_AORTA_HEADER requires the field to be
    named `aorta_header` AND typed exactly `aorta.sys.AortaHeader`."""

    _FALSE_SNIPPET = (
        "static constexpr bool HasAortaHeader() {\n    return false;\n  }"
    )
    _TRUE_SNIPPET = (
        "static constexpr bool HasAortaHeader() {\n    return true;\n  }"
    )

    def _generate(self, root_stem: str, lang: str) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            suffix = "h" if lang == "cpp" else "py"
            out = Path(tmp) / "{}_schema_meta.{}".format(root_stem, suffix)
            argv = [
                "--lang={}".format(lang),
                "--root", _fixture(root_stem + ".fbs"),
                "--bfbs", _fixture(root_stem + ".bfbs"),
                "--out", str(out),
            ]
            rc, err = _run_tool(argv)
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            return out.read_text(encoding="utf-8")

    def _specialization_chunk(self, text: str, fq_type: str) -> str:
        marker = "struct SchemaMeta<{}> {{".format(fq_type)
        self.assertIn(marker, text)
        start = text.index(marker)
        end = text.index("};", start)
        return text[start:end]

    def test_wrong_typed_aorta_header_field_is_false_cpp(self) -> None:
        text = self._generate("header_wrong_type", "cpp")
        chunk = self._specialization_chunk(text, "fixmeta::hdr::WrongMsg")
        self.assertIn(self._FALSE_SNIPPET, chunk)

    def test_wrong_typed_aorta_header_field_is_false_python(self) -> None:
        text = self._generate("header_wrong_type", "python")
        self.assertIn("HAS_AORTA_HEADER: bool = False\n", text)

    def test_correctly_typed_aorta_header_stays_true_cpp(self) -> None:
        # Positive control (guards against over-tightening): typed exactly
        # aorta.sys.AortaHeader -> true, matching every real registry schema.
        text = self._generate("header_right_type", "cpp")
        chunk = self._specialization_chunk(text, "fixmeta::hdr2::RightMsg")
        self.assertIn(self._TRUE_SNIPPET, chunk)

    def test_correctly_typed_aorta_header_stays_true_python(self) -> None:
        text = self._generate("header_right_type", "python")
        self.assertIn("HAS_AORTA_HEADER: bool = True\n", text)


class EnumsOnlyNamespaceBlockTest(unittest.TestCase):
    """Rootless (enums-only) schema — mirrors the one real consumer,
    cpp/test/tier2_action_codegen/xfer_types.fbs: the C++ meta must keep
    emitting the empty namespace block byte-identically, with the
    namespace derived from the root-local declared types instead of the
    first textual `namespace` statement. Control test (green before and
    after) — the byte-level arbiter for the real schema is the PRE/POST
    diff gate run when metadata moved to bfbs."""

    def test_enums_only_schema_keeps_empty_namespace_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "enums_only_schema_meta.h"
            rc, err = _run_tool(
                [
                    "--lang=cpp",
                    "--root", _fixture("enums_only.fbs"),
                    "--bfbs", _fixture("enums_only.bfbs"),
                    "--out", str(out),
                ]
            )
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            text = out.read_text(encoding="utf-8")
            self.assertIn(
                "namespace fixmeta {\nnamespace shared {\n"
                "}  // namespace shared\n}  // namespace fixmeta\n",
                text,
            )
            self.assertNotIn("template <>", text)  # no per-type specializations


class PythonMetaByteFlowTest(unittest.TestCase):
    """Byte-flow properties formerly covered by
    python/tests/test_schema_meta_codegen.py (see module docstring), now
    against a REAL bfbs: SCHEMA_BFBS round-trips byte-identically and
    SCHEMA_HASH_HEX == sha256(bfbs) full lowercase hex."""

    def test_bfbs_roundtrip_and_hash(self) -> None:
        with open(_fixture("header_right_type.bfbs"), "rb") as f:
            bfbs = f.read()
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "header_right_type_schema_meta.py"
            rc, err = _run_tool(
                [
                    "--lang=python",
                    "--root", _fixture("header_right_type.fbs"),
                    "--bfbs", _fixture("header_right_type.bfbs"),
                    "--out", str(out),
                ]
            )
            self.assertEqual(rc, 0, "tool failed: {}".format(err))
            spec = importlib.util.spec_from_file_location("_gen_meta_fixture", str(out))
            module = importlib.util.module_from_spec(spec)
            # The generated module's message-class re-export is guarded by
            # try/ImportError (metadata-only loads are supported), so
            # executing it without the fixture message classes is fine.
            spec.loader.exec_module(module)
            self.assertEqual(module.SCHEMA_BFBS, bfbs)
            self.assertEqual(module.SCHEMA_HASH_HEX, hashlib.sha256(bfbs).hexdigest())
            self.assertEqual(len(module.SCHEMA_HASH_HEX), 64)
            self.assertEqual(module.SCHEMA_HASH_HEX, module.SCHEMA_HASH_HEX.lower())
            self.assertEqual(module.ROOT_TYPE_NAME, "fixmeta.hdr2.RightMsg")
            self.assertIsNone(module.ROOT_TYPE)  # message classes absent here


if __name__ == "__main__":
    unittest.main()
