#!/usr/bin/env python3
"""Unit tests for resolve_schema_include_path, strip_fbs_comments, and
parse_schema_includes.

`StripFbsCommentsTest` and
`CommentedIncludeIgnoredTest` were moved here from the (retired)
`gen_message_wrapper_parser_test.py` — they test `schema_include_utils`
functions directly (never `gen_message_wrapper`'s own now-deleted regex
scanners), and were the ONLY coverage for `strip_fbs_comments`/
`parse_schema_includes` in the whole tree; both functions stay very much
alive (`gen_action_wrapper.py`'s include scanning and
`gen_message_wrapper.py`'s `_walk_include_fbs_closure` both depend on them),
so this coverage could not simply be retired with the rest of that file.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from schema_include_utils import (
    SchemaIncludeResolutionError,
    manifest_suffix_matches,
    parse_schema_includes,
    resolve_schema_include_path,
    strip_fbs_comments,
)


def _write_fbs(tmp: Path, content: str) -> str:
    path = tmp / "schema.fbs"
    path.write_text(content, encoding="utf-8")
    return str(path)


class StripFbsCommentsTest(unittest.TestCase):
    def test_block_comment_removed_line_structure_kept(self) -> None:
        text = "a\n/* one\ntwo */\nb\n"
        stripped = strip_fbs_comments(text)
        self.assertNotIn("one", stripped)
        self.assertNotIn("two", stripped)
        self.assertEqual(stripped.count("\n"), text.count("\n"))
        self.assertIn("a", stripped)
        self.assertIn("b", stripped)

    def test_line_comment_removed(self) -> None:
        self.assertEqual(strip_fbs_comments("x // y\n"), "x \n")

    def test_block_open_inside_line_comment_is_inert(self) -> None:
        # `function_msgs/*.msg` style: a `/*` inside a `//` comment must not
        # open a block comment that swallows the rest of the file.
        text = "// see function_msgs/*.msg constants\ntable T { a: int32; }\n"
        stripped = strip_fbs_comments(text)
        self.assertIn("table T", stripped)

    def test_line_comment_marker_inside_block_comment_is_inert(self) -> None:
        text = "/* x // y */ table T { a: int32; }\n"
        stripped = strip_fbs_comments(text)
        self.assertIn("table T", stripped)

    def test_comment_markers_inside_strings_survive(self) -> None:
        text = 'attribute "//not-a-comment";\n'
        self.assertEqual(strip_fbs_comments(text), text)


class CommentedIncludeIgnoredTest(unittest.TestCase):
    """A commented-out `include` statement must not
    be scanned as a live include."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_commented_include_is_ignored(self) -> None:
        fbs = _write_fbs(
            self.tmp,
            "// include \"schemas/system/aorta_base.fbs\";\n"
            '/* include "schemas/shared/service_common/status.fbs"; */\n'
            'include "schemas/system/real.fbs";\n'
            "namespace demo;\n",
        )
        self.assertEqual(parse_schema_includes(fbs), ["schemas/system/real.fbs"])


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


class ResolveSchemaIncludePathTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self._cwd = os.getcwd()

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def test_source_tree_rfind_branch(self) -> None:
        root = self.tmp / "src" / "message" / "schemas" / "topic" / "voice" / "voice_event.fbs"
        dep = self.tmp / "src" / "message" / "schemas" / "system" / "aorta_base.fbs"
        _touch(root)
        _touch(dep)

        resolved = resolve_schema_include_path(root, "schemas/system/aorta_base.fbs")
        self.assertEqual(resolved, dep)

    def test_source_tree_ancestor_message_branch(self) -> None:
        root = self.tmp / "src" / "message" / "wrapped" / "execute_task_action.fbs"
        dep = self.tmp / "src" / "message" / "schemas" / "shared" / "action_common" / "action_common.fbs"
        _touch(root)
        _touch(dep)

        resolved = resolve_schema_include_path(
            root,
            "schemas/shared/action_common/action_common.fbs",
        )
        self.assertEqual(resolved, dep)

    def test_bazel_external_generated_root_resolves_to_source(self) -> None:
        repo = "aorta+"
        generated_root = (
            self.tmp
            / "bazel-out"
            / "k8-fastbuild"
            / "bin"
            / "external"
            / repo
            / "message"
            / "schemas"
            / "action"
            / "rcp"
            / "execute_task_action.fbs"
        )
        source_dep = (
            self.tmp
            / "external"
            / repo
            / "message"
            / "schemas"
            / "shared"
            / "action_common"
            / "action_common.fbs"
        )
        _touch(generated_root)
        _touch(source_dep)

        resolved = resolve_schema_include_path(
            generated_root,
            "schemas/shared/action_common/action_common.fbs",
        )
        self.assertEqual(resolved, source_dep)

    def test_non_schemas_include_uses_sibling(self) -> None:
        root = self.tmp / "pkg" / "main.fbs"
        sibling = self.tmp / "pkg" / "helper.fbs"
        _touch(root)
        _touch(sibling)

        resolved = resolve_schema_include_path(root, "helper.fbs")
        self.assertEqual(resolved, sibling)

    def test_missing_schemas_include_raises(self) -> None:
        # An unresolvable
        # `schemas/`-rooted include is now a hard error naming the include
        # and the searched candidates — not the old wrong-by-construction
        # `generated_root.parent / include_path` fallback.
        repo = "aorta+"
        generated_root = (
            self.tmp
            / "bazel-out"
            / "k8"
            / "bin"
            / "external"
            / repo
            / "message"
            / "schemas"
            / "main.fbs"
        )
        _touch(generated_root)

        with self.assertRaises(SchemaIncludeResolutionError) as ctx:
            resolve_schema_include_path(generated_root, "schemas/missing.fbs")
        message = str(ctx.exception)
        self.assertIn("schemas/missing.fbs", message)
        self.assertIn(str(generated_root), message)

    def test_missing_sibling_include_raises(self) -> None:
        # A non-`schemas/` (bare sibling) include that resolves nowhere —
        # neither next to root_fbs, nor (for a `bazel-out`-generated root)
        # at its source-tree-relative equivalent — must also raise.
        root = self.tmp / "pkg" / "main.fbs"
        _touch(root)

        with self.assertRaises(SchemaIncludeResolutionError) as ctx:
            resolve_schema_include_path(root, "helper.fbs")
        message = str(ctx.exception)
        self.assertIn("helper.fbs", message)

    def test_action_expanded_sibling_resolves_via_bazel_out_strip(self) -> None:
        # Regression test for a real bug this hard-error change
        # surfaced (cpp/test/tier2_action_codegen's xfer_upload/xfer_download
        # + xfer_types fixture): `gen_action_wrapper` expands an action
        # schema into `{name}_action_gen/{name}.fbs`, one directory level
        # below the package that owns a LOCAL (same-package, non-`schemas/`)
        # sibling dep. The expanded file is a GENERATED (`bazel-out`-rooted)
        # path; the sibling it includes is still a SOURCE file at its
        # original, un-generated package path. flatc's own `-I` flags
        # (built from the dep's original package directory) already resolve
        # this at compile time; this scanner must reach the same file by
        # stripping the `bazel-out/<config>/bin/` prefix.
        # `.exists()` candidates built from the stripped path are RELATIVE
        # (mirroring the real `$(location ...)`-relative-to-exec-root
        # strings Bazel genrule commands actually receive), so this test
        # chdirs into a scratch dir and lays out both the generated
        # `bazel-out/...` root AND its source-tree-relative sibling under
        # it, exactly like a real Bazel exec root mounts them side by side.
        os.chdir(self.tmp)
        pkg = Path("cpp") / "test" / "tier2_action_codegen"
        expanded_root = (
            Path("bazel-out")
            / "darwin_arm64-fastbuild"
            / "bin"
            / "cpp"
            / "test"
            / "tier2_action_codegen"
            / "xfer_upload_action_action_gen"
            / "xfer_upload_action.fbs"
        )
        sibling = pkg / "xfer_types.fbs"
        _touch(self.tmp / expanded_root)
        _touch(self.tmp / sibling)

        resolved = resolve_schema_include_path(expanded_root, "xfer_types.fbs")
        self.assertEqual(resolved, sibling)

    def test_bin_source_collision_prefers_source(self) -> None:
        # Source must ALWAYS win over a (possibly
        # stale) bazel-out artifact at the same package-relative path. Both
        # a source copy and a bin-dir copy of "x.fbs" exist here with
        # DIFFERENT content — resolution must pick the SOURCE one.
        os.chdir(self.tmp)
        root = Path("bazel-out") / "cfg" / "bin" / "pkg" / "root.fbs"
        source_sibling = Path("pkg") / "x.fbs"
        bin_shadow_sibling = Path("bazel-out") / "cfg" / "bin" / "pkg" / "x.fbs"
        _touch(self.tmp / root)
        (self.tmp / source_sibling).parent.mkdir(parents=True, exist_ok=True)
        (self.tmp / source_sibling).write_text("SOURCE-OF-TRUTH", encoding="utf-8")
        (self.tmp / bin_shadow_sibling).parent.mkdir(parents=True, exist_ok=True)
        (self.tmp / bin_shadow_sibling).write_text("STALE-BIN-COPY", encoding="utf-8")

        resolved = resolve_schema_include_path(root, "x.fbs")
        self.assertEqual(resolved, source_sibling)
        self.assertEqual((self.tmp / resolved).read_text(encoding="utf-8"), "SOURCE-OF-TRUTH")

    def test_unrelated_ancestor_same_basename_raises(self) -> None:
        # The resolver must use a FIXED, deterministic
        # candidate set — never an open ancestor walk that could match an
        # unrelated same-basename file arbitrarily far up the tree. Here
        # "y.fbs" exists one level ABOVE every deterministic candidate this
        # (nested, action-expanded-shaped) root would check; it must NOT be
        # silently picked up — resolution must raise instead.
        os.chdir(self.tmp)
        root = (
            Path("bazel-out")
            / "cfg"
            / "bin"
            / "pkg"
            / "sub"
            / "name_action_gen"
            / "name.fbs"
        )
        _touch(self.tmp / root)
        unrelated = Path("pkg") / "y.fbs"  # one level above pkg/sub — out of reach
        _touch(self.tmp / unrelated)

        with self.assertRaises(SchemaIncludeResolutionError) as ctx:
            resolve_schema_include_path(root, "y.fbs")
        message = str(ctx.exception)
        self.assertIn("y.fbs", message)
        # The searched-candidates list is fixed/deterministic (2 source +
        # 1 bin-dir, never an open walk) — the unrelated ancestor path must
        # not even appear among the candidates that were tried.
        self.assertNotIn(str(unrelated), message)


class ManifestSuffixMatchesTest(unittest.TestCase):
    """Direct unit tests for `manifest_suffix_matches` — the path-boundary
    suffix-match rule the external-consumer include resolution relies on."""

    def test_matches_external_module_sandbox_path(self) -> None:
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            ["external/aorta~/message/schemas/system/aorta_base.fbs"],
        )
        self.assertEqual(matches, [Path("external/aorta~/message/schemas/system/aorta_base.fbs")])

    def test_matches_plain_message_root_path(self) -> None:
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            ["message/schemas/system/aorta_base.fbs"],
        )
        self.assertEqual(matches, [Path("message/schemas/system/aorta_base.fbs")])

    def test_does_not_match_wrong_basename(self) -> None:
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            ["message/schemas/system/other_aorta_base.fbs"],
        )
        self.assertEqual(matches, [])

    def test_does_not_match_when_intermediate_segment_differs(self) -> None:
        # "schemas" must align at a path boundary, not just be a substring
        # somewhere — "not_schemas" is a different path component.
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            ["message/not_schemas/system/aorta_base.fbs"],
        )
        self.assertEqual(matches, [])

    def test_bare_sibling_include_matches_on_basename_within_manifest(self) -> None:
        matches = manifest_suffix_matches(
            "xfer_types.fbs",
            ["cpp/test/tier2_action_codegen/xfer_types.fbs"],
        )
        self.assertEqual(matches, [Path("cpp/test/tier2_action_codegen/xfer_types.fbs")])

    def test_multiple_matches_returned_for_caller_to_reject(self) -> None:
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            [
                "external/aorta~/message/schemas/system/aorta_base.fbs",
                "external/aorta_pin~/message/schemas/system/aorta_base.fbs",
            ],
        )
        self.assertEqual(len(matches), 2)

    def test_identical_duplicate_entries_are_deduped(self) -> None:
        # A Tier-2 diamond dep (e.g. a root depending
        # on both PointCloud and LaserScan, whose closures both carry
        # Pose.fbs/Time.fbs/aorta_base.fbs) lists the SAME path multiple
        # times across the combined --include-fbs manifest (one
        # $(locations ...) expansion per dep's closure target). Identical
        # duplicates must collapse to a single match, not be treated as
        # ambiguous.
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            [
                "external/aorta~/message/schemas/system/aorta_base.fbs",
                "external/aorta~/message/schemas/system/aorta_base.fbs",
                "external/aorta~/message/schemas/system/aorta_base.fbs",
            ],
        )
        self.assertEqual(matches, [Path("external/aorta~/message/schemas/system/aorta_base.fbs")])

    def test_dedup_preserves_order_of_first_occurrence(self) -> None:
        matches = manifest_suffix_matches(
            "aorta_base.fbs",
            [
                "a/aorta_base.fbs",
                "b/aorta_base.fbs",
                "a/aorta_base.fbs",
            ],
        )
        self.assertEqual(matches, [Path("a/aorta_base.fbs"), Path("b/aorta_base.fbs")])

    def test_dedup_does_not_hide_genuinely_different_paths_with_same_suffix(self) -> None:
        # Two DIFFERENT modules' aorta_base.fbs sharing a suffix must still
        # be reported as 2 distinct matches (genuine ambiguity) — dedup
        # only collapses IDENTICAL paths, never merely-same-suffix ones.
        matches = manifest_suffix_matches(
            "schemas/system/aorta_base.fbs",
            [
                "external/aorta~/message/schemas/system/aorta_base.fbs",
                "external/aorta_pin~/message/schemas/system/aorta_base.fbs",
                "external/aorta~/message/schemas/system/aorta_base.fbs",  # dup of entry 1
            ],
        )
        self.assertEqual(
            matches,
            [
                Path("external/aorta~/message/schemas/system/aorta_base.fbs"),
                Path("external/aorta_pin~/message/schemas/system/aorta_base.fbs"),
            ],
        )


class ResolveWithManifestTest(unittest.TestCase):
    """`resolve_schema_include_path(..., manifest=...)` — the external-consumer
    container regression fix. Reproduces the exact failure shape: a
    Tier-2 external consumer's OWN schema (in a package with no `message/`
    ancestor at all) including `schemas/system/aorta_base.fbs`, which only
    exists in the SEPARATE `@aorta` module's sandboxed tree."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self._cwd = os.getcwd()

    def tearDown(self) -> None:
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def test_manifest_resolves_include_into_external_module_tree(self) -> None:
        # Root schema lives entirely outside any `message/` tree (for example,
        # src/aorta_schema/spatial/query_route.fbs) — the pre-existing
        # deterministic candidate search has NO way to construct
        # `external/aorta~/...` from this root path alone; only the manifest
        # (the build system's own declared closure) can.
        os.chdir(self.tmp)
        root = Path("src") / "aorta_schema" / "spatial" / "query_route.fbs"
        aorta_base = Path("external") / "aorta~" / "message" / "schemas" / "system" / "aorta_base.fbs"
        _touch(self.tmp / root)
        _touch(self.tmp / aorta_base)

        resolved = resolve_schema_include_path(
            root,
            "schemas/system/aorta_base.fbs",
            manifest=[str(aorta_base)],
        )
        self.assertEqual(resolved, aorta_base)

    def test_diamond_dep_duplicate_manifest_entries_resolve_without_ambiguity(self) -> None:
        # A Tier-2 root with diamond deps (e.g.
        # PointCloud + LaserScan, both transitively depending on
        # aorta_base.fbs) lists the SAME closure member multiple times
        # across the combined --include-fbs manifest. This must resolve
        # cleanly, not raise "ambiguous".
        os.chdir(self.tmp)
        root = Path("src") / "aorta_schema" / "spatial" / "root_diamond.fbs"
        aorta_base = Path("external") / "aorta~" / "message" / "schemas" / "system" / "aorta_base.fbs"
        _touch(self.tmp / root)
        _touch(self.tmp / aorta_base)

        resolved = resolve_schema_include_path(
            root,
            "schemas/system/aorta_base.fbs",
            manifest=[str(aorta_base), str(aorta_base), str(aorta_base)],
        )
        self.assertEqual(resolved, aorta_base)

    def test_manifest_match_does_not_require_filesystem_existence_check(self) -> None:
        # A manifest entry is trusted as-is (it came from the build system's
        # own declared `srcs`, which Bazel already guarantees exist at
        # genrule execution time) — resolution via manifest short-circuits
        # before any `.exists()` check, unlike the path-candidate fallback.
        resolved = resolve_schema_include_path(
            "src/aorta_schema/spatial/query_route.fbs",
            "schemas/system/aorta_base.fbs",
            manifest=["external/aorta~/message/schemas/system/aorta_base.fbs"],
        )
        self.assertEqual(resolved, Path("external/aorta~/message/schemas/system/aorta_base.fbs"))

    def test_ambiguous_manifest_match_raises(self) -> None:
        with self.assertRaises(SchemaIncludeResolutionError) as ctx:
            resolve_schema_include_path(
                "src/aorta_schema/spatial/query_route.fbs",
                "schemas/system/aorta_base.fbs",
                manifest=[
                    "external/aorta~/message/schemas/system/aorta_base.fbs",
                    "external/aorta_other~/message/schemas/system/aorta_base.fbs",
                ],
            )
        message = str(ctx.exception)
        self.assertIn("ambiguous", message)
        self.assertIn("aorta_other~", message)

    def test_bare_include_prefers_same_directory_sibling_over_ambiguous_manifest(
        self,
    ) -> None:
        # The manifest passed to a `gen_message_wrapper` invocation is the ROOT
        # schema's FULL combined transitive closure (a single module-level
        # list populated once from `--include-fbs`, reused for every include
        # resolved while recursively walking the whole graph — see
        # `_INCLUDE_MANIFEST` in gen_message_wrapper.py) — NOT rescoped to
        # the CURRENT includer's own closure as `manifest_suffix_matches`'
        # docstring claimed. Two unrelated subtrees can each legitimately
        # contain a same-basename file (e.g. two different `Pose.fbs`); for
        # a BARE sibling include (`include "Pose.fbs";`, no "schemas/"
        # prefix) flatc always resolves relative to the INCLUDING file's own
        # directory — the correct, unambiguous answer is a simple sibling
        # lookup that never needed the manifest at all. Before this fix, the
        # manifest's 2 same-basename entries were both returned by
        # `manifest_suffix_matches` (correctly — they are genuinely
        # different paths, not a diamond-dep duplicate) and the resolver
        # hard-errored as "ambiguous" despite the on-disk sibling being
        # unambiguous. Deterministic same-directory resolution must be tried
        # BEFORE bare-include manifest lookup, not after.
        os.chdir(self.tmp)
        includer = Path("services") / "foo" / "schema.fbs"
        correct_sibling = Path("services") / "foo" / "Pose.fbs"
        unrelated_same_basename = Path("services") / "bar" / "Pose.fbs"
        _touch(self.tmp / includer)
        _touch(self.tmp / correct_sibling)
        _touch(self.tmp / unrelated_same_basename)

        resolved = resolve_schema_include_path(
            includer,
            "Pose.fbs",
            manifest=[str(correct_sibling), str(unrelated_same_basename)],
        )
        self.assertEqual(resolved, correct_sibling)

    def test_qualified_include_still_prefers_manifest_and_rejects_ambiguity(
        self,
    ) -> None:
        # Companion to the fix above: "schemas/"-rooted (qualified) includes
        # must NOT get the same same-directory-first treatment — that is
        # exactly the Tier-2-external-module case
        # (`test_manifest_resolves_include_into_external_module_tree`) where
        # no on-disk sibling can ever exist next to the includer, and the
        # manifest is the ONLY source of truth. Genuine ambiguity for a
        # qualified include must still hard-error exactly as
        # `test_ambiguous_manifest_match_raises` already covers — this test
        # locks that the bare-include fix does not accidentally widen to
        # qualified includes too.
        with self.assertRaises(SchemaIncludeResolutionError):
            resolve_schema_include_path(
                "src/aorta_schema/spatial/query_route.fbs",
                "schemas/system/aorta_base.fbs",
                manifest=[
                    "external/aorta~/message/schemas/system/aorta_base.fbs",
                    "external/aorta_other~/message/schemas/system/aorta_base.fbs",
                ],
            )

    def test_manifest_miss_falls_back_to_path_candidates(self) -> None:
        # An include the manifest does not cover still resolves via the
        # existing deterministic candidate search (manifest is a safety-net
        # ADDITION, not a replacement, for schemas that DO fit the
        # directory-guessing shape).
        os.chdir(self.tmp)
        root = self.tmp / "pkg" / "main.fbs"
        sibling = self.tmp / "pkg" / "helper.fbs"
        _touch(root)
        _touch(sibling)

        resolved = resolve_schema_include_path(
            root,
            "helper.fbs",
            manifest=["some/unrelated/manifest/entry.fbs"],
        )
        self.assertEqual(resolved, sibling)

    def test_manifest_and_path_candidates_both_miss_reports_both(self) -> None:
        os.chdir(self.tmp)
        root = Path("src") / "aorta_schema" / "spatial" / "query_route.fbs"
        _touch(self.tmp / root)

        with self.assertRaises(SchemaIncludeResolutionError) as ctx:
            resolve_schema_include_path(
                root,
                "schemas/system/aorta_base.fbs",
                manifest=["external/aorta~/message/schemas/system/status.fbs"],
            )
        message = str(ctx.exception)
        self.assertIn("declared manifest entry", message)
        self.assertIn("status.fbs", message)
        self.assertIn("path candidate", message)


if __name__ == "__main__":
    unittest.main()
