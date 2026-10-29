#!/usr/bin/env python3
"""Unit tests for gen_action_wrapper's root_type handling and include scanning.

Before the fix, `gen_action_wrapper.py`'s own
``ROOT_RE = re.compile(r"\\broot_type\\s+([A-Za-z0-9_]+)\\s*;")`` cannot match a
QUALIFIED root_type (`root_type ns.Type;`, which contains a `.` the character
class excludes) -- `parse_root_type` silently returns `None`, and
`generate_action_wrapper`'s `root_type = parse_root_type(scan_text) or
goal_name` falls through to the "no root_type declared at all" default. When
the declared (qualified) value's bare name happens to equal `goal_name`, this
substitution is invisible (same final output). But when a schema author
explicitly points `root_type` at something else entirely (e.g. a copy-paste
mistake naming the Result table), the SAME silent substitution masks a real
authoring error instead of surfacing it -- exactly the class of defect this
repo's engineering convention requires to fail closed, not silently
"recover" via coincidence.

The fix (this file's RED->GREEN pair): widen the root_type regex to accept a
qualified name, extract its bare (rightmost) segment, and hard-error if that
bare segment does not equal the required Goal table -- rather than silently
discarding the declaration. Bare (unqualified) root_type -- the only form any
schema in this repo actually uses -- is unaffected (see
`BareRootTypeUnaffectedTest`; the byte-identical regeneration proof over the
REAL schemas lives in `gen_message_wrapper_action_name_test.sh` /
`gen_action_wrapper_test.sh`).

Also exercises that include scanning goes through the shared
`schema_include_utils.strip_fbs_comments`/`INCLUDE_RE` scanner rather than a
second, duplicated regex living in this file.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gen_action_wrapper as gaw  # noqa: E402

_COMMON_HEADER = (
    'include "schemas/shared/action_common/action_common.fbs";\n\n'
    "namespace demo.action;\n\n"
)

_TABLES = (
    "table ProbeGoal { x: int32 (id: 0); }\n\n"
    "table ProbeResult { y: int32 (id: 0); }\n\n"
    "table ProbeFeedbackData { z: int32 (id: 0); }\n\n"
)


def _write_fbs(tmp: Path, content: str) -> str:
    path = tmp / "probe_action.fbs"
    path.write_text(content, encoding="utf-8")
    return str(path)


class QualifiedRootTypeMatchingGoalTest(unittest.TestCase):
    """A qualified root_type that DOES point at the Goal table (redundant,
    but legal flatc syntax) must be recognized as such -- not silently
    mis-parsed into the "absent" case (which happens to produce the same
    output here, but for the wrong reason; see the mismatch test below for
    where that wrong reason becomes user-visible)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_qualified_root_type_is_recognized(self) -> None:
        fbs = _write_fbs(
            self.tmp,
            _COMMON_HEADER + _TABLES + "root_type demo.action.ProbeGoal;\n",
        )
        output = gaw.generate_action_wrapper(Path(fbs))
        self.assertEqual(output.count("root_type ProbeGoal;"), 1)


class QualifiedRootTypeMismatchTest(unittest.TestCase):
    """A qualified root_type that does NOT match the required Goal table is
    a real authoring error and must hard-error -- the pre-fix tool silently
    substituted `goal_name` instead (its regex simply failed to match the
    dotted spelling and fell through the "no root_type" branch, masking the
    mismatch rather than surfacing it). RED against the pre-fix tool: no
    exception is raised and the wrong root_type is silently emitted."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_qualified_root_type_mismatch_hard_errors(self) -> None:
        fbs = _write_fbs(
            self.tmp,
            _COMMON_HEADER + _TABLES + "root_type demo.action.ProbeResult;\n",
        )
        with self.assertRaises(gaw.GenerationError) as ctx:
            gaw.generate_action_wrapper(Path(fbs))
        message = str(ctx.exception)
        self.assertIn("ProbeResult", message)
        self.assertIn("ProbeGoal", message)


class BareRootTypeUnaffectedTest(unittest.TestCase):
    """Regression guard: the only form every real schema in this repo uses
    today (bare root_type, or no root_type statement at all) is unaffected
    by the fix."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_bare_root_type_still_works(self) -> None:
        fbs = _write_fbs(
            self.tmp,
            _COMMON_HEADER + _TABLES + "root_type ProbeGoal;\n",
        )
        output = gaw.generate_action_wrapper(Path(fbs))
        self.assertEqual(output.count("root_type ProbeGoal;"), 1)

    def test_missing_root_type_defaults_to_goal(self) -> None:
        fbs = _write_fbs(self.tmp, _COMMON_HEADER + _TABLES)
        output = gaw.generate_action_wrapper(Path(fbs))
        self.assertEqual(output.count("root_type ProbeGoal;"), 1)


class SharedIncludeScannerTest(unittest.TestCase):
    """Include scanning goes through `schema_include_utils`'s shared
    scanner (comment-stripping + the shared INCLUDE_RE), not a second,
    duplicated pattern local to this file."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_commented_include_is_ignored(self) -> None:
        fbs = _write_fbs(
            self.tmp,
            "// include \"schemas/shared/action_common/action_common.fbs\";\n"
            '/* include "schemas/shared/service_common/status.fbs"; */\n'
            'include "schemas/shared/action_common/action_common.fbs";\n\n'
            "namespace demo.action;\n\n" + _TABLES + "root_type ProbeGoal;\n",
        )
        # Must not raise "missing action_common include" -- the live
        # (non-commented) include is still found via the shared scanner.
        output = gaw.generate_action_wrapper(Path(fbs))
        self.assertIn("root_type ProbeGoal;", output)

    def test_missing_include_still_detected(self) -> None:
        fbs = _write_fbs(self.tmp, "namespace demo.action;\n\n" + _TABLES)
        with self.assertRaises(gaw.GenerationError) as ctx:
            gaw.generate_action_wrapper(Path(fbs))
        self.assertIn("action_common.fbs", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
