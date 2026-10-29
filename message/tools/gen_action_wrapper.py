#!/usr/bin/env python3
"""Generate action protocol wrapper FlatBuffers schema from a domain action schema."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

from schema_include_utils import parse_schema_includes, strip_fbs_comments

TABLE_RE = re.compile(r"\btable\s+([A-Za-z0-9_]+)\s*\{")
# Accepts a QUALIFIED root_type (`ns.Type`, with
# dots) as well as a bare one -- the pre-fix character class ([A-Za-z0-9_]+)
# could not match a dotted spelling at all, so a qualified declaration
# silently fell through to `None` (indistinguishable from "no root_type
# statement present"). See `generate_action_wrapper`'s validation of the
# parsed value against `goal_name`.
ROOT_RE = re.compile(r"\broot_type\s+([A-Za-z0-9_.]+)\s*;")
ROOT_LINE_RE = re.compile(r"(?m)^\s*root_type\s+[A-Za-z0-9_.]+\s*;\s*(?://.*)?\n?")
ACTION_COMMON_INCLUDE = "schemas/shared/action_common/action_common.fbs"


class GenerationError(RuntimeError):
    pass


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_table_names(text: str) -> list[str]:
    return [match.group(1).strip() for match in TABLE_RE.finditer(text)]


def parse_root_type(text: str) -> str | None:
    """Return the declared `root_type` value verbatim (bare or
    namespace-qualified), or `None` when the schema declares none at all.
    Callers that need the bare table name should split on the last `.`
    themselves -- this function does not silently pick a "closest" name
    for a qualified value, so "no root_type" and "a qualified root_type"
    stay distinguishable."""
    match = ROOT_RE.search(text)
    if match is None:
        return None
    value = match.group(1).strip()
    if not value:
        return None
    return value


def derive_action_prefix(table_names: list[str]) -> tuple[str, str, str]:
    goal_names = [name for name in table_names if name.endswith("Goal")]
    result_names = [name for name in table_names if name.endswith("Result")]
    feedback_names = [name for name in table_names if name.endswith("FeedbackData")]

    if len(goal_names) != 1:
        raise GenerationError(
            "expected exactly one '*Goal' table, found {}".format(goal_names)
        )
    if len(result_names) != 1:
        raise GenerationError(
            "expected exactly one '*Result' table, found {}".format(result_names)
        )
    if len(feedback_names) != 1:
        raise GenerationError(
            "expected exactly one '*FeedbackData' table, found {}".format(feedback_names)
        )

    goal_name = goal_names[0]
    result_name = result_names[0]
    feedback_name = feedback_names[0]

    prefix = goal_name[: -len("Goal")]
    if not prefix or result_name[: -len("Result")] != prefix or feedback_name[
        : -len("FeedbackData")
    ] != prefix:
        raise GenerationError(
            "action domain tables must share one common prefix: goal={}, result={}, feedback={}".format(
                goal_name,
                result_name,
                feedback_name,
            )
        )

    return prefix, goal_name, result_name, feedback_name


def strip_root_type(text: str) -> str:
    return ROOT_LINE_RE.sub("", text).rstrip()


def render_wrappers(
    action_prefix: str,
    goal_name: str,
    result_name: str,
    feedback_name: str,
) -> str:
    return f"""
// Auto-generated action protocol wrappers.
table {action_prefix}SendGoalRequest {{
  goal_id: string;
  goal: {goal_name};
}}

table {action_prefix}SendGoalResponse {{
  accepted: bool;
  goal_id: string;
  goal_secret: string;
  code: aorta.action.common.ActionReplyCode;
  status: aorta.action.common.ActionStatus;
  message: string;
  server_epoch: string;
}}

table {action_prefix}CancelGoalRequest {{
  goal_id: string;
  goal_secret: string;
}}

table {action_prefix}CancelGoalResponse {{
  accepted: bool;
  goal_id: string;
  code: aorta.action.common.ActionReplyCode;
  status: aorta.action.common.ActionStatus;
  message: string;
}}

table {action_prefix}Feedback {{
  goal_id: string;
  seq: ulong;
  stamp_ns: ulong;
  status: aorta.action.common.ActionStatus;
  data: {feedback_name};
  message: string;
}}

table {action_prefix}PublicStatus {{
  goal_id: string;
  status: aorta.action.common.ActionStatus;
  code: aorta.action.common.ActionReplyCode;
  accepted_stamp_ns: ulong;
  started_stamp_ns: ulong;
  finished_stamp_ns: ulong;
  last_feedback_seq: ulong;
  server_epoch: string;
}}

table {action_prefix}GetResultRequest {{
  goal_id: string;
  goal_secret: string;
}}

table {action_prefix}GetResultResponse {{
  goal_id: string;
  status: aorta.action.common.ActionStatus;
  accepted_stamp_ns: ulong;
  started_stamp_ns: ulong;
  finished_stamp_ns: ulong;
  last_feedback_seq: ulong;
  feedback_snapshot: {feedback_name};
  result: {result_name};
  code: aorta.action.common.ActionReplyCode;
  message: string;
  server_epoch: string;
}}
""".lstrip()


def generate_action_wrapper(root_fbs: Path) -> str:
    text = read_text(root_fbs)
    # Scan the comment-stripped text so a block-commented table (a normal way
    # to temporarily disable a type during iteration) is not parsed as a live
    # '*Goal'/'*Result' duplicate. The emitted
    # body below still uses the ORIGINAL text so author comments survive in
    # the generated wrapper schema.
    scan_text = strip_fbs_comments(text)
    # Route include scanning through the shared
    # `schema_include_utils` scanner (comment-stripping + `INCLUDE_RE`)
    # instead of a second, duplicated pattern local to this file.
    includes = parse_schema_includes(root_fbs)
    if ACTION_COMMON_INCLUDE not in includes:
        raise GenerationError(
            "action schema must include '{}' because generated wrappers reference "
            "aorta.action.common.ActionStatus and ActionReplyCode".format(
                ACTION_COMMON_INCLUDE
            )
        )
    table_names = parse_table_names(scan_text)
    action_prefix, goal_name, result_name, feedback_name = derive_action_prefix(table_names)

    # A declared root_type (bare or qualified)
    # must name the Goal table -- this tool's whole output shape assumes the
    # action root IS the goal. The pre-fix regex could not even SEE a
    # qualified declaration (see ROOT_RE's comment above) and silently
    # substituted `goal_name`, masking a genuine mismatch instead of
    # surfacing it. No schema in this repo declares root_type qualified
    # today (every one is bare-matching-goal or omits it entirely), so this
    # validation is a no-op for all real schemas.
    declared_root_type = parse_root_type(scan_text)
    if declared_root_type is not None:
        declared_bare = declared_root_type.rsplit(".", 1)[-1]
        if declared_bare != goal_name:
            raise GenerationError(
                "declared root_type '{}' (bare name '{}') does not match the "
                "required Goal table '{}'; an action schema's root_type must "
                "name its own '*Goal' table".format(
                    declared_root_type, declared_bare, goal_name,
                )
            )
    root_type = goal_name

    body = strip_root_type(text)
    wrappers = render_wrappers(action_prefix, goal_name, result_name, feedback_name)
    return "{}\n\n{}\nroot_type {};\n".format(body, wrappers.rstrip(), root_type)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-fbs", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    try:
        output = generate_action_wrapper(Path(args.root_fbs))
        Path(args.out).write_text(output, encoding="utf-8")
        return 0
    except GenerationError as err:
        print(f"gen_action_wrapper error: {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
