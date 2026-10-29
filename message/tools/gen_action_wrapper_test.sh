#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${TEST_SRCDIR}/${TEST_WORKSPACE}"
TOOL="${REPO_ROOT}/message/tools/gen_action_wrapper"
SCHEMA="${REPO_ROOT}/message/schemas/action/example/guard_action_probe.fbs"
OUT="${TEST_TMPDIR}/guard_action_probe_generated_action.fbs"

"${TOOL}" \
  --root-fbs="${SCHEMA}" \
  --out="${OUT}"

grep -Fq 'table GuardActionProbeSendGoalRequest {' "${OUT}"
grep -Fq 'goal: GuardActionProbeGoal;' "${OUT}"
grep -Fq 'table GuardActionProbeSendGoalResponse {' "${OUT}"
grep -Fq 'goal_secret: string;' "${OUT}"
grep -Fq 'table GuardActionProbeFeedback {' "${OUT}"
grep -Fq 'data: GuardActionProbeFeedbackData;' "${OUT}"
grep -Fq 'table GuardActionProbeGetResultResponse {' "${OUT}"
grep -Fq 'feedback_snapshot: GuardActionProbeFeedbackData;' "${OUT}"
grep -Fq 'code: aorta.action.common.ActionReplyCode;' "${OUT}"
grep -Fq 'status: aorta.action.common.ActionStatus;' "${OUT}"
grep -Fq 'root_type GuardActionProbeGoal;' "${OUT}"

ROOT_TYPE_COUNT="$(grep -c '^root_type GuardActionProbeGoal;$' "${OUT}")"
test "${ROOT_TYPE_COUNT}" -eq 1

ALL_TYPES_SCHEMA="${TEST_TMPDIR}/all_types_action.fbs"
ALL_TYPES_OUT="${TEST_TMPDIR}/all_types_action_generated_action.fbs"

cat > "${ALL_TYPES_SCHEMA}" <<'FBS'
include "schemas/system/aorta_base.fbs";
include "schemas/shared/action_common/action_common.fbs";

namespace demo.action;

enum Color:uint8 {
  RED = 0,
  GREEN = 1,
  BLUE = 2,
}

enum Mode:int16 {
  IDLE = 0,
  RUN = 1,
  STOP = 2,
}

struct Vec3 {
  x:float32;
  y:float32;
  z:float32;
}

table Child {
  id:uint32 (id: 0);
  name:string (id: 1);
  pose:Vec3 (id: 2);
}

table MetaInfo {
  tag:string (id: 0);
  level:int32 (id: 1);
}

union PayloadUnion {
  Child,
  MetaInfo,
}

table AllTypes {
  aorta_header:aorta.sys.AortaHeader (id: 0);
  flag:bool (id: 1);
  val_int8:int8 (id: 2);
  val_uint8:uint8 (id: 3);
  val_int16:int16 (id: 4);
  val_uint16:uint16 (id: 5);
  val_int32:int32 (id: 6);
  val_uint32:uint32 (id: 7);
  val_int64:int64 (id: 8);
  val_uint64:uint64 (id: 9);
  val_float32:float32 (id: 10);
  val_float64:float64 (id: 11);
  text:string (id: 12);
  color:Color (id: 13);
  mode:Mode (id: 14);
  pose:Vec3 (id: 15);
  child:Child (id: 16);
  payload:PayloadUnion (id: 18);
  meta:MetaInfo (id: 19);
  bytes:[uint8] (id: 20);
  ints:[int32] (id: 21);
  floats:[float32] (id: 22);
  names:[string] (id: 23);
  poses:[Vec3] (id: 24);
  children:[Child] (id: 25);
  colors:[Color] (id: 26);
  modes:[Mode] (id: 27);
}

table AllTypesGoal {
  body:AllTypes (id: 0);
}

table AllTypesResult {
  body:AllTypes (id: 0);
}

table AllTypesFeedbackData {
  body:AllTypes (id: 0);
}

root_type AllTypesGoal;
FBS

"${TOOL}" \
  --root-fbs="${ALL_TYPES_SCHEMA}" \
  --out="${ALL_TYPES_OUT}"

grep -Fq 'table AllTypesSendGoalRequest {' "${ALL_TYPES_OUT}"
grep -Fq 'goal: AllTypesGoal;' "${ALL_TYPES_OUT}"
grep -Fq 'table AllTypesSendGoalResponse {' "${ALL_TYPES_OUT}"
grep -Fq 'table AllTypesFeedback {' "${ALL_TYPES_OUT}"
grep -Fq 'data: AllTypesFeedbackData;' "${ALL_TYPES_OUT}"
grep -Fq 'table AllTypesGetResultResponse {' "${ALL_TYPES_OUT}"
grep -Fq 'result: AllTypesResult;' "${ALL_TYPES_OUT}"
grep -Fq 'code: aorta.action.common.ActionReplyCode;' "${ALL_TYPES_OUT}"
grep -Fq 'status: aorta.action.common.ActionStatus;' "${ALL_TYPES_OUT}"
BODY_FIELD_COUNT="$(grep -c 'body:AllTypes (id: 0);' "${ALL_TYPES_OUT}")"
test "${BODY_FIELD_COUNT}" -eq 3
grep -Fq 'root_type AllTypesGoal;' "${ALL_TYPES_OUT}"

MISSING_INCLUDE_SCHEMA="${TEST_TMPDIR}/missing_action_common.fbs"
MISSING_INCLUDE_OUT="${TEST_TMPDIR}/missing_action_common_generated_action.fbs"
MISSING_INCLUDE_ERR="${TEST_TMPDIR}/missing_action_common.stderr"

cat > "${MISSING_INCLUDE_SCHEMA}" <<'FBS'
namespace demo.action;

table DemoGoal {
  value:string;
}

table DemoResult {
  value:string;
}

table DemoFeedbackData {
  value:string;
}

root_type DemoGoal;
FBS

if "${TOOL}" \
  --root-fbs="${MISSING_INCLUDE_SCHEMA}" \
  --out="${MISSING_INCLUDE_OUT}" \
  2>"${MISSING_INCLUDE_ERR}"; then
  echo "expected missing action_common include to fail" >&2
  exit 1
fi

grep -Fq "action schema must include 'schemas/shared/action_common/action_common.fbs'" \
  "${MISSING_INCLUDE_ERR}"
