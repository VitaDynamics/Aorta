#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${TEST_SRCDIR}/${TEST_WORKSPACE}"
TOOL="${REPO_ROOT}/message/tools/gen_message_wrapper"
ACTION_TOOL="${REPO_ROOT}/message/tools/gen_action_wrapper"
SCHEMA_DIR="${REPO_ROOT}/message/schemas/topic/foxglove_schema"
# `:flatc` is an alias to `@flatbuffers//:flatc` (a DIFFERENT Bzlmod repo), so
# its runfiles path does not mirror this package the way `:gen_action_wrapper`
# does -- passed as $1 (runfiles-relative), absolutized before any `cd` below
# (matching `bfbs_stage_collision_test.sh`'s established pattern for the same
# reason).
FLATC="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"

# gen_message_wrapper.py requires --root-bfbs (the
# compiled bfbs is the wrapper emitters' only semantic source). Compiles
# `$1` (a root .fbs) to `$2` (the desired output .bfbs path), resolving
# "schemas/"-qualified includes via `-I "$3"`.
compile_bfbs() {
  local root_fbs="$1" out_bfbs="$2" include_dir="$3"
  local out_dir base natural_out
  out_dir="$(dirname "${out_bfbs}")"
  base="$(basename "${root_fbs}" .fbs)"
  natural_out="${out_dir}/${base}.bfbs"
  "${FLATC}" --binary --schema --bfbs-builtins -I "${include_dir}" -o "${out_dir}" "${root_fbs}"
  if [ "${natural_out}" != "${out_bfbs}" ]; then
    mv "${natural_out}" "${out_bfbs}"
  fi
}

POINT_CLOUD_RS="${TEST_TMPDIR}/point_cloud_msg.rs"
FRAME_TRANSFORMS_RS="${TEST_TMPDIR}/frame_transforms_msg.rs"
LASER_SCAN_CC="${TEST_TMPDIR}/laser_scan_msg.h"
ALL_TYPES_CC="${TEST_TMPDIR}/all_types_msg.h"
ALL_TYPES_VECTOR_UNION_CC="${TEST_TMPDIR}/all_types_cpp_vector_union_msg.h"
DEMO_CHAT_CC="${TEST_TMPDIR}/demo_chat_msg.h"
EXECUTE_TASK_RS="${TEST_TMPDIR}/guard_action_probe_msg.rs"
EXECUTE_TASK_CC="${TEST_TMPDIR}/guard_action_probe_msg.h"
EXECUTE_TASK_FBS="${REPO_ROOT}/message/schemas/action/example/guard_action_probe_action/guard_action_probe.fbs"
EXAMPLES_SENSOR_RS="${TEST_TMPDIR}/examples_sensor_msg.rs"
ALL_TYPES_CODEGEN_ROOT="${TEST_TMPDIR}/all_types_codegen_root"
ALL_TYPES_SERVICE_FBS="${ALL_TYPES_CODEGEN_ROOT}/message/schemas/service/example/all_types_service.fbs"
ALL_TYPES_SERVICE_RS="${TEST_TMPDIR}/all_types_service_msg.rs"
ALL_TYPES_SERVICE_CC="${TEST_TMPDIR}/all_types_service_msg.h"
ALL_TYPES_ACTION_ROOT_FBS="${ALL_TYPES_CODEGEN_ROOT}/message/schemas/action/example/all_types_action.fbs"
ALL_TYPES_ACTION_FBS="${ALL_TYPES_CODEGEN_ROOT}/message/schemas/action/example/all_types_action_action/all_types_action.fbs"
ALL_TYPES_ACTION_RS="${TEST_TMPDIR}/all_types_action_msg.rs"
ALL_TYPES_ACTION_CC="${TEST_TMPDIR}/all_types_action_msg.h"

write_all_types_defs() {
  cat <<'FBS'
include "schemas/system/aorta_base.fbs";

namespace demo;

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
FBS
}

POINT_CLOUD_BFBS="${TEST_TMPDIR}/PointCloud.bfbs"
compile_bfbs "${SCHEMA_DIR}/PointCloud.fbs" "${POINT_CLOUD_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=PointCloud \
  --schema-rel=schemas/topic/foxglove_schema \
  --root-fbs="${SCHEMA_DIR}/PointCloud.fbs" \
  --root-bfbs="${POINT_CLOUD_BFBS}" \
  --with-schema-meta \
  --out="${POINT_CLOUD_RS}"

grep -Fq 'pub mod Quaternion_generated;' "${POINT_CLOUD_RS}"
grep -Fq 'pub struct PoseMsg' "${POINT_CLOUD_RS}"
grep -Fq 'pub struct PackedElementFieldMsg' "${POINT_CLOUD_RS}"
grep -Fq 'let pose_offset = msg.pose.as_ref().map(|value| build_pose_typed' "${POINT_CLOUD_RS}"
grep -Fq 'pub r#type: Option<' "${POINT_CLOUD_RS}"
grep -Fq 'root_builder.add_type_(*value);' "${POINT_CLOUD_RS}"

FRAME_TRANSFORMS_BFBS="${TEST_TMPDIR}/FrameTransforms.bfbs"
compile_bfbs "${SCHEMA_DIR}/FrameTransforms.fbs" "${FRAME_TRANSFORMS_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=FrameTransforms \
  --schema-rel=schemas/topic/foxglove_schema \
  --root-fbs="${SCHEMA_DIR}/FrameTransforms.fbs" \
  --root-bfbs="${FRAME_TRANSFORMS_BFBS}" \
  --with-schema-meta \
  --out="${FRAME_TRANSFORMS_RS}"

grep -Fq 'pub struct FrameTransformMsg' "${FRAME_TRANSFORMS_RS}"
grep -Fq 'pub mod Vector3_generated;' "${FRAME_TRANSFORMS_RS}"
grep -Fq 'pub mod Quaternion_generated;' "${FRAME_TRANSFORMS_RS}"

LASER_SCAN_BFBS="${TEST_TMPDIR}/LaserScan.bfbs"
compile_bfbs "${SCHEMA_DIR}/LaserScan.fbs" "${LASER_SCAN_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=cpp \
  --stem=LaserScan \
  --root-fbs="${SCHEMA_DIR}/LaserScan.fbs" \
  --root-bfbs="${LASER_SCAN_BFBS}" \
  --out="${LASER_SCAN_CC}"

grep -Fq 'struct PoseMsg' "${LASER_SCAN_CC}"
grep -Fq 'struct Vector3Msg' "${LASER_SCAN_CC}"
grep -Fq 'struct QuaternionMsg' "${LASER_SCAN_CC}"
grep -Fq 'BuildPoseTyped' "${LASER_SCAN_CC}"

ALL_TYPES_BFBS="${TEST_TMPDIR}/all_types.bfbs"
compile_bfbs "${REPO_ROOT}/message/schemas/topic/example/all_types.fbs" "${ALL_TYPES_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=cpp \
  --stem=all_types \
  --root-fbs="${REPO_ROOT}/message/schemas/topic/example/all_types.fbs" \
  --root-bfbs="${ALL_TYPES_BFBS}" \
  --out="${ALL_TYPES_CC}"

ALL_TYPES_VECTOR_UNION_BFBS="${TEST_TMPDIR}/all_types_cpp_vector_union.bfbs"
compile_bfbs "${REPO_ROOT}/message/schemas/topic/example/all_types_cpp_vector_union.fbs" "${ALL_TYPES_VECTOR_UNION_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=cpp \
  --stem=all_types_cpp_vector_union \
  --root-fbs="${REPO_ROOT}/message/schemas/topic/example/all_types_cpp_vector_union.fbs" \
  --root-bfbs="${ALL_TYPES_VECTOR_UNION_BFBS}" \
  --out="${ALL_TYPES_VECTOR_UNION_CC}"

DEMO_CHAT_BFBS="${TEST_TMPDIR}/demo_chat.bfbs"
compile_bfbs "${REPO_ROOT}/message/schemas/topic/example/demo_chat.fbs" "${DEMO_CHAT_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=cpp \
  --stem=demo_chat \
  --root-fbs="${REPO_ROOT}/message/schemas/topic/example/demo_chat.fbs" \
  --root-bfbs="${DEMO_CHAT_BFBS}" \
  --out="${DEMO_CHAT_CC}"

grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_AORTAHEADERMSG_DEFINED' "${ALL_TYPES_CC}"
grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_AORTAHEADERMSG_DEFINED' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_AORTAHEADERMSG_DEFINED' "${DEMO_CHAT_CC}"

# TypedSubscribeTraits specializations are emitted ONLY for
# root-local tables (gen_message_wrapper.py `subscribe_tables`). AortaHeader
# is never the root type of these three example schemas, so its
# SUBSCRIBE_TRAITS specialization (guarded or not) must no longer appear at
# all -- these assertions flip from "guard exists" to "symbol absent".
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_AORTA_SYS_AORTAHEADER_DEFINED' "${ALL_TYPES_CC}"; then
  echo "unexpected include-table SUBSCRIBE_TRAITS guard for AortaHeader in ${ALL_TYPES_CC}" >&2
  exit 1
fi
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_AORTA_SYS_AORTAHEADER_DEFINED' "${ALL_TYPES_VECTOR_UNION_CC}"; then
  echo "unexpected include-table SUBSCRIBE_TRAITS guard for AortaHeader in ${ALL_TYPES_VECTOR_UNION_CC}" >&2
  exit 1
fi
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_AORTA_SYS_AORTAHEADER_DEFINED' "${DEMO_CHAT_CC}"; then
  echo "unexpected include-table SUBSCRIBE_TRAITS guard for AortaHeader in ${DEMO_CHAT_CC}" >&2
  exit 1
fi
if grep -Fq 'struct TypedSubscribeTraits<::aorta::sys::AortaHeader>' "${ALL_TYPES_CC}"; then
  echo "unexpected TypedSubscribeTraits<AortaHeader> specialization in ${ALL_TYPES_CC}" >&2
  exit 1
fi

# Positive golden lock: the absence assertions
# above only prove the include-table specialization is GONE -- they do not
# prove the ROOT-LOCAL specialization is still correctly emitted. Assert it
# by exact content: the specialization for the schema's own root type must
# still exist, unguarded (no owner-side guard), and must still use the
# root-specific `Verify<Root>Buffer`/`Get<Root>` identifier-checking path
# (gen_message_wrapper.py's `root_type is not None and table_name ==
# root_type` branch, which the root-local-only change does not touch) rather than
# the generic `VerifyBuffer<T>(nullptr)` fallback a non-root table gets.
grep -Fq 'struct TypedSubscribeTraits<::demo::AllTypes> {' "${ALL_TYPES_CC}"
grep -Fq 'return ::demo::VerifyAllTypesBuffer(verifier);' "${ALL_TYPES_CC}"
grep -Fq 'struct TypedSubscribeTraits<::demo::AllTypesCppVectorUnion> {' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq 'return ::demo::VerifyAllTypesCppVectorUnionBuffer(verifier);' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq 'struct TypedSubscribeTraits<::demo::Chat> {' "${DEMO_CHAT_CC}"
grep -Fq 'return ::demo::VerifyChatBuffer(verifier);' "${DEMO_CHAT_CC}"

# ... and NO owner-side guard was added for that root-local specialization
# (single-owner emission is sufficient; a guard would mask genuine
# inconsistent out-of-tree specializations).
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_DEMO_ALLTYPES_DEFINED' "${ALL_TYPES_CC}"; then
  echo "unexpected owner-side SUBSCRIBE_TRAITS guard for root-local AllTypes in ${ALL_TYPES_CC}" >&2
  exit 1
fi
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_DEMO_ALLTYPESCPPVECTORUNION_DEFINED' "${ALL_TYPES_VECTOR_UNION_CC}"; then
  echo "unexpected owner-side SUBSCRIBE_TRAITS guard for root-local AllTypesCppVectorUnion in ${ALL_TYPES_VECTOR_UNION_CC}" >&2
  exit 1
fi
if grep -Fq 'AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_DEMO_CHAT_DEFINED' "${DEMO_CHAT_CC}"; then
  echo "unexpected owner-side SUBSCRIBE_TRAITS guard for root-local Chat in ${DEMO_CHAT_CC}" >&2
  exit 1
fi

# Root tables now get the same TABLE_MSG guard treatment as include tables
# (guards are unconditional) -- assert the ROOT table's own
# guard now exists too (previously root tables were unguarded).
grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_ALLTYPESMSG_DEFINED' "${ALL_TYPES_CC}"
grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_ALLTYPESCPPVECTORUNIONMSG_DEFINED' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq '#ifndef AORTA_TYPED_WRAPPER_TABLE_MSG_DEMO_CHATMSG_DEFINED' "${DEMO_CHAT_CC}"

# Primary-template visibility: with include-table specializations
# gone, wrapper headers must #include the header that provides the
# TypedSubscribeTraits primary template (generic VerifyBuffer<T>/GetRoot<T>
# body), not just a forward declaration.
grep -Fq '#include "aorta/typed_subscribe.h"' "${ALL_TYPES_CC}"
grep -Fq '#include "aorta/typed_subscribe.h"' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq '#include "aorta/typed_subscribe.h"' "${DEMO_CHAT_CC}"

grep -Fq 'namespace typed_demo_all_types {' "${ALL_TYPES_CC}"
grep -Fq 'namespace typed_demo_all_types_cpp_vector_union {' "${ALL_TYPES_VECTOR_UNION_CC}"
grep -Fq 'namespace typed_demo_demo_chat {' "${DEMO_CHAT_CC}"

EXAMPLES_SENSOR_BFBS="${TEST_TMPDIR}/examples_sensor.bfbs"
compile_bfbs "${REPO_ROOT}/message/schemas/topic/example/examples_sensor.fbs" "${EXAMPLES_SENSOR_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=examples_sensor \
  --schema-rel=schemas/topic/example \
  --root-fbs="${REPO_ROOT}/message/schemas/topic/example/examples_sensor.fbs" \
  --root-bfbs="${EXAMPLES_SENSOR_BFBS}" \
  --with-schema-meta \
  --out="${EXAMPLES_SENSOR_RS}"

python3 - "${EXAMPLES_SENSOR_RS}" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
unexpected = "        }\n    }\n\n        }\n    }\n\n}\n\npub mod typed {\n"
if unexpected in text:
    raise SystemExit("unexpected extra braces in generated Rust schema module")
PY

EXECUTE_TASK_BFBS="${TEST_TMPDIR}/guard_action_probe.bfbs"
compile_bfbs "${EXECUTE_TASK_FBS}" "${EXECUTE_TASK_BFBS}" "${REPO_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=guard_action_probe \
  --schema-rel=schemas/action/example \
  --root-fbs="${EXECUTE_TASK_FBS}" \
  --root-bfbs="${EXECUTE_TASK_BFBS}" \
  --with-schema-meta \
  --out="${EXECUTE_TASK_RS}"

grep -Fq 'pub struct GuardActionProbeAction;' "${EXECUTE_TASK_RS}"
grep -Fq 'impl aorta_core::action::ActionSchema for GuardActionProbeAction {' "${EXECUTE_TASK_RS}"
grep -Fq 'impl aorta_core::action::ActionWire for GuardActionProbeAction {' "${EXECUTE_TASK_RS}"
grep -Fq 'type OwnedFeedbackDataMsg = owned::GuardActionProbeFeedbackDataMsg;' "${EXECUTE_TASK_RS}"
grep -Fq 'type OwnedResultMsg = owned::GuardActionProbeResultMsg;' "${EXECUTE_TASK_RS}"
grep -Fq 'fn encode_cancel_goal_request(' "${EXECUTE_TASK_RS}"
grep -Fq 'fn decode_cancel_goal_response(' "${EXECUTE_TASK_RS}"
grep -Fq 'fn encode_get_result_request(' "${EXECUTE_TASK_RS}"
grep -Fq 'fn decode_get_result_response(' "${EXECUTE_TASK_RS}"
grep -Fq 'fn decode_feedback_goal_id(' "${EXECUTE_TASK_RS}"
grep -Fq 'const ACTION_NAME: &' "${EXECUTE_TASK_RS}"
grep -Fq '"example/guard_action_probe"' "${EXECUTE_TASK_RS}"

"${TOOL}" \
  --lang=cpp \
  --stem=guard_action_probe \
  --schema-rel=schemas/action/example \
  --root-fbs="${EXECUTE_TASK_FBS}" \
  --root-bfbs="${EXECUTE_TASK_BFBS}" \
  --out="${EXECUTE_TASK_CC}"

grep -Fq 'struct GuardActionProbeAction {' "${EXECUTE_TASK_CC}"
grep -Fq 'using Goal = GuardActionProbeGoal;' "${EXECUTE_TASK_CC}"
grep -Fq 'using GoalMsg = GuardActionProbeGoalMsg;' "${EXECUTE_TASK_CC}"
grep -Fq 'using ResultMsg = GuardActionProbeResultMsg;' "${EXECUTE_TASK_CC}"
grep -Fq 'static constexpr const char* ActionName() {' "${EXECUTE_TASK_CC}"
grep -Fq 'return "example/guard_action_probe";' "${EXECUTE_TASK_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeSendGoalRequest(' "${EXECUTE_TASK_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeCancelGoalRequest(' "${EXECUTE_TASK_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeGetResultRequest(' "${EXECUTE_TASK_CC}"
grep -Fq 'static ResultMsg CopyResult(const Result& root)' "${EXECUTE_TASK_CC}"
grep -Fq 'static FeedbackDataMsg CopyFeedbackData(const FeedbackData& root)' "${EXECUTE_TASK_CC}"
grep -Fq 'inline ::aorta::action::example::GuardActionProbeResultMsg CopyGuardActionProbeResultMsg(' "${EXECUTE_TASK_CC}"
grep -Fq 'inline ::aorta::action::example::GuardActionProbeFeedbackDataMsg CopyGuardActionProbeFeedbackDataMsg(' "${EXECUTE_TASK_CC}"
grep -Fq 'switch (root.input_type()) {' "${EXECUTE_TASK_CC}"
grep -Fq 'case ::aorta::services::fixture_guard::GuardPayload_GuardMeta:' "${EXECUTE_TASK_CC}"
grep -Fq 'root.input_as_GuardMeta()' "${EXECUTE_TASK_CC}"
grep -Fq 'static_cast<::aorta::services::fixture_guard::GuardResource>(values->Get(i))' "${EXECUTE_TASK_CC}"

mkdir -p "${ALL_TYPES_CODEGEN_ROOT}/message/schemas/service/example"
mkdir -p "${ALL_TYPES_CODEGEN_ROOT}/message/schemas/action/example/all_types_action_action"
mkdir -p "${ALL_TYPES_CODEGEN_ROOT}/message/schemas"
ln -s "${REPO_ROOT}/message/schemas/system" "${ALL_TYPES_CODEGEN_ROOT}/message/schemas/system"
ln -s "${REPO_ROOT}/message/schemas/shared" "${ALL_TYPES_CODEGEN_ROOT}/message/schemas/shared"

write_all_types_defs > "${ALL_TYPES_SERVICE_FBS}"
cat >> "${ALL_TYPES_SERVICE_FBS}" <<'FBS'

table AllTypesServiceRequest {
  aorta_header:aorta.sys.AortaHeader (id: 0);
  body:AllTypes (id: 1);
}

table AllTypesServiceResponse {
  aorta_header:aorta.sys.AortaHeader (id: 0);
  body:AllTypes (id: 1);
}

root_type AllTypesServiceRequest;
FBS

ALL_TYPES_SERVICE_BFBS="${TEST_TMPDIR}/all_types_service.bfbs"
compile_bfbs "${ALL_TYPES_SERVICE_FBS}" "${ALL_TYPES_SERVICE_BFBS}" "${ALL_TYPES_CODEGEN_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=all_types_service \
  --schema-rel=schemas/service/example \
  --root-fbs="${ALL_TYPES_SERVICE_FBS}" \
  --root-bfbs="${ALL_TYPES_SERVICE_BFBS}" \
  --with-schema-meta \
  --out="${ALL_TYPES_SERVICE_RS}"

"${TOOL}" \
  --lang=cpp \
  --stem=all_types_service \
  --root-fbs="${ALL_TYPES_SERVICE_FBS}" \
  --root-bfbs="${ALL_TYPES_SERVICE_BFBS}" \
  --out="${ALL_TYPES_SERVICE_CC}"

grep -Fq 'pub struct AllTypesServiceRequestSchema;' "${ALL_TYPES_SERVICE_RS}"
grep -Fq 'pub struct AllTypesServiceResponseSchema;' "${ALL_TYPES_SERVICE_RS}"
grep -Fq "pub struct AllTypesMsg<'a> {" "${ALL_TYPES_SERVICE_RS}"
grep -Fq 'pub enum PayloadUnionMsg {' "${ALL_TYPES_SERVICE_RS}"
grep -Fq "pub body: Option<Box<AllTypesMsg<'a>>>" "${ALL_TYPES_SERVICE_RS}"
grep -Fq 'let colors_offset = msg.colors.as_ref().map(|value| builder.create_vector(value.as_slice()));' "${ALL_TYPES_SERVICE_RS}"
grep -Fq 'struct AllTypesServiceRequestMsg {' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'struct AllTypesServiceResponseMsg {' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'struct AllTypesMsg {' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'using PayloadUnionMsg = std::variant<' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'BuildPayloadUnionUnionTyped' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'CreateUninitializedVector<std::uint8_t>(enum_values.size(), &values);' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'values[i] = static_cast<std::uint8_t>(enum_values[i]);' "${ALL_TYPES_SERVICE_CC}"
grep -Fq 'CreateUninitializedVector<std::int16_t>(enum_values.size(), &values);' "${ALL_TYPES_SERVICE_CC}"

cat > "${ALL_TYPES_ACTION_ROOT_FBS}" <<'FBS'
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

"${ACTION_TOOL}" \
  --root-fbs="${ALL_TYPES_ACTION_ROOT_FBS}" \
  --out="${ALL_TYPES_ACTION_FBS}"

ALL_TYPES_ACTION_BFBS="${TEST_TMPDIR}/all_types_action.bfbs"
compile_bfbs "${ALL_TYPES_ACTION_FBS}" "${ALL_TYPES_ACTION_BFBS}" "${ALL_TYPES_CODEGEN_ROOT}/message"
"${TOOL}" \
  --lang=rust \
  --stem=all_types_action \
  --schema-rel=schemas/action/example \
  --root-fbs="${ALL_TYPES_ACTION_FBS}" \
  --root-bfbs="${ALL_TYPES_ACTION_BFBS}" \
  --with-schema-meta \
  --out="${ALL_TYPES_ACTION_RS}"

"${TOOL}" \
  --lang=cpp \
  --stem=all_types_action \
  --schema-rel=schemas/action/example \
  --root-fbs="${ALL_TYPES_ACTION_FBS}" \
  --root-bfbs="${ALL_TYPES_ACTION_BFBS}" \
  --out="${ALL_TYPES_ACTION_CC}"

grep -Fq 'pub struct AllTypesAction;' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'impl aorta_core::action::ActionSchema for AllTypesAction {' "${ALL_TYPES_ACTION_RS}"
grep -Fq "type GoalMsg<'a> = typed::AllTypesGoalMsg<'a>;" "${ALL_TYPES_ACTION_RS}"
grep -Fq 'type OwnedFeedbackDataMsg = owned::AllTypesFeedbackDataMsg;' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'type OwnedResultMsg = owned::AllTypesResultMsg;' "${ALL_TYPES_ACTION_RS}"
grep -Fq "pub struct AllTypesMsg<'a> {" "${ALL_TYPES_ACTION_RS}"
grep -Fq 'pub enum PayloadUnionMsg {' "${ALL_TYPES_ACTION_RS}"
grep -Fq "pub body: Option<Box<AllTypesMsg<'a>>>" "${ALL_TYPES_ACTION_RS}"
grep -Fq 'pub mod owned {' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'pub struct AllTypesResultMsg {' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'pub struct AllTypesFeedbackDataMsg {' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'fn clone_alltypes_owned_from_msg(' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'fn clone_alltypes_owned_from_root(' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'fn borrow_alltypes_owned_as_typed<' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'let colors_offset = msg.colors.as_ref().map(|value| builder.create_vector(value.as_slice()));' "${ALL_TYPES_ACTION_RS}"
grep -Fq 'struct AllTypesAction {' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'using GoalMsg = AllTypesGoalMsg;' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'struct AllTypesMsg {' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'std::optional<std::shared_ptr<AllTypesMsg>> body;' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'inline ::demo::action::AllTypesResultMsg CopyAllTypesResultMsg(' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'inline ::demo::action::AllTypesFeedbackDataMsg CopyAllTypesFeedbackDataMsg(' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'BuildPayloadUnionUnionTyped' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'CreateUninitializedVector<std::uint8_t>(enum_values.size(), &values);' "${ALL_TYPES_ACTION_CC}"
grep -Fq 'values[i] = static_cast<std::uint8_t>(enum_values[i]);' "${ALL_TYPES_ACTION_CC}"

# Regression guard: the action Result/Feedback (`body: AllTypes`) nest a local
# table, so the owned/borrow clone helpers recurse into child-table helpers. Each
# helper fn must be emitted contiguously at module scope — a child fn must never be
# spliced into the middle of a parent helper's struct/match literal. That breakage
# produced invalid Rust that only surfaced when a registered action's binding was
# compiled in CI, because grep -Fq checks substring presence, not structure. So
# verify the generated Rust structurally here.
python3 - "${ALL_TYPES_ACTION_RS}" <<'PY'
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()

# Feedback-data helpers are seeded first, so they are where a child-table helper
# would be spliced in if body emission were not buffered. One per helper family.
parents = [
    "fn clone_alltypesfeedbackdata_owned_from_msg(",
    "fn clone_alltypesfeedbackdata_owned_from_root(",
    "fn borrow_alltypesfeedbackdata_owned_as_typed<",
]
for needle in parents:
    start = next((i for i, ln in enumerate(lines) if ln.startswith(needle)), None)
    if start is None:
        raise SystemExit(f"expected helper not found: {needle}")
    depth = 0
    closed = False
    for i in range(start, len(lines)):
        ln = lines[i]
        if i > start and depth > 0 and ln.startswith("fn "):
            raise SystemExit(
                f"nested helper fn spliced into {needle!r} body: {ln!r}"
            )
        depth += ln.count("{") - ln.count("}")
        if i > start and depth == 0:
            closed = True
            break
    if not closed:
        raise SystemExit(f"unterminated helper fn: {needle}")
PY

RELATIVE_ROOT_DIR="${TEST_TMPDIR}/relative_execroot"
mkdir -p "${RELATIVE_ROOT_DIR}/bazel-out/darwin_arm64-fastbuild/bin/message/schemas/action/example/guard_action_probe_action"
ln -s "${REPO_ROOT}/message" "${RELATIVE_ROOT_DIR}/message"

cp "${EXECUTE_TASK_FBS}" \
  "${RELATIVE_ROOT_DIR}/bazel-out/darwin_arm64-fastbuild/bin/message/schemas/action/example/guard_action_probe_action/guard_action_probe.fbs"

(
  cd "${RELATIVE_ROOT_DIR}"
  "${TOOL}" \
    --lang=rust \
    --stem=guard_action_probe \
    --schema-rel=schemas/action/example \
    --root-fbs="bazel-out/darwin_arm64-fastbuild/bin/message/schemas/action/example/guard_action_probe_action/guard_action_probe.fbs" \
    --root-bfbs="${EXECUTE_TASK_BFBS}" \
    --with-schema-meta \
    --out="${TEST_TMPDIR}/guard_action_probe_msg_relative.rs"
)

grep -Fq 'pub mod action_common_generated;' "${TEST_TMPDIR}/guard_action_probe_msg_relative.rs"

# Test Bazel sandbox scenario: wrapper FBS in output tree, source schemas at
# execution root, no bazel-bin symlink.  This is the layout a sandboxed genrule
# sees when the wrapper was produced by the action wrapper genrule.
SANDBOX_ROOT="${TEST_TMPDIR}/sandbox_execroot"
mkdir -p "${SANDBOX_ROOT}/bazel-out/k8-fastbuild/bin/message/schemas/action/example/guard_action_probe_action"
ln -s "${REPO_ROOT}/message/schemas" "${SANDBOX_ROOT}/schemas"

cp "${EXECUTE_TASK_FBS}" \
  "${SANDBOX_ROOT}/bazel-out/k8-fastbuild/bin/message/schemas/action/example/guard_action_probe_action/guard_action_probe.fbs"

(
  cd "${SANDBOX_ROOT}"
  "${TOOL}" \
    --lang=cpp \
    --stem=guard_action_probe \
    --schema-rel=schemas/action/example \
    --root-fbs="bazel-out/k8-fastbuild/bin/message/schemas/action/example/guard_action_probe_action/guard_action_probe.fbs" \
    --root-bfbs="${EXECUTE_TASK_BFBS}" \
    --out="${TEST_TMPDIR}/guard_action_probe_sandbox_msg.h"
)

grep -Fq 'switch (root.input_type()) {' "${TEST_TMPDIR}/guard_action_probe_sandbox_msg.h"
grep -Fq 'case ::aorta::services::fixture_guard::GuardPayload_GuardLeaf:' "${TEST_TMPDIR}/guard_action_probe_sandbox_msg.h"
grep -Fq 'root.input_as_GuardLeaf()' "${TEST_TMPDIR}/guard_action_probe_sandbox_msg.h"

# ---------------------------------------------------------------------------
# RETIRED: the text "needle" re-export injections
# (aorta.action.common / reused-service `aorta::services` / other-service
# discovery) that patch_flatc_rust used to add here are gone. Foreign type
# references are now rewritten to ABSOLUTE `crate::<dep>_generated::<path>::<T>`
# paths from the root bfbs. The behaviours those blocks asserted are now covered
# by:
#   - message/tools/patch_flatc_rust_test.py  -- unit tests of the exact
#     reference->absolute mapping for the sys / services / action.common /
#     disjoint / deep-prefix shapes, plus the ambiguity hard-fail;
#   - the compiled action wrappers that reference these crates (e.g.
#     //message/bindings/rust:guard_action_probe_msg_rs referencing
#     guard_service_probe_generated + action_common_generated, and the data_upload
#     action referencing list_data_files_generated) which now build + round-trip
#     with the rewritten absolute paths (//rust:... , //cpp:... suites).
# ---------------------------------------------------------------------------
