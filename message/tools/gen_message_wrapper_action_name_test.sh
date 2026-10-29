#!/usr/bin/env bash

# Validates Tier-2 (local) action codegen: gen_message_wrapper emits the
# aggregate <Prefix>Action C++ traits struct when --action-name is supplied,
# even though the schema is NOT under the in-repo schemas/action/ path. This is
# the path aorta_action_library relies on for downstream local action schemas.

set -euo pipefail

REPO_ROOT="${TEST_SRCDIR}/${TEST_WORKSPACE}"
TOOL="${REPO_ROOT}/message/tools/gen_message_wrapper"
ACTION_TOOL="${REPO_ROOT}/message/tools/gen_action_wrapper"
# `:flatc` is an alias to `@flatbuffers//:flatc` (a DIFFERENT Bzlmod repo), so
# its runfiles path does not mirror this package the way `:gen_action_wrapper`
# does -- passed as $1 (runfiles-relative), absolutized before any `cd` below
# (matching `bfbs_stage_collision_test.sh`'s established pattern for the same
# reason).
FLATC="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"

# A local action schema lives outside schemas/action/. Provide a message root so
# the action_common include resolves, but with a non-action relative path (the
# generator is invoked with an empty --schema-rel, exactly like aorta_fbs_library).
ROOT="${TEST_TMPDIR}/tier2_action_root"
mkdir -p "${ROOT}/schemas"
ln -s "${REPO_ROOT}/message/schemas/shared" "${ROOT}/schemas/shared"

MINIMAL_FBS="${ROOT}/map_upload.fbs"
cat > "${MINIMAL_FBS}" <<'FBS'
include "schemas/shared/action_common/action_common.fbs";

namespace aorta.action.slam;

table MapUploadGoal {
  map_name: string;
}

table MapUploadResult {
  success: bool;
}

table MapUploadFeedbackData {
  progress_pct: ubyte;
}

root_type MapUploadGoal;
FBS

# 1. Expand the minimal single-action schema into the full 8-table protocol.
EXPANDED_FBS="${ROOT}/map_upload_expanded.fbs"
"${ACTION_TOOL}" --root-fbs="${MINIMAL_FBS}" --out="${EXPANDED_FBS}"

grep -Fq 'table MapUploadSendGoalRequest {' "${EXPANDED_FBS}"
grep -Fq 'table MapUploadGetResultResponse {' "${EXPANDED_FBS}"

# gen_message_wrapper.py requires --root-bfbs (the
# compiled bfbs is the wrapper emitters' only semantic source). Compile the
# expanded schema once here -- `-I "${ROOT}"` resolves its qualified
# `schemas/shared/action_common/action_common.fbs` include via the symlink
# set up above.
EXPANDED_BFBS="${ROOT}/map_upload_expanded.bfbs"
"${FLATC}" --binary --schema --bfbs-builtins -I "${ROOT}" -o "${ROOT}" "${EXPANDED_FBS}"

# 2. WITH --action-name: the aggregate action traits struct is emitted, with the
#    supplied routing name, despite the non-schemas/action/ path.
WITH_NAME_CC="${TEST_TMPDIR}/map_upload_msg.h"
"${TOOL}" \
  --lang=cpp \
  --stem=map_upload \
  --schema-rel= \
  --action-name=slam/map_asset/upload \
  --root-fbs="${EXPANDED_FBS}" \
  --root-bfbs="${EXPANDED_BFBS}" \
  --out="${WITH_NAME_CC}"

grep -Fq 'struct MapUploadAction {' "${WITH_NAME_CC}"
grep -Fq 'using Goal = MapUploadGoal;' "${WITH_NAME_CC}"
grep -Fq 'using GoalMsg = MapUploadGoalMsg;' "${WITH_NAME_CC}"
grep -Fq 'using ResultMsg = MapUploadResultMsg;' "${WITH_NAME_CC}"
grep -Fq 'static constexpr const char* ActionName() {' "${WITH_NAME_CC}"
grep -Fq 'return "slam/map_asset/upload";' "${WITH_NAME_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeSendGoalRequest(' "${WITH_NAME_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeCancelGoalRequest(' "${WITH_NAME_CC}"
grep -Fq 'static std::vector<uint8_t> EncodeGetResultRequest(' "${WITH_NAME_CC}"
grep -Fq 'static ResultMsg CopyResult(const Result& root)' "${WITH_NAME_CC}"
grep -Fq 'static FeedbackDataMsg CopyFeedbackData(const FeedbackData& root)' "${WITH_NAME_CC}"

# 3. Back-compat / negative: WITHOUT --action-name (and not under schemas/action/),
#    no aggregate struct is emitted — pre-existing behavior is unchanged.
NO_NAME_CC="${TEST_TMPDIR}/map_upload_no_name_msg.h"
"${TOOL}" \
  --lang=cpp \
  --stem=map_upload \
  --schema-rel= \
  --root-fbs="${EXPANDED_FBS}" \
  --root-bfbs="${EXPANDED_BFBS}" \
  --out="${NO_NAME_CC}"

if grep -Fq 'struct MapUploadAction {' "${NO_NAME_CC}"; then
  echo "unexpected action traits struct emitted without --action-name" >&2
  exit 1
fi

# The per-message typed wrappers must still be present in both cases.
grep -Fq 'struct MapUploadGoalMsg {' "${NO_NAME_CC}"
grep -Fq 'struct MapUploadGoalMsg {' "${WITH_NAME_CC}"

echo "tier-2 action-name codegen test passed"
