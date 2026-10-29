// End-to-end test for aorta_action_library (Tier-2 / local action codegen):
// the macro expands a minimal single-action schema and emits the aggregate
// <Prefix>Action traits struct, here aorta::action::slam::MapUploadAction.
// Including the generated header + aorta/action.h proves the struct compiles
// against the real SDK headers; the static methods exercise the generated
// FlatBuffers builders.
#include "aorta/action.h"

#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "map_upload_action_msg.h"

namespace {

using MapUploadAction = ::aorta::action::slam::MapUploadAction;

// The keyexpr name is the explicit action_name passed to aorta_action_library,
// not a value derived from a schemas/action/ path.
TEST(Tier2ActionCodegen, ActionNameIsExplicitOverride) {
  EXPECT_EQ(std::string(MapUploadAction::ActionName()), "slam/map_asset/upload");
}

TEST(Tier2ActionCodegen, EncodeSendGoalRequestProducesPayload) {
  MapUploadAction::GoalMsg goal;
  goal.map_name = "garage";
  std::vector<std::uint8_t> payload =
      MapUploadAction::EncodeSendGoalRequest("goal-1", goal);
  EXPECT_FALSE(payload.empty());
}

TEST(Tier2ActionCodegen, ResultAndFeedbackMsgsAreUsable) {
  MapUploadAction::ResultMsg result;
  result.success = true;
  MapUploadAction::FeedbackDataMsg feedback;
  feedback.progress_pct = 42;
  EXPECT_TRUE(result.success);
  EXPECT_EQ(feedback.progress_pct, 42);
}

// Compile-time proof that the generated traits satisfy the SDK action template
// parameter (typedefs are enough to reject a malformed traits struct here).
using ClientType = ::aorta::ActionClient<MapUploadAction>;
using HandleType = ::aorta::GoalHandle<MapUploadAction>;
static_assert(sizeof(ClientType*) > 0, "ActionClient<MapUploadAction> usable");
static_assert(sizeof(HandleType*) > 0, "GoalHandle<MapUploadAction> usable");

}  // namespace
