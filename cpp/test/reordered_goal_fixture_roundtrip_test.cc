#include "reordered_goal_probe_msg.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using ::aorta::action::example::ProbeKind_SECONDARY;
using ::aorta::action::example::ReorderedGoalProbeGoalMsg;
using ::aorta::detail::typed_aorta_action_example_reordered_goal_probe::BuildReorderedGoalProbeGoalTyped;
using ::aorta::detail::typed_aorta_action_example_reordered_goal_probe::CopyReorderedGoalProbeGoalMsg;

ReorderedGoalProbeGoalMsg MakeSentinelGoal() {
  ReorderedGoalProbeGoalMsg msg;
  msg.sequence = std::uint64_t{1234567890123ULL};
  msg.kind = ProbeKind_SECONDARY;
  msg.label = std::string("fixture-label");
  msg.samples = std::vector<std::int32_t>{-3, 0, 9};
  msg.enabled = true;
  return msg;
}

void ExpectSentinel(const ReorderedGoalProbeGoalMsg& msg) {
  ASSERT_TRUE(msg.sequence.has_value());
  EXPECT_EQ(*msg.sequence, 1234567890123ULL);
  ASSERT_TRUE(msg.kind.has_value());
  EXPECT_EQ(*msg.kind, ProbeKind_SECONDARY);
  ASSERT_TRUE(msg.label.has_value());
  EXPECT_EQ(*msg.label, "fixture-label");
  ASSERT_TRUE(msg.samples.has_value());
  EXPECT_EQ(*msg.samples, (std::vector<std::int32_t>{-3, 0, 9}));
  ASSERT_TRUE(msg.enabled.has_value());
  EXPECT_TRUE(*msg.enabled);
}

TEST(ReorderedGoalFixtureRoundTrip, CanonicalFieldOrderPreservesWireValues) {
  const auto msg = MakeSentinelGoal();
  flatbuffers::FlatBufferBuilder builder;
  const auto offset = BuildReorderedGoalProbeGoalTyped(builder, msg, {});
  builder.Finish(offset);

  const auto* root = ::aorta::action::example::GetReorderedGoalProbeGoal(
      builder.GetBufferPointer());
  ASSERT_NE(root, nullptr);
  EXPECT_EQ(root->sequence(), 1234567890123ULL);
  EXPECT_EQ(root->kind(), ProbeKind_SECONDARY);
  ASSERT_NE(root->label(), nullptr);
  EXPECT_EQ(root->label()->str(), "fixture-label");
  ASSERT_NE(root->samples(), nullptr);
  EXPECT_EQ(root->samples()->Get(0), -3);
  EXPECT_EQ(root->samples()->Get(2), 9);
  EXPECT_TRUE(root->enabled());

  ExpectSentinel(CopyReorderedGoalProbeGoalMsg(*root));
}

}  // namespace
