#include "guard_action_probe_msg.h"
#include "guard_service_probe_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryFixture, ActionThenServiceCompiles) {
  ::aorta::services::fixture_guard::GuardServiceProbeRequestMsg request;
  request.label = "fixture";
  ::aorta::action::example::GuardActionProbeGoalMsg goal;
  goal.label = "fixture";

  EXPECT_FALSE(
      TypedSubscribeTraits<
          ::aorta::services::fixture_guard::GuardServiceProbeRequest>::Verify(
          nullptr, 0));
}

}  // namespace
}  // namespace aorta
