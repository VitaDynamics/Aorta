#include "guard_action_probe_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPrimaryTemplateFixture,
     IncludedServiceTypeResolvesViaWrapperOnly) {
  EXPECT_FALSE(
      TypedSubscribeTraits<
          ::aorta::services::fixture_guard::GuardServiceProbeRequest>::Verify(
          nullptr, 0));
  EXPECT_EQ(
      TypedSubscribeTraits<
          ::aorta::services::fixture_guard::GuardServiceProbeRequest>::GetRoot(
          nullptr),
      nullptr);
}

}  // namespace
}  // namespace aorta
