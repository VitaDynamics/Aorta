#include <gtest/gtest.h>

#include <cstdlib>

#include "aorta/detail/reentrancy_check.h"
#include "mock_aorta.h"

namespace {

void HitOffSite() { AORTA_CHECK_NOT_REENTRANT("off-site"); }

TEST(ReentrancyCheckOffTest, UnsetAndGarbageModesAreSilent) {
  EXPECT_EXIT(
      {
        aorta::SetMockReentrantContext(true);
        unsetenv("AORTA_REENTRANT_ASSERT");
        HitOffSite();
        setenv("AORTA_REENTRANT_ASSERT", "1", 1);
        HitOffSite();
        std::_Exit(0);
      },
      testing::ExitedWithCode(0), "^$");
  EXPECT_EXIT(
      {
        aorta::SetMockReentrantContext(true);
        setenv("AORTA_REENTRANT_ASSERT", "0", 1);
        HitOffSite();
        std::_Exit(0);
      },
      testing::ExitedWithCode(0), "^$");
  EXPECT_EXIT(
      {
        aorta::SetMockReentrantContext(true);
        setenv("AORTA_REENTRANT_ASSERT", "garbage", 1);
        HitOffSite();
        std::_Exit(0);
      },
      testing::ExitedWithCode(0), "^$");
}

}  // namespace
