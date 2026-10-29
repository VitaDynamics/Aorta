#include <gtest/gtest.h>

#include <cstdlib>

#include "aorta/detail/reentrancy_check.h"
#include "mock_aorta.h"

namespace {

TEST(ReentrancyCheckAbortDeathTest, AbortsAtTheCallSite) {
  EXPECT_DEATH(
      {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        aorta::SetMockReentrantContext(true);
        AORTA_CHECK_NOT_REENTRANT("abort-site");
      },
      "Aorta fatal: reentrancy guard: abort-site");
}

}  // namespace
