#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "aorta/detail/reentrancy_check.h"
#include "mock_aorta.h"

namespace {

void HitWarnSite() { AORTA_CHECK_NOT_REENTRANT("warn-site"); }

TEST(ReentrancyCheckWarnTest, WarnsOncePerSiteAcrossThreads) {
  setenv("AORTA_REENTRANT_ASSERT", "1", 1);
  aorta::SetMockReentrantContext(true);
  testing::internal::CaptureStderr();
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([] {
      for (int hit = 0; hit < 100; ++hit) {
        HitWarnSite();
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  const std::string output = testing::internal::GetCapturedStderr();
  const std::string needle = "[aorta] reentrancy guard: warn-site\n";
  EXPECT_EQ(output, needle);
}

}  // namespace
