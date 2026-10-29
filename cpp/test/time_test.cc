// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for global time API.

#include "aorta/time.h"

#include <gtest/gtest.h>

#include "mock_aorta.h"

namespace aorta {
namespace {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

TEST(TimeTest, NowNsReturnsValueFromFfi) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillOnce(Invoke([](int64_t* out_ns) {
        *out_ns = 9876543210LL;
        return AORTA_OK;
      }));

  auto now_or = NowNs();
  ASSERT_TRUE(now_or.ok());
  EXPECT_EQ(now_or.value(), 9876543210LL);
}

TEST(TimeTest, NowNsReturnsErrorWhenFfiFails) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_now_ns(_)).WillOnce(Return(AORTA_ERR_INTERNAL));

  auto now_or = NowNs();
  ASSERT_FALSE(now_or.ok());
  EXPECT_EQ(now_or.status().code(), AORTA_ERR_INTERNAL);
}

}  // namespace
}  // namespace aorta
