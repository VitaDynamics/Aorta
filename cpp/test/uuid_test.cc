// Copyright (c) 2025-2026 VitaDynamics

#include "aorta/uuid.h"

#include <cstring>

#include <gtest/gtest.h>

#include "mock_aorta.h"

namespace aorta {
namespace {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

TEST(UuidTest, ReturnsGeneratedUuidV7String) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_uuid_v7(_, 37))
      .WillOnce(Invoke([](char *out, size_t) {
        std::strcpy(out, "019f8e0e-226c-7eb0-89c8-6ed19a11d7e9");
        return AORTA_OK;
      }));

  auto uuid_or = NewUuidV7();
  ASSERT_TRUE(uuid_or.ok());
  EXPECT_EQ(*uuid_or, "019f8e0e-226c-7eb0-89c8-6ed19a11d7e9");
}

TEST(UuidTest, PropagatesCoreFailure) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_uuid_v7(_, 37)).WillOnce(Return(AORTA_ERR_INTERNAL));

  auto uuid_or = NewUuidV7();
  ASSERT_FALSE(uuid_or.ok());
  EXPECT_EQ(uuid_or.status().code(), AORTA_ERR_INTERNAL);
}

} // namespace
} // namespace aorta
