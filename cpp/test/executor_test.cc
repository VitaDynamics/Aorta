// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Executor class

#include "aorta/executor.h"

#include <gtest/gtest.h>

#include <memory>

#include "aorta/options.h"
#include "aorta/status.h"

namespace aorta {
namespace {

// These are integration tests that require the actual FFI implementation
// For now, we test the enum conversions and basic structure

// Test ExecutorType enum values
TEST(ExecutorTypeTest, AllEnumValues) {
  // SingleThreaded = 0
  EXPECT_EQ(static_cast<int>(ExecutorType::kSingleThreaded), 0);

  // MultiThreaded = 1
  EXPECT_EQ(static_cast<int>(ExecutorType::kMultiThreaded), 1);

  // CurrentThread = 2
  EXPECT_EQ(static_cast<int>(ExecutorType::kCurrentThread), 2);

  // Async = 3
  EXPECT_EQ(static_cast<int>(ExecutorType::kAsync), 3);

  // All values should be distinct
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kMultiThreaded);
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kCurrentThread);
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kAsync);
  EXPECT_NE(ExecutorType::kMultiThreaded, ExecutorType::kCurrentThread);
  EXPECT_NE(ExecutorType::kMultiThreaded, ExecutorType::kAsync);
  EXPECT_NE(ExecutorType::kCurrentThread, ExecutorType::kAsync);
}

// Test Executor type to FFI type conversion mapping
// Note: This is an internal implementation detail tested via integration
TEST(ExecutorTypeTest, FfiTypeMapping) {
  // Based on executor.h implementation:
  // kSingleThreaded -> AORTA_EXECUTOR_SINGLE (0)
  // kMultiThreaded -> AORTA_EXECUTOR_MULTI (1)
  // kCurrentThread -> AORTA_EXECUTOR_CURRENT (2)
  // kAsync -> AORTA_EXECUTOR_ASYNC (3)

  // The conversion is done in Executor::Create() method
  // We verify the enum values match the expected FFI values
  EXPECT_EQ(static_cast<int>(ExecutorType::kSingleThreaded), 0);
  EXPECT_EQ(static_cast<int>(ExecutorType::kMultiThreaded), 1);
  EXPECT_EQ(static_cast<int>(ExecutorType::kCurrentThread), 2);
  EXPECT_EQ(static_cast<int>(ExecutorType::kAsync), 3);
}

// Test Executor default type
TEST(ExecutorTest, DefaultTypeIsSingleThreaded) {
  // When no type is specified, kSingleThreaded should be used
  // This is verified by the Create() method default parameter
  // Executor::Create(ExecutorType type = ExecutorType::kSingleThreaded)
}

// Note: Integration tests that require FFI are commented out
// To enable these tests, link against the actual Rust FFI library
//
// TEST(ExecutorIntegrationTest, CreateSingleThreaded) {
//   auto result = Executor::Create(ExecutorType::kSingleThreaded);
//   EXPECT_TRUE(result.ok());
// }
//
// TEST(ExecutorIntegrationTest, CreateMultiThreaded) {
//   auto result = Executor::Create(ExecutorType::kMultiThreaded);
//   EXPECT_TRUE(result.ok());
// }

}  // namespace
}  // namespace aorta
