// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Subscriber class and options

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "aorta/options.h"
#include "aorta/types.h"

namespace aorta {
namespace {

// Test CallbackGroup enum values
TEST(CallbackGroupTest, AllEnumValues) {
  EXPECT_EQ(static_cast<int>(CallbackGroup::kMutuallyExclusive), 0);
  EXPECT_EQ(static_cast<int>(CallbackGroup::kReentrant), 1);
  EXPECT_NE(CallbackGroup::kMutuallyExclusive, CallbackGroup::kReentrant);
}

// Test CallbackGroup to FFI mapping
TEST(CallbackGroupTest, FfiMapping) {
  // Based on options.h ToFfi implementation:
  // kMutuallyExclusive -> AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE (0)
  // kReentrant -> AORTA_CALLBACK_GROUP_REENTRANT (1)
  EXPECT_EQ(static_cast<int>(CallbackGroup::kMutuallyExclusive), 0);
  EXPECT_EQ(static_cast<int>(CallbackGroup::kReentrant), 1);
}

// ==================== SubscriberOptions Tests ====================

// Test SubscriberOptions default construction
TEST(SubscriberOptionsTest, DefaultConstruction) {
  SubscriberOptions options;
  EXPECT_EQ(options.callback_group, CallbackGroup::kMutuallyExclusive);
  EXPECT_EQ(options.deadline_ns, 0);
  EXPECT_TRUE(options.prefetch_schema_on_init);
  EXPECT_FALSE(options.enable_header_context);
  EXPECT_FALSE(options.enable_dedup);
  EXPECT_EQ(options.dedup_window, 1024u);
  EXPECT_FALSE(options.enable_reorder_check);
  EXPECT_FALSE(options.drop_out_of_order);
  EXPECT_EQ(options.overflow_policy, SubscriberOverflowPolicy::kDrop);
  EXPECT_EQ(options.overflow_relay_capacity, 1024u);
}

// Test SubscriberOptions construction with custom values
TEST(SubscriberOptionsTest, ConstructionWithValues) {
  SubscriberOptions options;
  options.callback_group = CallbackGroup::kReentrant;
  options.deadline_ns = 1000000;  // 1ms in nanoseconds
  options.prefetch_schema_on_init = true;
  options.enable_header_context = true;
  options.enable_dedup = true;
  options.dedup_window = 64;
  options.enable_reorder_check = true;
  options.drop_out_of_order = true;
  options.overflow_policy = SubscriberOverflowPolicy::kRelay;
  options.overflow_relay_capacity = 32;

  EXPECT_EQ(options.callback_group, CallbackGroup::kReentrant);
  EXPECT_EQ(options.deadline_ns, 1000000);
  EXPECT_TRUE(options.prefetch_schema_on_init);
  EXPECT_TRUE(options.enable_header_context);
  EXPECT_TRUE(options.enable_dedup);
  EXPECT_EQ(options.dedup_window, 64u);
  EXPECT_TRUE(options.enable_reorder_check);
  EXPECT_TRUE(options.drop_out_of_order);
  EXPECT_EQ(options.overflow_policy, SubscriberOverflowPolicy::kRelay);
  EXPECT_EQ(options.overflow_relay_capacity, 32u);
}

// Test SubscriberOptions copy semantics
TEST(SubscriberOptionsTest, CopySemantics) {
  SubscriberOptions original;
  original.callback_group = CallbackGroup::kReentrant;
  original.deadline_ns = 5000000;
  original.prefetch_schema_on_init = true;
  original.enable_header_context = true;
  original.enable_dedup = true;
  original.dedup_window = 64;
  original.enable_reorder_check = true;
  original.drop_out_of_order = true;
  original.overflow_policy = SubscriberOverflowPolicy::kRelay;
  original.overflow_relay_capacity = 16;

  SubscriberOptions copied = original;
  EXPECT_EQ(copied.callback_group, CallbackGroup::kReentrant);
  EXPECT_EQ(copied.deadline_ns, 5000000);
  EXPECT_TRUE(copied.prefetch_schema_on_init);
  EXPECT_TRUE(copied.enable_header_context);
  EXPECT_TRUE(copied.enable_dedup);
  EXPECT_EQ(copied.dedup_window, 64u);
  EXPECT_TRUE(copied.enable_reorder_check);
  EXPECT_TRUE(copied.drop_out_of_order);
  EXPECT_EQ(copied.overflow_policy, SubscriberOverflowPolicy::kRelay);
  EXPECT_EQ(copied.overflow_relay_capacity, 16u);

  // Modify original should not affect copy
  original.deadline_ns = 1000000;
  original.prefetch_schema_on_init = false;
  original.enable_header_context = false;
  original.enable_dedup = false;
  original.dedup_window = 1;
  original.enable_reorder_check = false;
  original.drop_out_of_order = false;
  original.overflow_policy = SubscriberOverflowPolicy::kDrop;
  original.overflow_relay_capacity = 1;
  EXPECT_EQ(copied.deadline_ns, 5000000);
  EXPECT_TRUE(copied.prefetch_schema_on_init);
  EXPECT_TRUE(copied.enable_header_context);
  EXPECT_TRUE(copied.enable_dedup);
  EXPECT_EQ(copied.dedup_window, 64u);
  EXPECT_TRUE(copied.enable_reorder_check);
  EXPECT_TRUE(copied.drop_out_of_order);
  EXPECT_EQ(copied.overflow_policy, SubscriberOverflowPolicy::kRelay);
  EXPECT_EQ(copied.overflow_relay_capacity, 16u);
}

// Test SubscriberOptions move semantics
TEST(SubscriberOptionsTest, MoveSemantics) {
  SubscriberOptions original;
  original.callback_group = CallbackGroup::kReentrant;
  original.deadline_ns = 5000000;
  original.prefetch_schema_on_init = true;
  original.enable_header_context = true;
  original.enable_dedup = true;
  original.dedup_window = 64;
  original.enable_reorder_check = true;
  original.drop_out_of_order = true;
  original.overflow_policy = SubscriberOverflowPolicy::kRelay;
  original.overflow_relay_capacity = 8;

  SubscriberOptions moved = std::move(original);
  EXPECT_EQ(moved.callback_group, CallbackGroup::kReentrant);
  EXPECT_EQ(moved.deadline_ns, 5000000);
  EXPECT_TRUE(moved.prefetch_schema_on_init);
  EXPECT_TRUE(moved.enable_header_context);
  EXPECT_TRUE(moved.enable_dedup);
  EXPECT_EQ(moved.dedup_window, 64u);
  EXPECT_TRUE(moved.enable_reorder_check);
  EXPECT_TRUE(moved.drop_out_of_order);
  EXPECT_EQ(moved.overflow_policy, SubscriberOverflowPolicy::kRelay);
  EXPECT_EQ(moved.overflow_relay_capacity, 8u);
}

// Test SubscriberOptions with zero deadline
TEST(SubscriberOptionsTest, ZeroDeadline) {
  SubscriberOptions options;
  options.deadline_ns = 0;

  EXPECT_EQ(options.deadline_ns, 0);
}

// Test SubscriberOptions with negative deadline
TEST(SubscriberOptionsTest, NegativeDeadline) {
  SubscriberOptions options;
  options.deadline_ns = -1000;

  EXPECT_EQ(options.deadline_ns, -1000);
}

// Test SubscriberOptions with max deadline
TEST(SubscriberOptionsTest, MaxDeadline) {
  SubscriberOptions options;
  options.deadline_ns = INT64_MAX;

  EXPECT_EQ(options.deadline_ns, INT64_MAX);
}

// Test SubscriberOptions with various deadline values
TEST(SubscriberOptionsTest, VariousDeadlines) {
  // 1 microsecond = 1000 nanoseconds
  {
    SubscriberOptions options;
    options.deadline_ns = 1000;
    EXPECT_EQ(options.deadline_ns, 1000);
  }

  // 1 millisecond = 1,000,000 nanoseconds
  {
    SubscriberOptions options;
    options.deadline_ns = 1000000;
    EXPECT_EQ(options.deadline_ns, 1000000);
  }

  // 1 second = 1,000,000,000 nanoseconds
  {
    SubscriberOptions options;
    options.deadline_ns = 1000000000;
    EXPECT_EQ(options.deadline_ns, 1000000000LL);
  }
}

// ==================== MessageContext Tests ====================

// Test MessageContext default construction
TEST(MessageContextTest, DefaultConstruction) {
  MessageContext ctx;
  EXPECT_TRUE(ctx.topic.empty());
  EXPECT_EQ(ctx.receive_stamp_ns, 0);
  EXPECT_EQ(ctx.priority, 0);
  EXPECT_TRUE(ctx.publisher_node.empty());
  EXPECT_EQ(ctx.sequence, 0u);
}

// Test MessageContext construction
TEST(MessageContextTest, ConstructionWithValues) {
  MessageContext ctx;
  ctx.topic = "aorta/default/pub/test";
  ctx.receive_stamp_ns = 1234567890;
  ctx.priority = 2;  // Normal priority
  ctx.publisher_node = "publisher_node";
  ctx.sequence = 42;

  EXPECT_EQ(ctx.topic, "aorta/default/pub/test");
  EXPECT_EQ(ctx.receive_stamp_ns, 1234567890);
  EXPECT_EQ(ctx.priority, 2);
  EXPECT_EQ(ctx.publisher_node, "publisher_node");
  EXPECT_EQ(ctx.sequence, 42u);
}

// Test MessageContext copy semantics
TEST(MessageContextTest, CopySemantics) {
  MessageContext original;
  original.topic = "original/topic";
  original.receive_stamp_ns = 1000;
  original.priority = 1;
  original.publisher_node = "original_publisher";
  original.sequence = 1;

  MessageContext copied = original;
  EXPECT_EQ(copied.topic, "original/topic");
  EXPECT_EQ(copied.receive_stamp_ns, 1000);
  EXPECT_EQ(copied.priority, 1);
  EXPECT_EQ(copied.publisher_node, "original_publisher");
  EXPECT_EQ(copied.sequence, 1u);
}

// Test MessageContext move semantics
TEST(MessageContextTest, MoveSemantics) {
  MessageContext original;
  original.topic = "move/topic";
  original.receive_stamp_ns = 1000;
  original.priority = 1;
  original.publisher_node = "move_publisher";
  original.sequence = 1;

  MessageContext moved = std::move(original);
  EXPECT_EQ(moved.topic, "move/topic");
  EXPECT_EQ(moved.receive_stamp_ns, 1000);
  EXPECT_EQ(moved.priority, 1);
  EXPECT_EQ(moved.publisher_node, "move_publisher");
  EXPECT_EQ(moved.sequence, 1u);

  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_TRUE(original.topic.empty());  // NOLINT
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_TRUE(original.publisher_node.empty());  // NOLINT
}

// Test MessageContext with empty fields
TEST(MessageContextTest, EmptyFields) {
  MessageContext ctx;
  ctx.topic = "";
  ctx.receive_stamp_ns = 0;
  ctx.priority = 0;
  ctx.publisher_node = "";
  ctx.sequence = 0;

  EXPECT_TRUE(ctx.topic.empty());
  EXPECT_EQ(ctx.receive_stamp_ns, 0);
  EXPECT_EQ(ctx.priority, 0);
  EXPECT_TRUE(ctx.publisher_node.empty());
  EXPECT_EQ(ctx.sequence, 0u);
}

// Test MessageContext with long topic
TEST(MessageContextTest, LongTopic) {
  MessageContext ctx;
  ctx.topic = std::string(256, 'a');

  EXPECT_EQ(ctx.topic.length(), 256u);
}

// Test MessageContext with various priority values
TEST(MessageContextTest, PriorityValues) {
  // Priority levels: 0=Realtime, 1=High, 2=Normal, 3=Low, 4=Background
  for (int i = 0; i <= 4; ++i) {
    MessageContext ctx;
    ctx.priority = i;
    EXPECT_EQ(ctx.priority, i);
  }
}

// Test MessageContext with max sequence
TEST(MessageContextTest, MaxSequence) {
  MessageContext ctx;
  ctx.sequence = UINT64_MAX;

  EXPECT_EQ(ctx.sequence, UINT64_MAX);
}

// Test MessageContext with negative receive_stamp (edge case)
TEST(MessageContextTest, NegativeTimestamp) {
  MessageContext ctx;
  ctx.receive_stamp_ns = -1;

  EXPECT_EQ(ctx.receive_stamp_ns, -1);
}

// ==================== MessageView Tests ====================

// Test MessageView default construction
TEST(MessageViewTest, DefaultConstruction) {
  MessageView view;
  EXPECT_EQ(view.data, nullptr);
  EXPECT_EQ(view.size, 0u);
}

// Test MessageView construction with data
TEST(MessageViewTest, ConstructionWithData) {
  uint8_t buffer[] = {0x01, 0x02, 0x03, 0x04, 0x05};
  MessageView view{buffer, sizeof(buffer)};

  EXPECT_EQ(view.data, buffer);
  EXPECT_EQ(view.size, 5u);
}

// Test MessageView with empty data
TEST(MessageViewTest, EmptyData) {
  MessageView view{nullptr, 0};

  EXPECT_EQ(view.data, nullptr);
  EXPECT_EQ(view.size, 0u);
}

// Test MessageView with zero size but non-null data
TEST(MessageViewTest, ZeroSizeNonNullData) {
  uint8_t dummy = 0;
  MessageView view{&dummy, 0};

  EXPECT_EQ(view.data, &dummy);
  EXPECT_EQ(view.size, 0u);
}

// Move-only contract is locked by static_asserts in
// cpp/test/types_test.cc::MessageViewTest::LifetimeContract.
// This file used to test that copy-construction was permitted; that
// pattern is now intentionally a compile error.

// Test MessageView with large data
TEST(MessageViewTest, LargeData) {
  std::vector<uint8_t> large_buffer(10000, 0xAB);
  MessageView view{large_buffer.data(), large_buffer.size()};

  EXPECT_EQ(view.data, large_buffer.data());
  EXPECT_EQ(view.size, 10000u);
}

// ==================== Integration Tests ====================

// Test subscriber callback signature compatibility
TEST(SubscriberIntegrationTest, CallbackSignature) {
  // Verify that we can create callbacks with the expected signature
  // Subscriber::Callback type: std::function<void(const MessageView&, const
  // MessageContextView&)>
  using CallbackType =
      std::function<void(const MessageView&, const MessageContextView&)>;

  CallbackType callback = [](const MessageView& view,
                             const MessageContextView& ctx) {
    // Callback implementation
    EXPECT_EQ(view.size, 0u);  // Empty view in test
    EXPECT_TRUE(ctx.topic.empty());
  };

  // Test invoking the callback
  MessageView view;
  MessageContextView ctx;
  callback(view, ctx);
}

// Test subscriber options integration with MessageContext
TEST(SubscriberIntegrationTest, OptionsAndContext) {
  SubscriberOptions options;
  options.callback_group = CallbackGroup::kReentrant;
  options.deadline_ns = 1000000;  // 1ms
  options.prefetch_schema_on_init = true;

  MessageContext ctx;
  ctx.topic = "aorta/default/pub/test";
  ctx.receive_stamp_ns = 1234567890;
  ctx.priority = 2;
  ctx.publisher_node = "test_pub";
  ctx.sequence = 42;

  // Simulate callback usage
  EXPECT_EQ(options.callback_group, CallbackGroup::kReentrant);
  EXPECT_TRUE(options.prefetch_schema_on_init);
  EXPECT_EQ(ctx.topic, "aorta/default/pub/test");
  EXPECT_EQ(ctx.sequence, 42u);
}

// Test all combinations of callback groups and priority levels
TEST(SubscriberIntegrationTest, AllCallbackGroupPriorityCombinations) {
  for (auto group :
       {CallbackGroup::kMutuallyExclusive, CallbackGroup::kReentrant}) {
    for (int priority = 0; priority <= 4; ++priority) {
      SubscriberOptions options;
      options.callback_group = group;

      MessageContext ctx;
      ctx.priority = priority;

      EXPECT_EQ(options.callback_group, group);
      EXPECT_EQ(ctx.priority, priority);
    }
  }
}

}  // namespace
}  // namespace aorta
