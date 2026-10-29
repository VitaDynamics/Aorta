// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Types (MessageView, MessageContext)

#include "aorta/types.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

namespace aorta {
namespace {

// ==================== MessageView Tests ====================

// Test default construction
TEST(MessageViewTest, DefaultConstruction) {
  MessageView view;
  EXPECT_EQ(view.data, nullptr);
  EXPECT_EQ(view.size, 0u);
}

// Test construction with data
TEST(MessageViewTest, ConstructionWithData) {
  uint8_t buffer[] = {0x01, 0x02, 0x03, 0x04, 0x05};
  MessageView view{buffer, sizeof(buffer)};

  EXPECT_EQ(view.data, buffer);
  EXPECT_EQ(view.size, 5u);
}

// Test empty view
TEST(MessageViewTest, EmptyView) {
  MessageView view{nullptr, 0};
  EXPECT_EQ(view.data, nullptr);
  EXPECT_EQ(view.size, 0u);
}

// Test view with null data but non-zero size (edge case)
TEST(MessageViewTest, NullDataWithSize) {
  MessageView view{nullptr, 100};
  EXPECT_EQ(view.data, nullptr);
  EXPECT_EQ(view.size, 100u);
}

// Test view access
TEST(MessageViewTest, DataAccess) {
  uint8_t buffer[] = {'h', 'e', 'l', 'l', 'o'};
  MessageView view{buffer, sizeof(buffer)};

  EXPECT_EQ(view.data[0], 'h');
  EXPECT_EQ(view.data[4], 'o');
}

// MessageView lifetime contract: copy is deleted, move is allowed.
//
// Copying a MessageView into a background
// thread / queue is a dangling-borrow bug because the underlying FFI
// buffer is only valid for the callback's stack frame. Deleting the
// copy ops makes that pattern a compile error; the user must call
// `view.ToVector()` and capture the owned vector instead.
//
// The static_asserts below lock the contract — accidentally restoring
// copy semantics in types.h would fail this test at compile time.
TEST(MessageViewTest, LifetimeContract) {
  static_assert(!std::is_copy_constructible_v<MessageView>,
                "MessageView must not be copy-constructible");
  static_assert(!std::is_copy_assignable_v<MessageView>,
                "MessageView must not be copy-assignable");
  static_assert(std::is_move_constructible_v<MessageView>,
                "MessageView must remain move-constructible so View() and "
                "ToMessageView() can return by value");
  static_assert(std::is_move_assignable_v<MessageView>,
                "MessageView must remain move-assignable");

  // Move smoke test: the buffer pointer survives the move.
  uint8_t buffer[] = {1, 2, 3};
  MessageView original{buffer, 3};
  const uint8_t* original_ptr = original.data;
  MessageView moved = std::move(original);
  EXPECT_EQ(moved.data, original_ptr);
  EXPECT_EQ(moved.size, 3u);
}

TEST(MessageViewTest, MultiSliceToVector) {
  uint8_t part1[] = {0x10, 0x11};
  uint8_t part2[] = {0x12, 0x13, 0x14};
  MessageSlice slices[] = {
      MessageSlice{part1, sizeof(part1)},
      MessageSlice{part2, sizeof(part2)},
  };

  MessageView view;
  view.size = sizeof(part1) + sizeof(part2);
  view.slices = slices;
  view.slice_count = 2;

  EXPECT_FALSE(view.IsContiguous());
  auto merged = view.ToVector();
  ASSERT_EQ(merged.size(), 5u);
  EXPECT_EQ(merged[0], 0x10);
  EXPECT_EQ(merged[1], 0x11);
  EXPECT_EQ(merged[2], 0x12);
  EXPECT_EQ(merged[3], 0x13);
  EXPECT_EQ(merged[4], 0x14);
}

TEST(MessageViewTest, SingleSliceIsContiguous) {
  uint8_t payload[] = {0xAB, 0xCD, 0xEF};
  MessageSlice slice{payload, sizeof(payload)};
  MessageView view;
  view.data = payload;
  view.size = sizeof(payload);
  view.slices = &slice;
  view.slice_count = 1;

  EXPECT_TRUE(view.IsContiguous());
  auto copied = view.ToVector();
  EXPECT_EQ(copied.size(), sizeof(payload));
  EXPECT_EQ(copied[0], 0xAB);
  EXPECT_EQ(copied[1], 0xCD);
  EXPECT_EQ(copied[2], 0xEF);
}

// ==================== MessageContext Tests ====================

// Test default construction
TEST(MessageContextTest, DefaultConstruction) {
  MessageContext ctx;
  EXPECT_TRUE(ctx.topic.empty());
  EXPECT_EQ(ctx.receive_stamp_ns, 0);
  EXPECT_EQ(ctx.priority, 0);
  EXPECT_TRUE(ctx.publisher_node.empty());
  EXPECT_EQ(ctx.sequence, 0u);
}

// Test construction with values
TEST(MessageContextTest, ConstructionWithValues) {
  MessageContext ctx;
  ctx.topic = "aorta/default/pub/camera/image";
  ctx.receive_stamp_ns = 1234567890;
  ctx.priority = 2;  // Normal priority
  ctx.publisher_node = "camera_node";
  ctx.sequence = 42;

  EXPECT_EQ(ctx.topic, "aorta/default/pub/camera/image");
  EXPECT_EQ(ctx.receive_stamp_ns, 1234567890);
  EXPECT_EQ(ctx.priority, 2);
  EXPECT_EQ(ctx.publisher_node, "camera_node");
  EXPECT_EQ(ctx.sequence, 42u);
}

// Test copy semantics
TEST(MessageContextTest, CopySemantics) {
  MessageContext original;
  original.topic = "test/topic";
  original.receive_stamp_ns = 1000;
  original.priority = 1;
  original.publisher_node = "publisher";
  original.sequence = 99;

  MessageContext copied = original;

  EXPECT_EQ(copied.topic, original.topic);
  EXPECT_EQ(copied.receive_stamp_ns, original.receive_stamp_ns);
  EXPECT_EQ(copied.priority, original.priority);
  EXPECT_EQ(copied.publisher_node, original.publisher_node);
  EXPECT_EQ(copied.sequence, original.sequence);
}

// Test move semantics
TEST(MessageContextTest, MoveSemantics) {
  MessageContext original;
  original.topic = "move/test/topic";
  original.publisher_node = "move_publisher";

  MessageContext moved = std::move(original);

  EXPECT_EQ(moved.topic, "move/test/topic");
  EXPECT_EQ(moved.publisher_node, "move_publisher");
}

// Test with long topic name
TEST(MessageContextTest, LongTopicName) {
  MessageContext ctx;
  ctx.topic = std::string(256, 'a');

  EXPECT_EQ(ctx.topic.length(), 256u);
}

// Test with empty fields
TEST(MessageContextTest, EmptyFields) {
  MessageContext ctx;
  ctx.topic = "";
  ctx.publisher_node = "";

  EXPECT_TRUE(ctx.topic.empty());
  EXPECT_TRUE(ctx.publisher_node.empty());
}

// Test priority values (valid range 0-4)
TEST(MessageContextTest, PriorityValues) {
  MessageContext ctx;

  // Realtime
  ctx.priority = 0;
  EXPECT_EQ(ctx.priority, 0);

  // High
  ctx.priority = 1;
  EXPECT_EQ(ctx.priority, 1);

  // Normal
  ctx.priority = 2;
  EXPECT_EQ(ctx.priority, 2);

  // Low
  ctx.priority = 3;
  EXPECT_EQ(ctx.priority, 3);

  // Background
  ctx.priority = 4;
  EXPECT_EQ(ctx.priority, 4);
}

// Test large sequence number
TEST(MessageContextTest, LargeSequenceNumber) {
  MessageContext ctx;
  ctx.sequence = std::numeric_limits<uint64_t>::max();

  EXPECT_EQ(ctx.sequence, std::numeric_limits<uint64_t>::max());
}

// Test negative receive_stamp_ns (edge case - simulation time)
TEST(MessageContextTest, NegativeTimestamp) {
  MessageContext ctx;
  ctx.receive_stamp_ns = -1000;

  EXPECT_EQ(ctx.receive_stamp_ns, -1000);
}

// ==================== MessageContextView Tests ====================

// Test default construction
TEST(MessageContextViewTest, DefaultConstruction) {
  MessageContextView ctx;
  EXPECT_TRUE(ctx.topic.empty());
  EXPECT_EQ(ctx.receive_stamp_ns, 0);
  EXPECT_EQ(ctx.priority, 0);
  EXPECT_TRUE(ctx.publisher_node.empty());
  EXPECT_EQ(ctx.sequence, 0u);
}

// Test construction with values (borrows from existing strings)
TEST(MessageContextViewTest, BorrowsFromStrings) {
  std::string topic = "aorta/default/pub/camera/image";
  std::string publisher = "camera_node";

  MessageContextView ctx;
  ctx.topic = topic;
  ctx.receive_stamp_ns = 1234567890;
  ctx.priority = 2;
  ctx.publisher_node = publisher;
  ctx.sequence = 42;

  EXPECT_EQ(ctx.topic, "aorta/default/pub/camera/image");
  EXPECT_EQ(ctx.receive_stamp_ns, 1234567890);
  EXPECT_EQ(ctx.priority, 2);
  EXPECT_EQ(ctx.publisher_node, "camera_node");
  EXPECT_EQ(ctx.sequence, 42u);

  // Verify it's actually borrowing (points into same storage)
  EXPECT_EQ(ctx.topic.data(), topic.data());
  EXPECT_EQ(ctx.publisher_node.data(), publisher.data());
}

// Test ToOwned produces independent MessageContext
TEST(MessageContextViewTest, ToOwnedProducesIndependentCopy) {
  std::string topic = "aorta/default/pub/sensor/data";
  std::string publisher = "sensor_node_1";

  MessageContextView view;
  view.topic = topic;
  view.receive_stamp_ns = 99999;
  view.priority = 3;
  view.publisher_node = publisher;
  view.sequence = 1001;

  MessageContext owned = view.ToOwned();

  EXPECT_EQ(owned.topic, "aorta/default/pub/sensor/data");
  EXPECT_EQ(owned.receive_stamp_ns, 99999);
  EXPECT_EQ(owned.priority, 3);
  EXPECT_EQ(owned.publisher_node, "sensor_node_1");
  EXPECT_EQ(owned.sequence, 1001u);

  // Owned copy must have its own storage (not borrowing from view source)
  EXPECT_NE(owned.topic.data(), topic.data());
  EXPECT_NE(owned.publisher_node.data(), publisher.data());
}

// Test ToOwned with empty fields
TEST(MessageContextViewTest, ToOwnedEmptyFields) {
  MessageContextView view;
  MessageContext owned = view.ToOwned();

  EXPECT_TRUE(owned.topic.empty());
  EXPECT_EQ(owned.receive_stamp_ns, 0);
  EXPECT_EQ(owned.priority, 0);
  EXPECT_TRUE(owned.publisher_node.empty());
  EXPECT_EQ(owned.sequence, 0u);
}

// MessageContextView lifetime contract: copy deleted, move allowed
// (same rationale as MessageView::LifetimeContract above).
TEST(MessageContextViewTest, LifetimeContract) {
  static_assert(!std::is_copy_constructible_v<MessageContextView>,
                "MessageContextView must not be copy-constructible");
  static_assert(!std::is_copy_assignable_v<MessageContextView>,
                "MessageContextView must not be copy-assignable");
  static_assert(std::is_move_constructible_v<MessageContextView>,
                "MessageContextView must remain move-constructible");
  static_assert(std::is_move_assignable_v<MessageContextView>,
                "MessageContextView must remain move-assignable");

  std::string topic = "test/topic";
  std::string publisher = "publisher";

  MessageContextView original;
  original.topic = topic;
  original.receive_stamp_ns = 1000;
  original.priority = 1;
  original.publisher_node = publisher;
  original.sequence = 99;

  // Move semantics still work; the borrowed string_view data pointer
  // survives the move (string_view copies/moves are pointer copies).
  const char* topic_ptr = original.topic.data();
  MessageContextView moved = std::move(original);
  EXPECT_EQ(moved.topic.data(), topic_ptr);
  EXPECT_EQ(moved.receive_stamp_ns, 1000);
  EXPECT_EQ(moved.priority, 1);
  EXPECT_EQ(moved.sequence, 99u);
}

// ==================== Integration Tests ====================

// Test MessageView and MessageContext together (simulating callback usage)
TEST(TypesIntegrationTest, CallbackScenario) {
  uint8_t payload[] = {0x08, 0x00, 0x00, 0x00};  // FlatBuffer header
  MessageView view{payload, sizeof(payload)};

  std::string topic = "aorta/default/pub/sensor/data";
  std::string publisher = "sensor_node_1";

  MessageContextView ctx;
  ctx.topic = topic;
  ctx.receive_stamp_ns = 1234567890123;
  ctx.priority = 2;
  ctx.publisher_node = publisher;
  ctx.sequence = 1001;

  // Verify view matches context
  EXPECT_EQ(view.size, 4u);
  EXPECT_EQ(ctx.topic, "aorta/default/pub/sensor/data");
  EXPECT_EQ(ctx.sequence, 1001u);

  // User can convert to owned if needed
  MessageContext owned = ctx.ToOwned();
  EXPECT_EQ(owned.topic, "aorta/default/pub/sensor/data");
  EXPECT_EQ(owned.sequence, 1001u);
}

}  // namespace
}  // namespace aorta
