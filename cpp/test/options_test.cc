// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Options classes (PublisherOptions, SubscriberOptions)

#include "aorta/options.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace aorta {
namespace {

// ==================== PublisherOptions Tests ====================

// Test default construction
TEST(PublisherOptionsTest, DefaultConstruction) {
  PublisherOptions opts;
  EXPECT_EQ(opts.schema_attach_policy, SchemaAttachPolicy::kInherit);
  EXPECT_TRUE(opts.schema_bfbs.empty());
  EXPECT_TRUE(opts.schema_root_type.empty());
  EXPECT_FALSE(opts.max_message_size.has_value());
  // congestion_control is now an optional override; default is "inherit QoS".
  EXPECT_FALSE(opts.congestion_control.has_value());
}

// Test ToFfi with default values
TEST(PublisherOptionsTest, ToFfiDefaultValues) {
  PublisherOptions opts;
  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_INHERIT);
  EXPECT_EQ(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 0u);
  EXPECT_EQ(ffi.max_message_size, 0u);
  EXPECT_EQ(ffi.schema_root_type, nullptr);
  // Default (std::nullopt) encodes as INHERIT, so Core resolves against the QoS
  // instead of the byte forcing Drop.
  EXPECT_EQ(ffi.congestion_control, AORTA_PUB_CONGESTION_INHERIT);
}

// Block (backpressure) is now selectable from the C++ SDK; a set
// override encodes as the explicit Drop/Block sentinel (not INHERIT).
TEST(PublisherOptionsTest, ToFfiCongestionControlBlock) {
  PublisherOptions opts;
  opts.congestion_control = CongestionControl::kBlock;
  aorta_pub_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.congestion_control, AORTA_PUB_CONGESTION_BLOCK);

  opts.congestion_control = CongestionControl::kDrop;
  ffi = opts.ToFfi();
  EXPECT_EQ(ffi.congestion_control, AORTA_PUB_CONGESTION_DROP);
}

// Test schema_attach_policy mapping
TEST(PublisherOptionsTest, SchemaAttachPolicyMapping) {
  struct TestCase {
    SchemaAttachPolicy cpp_policy;
    aorta_schema_attach_policy_t expected_ffi;
  };

  TestCase cases[] = {
      {SchemaAttachPolicy::kInherit, AORTA_SCHEMA_ATTACH_INHERIT},
      {SchemaAttachPolicy::kAlways, AORTA_SCHEMA_ATTACH_ALWAYS},
      {SchemaAttachPolicy::kNever, AORTA_SCHEMA_ATTACH_NEVER},
  };

  for (const auto& tc : cases) {
    PublisherOptions opts;
    opts.schema_attach_policy = tc.cpp_policy;
    aorta_pub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.schema_attach_policy, tc.expected_ffi);
  }
}

// Test schema_bfbs with data
TEST(PublisherOptionsTest, SchemaBfbsWithData) {
  PublisherOptions opts;
  opts.schema_bfbs = {0x01, 0x02, 0x03, 0x04, 0x05};

  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_NE(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 5u);
  EXPECT_EQ(ffi.schema_bfbs[0], 0x01);
  EXPECT_EQ(ffi.schema_bfbs[4], 0x05);
}

// Test empty schema_bfbs
TEST(PublisherOptionsTest, SchemaBfbsEmpty) {
  PublisherOptions opts;
  opts.schema_bfbs.clear();

  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 0u);
}

TEST(PublisherOptionsTest, SchemaRootTypeMapping) {
  PublisherOptions opts;
  EXPECT_EQ(opts.ToFfi().schema_root_type, nullptr);

  opts.schema_root_type = "demo.Foo";
  aorta_pub_options_t ffi = opts.ToFfi();
  ASSERT_NE(ffi.schema_root_type, nullptr);
  EXPECT_STREQ(ffi.schema_root_type, "demo.Foo");
}

// Test large schema_bfbs
TEST(PublisherOptionsTest, SchemaBfbsLarge) {
  PublisherOptions opts;
  opts.schema_bfbs.resize(10000, 0xAB);

  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_NE(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 10000u);
}

// Test max_message_size with value
TEST(PublisherOptionsTest, MaxMessageSizeWithValue) {
  PublisherOptions opts;
  opts.max_message_size = 4096;

  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.max_message_size, 4096u);
}

// Test max_message_size without value (defaults to 0)
TEST(PublisherOptionsTest, MaxMessageSizeWithoutValue) {
  PublisherOptions opts;
  opts.max_message_size = std::nullopt;

  aorta_pub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.max_message_size, 0u);
}

// Test ToFfi with schema_attach_policy does not affect max_message_size
TEST(PublisherOptionsTest, ToFfiFieldsIndependent) {
  PublisherOptions opts;
  opts.max_message_size = 8192;

  aorta_pub_options_t ffi = opts.ToFfi();

  // max_message_size should be set correctly
  EXPECT_EQ(ffi.max_message_size, 8192u);
  // Other fields should still be at their defaults
  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_INHERIT);
}

// Test copy semantics
TEST(PublisherOptionsTest, CopySemantics) {
  PublisherOptions original;
  original.schema_attach_policy = SchemaAttachPolicy::kAlways;
  original.schema_bfbs = {1, 2, 3};
  original.max_message_size = 2048;

  PublisherOptions copied = original;

  EXPECT_EQ(copied.schema_attach_policy, original.schema_attach_policy);
  EXPECT_EQ(copied.schema_bfbs, original.schema_bfbs);
  EXPECT_EQ(copied.max_message_size, original.max_message_size);
}

// Test move semantics
TEST(PublisherOptionsTest, MoveSemantics) {
  PublisherOptions original;
  original.schema_bfbs = {1, 2, 3, 4, 5};

  PublisherOptions moved = std::move(original);

  EXPECT_EQ(moved.schema_bfbs.size(), 5u);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_TRUE(original.schema_bfbs.empty());  // NOLINT
}

// ==================== SubscriberOptions Tests ====================

// Test default construction
TEST(SubscriberOptionsTest, DefaultConstruction) {
  SubscriberOptions opts;
  EXPECT_EQ(opts.callback_group, CallbackGroup::kMutuallyExclusive);
  EXPECT_EQ(opts.deadline_ns, 0);
  EXPECT_TRUE(opts.prefetch_schema_on_init);
  EXPECT_FALSE(opts.enable_header_context);
  EXPECT_FALSE(opts.include_topic_in_context);
  EXPECT_FALSE(opts.include_receive_timestamp);
  EXPECT_FALSE(opts.include_priority_in_context);
  EXPECT_FALSE(opts.include_publisher_timestamp);
  EXPECT_FALSE(opts.enable_dedup);
  EXPECT_EQ(opts.dedup_window, 1024u);
  EXPECT_FALSE(opts.enable_reorder_check);
  EXPECT_FALSE(opts.drop_out_of_order);
  EXPECT_EQ(opts.overflow_policy, SubscriberOverflowPolicy::kDrop);
  EXPECT_EQ(opts.overflow_relay_capacity, 1024u);
  EXPECT_TRUE(opts.schema_bfbs.empty());
}

// Test ToFfi with default values
TEST(SubscriberOptionsTest, ToFfiDefaultValues) {
  SubscriberOptions opts;
  aorta_sub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE);
  EXPECT_EQ(ffi.deadline_ns, 0);
  EXPECT_EQ(ffi.prefetch_schema_on_init, 1);
  EXPECT_EQ(ffi.enable_header_context, 0);
  EXPECT_EQ(ffi.include_topic_in_context, 0);
  EXPECT_EQ(ffi.include_receive_timestamp, 0);
  EXPECT_EQ(ffi.include_priority_in_context, 0);
  EXPECT_EQ(ffi.include_publisher_timestamp, 0);
  EXPECT_EQ(ffi.enable_dedup, 0);
  EXPECT_EQ(ffi.dedup_window, 1024u);
  EXPECT_EQ(ffi.enable_reorder_check, 0);
  EXPECT_EQ(ffi.drop_out_of_order, 0);
  EXPECT_EQ(ffi.overflow_policy, AORTA_SUB_OVERFLOW_DROP);
  EXPECT_EQ(ffi.overflow_relay_capacity, 1024u);
  EXPECT_EQ(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 0u);
}

// Test callback_group mapping
TEST(SubscriberOptionsTest, CallbackGroupMapping) {
  {
    SubscriberOptions opts;
    opts.callback_group = CallbackGroup::kMutuallyExclusive;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE);
  }

  {
    SubscriberOptions opts;
    opts.callback_group = CallbackGroup::kReentrant;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_REENTRANT);
  }
}

// Test deadline_ns with various values
TEST(SubscriberOptionsTest, DeadlineNsValues) {
  {
    SubscriberOptions opts;
    opts.deadline_ns = 1000000;  // 1ms in ns
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.deadline_ns, 1000000);
  }

  {
    SubscriberOptions opts;
    opts.deadline_ns = -1;  // Edge case: negative (should be preserved)
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.deadline_ns, -1);
  }

  {
    SubscriberOptions opts;
    opts.deadline_ns = std::numeric_limits<int64_t>::max();
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.deadline_ns, std::numeric_limits<int64_t>::max());
  }
}

TEST(SubscriberOptionsTest, PrefetchSchemaOnInitMapping) {
  {
    SubscriberOptions opts;
    opts.prefetch_schema_on_init = false;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.prefetch_schema_on_init, 0);
  }

  {
    SubscriberOptions opts;
    opts.prefetch_schema_on_init = true;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.prefetch_schema_on_init, 1);
  }
}

TEST(SubscriberOptionsTest, EnableHeaderContextMapping) {
  {
    SubscriberOptions opts;
    opts.enable_header_context = false;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.enable_header_context, 0);
  }

  {
    SubscriberOptions opts;
    opts.enable_header_context = true;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.enable_header_context, 1);
  }
}

TEST(SubscriberOptionsTest, ContextMetadataFlagsMapping) {
  {
    SubscriberOptions opts;
    opts.include_topic_in_context = false;
    opts.include_receive_timestamp = false;
    opts.include_priority_in_context = false;
    opts.include_publisher_timestamp = false;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.include_topic_in_context, 0);
    EXPECT_EQ(ffi.include_receive_timestamp, 0);
    EXPECT_EQ(ffi.include_priority_in_context, 0);
    EXPECT_EQ(ffi.include_publisher_timestamp, 0);
  }

  {
    SubscriberOptions opts;
    opts.include_topic_in_context = true;
    opts.include_receive_timestamp = true;
    opts.include_priority_in_context = true;
    opts.include_publisher_timestamp = true;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.include_topic_in_context, 1);
    EXPECT_EQ(ffi.include_receive_timestamp, 1);
    EXPECT_EQ(ffi.include_priority_in_context, 1);
    EXPECT_EQ(ffi.include_publisher_timestamp, 1);
  }
}

TEST(SubscriberOptionsTest, DedupAndReorderMapping) {
  {
    SubscriberOptions opts;
    opts.enable_dedup = true;
    opts.dedup_window = 64;
    opts.enable_reorder_check = true;
    opts.drop_out_of_order = true;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.enable_dedup, 1);
    EXPECT_EQ(ffi.dedup_window, 64u);
    EXPECT_EQ(ffi.enable_reorder_check, 1);
    EXPECT_EQ(ffi.drop_out_of_order, 1);
  }

  {
    SubscriberOptions opts;
    opts.enable_dedup = false;
    opts.dedup_window = 0;
    opts.enable_reorder_check = false;
    opts.drop_out_of_order = false;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.enable_dedup, 0);
    EXPECT_EQ(ffi.dedup_window, 0u);
    EXPECT_EQ(ffi.enable_reorder_check, 0);
    EXPECT_EQ(ffi.drop_out_of_order, 0);
  }
}

TEST(SubscriberOptionsTest, OverflowPolicyMapping) {
  {
    SubscriberOptions opts;
    opts.overflow_policy = SubscriberOverflowPolicy::kDrop;
    opts.overflow_relay_capacity = 128;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.overflow_policy, AORTA_SUB_OVERFLOW_DROP);
    EXPECT_EQ(ffi.overflow_relay_capacity, 128u);
  }

  {
    SubscriberOptions opts;
    opts.overflow_policy = SubscriberOverflowPolicy::kRelay;
    opts.overflow_relay_capacity = 64;
    aorta_sub_options_t ffi = opts.ToFfi();
    EXPECT_EQ(ffi.overflow_policy, AORTA_SUB_OVERFLOW_RELAY);
    EXPECT_EQ(ffi.overflow_relay_capacity, 64u);
  }
}

// QoS & buffering redesign: new knobs default to today's behavior, carried by
// the dedicated aorta_sub_buffering_t (aorta_sub_options_t is layout-frozen).
TEST(SubscriberOptionsTest, BufferingDefaults) {
  SubscriberOptions opts;
  EXPECT_EQ(opts.receive_depth, 0u);
  EXPECT_EQ(opts.delivery, DeliveryMode::kExecutor);
  EXPECT_EQ(opts.receive_overflow, ReceiveOverflow::kDropNewest);

  aorta_sub_buffering_t ffi = opts.ToFfiBuffering();
  EXPECT_EQ(ffi.receive_depth, 0u);
  EXPECT_EQ(ffi.delivery_mode, 0u);
  EXPECT_EQ(ffi.receive_overflow, 0u);
}

// ToFfiBuffering maps the Tier A cap and the pull-ring sugar onto the
// buffering companion struct.
TEST(SubscriberOptionsTest, ToFfiBufferingMapsKnobs) {
  {
    SubscriberOptions opts;
    opts.receive_depth = 32;  // Tier A cap on the executor path
    aorta_sub_buffering_t ffi = opts.ToFfiBuffering();
    EXPECT_EQ(ffi.receive_depth, 32u);
    EXPECT_EQ(ffi.delivery_mode, 0u);          // executor
    EXPECT_EQ(ffi.receive_overflow, 0u);       // drop-newest
  }
  {
    SubscriberOptions opts;
    opts.delivery = DeliveryMode::kInlinePull;
    opts.receive_overflow = ReceiveOverflow::kDropOldest;
    opts.receive_depth = 8;
    aorta_sub_buffering_t ffi = opts.ToFfiBuffering();
    EXPECT_EQ(ffi.delivery_mode, 1u);          // inline_pull
    EXPECT_EQ(ffi.receive_overflow, 1u);       // drop-oldest
    EXPECT_EQ(ffi.receive_depth, 8u);
  }
}

// Test copy semantics
TEST(SubscriberOptionsTest, CopySemantics) {
  SubscriberOptions original;
  original.callback_group = CallbackGroup::kReentrant;
  original.deadline_ns = 5000000;
  original.prefetch_schema_on_init = true;
  original.enable_header_context = true;
  original.include_topic_in_context = false;
  original.include_receive_timestamp = false;
  original.include_priority_in_context = false;
  original.include_publisher_timestamp = false;
  original.enable_dedup = true;
  original.dedup_window = 128;
  original.enable_reorder_check = true;
  original.drop_out_of_order = true;
  original.overflow_policy = SubscriberOverflowPolicy::kRelay;
  original.overflow_relay_capacity = 256;

  SubscriberOptions copied = original;

  EXPECT_EQ(copied.callback_group, original.callback_group);
  EXPECT_EQ(copied.deadline_ns, original.deadline_ns);
  EXPECT_EQ(copied.prefetch_schema_on_init, original.prefetch_schema_on_init);
  EXPECT_EQ(copied.enable_header_context, original.enable_header_context);
  EXPECT_EQ(copied.include_topic_in_context, original.include_topic_in_context);
  EXPECT_EQ(copied.include_receive_timestamp, original.include_receive_timestamp);
  EXPECT_EQ(copied.include_priority_in_context, original.include_priority_in_context);
  EXPECT_EQ(copied.include_publisher_timestamp, original.include_publisher_timestamp);
  EXPECT_EQ(copied.enable_dedup, original.enable_dedup);
  EXPECT_EQ(copied.dedup_window, original.dedup_window);
  EXPECT_EQ(copied.enable_reorder_check, original.enable_reorder_check);
  EXPECT_EQ(copied.drop_out_of_order, original.drop_out_of_order);
  EXPECT_EQ(copied.overflow_policy, original.overflow_policy);
  EXPECT_EQ(copied.overflow_relay_capacity, original.overflow_relay_capacity);
}

// ==================== ServiceOptions Tests ====================

TEST(ServiceOptionsTest, DefaultConstruction) {
  ServiceOptions opts;
  EXPECT_EQ(opts.schema_attach_policy, SchemaAttachPolicy::kNever);
  EXPECT_EQ(opts.callback_group, CallbackGroup::kMutuallyExclusive);
  EXPECT_EQ(opts.deadline_ns, 0);
  EXPECT_EQ(opts.rpc_overflow_policy, RpcOverflowPolicy::kFastFailBusy);
  EXPECT_EQ(opts.rpc_overflow_block_timeout_ms, 0u);
  EXPECT_TRUE(opts.service_schema_hash_hex.empty());
  EXPECT_TRUE(opts.service_schema_bfbs.empty());
  EXPECT_TRUE(opts.request_root_type.empty());
  EXPECT_TRUE(opts.response_root_type.empty());
}

TEST(ServiceOptionsTest, ToFfiDefaultValues) {
  ServiceOptions opts;
  aorta_service_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_NEVER);
  EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE);
  EXPECT_EQ(ffi.deadline_ns, 0);
  EXPECT_EQ(ffi.service_schema_hash_hex, nullptr);
  EXPECT_EQ(ffi.service_schema_hash_len, 0u);
  EXPECT_EQ(ffi.service_schema_bfbs, nullptr);
  EXPECT_EQ(ffi.service_schema_bfbs_len, 0u);
  EXPECT_EQ(ffi.request_root_type, nullptr);
  EXPECT_EQ(ffi.request_root_type_len, 0u);
  EXPECT_EQ(ffi.response_root_type, nullptr);
  EXPECT_EQ(ffi.response_root_type_len, 0u);
  EXPECT_EQ(ffi.rpc_overflow_policy, AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY);
  EXPECT_EQ(ffi.rpc_overflow_block_timeout_ms, 0u);
}

TEST(ServiceOptionsTest, ToFfiWithSchemaHashes) {
  ServiceOptions opts;
  opts.schema_attach_policy = SchemaAttachPolicy::kAlways;
  opts.service_schema_hash_hex =
      "1111111111111111111111111111111111111111111111111111111111111111";
  opts.service_schema_bfbs = {0x11, 0x22, 0x33};
  opts.request_root_type = "demo.rpc.Request";
  opts.response_root_type = "demo.rpc.Response";
  opts.rpc_overflow_policy = RpcOverflowPolicy::kBlock;
  opts.rpc_overflow_block_timeout_ms = 15;

  aorta_service_options_t ffi = opts.ToFfi();
  ASSERT_NE(ffi.service_schema_hash_hex, nullptr);
  ASSERT_NE(ffi.service_schema_bfbs, nullptr);
  ASSERT_NE(ffi.request_root_type, nullptr);
  ASSERT_NE(ffi.response_root_type, nullptr);
  EXPECT_EQ(ffi.service_schema_hash_len, opts.service_schema_hash_hex.size());
  EXPECT_EQ(ffi.service_schema_bfbs_len, opts.service_schema_bfbs.size());
  EXPECT_EQ(ffi.request_root_type_len, opts.request_root_type.size());
  EXPECT_EQ(ffi.response_root_type_len, opts.response_root_type.size());
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(ffi.service_schema_hash_hex),
                  ffi.service_schema_hash_len),
      opts.service_schema_hash_hex);
  EXPECT_EQ(std::vector<uint8_t>(ffi.service_schema_bfbs,
                                 ffi.service_schema_bfbs +
                                     ffi.service_schema_bfbs_len),
            opts.service_schema_bfbs);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(ffi.request_root_type),
                  ffi.request_root_type_len),
      opts.request_root_type);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(ffi.response_root_type),
                  ffi.response_root_type_len),
      opts.response_root_type);
  EXPECT_EQ(ffi.rpc_overflow_policy, AORTA_RPC_OVERFLOW_BLOCK);
  EXPECT_EQ(ffi.rpc_overflow_block_timeout_ms, 15u);
}

// ==================== TimerOptions Tests ====================

TEST(TimerOptionsTest, DefaultConstructionAndToFfi) {
  TimerOptions opts;
  EXPECT_EQ(opts.callback_group, CallbackGroup::kMutuallyExclusive);
  EXPECT_EQ(opts.callback_group_handle, 0u);
  EXPECT_TRUE(opts.auto_start);
  EXPECT_FALSE(opts.invoke_on_missed);
  EXPECT_EQ(opts.overflow_policy, TimerOverflowPolicy::kCoalesceLatest);
  EXPECT_EQ(opts.overflow_block_timeout_ms, 0u);

  aorta_timer_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE);
  EXPECT_EQ(ffi.callback_group_handle, 0u);
  EXPECT_EQ(ffi.auto_start, 1u);
  EXPECT_EQ(ffi.invoke_on_missed, 0u);
  EXPECT_EQ(ffi.overflow_policy, AORTA_TIMER_OVERFLOW_COALESCE_LATEST);
  EXPECT_EQ(ffi.overflow_block_timeout_ms, 0u);
}

TEST(TimerOptionsTest, ToFfiCustomValues) {
  TimerOptions opts;
  opts.callback_group = CallbackGroup::kReentrant;
  opts.callback_group_handle = 42;
  opts.auto_start = false;
  opts.invoke_on_missed = true;
  opts.overflow_policy = TimerOverflowPolicy::kBlock;
  opts.overflow_block_timeout_ms = 25;

  aorta_timer_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_REENTRANT);
  EXPECT_EQ(ffi.callback_group_handle, 42u);
  EXPECT_EQ(ffi.auto_start, 0u);
  EXPECT_EQ(ffi.invoke_on_missed, 1u);
  EXPECT_EQ(ffi.overflow_policy, AORTA_TIMER_OVERFLOW_BLOCK);
  EXPECT_EQ(ffi.overflow_block_timeout_ms, 25u);
}

TEST(ExecutionGroupTest, ExecutionPolicyMapsToExpectedCallbackGroup) {
  EXPECT_EQ(CallbackGroupFromExecutionPolicy(ExecutionPolicy::kSerialized),
            CallbackGroup::kMutuallyExclusive);
  EXPECT_EQ(CallbackGroupFromExecutionPolicy(ExecutionPolicy::kConcurrent),
            CallbackGroup::kReentrant);
}

// ==================== ClientOptions Tests ====================

TEST(ClientOptionsTest, DefaultConstruction) {
  ClientOptions opts;
  EXPECT_EQ(opts.timeout_ms, 5000u);
  EXPECT_EQ(opts.schema_attach_policy, SchemaAttachPolicy::kInherit);
  EXPECT_FALSE(opts.enforce_single_provider.has_value());
  EXPECT_FALSE(opts.multi_reply_check_window_ms.has_value());
  EXPECT_FALSE(opts.allow_retry.has_value());
  EXPECT_TRUE(opts.service_schema_hash_hex.empty());
}

TEST(ClientOptionsTest, ToFfiDefaultValues) {
  constexpr uint8_t kOptionalBoolInherit = 0;

  ClientOptions opts;
  aorta_client_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.timeout_ms, 5000u);
  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_INHERIT);
  EXPECT_EQ(ffi.enforce_single_provider, kOptionalBoolInherit);
  EXPECT_EQ(ffi.allow_retry, kOptionalBoolInherit);
  EXPECT_EQ(ffi.multi_reply_check_window_ms, 0u);
  EXPECT_EQ(ffi.service_schema_hash_hex, nullptr);
  EXPECT_EQ(ffi.service_schema_hash_len, 0u);
}

TEST(ClientOptionsTest, ToFfiWithSchemaHashes) {
  constexpr uint8_t kOptionalBoolTrue = 2;

  ClientOptions opts;
  opts.timeout_ms = 1234;
  opts.schema_attach_policy = SchemaAttachPolicy::kAlways;
  opts.enforce_single_provider = true;
  opts.multi_reply_check_window_ms = 77;
  opts.service_schema_hash_hex =
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

  aorta_client_options_t ffi = opts.ToFfi();
  ASSERT_NE(ffi.service_schema_hash_hex, nullptr);
  EXPECT_EQ(ffi.timeout_ms, 1234u);
  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_ALWAYS);
  EXPECT_EQ(ffi.enforce_single_provider, kOptionalBoolTrue);
  EXPECT_EQ(ffi.multi_reply_check_window_ms, 77u);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(ffi.service_schema_hash_hex),
                  ffi.service_schema_hash_len),
      opts.service_schema_hash_hex);
}

TEST(ClientOptionsTest, ToFfiWithStrictModeDisabled) {
  constexpr uint8_t kOptionalBoolFalse = 1;

  ClientOptions opts;
  opts.enforce_single_provider = false;

  aorta_client_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.enforce_single_provider, kOptionalBoolFalse);
  EXPECT_EQ(ffi.multi_reply_check_window_ms, 0u);
}

TEST(ClientOptionsTest, ToFfiWithAllowRetryEnabled) {
  constexpr uint8_t kOptionalBoolTrue = 2;

  ClientOptions opts;
  opts.allow_retry = true;

  aorta_client_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.allow_retry, kOptionalBoolTrue);
}

TEST(ClientOptionsTest, ToFfiWithAllowRetryDisabled) {
  constexpr uint8_t kOptionalBoolFalse = 1;

  ClientOptions opts;
  opts.allow_retry = false;

  aorta_client_options_t ffi = opts.ToFfi();
  EXPECT_EQ(ffi.allow_retry, kOptionalBoolFalse);
}

// ==================== Enum Value Tests ====================

// Test SchemaAttachPolicy enum values
TEST(EnumValueTest, SchemaAttachPolicyValues) {
  // Ensure enum values are distinct
  EXPECT_NE(SchemaAttachPolicy::kInherit, SchemaAttachPolicy::kAlways);
  EXPECT_NE(SchemaAttachPolicy::kInherit, SchemaAttachPolicy::kNever);
  EXPECT_NE(SchemaAttachPolicy::kAlways, SchemaAttachPolicy::kNever);
}

// Test CallbackGroup enum values
TEST(EnumValueTest, CallbackGroupValues) {
  EXPECT_NE(CallbackGroup::kMutuallyExclusive, CallbackGroup::kReentrant);
}

// Test ExecutorType enum values
TEST(EnumValueTest, ExecutorTypeValues) {
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kMultiThreaded);
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kCurrentThread);
  EXPECT_NE(ExecutorType::kSingleThreaded, ExecutorType::kAsync);
  EXPECT_NE(ExecutorType::kMultiThreaded, ExecutorType::kCurrentThread);
  EXPECT_NE(ExecutorType::kMultiThreaded, ExecutorType::kAsync);
  EXPECT_NE(ExecutorType::kCurrentThread, ExecutorType::kAsync);
}

TEST(EnumValueTest, SchedulerModeValues) {
  EXPECT_NE(SchedulerMode::kFifo, SchedulerMode::kPriority);
}

TEST(ExecutorOptionsTest, ToFfiDefaultValues) {
  ExecutorOptions opts;
  aorta_priority_scheduler_options_t priority{};
  aorta_executor_admission_options_t admission{};
  aorta_executor_class_policy_options_t class_policy{};
  aorta_executor_options_t ffi =
      opts.ToFfi(&priority, &admission, &class_policy);

  EXPECT_EQ(ffi.scheduler_mode, AORTA_SCHEDULER_FIFO);
  ASSERT_NE(ffi.priority, nullptr);
  ASSERT_NE(ffi.admission, nullptr);
  ASSERT_NE(ffi.class_policy, nullptr);
  EXPECT_EQ(ffi.priority->max_drain_per_cycle, 32u);
  EXPECT_EQ(ffi.priority->weights[0], 8u);
  EXPECT_EQ(ffi.priority->weights[1], 4u);
  EXPECT_EQ(ffi.priority->weights[2], 2u);
  EXPECT_EQ(ffi.priority->weights[3], 1u);
  EXPECT_EQ(ffi.priority->weights[4], 1u);
  EXPECT_EQ(ffi.priority->map_subscription_qos, 1u);
  EXPECT_EQ(ffi.admission->max_queued_events, 0u);
  EXPECT_EQ(ffi.admission->max_queued_bytes, 0u);
  EXPECT_EQ(ffi.class_policy->sub_overflow_policy, AORTA_SUB_OVERFLOW_DROP);
  EXPECT_EQ(ffi.class_policy->sub_overflow_relay_capacity, 1024u);
  EXPECT_EQ(ffi.class_policy->rpc_overflow_policy,
            AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY);
  EXPECT_EQ(ffi.class_policy->rpc_overflow_block_timeout_ms, 0u);
  EXPECT_EQ(ffi.class_policy->timer_overflow_policy,
            AORTA_TIMER_OVERFLOW_COALESCE_LATEST);
  EXPECT_EQ(ffi.class_policy->timer_overflow_block_timeout_ms, 0u);
}

TEST(ExecutorOptionsTest, ToFfiPriorityValues) {
  ExecutorOptions opts;
  opts.type = ExecutorType::kSingleThreaded;
  opts.scheduler_mode = SchedulerMode::kPriority;
  opts.priority.max_drain_per_cycle = 128;
  opts.priority.weights = {9, 7, 5, 3, 1};
  opts.priority.map_subscription_qos = false;
  opts.admission.max_queued_events = 123;
  opts.admission.max_queued_bytes = 456;
  opts.class_policy.sub_overflow_policy = SubscriberOverflowPolicy::kRelay;
  opts.class_policy.sub_overflow_relay_capacity = 2048;
  opts.class_policy.rpc_overflow_policy = RpcOverflowPolicy::kBlock;
  opts.class_policy.rpc_overflow_block_timeout_ms = 33;
  opts.class_policy.timer_overflow_policy = TimerOverflowPolicy::kBlock;
  opts.class_policy.timer_overflow_block_timeout_ms = 44;

  aorta_priority_scheduler_options_t priority{};
  aorta_executor_admission_options_t admission{};
  aorta_executor_class_policy_options_t class_policy{};
  aorta_executor_options_t ffi =
      opts.ToFfi(&priority, &admission, &class_policy);

  EXPECT_EQ(ffi.scheduler_mode, AORTA_SCHEDULER_PRIORITY);
  ASSERT_NE(ffi.priority, nullptr);
  ASSERT_NE(ffi.admission, nullptr);
  ASSERT_NE(ffi.class_policy, nullptr);
  EXPECT_EQ(ffi.priority->max_drain_per_cycle, 128u);
  EXPECT_EQ(ffi.priority->weights[0], 9u);
  EXPECT_EQ(ffi.priority->weights[1], 7u);
  EXPECT_EQ(ffi.priority->weights[2], 5u);
  EXPECT_EQ(ffi.priority->weights[3], 3u);
  EXPECT_EQ(ffi.priority->weights[4], 1u);
  EXPECT_EQ(ffi.priority->map_subscription_qos, 0u);
  EXPECT_EQ(ffi.admission->max_queued_events, 123u);
  EXPECT_EQ(ffi.admission->max_queued_bytes, 456u);
  EXPECT_EQ(ffi.class_policy->sub_overflow_policy, AORTA_SUB_OVERFLOW_RELAY);
  EXPECT_EQ(ffi.class_policy->sub_overflow_relay_capacity, 2048u);
  EXPECT_EQ(ffi.class_policy->rpc_overflow_policy, AORTA_RPC_OVERFLOW_BLOCK);
  EXPECT_EQ(ffi.class_policy->rpc_overflow_block_timeout_ms, 33u);
  EXPECT_EQ(ffi.class_policy->timer_overflow_policy, AORTA_TIMER_OVERFLOW_BLOCK);
  EXPECT_EQ(ffi.class_policy->timer_overflow_block_timeout_ms, 44u);
}

// ==================== Integration Tests ====================

// Test that PublisherOptions with all fields set converts correctly
TEST(PublisherOptionsIntegrationTest, FullConfiguration) {
  PublisherOptions opts;
  opts.schema_attach_policy = SchemaAttachPolicy::kAlways;
  opts.schema_bfbs = std::vector<uint8_t>{0x00, 0x01, 0x02, 0x03};
  opts.max_message_size = 8192;

  aorta_pub_options_t ffi = opts.ToFfi();

  // Verify all fields
  EXPECT_EQ(ffi.schema_attach_policy, AORTA_SCHEMA_ATTACH_ALWAYS);
  EXPECT_NE(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 4u);
  EXPECT_EQ(ffi.max_message_size, 8192u);
}

// Test that SubscriberOptions with all fields set converts correctly
TEST(SubscriberOptionsIntegrationTest, FullConfiguration) {
  SubscriberOptions opts;
  opts.callback_group = CallbackGroup::kReentrant;
  opts.deadline_ns = 10000000;  // 10ms
  opts.prefetch_schema_on_init = true;
  opts.enable_header_context = true;
  opts.include_topic_in_context = false;
  opts.include_receive_timestamp = false;
  opts.include_priority_in_context = false;
  opts.include_publisher_timestamp = false;
  opts.enable_dedup = true;
  opts.dedup_window = 256;
  opts.enable_reorder_check = true;
  opts.drop_out_of_order = true;
  opts.overflow_policy = SubscriberOverflowPolicy::kRelay;
  opts.overflow_relay_capacity = 32;
  opts.schema_bfbs = {1, 2, 3, 4};

  aorta_sub_options_t ffi = opts.ToFfi();

  EXPECT_EQ(ffi.callback_group, AORTA_CALLBACK_GROUP_REENTRANT);
  EXPECT_EQ(ffi.deadline_ns, 10000000);
  EXPECT_EQ(ffi.prefetch_schema_on_init, 1);
  EXPECT_EQ(ffi.enable_header_context, 1);
  EXPECT_EQ(ffi.include_topic_in_context, 0);
  EXPECT_EQ(ffi.include_receive_timestamp, 0);
  EXPECT_EQ(ffi.include_priority_in_context, 0);
  EXPECT_EQ(ffi.include_publisher_timestamp, 0);
  EXPECT_EQ(ffi.enable_dedup, 1);
  EXPECT_EQ(ffi.dedup_window, 256u);
  EXPECT_EQ(ffi.enable_reorder_check, 1);
  EXPECT_EQ(ffi.drop_out_of_order, 1);
  EXPECT_EQ(ffi.overflow_policy, AORTA_SUB_OVERFLOW_RELAY);
  EXPECT_EQ(ffi.overflow_relay_capacity, 32u);
  EXPECT_NE(ffi.schema_bfbs, nullptr);
  EXPECT_EQ(ffi.schema_bfbs_len, 4u);
}

}  // namespace
}  // namespace aorta
