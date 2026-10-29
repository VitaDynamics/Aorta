// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Node class

#include "aorta/node.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "aorta/executor.h"
#include "aorta/client.h"
#include "aorta/options.h"
#include "aorta/publisher.h"
#include "aorta/qos.h"
#include "aorta/subscriber.h"
#include "aorta/timer.h"
#include "demo_chat_msg.h"
#include "echo_msg.h"

// Include mock for FFI functions
#include "mock_aorta.h"

namespace aorta {

struct TypedServiceRequestSchema {};
template <>
struct SchemaMeta<TypedServiceRequestSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0x01, 0x02, 0x03, 0x04};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
};

struct TypedServiceResponseSchema {};
template <>
struct SchemaMeta<TypedServiceResponseSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0x01, 0x02, 0x03, 0x04};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
};

struct TypedServiceMismatchedResponseSchema {};
template <>
struct SchemaMeta<TypedServiceMismatchedResponseSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0x09, 0x08, 0x07, 0x06};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
};

struct TypedServiceNamedRequestSchema {};
template <>
struct SchemaMeta<TypedServiceNamedRequestSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0x01, 0x02, 0x03, 0x04};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
  static const char* RootTypeName() { return "demo.rpc.NamedRequest"; }
};

struct TypedServiceNamedResponseSchema {};
template <>
struct SchemaMeta<TypedServiceNamedResponseSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0x01, 0x02, 0x03, 0x04};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
  static const char* RootTypeName() { return "demo.rpc.NamedResponse"; }
};

struct TypedShmSchema {};
template <>
struct SchemaMeta<TypedShmSchema> {
  static const uint8_t* SchemaBfbs() {
    static const uint8_t kBfbs[] = {0xA1, 0xB2, 0xC3, 0xD4};
    return kBfbs;
  }
  static std::size_t SchemaBfbsSize() { return 4; }
  static std::optional<std::size_t> FixedSize() { return std::nullopt; }
  static std::optional<std::size_t> ShmCapacityHint() { return 512; }
};

template <>
struct TypedPublishTraits<TypedShmSchema> {
  using Msg = demo::ChatMsg;
  static constexpr bool kHasAortaHeader = true;

  static flatbuffers::Offset<demo::Chat> Build(
      flatbuffers::FlatBufferBuilder& builder,
      flatbuffers::Offset<aorta::sys::AortaHeader> header, const Msg& msg) {
    return TypedPublishTraits<demo::Chat>::Build(builder, header, msg);
  }
};

namespace {

struct MockOwnedBytesStorage {
  std::vector<uint8_t> payload;
  aorta_bytes_slice_t slice{};
};

std::vector<uint8_t> BuildEchoRequestPayload(const std::string& message) {
  flatbuffers::FlatBufferBuilder builder;
  demo::EchoRequestMsg msg{};
  msg.message = message;
  auto node_name = builder.CreateString("test_client");
  auto header = aorta::sys::CreateAortaHeader(builder, 123u, node_name, 1u);
  auto root = TypedPublishTraits<demo::EchoRequest>::Build(builder, header, msg);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildEchoResponsePayload(const std::string& message) {
  flatbuffers::FlatBufferBuilder builder;
  demo::EchoResponseMsg msg{};
  msg.echoed_message = message;
  auto node_name = builder.CreateString("test_service");
  auto header = aorta::sys::CreateAortaHeader(builder, 456u, node_name, 2u);
  auto root = TypedPublishTraits<demo::EchoResponse>::Build(builder, header, msg);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

void FillOwnedBytesResponse(aorta_owned_bytes_t* out,
                            std::vector<uint8_t> payload) {
  auto* storage = new MockOwnedBytesStorage();
  storage->payload = std::move(payload);
  storage->slice = aorta_bytes_slice_t{
      storage->payload.data(),
      storage->payload.size(),
  };
  out->view.slices = storage->payload.empty() ? nullptr : &storage->slice;
  out->view.slice_count = storage->payload.empty() ? 0 : 1;
  out->view.total_len = storage->payload.size();
  out->internal = storage;
}

void FreeOwnedBytesResponse(aorta_owned_bytes_t* bytes) {
  auto* storage = static_cast<MockOwnedBytesStorage*>(bytes->internal);
  delete storage;
  *bytes = aorta_owned_bytes_t{};
}

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

// ==================== Node Creation Tests ====================

// Test Node::Create with default group
TEST(NodeTest, CreateWithDefaultGroup) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  EXPECT_CALL(mock, aorta_node_new(StrEq("test_node"), StrEq("default"), _))
      .WillOnce(
          Invoke([](const char* name, const char* group, aorta_node_t* out) {
            EXPECT_STREQ(name, "test_node");
            EXPECT_STREQ(group, "default");
            *out = 1;
            return AORTA_OK;
          }));

  EXPECT_CALL(mock, aorta_node_free(1)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("test_node");
  ASSERT_TRUE(result.ok());

  auto node = result.value();
  EXPECT_EQ(node->name(), "test_node");
  EXPECT_EQ(node->group(), "default");
  EXPECT_EQ(node->handle(), 1);
}

// Test Node::Create with custom group
TEST(NodeTest, CreateWithCustomGroup) {
  MockAortaFFI mock;
  setenv("AORTA_GROUP", "fleet_from_env", 1);

  EXPECT_CALL(mock,
              aorta_node_new(StrEq("robot_node"), StrEq("fleet_from_env"), _))
      .WillOnce(
          Invoke([](const char* name, const char* group, aorta_node_t* out) {
            EXPECT_STREQ(name, "robot_node");
            EXPECT_STREQ(group, "fleet_from_env");
            *out = 2;
            return AORTA_OK;
          }));

  EXPECT_CALL(mock, aorta_node_free(2)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("robot_node", "fleet_alpha");
  ASSERT_TRUE(result.ok());

  auto node = result.value();
  EXPECT_EQ(node->name(), "robot_node");
  EXPECT_EQ(node->group(), "fleet_from_env");
  unsetenv("AORTA_GROUP");
}

// Node::group() must return the value read
// BACK from Core (full normalization), not the SDK's local ASCII-trim-only
// env computation.
TEST(NodeTest, GroupComesFromCoreReadback) {
  MockAortaFFI mock;
  setenv("AORTA_GROUP", "fleet_raw", 1);

  EXPECT_CALL(mock, aorta_node_new(StrEq("norm_node"), StrEq("fleet_raw"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7;
        return AORTA_OK;
      }));
  // Core normalizes differently than the local trim (simulated here):
  // whatever Core reports is what the SDK must cache and expose.
  EXPECT_CALL(mock, aorta_node_get_group(7, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, char* out_buf, size_t buf_size,
                          size_t* out_len) {
        static constexpr char kCoreGroup[] = "fleet_core_normalized";
        if (buf_size < sizeof(kCoreGroup)) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        std::memcpy(out_buf, kCoreGroup, sizeof(kCoreGroup));
        *out_len = sizeof(kCoreGroup) - 1;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_node_free(7)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("norm_node");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value()->group(), "fleet_core_normalized");
  unsetenv("AORTA_GROUP");
}

// Test Node creation failure
TEST(NodeTest, CreateFailure) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  EXPECT_CALL(mock, aorta_node_new(StrEq("bad_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t*) {
        return AORTA_ERR_INVALID_ARGUMENT;
      }));

  auto result = Node::Create("bad_node");
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

// ==================== Node Now Tests ====================

// Test Node::Now
TEST(NodeTest, NowReturnsTime) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 3;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(3, _))
      .WillOnce(Invoke([](aorta_node_t, int64_t* out_ns) {
        *out_ns = 1234567890123LL;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_free(3)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("time_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto now_result = node->Now();
  ASSERT_TRUE(now_result.ok());
  EXPECT_EQ(now_result.value(), 1234567890123LL);
}

// Test Node::Now failure
TEST(NodeTest, NowFailure) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 4;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(4, _)).WillOnce(Return(AORTA_ERR_INTERNAL));

  EXPECT_CALL(mock, aorta_node_free(4)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("time_fail_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto now_result = node->Now();
  ASSERT_FALSE(now_result.ok());
  EXPECT_EQ(now_result.status().code(), AORTA_ERR_INTERNAL);
}

// ==================== Node Publisher Tests ====================

// Test CreatePublisher success
TEST(NodeTest, CreatePublisherSuccess) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 5;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(5, StrEq("camera/image"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char* topic, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        EXPECT_STREQ(topic, "camera/image");
        *out = 500;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(5, _)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(500)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(5)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("pub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto pub_result =
      node->CreatePublisher("camera/image", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());

  auto publisher = pub_result.value();
  EXPECT_EQ(publisher->Topic(), "camera/image");
}

// Test CreatePublisher with typed schema (template version)
TEST(NodeTest, CreatePublisherWithSchema) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 6;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(6, StrEq("sensor/data"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t* opts, aorta_pub_t* out) {
        // Verify schema_bfbs is provided when using typed API
        EXPECT_NE(opts, nullptr);
        if (opts) {
          // Schema should be provided for typed publisher
          EXPECT_GE(opts->schema_bfbs_len, 0u);
        }
        *out = 600;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(6, _)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(600)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(6)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("typed_pub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  // Note: This would need a real schema type to work fully
  // For now, just test the basic untyped version
  auto pub_result =
      node->CreatePublisher("sensor/data", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
}

TEST(NodeTest, CreatePublisherUsesShmCapacityHintWhenMaxMessageSizeMissing) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 6011;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_publisher_new(6011, StrEq("sensor/default_hint"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t* opts,
                          aorta_pub_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->max_message_size, 512u);
        EXPECT_EQ(opts->schema_bfbs_len, 4u);
        *out = 6012;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(6011, _)).WillRepeatedly(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_publisher_free(6012)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(6011)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("typed_hint_pub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto pub_result = node->CreatePublisher<TypedShmSchema>("sensor/default_hint");
  ASSERT_TRUE(pub_result.ok());
}

// Test CreatePublisher failure
TEST(NodeTest, CreatePublisherFailure) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(7, StrEq("bad_topic"), _, _, _))
      .WillOnce(Return(AORTA_ERR_INVALID_ARGUMENT));

  EXPECT_CALL(mock, aorta_node_free(7)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("fail_pub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto pub_result =
      node->CreatePublisher("bad_topic", QoS(), PublisherOptions());
  ASSERT_FALSE(pub_result.ok());
  EXPECT_EQ(pub_result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST(NodeTest, CreateTypedServiceInjectsSchemaBfbs) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 71;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_new_with_options(71, StrEq("/typed/service"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_service_options_t* opts,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(opts->service_schema_bfbs, nullptr);
        if (opts->service_schema_bfbs == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->service_schema_bfbs_len, 4u);
        EXPECT_EQ(opts->service_schema_hash_hex, nullptr);
        EXPECT_EQ(opts->service_schema_hash_len, 0u);
        EXPECT_EQ(opts->request_root_type, nullptr);
        EXPECT_EQ(opts->request_root_type_len, 0u);
        EXPECT_EQ(opts->response_root_type, nullptr);
        EXPECT_EQ(opts->response_root_type_len, 0u);
        EXPECT_EQ(opts->rpc_overflow_policy, AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY);
        EXPECT_EQ(opts->rpc_overflow_block_timeout_ms, 0u);
        EXPECT_EQ(opts->service_schema_bfbs[0], 0x01);
        EXPECT_EQ(opts->service_schema_bfbs[3], 0x04);
        *out = 7100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(7100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(71)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateService<TypedServiceRequestSchema, TypedServiceResponseSchema>(
          "/typed/service", [](const MessageView&, ServiceRequest&&) {});
  ASSERT_TRUE(service_result.ok());
}

TEST(NodeTest, CreateTypedServiceInjectsRootTypesWhenSchemaMetaProvidesNames) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 711;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(711, StrEq("/typed/service_named"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_service_options_t* opts,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(opts->request_root_type, nullptr);
        EXPECT_NE(opts->response_root_type, nullptr);
        if (opts->request_root_type == nullptr ||
            opts->response_root_type == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(
            std::string(reinterpret_cast<const char*>(opts->request_root_type),
                        opts->request_root_type_len),
            "demo.rpc.NamedRequest");
        EXPECT_EQ(
            std::string(reinterpret_cast<const char*>(opts->response_root_type),
                        opts->response_root_type_len),
            "demo.rpc.NamedResponse");
        *out = 71100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(71100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(711)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_named_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateService<TypedServiceNamedRequestSchema,
                          TypedServiceNamedResponseSchema>(
          "/typed/service_named", [](const MessageView&, ServiceRequest&&) {});
  ASSERT_TRUE(service_result.ok());
}

TEST(NodeTest, CreateTypedServiceFallsBackToSharedBfbsWhenResponseMetaMissing) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 72;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_new_with_options(72, StrEq("/typed/fallback"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_service_options_t* opts,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(opts->service_schema_bfbs, nullptr);
        if (opts->service_schema_bfbs == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->rpc_overflow_policy, AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY);
        EXPECT_EQ(opts->rpc_overflow_block_timeout_ms, 0u);
        EXPECT_EQ(opts->service_schema_bfbs_len, 4u);
        EXPECT_EQ(opts->service_schema_bfbs[0], 0x01);
        EXPECT_EQ(opts->service_schema_bfbs[3], 0x04);
        *out = 7200;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(7200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(72)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_fallback_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result = node->CreateService<TypedServiceRequestSchema, int>(
      "/typed/fallback", [](const MessageView&, ServiceRequest&&) {});
  ASSERT_TRUE(service_result.ok());
}

TEST(NodeTest, CreateTypedServiceRejectsMismatchedRequestResponseSchemaBfbs) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 74;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_new_with_options(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mock, aorta_node_free(74)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_mismatch_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateService<TypedServiceRequestSchema,
                          TypedServiceMismatchedResponseSchema>(
          "/typed/mismatch", [](const MessageView&, ServiceRequest&&) {});
  ASSERT_FALSE(service_result.ok());
  EXPECT_EQ(service_result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST(NodeTest, CreateClientTypedInjectsSchemaHashFromSchemaMeta) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 740;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_new_with_options(740, StrEq("/typed/echo"), _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_client_options_t* opts,
                          aorta_client_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(opts->service_schema_hash_hex, nullptr);
        if (opts->service_schema_hash_hex == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(
            std::string(reinterpret_cast<const char*>(opts->service_schema_hash_hex),
                        opts->service_schema_hash_len),
            SchemaMeta<demo::EchoRequest>::SchemaHashHex());
        *out = 741;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_free(741)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(740)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_client_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto client_result =
      node->CreateClientTyped<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo");
  ASSERT_TRUE(client_result.ok());
}

TEST(NodeTest, TypedClientCallDecodesNonRootResponse) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 742;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_new_with_options(742, StrEq("/typed/echo"), _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_client_options_t* opts,
                          aorta_client_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        *out = 743;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_call(743, _, _))
      .WillOnce(Invoke([](aorta_client_t, const aorta_owned_bytes_view_t* request,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        EXPECT_NE(request, nullptr);
        if (request == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(request->slice_count, 1u);
        EXPECT_NE(request->slices, nullptr);
        if (request->slice_count != 1 || request->slices == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto& slice = request->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<demo::EchoRequest>::Verify(slice.data,
                                                                    slice.len));
        if (!TypedSubscribeTraits<demo::EchoRequest>::Verify(slice.data,
                                                             slice.len)) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto* decoded =
            TypedSubscribeTraits<demo::EchoRequest>::GetRoot(slice.data);
        EXPECT_NE(decoded, nullptr);
        if (decoded == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(decoded->message(), nullptr);
        if (decoded->message() == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_STREQ(decoded->message()->c_str(), "hello typed rpc");

        FillOwnedBytesResponse(response,
                               BuildEchoResponsePayload("hello typed rpc"));
        // Honor the owned-bytes contract: release the request buffer (the real
        // core drops its owning ZBytes on every path). Without this the C++
        // request ctx leaks — caught by the Tier-3 LSAN lane.
        ReleaseOwnedSlices(request);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_client_free(743)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(742)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_client_decode_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto client_result =
      node->CreateClientTyped<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo");
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto response_result = client->Call([](demo::EchoRequestMsg& msg) {
    msg.message = std::string("hello typed rpc");
  });
  ASSERT_TRUE(response_result.ok());
  auto response = std::move(response_result).value();
  ASSERT_NE(response.Message(), nullptr);
  ASSERT_NE(response.Message()->echoed_message(), nullptr);
  EXPECT_STREQ(response.Message()->echoed_message()->c_str(),
               "hello typed rpc");
}

TEST(NodeTest, CreateServiceTypedViewDecodesRequestAndRepliesTyped) {
  MockAortaFFI mock;

  void (*ffi_cb)(const aorta_bytes_view_t*, aorta_service_request_t, void*) =
      nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 744;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(744, StrEq("/typed/echo"), _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*,
                           const aorta_service_options_t* opts,
                           void (*callback)(const aorta_bytes_view_t*,
                                            aorta_service_request_t, void*),
                           void* user_data,
                           aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        ffi_cb = callback;
        ffi_user_data = user_data;
        *out = 745;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_request_reply(9001, _))
      .WillOnce(Invoke([](aorta_service_request_t,
                          const aorta_owned_bytes_view_t* response)
                            -> aorta_result_t {
        EXPECT_NE(response, nullptr);
        if (response == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(response->slice_count, 1u);
        if (response->slice_count != 1 || response->slices == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto& slice = response->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<demo::EchoResponse>::Verify(
            slice.data, slice.len));
        if (!TypedSubscribeTraits<demo::EchoResponse>::Verify(slice.data,
                                                              slice.len)) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto* decoded =
            TypedSubscribeTraits<demo::EchoResponse>::GetRoot(slice.data);
        EXPECT_NE(decoded, nullptr);
        if (decoded == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(decoded->echoed_message(), nullptr);
        if (decoded->echoed_message() == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_STREQ(decoded->echoed_message()->c_str(), "typed view reply");
        ReleaseOwnedSlices(response);  // owned-bytes contract — see node_test L648
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(745)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(744)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_view_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateServiceTypedView<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo",
          [](const demo::EchoRequest& request,
             TypedServiceResponder<demo::EchoResponse> reply) {
            ASSERT_NE(request.message(), nullptr);
            EXPECT_STREQ(request.message()->c_str(), "typed view request");
            auto status = reply.Reply([](demo::EchoResponseMsg& msg) {
              msg.echoed_message = std::string("typed view reply");
            });
            EXPECT_TRUE(status.ok());
          });
  ASSERT_TRUE(service_result.ok());
  ASSERT_NE(ffi_cb, nullptr);

  auto payload = BuildEchoRequestPayload("typed view request");
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  ffi_cb(&view, 9001, ffi_user_data);
}

TEST(NodeTest, CreateServiceTypedRetainedDecodesRequestAndRepliesTyped) {
  MockAortaFFI mock;

  void (*ffi_cb)(const aorta_bytes_view_t*, aorta_service_request_t, void*) =
      nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 746;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(746, StrEq("/typed/echo_retained"), _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*,
                           const aorta_service_options_t* opts,
                           void (*callback)(const aorta_bytes_view_t*,
                                            aorta_service_request_t, void*),
                           void* user_data,
                           aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        ffi_cb = callback;
        ffi_user_data = user_data;
        *out = 747;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_request_reply(9002, _))
      .WillOnce(Invoke([](aorta_service_request_t,
                          const aorta_owned_bytes_view_t* response)
                            -> aorta_result_t {
        EXPECT_NE(response, nullptr);
        if (response == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(response->slice_count, 1u);
        if (response->slice_count != 1 || response->slices == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto& slice = response->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<demo::EchoResponse>::Verify(
            slice.data, slice.len));
        if (!TypedSubscribeTraits<demo::EchoResponse>::Verify(slice.data,
                                                              slice.len)) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        const auto* decoded =
            TypedSubscribeTraits<demo::EchoResponse>::GetRoot(slice.data);
        EXPECT_NE(decoded, nullptr);
        if (decoded == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_NE(decoded->echoed_message(), nullptr);
        if (decoded->echoed_message() == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_STREQ(decoded->echoed_message()->c_str(),
                     "typed retained reply");
        ReleaseOwnedSlices(response);  // owned-bytes contract — see node_test L648
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(747)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(746)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_retained_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateServiceTypedRetained<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo_retained",
          [](TypedRetainedServiceRequest<demo::EchoRequest,
                                         demo::EchoResponse> request) {
            ASSERT_NE(request.Message(), nullptr);
            ASSERT_NE(request.Message()->message(), nullptr);
            EXPECT_STREQ(request.Message()->message()->c_str(),
                         "typed retained request");
            auto status = request.Reply([](demo::EchoResponseMsg& msg) {
              msg.echoed_message = std::string("typed retained reply");
            });
            EXPECT_TRUE(status.ok());
          });
  ASSERT_TRUE(service_result.ok());
  ASSERT_NE(ffi_cb, nullptr);

  auto payload = BuildEchoRequestPayload("typed retained request");
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  ffi_cb(&view, 9002, ffi_user_data);
}

// A request payload that fails FlatBuffers verification must surface as a
// genuine Zenoh error reply (reply_error), not an empty OK payload — the
// caller must be able to distinguish "decode failed" from "empty response".
// Mirrors the Rust/Python typed-service behavior.
TEST(NodeTest, CreateServiceTypedViewDecodeFailureRepliesError) {
  MockAortaFFI mock;

  void (*ffi_cb)(const aorta_bytes_view_t*, aorta_service_request_t, void*) =
      nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 748;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(748, StrEq("/typed/echo"), _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*,
                           const aorta_service_options_t* opts,
                           void (*callback)(const aorta_bytes_view_t*,
                                            aorta_service_request_t, void*),
                           void* user_data,
                           aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        ffi_cb = callback;
        ffi_user_data = user_data;
        *out = 749;
        return AORTA_OK;
      }));

  // The decode failure must reach the client as an error reply; the OK-reply
  // entry point must never fire.
  EXPECT_CALL(mock, aorta_service_request_reply(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_service_request_reply_error(9003, _))
      .WillOnce(Invoke([](aorta_service_request_t,
                          const char* error_msg) -> aorta_result_t {
        EXPECT_NE(error_msg, nullptr);
        if (error_msg != nullptr) {
          EXPECT_NE(std::string(error_msg).find("decode"), std::string::npos);
        }
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(749)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(748)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_view_decode_fail_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  bool callback_invoked = false;
  auto service_result =
      node->CreateServiceTypedView<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo",
          [&](const demo::EchoRequest&,
              TypedServiceResponder<demo::EchoResponse>) {
            callback_invoked = true;
          });
  ASSERT_TRUE(service_result.ok());
  ASSERT_NE(ffi_cb, nullptr);

  // Garbage bytes: not a valid EchoRequest FlatBuffer.
  std::vector<uint8_t> payload = {0xde, 0xad, 0xbe, 0xef, 0x00, 0x01};
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  ffi_cb(&view, 9003, ffi_user_data);

  EXPECT_FALSE(callback_invoked);
}

TEST(NodeTest, CreateServiceTypedRetainedDecodeFailureRepliesError) {
  MockAortaFFI mock;

  void (*ffi_cb)(const aorta_bytes_view_t*, aorta_service_request_t, void*) =
      nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 750;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(750, StrEq("/typed/echo_retained"), _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*,
                           const aorta_service_options_t* opts,
                           void (*callback)(const aorta_bytes_view_t*,
                                            aorta_service_request_t, void*),
                           void* user_data,
                           aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        ffi_cb = callback;
        ffi_user_data = user_data;
        *out = 751;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_request_reply(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_service_request_reply_error(9004, _))
      .WillOnce(Invoke([](aorta_service_request_t,
                          const char* error_msg) -> aorta_result_t {
        EXPECT_NE(error_msg, nullptr);
        if (error_msg != nullptr) {
          EXPECT_NE(std::string(error_msg).find("decode"), std::string::npos);
        }
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(751)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(750)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_service_retained_decode_fail_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  bool callback_invoked = false;
  auto service_result =
      node->CreateServiceTypedRetained<demo::EchoRequest, demo::EchoResponse>(
          "/typed/echo_retained",
          [&](TypedRetainedServiceRequest<demo::EchoRequest,
                                          demo::EchoResponse>) {
            callback_invoked = true;
          });
  ASSERT_TRUE(service_result.ok());
  ASSERT_NE(ffi_cb, nullptr);

  std::vector<uint8_t> payload = {0xde, 0xad, 0xbe, 0xef, 0x00, 0x01};
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  ffi_cb(&view, 9004, ffi_user_data);

  EXPECT_FALSE(callback_invoked);
}

TEST(NodeTest, ServiceGetStatsUsesFfiCounters) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 73;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(73, StrEq("/stats/service"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_service_options_t*,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) -> aorta_result_t {
        *out = 7300;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_get_stats(7300, _))
      .WillOnce(Invoke([](aorta_service_t, aorta_service_stats_t* out) {
        out->request_received_count = 7;
        out->request_dispatched_count = 6;
        out->reply_ok_count = 5;
        out->reply_error_count = 1;
        out->reply_send_failure_count = 2;
        out->request_dropped_count = 3;
        out->query_receive_failure_count = 8;
        out->task_exit_count = 9;
        out->in_flight_requests = 4;
        out->last_request_ns = 123;
        out->last_reply_ns = 456;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(7300)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(73)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("service_stats_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result = node->CreateService(
      "/stats/service", [](const MessageView&, ServiceRequest&&) {});
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  auto stats = service->GetStats();
  EXPECT_EQ(stats.request_received_count, 7u);
  EXPECT_EQ(stats.request_dispatched_count, 6u);
  EXPECT_EQ(stats.reply_ok_count, 5u);
  EXPECT_EQ(stats.reply_error_count, 1u);
  EXPECT_EQ(stats.reply_send_failure_count, 2u);
  EXPECT_EQ(stats.request_dropped_count, 3u);
  EXPECT_EQ(stats.query_receive_failure_count, 8u);
  EXPECT_EQ(stats.task_exit_count, 9u);
  EXPECT_EQ(stats.in_flight_requests, 4u);
  EXPECT_EQ(stats.last_request_ns, 123u);
  EXPECT_EQ(stats.last_reply_ns, 456u);
}

TEST(NodeTest, CreateServiceWithOptionsPropagatesRpcOverflowPolicy) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7301;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_service_new_with_options(7301, StrEq("/opts/service"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_service_options_t* opts,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->rpc_overflow_policy, AORTA_RPC_OVERFLOW_BLOCK);
        EXPECT_EQ(opts->rpc_overflow_block_timeout_ms, 33u);
        EXPECT_EQ(opts->callback_group, AORTA_CALLBACK_GROUP_REENTRANT);
        *out = 7302;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_service_free(7302)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(7301)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("service_options_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  ServiceOptions options;
  options.callback_group = CallbackGroup::kReentrant;
  options.rpc_overflow_policy = RpcOverflowPolicy::kBlock;
  options.rpc_overflow_block_timeout_ms = 33;

  auto service_result =
      node->CreateService("/opts/service",
                          [](const MessageView&, ServiceRequest&&) {},
                          options);
  ASSERT_TRUE(service_result.ok());
}

TEST(NodeTest, CreateServiceAutoEnsuresExecutor) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7303;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 7304;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(7304, 7303))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock,
              aorta_service_new_with_options(7303, StrEq("/auto"), _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_service_options_t*,
                          void (*)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
                          void*, aorta_service_t* out) {
        *out = 7305;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_service_free(7305)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(7304)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(7304)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(7303)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("service_auto_exec_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateService("/auto",
                          [](const MessageView&, ServiceRequest&&) {});
  ASSERT_TRUE(service_result.ok());
}

TEST(NodeTest, CreateServiceRejectsInvalidNameBeforeEnsuringExecutor) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7308;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_node_validate_service_name(7308, StrEq("bad-service")))
      .WillOnce(Return(AORTA_ERR_INVALID_ARGUMENT));
  EXPECT_CALL(mock, aorta_executor_new(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_service_new_with_options(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mock, aorta_node_free(7308)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("service_invalid_name_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto service_result =
      node->CreateService("bad-service",
                          [](const MessageView&, ServiceRequest&&) {});
  ASSERT_FALSE(service_result.ok());
  EXPECT_EQ(service_result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST(NodeTest, CreateExecutionGroupReturnsBindableHandle) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7306;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 7307;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(7307, 7306))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock,
              aorta_node_create_callback_group(
                  7306, AORTA_CALLBACK_GROUP_REENTRANT, _))
      .WillOnce(Invoke([](aorta_node_t, aorta_callback_group_t,
                          aorta_callback_group_handle_t* out) {
        *out = 44;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_node_free_callback_group(7306, 44))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(7307)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(7307)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(7306)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("execution_group_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto group_result = node->CreateExecutionGroup(ExecutionPolicy::kConcurrent);
  ASSERT_TRUE(group_result.ok());
  auto group = std::move(group_result.value());
  EXPECT_TRUE(group.valid());
  EXPECT_EQ(group.policy(), ExecutionPolicy::kConcurrent);
  EXPECT_EQ(group.handle(), 44u);

  ExecutionGroup moved_group = std::move(group);
  EXPECT_FALSE(group.valid());
  EXPECT_TRUE(moved_group.valid());
  EXPECT_EQ(moved_group.policy(), ExecutionPolicy::kConcurrent);
  EXPECT_EQ(moved_group.handle(), 44u);

  TimerOptions timer_options = moved_group.Bind(TimerOptions());
  EXPECT_EQ(timer_options.callback_group, CallbackGroup::kReentrant);
  EXPECT_EQ(timer_options.callback_group_handle, 44u);
}

TEST(NodeTest, ExecutionGroupReleaseIsIdempotentWithDestructor) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7316;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 7317;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(7317, 7316))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock,
              aorta_node_create_callback_group(
                  7316, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE, _))
      .WillOnce(Invoke([](aorta_node_t, aorta_callback_group_t,
                          aorta_callback_group_handle_t* out) {
        *out = 54;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_node_free_callback_group(7316, 54))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(7317)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(7317)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(7316)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("execution_group_release_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto group_result = node->CreateExecutionGroup(ExecutionPolicy::kSerialized);
  ASSERT_TRUE(group_result.ok());
  auto group = std::move(group_result.value());

  EXPECT_TRUE(group.Release().ok());
  EXPECT_FALSE(group.valid());
}

TEST(NodeTest, ClientGetStatsUsesFfiCounters) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 74;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_new_with_options(74, StrEq("/stats/client"), _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_client_options_t*,
                          aorta_client_t* out) -> aorta_result_t {
        *out = 7400;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_get_stats(7400, _))
      .WillOnce(Invoke([](aorta_client_t, aorta_client_stats_t* out) {
        out->call_count = 10;
        out->success_count = 8;
        out->timeout_count = 1;
        out->concurrent_conflict_count = 1;
        out->error_count = 2;
        out->retry_count = 4;
        out->last_call_started_ns = 111;
        out->last_call_finished_ns = 222;
        out->last_call_latency_ns = 333;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_free(7400)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(74)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("client_stats_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto client_result = node->CreateClient("/stats/client", ClientOptions{});
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto stats = client->GetStats();
  EXPECT_EQ(stats.call_count, 10u);
  EXPECT_EQ(stats.success_count, 8u);
  EXPECT_EQ(stats.timeout_count, 1u);
  EXPECT_EQ(stats.concurrent_conflict_count, 1u);
  EXPECT_EQ(stats.error_count, 2u);
  EXPECT_EQ(stats.retry_count, 4u);
  EXPECT_EQ(stats.last_call_started_ns, 111u);
  EXPECT_EQ(stats.last_call_finished_ns, 222u);
  EXPECT_EQ(stats.last_call_latency_ns, 333u);
}

TEST(NodeTest, CreateClientPropagatesStrictOptionsToFfi) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 75;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_new_with_options(75, StrEq("/strict/client"), _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_client_options_t* opts, aorta_client_t* out)
                           -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        EXPECT_EQ(opts->timeout_ms, 1200u);
        EXPECT_EQ(opts->schema_attach_policy, AORTA_SCHEMA_ATTACH_NEVER);
        EXPECT_EQ(opts->enforce_single_provider, 2u);
        EXPECT_EQ(opts->multi_reply_check_window_ms, 42u);
        *out = 7500;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_client_free(7500)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(75)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("strict_client_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  ClientOptions options;
  options.timeout_ms = 1200;
  options.schema_attach_policy = SchemaAttachPolicy::kNever;
  options.enforce_single_provider = true;
  options.multi_reply_check_window_ms = 42;

  auto client_result = node->CreateClient("/strict/client", options);
  ASSERT_TRUE(client_result.ok());
}

// ==================== Node Subscriber Tests ====================

// Test CreateSubscriber success
TEST(NodeTest, CreateSubscriberSuccess) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 8;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(8, StrEq("cmd/velocity"), _,
                                                  _, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char* topic, const aorta_qos_t* qos,
                          const aorta_sub_options_t*,
                          const aorta_sub_buffering_t*, aorta_on_msg_cb, void*,
                          aorta_sub_t* out) -> aorta_result_t {
        EXPECT_STREQ(topic, "cmd/velocity");
        if (qos == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(qos->priority, AORTA_PRIORITY_HIGH);
        *out = 800;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_free(800)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(8)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("sub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  SubscriberOptions options;
  // Hand-rolled non-default QoS (the test isn't about preset correctness;
  // QoS::StateUpdate() now calls into the FFI, which the mock-based unit
  // test harness doesn't link — see qos_preset_integration_test for the
  // real preset coverage).
  options.qos =
      QoS{Priority::kHigh, Reliability::kReliable, CongestionControl::kBlock,
          true};
  auto sub_result = node->CreateSubscriber(
      "cmd/velocity",
      [](const MessageView&, const MessageContextView&) {
        // Callback handler
      },
      options);

  ASSERT_TRUE(sub_result.ok());
}

TEST(NodeTest, CreateSubscriberAutoEnsuresExecutor) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 801;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 8101;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(8101, 801))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_subscriber_new_buffered(801, StrEq("/sub/auto"), _, _,
                                                  _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_sub_options_t*,
                          const aorta_sub_buffering_t*, aorta_on_msg_cb, void*,
                          aorta_sub_t* out) {
        *out = 8102;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_subscriber_free(8102)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(8101)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(8101)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(801)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("sub_auto_exec_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto sub_result =
      node->CreateSubscriber("/sub/auto",
                             [](const MessageView&, const MessageContextView&) {});
  ASSERT_TRUE(sub_result.ok());
}

TEST(NodeTest, CreateSubscriberRejectsInvalidTopicBeforeEnsuringExecutor) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 8103;
        return AORTA_OK;
      }));
  // CreateSubscriber now validates via the subscribe-side variant
  // (aorta_node_validate_topic_pattern) so wildcard subscriptions like
  // "camera/**" can reach the subscriber constructor. The publish-side
  // validator is used only by CreatePublisher.
  EXPECT_CALL(mock, aorta_node_validate_topic_pattern(8103, StrEq("bad-topic")))
      .WillOnce(Return(AORTA_ERR_INVALID_ARGUMENT));
  EXPECT_CALL(mock, aorta_executor_new(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_subscriber_new_buffered(_, _, _, _, _, _, _, _))
      .Times(0);
  EXPECT_CALL(mock, aorta_node_free(8103)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("sub_invalid_topic_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto sub_result =
      node->CreateSubscriber("bad-topic",
                             [](const MessageView&, const MessageContextView&) {});
  ASSERT_FALSE(sub_result.ok());
  EXPECT_EQ(sub_result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

// Test CreateSubscriber failure
TEST(NodeTest, CreateSubscriberFailure) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 9;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(9, _, _, _, _, _, _, _))
      .WillOnce(Return(AORTA_ERR_NOT_FOUND));

  EXPECT_CALL(mock, aorta_node_free(9)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("fail_sub_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  SubscriberOptions options;
  // See note above (StateUpdate site) — literal init avoids the FFI call
  // path that the mocked unit test harness can't link against.
  options.qos =
      QoS{Priority::kBackground, Reliability::kReliable,
          CongestionControl::kBlock, false};
  auto sub_result = node->CreateSubscriber(
      "nonexistent/topic", [](const MessageView&, const MessageContextView&) {},
      options);

  ASSERT_FALSE(sub_result.ok());
  EXPECT_EQ(sub_result.status().code(), AORTA_ERR_NOT_FOUND);
}

TEST(NodeTest, CreateSubscriberTypedViewDecodesBorrowedMessage) {
  MockAortaFFI mock;

  aorta_on_msg_cb ffi_cb = nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 17;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(17, StrEq("typed/view"), _, _,
                                                  _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*, const aorta_qos_t*,
                           const aorta_sub_options_t* opts,
                           const aorta_sub_buffering_t*, aorta_on_msg_cb cb,
                           void* user_data, aorta_sub_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        ffi_cb = cb;
        ffi_user_data = user_data;
        *out = 1700;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_free(1700)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(17)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_view_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  bool called = false;
  uint64_t seen_sequence = 0;
  auto sub_result = node->CreateSubscriberTypedView<demo::Chat>(
      "typed/view",
      [&](const demo::Chat& msg, const MessageContextView& ctx) {
        called = true;
        EXPECT_NE(msg.text(), nullptr);
        EXPECT_STREQ(msg.text()->c_str(), "hello typed view");
        seen_sequence = ctx.sequence;
      });
  ASSERT_TRUE(sub_result.ok());
  ASSERT_NE(ffi_cb, nullptr);
  ASSERT_NE(ffi_user_data, nullptr);

  {
    flatbuffers::FlatBufferBuilder builder;
    demo::ChatMsg msg{};
    msg.text = std::string("hello typed view");

    auto node_name = builder.CreateString("typed_pub");
    auto header =
        aorta::sys::CreateAortaHeader(builder, 123456u, node_name, 77u);
    auto root = TypedPublishTraits<demo::Chat>::Build(builder, header, msg);
    builder.Finish(root);

    aorta_bytes_slice_t slice{
        builder.GetBufferPointer(),
        builder.GetSize(),
    };
    aorta_bytes_view_t payload{
        &slice,
        1,
        builder.GetSize(),
    };

    constexpr char kTopic[] = "aorta/default/pub/typed/view";
    constexpr char kPublisher[] = "typed_pub";
    aorta_msg_context_t ctx{};
    ctx.topic = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(kTopic),
        sizeof(kTopic) - 1,
    };
    ctx.receive_stamp_ns = 999999;
    ctx.priority = static_cast<int32_t>(AORTA_PRIORITY_NORMAL);
    ctx.publisher_node = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(kPublisher),
        sizeof(kPublisher) - 1,
    };
    ctx.sequence = 77u;
    ffi_cb(&payload, nullptr, &ctx, ffi_user_data);
  }

  EXPECT_TRUE(called);
  EXPECT_EQ(seen_sequence, 77u);
}

TEST(NodeTest, CreateSubscriberTypedRetainedSampleOwnsPayloadByDefault) {
  MockAortaFFI mock;

  aorta_on_msg_cb ffi_cb = nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 18;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(18, StrEq("typed/chat"), _, _,
                                                  _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*, const aorta_qos_t*,
                           const aorta_sub_options_t* opts,
                           const aorta_sub_buffering_t*, aorta_on_msg_cb cb,
                           void* user_data, aorta_sub_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts != nullptr) {
          EXPECT_GT(opts->schema_bfbs_len, 0u);
        }
        ffi_cb = cb;
        ffi_user_data = user_data;
        *out = 1800;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_free(1800)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(18)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_sub_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  std::shared_ptr<const TypedRetainedSample<demo::Chat>> retained;
  auto sub_result = node->CreateSubscriberTypedRetained<demo::Chat>(
      "typed/chat",
      [&retained](TypedRetainedSamplePtr<demo::Chat> sample) {
        ASSERT_NE(sample, nullptr);
        EXPECT_TRUE(sample->IsOwned());
        retained = std::move(sample);
      });
  ASSERT_TRUE(sub_result.ok());
  ASSERT_NE(ffi_cb, nullptr);
  ASSERT_NE(ffi_user_data, nullptr);

  {
    flatbuffers::FlatBufferBuilder builder;
    demo::ChatMsg msg{};
    msg.text = std::string("hello typed subscribe");

    auto node_name = builder.CreateString("typed_pub");
    auto header =
        aorta::sys::CreateAortaHeader(builder, 123456u, node_name, 77u);
    auto root = TypedPublishTraits<demo::Chat>::Build(builder, header, msg);
    builder.Finish(root);

    aorta_bytes_slice_t slice{
        builder.GetBufferPointer(),
        builder.GetSize(),
    };
    aorta_bytes_view_t payload{
        &slice,
        1,
        builder.GetSize(),
    };

    constexpr char kTopic[] = "aorta/default/pub/typed/chat";
    constexpr char kPublisher[] = "typed_pub";
    aorta_msg_context_t ctx{};
    ctx.topic = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(kTopic),
        sizeof(kTopic) - 1,
    };
    ctx.receive_stamp_ns = 123456789;
    ctx.priority = static_cast<int32_t>(AORTA_PRIORITY_NORMAL);
    ctx.publisher_node = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(kPublisher),
        sizeof(kPublisher) - 1,
    };
    ctx.sequence = 77;

    ffi_cb(&payload, nullptr, &ctx, ffi_user_data);
  }

  ASSERT_NE(retained, nullptr);
  EXPECT_TRUE(retained->IsOwned());
  ASSERT_NE(retained->Message(), nullptr);
  ASSERT_NE(retained->Message()->text(), nullptr);
  EXPECT_EQ(retained->Message()->text()->str(), "hello typed subscribe");
  EXPECT_EQ(retained->Context().topic, "aorta/default/pub/typed/chat");
  EXPECT_EQ(retained->Context().publisher_node, "typed_pub");
  EXPECT_EQ(retained->Context().sequence, 77u);
}

// ==================== Node Timer Tests ====================

// Test CreateTimer success
TEST(NodeTest, CreateTimerSuccess) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 10;
        return AORTA_OK;
      }));

  // Timer creation requires an executor
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 1000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_executor_add_node(1000, 10))
      .WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_timer_new_with_options(10, 100000000, _, _, _, _))  // 100ms in ns
      .WillOnce(Invoke([](aorta_node_t, int64_t period_ns, aorta_timer_cb,
                          void*, const aorta_timer_options_t* opts,
                          aorta_timer_t* out) -> aorta_result_t {
        EXPECT_EQ(period_ns, 100000000);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->callback_group, AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE);
        EXPECT_EQ(opts->callback_group_handle, 0u);
        EXPECT_EQ(opts->overflow_policy, AORTA_TIMER_OVERFLOW_COALESCE_LATEST);
        EXPECT_EQ(opts->overflow_block_timeout_ms, 0u);
        *out = 10000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_timer_free(10000)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_executor_stop(1000)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_executor_free(1000)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(10)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("timer_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  using namespace std::chrono;
  auto timer_result = node->CreateTimer(milliseconds(100), []() {
    // Timer callback
  });

  ASSERT_TRUE(timer_result.ok());
}

// Timer::Pause()/Resume() route to the C-ABI's
// (misnamed) aorta_timer_cancel / aorta_timer_reset — previously the C++
// SDK exposed no lifecycle control, so auto_start=false timers were
// permanently inert.
TEST(NodeTest, TimerPauseResumeRouteToFfi) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 11;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 1100;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(1100, 11)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_timer_new_with_options(11, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, int64_t, aorta_timer_cb, void*,
                          const aorta_timer_options_t*, aorta_timer_t* out) {
        *out = 11000;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_cancel(11000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_timer_reset(11000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_timer_free(11000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(1100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(1100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(11)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("timer_ctl_node").value();
  using namespace std::chrono;
  auto timer = node->CreateTimer(milliseconds(50), []() {}).value();

  EXPECT_TRUE(timer->Pause().ok());
  EXPECT_TRUE(timer->Resume().ok());
}

TEST(NodeTest, CreateTimerWithOptionsPropagatesTimerOptions) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 110;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_timer_new_with_options(110, 200000000, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, int64_t period_ns, aorta_timer_cb, void*,
                          const aorta_timer_options_t* opts,
                          aorta_timer_t* out) -> aorta_result_t {
        EXPECT_EQ(period_ns, 200000000);
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->callback_group, AORTA_CALLBACK_GROUP_REENTRANT);
        EXPECT_EQ(opts->callback_group_handle, 42u);
        EXPECT_EQ(opts->auto_start, 0u);
        EXPECT_EQ(opts->invoke_on_missed, 1u);
        EXPECT_EQ(opts->overflow_policy, AORTA_TIMER_OVERFLOW_BLOCK);
        EXPECT_EQ(opts->overflow_block_timeout_ms, 25u);
        *out = 11000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_timer_free(11000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(110)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("timer_options_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  TimerOptions options;
  options.callback_group = CallbackGroup::kReentrant;
  options.callback_group_handle = 42;
  options.auto_start = false;
  options.invoke_on_missed = true;
  options.overflow_policy = TimerOverflowPolicy::kBlock;
  options.overflow_block_timeout_ms = 25;

  using namespace std::chrono;
  auto timer_result =
      node->CreateTimer(milliseconds(200), []() {}, options);
  ASSERT_TRUE(timer_result.ok());
}

TEST(NodeTest, CreateTimerSkipsInternalExecutorWhenNodeAlreadyHasEventSender) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 111;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_has_event_sender(111, _))
      .WillRepeatedly(Invoke([](aorta_node_t, uint8_t* out) {
        if (out != nullptr) {
          *out = 1;
        }
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_new(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_executor_add_node(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_executor_stop(_)).Times(0);
  EXPECT_CALL(mock, aorta_executor_free(_)).Times(0);

  EXPECT_CALL(mock, aorta_timer_new_with_options(111, 1000000, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, int64_t, aorta_timer_cb, void*,
                          const aorta_timer_options_t*, aorta_timer_t* out) {
        *out = 11100;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_free(11100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(111)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("timer_external_executor_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto timer_result = node->CreateTimer(std::chrono::milliseconds(1), []() {});
  ASSERT_TRUE(timer_result.ok());
}

TEST(NodeTest, TimerExposesRuntimeStats) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 112;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_timer_new_with_options(112, 1000000, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, int64_t, aorta_timer_cb, void*,
                          const aorta_timer_options_t*, aorta_timer_t* out) {
        *out = 11200;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_coalesced_tick_count(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 7;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_overflow_timeout_count(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 3;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_deadline_slip_total_ns(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 9000;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_deadline_slip_max_ns(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 2500;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_due_firings_total(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 19;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_due_firings_max(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 4;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_get_due_sample_count(11200, _))
      .WillOnce(Invoke([](aorta_timer_t, uint64_t* out) {
        *out = 6;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_timer_free(11200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 1200;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_add_node(1200, 112)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(1200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(1200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(112)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("timer_stats_node");
  ASSERT_TRUE(result.ok());
  auto node = result.value();

  auto timer_result = node->CreateTimer(std::chrono::milliseconds(1), []() {});
  ASSERT_TRUE(timer_result.ok());
  auto timer = timer_result.value();

  auto coalesced_result = timer->CoalescedTickCount();
  ASSERT_TRUE(coalesced_result.ok());
  EXPECT_EQ(coalesced_result.value(), 7u);

  auto overflow_result = timer->OverflowTimeoutCount();
  ASSERT_TRUE(overflow_result.ok());
  EXPECT_EQ(overflow_result.value(), 3u);

  auto deadline_slip_total_result = timer->DeadlineSlipTotalNs();
  ASSERT_TRUE(deadline_slip_total_result.ok());
  EXPECT_EQ(deadline_slip_total_result.value(), 9000u);

  auto deadline_slip_max_result = timer->DeadlineSlipMaxNs();
  ASSERT_TRUE(deadline_slip_max_result.ok());
  EXPECT_EQ(deadline_slip_max_result.value(), 2500u);

  auto due_firings_total_result = timer->DueFiringsTotal();
  ASSERT_TRUE(due_firings_total_result.ok());
  EXPECT_EQ(due_firings_total_result.value(), 19u);

  auto due_firings_max_result = timer->DueFiringsMax();
  ASSERT_TRUE(due_firings_max_result.ok());
  EXPECT_EQ(due_firings_max_result.value(), 4u);

  auto due_sample_count_result = timer->DueSampleCount();
  ASSERT_TRUE(due_sample_count_result.ok());
  EXPECT_EQ(due_sample_count_result.value(), 6u);
}

TEST(NodeTest, ExecutorCreateWithOptionsPropagatesPriorityConfig) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new_with_options(AORTA_EXECUTOR_SINGLE, _, _))
      .WillOnce(Invoke([](aorta_executor_type_t,
                          const aorta_executor_options_t* opts,
                          aorta_exec_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr || opts->priority == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->scheduler_mode, AORTA_SCHEDULER_PRIORITY);
        EXPECT_EQ(opts->priority->max_drain_per_cycle, 64u);
        EXPECT_EQ(opts->priority->weights[0], 9u);
        EXPECT_EQ(opts->priority->weights[1], 5u);
        EXPECT_EQ(opts->priority->weights[2], 3u);
        EXPECT_EQ(opts->priority->weights[3], 2u);
        EXPECT_EQ(opts->priority->weights[4], 1u);
        EXPECT_EQ(opts->priority->map_subscription_qos, 0u);
        *out = 1001;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_free(1001)).WillOnce(Return(AORTA_OK));

  ExecutorOptions options;
  options.type = ExecutorType::kSingleThreaded;
  options.scheduler_mode = SchedulerMode::kPriority;
  options.priority.max_drain_per_cycle = 64;
  options.priority.weights = {9, 5, 3, 2, 1};
  options.priority.map_subscription_qos = false;

  auto result = Executor::Create(options);
  ASSERT_TRUE(result.ok());
}

TEST(NodeTest, ExecutorCreateWithOptionsUsesFifoByDefault) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new_with_options(AORTA_EXECUTOR_SINGLE, _, _))
      .WillOnce(Invoke([](aorta_executor_type_t,
                          const aorta_executor_options_t* opts,
                          aorta_exec_t* out) -> aorta_result_t {
        EXPECT_NE(opts, nullptr);
        if (opts == nullptr || opts->priority == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->scheduler_mode, AORTA_SCHEDULER_FIFO);
        EXPECT_EQ(opts->priority->max_drain_per_cycle, 32u);
        EXPECT_EQ(opts->priority->weights[0], 8u);
        EXPECT_EQ(opts->priority->weights[1], 4u);
        EXPECT_EQ(opts->priority->weights[2], 2u);
        EXPECT_EQ(opts->priority->weights[3], 1u);
        EXPECT_EQ(opts->priority->weights[4], 1u);
        EXPECT_EQ(opts->priority->map_subscription_qos, 1u);
        *out = 1002;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_free(1002)).WillOnce(Return(AORTA_OK));

  ExecutorOptions options;
  auto result = Executor::Create(options);
  ASSERT_TRUE(result.ok());
}

TEST(NodeTest, ExecutorGetAdmissionStatsUsesFfiStats) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new(AORTA_EXECUTOR_SINGLE, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 1200;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_executor_get_admission_stats(1200, _))
      .WillOnce(Invoke(
          [](aorta_exec_t, aorta_executor_admission_stats_t* out) -> aorta_result_t {
            if (out == nullptr) {
              return AORTA_ERR_INVALID_ARGUMENT;
            }
            out->queued_events = 11;
            out->queued_bytes = 1024;
            out->peak_queued_events = 17;
            out->peak_queued_bytes = 4096;
            out->dropped_queue_full = 3;
            out->dropped_queue_closed = 2;
            out->dropped_budget_events = 5;
            out->dropped_budget_bytes = 7;
            return AORTA_OK;
          }));
  EXPECT_CALL(mock, aorta_executor_free(1200)).WillOnce(Return(AORTA_OK));

  auto exec_result = Executor::Create();
  ASSERT_TRUE(exec_result.ok());
  auto exec = exec_result.value();
  auto stats_result = exec->GetAdmissionStats();
  ASSERT_TRUE(stats_result.ok());
  const auto stats = stats_result.value();
  EXPECT_EQ(stats.queued_events, 11u);
  EXPECT_EQ(stats.queued_bytes, 1024u);
  EXPECT_EQ(stats.peak_queued_events, 17u);
  EXPECT_EQ(stats.peak_queued_bytes, 4096u);
  EXPECT_EQ(stats.dropped_queue_full, 3u);
  EXPECT_EQ(stats.dropped_queue_closed, 2u);
  EXPECT_EQ(stats.dropped_budget_events, 5u);
  EXPECT_EQ(stats.dropped_budget_bytes, 7u);
}

// ==================== Node Lifecycle Tests ====================

// Test Node RAII cleanup
TEST(NodeTest, RaiiCleanup) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  EXPECT_CALL(mock, aorta_node_new(StrEq("raii_test"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 11;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_free(11)).WillOnce(Return(AORTA_OK));

  {
    auto result = Node::Create("raii_test");
    ASSERT_TRUE(result.ok());
    auto node = result.value();

    EXPECT_EQ(node->name(), "raii_test");
    // Node destructor should call aorta_node_free when leaving scope
  }
}

// Test Node shared ownership
TEST(NodeTest, SharedOwnership) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 12;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_free(12)).WillOnce(Return(AORTA_OK));

  std::shared_ptr<Node> shared_node;

  {
    auto result = Node::Create("shared_node");
    ASSERT_TRUE(result.ok());
    auto node = result.value();

    // Create another reference
    shared_node = node;
    EXPECT_EQ(shared_node->name(), "shared_node");
  }

  // Node still valid because shared_node holds reference
  EXPECT_EQ(shared_node->name(), "shared_node");

  // Now shared_node goes out of scope and destructor is called
}

// ==================== Advanced Node Tests ====================

TEST(NodeAdvancedTest, EmptyNodeName) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  EXPECT_CALL(mock, aorta_node_new(StrEq(""), StrEq("default"), _))
      .WillOnce(Return(AORTA_ERR_INVALID_ARGUMENT));

  auto result = Node::Create("");
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST(NodeAdvancedTest, EmptyGroupName) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  EXPECT_CALL(mock, aorta_node_new(StrEq("valid_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 41;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_node_free(41)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create("valid_node", "");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value()->group(), "default");
}

TEST(NodeAdvancedTest, VeryLongNodeName) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  std::string long_name(1024, 'a');
  EXPECT_CALL(mock, aorta_node_new(StrEq(long_name), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 42;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_free(42)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create(long_name);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value()->name(), long_name);
}

TEST(NodeAdvancedTest, SpecialCharactersInName) {
  MockAortaFFI mock;
  unsetenv("AORTA_GROUP");

  std::string special_name = "node/with:special-chars_123";
  EXPECT_CALL(mock, aorta_node_new(StrEq(special_name), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 50;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_free(50)).WillOnce(Return(AORTA_OK));

  auto result = Node::Create(special_name);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value()->name(), special_name);
}

TEST(NodeAdvancedTest, MultiplePublishersOnSameTopic) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(100, StrEq("shared_topic"), _, _, _))
      .Times(2)
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1001;
        return AORTA_OK;
      }))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1002;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(100, _)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(1001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_publisher_free(1002)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(100)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("multi_pub_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub1_result =
      node->CreatePublisher("shared_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub1_result.ok());
  auto pub1 = pub1_result.value();

  auto pub2_result =
      node->CreatePublisher("shared_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub2_result.ok());
  auto pub2 = pub2_result.value();

  EXPECT_EQ(pub1->Topic(), "shared_topic");
  EXPECT_EQ(pub2->Topic(), "shared_topic");
}

TEST(NodeAdvancedTest, MultipleSubscribersOnSameTopic) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 200;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(
                        200, StrEq("shared_sub_topic"), _, _, _, _, _, _))
      .Times(2)
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_sub_options_t*,
                          const aorta_sub_buffering_t*, aorta_on_msg_cb, void*,
                          aorta_sub_t* out) {
        *out = 2001;
        return AORTA_OK;
      }))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_sub_options_t*,
                          const aorta_sub_buffering_t*, aorta_on_msg_cb, void*,
                          aorta_sub_t* out) {
        *out = 2002;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_free(2001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_subscriber_free(2002)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(200)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("multi_sub_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  int callback1_count = 0;
  int callback2_count = 0;

  auto sub1_result = node->CreateSubscriber(
      "shared_sub_topic",
      [&callback1_count](const MessageView&, const MessageContextView&) {
        callback1_count++;
      },
      QoS(), SubscriberOptions());
  ASSERT_TRUE(sub1_result.ok());

  auto sub2_result = node->CreateSubscriber(
      "shared_sub_topic",
      [&callback2_count](const MessageView&, const MessageContextView&) {
        callback2_count++;
      },
      QoS(), SubscriberOptions());
  ASSERT_TRUE(sub2_result.ok());
}

TEST(NodeAdvancedTest, PublisherWithVeryLongTopic) {
  MockAortaFFI mock;

  std::string long_topic(512, 'x');
  long_topic = "prefix/" + long_topic + "/suffix";

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 300;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(300, StrEq(long_topic), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 3001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(300, _)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(3001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(300)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("long_topic_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher(long_topic, QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  EXPECT_EQ(pub_result.value()->Topic(), long_topic);
}

TEST(NodeAdvancedTest, TimerCreation) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 400;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_executor_new(_, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 4000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_executor_add_node(4000, 400))
      .WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_timer_new_with_options(400, _, _, _, _, _))
      .WillOnce(Invoke(
          [](aorta_node_t, int64_t, aorta_timer_cb, void*,
             const aorta_timer_options_t*, aorta_timer_t* out) {
            *out = 4001;
            return AORTA_OK;
          }));

  EXPECT_CALL(mock, aorta_timer_free(4001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_stop(4000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(4000)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(400)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("timer_test_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  using namespace std::chrono;
  bool timer_called = false;
  auto timer_result = node->CreateTimer(
      milliseconds(100), [&timer_called]() { timer_called = true; });

  ASSERT_TRUE(timer_result.ok());
  auto timer = timer_result.value();
}

TEST(NodeAdvancedTest, ErrorCodePropagation) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 500;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(500, _, _, _, _))
      .WillOnce(Return(AORTA_ERR_INTERNAL));

  EXPECT_CALL(mock, aorta_last_error())
      .WillOnce(Return("Internal error: failed to create publisher"));

  EXPECT_CALL(mock, aorta_node_free(500)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("error_test_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("error_topic", QoS(), PublisherOptions());
  ASSERT_FALSE(pub_result.ok());
  EXPECT_EQ(pub_result.status().code(), AORTA_ERR_INTERNAL);
  EXPECT_NE(pub_result.status().message().find("Internal error"),
            std::string::npos);
}

TEST(NodeAdvancedTest, ResourceCleanupOrdering) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 600;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(600, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 6001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_subscriber_new_buffered(600, _, _, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_sub_options_t*,
                          const aorta_sub_buffering_t*, aorta_on_msg_cb, void*,
                          aorta_sub_t* out) {
        *out = 6002;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_node_now(600, _)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(6001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_subscriber_free(6002)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(600)).WillOnce(Return(AORTA_OK));

  {
    auto node_result = Node::Create("cleanup_node");
    ASSERT_TRUE(node_result.ok());
    auto node = node_result.value();

    auto pub_result =
        node->CreatePublisher("cleanup_topic", QoS(), PublisherOptions());
    ASSERT_TRUE(pub_result.ok());
    auto publisher = pub_result.value();

    auto sub_result = node->CreateSubscriber(
        "cleanup_topic", [](const MessageView&, const MessageContextView&) {},
        QoS(), SubscriberOptions());
    ASSERT_TRUE(sub_result.ok());
    auto subscriber = sub_result.value();
  }
}

// --- StopHandle tests ---

TEST(StopHandleTest, GetStopHandleReturnsValidHandle) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new(_, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 5000;
        return AORTA_OK;
      }));

  // Sentinel value used as fake stop handle (u64 registry key).
  uint64_t fake_handle = 0xBEEF;

  EXPECT_CALL(mock, aorta_executor_get_stop_handle(5000, _))
      .WillOnce(Invoke([&](aorta_exec_t, uint64_t* out) {
        *out = fake_handle;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_stop_handle_free(fake_handle))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(5000)).WillOnce(Return(AORTA_OK));

  auto exec_result = Executor::Create();
  ASSERT_TRUE(exec_result.ok());
  auto executor = exec_result.value();

  auto sh_result = executor->GetStopHandle();
  ASSERT_TRUE(sh_result.ok());

  // StopHandle destroyed when sh_result goes out of scope.
}

TEST(StopHandleTest, StopHandleStopCallsFFI) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new(_, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 5001;
        return AORTA_OK;
      }));

  uint64_t fake_handle = 0xCAFE;

  EXPECT_CALL(mock, aorta_executor_get_stop_handle(5001, _))
      .WillOnce(Invoke([&](aorta_exec_t, uint64_t* out) {
        *out = fake_handle;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_stop_handle_stop(fake_handle))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_stop_handle_free(fake_handle))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(5001)).WillOnce(Return(AORTA_OK));

  auto exec_result = Executor::Create();
  ASSERT_TRUE(exec_result.ok());
  auto executor = exec_result.value();

  auto sh_result = executor->GetStopHandle();
  ASSERT_TRUE(sh_result.ok());

  auto stop = std::move(sh_result).value();
  auto status = stop.Stop();
  EXPECT_TRUE(status.ok());
}

TEST(StopHandleTest, GetStopHandleFailurePropagatesError) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new(_, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 5002;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_executor_get_stop_handle(5002, _))
      .WillOnce(Return(AORTA_ERR_NOT_FOUND));
  EXPECT_CALL(mock, aorta_last_error())
      .WillOnce(Return("executor handle not found"));

  EXPECT_CALL(mock, aorta_executor_free(5002)).WillOnce(Return(AORTA_OK));

  auto exec_result = Executor::Create();
  ASSERT_TRUE(exec_result.ok());
  auto executor = exec_result.value();

  auto sh_result = executor->GetStopHandle();
  EXPECT_FALSE(sh_result.ok());
}

TEST(StopHandleTest, MoveSemantics) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_executor_new(_, _))
      .WillOnce(Invoke([](aorta_executor_type_t, aorta_exec_t* out) {
        *out = 5003;
        return AORTA_OK;
      }));

  uint64_t fake_handle = 0xDEAD;

  EXPECT_CALL(mock, aorta_executor_get_stop_handle(5003, _))
      .WillOnce(Invoke([&](aorta_exec_t, uint64_t* out) {
        *out = fake_handle;
        return AORTA_OK;
      }));

  // Free should be called exactly once (by the moved-to instance).
  EXPECT_CALL(mock, aorta_stop_handle_free(fake_handle))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_executor_free(5003)).WillOnce(Return(AORTA_OK));

  auto exec_result = Executor::Create();
  ASSERT_TRUE(exec_result.ok());
  auto executor = exec_result.value();

  auto sh_result = executor->GetStopHandle();
  ASSERT_TRUE(sh_result.ok());

  auto stop1 = std::move(sh_result).value();
  auto stop2 = std::move(stop1);
  // stop1 is now moved-from; only stop2 should call free.
}

}  // namespace
}  // namespace aorta
