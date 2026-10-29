// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Publisher (RecyclePool owned path, typed, stats, lifecycle)

#include "aorta/publisher.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "aorta/node.h"
#include "aorta/options.h"
#include "aorta/qos.h"
#include "aorta/status.h"
#include "all_types_msg.h"
#include "all_types_cpp_vector_union_msg.h"
#include "demo_chat_msg.h"

// Include mock for FFI functions
#include "mock_aorta.h"

namespace aorta {
namespace {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

// gmock action: honor the owned-bytes contract — release the buffer (via the
// shared ReleaseOwnedSlices in mock_aorta.h) then return `rc`. Replaces a bare
// `Return(rc)` on `publish_owned`, which leaks the Acquired RawPublishCtx (the
// LSAN tier catches it; production's real core always releases, on success and
// error alike). Tests that capture the release callback to drive the recycle
// path by hand call it themselves and don't use this.
auto PublishOwnedReleasing(aorta_result_t rc) {
  return [rc](aorta_pub_t, const aorta_owned_bytes_view_t* view)
             -> aorta_result_t {
    ReleaseOwnedSlices(view);
    return rc;
  };
}

class EnvVarGuard {
 public:
  EnvVarGuard(const char* name, const char* value) : name_(name) {
    const char* existing = std::getenv(name_);
    if (existing != nullptr) {
      had_old_ = true;
      old_value_ = existing;
    }
    setenv(name_, value, 1);
  }

  ~EnvVarGuard() {
    if (had_old_) {
      setenv(name_, old_value_.c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }

 private:
  const char* name_;
  bool had_old_{false};
  std::string old_value_;
};

// ==================== Publisher Typed Tests ====================

TEST(PublisherTypedTest, PublishTypedBuildsAndPublishes) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 11;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(11, StrEq("typed_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t* opts, aorta_pub_t* out) {
        EXPECT_NE(opts, nullptr);
        if (opts != nullptr) {
          EXPECT_NE(opts->schema_root_type, nullptr);
          if (opts->schema_root_type != nullptr) {
            EXPECT_STREQ(opts->schema_root_type, "demo.Chat");
          }
        }
        *out = 1100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 1234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1100, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_OK)));

  EXPECT_CALL(mock, aorta_publisher_free(1100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(11)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::Chat>("typed_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string("hello typed");
  });
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(publisher->GetStats().publish_count, 1u);
}

TEST(PublisherTypedTest, PublishTypedSupportsAllTypesSchema) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_all_types_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 12;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(12, StrEq("typed_all_types_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1200;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 2234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1200, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_OK)));

  EXPECT_CALL(mock, aorta_publisher_free(1200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(12)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_all_types_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::AllTypes>("typed_all_types_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::AllTypes>([](demo::AllTypesMsg& msg) {
    msg.flag = true;
    msg.val_int8 = static_cast<int8_t>(-8);
    msg.val_uint8 = static_cast<uint8_t>(8);
    msg.val_int16 = static_cast<int16_t>(-16);
    msg.val_uint16 = static_cast<uint16_t>(16);
    msg.val_int32 = static_cast<int32_t>(-32);
    msg.val_uint32 = static_cast<uint32_t>(32);
    msg.val_int64 = static_cast<int64_t>(-64);
    msg.val_uint64 = static_cast<uint64_t>(64);
    msg.val_float32 = 1.25f;
    msg.val_float64 = 2.5;

    msg.text = std::string("all types");
    msg.color = demo::Color_RED;
    msg.mode = demo::Mode_RUN;
    msg.pose = demo::Vec3(1.0f, 2.0f, 3.0f);

    auto child = std::make_shared<demo::ChildMsg>();
    child->id = 7;
    child->name = std::string("child");
    child->pose = demo::Vec3(4.0f, 5.0f, 6.0f);
    msg.child = child;
    msg.payload = demo::PayloadUnionMsg{child};

    auto meta = std::make_shared<demo::MetaInfoMsg>();
    meta->tag = std::string("meta");
    meta->level = 9;
    msg.meta = meta;

    const std::vector<uint8_t> bytes_data{1, 2, 3, 4};
    const std::vector<int32_t> ints_data{10, 20, 30};
    const std::vector<float> floats_data{0.5f, 1.5f};
    msg.bytes = bytes_data;
    msg.ints = ints_data;
    msg.floats = floats_data;
    msg.names = std::vector<std::string>{"alice", "bob"};
    msg.poses = std::vector<demo::Vec3>{demo::Vec3(0.0f, 1.0f, 2.0f)};
    msg.children = std::vector<std::shared_ptr<demo::ChildMsg>>{child};
    msg.colors = std::vector<demo::Color>{demo::Color_GREEN, demo::Color_BLUE};
    msg.modes = std::vector<demo::Mode>{demo::Mode_IDLE, demo::Mode_STOP};
  });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(publisher->GetStats().publish_count, 1u);
}

TEST(PublisherTypedTest, PublishTypedSupportsCppVectorUnionSchema) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_vector_union_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 13;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(13, StrEq("typed_vector_union_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1300;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 3234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1300, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_OK)));

  EXPECT_CALL(mock, aorta_publisher_free(1300)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(13)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_vector_union_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::AllTypesCppVectorUnion>(
      "typed_vector_union_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::AllTypesCppVectorUnion>(
      [](demo::AllTypesCppVectorUnionMsg& msg) {
        auto child = std::make_shared<demo::UnionChildMsg>();
        child->id = 1;
        child->text = std::string("child");

        auto meta = std::make_shared<demo::UnionMetaMsg>();
        meta->level = 9;

        msg.payload = demo::UnionPayloadMsg{child};
        msg.payloads = std::vector<demo::UnionPayloadMsg>{
            demo::UnionPayloadMsg{child},
            demo::UnionPayloadMsg{meta},
        };
      });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(publisher->GetStats().publish_count, 1u);
}

TEST(PublisherTypedTest, PublishTypedErrorIncrementsSendErrorCount) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_err_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 14;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(14, StrEq("typed_err_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1400;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 4234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1400, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_ERR_INTERNAL)));

  EXPECT_CALL(mock, aorta_last_error()).WillOnce(Return("typed publish error"));
  EXPECT_CALL(mock, aorta_publisher_free(1400)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(14)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_err_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::Chat>("typed_err_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string("typed fail");
  });
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INTERNAL);

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 1u);
  EXPECT_EQ(stats.send_error_count, 1u);
}

TEST(PublisherTypedTest, PublishTypedUsesCoreTimeSource) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_now_err_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 15;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(15, StrEq("typed_now_err_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1500;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillOnce(Invoke([](int64_t* out) {
        *out = 4234567890;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_publish_owned(1500, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_OK)));

  EXPECT_CALL(mock, aorta_publisher_free(1500)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(15)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_now_err_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::Chat>("typed_now_err_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string("typed now error");
  });
  EXPECT_TRUE(status.ok());

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 1u);
  EXPECT_EQ(stats.send_error_count, 0u);
  EXPECT_EQ(stats.last_publish_ns, 4234567890u);
}

// A same-publisher Publish() from inside a build
// callback previously deadlocked forever on the non-recursive
// staged_alloc_mutex. It must now complete via the unpooled fallback.
TEST(PublisherTest, ReentrantPublishInsideBuildCallbackDoesNotDeadlock) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(StrEq("reentrant_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 41;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_new(41, StrEq("reentrant_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 4100;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_now_ns(_))
      .Times(2)
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 9'000'000'000LL;
        return AORTA_OK;
      }));
  // Both the outer and the nested publish must reach the wire.
  EXPECT_CALL(mock, aorta_publisher_publish_owned(4100, _))
      .Times(2)
      .WillRepeatedly(Invoke([](aorta_pub_t,
                                const aorta_owned_bytes_view_t* view) {
        EXPECT_NE(view, nullptr);
        EXPECT_NE(view->slices, nullptr);
        EXPECT_NE(view->slices[0].release, nullptr);
        view->slices[0].release(view->slices[0].release_ctx,
                                view->slices[0].data, view->slices[0].len);
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_free(4100)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(41)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("reentrant_node", "default").value();
  auto publisher = node->CreatePublisher<demo::Chat>("reentrant_topic").value();

  Status inner_status(AORTA_ERR_INTERNAL, "not run");
  Status outer_status = publisher->Publish(
      [&](flatbuffers::FlatBufferBuilder& builder,
          flatbuffers::Offset<aorta::sys::AortaHeader> header) {
        // Nested publish on the SAME publisher from inside the callback.
        inner_status = publisher->Publish(
            [](flatbuffers::FlatBufferBuilder& inner_builder,
               flatbuffers::Offset<aorta::sys::AortaHeader> inner_header) {
              auto text = inner_builder.CreateString("nested");
              demo::ChatBuilder chat(inner_builder);
              chat.add_aorta_header(inner_header);
              chat.add_text(text);
              inner_builder.Finish(chat.Finish());
            });
        auto text = builder.CreateString("outer");
        demo::ChatBuilder chat(builder);
        chat.add_aorta_header(header);
        chat.add_text(text);
        builder.Finish(chat.Finish());
      });

  EXPECT_TRUE(outer_status.ok()) << outer_status.message();
  EXPECT_TRUE(inner_status.ok()) << inner_status.message();
}

TEST(PublisherTypedTest, PublishTypedSerializesConcurrentBuildsOnSharedPublisher) {
  MockAortaFFI mock;
  std::atomic<int64_t> next_now{7'000'000'000LL};
  std::atomic<int> concurrent{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<bool> go{false};

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_concurrent_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 18;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(18, StrEq("typed_concurrent_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1800;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .Times(2)
      .WillRepeatedly(Invoke([&](int64_t* out) {
        *out = next_now.fetch_add(1, std::memory_order_relaxed);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1800, _))
      .Times(2)
      .WillRepeatedly(Invoke([](aorta_pub_t,
                                const aorta_owned_bytes_view_t* view) {
        EXPECT_NE(view, nullptr);
        EXPECT_NE(view->slices, nullptr);
        EXPECT_NE(view->slices[0].release, nullptr);
        view->slices[0].release(view->slices[0].release_ctx, view->slices[0].data,
                                view->slices[0].len);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_free(1800)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(18)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_concurrent_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::Chat>("typed_concurrent_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto publish_once = [&](const char* text) {
    while (!go.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    return publisher->Publish([&](flatbuffers::FlatBufferBuilder& builder,
                                  flatbuffers::Offset<aorta::sys::AortaHeader> header) {
      const int now = concurrent.fetch_add(1, std::memory_order_seq_cst) + 1;
      int observed = max_concurrent.load(std::memory_order_seq_cst);
      while (now > observed &&
             !max_concurrent.compare_exchange_weak(observed, now,
                                                   std::memory_order_seq_cst,
                                                   std::memory_order_seq_cst)) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      auto text_offset = builder.CreateString(text);
      demo::ChatBuilder chat_builder(builder);
      chat_builder.add_aorta_header(header);
      chat_builder.add_text(text_offset);
      builder.Finish(chat_builder.Finish());
      concurrent.fetch_sub(1, std::memory_order_seq_cst);
    });
  };

  Status status1;
  Status status2;
  std::thread t1([&] { status1 = publish_once("first"); });
  std::thread t2([&] { status2 = publish_once("second"); });
  go.store(true, std::memory_order_release);
  t1.join();
  t2.join();

  EXPECT_TRUE(status1.ok());
  EXPECT_TRUE(status2.ok());
  EXPECT_EQ(max_concurrent.load(std::memory_order_seq_cst), 1);

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 2u);
  EXPECT_EQ(stats.send_error_count, 0u);
}

TEST(PublisherTypedTest, PublishTypedTracksOversizeStatsOnCopyPath) {
  MockAortaFFI mock;
  std::size_t published_len = 0;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_oversize_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 16;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(16, StrEq("typed_oversize_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t* opts, aorta_pub_t* out) {
        EXPECT_NE(opts, nullptr);
        if (opts) {
          EXPECT_EQ(opts->max_message_size, 64u);
        }
        *out = 1600;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 5234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1600, _))
      .WillOnce(Invoke(
          [&published_len](aorta_pub_t,
                           const aorta_owned_bytes_view_t* view) {
            published_len = view ? view->total_len : 0;
            ReleaseOwnedSlices(view);
            return AORTA_OK;
          }));

  EXPECT_CALL(mock, aorta_publisher_free(1600)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(16)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_oversize_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  PublisherOptions options;
  options.max_message_size = 64;
  auto pub_result =
      node->CreatePublisher<demo::Chat>("typed_oversize_topic", QoS(), options);
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string(2048, 'x');
  });
  EXPECT_TRUE(status.ok());
  EXPECT_GT(published_len, 64u);

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 1u);
  EXPECT_EQ(stats.send_error_count, 0u);
  EXPECT_EQ(stats.oversize_count, 1u);
  EXPECT_GT(stats.last_oversize_size, 64u);
  EXPECT_GE(stats.current_capacity, stats.last_oversize_size);
  EXPECT_GE(stats.max_capacity_seen, stats.current_capacity);
}

TEST(PublisherTypedTest, PublishTypedReusesDetachedOwnedBufferWrapperAfterRelease) {
  MockAortaFFI mock;
  using ReleaseFn = decltype(aorta_owned_bytes_slice_t{}.release);

  void* first_ctx = nullptr;
  void* second_ctx = nullptr;
  ReleaseFn first_release = nullptr;
  ReleaseFn second_release = nullptr;

  EXPECT_CALL(mock, aorta_node_new(StrEq("typed_reuse_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 17;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(17, StrEq("typed_reuse_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 1700;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 6234567890;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish_owned(1700, _))
      .WillOnce(Invoke([&](aorta_pub_t,
                           const aorta_owned_bytes_view_t* view) -> aorta_result_t {
        if (view == nullptr || view->slices == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        first_ctx = view->slices[0].release_ctx;
        first_release = view->slices[0].release;
        EXPECT_NE(first_ctx, nullptr);
        EXPECT_NE(first_release, nullptr);
        return AORTA_OK;
      }))
      .WillOnce(Invoke([&](aorta_pub_t,
                           const aorta_owned_bytes_view_t* view) -> aorta_result_t {
        if (view == nullptr || view->slices == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        second_ctx = view->slices[0].release_ctx;
        second_release = view->slices[0].release;
        EXPECT_NE(second_ctx, nullptr);
        EXPECT_NE(second_release, nullptr);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_free(1700)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(17)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("typed_reuse_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result = node->CreatePublisher<demo::Chat>("typed_reuse_topic");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status1 = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string("first");
  });
  ASSERT_TRUE(status1.ok());
  ASSERT_NE(first_release, nullptr);
  first_release(first_ctx, nullptr, 0);

  auto status2 = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
    msg.text = std::string("second");
  });
  ASSERT_TRUE(status2.ok());
  ASSERT_NE(second_release, nullptr);
  EXPECT_EQ(second_ctx, first_ctx);
  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.detached_wrapper_reuse_miss_count, 0u);
  EXPECT_EQ(stats.detached_wrapper_reuse_hit_count, 0u);
  second_release(second_ctx, nullptr, 0);
}

// ==================== Publisher Capacity Tests ====================

// Test capacity initialization with max_message_size
TEST(PublisherCapacityTest, CapacityFromMaxMessageSize) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 4;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(4, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t* opts, aorta_pub_t* out) {
        EXPECT_NE(opts, nullptr);
        if (opts) {
          EXPECT_EQ(opts->max_message_size, 4096u);
        }
        *out = 400;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(400, _, _))
      .WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(400)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(4)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("cap_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  PublisherOptions options;
  options.max_message_size = 4096;

  auto pub_result = node->CreatePublisher("cap_topic", QoS(), options);
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.current_capacity, 4096u);
}

// ==================== Publisher Stats Tests ====================

// Test publish count tracking
TEST(PublisherStatsTest, PublishCountTracking) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 5;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(5, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 500;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        static int64_t time = 1000;
        *out = time++;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_publish(500, _, _))
      .WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(500)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(5)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("stats_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("stats_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  // Initial state
  auto stats0 = publisher->GetStats();
  EXPECT_EQ(stats0.publish_count, 0u);

  // Publish one message
  uint8_t data1[] = {0x01};
  EXPECT_TRUE(publisher->PublishBytes(data1, 1).ok());
  auto stats1 = publisher->GetStats();
  EXPECT_EQ(stats1.publish_count, 1u);
  EXPECT_GT(stats1.last_publish_ns, 0u);

  // Publish more messages
  for (int i = 0; i < 9; ++i) {
    EXPECT_TRUE(publisher->PublishBytes(data1, 1).ok());
  }
  auto stats2 = publisher->GetStats();
  EXPECT_EQ(stats2.publish_count, 10u);
}

// Test send error count tracking
TEST(PublisherStatsTest, SendErrorTracking) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 6;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(6, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 600;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  // First 3 calls succeed, then fail
  EXPECT_CALL(mock, aorta_publisher_publish(600, _, _))
      .WillOnce(Return(AORTA_OK))
      .WillOnce(Return(AORTA_OK))
      .WillOnce(Return(AORTA_OK))
      .WillRepeatedly(Return(AORTA_ERR_INTERNAL));

  EXPECT_CALL(mock, aorta_publisher_free(600)).WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_node_free(6)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("err_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("err_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  uint8_t data[] = {0x01};

  // 3 successful publishes
  EXPECT_TRUE(publisher->PublishBytes(data, 1).ok());
  EXPECT_TRUE(publisher->PublishBytes(data, 1).ok());
  EXPECT_TRUE(publisher->PublishBytes(data, 1).ok());

  // 2 failed publishes
  EXPECT_FALSE(publisher->PublishBytes(data, 1).ok());
  EXPECT_FALSE(publisher->PublishBytes(data, 1).ok());

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 5u);
  EXPECT_EQ(stats.send_error_count, 2u);
}

// ==================== Publisher Lifecycle Tests ====================

// Test publisher RAII cleanup
TEST(PublisherLifecycleTest, RaiiCleanup) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 7;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(7, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 700;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  // Expect publisher to be freed
  EXPECT_CALL(mock, aorta_publisher_free(700)).WillOnce(Return(AORTA_OK));

  // Expect node to be freed
  EXPECT_CALL(mock, aorta_node_free(7)).WillOnce(Return(AORTA_OK));

  {
    auto node_result = Node::Create("raii_node", "default");
    ASSERT_TRUE(node_result.ok());
    auto node = node_result.value();

    {
      auto pub_result =
          node->CreatePublisher("raii_topic", QoS(), PublisherOptions());
      ASSERT_TRUE(pub_result.ok());
      auto publisher = pub_result.value();

      // Publisher should be valid here
      EXPECT_EQ(publisher->Topic(), "raii_topic");

      // Publisher destructor should call aorta_publisher_free when leaving
      // scope
    }

    // Node destructor should call aorta_node_free when leaving scope
  }
}

// ==================== Advanced Publisher Tests ====================

TEST(PublisherAdvancedTest, PublishLargeMessage) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 700;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(700, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 7001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(7001, _, _))
      .WillOnce(Invoke([](aorta_pub_t, const uint8_t*, size_t len) {
        EXPECT_GE(len, 64 * 1024);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_free(7001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(700)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("large_msg_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  PublisherOptions opts;
  opts.max_message_size = 128 * 1024;
  auto pub_result = node->CreatePublisher("large_topic", QoS(), opts);
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<uint8_t> large_data(64 * 1024, 0xAB);
  auto status = publisher->PublishBytes(large_data.data(), large_data.size());
  EXPECT_TRUE(status.ok());
}

TEST(PublisherAdvancedTest, PublishEmptyMessage) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 800;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(800, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 8001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(8001, _, 0))
      .WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(8001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(800)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("empty_msg_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("empty_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishBytes(nullptr, 0);
  EXPECT_TRUE(status.ok());
}

TEST(PublisherAdvancedTest, HighFrequencyPublish) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 900;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(900, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 9001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(9001, _, _))
      .Times(1000)
      .WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(9001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(900)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("high_freq_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("high_freq_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  for (int i = 0; i < 1000; ++i) {
    uint8_t data[] = {static_cast<uint8_t>(i & 0xFF)};
    auto status = publisher->PublishBytes(data, sizeof(data));
    EXPECT_TRUE(status.ok());
  }

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 1000u);
}

TEST(PublisherAdvancedTest, VariableSizeMessages) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 1000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(1000, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 10001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(10001, _, _))
      .Times(5)
      .WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(10001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1000)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("var_size_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  PublisherOptions opts;
  opts.max_message_size = 8192;
  auto pub_result = node->CreatePublisher("var_size_topic", QoS(), opts);
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<size_t> sizes = {10, 100, 1000, 4096, 8192};
  for (size_t size : sizes) {
    std::vector<uint8_t> data(size, 0xCC);
    auto status = publisher->PublishBytes(data.data(), data.size());
    EXPECT_TRUE(status.ok());
  }
}

TEST(PublisherAdvancedTest, PublishFailureHandling) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 1100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(1100, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 11001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(11001, _, _))
      .WillOnce(Return(AORTA_OK))
      .WillOnce(Return(AORTA_ERR_INTERNAL))
      .WillOnce(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_last_error()).WillOnce(Return("Network error"));

  EXPECT_CALL(mock, aorta_publisher_free(11001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1100)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("fail_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("fail_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  uint8_t data[] = {0x01, 0x02, 0x03};

  auto status1 = publisher->PublishBytes(data, sizeof(data));
  EXPECT_TRUE(status1.ok());

  auto status2 = publisher->PublishBytes(data, sizeof(data));
  EXPECT_FALSE(status2.ok());
  EXPECT_EQ(status2.code(), AORTA_ERR_INTERNAL);

  auto status3 = publisher->PublishBytes(data, sizeof(data));
  EXPECT_TRUE(status3.ok());

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 3u);
  EXPECT_EQ(stats.send_error_count, 1u);
}

TEST(PublisherAdvancedTest, TopicNameEdgeCases) {
  MockAortaFFI mock;

  std::string topic_with_slash = "robot/status/battery";
  std::string topic_with_underscore = "robot_status_battery";
  std::string topic_with_dash = "robot-status-battery";

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .Times(3)
      .WillRepeatedly(Invoke([](const char*, const char*, aorta_node_t* out) {
        static int counter = 1200;
        *out = counter++;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(_, StrEq(topic_with_slash), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 12001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock,
              aorta_publisher_new(_, StrEq(topic_with_underscore), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 12002;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(_, StrEq(topic_with_dash), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 12003;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(12001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_publisher_free(12002)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_publisher_free(12003)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1200)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1201)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1202)).WillOnce(Return(AORTA_OK));

  {
    auto node1 = Node::Create("node1").value();
    auto pub1 =
        node1->CreatePublisher(topic_with_slash, QoS(), PublisherOptions())
            .value();
    EXPECT_EQ(pub1->Topic(), topic_with_slash);
  }

  {
    auto node2 = Node::Create("node2").value();
    auto pub2 =
        node2->CreatePublisher(topic_with_underscore, QoS(), PublisherOptions())
            .value();
    EXPECT_EQ(pub2->Topic(), topic_with_underscore);
  }

  {
    auto node3 = Node::Create("node3").value();
    auto pub3 =
        node3->CreatePublisher(topic_with_dash, QoS(), PublisherOptions())
            .value();
    EXPECT_EQ(pub3->Topic(), topic_with_dash);
  }
}

TEST(PublisherAdvancedTest, StatsOverflow) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 1300;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(1300, _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 13001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_)).WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_publish(13001, _, _))
      .Times(100)
      .WillRepeatedly(Return(AORTA_OK));

  EXPECT_CALL(mock, aorta_publisher_free(13001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(1300)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("overflow_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("overflow_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  for (int i = 0; i < 100; ++i) {
    uint8_t data[] = {0xAA};
    auto status = publisher->PublishBytes(data, sizeof(data));
    EXPECT_TRUE(status.ok());
  }

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 100u);
}

// ==================== PublishOwned Tests ====================

// Test PublishOwned happy path: delegates to FFI, increments stats
TEST(PublisherPublishOwnedTest, HappyPathDelegatesAndTracksStats) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2000;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(2000, StrEq("owned_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 20001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 9999;
        return AORTA_OK;
      }));

  // Verify the owned bytes view structure passed to FFI
  EXPECT_CALL(mock, aorta_publisher_publish_owned(20001, _))
      .WillOnce(Invoke([](aorta_pub_t, const aorta_owned_bytes_view_t* view) {
        EXPECT_NE(view, nullptr);
        EXPECT_EQ(view->slice_count, 1u);
        EXPECT_EQ(view->total_len, 4u);
        EXPECT_NE(view->slices, nullptr);
        EXPECT_EQ(view->slices[0].len, 4u);
        EXPECT_NE(view->slices[0].data, nullptr);
        EXPECT_EQ(view->slices[0].data[0], 0xDE);
        EXPECT_EQ(view->slices[0].data[1], 0xAD);
        EXPECT_EQ(view->slices[0].data[2], 0xBE);
        EXPECT_EQ(view->slices[0].data[3], 0xEF);
        // release function pointer should be set
        EXPECT_NE(view->slices[0].release, nullptr);
        ReleaseOwnedSlices(view);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_free(20001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2000)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("owned_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("owned_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  // Initial state
  auto stats0 = publisher->GetStats();
  EXPECT_EQ(stats0.publish_count, 0u);
  EXPECT_EQ(stats0.send_error_count, 0u);

  // Publish owned data with a release callback
  uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
  auto release_fn = [](void* ctx, const uint8_t*, std::size_t) {
    (void)ctx;
  };

  auto status =
      publisher->PublishOwned(data, sizeof(data), release_fn, nullptr);
  EXPECT_TRUE(status.ok());

  auto stats1 = publisher->GetStats();
  EXPECT_EQ(stats1.publish_count, 1u);
  EXPECT_EQ(stats1.send_error_count, 0u);
  EXPECT_GT(stats1.last_publish_ns, 0u);
}

// Test PublishOwned error path: FFI returns error, send_error_count incremented
TEST(PublisherPublishOwnedTest, ErrorPathIncrementsSendErrorCount) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2100;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_new(2100, StrEq("owned_err_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 21001;
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 5555;
        return AORTA_OK;
      }));

  // First call succeeds, second fails
  EXPECT_CALL(mock, aorta_publisher_publish_owned(21001, _))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_OK)))
      .WillOnce(Invoke(PublishOwnedReleasing(AORTA_ERR_INTERNAL)));

  EXPECT_CALL(mock, aorta_last_error()).WillOnce(Return("owned publish error"));

  EXPECT_CALL(mock, aorta_publisher_free(21001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2100)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("owned_err_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto pub_result =
      node->CreatePublisher("owned_err_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  uint8_t data[] = {0x01, 0x02};
  auto noop_release = [](void*, const uint8_t*, std::size_t) {};

  // First publish succeeds
  auto status1 = publisher->PublishOwned(data, sizeof(data), noop_release, nullptr);
  EXPECT_TRUE(status1.ok());

  // Second publish fails
  auto status2 = publisher->PublishOwned(data, sizeof(data), noop_release, nullptr);
  EXPECT_FALSE(status2.ok());
  EXPECT_EQ(status2.code(), AORTA_ERR_INTERNAL);

  auto stats = publisher->GetStats();
  EXPECT_EQ(stats.publish_count, 2u);
  EXPECT_EQ(stats.send_error_count, 1u);
}

// PublishOwned's validation-failure paths must
// invoke release_fn so the caller-handed buffer's cleanup state
// doesn't leak (the function takes ownership the moment it's
// called; the happy path's release pairing happens deep inside
// the FFI queue drain, so validation short-circuits must run it
// themselves).

namespace {

struct ReleaseProbe {
  std::size_t calls = 0;
  const uint8_t* last_data = nullptr;
  std::size_t last_len = 0;
};

void RecordRelease(void* ctx, const uint8_t* data, std::size_t len) {
  auto* probe = static_cast<ReleaseProbe*>(ctx);
  probe->calls += 1;
  probe->last_data = data;
  probe->last_len = len;
}

}  // namespace

// Null `data` short-circuits validation but the buffer was
// (notionally) handed to us — release_fn must still fire.
TEST(PublisherPublishOwnedTest, NullDataValidationFailureInvokesReleaseFn) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2200;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_new(2200, StrEq("l5_null_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 22001;
        return AORTA_OK;
      }));
  // No publish call expected — validation must short-circuit
  // before reaching the FFI.
  EXPECT_CALL(mock, aorta_publisher_publish_owned(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_publisher_free(22001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2200)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("l5_null_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto pub_result =
      node->CreatePublisher("l5_null_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  ReleaseProbe probe;
  auto status =
      publisher->PublishOwned(nullptr, 4, &RecordRelease, &probe);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INVALID_ARGUMENT);
  // The release contract: release_fn was invoked with the exact triple
  // the caller passed in (ctx, data, len), even though we
  // short-circuited.
  EXPECT_EQ(probe.calls, 1u);
  EXPECT_EQ(probe.last_data, nullptr);
  EXPECT_EQ(probe.last_len, 4u);
}

// Zero `len` with valid `data` short-circuits validation —
// the caller's release_ctx still needs cleanup.
TEST(PublisherPublishOwnedTest, ZeroLenValidationFailureInvokesReleaseFn) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2300;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_new(2300, StrEq("l5_zerolen_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 23001;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_publish_owned(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_publisher_free(23001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2300)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("l5_zerolen_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto pub_result =
      node->CreatePublisher("l5_zerolen_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  uint8_t buf[] = {0xAA};
  ReleaseProbe probe;
  auto status =
      publisher->PublishOwned(buf, 0, &RecordRelease, &probe);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_EQ(probe.calls, 1u);
  EXPECT_EQ(probe.last_data, buf);
  EXPECT_EQ(probe.last_len, 0u);
}

// RawCtxPool's back-reference from in-flight ctx is
// `weak_ptr<RawCtxPool>` (not `shared_ptr`) so publisher teardown
// deterministically destroys the pool. Any release callback fired after
// teardown sees `weak.lock() == nullptr` and reclaims the orphaned ctx
// directly. The previous shared_ptr-back-ref design required a careful
// "move shared_ptrs out BEFORE pushing back" dance to avoid a cycle —
// trivial to break in future edits. This test exercises the actual
// post-teardown path through Publish (which acquires a RawPublishCtx
// from the pool and registers a release callback). The mock captures the
// callback + ctx but does NOT invoke them during publish, simulating
// Zenoh holding the buffer when the publisher goes away. After dropping
// publisher AND node, we invoke the callback: with the old shared_ptr
// design it would push the ctx back into a now-shared-only-via-itself
// pool (no real harm but the design's correctness rested on the manual
// move); with the new weak_ptr design the lock() returns nullptr and the
// ctx is deleted. The test passes if no UAF / no double-free fires (ASan
// would catch either).
TEST(PublisherPublishOwnedTest, L1ReleaseCallbackAfterPublisherTeardownIsSafe) {
  MockAortaFFI mock;
  void* captured_ctx = nullptr;
  decltype(aorta_owned_bytes_slice_t{}.release) captured_release = nullptr;

  EXPECT_CALL(mock, aorta_node_new(StrEq("l1_node"), StrEq("default"), _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2500;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_new(2500, StrEq("l1_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 25001;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_now_ns(_))
      .WillRepeatedly(Invoke([](int64_t* out) {
        *out = 1234567890;
        return AORTA_OK;
      }));

  // Mock publish_owned: capture release + ctx, but DO NOT invoke release.
  // This simulates Zenoh queueing the buffer while we tear down the
  // publisher below — the in-flight ctx must outlive teardown.
  EXPECT_CALL(mock, aorta_publisher_publish_owned(25001, _))
      .WillOnce(Invoke([&](aorta_pub_t,
                           const aorta_owned_bytes_view_t* view) -> aorta_result_t {
        EXPECT_NE(view, nullptr);
        EXPECT_NE(view->slices, nullptr);
        captured_ctx = view->slices[0].release_ctx;
        captured_release = view->slices[0].release;
        EXPECT_NE(captured_ctx, nullptr);
        EXPECT_NE(captured_release, nullptr);
        return AORTA_OK;
      }));

  EXPECT_CALL(mock, aorta_publisher_free(25001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2500)).WillOnce(Return(AORTA_OK));

  {
    auto node = Node::Create("l1_node", "default").value();
    auto publisher = node->CreatePublisher<demo::Chat>("l1_topic").value();
    auto status = publisher->PublishTyped<demo::Chat>([](demo::ChatMsg& msg) {
      msg.text = std::string("teardown-while-in-flight");
    });
    ASSERT_TRUE(status.ok());
    ASSERT_NE(captured_release, nullptr);
    // publisher + node destruct at end of scope → PublisherState destructs
    // → raw_ctx_pool shared_ptr drops to 0 → RawCtxPool destructs (under
    // weak_ptr design; under old design it would be kept alive by the
    // in-flight ctx's shared_ptr back-ref).
  }

  // Invoke the captured release callback AFTER publisher + node teardown.
  // With weak_ptr back-ref: lock() returns nullptr → ctx is delete'd.
  // Must not crash, must not double-free, must not push to a dead pool.
  captured_release(captured_ctx, nullptr, 0);
}

// Boundary case: when release_fn itself is null we have
// nothing to invoke; the caller is on the hook for cleanup.
// Confirm we don't crash trying to call a null function pointer.
TEST(PublisherPublishOwnedTest, NullReleaseFnReturnsErrorWithoutInvoking) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 2400;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_new(2400, StrEq("l5_nullfn_topic"), _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const aorta_qos_t*,
                          const aorta_pub_options_t*, aorta_pub_t* out) {
        *out = 24001;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_publisher_publish_owned(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_publisher_free(24001)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(2400)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("l5_nullfn_node", "default");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto pub_result =
      node->CreatePublisher("l5_nullfn_topic", QoS(), PublisherOptions());
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  uint8_t buf[] = {0xBB};
  auto status =
      publisher->PublishOwned(buf, sizeof(buf), nullptr, nullptr);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INVALID_ARGUMENT);
}

}  // namespace
}  // namespace aorta
