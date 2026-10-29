#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "aorta/pull_subscriber.h"
#include "integration/network_test_fixture.h"
#include "all_types_msg.h"

namespace aorta {
namespace testing {

class PubSubIntegrationTest : public NetworkTestFixture {
 protected:
  void SetUp() override {
    NetworkTestFixture::SetUp();
    auto node_result = CreateTestNode();
    ASSERT_TRUE(node_result.ok());
  }
};

TEST_F(PubSubIntegrationTest, SinglePublisherSingleSubscriber) {
  std::atomic<int> msg_count{0};

  auto sub_result = CreateSubscriber(
      "test_topic_single", [&](const MessageView& msg, const MessageContextView&) {
        (void)msg;
        msg_count++;
      });
  ASSERT_TRUE(sub_result.ok());
  auto subscriber = sub_result.value();

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_single");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<uint8_t> data = {0x01, 0x02, 0x03};
  auto status = publisher->PublishBytes(data);
  ASSERT_TRUE(status.ok());

  ASSERT_TRUE(WaitForMessages(msg_count, 1, std::chrono::seconds(5)));
  EXPECT_EQ(msg_count.load(), 1);
}

TEST_F(PubSubIntegrationTest, CreatePublisherRejectsInvalidTopicName) {
  // Pre-PR-#90 input was "bad-topic" (hyphen rejected). Post-relaxation
  // hyphen is accepted, so use a Zenoh-reserved char that the new
  // validator still rejects.
  auto pub_result = CreatePublisher("bad#topic");
  ASSERT_FALSE(pub_result.ok());
  const auto& msg = pub_result.status().message();
  EXPECT_TRUE(msg.find("reserved by Zenoh") != std::string::npos ||
              msg.find("'#'") != std::string::npos)
      << "unexpected error: " << msg;
}

TEST_F(PubSubIntegrationTest, MultipleMessages) {
  std::atomic<int> msg_count{0};

  auto sub_result =
      CreateSubscriber("test_topic_multiple",
                       [&](const MessageView& msg, const MessageContextView&) {
                         (void)msg;
                         msg_count++;
                       });
  ASSERT_TRUE(sub_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_multiple");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  const int num_messages = 10;
  for (int i = 0; i < num_messages; ++i) {
    std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
    auto status = publisher->PublishBytes(data);
    ASSERT_TRUE(status.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(
      WaitForMessages(msg_count, num_messages, std::chrono::seconds(5)));
  EXPECT_EQ(msg_count.load(), num_messages);
}

TEST_F(PubSubIntegrationTest, LargeMessage) {
  const size_t large_size = 64 * 1024;
  std::atomic<size_t> received_size{0};

  auto sub_result = CreateSubscriber(
      "test_topic_large", [&](const MessageView& msg, const MessageContextView&) {
        received_size = msg.size;
      });
  ASSERT_TRUE(sub_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_large");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<uint8_t> large_data(large_size);
  for (size_t i = 0; i < large_size; ++i) {
    large_data[i] = static_cast<uint8_t>(i & 0xFF);
  }

  auto status = publisher->PublishBytes(large_data);
  ASSERT_TRUE(status.ok());

  ASSERT_TRUE(WaitFor([&]() { return received_size.load() > 0; },
                      std::chrono::seconds(5)));
  EXPECT_EQ(received_size.load(), large_size);
}

TEST_F(PubSubIntegrationTest, MessageOrdering) {
  std::vector<uint8_t> received_sequence;
  std::mutex sequence_mutex;

  auto sub_result = CreateSubscriber(
      "test_topic_order", [&](const MessageView& msg, const MessageContextView&) {
        std::lock_guard<std::mutex> lock(sequence_mutex);
        if (msg.size > 0) {
          auto payload = msg.ToVector();
          if (!payload.empty()) {
            received_sequence.push_back(payload.front());
          }
        }
      });
  ASSERT_TRUE(sub_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_order");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  const int num_messages = 5;
  for (int i = 0; i < num_messages; ++i) {
    std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
    auto status = publisher->PublishBytes(data);
    ASSERT_TRUE(status.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  ASSERT_TRUE(WaitFor(
      [&]() {
        std::lock_guard<std::mutex> lock(sequence_mutex);
        return received_sequence.size() >= num_messages;
      },
      std::chrono::seconds(5)));

  std::lock_guard<std::mutex> lock(sequence_mutex);
  EXPECT_EQ(received_sequence.size(), num_messages);

  for (int i = 0; i < num_messages; ++i) {
    EXPECT_EQ(received_sequence[i], i)
        << "Message " << i << " received out of order";
  }
}

TEST_F(PubSubIntegrationTest, MultipleSubscribers) {
  std::atomic<int> sub1_count{0};
  std::atomic<int> sub2_count{0};

  auto sub1_result =
      CreateSubscriber("test_topic_multi_sub",
                       [&](const MessageView& msg, const MessageContextView&) {
                         (void)msg;
                         sub1_count++;
                       });
  ASSERT_TRUE(sub1_result.ok());

  auto sub2_result =
      CreateSubscriber("test_topic_multi_sub",
                       [&](const MessageView& msg, const MessageContextView&) {
                         (void)msg;
                         sub2_count++;
                       });
  ASSERT_TRUE(sub2_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_multi_sub");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<uint8_t> data = {0xAA, 0xBB};
  auto status = publisher->PublishBytes(data);
  ASSERT_TRUE(status.ok());

  ASSERT_TRUE(WaitForMessages(sub1_count, 1, std::chrono::seconds(5)));
  ASSERT_TRUE(WaitForMessages(sub2_count, 1, std::chrono::seconds(5)));

  EXPECT_EQ(sub1_count.load(), 1);
  EXPECT_EQ(sub2_count.load(), 1);
}

TEST_F(PubSubIntegrationTest, TypedExampleRoundTrip) {
  struct ExampleSnapshot {
    bool has_header = false;
    bool flag = false;
    std::string text;
    uint32_t child_id = 0;
    std::string child_name;
    float pose_x = 0.0f;
  };

  std::mutex snapshot_mutex;
  std::shared_ptr<ExampleSnapshot> snapshot;

  auto sub_result = node_->CreateSubscriberTypedRetained<demo::AllTypes>(
      "typed_example",
      [&](TypedRetainedSamplePtr<demo::AllTypes> sample) {
        auto* message = sample != nullptr ? sample->Message() : nullptr;
        if (message == nullptr) {
          return;
        }

        auto next = std::make_shared<ExampleSnapshot>();
        next->has_header = (message->aorta_header() != nullptr);
        next->flag = message->flag();
        if (message->text() != nullptr) {
          next->text = message->text()->str();
        }
        if (message->child() != nullptr) {
          next->child_id = message->child()->id();
          if (message->child()->name() != nullptr) {
            next->child_name = message->child()->name()->str();
          }
          if (message->child()->pose() != nullptr) {
            next->pose_x = message->child()->pose()->x();
          }
        }

        std::lock_guard<std::mutex> lock(snapshot_mutex);
        snapshot = std::move(next);
      });
  ASSERT_TRUE(sub_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = node_->CreatePublisher<demo::AllTypes>("typed_example");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  auto status = publisher->PublishTyped<demo::AllTypes>(
      [](demo::AllTypesMsg& msg) {
        auto child = std::make_shared<demo::ChildMsg>();
        child->id = 42u;
        child->name = std::string("fixture-child");
        child->pose = demo::Vec3(1.5f, 2.5f, 3.5f);
        msg.flag = true;
        msg.text = std::string("fixture-message");
        msg.child = std::move(child);
      });
  ASSERT_TRUE(status.ok());

  ASSERT_TRUE(WaitFor(
      [&]() {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        return snapshot != nullptr;
      },
      std::chrono::seconds(5)));

  std::shared_ptr<ExampleSnapshot> actual;
  {
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    actual = snapshot;
  }

  ASSERT_NE(actual, nullptr);
  EXPECT_TRUE(actual->has_header);
  EXPECT_TRUE(actual->flag);
  EXPECT_EQ(actual->text, "fixture-message");
  EXPECT_EQ(actual->child_id, 42u);
  EXPECT_EQ(actual->child_name, "fixture-child");
  EXPECT_FLOAT_EQ(actual->pose_x, 1.5f);
}

TEST_F(PubSubIntegrationTest, PublisherBeforeSubscriber) {
  auto pub_result = CreatePublisher("test_topic_pub_first");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  std::vector<uint8_t> data = {0x11};
  auto status = publisher->PublishBytes(data);
  ASSERT_TRUE(status.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  std::atomic<int> msg_count{0};
  auto sub_result =
      CreateSubscriber("test_topic_pub_first",
                       [&](const MessageView& msg, const MessageContextView&) {
                         (void)msg;
                         msg_count++;
                       });
  ASSERT_TRUE(sub_result.ok());

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto status2 = publisher->PublishBytes(data);
  ASSERT_TRUE(status2.ok());

  ASSERT_TRUE(WaitForMessages(msg_count, 1, std::chrono::seconds(5)));
}

// QoS & buffering redesign — Tier B-pull: a native keep-last ring keeps the
// newest N samples; the application drains them with TryRecv.
TEST_F(PubSubIntegrationTest, PullSubscriberKeepsLastN) {
  constexpr std::size_t kDepth = 4;
  auto pull_result = node_->CreatePullSubscriber("test_topic_pull", kDepth);
  ASSERT_TRUE(pull_result.ok());
  PullSubscriber pull = std::move(pull_result).value();
  EXPECT_EQ(pull.DroppedCount(), 0u);

  GiveTimeForAsyncOps(std::chrono::milliseconds(400));  // discovery

  auto pub_result = CreatePublisher("test_topic_pull");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  // Publish 0..10 without draining, so the ring overflows and keeps the last N.
  for (int i = 0; i < 10; ++i) {
    std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
    ASSERT_TRUE(publisher->PublishBytes(data).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  GiveTimeForAsyncOps(std::chrono::milliseconds(300));

  std::vector<uint8_t> got;
  while (true) {
    auto recv = pull.TryRecv();
    ASSERT_TRUE(recv.ok());
    if (!recv.value().has_value()) {
      break;
    }
    auto bytes = recv.value().value().ToVector();
    ASSERT_FALSE(bytes.empty());
    got.push_back(bytes[0]);
  }

  ASSERT_FALSE(got.empty());
  EXPECT_LE(got.size(), kDepth) << "ring exceeded its depth";
  EXPECT_NE(std::find(got.begin(), got.end(), 9), got.end())
      << "ring must retain the newest sample";
}

// QoS & buffering redesign — Tier A: receive_depth caps in-flight events on a
// slow callback and sheds the rest (drop-newest), observable via DroppedCount.
TEST_F(PubSubIntegrationTest, ReceiveDepthCapsAndSheds) {
  constexpr std::size_t kCap = 2;
  std::atomic<int> delivered{0};

  SubscriberOptions opts;
  opts.receive_depth = kCap;
  auto sub_result = node_->CreateSubscriber(
      "test_topic_tier_a",
      [&](const MessageView&, const MessageContextView&) {
        delivered++;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      },
      opts);
  ASSERT_TRUE(sub_result.ok());
  auto subscriber = sub_result.value();

  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  auto pub_result = CreatePublisher("test_topic_tier_a");
  ASSERT_TRUE(pub_result.ok());
  auto publisher = pub_result.value();

  // Warm up discovery.
  for (int i = 0; i < 30 && delivered.load() == 0; ++i) {
    std::vector<uint8_t> data = {0};
    ASSERT_TRUE(publisher->PublishBytes(data).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
  }
  ASSERT_GT(delivered.load(), 0);

  // Burst far more than the cap, faster than the slow callback can drain.
  for (int i = 0; i < 40; ++i) {
    std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
    ASSERT_TRUE(publisher->PublishBytes(data).ok());
  }

  // Let the gate shed and the callbacks drain.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_GT(subscriber->DroppedCount(), 0u)
      << "the gate should shed under a 40-message burst at cap " << kCap;
}

}  // namespace testing
}  // namespace aorta
