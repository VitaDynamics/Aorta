#include "network_test_fixture.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

namespace aorta {
namespace testing {

namespace {

uint64_t CurrentProcessId() {
#if defined(_WIN32)
  return static_cast<uint64_t>(_getpid());
#else
  return static_cast<uint64_t>(getpid());
#endif
}

}  // namespace

void NetworkTestFixture::SetUp() {
  // Node will be created on demand by CreateTestNode()
}

void NetworkTestFixture::TearDown() {
  // Node will be destroyed when shared_ptr goes out of scope
  node_.reset();
}

StatusOr<std::shared_ptr<Node>> NetworkTestFixture::CreateTestNode(
    const std::string& group) {
  static std::atomic<uint64_t> node_counter{0};
  if (node_) {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "Node already created for this test");
  }

  const auto id = node_counter.fetch_add(1);
  std::string node_name =
      std::string("test_node_") +
      std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()) +
      "_p" + std::to_string(CurrentProcessId()) + "_" + std::to_string(id);

  auto result = Node::Create(node_name, group);
  if (!result.ok()) {
    return result;
  }

  node_ = result.value();
  return node_;
}

StatusOr<std::shared_ptr<Publisher>> NetworkTestFixture::CreatePublisher(
    const std::string& topic, const QoS& qos, const PublisherOptions& options) {
  if (!node_) {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "Must call CreateTestNode() first");
  }

  return node_->CreatePublisher(topic, qos, options);
}

StatusOr<std::shared_ptr<Subscriber>> NetworkTestFixture::CreateSubscriber(
    const std::string& topic,
    std::function<void(const MessageView&, const MessageContextView&)> callback,
    const QoS& qos, const SubscriberOptions& options) {
  if (!node_) {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "Must call CreateTestNode() first");
  }

  return node_->CreateSubscriber(topic, callback, qos, options);
}

bool NetworkTestFixture::WaitForMessages(std::atomic<int>& message_count,
                                         int expected_count,
                                         std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (message_count.load() < expected_count) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

void NetworkTestFixture::GiveTimeForAsyncOps(
    std::chrono::milliseconds duration) {
  std::this_thread::sleep_for(duration);
}

}  // namespace testing
}  // namespace aorta
