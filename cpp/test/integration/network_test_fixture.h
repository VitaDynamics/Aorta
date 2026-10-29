#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "aorta/node.h"
#include "aorta/publisher.h"
#include "aorta/qos.h"
#include "aorta/subscriber.h"

namespace aorta {
namespace testing {

/**
 * Base test fixture for network integration tests using real Zenoh
 * communication.
 *
 * This fixture manages the lifecycle of Node instances and provides helper
 * methods for creating publishers and subscribers, as well as synchronization
 * utilities for testing async message delivery.
 *
 * Key features:
 * - Creates a real Node with Zenoh network communication
 * - Provides helpers for creating publishers and subscribers
 * - Offers synchronization primitives (condition variables) for waiting on
 * messages
 * - Cleans up resources properly in teardown
 */
class NetworkTestFixture : public ::testing::Test {
 protected:
  // Test lifecycle methods
  void SetUp() override;
  void TearDown() override;

  /**
   * Creates a test node with a unique name based on the test name.
   * Can only be called once per test.
   */
  StatusOr<std::shared_ptr<Node>> CreateTestNode(
      const std::string& group = "default");

  /**
   * Creates a publisher on the test node.
   * Must call CreateTestNode first.
   */
  StatusOr<std::shared_ptr<Publisher>> CreatePublisher(
      const std::string& topic, const QoS& qos = QoS(),
      const PublisherOptions& options = PublisherOptions());

  /**
   * Creates a subscriber with a callback that stores messages.
   * Must call CreateTestNode first.
   *
   * The callback is called each time a message is received.
   */
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
      const std::string& topic,
      std::function<void(const MessageView&, const MessageContextView&)> callback,
      const QoS& qos = QoS(),
      const SubscriberOptions& options = SubscriberOptions());

  /**
   * Waits for messages to arrive with a timeout.
   * Returns true if at least `expected_count` messages arrived within timeout.
   */
  bool WaitForMessages(
      std::atomic<int>& message_count, int expected_count,
      std::chrono::milliseconds timeout = std::chrono::seconds(5));

  /**
   * Waits for a condition with timeout.
   * Returns true if condition becomes true within timeout.
   */
  template <typename Predicate>
  bool WaitFor(Predicate pred,
               std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
  }

  /**
   * Sleeps briefly to allow async operations to complete.
   * Useful for giving subscribers time to connect.
   */
  void GiveTimeForAsyncOps(
      std::chrono::milliseconds duration = std::chrono::milliseconds(100));

  // Test node for this fixture
  std::shared_ptr<Node> node_;

  // Synchronization primitives
  std::mutex sync_mutex_;
  std::condition_variable sync_cv_;
};

/**
 * Helper class for collecting messages from a subscriber.
 */
class MessageCollector {
 public:
  explicit MessageCollector(NetworkTestFixture* fixture) { (void)fixture; }

  /**
   * Creates a callback that collects messages.
   */
  std::function<void(const MessageView&, const MessageContextView&)>
  CreateCollectingCallback() {
    return [this](const MessageView& msg, const MessageContextView&) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (msg.data && msg.size > 0) {
        messages_.push_back(
            std::vector<uint8_t>(msg.data, msg.data + msg.size));
      }
      count_++;
      cv_.notify_one();
    };
  }

  /**
   * Waits for a specific number of messages.
   */
  bool WaitForMessages(int expected_count, std::chrono::milliseconds timeout =
                                               std::chrono::seconds(5)) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this, expected_count]() {
      return count_ >= expected_count;
    });
  }

  /**
   * Gets the collected messages.
   */
  const std::vector<std::vector<uint8_t>>& GetMessages() const {
    return messages_;
  }

  /**
   * Gets the count of received messages.
   */
  int GetMessageCount() const { return count_; }

  /**
   * Clears all collected messages.
   */
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    messages_.clear();
    count_ = 0;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<std::vector<uint8_t>> messages_;
  int count_ = 0;
};

}  // namespace testing
}  // namespace aorta
