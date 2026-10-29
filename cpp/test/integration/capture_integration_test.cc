// Integration tests for the C++ capture + schema-resolver wrappers over the
// C-ABI. Drives real Zenoh: publish → capture, and resolver error paths.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <vector>

#include "aorta/capture.h"
#include "aorta/node.h"
#include "aorta/publisher.h"
#include "network_test_fixture.h"

namespace aorta {
namespace testing {
namespace {

TEST_F(NetworkTestFixture, CaptureSubscriberReceivesLivePublish) {
  auto node = CreateTestNode("default");
  ASSERT_TRUE(node.ok()) << node.status().message();

  std::mutex m;
  std::string got_key;
  std::string got_suffix;
  std::vector<uint8_t> got_payload;
  std::atomic<int> count{0};
  bool got_live = false;
  bool had_hash = true;

  std::vector<CaptureSelector> selectors = {
      {CaptureSelectorKind::kTopic, "cap_cpp/chatter"}};
  auto sub = node_->CreateCaptureSubscriber(
      selectors, [&](const CapturedSampleView& s) {
        std::lock_guard<std::mutex> lock(m);
        got_key = std::string(s.key);
        got_suffix = std::string(s.key_suffix);
        got_payload = s.PayloadToVector();
        got_live = (s.kind == CaptureKind::kLive);
        had_hash = s.schema_hash_hex.has_value();
        count.fetch_add(1);
      });
  ASSERT_TRUE(sub.ok()) << sub.status().message();

  GiveTimeForAsyncOps(std::chrono::milliseconds(300));

  auto pub = CreatePublisher("cap_cpp/chatter");
  ASSERT_TRUE(pub.ok()) << pub.status().message();
  std::vector<uint8_t> data = {'h', 'i', '-', 'c', 'p', 'p'};
  ASSERT_TRUE(pub.value()->PublishBytes(data).ok());

  ASSERT_TRUE(WaitFor([&] { return count.load() > 0; },
                      std::chrono::seconds(3)));

  std::lock_guard<std::mutex> lock(m);
  EXPECT_EQ(got_suffix, "pub/cap_cpp/chatter");
  EXPECT_NE(got_key.find("/pub/cap_cpp/chatter"), std::string::npos);
  EXPECT_EQ(got_payload, data);
  EXPECT_TRUE(got_live);
  // Default attach policy is "never" → no schema hash on the wire.
  EXPECT_FALSE(had_hash);
}

TEST_F(NetworkTestFixture, SchemaResolverReportsNotFoundForMissingKey) {
  auto node = CreateTestNode("default");
  ASSERT_TRUE(node.ok()) << node.status().message();

  SchemaResolverOptions opts;
  opts.by_hash_timeout_ms = 200;
  opts.by_key_timeout_ms = 200;
  auto resolver = node_->CreateSchemaResolver(opts);
  ASSERT_TRUE(resolver.ok()) << resolver.status().message();

  auto result = resolver.value()->ResolveByKey("pub/does/not/exist");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_NOT_FOUND);

  // clear_key_cache is a no-op on an empty cache but must succeed.
  EXPECT_TRUE(resolver.value()->ClearKeyCache().ok());
}

}  // namespace
}  // namespace testing
}  // namespace aorta
