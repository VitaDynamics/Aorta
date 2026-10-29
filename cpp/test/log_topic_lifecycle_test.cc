#include "aorta/node.h"
#include "gtest/gtest.h"

// Real C++ -> C ABI -> Core -> Zenoh lifetime regression, with log forwarding
// explicitly enabled. Under ASAN the process must also exit without leaks.
TEST(LogTopicLifecycleTest, RepeatedNodeAndClientDestruction) {
  for (int i = 0; i < 64; ++i) {
    auto node = aorta::Node::Create("log_lifetime", "log_lifetime");
    ASSERT_TRUE(node.ok());
    if (i % 2 == 0) {
      auto service = node.value()->CreateService(
          "test/log_lifetime",
          [](const aorta::MessageView &, aorta::ServiceRequest &&request) {
            request.Release();
          });
      ASSERT_TRUE(service.ok());
      auto client = node.value()->CreateClient("test/log_lifetime");
      ASSERT_TRUE(client.ok());
    }
  }
}

TEST(ServiceOwnerLifetimeTest, RepeatedServicesReleaseAdvisoryQueries) {
  for (int i = 0; i < 128; ++i) {
    auto node = aorta::Node::Create("owner_lifetime", "log_lifetime");
    ASSERT_TRUE(node.ok());
    auto service = node.value()->CreateService(
        "test/owner_lifetime",
        [](const aorta::MessageView &, aorta::ServiceRequest &&request) {
          request.Release();
        });
    ASSERT_TRUE(service.ok());
  }
}
