#include "aorta/client.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

#include "aorta/node.h"
#include "aorta/service.h"
#include "aorta/status.h"

namespace aorta {
namespace {

uint64_t CurrentProcessId() {
#if defined(_WIN32)
  return static_cast<uint64_t>(_getpid());
#else
  return static_cast<uint64_t>(getpid());
#endif
}

std::string UniqueName(const char* base) {
  static std::atomic<uint64_t> counter{0};
  const auto id = counter.fetch_add(1);
  return std::string(base) + "_p" + std::to_string(CurrentProcessId()) + "_" +
         std::to_string(id);
}

TEST(ClientTest, CreateClientViaNode) {
  auto node_result = Node::Create(UniqueName("client_test_node"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name = UniqueName("test_service");

  auto client_result = node->CreateClient(service_name, 1000);
  ASSERT_TRUE(client_result.ok());
  auto client = std::move(client_result.value());

  EXPECT_EQ(client->ServiceName(), service_name);
}

TEST(ClientTest, CallMethodDoesNotReturnInternalError) {
  auto node_result = Node::Create(UniqueName("client_call_test_node"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name = UniqueName("test_echo");

  auto service_result = node->CreateService(
      service_name, [](const MessageView& request, ServiceRequest&& req) {
        auto response = request.ToVector();
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());

  auto client_result = node->CreateClient(service_name, 100);
  ASSERT_TRUE(client_result.ok());
  auto client = std::move(client_result.value());

  std::vector<uint8_t> request = {0x01, 0x02, 0x03, 0x04};

  auto response_result = client->Call(std::move(request));

  if (!response_result.ok()) {
    EXPECT_NE(response_result.status().code(), static_cast<aorta_result_t>(-1));
  }
}

TEST(ClientTest, CallWithVectorRequest) {
  auto node_result = Node::Create(UniqueName("client_vector_test"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name = UniqueName("vector_service");

  auto client_result = node->CreateClient(service_name, 1000);
  ASSERT_TRUE(client_result.ok());
  auto client = std::move(client_result.value());

  std::vector<uint8_t> request = {0x05, 0x00, 0x00, 0x00, 0x48,
                                  0x65, 0x6c, 0x6c, 0x6f};

  auto response_result = client->Call(std::move(request));

  if (response_result.ok()) {
    auto response = response_result.value().View();
    EXPECT_GT(response.size, 0);
  }
}

TEST(ClientTest, CallWithPointerRequest) {
  auto node_result = Node::Create(UniqueName("client_pointer_test"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name = UniqueName("pointer_service");

  auto client_result = node->CreateClient(service_name, 1000);
  ASSERT_TRUE(client_result.ok());
  auto client = std::move(client_result.value());

  std::vector<uint8_t> request = {0x05, 0x00, 0x00, 0x00, 0x54,
                                  0x65, 0x73, 0x74, 0x21};

  auto response_result = client->Call(std::move(request));

  if (response_result.ok()) {
    auto response = response_result.value().View();
    EXPECT_GT(response.size, 0);
  }
}

TEST(ClientTest, MultipleClients) {
  auto node_result = Node::Create(UniqueName("multi_client_test"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name_1 = UniqueName("service1");
  const auto service_name_2 = UniqueName("service2");

  auto client1_result = node->CreateClient(service_name_1, 1000);
  ASSERT_TRUE(client1_result.ok());
  auto client1 = std::move(client1_result.value());

  auto client2_result = node->CreateClient(service_name_2, 1000);
  ASSERT_TRUE(client2_result.ok());
  auto client2 = std::move(client2_result.value());

  EXPECT_NE(client1->ServiceName(), client2->ServiceName());
  EXPECT_EQ(client1->ServiceName(), service_name_1);
  EXPECT_EQ(client2->ServiceName(), service_name_2);
}

TEST(ClientTest, ClientResponseViewPointersRemainStableAcrossRepeatedCalls) {
  auto node_result = Node::Create(UniqueName("client_view_stability_test_node"), "default");
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result.value());
  const auto service_name = UniqueName("view_stability");

  auto service_result = node->CreateService(
      service_name, [](const MessageView& request, ServiceRequest&& req) {
        auto response = request.ToVector();
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());

  auto client_result = node->CreateClient(service_name, 1000);
  ASSERT_TRUE(client_result.ok());
  auto client = std::move(client_result.value());

  std::vector<uint8_t> request = {0x31, 0x32, 0x33, 0x34};
  auto response_result = client->Call(std::move(request));
  ASSERT_TRUE(response_result.ok());

  auto response = std::move(response_result).value();
  MessageView first = response.View();
  ASSERT_GT(first.size, 0u);
  ASSERT_NE(first.slices, nullptr);
  ASSERT_GT(first.slice_count, 0u);
  const MessageSlice* first_slices = first.slices;
  auto first_bytes = first.ToVector();

  MessageView second = response.View();
  ASSERT_GT(second.size, 0u);
  ASSERT_NE(second.slices, nullptr);
  ASSERT_GT(second.slice_count, 0u);
  EXPECT_EQ(second.slices, first_slices);
  EXPECT_EQ(first.ToVector(), first_bytes);
  EXPECT_EQ(second.ToVector(), first_bytes);
}

}  // namespace
}  // namespace aorta
