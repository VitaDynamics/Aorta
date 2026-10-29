#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

#include "aorta/client.h"
#include "aorta/service.h"
#include "aorta/trace.h"
#include "integration/network_test_fixture.h"

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

std::string UniqueName(const char* base) {
  static std::atomic<uint64_t> counter{0};
  const auto id = counter.fetch_add(1);
  return std::string(base) + "_p" + std::to_string(CurrentProcessId()) + "_" +
         std::to_string(id);
}

}  // namespace

class ServiceClientIntegrationTest : public NetworkTestFixture {
 protected:
  void SetUp() override {
    NetworkTestFixture::SetUp();
    auto node_result = CreateTestNode();
    ASSERT_TRUE(node_result.ok());
  }
};

TEST_F(ServiceClientIntegrationTest, ServiceCreationAndDestruction) {
  std::atomic<bool> service_called{false};
  const auto service_name = UniqueName("test_service_create");

  // Create service
  auto service_result = node_->CreateService(
      service_name,
      [&](const MessageView& request, ServiceRequest&& req) {
        (void)request;
        service_called = true;
        req.Release();
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Verify service created
  EXPECT_NE(service, nullptr);
  EXPECT_EQ(service->Name(), service_name);

  // Destroy service by letting shared_ptr go out of scope
  service.reset();

  // Verify no crashes after destruction
  EXPECT_FALSE(service_called);
}

TEST_F(ServiceClientIntegrationTest, ClientCreationAndDestruction) {
  const auto service_name = UniqueName("test_service_client");

  // Create client (service doesn't need to exist)
  auto client_result = node_->CreateClient(service_name, 5000);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  // Verify client created
  EXPECT_NE(client, nullptr);
  EXPECT_EQ(client->ServiceName(), service_name);

  // Destroy client by letting shared_ptr go out of scope
  client.reset();

  // Verify no crashes after destruction
}

TEST_F(ServiceClientIntegrationTest, CreateServiceRejectsInvalidServiceName) {
  // Pre-PR-#90 input was "bad-service" (hyphen rejected). Post-
  // relaxation hyphen is accepted, so use a Zenoh-reserved char.
  auto service_result = node_->CreateService(
      "bad#service", [&](const MessageView& request, ServiceRequest&& req) {
        (void)request;
        req.Release();
      });
  ASSERT_FALSE(service_result.ok());
  const auto& msg = service_result.status().message();
  EXPECT_TRUE(msg.find("reserved by Zenoh") != std::string::npos ||
              msg.find("'#'") != std::string::npos)
      << "unexpected error: " << msg;
}

TEST_F(ServiceClientIntegrationTest, EchoService) {
  std::atomic<bool> service_called{false};
  const auto service_name = UniqueName("echo_service");

  // Create service that echoes request
  auto service_result = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        service_called = true;
        auto response = request.ToVector();
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Give service time to be ready
  GiveTimeForAsyncOps(std::chrono::milliseconds(200));

  // Create client
  auto client_result = node_->CreateClient(service_name, 5000);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  // Call service
  const std::vector<uint8_t> expected = {1, 2, 3, 4, 5};
  auto request = expected;
  auto response_result = client->Call(std::move(request));
  ASSERT_TRUE(response_result.ok());
  auto response = response_result.value().ToVector();

  // Verify service was called
  EXPECT_TRUE(service_called);

  // Verify response matches request (echo)
  EXPECT_EQ(response.size(), expected.size());
  EXPECT_EQ(response, expected);
}

TEST_F(ServiceClientIntegrationTest, DeferredWorkerCarriesTheRequestTraceContext) {
  const auto service_name = UniqueName("trace_ctx_service");

  // The deferred-reply pattern under test: the callback captures the
  // request's trace context and a worker thread installs it via
  // ContextGuard — spans recorded there would stay attached to the
  // request's trace (asserted at the context level here; span parentage
  // is covered by the Rust core integration test).
  std::atomic<bool> ctx_valid{false};
  std::atomic<uint64_t> ctx_span_id{0};
  std::atomic<bool> worker_saw_request_ctx{false};

  aorta::trace::set_enabled(true);

  auto service_result = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        const auto ctx = req.TraceContext();
        ctx_valid = ctx.valid;
        ctx_span_id = ctx.span_id;
        std::thread worker([&, ctx]() {
          aorta::trace::ContextGuard guard(ctx);
          const auto seen = aorta::trace::current_context();
          worker_saw_request_ctx = seen.valid && seen.span_id == ctx.span_id;
        });
        worker.join();
        auto response = request.ToVector();
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();
  GiveTimeForAsyncOps(std::chrono::milliseconds(200));

  auto client_result = node_->CreateClient(service_name, 5000);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  std::vector<uint8_t> request = {7, 7, 7};
  auto response_result = client->Call(std::move(request));
  aorta::trace::set_enabled(false);
  ASSERT_TRUE(response_result.ok());

  EXPECT_TRUE(ctx_valid.load())
      << "with tracing enabled the request must expose its trace context";
  EXPECT_NE(ctx_span_id.load(), 0u) << "the context must carry the server root span id";
  EXPECT_TRUE(worker_saw_request_ctx.load())
      << "a ContextGuard on the worker thread must install the request's context";
}

TEST_F(ServiceClientIntegrationTest, ClientTimeout) {
  const auto service_name = UniqueName("timeout_service");

  // Create service that doesn't reply (just releases)
  auto service_result = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        (void)request;
        // Release without replying
        req.Release();
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Give service time to be ready
  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  // Create client with short timeout
  auto client_result = node_->CreateClient(service_name, 100);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  // Call service and expect timeout
  std::vector<uint8_t> request_data = {1, 2, 3};
  auto response_result = client->Call(std::move(request_data));

  // Should fail with timeout error
  EXPECT_FALSE(response_result.ok());
  EXPECT_EQ(response_result.status().code(), -3);  // AORTA_ERR_TIMEOUT
}

TEST_F(ServiceClientIntegrationTest, StrictClientDetectsMultipleProviders) {
  const auto service_name = UniqueName("strict_multi_provider_service");

  std::atomic<int> provider_a_calls{0};
  std::atomic<int> provider_b_calls{0};
  // Declare two services with the same name on the same Session to avoid flaky
  // cross-session discovery in CI (multicast scouting can be unreliable).
  auto service_a = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        provider_a_calls++;
        auto response = request.ToVector();
        response.push_back(0xA1);
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_a.ok()) << service_a.status().message();
  auto service_b = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        provider_b_calls++;
        auto response = request.ToVector();
        response.push_back(0xB2);
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_b.ok()) << service_b.status().message();

  GiveTimeForAsyncOps(std::chrono::milliseconds(200));
  ClientOptions warmup_options;
  warmup_options.timeout_ms = 200;
  warmup_options.schema_attach_policy = SchemaAttachPolicy::kNever;
  warmup_options.enforce_single_provider = false;
  warmup_options.multi_reply_check_window_ms = 200;
  auto warmup_client_result = node_->CreateClient(service_name, warmup_options);
  ASSERT_TRUE(warmup_client_result.ok()) << warmup_client_result.status().message();
  auto warmup_client = warmup_client_result.value();
  ASSERT_TRUE(WaitFor(
      [&]() {
        std::vector<uint8_t> probe = {0x55};
        auto _ = warmup_client->Call(std::move(probe));
        return provider_a_calls.load() > 0 && provider_b_calls.load() > 0;
      },
      std::chrono::seconds(5)));

  const int before_a = provider_a_calls.load();
  const int before_b = provider_b_calls.load();

  ClientOptions options;
  options.timeout_ms = 5000;
  options.schema_attach_policy = SchemaAttachPolicy::kNever;
  options.enforce_single_provider = true;
  options.multi_reply_check_window_ms = 1500;
  auto client_result = node_->CreateClient(service_name, options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  auto client = client_result.value();

  std::vector<uint8_t> request = {1, 2, 3};
  auto response_result = client->Call(std::move(request));
  EXPECT_FALSE(response_result.ok());
  EXPECT_EQ(response_result.status().code(), AORTA_ERR_CONCURRENT_CONFLICT);

  auto stats = client->GetStats();
  EXPECT_EQ(stats.call_count, 1u);
  EXPECT_EQ(stats.success_count, 0u);
  EXPECT_EQ(stats.timeout_count, 0u);
  EXPECT_EQ(stats.concurrent_conflict_count, 1u);
  EXPECT_EQ(stats.error_count, 0u);
  EXPECT_GE(provider_a_calls.load() - before_a, 1);
  EXPECT_GE(provider_b_calls.load() - before_b, 1);
}

TEST_F(ServiceClientIntegrationTest, NonStrictClientAcceptsFirstProviderReply) {
  const auto service_name = UniqueName("nonstrict_multi_provider_service");
  auto service_a = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        auto response = request.ToVector();
        response.push_back(0xA1);
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_a.ok()) << service_a.status().message();
  auto service_b = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        auto response = request.ToVector();
        response.push_back(0xB2);
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_b.ok()) << service_b.status().message();

  GiveTimeForAsyncOps(std::chrono::milliseconds(150));
  ClientOptions warmup_options;
  warmup_options.timeout_ms = 200;
  warmup_options.schema_attach_policy = SchemaAttachPolicy::kNever;
  warmup_options.enforce_single_provider = false;
  warmup_options.multi_reply_check_window_ms = 200;
  auto warmup_client_result = node_->CreateClient(service_name, warmup_options);
  ASSERT_TRUE(warmup_client_result.ok()) << warmup_client_result.status().message();
  auto warmup_client = warmup_client_result.value();
  ASSERT_TRUE(WaitFor(
      [&]() {
        std::vector<uint8_t> probe = {0x66};
        auto response = warmup_client->Call(std::move(probe));
        return response.ok();
      },
      std::chrono::seconds(5)));

  ClientOptions options;
  options.timeout_ms = 3000;
  options.schema_attach_policy = SchemaAttachPolicy::kNever;
  options.enforce_single_provider = false;
  options.multi_reply_check_window_ms = 500;
  auto client_result = node_->CreateClient(service_name, options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  auto client = client_result.value();

  std::vector<uint8_t> request = {7, 8, 9};
  auto response_result = client->Call(std::move(request));
  ASSERT_TRUE(response_result.ok());
  auto response = response_result.value().ToVector();
  ASSERT_EQ(response.size(), 4u);
  EXPECT_EQ(response[0], 7);
  EXPECT_EQ(response[1], 8);
  EXPECT_EQ(response[2], 9);
  EXPECT_TRUE(response[3] == 0xA1 || response[3] == 0xB2);

  auto stats = client->GetStats();
  EXPECT_EQ(stats.call_count, 1u);
  EXPECT_EQ(stats.success_count, 1u);
  EXPECT_EQ(stats.timeout_count, 0u);
  EXPECT_EQ(stats.concurrent_conflict_count, 0u);
  EXPECT_EQ(stats.error_count, 0u);
}

TEST_F(ServiceClientIntegrationTest, AsyncReply) {
  std::atomic<bool> service_called{false};
  std::atomic<bool> reply_sent{false};
  const auto service_name = UniqueName("async_service");

  // Create service that replies asynchronously
  auto service_result = node_->CreateService(
      service_name, [&](const MessageView& request, ServiceRequest&& req) {
        service_called = true;
        auto request_copy = request.ToVector();

        // Spawn thread to reply after delay
        std::thread(
            [&reply_sent](std::vector<uint8_t> req_copy,
                          ServiceRequest req_moved) {
              // Simulate async work
              std::this_thread::sleep_for(std::chrono::milliseconds(50));

              // Reply with response
              auto response = req_copy;
              // Add marker byte to show async processing
              response.push_back(0xFF);

              req_moved.Reply(std::move(response));
              reply_sent = true;
            },
            std::move(request_copy), std::move(req))
            .detach();

        // Service callback returns immediately
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Give service time to be ready
  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  // Create client with sufficient timeout
  auto client_result = node_->CreateClient(service_name, 2000);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  // Call service
  const std::vector<uint8_t> expected = {0xAA, 0xBB, 0xCC};
  std::vector<uint8_t> request_data = expected;
  auto response_result = client->Call(std::move(request_data));
  ASSERT_TRUE(response_result.ok());
  auto response = response_result.value().ToVector();

  // Verify service was called
  EXPECT_TRUE(service_called);

  // Verify response contains original data + marker byte
  EXPECT_EQ(response.size(), expected.size() + 1);
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(response[i], expected[i]);
  }
  EXPECT_EQ(response.back(), 0xFF);

  // Wait for reply thread to complete
  ASSERT_TRUE(
      WaitFor([&]() { return reply_sent.load(); }, std::chrono::seconds(2)));
}

TEST_F(ServiceClientIntegrationTest, MultipleCallsToSameService) {
  std::atomic<int> call_count{0};
  const auto service_name = UniqueName("multi_call_service");

  // Create service that counts calls
  auto service_result = node_->CreateService(
      service_name,
      [&](const MessageView& request, ServiceRequest&& req) {
        const int current = call_count.fetch_add(1) + 1;
        // Reply with call count
        auto response = request.ToVector();
        response.push_back(static_cast<uint8_t>(current));
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Wait for service to become reachable (service declaration is async).
  ClientOptions warmup_options;
  warmup_options.timeout_ms = 200;
  warmup_options.schema_attach_policy = SchemaAttachPolicy::kNever;
  warmup_options.enforce_single_provider = false;
  warmup_options.multi_reply_check_window_ms = 200;
  auto warmup_client_result = node_->CreateClient(service_name, warmup_options);
  ASSERT_TRUE(warmup_client_result.ok()) << warmup_client_result.status().message();
  auto warmup_client = warmup_client_result.value();
  ASSERT_TRUE(WaitFor(
      [&]() {
        std::vector<uint8_t> probe = {0x77};
        auto response = warmup_client->Call(std::move(probe));
        return response.ok();
      },
      std::chrono::seconds(5)));
  const int baseline = call_count.load();

  // Create client
  auto client_result = node_->CreateClient(service_name, 5000);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  auto client = client_result.value();

  // Make multiple calls
  const int num_calls = 5;
  for (int i = 0; i < num_calls; ++i) {
    std::vector<uint8_t> request_data = {static_cast<uint8_t>(i)};
    auto response_result = client->Call(std::move(request_data));
    ASSERT_TRUE(response_result.ok()) << response_result.status().message();
    auto response = response_result.value().ToVector();

    // Verify response contains request data + call number
    EXPECT_EQ(response.size(), 2);
    EXPECT_EQ(response[0], i);
    EXPECT_EQ(response[1], static_cast<uint8_t>(baseline + i + 1));
  }

  // Verify all calls were made
  EXPECT_EQ(call_count.load(), baseline + num_calls);
}

TEST_F(ServiceClientIntegrationTest, LargePayload) {
  std::atomic<bool> service_called{false};
  const size_t large_size = 64 * 1024;  // 64KB
  const auto service_name = UniqueName("large_payload_service");

  // Create service that echoes large payload
  auto service_result = node_->CreateService(
      service_name,
      [&](const MessageView& request, ServiceRequest&& req) {
        service_called = true;
        auto response = request.ToVector();
        req.Reply(std::move(response));
      });
  ASSERT_TRUE(service_result.ok());
  auto service = service_result.value();

  // Give service time to be ready
  GiveTimeForAsyncOps(std::chrono::milliseconds(100));

  // Create client
  auto client_result = node_->CreateClient(service_name, 5000);
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  // Create large payload
  std::vector<uint8_t> request_data(large_size);
  for (size_t i = 0; i < large_size; ++i) {
    request_data[i] = static_cast<uint8_t>(i & 0xFF);
  }
  auto expected = request_data;

  // Call service with large payload
  auto response_result = client->Call(std::move(request_data));
  ASSERT_TRUE(response_result.ok());
  auto response = response_result.value().ToVector();

  // Verify service was called
  EXPECT_TRUE(service_called);

  // Verify large response
  EXPECT_EQ(response.size(), large_size);
  EXPECT_EQ(response, expected);
}

}  // namespace testing
}  // namespace aorta
