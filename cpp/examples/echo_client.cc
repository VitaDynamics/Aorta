#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "echo_msg.h"
#include "schema_attach_policy.h"

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "echo_client";
  std::string group = "default";
  std::string service_name = "/echo";
  std::string schema_attach = "never";
  bool enforce_single_provider = false;
  std::optional<uint64_t> multi_reply_window_ms;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--name" && i + 1 < argc) {
      node_name = argv[++i];
    } else if (arg == "--group" && i + 1 < argc) {
      group = argv[++i];
    } else if (arg == "--service" && i + 1 < argc) {
      service_name = argv[++i];
    } else if (arg == "--schema-attach" && i + 1 < argc) {
      schema_attach = argv[++i];
    } else if (arg == "--enforce-single-provider") {
      enforce_single_provider = true;
    } else if (arg == "--multi-reply-window-ms" && i + 1 < argc) {
      const long long parsed = std::atoll(argv[++i]);
      if (parsed > 0) {
        multi_reply_window_ms = static_cast<uint64_t>(parsed);
      }
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  aorta::SchemaAttachPolicy attach_policy = aorta::SchemaAttachPolicy::kNever;
  if (!aorta::examples::ParseSchemaAttachPolicy(schema_attach, &attach_policy)) {
    AORTA_LOG(ERROR) << "Invalid --schema-attach value: " << schema_attach
                     << " (expected never|always|inherit)";
    return 2;
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  AORTA_LOG(INFO) << "Echo client node=" << node->name() << " group="
                  << node->group() << " service=" << service_name;
  std::string multi_reply_window_text =
      multi_reply_window_ms.has_value()
          ? std::to_string(*multi_reply_window_ms)
          : "(none)";
  AORTA_LOG(INFO) << "schema_attach=" << schema_attach
                  << " enforce_single_provider="
                  << (enforce_single_provider ? "true" : "false")
                  << " multi_reply_window_ms=" << multi_reply_window_text;

  aorta::ClientOptions options;
  options.timeout_ms = 2000;
  options.schema_attach_policy = attach_policy;
  if (enforce_single_provider) {
    options.enforce_single_provider = true;
  }
  if (multi_reply_window_ms.has_value()) {
    options.multi_reply_check_window_ms = *multi_reply_window_ms;
  }

  auto client = aorta::Must(
      node->CreateClientTyped<demo::EchoRequest, demo::EchoResponse>(
          service_name, options),
      "Create typed client");

  AORTA_LOG(INFO) << "Echo client ready, sending one request per second.";
  AORTA_LOG(INFO) << "Press Ctrl+C to exit";

  uint64_t counter = 0;
  while (g_running) {
    const std::string payload =
        "echo request #" + std::to_string(counter) + " from " + node->name();

    auto call_result = client->Call([&](demo::EchoRequestMsg& msg) {
      msg.message = payload;
    });
    if (!call_result.ok()) {
      AORTA_LOG(WARN) << "Echo call failed: " << call_result.status().message();
    } else {
      auto response = std::move(call_result).value();
      if (response.Message() == nullptr ||
          response.Message()->echoed_message() == nullptr) {
        AORTA_LOG(WARN) << "Echo response decode succeeded but echoed_message is missing";
      } else {
        const std::string response_text =
            response.Message()->echoed_message()->str();
        AORTA_LOG(INFO) << "[echo ok] text=\"" << response_text << "\"";
      }
    }

    counter++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
