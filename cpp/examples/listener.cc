#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cpp_listener";
  std::string group = "default";
  std::string topic = "/demo/chatter";
  bool enable_header_context = false;
  bool enable_dedup = false;
  std::size_t dedup_window = 1024;
  bool enable_reorder_check = false;
  bool drop_out_of_order = false;

  // Parse command line arguments
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--name" && i + 1 < argc) {
      node_name = argv[++i];
    } else if (arg == "--group" && i + 1 < argc) {
      group = argv[++i];
    } else if (arg == "--topic" && i + 1 < argc) {
      topic = argv[++i];
    } else if (arg == "--header-context") {
      enable_header_context = true;
    } else if (arg == "--dedup") {
      enable_dedup = true;
    } else if (arg == "--dedup-window" && i + 1 < argc) {
      const long long parsed = std::atoll(argv[++i]);
      if (parsed > 0) {
        dedup_window = static_cast<std::size_t>(parsed);
      }
    } else if (arg == "--reorder-check") {
      enable_reorder_check = true;
    } else if (arg == "--drop-out-of-order") {
      drop_out_of_order = true;
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  AORTA_LOG(INFO) << "cpp listener node=" << node->name() << " group=" << node->group()
                  << " topic=" << topic;
  AORTA_LOG(INFO) << "subscriber strategy: header_context="
                  << (enable_header_context ? "true" : "false")
                  << " dedup=" << (enable_dedup ? "true" : "false")
                  << " dedup_window=" << dedup_window
                  << " reorder_check=" << (enable_reorder_check ? "true" : "false")
                  << " drop_out_of_order=" << (drop_out_of_order ? "true" : "false");

  // Attach BEFORE creating any callback-capable primitive: the first executor
  // attached to a node owns it, so a subscriber created first would bind to the
  // SDK auto-executor and this one would spin without ever receiving an event.
  auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
  aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");

  aorta::SubscriberOptions sub_options;
  sub_options.enable_header_context = enable_header_context;
  sub_options.enable_dedup = enable_dedup;
  sub_options.dedup_window = dedup_window;
  sub_options.enable_reorder_check = enable_reorder_check;
  sub_options.drop_out_of_order = drop_out_of_order;

  auto subscriber = aorta::Must(node->CreateSubscriber(
      topic,
      [](const aorta::MessageView& msg, const aorta::MessageContextView& ctx) {
        std::vector<uint8_t> owned;
        const uint8_t* payload = msg.data;
        if (!payload && msg.size > 0) {
          owned = msg.ToVector();
          payload = owned.empty() ? nullptr : owned.data();
        }
        auto chat = payload ? demo::GetChat(payload) : nullptr;
        if (!chat) {
          AORTA_LOG(WARN) << "Failed to parse Chat message";
          return;
        }

        if (ctx.sequence != 0 || !ctx.publisher_node.empty()) {
          AORTA_LOG(INFO) << "ctx header: seq=" << ctx.sequence
                          << " node=" << ctx.publisher_node;
        }

        const char* text = chat->text() ? chat->text()->c_str() : "(null)";
        auto header = chat->aorta_header();

        if (header) {
          AORTA_LOG(INFO) << "[seq="
                          << static_cast<uint64_t>(header->sequence())
                          << " stamp="
                          << static_cast<uint64_t>(header->publish_stamp_ns())
                          << "] " << text;
        } else {
          AORTA_LOG(INFO) << "[no header] " << text;
        }
      },
      aorta::QoS(), sub_options),
      "Create subscriber");

  AORTA_LOG(INFO) << "Listening for messages...";
  while (g_running) {
    // SpinOnce returns TIMEOUT when no event arrives within the window —
    // a normal idle tick, not an error (treating it as fatal killed every
    // idle listener ~100ms after start; caught by the cross-language
    // interop matrix).
    aorta::Status status = executor->SpinOnce(std::chrono::milliseconds(100));
    if (!status.ok() && status.code() != AORTA_ERR_TIMEOUT) {
      aorta::CheckOk(status, "Executor::SpinOnce");
    }
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
