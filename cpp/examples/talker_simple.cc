#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cpp_talker_simple";
  std::string group = "default";
  std::string topic = "/demo/chatter";

  // Parse command line arguments
  for (int i = 1; i < argc - 1; i++) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--topic") {
      topic = argv[++i];
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  AORTA_LOG(INFO) << "cpp talker_simple node=" << node->name()
                  << " group=" << node->group() << " topic=" << topic;

  auto publisher = aorta::Must(node->CreatePublisher<demo::Chat>(topic),
                               "Create publisher");

  uint64_t counter = 0;
  while (g_running.load(std::memory_order_relaxed)) {
    char payload[256];
    std::snprintf(payload, sizeof(payload), "hello %" PRIu64 " from %s",
                  counter, node->name().c_str());

    aorta::CheckOk(
        publisher->PublishTyped<demo::Chat>(
            [&](demo::ChatMsg& msg) { msg.text = std::string(payload); }),
        "Publish typed chat");

    counter++;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
