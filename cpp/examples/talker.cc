#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  (void)aorta::InitLoggingFromEnv();

  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  sigaddset(&signal_set, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
    AORTA_LOG(ERROR) << "Failed to block signal mask";
    return 1;
  }

  std::string node_name = "cpp_talker";
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

  AORTA_LOG(INFO) << "cpp talker node=" << node->name() << " group=" << node->group()
                  << " topic=" << topic;

  auto publisher = aorta::Must(node->CreatePublisher<demo::Chat>(topic),
                               "Create publisher");

  auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
  aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");

  auto cb_group = aorta::Must(
      executor->CreateCallbackGroup(aorta::CallbackGroup::kMutuallyExclusive),
      "Create callback group");

  aorta::TimerOptions timer_options;
  timer_options.callback_group_handle = cb_group;

  auto counter = std::make_shared<uint64_t>(0);
  auto timer = aorta::Must(node->CreateTimer(
      std::chrono::milliseconds(500),
      [publisher, counter, node_name = node->name()]() {
        const uint64_t current = (*counter)++;
        char payload[256];
        std::snprintf(payload, sizeof(payload), "hello %" PRIu64 " from %s",
                      current, node_name.c_str());

        aorta::CheckOk(
            publisher->PublishTyped<demo::Chat>(
                [&](demo::ChatMsg& msg) { msg.text = std::string(payload); }),
            "Publish typed chat");
      },
      timer_options),
      "Create timer");
  (void)timer;

  std::thread signal_thread([executor, signal_set]() mutable {
    int signal_number = 0;
    if (sigwait(&signal_set, &signal_number) == 0) {
      AORTA_LOG(INFO) << "Signal received (" << signal_number
                      << "), stopping executor...";
      aorta::CheckOk(executor->Stop(), "Executor::Stop");
    }
  });

  AORTA_LOG(INFO) << "Talker timer started: period=500ms. Press Ctrl+C to exit.";
  auto spin_status = executor->Spin();
  if (!spin_status.ok()) {
    AORTA_LOG(ERROR) << "Executor spin failed: " << spin_status.message();
  }
  if (signal_thread.joinable()) {
    signal_thread.join();
  }
  AORTA_LOG(INFO) << "Shutting down...";
  return spin_status.ok() ? 0 : 1;
  });
}
