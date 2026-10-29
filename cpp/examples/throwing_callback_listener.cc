// A runnable demo of subscriber-callback
// panic isolation. The subscriber's
// callback unconditionally throws after the first N messages —
// the dispatcher in `cpp/src/subscriber.cc` catches the exception
// (the `extern "C"` boundary requires `noexcept`) and
// keeps the subscription alive. Run this against any
// `/demo/chatter` publisher (for example `talker_simple`) and
// observe that:
//
//   1. The first N messages print normally.
//   2. Each subsequent message logs a single "subscriber callback
//      threw" warning (rate-limited) — the process does NOT
//      terminate.
//   3. The `in_flight` counter (printed on Ctrl-C) returns to 0
//      because the RAII scope guard in the dispatcher decrements
//      it on the way out of `catch (...)`.
//
// Pair with: bazel run //cpp/examples:talker_simple

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

namespace {

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cpp_throwing_listener";
  std::string group = "default";
  std::string topic = "/demo/chatter";
  std::size_t throw_after = 3;  // first 3 messages print normally.

  for (int i = 1; i < argc - 1; ++i) {
    const std::string arg = argv[i];
    if (arg == "--name") node_name = argv[++i];
    else if (arg == "--group") group = argv[++i];
    else if (arg == "--topic") topic = argv[++i];
    else if (arg == "--throw-after") throw_after = std::stoul(argv[++i]);
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Node");

  std::atomic<std::size_t> received{0};
  std::atomic<std::size_t> thrown{0};

  // Attach BEFORE the subscriber: the first executor attached to a node owns
  // it, so creating the subscriber first would bind it to the SDK
  // auto-executor and leave this one spinning empty.
  auto executor = aorta::Must(aorta::Executor::Create(), "Executor");
  aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");

  aorta::SubscriberOptions sub_options;
  auto subscriber = aorta::Must(
      node->CreateSubscriber<demo::Chat>(
          topic,
          [&received, &thrown, throw_after](const aorta::MessageView& msg,
                                            const aorta::MessageContextView&) {
            const auto n = received.fetch_add(1) + 1;
            const auto* chat = msg.data ? demo::GetChat(msg.data) : nullptr;
            const char* text = chat && chat->text() ? chat->text()->c_str()
                                                    : "(null)";
            std::cout << "[" << n << "] received: " << text << std::endl;

            if (n > throw_after) {
              thrown.fetch_add(1);
              // Throwing here exercises the panic-isolation path.
              // The dispatcher's `try { ... } catch (...)` in
              // `cpp/src/subscriber.cc` swallows + rate-limits the
              // log, then the RAII scope guard runs, decrementing
              // `in_flight` so subscriber teardown can still drain.
              throw std::runtime_error(
                  "intentional throw from demo callback");
            }
          },
          aorta::QoS(), sub_options),
      "CreateSubscriber");

  std::cout << "Listening on '" << topic << "' — callback throws after "
            << throw_after << " messages." << std::endl;
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

  std::cout << "\nshutdown — received=" << received.load()
            << " threw=" << thrown.load() << std::endl;
  std::cout << "process survived " << thrown.load()
            << " in-callback exceptions (panic isolation works)."
            << std::endl;
  return 0;
  });
}
