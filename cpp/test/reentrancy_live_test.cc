#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "aorta/action.h"
#include "aorta/client.h"
#include "aorta/executor.h"
#include "aorta/node.h"
#include "aorta/publisher.h"
#include "aorta/pull_subscriber.h"
#include "aorta/subscriber.h"
#include "aorta/timer.h"
#include "flow_resume_probe_msg.h"

namespace aorta {
namespace {

using TestAction = ::aorta::action::example::FlowResumeProbeAction;

const char* kSendGoalId = "11111111-2222-3333-4444-555555555555";

::aorta::action::example::FlowResumeProbeGoalMsg MakeProbeGoal() {
  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("reentrancy");
  return goal;
}

TEST(ReentrancyLiveDeathTest,
     SingleThreadedTimerSelfDestructionAbortsBeforeDrainWait) {
  EXPECT_DEATH(
      {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        auto node = Node::Create("reentrancy_live_timer").value();
        auto executor =
            Executor::Create(ExecutorType::kSingleThreaded).value();
        if (!executor->AddNode(node).ok()) {
          std::abort();
        }
        std::shared_ptr<Timer> timer;
        timer = node
                    ->CreateTimer(std::chrono::milliseconds(1),
                                  [&] { timer.reset(); })
                    .value();
        (void)executor->Spin();
      },
      "Aorta fatal: reentrancy guard: Timer::~Timer");
}

TEST(ReentrancyLiveDeathTest,
     ClientCallFromSubscriberCallbackAbortsUnderMode2) {
  // Class-B check on Client::Call: the subscriber callback runs on the
  // executor dispatch thread (detector set), so mode=2 must abort at the
  // C++ call site before the C call is even reached.
  EXPECT_DEATH(
      {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        auto node = Node::Create("reentrancy_live_call_node", "default").value();
        auto executor =
            Executor::Create(ExecutorType::kSingleThreaded).value();
        if (!executor->AddNode(node).ok()) {
          std::abort();
        }
        auto client = node->CreateClient("reentrancy/live_call_svc", 1000).value();
        auto sub = node
                       ->CreateSubscriber(
                           "reentrancy/live_call_topic",
                           [client](const MessageView&,
                                    const MessageContextView&) {
                             (void)client->Call(
                                 std::vector<uint8_t>{1, 2, 3});
                           })
                       .value();
        std::thread spin([&executor] { (void)executor->Spin(); });
        auto pub = node->CreatePublisher("reentrancy/live_call_topic", QoS(),
                                         PublisherOptions())
                       .value();
        const uint8_t payload[] = {'x'};
        // Publish until the callback fires; the abort kills the child.
        for (int i = 0; i < 300; ++i) {
          (void)pub->PublishBytes(payload, 1);
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      },
      "Aorta fatal: reentrancy guard: Client::Call");
}

TEST(ReentrancyLiveDeathTest,
     SendGoalWithIdFromSubscriberCallbackAbortsUnderMode2) {
  // The send-goal check lives at the shared commit point
  // (SendGoalPayloadTraced), so the SendGoalWithId entry is covered too:
  // mode=2 must abort at the C++ call site before the C call is reached.
  EXPECT_DEATH(
      {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        auto node =
            Node::Create("reentrancy_live_sendgoal_node", "default").value();
        auto executor =
            Executor::Create(ExecutorType::kSingleThreaded).value();
        if (!executor->AddNode(node).ok()) {
          std::abort();
        }
        auto client = node
                          ->CreateActionClient<TestAction>(
                              "reentrancy/live_sendgoal", ActionClientOptions{})
                          .value();
        auto sub = node
                       ->CreateSubscriber(
                           "reentrancy/live_sendgoal_topic",
                           [client](const MessageView&,
                                    const MessageContextView&) {
                             (void)client->SendGoalWithId(kSendGoalId,
                                                           MakeProbeGoal());
                           })
                       .value();
        std::thread spin([&executor] { (void)executor->Spin(); });
        auto pub = node->CreatePublisher("reentrancy/live_sendgoal_topic",
                                         QoS(), PublisherOptions())
                       .value();
        const uint8_t payload[] = {'x'};
        // Publish until the callback fires; the abort kills the child.
        for (int i = 0; i < 300; ++i) {
          (void)pub->PublishBytes(payload, 1);
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      },
      "Aorta fatal: reentrancy guard: ActionClient::SendGoal");
}

TEST(ReentrancyLiveTest, PullProbesFromCallbackProduceNoDiagnosticUnderMode2) {
  // Class P: TryRecv and Recv(timeout=0) must NOT carry a check. Run under
  // mode=2 in a forked child: a false-positive check inside the callback
  // would abort the child and fail the EXPECT_EXIT. The parse-once mode
  // static is per-process, so the mode=2 leg must live in its own process.
  // Forked BEFORE any parent-process test creates a zenoh session: a fork
  // after sessions exist inherits locked/vanished-thread state and the
  // child's own session creation dies with SIGTRAP.
  EXPECT_EXIT(
      {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        auto node =
            Node::Create("reentrancy_live_pull_node", "default").value();
        auto executor =
            Executor::Create(ExecutorType::kSingleThreaded).value();
        if (!executor->AddNode(node).ok()) {
          std::abort();
        }
        auto pull = std::make_shared<PullSubscriber>(
            node->CreatePullSubscriber("reentrancy/live_pull_topic", 16)
                .value());
        std::atomic<bool> ran{false};
        auto sub = node
                       ->CreateSubscriber(
                           "reentrancy/live_trigger_topic",
                           [&](const MessageView&, const MessageContextView&) {
                             auto t = pull->TryRecv();
                             EXPECT_TRUE(t.ok());
                             auto r = pull->Recv(std::chrono::nanoseconds(0));
                             EXPECT_TRUE(r.ok());
                             ran.store(true);
                           })
                       .value();
        std::thread spin([&executor] { (void)executor->Spin(); });
        auto pub = node->CreatePublisher("reentrancy/live_trigger_topic", QoS(),
                                         PublisherOptions())
                       .value();
        const uint8_t payload[] = {'x'};
        for (int i = 0; i < 300 && !ran.load(); ++i) {
          (void)pub->PublishBytes(payload, 1);
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_TRUE(ran.load());
        (void)executor->Stop();
        spin.join();
        _exit(0);
      },
      testing::ExitedWithCode(0), "");
}

TEST(ReentrancyLiveTest, ClientCallFromCallbackReturnsReentrantWithModeOff) {
  // Same call with the diagnostics OFF (env unset): the check is inert and
  // the C entry itself refuses with AORTA_ERR_REENTRANT.
  auto node = Node::Create("reentrancy_live_call_node", "default").value();
  auto executor = Executor::Create(ExecutorType::kSingleThreaded).value();
  ASSERT_TRUE(executor->AddNode(node).ok());
  auto client = node->CreateClient("reentrancy/live_call_svc", 1000).value();
  std::atomic<bool> ran{false};
  std::atomic<int> observed{0};
  auto sub = node
                 ->CreateSubscriber(
                     "reentrancy/live_call_topic",
                     [&](const MessageView&, const MessageContextView&) {
                       auto result =
                           client->Call(std::vector<uint8_t>{1, 2, 3});
                       observed.store(result.status().code());
                       ran.store(true);
                     })
                 .value();
  std::thread spin([&executor] { (void)executor->Spin(); });
  auto pub = node->CreatePublisher("reentrancy/live_call_topic", QoS(),
                                   PublisherOptions())
                 .value();
  const uint8_t payload[] = {'x'};
  for (int i = 0; i < 300 && !ran.load(); ++i) {
    (void)pub->PublishBytes(payload, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(ran.load());
  EXPECT_EQ(observed.load(), AORTA_ERR_REENTRANT);
  (void)executor->Stop();
  spin.join();
}

TEST(ReentrancyLiveTest,
     SendGoalWithIdFromCallbackReturnsReentrantWithModeOff) {
  // Same call with the diagnostics OFF (env unset): the check at the
  // shared send-goal commit point is inert and the C entry itself
  // refuses with AORTA_ERR_REENTRANT.
  auto node =
      Node::Create("reentrancy_live_sendgoal_node", "default").value();
  auto executor = Executor::Create(ExecutorType::kSingleThreaded).value();
  ASSERT_TRUE(executor->AddNode(node).ok());
  auto client = node
                    ->CreateActionClient<TestAction>("reentrancy/live_sendgoal",
                                                     ActionClientOptions{})
                    .value();
  std::atomic<bool> ran{false};
  std::atomic<int> observed{0};
  auto sub = node
                 ->CreateSubscriber(
                     "reentrancy/live_sendgoal_topic",
                     [&](const MessageView&, const MessageContextView&) {
                       auto result =
                           client->SendGoalWithId(kSendGoalId, MakeProbeGoal());
                       observed.store(result.status().code());
                       ran.store(true);
                     })
                 .value();
  std::thread spin([&executor] { (void)executor->Spin(); });
  auto pub = node->CreatePublisher("reentrancy/live_sendgoal_topic", QoS(),
                                   PublisherOptions())
                 .value();
  const uint8_t payload[] = {'x'};
  for (int i = 0; i < 300 && !ran.load(); ++i) {
    (void)pub->PublishBytes(payload, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(ran.load());
  EXPECT_EQ(observed.load(), AORTA_ERR_REENTRANT);
  (void)executor->Stop();
  spin.join();
}


}  // namespace
}  // namespace aorta
