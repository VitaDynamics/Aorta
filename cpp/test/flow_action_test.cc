// Flow action verbs — REAL end-to-end (no mock FFI): a live
// ActionServer, a coroutine driving co_await send_goal / wait_result /
// cancel_goal, GoalGuard cancel-on-drop, and the cancelled-scope entry
// refusal. Exercises the aorta_action_client_*_ctx bridge through the
// typed C++ layer.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "aorta/flow_action.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/node.h"
#include "flow_resume_probe_msg.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;
using TestAction = ::aorta::action::example::FlowResumeProbeAction;

namespace {

::aorta::action::example::FlowResumeProbeGoalMsg make_goal(const char* source) {
  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string(source);
  return goal;
}

struct Outcomes {
  std::atomic<bool> done{false};
  std::atomic<bool> succeeded_ok{false};
  std::atomic<uint64_t> duration_ms{0};
  std::atomic<bool> cancel_accepted{false};
  std::atomic<int> cancel_code{0};
  std::atomic<bool> cancelled_terminal{false};
  std::atomic<int> cancelled_status{-1};
  std::atomic<bool> guard_cancelled_terminal{false};
  std::atomic<int> guard_status{-1};
  // D6: the entry-refusal outcome (case 4) is a Stopped::Cancelled — track
  // is_cancel() + the exact raw_reason() rather than a bare status code.
  std::atomic<bool> refusal_is_cancel{false};
  std::atomic<int> refusal_raw_reason{-1};
  // Regression pin (case 4b): the SAME entry refusal
  // as case 4, but with a DIFFERENT reason (kShutdown, not kExplicit) on
  // a SEPARATE dead scope — pins flow_action.hpp's
  // detail::action_ctx_awaiter::refuse_cancelled_entry(), which every
  // action verb's await_suspend now calls INLINE before touching the FFI
  // (gate 6.1 uniformity with every other ambient verb's entry gate).
  std::atomic<bool> refusal_shutdown_is_cancel{false};
  std::atomic<int> refusal_shutdown_raw_reason{-1};
  // B8 fail-safe: invalid Scope refuses (never a scope-less escape).
  std::atomic<bool> refusal_invalid_is_cancel{false};
  // D6 fidelity e2e (case 5): a scope cancelled WHILE a wait_result is
  // parked must resolve with the exact byte too (not just the pre-cancelled
  // entry-refusal path).
  std::atomic<bool> parked_cancel_is_cancel{false};
  std::atomic<int> parked_cancel_raw_reason{-1};
  std::atomic<bool> ambient_verbs_ok{false};
  // I-5 pin: the completion's continuation must
  // resume ON the executor dispatch thread — proving the routed
  // deliver_unbound thunk still travels aorta_executor_post_resume,
  // never an inline resolver-thread resume. Direct thread::id compare
  // (no hash collisions).
  std::atomic<std::thread::id> result_resume_tid{};
  // I-4 pin: a REAL flow::call success
  // completion, same assertion — the call's own routed thunk.
  std::atomic<std::thread::id> call_resume_tid{};
  std::atomic<bool> call_ok{false};
};

flow::Task<void> drive_action(
    std::shared_ptr<aorta::ActionClient<TestAction>> client, flow::Scope scope,
    uint64_t exec, uint64_t rpc_client, flow::Scope dead_scope,
    flow::Scope dead_shutdown_scope, Outcomes& out) {
  // 1) send → accepted → await terminal SUCCEEDED.
  auto accepted =
      co_await flow::send_goal{client, exec, make_goal("succeed"), scope};
  if (accepted.ok()) {
    auto result = co_await flow::wait_result{accepted.value(), exec,
                                              std::chrono::milliseconds(5000),
                                              scope};
    if (result.ok() &&
        result.value().status == aorta::ActionStatus::kSucceeded &&
        result.value().result.has_value()) {
      out.succeeded_ok.store(true, std::memory_order_seq_cst);
      out.duration_ms.store(result.value().result->duration_ms.value_or(0),
                            std::memory_order_seq_cst);
    }
    // I-5 pin: the statement AFTER the await runs where the delivery
    // trampoline resumed us — the executor spin thread.
    out.result_resume_tid.store(std::this_thread::get_id(),
                                std::memory_order_seq_cst);
  }

  // Pin: a REAL RPC round-trip through flow::call's routed
  // completion (explicit form, scope-LAST order) — post-await code runs on
  // the executor spin thread.
  {
    static const uint8_t ping[4] = {'p', 'i', 'n', 'g'};
    auto reply = co_await flow::call{rpc_client, exec, ping, sizeof(ping),
                                     scope};
    out.call_resume_tid.store(std::this_thread::get_id(),
                              std::memory_order_seq_cst);
    out.call_ok.store(reply.ok() && reply->size() == sizeof(ping),
                      std::memory_order_seq_cst);
  }

  // 1b) the AMBIENT verb forms: {scope, executor} come
  //     from THIS frame's promise context — all three verbs, zero handles
  //     threaded through the calls.
  auto ga = co_await flow::send_goal{client, make_goal("wait-cancel")};
  if (ga.ok()) {
    auto cancel = co_await flow::cancel_goal{ga.value()};
    auto result = co_await flow::wait_result{ga.value(),
                                              std::chrono::milliseconds(5000)};
    out.ambient_verbs_ok.store(
        cancel.ok() && result.ok() &&
            result.value().status == aorta::ActionStatus::kCanceled,
        std::memory_order_seq_cst);
  }

  // 2) checked cancel: a wait-cancel goal accepts the cancel and reaches
  //    the CANCELED terminal.
  auto g2 =
      co_await flow::send_goal{client, exec, make_goal("wait-cancel"), scope};
  if (g2.ok()) {
    auto cancel = co_await flow::cancel_goal{g2.value(), exec, scope};
    out.cancel_accepted.store(cancel.ok(), std::memory_order_seq_cst);
    // D6: cancel_goal's failure path is normally a REJECTION (a fault, via
    // CancelRejectedStatus) but stays diagnostic-safe if it were ever a
    // scope-cancel/deadline stop instead (raw_reason() rather than a fault
    // code).
    out.cancel_code.store(
        cancel.ok()
            ? 0
            : (cancel.stopped().is_fault() ? cancel.stopped().fault().code()
                                            : cancel.stopped().raw_reason()),
        std::memory_order_seq_cst);
    auto result = co_await flow::wait_result{g2.value(), exec,
                                              std::chrono::milliseconds(5000),
                                              scope};
    out.cancelled_terminal.store(
        result.ok() &&
            result.value().status == aorta::ActionStatus::kCanceled,
        std::memory_order_seq_cst);
    out.cancelled_status.store(
        result.ok() ? static_cast<int>(result.value().status) : -2,
        std::memory_order_seq_cst);
  }

  // 3) GoalGuard: dropping the armed guard fires a detached cancel.
  auto g3 =
      co_await flow::send_goal{client, exec, make_goal("wait-cancel"), scope};
  if (g3.ok()) {
    flow::AcceptedGoal<TestAction> probe = g3.value();  // copy for the wait
    { flow::GoalGuard<TestAction> guard(std::move(g3).value()); }  // drop
    auto result = co_await flow::wait_result{probe, exec,
                                              std::chrono::milliseconds(5000),
                                              scope};
    out.guard_cancelled_terminal.store(
        result.ok() &&
            result.value().status == aorta::ActionStatus::kCanceled,
        std::memory_order_seq_cst);
    out.guard_status.store(
        result.ok() ? static_cast<int>(result.value().status) : -2,
        std::memory_order_seq_cst);
  }

  // 4) entry refusal: a cancelled scope refuses send_goal INLINE (an
  //    RPC-layer error, not an unwind) — nothing goes on the wire. D6: the
  //    refusal is a Stopped::Cancelled riding the same run_in_ctx_scope
  //    checkpoint-first path as (5) below — the dead scope's EXACT cancel
  //    reason (kExplicit, stamped at test setup) survives through the C ABI
  //    completion record, not a folded generic CANCELLED.
  auto refused =
      co_await flow::send_goal{client, exec, make_goal("never"), dead_scope};
  if (!refused.ok()) {
    out.refusal_is_cancel.store(refused.stopped().is_cancel(),
                                std::memory_order_seq_cst);
    out.refusal_raw_reason.store(refused.stopped().raw_reason(),
                                 std::memory_order_seq_cst);
  }

  // 4b) refuse_cancelled_entry() regression pin: the SAME pre-cancelled
  //     entry refusal as (4), explicit form, but on a scope cancelled with
  //     kShutdown instead of kExplicit — proves the entry gate carries
  //     the scope's EXACT raw reason through, not a hardcoded/coincidental
  //     value that happens to match (4)'s reason.
  auto refused_shutdown = co_await flow::send_goal{
      client, exec, make_goal("never-shutdown"), dead_shutdown_scope};
  if (!refused_shutdown.ok()) {
    out.refusal_shutdown_is_cancel.store(
        refused_shutdown.stopped().is_cancel(), std::memory_order_seq_cst);
    out.refusal_shutdown_raw_reason.store(
        refused_shutdown.stopped().raw_reason(), std::memory_order_seq_cst);
  }

  // 4c) Fail-safe: an INVALID/moved-from Scope
  //     must refuse at entry (is_cancelled() reads true for invalid) —
  //     never run as a scope-less action (the Rust bridge would treat
  //     handle 0 as "no scope" and just execute).
  auto refused_invalid = co_await flow::send_goal{
      client, exec, make_goal("never-invalid"), flow::Scope()};
  out.refusal_invalid_is_cancel.store(
      !refused_invalid.ok() && refused_invalid.stopped().is_cancel(),
      std::memory_order_seq_cst);

  // 5) D6 fidelity e2e: cancelling the scope of a PARKED wait_result with a
  //    specific reason (kShutdown) must resolve with the exact byte too —
  //    the same completion-record carrier (4) exercises pre-cancelled, now
  //    observed mid-flight instead.
  auto g4 =
      co_await flow::send_goal{client, exec, make_goal("wait-cancel"), scope};
  if (g4.ok()) {
    flow::Scope parked_scope =
        flow::Scope::root({.name = "flow-action-parked"});
    std::thread canceller([parked_scope]() mutable {
      std::this_thread::sleep_for(50ms);
      parked_scope.cancel(flow::CancelReason::kShutdown);
    });
    auto result = co_await flow::wait_result{
        g4.value(), exec, std::chrono::milliseconds(5000), parked_scope};
    canceller.join();
    if (!result.ok()) {
      out.parked_cancel_is_cancel.store(result.stopped().is_cancel(),
                                        std::memory_order_seq_cst);
      out.parked_cancel_raw_reason.store(result.stopped().raw_reason(),
                                         std::memory_order_seq_cst);
    }
    // `parked_scope` has no spawned children (only "watched" by the
    // now-resolved wait_result) — no explicit close() needed, and close()
    // is reentrancy-guarded (AORTA_ERR_REENTRANT) from inside a dispatched
    // coroutine frame like this one anyway; the last copy going out of
    // scope frees the handle via the plain (non-blocking) free path.
    //
    // The server-side goal is still running (only the LOCAL wait stopped)
    // — cancel it for real so it does not linger for its own 10s
    // server-loop deadline past this test.
    (void)co_await flow::cancel_goal{g4.value(), exec, scope};
  }

  out.done.store(true, std::memory_order_seq_cst);
}

TEST(FlowAction, EndToEndSendAwaitCancelGuardAndRefusal) {
  auto exec_or = aorta::Executor::Create();
  ASSERT_TRUE(exec_or.ok());
  auto exec = exec_or.value();
  auto rt = flow::LaneRuntime::SingleThread();
  flow::FacadeAttachment facade(*rt, exec->handle());
  ASSERT_TRUE(facade.attached());

  auto server_node_or = aorta::Node::Create("flow_action_srv");
  ASSERT_TRUE(server_node_or.ok());
  auto server_node = server_node_or.value();
  auto client_node_or = aorta::Node::Create("flow_action_cli");
  ASSERT_TRUE(client_node_or.ok());
  auto client_node = client_node_or.value();
  ASSERT_TRUE(exec->AddNode(server_node).ok());
  ASSERT_TRUE(exec->AddNode(client_node).ok());
  std::thread spin([&exec] { (void)exec->Spin(); });
  const std::thread::id spin_tid = spin.get_id();

  aorta::ActionServerOptions options;
  options.supports_cancel = true;
  auto server_or = aorta::ActionServer<TestAction>::Serve(
      server_node, TestAction::ActionName(), options,
      [](aorta::RetainedActionGoal<TestAction> goal,
         aorta::ActionExecutionContext<TestAction> ctx) {
        const std::string source =
            goal.request()->goal()->source() != nullptr
                ? goal.request()->goal()->source()->str()
                : std::string();
        if (source == "succeed") {
          ::aorta::action::example::FlowResumeProbeResultMsg result;
          result.duration_ms = 42;
          (void)ctx.Succeed(result);
          return;
        }
        // wait-cancel: the REAL-server contract is synchronous completion —
        // returning without a terminal auto-aborts the goal. Block THIS
        // dispatch (a core runtime worker, not the executor) until the
        // cancel flag flips, then finish CANCELED.
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!ctx.IsCancelRequested() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::sleep_for(5ms);
        }
        (void)ctx.Cancel(aorta::ActionReplyCode::kCanceled, "flow cancel");
      });
  ASSERT_TRUE(server_or.ok()) << server_or.status().message();

  auto client_or =
      client_node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_or.ok());
  auto client = client_or.value();

  // Discovery: the client can see the server's info queryable.
  {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    for (;;) {
      if (client->Info().ok()) break;
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "action server never became discoverable";
      std::this_thread::sleep_for(20ms);
    }
  }

  flow::Scope scope = flow::Scope::root({.name = "flow-action-test"});
  flow::Scope dead = flow::Scope::root({.name = "flow-action-dead"});
  dead.cancel(flow::CancelReason::kExplicit);
  flow::Scope dead_shutdown =
      flow::Scope::root({.name = "flow-action-dead-shutdown"});
  dead_shutdown.cancel(flow::CancelReason::kShutdown);

  // I-4 pin plumbing: a loopback echo service + raw client on the
  // SAME executor; the fresh client's first call is gated on
  // WaitForMatching below (the deterministic discovery guarantee).
  auto echo_service_or = server_node->CreateService(
      "flow_action_echo",
      [](const aorta::MessageView& view, aorta::ServiceRequest&& req) {
        (void)std::move(req).Reply(view.ToVector());
      });
  ASSERT_TRUE(echo_service_or.ok());
  auto echo_service = echo_service_or.value();
  (void)echo_service;
  auto rpc_client_or = client_node->CreateClient("flow_action_echo");
  ASSERT_TRUE(rpc_client_or.ok());
  auto rpc_client = rpc_client_or.value();
  // Gate the fresh client's FIRST call on matching —
  // the publish-before-route-known race (client.h contract; core
  // client.rs:274). Deterministic, not "enough traffic went by".
  auto matched = rpc_client->WaitForMatching(10000);
  ASSERT_TRUE(matched.ok());
  ASSERT_TRUE(matched.value()) << "echo service never became routable";

  Outcomes out;
  auto spawned = flow::spawn_on(
      *rt, scope,
      drive_action(client, scope, exec->handle(), rpc_client->handle(), dead,
                   dead_shutdown, out));
  ASSERT_TRUE(spawned.ok());

  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (!out.done.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  ASSERT_TRUE(out.done.load()) << "coroutine mission did not complete";

  EXPECT_TRUE(out.succeeded_ok.load()) << "send_goal → wait_result happy path";
  EXPECT_EQ(out.duration_ms.load(), 42u) << "typed result decoded";
  EXPECT_TRUE(out.cancel_accepted.load())
      << "checked cancel accepted; code=" << out.cancel_code.load();
  EXPECT_TRUE(out.cancelled_terminal.load())
      << "cancel reaches CANCELED; status=" << out.cancelled_status.load();
  EXPECT_TRUE(out.guard_cancelled_terminal.load())
      << "GoalGuard drop cancels the goal; status=" << out.guard_status.load();
  EXPECT_TRUE(out.refusal_is_cancel.load())
      << "cancelled scope refuses entry as a Stopped::Cancelled, not a fault";
  EXPECT_EQ(out.refusal_raw_reason.load(),
            static_cast<int>(flow::CancelReason::kExplicit))
      << "the dead scope's exact cancel reason (kExplicit) survives the "
         "entry-refusal completion record";
  EXPECT_TRUE(out.refusal_shutdown_is_cancel.load())
      << "refuse_cancelled_entry(): a pre-cancelled scope refuses "
         "send_goal's entry as a Stopped::Cancelled, not a fault";
  EXPECT_TRUE(out.refusal_invalid_is_cancel.load())
      << "B8: an invalid Scope must refuse at entry, not run scope-less";
  EXPECT_EQ(out.refusal_shutdown_raw_reason.load(),
            static_cast<int>(flow::CancelReason::kShutdown))
      << "refuse_cancelled_entry(): the dead scope's EXACT cancel reason "
         "(kShutdown) survives the entry-refusal completion record — a "
         "different reason than case 4's kExplicit, so this cannot pass "
         "by coincidentally matching a hardcoded/folded value";
  EXPECT_TRUE(out.parked_cancel_is_cancel.load())
      << "a scope cancelled mid-wait_result resolves as Stopped::Cancelled";
  EXPECT_EQ(out.parked_cancel_raw_reason.load(), 3)
      << "D6 fidelity: the Shutdown reason (byte 3) survives the "
         "run_in_ctx_scope record for a PARKED wait_result, same as the "
         "pre-cancelled entry refusal";
  EXPECT_TRUE(out.ambient_verbs_ok.load())
      << "ambient send_goal/cancel_goal/wait_result ride the frame's "
         "promise context";
  EXPECT_EQ(out.result_resume_tid.load(), spin_tid)
      << "the routed completion still delivers through "
         "aorta_executor_post_resume — post-await code runs on the "
         "executor spin thread, never inline on the resolver";
  EXPECT_TRUE(out.call_ok.load())
      << "the real flow::call round-trip echoed";
  EXPECT_EQ(out.call_resume_tid.load(), spin_tid)
      << "flow::call's routed completion delivers through "
         "aorta_executor_post_resume onto the executor spin thread";

  EXPECT_TRUE(scope.close().ok());
  (void)dead.close();
  (void)dead_shutdown.close();
  (void)exec->Stop();
  spin.join();
  EXPECT_TRUE(facade.detach().ok());
}


// ---------------------------------------------------------------------------
// SerialDomain: the ACTION delivery_status_ per-caller stamp
// row, through the REAL action thunks — a BOUND frame's parked
// wait_result whose terminal outcome arrives AFTER the executor stopped
// takes the stamped CANCEL delivery (the frame UNWINDS; the RPC-layer
// Stopped-value path — case 5 above — does not unwind, which is exactly
// the two-layer distinction under test).
// ---------------------------------------------------------------------------

std::atomic<bool> g_serial_release_goal{false};

flow::Task<void> serial_bound_wait(
    std::shared_ptr<aorta::ActionClient<TestAction>> client, flow::Scope scope,
    uint64_t exec, std::atomic<int>* stage) {
  auto accepted =
      co_await flow::send_goal{client, exec, make_goal("hold"), scope};
  if (!accepted.ok()) {
    stage->store(-1, std::memory_order_seq_cst);
    co_return;
  }
  stage->store(1, std::memory_order_seq_cst);  // parked from here
  (void)co_await flow::wait_result{accepted.value(), exec,
                                   std::chrono::milliseconds(30000), scope};
  // Reached ONLY on a non-teardown delivery — the row expects the
  // unwind (delivery_status_ = CANCELLED stamped before the resume).
  stage->store(2, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowAction, SerialBoundWaitResultTeardownDeliveryStampsCancel) {
  // Server on its OWN executor: the terminal outcome must be deliverable
  // AFTER the client runtime's executor stopped.
  auto server_node_or = aorta::Node::Create("serial_action_srv");
  ASSERT_TRUE(server_node_or.ok());
  auto server_node = server_node_or.value();
  auto server_exec_or = aorta::Executor::Create();
  ASSERT_TRUE(server_exec_or.ok());
  auto server_exec = server_exec_or.value();
  ASSERT_TRUE(server_exec->AddNode(server_node).ok());
  std::thread server_spin([&server_exec] { (void)server_exec->Spin(); });

  g_serial_release_goal.store(false, std::memory_order_seq_cst);
  aorta::ActionServerOptions options;
  auto server_or = aorta::ActionServer<TestAction>::Serve(
      server_node, TestAction::ActionName(), options,
      [](aorta::RetainedActionGoal<TestAction> goal,
         aorta::ActionExecutionContext<TestAction> ctx) {
        (void)goal;
        // Hold the goal open (a core runtime worker blocks, not an
        // executor) until the test releases it.
        const auto deadline = std::chrono::steady_clock::now() + 30s;
        while (!g_serial_release_goal.load(std::memory_order_seq_cst) &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::sleep_for(2ms);
        }
        ::aorta::action::example::FlowResumeProbeResultMsg result;
        result.duration_ms = 1;
        (void)ctx.Succeed(result);
      });
  ASSERT_TRUE(server_or.ok()) << server_or.status().message();

  auto client_node_or = aorta::Node::Create("serial_action_cli");
  ASSERT_TRUE(client_node_or.ok());
  auto client_node = client_node_or.value();
  auto client_or =
      client_node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_or.ok());
  auto client = client_or.value();
  {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    for (;;) {
      if (client->Info().ok()) break;
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "action server never became discoverable";
      std::this_thread::sleep_for(20ms);
    }
  }

  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0}, {client_node->handle()});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "action-stamp");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  std::atomic<int> stage{0};
  auto h = rt->spawn(scope, d,
                     serial_bound_wait(client, scope, rt->executor(), &stage));
  ASSERT_TRUE(h.ok());
  {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (stage.load(std::memory_order_seq_cst) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(2ms);
    }
  }
  ASSERT_EQ(stage.load(), 1) << "the bound wait_result never parked";

  // External stop: the bridge closes the gate; the parked wait's
  // eventual delivery must take the STAMPED cancel.
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  g_serial_release_goal.store(true, std::memory_order_seq_cst);

  ASSERT_TRUE(h.wait_for(30s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok())
      << "the teardown delivery must UNWIND (delivery_status_ stamped "
         "CANCELLED before the resume) — never resolve a value";
  EXPECT_EQ(stage.load(), 1)
      << "post-await code ran despite the teardown delivery";

  ASSERT_TRUE(server_exec->Stop().ok());
  server_spin.join();
}


// ---------------------------------------------------------------------------
// SerialDomain: the ACTION F-2 leg — a BOUND
// frame's EXPLICIT action verb naming a foreign executor refuses at the
// stamp (refuse_serial_foreign), before any goal machinery exists. No
// server needed: the refusal precedes the FFI registration.
// ---------------------------------------------------------------------------

flow::Task<void> serial_foreign_send_goal(
    std::shared_ptr<aorta::ActionClient<TestAction>> client,
    uint64_t foreign_exec, flow::Scope scope, std::atomic<int>* verdict) {
  auto accepted = co_await flow::send_goal{client, foreign_exec,
                                           make_goal("never"), scope};
  if (!accepted.ok() && accepted.stopped().is_fault() &&
      accepted.stopped().fault().message().find("foreign executor") !=
          std::string::npos) {
    verdict->store(1, std::memory_order_seq_cst);
  } else {
    verdict->store(2, std::memory_order_seq_cst);
  }
  co_return;
}

TEST(FlowAction, SerialBoundForeignExecutorSendGoalRefusesAtStamp) {
  auto client_node_or = aorta::Node::Create("serial_f2_action_cli");
  ASSERT_TRUE(client_node_or.ok());
  auto client_node = client_node_or.value();
  auto client_or =
      client_node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_or.ok());
  auto client = client_or.value();

  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::SingleThread();  // the foreign executor
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "action-f2");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rta->root_scope();
  std::atomic<int> verdict{0};
  auto h = rta->spawn(scope, d,
                      serial_foreign_send_goal(client, rtb->executor(),
                                               scope, &verdict));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_EQ(verdict.load(), 1)
      << "a bound frame's explicit send_goal naming a foreign executor "
         "must refuse at the stamp with the [serial] fault (F-2)";
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
