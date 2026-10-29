// B2 misuse distinguishability: a REFUSED await (second concurrent
// awaiter on a single-awaiter surface → INVALID_ARGUMENT, stale/freed
// handle → NOT_FOUND) logs at ERROR — with the raw code and the surface
// name — before the generic `cancelled_unwind` flattening. Lifecycle
// resolutions (stop, scope cancel, destroy-wake) stay quiet.
//
// OWN BINARY on purpose: the core's logging config is a process-wide
// once (OnceLock). The suite claims it FIRST with a file sink, then
// asserts on the file — deterministic, no ordering dependence on other
// tests' auto-init.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "aorta/flow_action.hpp"
#include "aorta/flow_readiness.hpp"
#include "aorta/flow_subscriber.hpp"
#include "aorta/logging.h"
#include "aorta/node.h"
#include "flow_resume_probe_msg.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

const std::string& LogFilePath() {
  static const std::string path = [] {
    const char* tmp = std::getenv("TEST_TMPDIR");
    std::string dir = (tmp != nullptr && *tmp != '\0') ? tmp : "/tmp";
    return dir + "/flow_refusal_log_test.log";
  }();
  return path;
}

std::string ReadLogFile() {
  std::ifstream in(LogFilePath());
  std::ostringstream out;
  out << in.rdbuf();
  return out.str();
}

// The file sink is tracing_appender::non_blocking — a worker thread
// drains it asynchronously, so poll with a deadline (the suite's usual
// wait-loop shape) instead of reading once.
[[nodiscard]] bool WaitForLogSubstring(const std::string& needle,
                                       std::chrono::seconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (ReadLogFile().find(needle) != std::string::npos) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

class RefusalLogHarness : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Claim the process-wide logging once with a file sink BEFORE any
    // FFI write can auto-init it from env. Default filter passes ERROR.
    aorta::LogConfig cfg;
    cfg.file_path = LogFilePath();
    cfg.stderr_enabled = false;
    ASSERT_TRUE(aorta::ConfigureLogging(cfg).ok());
    // Probe: prove the file sink is live (a lost config race would
    // otherwise surface as a confusing per-test timeout).
    AORTA_LOG(ERROR) << "flow-refusal-log-test-probe";
    ASSERT_TRUE(WaitForLogSubstring("flow-refusal-log-test-probe"))
        << "log file sink did not come up at " << LogFilePath();
  }

  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::SingleThread();
    attachment_ = std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached());
    spin_ = std::thread([this] { aorta_executor_spin(exec_); });
  }

  void TearDown() override {
    (void)aorta_executor_stop(exec_);
    if (spin_.joinable()) spin_.join();
    attachment_.reset();
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
};

// Scope-full explicit form (the scope-less overload was removed): the
// un-cancelled scope branch is a bystander; the pinned refusal still
// comes from the window's set_notify on the SECOND concurrent awaiter.
flow::Task<void> consume_aligned_forever(flow::LaneRuntime& rt,
                                         flow::WindowSync& ws,
                                         flow::Scope scope,
                                         std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      (void)co_await flow::aligned(ws, rt, scope);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(RefusalLogHarness, SecondAlignedAwaiterRefusalLogsError) {
  // The SecondAlignedAwaiterIsRefused shape (order-free F5 contract),
  // plus the B2 assertion: the refusal — INVALID_ARGUMENT from the
  // window's set_notify — must hit the log at ERROR with the surface
  // name and the symbolic code before the frame unwinds.
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound_a{false};
  std::atomic<bool> unwound_b{false};
  auto a = flow::spawn_on(
      *rt_, scope, consume_aligned_forever(*rt_, *ws, scope, unwound_a));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(
      *rt_, scope, consume_aligned_forever(*rt_, *ws, scope, unwound_b));
  ASSERT_TRUE(b.ok());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound_a.load() && !unwound_b.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_NE(unwound_a.load(), unwound_b.load())
      << "F5: exactly one awaiter is refused";
  EXPECT_TRUE(WaitForLogSubstring("flow::aligned: await REFUSED"))
      << "the refusal did not reach the log at ERROR";
  EXPECT_TRUE(WaitForLogSubstring("INVALID_ARGUMENT", 1s))
      << "the symbolic refusal code is missing";
#endif
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> aligned_stale_scope_arm(flow::LaneRuntime& rt,
                                         flow::WindowSync& ws,
                                         std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::detail::aligned_loop(ws.native_handle(), &rt,
                                              /*window_ns=*/0,
                                              /*scope_handle=*/0xdead'beef);
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(RefusalLogHarness, StaleScopeArmRefusalLogsNotFound) {
  // The AuxArmFailure shape: `aorta_scope_arm_arbiter` on a stale scope
  // handle refuses NOT_FOUND — a programming error the fail_armed ladder
  // then flattens to a generic cancel. B2: the NOT_FOUND must be logged.
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> refused{false};
  auto b = flow::spawn_on(*rt_, scope,
                          aligned_stale_scope_arm(*rt_, *ws, refused));
  ASSERT_TRUE(b.ok());
#if defined(__cpp_exceptions)
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!refused.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(refused.load());
  EXPECT_TRUE(WaitForLogSubstring("NOT_FOUND"))
      << "the stale-scope refusal code is missing from the log";
#endif
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> wave_on_handle(flow::LaneRuntime& rt, uint64_t set_handle,
                                std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::detail::wave_loop(set_handle, &rt, /*window_ns=*/0,
                                           /*scope_handle=*/0);
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(RefusalLogHarness, WaitWaveOnFreedSetLogsError) {
  // Entry-time misuse (NOT the parked destroy-wake, which is a designed
  // lifecycle path and stays quiet): starting wait_wave against an
  // already-freed set refuses NOT_FOUND at the baseline probe.
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());
  const uint64_t stale = set->native_handle();
  set->reset();  // freed BEFORE the wait starts

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope, wave_on_handle(*rt_, stale, unwound));
  ASSERT_TRUE(s.ok());
#if defined(__cpp_exceptions)
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(unwound.load());
  EXPECT_TRUE(WaitForLogSubstring("flow::wait_wave: await REFUSED"))
      << "the freed-set entry refusal did not reach the log at ERROR";
#endif
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<int> null_rt_probe_value(int v) { co_return v; }

flow::Task<void> ambient_join_on_executor_frame(std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::join(null_rt_probe_value(1),
                              null_rt_probe_value(2));
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(RefusalLogHarness, AmbientArbiterVerbOnNullRtLogsInvalidArgument) {
  // An ambient arbiter-family verb on an executor-only
  // frame (promise context rt == nullptr) is a refusal-class event —
  // ERROR log at the refusal site naming the fix, then the standard
  // cancelled_unwind.
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(exec_, scope,
                          ambient_join_on_executor_frame(unwound));
  ASSERT_TRUE(s.ok());
#if defined(__cpp_exceptions)
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(unwound.load()) << "the null-rt refusal must unwind the frame";
  EXPECT_TRUE(WaitForLogSubstring("flow::join: await REFUSED"))
      << "the null-rt refusal did not reach the log at ERROR";
  EXPECT_TRUE(WaitForLogSubstring("Spawn this task on a LaneRuntime", 1s))
      << "the refusal message must name the fix";
#endif
  ASSERT_TRUE(scope.close().ok());
}

// Recorded design deviation: the ambient call/action
// forms share the explicit awaiter classes (the mandated ctor/brace
// spellings), so a FOREIGN (non-flow) coroutine awaiting them cannot be
// rejected at compile time — the refusal is loud at RUNTIME instead:
// raw INVALID_ARGUMENT through the RPC layer (.nothrow() / StatusOr) and
// the B2 ERROR log at the refusal site.
struct ForeignRunner {
  struct promise_type {  // deliberately NOT a flow promise
    ForeignRunner get_return_object() { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() {}
  };
};

// Every await below resolves INLINE (the refusal returns false from
// await_suspend), so these coroutines run to completion synchronously.
//
// Outcome no longer carries a raw status code (no
// `.status` member); the refusal here is always a fault
// (Stopped::Fault(AORTA_ERR_INVALID_ARGUMENT, ...) — see flow.hpp's
// ambient-call-on-a-non-flow-frame branch), so read it through
// stopped().fault().code().
ForeignRunner foreign_ambient_call(std::atomic<int>& delivery,
                                   std::atomic<int>& code) {
  const uint8_t payload[1] = {0};
  auto r = co_await flow::call(/*client=*/0, payload, 0).nothrow();
  delivery.store(r.delivery, std::memory_order_seq_cst);
  code.store(r.reply.ok() ? 0 : r.reply.stopped().fault().code(),
            std::memory_order_seq_cst);
}

using RefusalTestAction = ::aorta::action::example::FlowResumeProbeAction;

ForeignRunner foreign_ambient_send_goal(std::atomic<int>& code) {
  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("never");
  // Null client is safe here: the ambient-unbound refusal fires BEFORE
  // the awaiter ever touches the client handle.
  auto sr = co_await flow::send_goal{
      std::shared_ptr<aorta::ActionClient<RefusalTestAction>>(), goal};
  // D6: the ambient-unbound refusal is a Stopped::Fault(INVALID_ARGUMENT)
  // (see flow_action.hpp's refuse_unbound_ambient), not a raw status code.
  code.store(sr.ok() ? 0 : sr.stopped().fault().code(),
            std::memory_order_seq_cst);
}

TEST_F(RefusalLogHarness, AmbientCallAndActionOnForeignCoroutineRefuse) {
  std::atomic<int> call_delivery{-1};
  std::atomic<int> call_code{0};
  foreign_ambient_call(call_delivery, call_code);  // runs synchronously
  EXPECT_EQ(call_delivery.load(), AORTA_OK)
      << "the refusal is RPC-layer, not a teardown delivery";
  EXPECT_EQ(call_code.load(), AORTA_ERR_INVALID_ARGUMENT)
      << "ambient call on a foreign frame surfaces the raw code via "
         ".nothrow()";
  EXPECT_TRUE(WaitForLogSubstring("flow::call: await REFUSED"))
      << "the foreign-frame ambient call refusal did not reach the log";

  std::atomic<int> goal_code{0};
  foreign_ambient_send_goal(goal_code);  // runs synchronously
  EXPECT_EQ(goal_code.load(), AORTA_ERR_INVALID_ARGUMENT)
      << "ambient send_goal on a foreign frame surfaces INVALID_ARGUMENT "
         "through Outcome::stopped().fault()";
  EXPECT_TRUE(WaitForLogSubstring("flow::send_goal: await REFUSED"))
      << "the foreign-frame ambient send_goal refusal did not reach the log";
}

flow::Task<void> consume_recv_forever(flow::LaneRuntime& rt, flow::Scope scope,
                                      aorta::PullSubscriber& sub,
                                      std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      (void)co_await flow::recv(sub, rt, scope);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(RefusalLogHarness, SecondRecvAwaiterRefusalLogsError) {
  // The SecondRecvAwaiterIsRefused shape over a real pull subscriber:
  // the losing awaiter's set_notify refusal (INVALID_ARGUMENT) must log
  // at ERROR under the "flow::recv" surface name.
  auto node = aorta::Node::Create("flow_refusal_log_test");
  ASSERT_TRUE(node.ok()) << node.status().message();
  auto sub = node.value()->CreatePullSubscriber("test/flow_refusal_log", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();
  std::this_thread::sleep_for(100ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound_a{false};
  std::atomic<bool> unwound_b{false};
  auto a = flow::spawn_on(
      *rt_, scope, consume_recv_forever(*rt_, scope, sub.value(), unwound_a));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(
      *rt_, scope, consume_recv_forever(*rt_, scope, sub.value(), unwound_b));
  ASSERT_TRUE(b.ok());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound_a.load() && !unwound_b.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_NE(unwound_a.load(), unwound_b.load())
      << "F5: exactly one recv() awaiter is refused";
  EXPECT_TRUE(WaitForLogSubstring("flow::recv: await REFUSED"))
      << "the recv refusal did not reach the log at ERROR";
#endif
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
