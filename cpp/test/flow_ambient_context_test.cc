// Promise-carried flow context: the ambient verb surface over the
// context core.
//
// Pinned here:
//   1. The FlowPromise concept (positive on every Task promise, negative
//      on a foreign promise). The REJECTION side (a foreign coroutine
//      actually awaiting flow surfaces) is a compile error by design and
//      is proven by the scripted expect-fail TUs in test/expectfail/.
//   2. Stamping (the nullability matrix):
//      spawn_on(exec)  → {rt = nullptr, exec, scope};
//      spawn_on(rt)    → {&rt, rt.fallback_executor(), scope};
//      spawn_on(DISABLED rt) → {&rt, fallback} — NOT nullptr (the
//      Blocker-2 rider: a Disabled runtime is a valid rollback plane).
//   3. Inheritance: a 3-deep child co_await chain sees the identical
//      context; priority still propagates (existing A1 behavior).
//   4. Ambient verb behavior: sleep on both planes, resume(), the
//      join/race/timeout/offload happy paths, ambient recv round-trip
//      over a real pull subscriber, and the null-rt refusal
//      (executor-only frame + ambient arbiter verb → cancelled_unwind;
//      the ERROR-log leg lives in flow_refusal_log_test, which owns the
//      process-wide logging config).
//   5. Scope ownership: the frame holds an OWNING Scope copy
//      — alive after the caller drops its handle — and the detached-root
//      final_suspend releases the last copy without incident (TSan
//      sweeps this in CI).
//   6. Ambient endpoint wrappers preserve the endpoint KIND (B1).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <tuple>
#include <utility>

#include "aorta/flow_readiness.hpp"
#include "aorta/flow_subscriber.hpp"
#include "aorta/node.h"
#include "aorta/publisher.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

// ---------------------------------------------------------------------------
// 1. Concept pins. A requires-probe cannot prove the
//    rejection (the teaching fallback makes the expression valid); the
//    concept itself is pinned here, the rejection by test/expectfail/.
// ---------------------------------------------------------------------------
static_assert(flow::detail::FlowPromise<flow::Task<int>::promise_type>);
static_assert(flow::detail::FlowPromise<flow::Task<void>::promise_type>);
static_assert(
    flow::detail::FlowPromise<flow::EndpointTask<int>::promise_type>);

struct foreign_promise_probe {
  uint8_t priority = 255;  // priority alone is NOT a flow context
};
static_assert(!flow::detail::FlowPromise<foreign_promise_probe>);

// ---------------------------------------------------------------------------
// 6. Ambient endpoint wrappers preserve the endpoint kind (the full
//    overload matrix lives in flow_endpoint_tag_test).
// ---------------------------------------------------------------------------
static_assert(flow::is_endpoint_task_v<decltype(flow::recv(
                  std::declval<aorta::PullSubscriber&>()))>);
static_assert(flow::is_endpoint_task_v<decltype(flow::recv_for(
                  std::declval<aorta::PullSubscriber&>(),
                  std::declval<std::chrono::nanoseconds>()))>);
static_assert(flow::is_endpoint_task_v<decltype(flow::aligned(
                  std::declval<flow::WindowSync&>()))>);
static_assert(flow::is_endpoint_task_v<decltype(flow::wait_wave(
                  std::declval<flow::ReadinessSet&>()))>);

// ---------------------------------------------------------------------------
// Harness: one core executor + a lane runtime attached as the facade
// (the flow_subscriber_test shape).
// ---------------------------------------------------------------------------
class AmbientHarness : public ::testing::Test {
 protected:
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

  template <typename Pred>
  static bool WaitFor(Pred pred, std::chrono::seconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    return pred();
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
};

// ---------------------------------------------------------------------------
// 2. Stamping — read the whole context back through this_coro (the
//    aggregate tag AND the sugar readers must agree).
// ---------------------------------------------------------------------------
struct CtxProbe {
  flow::LaneRuntime* rt = reinterpret_cast<flow::LaneRuntime*>(0x1);  // poison
  uint64_t exec = 0;
  uint64_t scope_handle = 0;
  bool scope_valid = false;
  bool scope_cancelled = true;
  uint8_t priority = 0;
  bool readers_agree = false;
  std::atomic<bool> done{false};
};

flow::Task<void> probe_ctx(CtxProbe& out) {
  auto ctx = co_await flow::this_coro::context;
  out.rt = ctx.rt;
  out.exec = ctx.exec;
  out.scope_handle = ctx.scope.handle();
  out.scope_valid = ctx.scope.valid();
  out.priority = ctx.priority;
  flow::Scope sugar_scope = co_await flow::this_coro::scope;
  out.scope_cancelled = sugar_scope.is_cancelled();
  auto* sugar_rt = co_await flow::this_coro::runtime;
  const uint64_t sugar_exec = co_await flow::this_coro::executor;
  out.readers_agree = sugar_rt == ctx.rt && sugar_exec == ctx.exec &&
                      sugar_scope.handle() == ctx.scope.handle();
  out.done.store(true, std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, ExecutorOnlySpawnStampsNullRtPlane) {
  flow::Scope scope = flow::Scope::root();
  CtxProbe p;
  auto s = flow::spawn_on(exec_, scope, probe_ctx(p));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(WaitFor([&] { return p.done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(p.rt, nullptr) << "executor-only plane: rt is ABSENT";
  EXPECT_EQ(p.exec, exec_);
  EXPECT_EQ(p.scope_handle, scope.handle());
  EXPECT_TRUE(p.scope_valid);
  EXPECT_FALSE(p.scope_cancelled);
  EXPECT_TRUE(p.readers_agree);
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(AmbientHarness, LaneSpawnStampsRuntimeAndFallbackExecutor) {
  flow::Scope scope = flow::Scope::root();
  CtxProbe p;
  auto s = flow::spawn_on(*rt_, scope, probe_ctx(p));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(WaitFor([&] { return p.done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(p.rt, rt_.get());
  EXPECT_EQ(p.exec, rt_->fallback_executor());
  EXPECT_EQ(p.scope_handle, scope.handle());
  EXPECT_TRUE(p.readers_agree);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> probe_ctx_after_ambient_sleep(CtxProbe& out) {
  // The Disabled-runtime arbiter leg: the timer arms on the runtime's
  // registry, the resume rides the fallback executor.
  co_await flow::sleep(1ms);
  co_await probe_ctx(out);
}

TEST(FlowAmbientRollback, DisabledRuntimeStampsRuntimeNotNull) {
  // The rollback delegation must NOT
  // collapse "disabled" into "absent" — ambient arbiter verbs keep
  // working on the queue path.
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);
  EXPECT_FALSE(facade.attached()) << "rollback = not-attaching";
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });

  flow::Scope scope = flow::Scope::root();
  CtxProbe p;
  auto s = flow::spawn_on(*rt, scope, probe_ctx_after_ambient_sleep(p));
  ASSERT_TRUE(s.ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!p.done.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(p.done.load(std::memory_order_seq_cst))
      << "ambient sleep must work on the Disabled rollback plane";
  EXPECT_EQ(p.rt, rt.get()) << "Disabled runtime stamps &rt, NOT nullptr";
  EXPECT_EQ(p.exec, rt->fallback_executor());
  ASSERT_TRUE(scope.close().ok());
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// ---------------------------------------------------------------------------
// 3. Inheritance — a 3-deep chain sees the identical context (the
//    unconditional parent→child copy), including the priority byte.
// ---------------------------------------------------------------------------
flow::Task<int> chain_leaf(CtxProbe& out) {
  co_await probe_ctx(out);
  co_return 7;
}
flow::Task<int> chain_mid(CtxProbe& out) { co_return co_await chain_leaf(out); }
flow::Task<void> chain_root(CtxProbe& root_out, CtxProbe& leaf_out,
                            std::atomic<bool>& done) {
  co_await probe_ctx(root_out);
  (void)co_await chain_mid(leaf_out);
  done.store(true, std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, ThreeDeepChildChainInheritsIdenticalContext) {
  flow::Scope scope = flow::Scope::root(
      {.priority = aorta::EventPriority::kHigh, .name = "ws1-inherit"});
  CtxProbe root_p;
  CtxProbe leaf_p;
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope, chain_root(root_p, leaf_p, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(WaitFor([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(leaf_p.rt, root_p.rt);
  EXPECT_EQ(leaf_p.exec, root_p.exec);
  EXPECT_EQ(leaf_p.scope_handle, root_p.scope_handle);
  EXPECT_EQ(leaf_p.scope_handle, scope.handle());
  EXPECT_EQ(leaf_p.priority, root_p.priority)
      << "the A1 priority byte still propagates through the chain";
  EXPECT_EQ(root_p.priority, 1) << "the root stamp comes from the scope";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// 4. Ambient verb behavior.
// ---------------------------------------------------------------------------
flow::Task<int> value_after_ambient_sleep(std::chrono::milliseconds d, int v) {
  co_await flow::sleep(d);
  co_return v;
}

flow::Task<void> ambient_verbs_body(std::atomic<int>& out) {
  co_await flow::resume();  // ambient hop (lane plane here)
  auto [a, b] = co_await flow::join(value_after_ambient_sleep(1ms, 1),
                                    value_after_ambient_sleep(2ms, 2));
  int w = co_await flow::race(value_after_ambient_sleep(1ms, 10),
                              value_after_ambient_sleep(200ms, 20));
  int t = co_await flow::timeout(2s, value_after_ambient_sleep(1ms, 100));
  int o = co_await flow::offload(aorta::EventPriority::kBackground,
                                 [] { return 1000; });
  if (co_await flow::resume().nothrow() != AORTA_OK) {
    co_return;
  }
  out.store(a + b + w + t + o, std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, AmbientCombinatorsHappyPathOnLanePlane) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(*rt_, scope, ambient_verbs_body(out));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      WaitFor([&] { return out.load(std::memory_order_seq_cst) != 0; }, 10s));
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 1 + 2 + 10 + 100 + 1000);
}

TEST(FlowAmbientRollback, AmbientCombinatorsRunOnDisabledRuntimeQueuePath) {
  // Rollback parity: the SAME ambient body — resume() +
  // join + race + timeout + offload — on a DISABLED runtime plane rides
  // the queue path (arbiter registry on the runtime object; resumes via
  // the fallback executor) and produces the identical result.
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);
  EXPECT_FALSE(facade.attached()) << "rollback = not-attaching";
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(*rt, scope, ambient_verbs_body(out));
  ASSERT_TRUE(s.ok());
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (out.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 1 + 2 + 10 + 100 + 1000)
      << "ambient join/race/timeout/offload/resume all rode the queue path";
  ASSERT_TRUE(scope.close().ok());
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

flow::Task<void> ambient_sleep_body(std::atomic<int>& stages) {
  co_await flow::sleep(1ms);  // parked leg
  stages.fetch_add(1, std::memory_order_seq_cst);
  co_await flow::sleep(0ms);  // 0-delay fast path (both legs share it)
  stages.fetch_add(1, std::memory_order_seq_cst);
  if (co_await flow::sleep(1ms).nothrow() == AORTA_OK) {
    stages.fetch_add(1, std::memory_order_seq_cst);
  }
  co_await flow::resume();  // ambient hop on this plane
  stages.fetch_add(1, std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, AmbientSleepOnLaneFrame) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> stages{0};
  auto s = flow::spawn_on(*rt_, scope, ambient_sleep_body(stages));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      WaitFor([&] { return stages.load(std::memory_order_seq_cst) == 4; }));
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(AmbientHarness, AmbientSleepOnExecutorOnlyFrame) {
  // rt == nullptr → the sleep_on (queue timer) machinery on the context
  // executor; resume() takes the executor leg too.
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> stages{0};
  auto s = flow::spawn_on(exec_, scope, ambient_sleep_body(stages));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      WaitFor([&] { return stages.load(std::memory_order_seq_cst) == 4; }));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> consume_one_ambient(aorta::PullSubscriber& sub,
                                     std::atomic<int>& got_bytes,
                                     std::atomic<bool>& done) {
  aorta::PullSample s = co_await flow::recv(sub);
  got_bytes.store(static_cast<int>(s.Size()), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, AmbientRecvRoundTrip) {
  auto node = aorta::Node::Create("flow_ambient_ctx_test");
  ASSERT_TRUE(node.ok()) << node.status().message();
  auto sub = node.value()->CreatePullSubscriber("test/flow_ambient_recv", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();
  auto pub = node.value()->CreatePublisher("test/flow_ambient_recv");
  ASSERT_TRUE(pub.ok()) << pub.status().message();
  std::this_thread::sleep_for(300ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> got_bytes{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_one_ambient(sub.value(), got_bytes, done));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // frame parks on recv()

  const std::string payload = "hello-ambient";
  ASSERT_TRUE(pub.value()
                  ->PublishBytes(
                      reinterpret_cast<const uint8_t*>(payload.data()),
                      payload.size())
                  .ok());
  ASSERT_TRUE(WaitFor([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(got_bytes.load(), static_cast<int>(payload.size()));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> ambient_recv_for_deadline(aorta::PullSubscriber& sub,
                                           std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::recv_for(sub, 30ms);  // nothing publishes
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(AmbientHarness, AmbientRecvForDeadlineUnwinds) {
  auto node = aorta::Node::Create("flow_ambient_ctx_test_ddl");
  ASSERT_TRUE(node.ok()) << node.status().message();
  auto sub =
      node.value()->CreatePullSubscriber("test/flow_ambient_recv_ddl", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          ambient_recv_for_deadline(sub.value(), unwound));
  ASSERT_TRUE(s.ok());
#if defined(__cpp_exceptions)
  EXPECT_TRUE(
      WaitFor([&] { return unwound.load(std::memory_order_seq_cst); }))
      << "the ambient recv_for window must ride the same arbiter group";
#endif
  ASSERT_TRUE(scope.close().ok());
}

// The null-rt refusal: an ambient arbiter verb on an executor-only
// frame unwinds cancelled_unwind (the ERROR-log assertion rides in
// flow_refusal_log_test, which owns the process logging config).
flow::Task<int> plain_value(int v) { co_return v; }

flow::Task<void> ambient_join_on_null_rt(std::atomic<int>& outcome) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::join(plain_value(1), plain_value(2));
    outcome.store(-1, std::memory_order_seq_cst);  // must not resolve
  } catch (const flow::cancelled_unwind&) {
    outcome.store(1, std::memory_order_seq_cst);
    co_return;
  }
#else
  outcome.store(1, std::memory_order_seq_cst);
  co_return;
#endif
}

TEST_F(AmbientHarness, AmbientJoinOnExecutorOnlyFrameRefuses) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s = flow::spawn_on(exec_, scope, ambient_join_on_null_rt(outcome));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      WaitFor([&] { return outcome.load(std::memory_order_seq_cst) != 0; }));
#if defined(__cpp_exceptions)
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 1)
      << "null-rt ambient join must unwind, not resolve";
#endif
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> ambient_recv_on_null_rt(aorta::PullSubscriber& sub,
                                         std::atomic<int>& outcome) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::recv(sub);
    outcome.store(-1, std::memory_order_seq_cst);
  } catch (const flow::cancelled_unwind&) {
    outcome.store(1, std::memory_order_seq_cst);
    co_return;
  }
#else
  outcome.store(1, std::memory_order_seq_cst);
  co_return;
#endif
}

TEST_F(AmbientHarness, AmbientRecvOnExecutorOnlyFrameRefuses) {
  // The Shape-2 wrappers refuse BEFORE delegating.
  auto node = aorta::Node::Create("flow_ambient_ctx_test_nullrt");
  ASSERT_TRUE(node.ok()) << node.status().message();
  auto sub =
      node.value()->CreatePullSubscriber("test/flow_ambient_recv_nullrt", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s = flow::spawn_on(exec_, scope,
                          ambient_recv_on_null_rt(sub.value(), outcome));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      WaitFor([&] { return outcome.load(std::memory_order_seq_cst) != 0; }));
#if defined(__cpp_exceptions)
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 1);
#endif
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// 5. Scope ownership: the frame's promise holds an
//    OWNING Scope copy — the scope stays valid inside the frame after
//    the caller drops every handle it had; the detached root's
//    final_suspend then releases the LAST copy (aorta_scope_free runs on
//    the lane worker) without incident.
// ---------------------------------------------------------------------------
flow::Task<void> scope_alive_after_caller_drop(std::atomic<int>& phase) {
  co_await flow::sleep(50ms);  // the caller drops its Scope copy meanwhile
  flow::Scope mine = co_await flow::this_coro::scope;
  phase.store(mine.valid() && !mine.is_cancelled() ? 1 : -1,
              std::memory_order_seq_cst);
}

TEST_F(AmbientHarness, FrameOwnsScopeCopyAndDetachedRootReleasesIt) {
  std::atomic<int> phase{0};
  {
    flow::Scope scope = flow::Scope::root({.name = "ws1-owning-copy"});
    auto s = flow::spawn_on(*rt_, scope, scope_alive_after_caller_drop(phase));
    ASSERT_TRUE(s.ok());
    scope = flow::Scope();  // drop the caller's LAST handle while parked
  }
  ASSERT_TRUE(
      WaitFor([&] { return phase.load(std::memory_order_seq_cst) != 0; }));
  EXPECT_EQ(phase.load(std::memory_order_seq_cst), 1)
      << "the promise copy must keep the scope alive (owning, not a raw "
         "handle)";
  // The frame has finished (or is finishing): its final_suspend destroys
  // the promise and with it the LAST Scope copy — aorta_scope_free on
  // the resume plane. Nothing to assert beyond "no crash"; give the
  // release a moment so TSan/ASan sweep the path deterministically.
  std::this_thread::sleep_for(50ms);
}

}  // namespace
