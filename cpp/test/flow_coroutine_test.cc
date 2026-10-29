// Flow — the opt-in C++20 coroutine surface against
// the REAL C ABI: executor + reserved CoroutineResume lane + scope handles.
// Network-free: no zenoh session is opened.
//
// NOTE the coroutine style: state is passed as PARAMETERS (bound into the
// frame), never captured by a lambda — the closure object dies with the full
// expression while the frame lives on (the flow.hpp header-doc pitfall).

#include <aorta/flow.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>

namespace flow = aorta::flow;

namespace {

class FlowCoroutineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec_), AORTA_OK);
    spin_ = std::thread([this] {
      spin_tid_.store(std::this_thread::get_id(), std::memory_order_seq_cst);
      aorta_executor_spin(exec_);
    });
  }

  void TearDown() override {
    ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
    spin_.join();
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
  }

  uint64_t exec_ = 0;
  std::thread spin_;
  std::atomic<std::thread::id> spin_tid_{};
};

flow::Task<void> two_stage_body(uint64_t exec, std::atomic<int>& stage,
                                std::atomic<std::thread::id>& resumed_on) {
  stage.store(1, std::memory_order_seq_cst);
  // Hop (again) onto the executor: proves the post_resume lane round-trip.
  // (Unwind default: a teardown delivery would unwind this frame here.)
  co_await flow::resume_on{exec};
  resumed_on.store(std::this_thread::get_id(), std::memory_order_seq_cst);
  stage.store(2, std::memory_order_seq_cst);
  co_return;
}

// The router's UNBOUND passthrough
// contract — deliver_unbound runs exactly once, verbatim, and
// stamp_teardown is NOT invoked (no bound tier exists yet). This is the
// choke point every external-resumption site now funnels through
// (I-4 call, I-5 action, I-6 offload); the bit-identical
// pins are the existing call/action/offload suites staying green.
TEST(FlowRouteResume, UnboundPassthroughInvokesDeliverExactlyOnce) {
  struct Probe {
    int delivered = 0;
    int stamped = 0;
  } probe;
  flow::detail::RouteDelivery d{
      [](void* c) { static_cast<Probe*>(c)->delivered++; },
      [](void* c) { static_cast<Probe*>(c)->stamped++; }, nullptr, nullptr,
      &probe};
  flow::detail::route_resume(nullptr, std::coroutine_handle<>{}, d);
  EXPECT_EQ(probe.delivered, 1);
  EXPECT_EQ(probe.stamped, 0)
      << "the passthrough has no bound tier: teardown stamping must not "
         "fire on it";
}

TEST_F(FlowCoroutineTest, SpawnedCoroutineRunsOnTheExecutorAndCloseWaits) {
  flow::Scope scope = flow::Scope::root({.name = "cpp-flow-test"});
  ASSERT_TRUE(scope.valid());

  std::atomic<int> stage{0};
  std::atomic<std::thread::id> resumed_on{};

  auto spawned =
      flow::spawn_on(exec_, scope, two_stage_body(exec_, stage, resumed_on));
  ASSERT_TRUE(spawned.ok());

  // close() blocks until the coroutine frame truly completes
  // (final_suspend balances the accounting slot).
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(stage.load(std::memory_order_seq_cst), 2);
  EXPECT_EQ(resumed_on.load(std::memory_order_seq_cst),
            spin_tid_.load(std::memory_order_seq_cst))
      << "the resumed slice must run on the executor's dispatch thread";
}

flow::Task<void> poll_until_cancelled(uint64_t exec, flow::Scope scope,
                                      std::atomic<bool>& observed) {
  // Cooperative loop: poll the RT-safe flag between executor hops
  // (executor teardown unwinds the frame at the await — the default).
  while (!scope.is_cancelled()) {
    co_await flow::resume_on{exec};
  }
  observed.store(true, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, CancelIsObservableThroughTheRtSafeFlag) {
  flow::Scope root = flow::Scope::root();
  flow::Scope child = root.child();
  EXPECT_FALSE(child.is_cancelled());

  std::atomic<bool> observed_cancel{false};
  auto spawned =
      flow::spawn_on(exec_, child, poll_until_cancelled(exec_, child, observed_cancel));
  ASSERT_TRUE(spawned.ok());

  root.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(child.close().ok());
  EXPECT_TRUE(observed_cancel.load(std::memory_order_seq_cst));
  EXPECT_TRUE(child.is_cancelled()) << "cancel cascades root -> child";
  EXPECT_EQ(child.checkpoint().code(), AORTA_ERR_CANCELLED);
}

TEST_F(FlowCoroutineTest, BudgetNarrowsAndExpiryIsObserved) {
  flow::Scope scope = flow::Scope::root({.budget = std::chrono::nanoseconds(1)});  // expires at once
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  auto remaining = scope.remaining();
  ASSERT_TRUE(remaining.has_value());
  EXPECT_EQ(remaining->count(), 0);
  EXPECT_EQ(scope.checkpoint().code(), AORTA_ERR_CANCELLED)
      << "the first checkpoint observes expiry";
  EXPECT_TRUE(scope.is_cancelled());
}

flow::Task<void> trivial_body() { co_return; }

TEST_F(FlowCoroutineTest, SpawnIntoAClosingScopeIsRefused) {
  flow::Scope scope = flow::Scope::root();
  ASSERT_TRUE(scope.close().ok());  // one-way closing gate
  auto spawned = flow::spawn_on(exec_, scope, trivial_body());
  EXPECT_EQ(spawned.status, AORTA_ERR_CANCELLED)
      << "a closing scope refuses new tasks (no orphan past close)";
}

flow::Task<void> mark_ran(std::atomic<bool>& ran) {
  ran.store(true, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, StoppedExecutorPostIsRefusedAndResumesInline) {
  uint64_t stopped_exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &stopped_exec), AORTA_OK);
  ASSERT_EQ(aorta_executor_stop(stopped_exec), AORTA_OK);

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> ran_inline{false};
  auto spawned = flow::spawn_on(stopped_exec, scope, mark_ran(ran_inline));
  EXPECT_EQ(spawned.status, AORTA_ERR_CANCELLED);
  EXPECT_TRUE(ran_inline.load(std::memory_order_seq_cst))
      << "a refused post resumes inline (resume, never destroy)";
  ASSERT_TRUE(scope.close().ok()) << "the inline run balanced accounting";
  ASSERT_EQ(aorta_executor_free(stopped_exec), AORTA_OK);
}

}  // namespace

// RAII sentinel: proves the frame's DESTRUCTORS ran during the unwind.
struct UnwindSentinel {
  std::atomic<bool>* flag;
  ~UnwindSentinel() { flag->store(true, std::memory_order_seq_cst); }
};

flow::Task<void> ping_pong_until_torn_down(uint64_t exec,
                                           std::atomic<bool>& unwound) {
  UnwindSentinel sentinel{&unwound};
  for (;;) {
    // Default behavior: the stop-drain delivery UNWINDS here — no status
    // check can be forgotten; the sentinel destructor is the proof.
    co_await flow::resume_on{exec};
  }
}

flow::Task<void> ping_pong_nothrow(uint64_t exec, std::atomic<bool>& refused) {
  for (;;) {
    // Expert opt-out keeps the status-branch style covered.
    if (co_await flow::resume_on{exec}.nothrow() != AORTA_OK) {
      refused.store(true, std::memory_order_seq_cst);
      co_return;
    }
  }
}

TEST_F(FlowCoroutineTest, StopResumesInFlightFramesIntoCancellation) {
  // An ACCEPTED resume must never be silently
  // dropped by a concurrent executor stop — the stop drain resumes it into
  // cancellation, so the frame completes and close() returns.
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  std::atomic<bool> refused{false};
  auto spawned =
      flow::spawn_on(exec_, scope, ping_pong_until_torn_down(exec_, unwound));
  ASSERT_TRUE(spawned.ok());
  auto nothrow_probe =
      flow::spawn_on(exec_, scope, ping_pong_nothrow(exec_, refused));
  ASSERT_TRUE(nothrow_probe.ok());

  std::this_thread::sleep_for(std::chrono::milliseconds(30));  // let them spin
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);

  // No timeout crutch: if a frame leaked, close() would hang and the
  // bazel test timeout would flag it.
  ASSERT_TRUE(scope.close().ok())
      << "every accepted resume was resumed into cancellation";
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst))
      << "unwind default: destructors ran during teardown";
  EXPECT_TRUE(refused.load(std::memory_order_seq_cst))
      << "nothrow opt-out still observes the raw status";
}

TEST_F(FlowCoroutineTest, MultiWorkerExecutorDispatchesSpawnedFrames) {
  aorta_executor_sched_options_t opts{};
  opts.worker_count = 3;
  opts.sched = 1;
  uint64_t multi = 0;
  ASSERT_EQ(
      aorta_executor_new_with_sched_options(AORTA_EXECUTOR_MULTI, &opts, &multi),
      AORTA_OK);
  std::thread multi_spin([multi] { aorta_executor_spin(multi); });

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> completed{0};
  for (int i = 0; i < 8; ++i) {
    auto spawned = flow::spawn_on(
        multi, scope,
        [](uint64_t exec, std::atomic<int>& done) -> flow::Task<void> {
          co_await flow::resume_on{exec};
          done.fetch_add(1, std::memory_order_seq_cst);
          co_return;
        }(multi, completed));
    ASSERT_TRUE(spawned.ok());
  }
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(completed.load(std::memory_order_seq_cst), 8);

  ASSERT_EQ(aorta_executor_stop(multi), AORTA_OK);
  multi_spin.join();
  ASSERT_EQ(aorta_executor_free(multi), AORTA_OK);
}

TEST_F(FlowCoroutineTest, InvalidAndMovedFromScopesYieldInvalidChildren) {
  // Handle 0 must never silently mean "new root".
  flow::Scope invalid;
  EXPECT_FALSE(invalid.valid());
  EXPECT_FALSE(invalid.child().valid());

  flow::Scope source = flow::Scope::root();
  flow::Scope moved = std::move(source);
  EXPECT_TRUE(moved.valid());
  EXPECT_FALSE(source.child().valid())  // NOLINT(bugprone-use-after-move)
      << "a moved-from scope must not mint a new root";
  ASSERT_TRUE(moved.close().ok());
}

TEST_F(FlowCoroutineTest, ScopeCopiesShareOwnershipAndCloseIsIdempotent) {
  // Copies act on the SAME scope; the last owner frees exactly once.
  flow::Scope a = flow::Scope::root({.name = "shared"});
  flow::Scope b = a;  // copy: shared ownership
  EXPECT_EQ(a.handle(), b.handle());
  a.cancel(flow::CancelReason::kShutdown);
  EXPECT_TRUE(b.is_cancelled()) << "copies observe the same cancel state";
  ASSERT_TRUE(a.close().ok());
  ASSERT_TRUE(b.close().ok()) << "close is idempotent across copies";
}

flow::Task<void> co_owner_probe(uint64_t exec, flow::Scope scope,
                                std::atomic<bool>& observed_cancel) {
  // The frame CO-OWNS its Scope copy: even after every caller-side handle
  // is gone, this poll reads valid shared state.
  while (!scope.is_cancelled()) {
    co_await flow::resume_on{exec};
  }
  observed_cancel.store(true, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, FrameCoOwnsItsScopePastTheCallersLastHandle) {
  std::atomic<bool> observed{false};
  flow::Scope keeper;  // taken before the original dies, to cancel/close
  {
    flow::Scope original = flow::Scope::root({.name = "temp-owner"});
    keeper = original;
    auto spawned =
        flow::spawn_on(exec_, original, co_owner_probe(exec_, original, observed));
    ASSERT_TRUE(spawned.ok());
    // `original` dies HERE; the frame's by-value copy keeps the scope alive.
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  keeper.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(keeper.close().ok());
  EXPECT_TRUE(observed.load(std::memory_order_seq_cst));
}

TEST_F(FlowCoroutineTest, OutcomeValueAndStoppedRoundTripThroughTheFactories) {
  // Outcome<T> is built ONLY via the
  // Value()/Stop() factories — no aggregate {status, value}, and no
  // ToStatus() flattening (that would re-lose the reason vocabulary; map
  // explicitly at terminal edges via stopped() instead).
  auto ok_result = flow::Outcome<int>::Value(7);
  EXPECT_TRUE(ok_result.ok());
  EXPECT_EQ(*ok_result, 7);

  auto cancelled = flow::Outcome<int>::Stop(
      flow::Stopped::Cancelled(flow::CancelReason::kExplicit));
  EXPECT_FALSE(cancelled.ok());
  EXPECT_TRUE(cancelled.stopped().is_cancel());
  EXPECT_FALSE(cancelled.stopped().is_fault());
  EXPECT_EQ(cancelled.stopped().cancel_reason(), flow::CancelReason::kExplicit);

  auto faulted = flow::Outcome<int>::Stop(
      flow::Stopped::Fault(aorta::Status(AORTA_ERR_INTERNAL, "boom")));
  EXPECT_FALSE(faulted.ok());
  EXPECT_TRUE(faulted.stopped().is_fault());
  EXPECT_EQ(faulted.stopped().fault().code(), AORTA_ERR_INTERNAL);
  EXPECT_EQ(faulted.stopped().fault().message(), "boom")
      << "the fault's diagnostic text survives verbatim";
}

TEST_F(FlowCoroutineTest, CoInvokeMakesCapturingLambdasSafe) {
  // The dangling repro, made safe — the lambda AND the
  // source string are destroyed before the frame ever resumes; co_invoke's
  // wrapper frame owns copies.
  flow::Scope scope = flow::Scope::root({.name = "co-invoke"});
  auto out = std::make_shared<std::string>();
  flow::Task<void> wrapped = [&] {
    std::string secret = "frame-owned";
    auto lambda = [secret, out](uint64_t exec) -> flow::Task<void> {
      co_await flow::resume_on{exec};
      *out = secret;  // capture must still be alive here
      co_return;
    };
    return flow::co_invoke(std::move(lambda), exec_);
  }();  // lambda + secret die HERE, before the frame runs
  auto spawned = flow::spawn_on(exec_, scope, std::move(wrapped));
  ASSERT_TRUE(spawned.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(*out, "frame-owned");
}

TEST_F(FlowCoroutineTest, SpawnWithInjectsACoOwnedScope) {
  // spawn_with hands the body a by-value Scope copy; the
  // caller drops every handle before the body observes cancellation.
  auto observed = std::make_shared<std::atomic<bool>>(false);
  flow::Scope keeper;
  {
    flow::Scope scope = flow::Scope::root({.name = "spawn-with"});
    keeper = scope;
    auto spawned = flow::spawn_with(
        exec_, scope,
        [observed](flow::Scope body_scope, uint64_t exec) -> flow::Task<void> {
          while (!body_scope.is_cancelled()) {
            co_await flow::resume_on{exec};
          }
          observed->store(true, std::memory_order_seq_cst);
          co_return;
        },
        exec_);
    ASSERT_TRUE(spawned.ok());
  }  // caller's `scope` copy dies here
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  keeper.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(keeper.close().ok());
  EXPECT_TRUE(observed->load(std::memory_order_seq_cst));
}

flow::Task<int> inner_value_task() { co_return 21; }

flow::Task<void> outer_awaits_inner(std::atomic<int>& out) {
  int v = co_await inner_value_task();  // Task<T> value plumbing + eptr path
  out.store(v * 2, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, NestedTaskValueAndCompletionPropagate) {
  flow::Scope scope = flow::Scope::root({.name = "nested"});
  std::atomic<int> out{0};
  auto spawned = flow::spawn_on(exec_, scope, outer_awaits_inner(out));
  ASSERT_TRUE(spawned.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 42);
}

flow::Task<void> throwing_body(uint64_t exec) {
  co_await flow::resume_on{exec};
  throw std::runtime_error("deliberate fault");
}

flow::Task<void> sibling_watches(uint64_t exec, flow::Scope scope,
                                 std::atomic<uint8_t>& reason_out) {
  while (!scope.is_cancelled()) {
    co_await flow::resume_on{exec};
  }
  reason_out.store(1, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, EscapedExceptionFailsFastLikeARustPanic) {
  // A throw out of a Task body no longer terminates the
  // process — it cancels the (non-supervised) scope with CHILD_FAILED,
  // exactly like a Rust child panic.
  flow::Scope scope = flow::Scope::root({.name = "fail-fast"});
  std::atomic<uint8_t> sibling_saw{0};
  auto watcher =
      flow::spawn_on(exec_, scope, sibling_watches(exec_, scope, sibling_saw));
  ASSERT_TRUE(watcher.ok());
  auto thrower = flow::spawn_on(exec_, scope, throwing_body(exec_));
  ASSERT_TRUE(thrower.ok());

  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(scope.is_cancelled()) << "fail-fast cancelled the scope";
  EXPECT_EQ(sibling_saw.load(std::memory_order_seq_cst), 1);
}

TEST_F(FlowCoroutineTest, SupervisedScopeContainsAnEscapedException) {
  flow::Scope root = flow::Scope::root({.name = "supervised-root"});
  flow::Scope probe = root.child({.supervision = flow::Supervision::kIsolate, .name = "probe"});
  auto thrower = flow::spawn_on(exec_, probe, throwing_body(exec_));
  ASSERT_TRUE(thrower.ok());
  ASSERT_TRUE(probe.close().ok());
  EXPECT_FALSE(root.is_cancelled())
      << "supervised containment: the failure never reaches the parent";
  ASSERT_TRUE(root.close().ok());
}

flow::Task<void> nothrow_normal_path(uint64_t exec, std::atomic<bool>& ok) {
  // Coverage gap: the .nothrow() NORMAL path (live executor → AORTA_OK).
  if (co_await flow::resume_on{exec}.nothrow() == AORTA_OK) {
    ok.store(true, std::memory_order_seq_cst);
  }
  co_return;
}

TEST_F(FlowCoroutineTest, NothrowNormalPathReturnsOk) {
  flow::Scope scope = flow::Scope::root({.name = "nothrow-ok"});
  std::atomic<bool> ok{false};
  auto spawned = flow::spawn_on(exec_, scope, nothrow_normal_path(exec_, ok));
  ASSERT_TRUE(spawned.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(ok.load(std::memory_order_seq_cst));
}

flow::Task<void> swallowing_catch_all(uint64_t exec,
                                      std::atomic<int>& post_swallow) {
  // Pins the DOCUMENTED (bad) behavior: a bare non-rethrowing catch(...)
  // swallows the teardown unwind and the frame keeps running — the caveat
  // in the flow.hpp preamble. If a future mitigation changes this,
  // this test changes with it.
  try {
    for (;;) {
      co_await flow::resume_on{exec};
    }
  } catch (...) {
    // deliberately swallow — DO NOT copy this pattern into real code
  }
  post_swallow.fetch_add(1, std::memory_order_seq_cst);
  co_return;
}

TEST_F(FlowCoroutineTest, BareCatchAllSwallowsTheUnwindAsDocumented) {
  flow::Scope scope = flow::Scope::root({.name = "swallow"});
  std::atomic<int> post{0};
  auto spawned =
      flow::spawn_on(exec_, scope, swallowing_catch_all(exec_, post));
  ASSERT_TRUE(spawned.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(post.load(std::memory_order_seq_cst), 1)
      << "the frame ran past the swallow — the documented caveat";
}

TEST_F(FlowCoroutineTest, EscapedExceptionReasonIsChildFailed) {
  // Coverage gap: assert the REASON byte, not just that a cancel
  // happened.
  flow::Scope scope = flow::Scope::root({.name = "reason-check"});
  auto thrower = flow::spawn_on(exec_, scope, throwing_body(exec_));
  ASSERT_TRUE(thrower.ok());
  ASSERT_TRUE(scope.close().ok());
  const uint8_t* flag = nullptr;
  ASSERT_EQ(aorta_scope_cancelled_flag_ptr(scope.handle(), &flag), AORTA_OK);
  EXPECT_EQ(reinterpret_cast<const std::atomic<uint8_t>*>(flag)->load(
                std::memory_order_acquire),
            AORTA_CANCEL_REASON_CHILD_FAILED);
}

flow::Task<void> record_order(std::shared_ptr<std::vector<int>> order,
                              std::shared_ptr<std::mutex> m, int tag) {
  {
    std::lock_guard<std::mutex> lock(*m);
    order->push_back(tag);
  }
  co_return;
}

TEST_F(FlowCoroutineTest, ScopePriorityOrdersLaneDispatch) {
  // A1 end-to-end: frames spawned from a HIGH-priority scope
  // dispatch before LOW ones under contention. Deterministic: both
  // first-slice posts queue while the executor is NOT yet spinning; the
  // priority scheduler then orders them.
  aorta_executor_sched_options_t opts{};
  opts.worker_count = 1;
  opts.sched = 1;  // priority mode
  uint64_t exec = 0;
  ASSERT_EQ(
      aorta_executor_new_with_sched_options(AORTA_EXECUTOR_SINGLE, &opts, &exec),
      AORTA_OK);

  flow::Scope root = flow::Scope::root({.name = "prio-root"});
  flow::Scope lo =
      root.child({.priority = aorta::EventPriority::kLow, .name = "prio-lo"});
  flow::Scope hi =
      root.child({.priority = aorta::EventPriority::kHigh, .name = "prio-hi"});
  auto order = std::make_shared<std::vector<int>>();
  auto m = std::make_shared<std::mutex>();

  // LOW spawned FIRST — FIFO would run it first; priority must not.
  ASSERT_TRUE(flow::spawn_on(exec, lo, record_order(order, m, 0)).ok());
  ASSERT_TRUE(flow::spawn_on(exec, hi, record_order(order, m, 1)).ok());

  std::thread spin([exec] { aorta_executor_spin(exec); });
  ASSERT_TRUE(lo.close().ok());
  ASSERT_TRUE(hi.close().ok());
  ASSERT_TRUE(root.close().ok());
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);

  ASSERT_EQ(order->size(), 2u);
  EXPECT_EQ((*order)[0], 1) << "high-priority scope dispatched first";
  EXPECT_EQ((*order)[1], 0);
}

flow::Task<void> periodic_via_sleep(uint64_t exec, flow::Scope scope,
                                    std::atomic<int>& ticks) {
  while (!scope.is_cancelled()) {
    co_await flow::sleep_on{exec, std::chrono::milliseconds(5)};
    ticks.fetch_add(1, std::memory_order_seq_cst);
  }
}

TEST_F(FlowCoroutineTest, SleepOnTicksAndUnwindsOnStop) {
  // The periodic idiom without busy-spinning — and executor stop
  // resumes a SLEEPING frame into cancellation promptly.
  flow::Scope scope = flow::Scope::root({.name = "sleeper"});
  std::atomic<int> ticks{0};
  ASSERT_TRUE(
      flow::spawn_on(exec_, scope, periodic_via_sleep(exec_, scope, ticks))
          .ok());
  // Poll, don't assume a 40ms window fits 3 ticks + dispatch latency
  // (TSan/ASan CI runs 2-3x slower).
  const auto tick_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ticks.load(std::memory_order_seq_cst) < 3 &&
         std::chrono::steady_clock::now() < tick_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_GE(ticks.load(std::memory_order_seq_cst), 3) << "it ticks";

  auto started = std::chrono::steady_clock::now();
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  ASSERT_TRUE(scope.close().ok());
  auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_LT(elapsed, std::chrono::seconds(2))
      << "a sleeping frame was cancelled promptly, not after its delay";
}
