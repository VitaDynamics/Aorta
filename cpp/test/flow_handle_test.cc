// TaskHandle: the joinable spawn (Rust `Scope::spawn` →
// `TaskHandle<T>` parity). `spawn_on` returns a handle whose completion is
// an arbiter branch: `co_await handle` yields `Outcome<T>`; a discarded
// handle is today's fire-and-forget spawn (the scope still owns the task's
// lifetime — the handle only carries the result).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

// The combinator-test harness: a lane pool + one drainer thread.
class HandleHarness {
 public:
  HandleHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~HandleHarness() {
    rt_->pool().close();
    stop_.store(true, std::memory_order_release);
    drainer_.join();
    rt_->pool().drain_all();
  }
  flow::LaneRuntime& rt() { return *rt_; }

 private:
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::atomic<bool> stop_{false};
  std::thread drainer_;
};

template <typename Pred>
bool wait_until(Pred pred, std::chrono::seconds budget = 5s) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (!pred() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

flow::Task<int> value_after(std::chrono::milliseconds d, int v) {
  if (d.count() > 0) {
    co_await flow::sleep(d);  // ambient — arbiter timer on the frame's rt
  }
  co_return v;
}

flow::Task<void> flag_after(std::chrono::milliseconds d,
                            std::atomic<bool>& flag) {
  if (d.count() > 0) {
    co_await flow::sleep(d);
  }
  flag.store(true, std::memory_order_seq_cst);
}

// Outcome no longer carries a raw status code (no `.status`
// member), so classify a Stopped explicitly for these tests'
// single-int out channel. A fault keeps its real code; every cancel-kind
// (explicit/deadline/shutdown) folds to AORTA_ERR_CANCELLED — the exact
// signal these tests asserted before the Stopped vocabulary landed.
int stop_signal(const flow::Stopped& s) {
  return s.is_fault() ? s.fault().code()
                      : static_cast<int>(AORTA_ERR_CANCELLED);
}

// --- co_await handle → Outcome<T> -----------------------------------------

flow::Task<void> await_value_handle(flow::TaskHandle<int> h,
                                    std::atomic<int>& out,
                                    std::atomic<bool>& done) {
  flow::Outcome<int> o = co_await std::move(h);
  // Test values are positive; aorta status codes are negative — the sign
  // tells value-vs-stop apart in the single out channel.
  out.store(o.ok() ? *o : stop_signal(o.stopped()),
            std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowHandle, AwaitDeliversTheChildValue) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, value_after(5ms, 42));
  ASSERT_TRUE(child.ok());
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_value_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 42);
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowHandle, LateAwaitTakesTheFastPath) {
  // The child completes long before anyone awaits: the await must resolve
  // immediately (done fast path — no arbiter group is ever created).
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, value_after(0ms, 7));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_value_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 7);
  ASSERT_TRUE(scope.close().ok());
}

// --- fire-and-forget: a discarded handle changes nothing -------------------

TEST(FlowHandle, DiscardedHandleStillRunsTheTask) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> ran{false};
  {
    auto ignored = flow::spawn_on(h.rt(), scope, flag_after(0ms, ran));
    ASSERT_TRUE(ignored.ok());
  }  // handle dropped here; the scope owns the task's lifetime
  ASSERT_TRUE(wait_until([&] { return ran.load(std::memory_order_seq_cst); }));
  ASSERT_TRUE(scope.close().ok());
}

// --- take_outcome(): the synchronous seam after scope.close() -------------

TEST(FlowHandle, TakeOutcomeAfterCloseReadsTheValue) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, value_after(5ms, 99));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(scope.close().ok());  // BLOCKS until the child finished
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(**o, 99);
  // One-shot: a second take yields nothing.
  EXPECT_FALSE(child.take_outcome().has_value());
}

TEST(FlowHandle, TakeOutcomeBeforeCompletionIsEmpty) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, value_after(50ms, 1));
  ASSERT_TRUE(child.ok());
  EXPECT_FALSE(child.take_outcome().has_value());  // not done yet — and NOT
                                                   // consumed by the probe
  ASSERT_TRUE(scope.close().ok());
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok());
}

// --- child stopped by a deadline → cancelled outcome, not a value ----------

flow::Task<int> never_finishes_inside(std::chrono::milliseconds window) {
  // timeout() unwinds this frame with cancelled_unwind when the window
  // elapses — the handle must classify that as a CANCELLED outcome. The
  // loser branch (400ms) drains structurally under the scope, so keep it
  // short: scope.close() at the test tail waits it out.
  int v = co_await flow::timeout(window, value_after(400ms, 1));
  co_return v;
}

TEST(FlowHandle, CancelledChildYieldsCancelledOutcome) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child =
      flow::spawn_on(h.rt(), scope, never_finishes_inside(10ms));
  ASSERT_TRUE(child.ok());
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_value_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst),
            static_cast<int>(AORTA_ERR_CANCELLED));
  ASSERT_TRUE(scope.close().ok());
}

// --- child failure → fault outcome (supervised: contained, Rust parity) ---

flow::Task<int> throwing_child() {
  co_await flow::resume();  // move off the spawn slice first
  throw std::runtime_error("boom");
}

TEST(FlowHandle, FailedChildYieldsFaultOutcome) {
  HandleHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // contain ChildFailed to this subtree
  flow::Scope nursery = root.child(opts);
  auto child = flow::spawn_on(h.rt(), nursery, throwing_child());
  ASSERT_TRUE(child.ok());
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), root, await_value_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  const int seen = out.load(std::memory_order_seq_cst);
  EXPECT_EQ(seen, static_cast<int>(AORTA_ERR_INTERNAL))
      << "failure classifies as fault, not cancel";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- spawn refusal: the handle carries the admission status ---------------

TEST(FlowHandle, SpawnOnClosedScopeRefusalReadsShutdown) {
  // Read-after-move regression (TCK `close_vs_racing_spawn` finding): the
  // spawn_on refusal lambda moved the Scope into spawn_detached, then
  // spawn_with_handle read raw_cancel_reason() off the MOVED-FROM object —
  // the B8 invalid-scope fail-safe turned every closed-scope refusal into
  // kExplicit. The real state (closed, never cancelled, no reason byte)
  // must flow to build_stopped's honest Shutdown default — Rust parity:
  // a post-close spawn resolves Stopped::cancelled(Shutdown).
  HandleHarness harness;
  flow::Scope root = flow::Scope::root();
  flow::Scope closed = root.child();
  ASSERT_TRUE(closed.close().ok());
  auto h = flow::spawn_on(harness.rt(), closed,
                          []() -> flow::Task<int> { co_return 1; }());
  EXPECT_FALSE(h.ok());
  ASSERT_TRUE(h.wait_for(std::chrono::seconds(5)));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  ASSERT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<uint8_t>(flow::CancelReason::kShutdown));
  ASSERT_TRUE(root.close().ok());
}

TEST(FlowHandle, SpawnOnCancelledScopeRefusesAndAwaitResolves) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  flow::Scope doomed = scope.child();
  doomed.cancel();
  ASSERT_TRUE(doomed.close().ok());
  auto child = flow::spawn_on(h.rt(), doomed, value_after(0ms, 1));
  EXPECT_FALSE(child.ok()) << "spawn admission must refuse a closed scope";
  // The refused handle still resolves (immediately) instead of parking.
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_value_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_LT(out.load(std::memory_order_seq_cst), 0)
      << "the admission refusal (a negative status code) is the outcome";
  ASSERT_TRUE(scope.close().ok());
}

// --- misuse: double await refuses loudly (B2) ------------------------------

flow::Task<void> double_await(flow::TaskHandle<int> h, std::atomic<int>& legs,
                              std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await h;
    legs.fetch_add(1, std::memory_order_seq_cst);
    (void)co_await h;  // second await on the SAME handle: refusal
    legs.fetch_add(100, std::memory_order_seq_cst);  // must not run
  } catch (...) {
    // cancelled_unwind is internal; observing "we unwound" is enough. The
    // rethrow keeps the frame's teardown semantics honest.
    done.store(true, std::memory_order_seq_cst);
    throw;
  }
#endif
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowHandle, SecondAwaitOnTheSameHandleRefuses) {
  HandleHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // the refusal unwind fails the consumer task
  flow::Scope nursery = root.child(opts);
  auto child = flow::spawn_on(h.rt(), nursery, value_after(0ms, 5));
  ASSERT_TRUE(child.ok());
  std::atomic<int> legs{0};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), nursery, double_await(std::move(child), legs, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(legs.load(std::memory_order_seq_cst), 1)
      << "the first await succeeded; the second refused (no leg 100)";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- teardown: a parked await unwinds; nothing dangles ---------------------

flow::Task<void> park_on(flow::TaskHandle<int> h, std::atomic<bool>& parked,
                         std::atomic<bool>& unwound) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&unwound};
  parked.store(true, std::memory_order_seq_cst);
  (void)co_await std::move(h);  // parks: the child never completes
}

TEST(FlowHandle, PoolCloseResolvesAParkedHandleAwait) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> unwound{false};
  {
    HandleHarness h;
    auto child = flow::spawn_on(h.rt(), scope, value_after(60s, 1));
    ASSERT_TRUE(child.ok());
    auto consumer = flow::spawn_on(
        h.rt(), scope, park_on(std::move(child), parked, unwound));
    ASSERT_TRUE(consumer.ok());
    ASSERT_TRUE(
        wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
    // The parked flag flips just BEFORE the co_await — give the arm a
    // settle window so the teardown exercises a genuinely ARMED branch
    // (not the pre-park stop-arm refusal).
    std::this_thread::sleep_for(50ms);
    // Harness destructor: pool close + drain — every parked frame must
    // resolve (the child's sleep revokes at ~LaneRuntime; its unwind
    // delivers the handle branch; the consumer unwinds in turn).
  }
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst))
      << "the parked consumer frame must unwind at teardown, not leak";
  ASSERT_TRUE(scope.close().ok());
}

// --- cross-runtime teardown -------------------------------------------------
// The consumer awaits on runtime B a handle whose child runs on runtime A.
// B tears down FIRST: its stop sweep must revoke the registered handle
// branch (the consumer unwinds); the child's LATE completion on A must be
// a CAS-loser no-op — never a resume through B's freed runtime.

TEST(FlowHandle, AwaitingRuntimeTeardownBeforeTheChildCompletes) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> consumer_resolved{false};
  auto a = std::make_unique<HandleHarness>();  // outlives B
  auto child = flow::spawn_on(a->rt(), scope, value_after(60s, 1));
  ASSERT_TRUE(child.ok());
  {
    HandleHarness b;
    auto consumer = flow::spawn_on(
        b.rt(), scope, park_on(std::move(child), parked, consumer_resolved));
    ASSERT_TRUE(consumer.ok());
    ASSERT_TRUE(
        wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
    std::this_thread::sleep_for(50ms);  // settle: the await must be ARMED
    // B's teardown: the registered handle branch is swept — the consumer
    // resolves HERE, while the child still sleeps on A.
  }
  EXPECT_TRUE(consumer_resolved.load(std::memory_order_seq_cst))
      << "B's sweep must resolve the parked cross-runtime await";
  // A's teardown revokes the child's sleep; its unwind completes the
  // (long-revoked) branch — must be a silent no-op, not a UAF. ASan/TSan
  // runs of this suite are the real assertion here.
  a.reset();
  ASSERT_TRUE(scope.close().ok());
}

// --- executor-only frames refuse handle awaits, timing-independent ---------
// (The null-rt refusal must be a property of
// the FRAME, not of whether the child happened to finish first.)

flow::Task<void> await_on_bare_executor(flow::TaskHandle<int> h,
                                        std::atomic<int>& legs,
                                        std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await std::move(h);
    legs.fetch_add(1, std::memory_order_seq_cst);  // must not run
  } catch (...) {
    done.store(true, std::memory_order_seq_cst);
    throw;
  }
#endif
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowHandle, BareExecutorFrameRefusesEvenAFinishedHandle) {
  HandleHarness h;  // the child runs (and finishes) on the lane plane
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, value_after(0ms, 3));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });
  std::atomic<int> legs{0};
  std::atomic<bool> done{false};
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // the refusal unwind fails the consumer task
  flow::Scope nursery = scope.child(opts);
  auto consumer = flow::spawn_on(
      exec, nursery, await_on_bare_executor(std::move(child), legs, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(legs.load(std::memory_order_seq_cst), 0)
      << "an executor-only frame must refuse even an already-done handle";
  ASSERT_TRUE(nursery.close().ok());
  (void)aorta_executor_stop(exec);
  spin.join();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
  ASSERT_TRUE(scope.close().ok());
}

// --- Outcome<void> handles -------------------------------------------------

flow::Task<void> await_void_handle(flow::TaskHandle<> h,
                                   std::atomic<int>& code,
                                   std::atomic<bool>& done) {
  flow::Outcome<void> o = co_await std::move(h);
  code.store(o.ok() ? 0 : stop_signal(o.stopped()),
             std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowHandle, VoidHandleAwaitsCompletion) {
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> ran{false};
  auto child = flow::spawn_on(h.rt(), scope, flag_after(5ms, ran));
  ASSERT_TRUE(child.ok());
  std::atomic<int> code{-1};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_void_handle(std::move(child), code, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(ran.load(std::memory_order_seq_cst))
      << "the handle resolved only after the child actually ran";
  EXPECT_EQ(code.load(std::memory_order_seq_cst), 0);
  ASSERT_TRUE(scope.close().ok());
}

// --- torn delivery with an OK outcome ---------------------------------------

flow::Task<int> value_when(std::atomic<bool>& go) {
  while (!go.load(std::memory_order_seq_cst)) {
    co_await flow::sleep(2ms);
  }
  co_return 7;
}

flow::Task<void> await_handle_sentinel(flow::TaskHandle<int> h,
                                       std::atomic<bool>& parked,
                                       std::atomic<bool>& observed,
                                       std::atomic<bool>& resolved) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  parked.store(true, std::memory_order_seq_cst);
  (void)co_await std::move(h);
  observed.store(true, std::memory_order_seq_cst);  // must NOT run when torn
}

TEST(FlowHandle, TornDeliveryWithAnOkChildOutcomeUnwinds) {
  // The torn-plane rule generalized past cancelled(): a child that
  // completes OK just before the awaiting plane tears down delivers its
  // resume through the teardown channel (refused post → torn inline).
  // The branch slot reads OK and a real value sits in the state — the
  // await must STILL unwind: returning the value would run the
  // continuation on the resolver/teardown thread (M8).
  HandleHarness a;  // the child's runtime — alive throughout
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> go{false};
  std::atomic<bool> parked{false};
  std::atomic<bool> observed{false};
  std::atomic<bool> resolved{false};
  auto child = flow::spawn_on(a.rt(), scope, value_when(go));
  ASSERT_TRUE(child.ok());
  auto consumer = flow::spawn_on(
      *rt, scope,
      await_handle_sentinel(std::move(child), parked, observed, resolved));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(50ms);  // settle: the await must be ARMED
  (void)aorta_executor_stop(exec);
  spin.join();  // the awaiting plane is torn from here
  go.store(true, std::memory_order_seq_cst);  // the child completes OK now
  ASSERT_TRUE(
      wait_until([&] { return resolved.load(std::memory_order_seq_cst); }));
  EXPECT_FALSE(observed.load(std::memory_order_seq_cst))
      << "a torn delivery must unwind even when the child completed OK";
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(facade.detach().ok());
  rt.reset();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// --- spawn_with hands the body a REAL child scope ---------------------------

flow::Task<void> cancel_own_scope(flow::Scope s,
                                  std::atomic<bool>* self_cancelled,
                                  std::atomic<bool>* done) {
  s.cancel();
  self_cancelled->store(s.is_cancelled(), std::memory_order_seq_cst);
  done->store(true, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowHandle, SpawnWithHandsTheBodyARealChildScope) {
  // Rust `Scope::spawn_with` parity (the Rust side made the same fix):
  // the body's Scope is a REAL child — s.cancel() cancels
  // only this task's subtree, never the parent scope or its siblings.
  // The old by-value-copy port let the identical body cancel the whole
  // parent scope.
  HandleHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> self_cancelled{false};
  std::atomic<bool> done{false};
  auto handle = flow::spawn_with(h.rt(), scope, &cancel_own_scope,
                                 &self_cancelled, &done);
  ASSERT_TRUE(handle.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(self_cancelled.load(std::memory_order_seq_cst))
      << "the body's own scope must observe its cancel";
  EXPECT_FALSE(scope.is_cancelled())
      << "the body's cancel must stay in its own subtree (child scope)";
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
