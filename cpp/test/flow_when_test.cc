// The handle combinators (when_all / when_any), the N-ary
// join family (variadic join, join_all) and the cancellation wait
// (flow::cancelled). Rust parity targets: core/src/flow/combinators.rs
// (when_all/when_any) and Scope::cancelled.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

class WhenHarness {
 public:
  WhenHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~WhenHarness() {
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
    co_await flow::sleep(d);
  }
  co_return v;
}

flow::Task<int> failing_after(std::chrono::milliseconds d) {
  if (d.count() > 0) {
    co_await flow::sleep(d);
  }
  throw std::runtime_error("branch failed");
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

// --- when_all ---------------------------------------------------------------

flow::Task<void> when_all_body(flow::LaneRuntime& rt, flow::Scope scope,
                               std::atomic<int>& sum,
                               std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<int>> handles;
  handles.push_back(flow::spawn_on(rt, scope, value_after(8ms, 1)));
  handles.push_back(flow::spawn_on(rt, scope, value_after(2ms, 2)));
  handles.push_back(flow::spawn_on(rt, scope, value_after(5ms, 4)));
  auto all = co_await flow::when_all(std::move(handles));  // ambient rt
  if (all.ok()) {
    // Input order, not completion order.
    sum.store((*all)[0] * 100 + (*all)[1] * 10 + (*all)[2],
              std::memory_order_seq_cst);
  } else {
    sum.store(stop_signal(all.stopped()), std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, WhenAllDeliversInInputOrder) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> sum{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          when_all_body(h.rt(), scope, sum, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(sum.load(std::memory_order_seq_cst), 124);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> when_all_fault_body(flow::LaneRuntime& rt,
                                     flow::Scope nursery,
                                     std::atomic<int>& out,
                                     std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<int>> handles;
  handles.push_back(flow::spawn_on(rt, nursery, value_after(2ms, 1)));
  handles.push_back(flow::spawn_on(rt, nursery, failing_after(5ms)));
  auto all = co_await flow::when_all(std::move(handles));
  out.store(all.ok() ? 0 : stop_signal(all.stopped()),
            std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, WhenAllReportsTheFault) {
  WhenHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // contain the branch failure to the nursery
  flow::Scope nursery = root.child(opts);
  std::atomic<int> out{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), root,
                          when_all_fault_body(h.rt(), nursery, out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst),
            static_cast<int>(AORTA_ERR_INTERNAL))
      << "fault outranks any sibling cancels in the when_all error pick";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- when_any ---------------------------------------------------------------

flow::Task<void> when_any_body(flow::LaneRuntime& rt, flow::Scope scope,
                               std::atomic<int>& out,
                               std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<int>> handles;
  handles.push_back(flow::spawn_on(rt, scope, value_after(80ms, 10)));
  handles.push_back(flow::spawn_on(rt, scope, value_after(3ms, 20)));
  handles.push_back(flow::spawn_on(rt, scope, value_after(80ms, 30)));
  auto any = co_await flow::when_any(std::move(handles));
  if (any.ok()) {
    out.store(static_cast<int>(any->first) * 100 + any->second,
              std::memory_order_seq_cst);
  } else {
    out.store(stop_signal(any.stopped()), std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, WhenAnyResolvesOnTheFirstCleanCompletion) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          when_any_body(h.rt(), scope, out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 120)
      << "index 1 (value 20) wins; the 80ms losers drain under the scope";
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> when_any_empty_body() {
  std::vector<flow::TaskHandle<int>> handles;  // deliberately empty
  // The B2 misuse refusal UNWINDS this frame at await_resume — nothing
  // after this line runs; the stop is observed on the wrapper's handle.
  (void)co_await flow::when_any(std::move(handles));
}

TEST(FlowWhen, EmptyWhenAnyRefusalReportsExplicit) {
  // Rust parity (core/src/flow/combinators.rs): when_any(vec![]) ==
  // Err(Stopped::cancelled(Explicit)) — the B2 misuse refusal is an
  // EXPLICIT programmatic stop, not a shutdown fold. TCK row
  // `when_any_empty` pins the same record on both planes.
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  auto handle = flow::spawn_on(h.rt(), scope, when_any_empty_body());
  ASSERT_TRUE(handle.ok());
  ASSERT_TRUE(handle.wait_for(std::chrono::seconds(5)));
  auto o = handle.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  ASSERT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<int>(flow::CancelReason::kExplicit));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> when_any_all_stopped_body(flow::LaneRuntime& rt,
                                           flow::Scope nursery,
                                           std::atomic<int>& out,
                                           std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<int>> handles;
  handles.push_back(flow::spawn_on(rt, nursery, failing_after(2ms)));
  handles.push_back(flow::spawn_on(rt, nursery, failing_after(4ms)));
  auto any = co_await flow::when_any(std::move(handles));
  out.store(any.ok() ? 0 : stop_signal(any.stopped()),
            std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, WhenAnyWithEveryChildStoppedReportsTheStop) {
  WhenHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;
  flow::Scope nursery = root.child(opts);
  std::atomic<int> out{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      h.rt(), root, when_any_all_stopped_body(h.rt(), nursery, out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst),
            static_cast<int>(AORTA_ERR_INTERNAL))
      << "no winner: the last stop code reports (Rust last_error parity)";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- variadic join / join_all ----------------------------------------------

flow::Task<double> value_after_d(std::chrono::milliseconds d, double v) {
  co_await flow::sleep(d);
  co_return v;
}

flow::Task<void> join3_body(std::atomic<int>& out, std::atomic<bool>& done) {
  // Three branches, MIXED value types, ambient {rt, scope}.
  auto [a, b, c] = co_await flow::join(value_after(5ms, 7),
                                       value_after(2ms, 9),
                                       value_after_d(3ms, 0.5));
  out.store(a * 10 + b + static_cast<int>(c * 2), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, VariadicJoinResolvesThreeMixedBranches) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope, join3_body(out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 80);  // 7*10 + 9 + 1
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> join_all_body(std::atomic<int>& out,
                               std::atomic<bool>& done) {
  std::vector<flow::Task<int>> tasks;
  for (int i = 0; i < 5; ++i) {
    tasks.push_back(value_after(std::chrono::milliseconds(1 + (7 * i) % 5),
                                i + 1));
  }
  std::vector<int> values = co_await flow::join_all(std::move(tasks));
  int acc = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    acc = acc * 10 + values[i];  // order-sensitive fold: input order pins
  }
  out.store(acc, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, JoinAllResolvesAVectorInInputOrder) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope, join_all_body(out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 12345);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> join_all_empty_body(std::atomic<int>& out,
                                     std::atomic<bool>& done) {
  std::vector<flow::Task<int>> tasks;  // empty: vacuously complete
  std::vector<int> values = co_await flow::join_all(std::move(tasks));
  out.store(static_cast<int>(values.size()) + 100, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, JoinAllOverAnEmptyVectorResolvesImmediately) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope, join_all_empty_body(out, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 100);
  ASSERT_TRUE(scope.close().ok());
}

// --- spawn_with on the lane plane ------------------------------------------

TEST(FlowWhen, SpawnWithLaneRuntimeOverloadInjectsTheScope) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> saw_valid_scope{false};
  std::atomic<bool> done{false};
  auto s = flow::spawn_with(
      h.rt(), scope,
      [](flow::Scope body_scope, std::atomic<bool>* saw,
         std::atomic<bool>* flag) -> flow::Task<void> {
        saw->store(body_scope.valid(), std::memory_order_seq_cst);
        flag->store(true, std::memory_order_seq_cst);
        co_return;
      },
      &saw_valid_scope, &done);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(saw_valid_scope.load(std::memory_order_seq_cst));
  ASSERT_TRUE(scope.close().ok());
}

// --- cross-runtime teardown -------------------------------------------------

flow::Task<void> park_on_when_all(std::vector<flow::TaskHandle<int>> hs,
                                  std::atomic<bool>& parked,
                                  std::atomic<bool>& resolved) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  parked.store(true, std::memory_order_seq_cst);
  (void)co_await flow::when_all(std::move(hs));
}

TEST(FlowWhen, AwaitingRuntimeTeardownResolvesAParkedWhenAll) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> resolved{false};
  auto a = std::make_unique<WhenHarness>();  // the child's runtime, outlives B
  std::vector<flow::TaskHandle<int>> handles;
  for (int i = 0; i < 3; ++i) {  // several branches: the sweep must revoke
    handles.push_back(                     // the WHOLE registered set
        flow::spawn_on(a->rt(), scope, value_after(60000ms, i)));
    ASSERT_TRUE(handles.back().ok());
  }
  {
    WhenHarness b;  // the AWAITING runtime — torn down first
    auto consumer = flow::spawn_on(
        b.rt(), scope,
        park_on_when_all(std::move(handles), parked, resolved));
    ASSERT_TRUE(consumer.ok());
    ASSERT_TRUE(
        wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
    std::this_thread::sleep_for(50ms);  // settle: the arms must be LIVE
  }
  EXPECT_TRUE(resolved.load(std::memory_order_seq_cst))
      << "B's sweep must resolve the parked cross-runtime when_all";
  a.reset();  // the child's late resolution must be a no-op, never a UAF
  ASSERT_TRUE(scope.close().ok());
}

// --- the 255-branch arbiter cap refuses loudly (Minor 3) --------------------

flow::Task<void> join_all_overflow(std::atomic<int>& legs,
                                   std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    std::vector<flow::Task<int>> tasks;
    for (int i = 0; i < 256; ++i) {
      tasks.push_back(value_after(0ms, i));
    }
    (void)co_await flow::join_all(std::move(tasks));
    legs.fetch_add(1, std::memory_order_seq_cst);  // must not run
  } catch (...) {
    done.store(true, std::memory_order_seq_cst);
    throw;
  }
#endif
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, JoinAllOverTheArbiterCapRefusesLoudly) {
  WhenHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // the refusal unwind fails the task
  flow::Scope nursery = root.child(opts);
  std::atomic<int> legs{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), nursery, join_all_overflow(legs, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(legs.load(std::memory_order_seq_cst), 0)
      << ">255 branches must refuse (the arbiter group cap), not truncate";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- cancelled() ------------------------------------------------------------

flow::Task<void> wait_for_cancel(std::atomic<bool>& parked,
                                 std::atomic<bool>& observed,
                                 std::atomic<bool>& done) {
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::cancelled();  // ambient: the frame's own scope
  // Returning normally MEANS the scope is cancelled — cleanup runs here.
  observed.store(true, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, CancelledResolvesWhenTheScopeCancels) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> observed{false};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          wait_for_cancel(parked, observed, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);  // give the park time to arm
  EXPECT_FALSE(done.load(std::memory_order_seq_cst))
      << "cancelled() must not resolve before the cancel";
  scope.cancel();
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(observed.load(std::memory_order_seq_cst));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> wait_on_precancelled(flow::LaneRuntime& rt,
                                      flow::Scope watched,
                                      std::atomic<bool>& done) {
  co_await flow::cancelled(rt, watched);  // explicit form, foreign scope
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowWhen, CancelledOnAnAlreadyCancelledScopeResolvesImmediately) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  flow::Scope watched = flow::Scope::root();
  watched.cancel();
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          wait_on_precancelled(h.rt(), watched, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  ASSERT_TRUE(watched.close().ok());
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> await_cancelled_after_the_fact(std::atomic<bool>& proceed,
                                                std::atomic<bool>& done) {
  // Block (no co_await — any other ambient verb would unwind once the
  // scope cancels) until the test has ALREADY cancelled our scope, then
  // enter the verb: the ambient entry gate must let cancelled() through
  // to resolve normally — the detail::cancelled_awaiter exemption.
  while (!proceed.load(std::memory_order_seq_cst)) {
    std::this_thread::sleep_for(1ms);
  }
  co_await flow::cancelled();  // own scope, cancelled BEFORE the await
  done.store(true, std::memory_order_seq_cst);
}

// Pins the entry-gate exemption's TYPE identity: after the
// detail-namespace move the exemption keys
// on detail::cancelled_awaiter via is_same — if that check regresses
// (wrong type after a rename/move), a pre-cancelled ambient cancelled()
// would entry-refuse or unwind instead of resolving, and only this test
// would see it (the explicit-form twin above bypasses the ambient
// wrapper entirely).
TEST(FlowWhen, AmbientCancelledOnAnAlreadyCancelledScopeStillResolves) {
  WhenHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> proceed{false};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          await_cancelled_after_the_fact(proceed, done));
  ASSERT_TRUE(s.ok());
  scope.cancel();
  proceed.store(true, std::memory_order_seq_cst);
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> wait_for_cancel_sentinel(std::atomic<bool>& parked,
                                          std::atomic<bool>& observed,
                                          std::atomic<bool>& resolved) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};  // fires on unwind AND on normal return
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::cancelled();
  observed.store(true, std::memory_order_seq_cst);  // cleanup leg
}

TEST(FlowWhen, DisabledPlaneTeardownDeliveryUnwindsEvenWhenTheFlagIsSet) {
  // The torn-plane rule must hold on the Disabled/
  // rollback plane too, where there is no pool to probe — tornness is
  // judged at the DELIVERY layer. Script: park cancelled() on a Disabled
  // runtime, STOP the executor first, THEN cancel the scope — the
  // watcher's resume post refuses on the stopped executor and delivers
  // inline (a teardown delivery): the frame must UNWIND, not run
  // the cleanup on the timer thread, even though the flag reads
  // cancelled by then.
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);  // wires the fallback routing
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> observed{false};
  std::atomic<bool> resolved{false};
  auto s = flow::spawn_on(
      *rt, scope, wait_for_cancel_sentinel(parked, observed, resolved));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(50ms);  // settle: the watcher must be ARMED
  (void)aorta_executor_stop(exec);
  spin.join();
  scope.cancel();  // NOW the flag is set — but the plane is already torn
  ASSERT_TRUE(
      wait_until([&] { return resolved.load(std::memory_order_seq_cst); }));
  EXPECT_FALSE(observed.load(std::memory_order_seq_cst))
      << "a teardown delivery must unwind even when the flag reads "
         "cancelled (Disabled plane)";
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(facade.detach().ok());
  rt.reset();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

flow::Task<void> sleep_sentinel(std::atomic<bool>& parked,
                                std::atomic<bool>& observed,
                                std::atomic<bool>& resolved) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::sleep(300ms);
  observed.store(true, std::memory_order_seq_cst);  // must NOT run when torn
}

TEST(FlowWhen, TornSleepDeliveryCarryingTheTimerOkUnwinds) {
  // The torn-plane rule generalized to every consumer:
  // park an ambient sleep on a Disabled runtime, stop the executor, and
  // let the timer FIRE — the branch completes OK, but the resume post
  // refuses on the stopped executor and delivers inline with the torn
  // mark. settle() must report Stopped (unwind), never return the OK to
  // user code on the core timer thread.
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> observed{false};
  std::atomic<bool> resolved{false};
  auto s = flow::spawn_on(*rt, scope,
                          sleep_sentinel(parked, observed, resolved));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(50ms);  // settle: the timer arm must be LIVE
  (void)aorta_executor_stop(exec);
  spin.join();  // the plane is torn from here; the 300ms timer still fires
  ASSERT_TRUE(
      wait_until([&] { return resolved.load(std::memory_order_seq_cst); }));
  EXPECT_FALSE(observed.load(std::memory_order_seq_cst))
      << "a torn timer delivery must unwind even though the timer "
         "completed its branch OK";
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(facade.detach().ok());
  rt.reset();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

TEST(FlowWhen, PoolCloseUnwindsAParkedCancelledWait) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> observed{false};
  std::atomic<bool> done{false};
  {
    WhenHarness h;
    auto s = flow::spawn_on(h.rt(), scope,
                            wait_for_cancel(parked, observed, done));
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(
        wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
    std::this_thread::sleep_for(50ms);  // settle: the watcher must be ARMED
    // Harness teardown: the stop sweep revokes the scope-watcher arm; the
    // frame must UNWIND (scope was never cancelled), not run the cleanup.
  }
  EXPECT_FALSE(observed.load(std::memory_order_seq_cst))
      << "a teardown delivery must unwind, not impersonate a cancel";
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
