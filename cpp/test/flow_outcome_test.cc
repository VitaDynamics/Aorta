// Outcome<T>/Stopped contract tests.
//
// The value-or-Stopped vocabulary itself (cpp/include/aorta/flow.hpp):
// Value()/Stop() factories, ok()/value()/stopped() with the fatal
// wrong-side-access discipline, move-only non-default-constructible
// payloads, and the "a stopped Outcome<T> never constructs a T" contract.
// Migrated consumers (flow_handle_test.cc, flow_when_test.cc, ...) already
// exercise the vocabulary end to end through TaskHandle/when_all/when_any;
// this file pins the FOUNDATIONAL guarantees listed above by name,
// so a future Outcome refactor breaks a test HERE first.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

// The combinator-test harness precedent (flow_when_test.cc /
// flow_handle_test.cc): a lane pool + one drainer thread.
class OutcomeHarness {
 public:
  OutcomeHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~OutcomeHarness() {
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

// --- (a) move-only, non-default-constructible T round-trips ---------------

struct MoveOnlyNonDefault {
  explicit MoveOnlyNonDefault(int v) : v(v) {}
  MoveOnlyNonDefault(MoveOnlyNonDefault&&) noexcept = default;
  MoveOnlyNonDefault& operator=(MoveOnlyNonDefault&&) noexcept = default;
  MoveOnlyNonDefault(const MoveOnlyNonDefault&) = delete;
  MoveOnlyNonDefault& operator=(const MoveOnlyNonDefault&) = delete;
  int v;
};

flow::Task<MoveOnlyNonDefault> move_only_task(int v) {
  co_return MoveOnlyNonDefault(v);
}

flow::Task<void> await_move_only_handle(
    flow::TaskHandle<MoveOnlyNonDefault> h, std::atomic<int>& out,
    std::atomic<bool>& done) {
  flow::Outcome<MoveOnlyNonDefault> o = co_await std::move(h);
  out.store(o.ok() ? std::move(o).value().v : -1, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, MoveOnlyNonDefaultConstructibleValueRoundTripsThroughCoAwait) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, move_only_task(11));
  ASSERT_TRUE(child.ok());
  std::atomic<int> out{-2};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_move_only_handle(std::move(child), out, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 11);
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowOutcome, MoveOnlyNonDefaultConstructibleValueRoundTripsThroughTakeOutcome) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, move_only_task(23));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(scope.close().ok());  // BLOCKS until the child finished
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  MoveOnlyNonDefault moved = std::move(*o).value();
  EXPECT_EQ(moved.v, 23);
}

// --- (b) a child exception classifies as a fault, code + message intact ---

flow::Task<int> motor_fault_task() {
  co_await flow::resume();  // move off the spawn slice first
  throw std::runtime_error("motor fault");
}

flow::Task<void> await_fault_handle(flow::TaskHandle<int> h,
                                    std::atomic<bool>& is_fault,
                                    std::atomic<int>& code,
                                    std::string& message,
                                    std::atomic<bool>& done) {
  flow::Outcome<int> o = co_await std::move(h);
  if (!o.ok()) {
    is_fault.store(o.stopped().is_fault(), std::memory_order_seq_cst);
    code.store(o.stopped().fault().code(), std::memory_order_seq_cst);
    message = o.stopped().fault().message();
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, ChildExceptionYieldsAFaultWithCodeAndMessage) {
  OutcomeHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;  // contain ChildFailed to this subtree (WhenAll
                           // ReportsTheFault precedent, flow_when_test.cc)
  flow::Scope nursery = root.child(opts);
  auto child = flow::spawn_on(h.rt(), nursery, motor_fault_task());
  ASSERT_TRUE(child.ok());
  std::atomic<bool> is_fault{false};
  std::atomic<int> code{0};
  std::string message;
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), root,
      await_fault_handle(std::move(child), is_fault, code, message, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(is_fault.load(std::memory_order_seq_cst));
  EXPECT_EQ(code.load(std::memory_order_seq_cst), AORTA_ERR_INTERNAL)
      << "an escaped exception classifies as a fault (Rust panic parity)";
  EXPECT_NE(message.find("motor fault"), std::string::npos)
      << "the exception text survives verbatim into the fault (message was: "
      << message << ")";
  ASSERT_TRUE(nursery.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// --- (c) a cancelled parked child scope carries the EXACT raw reason ------

// The one verb that watches scope cancellation directly (see flow_tck_
// emitter.cc's cancel_reason_observation scenario, which this mirrors):
// `co_await flow::cancelled()` resolves NORMALLY on a genuine cancel, so
// the child converts that observation into the SDK's own unwind token to
// produce a Stopped outcome at the handle.
flow::Task<void> park_until_cancelled_then_unwind() {
  co_await flow::cancelled();  // ambient: the frame's own spawned scope
  throw flow::cancelled_unwind{};
}

TEST(FlowOutcome, CancelledParkedChildScopeCarriesTheExactRawReason) {
  OutcomeHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope child_scope = root.child();
  auto child =
      flow::spawn_on(h.rt(), child_scope, park_until_cancelled_then_unwind());
  ASSERT_TRUE(child.ok());
  std::this_thread::sleep_for(20ms);  // settle: the watcher must be armed
  child_scope.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(child_scope.close().ok());  // BLOCKS until the child stopped
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  EXPECT_TRUE(o->stopped().is_cancel());
  EXPECT_FALSE(o->stopped().is_fault());
  EXPECT_EQ(o->stopped().raw_reason(), 3);
  EXPECT_EQ(o->stopped().cancel_reason(), flow::CancelReason::kShutdown);
  ASSERT_TRUE(root.close().ok());
}

// --- (d) Outcome<void> shapes -----------------------------------------------

flow::Task<void> quick_ok() { co_return; }

flow::Task<void> await_void_handle_ok(flow::TaskHandle<void> h,
                                      std::atomic<bool>& ok,
                                      std::atomic<bool>& done) {
  flow::Outcome<void> o = co_await std::move(h);
  ok.store(o.ok(), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, VoidTaskHandleAwaitYieldsAnOkOutcome) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, quick_ok());
  ASSERT_TRUE(child.ok());
  std::atomic<bool> observed_ok{false};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope,
      await_void_handle_ok(std::move(child), observed_ok, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(observed_ok.load(std::memory_order_seq_cst));
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> empty_when_all_void_body(std::atomic<bool>& ok,
                                          std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<void>> handles;  // empty: vacuously complete
  flow::Outcome<void> o = co_await flow::when_all(std::move(handles));
  ok.store(o.ok(), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, WhenAllOverEmptyVoidHandlesYieldsAnOkOutcome) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> observed_ok{false};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, empty_when_all_void_body(observed_ok, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_TRUE(observed_ok.load(std::memory_order_seq_cst));
  ASSERT_TRUE(scope.close().ok());
}

// --- (e) death tests: fatal wrong-side access, both directions ------------

TEST(FlowOutcomeDeathTest, ValueOnAStoppedOutcomeAborts) {
  auto o = flow::Outcome<int>::Stop(
      flow::Stopped::Cancelled(flow::CancelReason::kExplicit));
  EXPECT_DEATH(static_cast<void>(o.value()), "Aorta fatal");
}

TEST(FlowOutcomeDeathTest, StoppedOnAValueOutcomeAborts) {
  auto o = flow::Outcome<int>::Value(5);
  EXPECT_DEATH(static_cast<void>(o.stopped()), "Aorta fatal");
}

// Bonus (same fatal wrong-side-access discipline, one level down on
// Stopped itself): fault() on a cancellation and
// cancel_reason() on a fault are equally fatal, not just Outcome's own
// value()/stopped() pair.
TEST(FlowOutcomeDeathTest, FaultOnACancelledStoppedAborts) {
  auto stopped = flow::Stopped::Cancelled(flow::CancelReason::kExplicit);
  EXPECT_DEATH(static_cast<void>(stopped.fault()), "Aorta fatal");
}

TEST(FlowOutcomeDeathTest, CancelledWithZeroReasonByteAborts) {
  // Gate 2.1: a zero byte would masquerade as the fault discriminant —
  // refused fatally, never silently rewritten as Explicit.
  EXPECT_DEATH((void)flow::Stopped::Cancelled(static_cast<flow::CancelReason>(0)),
               "Aorta fatal");
}

TEST(FlowOutcomeDeathTest, CancelledRawZeroAborts) {
  EXPECT_DEATH((void)flow::Stopped::CancelledRaw(0), "Aorta fatal");
}

TEST(FlowOutcomeDeathTest, CancelReasonOnAFaultStoppedAborts) {
  auto stopped = flow::Stopped::Fault(aorta::Status(AORTA_ERR_INTERNAL, "x"));
  EXPECT_DEATH(static_cast<void>(stopped.cancel_reason()), "Aorta fatal");
}

// --- (f) a stopped Outcome<T> never constructs a T -------------------------

struct CountingType {
  static std::atomic<int> constructions;
  explicit CountingType(int v) : v(v) {
    constructions.fetch_add(1, std::memory_order_seq_cst);
  }
  CountingType(CountingType&& other) noexcept : v(other.v) {
    constructions.fetch_add(1, std::memory_order_seq_cst);
  }
  CountingType& operator=(CountingType&&) noexcept = default;
  CountingType(const CountingType&) = delete;
  CountingType& operator=(const CountingType&) = delete;
  int v;
};
std::atomic<int> CountingType::constructions{0};

// Same "watch the scope cancel directly, then unwind" shape as (c) — the
// payload type is never touched on this path if the Outcome<T>/Stopped contract holds
// (a stopped Outcome<T> stores NOTHING in the T slot of its variant).
flow::Task<CountingType> never_produces_a_counting_type() {
  co_await flow::cancelled();
  throw flow::cancelled_unwind{};
}

TEST(FlowOutcome, CancelledOutcomeConstructsZeroInstancesOfThePayloadType) {
  CountingType::constructions.store(0, std::memory_order_seq_cst);
  OutcomeHarness h;
  flow::Scope root = flow::Scope::root();
  flow::Scope child_scope = root.child();
  auto child = flow::spawn_on(h.rt(), child_scope,
                              never_produces_a_counting_type());
  ASSERT_TRUE(child.ok());
  std::this_thread::sleep_for(20ms);  // settle: the watcher must be armed
  child_scope.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(child_scope.close().ok());
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  EXPECT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(CountingType::constructions.load(std::memory_order_seq_cst), 0)
      << "a stopped Outcome<T> must never construct a T";
  ASSERT_TRUE(root.close().ok());
}


// --- Reason snapshots at STOP time -----------------------------------------
//
// RED by design against the pre-fix code: build_stopped
// re-queries the MUTABLE parent scope at take() time, so (1) a deadline stop
// misattributes as Explicit (the wrapper's cancel status is always
// AORTA_ERR_CANCELLED — the TIMEOUT fallback is dead code), and (2) a parent
// cancel AFTER the child completed retroactively re-labels the outcome.
// Green when the stop reason is snapshotted at unwind time.

flow::Task<int> slow_value() {
  co_await flow::sleep(400ms);
  co_return 1;
}

flow::Task<int> deadline_stopped_child() {
  // timeout() unwinds THIS frame with a deadline stop at 10ms.
  int v = co_await flow::timeout(10ms, slow_value());
  co_return v;
}

flow::Task<void> await_stop_shape(flow::TaskHandle<int> h,
                                  std::atomic<int>& kind_out,
                                  std::atomic<int>& raw_out,
                                  std::atomic<bool>& done) {
  flow::Outcome<int> o = co_await std::move(h);
  if (!o.ok()) {
    kind_out.store(static_cast<int>(o.stopped().kind()),
                   std::memory_order_seq_cst);
    raw_out.store(o.stopped().raw_reason(), std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, DeadlineStoppedChildReportsDeadlineExceeded) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, deadline_stopped_child());
  ASSERT_TRUE(child.ok());
  std::atomic<int> kind{-1};
  std::atomic<int> raw{-1};
  std::atomic<bool> done{false};
  auto consumer = flow::spawn_on(
      h.rt(), scope, await_stop_shape(std::move(child), kind, raw, done));
  ASSERT_TRUE(consumer.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(kind.load(std::memory_order_seq_cst),
            static_cast<int>(flow::StopKind::kDeadline))
      << "a timeout-stopped child must classify as kDeadline, not Explicit";
  EXPECT_EQ(raw.load(std::memory_order_seq_cst),
            static_cast<int>(flow::CancelReason::kDeadlineExceeded));
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowOutcome, LateParentCancelDoesNotReattributeACompletedChild) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  auto child = flow::spawn_on(h.rt(), scope, deadline_stopped_child());
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  // The child ALREADY completed (deadline stop). A LATER parent cancel
  // must not rewrite history at take_outcome() time.
  scope.cancel(flow::CancelReason::kShutdown);
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  EXPECT_TRUE(o->stopped().is_deadline())
      << "completed-child outcome must keep its OWN stop reason";
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<int>(flow::CancelReason::kDeadlineExceeded))
      << "the late Shutdown cancel must not be retro-attributed";
  ASSERT_TRUE(scope.close().ok());
}

// --- (g) move-only T through when_any / when_all --------------------------
//
// when_any's winner is moved out of Outcome<pair<size_t, T>> — the pair
// itself is only move-constructible when T is (the "no default-
// constructed T on the stop path" contract extends to the combinators, not
// just plain TaskHandle). when_all's value path is `vector<T>::push_back
// (std::move(...))` after a `reserve()` — no reallocation, so it never
// needs T's copy ctor either; both combinators work over MoveOnlyNonDefault
// unmodified (declared in section (a) above).

flow::Task<MoveOnlyNonDefault> move_only_after(std::chrono::milliseconds d,
                                               int v) {
  if (d.count() > 0) {
    co_await flow::sleep(d);
  }
  co_return MoveOnlyNonDefault(v);
}

flow::Task<void> when_any_move_only_body(flow::LaneRuntime& rt,
                                         flow::Scope scope,
                                         std::atomic<int>& winner_idx,
                                         std::atomic<int>& winner_v,
                                         std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<MoveOnlyNonDefault>> handles;
  handles.push_back(flow::spawn_on(rt, scope, move_only_after(60ms, 10)));
  handles.push_back(flow::spawn_on(rt, scope, move_only_after(3ms, 20)));
  auto any = co_await flow::when_any(std::move(handles));
  if (any.ok()) {
    // Move the pair OUT of the Outcome (the `&&` value() overload), then
    // decompose — proves the winner's move-only payload survives the
    // combinator's internal hand-off without ever being copied.
    auto [idx, moved] = std::move(any).value();
    winner_idx.store(static_cast<int>(idx), std::memory_order_seq_cst);
    winner_v.store(moved.v, std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, WhenAnyOverMoveOnlyValueMovesTheWinnerOut) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> winner_idx{-1};
  std::atomic<int> winner_v{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      h.rt(), scope,
      when_any_move_only_body(h.rt(), scope, winner_idx, winner_v, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(winner_idx.load(std::memory_order_seq_cst), 1)
      << "the 3ms branch wins over the 60ms loser";
  EXPECT_EQ(winner_v.load(std::memory_order_seq_cst), 20);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> when_all_move_only_body(flow::LaneRuntime& rt,
                                         flow::Scope scope,
                                         std::atomic<int>& sum,
                                         std::atomic<bool>& done) {
  std::vector<flow::TaskHandle<MoveOnlyNonDefault>> handles;
  handles.push_back(flow::spawn_on(rt, scope, move_only_after(8ms, 1)));
  handles.push_back(flow::spawn_on(rt, scope, move_only_after(2ms, 2)));
  auto all = co_await flow::when_all(std::move(handles));
  if (all.ok()) {
    auto values = std::move(all).value();  // vector<MoveOnlyNonDefault>&&
    sum.store(values[0].v * 10 + values[1].v, std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowOutcome, WhenAllOverMoveOnlyValuesRoundTripsInInputOrder) {
  OutcomeHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> sum{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(h.rt(), scope,
                          when_all_move_only_body(h.rt(), scope, sum, done));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_seq_cst); }));
  EXPECT_EQ(sum.load(std::memory_order_seq_cst), 12)
      << "input order preserved (branch 0 * 10 + branch 1), not completion "
         "order";
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
