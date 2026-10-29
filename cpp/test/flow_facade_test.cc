// Flow the ST coexistence facade — combined wait,
// stop bridge (all origins), backlog liveness, reentrancy marks.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

class FacadeHarness : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::SingleThread();
    attachment_ =
        std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached());
    spin_ = std::thread([this] {
      spin_tid_.store(std::this_thread::get_id(), std::memory_order_seq_cst);
      aorta_executor_spin(exec_);
    });
    std::this_thread::sleep_for(20ms);
  }

  void TearDown() override {
    (void)aorta_executor_stop(exec_);
    if (spin_.joinable()) spin_.join();
    attachment_.reset();  // detach (quiesce) after the spin returned
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
  std::atomic<std::thread::id> spin_tid_{};
};

flow::Task<void> hop_n(flow::LaneRuntime& rt, int n, std::atomic<int>& hops,
                       std::atomic<std::thread::id>& last_on) {
  for (int i = 0; i < n; i++) {
    co_await flow::resume_on_lane{&rt};
    last_on.store(std::this_thread::get_id(), std::memory_order_seq_cst);
    hops.fetch_add(1, std::memory_order_seq_cst);
  }
}

TEST_F(FacadeHarness, CombinedWaitDispatchesLaneTasksOnTheSpinThread) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> hops{0};
  std::atomic<std::thread::id> last_on{};
  auto s = flow::spawn_on(*rt_, scope, hop_n(*rt_, 5, hops, last_on));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (hops.load(std::memory_order_seq_cst) < 5 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(hops.load(std::memory_order_seq_cst), 5);
  EXPECT_EQ(last_on.load(std::memory_order_seq_cst),
            spin_tid_.load(std::memory_order_seq_cst))
      << "ST mode: lane frames run ON the spin thread (no-concurrency "
         "guarantee by construction)";
  ASSERT_TRUE(scope.close().ok());
}

struct DrainThreadSentinel {
  std::atomic<bool>* unwound;
  std::atomic<std::thread::id>* on;
  ~DrainThreadSentinel() {
    on->store(std::this_thread::get_id(), std::memory_order_seq_cst);
    unwound->store(true, std::memory_order_seq_cst);
  }
};

flow::Task<void> ping_forever(flow::LaneRuntime& rt, std::atomic<bool>& unwound,
                              std::atomic<std::thread::id>& drained_on) {
  DrainThreadSentinel sentinel{&unwound, &drained_on};
  for (;;) {
    co_await flow::resume_on_lane{&rt};
  }
}

TEST_F(FacadeHarness, StopBridgeDrainsLaneFramesOnTheSpinThread) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  std::atomic<std::thread::id> drained_on{};
  auto s = flow::spawn_on(*rt_, scope,
                          ping_forever(*rt_, unwound, drained_on));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(30ms);  // frame ping-ponging on the spin
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);  // any-origin stop
  spin_.join();  // the final stop-drain pass runs before spin returns
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst))
      << "the stop bridge drained the lane frame (no un-resumed frame)";
  EXPECT_EQ(drained_on.load(std::memory_order_seq_cst),
            spin_tid_.load(std::memory_order_seq_cst))
      << "drain-thread identity: the SPIN thread ran the drop path";
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(FacadeHarness, BacklogBeyondBatchDrainsWithoutFurtherPosts) {
  // R1-F8: backlog > LANE_DRAIN_BATCH with an empty event queue and no
  // further posts must drain to empty (the spin must not park while the
  // hook reports backlog).
  std::atomic<int> ran{0};
  for (int i = 0; i < 100; i++) {  // >> the 16-item batch
    aorta::lanes::Work w;
    w.run = [](void* ctx) {
      static_cast<std::atomic<int>*>(ctx)->fetch_add(1,
                                                     std::memory_order_seq_cst);
    };
    w.ctx = &ran;
    ASSERT_EQ(rt_->pool().post(3, w), aorta::lanes::Post::OK);
  }
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (ran.load(std::memory_order_seq_cst) < 100 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(ran.load(std::memory_order_seq_cst), 100)
      << "backlog drained to empty with zero additional wakes";
}

flow::Task<void> stop_from_frame(uint64_t exec, std::atomic<bool>& stopped_rc) {
  // A frame on the SPIN PATH calls stop: legal, must not deadlock — the
  // on_stop bridge is trampoline-class (flags only).
  stopped_rc.store(aorta_executor_stop(exec) == AORTA_OK,
                   std::memory_order_seq_cst);
  co_return;
}

TEST_F(FacadeHarness, StopFromALaneFrameDoesNotDeadlock) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> stopped{false};
  auto s = flow::spawn_on(*rt_, scope, stop_from_frame(exec_, stopped));
  ASSERT_TRUE(s.ok());
  spin_.join();  // returns because the frame stopped the executor
  EXPECT_TRUE(stopped.load(std::memory_order_seq_cst));
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace

// ---- Rollback smoke: combinators on the queue path -----------------------

flow::Task<void> rollback_body(flow::LaneRuntime& rt, flow::Scope scope,
                               std::atomic<int>& out) {
  auto [a, b] = co_await flow::join(
      rt, scope,
      [](flow::LaneRuntime& r) -> flow::Task<int> {
        co_await flow::sleep(r, std::chrono::milliseconds(2));
        co_return 20;
      }(rt),
      [](flow::LaneRuntime& r) -> flow::Task<int> {
        co_await flow::resume_on_lane{&r};
        co_return 22;
      }(rt));
  out.store(a + b, std::memory_order_seq_cst);
}

TEST(FacadeRollback, DisabledRuntimeRunsCombinatorsOnTheQueuePath) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  flow::FacadeAttachment facade(*rt, exec);
  EXPECT_FALSE(facade.attached()) << "rollback = not-attaching";
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(*rt, scope, rollback_body(*rt, scope, out));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (out.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 42)
      << "join + sleep + resume hop all rode the core executor";
  ASSERT_TRUE(scope.close().ok());
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// ---- Disabled-runtime bounded-teardown pair --------------------------------
//
// (a) A frame parked on a Disabled runtime's arbiter (ambient sleep — the
// rollback plane's timer) survives executor stop UN-revoked: no attach
// hook ever sweeps a Disabled runtime (the Disabled-runtime honesty contract). Under
// the stop-then-destroy precondition, ~LaneRuntime's UNCONDITIONAL sweep
// must then resolve it — the revoke's resume post refuses on the stopped
// executor and the frame resumes INLINE on the destructor's thread, so
// completion is observable BEFORE rt.reset() returns; teardown is bounded
// (never waits out the timer); no UAF (TSan/ASan sweep in CI).
//
// RED state (verified 2026-07-11 by reverting the one line): re-gate the
// ~LaneRuntime revoke sweep with `enabled()` (the previous code) and
// (a) fails at the post-destructor assert — the frames stay parked past
// destruction, holding a dangling rt*.

flow::Task<void> parked_disabled_sleeper(std::atomic<bool>& parked,
                                         std::atomic<bool>& resolved) {
  // Frame-destruction sentinel (the StopWithInFlightCombinator pattern):
  // fires on ANY exit — the teardown unwind included — without a try/catch.
  struct Sentinel {
    std::atomic<bool>* f;
    ~Sentinel() { f->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  parked.store(true, std::memory_order_seq_cst);
  // Ambient arbiter leg on the Disabled plane; only a revoke sweep can
  // resolve this within the test's lifetime.
  co_await flow::sleep(std::chrono::seconds(600));
}

TEST(FacadeRollback, DisabledRuntimeDestructorSweepResolvesParkedFramesInline) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::Disabled();
  auto facade = std::make_unique<flow::FacadeAttachment>(*rt, exec);
  EXPECT_FALSE(facade->attached()) << "rollback = not-attaching";
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked_a{false};
  std::atomic<bool> parked_b{false};
  std::atomic<bool> resolved_a{false};
  std::atomic<bool> resolved_b{false};
  ASSERT_TRUE(
      flow::spawn_on(*rt, scope, parked_disabled_sleeper(parked_a, resolved_a))
          .ok());
  ASSERT_TRUE(
      flow::spawn_on(*rt, scope, parked_disabled_sleeper(parked_b, resolved_b))
          .ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!(parked_a.load(std::memory_order_seq_cst) &&
           parked_b.load(std::memory_order_seq_cst)) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(parked_a.load() && parked_b.load());
  std::this_thread::sleep_for(100ms);  // the arms land just past the flags

  // The precondition: executor stopped, stop-drain completed.
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  EXPECT_FALSE(resolved_a.load() || resolved_b.load())
      << "no stop-sweep exists for Disabled runtimes — the "
         "frames stay parked until ~LaneRuntime";

  facade.reset();
  const auto t0 = std::chrono::steady_clock::now();
  rt.reset();  // ~LaneRuntime: the UNCONDITIONAL revoke sweep
  const auto dtor_elapsed = std::chrono::steady_clock::now() - t0;
  // ASSERT (not EXPECT): in the red state (enabled()-gated sweep) the
  // frames stay parked forever — scope.close() below would hang to the
  // bazel timeout instead of failing cleanly.
  ASSERT_TRUE(resolved_a.load(std::memory_order_seq_cst) &&
              resolved_b.load(std::memory_order_seq_cst))
      << "the destructor sweep must resolve parked frames INLINE — "
         "completion observable before the dtor returns";
  EXPECT_LT(dtor_elapsed, 5s)
      << "teardown must be bounded, never waiting out the 600s timer";
  ASSERT_TRUE(scope.close().ok())
      << "accounting balanced by the inline resumes";
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// (b) The armed-timer-fires-DURING-destructor-sweep race: a SHORT timer on
// a Disabled runtime may fire concurrently with ~LaneRuntime's revoke
// sweep after executor stop+drain. The arbiter's exactly-once CAS decides
// the winner (revoke-first → inline resume on the destructor thread;
// fire-first → refused post → inline resume on the core timer thread; the
// loser's late action is a CAS no-op) — the frame resolves EXACTLY once
// either way. Looped with the fire window varied around the destroy point
// so TSan sees real interleavings of both orders.
flow::Task<void> short_disabled_sleeper(std::chrono::microseconds window,
                                        std::atomic<bool>& started,
                                        std::atomic<int>& resolved) {
  struct Sentinel {
    std::atomic<int>* n;
    ~Sentinel() { n->fetch_add(1, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  started.store(true, std::memory_order_seq_cst);
  co_await flow::sleep(window);
}

TEST(FacadeRollback, DisabledRuntimeDestructorSweepRacesTimerFire) {
  for (int i = 0; i < 200; i++) {
    uint64_t exec = 0;
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
    auto rt = flow::LaneRuntime::Disabled();
    auto facade = std::make_unique<flow::FacadeAttachment>(*rt, exec);
    std::thread spin([exec] { (void)aorta_executor_spin(exec); });

    flow::Scope scope = flow::Scope::root();
    std::atomic<bool> started{false};
    std::atomic<int> resolved{0};
    // Two swept knobs so the 200 iterations cover ALL THREE regimes —
    // fire-before-destroy, fire-during-sweep, revoke-before-fire: the
    // timer window (0.1..2ms) and a pre-destroy delay (0..2.46ms in a
    // 37µs-step pseudo-random sweep) that walks the destroy point back
    // and forth across the fire time.
    const auto window = std::chrono::microseconds(100 + 100 * (i % 20));
    const auto pre_destroy_delay =
        std::chrono::microseconds((static_cast<unsigned>(i) * 37) % 2460);
    ASSERT_TRUE(flow::spawn_on(*rt, scope,
                               short_disabled_sleeper(window, started,
                                                      resolved))
                    .ok());
    while (!started.load(std::memory_order_seq_cst)) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
    spin.join();  // stop-drain complete; the timer may fire ANY moment now
    facade.reset();
    std::this_thread::sleep_for(pre_destroy_delay);
    rt.reset();  // dtor revoke sweep vs a concurrent timer fire (CAS-once)
    // scope.close() is the completion barrier for a timer-thread resume
    // still in flight; afterwards the frame must have resolved EXACTLY
    // once, whichever side won the CAS.
    ASSERT_TRUE(scope.close().ok()) << "iteration " << i;
    ASSERT_EQ(resolved.load(std::memory_order_seq_cst), 1)
        << "iteration " << i
        << ": the arbiter exactly-once CAS must resolve the frame once";
    ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
  }
}

// ---- Regressions: stop with in-flight combinators ----------------------------

flow::Task<void> combinator_in_flight(flow::LaneRuntime& rt, flow::Scope scope,
                                      std::atomic<bool>& unwound) {
  struct Sentinel {
    std::atomic<bool>* f;
    ~Sentinel() { f->store(true, std::memory_order_seq_cst); }
  } sentinel{&unwound};
  // A join whose branches never complete on their own: only the
  // stop bridge can resolve this frame.
  auto [a, b] = co_await flow::join(
      rt, scope,
      [](flow::LaneRuntime& r) -> flow::Task<int> {
        co_await flow::sleep(r, std::chrono::seconds(600));
        co_return 1;
      }(rt),
      [](flow::LaneRuntime& r) -> flow::Task<int> {
        co_await flow::sleep(r, std::chrono::seconds(600));
        co_return 2;
      }(rt));
  (void)a;
  (void)b;
}

TEST_F(FacadeHarness, StopWithInFlightCombinatorResolvesTheParentFrame) {
  // Executor stop must resolve join/race branches (the
  // wrappers' unwind completes them CANCELLED via the RAII guard) — the
  // parent frame resumes, unwinds, and scope.close() returns instead of
  // hanging forever.
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          combinator_in_flight(*rt_, scope, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(50ms);  // suspended in the join
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  // No timeout crutch: a stranded frame hangs close() → bazel timeout.
  ASSERT_TRUE(scope.close().ok())
      << "the parent frame resolved through the stop bridge";
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst));
}

flow::Task<void> tight_timeout_loop(flow::LaneRuntime& rt, flow::Scope scope,
                                    std::atomic<int>& rounds) {
  for (int i = 0; i < 1000000; i++) {
    int v = co_await flow::timeout(
        rt, std::chrono::milliseconds(2),
        [](flow::LaneRuntime& r) -> flow::Task<int> {
          co_await flow::resume_on_lane{&r};
          co_return 5;
        }(rt),
        scope);
    (void)v;
    rounds.fetch_add(1, std::memory_order_seq_cst);
  }
}

TEST_F(FacadeHarness, TimerCompletionRacingStopNeverStrandsAFrame) {
  // Timers completing exactly as stop lands must still
  // deliver their resume (residue path) — hammer the window.
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> rounds{0};
  auto s = flow::spawn_on(*rt_, scope,
                          tight_timeout_loop(*rt_, scope, rounds));
  ASSERT_TRUE(s.ok());
  while (rounds.load(std::memory_order_seq_cst) < 20) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);  // mid-flight
  spin_.join();
  ASSERT_TRUE(scope.close().ok()) << "no stranded frame at any point";
}

TEST(FacadeQuiesce, HookNeverRunsAfterDetachReturns) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::SingleThread();
  auto attachment = std::make_unique<flow::FacadeAttachment>(*rt, exec);
  ASSERT_TRUE(attachment->attached());
  std::thread spin([exec] { (void)aorta_executor_spin(exec); });
  std::this_thread::sleep_for(20ms);
  // Post work continuously from another thread while detaching.
  std::atomic<bool> stop_posting{false};
  std::atomic<int> ran{0};
  std::thread poster([&] {
    while (!stop_posting.load(std::memory_order_acquire)) {
      aorta::lanes::Work w;
      w.run = [](void* ctx) {
        static_cast<std::atomic<int>*>(ctx)->fetch_add(
            1, std::memory_order_seq_cst);
      };
      w.ctx = &ran;
      (void)rt->pool().post(2, w);
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });
  std::this_thread::sleep_for(20ms);
  EXPECT_TRUE(attachment->detach().ok());  // quiesce barrier
  int at_detach = ran.load(std::memory_order_seq_cst);
  std::this_thread::sleep_for(50ms);
  // Items posted after detach are NOT drained by the spin (hook gone).
  // Allow only the in-flight batch that raced the barrier itself.
  int after = ran.load(std::memory_order_seq_cst);
  EXPECT_LE(after - at_detach, 16)
      << "the hook kept running after detach returned";
  stop_posting.store(true, std::memory_order_release);
  poster.join();
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  spin.join();
  rt->pool().close();
  rt->pool().drain_all();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// (telemetry v6): the facade's drain hook reports per-class-lane
// counters through aorta_executor_update_lane_telemetry — pinned by
// running work through the facade and checking the pool-side counters
// that feed the report (the core-side snapshot is covered by the Rust
// telemetry tests).
TEST_F(FacadeHarness, DrainHookFeedsLaneTelemetry) {
  std::atomic<int> ran{0};
  for (int i = 0; i < 8; ++i) {
    aorta::lanes::Work w;
    w.run = [](void* c) { static_cast<std::atomic<int>*>(c)->fetch_add(1); };
    w.drop = [](void*) {};
    w.ctx = &ran;
    ASSERT_EQ(rt_->pool().post(2, w), aorta::lanes::Post::OK);
  }
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (ran.load() < 8 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(ran.load(), 8);
  const auto st = rt_->pool().stats(2);
  EXPECT_GE(st.enqueued, 8u);
  EXPECT_GE(st.high_water, 1u) << "high-water tracked at enqueue";
}

// ---- carry-forwards: MT-mode facade -----------------------------------

class MtFacadeHarness : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_MULTI, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::MultiThread({0, 0, 3, 0});
    attachment_ = std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached()) << "D6b pairing: MT + MT accepted";
    spin_ = std::thread([this] { aorta_executor_spin(exec_); });
    std::this_thread::sleep_for(20ms);
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

TEST_F(MtFacadeHarness, MtSmokeLaneTasksRunOnWorkers) {
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> hops{0};
  std::atomic<std::thread::id> last_on{};
  auto s = flow::spawn_on(*rt_, scope, hop_n(*rt_, 5, hops, last_on));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (hops.load() < 5 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(hops.load(), 5);
  EXPECT_NE(last_on.load(), std::this_thread::get_id())
      << "MT: frames run on pool workers, not the test thread";
  scope.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(scope.close().ok());
}

namespace {
struct MePayload {
  std::atomic<int>* concurrent;
  std::atomic<int>* max_seen;
  std::atomic<int>* done;
};
void me_run(void* c) {
  auto* p = static_cast<MePayload*>(c);
  const int now = p->concurrent->fetch_add(1) + 1;
  int prev = p->max_seen->load();
  while (now > prev && !p->max_seen->compare_exchange_weak(prev, now)) {
  }
  std::this_thread::sleep_for(1ms);
  p->concurrent->fetch_sub(1);
  p->done->fetch_add(1);
  delete p;
}
void me_drop(void* c) { delete static_cast<MePayload*>(c); }
}  // namespace

TEST_F(MtFacadeHarness, ExplicitGroupHoldsMutualExclusionAcrossWorkers) {
  // wiring pin: an explicit-ME group descriptor routes through ONE
  // SerialGroup — members never overlap even with 3 workers on the lane.
  std::atomic<int> concurrent{0}, max_seen{0}, done{0};
  // group_desc: type=1 (explicit-ME), declared class=2, handle=42.
  const uint64_t desc = (uint64_t{42} << 10) | (uint64_t{2} << 2) | 1;
  for (int i = 0; i < 24; ++i) {
    auto* p = new MePayload{&concurrent, &max_seen, &done};
    ASSERT_EQ(attachment_->forward_for_test(&me_run, &me_drop, p, 2, desc), 1);
  }
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (done.load() < 24 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(done.load(), 24);
  EXPECT_EQ(max_seen.load(), 1) << "mutual exclusion held";
}

TEST_F(MtFacadeHarness, ReentrantDescriptorAllowsOverlap) {
  std::atomic<int> concurrent{0}, max_seen{0}, done{0};
  for (int i = 0; i < 24; ++i) {
    auto* p = new MePayload{&concurrent, &max_seen, &done};
    ASSERT_EQ(attachment_->forward_for_test(&me_run, &me_drop, p, 2,
                                            /*reentrant*/ 2),
              1);
  }
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (done.load() < 24 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(done.load(), 24);
  EXPECT_GT(max_seen.load(), 1) << "reentrant items overlap on 3 workers";
}

TEST(MtFacadePairing, StExecutorRejectsMtPool) {
  // D6b: misconfig tests both directions (the MT+MT accept is the
  // harness; here ST executor + MT pool must fail).
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::MultiThread({0, 0, 2, 0});
  flow::FacadeAttachment att(*rt, exec);
  EXPECT_FALSE(att.attached()) << "ST executor + MT pool = INVALID_ARGUMENT";
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// ---- carry-forwards: remaining pins -----------------------------------

TEST_F(FacadeHarness, ReentrancyParityScopeCloseFromALaneFrame) {
  // A lane frame calling scope.close() re-enters the runtime from a
  // callback context — must classify REENTRANT (ST), exactly like a queue-path
  // executor callback would.
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> rc{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      *rt_, scope,
      [](flow::LaneRuntime& r, std::atomic<int>& rc,
         std::atomic<bool>& done) -> flow::Task<void> {
        co_await flow::resume_on_lane{&r};
        flow::Scope inner = flow::Scope::root();
        rc.store(aorta_scope_close(inner.handle()));
        done.store(true, std::memory_order_seq_cst);
      }(*rt_, rc, done));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load());
  EXPECT_EQ(rc.load(), AORTA_ERR_REENTRANT)
      << "lane frames carry the callback-context marks (D7 parity)";
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(MtFacadeHarness, ReentrancyParityOnWorkers) {
  // Same parity on MT lane workers (D7: workers set the mark pair).
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> rc{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      *rt_, scope,
      [](flow::LaneRuntime& r, std::atomic<int>& rc,
         std::atomic<bool>& done) -> flow::Task<void> {
        co_await flow::resume_on_lane{&r};
        flow::Scope inner = flow::Scope::root();
        rc.store(aorta_scope_close(inner.handle()));
        done.store(true, std::memory_order_seq_cst);
      }(*rt_, rc, done));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load());
  EXPECT_EQ(rc.load(), AORTA_ERR_REENTRANT);
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(MtFacadeHarness, StopViaStopHandleDrainsLaneFrames) {
  // Stop-handle origin (not aorta_executor_stop): the on_stop bridge
  // must fire for EVERY stop origin; parked lane frames resolve.
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> hops{0};
  std::atomic<std::thread::id> last_on{};
  auto s = flow::spawn_on(*rt_, scope, hop_n(*rt_, 1000000, hops, last_on));
  ASSERT_TRUE(s.ok());
  const auto hop_deadline = std::chrono::steady_clock::now() + 10s;
  while (hops.load() < 10 &&
         std::chrono::steady_clock::now() < hop_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_GE(hops.load(), 10) << "frame must start hopping before the stop";
  uint64_t sh = 0;
  ASSERT_EQ(aorta_executor_get_stop_handle(exec_, &sh), AORTA_OK);
  ASSERT_EQ(aorta_stop_handle_stop(sh), AORTA_OK);
  ASSERT_EQ(aorta_stop_handle_free(sh), AORTA_OK);
  spin_.join();
  EXPECT_TRUE(scope.close().ok())
      << "stop-handle origin bridges through on_stop; no stranded frame";
}

TEST_F(FacadeHarness, StInterleavingFairnessRatio) {
  // The spin thread services BOTH the lane backlog and its own queue;
  // neither source may starve: with a deep lane backlog, queue events
  // (here: post_resume items via the reserved path... simplest visible
  // second source: a second WAVE of lane posts at a different class)
  // must still make progress within the same spin session. We pin the
  // interleave by checking that realtime-class items posted AFTER a
  // deep low-class backlog complete BEFORE the backlog finishes.
  std::atomic<int> low_done{0}, rt_done{0};
  std::atomic<int> rt_seen_low_at{-1};
  for (int i = 0; i < 400; ++i) {
    aorta::lanes::Work w;
    w.run = [](void* c) {
      static_cast<std::atomic<int>*>(c)->fetch_add(1);
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    };
    w.ctx = &low_done;
    ASSERT_EQ(rt_->pool().post(3, w), aorta::lanes::Post::OK);
  }
  struct RtCtx {
    std::atomic<int>* rt_done;
    std::atomic<int>* low_done;
    std::atomic<int>* seen;
  };
  static RtCtx rt_ctx;
  rt_ctx = {&rt_done, &low_done, &rt_seen_low_at};
  for (int i = 0; i < 8; ++i) {
    aorta::lanes::Work w;
    w.run = [](void* c) {
      auto* ctx = static_cast<RtCtx*>(c);
      ctx->seen->store(ctx->low_done->load());
      ctx->rt_done->fetch_add(1);
    };
    w.ctx = &rt_ctx;
    ASSERT_EQ(rt_->pool().post(0, w), aorta::lanes::Post::OK);
  }
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while ((low_done.load() < 400 || rt_done.load() < 8) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(low_done.load(), 400);
  ASSERT_EQ(rt_done.load(), 8);
  EXPECT_LT(rt_seen_low_at.load(), 400)
      << "realtime items ran BEFORE the low-class backlog drained "
         "(class-ordered WRR interleave, no starvation)";
}

// ---- Facade battery, batch 2 ------------------------------------------------

TEST_F(MtFacadeHarness, MonotoneWarnFiresExactlyOnce) {
  // D4: a member whose OWN class outranks its group's declared class
  // trips the one-shot WARN (execution stays at the group class).
  std::atomic<int> done{0};
  // group_desc: explicit-ME (1), declared class 2 (normal), handle 7.
  const uint64_t desc = (uint64_t{7} << 10) | (uint64_t{2} << 2) | 1;
  struct Ctx {
    std::atomic<int>* done;
  };
  static Ctx ctx;
  ctx = {&done};
  auto run = [](void* c) { static_cast<Ctx*>(c)->done->fetch_add(1); };
  auto drop = [](void*) {};
  ASSERT_FALSE(attachment_->monotone_warned_for_test());
  // Member class 0 (realtime) > declared 2 → WARN latches.
  ASSERT_EQ(attachment_->forward_for_test(run, drop, &ctx, 0, desc), 1);
  // Second violation: latch already set (one-shot).
  ASSERT_EQ(attachment_->forward_for_test(run, drop, &ctx, 0, desc), 1);
  EXPECT_TRUE(attachment_->monotone_warned_for_test());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (done.load() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(done.load(), 2);
}

TEST_F(MtFacadeHarness, GroupTeardownAtDetachDeliversQueuedMembers) {
  // Pin: detach on a LIVE executor clears the group map while a
  // drain marker may still be queued — the shared_ptr cells keep the
  // group alive; queued members resolve (run or drop), never UAF
  // (ASan-covered in the sanitizer sweep).
  std::atomic<int> resolved{0};
  const uint64_t desc = (uint64_t{9} << 10) | (uint64_t{2} << 2) | 1;
  struct Ctx {
    std::atomic<int>* resolved;
  };
  static Ctx ctx;
  ctx = {&resolved};
  auto run = [](void* c) { static_cast<Ctx*>(c)->resolved->fetch_add(1); };
  auto drop = [](void* c) { static_cast<Ctx*>(c)->resolved->fetch_add(1); };
  for (int i = 0; i < 16; i++) {
    ASSERT_EQ(attachment_->forward_for_test(run, drop, &ctx, 2, desc), 1);
  }
  EXPECT_TRUE(attachment_->detach().ok());  // live pool, group map cleared
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (resolved.load() < 16 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(resolved.load(), 16) << "every member resolved post-detach";
}

TEST_F(FacadeHarness, ForwardedDeadlineCrossesTheClockEpochCorrectly) {
  // The deadline crosses the hook as RELATIVE ns and is
  // re-anchored in the lane's steady_clock domain. A generous window
  // must RUN (the epoch-mismatch bug insta-expired everything); a
  // 1ns window must take the expiry drop at dequeue.
  std::atomic<int> ran{0}, dropped{0};
  struct Ctx {
    std::atomic<int>* ran;
    std::atomic<int>* dropped;
  };
  static Ctx ctx;
  ctx = {&ran, &dropped};
  auto run = [](void* c) { static_cast<Ctx*>(c)->ran->fetch_add(1); };
  auto drop = [](void* c) { static_cast<Ctx*>(c)->dropped->fetch_add(1); };

  ASSERT_EQ(attachment_->forward_for_test(run, drop, &ctx, 2, 0,
                                          60ull * 1000 * 1000 * 1000),
            1);
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (ran.load() < 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(ran.load(), 1) << "60s window RUNS (no insta-expiry)";
  EXPECT_EQ(dropped.load(), 0);

  ASSERT_EQ(attachment_->forward_for_test(run, drop, &ctx, 2, 0, 1), 1);
  while (dropped.load() < 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(dropped.load(), 1) << "1ns window expires at dequeue";
  EXPECT_EQ(ran.load(), 1);
}
