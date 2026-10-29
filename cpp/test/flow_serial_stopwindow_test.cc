// SerialDomain — the STOP-WINDOW family, driven through the
// core `test-seams` cargo feature (a pause slot in BOTH Rust stop paths
// between the stop-flag store and the on_stop bridge, installed via the
// feature-gated `aorta_test_stop_window_seam` C symbol) plus the
// AORTA_LANES_TEST_SEAMS router pin seam. This binary links
// //core:aorta_cpp_test_seams — the ONLY Core with the feature — and
// must NEVER share a binary with the regular :aorta_cpp (one Core per
// binary; two registries would split executor handles).
//
// Rows: the stop-flag-window (final drain cannot
// zero-exit before BRIDGED; a pre-OR pinned producer's post is
// delivered), no-run-before-BRIDGED (the drain hook's entry check runs
// NO queued work while the bridge is pending; post-BRIDGED the work
// drains to DROP), the deterministic concurrent-double-stop (a loser
// on_stop past a completed bridge returns without re-bridging), and the
// external-stop pinned-window (a REAL router pin held between admission
// and post keeps the final drain alive; the released producer's residue
// delivers CANCEL).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"
#include "aorta/flow_readiness.hpp"
#include "aorta/flow_runtime.hpp"

extern "C" {
// The feature-gated seam installer (core test-seams feature; absent
// from release cores and excluded from the public header by name).
aorta_result_t aorta_test_stop_window_seam(void (*cb)(void*), void* ctx);
}

namespace flow = aorta::flow;
namespace lanes = aorta::lanes;
using namespace std::chrono_literals;

namespace {

template <typename Pred>
bool wait_until(Pred pred, std::chrono::seconds budget = 5s) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (!pred() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

/// A blocking gate the seam callbacks park on.
struct SeamGate {
  std::mutex m;
  std::condition_variable cv;
  bool open = false;
  std::atomic<uint32_t> arrivals{0};
  /// PassFirstN mode: arrivals 1..pass_first flow
  /// through unparked (e.g. a bound spawn's I-1a initial-dispatch pin);
  /// later arrivals park. 0 = park every arrival.
  std::atomic<uint32_t> pass_first{0};

  static void park(void* ctx) {
    auto* self = static_cast<SeamGate*>(ctx);
    const uint32_t n =
        self->arrivals.fetch_add(1, std::memory_order_seq_cst) + 1;
    if (n <= self->pass_first.load(std::memory_order_seq_cst)) {
      return;
    }
    std::unique_lock<std::mutex> lk(self->m);
    self->cv.wait(lk, [self] { return self->open; });
  }
  void release() {
    {
      std::lock_guard<std::mutex> lk(m);
      open = true;
    }
    cv.notify_all();
  }
};

/// Manual lifetime wiring (the facade probes are the observables the
/// Runtime wrapper hides): executor + MT LaneRuntime + facade + spin.
struct Harness {
  uint64_t exec = 0;
  std::unique_ptr<flow::LaneRuntime> rt;
  std::unique_ptr<flow::FacadeAttachment> facade;
  std::shared_ptr<flow::SerialControl> control;
  std::thread spin;
  std::atomic<bool> spin_done{false};

  void up(std::vector<uint32_t> workers) {
    const bool mt = !workers.empty();
    ASSERT_EQ(aorta_executor_new(
                  mt ? AORTA_EXECUTOR_MULTI : AORTA_EXECUTOR_SINGLE, &exec),
              AORTA_OK);
    rt = mt ? flow::LaneRuntime::MultiThread(std::move(workers))
            : flow::LaneRuntime::SingleThread();
    facade = std::make_unique<flow::FacadeAttachment>(*rt, exec);
    ASSERT_TRUE(facade->attached());
    control = std::make_shared<flow::SerialControl>();
    facade->bind_serial_control(control);
    spin = std::thread([this] {
      (void)aorta_executor_spin(exec);
      spin_done.store(true, std::memory_order_seq_cst);
    });
  }

  void down() {
    if (spin.joinable()) {
      spin.join();
    }
    if (facade != nullptr) {
      (void)facade->detach();
      facade.reset();
    }
    rt.reset();
    if (exec != 0) {
      ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
      exec = 0;
    }
  }

  flow::SerialDomain make_domain(size_t lane = 2,
                                 uint32_t batch_budget = 16) {
    auto st = std::make_shared<flow::detail::SerialDomainState>();
    st->group = std::make_shared<lanes::SerialGroup>(rt->pool(), lane,
                                                     batch_budget, true);
    st->group->bind_self(st->group);
    st->gate = std::shared_ptr<flow::detail::AdmissionGate>(control,
                                                            &control->gate);
    st->rt = rt.get();
    st->name = "seam-domain";
    control->live_domains.fetch_add(1, std::memory_order_acq_rel);
    // NOTE: the manual state skips the Runtime deleter — balance the
    // count in the test body before detach.
    return flow::detail::serial_access::make(std::move(st));
  }
};

// ---------------------------------------------------------------------------
// Row 1: the stop-flag-window — with the stop PAUSED between the flag
// store and on_stop, the spin thread's final drain cannot zero-exit
// (the entry check reports 1 while the bridge is pending); after the
// release the bridge completes and a pre-OR pinned producer's post is
// still counted and delivered.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, FinalDrainWaitsForBridgeAndPinnedProducer) {
  Harness h;
  h.up({0, 0, 1, 0});

  SeamGate gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &gate), AORTA_OK);

  // A pre-OR pinned producer, mid-post when the stop lands.
  ASSERT_TRUE(h.control->gate.pin());

  std::thread stopper([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return gate.arrivals.load() >= 1; }));

  // The window is OPEN: flag published, bridge pending — the spin
  // thread must not exit (entry check reports 1).
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(h.spin_done.load()) << "final drain zero-exited inside the "
                                      "stop window (before BRIDGED)";
  EXPECT_EQ(h.facade->bridge_state_for_test(), 0u);  // UNBRIDGED

  gate.release();
  stopper.join();
  ASSERT_TRUE(wait_until(
      [&] { return h.facade->bridge_state_for_test() == 2u; }));
  EXPECT_TRUE(h.control->gate.closed()) << "the winner closes the gate";

  // Bridge done, but the PIN window is still open: remaining ≥ 1.
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(h.spin_done.load())
      << "final drain returned while a producer pin was held";

  // The pinned producer finishes its bounded post (a residue delivery —
  // the pool is closed) and unpins; the final drain delivers it.
  std::atomic<uint32_t> delivered{0};
  lanes::Work w;
  w.run = [](void* c) {
    static_cast<std::atomic<uint32_t>*>(c)->fetch_add(1);
  };
  w.drop = [](void* c) {
    static_cast<std::atomic<uint32_t>*>(c)->fetch_add(1);
  };
  w.ctx = &delivered;
  h.rt->pool().post_residue(w);
  h.control->gate.unpin();

  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }))
      << "the final drain never completed after the unpin";
  EXPECT_EQ(delivered.load(), 1u)
      << "the pre-OR producer's post was stranded";

  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  h.down();
}

// ---------------------------------------------------------------------------
// Row 2: no-run-before-BRIDGED — the executor's OWN final-drain hook
// must not run queued pool work while the bridge is pending (the STOP
// PROTOCOL'S drain may not extend user work); after the bridge it
// drains to DROP (close-before-drain). Scope: this norm binds the
// FINAL-DRAIN path only — hence the zero-worker lane below. MT lane
// workers running already-queued work until pool.close() is the
// pre-existing lane-plane async-stop semantic (stop is anchored at
// pool close; public stop is signaling-only), for bound and unbound
// work alike.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, QueuedWorkNeverRunsInsideTheStopWindow) {
  Harness h;
  h.up({0, 0, 1, 0});

  // Queue work on a ZERO-WORKER lane (class 0): only the spin thread's
  // drain hook can take it.
  std::atomic<uint32_t> ran{0};
  std::atomic<uint32_t> dropped{0};
  struct Probe {
    std::atomic<uint32_t>* ran;
    std::atomic<uint32_t>* dropped;
  } probe{&ran, &dropped};
  lanes::Work w;
  w.run = [](void* c) { static_cast<Probe*>(c)->ran->fetch_add(1); };
  w.drop = [](void* c) { static_cast<Probe*>(c)->dropped->fetch_add(1); };
  w.ctx = &probe;

  SeamGate gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &gate), AORTA_OK);
  std::thread stopper([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return gate.arrivals.load() >= 1; }));

  // Inside the window: post the work, then drive the hook directly —
  // the entry check must return WITHOUT touching the pool.
  ASSERT_EQ(h.rt->pool().post_resume_item(0, w), lanes::Post::OK);
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(h.facade->drain_for_test(64), 1u)
        << "the bridge-pending drain hook must report 1 and do nothing";
  }
  EXPECT_EQ(ran.load(), 0u) << "queued work RAN inside the stop window";
  EXPECT_EQ(dropped.load(), 0u);

  gate.release();
  stopper.join();
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }));
  EXPECT_EQ(ran.load(), 0u)
      << "close-before-drain: post-BRIDGED delivery must be a DROP";
  EXPECT_EQ(dropped.load(), 1u);

  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  h.down();
}

// ---------------------------------------------------------------------------
// Row 3: deterministic concurrent-double-stop — stop A parks in the
// window; stop B runs to completion and bridges; the released A's
// on_stop LOSES the CAS and returns without re-bridging (the
// late-empty-on_stop hole).
// ---------------------------------------------------------------------------

struct FirstOnlyGate {
  SeamGate inner;
  std::atomic<bool> first_taken{false};

  static void park_first_only(void* ctx) {
    auto* self = static_cast<FirstOnlyGate*>(ctx);
    if (!self->first_taken.exchange(true, std::memory_order_seq_cst)) {
      SeamGate::park(&self->inner);  // stop A parks
      return;
    }
    self->inner.arrivals.fetch_add(1, std::memory_order_seq_cst);  // B passes
  }
};

TEST(FlowSerialStopWindow, LoserOnStopPastACompletedBridgeIsInert) {
  Harness h;
  h.up({0, 0, 1, 0});

  FirstOnlyGate gate;
  ASSERT_EQ(
      aorta_test_stop_window_seam(&FirstOnlyGate::park_first_only, &gate),
      AORTA_OK);

  std::thread stop_a([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return gate.first_taken.load(); }));

  // Stop B runs the WHOLE sequence while A is parked pre-on_stop.
  std::thread stop_b([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  stop_b.join();
  EXPECT_EQ(h.facade->bridge_state_for_test(), 2u)
      << "stop B must bridge while A is parked";
  EXPECT_TRUE(h.rt->pool().closed());
  EXPECT_TRUE(h.control->gate.closed());

  // Release A: its on_stop loses the CAS and returns — state stays
  // BRIDGED, nothing re-runs, nothing hangs.
  gate.inner.release();
  stop_a.join();
  EXPECT_EQ(h.facade->bridge_state_for_test(), 2u);
  EXPECT_EQ(h.facade->dbg_sweeps(), 1u)
      << "exactly ONE winner may run the bridge (arm sweep) — a loser "
         "re-running it is the double-stop hole";
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }));

  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  h.down();
}

// ---------------------------------------------------------------------------
// Row 4: the external-stop pinned-window — a REAL router pin (a bound
// frame's resolver mid-delivery) held between admission and post; the
// external stop's final drain must wait it out; the released producer's
// member lands in the residue and the frame observes CANCEL.
// ---------------------------------------------------------------------------

flow::Task<void> park_briefly(std::atomic<bool>* entered) {
  entered->store(true, std::memory_order_seq_cst);
  co_await flow::sleep(200ms);  // the resolver drives the routed resume
  co_return;
}

TEST(FlowSerialStopWindow, ExternalStopWaitsOutARealRouterPin) {
  Harness h;
  h.up({0, 0, 1, 0});
  auto d = h.make_domain(/*lane=*/2);

  SeamGate pin_gate;
  flow::Scope scope = flow::Scope::root({.name = "pin-window"});
  std::atomic<bool> entered{false};
  auto handle = flow::spawn_on(*h.rt, scope, d, park_briefly(&entered));
  ASSERT_TRUE(handle.ok());
  // Install the seam only AFTER the first slice ran — the I-1a bound
  // initial dispatch pins too, and parking the SPAWNING thread there
  // would deadlock the test itself.
  ASSERT_TRUE(wait_until([&] { return entered.load(); }));
  flow::detail::route_pin_seam_ctx = &pin_gate;
  flow::detail::route_pin_seam = &SeamGate::park;

  // The timer resolves ~200ms in; its routed resume PINS and parks on
  // the seam (a real producer mid-window).
  ASSERT_TRUE(wait_until([&] { return pin_gate.arrivals.load() >= 1; }));
  EXPECT_EQ(h.control->gate.live_pins(), 1u);

  // External stop while the pin is held: the final drain must NOT
  // return (remaining ≥ live pins).
  std::thread stopper([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  stopper.join();  // stop itself is signaling-only and returns
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(h.spin_done.load())
      << "the final drain returned while a router pin was held";

  // Release the producer: it posts into the CLOSED pool (residue) and
  // unpins; the final drain delivers the member as a CANCEL.
  pin_gate.release();
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }))
      << "the final drain never completed after the pin released";
  ASSERT_TRUE(handle.wait_for(10s));
  auto o = handle.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok()) << "the pinned producer's member must deliver "
                           "CANCEL through the residue, never a value";

  flow::detail::route_pin_seam = nullptr;
  flow::detail::route_pin_seam_ctx = nullptr;
  h.control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  h.down();
}


// ---------------------------------------------------------------------------
// Regression (queued-drop leg): a bound spawn QUEUED as a
// domain member and then dropped by the stop teardown must not run ONE
// user statement — the pre-start cancel stamps and the wrapper link
// refuses the child; the handle still resolves CANCELLED.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, QueuedBoundStartDropRunsNoUserStatement) {
  Harness h;
  h.up({0, 0, 1, 0});
  // Domain strand on lane 0 (zero workers): only the spin thread's
  // drain hook can take the marker — and the held stop window blocks it.
  auto d = h.make_domain(/*lane=*/0);

  SeamGate gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &gate), AORTA_OK);
  std::thread stopper([&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return gate.arrivals.load() >= 1; }));

  // Inside the window: the gate is still OPEN (on_stop pending), the
  // drains are blocked — the bound first slice stays QUEUED.
  flow::Scope scope = flow::Scope::root({.name = "queued-drop"});
  std::atomic<bool> user_ran{false};
  auto handle = flow::detail::spawn_with_handle(
      flow::co_invoke(
          [](flow::Scope, std::atomic<bool>* ran) -> flow::Task<void> {
            ran->store(true, std::memory_order_seq_cst);
            co_return;
          },
          scope.child(), &user_ran),
      scope, [&](flow::Task<void> wrapper) {
        return flow::detail::spawn_detached(*h.rt, scope, std::move(wrapper),
                                            &d);
      });
  ASSERT_TRUE(handle.ok());
  EXPECT_FALSE(user_ran.load());

  // Release: on_stop closes gate+pool; the final drain delivers the
  // queued marker as a DROP → member drop → pre-start cancel.
  gate.release();
  stopper.join();
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }));
  ASSERT_TRUE(handle.wait_for(10s));
  auto o = handle.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok())
      << "the queued bound start's drop must deliver CANCEL";
  EXPECT_FALSE(user_ran.load())
      << "a user statement executed on the queued-drop start";

  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  h.control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  h.down();
}

// ---------------------------------------------------------------------------
// Attach-vs-stop TOCTOU leg: a facade attaching INSIDE the held
// stop window (flag published, bridge pending) must take the
// detach-and-fail handshake — no successful facade on a stopping
// executor.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, AttachInsideTheStopWindowDetachesAndFails) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);

  SeamGate gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &gate), AORTA_OK);
  std::thread stopper([&exec] { EXPECT_EQ(aorta_executor_stop(exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return gate.arrivals.load() >= 1; }));

  // The flag is PUBLISHED (store precedes the seam); the recheck cannot
  // miss it — the attach must fail while the stop is still mid-window.
  auto rt = flow::LaneRuntime::SingleThread();
  flow::FacadeAttachment facade(*rt, exec);
  EXPECT_FALSE(facade.attached());
  ASSERT_NE(facade.attach_refusal(), nullptr);
  EXPECT_STREQ(facade.attach_refusal(), "executor already stopped");

  gate.release();
  stopper.join();
  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

// ---------------------------------------------------------------------------
// Regression: SerialDomain::valid() reads the stop flag
// UNDER A GATE PIN — a concurrent ~Core must block in close_and_quiesce
// until the read finishes, so the flag is never dereferenced against a
// freed executor. Driven deterministically through the valid()-pin seam.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, ValidPinHoldsRuntimeTeardownDuringFlagRead) {
  std::optional<flow::Outcome<flow::Runtime>> rt(
      flow::Runtime::MultiThread({0, 0, 1, 0}));
  ASSERT_TRUE(rt->ok());
  auto d = (*rt)->create_serial(aorta::EventPriority::kNormal, "valid-pin");
  ASSERT_TRUE(d.valid());

  SeamGate pin_gate;
  flow::detail::valid_pin_seam_ctx = &pin_gate;
  flow::detail::valid_pin_seam = &SeamGate::park;

  // T1 parks INSIDE valid(), pinned, about to read the stop flag.
  std::atomic<int> verdict{-1};
  std::thread reader([&] {
    verdict.store(d.valid() ? 1 : 0, std::memory_order_seq_cst);
  });
  ASSERT_TRUE(wait_until([&] { return pin_gate.arrivals.load() >= 1; }));
  // The reader's admission is REAL: exactly one live pin.
  const auto& st = flow::detail::serial_access::state(d);
  EXPECT_EQ(st->gate->live_pins(), 1u);
  // Disarm the seam so ~Core's own quiesce (and any later valid()) can
  // not park on it.
  flow::detail::valid_pin_seam = nullptr;
  flow::detail::valid_pin_seam_ctx = nullptr;

  // T2 destroys the Runtime: ~Core's step-0 close_and_quiesce must
  // BLOCK on T1's pin — the executor cannot be freed under the read.
  // The baseline MUST be sampled BEFORE the destroyer
  // exists — its quiesce counts ONCE per call, and a fast destroyer
  // could burn that increment before a late baseline read (permanent
  // false-negative, then std::terminate through the skipped joins).
  const uint64_t waits_before =
      flow::detail::AdmissionGate::dbg_quiesce_waits.load();
  std::atomic<bool> destroyed{false};
  std::thread destroyer([&] {
    rt.reset();
    destroyed.store(true, std::memory_order_seq_cst);
  });
  // Prove the destructor is INSIDE the step-0 quiesce
  // WAIT LOOP observing our pin — CLOSED alone is also produced by a
  // degenerate close()-only implementation, and on an MT runtime the
  // facade final-drain's pin report could masquerade as the barrier.
  ASSERT_TRUE(wait_until([&] {
    return flow::detail::AdmissionGate::dbg_quiesce_waits.load() >
           waits_before;
  })) << "close_and_quiesce never observed the live pin";
  EXPECT_TRUE(st->gate->closed());
  EXPECT_EQ(st->gate->live_pins(), 1u)
      << "the reader's pin must still be held against the quiesce";
  EXPECT_FALSE(destroyed.load())
      << "~Core completed while a valid() pin held the flag-read window";

  pin_gate.release();
  reader.join();
  destroyer.join();
  // The read completed against a LIVE executor (whatever it returned);
  // afterwards the domain reads dead.
  EXPECT_NE(verdict.load(), -1);
  EXPECT_FALSE(d.valid());
}

// ---------------------------------------------------------------------------
// Runtime::create SURFACES the stopped-before-attach
// detach-and-fail — through the real create path, via the pre-attach
// seam.
// ---------------------------------------------------------------------------

TEST(FlowSerialStopWindow, RuntimeCreateSurfacesStoppedBeforeAttach) {
  flow::create_pre_attach_seam = [](uint64_t exec) {
    ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  };
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  flow::create_pre_attach_seam = nullptr;
  ASSERT_FALSE(rt.ok())
      << "create must FAIL when the executor stopped before the attach";
  const auto& stopped = rt.stopped();
  ASSERT_TRUE(stopped.is_fault());
  EXPECT_NE(stopped.fault().message().find("executor already stopped"),
            std::string::npos)
      << stopped.fault().message();
}

// ---------------------------------------------------------------------------
// The no-op teardown-start rows through the PUBLIC spawn
// race arm — the stop lands AFTER d.valid()/pairing passed and BEFORE
// the dispatch (the admit seam window). ST refused-post + rollback
// refused-post legs; plus the queued/stop-drain delivery legs.
// ---------------------------------------------------------------------------

flow::Task<void> user_marker_body(flow::Scope, std::atomic<bool>* ran) {
  ran->store(true, std::memory_order_seq_cst);
  co_return;
}

void run_public_spawn_race_leg(bool rollback) {
  if (rollback) {
    ::setenv("AORTA_CPP_LANES", "0", 1);
  }
  auto rt = rollback ? flow::Runtime::FromEnv() : flow::Runtime::SingleThread();
  if (rollback) {
    ::unsetenv("AORTA_CPP_LANES");
  }
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "race-arm");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();

  SeamGate admit_gate;
  flow::detail::bound_spawn_admit_seam_ctx = &admit_gate;
  flow::detail::bound_spawn_admit_seam = &SeamGate::park;

  std::atomic<bool> user_ran{false};
  flow::TaskHandle<void> h;
  std::thread spawner([&] {
    h = rt->spawn_with(scope, d, &user_marker_body, &user_ran);
  });
  // The spawner PASSED d.valid() and is parked before the dispatch.
  ASSERT_TRUE(wait_until([&] { return admit_gate.arrivals.load() >= 1; }));
  flow::detail::bound_spawn_admit_seam = nullptr;
  flow::detail::bound_spawn_admit_seam_ctx = nullptr;

  // The stop lands INSIDE the race window.
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  admit_gate.release();
  spawner.join();

  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok())
      << "the raced bound start must deliver CANCEL through the public "
         "spawn arm";
  EXPECT_FALSE(user_ran.load())
      << "a user statement executed on the raced start";
  (void)scope.close();
}

TEST(FlowSerialStopWindow, PublicSpawnStopRaceStRunsNoUserStatement) {
  run_public_spawn_race_leg(/*rollback=*/false);
}

TEST(FlowSerialStopWindow, PublicSpawnStopRaceRollbackRunsNoUserStatement) {
  run_public_spawn_race_leg(/*rollback=*/true);
}

TEST(FlowSerialStopWindow, StNoOpQueuedStartDropRunsNoUserStatement) {
  // The ST queued-drop delivery leg, PUBLIC spawn, two seams: the
  // spawner passes d.valid() and parks at the ADMIT seam; the stop then
  // publishes the flag and parks at the STOP seam (drains blocked, pool
  // still open); the released spawner's lane post QUEUES; the released
  // stop closes the pool and the final drain delivers the queued first
  // slice through Work.drop → bound_start::drop. (Spawning INSIDE the
  // window through the public API is correctly F-1-refused — valid()
  // observes the flag — so the race arm needs the admit seam.)
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "st-queued");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();

  SeamGate admit_gate;
  flow::detail::bound_spawn_admit_seam_ctx = &admit_gate;
  flow::detail::bound_spawn_admit_seam = &SeamGate::park;
  std::atomic<bool> user_ran{false};
  flow::TaskHandle<void> h;
  std::thread spawner([&] {
    h = rt->spawn_with(scope, d, &user_marker_body, &user_ran);
  });
  ASSERT_TRUE(wait_until([&] { return admit_gate.arrivals.load() >= 1; }));
  flow::detail::bound_spawn_admit_seam = nullptr;
  flow::detail::bound_spawn_admit_seam_ctx = nullptr;

  SeamGate stop_gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &stop_gate),
            AORTA_OK);
  std::thread stopper(
      [&] { EXPECT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return stop_gate.arrivals.load() >= 1; }));

  // Inside the window: pool open, drains blocked — the released
  // spawner's first slice QUEUES on the lane.
  admit_gate.release();
  spawner.join();
  ASSERT_TRUE(h.ok());
  EXPECT_FALSE(user_ran.load());

  stop_gate.release();
  stopper.join();
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok());
  EXPECT_FALSE(user_ran.load())
      << "the ST queued bound start's DROP delivery ran user code";
  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  (void)scope.close();
}

flow::Task<void> spin_blocker_body(flow::Scope, std::atomic<bool>* entered,
                                   std::atomic<bool>* release) {
  entered->store(true, std::memory_order_seq_cst);
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (!release->load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  co_return;
}

TEST(FlowSerialStopWindow, RollbackStopDrainStartRunsNoUserStatement) {
  // The rollback stop-drain leg (bound_start::trampoline's non-OK
  // branch): the blocker occupies the spin thread, the bound start's
  // resume event QUEUES, the stop lands, and the final stop-drain
  // delivers the event with a non-OK status.
  ::setenv("AORTA_CPP_LANES", "0", 1);
  auto rt = flow::Runtime::FromEnv();
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "rb-drain");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();

  std::atomic<bool> blocker_entered{false};
  std::atomic<bool> blocker_release{false};
  auto hb = rt->spawn_with(scope, &spin_blocker_body, &blocker_entered,
                           &blocker_release);
  ASSERT_TRUE(hb.ok());
  ASSERT_TRUE(wait_until([&] { return blocker_entered.load(); }));

  // The spin thread is inside the blocker's body: the bound start's
  // executor post QUEUES behind it.
  std::atomic<bool> user_ran{false};
  auto h = rt->spawn_with(scope, d, &user_marker_body, &user_ran);
  ASSERT_TRUE(h.ok());

  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  blocker_release.store(true, std::memory_order_seq_cst);

  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok())
      << "the stop-drained bound start must deliver CANCEL (trampoline "
         "non-OK branch)";
  EXPECT_FALSE(user_ran.load())
      << "the rollback stop-drain start ran user code";
  ASSERT_TRUE(hb.wait_for(10s));
  (void)scope.close();
}

TEST(FlowSerialStopWindow, ValidPinHoldsRollbackTeardownDuringFlagRead) {
  // Disabled/rollback variant: NO facade drain barrier
  // exists on this leg — the step-0 quiesce is the ONLY thing standing
  // between the flag read and the executor free, so this row fails
  // against any implementation that degrades close_and_quiesce.
  ::setenv("AORTA_CPP_LANES", "0", 1);
  std::optional<flow::Outcome<flow::Runtime>> rt(flow::Runtime::FromEnv());
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rt->ok());
  EXPECT_FALSE((*rt)->lanes_attached());
  auto d = (*rt)->create_serial(aorta::EventPriority::kNormal, "rb-valid");
  ASSERT_TRUE(d.valid());

  SeamGate pin_gate;
  flow::detail::valid_pin_seam_ctx = &pin_gate;
  flow::detail::valid_pin_seam = &SeamGate::park;
  std::atomic<int> verdict{-1};
  std::thread reader([&] {
    verdict.store(d.valid() ? 1 : 0, std::memory_order_seq_cst);
  });
  ASSERT_TRUE(wait_until([&] { return pin_gate.arrivals.load() >= 1; }));
  const auto& st = flow::detail::serial_access::state(d);
  EXPECT_EQ(st->gate->live_pins(), 1u);
  flow::detail::valid_pin_seam = nullptr;
  flow::detail::valid_pin_seam_ctx = nullptr;

  // Baseline BEFORE the destroyer exists (see the MT row).
  const uint64_t waits_before =
      flow::detail::AdmissionGate::dbg_quiesce_waits.load();
  std::atomic<bool> destroyed{false};
  std::thread destroyer([&] {
    rt.reset();
    destroyed.store(true, std::memory_order_seq_cst);
  });
  ASSERT_TRUE(wait_until([&] {
    return flow::detail::AdmissionGate::dbg_quiesce_waits.load() >
           waits_before;
  })) << "the rollback ~Core's quiesce never observed the live pin";
  EXPECT_EQ(st->gate->live_pins(), 1u);
  EXPECT_FALSE(destroyed.load())
      << "the rollback ~Core completed under a held valid() pin — the "
         "flag read had no barrier";

  pin_gate.release();
  reader.join();
  destroyer.join();
  EXPECT_NE(verdict.load(), -1);
  EXPECT_FALSE(d.valid());
}

// ---------------------------------------------------------------------------
// Pre-armed wait_wave fast path: a wave landing between the
// baseline snapshot and the probe completes WITHOUT an arm — allowed on
// a foreign plane for a bound frame (a parked arm would be F-2-refused,
// so the clean completion IS the fast-path proof). Deterministic via
// the wave-baseline seam: the seam callback satisfies the set INLINE on
// the awaiting thread, exactly inside the production window.
// ---------------------------------------------------------------------------

flow::Task<void> wave_fast_probe(flow::ReadinessSet* set,
                                 flow::LaneRuntime* rtb, flow::Scope scope,
                                 std::atomic<int>* verdict) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::wait_wave(*set, *rtb, std::move(scope));
    verdict->store(1, std::memory_order_seq_cst);  // fast path completed
  } catch (...) {
    verdict->store(2, std::memory_order_seq_cst);  // parked → F-2 refusal
  }
#endif
  co_return;
}

TEST(FlowSerialStopWindow, PreArmedWaveFastPathCompletesOnForeignPlane) {
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());
  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "wave-fast");
  ASSERT_TRUE(d.valid());

  // The seam satisfies the set INSIDE the baseline→probe window.
  // RAII install/restore: a mid-test ASSERT must not
  // leave the globals pointing at a destroyed set.
  struct WaveSeamGuard {
    ~WaveSeamGuard() {
      flow::detail::wave_baseline_seam = nullptr;
      flow::detail::wave_baseline_seam_ctx = nullptr;
    }
  } seam_guard;
  // ctx FIRST, callback LAST: the fire site's callback
  // acquire-load is the publication gate for the PAIR.
  flow::detail::wave_baseline_seam_ctx = &*set;
  flow::detail::wave_baseline_seam = [](void* ctx) {
    (void)static_cast<flow::ReadinessSet*>(ctx)->satisfy(0);
  };

  flow::Scope scope = rta->root_scope();
  std::atomic<int> verdict{0};
  auto h = rta->spawn(scope, d,
                      wave_fast_probe(&*set, &rtb->lanes(), scope, &verdict));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok());
  EXPECT_EQ(verdict.load(), 1)
      << "a wave landing in the pre-armed window must complete on the "
         "ready fast path — no arm, no F-2 refusal";
  (void)scope.close();
}

// ---------------------------------------------------------------------------
// The combinator ordering row with a SAME-SLICE
// proof — winner, parent continuation, and loser drain share ONE drain
// slice epoch (the group pointer alone cannot prove slice identity),
// stay in-domain, and never overlap (busy windows held long enough that
// broken concurrency deterministically collides).
// ---------------------------------------------------------------------------

struct EpochOrderState {
  const void* group = nullptr;
  std::atomic<bool> busy{false};
  std::atomic<uint32_t> overlaps{0};
  std::atomic<uint32_t> off_domain{0};
  std::atomic<uint64_t> seq{0};
  std::atomic<uint64_t> winner_seq{0};
  std::atomic<uint64_t> parent_seq{0};
  std::atomic<uint64_t> loser_seq{0};
  std::atomic<uint64_t> winner_epoch{0};
  std::atomic<uint64_t> parent_epoch{0};
  std::atomic<uint64_t> loser_epoch{0};

  uint64_t enter(std::atomic<uint64_t>* epoch_out) {
    if (busy.exchange(true, std::memory_order_seq_cst)) {
      overlaps.fetch_add(1, std::memory_order_seq_cst);
    }
    if (aorta::lanes::g_serial_slice_group != group) {
      off_domain.fetch_add(1, std::memory_order_seq_cst);
    }
    epoch_out->store(aorta::lanes::g_serial_slice_epoch,
                     std::memory_order_seq_cst);
    // Hold the critical section long enough that a broken concurrent
    // implementation collides deterministically.
    const auto until = std::chrono::steady_clock::now() + 200us;
    while (std::chrono::steady_clock::now() < until) {
    }
    return seq.fetch_add(1, std::memory_order_seq_cst) + 1;
  }
  void exit() { busy.store(false, std::memory_order_seq_cst); }
};

// NO wall-clock races — every event is gated on an
// atomic released by the choreography, so the row is deterministic:
//   winner hops until go_winner → records → completes;
//   parent's continuation records → releases the loser;
//   loser hops until released → records LAST.
flow::Task<int> epoch_winner(EpochOrderState* os, std::atomic<bool>* started,
                             std::atomic<bool>* go) {
  started->store(true, std::memory_order_seq_cst);
  while (!go->load(std::memory_order_seq_cst)) {
    co_await flow::resume();
  }
  os->winner_seq.store(os->enter(&os->winner_epoch),
                       std::memory_order_seq_cst);
  os->exit();
  co_return 1;
}

flow::Task<int> epoch_loser(EpochOrderState* os, std::atomic<bool>* started,
                            std::atomic<bool>* release) {
  started->store(true, std::memory_order_seq_cst);
  while (!release->load(std::memory_order_seq_cst)) {
    co_await flow::resume();
  }
  os->loser_seq.store(os->enter(&os->loser_epoch),
                      std::memory_order_seq_cst);
  os->exit();
  co_return 2;
}

flow::Task<void> epoch_barrier(std::atomic<bool>* barrier_entered,
                               std::atomic<bool>* barrier_release) {
  // The slice-holding member: parks the DRAIN inside one slice while
  // the hop members queue behind it.
  barrier_entered->store(true, std::memory_order_seq_cst);
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (!barrier_release->load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
  }
  co_return;
}

flow::Task<void> epoch_parent(EpochOrderState* os,
                              std::atomic<bool>* winner_started,
                              std::atomic<bool>* go_winner,
                              std::atomic<bool>* loser_started,
                              std::atomic<bool>* release_loser,
                              std::atomic<int>* winner_value) {
  int v = co_await flow::race(epoch_winner(os, winner_started, go_winner),
                              epoch_loser(os, loser_started, release_loser));
  os->parent_seq.store(os->enter(&os->parent_epoch),
                       std::memory_order_seq_cst);
  os->exit();
  winner_value->store(v, std::memory_order_seq_cst);
  // The parent CONTINUATION releases the loser: its
  // pending hop member is already queued in THIS slice — the loser's
  // record lands after the parent's, same epoch.
  release_loser->store(true, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerialStopWindow, RaceEventsShareOneDrainSliceEpoch) {
  Harness h;
  h.up({0, 0, 1, 0});
  // NO pre-barrier churn — the barrier is the FIRST
  // member of a FRESH domain, so the slice's budget cursor is exactly
  // known (ran=0 at the barrier) and the whole bounded choreography
  // (~8 members) fits the default budget of 16 with room to spare. The
  // branch bodies only run AFTER the release, inside the barrier's
  // slice, gated on atomics — zero wall-clock, zero unbounded hops.
  auto d = h.make_domain(/*lane=*/2);
  EpochOrderState os;
  {
    const auto& st = flow::detail::serial_access::state(d);
    os.group = st->group.get();
  }
  flow::Scope scope = flow::Scope::root({.name = "epoch-order"});

  // 1. The barrier is the fresh strand's FIRST member and holds the
  // slice open.
  std::atomic<bool> barrier_entered{false};
  std::atomic<bool> barrier_release{false};
  auto hb = flow::spawn_on(*h.rt, scope, d,
                           epoch_barrier(&barrier_entered, &barrier_release));
  ASSERT_TRUE(hb.ok());
  ASSERT_TRUE(wait_until([&] { return barrier_entered.load(); }));

  // 2. The parent (and through it the race branches) queues BEHIND the
  // barrier; nothing of it runs yet.
  std::atomic<bool> winner_started{false};
  std::atomic<bool> go_winner{false};
  std::atomic<bool> loser_started{false};
  std::atomic<bool> release_loser{false};
  std::atomic<int> winner_value{0};
  auto hp = flow::spawn_on(*h.rt, scope, d,
                           epoch_parent(&os, &winner_started, &go_winner,
                                        &loser_started, &release_loser,
                                        &winner_value));
  ASSERT_TRUE(hp.ok());

  // 3. Arm the winner BEFORE anything runs: after the release the
  // winner's body sees go immediately (zero hops); the loser hops a
  // bounded once-or-twice until the parent continuation releases it.
  go_winner.store(true, std::memory_order_seq_cst);

  // 4. Release: ONE slice (the barrier's) drains the whole choreography
  // — parent initial → branch initials → winner record → parent
  // continuation record (which releases the loser) → loser record.
  barrier_release.store(true, std::memory_order_seq_cst);
  ASSERT_TRUE(hb.wait_for(10s));
  ASSERT_TRUE(hp.wait_for(10s));
  // Gate on loser_seq, the LAST write of the loser's record:
  // enter() publishes the epoch first, then spins ~200us before returning
  // the sequence number, so waiting on loser_epoch could sample loser_seq
  // while it was still 0 (seen under ASAN as "2 vs 0" on the ordering assert).
  ASSERT_TRUE(wait_until([&] {
    return os.loser_seq.load(std::memory_order_seq_cst) != 0;
  })) << "the loser drain never recorded";
  auto op = hp.take_outcome();
  ASSERT_TRUE(op.has_value());
  ASSERT_TRUE(op->ok());
  EXPECT_EQ(winner_value.load(), 1) << "the released branch wins";
  EXPECT_EQ(os.overlaps.load(), 0u);
  EXPECT_EQ(os.off_domain.load(), 0u);
  EXPECT_GT(os.winner_epoch.load(), 0u);
  EXPECT_EQ(os.winner_epoch.load(), os.parent_epoch.load())
      << "winner and parent continuation must share ONE drain slice";
  EXPECT_EQ(os.winner_epoch.load(), os.loser_epoch.load())
      << "the loser drain shares the same slice";
  EXPECT_LT(os.winner_seq.load(), os.parent_seq.load())
      << "the parent continuation follows the winner";
  EXPECT_LT(os.parent_seq.load(), os.loser_seq.load())
      << "the loser drain follows the parent continuation";

  ASSERT_EQ(aorta_executor_stop(h.exec), AORTA_OK);
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }));
  h.control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  h.down();
}

// ---------------------------------------------------------------------------
// F-9 end-to-end: a QUEUED domain member dropped by
// the stop teardown delivers the member DROP inside the domain's slice
// mark — recorded by a frame-local destructor during the unwind.
// ---------------------------------------------------------------------------

struct UnwindMarkRecorder {
  const void* group;
  std::atomic<int>* verdict;
  ~UnwindMarkRecorder() {
    verdict->store(aorta::lanes::g_serial_slice_group == group ? 1 : 2,
                   std::memory_order_seq_cst);
  }
};

flow::Task<void> parked_with_recorder(const void* group,
                                      std::atomic<int>* verdict,
                                      std::atomic<bool>* entered) {
  UnwindMarkRecorder rec{group, verdict};
  entered->store(true, std::memory_order_seq_cst);
  co_await flow::sleep(60s);  // parked; the resume member gets dropped
  co_return;
}

flow::Task<void> parked_with_recorder2(const void* group,
                                       std::atomic<int>* verdict,
                                       std::atomic<bool>* entered) {
  UnwindMarkRecorder rec{group, verdict};
  entered->store(true, std::memory_order_seq_cst);
  co_await flow::sleep(100ms);  // resolves NATURALLY while the gate is
                                // open — the pin seam catches it
  co_return;
}

TEST(FlowSerialStopWindow, QueuedMemberDropDeliversInsideTheSliceMark) {
  // The row must take the REAL queued-marker-drop path —
  // the router's CLOSED inline-cancel also carries a slice mark, so the
  // choreography pins the member into the queue BEFORE the gate closes:
  //   1. the sleep resolution PINS while the gate is OPEN and parks at
  //      the route-pin seam (before the member post);
  //   2. the stop parks at the stop-window seam (flag published, on_stop
  //      pending — the drain entry check now blocks all drains);
  //   3. the released producer posts the member + marker (pool still
  //      open → QUEUED, undrainable);
  //   4. the released stop closes gate + pool; the final drain delivers
  //      the marker DROP → drain_drop → member drop → the unwind runs
  //      INSIDE drain_drop's slice mark;
  //   5. the drain_drop entry counter proves the path.
  Harness h;
  h.up({0, 0, 1, 0});
  auto d = h.make_domain(/*lane=*/0);  // zero-worker lane: only the spin
                                       // thread's hook can drain it
  const void* group;
  {
    const auto& st = flow::detail::serial_access::state(d);
    group = st->group.get();
  }
  flow::Scope scope = flow::Scope::root({.name = "f9-mark"});
  std::atomic<int> verdict{0};
  std::atomic<bool> entered{false};
  auto handle = flow::spawn_on(*h.rt, scope, d,
                               parked_with_recorder(group, &verdict,
                                                    &entered));
  ASSERT_TRUE(handle.ok());
  ASSERT_TRUE(wait_until([&] { return entered.load(); }));

  // Re-park the frame on a SHORT sleep? No — parked_with_recorder
  // sleeps 60s; drive its resolution via the STOP SWEEP would close the
  // gate first. Instead: the pin seam catches the resolution of a
  // second, short-sleep bound task... Simpler and exact: revoke-driven
  // resolution is what closes the gate — so use a 100ms sleep task
  // whose TIMER resolves naturally while the gate is open.
  (void)handle;  // (the 60s task exercises the parked-frame leg below)

  // 1. Install the seam BEFORE the 100ms task exists (previously,
  // installing after `entered2` raced the timer — a >100ms scheduling
  // stall made a CORRECT implementation miss the seam — and the plain
  // pointer write raced the resolver's read; the seams are atomic now
  // and the install happens-before the spawn). PassFirst(1): arrival 1
  // is the task's own I-1a initial-dispatch pin (it flows through);
  // arrival 2 is the timer resolution — the one we park. The timer
  // cannot fire before its arm, and its arm follows arrival 1.
  SeamGate pin_gate;
  pin_gate.pass_first.store(1, std::memory_order_seq_cst);
  flow::detail::route_pin_seam_ctx = &pin_gate;
  flow::detail::route_pin_seam = &SeamGate::park;
  struct PinSeamGuard {
    ~PinSeamGuard() {
      flow::detail::route_pin_seam = nullptr;
      flow::detail::route_pin_seam_ctx = nullptr;
    }
  } pin_guard;

  std::atomic<int> verdict2{0};
  std::atomic<bool> entered2{false};
  auto h2 = flow::spawn_on(*h.rt, scope, d,
                           parked_with_recorder2(group, &verdict2,
                                                 &entered2));
  ASSERT_TRUE(h2.ok());
  ASSERT_TRUE(wait_until([&] { return entered2.load(); }));
  // Arrival 2 = the timer resolution, pinned and parked.
  ASSERT_TRUE(wait_until([&] { return pin_gate.arrivals.load() >= 2; }));
  EXPECT_EQ(h.control->gate.live_pins(), 1u);

  // 2. The stop parks in the window; drains are blocked from here.
  SeamGate stop_gate;
  ASSERT_EQ(aorta_test_stop_window_seam(&SeamGate::park, &stop_gate),
            AORTA_OK);
  std::thread stopper(
      [&h] { EXPECT_EQ(aorta_executor_stop(h.exec), AORTA_OK); });
  ASSERT_TRUE(wait_until([&] { return stop_gate.arrivals.load() >= 1; }));

  // 3. The producer posts the member + marker into the OPEN pool.
  const uint64_t drops_before =
      aorta::lanes::SerialGroup::dbg_drain_drop_entries.load();
  pin_gate.release();
  ASSERT_TRUE(wait_until([&] { return h.control->gate.live_pins() == 0; }));

  // 4. Release the stop: gate + pool close; the final drain delivers
  // the queued marker as a DROP.
  stop_gate.release();
  stopper.join();
  ASSERT_TRUE(wait_until([&] { return h.spin_done.load(); }));

  ASSERT_TRUE(h2.wait_for(10s));
  auto o2 = h2.take_outcome();
  ASSERT_TRUE(o2.has_value());
  EXPECT_FALSE(o2->ok());
  EXPECT_EQ(verdict2.load(), 1)
      << "the queued member's DROP must deliver the unwind INSIDE the "
         "domain's slice mark (F-9)";
  // 5. PATH PROOF: the delivery came through drain_drop (the marker
  // drop), not the router's CLOSED inline-cancel.
  EXPECT_GE(aorta::lanes::SerialGroup::dbg_drain_drop_entries.load(),
            drops_before + 1)
      << "the delivery never took the queued-marker drain_drop path";

  ASSERT_TRUE(handle.wait_for(10s));  // the 60s-parked frame: swept
  ASSERT_EQ(aorta_test_stop_window_seam(nullptr, nullptr), AORTA_OK);
  h.control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  h.down();
}
}  // namespace
