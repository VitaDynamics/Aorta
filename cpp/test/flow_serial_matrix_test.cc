// SerialDomain — the misuse-matrix sweep:
// the F-3/F-4/F-6 diagnostics, the remaining ready-fast-path rows
// (queued-sample recv + ready aligned here; sleep-0 and already-cancelled
// live in flow_serial_test.cc; the pre-armed wait_wave row needs the
// baseline seam and lives in flow_serial_stopwindow_test.cc). The
// combinator ordering row (winner + parent continuation + loser drain
// in ONE drain-slice epoch) needs the slice-epoch seam and lives in
// flow_serial_stopwindow_test.cc too.
//
// F-1..F-11 index (where each row is pinned end-to-end):
//   F-1  flow_serial_test.cc  SpawnRefusesInvalidForeignAndTornDownDomains
//   F-2  flow_serial_test.cc  ForeignPlaneArmsRefuseLoudly (sleep,
//        resume_on_lane, resume_on, sleep_on, cancelled, join),
//        NoOpDomainsKeepForeignArmRefusals (ST + rollback),
//        CallWithForeignExecutorRefusesAtStamp;
//        THIS FILE            NotReadyReadinessVerbsRefuseForeignArms
//        (recv/aligned/wait_wave, forced not-ready);
//        flow_action_test.cc  SerialBoundForeignExecutorSendGoal-
//        RefusesAtStamp
//   F-3  THIS FILE            MonotoneWarnFiresOncePerDomain;
//        flow_serial_diag_log_test.cc F3WarnOncePerDomainOnMtStrandsOnly
//        (level/count/negative legs + domain-lane stats)
//   F-4  THIS FILE            UnboundSpawnInsideSliceLogsOnce;
//        flow_serial_diag_log_test.cc F4DebugOnceThroughBothOverloads
//   F-5  flow_serial_test.cc  CanaryDetectsRawHandleResumeAtNextVerbEntry
//   F-6  THIS FILE            BlockingWaitInsideSliceWarnsOnce;
//        flow_serial_diag_log_test.cc F6WarnOnceOnSameDomainDoneHandle
//   F-7  flow_serial_test.cc  DroppedLastHandleWhileBoundFramesHop...
//   F-7b unreachable by construction (ownership invariant: frame
//        promises co-own the state; no test can mint the forbidden
//        state without breaking the invariant it guards)
//   F-8  flow_serial_test.cc  JoinBranchesInheritTheDomain...,
//        ExplicitSpawnEscapesTheDomain
//   F-9  flow_serial_test.cc  ExternalStopStampsParkedBoundFrame...,
//        RuntimeTeardownWithParkedBoundFrameStampsCancel;
//        flow_serial_stopwindow_test.cc QueuedBoundStartDropRunsNoUser-
//        Statement + QueuedMemberDropDeliversInsideTheSliceMark (the
//        queued marker drop → member drop, slice-marked)
//   F-10 lanes tier, lanes_test.cc ResumeMemberExemptFromLane-
//        Budgets + MarkerDroppedWhileNotifiedTerminates — the
//        never-shed intake semantics live below the flow tier
//   F-11 flow_serial_test.cc  LateCallCompletionAfterTeardownStamps...

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"
#include "aorta/flow_readiness.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/flow_subscriber.hpp"
#include "aorta/node.h"
#include "aorta/publisher.h"
#include "aorta/subscriber.h"

namespace flow = aorta::flow;
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

const void* group_of(const flow::SerialDomain& d) {
  const auto& st = flow::detail::serial_access::state(d);
  return st != nullptr ? static_cast<const void*>(st->group.get()) : nullptr;
}

// ---------------------------------------------------------------------------
// F-3: a bound task declared MORE urgent than its domain executes at
// the DOMAIN class; one-shot per-domain WARN.
// ---------------------------------------------------------------------------

flow::Task<void> record_slice(const void* group, std::atomic<int>* verdict) {
  verdict->store(aorta::lanes::g_serial_slice_group == group ? 1 : 2,
                 std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerialMatrix, MonotoneWarnFiresOncePerDomain) {
  auto rt = flow::Runtime::MultiThread({0, 0, 0, 1});
  ASSERT_TRUE(rt.ok());
  // Domain at kLow (lane 3); a DEFAULT scope resolves to kNormal
  // (lane 2) — more urgent than the domain: the monotone case.
  auto d = rt->create_serial(aorta::EventPriority::kLow, "monotone");
  ASSERT_TRUE(d.valid());
  const auto& st = flow::detail::serial_access::state(d);
  EXPECT_FALSE(st->monotone_warned.load(std::memory_order_acquire));

  flow::Scope scope = rt->root_scope();
  std::atomic<int> verdict{0};
  auto h = rt->spawn(scope, d, record_slice(group_of(d), &verdict));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_TRUE(st->monotone_warned.load(std::memory_order_acquire))
      << "a bound task above the domain's class must latch the one-shot "
         "WARN (F-3)";
  EXPECT_EQ(verdict.load(), 1)
      << "the bound task executes INSIDE the domain slice — i.e. at the "
         "DOMAIN class (its strand rides the domain's lane)";

  // One-shot: a second over-urgent spawn does not double-log (the latch
  // is already set; nothing to assert beyond it staying set — the
  // exchange is the log gate).
  std::atomic<int> verdict2{0};
  auto h2 = rt->spawn(scope, d, record_slice(group_of(d), &verdict2));
  ASSERT_TRUE(h2.ok());
  ASSERT_TRUE(h2.wait_for(10s));
  EXPECT_TRUE(st->monotone_warned.load(std::memory_order_acquire));
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// F-4: an UNBOUND spawn from inside a domain drain slice logs once at
// DEBUG — the legal parallel escape.
// ---------------------------------------------------------------------------

flow::Task<void> noop_child(flow::Scope, std::atomic<bool>* ran) {
  ran->store(true, std::memory_order_seq_cst);
  co_return;
}

flow::Task<void> escape_from_slice(const flow::Runtime* rt, flow::Scope scope,
                                   std::atomic<bool>* child_ran) {
  // Inside the domain slice: an UNBOUND spawn — legal, DEBUG-logged.
  auto h = rt->spawn_with(scope, &noop_child, child_ran);
  std::vector<flow::TaskHandle<void>> hv;
  hv.push_back(std::move(h));
  auto joined = co_await flow::when_all(std::move(hv));
  if (!joined.ok()) {
    throw std::runtime_error("escape_from_slice: child join was not ok");
  }
  co_return;
}

TEST(FlowSerialMatrix, UnboundSpawnInsideSliceLogsOnce) {
  flow::detail::g_serial_f4_logged.store(false, std::memory_order_seq_cst);
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "f4");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> child_ran{false};
  auto h = rt->spawn(scope, d, escape_from_slice(&*rt, scope, &child_ran));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok()) << "escape_from_slice parent must complete ok";
  EXPECT_TRUE(child_ran.load()) << "the escape child must RUN (legal)";
  EXPECT_TRUE(flow::detail::g_serial_f4_logged.load(std::memory_order_acquire))
      << "the F-4 DEBUG aid must latch on an unbound spawn from inside "
         "a domain slice";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// F-6: a blocking wait_for inside a domain drain slice WARNs (best
// effort; the awaited handle here is already done, so no deadlock).
// ---------------------------------------------------------------------------

flow::Task<void> blocking_wait_probe(const flow::Runtime* rt,
                                     flow::Scope scope) {
  std::atomic<bool> done{false};
  auto h = rt->spawn_with(
      scope, [](flow::Scope, std::atomic<bool>* d2) -> flow::Task<void> {
        d2->store(true, std::memory_order_seq_cst);
        co_return;
      },
      &done);
  // Let the (unbound, parallel) child finish first so the blocking wait
  // below returns immediately — the CHECK, not the deadlock, is under
  // test.
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  (void)h.wait_for(1s);  // F-6: fires the aid from inside the slice
  co_return;
}

TEST(FlowSerialMatrix, BlockingWaitInsideSliceWarnsOnce) {
  flow::detail::g_serial_f6_warned.store(false, std::memory_order_seq_cst);
  // TWO workers: the probe's in-slice wait must not starve the unbound
  // child it waits on (the second worker runs it in parallel).
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "f6");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, d, blocking_wait_probe(&*rt, scope));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_TRUE(flow::detail::g_serial_f6_warned.load(std::memory_order_acquire))
      << "wait_for inside a domain slice must latch the F-6 WARN";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Ready fast paths on a FOREIGN plane (bound frame): completions
// that never arm are allowed — a parked arm would be F-2-refused, so a
// clean completion IS the proof the fast path was taken.
// ---------------------------------------------------------------------------

flow::Task<void> recv_fast_path(aorta::PullSubscriber& sub,
                                flow::LaneRuntime* rtb, flow::Scope scope,
                                std::atomic<int>* verdict) {
  aorta::PullSample s = co_await flow::recv(sub, *rtb, std::move(scope));
  verdict->store(s.Size() > 0 ? 1 : 2, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerialMatrix, QueuedSampleRecvFastPathCompletesOnForeignPlane) {
  auto node = aorta::Node::Create("serial_matrix_recv");
  ASSERT_TRUE(node.ok());
  auto sub = node.value()->CreatePullSubscriber("test/serial_fastpath", 4);
  ASSERT_TRUE(sub.ok());
  auto pub = node.value()->CreatePublisher("test/serial_fastpath");
  ASSERT_TRUE(pub.ok());
  std::this_thread::sleep_for(300ms);
  const uint8_t payload[] = {0x7E, 0x7E};
  ASSERT_TRUE(pub.value()->PublishBytes(payload, sizeof(payload)).ok());
  // PROOF the sample is QUEUED pre-await (a fixed sleep
  // could spawn before it lands and fake-green a missing foreign gate
  // through the PARKED path): the non-popping pending probe.
  {
    uint8_t pending = 0;
    ASSERT_TRUE(wait_until([&] {
      return aorta_sub_pending(sub.value().native_handle(), &pending) ==
                 AORTA_OK &&
             pending != 0;
    })) << "the published sample never queued";
  }

  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "recv-fast");
  flow::Scope scope = rta->root_scope();
  std::atomic<int> verdict{0};
  auto h = rta->spawn(scope, d,
                      recv_fast_path(sub.value(), &rtb->lanes(), scope,
                                     &verdict));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok())
      << "a QUEUED sample completes on the ready fast path without an "
         "arm — no F-2 refusal may fire";
  EXPECT_EQ(verdict.load(), 1);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> aligned_fast_try(flow::WindowSync* ws, flow::LaneRuntime* rtb,
                                  flow::Scope scope,
                                  std::atomic<int>* verdict) {
#if defined(__cpp_exceptions)
  try {
    flow::AlignedSet set = co_await flow::aligned(*ws, *rtb, std::move(scope));
    (void)set;
    verdict->store(1, std::memory_order_seq_cst);  // fast path completed
  } catch (...) {
    verdict->store(2, std::memory_order_seq_cst);  // parked → F-2 refusal
  }
#endif
  co_return;
}

TEST(FlowSerialMatrix, ReadyAlignedFastPathCompletesOnForeignPlane) {
  auto node = aorta::Node::Create("serial_matrix_aligned");
  ASSERT_TRUE(node.ok());
  auto pull = node.value()->CreatePullSubscriber("test/serial_aligned", 4);
  ASSERT_TRUE(pull.ok());
  auto ws = flow::WindowSync::create(1, 1s);
  ASSERT_TRUE(ws.ok());
  ASSERT_EQ(flow::bind_window(pull.value(), *ws, 0), AORTA_OK);
  auto pub = node.value()->CreatePublisher("test/serial_aligned");
  ASSERT_TRUE(pub.ok());
  std::this_thread::sleep_for(300ms);

  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "aligned-fast");

  // Publish-then-try until one attempt lands on the READY fast path
  // (the ready set stays queued in the window until consumed, so after
  // the sample lands one attempt deterministically fast-paths; a parked
  // attempt is F-2-refused and retried).
  const uint8_t payload[] = {0x11};
  int fast = 0;
  for (int i = 0; i < 100 && fast != 1; ++i) {
    ASSERT_TRUE(pub.value()->PublishBytes(payload, 1).ok());
    std::this_thread::sleep_for(20ms);
    flow::Scope scope = rta->root_scope();
    std::atomic<int> verdict{0};
    auto h = rta->spawn(scope, d,
                        aligned_fast_try(&*ws, &rtb->lanes(), scope,
                                         &verdict));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    if (verdict.load() == 1) {
      fast = 1;
    }
    (void)scope.close();
  }
  EXPECT_EQ(fast, 1)
      << "a ready aligned set must complete on the fast path without an "
         "arm on the foreign plane";
}


// ---------------------------------------------------------------------------
// The NOT-READY foreign-plane ARM refusals for the
// three readiness verbs — the F-2 gate the fast-path rows deliberately
// bypass. Nothing is published/satisfied, so every await must ARM — and
// the foreign arm must refuse (the frame unwinds; the handle resolves
// stopped).
// ---------------------------------------------------------------------------

flow::Task<void> arm_foreign_recv(aorta::PullSubscriber& sub,
                                  flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::recv(sub, *rtb, std::move(scope));
  co_return;
}

flow::Task<void> arm_foreign_aligned(flow::WindowSync* ws,
                                     flow::LaneRuntime* rtb,
                                     flow::Scope scope) {
  (void)co_await flow::aligned(*ws, *rtb, std::move(scope));
  co_return;
}

flow::Task<void> arm_foreign_wave(flow::ReadinessSet* set,
                                  flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::wait_wave(*set, *rtb, std::move(scope));
  co_return;
}

TEST(FlowSerialMatrix, NotReadyReadinessVerbsRefuseForeignArms) {
  auto node = aorta::Node::Create("serial_matrix_notready");
  ASSERT_TRUE(node.ok());
  auto sub = node.value()->CreatePullSubscriber("test/serial_notready", 4);
  ASSERT_TRUE(sub.ok());
  auto pull2 = node.value()->CreatePullSubscriber("test/serial_notready2", 4);
  ASSERT_TRUE(pull2.ok());
  auto ws = flow::WindowSync::create(1, 1s);
  ASSERT_TRUE(ws.ok());
  ASSERT_EQ(flow::bind_window(pull2.value(), *ws, 0), AORTA_OK);
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());

  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "notready");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rta->root_scope();

  struct Row {
    const char* what;
    flow::TaskHandle<void> h;
  };
  std::vector<Row> rows;
  rows.push_back({"recv(sub, rtB) not-ready",
                  rta->spawn(scope, d,
                             arm_foreign_recv(sub.value(), &rtb->lanes(),
                                              scope))});
  rows.push_back({"aligned(ws, rtB) not-ready",
                  rta->spawn(scope, d,
                             arm_foreign_aligned(&*ws, &rtb->lanes(),
                                                 scope))});
  rows.push_back({"wait_wave(set, rtB) not-ready",
                  rta->spawn(scope, d,
                             arm_foreign_wave(&*set, &rtb->lanes(),
                                              scope))});
  for (auto& row : rows) {
    ASSERT_TRUE(row.h.ok()) << row.what;
    ASSERT_TRUE(row.h.wait_for(10s)) << row.what;
    auto o = row.h.take_outcome();
    ASSERT_TRUE(o.has_value()) << row.what;
    EXPECT_FALSE(o->ok())
        << row.what << ": a not-ready foreign-plane arm must refuse "
        << "(F-2) — it must never park against the foreign runtime";
  }
  ASSERT_TRUE(scope.close().ok());
}
}  // namespace
