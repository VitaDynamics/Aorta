// SerialDomain — the flow-tier family: the mutual-exclusion guarantee
// under an MT hammer, first-segment exclusion (I-1a), combinator-branch
// inheritance, the F-2 arm-point refusals, the
// lifetime rows (external stop, teardown, detach refusal, attach
// handshake), the F-5 occupancy canary, and the ST/Disabled no-op leg.
// The stop-flag-window family (which needs the core `test-seams`
// feature) lives in flow_serial_stopwindow_test.cc — a SEPARATE binary
// linking the seam-enabled Core exclusively.
//
// Coroutine style: state rides PARAMETERS (bound into the frame), never
// lambda captures (the co_invoke trap — see flow.hpp header doc).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "aorta/executor.h"
#include "aorta/flow_action.hpp"
#include "aorta/flow_combinators.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/node.h"

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

/// The domain's lanes-layer identity, for slice-mark comparisons.
const void* group_of(const flow::SerialDomain& d) {
  const auto& st = flow::detail::serial_access::state(d);
  return st != nullptr ? static_cast<const void*>(st->group.get()) : nullptr;
}

// ---------------------------------------------------------------------------
// ST / Disabled no-op domains.
// ---------------------------------------------------------------------------

flow::Task<void> record_reader_domain(flow::SerialDomain* out) {
  *out = co_await flow::this_coro::serial_domain;
  co_return;
}

TEST(FlowSerial, StRuntimeCreatesValidNoOpDomainAndBoundSpawnRuns) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "st-noop");
  ASSERT_TRUE(d.valid());
  EXPECT_STREQ(d.name(), "st-noop");
  // No lanes strand exists — binding stamps, routing
  // short-circuits to the single-consumer path.
  EXPECT_EQ(group_of(d), nullptr);

  flow::Scope scope = rt->root_scope();
  flow::SerialDomain seen;
  auto h = rt->spawn(scope, d, record_reader_domain(&seen));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(5s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok());
  // this_coro reader: the binding is stamped even on the no-op leg.
  EXPECT_TRUE(seen.valid());
  EXPECT_STREQ(seen.name(), "st-noop");
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowSerial, RollbackLegCreatesValidNoOpDomain) {
  ::setenv("AORTA_CPP_LANES", "0", 1);
  auto rt = flow::Runtime::FromEnv();
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rt.ok());
  EXPECT_FALSE(rt->lanes_attached());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "rollback");
  ASSERT_TRUE(d.valid());
  EXPECT_EQ(group_of(d), nullptr);

  flow::Scope scope = rt->root_scope();
  flow::SerialDomain seen;
  auto h = rt->spawn(scope, d, record_reader_domain(&seen));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(5s));
  EXPECT_TRUE(seen.valid());
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// F-1: invalid / foreign / dead domains refuse with a handle that
// RESOLVES.
// ---------------------------------------------------------------------------

flow::Task<void> never_runs(std::atomic<bool>* ran) {
  ran->store(true, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerial, SpawnRefusesInvalidForeignAndTornDownDomains) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> ran{false};

  // Default-constructed (invalid) domain.
  auto h1 = rt->spawn(scope, flow::SerialDomain{}, never_runs(&ran));
  EXPECT_FALSE(h1.ok());
  EXPECT_EQ(h1.status, AORTA_ERR_INVALID_ARGUMENT);
  ASSERT_TRUE(h1.wait_for(1s));  // refused handles RESOLVE, never park

  // A domain created by a DIFFERENT runtime (B8 loud-invalid).
  auto other = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(other.ok());
  auto foreign = other->create_serial(aorta::EventPriority::kNormal, "other");
  ASSERT_TRUE(foreign.valid());
  auto h2 = rt->spawn(scope, foreign, never_runs(&ran));
  EXPECT_FALSE(h2.ok());
  EXPECT_EQ(h2.status, AORTA_ERR_INVALID_ARGUMENT);

  // A domain whose runtime was torn down (gate CLOSED → invalid).
  auto dying = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(dying.ok());
  auto dead = dying->create_serial(aorta::EventPriority::kNormal, "dead");
  ASSERT_TRUE(dead.valid());
  { auto grave = std::move(*dying); }  // ~Runtime: gate closes first
  EXPECT_FALSE(dead.valid());
  auto h3 = rt->spawn(scope, dead, never_runs(&ran));
  EXPECT_FALSE(h3.ok());
  EXPECT_EQ(h3.status, AORTA_ERR_INVALID_ARGUMENT);

  EXPECT_FALSE(ran.load());
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// I-1a first-segment exclusion + mutual exclusion under the MT
// hammer (TSan-meaningful: the shared counter is a PLAIN uint64_t).
// ---------------------------------------------------------------------------

struct HammerState {
  uint64_t counter = 0;                 // unlocked on purpose
  std::atomic<bool> busy{false};        // overlap detector
  std::atomic<uint32_t> overlaps{0};
  std::atomic<uint32_t> first_slice_in_domain{0};
  const void* group = nullptr;
};

flow::Task<void> hammer_body(HammerState* hs, int iters) {
  // I-1a: the FIRST user segment already runs inside the domain slice.
  if (aorta::lanes::g_serial_slice_group == hs->group) {
    hs->first_slice_in_domain.fetch_add(1, std::memory_order_seq_cst);
  }
  for (int i = 0; i < iters; ++i) {
    if (hs->busy.exchange(true, std::memory_order_seq_cst)) {
      hs->overlaps.fetch_add(1, std::memory_order_seq_cst);
    }
    hs->counter++;  // the domain's claim: serialized without a lock
    hs->busy.store(false, std::memory_order_seq_cst);
    co_await flow::resume();  // external re-entry through the router
  }
  co_return;
}

TEST(FlowSerial, MutualExclusionHoldsAcrossBoundTasksUnderMtHammer) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "hammer");
  ASSERT_TRUE(d.valid());
  ASSERT_NE(group_of(d), nullptr);

  HammerState hs;
  hs.group = group_of(d);
  constexpr int kTasks = 4;
  constexpr int kIters = 200;
  flow::Scope scope = rt->root_scope();
  std::vector<flow::TaskHandle<void>> handles;
  for (int t = 0; t < kTasks; ++t) {
    auto h = rt->spawn(scope, d, hammer_body(&hs, kIters));
    ASSERT_TRUE(h.ok());
    handles.push_back(std::move(h));
  }
  for (auto& h : handles) {
    ASSERT_TRUE(h.wait_for(30s));
  }
  EXPECT_EQ(hs.counter, static_cast<uint64_t>(kTasks) * kIters);
  EXPECT_EQ(hs.overlaps.load(), 0u) << "two bound segments overlapped";
  EXPECT_EQ(hs.first_slice_in_domain.load(), static_cast<uint32_t>(kTasks))
      << "a bound spawn's FIRST segment ran outside the domain (I-1a)";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Join branches INHERIT the domain (serialized, in-slice);
// explicit spawn does NOT (the legal parallel spelling).
// ---------------------------------------------------------------------------

flow::Task<int> branch_probe(HammerState* hs, int v) {
  if (aorta::lanes::g_serial_slice_group != hs->group) {
    hs->overlaps.fetch_add(100, std::memory_order_seq_cst);  // wrong plane
  }
  if (hs->busy.exchange(true, std::memory_order_seq_cst)) {
    hs->overlaps.fetch_add(1, std::memory_order_seq_cst);
  }
  hs->counter++;
  hs->busy.store(false, std::memory_order_seq_cst);
  co_await flow::resume();
  co_return v;
}

flow::Task<int> join_parent(HammerState* hs) {
  auto [a, b] = co_await flow::join(branch_probe(hs, 1), branch_probe(hs, 2));
  co_return a + b;
}

TEST(FlowSerial, JoinBranchesInheritTheDomainAndStaySerialized) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "join");
  ASSERT_TRUE(d.valid());
  HammerState hs;
  hs.group = group_of(d);
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, d, join_parent(&hs));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(**o, 3);
  EXPECT_EQ(hs.counter, 2u);
  EXPECT_EQ(hs.overlaps.load(), 0u)
      << "join branches escaped the domain or overlapped";
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> unbound_probe(HammerState* hs, std::atomic<int>* outside) {
  if (aorta::lanes::g_serial_slice_group != hs->group) {
    outside->fetch_add(1, std::memory_order_seq_cst);
  }
  co_return;
}

flow::Task<void> escape_parent(const flow::Runtime* rt, HammerState* hs,
                               std::atomic<int>* outside, flow::Scope scope) {
  // Explicit spawn does NOT inherit — the visible parallel escape.
  auto h = rt->spawn(scope, unbound_probe(hs, outside));
  std::vector<flow::TaskHandle<void>> hv;
  hv.push_back(std::move(h));
  auto joined = co_await flow::when_all(std::move(hv));
  if (!joined.ok()) {
    // Surface the child's stop as this frame's own failure — the test
    // asserts the parent outcome, so a swallowed join would hide it.
    throw std::runtime_error("escape_parent: child join was not ok");
  }
  co_return;
}

TEST(FlowSerial, ExplicitSpawnEscapesTheDomain) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "escape");
  ASSERT_TRUE(d.valid());
  HammerState hs;
  hs.group = group_of(d);
  std::atomic<int> outside{0};
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, d, escape_parent(&*rt, &hs, &outside, scope));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(outside.load(), 1) << "the unbound child ran inside the domain";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// F-2: foreign-plane ARM refusals (each verb refuses BEFORE any foreign
// arm exists; the frame unwinds and the handle resolves stopped).
// ---------------------------------------------------------------------------

flow::Task<void> arm_foreign_sleep(flow::LaneRuntime* rtb) {
  co_await flow::sleep(*rtb, 5ms);  // refused: foreign plane
  co_return;
}

flow::Task<void> arm_foreign_resume_on_lane(flow::LaneRuntime* rtb) {
  co_await flow::resume_on_lane{rtb};
  co_return;
}

flow::Task<void> arm_exec_verb(uint64_t exec) {
  co_await flow::resume_on{exec};  // refused: ANY bound frame
  co_return;
}

flow::Task<void> arm_exec_sleep(uint64_t exec) {
  co_await flow::sleep_on{exec, 1ms};  // refused: ANY bound frame
  co_return;
}

flow::Task<void> arm_foreign_cancelled(flow::LaneRuntime* rtb,
                                       flow::Scope live) {
  co_await flow::cancelled(*rtb, std::move(live));
  co_return;
}

flow::Task<int> foreign_join_branch(int v) { co_return v; }

flow::Task<void> arm_foreign_join(flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::join(*rtb, std::move(scope), foreign_join_branch(1),
                            foreign_join_branch(2));
  co_return;
}

TEST(FlowSerial, ForeignPlaneArmsRefuseLoudly) {
  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "f2");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rta->root_scope();

  struct Row {
    const char* what;
    flow::TaskHandle<void> h;
  };
  std::vector<Row> rows;
  rows.push_back({"sleep(rtB, 5ms)",
                  rta->spawn(scope, d, arm_foreign_sleep(&rtb->lanes()))});
  rows.push_back(
      {"resume_on_lane{rtB}",
       rta->spawn(scope, d, arm_foreign_resume_on_lane(&rtb->lanes()))});
  rows.push_back(
      {"resume_on{exec}", rta->spawn(scope, d, arm_exec_verb(rta->executor()))});
  rows.push_back(
      {"sleep_on{exec}", rta->spawn(scope, d, arm_exec_sleep(rta->executor()))});
  rows.push_back({"cancelled(rtB, live)",
                  rta->spawn(scope, d,
                             arm_foreign_cancelled(&rtb->lanes(),
                                                   rta->root_scope()))});
  rows.push_back({"join(rtB, ...)",
                  rta->spawn(scope, d,
                             arm_foreign_join(&rtb->lanes(), scope))});
  for (auto& row : rows) {
    ASSERT_TRUE(row.h.ok()) << row.what;
    ASSERT_TRUE(row.h.wait_for(10s)) << row.what;
    auto o = row.h.take_outcome();
    ASSERT_TRUE(o.has_value()) << row.what;
    EXPECT_FALSE(o->ok()) << row.what
                          << ": a foreign-plane arm must refuse (F-2)";
  }
  ASSERT_TRUE(scope.close().ok());
}

// Ready fast paths never arm — allowed and harmless.
flow::Task<void> ready_fast_paths(flow::LaneRuntime* rtb,
                                  flow::Scope cancelled_scope,
                                  std::atomic<bool>* done) {
  co_await flow::sleep(*rtb, 0ms);  // zero-delay: await_ready fast path
  co_await flow::cancelled(*rtb, std::move(cancelled_scope));  // already set
  done->store(true, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerial, ReadyFastPathsCompleteWithoutRefusal) {
  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "ready");
  flow::Scope scope = rta->root_scope();
  flow::Scope pre_cancelled = rta->root_scope();
  pre_cancelled.cancel(flow::CancelReason::kExplicit);
  std::atomic<bool> done{false};
  auto h = rta->spawn(scope, d,
                      ready_fast_paths(&rtb->lanes(),
                                       std::move(pre_cancelled), &done));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_TRUE(o->ok()) << "a ready fast path must complete, not refuse";
  EXPECT_TRUE(done.load());
  ASSERT_TRUE(scope.close().ok());
}

// F-2 I-4: the explicit call form compares its executor argument at the
// stamp — foreign → refusal BEFORE any RPC exists.
flow::Task<void> call_foreign_exec(uint64_t foreign_exec,
                                   std::atomic<int>* verdict,
                                   flow::Scope scope) {
  static const uint8_t ping[] = {1, 2, 3};
  auto r = co_await flow::call{/*client=*/0x1234, foreign_exec, ping,
                               sizeof(ping), std::move(scope)}
               .nothrow();
  if (!r.reply.ok() && r.delivery == AORTA_OK) {
    verdict->store(1, std::memory_order_seq_cst);  // RPC-layer refusal
  }
  co_return;
}

TEST(FlowSerial, CallWithForeignExecutorRefusesAtStamp) {
  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::SingleThread();
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "call-f2");
  flow::Scope scope = rta->root_scope();
  std::atomic<int> verdict{0};
  auto h = rta->spawn(scope, d,
                      call_foreign_exec(rtb->executor(), &verdict, scope));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_EQ(verdict.load(), 1)
      << "the foreign-executor call must refuse at the stamp (F-2)";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// External stop closes the gate through the on_stop bridge — a
// parked bound frame's teardown delivery is a STAMPED cancel (the
// ArbiterFrame::torn per-caller stamp row, through the REAL thunks).
// ---------------------------------------------------------------------------

flow::Task<void> park_on_sleep(std::atomic<bool>* entered) {
  entered->store(true, std::memory_order_seq_cst);
  co_await flow::sleep(5s);
  co_return;
}

TEST(FlowSerial, ExternalStopStampsParkedBoundFrameCancelled) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "ext-stop");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> entered{false};
  auto h = rt->spawn(scope, d, park_on_sleep(&entered));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(wait_until([&] { return entered.load(); }));

  // The user-reachable stop origin.
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);

  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok()) << "the teardown delivery must be a CANCEL, "
                           "never a value (ArbiterFrame torn stamp)";
  // The bridge closed the gate: the domain reads dead.
  EXPECT_TRUE(wait_until([&] { return !d.valid(); }))
      << "external stop must close the admission gate";
}

// Teardown with a live parked bound frame: ~Runtime (gate first) — the
// straggler's delivery degrades to the stamped inline-cancel; never a
// UAF (ASan-clean is the row's point).
TEST(FlowSerial, RuntimeTeardownWithParkedBoundFrameStampsCancel) {
  std::optional<flow::Outcome<flow::Runtime>> rt(
      flow::Runtime::MultiThread({0, 0, 1, 0}));
  ASSERT_TRUE(rt->ok());
  auto d = (*rt)->create_serial(aorta::EventPriority::kNormal, "teardown");
  flow::Scope scope = (*rt)->root_scope();
  std::atomic<bool> entered{false};
  auto h = (*rt)->spawn(scope, d, park_on_sleep(&entered));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(wait_until([&] { return entered.load(); }));

  rt.reset();  // ~Core: gate close_and_quiesce → stop → join → free

  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok());
  EXPECT_FALSE(d.valid());
}

// ---------------------------------------------------------------------------
// Late completion (F-11): a call whose reply lands AFTER ~Runtime
// takes the STAMPED inline-cancel through the router's CLOSED gate —
// cancel observed, not a value; no UAF (the call delivery_status_
// per-caller stamp row, through the REAL call thunks).
// ---------------------------------------------------------------------------

flow::Task<void> bound_call(uint64_t client, uint64_t exec,
                            std::atomic<int>* saw, flow::Scope scope) {
  static const uint8_t ping[] = {42};
  auto r = co_await flow::call{client, exec, ping, sizeof(ping),
                               std::move(scope)}
               .nothrow();
  // Expected: the teardown-window delivery — the CANCEL layer.
  saw->store(r.delivery == AORTA_ERR_CANCELLED ? 1 : 2,
             std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerial, LateCallCompletionAfterTeardownStampsInlineCancel) {
  // Server side lives on its OWN executor so it survives the client
  // runtime's death and can reply into the closed gate.
  auto server_node_or = aorta::Node::Create("serial_late_srv");
  ASSERT_TRUE(server_node_or.ok());
  auto server_node = server_node_or.value();

  // Attach BEFORE the service: the first executor attached to a node owns it,
  // so creating the service first binds it to the SDK auto-executor and this
  // executor never receives the request.
  auto server_exec_or = aorta::Executor::Create();
  ASSERT_TRUE(server_exec_or.ok());
  auto server_exec = server_exec_or.value();
  ASSERT_TRUE(server_exec->AddNode(server_node).ok());

  std::promise<aorta::ServiceRequest> req_slot;
  auto req_future = req_slot.get_future();
  std::atomic<bool> req_taken{false};
  auto service_or = server_node->CreateService(
      "serial_late_echo",
      [&req_slot, &req_taken](const aorta::MessageView&,
                              aorta::ServiceRequest&& req) {
        if (!req_taken.exchange(true)) {
          req_slot.set_value(std::move(req));
        }
      });
  ASSERT_TRUE(service_or.ok());
  auto service = service_or.value();
  (void)service;
  std::thread server_spin([&server_exec] { (void)server_exec->Spin(); });

  auto client_node_or = aorta::Node::Create("serial_late_cli");
  ASSERT_TRUE(client_node_or.ok());
  auto client_node = client_node_or.value();
  auto rpc_client_or = client_node->CreateClient("serial_late_echo");
  ASSERT_TRUE(rpc_client_or.ok());
  auto rpc_client = rpc_client_or.value();
  auto matched = rpc_client->WaitForMatching(10000);
  ASSERT_TRUE(matched.ok());
  ASSERT_TRUE(matched.value());

  std::atomic<int> saw{0};
  {
    std::optional<flow::Outcome<flow::Runtime>> rt(
        flow::Runtime::MultiThread({0, 0, 1, 0},
                                   {client_node->handle()}));
    ASSERT_TRUE(rt->ok());
    auto d = (*rt)->create_serial(aorta::EventPriority::kNormal, "late");
    flow::Scope scope = (*rt)->root_scope();
    auto h = (*rt)->spawn(scope, d,
                          bound_call(rpc_client->handle(),
                                     (*rt)->executor(), &saw, scope));
    ASSERT_TRUE(h.ok());
    // The server HOLDS the request — the call is parked mid-flight.
    ASSERT_EQ(req_future.wait_for(10s), std::future_status::ready);
    rt.reset();  // teardown with the reply still outstanding
  }
  // Reply into the dead runtime: the completion must take the CLOSED
  // gate's stamped inline-cancel (no pool touch, no UAF).
  {
    auto req = req_future.get();
    const uint8_t pong[] = {7};
    (void)std::move(req).Reply(std::vector<uint8_t>(pong, pong + 1));
  }
  ASSERT_TRUE(wait_until([&] { return saw.load() != 0; }, 10s))
      << "the late completion never delivered";
  EXPECT_EQ(saw.load(), 1)
      << "a late completion must deliver CANCEL, never a value (F-11)";

  ASSERT_TRUE(server_exec->Stop().ok());
  server_spin.join();
}

// ---------------------------------------------------------------------------
// Attach handshake + detach refusal + bridge (facade tier).
// ---------------------------------------------------------------------------

TEST(FlowSerial, AttachToStoppedExecutorDetachesAndFails) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);  // stop BEFORE attach

  auto rt = flow::LaneRuntime::SingleThread();
  flow::FacadeAttachment facade(*rt, exec);
  EXPECT_FALSE(facade.attached())
      << "no successful facade may attach to a stopped executor";
  ASSERT_NE(facade.attach_refusal(), nullptr);
  EXPECT_STREQ(facade.attach_refusal(), "executor already stopped");
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

TEST(FlowSerial, AttachThenStopBridgesNormally) {
  // The race leg: a stop beginning AFTER a false recheck finds the
  // installed hook and performs the normal on_stop bridge.
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::SingleThread();
  flow::FacadeAttachment facade(*rt, exec);
  ASSERT_TRUE(facade.attached());
  EXPECT_EQ(facade.bridge_state_for_test(), 0u);  // UNBRIDGED
  ASSERT_EQ(aorta_executor_stop(exec), AORTA_OK);
  EXPECT_EQ(facade.bridge_state_for_test(), 2u)
      << "the winning on_stop must release-store BRIDGED";
  EXPECT_TRUE(rt->pool().closed());
  ASSERT_TRUE(facade.detach().ok());
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

TEST(FlowSerial, DetachRefusesWhileLiveDomainsOnOpenGate) {
  uint64_t exec = 0;
  ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec), AORTA_OK);
  auto rt = flow::LaneRuntime::SingleThread();
  auto facade = std::make_unique<flow::FacadeAttachment>(*rt, exec);
  ASSERT_TRUE(facade->attached());
  auto control = std::make_shared<flow::SerialControl>();
  facade->bind_serial_control(control);

  // One live domain on an OPEN gate: the detach-refusal predicate holds.
  control->live_domains.fetch_add(1, std::memory_order_acq_rel);
  auto refused = facade->detach();
  EXPECT_FALSE(refused.ok());
  EXPECT_EQ(refused.code(), AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_NE(refused.message().find("1 live serial domains"),
            std::string::npos)
      << refused.message();
  EXPECT_TRUE(facade->attached()) << "a refused detach must not detach";

  // CLOSED gate: every domain is inert — the destructor-path detach
  // passes (the destructor hole).
  control->gate.close();
  EXPECT_TRUE(facade->detach().ok());
  EXPECT_FALSE(facade->attached());

  control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  facade.reset();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

TEST(FlowSerial, CreateSerialAfterExternalStopReadsInvalid) {
  // The gate closed through the on_stop bridge: create-after-stop is
  // REFUSED loudly with the DEFAULT invalid handle (the loud-failure
  // model) — spawns with it refuse (F-1); nothing silently
  // half-works.
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d1 = rt->create_serial(aorta::EventPriority::kNormal, "pre");
  EXPECT_TRUE(d1.valid());
  ASSERT_NE(group_of(d1), nullptr);
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  ASSERT_TRUE(wait_until([&] { return !d1.valid(); }));
  auto d2 = rt->create_serial(aorta::EventPriority::kNormal, "post");
  EXPECT_FALSE(d2.valid());
  EXPECT_STREQ(d2.name(), "")
      << "create-after-stop must return the DEFAULT invalid handle "
         "(the loud-failure model), not a "
         "constructed state carrying the caller's name";
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> ran{false};
  auto h = rt->spawn(scope, d2, never_runs(&ran));
  EXPECT_FALSE(h.ok());
  EXPECT_EQ(h.status, AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_FALSE(ran.load());
}

// ---------------------------------------------------------------------------
// Occupancy canary (F-5): a custom awaiter resuming the raw handle runs the
// bound frame OUTSIDE its domain slice — detected at the next verb
// entry, WARN latched once per domain.
// ---------------------------------------------------------------------------

struct rogue_awaiter {
  std::thread* worker;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) {
    // The out-of-contract shape: capture the RAW handle and resume
    // it from an arbitrary thread.
    //
    // TSAN choreography (CI round: cross-language TSAN flagged the
    // original spelling): once the rogue thread MAY resume, this
    // awaiter object — which lives IN the coroutine frame — may be
    // clobbered by the resumed body's next awaiter, and the
    // move-assignment into *worker may still be running when the main
    // thread (synchronized only with the FRAME's completion) joins it.
    // So: copy the member BEFORE launching, and gate the resume until
    // the assignment has landed (release/acquire edge). The resumption
    // itself stays exactly as out-of-contract as before — a raw
    // foreign-thread resume with no domain involvement.
    std::thread* out = worker;
    auto gate = std::make_shared<std::atomic<bool>>(false);
    *out = std::thread([h, gate] {
      while (!gate->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      h.resume();
    });
    gate->store(true, std::memory_order_release);
  }
  void await_resume() const noexcept {}
};

flow::Task<void> rogue_body(std::thread* worker, std::atomic<bool>* resumed) {
  co_await rogue_awaiter{worker};
  resumed->store(true, std::memory_order_seq_cst);
  // The next Flow-verb entry is the canary checkpoint.
  co_await flow::resume();
  co_return;
}

TEST(FlowSerial, CanaryDetectsRawHandleResumeAtNextVerbEntry) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "canary");
  ASSERT_TRUE(d.valid());
  const auto& st = flow::detail::serial_access::state(d);
  EXPECT_FALSE(st->canary_warned.load(std::memory_order_acquire));

  flow::Scope scope = rt->root_scope();
  std::thread worker;
  std::atomic<bool> resumed{false};
  auto h = rt->spawn(scope, d, rogue_body(&worker, &resumed));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  worker.join();
  EXPECT_TRUE(resumed.load());
  EXPECT_TRUE(st->canary_warned.load(std::memory_order_acquire))
      << "the occupancy canary must detect the out-of-slice segment at "
         "the next verb entry (F-5)";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Gate linearization: a pin storm racing close_and_quiesce — no
// pin admitted after the CLOSED OR is observed (TSan-meaningful).
// ---------------------------------------------------------------------------

TEST(FlowSerial, GateLinearizationUnderPinStorm) {
  // TSan-meaningful storm: pins race close_and_quiesce; after the OR no
  // NEW pin is ever admitted (asserted post-join, race-free: every pin
  // admitted after close() would still be live or recorded below).
  flow::detail::AdmissionGate gate;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> admitted{0};
  std::atomic<uint64_t> admitted_after_seen_closed{0};
  std::atomic<uint64_t> attempts_after_seen_closed{0};
  std::vector<std::thread> pinners;
  for (int t = 0; t < 4; ++t) {
    pinners.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        // Linearization-sound verdict: observe closed()
        // BEFORE attempting the pin. The CLOSED bit is monotonic, so an
        // attempt that already SAW it must fail — a success here is a
        // real post-OR admission. (A pin that succeeded before the OR
        // and only later reads CLOSED is legal and deliberately not
        // counted.)
        const bool saw_closed = gate.closed();
        if (saw_closed) {
          attempts_after_seen_closed.fetch_add(1, std::memory_order_relaxed);
        }
        if (gate.pin()) {
          if (saw_closed) {
            admitted_after_seen_closed.fetch_add(1,
                                                 std::memory_order_relaxed);
          }
          admitted.fetch_add(1, std::memory_order_relaxed);
          gate.unpin();
        }
      }
    });
  }
  std::this_thread::sleep_for(20ms);
  gate.close_and_quiesce();
  EXPECT_TRUE(gate.closed());
  // Prove the verdict had teeth — wait until post-close
  // attempts actually HAPPENED before stopping the storm.
  ASSERT_TRUE(wait_until([&] {
    return attempts_after_seen_closed.load(std::memory_order_relaxed) >= 8;
  }));
  stop.store(true, std::memory_order_release);
  for (auto& t : pinners) {
    t.join();
  }
  EXPECT_GT(admitted.load(), 0u);
  EXPECT_GE(attempts_after_seen_closed.load(), 8u);
  EXPECT_EQ(admitted_after_seen_closed.load(), 0u)
      << "a pin observed CLOSED and was still admitted";
  EXPECT_EQ(gate.live_pins(), 0u);
}

TEST(FlowSerial, CloseAndQuiesceBlocksWhileAProducerPinIsHeld) {
  // Paused-pinned-producer row: an ADMITTED pin suspended
  // mid-window must hold close_and_quiesce until the unpin.
  flow::detail::AdmissionGate gate;
  ASSERT_TRUE(gate.pin());
  std::atomic<bool> quiesced{false};
  std::thread closer([&] {
    gate.close_and_quiesce();
    quiesced.store(true, std::memory_order_seq_cst);
  });
  // The OR lands promptly; the QUIESCE must not.
  ASSERT_TRUE(wait_until([&] { return gate.closed(); }));
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(quiesced.load())
      << "close_and_quiesce returned while an admitted pin was held";
  gate.unpin();
  ASSERT_TRUE(wait_until([&] { return quiesced.load(); }));
  closer.join();
  EXPECT_EQ(gate.live_pins(), 0u);
}


// ---------------------------------------------------------------------------
// Regression: no-op domains keep the FULL binding
// contract — ST / Disabled(rollback) bound frames refuse foreign-plane
// arms exactly like MT ones (the binding probe, not the routing one).
// ---------------------------------------------------------------------------

flow::Task<void> st_foreign_probe(flow::LaneRuntime* rtb, uint64_t own_exec,
                                  std::atomic<int>* refusals) {
#if defined(__cpp_exceptions)
  try {
    co_await flow::sleep(*rtb, 5ms);
  } catch (...) {
    refusals->fetch_add(1, std::memory_order_seq_cst);
  }
  try {
    co_await flow::resume_on{own_exec};
  } catch (...) {
    refusals->fetch_add(1, std::memory_order_seq_cst);
  }
#endif
  co_return;
}

TEST(FlowSerial, NoOpDomainsKeepForeignArmRefusals) {
  // ST leg.
  auto st_rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(st_rt.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = st_rt->create_serial(aorta::EventPriority::kNormal, "st-f2");
  ASSERT_TRUE(d.valid());
  ASSERT_EQ(group_of(d), nullptr);  // no strand — binding only
  flow::Scope scope = st_rt->root_scope();
  std::atomic<int> refusals{0};
  auto h = st_rt->spawn(
      scope, d,
      st_foreign_probe(&rtb->lanes(), st_rt->executor(), &refusals));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_EQ(refusals.load(), 2)
      << "an ST no-op domain must refuse foreign-plane and "
         "executor-thread arms (binding survives without a strand)";
  ASSERT_TRUE(scope.close().ok());

  // Rollback (Disabled) leg.
  ::setenv("AORTA_CPP_LANES", "0", 1);
  auto rb = flow::Runtime::FromEnv();
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rb.ok());
  auto d2 = rb->create_serial(aorta::EventPriority::kNormal, "rb-f2");
  ASSERT_TRUE(d2.valid());
  flow::Scope scope2 = rb->root_scope();
  std::atomic<int> refusals2{0};
  auto h2 = rb->spawn(
      scope2, d2,
      st_foreign_probe(&rtb->lanes(), rb->executor(), &refusals2));
  ASSERT_TRUE(h2.ok());
  ASSERT_TRUE(h2.wait_for(10s));
  EXPECT_EQ(refusals2.load(), 2)
      << "a rollback no-op domain keeps the same refusals";
  ASSERT_TRUE(scope2.close().ok());
}

// ---------------------------------------------------------------------------
// Regression (inline CLOSED leg): a bound spawn delivered
// through the CLOSED gate must not run ONE user statement — the SDK
// wrapper's link refuses the child; accounting still balances.
// ---------------------------------------------------------------------------

TEST(FlowSerial, ClosedGateBoundStartRunsNoUserStatement) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "p02");
  ASSERT_TRUE(d.valid());
  // Force the gate CLOSED while keeping the pool alive: the race window
  // (d.valid() passing before the close, the router pinning after) is
  // driven deterministically by closing the gate directly, then calling
  // the detail spawn (the public spawn re-checks valid() first).
  const auto& st = flow::detail::serial_access::state(d);
  st->gate->close();
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> user_ran{false};
  auto h = flow::detail::spawn_with_handle(
      flow::co_invoke(
          [](flow::Scope, std::atomic<bool>* ran) -> flow::Task<void> {
            ran->store(true, std::memory_order_seq_cst);
            co_return;
          },
          scope.child(), &user_ran),
      scope, [&](flow::Task<void> wrapper) {
        return flow::detail::spawn_detached(rt->lanes(), scope,
                                            std::move(wrapper), &d);
      });
  ASSERT_TRUE(h.ok());  // admission succeeded; the DELIVERY was the cancel
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok())
      << "a CLOSED-gate bound start must deliver CANCEL, never a value";
  EXPECT_FALSE(user_ran.load())
      << "a user statement executed on the CLOSED-gate start";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Regression (ASan row): the LAST domain owner can be a
// bound frame's promise — a fast in-domain resume destroying the frame
// must not race the router's unpin into freed state.
// ---------------------------------------------------------------------------

flow::Task<void> hop_many(int hops) {
  for (int i = 0; i < hops; ++i) {
    co_await flow::resume();  // each hop: pin → member post → unpin
  }
  co_return;
}

TEST(FlowSerial, DroppedLastHandleWhileBoundFramesHopIsAsanClean) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  flow::Scope scope = rt->root_scope();
  std::vector<flow::TaskHandle<void>> handles;
  {
    auto d = rt->create_serial(aorta::EventPriority::kNormal, "p03");
    ASSERT_TRUE(d.valid());
    for (int t = 0; t < 4; ++t) {
      auto h = rt->spawn(scope, d, hop_many(300));
      ASSERT_TRUE(h.ok());
      handles.push_back(std::move(h));
    }
    // Drop the ONLY user handle: from here the frames' promises are the
    // state's last owners; every completion may free it mid-post.
  }
  for (auto& h : handles) {
    ASSERT_TRUE(h.wait_for(30s));
    auto o = h.take_outcome();
    ASSERT_TRUE(o.has_value());
    EXPECT_TRUE(o->ok());
  }
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Regression: a Disabled/rollback runtime has no on_stop
// bridge — the domain's validity observes the stop flag DIRECTLY, so an
// external stop still invalidates it and spawns refuse.
// ---------------------------------------------------------------------------

TEST(FlowSerial, RollbackExternalStopInvalidatesTheDomain) {
  ::setenv("AORTA_CPP_LANES", "0", 1);
  auto rt = flow::Runtime::FromEnv();
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rt.ok());
  EXPECT_FALSE(rt->lanes_attached());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "rb-stop");
  ASSERT_TRUE(d.valid());
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  EXPECT_FALSE(d.valid())
      << "a rollback runtime's external stop must invalidate the "
         "domain (the stop flag is observed directly)";
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> ran{false};
  auto h = rt->spawn(scope, d, never_runs(&ran));
  EXPECT_FALSE(h.ok());
  EXPECT_EQ(h.status, AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_FALSE(ran.load());
}

// ---------------------------------------------------------------------------
// Offload run AND drop re-entry (the I-6 bound channels).
// ---------------------------------------------------------------------------

flow::Task<void> offload_run_probe(const void* group,
                                   std::atomic<int>* verdict) {
  int v = co_await flow::offload(aorta::EventPriority::kLow,
                                 [] { return 7; });
  // The continuation re-entered the domain: the slice mark is ours.
  verdict->store(
      (v == 7 && aorta::lanes::g_serial_slice_group == group) ? 1 : 2,
      std::memory_order_seq_cst);
  co_return;
}

TEST(FlowSerial, OffloadRunChannelReentersTheDomain) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 1});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "offload");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  std::atomic<int> verdict{0};
  auto h = rt->spawn(scope, d, offload_run_probe(group_of(d), &verdict));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  EXPECT_EQ(verdict.load(), 1)
      << "the offload completion must re-enter the domain (I-6 run)";
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> offload_blocker(std::atomic<bool>* entered,
                                 std::atomic<bool>* release) {
  co_await flow::offload(aorta::EventPriority::kLow, [entered, release] {
    entered->store(true, std::memory_order_seq_cst);
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!release->load(std::memory_order_seq_cst) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    return 0;
  });
  co_return;
}

flow::Task<void> offload_queued_probe(std::atomic<bool>* closure_ran) {
  co_await flow::offload(aorta::EventPriority::kLow, [closure_ran] {
    closure_ran->store(true, std::memory_order_seq_cst);
    return 0;
  });
  co_return;
}

TEST(FlowSerial, OffloadDropChannelDeliversCancelThroughTheDomain) {
  // The DROP thunk itself (a queued, never-run offload
  // Work dropped at pool close) — not the run thunk's CLOSED-gate
  // completion. One Low worker: the blocker occupies it; the probe's
  // Work sits QUEUED behind it when the stop closes the pool, so its
  // delivery is Work.drop → offload drop channel → domain CANCEL.
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 1});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "offdrop");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> blocker_entered{false};
  std::atomic<bool> blocker_release{false};
  std::atomic<bool> probe_closure_ran{false};
  auto hb = rt->spawn(scope, d,
                      offload_blocker(&blocker_entered, &blocker_release));
  ASSERT_TRUE(hb.ok());
  ASSERT_TRUE(wait_until([&] { return blocker_entered.load(); }));
  // The domain is serialized — spawn the probe UNBOUND? No: a bound
  // probe would queue BEHIND the blocker in the domain, never reaching
  // its offload. The probe rides the SAME domain but its offload post
  // happens after its first slice; the blocker is parked INSIDE an
  // offload (occupancy released), so the probe's slice runs and its
  // offload Work queues on the busy Low lane.
  auto hp = rt->spawn(scope, d, offload_queued_probe(&probe_closure_ran));
  ASSERT_TRUE(hp.ok());
  // PROOF the probe reached await_suspend and queued its Work behind
  // the busy worker (a fixed sleep could stop before the
  // probe's slice ran, cancel it at bound-start, and fake-green this
  // row without ever touching drop_thunk). Low = lane 3; the blocker is
  // RUNNING (not queued), so queued_now==1 is uniquely the probe.
  ASSERT_TRUE(wait_until(
      [&] { return rt->lanes().pool().stats(3).queued_now >= 1; }));
  const uint64_t dropped_before = rt->lanes().pool().stats(3).dropped_closed;
  // Stop: pool close → the QUEUED probe Work delivers through
  // Work.drop (the offload drop channel); the blocker finishes after.
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  blocker_release.store(true, std::memory_order_seq_cst);
  ASSERT_TRUE(hb.wait_for(10s));
  ASSERT_TRUE(hp.wait_for(10s));
  auto ob = hb.take_outcome();
  auto op = hp.take_outcome();
  ASSERT_TRUE(ob.has_value());
  ASSERT_TRUE(op.has_value());
  EXPECT_FALSE(op->ok())
      << "a queued offload dropped at pool close must deliver CANCEL "
         "(I-6 drop thunk)";
  EXPECT_FALSE(probe_closure_ran.load())
      << "the dropped offload's closure must never run";
  // The stat trails the outcome: worker_loop delivers w.drop (which
  // resolves hp above) BEFORE re-locking to bump dropped_closed — on a
  // loaded runner this thread can observe the resolution first (seen
  // once on CI ubsan, 2026-07-21). Wait for the counter to converge.
  ASSERT_TRUE(wait_until([&] {
    return rt->lanes().pool().stats(3).dropped_closed >= dropped_before + 1;
  }));
  EXPECT_GE(rt->lanes().pool().stats(3).dropped_closed, dropped_before + 1)
      << "the probe's queued Work must have been DROPPED at close (the "
         "real drop_thunk path), not consumed any other way";
}

// ---------------------------------------------------------------------------
// Detach-refusal configuration matrix — MT and mixed zero-worker
// (the ST leg lives in DetachRefusesWhileLiveDomainsOnOpenGate above).
// ---------------------------------------------------------------------------

void run_detach_refusal_config(std::vector<uint32_t> workers) {
  uint64_t exec = 0;
  const bool mt = !workers.empty();
  ASSERT_EQ(aorta_executor_new(
                mt ? AORTA_EXECUTOR_MULTI : AORTA_EXECUTOR_SINGLE, &exec),
            AORTA_OK);
  auto rt = mt ? flow::LaneRuntime::MultiThread(std::move(workers))
               : flow::LaneRuntime::SingleThread();
  auto facade = std::make_unique<flow::FacadeAttachment>(*rt, exec);
  ASSERT_TRUE(facade->attached());
  auto control = std::make_shared<flow::SerialControl>();
  facade->bind_serial_control(control);
  control->live_domains.fetch_add(1, std::memory_order_acq_rel);
  auto refused = facade->detach();
  EXPECT_FALSE(refused.ok());
  EXPECT_NE(refused.message().find("1 live serial domains"),
            std::string::npos);
  EXPECT_TRUE(facade->attached());
  control->gate.close();
  EXPECT_TRUE(facade->detach().ok());
  control->live_domains.fetch_sub(1, std::memory_order_acq_rel);
  facade.reset();
  ASSERT_EQ(aorta_executor_free(exec), AORTA_OK);
}

TEST(FlowSerial, DetachRefusalOnMtConfig) {
  run_detach_refusal_config({0, 0, 2, 0});
}

TEST(FlowSerial, DetachRefusalOnMixedZeroWorkerConfig) {
  run_detach_refusal_config({1, 0, 0, 0});  // domain lanes 2/3 unworked
}

// ---------------------------------------------------------------------------
// Regression: a NO-OP domain's teardown start executes
// ZERO user statements too — the pre-start-cancel contract follows the
// BINDING, not the strand. ST (refused lane post) + rollback (refused
// executor post) legs, driven through the real spawn plumbing.
// ---------------------------------------------------------------------------

void run_noop_teardown_start_leg(bool rollback) {
  if (rollback) {
    ::setenv("AORTA_CPP_LANES", "0", 1);
  }
  auto rt = rollback ? flow::Runtime::FromEnv() : flow::Runtime::SingleThread();
  if (rollback) {
    ::unsetenv("AORTA_CPP_LANES");
  }
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "noop-teardown");
  ASSERT_TRUE(d.valid());
  ASSERT_EQ(group_of(d), nullptr);
  // External stop: the pool (ST) / executor (rollback) refuses the
  // first post; the refused-post degradation must stamp the pre-start
  // cancel — never run the user body inline on this thread.
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> user_ran{false};
  // Direct detail spawn: the public spawn refuses at d.valid() (F-1);
  // the row drives the RACE arm (valid() passed before the stop) —
  // deterministically, by skipping the recheck.
  auto h = flow::detail::spawn_with_handle(
      flow::co_invoke(
          [](flow::Scope, std::atomic<bool>* ran) -> flow::Task<void> {
            ran->store(true, std::memory_order_seq_cst);
            co_return;
          },
          scope.child(), &user_ran),
      scope, [&](flow::Task<void> wrapper) {
        return flow::detail::spawn_detached(rt->lanes(), scope,
                                            std::move(wrapper), &d);
      });
  ASSERT_TRUE(h.wait_for(10s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  EXPECT_FALSE(o->ok()) << "the no-op teardown start must deliver CANCEL";
  EXPECT_FALSE(user_ran.load())
      << "a user statement executed on a no-op domain's "
         "teardown start";
  (void)scope.close();
}

TEST(FlowSerial, StNoOpTeardownStartRunsNoUserStatement) {
  run_noop_teardown_start_leg(/*rollback=*/false);
}

TEST(FlowSerial, RollbackNoOpTeardownStartRunsNoUserStatement) {
  run_noop_teardown_start_leg(/*rollback=*/true);
}
}  // namespace
