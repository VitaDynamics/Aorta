// Flow TCK emitter — C++ plane, Phase 0 (test/flow_tck/SCHEMA.md).
//
// Writes ONLY newline-delimited JSON event records to stdout, one per
// observed event, in observation order (logs go to stderr) — the TCK
// driver (test/flow_tck/driver.py) parses this stream, normalizes it per
// SCHEMA.md, and diffs it against test/flow_tck/expected/<scenario>*.json.
//
// Scenario name is argv[1]. Setup uses flow::Runtime (the golden
// path — see cpp/test/flow_runtime_test.cc): the simpler of the two
// harnesses the existing Flow test suite offers. The WhenHarness in
// cpp/test/flow_when_test.cc manages its own drain thread and pool
// lifetime by hand; flow::Runtime owns the executor + LaneRuntime +
// FacadeAttachment + spin thread and encodes the stop-then-destroy
// teardown order structurally — the right choice for a short-lived,
// single-scenario binary that just wants to spawn one task and observe
// its outcome.
//
// Wave-1 note (test/flow_tck/scenarios.json): every emission below routes
// through emit_lifecycle/emit_value/emit_stopped(_cancelled|_fault)/
// emit_counter, which serialize "assign seq + write the line" under ONE
// mutex (SCHEMA.md "Emission rules") — several wave-1 scenarios emit from
// BOTH the main thread and a spawned task's own runtime thread
// concurrently (parent_child_cancel_order, budget_expiry_checkpoint), so
// a plain global counter would race.
#include "aorta/client.h"
#include "aorta/flow_combinators.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/lanes.hpp"
#include "aorta/node.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

uint64_t g_seq = 0;
// Emission rules (SCHEMA.md v1): one mutex around "assign seq + write
// line" — task bodies emit from runtime threads in several wave-1
// scenarios (parent_child_cancel_order, budget_expiry_checkpoint,
// sibling_fail_fast, isolated_child_failure, ...), racing the main
// thread's own emits.
std::mutex g_emit_mutex;

// Minimal JSON string escaping for the free-text fields we do emit
// (fault messages, stringified values) — the scenarios below only ever
// produce plain ASCII text, but escaping defensively costs nothing and
// keeps this emitter honest about being a JSON producer.
std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      default:
        out += c;
    }
  }
  return out;
}

void emit_lifecycle(const std::string& scenario, const char* op,
                    const char* phase) {
  std::lock_guard<std::mutex> lock(g_emit_mutex);
  std::printf(
      "{\"scenario\":\"%s\",\"seq\":%llu,\"op\":\"%s\",\"event\":\"lifecycle\""
      ",\"phase\":\"%s\"}\n",
      scenario.c_str(), static_cast<unsigned long long>(g_seq++), op, phase);
  std::fflush(stdout);
}

// Baseline divergence, RESOLVED (test/flow_tck/SCHEMA.md, "Phase 0
// baseline-divergence scenario"): the C ABI's completion record
// and the Outcome<T>/Stopped vocabulary (defined in
// cpp/include/aorta/flow.hpp) together carry the exact wire-level
// CancelReason end to end on the C++ plane too — `TaskHandle::take_outcome`
// returns a `Stopped` whose `raw_reason()` is the precise byte, matching
// the Rust plane (rust/examples/flow_tck_emitter.rs) bit for bit. This
// emits the REAL reason instead of the old fixed "unknown"/0 pair.
//
// `raw_reason` naming mirrors the Rust emitter's `cancel_reason_name`
// (`aorta::flow::CancelReason` is an open enum, carried loss-free — so
// an unnamed future byte still falls back to "unknown").
const char* cancel_reason_name(uint8_t raw_reason) {
  switch (raw_reason) {
    case AORTA_CANCEL_REASON_EXPLICIT:
      return "explicit";
    case AORTA_CANCEL_REASON_DEADLINE:
      return "deadline";
    case AORTA_CANCEL_REASON_SHUTDOWN:
      return "shutdown";
    case AORTA_CANCEL_REASON_CHILD_FAILED:
      return "child_failed";
    default:
      return "unknown";
  }
}

void emit_stopped_cancelled(const std::string& scenario, const char* op,
                            const char* reason, uint8_t raw_reason) {
  std::lock_guard<std::mutex> lock(g_emit_mutex);
  std::printf(
      "{\"scenario\":\"%s\",\"seq\":%llu,\"op\":\"%s\",\"event\":\"stopped\""
      ",\"kind\":\"cancelled\",\"reason\":\"%s\",\"raw_reason\":%u}\n",
      scenario.c_str(), static_cast<unsigned long long>(g_seq++), op, reason,
      static_cast<unsigned>(raw_reason));
  std::fflush(stdout);
}

// --- wave-1 additions below (SCHEMA.md v1) --------------------------------

void emit_stopped_fault(const std::string& scenario, const char* op, int code,
                        const std::string& message) {
  std::lock_guard<std::mutex> lock(g_emit_mutex);
  std::printf(
      "{\"scenario\":\"%s\",\"seq\":%llu,\"op\":\"%s\",\"event\":\"stopped\""
      ",\"kind\":\"fault\",\"code\":%d,\"message\":\"%s\"}\n",
      scenario.c_str(), static_cast<unsigned long long>(g_seq++), op, code,
      json_escape(message).c_str());
  std::fflush(stdout);
}

void emit_value(const std::string& scenario, const char* op,
                const std::string& value) {
  std::lock_guard<std::mutex> lock(g_emit_mutex);
  std::printf(
      "{\"scenario\":\"%s\",\"seq\":%llu,\"op\":\"%s\",\"event\":\"value\""
      ",\"value\":\"%s\"}\n",
      scenario.c_str(), static_cast<unsigned long long>(g_seq++), op,
      json_escape(value).c_str());
  std::fflush(stdout);
}

void emit_counter(const std::string& scenario, const char* op,
                  const char* name, long long value) {
  std::lock_guard<std::mutex> lock(g_emit_mutex);
  std::printf(
      "{\"scenario\":\"%s\",\"seq\":%llu,\"op\":\"%s\",\"event\":\"counter\""
      ",\"name\":\"%s\",\"value\":%lld}\n",
      scenario.c_str(), static_cast<unsigned long long>(g_seq++), op, name,
      value);
  std::fflush(stdout);
}

// THE one Stopped -> record mapping (SCHEMA.md "Emission rules": every
// semantic field derives from the OBSERVED object, never a constant) —
// every wave-1 scenario below that classifies a stop routes through this,
// so the emitted kind/reason/raw_reason/code/message are always read off
// the actual `flow::Stopped` the runtime delivered.
void emit_stopped(const std::string& scenario, const char* op,
                  const flow::Stopped& s) {
  if (s.is_fault()) {
    emit_stopped_fault(scenario, op, static_cast<int>(s.fault().code()),
                       s.fault().message());
  } else {
    const uint8_t raw = s.raw_reason();
    emit_stopped_cancelled(scenario, op, cancel_reason_name(raw), raw);
  }
}

[[noreturn]] void die(const std::string& what) {
  std::fprintf(stderr, "flow_tck_emitter: %s\n", what.c_str());
  std::fflush(stderr);
  std::exit(2);
}

// The ONE non-coroutine consumer seam every wave-1 scenario uses:
// BLOCKING bounded wait, then the one-shot synchronous read (the same
// wait_for()+take_outcome() idiom run_cancel_reason_observation uses
// below) — never let a scenario hang the process. An unexpected timeout
// or a stolen outcome are both harness bugs (exit 2, per the CLI
// contract) EXCEPT for budget_expiry_parked, where a wait_for() timeout
// IS the finding this row exists to surface (see run_budget_expiry_parked).
template <typename T>
flow::Outcome<T> wait_and_take(flow::TaskHandle<T>& handle,
                               const std::string& where) {
  if (!handle.wait_for(5s)) {
    die(where + ": did not stop within the 5s budget");
  }
  auto outcome = handle.take_outcome();
  if (!outcome.has_value()) {
    die(where + ": outcome already consumed");
  }
  return std::move(*outcome);
}

// --- `cancel_reason_observation`: a parent scope cancels a parked child
// with Shutdown; the child's TaskHandle observes the stop and this
// emits the stopped record.
//
// The park itself mirrors the `CancelledResolvesWhenTheScopeCancels`
// idiom in cpp/test/flow_when_test.cc: `co_await flow::cancelled()` —
// ambient, the frame's own (spawned) scope; it is the ONLY Flow verb
// that watches scope cancellation directly (`aorta_scope_arm_arbiter` —
// grep cpp/include/aorta/*.hpp for `is_cancelled()` and it is the one
// hit outside flow.hpp's own Scope class). Every other verb (sleep,
// resume_on, recv, ...) only reacts to EXECUTOR/pool teardown, not to a
// bare `scope.cancel()`.
//
// By design (see the doc comment on cancelled_awaiter in
// flow_combinators.hpp) `co_await flow::cancelled()` RESOLVES NORMALLY
// on a genuine cancel — "run your cleanup, then let the next verb
// unwind" — so a bare `co_await flow::cancelled();` followed by nothing
// else completes the coroutine OK, and the wrapping TaskHandle's
// Outcome reads ok()==true, NOT cancelled (verified empirically while
// building this emitter). There is no OTHER public verb this scenario
// can chain to get the "next verb" unwind against a plain scope-cancel
// (no timer/deadline/executor-stop involved), so the child explicitly
// converts the observation into the SDK's own unwind token — the same
// token every other awaiter above throws internally on a teardown
// delivery (flow.hpp: "any exception — including the internal
// cancelled_unwind — is stored [in the promise] uniformly", so a
// user-thrown one classifies identically at the TaskHandle). Outcome<T> then
// classifies that unwind through the child's OWN scope (flow.hpp's
// `handle_state::build_stopped`) — the same `scope` this function
// cancels below — so the delivered `Stopped::raw_reason()` is the exact
// byte passed to `scope.cancel()`, not a fold to "unknown".
int run_cancel_reason_observation(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    std::fprintf(stderr,
                 "flow_tck_emitter: Runtime::SingleThread failed (code %d, "
                 "%s)\n",
                 rt.stopped().fault().code(),
                 rt.stopped().fault().message().c_str());
    return 2;
  }
  flow::Scope scope = rt->root_scope();

  auto handle = rt->spawn(scope, []() -> flow::Task<void> {
    co_await flow::cancelled();  // ambient: the frame's own spawned scope
    // Returning normally here MEANS the scope is cancelled (per
    // flow::cancelled()'s own contract) — propagate that into the
    // TaskHandle's Outcome (see the comment above run_cancel_reason_observation).
    flow::stop_now();  // D7: the public spelling (was the internal-token workaround)
  }());
  if (!handle.ok()) {
    std::fprintf(stderr, "flow_tck_emitter: spawn refused (status %d)\n",
                 static_cast<int>(handle.status));
    return 2;
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  scope.cancel(flow::CancelReason::kShutdown);
  emit_lifecycle(scenario, "scope.cancel", "parent_cancelled");

  if (!handle.wait_for(5s)) {
    std::fprintf(stderr,
                 "flow_tck_emitter: child did not stop within the 5s budget\n");
    return 2;
  }
  auto outcome = handle.take_outcome();
  if (!outcome.has_value()) {
    std::fprintf(stderr, "flow_tck_emitter: outcome already consumed\n");
    return 2;
  }
  if (outcome->ok() || outcome->stopped().kind() != flow::StopKind::kCancelled) {
    std::fprintf(
        stderr,
        "flow_tck_emitter: unexpected outcome (ok=%d, stop_kind=%d)\n",
        outcome->ok() ? 1 : 0,
        outcome->ok() ? -1 : static_cast<int>(outcome->stopped().kind()));
    return 2;
  }
  const uint8_t raw_reason = outcome->stopped().raw_reason();
  emit_stopped_cancelled(scenario, "task.await",
                         cancel_reason_name(raw_reason), raw_reason);

  (void)scope.close();
  return 0;
}

// ---------------------------------------------------------------------------
// Wave-1 shared task bodies (SCHEMA.md v1). Named functions, not
// capturing lambdas (flow.hpp header doc: a capturing lambda coroutine's
// closure dies with the full expression while the frame lives on) — state
// crosses a co_await only via bound-by-value PARAMETERS.
// ---------------------------------------------------------------------------

flow::Task<int> seven_body() { co_return 7; }

// Move-only payload (flow.hpp: Outcome<T> never default-
// constructs T on the stop path, so a Clone-less type works unmodified).
struct MoveOnlyFrame {
  std::string label;
  explicit MoveOnlyFrame(std::string l) : label(std::move(l)) {}
  MoveOnlyFrame(MoveOnlyFrame&&) = default;
  MoveOnlyFrame& operator=(MoveOnlyFrame&&) = default;
  MoveOnlyFrame(const MoveOnlyFrame&) = delete;
  MoveOnlyFrame& operator=(const MoveOnlyFrame&) = delete;
};

flow::Task<MoveOnlyFrame> move_only_body() {
  co_return MoveOnlyFrame(std::string("motor-frame"));
}

// The cancel_reason_observation idiom (see its doc comment above), reused
// verbatim by every scenario that just needs "park until this frame's own
// scope cancels, then convert the observation into a stop".
flow::Task<void> park_until_cancelled_void() {
  co_await flow::cancelled();
  flow::stop_now();
}

// Same idiom, T=int spelling — for when_any_first_wins' loser branches
// (when_any requires every handle in the vector to share one T). The
// co_return after stop_now() is dead code (stop_now() is [[noreturn]]) but
// syntactically required: promise_storage<int> has no return_void().
flow::Task<int> park_until_cancelled_int() {
  co_await flow::cancelled();
  flow::stop_now();
  co_return 0;
}

flow::Task<void> throw_fault_body(std::string message) {
  co_await flow::resume();  // move off the spawn slice first (flow_handle_test.cc precedent)
  throw std::runtime_error(message);
}

// budget_expiry_checkpoint: the task body OWNS the checkpoint loop and
// self-reports the moment it observes expiry — main only consumes the
// resulting TaskHandle afterward (SCHEMA.md "the emitter synchronizes"
// rule: the atomic flag below is main's cue that the body's own
// checkpoint_observed_deadline record has already been written).
flow::Task<void> budget_checkpoint_body(std::string scenario, flow::Scope scope,
                                        std::atomic<bool>* stopped_flag) {
  for (;;) {
    aorta::Status st = scope.checkpoint();
    if (!st.ok()) {
      emit_lifecycle(scenario, "budget.checkpoint",
                     "checkpoint_observed_deadline");
      stopped_flag->store(true, std::memory_order_seq_cst);
      flow::stop_now();
    }
    co_await flow::sleep(10ms);
  }
}

flow::Task<void> parent_child_body(std::string scenario,
                                   std::atomic<bool>* observed_flag) {
  co_await flow::cancelled();
  emit_lifecycle(scenario, "task.body", "child_observed_cancel");
  observed_flag->store(true, std::memory_order_seq_cst);
  flow::stop_now();
}

flow::Task<int> sleep_then_value_body(std::chrono::milliseconds d, int v) {
  if (d.count() > 0) {
    co_await flow::sleep(d);
  }
  co_return v;
}

flow::Task<void> trivial_void_body() { co_return; }

flow::Task<int> return_now_body(int v) { co_return v; }

std::string stringify_int_vector(const std::vector<int>& values) {
  std::string out = "[";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != 0) {
      out += ",";
    }
    out += std::to_string(values[i]);
  }
  out += "]";
  return out;
}

// Combinator wrapper tasks (per-scenario notes: "run the combinator INSIDE
// a wrapper task" — co_await'ing when_all/when_any needs a flow coroutine
// frame; main() is not one). Each wrapper co_returns the stringified
// success value; a non-ok combinator result is propagated by RETHROWING
// the OBSERVED Stopped's own text (never a constant — see the emit_stopped
// doc comment) so the outer TaskHandle's own Outcome carries it.

flow::Task<std::string> when_all_empty_wrapper() {
  std::vector<flow::TaskHandle<int>> handles;  // empty: vacuously ok([])
  auto result = co_await flow::when_all(std::move(handles));
  co_return stringify_int_vector(result.value());
}

flow::Task<std::string> when_all_multi_wrapper(flow::Runtime& rt,
                                               flow::Scope scope) {
  std::vector<flow::TaskHandle<int>> handles;
  // Staggered completion order, INPUT order preserved in the result
  // (flow_outcome_test.cc: WhenAllOverMoveOnlyValuesRoundTripsInInputOrder).
  handles.push_back(rt.spawn(scope, sleep_then_value_body(30ms, 1)));
  handles.push_back(rt.spawn(scope, sleep_then_value_body(15ms, 2)));
  handles.push_back(rt.spawn(scope, sleep_then_value_body(5ms, 3)));
  auto result = co_await flow::when_all(std::move(handles));
  co_return stringify_int_vector(result.value());
}

flow::Task<std::string> when_all_fault_wrapper(flow::Runtime& rt,
                                               flow::Scope scope) {
  std::vector<flow::TaskHandle<void>> handles;
  // A parks on this SAME scope's cancel token; B faults. Default
  // supervision is FailFast (flow.hpp Scope::Options), so B's fault
  // cascades a ChildFailed cancel onto `scope` — which is what wakes A
  // (documented C++ delta vs Rust: a C++ frame is never destroyed while
  // suspended, so when_all's branches all resolve via that cascade
  // instead of a hard abort; flow_combinators.hpp's when_all doc comment).
  handles.push_back(rt.spawn(scope, park_until_cancelled_void()));
  handles.push_back(rt.spawn(scope, throw_fault_body("tck-when-fault")));
  auto result = co_await flow::when_all(std::move(handles));
  if (!result.ok()) {
    // Fault outranks cancel in when_all's own error pick (flow_combinators.hpp),
    // so a non-ok result here is always B's fault — propagate its OBSERVED
    // text (never a constant) as this frame's own failure.
    throw std::runtime_error(result.stopped().fault().message());
  }
  co_return std::string("ok");  // unreachable on the tested path
}

flow::Task<std::string> when_any_first_wins_wrapper(flow::Runtime& rt,
                                                     flow::Scope scope) {
  std::vector<flow::TaskHandle<int>> handles;
  handles.push_back(rt.spawn(scope, return_now_body(9)));
  handles.push_back(rt.spawn(scope, park_until_cancelled_int()));
  handles.push_back(rt.spawn(scope, park_until_cancelled_int()));
  auto result = co_await flow::when_any(std::move(handles));
  if (!result.ok()) {
    throw std::runtime_error(
        "when_any_first_wins: unexpected stop, want the index-0 value");
  }
  auto [idx, value] = std::move(result).value();
  // "index:value" — the winner IDENTITY is part of the contract (a
  // value-only record would stay green if a wrong same-valued branch won).
  co_return std::to_string(idx) + ":" + std::to_string(value);
}

flow::Task<std::string> when_any_empty_wrapper() {
  std::vector<flow::TaskHandle<int>> handles;  // empty: when_any REFUSES
  auto result = co_await flow::when_any(std::move(handles));
  co_return std::to_string(result.value().second);  // unreachable: unwinds
}

// ---------------------------------------------------------------------------
// Wave-1 scenario drivers.
// ---------------------------------------------------------------------------

// value_delivery / lanes_rollback: identical shape by design — lanes_rollback
// (test/flow_tck/scenarios.json "runs": [{}, {"env":{"AORTA_CPP_LANES":"0"}}])
// pins that flow::Runtime::SingleThread()'s AORTA_CPP_LANES=0 rollback path
// (cpp/include/aorta/lane_runtime.h) is observably IDENTICAL to the
// lane-enabled path — the row's whole point — so both scenario ids share
// this one driver verbatim; only the process env selects the executor.
int run_value_delivery_like(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, seven_body());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");
  auto outcome = wait_and_take(handle, scenario + " task");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "task.await", std::to_string(*outcome));
  (void)scope.close();
  return 0;
}

int run_move_only_value(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, move_only_body());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");
  auto outcome = wait_and_take(handle, scenario + " task");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "task.await", "moved:" + outcome.value().label);
  (void)scope.close();
  return 0;
}

int run_cancel_reason_explicit(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, park_until_cancelled_void());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  scope.cancel(flow::CancelReason::kExplicit);
  emit_lifecycle(scenario, "scope.cancel", "parent_cancelled");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

// budget_expiry_entry: the SPAWN ITSELF observes the already-expired
// budget (core/src/flow/scope.rs `begin_external_child`: "Spawning on an
// expired budget fires the token first (rule 3); the child is then born
// into a cancelled scope and should exit at its first checkpoint" — the
// admission is NOT refused, so `handle.ok()` reads true here, unlike
// close_vs_racing_spawn's closed-scope refusal below).
int run_budget_expiry_entry(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.budget = std::chrono::milliseconds(1);
  flow::Scope scope = root.child(opts);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));  // let the budget expire

  auto handle = rt->spawn(scope, park_until_cancelled_void());
  if (!handle.ok()) {
    die(scenario + ": spawn refused (expected admission on an expired-but-"
                   "not-yet-observed budget)");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_on_expired");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_budget_expiry_checkpoint(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.budget = std::chrono::milliseconds(80);
  flow::Scope scope = root.child(opts);

  std::atomic<bool> stopped_flag{false};
  auto handle =
      rt->spawn(scope, budget_checkpoint_body(scenario, scope, &stopped_flag));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!stopped_flag.load(std::memory_order_seq_cst) &&
        std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (!stopped_flag.load(std::memory_order_seq_cst)) {
    die(scenario + ": the checkpoint loop never observed the deadline "
                   "within the 5s budget");
  }
  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

// budget_expiry_parked: KNOWN RISK (inventory finding, restated in the
// per-scenario task brief) — `co_await flow::cancelled()` only wakes when
// the scope's cancel TOKEN actually fires
// (core/src/ffi/c_api/arbiter.rs `aorta_scope_arm_arbiter` watches
// `scope_ref.token().cancelled()`, nothing more), and the token only fires
// from a `checkpoint()` call observing expiry (core/src/flow/scope.rs
// `Scope::checkpoint`) — there is no independent background timer wiring
// budget expiry to the token. A parked task that never itself checkpoints
// (and that nothing else checkpoints on its behalf) may therefore park
// PAST its budget with no automatic wake. This driver does NOT paper over
// that by polling from the task body (the brief explicitly forbids it) —
// wait_and_take's 5s timeout is the intended probe, and a timeout here is
// the expected finding, not a harness bug.
int run_budget_expiry_parked(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.budget = std::chrono::milliseconds(80);
  flow::Scope scope = root.child(opts);

  auto handle = rt->spawn(scope, park_until_cancelled_void());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_fault_message_exact(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle =
      rt->spawn(scope, throw_fault_body("tck: exact fault text"));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a fault outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_parent_child_cancel_order(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> observed{false};
  auto handle = rt->spawn(scope, parent_child_body(scenario, &observed));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  // Emitted BEFORE cancel() so the ORDER is a program-order guarantee
  // (SCHEMA.md "the emitter synchronizes... the ORDER in the expectation
  // file is the contract"), not a timing race against the child thread.
  emit_lifecycle(scenario, "scope.cancel", "parent_cancelling");
  scope.cancel(flow::CancelReason::kShutdown);

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!observed.load(std::memory_order_seq_cst) &&
        std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (!observed.load(std::memory_order_seq_cst)) {
    die(scenario + ": the child never observed the parent's cancel within "
                   "the 5s budget");
  }
  auto outcome = wait_and_take(handle, scenario + " task");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_sibling_fail_fast(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kFailFast;  // explicit (also the default)
  flow::Scope nursery = root.child(opts);

  auto a = rt->spawn(nursery, throw_fault_body("tck-sibling-fault"));
  if (!a.ok()) {
    die(scenario + ": spawn A refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_a");

  auto b = rt->spawn(nursery, park_until_cancelled_void());
  if (!b.ok()) {
    die(scenario + ": spawn B refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_b");

  auto oa = wait_and_take(a, scenario + " task A");
  if (oa.ok()) {
    die(scenario + ": expected A to fault");
  }
  emit_stopped(scenario, "task.await", oa.stopped());

  auto ob = wait_and_take(b, scenario + " task B");
  if (ob.ok()) {
    die(scenario + ": expected B to be cancelled by the fail-fast cascade");
  }
  emit_stopped(scenario, "task.await", ob.stopped());

  // The nursery's own cancel-reason byte, read directly off the scope
  // AFTER both children delivered (so the cascade has provably already
  // fired — B's own cancelled outcome could not exist otherwise).
  emit_counter(scenario, "scope.stats", "scope_cancel_reason_raw",
              static_cast<long long>(nursery.raw_cancel_reason()));
  (void)nursery.close();
  (void)root.close();
  return 0;
}

int run_isolated_child_failure(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.supervision = flow::Supervision::kIsolate;
  flow::Scope nursery = root.child(opts);

  auto a = rt->spawn(nursery, throw_fault_body("tck-isolated-fault"));
  if (!a.ok()) {
    die(scenario + ": spawn A refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_a");

  auto b = rt->spawn(nursery, sleep_then_value_body(20ms, 42));
  if (!b.ok()) {
    die(scenario + ": spawn B refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_b");

  auto oa = wait_and_take(a, scenario + " task A");
  if (oa.ok()) {
    die(scenario + ": expected A to fault");
  }
  emit_stopped(scenario, "task.await", oa.stopped());

  auto ob = wait_and_take(b, scenario + " task B");
  if (!ob.ok()) {
    die(scenario + ": expected B to be UNAFFECTED (Isolate contains A's "
                   "fault to this subtree)");
  }
  emit_value(scenario, "task.await", std::to_string(*ob));

  emit_counter(scenario, "scope.stats", "scope_cancelled",
              nursery.is_cancelled() ? 1 : 0);
  (void)nursery.close();
  (void)root.close();
  return 0;
}

int run_when_all_empty(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, when_all_empty_wrapper());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  auto outcome = wait_and_take(handle, scenario + " wrapper");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "when_all", *outcome);
  (void)scope.close();
  return 0;
}

int run_when_all_multi(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle =
      rt->spawn(scope, when_all_multi_wrapper(*rt, scope));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  auto outcome = wait_and_take(handle, scenario + " wrapper");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "when_all", *outcome);
  (void)scope.close();
  return 0;
}

int run_when_all_fault(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle =
      rt->spawn(scope, when_all_fault_wrapper(*rt, scope));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  auto outcome = wait_and_take(handle, scenario + " wrapper");
  if (outcome.ok()) {
    die(scenario + ": expected the wrapper to fault");
  }
  emit_stopped(scenario, "when_all", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_when_any_first_wins(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle =
      rt->spawn(scope, when_any_first_wins_wrapper(*rt, scope));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  auto outcome = wait_and_take(handle, scenario + " wrapper");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "when_any", *outcome);
  // NOT scope.close(): the two loser branches are parked on
  // flow::cancelled() against a scope nothing ever cancels here (only the
  // Runtime's own teardown sweep resolves them — flow_handle_test.cc
  // PoolCloseResolvesAParkedHandleAwait is the same contract) — close()
  // would BLOCK waiting for them. `rt`'s destructor at scope exit tears
  // down cleanly regardless.
  return 0;
}

int run_when_any_empty(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, when_any_empty_wrapper());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  auto outcome = wait_and_take(handle, scenario + " wrapper");
  if (outcome.ok()) {
    die(scenario + ": expected the empty when_any refusal to cancel the "
                   "wrapper");
  }
  emit_stopped(scenario, "when_any", outcome.stopped());
  (void)scope.close();
  return 0;
}

int run_close_vs_racing_spawn(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();

  aorta::Status closed = scope.close();  // nothing spawned yet: returns promptly
  if (!closed.ok()) {
    die(scenario + ": scope.close() failed before the racing spawn");
  }
  emit_lifecycle(scenario, "scope.close", "closed");

  auto handle = rt->spawn(scope, trivial_void_body());
  if (handle.ok()) {
    die(scenario + ": expected the post-close spawn to be refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawn_refused");

  auto outcome = wait_and_take(handle, scenario + " refused handle");
  if (outcome.ok()) {
    die(scenario + ": expected a cancelled outcome");
  }
  emit_stopped(scenario, "task.await", outcome.stopped());
  return 0;
}

// close_spawn_race (test/flow_tck/expected/close_spawn_race.json,
// scenarios.json manifest note): the GENUINELY CONCURRENT half of the
// "close versus racing spawn" family — run_close_vs_racing_spawn
// above is the deterministic SEQUENTIAL half (close() always returns
// strictly BEFORE the racing spawn is even attempted); this scenario
// races Scope::close() against a CONCURRENT Runtime::spawn() call from a
// separate OS thread on a FRESH child scope each of 50 iterations
// (aorta_scope_spawn is a thread-safe FFI entry — the task brief's own
// wording; the core-side admission race this exercises is exactly what
// run_close_vs_racing_spawn's doc comment reasons about sequentially).
//
// Cross-plane asymmetry (see rust/examples/flow_tck_emitter.rs's
// close_spawn_race, which needs a side-channel `ran` flag instead):
// TaskHandle::status/.ok() reports the spawn ADMISSION synchronously at
// rt->spawn()'s OWN return (flow.hpp: the returned TaskHandle<T>'s
// .ok()/.status report the ADMISSION) — the classification below reads
// that field directly.
//
// Scope::close() (unlike Rust's `async fn close()`) is a plain BLOCKING
// call with no timeout parameter — bounding it externally (the task
// brief's "count close_incomplete if close() ... hangs (bound it)")
// means running it on its own thread and giving OURSELVES an external
// deadline; a hang past the deadline detaches that thread (heap-held
// state via shared_ptr keeps this memory-safe) instead of hanging this
// whole scenario binary forever.
//
// Start-line synchronization: the closer and racer threads each signal
// `started`, then busy-spin (no OS wait/yield — an uninterruptible
// react-to-the-flip loop) on a shared `go` flag; this function's own
// thread waits for both `started` signals, then flips `go`. A per-side
// RANDOM jitter spin (see jitter_spin_cpp) exists because without it one
// side can have a small but SYSTEMATIC latency edge and the race stops
// actually racing (see the delivering report's SANITY section for the
// observed per-run admitted/refused split this produces).
namespace {

struct CloseSpawnRaceState {
  std::atomic<bool> go{false};
  std::atomic<bool> closer_started{false};
  std::atomic<bool> racer_started{false};
  std::atomic<bool> close_done{false};
};

uint64_t race_jitter_seed_cpp(uint64_t iteration, uint64_t salt) {
  const auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
  const uint64_t nanos = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
  return nanos ^ (iteration * 0x9E3779B97F4A7C15ULL) ^
        (salt * 0xBF58476D1CE4E5B9ULL);
}

// Cheap dependency-free jitter (mirrors rust/examples/flow_tck_emitter.rs's
// jitter_spin): a tiny RANDOM (not hardcoded) bounded spin so the two
// racing threads' relative arrival time at their own atomic op varies
// across iterations instead of one side systematically winning. `extra`
// is a fixed per-call handicap ON TOP of the random component — measured
// empirically (see the delivering report's SANITY section): the two
// racing FFI calls (aorta_scope_close's mark-closing-subtree vs.
// aorta_scope_begin_task's admission check) are NOT equally expensive,
// so a pure symmetric random spin alone left admission winning ~85% of
// the time; alternating a bounded handicap by iteration parity (below)
// counteracts that WITHOUT hand-picking either counter's final value —
// the split itself is still read off `.ok()`/outcomes, never asserted.
void jitter_spin_cpp(uint64_t seed, uint64_t extra) {
  uint64_t x = seed ^ 0x2545F4914F6CDD1DULL;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  const uint64_t spins = (x % 4000) + extra;
  volatile uint64_t sink = 0;
  for (uint64_t i = 0; i < spins; ++i) {
    sink += i;
  }
  (void)sink;
}

}  // namespace

constexpr uint64_t kRaceIterations = 50;

struct CloseSpawnRaceRound {
  uint64_t admitted_lost = 0;
  uint64_t refused_unresolved = 0;
  uint64_t close_incomplete = 0;
  uint64_t observed_admitted = 0;
  uint64_t observed_refused = 0;
};

// One 50-iteration racing round at a given handicap magnitude; the caller
// owns escalation and the fail-closed postcondition (mirrors the Rust
// leg). Start-line failures still die() here: those are harness-health
// conditions no escalation can repair. A fresh Runtime per round keeps a
// probe round's closed root from poisoning the next round.
static CloseSpawnRaceRound run_close_spawn_race_round(
    const std::string& scenario, uint64_t handicap) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope root = rt->root_scope();

  const uint64_t kIterations = kRaceIterations;
  uint64_t admitted_lost = 0;
  uint64_t refused_unresolved = 0;
  uint64_t close_incomplete = 0;
  uint64_t observed_admitted = 0;
  uint64_t observed_refused = 0;

  for (uint64_t i = 0; i < kIterations; ++i) {
    flow::Scope::Options opts;
    opts.name = "tck-close-spawn-race-child";
    flow::Scope child_scope = root.child(opts);

    auto state = std::make_shared<CloseSpawnRaceState>();

    // Alternating handicap (see jitter_spin_cpp's doc comment): the two
    // FFI operations are not equally expensive, so a purely symmetric
    // random jitter left one side systematically winning — swinging the
    // handicap by ITERATION PARITY (not the emitted-value logic) fixes
    // that without ever hand-picking a counter's final value. The
    // MAGNITUDE escalates across rounds in the caller: a fixed constant
    // cannot cover every runner speed class (3rd GH-hosted recurrence,
    // 2026-07-14).
    const uint64_t closer_extra = (i % 2 == 0) ? 0 : handicap;
    const uint64_t racer_extra = (i % 2 == 0) ? handicap : 0;

    std::thread closer([child_scope, state, i, closer_extra]() {
      state->closer_started.store(true, std::memory_order_release);
      uint64_t spins = 0;
      while (!state->go.load(std::memory_order_acquire)) {
        if (++spins > 200000000ull) {
          break;  // safety valve; the ready-wait below has its own 5s bound
        }
      }
      jitter_spin_cpp(race_jitter_seed_cpp(i, 0), closer_extra);
      (void)child_scope.close();  // BLOCKING; close_done is the signal
      state->close_done.store(true, std::memory_order_release);
    });

    flow::TaskHandle<> racer_handle;
    std::thread racer([&racer_handle, &rt, child_scope, state, i, racer_extra]() {
      state->racer_started.store(true, std::memory_order_release);
      uint64_t spins = 0;
      while (!state->go.load(std::memory_order_acquire)) {
        if (++spins > 200000000ull) {
          break;
        }
      }
      jitter_spin_cpp(race_jitter_seed_cpp(i, 1), racer_extra);
      racer_handle = rt->spawn(child_scope, trivial_void_body());
    });

    const auto ready_deadline = std::chrono::steady_clock::now() + 5s;
    while ((!state->closer_started.load(std::memory_order_acquire) ||
            !state->racer_started.load(std::memory_order_acquire)) &&
          std::chrono::steady_clock::now() < ready_deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    if (!state->closer_started.load(std::memory_order_acquire) ||
        !state->racer_started.load(std::memory_order_acquire)) {
      // FAIL CLOSED: logging and
      // continuing degenerates this iteration into a non-concurrent
      // close-then-spawn — the scenario would still report all-zero
      // invariants and PASS without ever having raced anything.
      die(scenario + ": close_spawn_race: iteration " + std::to_string(i) +
          ": a racing thread never reached its start line within 5s");
    }
    state->go.store(true, std::memory_order_release);

    racer.join();  // bounded implicitly: one synchronous admission call

    const auto close_deadline = std::chrono::steady_clock::now() + 5s;
    bool close_ok = false;
    while (std::chrono::steady_clock::now() < close_deadline) {
      if (state->close_done.load(std::memory_order_acquire)) {
        close_ok = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (close_ok) {
      closer.join();
    } else {
      close_incomplete++;
      closer.detach();  // heap-held `state` keeps this memory-safe
    }

    const bool admitted = racer_handle.ok();
    if (admitted) {
      observed_admitted++;
    } else {
      observed_refused++;
    }

    if (!racer_handle.wait_for(5s)) {
      if (admitted) {
        admitted_lost++;
      } else {
        refused_unresolved++;
      }
      continue;
    }
    // value OR cancelled both fine — only the admission classification
    // above matters for this scenario's counters.
    auto outcome = racer_handle.take_outcome();
    if (!outcome.has_value()) {
      die(scenario + ": iteration " + std::to_string(i) +
          ": outcome already consumed (harness bug)");
    }
  }

  (void)root.close();
  return CloseSpawnRaceRound{admitted_lost, refused_unresolved,
                             close_incomplete, observed_admitted,
                             observed_refused};
}

int run_close_spawn_race(const std::string& scenario) {
  // Adaptive handicap escalation (mirrors the Rust leg): a fully
  // one-sided round escalates 4x and re-races; emitted records come from
  // the final (non-degenerate) round ONLY, and the ENFORCED
  // both-outcomes postcondition applies
  // after the last escalation exactly as before.
  constexpr int kMaxRounds = 5;
  uint64_t handicap = 6000;
  CloseSpawnRaceRound r;
  for (int round = 0;; ++round) {
    r = run_close_spawn_race_round(scenario, handicap);
    if (r.observed_admitted > 0 && r.observed_refused > 0) {
      break;
    }
    if (round + 1 >= kMaxRounds) {
      die(scenario + ": close_spawn_race: degenerate run — admitted=" +
          std::to_string(r.observed_admitted) +
          ", refused=" + std::to_string(r.observed_refused) + " after " +
          std::to_string(kMaxRounds) + " handicap escalations to " +
          std::to_string(handicap) +
          " (both must be > 0 to prove a genuine race occurred)");
    }
    std::fprintf(stderr,
                 "flow_tck_emitter[%s]: close_spawn_race: round %d "
                 "one-sided (admitted=%llu, refused=%llu) — escalating "
                 "handicap %llu -> %llu for this runner's speed class\n",
                 scenario.c_str(), round,
                 (unsigned long long)r.observed_admitted,
                 (unsigned long long)r.observed_refused,
                 (unsigned long long)handicap,
                 (unsigned long long)(handicap * 4));
    handicap *= 4;
  }

  emit_counter(scenario, "scope.close", "iterations",
              static_cast<long long>(kRaceIterations));
  emit_counter(scenario, "scope.close", "admitted_lost",
              static_cast<long long>(r.admitted_lost));
  emit_counter(scenario, "scope.close", "refused_unresolved",
              static_cast<long long>(r.refused_unresolved));
  emit_counter(scenario, "scope.close", "close_incomplete",
              static_cast<long long>(r.close_incomplete));

  // DRIVER-DROPPED (SCHEMA.md "Normalization": op prefix `debug.`) —
  // humans-only visibility into the timing-dependent admitted/refused
  // split the final round actually produced.
  emit_counter(scenario, "debug.close_spawn_race", "admitted",
              static_cast<long long>(r.observed_admitted));
  emit_counter(scenario, "debug.close_spawn_race", "refused",
              static_cast<long long>(r.observed_refused));

  return 0;
}

int run_stale_handle(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, seven_body());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  if (!handle.wait_for(5s)) {
    die(scenario + ": task did not stop within the 5s budget");
  }
  // Pinning take_outcome() staleness ONLY (per the task brief): two
  // take_outcome() calls on the SAME already-finished handle, never a
  // second co_await.
  auto first = handle.take_outcome();
  if (!first.has_value() || !first->ok()) {
    die(scenario + ": expected the first take_outcome() to deliver a value");
  }
  emit_value(scenario, "task.take_outcome", std::to_string(**first));

  auto second = handle.take_outcome();
  if (second.has_value()) {
    die(scenario + ": expected the second take_outcome() to be empty "
                   "(stale/already-consumed handle)");
  }
  emit_lifecycle(scenario, "task.take_outcome", "second_take_empty");
  (void)scope.close();
  return 0;
}

// ---------------------------------------------------------------------------
// Wave-2 additions below (SCHEMA.md v1, `scope.shutdown_grace` op).
// `flow::Scope::shutdown_with_grace` (cpp/include/aorta/flow.hpp, right
// after shutdown()) is the C++ Rust-parity bounded teardown: cancel(reason)
// + close() bounded by `grace`; on expiry the still-running subtree is
// DETACHED and the observed count returned as an `aorta::StatusOr<uint64_t>`
// — a non-ok status means the call itself failed (invalid scope), which is
// a harness error (die()), never folded into a made-up detached count.
// ---------------------------------------------------------------------------

// The straggler body proven by
// FlowRuntime.GraceShutdownDetachesStragglersAndReportsTheCount
// (cpp/test/flow_runtime_test.cc): signal entry via an atomic, THEN block
// the lane thread synchronously (std::this_thread::sleep_for, no co_await
// before it) — the entry gate cannot refuse the task once `entered` is
// observed true, and no cooperative cancellation point exists for the
// grace window to catch, so it reliably outlives a short grace.
flow::Task<void> shutdown_straggler_body(std::atomic<bool>* entered) {
  entered->store(true, std::memory_order_seq_cst);
  std::this_thread::sleep_for(500ms);
  co_return;
}

// shutdown_clean: a quick child completes well inside a generous (2s)
// grace window — shutdown_with_grace's own bounded close() drains the
// subtree normally and returns 0 detached (the clean-drain path). Matches
// expected/shutdown_clean.json.
int run_shutdown_clean(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();
  auto handle = rt->spawn(scope, seven_body());
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (!outcome.ok()) {
    die(scenario + ": expected a value outcome");
  }
  emit_value(scenario, "task.await", std::to_string(*outcome));

  // shutdown_with_grace performs its own cancel()+close() pair — a
  // separate scope.close() call would just be a redundant no-op here.
  auto detached = scope.shutdown_with_grace(flow::CancelReason::kShutdown, 2s);
  if (!detached.ok()) {
    die(scenario + ": shutdown_with_grace failed (code " +
        std::to_string(static_cast<int>(detached.status().code())) + ", " +
        detached.status().message() + ")");
  }
  emit_counter(scenario, "scope.shutdown_grace", "detached",
              static_cast<long long>(*detached));
  return 0;
}

// shutdown_grace_expired: the straggler signals entry, then blocks past a
// short (50ms) grace — shutdown_with_grace detaches it instead of blocking
// close() forever, and returns the OBSERVED detached count (1). Matches
// expected/shutdown_grace_expired.json.
int run_shutdown_grace_expired(const std::string& scenario) {
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }
  flow::Scope scope = rt->root_scope();

  std::atomic<bool> entered{false};
  auto handle = rt->spawn(scope, shutdown_straggler_body(&entered));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "spawned_straggler");

  // Wait for the straggler to actually signal entry BEFORE calling
  // shutdown — otherwise the entry gate could still refuse or race the
  // task before it ever reaches the blocking sleep (mirrors
  // FlowRuntime.GraceShutdownDetachesStragglersAndReportsTheCount,
  // cpp/test/flow_runtime_test.cc).
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!entered.load(std::memory_order_seq_cst) &&
        std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (!entered.load(std::memory_order_seq_cst)) {
    die(scenario + ": the straggler never signalled entry within the 5s "
                   "budget");
  }

  auto detached =
      scope.shutdown_with_grace(flow::CancelReason::kShutdown, 50ms);
  if (!detached.ok()) {
    die(scenario + ": shutdown_with_grace failed (code " +
        std::to_string(static_cast<int>(detached.status().code())) + ", " +
        detached.status().message() + ")");
  }
  emit_counter(scenario, "scope.shutdown_grace", "detached",
              static_cast<long long>(*detached));

  // Let the straggler actually finish its blocking sleep before the
  // process exits (mirrors the proven test's own "let it drain before
  // teardown asserts anything") — non-fatal: this is cleanup hygiene, not
  // part of the observed shutdown_with_grace contract emitted above.
  if (!handle.wait_for(5s)) {
    std::fprintf(stderr,
                 "flow_tck_emitter[%s]: straggler did not settle within 5s "
                 "after detach (non-fatal)\n",
                 scenario.c_str());
    std::fflush(stderr);
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Wave-3 additions below (SCHEMA.md v1).
// ---------------------------------------------------------------------------

// admission_counters: drives the C++ plane's NATIVE lane-pool admission
// tier (aorta::lanes::LanePool, cpp/include/aorta/lanes.hpp) to produce
// EXACTLY 3 full-queue admission refusals and 2 deadline drops, then emits
// the OBSERVED counter reads under the SCHEMA.md unified logical names.
//
// Two SEPARATE single-lane, ZERO-worker LanePools (constructed directly,
// bypassing flow::Runtime/LaneRuntime's fixed 4096-event default config —
// lane_runtime.h's default_config() has no capacity override in its public
// constructors) isolate the two mechanisms so neither counter's post/
// dequeue timing can perturb the other:
//   - admission_refused: lane capacity 1, NO workers (workers_per_lane
//     stays empty — nothing ever auto-drains this pool), so the ONE
//     filler post saturates the lane and 3 more posts each observe the
//     REAL Post::SHED_FULL refusal (lanes.hpp post(), the native
//     admission tier — lane.stats.shed_full++ at lanes.hpp:193).
//   - deadline_dropped: lane capacity 8, 2 items posted with an
//     ALREADY-expired deadline_ns, then drained one at a time via
//     LanePool::drain_one() (the public ST-owner manual-drain entry
//     point, lanes.hpp:422) — pop_locked's EDF-first dequeue
//     (lanes.hpp:577) observes the expired deadline and bumps the native
//     dropped_expired counter (lanes.hpp:606), routing the item to `drop`
//     instead of `run`.
int run_admission_counters(const std::string& scenario) {
  constexpr size_t kLane = 0;

  // --- admission_refused.
  aorta::lanes::PoolConfig refused_cfg;
  aorta::lanes::LaneConfig refused_lane_cfg;
  refused_lane_cfg.name = "tck-admission-refused";
  refused_lane_cfg.max_queued_events = 1;
  refused_cfg.lanes.push_back(refused_lane_cfg);
  aorta::lanes::LanePool refused_pool(std::move(refused_cfg));

  aorta::lanes::Work filler{};
  if (refused_pool.post(kLane, filler) != aorta::lanes::Post::OK) {
    die(scenario +
        ": filler post unexpectedly refused (lane not yet at capacity)");
  }
  int observed_refusals = 0;
  for (int i = 0; i < 3; i++) {
    aorta::lanes::Work w{};
    const aorta::lanes::Post post_result = refused_pool.post(kLane, w);
    if (post_result != aorta::lanes::Post::SHED_FULL) {
      die(scenario + ": expected Post::SHED_FULL on a saturated lane, got " +
          std::to_string(static_cast<int>(post_result)));
    }
    observed_refusals++;
  }
  const uint64_t admission_refused = refused_pool.stats(kLane).shed_full;
  if (observed_refusals != 3 || admission_refused != 3) {
    die(scenario +
        ": expected 3 observed refusals and shed_full==3, got observed=" +
        std::to_string(observed_refusals) +
        " shed_full=" + std::to_string(admission_refused));
  }
  emit_counter(scenario, "queue.stats", "admission_refused",
              static_cast<long long>(admission_refused));

  // --- deadline_dropped.
  aorta::lanes::PoolConfig expiry_cfg;
  aorta::lanes::LaneConfig expiry_lane_cfg;
  expiry_lane_cfg.name = "tck-admission-expiry";
  expiry_lane_cfg.max_queued_events = 8;
  expiry_cfg.lanes.push_back(expiry_lane_cfg);
  aorta::lanes::LanePool expiry_pool(std::move(expiry_cfg));

  const uint64_t now_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  // 10ms in the past: comfortably expired by the time drain_one() reads
  // the clock again below, with no arithmetic-underflow risk (mono_ns() is
  // already well past 10ms by the time any process reaches this line).
  const uint64_t expired_deadline_ns =
      now_ns > 10'000'000ull ? now_ns - 10'000'000ull : 1;
  for (int i = 0; i < 2; i++) {
    aorta::lanes::Work w{};
    w.deadline_ns = expired_deadline_ns;
    if (expiry_pool.post(kLane, w) != aorta::lanes::Post::OK) {
      die(scenario + ": expired-deadline post unexpectedly refused");
    }
  }
  for (int i = 0; i < 2; i++) {
    if (!expiry_pool.drain_one()) {
      die(scenario + ": drain_one found nothing to drain on the expiry lane");
    }
  }
  const uint64_t deadline_dropped = expiry_pool.stats(kLane).dropped_expired;
  if (deadline_dropped != 2) {
    die(scenario + ": expected dropped_expired==2, got " +
        std::to_string(deadline_dropped));
  }
  emit_counter(scenario, "queue.stats", "deadline_dropped",
              static_cast<long long>(deadline_dropped));
  return 0;
}

// budget_expiry_rpc_call task body: co_await's the explicit-args
// (raw-handle) form of flow::call, scope LAST, and co_returns the RPC-layer
// Outcome verbatim (never throwing on an RPC-layer refusal — only a real
// teardown would; see flow.hpp's call::await_resume doc comment).
flow::Task<flow::Outcome<std::vector<uint8_t>>> budget_expiry_rpc_call_body(
    std::shared_ptr<aorta::Client> client, uint64_t exec, flow::Scope scope) {
  std::vector<uint8_t> payload{1};
  auto result = co_await flow::call{client->handle(), exec, payload.data(),
                                    payload.size(), scope};
  co_return result;
}

// budget_expiry_rpc_call: a flow::call issued under an ALREADY-EXPIRED
// scope budget stops with DeadlineExceeded. The Rust bridge's
// aorta_client_call_ctx (core/src/ffi/c_api/flow.rs) spawns
// client_arc.call_in(scope, ...) on the core tokio runtime; call_in's OWN
// scope.checkpoint() (core/src/client.rs:441, the first statement)
// observes the expired budget and resolves immediately — no query is ever
// issued. The C ABI completion record carries the EXACT raw_reason
// byte through to the delivered flow::Stopped, same as the Rust plane.
// Setup mirrors cpp/test/flow_action_test.cc (a real Node + a
// flow::Runtime-owned, actively-spinning executor — the entry-refusal
// path still needs a live executor to dispatch the eventual resume post,
// see call::on_done/await_suspend in cpp/include/aorta/flow.hpp; it does
// NOT need a live service provider, since the refusal fires before any
// query is issued).
int run_budget_expiry_rpc_call(const std::string& scenario) {
  setenv("AORTA_TEST_LOCAL_ROUTER", "1", 0);

  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }

  auto node = aorta::Node::Create("tck-budget-rpc-call-node");
  if (!node.ok()) {
    die(scenario + ": Node::Create failed: " + node.status().message());
  }
  // NOT rt->add_node(node): a plain RPC Client (unlike an ActionClient) has
  // no inbound subscriptions needing dispatch, so the node never needs
  // adding to an executor — and flow::Runtime::SingleThread()'s spin
  // thread is ALREADY running by the time this function gets the Runtime
  // back (Runtime::create() starts it as its last step), so calling
  // add_node afterward deadlocks against the executor's own internal lock
  // (confirmed empirically: aorta_executor_add_node blocks forever on a
  // mutex the spinning executor holds — cpp/test/flow_action_test.cc's
  // AddNode calls all happen BEFORE its manually-started spin thread,
  // which flow::Runtime's golden path does not give a hook for).
  auto client = (*node)->CreateClient("tck-budget-rpc-call-svc");
  if (!client.ok()) {
    die(scenario + ": CreateClient failed: " + client.status().message());
  }

  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.name = "tck-budget-rpc-call-root";
  opts.budget = std::chrono::milliseconds(1);
  flow::Scope scope = root.child(opts);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));  // let it expire

  auto handle = rt->spawn(
      scope, budget_expiry_rpc_call_body(client.value(), rt->executor(), scope));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }
  emit_lifecycle(scenario, "call", "call_on_expired_budget");

  auto outcome = wait_and_take(handle, scenario + " task");
  if (!outcome.ok()) {
    die(scenario + ": the wrapper task itself did not complete normally");
  }
  const flow::Outcome<std::vector<uint8_t>>& call_outcome = *outcome;
  if (call_outcome.ok()) {
    die(scenario + ": expected the call to refuse on the expired budget");
  }
  emit_stopped(scenario, "call", call_outcome.stopped());
  (void)scope.close();
  return 0;
}

// budget_expiry_queued_server (test/flow_tck/expected/
// budget_expiry_queued_server.json): mirrors
// rust/examples/flow_tck_emitter.rs::budget_expiry_queued_server verbatim
// (see its doc comment for the full deadline-over-RPC mechanism —
// core/src/service.rs `wrap_handler_with_deadline_gate`/
// `deadline_gate_drops`, and `request_dropped_deadline_count`, a SUBSET of
// `request_dropped_count`).
//
// A gate request occupies the service's single dispatch lane (blocked on
// an atomic flag) while a SECOND, budgeted request queues behind it; the
// budgeted call's own client-side scope budget (200ms) expires first and
// resolves `Stopped(Cancelled(DeadlineExceeded))` independently of the
// server (the caller's own guard has already fired by
// definition). Only THEN is the gate released — letting the executor
// dequeue the budgeted request's event, where the deadline gate observes
// the already-expired wire-stamped budget and drops it without ever
// calling the real handler. The emitter polls `Service::GetStats()`
// (bounded, OBSERVED reads) until `request_dropped_deadline_count` reads
// 1.
//
// Unlike `budget_expiry_rpc_call` (a plain Client, no inbound dispatch),
// a Service's request handler only ever runs once its node rides an
// executor (`flow::Runtime`'s factory-nodes wiring, cpp/examples/
// flow_demo.cc) — the deadline gate itself is checked at DISPATCH time
// (when the executor's single lane pulls the posted Event and invokes the
// wrapped handler), not at receipt time, which is exactly why the second
// request can sit queued past its own deadline before being evaluated.
int run_budget_expiry_queued_server(const std::string& scenario) {
  setenv("AORTA_TEST_LOCAL_ROUTER", "1", 0);

  // Declared before node/service/rt so they outlive the Service (whose
  // handler references them) through the whole teardown sequence —
  // Service's destructor waits for in-flight callbacks before returning
  // (service.h's CallbackState in-flight drain), so this ordering is belt-and-
  // braces, not load-bearing, but keeps the lifetime obviously correct.
  std::atomic<bool> released{false};
  std::atomic<bool> unexpected_run{false};
  std::atomic<bool> gate_entered{false};

  auto node = aorta::Node::Create("tck-budget-queued-node");
  if (!node.ok()) {
    die(scenario + ": Node::Create failed: " + node.status().message());
  }

  auto service = (*node)->CreateService(
      "tck-budget-queued-svc",
      [&released, &unexpected_run, &gate_entered](
          const aorta::MessageView& view, aorta::ServiceRequest&& req) {
        static const std::vector<uint8_t> kGate{'g', 'a', 't', 'e'};
        if (view.ToVector() == kGate) {
          // OBSERVABLE occupancy barrier (GH-hosted CI, 2026-07-15 —
          // mirrors the Rust plane): proves the route works AND that
          // this dispatch lane is now blocked.
          gate_entered.store(true, std::memory_order_seq_cst);
          // Block THIS single dispatch lane until the emitter releases
          // the gate — every request behind this one sits queued on the
          // executor for the duration (mirrors the Rust plane's gate
          // handler).
          while (!released.load(std::memory_order_seq_cst)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          (void)req.Reply(std::vector<uint8_t>{'r', 'e', 'l'});
        } else {
          // The deadline gate should have dropped the budgeted request in
          // queue — reaching the real handler at all is a harness
          // finding, not expected behavior.
          unexpected_run.store(true, std::memory_order_seq_cst);
          (void)req.Reply(std::vector<uint8_t>{'u', 'n', 'x'});
        }
      });
  if (!service.ok()) {
    die(scenario + ": CreateService failed: " + service.status().message());
  }

  // NOT Runtime::SingleThread({node->handle()}): this scenario REQUIRES the
  // service handler and the budgeted flow task to sit on DIFFERENT executors.
  // The gate handler blocks its dispatch lane on purpose; if the service ran
  // on the Runtime's single-threaded executor it would block the very thread
  // the flow task needs to observe its expired budget, and the call would
  // never stop. The service therefore stays on the node's own auto-executor
  // (started by CreateService, on its own thread), and — as in
  // run_budget_expiry_rpc_call — a plain RPC Client has no inbound
  // subscriptions needing dispatch, so the node never needs adding here.
  //
  // Passing the handle used to LOOK like it wired the node to the Runtime,
  // but the auto-executor already owned the node and kept dispatching; the
  // Runtime's attach silently clobbered the node cell and its executor never
  // saw a request. `Executor::add_node` now refuses that second claim,
  // which is what surfaced this.
  auto rt = flow::Runtime::SingleThread();
  if (!rt.ok()) {
    die(scenario + ": Runtime::SingleThread failed");
  }

  auto gate_client = (*node)->CreateClient("tck-budget-queued-svc");
  if (!gate_client.ok()) {
    die(scenario + ": CreateClient(gate) failed: " +
        gate_client.status().message());
  }
  auto client = (*node)->CreateClient("tck-budget-queued-svc");
  if (!client.ok()) {
    die(scenario + ": CreateClient failed: " + client.status().message());
  }

  // Let the queryable declare + the local router's routing tables settle
  // (mirrors run_budget_expiry_rpc_call / the Rust plane).
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // The gate call is a plain (non-flow) synchronous call — no Scope, so
  // no wire-stamped deadline attachment, so the deadline gate never drops
  // it (core/src/service.rs `expired_in_queue`: `None` wire_budget is
  // never expired). Runs on its own thread so it can block this dispatch
  // lane while the emitter's main thread proceeds to send the budgeted
  // request behind it.
  // Route + occupancy barrier (mirrors the Rust plane): retry the gate
  // call until the handler proves via gate_entered that it is running
  // and blocking the dispatch lane — a routing race can drop an attempt
  // wholesale under CI load. Bounded and fail-closed. Threads park in
  // the vector so every call's reply (delivered after release) is
  // joined before teardown.
  std::shared_ptr<aorta::Client> gate_client_ptr = gate_client.value();
  std::vector<std::thread> gate_threads;
  int gate_attempts = 0;
  while (!gate_entered.load(std::memory_order_seq_cst)) {
    if (++gate_attempts > 10) {
      die(scenario + ": gate request never reached the handler after 10 "
          "attempts");
    }
    gate_threads.emplace_back([gate_client_ptr] {
      (void)gate_client_ptr->Call(std::vector<uint8_t>{'g', 'a', 't', 'e'});
    });
    const auto attempt_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!gate_entered.load(std::memory_order_seq_cst) &&
           std::chrono::steady_clock::now() < attempt_deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  flow::Scope root = rt->root_scope();
  flow::Scope::Options opts;
  opts.name = "tck-budget-queued-root";
  opts.budget = std::chrono::milliseconds(200);
  flow::Scope scope = root.child(opts);

  emit_lifecycle(scenario, "call", "request_sent");
  auto handle = rt->spawn(
      scope, budget_expiry_rpc_call_body(client.value(), rt->executor(), scope));
  if (!handle.ok()) {
    die(scenario + ": spawn refused");
  }

  auto outcome = wait_and_take(handle, scenario + " call");
  if (!outcome.ok()) {
    die(scenario + ": the wrapper task itself did not complete normally");
  }
  const flow::Outcome<std::vector<uint8_t>>& call_outcome = *outcome;
  if (call_outcome.ok()) {
    die(scenario + ": expected the call to be dropped by the deadline gate");
  }
  emit_stopped(scenario, "call", call_outcome.stopped());
  (void)scope.close();

  // Only now release the blocked gate handler — the deadline gate must
  // observe the ALREADY-expired budget when it finally dequeues the
  // budgeted request.
  released.store(true, std::memory_order_seq_cst);
  for (auto& t : gate_threads) {
    t.join();
  }

  const auto poll_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  uint64_t dropped = 0;
  while (std::chrono::steady_clock::now() < poll_deadline) {
    dropped = service.value()->GetStats().request_dropped_deadline_count;
    if (dropped >= 1) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  if (dropped != 1) {
    die(scenario +
        ": expected request_dropped_deadline_count==1 within 5s, got " +
        std::to_string(dropped));
  }
  if (unexpected_run.load(std::memory_order_seq_cst)) {
    die(scenario +
        ": the budgeted request's real handler ran — the deadline gate "
        "did not drop it");
  }
  emit_counter(scenario, "service.stats", "request_dropped_deadline",
              static_cast<long long>(dropped));
  return 0;
}

}  // namespace


// ---------------------------------------------------------------------------
// SerialDomain rows — manifest-qualified C++-plane
// scenarios (the D2 single-plane precedent): the Rust plane's
// equivalent is type-system-enforced sharing or actor tasks. Every
// stream below is DETERMINISTIC: emissions come either from the main
// thread after joins, or strictly sequentially from within one frame.
// ---------------------------------------------------------------------------

struct SerialHammerShared {
  uint64_t counter = 0;  // unlocked on purpose
  std::atomic<bool> busy{false};
  std::atomic<uint32_t> overlaps{0};
};

flow::Task<void> serial_hammer_body(SerialHammerShared* sh, int iters) {
  for (int i = 0; i < iters; ++i) {
    if (sh->busy.exchange(true, std::memory_order_seq_cst)) {
      sh->overlaps.fetch_add(1, std::memory_order_seq_cst);
    }
    sh->counter++;
    sh->busy.store(false, std::memory_order_seq_cst);
    co_await flow::resume();
  }
  co_return;
}

int run_serial_mutual_exclusion(const std::string& scenario) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  if (!rt.ok()) {
    die(scenario + ": Runtime::MultiThread failed");
  }
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "tck-mutex");
  if (!d.valid()) {
    die(scenario + ": create_serial failed");
  }
  flow::Scope scope = rt->root_scope();
  SerialHammerShared sh;
  constexpr int kIters = 200;
  auto h1 = rt->spawn(scope, d, serial_hammer_body(&sh, kIters));
  auto h2 = rt->spawn(scope, d, serial_hammer_body(&sh, kIters));
  if (!h1.ok() || !h2.ok()) {
    die(scenario + ": bound spawn refused");
  }
  emit_lifecycle(scenario, "task.spawn", "two_bound_tasks_spawned");
  if (!h1.wait_for(30s) || !h2.wait_for(30s)) {
    die(scenario + ": hammer tasks did not finish");
  }
  emit_counter(scenario, "queue.stats", "final_count",
               static_cast<long long>(sh.counter));
  emit_counter(scenario, "queue.stats", "overlaps",
               static_cast<long long>(sh.overlaps.load()));
  (void)scope.close();
  return 0;
}

flow::Task<int> serial_branch_probe(const void* group,
                                    std::atomic<uint32_t>* off_domain,
                                    int v) {
  if (aorta::lanes::g_serial_slice_group != group) {
    off_domain->fetch_add(1, std::memory_order_seq_cst);
  }
  co_return v;
}

flow::Task<int> serial_join_parent(const void* group,
                                   std::atomic<uint32_t>* off_domain) {
  auto [a, b] = co_await flow::join(serial_branch_probe(group, off_domain, 1),
                                    serial_branch_probe(group, off_domain, 2));
  co_return a + b;
}

int run_serial_combinator_inheritance(const std::string& scenario) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  if (!rt.ok()) {
    die(scenario + ": Runtime::MultiThread failed");
  }
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "tck-join");
  if (!d.valid()) {
    die(scenario + ": create_serial failed");
  }
  const void* group =
      flow::detail::serial_access::state(d)->group.get();
  flow::Scope scope = rt->root_scope();
  std::atomic<uint32_t> off_domain{0};
  auto h = rt->spawn(scope, d, serial_join_parent(group, &off_domain));
  if (!h.ok()) {
    die(scenario + ": bound spawn refused");
  }
  if (!h.wait_for(10s)) {
    die(scenario + ": join parent did not finish");
  }
  auto o = h.take_outcome();
  if (!o.has_value() || !o->ok()) {
    die(scenario + ": join parent did not deliver a value");
  }
  emit_value(scenario, "when_all", std::to_string(**o));
  emit_counter(scenario, "queue.stats", "branches_off_domain",
               static_cast<long long>(off_domain.load()));
  (void)scope.close();
  return 0;
}

flow::Task<void> serial_offload_body(const std::string scenario,
                                     const void* group) {
  emit_lifecycle(scenario, "task.body",
                 aorta::lanes::g_serial_slice_group == group
                     ? "before_offload_in_domain"
                     : "before_offload_OFF_DOMAIN");
  const void* closure_mark = nullptr;
  (void)co_await flow::offload(aorta::EventPriority::kLow,
                               [&closure_mark] {
                                 closure_mark =
                                     aorta::lanes::g_serial_slice_group;
                                 return 0;
                               });
  emit_lifecycle(scenario, "task.body",
                 closure_mark == group ? "closure_RAN_IN_DOMAIN"
                                       : "closure_off_domain");
  emit_lifecycle(scenario, "task.body",
                 aorta::lanes::g_serial_slice_group == group
                     ? "continuation_in_domain"
                     : "continuation_OFF_DOMAIN");
  co_return;
}

int run_serial_offload_reentry(const std::string& scenario) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 1});
  if (!rt.ok()) {
    die(scenario + ": Runtime::MultiThread failed");
  }
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "tck-offload");
  if (!d.valid()) {
    die(scenario + ": create_serial failed");
  }
  const void* group =
      flow::detail::serial_access::state(d)->group.get();
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, d, serial_offload_body(scenario, group));
  if (!h.ok()) {
    die(scenario + ": bound spawn refused");
  }
  if (!h.wait_for(10s)) {
    die(scenario + ": offload task did not finish");
  }
  (void)scope.close();
  return 0;
}

flow::Task<void> serial_parked_body(std::atomic<bool>* entered) {
  entered->store(true, std::memory_order_seq_cst);
  co_await flow::sleep(60s);
  co_return;
}

int run_serial_teardown_drop(const std::string& scenario) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  if (!rt.ok()) {
    die(scenario + ": Runtime::MultiThread failed");
  }
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "tck-teardown");
  if (!d.valid()) {
    die(scenario + ": create_serial failed");
  }
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> entered{false};
  auto h = rt->spawn(scope, d, serial_parked_body(&entered));
  if (!h.ok()) {
    die(scenario + ": bound spawn refused");
  }
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!entered.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (!entered.load()) {
    die(scenario + ": bound task never entered");
  }
  // `entered` only proves the body ran its
  // first statement — a stop landing between that store and the sleep's
  // await_suspend completing would be a PRE-ARM cancellation with the
  // same event stream. The ARM-COMPLETE rendezvous on the runtime's
  // arbiter-arm ledger proves the frame actually parked before the stop
  // is issued. The delivery channel is then the stop bridge's revoke
  // sweep -> CLOSED-gate stamp; the queued-marker DROP path has
  // its own seams-gated witness (F-9 row, flow_serial_stopwindow_test).
  while (rt->lanes().live_arm_count() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (rt->lanes().live_arm_count() == 0) {
    die(scenario + ": the sleep arm never registered (frame not parked)");
  }
  emit_lifecycle(scenario, "task.body", "parked_in_domain");
  // The user-reachable external stop: gate closes through the
  // on_stop bridge; the parked frame's teardown delivery is a STAMPED
  // cancel — never a value.
  if (aorta_executor_stop(rt->executor()) != AORTA_OK) {
    die(scenario + ": aorta_executor_stop failed");
  }
  emit_lifecycle(scenario, "scope.cancel", "external_stop_issued");
  if (!h.wait_for(10s)) {
    die(scenario + ": parked frame never resolved after the stop");
  }
  auto o = h.take_outcome();
  if (!o.has_value() || o->ok()) {
    die(scenario + ": expected a stopped outcome after teardown");
  }
  emit_stopped(scenario, "task.take_outcome", o->stopped());
  return 0;
}

flow::Task<void> serial_foreign_arm_body(flow::LaneRuntime* rtb) {
  co_await flow::sleep(*rtb, 5ms);  // F-2: refused before any foreign arm
  co_return;
}

int run_serial_cross_runtime_refusal(const std::string& scenario) {
  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  if (!rta.ok() || !rtb.ok()) {
    die(scenario + ": Runtime::MultiThread failed");
  }
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "tck-foreign");
  if (!d.valid()) {
    die(scenario + ": create_serial failed");
  }
  flow::Scope scope = rta->root_scope();
  auto h = rta->spawn(scope, d, serial_foreign_arm_body(&rtb->lanes()));
  if (!h.ok()) {
    die(scenario + ": bound spawn refused");
  }
  if (!h.wait_for(10s)) {
    die(scenario + ": foreign-arm task never resolved");
  }
  auto o = h.take_outcome();
  if (!o.has_value() || o->ok()) {
    die(scenario + ": expected the foreign-plane arm to refuse (F-2)");
  }
  emit_stopped(scenario, "task.take_outcome", o->stopped());
  (void)scope.close();
  return 0;
}

flow::Task<void> serial_late_call_body(const std::string scenario,
                                       uint64_t client, uint64_t exec,
                                       flow::Scope scope) {
  static const uint8_t ping[] = {42};
  auto r = co_await flow::call{client, exec, ping, sizeof(ping),
                               std::move(scope)}
               .nothrow();
  // Late-completion degradation: the delivery layer observed the
  // stamped CANCEL, never a value. Also
  // prove the REPLY itself landed — `.nothrow()` separates the two
  // layers (delivery stamp vs RPC reply), so a reply-send failure
  // resolved by some other completion would otherwise produce the same
  // stream. All three must hold: stamped delivery, OK reply, {7} pong.
  const bool stamped = r.delivery == AORTA_ERR_CANCELLED;
  const bool reply_landed =
      r.reply.ok() && (*r.reply).size() == 1 && (*r.reply)[0] == 7;
  emit_lifecycle(scenario, "call",
                 stamped && reply_landed
                     ? "late_delivery_stamped_cancel_reply_landed"
                     : "late_delivery_UNEXPECTED");
  co_return;
}

int run_serial_late_completion_degradation(const std::string& scenario) {
  auto server_node_or = aorta::Node::Create("tck_serial_late_srv");
  if (!server_node_or.ok()) {
    die(scenario + ": server node create failed");
  }
  auto server_node = server_node_or.value();
  // Attach BEFORE the service: the first executor attached to a node owns it,
  // so creating the service first binds it to the SDK auto-executor and this
  // executor never receives the request.
  auto server_exec_or = aorta::Executor::Create();
  if (!server_exec_or.ok()) {
    die(scenario + ": server executor create failed");
  }
  auto server_exec = server_exec_or.value();
  if (!server_exec->AddNode(server_node).ok()) {
    die(scenario + ": server AddNode failed");
  }

  std::mutex req_m;
  std::optional<aorta::ServiceRequest> held_req;
  std::atomic<bool> req_taken{false};
  auto service_or = server_node->CreateService(
      "tck_serial_late_echo",
      [&](const aorta::MessageView&, aorta::ServiceRequest&& req) {
        if (!req_taken.exchange(true)) {
          std::lock_guard<std::mutex> lk(req_m);
          held_req.emplace(std::move(req));
        }
      });
  if (!service_or.ok()) {
    die(scenario + ": service create failed");
  }
  auto service = service_or.value();
  (void)service;
  std::thread server_spin([&server_exec] { (void)server_exec->Spin(); });

  auto client_node_or = aorta::Node::Create("tck_serial_late_cli");
  if (!client_node_or.ok()) {
    die(scenario + ": client node create failed");
  }
  auto client_node = client_node_or.value();
  auto rpc_client_or = client_node->CreateClient("tck_serial_late_echo");
  if (!rpc_client_or.ok()) {
    die(scenario + ": client create failed");
  }
  auto rpc_client = rpc_client_or.value();
  auto matched = rpc_client->WaitForMatching(10000);
  if (!matched.ok() || !matched.value()) {
    die(scenario + ": echo service never became routable");
  }

  flow::TaskHandle<void> h;
  {
    auto rt_or = flow::Runtime::MultiThread({0, 0, 1, 0},
                                            {client_node->handle()});
    if (!rt_or.ok()) {
      die(scenario + ": client Runtime create failed");
    }
    auto rt = std::move(*rt_or);
    auto d = rt.create_serial(aorta::EventPriority::kNormal, "tck-late");
    if (!d.valid()) {
      die(scenario + ": create_serial failed");
    }
    flow::Scope scope = rt.root_scope();
    h = rt.spawn(scope, d,
                 serial_late_call_body(scenario, rpc_client->handle(),
                                       rt.executor(), scope));
    if (!h.ok()) {
      die(scenario + ": bound spawn refused");
    }
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!req_taken.load(std::memory_order_seq_cst) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    if (!req_taken.load()) {
      die(scenario + ": the server never received the call");
    }
    emit_lifecycle(scenario, "call", "call_parked_serverside_held");
    // ~Runtime here: gate close_and_quiesce → stop → join → free — with
    // the reply still outstanding.
  }
  emit_lifecycle(scenario, "scope.close", "runtime_destroyed");
  {
    std::lock_guard<std::mutex> lk(req_m);
    const uint8_t pong[] = {7};
    // The reply send MUST succeed — a swallowed failure here
    // would let a timeout/other completion masquerade as the late reply.
    if (!std::move(*held_req)
             .Reply(std::vector<uint8_t>(pong, pong + 1))
             .ok()) {
      die(scenario + ": late Reply() send failed");
    }
    held_req.reset();
  }
  if (!h.wait_for(10s)) {
    die(scenario + ": the late completion never resolved the frame");
  }
  // The .nothrow() form absorbs the teardown at the DELIVERY layer
  // (already emitted above as the stamped cancel); the frame then runs
  // to normal completion — the outcome is OK by the nothrow contract.
  auto o = h.take_outcome();
  if (!o.has_value() || !o->ok()) {
    die(scenario + ": expected the nothrow frame to complete after the "
                   "stamped delivery");
  }
  emit_lifecycle(scenario, "task.take_outcome", "frame_completed");
  if (!server_exec->Stop().ok()) {
    die(scenario + ": server executor stop failed");
  }
  server_spin.join();
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: flow_tck_emitter <scenario>\n");
    return 2;
  }
  const std::string scenario = argv[1];
  if (scenario == "serial_mutual_exclusion") {
    return run_serial_mutual_exclusion(scenario);
  }
  if (scenario == "serial_combinator_inheritance") {
    return run_serial_combinator_inheritance(scenario);
  }
  if (scenario == "serial_offload_reentry") {
    return run_serial_offload_reentry(scenario);
  }
  if (scenario == "serial_teardown_drop") {
    return run_serial_teardown_drop(scenario);
  }
  if (scenario == "serial_cross_runtime_refusal") {
    return run_serial_cross_runtime_refusal(scenario);
  }
  if (scenario == "serial_late_completion_degradation") {
    return run_serial_late_completion_degradation(scenario);
  }
  if (scenario == "cancel_reason_observation") {
    return run_cancel_reason_observation(scenario);
  }
  if (scenario == "value_delivery" || scenario == "lanes_rollback") {
    return run_value_delivery_like(scenario);
  }
  if (scenario == "move_only_value") {
    return run_move_only_value(scenario);
  }
  if (scenario == "cancel_reason_explicit") {
    return run_cancel_reason_explicit(scenario);
  }
  if (scenario == "budget_expiry_entry") {
    return run_budget_expiry_entry(scenario);
  }
  if (scenario == "budget_expiry_checkpoint") {
    return run_budget_expiry_checkpoint(scenario);
  }
  if (scenario == "budget_expiry_parked") {
    return run_budget_expiry_parked(scenario);
  }
  if (scenario == "fault_message_exact") {
    return run_fault_message_exact(scenario);
  }
  if (scenario == "parent_child_cancel_order") {
    return run_parent_child_cancel_order(scenario);
  }
  if (scenario == "sibling_fail_fast") {
    return run_sibling_fail_fast(scenario);
  }
  if (scenario == "isolated_child_failure") {
    return run_isolated_child_failure(scenario);
  }
  if (scenario == "when_all_empty") {
    return run_when_all_empty(scenario);
  }
  if (scenario == "when_all_multi") {
    return run_when_all_multi(scenario);
  }
  if (scenario == "when_all_fault") {
    return run_when_all_fault(scenario);
  }
  if (scenario == "when_any_first_wins") {
    return run_when_any_first_wins(scenario);
  }
  if (scenario == "when_any_empty") {
    return run_when_any_empty(scenario);
  }
  if (scenario == "close_vs_racing_spawn") {
    return run_close_vs_racing_spawn(scenario);
  }
  if (scenario == "close_spawn_race") {
    return run_close_spawn_race(scenario);
  }
  if (scenario == "stale_handle") {
    return run_stale_handle(scenario);
  }
  if (scenario == "shutdown_clean") {
    return run_shutdown_clean(scenario);
  }
  if (scenario == "shutdown_grace_expired") {
    return run_shutdown_grace_expired(scenario);
  }
  if (scenario == "admission_counters") {
    return run_admission_counters(scenario);
  }
  if (scenario == "budget_expiry_rpc_call") {
    return run_budget_expiry_rpc_call(scenario);
  }
  if (scenario == "budget_expiry_queued_server") {
    return run_budget_expiry_queued_server(scenario);
  }
  std::fprintf(stderr, "flow_tck_emitter: unknown scenario %s\n",
              scenario.c_str());
  return 2;
}
