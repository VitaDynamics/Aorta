// NEGATIVE compile test: old explicit parameter orders.
//
// The pre-freeze explicit `cancel_goal{...}` took the raw shape
// `{goal, scope, executor}`; the frozen rule ("primary object,
// plane, operation args, scope LAST") ports the action verbs too:
// `cancel_goal{goal, executor, scope}` (flow_action.hpp). The old order
// was DELETED (no deprecated aliases) — this call must fail
// overload resolution / deduction-guide matching.
//
// Uses the REAL generated action type
// (`aorta::action::example::FlowResumeProbeAction`) — same harness shape as
// cpp/test/flow_action_test.cc — rather than a hand-rolled stand-in, so
// this negative proves the real deduction-guide set, not a shim's. Needs
// the generated action headers on the include path: run_expectfail.sh's
// $ACTION_FLAGS (see its header comment for the extra `bazel build`
// prereq beyond `//core:aorta_ffi_headers`).
//
// EXPECTED: fails to compile — "no matching function" (constructor
// overload / deduction-guide rejection; GCC and Clang phrase this
// slightly differently, both covered by check_old_order()).
#include <aorta/flow_action.hpp>

#include "flow_resume_probe_msg.h"

namespace flow = aorta::flow;
using TestAction = ::aorta::action::example::FlowResumeProbeAction;

flow::Task<void> misuse(flow::Scope scope, uint64_t exec) {
  // Default-constructed identity: AcceptedGoal is a plain aggregate (no
  // live client/server needed — this TU only probes overload resolution,
  // never awaits/resolves the verb).
  flow::AcceptedGoal<TestAction> goal{};
  // OLD order: scope second. The frozen signature is
  // (goal, executor, scope).
  auto result = co_await flow::cancel_goal{goal, scope, exec};
  (void)result;
}

int main() { return 0; }
