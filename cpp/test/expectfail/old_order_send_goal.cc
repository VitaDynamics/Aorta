// NEGATIVE compile test: old explicit parameter orders.
//
// The pre-freeze explicit `send_goal{...}` took the raw shape
// `{client, scope, executor, goal}`; the frozen rule ("primary
// object, plane, operation args, scope LAST") ports the action verbs too:
// `send_goal{client, executor, goal, scope}` (flow_action.hpp). The old
// order was DELETED (no deprecated aliases) — this call must fail
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

namespace {
::aorta::action::example::FlowResumeProbeGoalMsg make_goal() {
  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.command = std::string("old-order-probe");
  return goal;
}
}  // namespace

flow::Task<void> misuse(
    std::shared_ptr<aorta::ActionClient<TestAction>> client, uint64_t exec,
    flow::Scope scope) {
  // OLD order: scope second. The frozen signature is
  // (client, executor, goal, scope).
  auto accepted =
      co_await flow::send_goal{client, scope, exec, make_goal()};
  (void)accepted;
}

int main() { return 0; }
