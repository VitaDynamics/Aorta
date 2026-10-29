// NEGATIVE compile test: old explicit parameter orders.
//
// The pre-freeze explicit `race()` took `(rt, scope, a, b)`; the
// frozen rule puts scope LAST: `race(rt, a, b, scope)`. The old order was
// DELETED (no deprecated aliases) — this call must fail overload
// resolution.
//
// NOTE: `join(rt, scope, tasks...)` is NOT here — it is the RECORDED
// EXEMPTION (a fixed parameter cannot follow a deduced pack), so its
// scope-after-plane order is the frozen final shape.
//
// EXPECTED: fails to compile — "no matching function".
#include <aorta/flow_combinators.hpp>

namespace flow = aorta::flow;

flow::Task<int> value() { co_return 1; }

flow::Task<void> misuse(flow::LaneRuntime& rt, flow::Scope scope) {
  // OLD order: scope second. The frozen signature is (rt, a, b, scope).
  int v = co_await flow::race(rt, scope, value(), value());
  (void)v;
  co_return;
}

int main() { return 0; }
