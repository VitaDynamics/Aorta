// NEGATIVE compile test: old explicit parameter orders.
//
// The pre-freeze explicit `join_all()` took `(rt, scope, tasks)`; the
// frozen rule puts scope LAST: `join_all(rt, tasks, scope)` (unlike
// `join()`, join_all takes a `std::vector<Task<T,K>>` rather than a
// deduced pack, so nothing stops scope from moving to the final position —
// it is NOT a recorded exemption). The old order was DELETED (no
// deprecated aliases) — this call must fail overload resolution.
//
// EXPECTED: fails to compile — "no matching function".
#include <aorta/flow_combinators.hpp>

#include <vector>

namespace flow = aorta::flow;

flow::Task<int> value() { co_return 1; }

flow::Task<void> misuse(flow::LaneRuntime& rt, flow::Scope scope) {
  std::vector<flow::Task<int>> tasks;
  tasks.push_back(value());
  // OLD order: scope second. The frozen signature is (rt, tasks, scope).
  auto v = co_await flow::join_all(rt, scope, std::move(tasks));
  (void)v;
}

int main() { return 0; }
