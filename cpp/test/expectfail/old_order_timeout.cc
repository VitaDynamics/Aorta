// NEGATIVE compile test: old explicit parameter orders.
//
// The PRE-FREEZE explicit `timeout()` overload was
// `timeout(LaneRuntime& rt, Scope scope, nanoseconds window, Task<T,K>
// task)`. The "one explicit-parameter rule" of the API
// freeze is:
//   "primary object, execution plane, operation-specific arguments,
//    scope LAST"
// The API freeze DELETED that overload; the only explicit
// `timeout()` now takes scope last.
//
// This TU pins the deleted pre-freeze call shape as a NEGATIVE compile
// test: it MUST fail to compile ("no matching function"). If it ever
// compiles again, the old parameter order has crept back in —
// run_expectfail.sh's check_old_order() gates on exactly that (a
// compile SUCCESS here fails the suite).
#include <aorta/flow_combinators.hpp>

#include <chrono>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

static flow::Task<int> child() { co_return 1; }

flow::Task<void> probe_old_order_timeout(flow::LaneRuntime& rt,
                                         flow::Scope scope) {
  // PRE-FREEZE order (rt, scope, window, task) — deleted by the API
  // freeze; must NOT compile.
  int v = co_await flow::timeout(rt, scope, 50ms, child());
  (void)v;
}
