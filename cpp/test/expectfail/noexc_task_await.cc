// EXPECTED: rejected via the D1 choke gate (graduated from a former RED
// gap — see git history for the pre-D1 "EXPECTED TODAY: compiles" text).
//
// Covers: `Task<T, K>` co_await (await_resume) and EndpointTask awaits,
// which previously compiled under -fno-exceptions without being gated.
//
// D1's remediation is the CHOKE POINT: `Task<T,K>::promise_
// type` (flow.hpp) static_asserts on INSTANTIATION under -fno-exceptions
// — defining OR awaiting any flow::Task/EndpointTask coroutine trips it.
// `probe_task_await` below is itself a `flow::Task<void>` coroutine, so
// its own promise_type instantiation rejects before its body (the
// `co_await child()` on a plain `Task<int>`) is ever reached — both are
// choke-gated regardless. The rejection carries the teaching text "C++
// Flow requires exception support" (run_expectfail.sh's needle).
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow.hpp>

namespace flow = aorta::flow;

static flow::Task<int> child() { co_return 1; }

flow::Task<void> probe_task_await() {
  int v = co_await child();  // rejected: the choke gate fires on
                              // probe_task_await's own promise_type first
  (void)v;
}
