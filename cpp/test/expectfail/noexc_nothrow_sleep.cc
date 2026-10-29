// EXPECTED: rejected via the D1 choke gate (graduated from a former RED
// gap — see git history for the pre-D1 "EXPECTED TODAY: compiles" text,
// and cpp/test/flow_noexcept_test.cc's git history for the deleted
// `all_nothrow_body` GREEN runtime coverage this TU's rejection
// replaces).
//
// Covers: sleep / sleep_on / resume_on — default AND .nothrow() forms.
//
// `flow::sleep(1ms).nothrow()` was the pre-D1 sanctioned non-throwing
// convenience. D1's CHOKE POINT already rejects this TU on
// probe_nothrow_sleep's OWN promise_type (it is a `flow::Task<void>`
// coroutine) before the `.nothrow()` call is reached — and the BELT gate
// additionally covers `ambient_sleep::nothrow_awaiter::
// await_resume` itself (flow_combinators.hpp) with its own dependent
// static_assert, for the foreign-frame case where a non-flow coroutine
// awaits the same `.nothrow()` form directly (see
// noexc_foreign_nothrow_sleep_on.cc). The C++ Flow contract is
// "Flow is not part of [the -fno-exceptions consumer's] surface" at all,
// not "only the throwing spellings are off-limits" — this TU is the
// choke-gate evidence for the ambient-verb case; the belt-gate evidence
// lives in the *_foreign_* TUs. The rejection carries the teaching text
// "C++ Flow requires exception support" (run_expectfail.sh's needle).
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow_combinators.hpp>

#include <chrono>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

flow::Task<void> probe_nothrow_sleep() {
  // rejected: the choke gate fires on probe_nothrow_sleep's own
  // promise_type first (D1 revokes this "sanctioned" pre-D1 spelling).
  (void)co_await flow::sleep(1ms).nothrow();
}
