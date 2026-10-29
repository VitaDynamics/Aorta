// EXPECTED: rejected via the D1 choke gate (graduated from a former RED
// gap — see git history for the pre-D1 "EXPECTED TODAY: compiles" text,
// and cpp/test/flow_noexcept_test.cc's git history for the deleted
// `endpoint_compile_only` compile-only coverage this TU's rejection
// replaces).
//
// Covers: Task<T, K> co_await (await_resume) and EndpointTask awaits —
// recv() returns EndpointTask<PullSample>, which shares
// Task<T,K>::await_resume.
//
// `co_await flow::recv(sub)` (the AMBIENT form) used to compile "by
// design" under -fno-exceptions (the endpoint verbs' happy path never
// throws). That "by design" carve-out is exactly the gap D1 closes: under
// the frozen D1 contract, -fno-exceptions consumers should never reach
// any Flow await surface, endpoint or not. Two independent D1 gates now
// cover this TU: `probe_recv` below is itself a `flow::Task<void>`
// coroutine, so the CHOKE POINT (Task's promise_type) rejects
// it before the body runs; separately, `flow::recv<R>` (flow_subscriber.
// hpp) is now a template keyed on its own return type purely to keep
// HEADER INCLUSION legal under -fno-exceptions (deferring `Task<PullSample,
// task_kind_endpoint>::promise_type` instantiation to the call site) — an
// ACTUAL call, like this one, still instantiates it and still trips the
// same choke-gate static_assert, once reached. The rejection carries the
// teaching text "C++ Flow requires exception support" (run_expectfail.sh's
// needle).
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow_subscriber.hpp>

namespace flow = aorta::flow;

flow::Task<void> probe_recv(aorta::PullSubscriber& sub) {
  // rejected: the choke gate fires on probe_recv's own promise_type
  // first (and, independently, on flow::recv<...>'s promise_type once
  // instantiated by this call).
  aorta::PullSample s = co_await flow::recv(sub);
  (void)s;
}
