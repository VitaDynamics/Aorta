// D1 BELT evidence (choke-gate completion
// sweep): a NON-flow coroutine co_awaits the EXPLICIT `flow::sleep(rt,
// 1ms)` form under -fno-exceptions.
//
// Like noexc_foreign_nothrow_sleep_on.cc, this is exactly what the CHOKE
// POINT (Task's promise_type) cannot catch — the awaiting frame here,
// `foreign_task`, is not a `flow::Task`. The explicit `sleep` struct
// (flow_combinators.hpp) historically had a SILENT status-return fallback
// under -fno-exceptions ("behavior unchanged (including the -fno-
// exceptions silent status return)" per its old header comment) — the
// D1 gate REVOKES that: its `await_resume` now carries the same dependent
// static_assert pattern as every other Flow await surface, closing this
// belt gap too (unlike the ambient `sleep(d)` form, which was already
// gated before this sweep).
// Expected diagnostic: "C++ Flow requires exception support"
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow_combinators.hpp>

#include <chrono>
#include <coroutine>

using namespace std::chrono_literals;

struct foreign_task {
  struct promise_type {  // deliberately NOT derived from the flow context
    foreign_task get_return_object() { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() {}
  };
};

foreign_task probe_explicit_sleep(aorta::flow::LaneRuntime& rt) {
  // rejected: the explicit sleep struct's belt gate fires even though
  // this frame never instantiates Task's promise_type at all.
  (void)co_await aorta::flow::sleep(rt, 1ms);
}
