// D1 BELT evidence (choke-gate completion
// sweep): a NON-flow coroutine co_awaiting `sleep_on{...}.nothrow()`
// under -fno-exceptions.
//
// This is exactly the case the CHOKE POINT (Task's promise_type static_
// assert) CANNOT catch: the awaiting frame here, `foreign_task`, is not
// a `flow::Task` at all — its promise_type never touches Flow's
// machinery. `sleep_on::await_suspend<P>` (flow.hpp) is deliberately
// UNCONSTRAINED (no FlowPromise requirement — foreign frames legitimately
// need to be able to suspend on it, e.g. via the ambient-verb teaching-fallback
// design), so suspension itself compiles clean here. The only gate left
// is the BELT: `sleep_on::nothrow_awaiter::await_resume`'s own dependent
// static_assert, which REVOKES the historical "silent status return"
// `.nothrow()` gave under -fno-exceptions. The D1 contract is "Flow
// is not part of a -fno-exceptions consumer's surface at all" — not even
// its non-throwing, foreign-frame-reachable opt-outs.
// Expected diagnostic: "C++ Flow requires exception support"
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow.hpp>

#include <chrono>
#include <coroutine>
#include <cstdint>

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

foreign_task probe_foreign_nothrow_sleep_on(uint64_t exec) {
  // rejected: sleep_on::nothrow_awaiter::await_resume's belt gate fires
  // even though this frame never instantiates Task's promise_type at all.
  (void)co_await aorta::flow::sleep_on{exec, 1ms}.nothrow();
}
