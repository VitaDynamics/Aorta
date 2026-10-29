// Expect-fail TU 1: a FOREIGN (non-flow)
// coroutine awaiting a flow Task must FAIL to compile — the promise-
// carried context cannot cross into a frame that has no context slot
// (P3552 rationale: fail-compile beats silently changing context).
// Expected diagnostic: the Task await_suspend teaching static_assert
//   "awaiting a flow Task requires a flow coroutine"
#include <aorta/flow.hpp>

#include <coroutine>

struct foreign_task {
  struct promise_type {  // deliberately NOT derived from the flow context
    foreign_task get_return_object() { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() {}
  };
};

static aorta::flow::Task<int> flow_value() { co_return 1; }

foreign_task probe() {
  (void)co_await flow_value();  // must NOT compile
}
