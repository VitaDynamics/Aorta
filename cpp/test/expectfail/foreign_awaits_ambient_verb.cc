// Expect-fail TU 3: a foreign coroutine awaiting
// an ambient verb (the unified hop flow::resume(), the ambient sleep, and
// the ambient combinator door via flow::join) must FAIL to compile with
// the teaching text
//   "flow ambient verbs require a flow coroutine"
#include <aorta/flow_combinators.hpp>

#include <chrono>
#include <coroutine>

struct foreign_task {
  struct promise_type {
    foreign_task get_return_object() { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() {}
  };
};

foreign_task probe() {
  co_await aorta::flow::resume();  // must NOT compile
}

foreign_task probe_sleep() {
  (void)co_await aorta::flow::sleep(std::chrono::milliseconds(1));
}

// The ambient combinator door (detail::ambient_combinator_awaiter) carries
// the same teaching fallback — pinned here through flow::join.
static aorta::flow::Task<int> flow_child() { co_return 1; }

foreign_task probe_join() {
  (void)co_await aorta::flow::join(flow_child(), flow_child());
}
