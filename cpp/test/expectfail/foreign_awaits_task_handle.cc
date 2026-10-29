// Expect-fail TU 4: a foreign coroutine awaiting a flow::TaskHandle
// must FAIL to compile with the teaching text
//   "co_await TaskHandle requires a flow coroutine"
// (outside flow tasks the handle's seam is wait_for()/take_outcome()).
#include <aorta/flow.hpp>

#include <coroutine>
#include <utility>

struct foreign_task {
  struct promise_type {
    foreign_task get_return_object() { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() {}
  };
};

foreign_task probe(aorta::flow::TaskHandle<int> h) {
  (void)co_await std::move(h);  // must NOT compile
}
