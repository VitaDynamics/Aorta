// Expect-fail TU 2: a foreign coroutine awaiting
// a this_coro context reader must FAIL to compile with the teaching text
//   "flow::this_coro::* requires a flow coroutine"
#include <aorta/flow.hpp>

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
  auto ctx = co_await aorta::flow::this_coro::context;  // must NOT compile
  (void)ctx;
}
