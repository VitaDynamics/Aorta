// Expect-fail harness — POSITIVE CONTROL: proves the
// REAL flow headers compile in this environment with the exact flags the
// negative TUs use, so a negative TU's failure can only come from the
// teaching static_assert, never from a broken include path or toolchain.
#include <aorta/flow_combinators.hpp>
#include <aorta/flow_readiness.hpp>
#include <aorta/flow_subscriber.hpp>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

static flow::Task<int> child() {
  co_await flow::sleep(1ms);
  co_return 1;
}

static flow::Task<void> flow_body(aorta::PullSubscriber& sub) {
  co_await flow::resume();
  auto ctx = co_await flow::this_coro::context;
  (void)ctx;
  auto [a, b] = co_await flow::join(child(), child());
  (void)(a + b);
  aorta::PullSample s = co_await flow::recv(sub);
  (void)s;
}

int main() {
  auto* keep = &flow_body;
  (void)keep;
  return 0;
}
