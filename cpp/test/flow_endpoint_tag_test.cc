// B1 hardening — the endpoint task tag (flow.hpp task_kind_endpoint).
//
// Compile-time contract, pinned by static_asserts (the
// dispatcher_noexcept_test pattern — a POSITIVE target whose asserts pin
// the trait values):
//   1. every parked endpoint verb (recv/recv_for, aligned/aligned_for,
//      wait_wave/wait_wave_for) returns an ENDPOINT-tagged task;
//   2. regular tasks are not endpoint-tagged;
//   3. timeout()/race() awaiters instantiate for REGULAR kinds (their
//      class-scope static_assert passes) and join() instantiates for
//      BOTH kinds (join resumes only after every branch resolves — no
//      loser is ever stranded);
//   4. endpoint tasks stay valid for direct co_await and join()
//      (runtime tests below).
//
// The REJECTION side (timeout()/race() over an endpoint verb) cannot be a
// runnable test — it is a compile error by design, and the repo has no
// negative-compilation harness. To reproduce the red state by hand,
// uncomment ONE of the probes at the bottom of this file and rebuild this
// target; each fails with:
//
//   static assertion failed ...: use recv_for / aligned_for /
//   wait_wave_for — wrapping a parked endpoint verb in timeout()/race()
//   strands the loser branch
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <tuple>
#include <utility>

#include "aorta/flow_readiness.hpp"
#include "aorta/flow_subscriber.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

// ---------------------------------------------------------------------------
// 1. Every endpoint verb overload returns an endpoint-tagged task.
// ---------------------------------------------------------------------------
using SubT = aorta::PullSubscriber&;
using RtT = flow::LaneRuntime&;
using ScopeT = flow::Scope;
using WsT = flow::WindowSync&;
using SetT = flow::ReadinessSet&;
using NsT = std::chrono::nanoseconds;

template <typename... Args>
using recv_t = decltype(flow::recv(std::declval<Args>()...));
template <typename... Args>
using recv_for_t = decltype(flow::recv_for(std::declval<Args>()...));
template <typename... Args>
using aligned_t = decltype(flow::aligned(std::declval<Args>()...));
template <typename... Args>
using aligned_for_t = decltype(flow::aligned_for(std::declval<Args>()...));
template <typename... Args>
using wait_wave_t = decltype(flow::wait_wave(std::declval<Args>()...));
template <typename... Args>
using wait_wave_for_t =
    decltype(flow::wait_wave_for(std::declval<Args>()...));

static_assert(flow::is_endpoint_task_v<recv_t<SubT, RtT, ScopeT>>);
static_assert(flow::is_endpoint_task_v<recv_for_t<SubT, RtT, NsT, ScopeT>>);
static_assert(flow::is_endpoint_task_v<aligned_t<WsT, RtT, ScopeT>>);
static_assert(flow::is_endpoint_task_v<aligned_for_t<WsT, RtT, NsT, ScopeT>>);
static_assert(flow::is_endpoint_task_v<wait_wave_t<SetT, RtT, ScopeT>>);
static_assert(
    flow::is_endpoint_task_v<wait_wave_for_t<SetT, RtT, NsT, ScopeT>>);

// The cancellation-blind scope-less EXPLICIT overloads are REMOVED:
// the explicit tier is scope-full only, recv parity. Pin the removal — no viable
// overload may resolve for the scope-less argument shapes. (Concepts,
// not bare requires-expressions: only a DEPENDENT call is
// SFINAE-friendly — a namespace-scope requires over concrete types
// makes the failed call a hard error.)
template <typename... Args>
concept aligned_callable = requires(Args... args) { flow::aligned(args...); };
template <typename... Args>
concept aligned_for_callable =
    requires(Args... args) { flow::aligned_for(args...); };
template <typename... Args>
concept wait_wave_callable =
    requires(Args... args) { flow::wait_wave(args...); };
template <typename... Args>
concept wait_wave_for_callable =
    requires(Args... args) { flow::wait_wave_for(args...); };

static_assert(!aligned_callable<WsT, RtT>);
static_assert(!aligned_for_callable<WsT, RtT, NsT>);
static_assert(!wait_wave_callable<SetT, RtT>);
static_assert(!wait_wave_for_callable<SetT, RtT, NsT>);
// Positive controls: the scope-full shapes resolve (a probe that would
// reject EVERYTHING must not pass the negative asserts above).
static_assert(aligned_callable<WsT, RtT, ScopeT>);
static_assert(aligned_for_callable<WsT, RtT, NsT, ScopeT>);
static_assert(wait_wave_callable<SetT, RtT, ScopeT>);
static_assert(wait_wave_for_callable<SetT, RtT, NsT, ScopeT>);

// The AMBIENT endpoint forms are EndpointTask wrappers — the kind
// is preserved, so the B1 timeout()/race() rejection fires on ambient
// forms exactly as on explicit ones.
static_assert(flow::is_endpoint_task_v<recv_t<SubT>>);
static_assert(flow::is_endpoint_task_v<recv_for_t<SubT, NsT>>);
static_assert(flow::is_endpoint_task_v<aligned_t<WsT>>);
static_assert(flow::is_endpoint_task_v<aligned_for_t<WsT, NsT>>);
static_assert(flow::is_endpoint_task_v<wait_wave_t<SetT>>);
static_assert(flow::is_endpoint_task_v<wait_wave_for_t<SetT, NsT>>);

// ---------------------------------------------------------------------------
// 2. Regular tasks are NOT endpoint-tagged; the tag survives cvref.
// ---------------------------------------------------------------------------
static_assert(!flow::is_endpoint_task_v<flow::Task<int>>);
static_assert(!flow::is_endpoint_task_v<flow::Task<void>>);
static_assert(!flow::is_endpoint_task_v<flow::Task<aorta::PullSample>>);
static_assert(flow::is_endpoint_task_v<flow::EndpointTask<int>&&>);
static_assert(flow::is_endpoint_task_v<const flow::EndpointTask<int>&>);

// ---------------------------------------------------------------------------
// 3. Combinator awaiters instantiate for the legal kind combinations (the
//    class-scope B1 static_asserts pass). sizeof forces the class template
//    instantiation, so a regression here fails THIS positive build.
// ---------------------------------------------------------------------------
static_assert(sizeof(flow::detail::timeout_awaiter<int>) > 0);
static_assert(sizeof(flow::detail::race_awaiter<int>) > 0);
static_assert(
    sizeof(flow::detail::join_awaiter<flow::Task<int>, flow::Task<int>>) > 0);
// join accepts endpoint kinds (every branch resolves before the frame
// resumes — no stranded loser). join_awaiter is variadic over the
// TASK types now (three-way mixed-kind instantiation pins that too).
static_assert(sizeof(flow::detail::join_awaiter<flow::EndpointTask<int>,
                                        flow::EndpointTask<int>>) > 0);
static_assert(sizeof(flow::detail::join_awaiter<flow::Task<int>,
                                        flow::EndpointTask<int>,
                                        flow::Task<int>>) > 0);

// ---------------------------------------------------------------------------
// 4. Runtime: endpoint-kind tasks remain first-class for direct co_await
//    and join(). (The real parked verbs are exercised by
//    flow_subscriber_test / flow_readiness_test; here the KIND itself is
//    driven through the Task machinery and the join spine.)
// ---------------------------------------------------------------------------
class CombinatorHarness {
 public:
  CombinatorHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~CombinatorHarness() {
    rt_->pool().close();
    stop_.store(true, std::memory_order_release);
    drainer_.join();
    rt_->pool().drain_all();
  }
  flow::LaneRuntime& rt() { return *rt_; }

 private:
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::atomic<bool> stop_{false};
  std::thread drainer_;
};

flow::EndpointTask<int> endpoint_value(flow::LaneRuntime& rt,
                                       std::chrono::milliseconds d, int v) {
  if (d.count() > 0) {
    co_await flow::sleep(rt, d);
  }
  co_return v;
}

flow::Task<void> await_endpoint_directly(flow::LaneRuntime& rt,
                                         std::atomic<int>& out) {
  int v = co_await endpoint_value(rt, 1ms, 41);
  out.store(v + 1, std::memory_order_seq_cst);
}

TEST(FlowEndpointTag, EndpointTaskIsDirectlyAwaitable) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(h.rt(), scope, await_endpoint_directly(h.rt(), out));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 42)
      << "the endpoint kind plumbs values through co_await unchanged";
}

flow::Task<void> join_endpoints(flow::LaneRuntime& rt, flow::Scope scope,
                                std::atomic<int>& sum) {
  auto [a, b] = co_await flow::join(rt, scope, endpoint_value(rt, 1ms, 1),
                                    endpoint_value(rt, 2ms, 2));
  sum.store(a + b, std::memory_order_seq_cst);
}

TEST(FlowEndpointTag, JoinAcceptsEndpointTasks) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> sum{0};
  auto s = flow::spawn_on(h.rt(), scope, join_endpoints(h.rt(), scope, sum));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (sum.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(scope.close().ok());
  EXPECT_EQ(sum.load(std::memory_order_seq_cst), 3)
      << "join() over endpoint tasks resolves both branches";
}

// ---------------------------------------------------------------------------
// COMPILE-FAIL PROBES (keep commented). Uncommenting any one of these must
// fail this target's build with the endpoint-verb teaching static_assert
// quoted in the header comment. Verified against clang on 2026-07-11.
// ---------------------------------------------------------------------------
// flow::Task<void> probe_timeout_recv(aorta::PullSubscriber& sub,
//                                     flow::LaneRuntime& rt,
//                                     flow::Scope scope) {
//   (void)co_await flow::timeout(rt, 50ms,
//                                flow::recv(sub, rt, scope), scope);
// }
// flow::Task<void> probe_race_wait_wave(flow::ReadinessSet& set,
//                                       flow::LaneRuntime& rt,
//                                       flow::Scope scope) {
//   (void)co_await flow::race(rt, flow::wait_wave(set, rt, scope),
//                             flow::wait_wave(set, rt, scope), scope);
// }
// flow::Task<void> probe_timeout_aligned(flow::WindowSync& ws,
//                                        flow::LaneRuntime& rt,
//                                        flow::Scope scope) {
//   (void)co_await flow::timeout(rt, 50ms,
//                                flow::aligned(ws, rt, scope), scope);
// }
// The AMBIENT forms are rejected identically (the wrapper is an
// EndpointTask; the inner race/timeout awaiter's static_assert fires).
// flow::Task<void> probe_ambient_timeout_recv(aorta::PullSubscriber& sub) {
//   (void)co_await flow::timeout(50ms, flow::recv(sub));
// }
// flow::Task<void> probe_ambient_race_wait_wave(flow::ReadinessSet& set) {
//   (void)co_await flow::race(flow::wait_wave(set), flow::wait_wave(set));
// }

}  // namespace
