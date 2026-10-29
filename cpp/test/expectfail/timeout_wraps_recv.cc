// Expect-fail TU: timeout(recv(...)) — green characterization of the
// EXISTING B1 teaching gate (its diagnostics already fire).
//
// `timeout()` wrapping a PARKED endpoint verb (recv) strands the loser: the
// timed-out recv() keeps the subscriber's single-awaiter slot parked with
// no data and no cancel path until executor stop. The class-scope
// static_assert in timeout_awaiter (flow_combinators.hpp, ~line 991) must
// reject this at compile time with the teaching text:
//   "use recv_for / aligned_for / wait_wave_for — wrapping a parked
//    endpoint verb in timeout()/race() strands the loser branch"
//
// Mirrors the commented-out probe_timeout_recv() in
// cpp/test/flow_endpoint_tag_test.cc (that commented-out probe can be
// deleted now that this real negative TU is wired into CI/Bazel).
#include <aorta/flow_readiness.hpp>
#include <aorta/flow_subscriber.hpp>

#include <chrono>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

flow::Task<void> probe_timeout_recv(aorta::PullSubscriber& sub,
                                    flow::LaneRuntime& rt,
                                    flow::Scope scope) {
  (void)co_await flow::timeout(rt, 50ms,
                               flow::recv(sub, rt, scope), scope);  // must NOT compile
}
