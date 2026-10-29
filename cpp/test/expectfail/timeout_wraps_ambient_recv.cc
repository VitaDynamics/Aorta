// Expect-fail TU: the equivalent ambient endpoint forms — green
// characterization of the EXISTING B1 teaching gate.
//
// Same misuse as timeout_wraps_recv.cc — timeout() wrapping a parked
// recv() — but spelled with the AMBIENT forms of both `timeout` and
// `recv` (no explicit {rt, scope} arguments; both resolve from the frame's
// promise context at await_suspend). The endpoint-kind rejection fires
// identically on the ambient wrapper: `ambient_combinator_awaiter` holds
// the inner `timeout_awaiter<T, K>` BY VALUE, so its class-scope
// static_assert instantiates the same way. Expected teaching text
// (flow_combinators.hpp, ~line 991-994):
//   "use recv_for / aligned_for / wait_wave_for — wrapping a parked
//    endpoint verb in timeout()/race() strands the loser branch"
//
// Mirrors the commented-out probe_ambient_timeout_recv() in
// cpp/test/flow_endpoint_tag_test.cc ("The AMBIENT forms are
// rejected identically").
#include <aorta/flow_subscriber.hpp>

#include <chrono>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

flow::Task<void> probe_ambient_timeout_recv(aorta::PullSubscriber& sub) {
  (void)co_await flow::timeout(50ms, flow::recv(sub));  // must NOT compile
}
