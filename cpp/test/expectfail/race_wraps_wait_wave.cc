// Expect-fail TU: race(wait_wave(...), ...) — green characterization of
// the EXISTING B1 teaching gate.
//
// `race()` over a wait_wave() endpoint task strands the loser branch: the
// LOSING side's ReadinessSet slot stays parked (single-awaiter contract)
// with no cancel path until executor stop. The class-scope static_assert
// in race_awaiter (flow_combinators.hpp, ~line 706) must reject this at
// compile time with the teaching text:
//   "use recv_for / aligned_for / wait_wave_for — wrapping a parked
//    endpoint verb in timeout()/race() strands the loser branch"
//
// Mirrors the commented-out probe_race_wait_wave() in
// cpp/test/flow_endpoint_tag_test.cc.
#include <aorta/flow_readiness.hpp>

#include <chrono>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

flow::Task<void> probe_race_wait_wave(flow::ReadinessSet& set,
                                      flow::LaneRuntime& rt,
                                      flow::Scope scope) {
  (void)co_await flow::race(rt, flow::wait_wave(set, rt, scope),
                            flow::wait_wave(set, rt, scope), scope);  // must NOT compile
}
