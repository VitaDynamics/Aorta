// Outcome<T>/Stopped POSITIVE compile proof.
//
// Contract under test: Outcome<T> must support move-only,
// non-default-constructible T — a stopped outcome never constructs a T,
// and the value moves out through value()&&.
//
// GRADUATED (cpp/include/aorta/flow.hpp now has the final contract):
// this TU was written against the FINAL API from the start — it
// used to FAIL to compile against the old aggregate {status, T value{}}
// Outcome (no Value/Stop factories: representing a stopped outcome forced
// T's default construction). Now that Outcome is the variant<T, Stopped>
// shape with Value()/Stop() factories, this TU is the POSITIVE proof —
// wired through run_expectfail.sh's gating positive-compile check (moved
// out of Set D / check_red), not the RED section anymore.
#include <utility>

#include "aorta/flow.hpp"

namespace {

struct MoveOnlyNonDefault {
  explicit MoveOnlyNonDefault(int v) : v(v) {}
  MoveOnlyNonDefault(MoveOnlyNonDefault&&) noexcept = default;
  MoveOnlyNonDefault& operator=(MoveOnlyNonDefault&&) noexcept = default;
  MoveOnlyNonDefault(const MoveOnlyNonDefault&) = delete;
  MoveOnlyNonDefault& operator=(const MoveOnlyNonDefault&) = delete;
  int v;
};

// A stopped outcome carries NO T — no default construction anywhere.
aorta::flow::Outcome<MoveOnlyNonDefault> stopped_without_value() {
  return aorta::flow::Outcome<MoveOnlyNonDefault>::Stop(
      aorta::flow::Stopped::Cancelled(aorta::flow::CancelReason::kShutdown));
}

// A value outcome moves the payload out (rvalue accessor).
MoveOnlyNonDefault move_through() {
  auto o = aorta::flow::Outcome<MoveOnlyNonDefault>::Value(
      MoveOnlyNonDefault(7));
  return std::move(o).value();
}

}  // namespace

int main() {
  (void)&stopped_without_value;
  (void)&move_through;
  return 0;
}
