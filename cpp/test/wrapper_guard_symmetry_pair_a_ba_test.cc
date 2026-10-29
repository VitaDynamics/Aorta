// Copyright (c) 2025-2026 VitaDynamics
//
// Codegen guard-symmetry probe, Pair A, order
// B-then-A: `guard_service_probe` (owner) included BEFORE `guard_action_probe`
// (includer). See wrapper_guard_symmetry_pair_a_ab_test.cc for the full
// rationale -- this file exercises the OTHER include order, since guard
// asymmetry (unguarded owner + guarded includer) is a redefinition in
// EITHER order.

#include "guard_service_probe_msg.h"
#include "guard_action_probe_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPairA, BAOrderCompilesAndSharedTraitResolves) {
  ::aorta::services::fixture_guard::GuardServiceProbeRequestMsg request;
  request.label = "probe";
  ::aorta::action::example::GuardActionProbeGoalMsg goal;
  goal.label = "probe";

  EXPECT_FALSE(TypedSubscribeTraits<::aorta::services::fixture_guard::GuardServiceProbeRequest>::Verify(
      nullptr, 0));
}

}  // namespace
}  // namespace aorta
