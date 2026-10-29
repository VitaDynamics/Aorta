// Copyright (c) 2025-2026 VitaDynamics
//
// Codegen guard-symmetry probe, Pair B, order
// B-then-A: `guard_fixture_includer` included BEFORE `guard_fixture_owner`.
// See wrapper_guard_symmetry_pair_b_ab_test.cc for the full rationale.

#include "guard_fixture_includer_msg.h"
#include "guard_fixture_owner_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPairB, BAOrderCompilesAndSharedSymbolsResolve) {
  ::aorta::example::guard_fixture::FixtureLeafMsg leaf;
  leaf.value = 7;

  ::aorta::example::guard_fixture::FixtureUnionMsg payload =
      std::make_shared<::aorta::example::guard_fixture::FixtureLeafMsg>(leaf);
  EXPECT_TRUE(std::holds_alternative<
              std::shared_ptr<::aorta::example::guard_fixture::FixtureLeafMsg>>(
      payload));

  EXPECT_FALSE(
      TypedSubscribeTraits<::aorta::example::guard_fixture::FixtureLeaf>::Verify(
          nullptr, 0));
}

}  // namespace
}  // namespace aorta
