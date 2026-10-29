// Copyright (c) 2025-2026 VitaDynamics
//
// Codegen guard-symmetry probe, Pair B, order
// A-then-B: `guard_fixture_owner` included BEFORE `guard_fixture_includer`.
//
// No in-tree owner/includer pair currently shares a same-namespace TABLE_MSG
// struct or UNION_MSG variant alias (the owner/includer pair with unions --
// guard_action_probe/guard_service_probe -- lands in different C++
// namespaces, so its union Msg types don't collide). This fixture
// pair (message/schemas/topic/example/guard_fixture_{owner,includer}.fbs,
// SAME namespace `aorta.example.guard_fixture`, includer includes owner)
// reproduces the TABLE_MSG + UNION_MSG + SUBSCRIBE_TRAITS guard-asymmetry
// classes directly: pre-fix, the owner's wrapper emits `FixtureLeafMsg`
// (struct), `FixtureUnionMsg` (variant alias), and
// `TypedSubscribeTraits<FixtureLeaf>` UNGUARDED; the includer's wrapper
// re-emits the identical symbols guarded.

#include "guard_fixture_owner_msg.h"
#include "guard_fixture_includer_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPairB, ABOrderCompilesAndSharedSymbolsResolve) {
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
