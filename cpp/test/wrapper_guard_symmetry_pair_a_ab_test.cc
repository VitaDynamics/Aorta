// Copyright (c) 2025-2026 VitaDynamics
//
// Codegen guard-symmetry probe, Pair A, order
// A-then-B: `guard_action_probe` (includer of `guard_service_probe`) included BEFORE
// `guard_service_probe` (owner) in the same translation unit.
//
// Pre-fix, `guard_service_probe_msg.h` (owner) emits `TypedSubscribeTraits<T>`
// specializations for its local tables UNGUARDED, while
// `guard_action_probe_msg.h` (includer, via `action_common.fbs` ->
// `guard_service_probe.fbs`) re-emits the SAME fully-qualified specializations
// guarded (`#ifndef AORTA_TYPED_WRAPPER_SUBSCRIBE_TRAITS_...`). Co-including
// both headers in either order is a redefinition error pre-fix. This TU
// covers the trait-only conflict class (no shared TABLE_MSG symbol: Msg
// structs are namespaced by each wrapper's OWN root namespace, which differs
// here -- `aorta::action::example` vs `aorta::services::fixture_guard` -- so only the
// SUBSCRIBE_TRAITS specialization, keyed off the table's original namespace,
// collides).

#include "guard_action_probe_msg.h"
#include "guard_service_probe_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPairA, ABOrderCompilesAndSharedTraitResolves) {
  ::aorta::services::fixture_guard::GuardServiceProbeRequestMsg request;
  request.label = "probe";
  ::aorta::action::example::GuardActionProbeGoalMsg goal;
  goal.label = "probe";

  // The fully-qualified specialization is only ever provided by the owner
  // (guard_service_probe's own wrapper) post-fix; pre-fix both wrappers provide
  // it. Either way this call must resolve to a single definition.
  EXPECT_FALSE(TypedSubscribeTraits<::aorta::services::fixture_guard::GuardServiceProbeRequest>::Verify(
      nullptr, 0));
}

// Behavioral sanity check: `Verify(nullptr, 0)`
// above cannot distinguish "the root-local specialization is selected" from
// "every specialization was accidentally deleted and the primary template's
// generic fallback silently took over" -- both paths return false for a
// null/empty buffer, and (codegen does not change Verify semantics) the
// primary template's generic `VerifyBuffer<T>(nullptr)` would ALSO accept a
// well-formed buffer of the correct root type, so this does not by itself
// prove which code path ran. The actual lock on "the root-local
// specialization still exists, unguarded, and still uses the root-specific
// `Verify<Root>Buffer`" is the golden assertions in
// gen_message_wrapper_include_table_test.sh; this test is end-to-end
// behavioral sanity for guard_action_probe's own root type, not a compile-time
// selection proof.
TEST(WrapperGuardSymmetryPairA, RootSpecializationAcceptsAValidBuffer) {
  flatbuffers::FlatBufferBuilder builder;
  auto goal = ::aorta::action::example::CreateGuardActionProbeGoalDirect(builder, "probe-source");
  builder.Finish(goal);

  const uint8_t* data = builder.GetBufferPointer();
  const std::size_t size = builder.GetSize();

  EXPECT_TRUE(
      TypedSubscribeTraits<::aorta::action::example::GuardActionProbeGoal>::Verify(data, size));
  const auto* root =
      TypedSubscribeTraits<::aorta::action::example::GuardActionProbeGoal>::GetRoot(data);
  ASSERT_NE(root, nullptr);
  ASSERT_NE(root->label(), nullptr);
  EXPECT_EQ(root->label()->str(), "probe-source");
}

}  // namespace
}  // namespace aorta
