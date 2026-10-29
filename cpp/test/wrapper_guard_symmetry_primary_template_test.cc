// Copyright (c) 2025-2026 VitaDynamics
//
// Codegen guard-symmetry probe: primary-template
// visibility.
//
// Deliberately includes ONLY the `guard_action_probe` wrapper header -- no
// "aorta/node.h", no "aorta/action.h", nothing else that might incidentally
// pull in `aorta/typed_subscribe.h`. Post-fix, `TypedSubscribeTraits`
// specializations are emitted only for ROOT-LOCAL tables
// (gen_message_wrapper.py `subscribe_tables`), so `GuardServiceProbeRequest`
// (an include-table here, owned by `guard_service_probe.fbs`) no longer gets its
// own specialization in this header. Calling
// `TypedSubscribeTraits<GuardServiceProbeRequest>` must still resolve --
// through the PRIMARY TEMPLATE now made visible via the wrapper header's
// `#include "aorta/typed_subscribe.h"` -- rather than hitting an incomplete
// type. cpp/include/aorta/typed_subscribe.h's primary template already has
// a full generic body (`VerifyBuffer<T>(nullptr)` / `GetRoot<T>`), which is
// exactly what the removed specialization did for a non-root type (no
// Verify-semantics delta), so behavior is unchanged.
//
// NOTE: this TU is green both before and after the guard-symmetry fix
// (pre-fix, guard_action_probe's wrapper still emits a guarded specialization
// for GuardServiceProbeRequest, since ALL tables reachable from the include
// closure currently get one; the call resolves via that specialization
// instead of the primary template). It locks in that removing the
// include-table specializations does not regress without pairing it with the
// typed_subscribe.h include -- i.e. it validates a property that could only
// fail if the two changes were applied out of sync with each other.

#include "guard_action_probe_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(WrapperGuardSymmetryPrimaryTemplate, IncludeTableTraitResolvesViaWrapperOnly) {
  EXPECT_FALSE(
      TypedSubscribeTraits<::aorta::services::fixture_guard::GuardServiceProbeRequest>::Verify(
          nullptr, 0));
  EXPECT_EQ(
      TypedSubscribeTraits<::aorta::services::fixture_guard::GuardServiceProbeRequest>::GetRoot(
          nullptr),
      nullptr);
}

}  // namespace
}  // namespace aorta
