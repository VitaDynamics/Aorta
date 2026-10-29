#include <gtest/gtest.h>

#include <stdexcept>

#include "aorta/action.h"

namespace aorta {
namespace detail {

bool PeerSeesActionServerState(ActionServerCallbackStateBase* expected);

namespace {

class TestActionServerState final : public ActionServerCallbackStateBase {
 public:
  void OnExecute(const aorta_bytes_view_t*, aorta_action_context_t) override {}
};

void SetIdentityAndThrow(ActionServerCallbackStateBase* state) {
  ActionServerCallbackIdentityGuard guard(state);
  throw std::runtime_error("test");
}

TEST(ActionReentrancyTlsTest, IdentityIsSharedAcrossTranslationUnits) {
  TestActionServerState state;
  ActionServerCallbackIdentityGuard guard(&state);
  EXPECT_TRUE(PeerSeesActionServerState(&state));
}

TEST(ActionReentrancyTlsTest, NestedGuardRestoresOuterIdentity) {
  TestActionServerState outer;
  TestActionServerState inner;
  ActionServerCallbackIdentityGuard outer_guard(&outer);
  {
    ActionServerCallbackIdentityGuard inner_guard(&inner);
    EXPECT_EQ(CurrentActionServerCallbackState(), &inner);
  }
  EXPECT_EQ(CurrentActionServerCallbackState(), &outer);
}

TEST(ActionReentrancyTlsTest, ThrowingScopeRestoresPreviousIdentity) {
  TestActionServerState outer;
  TestActionServerState inner;
  ActionServerCallbackIdentityGuard outer_guard(&outer);
  EXPECT_THROW(SetIdentityAndThrow(&inner), std::runtime_error);
  EXPECT_EQ(CurrentActionServerCallbackState(), &outer);
}

}  // namespace
}  // namespace detail
}  // namespace aorta
