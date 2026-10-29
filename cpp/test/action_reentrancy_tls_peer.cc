#include "aorta/action.h"

namespace aorta {
namespace detail {

bool PeerSeesActionServerState(ActionServerCallbackStateBase* expected) {
  return CurrentActionServerCallbackState() == expected;
}

}  // namespace detail
}  // namespace aorta
