// Proves two Tier-2 actions can share an enum via a local (same-package)
// include and that BOTH generated headers can be included in one translation
// unit without redefining the shared enum. This mirrors map_asset's
// upload/download sharing MapTransferPhase, and exercises local-include
// resolution through aorta_action_library's expanded-fbs genrule.
#include "aorta/action.h"

#include <string>

#include "gtest/gtest.h"
#include "xfer_download_action_msg.h"
#include "xfer_upload_action_msg.h"

namespace {

using XferUploadAction = ::aorta::action::xfer::XferUploadAction;
using XferDownloadAction = ::aorta::action::xfer::XferDownloadAction;

TEST(SharedEnumAction, BothActionsGenerateAndKeepDistinctNames) {
  EXPECT_EQ(std::string(XferUploadAction::ActionName()), "xfer/upload");
  EXPECT_EQ(std::string(XferDownloadAction::ActionName()), "xfer/download");
}

TEST(SharedEnumAction, SharedEnumIsTheSameTypeAcrossBothActions) {
  // Both feedback msgs reference the one shared enum; assigning a single enum
  // value into each compiles only if it is literally the same type.
  const ::aorta::action::xfer::XferPhase phase =
      ::aorta::action::xfer::XferPhase_TRANSFERRING;
  XferUploadAction::FeedbackDataMsg up;
  up.phase = phase;
  XferDownloadAction::FeedbackDataMsg down;
  down.phase = phase;
  EXPECT_EQ(up.phase, down.phase);
}

}  // namespace
