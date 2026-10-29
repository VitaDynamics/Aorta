#include "aorta/uuid.h"

#include "internal/ffi.h"

namespace aorta {

StatusOr<std::string> NewUuidV7() {
  char out[37]{};
  const aorta_result_t rc = aorta_uuid_v7(out, sizeof(out));
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return std::string(out);
}

} // namespace aorta
