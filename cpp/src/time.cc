#include "aorta/time.h"

#include "internal/ffi.h"

namespace aorta {

StatusOr<int64_t> NowNs() {
  int64_t out = 0;
  aorta_result_t rc = aorta_now_ns(&out);
  if (rc != AORTA_OK) {
    return StatusOr<int64_t>(Status::FromResult(rc));
  }
  return StatusOr<int64_t>(out);
}

}  // namespace aorta
