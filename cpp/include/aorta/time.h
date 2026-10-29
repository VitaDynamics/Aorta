#pragma once

#include <cstdint>

#include "aorta/status.h"

namespace aorta {

// Current time in nanoseconds from core time source.
//
// Use this API when there is no Node instance available.
StatusOr<int64_t> NowNs();

}  // namespace aorta
