#pragma once

#include <string>

#include "aorta/status.h"

namespace aorta {

// Generates a canonical lowercase hyphenated UUIDv7 string.
StatusOr<std::string> NewUuidV7();

} // namespace aorta
