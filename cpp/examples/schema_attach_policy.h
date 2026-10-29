#pragma once

#include <string>

#include "aorta/aorta.h"

namespace aorta {
namespace examples {

inline bool ParseSchemaAttachPolicy(const std::string& value,
                                    SchemaAttachPolicy* out_policy) {
  if (value == "never") {
    *out_policy = SchemaAttachPolicy::kNever;
    return true;
  }
  if (value == "always") {
    *out_policy = SchemaAttachPolicy::kAlways;
    return true;
  }
  if (value == "inherit") {
    *out_policy = SchemaAttachPolicy::kInherit;
    return true;
  }
  return false;
}

}  // namespace examples
}  // namespace aorta
