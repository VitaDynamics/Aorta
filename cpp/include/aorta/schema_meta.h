#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace aorta {

template <typename T>
struct SchemaMeta {
  static const uint8_t* SchemaBfbs() { return nullptr; }
  static std::size_t SchemaBfbsSize() { return 0; }
  static std::optional<std::size_t> FixedSize() { return std::nullopt; }
  static std::optional<std::size_t> ShmCapacityHint() { return std::nullopt; }
  static const char* SchemaHashHex() { return nullptr; }
  static const char* RootTypeName() { return nullptr; }
};

}  // namespace aorta
