#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "aorta_base_generated.h"

namespace aorta {

struct HeaderValues {
  uint64_t publish_stamp_ns = 0;
  std::string publisher_node;
  uint64_t sequence = 0;
};

inline flatbuffers::Offset<aorta::sys::AortaHeader> BuildAortaHeader(
    flatbuffers::FlatBufferBuilder& builder,
    uint64_t publish_stamp_ns, std::string_view publisher_node,
    uint64_t sequence) {
  flatbuffers::Offset<flatbuffers::String> node;
  if (!publisher_node.empty()) {
    node = builder.CreateString(publisher_node.data(), publisher_node.size());
  }
  return aorta::sys::CreateAortaHeader(builder, publish_stamp_ns, node,
                                       sequence);
}

inline flatbuffers::Offset<aorta::sys::AortaHeader> BuildAortaHeader(
    flatbuffers::FlatBufferBuilder& builder,
    const HeaderValues& values) {
  return BuildAortaHeader(builder, values.publish_stamp_ns,
                          std::string_view(values.publisher_node.data(),
                                           values.publisher_node.size()),
                          values.sequence);
}

}  // namespace aorta
