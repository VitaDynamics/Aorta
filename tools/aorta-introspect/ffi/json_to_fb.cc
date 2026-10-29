#include "json_to_fb.h"

#include <algorithm>
#include <flatbuffers/idl.h>
#include <new>
#include <string>

namespace {
thread_local std::string g_last_error;

void set_error(const std::string& msg) { g_last_error = msg; }

bool deserialize_schema(const uint8_t* bfbs_data,
                        size_t bfbs_size,
                        flatbuffers::Parser* parser) {
  if (!parser->Deserialize(bfbs_data, bfbs_size)) {
    if (parser->error_.empty()) {
      set_error("failed to deserialize BFBS schema");
    } else {
      set_error(parser->error_);
    }
    return false;
  }
  return true;
}

bool apply_root_type(const char* root_type,
                     size_t root_type_size,
                     flatbuffers::Parser* parser) {
  if (root_type == nullptr) {
    if (root_type_size == 0) {
      return true;
    }
    set_error("root_type pointer is null but size is non-zero");
    return false;
  }
  if (root_type_size == 0) {
    set_error("root_type is empty");
    return false;
  }
  const std::string root(root_type, root_type_size);
  if (!parser->SetRootType(root.c_str())) {
    if (parser->error_.empty()) {
      set_error("failed to set root type");
    } else {
      set_error(parser->error_);
    }
    return false;
  }
  return true;
}
}  // namespace

extern "C" int aorta_introspect_json_to_fb(const uint8_t* bfbs_data,
                                            size_t bfbs_size,
                                            const char* json_data,
                                            size_t json_size,
                                            const char* root_type,
                                            size_t root_type_size,
                                            uint8_t** out_fb,
                                            size_t* out_fb_size) {
  if (out_fb == nullptr || out_fb_size == nullptr) {
    set_error("out buffer pointer is null");
    return 1;
  }
  *out_fb = nullptr;
  *out_fb_size = 0;

  if (bfbs_data == nullptr || bfbs_size == 0) {
    set_error("bfbs_data is empty");
    return 1;
  }
  if (json_data == nullptr) {
    set_error("json_data is null");
    return 1;
  }

  flatbuffers::Parser parser;
  if (!deserialize_schema(bfbs_data, bfbs_size, &parser)) {
    return 1;
  }
  if (!apply_root_type(root_type, root_type_size, &parser)) {
    return 1;
  }

  const std::string json(json_data, json_size);
  if (!parser.ParseJson(json.c_str())) {
    if (parser.error_.empty()) {
      set_error("failed to parse JSON against BFBS schema");
    } else {
      set_error(parser.error_);
    }
    return 1;
  }

  const uint8_t* buffer_ptr = parser.builder_.GetBufferPointer();
  const size_t buffer_size = parser.builder_.GetSize();
  uint8_t* out = new (std::nothrow) uint8_t[buffer_size];
  if (out == nullptr && buffer_size > 0) {
    set_error("failed to allocate output buffer");
    return 1;
  }

  if (buffer_size > 0) {
    std::copy(buffer_ptr, buffer_ptr + buffer_size, out);
  }
  *out_fb = out;
  *out_fb_size = buffer_size;
  g_last_error.clear();
  return 0;
}

extern "C" void aorta_introspect_free_bytes(uint8_t* data) { delete[] data; }

extern "C" const char* aorta_introspect_json_to_fb_last_error(void) {
  if (g_last_error.empty()) {
    return "";
  }
  return g_last_error.c_str();
}
