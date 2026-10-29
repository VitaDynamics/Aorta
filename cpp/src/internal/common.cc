#include "common.h"

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>

#include "aorta/logging.h"

namespace aorta::internal {
namespace {

constexpr uint64_t kDefaultLogIntervalNs = 1'000'000'000;
constexpr const char* kDefaultLogScope = "aorta_sdk";

void aorta_cpp_release_owned_slice(void* user_data, const uint8_t* data,
                                   std::size_t len) {
  (void)data;
  (void)len;
  delete static_cast<std::vector<uint8_t>*>(user_data);
}

const char* ResolveScope(const char* scope) {
  if (scope == nullptr || *scope == '\0') {
    return kDefaultLogScope;
  }
  return scope;
}

std::string FormatLogMessage(const char* format, va_list args) {
  if (format == nullptr || *format == '\0') {
    return std::string();
  }

  va_list count_args;
  va_copy(count_args, args);
  int required = std::vsnprintf(nullptr, 0, format, count_args);
  va_end(count_args);
  if (required < 0) {
    return "log formatting failed";
  }
  if (required == 0) {
    return std::string();
  }

  std::string output(static_cast<std::size_t>(required), '\0');
  std::vsnprintf(output.data(), output.size() + 1, format, args);
  return output;
}

std::string StringFromView(const aorta_string_view_t& view) {
  if (view.data == nullptr || view.len == 0) {
    return {};
  }
  return std::string(reinterpret_cast<const char*>(view.data), view.len);
}

std::string_view StringViewFromView(const aorta_string_view_t& view) {
  if (view.data == nullptr || view.len == 0) {
    return {};
  }
  return std::string_view(reinterpret_cast<const char*>(view.data), view.len);
}

}  // namespace

uint64_t LogIntervalNs() {
  static uint64_t value = []() -> uint64_t {
    const char* env = std::getenv("AORTA_LOG_RATE_LIMIT_MS");
    if (!env || *env == '\0') {
      return kDefaultLogIntervalNs;
    }
    char* end = nullptr;
    long ms = std::strtol(env, &end, 10);
    if (end == env || ms <= 0) {
      return kDefaultLogIntervalNs;
    }
    return static_cast<uint64_t>(ms) * 1'000'000;
  }();
  return value;
}

uint64_t NowNs() {
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::string ResolveGroupFromEnv() {
  const char* env = std::getenv("AORTA_GROUP");
  if (env == nullptr) {
    return "default";
  }
  std::string value(env);
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "default";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

void MaybeLogCallbackException(const char* kind,
                               std::atomic<uint64_t>& last_log_ns) {
  uint64_t now_ns = NowNs();
  uint64_t last = last_log_ns.load(std::memory_order_relaxed);
  if (now_ns - last < LogIntervalNs()) {
    return;
  }
  if (!last_log_ns.compare_exchange_weak(last, now_ns,
                                         std::memory_order_relaxed)) {
    return;
  }
  LogWarnf("aorta_sdk", "exception in %s callback; ignored.", kind);
}

void LogWarn(const char* scope, const std::string& message) {
  if (message.empty()) {
    return;
  }
  AORTA_LOG_SCOPE(WARN, ResolveScope(scope)) << message;
}

void LogWarnf(const char* scope, const char* format, ...) {
  va_list args;
  va_start(args, format);
  std::string message = FormatLogMessage(format, args);
  va_end(args);
  LogWarn(scope, message);
}

MessageContext ToMessageContext(const aorta_msg_context_t* ctx) {
  MessageContext out;
  if (!ctx) {
    return out;
  }
  out.topic = StringFromView(ctx->topic);
  out.receive_stamp_ns = ctx->receive_stamp_ns;
  out.priority = ctx->priority;
  out.publisher_node = StringFromView(ctx->publisher_node);
  out.sequence = ctx->sequence;
  return out;
}

MessageContextView ToMessageContextView(const aorta_msg_context_t* ctx) {
  MessageContextView out;
  if (!ctx) {
    return out;
  }
  out.topic = StringViewFromView(ctx->topic);
  out.receive_stamp_ns = ctx->receive_stamp_ns;
  out.priority = ctx->priority;
  out.publisher_node = StringViewFromView(ctx->publisher_node);
  out.sequence = ctx->sequence;
  return out;
}

MessageView ToMessageView(const aorta_bytes_view_t* payload,
                          MessageSlice* single_slice,
                          std::vector<MessageSlice>* multi_slices) {
  MessageView out;
  if (!payload) {
    return out;
  }

  out.size = payload->total_len;
  if (payload->slice_count == 0 || payload->slices == nullptr) {
    return out;
  }

  const auto* ffi_slices = payload->slices;
  if (payload->slice_count == 1) {
    if (single_slice) {
      *single_slice = MessageSlice{ffi_slices[0].data, ffi_slices[0].len};
      out.slices = single_slice;
      out.slice_count = 1;
      out.data = single_slice->data;
      out.size = single_slice->size;
    } else {
      out.data = ffi_slices[0].data;
      out.size = ffi_slices[0].len;
    }
    return out;
  }

  if (!multi_slices) {
    return out;
  }
  multi_slices->clear();
  multi_slices->reserve(payload->slice_count);
  for (std::size_t i = 0; i < payload->slice_count; ++i) {
    multi_slices->push_back(
        MessageSlice{ffi_slices[i].data, ffi_slices[i].len});
  }
  out.slices = multi_slices->data();
  out.slice_count = multi_slices->size();
  out.data = nullptr;
  out.size = payload->total_len;
  return out;
}

void BuildOwnedBytesView(std::vector<uint8_t>&& payload,
                         aorta_owned_bytes_slice_t* out_slice,
                         aorta_owned_bytes_view_t* out_view) {
  // Heap-allocates a vector control block (~24 bytes) to transfer ownership
  // across the FFI boundary. The payload data itself is moved (pointer swap),
  // so no bulk allocation occurs here. In practice the 24-byte new cannot
  // fail under normal conditions; if it did throw std::bad_alloc, it would
  // propagate to the C++ caller before crossing the FFI boundary.
  auto* owned = new std::vector<uint8_t>(std::move(payload));
  out_slice->data = owned->empty() ? nullptr : owned->data();
  out_slice->len = owned->size();
  out_slice->release = &aorta_cpp_release_owned_slice;
  out_slice->release_ctx = owned;

  out_view->slices = out_slice;
  out_view->slice_count = 1;
  out_view->total_len = out_slice->len;
}

}  // namespace aorta::internal
