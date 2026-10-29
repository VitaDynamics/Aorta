#include "aorta/capture.h"

#include "aorta/detail/reentrancy_check.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>
#include <mutex>
#include <string>
#include <vector>

#include "aorta/convenience.h"
#include "aorta/node.h"
#include "internal/common.h"

namespace aorta {
namespace {

std::atomic<uint64_t> g_last_capture_exception_log_ns{0};
constexpr auto kCaptureDestructorWaitTimeout = std::chrono::seconds(2);

std::string_view StrView(const aorta_string_view_t& v) {
  if (v.data == nullptr || v.len == 0) return std::string_view{};
  return std::string_view(reinterpret_cast<const char*>(v.data), v.len);
}

ResolvedSchema CopyResolved(const aorta_resolved_schema_t& out) {
  ResolvedSchema schema;
  if (out.schema_hash.hex != nullptr && out.schema_hash.len > 0) {
    schema.schema_hash_hex.assign(
        reinterpret_cast<const char*>(out.schema_hash.hex), out.schema_hash.len);
  }
  schema.root_type_name = std::string(StrView(out.root_type_name));
  if (out.bfbs.slice_count >= 1 && out.bfbs.slices != nullptr) {
    const auto& sl = out.bfbs.slices[0];
    if (sl.data != nullptr && sl.len > 0) {
      schema.bfbs.assign(sl.data, sl.data + sl.len);
    }
  }
  return schema;
}

}  // namespace

extern "C" void aorta_cpp_on_capture(const aorta_captured_sample_t* sample,
                                     bool is_snapshot_done, int32_t done_reason,
                                     void* user_data) noexcept {
  (void)done_reason;
  try {
    auto* state = static_cast<CaptureSubscriber::CallbackState*>(user_data);
    if (state == nullptr) {
      return;
    }
    // A live CaptureSubscriber never emits SnapshotDone; ignore if it ever does.
    if (is_snapshot_done || sample == nullptr) {
      return;
    }
    // Reserve an in-flight slot under the SAME mutex the destructor uses (see
    // subscriber.cc for the TOCTOU/UAF rationale).
    {
      std::lock_guard<std::mutex> lock(state->in_flight_mutex);
      if (state->shutting_down.load(std::memory_order_acquire)) {
        return;
      }
      state->in_flight.fetch_add(1, std::memory_order_acq_rel);
    }
    struct ScopeGuard {
      CaptureSubscriber::CallbackState* state;
      ~ScopeGuard() noexcept {
        std::lock_guard<std::mutex> lock(state->in_flight_mutex);
        if (state->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          state->in_flight_cv.notify_all();
        }
      }
    } scope_guard{state};

    if (!state->cb) {
      return;
    }
    const aorta_captured_sample_t& s = *sample;
    CapturedSampleView view;
    view.key = StrView(s.key);
    view.key_suffix = StrView(s.key_suffix);
    if (s.payload.slice_count >= 1 && s.payload.slices != nullptr) {
      view.payload = s.payload.slices[0].data;
      view.payload_size = s.payload.slices[0].len;
    }
    if (s.has_schema_hash && s.schema_hash.hex != nullptr && s.schema_hash.len > 0) {
      view.schema_hash_hex = std::string_view(
          reinterpret_cast<const char*>(s.schema_hash.hex), s.schema_hash.len);
    }
    if (s.has_traceparent) {
      TraceParent tp;
      std::memcpy(tp.trace_id.data(), s.traceparent.trace_id, 16);
      tp.span_id = s.traceparent.span_id;
      tp.flags = s.traceparent.flags;
      view.traceparent = tp;
    }
    if (s.has_publish_time) {
      view.publish_time_ns = s.publish_time_ns;
    }
    view.recv_time_ns = s.recv_time_ns;
    view.kind = (s.kind == AORTA_CAPTURE_KIND_SNAPSHOT) ? CaptureKind::kSnapshot
                                                        : CaptureKind::kLive;
    state->cb(view);
  } catch (const FatalError& e) {
    std::fprintf(stderr, "Aorta fatal (from capture callback): %s\n", e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    internal::MaybeLogCallbackException("capture", g_last_capture_exception_log_ns);
  }
}

CaptureSubscriber::~CaptureSubscriber() {
  if (state_) {
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    state_->shutting_down.store(true, std::memory_order_release);
  }
  if (handle_ != 0) {
    // reentrancy: F (spawn-and-detach free — OK from a callback)
    aorta_capture_subscriber_free(handle_);
    handle_ = 0;
  }
  bool drained = true;
  if (state_) {
    AORTA_CHECK_NOT_REENTRANT("CaptureSubscriber::~CaptureSubscriber");
    std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
    drained = state_->in_flight_cv.wait_for(
        lock, kCaptureDestructorWaitTimeout, [state = state_.get()] {
          return state->in_flight.load(std::memory_order_acquire) == 0;
        });
  }
  if (drained) {
    state_.reset();
  } else {
    internal::LogWarnf(
        "aorta_sdk",
        "capture subscriber destructor timed out waiting for in-flight "
        "callbacks; leaking CallbackState to avoid UAF");
    (void)state_.release();
  }
}

SchemaResolver::~SchemaResolver() {
  if (handle_ != 0) {
    aorta_schema_resolver_free(handle_);
    handle_ = 0;
  }
}

StatusOr<ResolvedSchema> SchemaResolver::ResolveByHash(std::string_view hash_hex) {
  aorta_schema_hash_view_t view;
  view.hex = reinterpret_cast<const uint8_t*>(hash_hex.data());
  view.len = hash_hex.size();
  aorta_resolved_schema_t out{};
  AORTA_CHECK_NOT_REENTRANT("SchemaResolver::ResolveByHash");
  aorta_result_t rc = aorta_schema_resolver_resolve_by_hash(handle_, view, &out);
  if (rc != AORTA_OK) {
    return StatusOr<ResolvedSchema>(Status::FromResult(rc));
  }
  ResolvedSchema schema = CopyResolved(out);
  aorta_resolved_schema_free(&out);
  return StatusOr<ResolvedSchema>(std::move(schema));
}

StatusOr<ResolvedSchema> SchemaResolver::ResolveByKey(std::string_view key_suffix) {
  std::string key(key_suffix);  // NUL-terminated for the C-ABI
  aorta_resolved_schema_t out{};
  AORTA_CHECK_NOT_REENTRANT("SchemaResolver::ResolveByKey");
  aorta_result_t rc =
      aorta_schema_resolver_resolve_by_key(handle_, key.c_str(), &out);
  if (rc != AORTA_OK) {
    return StatusOr<ResolvedSchema>(Status::FromResult(rc));
  }
  ResolvedSchema schema = CopyResolved(out);
  aorta_resolved_schema_free(&out);
  return StatusOr<ResolvedSchema>(std::move(schema));
}

Status SchemaResolver::ClearKeyCache() {
  return Status::FromResult(aorta_schema_resolver_clear_key_cache(handle_));
}

StatusOr<std::shared_ptr<CaptureSubscriber>> Node::CreateCaptureSubscriber(
    std::vector<CaptureSelector> selectors,
    CaptureSubscriber::SampleCallback callback) {
  if (selectors.empty()) {
    return StatusOr<std::shared_ptr<CaptureSubscriber>>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "no capture selectors"));
  }
  auto state = std::make_unique<CaptureSubscriber::CallbackState>();
  state->cb = std::move(callback);

  // The C selector array's `pattern` pointers borrow from `selectors`, which
  // outlives the synchronous `_new` call below.
  std::vector<aorta_capture_selector_t> ffi;
  ffi.reserve(selectors.size());
  for (const auto& sel : selectors) {
    int32_t kind = AORTA_CAPTURE_SELECTOR_TOPIC;
    switch (sel.kind) {
      case CaptureSelectorKind::kTopic:
        kind = AORTA_CAPTURE_SELECTOR_TOPIC;
        break;
      case CaptureSelectorKind::kContext:
        kind = AORTA_CAPTURE_SELECTOR_CONTEXT;
        break;
      case CaptureSelectorKind::kTelemetry:
        kind = AORTA_CAPTURE_SELECTOR_TELEMETRY;
        break;
      case CaptureSelectorKind::kRpcRecord:
        kind = AORTA_CAPTURE_SELECTOR_RPC_RECORD;
        break;
      case CaptureSelectorKind::kRawKeyExpr:
        kind = AORTA_CAPTURE_SELECTOR_RAW_KEYEXPR;
        break;
    }
    ffi.push_back(aorta_capture_selector_t{kind, sel.pattern.c_str()});
  }

  aorta_capture_sub_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("Node::CreateCaptureSubscriber");
  aorta_result_t rc = aorta_capture_subscriber_new(
      handle_, ffi.data(), ffi.size(), &aorta_cpp_on_capture, state.get(), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<CaptureSubscriber>>(Status::FromResult(rc));
  }
  return StatusOr<std::shared_ptr<CaptureSubscriber>>(
      std::shared_ptr<CaptureSubscriber>(
          new CaptureSubscriber(handle, std::move(state), shared_from_this())));
}

StatusOr<std::shared_ptr<SchemaResolver>> Node::CreateSchemaResolver(
    const SchemaResolverOptions& options) {
  aorta_schema_resolver_opts_t opts;
  opts.cache_max_entries = options.cache_max_entries;
  opts.by_hash_timeout_ms = options.by_hash_timeout_ms;
  opts.by_key_timeout_ms = options.by_key_timeout_ms;
  aorta_schema_resolver_t handle = 0;
  aorta_result_t rc = aorta_schema_resolver_new(handle_, &opts, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<SchemaResolver>>(Status::FromResult(rc));
  }
  return StatusOr<std::shared_ptr<SchemaResolver>>(std::shared_ptr<SchemaResolver>(
      new SchemaResolver(handle, shared_from_this())));
}

}  // namespace aorta
