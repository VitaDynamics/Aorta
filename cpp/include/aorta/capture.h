#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "aorta/status.h"
#include "aorta/types.h"

namespace aorta {

class Node;

// ----------------------------------------------------------------------------
// Raw capture — a recorder/observer snoop over any aorta namespace.
// ----------------------------------------------------------------------------

enum class CaptureKind { kLive, kSnapshot };
// NOTE: `SnapshotDoneReason` is intentionally NOT declared here — the ctx
// snapshot reason type already lives in "aorta/contextstore.h" and the C++
// capture wrapper currently exposes only the live `CaptureSubscriber` (the
// `aorta_capture_context_snapshot` C-ABI has no C++ wrapper yet).
enum class CaptureSelectorKind {
  kTopic,
  kContext,
  kTelemetry,
  kRpcRecord,
  kRawKeyExpr,
};

/// What to capture (resolved through the aorta key conventions; wildcards ok).
struct CaptureSelector {
  CaptureSelectorKind kind;
  std::string pattern;
};

/// Propagated trace context carried on a message attachment.
struct TraceParent {
  std::array<uint8_t, 16> trace_id{};
  uint64_t span_id = 0;
  uint8_t flags = 0;
};

/// A single captured message. **All views borrow FFI memory and are valid ONLY
/// during the callback** — copy what you keep (e.g. `PayloadToVector()`).
struct [[nodiscard]] CapturedSampleView {
  std::string_view key;
  std::string_view key_suffix;
  const uint8_t* payload = nullptr;  // contiguous
  std::size_t payload_size = 0;
  /// Opportunistic schema hash (hex). Usually empty (default attach policy is
  /// "never"); resolve by key instead. See `SchemaResolver`.
  std::optional<std::string_view> schema_hash_hex;
  std::optional<TraceParent> traceparent;
  std::optional<uint64_t> publish_time_ns;
  uint64_t recv_time_ns = 0;
  CaptureKind kind = CaptureKind::kLive;

  CapturedSampleView() = default;
  CapturedSampleView(const CapturedSampleView&) = delete;
  CapturedSampleView& operator=(const CapturedSampleView&) = delete;
  CapturedSampleView(CapturedSampleView&&) = default;
  CapturedSampleView& operator=(CapturedSampleView&&) = default;

  /// Owned copy of the payload that outlives the callback.
  [[nodiscard]] std::vector<uint8_t> PayloadToVector() const {
    if (!payload || payload_size == 0) return {};
    return std::vector<uint8_t>(payload, payload + payload_size);
  }
};

// Dispatcher trampoline — `noexcept` (the Rust `extern "C"` caller is nounwind).
extern "C" void aorta_cpp_on_capture(const aorta_captured_sample_t* sample,
                                     bool is_snapshot_done, int32_t done_reason,
                                     void* user_data) noexcept;

/// A live raw-capture subscription. RAII: the destructor drains in-flight
/// callbacks (bounded) then frees, leaking the callback state on timeout to
/// avoid UAF (mirrors `Subscriber`). Move-only.
class CaptureSubscriber {
 public:
  using SampleCallback = std::function<void(const CapturedSampleView&)>;

  ~CaptureSubscriber();
  CaptureSubscriber(const CaptureSubscriber&) = delete;
  CaptureSubscriber& operator=(const CaptureSubscriber&) = delete;

 private:
  friend class Node;
  friend void aorta_cpp_on_capture(const aorta_captured_sample_t*, bool, int32_t,
                                   void*) noexcept;

  struct CallbackState {
    SampleCallback cb;
    std::atomic<uint32_t> in_flight{0};
    std::atomic<bool> shutting_down{false};
    std::mutex in_flight_mutex;
    std::condition_variable in_flight_cv;
  };

  CaptureSubscriber(aorta_capture_sub_t handle,
                    std::unique_ptr<CallbackState> state,
                    std::shared_ptr<Node> node)
      : handle_(handle), state_(std::move(state)), node_(std::move(node)) {}

  aorta_capture_sub_t handle_{0};
  std::unique_ptr<CallbackState> state_;
  std::shared_ptr<Node> node_;
};

// ----------------------------------------------------------------------------
// Schema resolver — schema_hash / key → bfbs + root type.
// ----------------------------------------------------------------------------

/// A resolved schema with owned buffers (outlives the resolver call).
struct ResolvedSchema {
  std::string schema_hash_hex;
  std::string root_type_name;
  std::vector<uint8_t> bfbs;
};

struct SchemaResolverOptions {
  std::size_t cache_max_entries = 256;
  uint32_t by_hash_timeout_ms = 2000;
  uint32_t by_key_timeout_ms = 2000;
};

/// Resolves schemas over the network with content-addressed + key caches.
/// Blocking; resolve lazily off the capture delivery path. Move-only.
class SchemaResolver {
 public:
  ~SchemaResolver();
  SchemaResolver(const SchemaResolver&) = delete;
  SchemaResolver& operator=(const SchemaResolver&) = delete;

  /// Resolve by content hash (hex). Cached forever (content-addressed).
  StatusOr<ResolvedSchema> ResolveByHash(std::string_view hash_hex);
  /// Resolve by topic key suffix (e.g. `pub/camera/front`). Run-scoped cache.
  StatusOr<ResolvedSchema> ResolveByKey(std::string_view key_suffix);

  /// Drop the mutable key→schema cache (e.g. after a known re-registration).
  /// The content-addressed by-hash cache is left intact.
  Status ClearKeyCache();

 private:
  friend class Node;
  SchemaResolver(aorta_schema_resolver_t handle, std::shared_ptr<Node> node)
      : handle_(handle), node_(std::move(node)) {}

  aorta_schema_resolver_t handle_{0};
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
