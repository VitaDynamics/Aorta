#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "aorta/header.h"
#include "aorta/options.h"
#include "aorta/status.h"
#include "aorta/typed_publish.h"

namespace aorta {

class Node;
namespace internal { class RecyclePool; }
struct PublisherState;

class Publisher {
 public:
  struct Stats {
    uint64_t publish_count = 0;
    uint64_t oversize_count = 0;
    std::size_t last_oversize_size = 0;
    std::size_t current_capacity = 0;
    std::size_t max_capacity_seen = 0;
    uint64_t last_publish_ns = 0;
    uint64_t send_error_count = 0;
    uint64_t detached_wrapper_reuse_hit_count = 0;
    uint64_t detached_wrapper_reuse_miss_count = 0;
    uint64_t stage_hit_count = 0;
    uint64_t stage_miss_count = 0;
    uint64_t build_total_ns = 0;
    uint64_t build_max_ns = 0;
    uint64_t build_sample_count = 0;
    uint64_t transport_total_ns = 0;
    uint64_t transport_max_ns = 0;
    uint64_t transport_sample_count = 0;
  };

  using BuildCallback =
      std::function<void(flatbuffers::FlatBufferBuilder&,
                         flatbuffers::Offset<aorta::sys::AortaHeader>)>;

  ~Publisher();

  Status PublishBytes(const uint8_t* data, std::size_t len) const;
  Status PublishBytes(const std::vector<uint8_t>& data) const;

  /// Publish with a std::function callback (ABI-stable overload).
  Status Publish(BuildCallback build) const;

  /// Publish with a generic callable (avoids std::function overhead).
  /// Preferred for hot-path usage where the callback is a lambda.
  template <typename F,
            typename = std::enable_if_t<
                !std::is_same<std::decay_t<F>, BuildCallback>::value>>
  __attribute__((always_inline))
  Status Publish(F&& build) const {
    using Fn = std::decay_t<F>;
    Fn fn{std::forward<F>(build)};
    return PublishErased(
        static_cast<void*>(&fn),
        [](void* ctx, flatbuffers::FlatBufferBuilder& builder,
           flatbuffers::Offset<aorta::sys::AortaHeader> header) {
          (*static_cast<Fn*>(ctx))(builder, header);
        });
  }

  template <typename T, typename FillFn>
  Status PublishTyped(FillFn&& fill) const {
    static_assert(
        detail::HasTypedPublishTraits<T>::value,
        "TypedPublishTraits<T> specialization is missing. "
        "Include the generated message wrapper header (for example *_msg.h).");
    static_assert(
        TypedPublishTraits<T>::kHasAortaHeader,
        "PublishTyped<T> requires schema root table field 'aorta_header'.");
    typename TypedPublishTraits<T>::Msg msg{};
    std::forward<FillFn>(fill)(msg);
    return PublishTypedMessage<T>(msg);
  }
  template <typename T>
  Status PublishTypedMessage(
      const typename TypedPublishTraits<T>::Msg& msg) const {
    static_assert(
        detail::HasTypedPublishTraits<T>::value,
        "TypedPublishTraits<T> specialization is missing. "
        "Include the generated message wrapper header (for example *_msg.h).");
    static_assert(
        TypedPublishTraits<T>::kHasAortaHeader,
        "PublishTyped<T> requires schema root table field 'aorta_header'.");
    return Publish([&msg](flatbuffers::FlatBufferBuilder& builder,
                          flatbuffers::Offset<aorta::sys::AortaHeader> header) {
      auto root = TypedPublishTraits<T>::Build(builder, header, msg);
      builder.Finish(root);
    });
  }
  /// Publish reusing a caller-owned `FlatBufferBuilder`.
  ///
  /// Use this when amortizing a builder across multiple publishes is more
  /// efficient than constructing a fresh one per call (the default
  /// `Publish(...)` overloads internally use a pooled builder).
  ///
  /// **Ownership.** The `builder` reference must remain valid for the
  /// duration of the call and is mutated in-place: this method writes the
  /// `AortaHeader`, invokes `build(builder, header)`, finishes the
  /// builder, and reads `builder.GetBufferPointer()` before returning.
  /// After the call returns, the caller is free to `builder.Clear()` and
  /// reuse it for the next publish.
  ///
  /// **Threading.** The builder must be used from a single thread —
  /// `flatbuffers::FlatBufferBuilder` is not thread-safe and this method
  /// performs no internal locking around it. Do not share a builder
  /// across concurrent publishes (use a thread-local or per-publisher
  /// builder, or just fall back to the standard `Publish(...)` overloads
  /// which manage builders internally).
  ///
  /// **Pre-call state.** The builder may carry residue from a previous
  /// publish (e.g. an in-progress `Clear()` having not yet been called).
  /// This method writes its header offset assuming the builder is ready
  /// to accept new fields; callers reusing a builder should `Clear()` it
  /// before passing it in.
  Status PublishWithBuilder(flatbuffers::FlatBufferBuilder& builder,
                            BuildCallback build) const;

  /// Zero-copy publish. Transfers ownership of `data` (length `len`) to the
  /// transport layer. `release_fn` is invoked with `release_ctx`, `data`, and
  /// `len` once the payload has been fully sent (or on error). The caller must
  /// not access `data` after this call returns.
  using ReleaseFunc = void (*)(void* ctx, const uint8_t* data, std::size_t len);
  Status PublishOwned(const uint8_t* data, std::size_t len,
                      ReleaseFunc release_fn, void* release_ctx) const;

  const std::string& Topic() const;
  Stats GetStats() const;

  /// One-shot readiness check: true if at least one matching subscriber
  /// currently exists for this publisher's key expression. Reads local routing
  /// tables (no network round-trip). Use to gate the first publish on a fresh
  /// client and avoid the publish-before-route-known drop.
  StatusOr<bool> MatchingStatus() const;

  /// Wait up to `timeout_ms` for a matching subscriber to appear; returns the
  /// final matching state (`false` on timeout, which is not an error — callers
  /// may "fire anyway").
  StatusOr<bool> WaitForMatching(uint64_t timeout_ms) const;

 private:
  friend class Node;

  Publisher(aorta_pub_t handle, std::shared_ptr<Node> node, std::string topic,
            std::string node_name, const PublisherOptions& options);

  /// Type-erased publish implementation.  Both the std::function overload and
  /// the template overload forward here to share a single compiled body.
  using ErasedBuildFn = void (*)(void* ctx,
                                 flatbuffers::FlatBufferBuilder& builder,
                                 flatbuffers::Offset<aorta::sys::AortaHeader>);
  Status PublishErased(void* ctx, ErasedBuildFn invoke) const;

  Status BuildAndPublish(flatbuffers::FlatBufferBuilder& builder,
                         BuildCallback build,
                         std::size_t* payload_len_out,
                         uint64_t timestamp_ns) const;
  Status PublishDetachedBuffer(flatbuffers::DetachedBuffer&& detached) const;
  void HandleOversize(std::size_t payload_len,
                      std::size_t capacity_before) const;
  void UpdateCapacityStats(std::size_t new_capacity) const;
  void MaybeLogOversize(std::size_t payload_len,
                        std::size_t capacity_before) const;
  uint64_t RecordTimestamp() const;
  StatusOr<HeaderValues> MakeHeaderValues() const;
  StatusOr<HeaderValues> MakeHeaderValues(uint64_t timestamp_ns) const;
  std::size_t CurrentCapacity() const;

  std::shared_ptr<PublisherState> state_;
};

}  // namespace aorta
