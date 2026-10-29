#pragma once

// Pull-mode (Tier B-pull) subscriber for the QoS & buffering redesign.
//
// A `PullSubscriber` owns a Zenoh-native keep-last ring (declared via
// `aorta_subscriber_new_buffered` with `delivery_mode = InlinePull` in the
// `aorta_sub_buffering_t` companion). Instead of a
// callback on the shared executor, the application drains samples by calling
// `Recv` / `TryRecv` from its own thread — the latest-wins sensor idiom, with
// structural per-topic isolation and no executor hop.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <aorta.h>

#include "aorta/detail/reentrancy_check.h"
#include "aorta/status.h"
#include "aorta/types.h"

namespace aorta {

class Node;

/// RAII owner of a single pulled sample. Move-only: the destructor releases the
/// backing storage via `aorta_pull_sample_free`. The borrowed accessors
/// (`ContiguousData`, `Payload`) are valid only while this object is alive.
class PullSample {
 public:
  PullSample() = default;
  explicit PullSample(aorta_pull_sample_t raw) : raw_(raw), valid_(true) {}

  PullSample(const PullSample&) = delete;
  PullSample& operator=(const PullSample&) = delete;

  PullSample(PullSample&& other) noexcept { MoveFrom(std::move(other)); }
  PullSample& operator=(PullSample&& other) noexcept {
    if (this != &other) {
      Reset();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  ~PullSample() { Reset(); }

  /// Total payload size in bytes.
  [[nodiscard]] std::size_t Size() const { return raw_.payload.total_len; }

  /// Contiguous payload pointer, or `nullptr` if the payload spans multiple
  /// slices (rare; use `ToVector()` for the always-correct copy). Pull samples
  /// produced by a single publish are contiguous, so this is the fast path for
  /// FlatBuffers `GetRoot`.
  [[nodiscard]] const uint8_t* ContiguousData() const {
    if (raw_.payload.slice_count <= 1 && raw_.payload.slices != nullptr) {
      return raw_.payload.slices[0].data;
    }
    return nullptr;
  }

  /// An owned copy of the payload bytes (handles multi-slice payloads).
  [[nodiscard]] std::vector<uint8_t> ToVector() const {
    std::vector<uint8_t> out;
    if (raw_.payload.total_len == 0 || raw_.payload.slices == nullptr) {
      return out;
    }
    out.reserve(raw_.payload.total_len);
    for (std::size_t i = 0; i < raw_.payload.slice_count; ++i) {
      const auto& slice = raw_.payload.slices[i];
      if (slice.data != nullptr && slice.len > 0) {
        out.insert(out.end(), slice.data, slice.data + slice.len);
      }
    }
    return out;
  }

  /// An owned snapshot of the message context.
  [[nodiscard]] MessageContext Context() const {
    MessageContext ctx;
    if (raw_.context.topic.data != nullptr && raw_.context.topic.len > 0) {
      ctx.topic.assign(reinterpret_cast<const char*>(raw_.context.topic.data),
                       raw_.context.topic.len);
    }
    ctx.receive_stamp_ns = raw_.context.receive_stamp_ns;
    ctx.priority = raw_.context.priority;
    if (raw_.context.publisher_node.data != nullptr &&
        raw_.context.publisher_node.len > 0) {
      ctx.publisher_node.assign(
          reinterpret_cast<const char*>(raw_.context.publisher_node.data),
          raw_.context.publisher_node.len);
    }
    ctx.sequence = raw_.context.sequence;
    return ctx;
  }

 private:
  void Reset() {
    if (valid_) {
      aorta_pull_sample_free(&raw_);
      valid_ = false;
    }
  }

  void MoveFrom(PullSample&& other) noexcept {
    raw_ = other.raw_;
    valid_ = other.valid_;
    other.valid_ = false;
  }

  aorta_pull_sample_t raw_{};
  bool valid_ = false;
};

/// Pull-mode subscriber. Move-only RAII over the underlying `aorta_sub_t`.
class PullSubscriber {
 public:
  PullSubscriber() = default;

  PullSubscriber(const PullSubscriber&) = delete;
  PullSubscriber& operator=(const PullSubscriber&) = delete;

  PullSubscriber(PullSubscriber&& other) noexcept { MoveFrom(std::move(other)); }
  PullSubscriber& operator=(PullSubscriber&& other) noexcept {
    if (this != &other) {
      Reset();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  ~PullSubscriber() { Reset(); }

  /// Block until the next sample arrives. Returns the sample, or an error
  /// status if the subscriber has disconnected.
  StatusOr<PullSample> Recv() {
    AORTA_CHECK_NOT_REENTRANT("PullSubscriber::Recv");
    aorta_pull_sample_t raw{};
    aorta_result_t rc = aorta_sub_recv(handle_, &raw, -1);
    if (rc == AORTA_OK) {
      return StatusOr<PullSample>(PullSample(raw));
    }
    return StatusOr<PullSample>(Status::FromResult(rc));
  }

  /// Block up to `timeout`. `std::nullopt` means the timeout elapsed with no
  /// sample; a non-ok status means the subscriber disconnected.
  StatusOr<std::optional<PullSample>> Recv(std::chrono::nanoseconds timeout) {
    return RecvImpl(timeout.count());
  }

  /// Non-blocking. `std::nullopt` means the ring is currently empty.
  StatusOr<std::optional<PullSample>> TryRecv() { return RecvImpl(0); }

  /// The raw C-ABI handle (Phase 4: the flow `recv()` awaiter wires the
  /// data-available notify hook through it).
  aorta_sub_t native_handle() const { return handle_; }

  /// Per-subscriber Tier A shed count (always 0 for pull subscribers, whose
  /// drops happen natively inside the ring). Present for API symmetry.
  [[nodiscard]] std::size_t DroppedCount() const {
    return handle_ == 0 ? 0 : aorta_sub_dropped_count(handle_);
  }

  [[nodiscard]] bool valid() const { return handle_ != 0; }

 private:
  friend class Node;

  PullSubscriber(aorta_sub_t handle, std::shared_ptr<Node> node)
      : handle_(handle), node_(std::move(node)) {}

  StatusOr<std::optional<PullSample>> RecvImpl(int64_t timeout_ns) {
    if (timeout_ns != 0) {
      AORTA_CHECK_NOT_REENTRANT("PullSubscriber::Recv");
    }
    aorta_pull_sample_t raw{};
    aorta_result_t rc = aorta_sub_recv(handle_, &raw, timeout_ns);
    if (rc == AORTA_OK) {
      return StatusOr<std::optional<PullSample>>(
          std::optional<PullSample>(PullSample(raw)));
    }
    if (rc == AORTA_ERR_TIMEOUT) {
      return StatusOr<std::optional<PullSample>>(
          std::optional<PullSample>(std::nullopt));
    }
    return StatusOr<std::optional<PullSample>>(Status::FromResult(rc));
  }

  void Reset() {
    if (handle_ != 0) {
      // reentrancy: F (spawn-and-detach free — OK from a callback)
      aorta_subscriber_free(handle_);
      handle_ = 0;
    }
    node_.reset();
  }

  void MoveFrom(PullSubscriber&& other) noexcept {
    handle_ = other.handle_;
    node_ = std::move(other.node_);
    other.handle_ = 0;
  }

  aorta_sub_t handle_ = 0;
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
