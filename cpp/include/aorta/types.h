#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace aorta {

struct MessageSlice {
  const uint8_t* data = nullptr;
  std::size_t size = 0;
};

/// Borrowed view into an inbound message payload.
///
/// **Lifetime contract:** All pointer fields (`data`, `slices`, and each
/// slice's `data`) reference FFI-owned memory that is only valid for the
/// duration of the callback that received this view. Capturing a
/// `MessageView` by value into a `std::thread`, `std::async`, a tokio
/// task on the Rust side, a deferred queue, or any structure that
/// outlives the callback **will dangle**. Use `ToVector()` to obtain an
/// owned `std::vector<uint8_t>` copy that survives the callback.
///
/// To make the contract a compile-time error rather than a debug-only
/// runtime crash:
/// - The struct is marked `[[nodiscard]]` so a `MessageView` returned
///   from `ClientResponse::View()` etc. cannot be silently dropped.
/// - Copy construction and copy assignment are deleted. The most
///   common dangling pattern was `[](const MessageView& v, ...) { auto
///   copy = v; bg_thread.post(copy); }` — this now fails to compile;
///   the user must call `v.ToVector()` and capture the vector instead.
///   Returning by value (e.g. `ClientResponse::View()`) is still
///   supported via mandatory copy elision and the defaulted move ops.
struct [[nodiscard]] MessageView {
  const uint8_t* data = nullptr;
  std::size_t size = 0;  // Total payload size in bytes.
  const MessageSlice* slices = nullptr;
  std::size_t slice_count = 0;

  MessageView() = default;
  MessageView(const MessageView&) = delete;
  MessageView& operator=(const MessageView&) = delete;
  MessageView(MessageView&&) = default;
  MessageView& operator=(MessageView&&) = default;

  [[nodiscard]] bool IsContiguous() const { return slice_count <= 1; }

  /// Returns an owned copy of the payload bytes that outlives the
  /// callback. This is the only safe way to retain message data past
  /// the receive callback boundary.
  [[nodiscard]] std::vector<uint8_t> ToVector() const {
    if (size == 0) {
      return {};
    }
    if (slice_count == 0) {
      if (!data) {
        return {};
      }
      return std::vector<uint8_t>(data, data + size);
    }
    if (slice_count == 1) {
      const auto& slice = slices[0];
      if (!slice.data || slice.size == 0) {
        return {};
      }
      return std::vector<uint8_t>(slice.data, slice.data + slice.size);
    }

    std::vector<uint8_t> out;
    out.reserve(size);
    for (std::size_t i = 0; i < slice_count; ++i) {
      const auto& slice = slices[i];
      if (slice.data && slice.size > 0) {
        out.insert(out.end(), slice.data, slice.data + slice.size);
      }
    }
    return out;
  }
};

struct MessageContext {
  std::string topic;
  int64_t receive_stamp_ns = 0;
  int32_t priority = 0;
  std::string publisher_node;
  uint64_t sequence = 0;
};

/// Zero-copy view into message context fields.
///
/// **Lifetime contract:** All `string_view` fields borrow from FFI memory
/// and are only valid during callback execution. Use `ToOwned()` to obtain
/// a `MessageContext` with owned strings if the data must outlive the
/// callback. The struct is `[[nodiscard]]` so a context view returned from
/// an accessor can't be silently dropped; copy construction is deleted to
/// catch the same lambda-capture / background-thread dangling pattern that
/// `MessageView` guards against.
struct [[nodiscard]] MessageContextView {
  std::string_view topic;
  int64_t receive_stamp_ns = 0;
  int32_t priority = 0;
  std::string_view publisher_node;
  uint64_t sequence = 0;

  MessageContextView() = default;
  MessageContextView(const MessageContextView&) = delete;
  MessageContextView& operator=(const MessageContextView&) = delete;
  MessageContextView(MessageContextView&&) = default;
  MessageContextView& operator=(MessageContextView&&) = default;

  [[nodiscard]] MessageContext ToOwned() const {
    return MessageContext{
        std::string(topic),
        receive_stamp_ns,
        priority,
        std::string(publisher_node),
        sequence,
    };
  }
};

}  // namespace aorta
