// Flow Watch — the depth-1 latest-value watch cell, C++ sugar over the
// `aorta_watch_*` C ABI (closes the recorded follow-up in flow_demo.cc).
//
// `Watch::create(node, topic)` declares the Zenoh-fed watch
// (`Node::watch`); `load()` copies the LATEST value out (soft-RT read —
// the load is one buffer copy; zero-copy stays on the pull/sample-handle
// path); `store()` is the foreign-producer write (any thread, no zenoh).
// Header-only C++17 — ships with the BASELINE SDK headers (not the
// C++20 flow target); no coroutines needed.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <aorta.h>

#include "aorta/detail/reentrancy_check.h"
#include "aorta/status.h"

namespace aorta::flow {

/// One loaded watch value: seq pairs with the payload from ONE atomic
/// snapshot (change-detection/dedup safe).
struct WatchValue {
  uint64_t seq = 0;
  std::vector<uint8_t> payload;
};

/// Tri-state load outcome — C++17-baseline local carrier
/// (flow.hpp's Outcome is C++20-only): `ok() && value` = latest value,
/// `ok() && !value` = watch genuinely empty, `!ok()` = freed handle or a
/// real failure.
struct WatchLoadResult {
  aorta_result_t status = AORTA_OK;
  std::optional<WatchValue> value;
  [[nodiscard]] bool ok() const { return status == AORTA_OK; }
};

/// RAII depth-1 latest-value stream (`Node::watch`). Move-only; the watch
/// (and its subscriber) is released on destruction.
class Watch {
 public:
  /// Declare a watch over `topic` on `node` (the C node handle —
  /// `Node::handle()`).
  // Reasoned factory: baseline (C++17) vocabulary — StatusOr,
  // NOT flow::Outcome (this header must not depend on the C++20 target).
  static StatusOr<Watch> create(uint64_t node, const std::string& topic) {
    aorta_watch_t handle = 0;
    AORTA_CHECK_NOT_REENTRANT("Watch::create");
    const aorta_result_t rc = aorta_watch_create(node, topic.c_str(), &handle);
    if (rc != AORTA_OK) {
      return StatusOr<Watch>(Status::FromResult(rc));
    }
    return StatusOr<Watch>(Watch(handle));
  }

  Watch() = default;
  Watch(Watch&& other) noexcept : handle_(std::exchange(other.handle_, 0)) {}
  Watch& operator=(Watch&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, 0);
    }
    return *this;
  }
  Watch(const Watch&) = delete;
  Watch& operator=(const Watch&) = delete;
  ~Watch() { reset(); }

  [[nodiscard]] bool valid() const { return handle_ != 0; }
  [[nodiscard]] aorta_watch_t native_handle() const { return handle_; }

  /// Copy the latest value out (`std::nullopt` while the watch is empty
  /// or after the watch was freed). Sizes the buffer in one retry when
  /// the value outgrew the initial guess. NOTE: this
  /// convenience form collapses empty / freed-handle / error into one
  /// `nullopt` — use `load_checked()` when the distinction matters.
  std::optional<WatchValue> load(size_t size_hint = 256) const {
    WatchLoadResult r = load_checked(size_hint);
    return r.ok() ? std::move(r.value) : std::nullopt;
  }

  /// Tri-state load: `ok() + value` = latest value;
  /// `ok() + nullopt` = watch genuinely empty; error status = freed
  /// handle or a real failure (NOT_FOUND after the owner released it).
  WatchLoadResult load_checked(size_t size_hint = 256) const {
    WatchValue value;
    value.payload.resize(size_hint);
    aorta_result_t last_rc = AORTA_OK;
    for (int attempt = 0; attempt < 2; ++attempt) {
      size_t len = 0;
      uint8_t has = 0;
      const aorta_result_t rc =
          aorta_watch_load(handle_, value.payload.data(), value.payload.size(),
                          &len, &has, &value.seq);
      if (rc == AORTA_OK) {
        if (has == 0) {
          return WatchLoadResult{AORTA_OK, std::nullopt};
        }
        value.payload.resize(len);
        return WatchLoadResult{AORTA_OK,
                               std::optional<WatchValue>(std::move(value))};
      }
      if (rc == AORTA_ERR_BUFFER_TOO_SMALL) {
        value.payload.resize(len);
        last_rc = rc;
        continue;  // one retry with the exact size
      }
      return WatchLoadResult{rc, std::nullopt};
    }
    // The value kept growing between the two calls — report it rather
    // than masking as empty.
    return WatchLoadResult{last_rc, std::nullopt};
  }

  /// Foreign-producer store (any thread; copies; no zenoh publish).
  aorta_result_t store(const uint8_t* data, size_t len) {
    return aorta_watch_store(handle_, data, len);
  }

  /// Total values ever stored (RX + foreign) — observability.
  [[nodiscard]] uint64_t stored_total() const {
    uint64_t total = 0;
    (void)aorta_watch_stored_total(handle_, &total);
    return total;
  }

  void reset() {
    if (handle_ != 0) {
      (void)aorta_watch_free(handle_);
      handle_ = 0;
    }
  }

 private:
  explicit Watch(aorta_watch_t handle) : handle_(handle) {}
  aorta_watch_t handle_{0};
};

}  // namespace aorta::flow
