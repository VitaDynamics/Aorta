// Copyright (c) 2025-2026 VitaDynamics
// Lock-free buffer recycling pool implementing flatbuffers::Allocator.
//
// Uses an intrusive Treiber stack: each pooled buffer stores {next_ptr,
// capacity} in its first 16 bytes.
//
// ── Load-bearing concurrency contract ──────────────────────────────────
// Thread safety is strictly MPSC: deallocate() (push) is safe from any
// number of threads concurrently, but ALL pop paths — allocate() and
// try_pop() — MUST be serialized by the caller. In this SDK that is
// enforced by PublisherState::staged_alloc_mutex: the only pop call sites
// are FlatBufferBuilder growth and StagedAllocator::TryStage(), both of
// which run inside that mutex (Prewarm() runs single-threaded at
// construction). An untagged Treiber-stack pop is ABA-UNSOUND under
// concurrent pops — two callers can end up owning the same buffer (exactly
// this defect was previously fixed in the sibling Rust and C++ publisher
// pools). If you ever need to pop outside
// staged_alloc_mutex, replace this structure with a mutex-guarded free
// list (as RawCtxPool in publisher.cc does) instead of relaxing the
// contract.
//
// Behaviorally mirrors the Rust SDK RecyclePool (rust/src/publisher.rs):
// depth cap 8, max-capacity gate, LIFO reuse.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>

#include "flatbuffers/flatbuffers.h"

namespace aorta::internal {

class RecyclePool : public flatbuffers::Allocator {
 public:
  /// Minimum buffer size that can be pooled.  Smaller buffers cannot hold the
  /// intrusive {next_ptr, capacity} header and are freed immediately.
  static constexpr std::size_t kHeaderSize = 16;

  /// Maximum number of buffers to keep in the pool.  Beyond this depth,
  /// returned buffers are freed immediately, preventing unbounded RSS growth
  /// under backpressure.
  static constexpr std::size_t kMaxPoolDepth = 8;

  /// Buffers larger than this are not pooled.
  /// Set via set_max_buffer_capacity(); defaults to SIZE_MAX (no limit).
  /// Prevents occasional oversized messages from permanently inflating RSS.

  RecyclePool() : head_(nullptr), depth_(0), max_buffer_capacity_(SIZE_MAX) {}

  ~RecyclePool() override {
    // Drain all pooled buffers.
    uint8_t* ptr = head_.load(std::memory_order_acquire);
    while (ptr) {
      uint8_t* next = nullptr;
      std::memcpy(&next, ptr, sizeof(next));
      std::free(ptr);
      ptr = next;
    }
  }

  // Non-copyable, non-movable.
  RecyclePool(const RecyclePool&) = delete;
  RecyclePool& operator=(const RecyclePool&) = delete;

  /// Set the maximum buffer capacity that will be pooled.
  /// Buffers larger than this are freed immediately on deallocate().
  void set_max_buffer_capacity(std::size_t cap) {
    max_buffer_capacity_ = cap;
  }

  /// Allocate a buffer of `size` bytes.  Pops from the pool if available and
  /// capacity >= size, otherwise falls back to malloc.
  uint8_t* allocate(std::size_t size) override {
    // Try to pop from the pool.
    while (true) {
      uint8_t* ptr = head_.load(std::memory_order_acquire);
      if (!ptr) break;

      uint8_t* next = nullptr;
      std::size_t cap = 0;
      std::memcpy(&next, ptr, sizeof(next));
      std::memcpy(&cap, ptr + sizeof(next), sizeof(cap));

      if (head_.compare_exchange_weak(ptr, next,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        // Popped successfully.
        depth_.fetch_sub(1, std::memory_order_relaxed);
        if (cap >= size) {
          return ptr;  // Reuse this buffer.
        }
        // Buffer too small — free it and fall through to malloc.
        std::free(ptr);
        break;
      }
      // CAS failed, retry.
    }

    // Allocate fresh.
    auto* p = static_cast<uint8_t*>(std::malloc(size));
    if (!p) throw std::bad_alloc();
    return p;
  }

  /// Non-allocating probe: try to pop a buffer from the pool.
  /// Returns nullptr if the pool is empty. On success, writes the capacity
  /// of the returned buffer into *out_cap.
  /// Used by StagedAllocator to pre-fetch a warm buffer.
  uint8_t* try_pop(std::size_t* out_cap) {
    while (true) {
      uint8_t* ptr = head_.load(std::memory_order_acquire);
      if (!ptr) return nullptr;

      uint8_t* next = nullptr;
      std::size_t cap = 0;
      std::memcpy(&next, ptr, sizeof(next));
      std::memcpy(&cap, ptr + sizeof(next), sizeof(cap));

      if (head_.compare_exchange_weak(ptr, next,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        depth_.fetch_sub(1, std::memory_order_relaxed);
        if (out_cap) *out_cap = cap;
        return ptr;
      }
      // CAS failed, retry.
    }
  }

  /// Return a buffer to the pool for future reuse.
  /// Called from zenoh drop threads (MPSC: multiple producers).
  void deallocate(uint8_t* p, std::size_t size) override {
    if (!p) return;
    if (size < kHeaderSize) {
      // Too small to hold the intrusive header — free immediately.
      std::free(p);
      return;
    }

    // Reject oversized buffers to prevent occasional large messages
    // from permanently inflating RSS.
    if (size > max_buffer_capacity_) {
      std::free(p);
      return;
    }

    // Reserve capacity before publishing to the stack. A concurrent pop may
    // observe the node immediately after publication and decrement depth_.
    std::size_t depth = depth_.load(std::memory_order_relaxed);
    while (true) {
      if (depth >= kMaxPoolDepth) {
        std::free(p);
        return;
      }
      if (depth_.compare_exchange_weak(depth, depth + 1,
                                       std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
        break;
      }
    }

    // Write intrusive header: {next_ptr, capacity}.
    uint8_t* old_head = head_.load(std::memory_order_relaxed);
    while (true) {
      std::memcpy(p, &old_head, sizeof(old_head));
      std::memcpy(p + sizeof(old_head), &size, sizeof(size));

      if (head_.compare_exchange_weak(old_head, p,
                                       std::memory_order_release,
                                       std::memory_order_relaxed)) {
        return;
      }
      // CAS failed, old_head updated by the runtime — retry.
    }
  }

 private:
  std::atomic<uint8_t*> head_;
  std::atomic<std::size_t> depth_;
  std::size_t max_buffer_capacity_;
};

}  // namespace aorta::internal
