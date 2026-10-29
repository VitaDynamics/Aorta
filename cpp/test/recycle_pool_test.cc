// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for RecyclePool — lock-free buffer recycling via Treiber stack.

#include "internal/recycle_pool.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <thread>
#include <vector>

namespace aorta::internal {
namespace {

// Basic allocate → deallocate → re-allocate round trip.
// The second allocate should return the exact same pointer (pooled).
TEST(RecyclePoolTest, AllocateDeallocateRoundTrip) {
  RecyclePool pool;
  uint8_t* buf = pool.allocate(1024);
  ASSERT_NE(buf, nullptr);
  // Write some data to prove the buffer is usable.
  std::memset(buf, 0xAB, 1024);
  // Return to pool.
  pool.deallocate(buf, 1024);
  // Allocate again — should get the same buffer back (LIFO).
  uint8_t* buf2 = pool.allocate(1024);
  EXPECT_EQ(buf, buf2);
  pool.deallocate(buf2, 1024);
}

// When the pooled buffer is too small for the requested size, allocate() should
// free the pooled buffer and fall back to malloc.  We can't assert pointer
// inequality because malloc may reuse the same address, but we verify the pool
// doesn't crash and the returned buffer is usable at the requested size.
TEST(RecyclePoolTest, AllocateLargerThanPooled) {
  RecyclePool pool;
  uint8_t* buf = pool.allocate(1024);
  pool.deallocate(buf, 1024);
  // Request larger than pooled — pool should discard the 1024 buffer.
  uint8_t* buf2 = pool.allocate(2048);
  ASSERT_NE(buf2, nullptr);
  // Verify the buffer is usable at the full requested size.
  std::memset(buf2, 0xCD, 2048);
  EXPECT_EQ(buf2[2047], 0xCD);
  pool.deallocate(buf2, 2048);
}

// Buffers smaller than kHeaderSize cannot hold intrusive metadata
// and should be freed immediately (not pooled).
TEST(RecyclePoolTest, TinyBufferNotPooled) {
  RecyclePool pool;
  uint8_t* buf = pool.allocate(8);
  ASSERT_NE(buf, nullptr);
  // Deallocate with size < kHeaderSize — should be freed, not pooled.
  pool.deallocate(buf, 8);
  // Next allocate should return a fresh pointer (nothing pooled).
  uint8_t* buf2 = pool.allocate(8);
  ASSERT_NE(buf2, nullptr);
  // Can't guarantee inequality (malloc may reuse), but at least it works.
  pool.deallocate(buf2, 8);
}

// Destructor should free all pooled buffers.
// Under AddressSanitizer this will detect leaks if drain doesn't work.
TEST(RecyclePoolTest, DestructorFreesAllBuffers) {
  {
    RecyclePool pool;
    for (int i = 0; i < 10; ++i) {
      uint8_t* buf = pool.allocate(4096);
      pool.deallocate(buf, 4096);
    }
    // Pool destructor should free everything.
  }
}

// Multiple deallocate calls (stacking) followed by allocate pops LIFO.
TEST(RecyclePoolTest, StackOrdering) {
  RecyclePool pool;
  uint8_t* a = pool.allocate(1024);
  uint8_t* b = pool.allocate(1024);
  // Push both into the pool.
  pool.deallocate(a, 1024);
  pool.deallocate(b, 1024);
  // Pop should return b first (LIFO / Treiber stack).
  uint8_t* first = pool.allocate(1024);
  EXPECT_EQ(first, b);
  uint8_t* second = pool.allocate(1024);
  EXPECT_EQ(second, a);
  pool.deallocate(first, 1024);
  pool.deallocate(second, 1024);
}

// Null deallocate is a no-op.
TEST(RecyclePoolTest, DeallocateNullIsNoop) {
  RecyclePool pool;
  pool.deallocate(nullptr, 1024);  // Should not crash.
}

// Verify FlatBufferBuilder integration — allocate via pool, build, release.
TEST(RecyclePoolTest, FlatBufferBuilderIntegration) {
  auto pool = std::make_shared<RecyclePool>();
  // own_allocator = false — pool lifetime managed by shared_ptr.
  flatbuffers::FlatBufferBuilder builder(256, pool.get(), /*own_allocator=*/false);
  auto str = builder.CreateString("hello world");
  builder.Finish(str);

  EXPECT_GT(builder.GetSize(), 0u);

  // Release transfers buffer to DetachedBuffer.
  // DetachedBuffer's destructor will call pool->deallocate().
  auto detached = builder.Release();
  EXPECT_GT(detached.size(), 0u);
}

// Concurrent deallocate from multiple threads (MPSC pattern).
TEST(RecyclePoolTest, ConcurrentDeallocate) {
  RecyclePool pool;
  constexpr int kThreads = 4;
  constexpr int kBuffersPerThread = 100;

  // Pre-allocate buffers.
  std::vector<uint8_t*> buffers(kThreads * kBuffersPerThread);
  for (auto& buf : buffers) {
    buf = pool.allocate(256);
    ASSERT_NE(buf, nullptr);
  }

  // Concurrently deallocate from multiple threads.
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&pool, &buffers, t]() {
      for (int i = 0; i < kBuffersPerThread; ++i) {
        pool.deallocate(buffers[t * kBuffersPerThread + i], 256);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  // The pool caps its depth at kMaxPoolDepth: at most that many of the
  // concurrently-deallocated buffers were pooled, the rest were freed.
  // Drain via try_pop and assert every pooled pointer is one of the
  // originals and unique — a corrupted free list (lost or duplicated
  // nodes) fails these checks.
  std::set<uint8_t*> originals(buffers.begin(), buffers.end());
  std::set<uint8_t*> popped;
  std::size_t cap = 0;
  while (uint8_t* p = pool.try_pop(&cap)) {
    EXPECT_EQ(cap, 256u);
    EXPECT_EQ(originals.count(p), 1u) << "pool returned a foreign pointer";
    EXPECT_TRUE(popped.insert(p).second) << "pool returned a pointer twice";
    ASSERT_LE(popped.size(), RecyclePool::kMaxPoolDepth)
        << "pool depth exceeded kMaxPoolDepth";
  }
  EXPECT_GT(popped.size(), 0u);
  // Popped buffers are owned by us now; hand them back so the pool
  // destructor frees whatever fits and ASAN checks the rest.
  for (uint8_t* p : popped) {
    pool.deallocate(p, 256);
  }
}

}  // namespace
}  // namespace aorta::internal
