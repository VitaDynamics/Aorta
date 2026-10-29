#include "aorta/publisher.h"

#include "aorta/detail/reentrancy_check.h"

#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <algorithm>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "aorta/node.h"
#include "aorta/time.h"
#include "internal/common.h"
#include "internal/ffi.h"
#include "internal/recycle_pool.h"

namespace aorta {
namespace {

constexpr std::size_t kDefaultVarCapacity = 4 * 1024;

uint64_t SteadyNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void RecordMaxRelaxed(std::atomic<uint64_t>& target, uint64_t value) {
  uint64_t current = target.load(std::memory_order_relaxed);
  while (value > current &&
         !target.compare_exchange_weak(current, value,
                                       std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
  }
}

// ── P16 Double-Buffer Allocator ──────────────────────────────────
// Pre-allocates a warm spare buffer at construction time so the
// FlatBufferBuilder always gets a cache-warm buffer, even when Zenoh
// hasn't returned the previous publish's buffer to the RecyclePool yet.
//
// Problem: At 5000Hz with 1MB payloads, Zenoh's transport thread may
// still be serializing the previous buffer when the next publish starts.
// The RecyclePool is empty → malloc() returns cold pages that require
// ~16K LLC misses (1MB / 64B cache line) to fault in from DRAM.
// This caused C++ to have 18.4x more LLC misses than Rust.
//
// Solution: Keep a dedicated spare buffer that is pre-allocated and
// touched (warm in cache/TLB).  On allocate(), return the spare.
// On TryStage(), try to reclaim a buffer from the RecyclePool as
// the new spare.  If the pool is empty, allocate AND warm a fresh
// buffer immediately — paying the warming cost (memset) once rather
// than suffering per-cache-line DRAM latency during the hot publish
// path.
//
// Memory overhead: one extra buffer (~2MB for 1M payload publishers).
// This matches Rust's effective behavior where the TLS builder cache
// always holds a warm Vec<u8> from the previous publish cycle.
class StagedAllocator : public flatbuffers::Allocator {
  public:
   explicit StagedAllocator(internal::RecyclePool* pool) : pool_(pool) {}

   ~StagedAllocator() override {
     if (staged_buf_) {
       std::free(staged_buf_);
       staged_buf_ = nullptr;
     }
   }

   // Non-copyable, non-movable.
   StagedAllocator(const StagedAllocator&) = delete;
   StagedAllocator& operator=(const StagedAllocator&) = delete;

   /// Record the desired capacity for deferred allocation.
   /// The actual malloc+memset is deferred to the first TryStage() call,
   /// so publishers that are declared but never used don't commit RSS.
   void Prewarm(std::size_t capacity) {
     prewarm_capacity_ = capacity;
   }

   uint8_t* allocate(std::size_t size) override {
     // Fast path: use the staged buffer if it fits.
     if (staged_buf_ && staged_cap_ >= size) {
       uint8_t* buf = staged_buf_;
       staged_buf_ = nullptr;
       staged_cap_ = 0;
       return buf;
     }
     // Staged buffer too small — free it.
     if (staged_buf_) {
       std::free(staged_buf_);
       staged_buf_ = nullptr;
       staged_cap_ = 0;
     }
     // Fall through to RecyclePool (which may malloc cold pages).
     uint8_t* p = pool_->allocate(size);
     return p;
   }

   void deallocate(uint8_t* p, std::size_t size) override {
     pool_->deallocate(p, size);
   }

   /// Try to reclaim a buffer from the RecyclePool as the spare.
   /// If the pool returns a buffer that is too small, free it and
   /// fall through to the warm-allocate path.
   /// If the pool is empty (Zenoh hasn't released the previous buffer
   /// yet), allocate AND warm a fresh buffer so the next allocate()
   /// gets cache-warm pages instead of cold malloc pages.
   void TryStage(std::size_t min_capacity) {
     if (staged_buf_) return;  // Already have a spare.

     // Deferred prewarm: on first TryStage, use the prewarm capacity
     // if it's larger than the requested min_capacity.
     std::size_t effective_min = std::max(min_capacity, prewarm_capacity_);
     prewarm_capacity_ = 0;  // Only apply once.

     staged_buf_ = pool_->try_pop(&staged_cap_);
     if (staged_buf_) {
       if (staged_cap_ >= effective_min) {
         ++stage_hit_count_;
         return;
       }
       // Buffer too small — free and fall through to warm-allocate.
       std::free(staged_buf_);
       staged_buf_ = nullptr;
       staged_cap_ = 0;
     }
     // Pool empty or undersized: allocate + warm a fresh buffer.
     staged_cap_ = effective_min;
     staged_buf_ = static_cast<uint8_t*>(std::malloc(staged_cap_));
     if (staged_buf_) {
       std::memset(staged_buf_, 0, staged_cap_);
       ++stage_miss_count_;
     } else {
       staged_cap_ = 0;
       ++stage_miss_count_;
     }
   }

   // Diagnostic counters for TryStage hit/miss.
   uint64_t stage_hit_count() const {
     return stage_hit_count_.load(std::memory_order_relaxed);
   }
   uint64_t stage_miss_count() const {
     return stage_miss_count_.load(std::memory_order_relaxed);
   }

  private:
   internal::RecyclePool* pool_;   // Non-owning.
   uint8_t* staged_buf_{nullptr};  // Cached warm buffer.
   std::size_t staged_cap_{0};     // Capacity of staged buffer.
   std::size_t prewarm_capacity_{0};  // Deferred prewarm size.
   std::atomic<uint64_t> stage_hit_count_{0};
   std::atomic<uint64_t> stage_miss_count_{0};
 };

// ── P2 ReleaseRaw context ────────────────────────────────────────
// Lightweight context for the zero-copy publish release callback.
// Holds the raw buffer pointer and allocated size needed to return the
// memory to the RecyclePool, plus a pool pointer to recycle the context
// struct itself.  This replaces the heavier DetachedOwnedBuffer which
// required shared_ptr<RecyclePool>, weak_ptr<DetachedOwnedBufferPool>,
// and a full DetachedBuffer move.

struct RawPublishCtx;

// Concurrency: fully MPMC. Acquire() runs on publisher threads outside
// staged_alloc_mutex; Release() runs on arbitrary Zenoh callback threads.
// An earlier implementation used an untagged lock-free Treiber stack here,
// which is ABA-unsound under concurrent Acquire() (two publishers can end
// up owning the same ctx). The free
// list is now guarded by its own mutex: the critical section is a couple
// of pointer writes, so an uncontended lock costs the same as the CAS it
// replaces and the pool's safety no longer depends on where callers sit
// relative to other locks.
class RawCtxPool {
 public:
  RawCtxPool() = default;
  ~RawCtxPool();  // Defined after RawPublishCtx.

  RawPublishCtx* Acquire();
  void Release(RawPublishCtx* ctx);

 private:
  std::mutex mutex_;
  RawPublishCtx* head_ = nullptr;  // Intrusive free list, guarded by mutex_.
};

struct RawPublishCtx {
  uint8_t* raw_buf;              // Start of allocated block (from ReleaseRaw)
  std::size_t allocated_bytes;   // Total allocation size (for deallocate)
  // `pool` is shared_ptr: in-flight ctxs must keep the RecyclePool alive so
  // `pool->deallocate(raw_buf, …)` in the release callback remains valid.
  std::shared_ptr<internal::RecyclePool> pool;
  // `ctx_pool` is weak_ptr to break the ownership cycle: PublisherState owns
  // RawCtxPool which owns recycled ctxs which would otherwise own back to
  // RawCtxPool. A weak back-ref means publisher teardown can deterministically
  // destroy RawCtxPool; any release callback that fires after teardown sees
  // `lock() == nullptr` and just deletes the orphaned ctx.
  std::weak_ptr<RawCtxPool> ctx_pool;
  RawPublishCtx* next;           // Intrusive free-list link
};

RawCtxPool::~RawCtxPool() {
  // No lock: destruction requires exclusive ownership by contract
  // (PublisherState holds the last shared_ptr; late release callbacks see
  // an expired weak_ptr and never reach this pool).
  auto* ptr = head_;
  while (ptr != nullptr) {
    auto* next = ptr->next;
    delete ptr;
    ptr = next;
  }
}

RawPublishCtx* RawCtxPool::Acquire() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (head_ != nullptr) {
      auto* ptr = head_;
      head_ = ptr->next;
      ptr->next = nullptr;
      return ptr;
    }
  }
  // Pool empty — allocate outside the lock.
  return new (std::nothrow) RawPublishCtx{};
}

void RawCtxPool::Release(RawPublishCtx* ctx) {
  if (ctx == nullptr) return;
  std::lock_guard<std::mutex> lock(mutex_);
  ctx->next = head_;
  head_ = ctx;
}

// ── Reentrant-publish support ───────────────────────────────────────
// PublishErased holds staged_alloc_mutex across the user's build callback,
// so a same-publisher nested Publish() from inside that callback (e.g. a
// diagnostic publish) previously deadlocked forever on the non-recursive
// mutex. Track which publisher states this thread is currently building
// for; a nested publish detects itself and falls back to an unpooled
// malloc builder (mirroring the Rust SDK's ReentrancyGuard fallback).

thread_local std::vector<const void*> t_publishing_states;

bool IsPublishingOnThisThread(const void* state) {
  return std::find(t_publishing_states.begin(), t_publishing_states.end(),
                   state) != t_publishing_states.end();
}

class PublishingScope {
 public:
  explicit PublishingScope(const void* state) {
    t_publishing_states.push_back(state);
  }
  ~PublishingScope() { t_publishing_states.pop_back(); }
  PublishingScope(const PublishingScope&) = delete;
  PublishingScope& operator=(const PublishingScope&) = delete;
};

// Plain malloc-backed flatbuffers allocator for the reentrant fallback.
// It bypasses StagedAllocator/RecyclePool POPS entirely (pool pops must
// stay serialized under staged_alloc_mutex — see recycle_pool.h's
// concurrency contract) while staying malloc/free-compatible with
// RecyclePool::deallocate, so the detached buffer flows through the SAME
// release path (ReleaseRawPublishCtx -> pool->deallocate) as pooled ones.
class MallocAllocator : public flatbuffers::Allocator {
 public:
  uint8_t* allocate(std::size_t size) override {
    auto* p = static_cast<uint8_t*>(std::malloc(size));
    if (p == nullptr) throw std::bad_alloc();
    return p;
  }
  void deallocate(uint8_t* p, std::size_t) override { std::free(p); }
};

void MaybeWarnReentrantPublish() {
  static std::atomic<uint64_t> last_log_ns{0};
  const uint64_t now_ns = SteadyNowNs();
  uint64_t last = last_log_ns.load(std::memory_order_relaxed);
  if (now_ns - last < internal::LogIntervalNs()) {
    return;
  }
  if (!last_log_ns.compare_exchange_weak(last, now_ns,
                                         std::memory_order_relaxed)) {
    return;
  }
  internal::LogWarnf("aorta_sdk",
                     "reentrant Publish() detected inside a build callback; "
                     "the nested publish uses an unpooled builder");
}

void ReleaseRawPublishCtx(void* ctx, const uint8_t*, std::size_t) {
  auto* rc = static_cast<RawPublishCtx*>(ctx);
  // Hoist the RecyclePool shared_ptr out before touching rc — once we delete
  // or recycle `rc`, its `pool` field is gone.
  auto pool = std::move(rc->pool);
  auto ctx_pool = rc->ctx_pool.lock();
  pool->deallocate(rc->raw_buf, rc->allocated_bytes);
  if (ctx_pool) {
    // Clear the now-empty weak_ptr so a recycled ctx starts fresh.
    rc->ctx_pool.reset();
    ctx_pool->Release(rc);
  } else {
    // Publisher (and therefore RawCtxPool) already torn down — no recycle
    // target survives. The ctx is orphaned; reclaim it directly.
    delete rc;
  }
}

// Matches Rust SDK env var: AORTA_HEADER_INCLUDE_PUBLISHER_NODE.
// Default true; set to "0"/"false"/"no"/"off" to omit publisher_node from
// the serialized AortaHeader, saving ~20-60ns per publish.
bool IncludePublisherNodeInHeader() {
  static const bool value = []() -> bool {
    const char* env = std::getenv("AORTA_HEADER_INCLUDE_PUBLISHER_NODE");
    if (!env || *env == '\0') {
      return true;
    }
    std::string s(env);
    const auto begin = s.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
      return true;
    }
    const auto end = s.find_last_not_of(" \t\r\n");
    s = s.substr(begin, end - begin + 1);
    for (auto& c : s) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return !(s == "0" || s == "false" || s == "no" || s == "off");
  }();
  return value;
}

}  // namespace

struct PublisherState {
  PublisherState(aorta_pub_t pub_handle, std::shared_ptr<Node> pub_node,
                 std::string pub_topic, std::string pub_node_name,
                 const PublisherOptions& options)
      : handle(pub_handle),
        node(std::move(pub_node)),
        topic(std::move(pub_topic)),
        node_name(std::move(pub_node_name)),
        include_publisher_node(IncludePublisherNodeInHeader()),
        base_capacity(options.max_message_size.value_or(0)),
        recycle_pool(std::make_shared<internal::RecyclePool>()),
        staged_alloc(recycle_pool.get()),
        raw_ctx_pool(std::make_shared<RawCtxPool>()),
        sequence(0) {
    if (base_capacity == 0) {
      internal::LogWarnf(
          "aorta_sdk",
          "publisher '%s' created without max_message_size; using default "
          "capacity %zu bytes for variable-size message.",
          topic.c_str(), kDefaultVarCapacity);
      base_capacity = kDefaultVarCapacity;
    }

    current_capacity.store(base_capacity, std::memory_order_relaxed);
    max_capacity_seen.store(base_capacity, std::memory_order_relaxed);

    // Cap the recycle pool: reject buffers larger than 4× base_capacity
    // to prevent occasional oversized messages from inflating RSS.
    recycle_pool->set_max_buffer_capacity(
        base_capacity <= SIZE_MAX / 4 ? base_capacity * 4 : SIZE_MAX);

    // P16: Pre-allocate and warm the spare buffer so the first publish
    // (and all subsequent ones) always get a cache-warm buffer.
    // Note: Prewarm is deferred to the first TryStage() call to avoid
    // committing RSS for publishers that are declared but never used.
    staged_alloc.Prewarm(base_capacity);
  }

  ~PublisherState() {
    if (handle != 0) {
      // reentrancy: F (spawn-and-detach free — OK from a callback)
      aorta_publisher_free(handle);
      handle = 0;
    }
  }

  aorta_pub_t handle{0};
  std::shared_ptr<Node> node;
  std::string topic;
  std::string node_name;
  bool include_publisher_node{true};
  std::size_t base_capacity{0};
  std::shared_ptr<internal::RecyclePool> recycle_pool;
  mutable std::mutex staged_alloc_mutex;
  StagedAllocator staged_alloc;
  std::shared_ptr<RawCtxPool> raw_ctx_pool;

  mutable std::atomic<uint64_t> sequence{0};
  mutable std::atomic<uint64_t> publish_count{0};
  mutable std::atomic<uint64_t> oversize_count{0};
  mutable std::atomic<std::size_t> last_oversize_size{0};
  mutable std::atomic<std::size_t> current_capacity{0};
  mutable std::atomic<std::size_t> max_capacity_seen{0};
  mutable std::atomic<uint64_t> last_publish_ns{0};
  mutable std::atomic<uint64_t> send_error_count{0};
  mutable std::atomic<uint64_t> last_oversize_log_ns{0};
  mutable std::atomic<uint64_t> detached_wrapper_reuse_hit_count{0};
  mutable std::atomic<uint64_t> detached_wrapper_reuse_miss_count{0};
  mutable std::atomic<uint64_t> build_total_ns{0};
  mutable std::atomic<uint64_t> build_max_ns{0};
  mutable std::atomic<uint64_t> build_sample_count{0};
  mutable std::atomic<uint64_t> transport_total_ns{0};
  mutable std::atomic<uint64_t> transport_max_ns{0};
  mutable std::atomic<uint64_t> transport_sample_count{0};
};

Publisher::Publisher(aorta_pub_t handle, std::shared_ptr<Node> node,
                     std::string topic, std::string node_name,
                     const PublisherOptions& options)
    : state_(std::make_shared<PublisherState>(handle, std::move(node),
                                              std::move(topic),
                                              std::move(node_name), options)) {}

Publisher::~Publisher() = default;

const std::string& Publisher::Topic() const { return state_->topic; }

Publisher::Stats Publisher::GetStats() const {
  std::atomic_thread_fence(std::memory_order_acquire);
  Stats out;
  out.publish_count = state_->publish_count.load(std::memory_order_relaxed);
  out.oversize_count = state_->oversize_count.load(std::memory_order_relaxed);
  out.last_oversize_size =
      state_->last_oversize_size.load(std::memory_order_relaxed);
  out.current_capacity =
      state_->current_capacity.load(std::memory_order_relaxed);
  out.max_capacity_seen =
      state_->max_capacity_seen.load(std::memory_order_relaxed);
  out.last_publish_ns =
      state_->last_publish_ns.load(std::memory_order_relaxed);
  out.send_error_count =
      state_->send_error_count.load(std::memory_order_relaxed);
  out.detached_wrapper_reuse_hit_count =
      state_->detached_wrapper_reuse_hit_count.load(std::memory_order_relaxed);
  out.detached_wrapper_reuse_miss_count =
      state_->detached_wrapper_reuse_miss_count.load(std::memory_order_relaxed);
  out.stage_hit_count = state_->staged_alloc.stage_hit_count();
  out.stage_miss_count = state_->staged_alloc.stage_miss_count();
  out.build_total_ns = state_->build_total_ns.load(std::memory_order_relaxed);
  out.build_max_ns = state_->build_max_ns.load(std::memory_order_relaxed);
  out.build_sample_count =
      state_->build_sample_count.load(std::memory_order_relaxed);
  out.transport_total_ns =
      state_->transport_total_ns.load(std::memory_order_relaxed);
  out.transport_max_ns = state_->transport_max_ns.load(std::memory_order_relaxed);
  out.transport_sample_count =
      state_->transport_sample_count.load(std::memory_order_relaxed);
  return out;
}

StatusOr<bool> Publisher::MatchingStatus() const {
  AORTA_CHECK_NOT_REENTRANT("Publisher::MatchingStatus");
  uint8_t matching = 0;
  aorta_result_t rc =
      aorta_publisher_matching_status(state_->handle, &matching);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return matching != 0;
}

StatusOr<bool> Publisher::WaitForMatching(uint64_t timeout_ms) const {
  AORTA_CHECK_NOT_REENTRANT("Publisher::WaitForMatching");
  uint8_t matching = 0;
  aorta_result_t rc =
      aorta_publisher_wait_for_matching(state_->handle, timeout_ms, &matching);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return matching != 0;
}

Status Publisher::PublishBytes(const uint8_t* data, std::size_t len) const {
  aorta_result_t rc = aorta_publisher_publish(state_->handle, data, len);
  state_->publish_count.fetch_add(1, std::memory_order_relaxed);
  RecordTimestamp();
  if (rc != AORTA_OK) {
    state_->send_error_count.fetch_add(1, std::memory_order_relaxed);
  }
  return Status::FromResult(rc);
}

Status Publisher::PublishBytes(const std::vector<uint8_t>& data) const {
  return PublishBytes(data.data(), data.size());
}

Status Publisher::PublishOwned(const uint8_t* data, std::size_t len,
                               ReleaseFunc release_fn,
                               void* release_ctx) const {
  // Order matters: check release_fn first. If it's null the caller
  // already owns cleanup (they couldn't have transferred
  // ownership through us anyway), so there's nothing to release on
  // our side. The data/len check below assumes release_fn is
  // non-null so it can run cleanup on validation-failure paths.
  if (release_fn == nullptr) {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "release_fn must not be null for owned publish");
  }
  if (data == nullptr || len == 0) {
    // PublishOwned takes ownership of the
    // `(data, release_ctx)` pair the moment it is called; the
    // happy path's release pairing happens deep inside the FFI
    // queue drain. On a validation-failure short-circuit we must
    // run `release_fn` ourselves so caller-side cleanup state
    // does not leak. release_fn impls receive the same `(ctx,
    // data, len)` triple they would on the happy-path teardown,
    // where `(data == nullptr, len == 0)` is already a legal
    // "nothing to release" shape.
    release_fn(release_ctx, data, len);
    return Status(AORTA_ERR_INVALID_ARGUMENT, "data is null or empty");
  }

  aorta_owned_bytes_slice_t slice{};
  slice.data = data;
  slice.len = len;
  slice.release = release_fn;
  slice.release_ctx = release_ctx;

  aorta_owned_bytes_view_t view{};
  view.slices = &slice;
  view.slice_count = 1;
  view.total_len = len;

  aorta_result_t rc = aorta_publisher_publish_owned(state_->handle, &view);
  state_->publish_count.fetch_add(1, std::memory_order_relaxed);
  RecordTimestamp();
  if (rc != AORTA_OK) {
    state_->send_error_count.fetch_add(1, std::memory_order_relaxed);
  }
  return Status::FromResult(rc);
}

Status Publisher::Publish(BuildCallback build) const {
  return PublishErased(
      static_cast<void*>(&build),
      [](void* ctx, flatbuffers::FlatBufferBuilder& builder,
         flatbuffers::Offset<aorta::sys::AortaHeader> header) {
        (*static_cast<BuildCallback*>(ctx))(builder, header);
      });
}

Status Publisher::PublishErased(void* ctx, ErasedBuildFn invoke) const {
  state_->publish_count.fetch_add(1, std::memory_order_relaxed);
  uint64_t ts = RecordTimestamp();

  // P11c: Inline header construction — eliminates StatusOr<HeaderValues>
  // wrapper overhead. The only error case (ts==0) essentially never occurs
  // with clock_gettime, so the branch is perfectly predicted.
  if (__builtin_expect(ts == 0, 0)) {
    return Status(AORTA_ERR_INTERNAL, "failed to read node clock");
  }
  std::string_view publisher_node;
  if (state_->include_publisher_node) {
    publisher_node =
        std::string_view(state_->node_name.data(), state_->node_name.size());
  }
  uint64_t sequence = state_->sequence.fetch_add(1, std::memory_order_relaxed);

  const std::size_t capacity = CurrentCapacity();
  std::size_t allocated_bytes = 0;
  std::size_t offset = 0;
  uint8_t* raw_buf = nullptr;
  std::size_t payload_len = 0;

  // Runs header build + user callback + detach on the given builder. The
  // PublishingScope makes a same-publisher nested Publish() inside `invoke`
  // detect itself and take the reentrant branch below instead of
  // deadlocking on staged_alloc_mutex.
  auto run_build = [&](flatbuffers::FlatBufferBuilder& builder) -> Status {
    auto header = BuildAortaHeader(builder, ts, publisher_node, sequence);
    {
      PublishingScope scope(state_.get());
      invoke(ctx, builder, header);
    }

    if (builder.GetSize() == 0) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "builder produced empty payload");
    }

    payload_len = static_cast<std::size_t>(builder.GetSize());
    if (payload_len > capacity) {
      HandleOversize(payload_len, capacity);
    }

    // P2: Use ReleaseRaw to bypass DetachedBuffer/DetachedOwnedBuffer overhead.
    // ReleaseRaw returns the raw allocation pointer; the serialized data starts
    // at (raw_buf + offset) with length (allocated_bytes - offset).
    raw_buf = builder.ReleaseRaw(allocated_bytes, offset);
    return Status::Ok();
  };

  const bool reentrant = IsPublishingOnThisThread(state_.get());
  const uint64_t build_start_ns = SteadyNowNs();
  if (reentrant) {
    // Nested publish on the same publisher: staged_alloc_mutex is already
    // held by this thread's outer publish. Build on an unpooled malloc
    // allocator (free-compatible with RecyclePool::deallocate) so no lock
    // and no pool pop is needed.
    MaybeWarnReentrantPublish();
    MallocAllocator malloc_alloc;
    flatbuffers::FlatBufferBuilder builder(capacity, &malloc_alloc);
    Status build_status = run_build(builder);
    if (!build_status.ok()) {
      return build_status;
    }
  } else {
    std::lock_guard<std::mutex> staged_lock(state_->staged_alloc_mutex);
    // P8: Use StagedAllocator which serves a pre-fetched warm buffer first,
    // falling back to RecyclePool (which may malloc cold pages) on miss.
    flatbuffers::FlatBufferBuilder builder(capacity, &state_->staged_alloc);
    Status build_status = run_build(builder);
    if (!build_status.ok()) {
      return build_status;
    }
    state_->staged_alloc.TryStage(capacity);
  }
  const uint64_t build_ns = SteadyNowNs() - build_start_ns;

  if (raw_buf == nullptr || allocated_bytes < offset) {
    return Status(AORTA_ERR_INTERNAL, "failed to detach publish buffer");
  }

  const std::size_t data_len = allocated_bytes - offset;

  if (data_len == 0) {
    state_->recycle_pool->deallocate(raw_buf, allocated_bytes);
    return Status(AORTA_ERR_INVALID_ARGUMENT, "builder produced empty payload");
  }

  if (payload_len != data_len) {
    state_->recycle_pool->deallocate(raw_buf, allocated_bytes);
    return Status(AORTA_ERR_INTERNAL, "detached publish buffer length mismatch");
  }

  const uint8_t* data = raw_buf + offset;

  state_->build_total_ns.fetch_add(build_ns, std::memory_order_relaxed);
  state_->build_sample_count.fetch_add(1, std::memory_order_relaxed);
  RecordMaxRelaxed(state_->build_max_ns, build_ns);

  auto* rctx = state_->raw_ctx_pool->Acquire();
  if (!rctx) {
    // OOM: fall back to freeing the buffer directly.
    state_->recycle_pool->deallocate(raw_buf, allocated_bytes);
    return Status(AORTA_ERR_INTERNAL,
                  "OOM: failed to allocate RawPublishCtx");
  }
  rctx->raw_buf = raw_buf;
  rctx->allocated_bytes = allocated_bytes;
  rctx->pool = state_->recycle_pool;       // shared_ptr copy: ensures pool outlives callback
  rctx->ctx_pool = state_->raw_ctx_pool;   // weak_ptr observe: breaks the ownership cycle

  aorta_owned_bytes_slice_t slice{};
  slice.data = data;
  slice.len = data_len;
  slice.release = &ReleaseRawPublishCtx;
  slice.release_ctx = rctx;

  aorta_owned_bytes_view_t view{};
  view.slices = &slice;
  view.slice_count = 1;
  view.total_len = data_len;

  const uint64_t transport_start_ns = SteadyNowNs();
  aorta_result_t rc = aorta_publisher_publish_owned(state_->handle, &view);
  const uint64_t transport_ns = SteadyNowNs() - transport_start_ns;
  state_->transport_total_ns.fetch_add(transport_ns, std::memory_order_relaxed);
  state_->transport_sample_count.fetch_add(1, std::memory_order_relaxed);
  RecordMaxRelaxed(state_->transport_max_ns, transport_ns);
  if (rc != AORTA_OK) {
    state_->send_error_count.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::FromResult(rc);
}

Status Publisher::PublishWithBuilder(flatbuffers::FlatBufferBuilder& builder,
                                     BuildCallback build) const {
  state_->publish_count.fetch_add(1, std::memory_order_relaxed);
  uint64_t ts = RecordTimestamp();
  auto capacity_before = CurrentCapacity();
  std::size_t payload_len = 0;
  Status status =
      BuildAndPublish(builder, std::move(build), &payload_len, ts);
  if (payload_len > 0) {
    HandleOversize(payload_len, capacity_before);
  }
  return status;
}

Status Publisher::BuildAndPublish(flatbuffers::FlatBufferBuilder& builder,
                                  BuildCallback build,
                                  std::size_t* payload_len_out,
                                  uint64_t timestamp_ns) const {
  if (!payload_len_out) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "payload_len_out is null");
  }
  auto values_or = MakeHeaderValues(timestamp_ns);
  if (!values_or.ok()) {
    return values_or.status();
  }
  auto header = BuildAortaHeader(builder, values_or.value());
  const uint64_t build_start_ns = SteadyNowNs();
  build(builder, header);

  const auto* data = builder.GetBufferPointer();
  const auto size = static_cast<std::size_t>(builder.GetSize());
  *payload_len_out = size;
  if (size == 0) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "builder produced empty payload");
  }

  const uint64_t build_ns = SteadyNowNs() - build_start_ns;
  state_->build_total_ns.fetch_add(build_ns, std::memory_order_relaxed);
  state_->build_sample_count.fetch_add(1, std::memory_order_relaxed);
  RecordMaxRelaxed(state_->build_max_ns, build_ns);

  const uint64_t transport_start_ns = SteadyNowNs();
  aorta_result_t rc = aorta_publisher_publish(state_->handle, data, size);
  const uint64_t transport_ns = SteadyNowNs() - transport_start_ns;
  state_->transport_total_ns.fetch_add(transport_ns, std::memory_order_relaxed);
  state_->transport_sample_count.fetch_add(1, std::memory_order_relaxed);
  RecordMaxRelaxed(state_->transport_max_ns, transport_ns);
  if (rc != AORTA_OK) {
    state_->send_error_count.fetch_add(1, std::memory_order_relaxed);
  }

  builder.Clear();
  return Status::FromResult(rc);
}

Status Publisher::PublishDetachedBuffer(
    flatbuffers::DetachedBuffer&& detached) const {
  const auto* payload_data = detached.data();
  const auto payload_len = static_cast<std::size_t>(detached.size());
  if (payload_len == 0 || payload_data == nullptr) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "detached payload is empty");
  }

  // Heap-allocate the DetachedBuffer so the release callback can destroy it.
  // This path is not on the hot Publish(BuildCallback) loop (which uses
  // ReleaseRaw), so the allocation overhead is acceptable.
  auto* heap_buf = new (std::nothrow)
      flatbuffers::DetachedBuffer(std::move(detached));
  if (!heap_buf) {
    return Status(AORTA_ERR_INTERNAL,
                  "OOM: failed to allocate DetachedBuffer on heap");
  }

  aorta_owned_bytes_slice_t slice{};
  slice.data = heap_buf->data();
  slice.len = static_cast<std::size_t>(heap_buf->size());
  slice.release = [](void* ctx, const uint8_t*, std::size_t) {
    delete static_cast<flatbuffers::DetachedBuffer*>(ctx);
  };
  slice.release_ctx = heap_buf;

  aorta_owned_bytes_view_t view{};
  view.slices = &slice;
  view.slice_count = 1;
  view.total_len = slice.len;

  aorta_result_t rc = aorta_publisher_publish_owned(state_->handle, &view);
  if (rc != AORTA_OK) {
    state_->send_error_count.fetch_add(1, std::memory_order_relaxed);
  }
  return Status::FromResult(rc);
}

void Publisher::HandleOversize(std::size_t payload_len,
                               std::size_t capacity_before) const {
  if (payload_len <= capacity_before) {
    return;
  }
  state_->oversize_count.fetch_add(1, std::memory_order_relaxed);
  state_->last_oversize_size.store(payload_len, std::memory_order_relaxed);
  MaybeLogOversize(payload_len, capacity_before);
  UpdateCapacityStats(payload_len);
}

void Publisher::UpdateCapacityStats(std::size_t new_capacity) const {
  std::size_t current = state_->current_capacity.load(std::memory_order_relaxed);
  while (new_capacity > current) {
    if (state_->current_capacity.compare_exchange_weak(
            current, new_capacity, std::memory_order_relaxed)) {
      break;
    }
  }

  std::size_t max_seen =
      state_->max_capacity_seen.load(std::memory_order_relaxed);
  while (new_capacity > max_seen) {
    if (state_->max_capacity_seen.compare_exchange_weak(
            max_seen, new_capacity, std::memory_order_relaxed)) {
      break;
    }
  }
}

void Publisher::MaybeLogOversize(std::size_t payload_len,
                                 std::size_t capacity_before) const {
  uint64_t now_ns = internal::NowNs();
  uint64_t last = state_->last_oversize_log_ns.load(std::memory_order_relaxed);
  if (now_ns - last < internal::LogIntervalNs()) {
    return;
  }
  if (!state_->last_oversize_log_ns.compare_exchange_weak(
          last, now_ns, std::memory_order_relaxed)) {
    return;
  }
  internal::LogWarnf(
      "aorta_sdk",
      "publisher '%s' payload %zu exceeded capacity %zu; builder will grow.",
      state_->topic.c_str(), payload_len, capacity_before);
}

StatusOr<HeaderValues> Publisher::MakeHeaderValues() const {
  return MakeHeaderValues(RecordTimestamp());
}

StatusOr<HeaderValues> Publisher::MakeHeaderValues(
    uint64_t timestamp_ns) const {
  if (timestamp_ns == 0) {
    return StatusOr<HeaderValues>(
        Status(AORTA_ERR_INTERNAL, "failed to read node clock"));
  }
  HeaderValues values;
  values.publish_stamp_ns = timestamp_ns;
  if (state_->include_publisher_node) {
    values.publisher_node = state_->node_name;
  }
  values.sequence = state_->sequence.fetch_add(1, std::memory_order_relaxed);
  return StatusOr<HeaderValues>(values);
}

uint64_t Publisher::RecordTimestamp() const {
  uint64_t ns = 0;
  auto now_or = NowNs();
  if (now_or.ok()) {
    ns = static_cast<uint64_t>(now_or.value());
  }
  state_->last_publish_ns.store(ns, std::memory_order_relaxed);
  return ns;
}

std::size_t Publisher::CurrentCapacity() const {
  std::size_t capacity =
      state_->current_capacity.load(std::memory_order_relaxed);
  if (capacity < state_->base_capacity) {
    return state_->base_capacity;
  }
  return capacity;
}

}  // namespace aorta
