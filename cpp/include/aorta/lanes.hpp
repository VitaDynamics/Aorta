// AortaLanes — the C++ plane's dispatch pool (Flow differentiated
// runtime).
//
// Scope-frozen the queue path: fixed lanes, fixed workers, FIFO + EDF
// dual queue with the K-bypass anti-starvation bound, predicate-loop
// parking with the glibc-25847 timed-wait belt, two-phase shutdown with
// a residue drain, per-lane event+byte budgets. NO pause, NO resize, NO
// reset, NO dynamic priorities, NO stealing — additions require a design
// revision plus the stress-test gauntlet.
//
// This layer is C++17 and knows nothing about coroutines: a work item is
// {run, drop, ctx}. Exactly ONE of run/drop executes, exactly once
// (for a coroutine item, drop = resume-into-cancellation — that mapping
// lives in the task layer, not here).
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "aorta/detail/reentrancy_check.h"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace aorta {
namespace lanes {

/// Work item — mirrors the `aorta_delivery` shape minus the
/// core-owned fields this standalone layer doesn't consume yet. The pool
/// owns the item between post() and the run/drop call; `run`/`drop` free
/// whatever `ctx` owns.
struct Work {
  void (*run)(void* ctx) = nullptr;
  void (*drop)(void* ctx) = nullptr;
  void* ctx = nullptr;
  /// Absolute monotonic ns; 0 = no deadline (FIFO item).
  uint64_t deadline_ns = 0;
  /// Payload accounting toward the lane byte budget (0 = header-only).
  uint32_t cost_bytes = 0;
  /// Set on group MEMBERS that hold a lane-budget
  /// reservation (released when the member leaves the group queue).
  uint8_t group_reserved = 0;
};

enum class Post {
  OK = 0,
  /// Lane closed — the CALLER owns delivery of the drop path
  /// (trampoline-class callers push to the residue via post_residue()
  /// instead of running user code in place).
  REFUSED_CLOSED,
  /// Budget full — the item was SHED: its drop ran inline (the shed
  /// accounting thunk is trampoline-class by contract).
  SHED_FULL,
  /// Budget full — the item was NOT consumed (the CALLER owns the
  /// refusal so the executor's overflow policies stay expressible;
  /// only `post_callback_item` returns this). Gauntlet-covered.
  REFUSED_FULL,
};

/// Per-lane OS scheduling policy (applied to WORKER threads only — a
/// no-op in single-threaded mode and on non-Linux hosts).
struct LanePolicy {
#if defined(__linux__)
  int sched_policy = SCHED_OTHER;  // SCHED_FIFO/RR need CAP/rtprio
#else
  int sched_policy = 0;
#endif
  int sched_priority = 0;  // FIFO/RR priority (keep <= 30)
  int nice = 0;            // for SCHED_OTHER lanes
};

struct LaneConfig {
  const char* name = "default";
  LanePolicy policy{};
  /// Event/byte budgets, mirroring the executor admission tier.
  /// 0 = unlimited (tests only; ship bounded).
  size_t max_queued_events = 4096;
  size_t max_queued_bytes = 64u << 20;
  /// FIFO head may be bypassed by deadline items at most K consecutive
  /// times. 0 disables EDF preference entirely.
  uint32_t edf_bypass_limit = 8;
};

struct PoolConfig {
  /// Worker-thread callback-context marks: entered around
  /// every item a WORKER runs so blocking-FFI re-entry classification
  /// behaves exactly as on queue dispatch threads. Dependency-free fn
  /// pointers (the SDK wires them to the aorta C ABI); null = no-op.
  uint8_t (*ctx_enter)() = nullptr;
  void (*ctx_exit)(uint8_t) = nullptr;
  std::vector<LaneConfig> lanes;   // index = class id (0 = highest)
  /// Workers per lane. Empty or all-zero = single-threaded mode: no pool
  /// threads; the owner drains via drain_all()/drain_one().
  std::vector<uint32_t> workers_per_lane;
  /// Cross-lane WRR weights for the ST drainer (the executor's proven
  /// fairness model; strict class order would starve the tail classes).
  std::vector<uint32_t> st_wrr_weights{8, 4, 2, 1};
  /// The glibc-25847 belt: parked workers use a bounded timed wait so a
  /// lost signal (present on glibc < 2.41 — the fleet is 2.35) costs
  /// bounded latency instead of a hang. Not the correctness argument —
  /// the predicate loop with signal-under-mutex is.
  std::chrono::milliseconds park_belt{2};
};

struct LaneStats {
  uint64_t enqueued = 0;
  uint64_t ran = 0;
  uint64_t dropped_closed = 0;   // residue-drained after close
  uint64_t shed_full = 0;        // budget shed at post
  uint64_t dropped_expired = 0;  // deadline expired at dequeue
  uint64_t edf_bypasses = 0;     // FIFO head bypassed by a deadline item
  uint64_t queued_now = 0;
  uint64_t bytes_now = 0;
  uint64_t high_water = 0;  // max observed queued_events (telemetry v6)
};

class LanePool {
 public:
  explicit LanePool(PoolConfig cfg) : cfg_(std::move(cfg)) {
    lanes_.reserve(cfg_.lanes.size());
    for (size_t i = 0; i < cfg_.lanes.size(); i++) {
      lanes_.emplace_back(new Lane(cfg_.lanes[i]));
    }
    const bool mt = [&] {
      for (auto w : cfg_.workers_per_lane)
        if (w > 0) return true;
      return false;
    }();
    if (mt) {
      for (size_t i = 0; i < lanes_.size(); i++) {
        uint32_t n =
            i < cfg_.workers_per_lane.size() ? cfg_.workers_per_lane[i] : 0;
        lanes_[i]->workers = n;
        for (uint32_t w = 0; w < n; w++) {
          workers_.emplace_back([this, i] { worker_loop(*lanes_[i]); });
          apply_policy(workers_.back(), cfg_.lanes[i].policy);
        }
      }
    }
  }

  ~LanePool() {
    close();
    join();
  }

  LanePool(const LanePool&) = delete;
  LanePool& operator=(const LanePool&) = delete;

  size_t lane_count() const { return lanes_.size(); }

  /// True when the calling thread is one of THIS pool's workers — the
  /// self-teardown guard's probe (flow::Runtime: destroying the runtime
  /// on one of its own dispatch threads would self-join in ~LanePool).
  /// `workers_` is stable outside close()/join(), and every legitimate
  /// caller probes strictly BEFORE initiating teardown.
  bool on_worker_thread() const {
    const auto self = std::this_thread::get_id();
    for (const auto& t : workers_) {
      if (t.get_id() == self) {
        return true;
      }
    }
    return false;
  }

  /// Post a work item to a class lane. See `Post` for the outcome
  /// contract; exactly one of run/drop executes exactly once in every
  /// outcome (SHED runs drop inline — trampoline-class by contract;
  /// REFUSED_CLOSED leaves delivery to the caller: user-code drops go to
  /// post_residue(), trivial drops may run in place).
  Post post(size_t lane_idx, Work w) {
    Lane& lane = lane_at(lane_idx);
    bool shed = false;
    {
      std::lock_guard<std::mutex> lk(lane.m);
      if (lane.closed) {
        return Post::REFUSED_CLOSED;
      }
      if ((lane.cfg.max_queued_events &&
           lane.queued_events >= lane.cfg.max_queued_events) ||
          (lane.cfg.max_queued_bytes &&
           lane.queued_bytes + w.cost_bytes > lane.cfg.max_queued_bytes)) {
        lane.stats.shed_full++;
        shed = true;
      } else {
        enqueue_locked(lane, w);
      }
    }
    if (shed) {
      // The shed drop runs OUTSIDE lane.m — a group drain
      // marker's drop delivers member drops and RELOCKS this lane
      // (release_group_member); inline-under-mutex was a recursive-lock
      // deadlock wedging every producer/worker on the lane.
      if (w.drop) w.drop(w.ctx);
      return Post::SHED_FULL;
    }
    void (*hook)(void*) = nullptr;
    void* hook_ctx = nullptr;
    {
      // ST park wake: epoch-bump + signal under the pool mutex (the
      // register-then-recheck twin of the lane-level protocol). The wake
      // hook PAIR is copied under the same mutex — set_wake_hook races a
      // concurrent post otherwise (TSan-caught); the invoke stays
      // outside the lock.
      std::lock_guard<std::mutex> lk(st_m_);
      st_epoch_++;
      st_cv_.notify_one();
      hook = wake_hook_;
      hook_ctx = wake_hook_ctx_;
    }
    if (hook) hook(hook_ctx);
    return Post::OK;
  }

  /// Reserve one event-budget slot on `lane_idx`
  /// for a serial-group MEMBER (members live in the group queue, not
  /// the lane, but must count toward the SAME admission tier). Returns
  /// false when the budget is full or the pool closed.
  bool reserve_group_member(size_t lane_idx, uint32_t cost_bytes = 0) {
    Lane& lane = lane_at(lane_idx);
    std::lock_guard<std::mutex> lk(lane.m);
    if (lane.closed) {
      return false;
    }
    if (lane.cfg.max_queued_events &&
        lane.queued_events + lane.group_reserved_events >=
            lane.cfg.max_queued_events) {
      // A NON-CONSUMING refusal is not a shed — the caller
      // keeps the item and its own drop-accounting fires (counting here
      // double-attributed every refusal).
      return false;
    }
    // Members must count toward BOTH halves of
    // the lane's admission tier — ordinary post() enforces bytes too;
    // counting events alone let large-payload grouped subscriptions
    // grow unbounded outside the byte budget.
    if (lane.cfg.max_queued_bytes &&
        lane.queued_bytes + lane.group_reserved_bytes + cost_bytes >
            lane.cfg.max_queued_bytes) {
      return false;
    }
    lane.group_reserved_events++;
    lane.group_reserved_bytes += cost_bytes;
    return true;
  }

  void release_group_member(size_t lane_idx, uint32_t cost_bytes = 0) {
    Lane& lane = lane_at(lane_idx);
    std::lock_guard<std::mutex> lk(lane.m);
    if (lane.group_reserved_events > 0) {
      lane.group_reserved_events--;
    }
    lane.group_reserved_bytes -=
        cost_bytes <= lane.group_reserved_bytes ? cost_bytes
                                                : lane.group_reserved_bytes;
  }

  /// True once close() began. The stop-arm gate: combinators
  /// must not arm NEW timers against a closing pool — a late timer
  /// completion would land its residue after the final stop-drain.
  bool closed() const { return closed_.load(std::memory_order_acquire); }

  /// Budget-checked post that NEVER consumes the item on refusal
  /// (forwarded C++ callbacks — the caller implements the
  /// caller overflow policy on REFUSED_FULL; REFUSED_CLOSED as ever).
  Post post_callback_item(size_t lane_idx, const Work& w) {
    Lane& lane = lane_at(lane_idx);
    {
      std::lock_guard<std::mutex> lk(lane.m);
      if (lane.closed) {
        return Post::REFUSED_CLOSED;
      }
      if ((lane.cfg.max_queued_events &&
           lane.queued_events >= lane.cfg.max_queued_events) ||
          (lane.cfg.max_queued_bytes &&
           lane.queued_bytes + w.cost_bytes > lane.cfg.max_queued_bytes)) {
        // Non-consuming refusal ≠ shed (see reserve_group_member note).
        return Post::REFUSED_FULL;  // caller-owned: item untouched
      }
      const uint64_t seq = lane.next_seq++;
      if (w.deadline_ns != 0) {
        lane.edf.push(EdfItem{w.deadline_ns, seq, w});
      } else {
        lane.fifo.push_back(FifoItem{seq, w});
      }
      lane.queued_events++;
      if (lane.queued_events > lane.stats.high_water) {
        lane.stats.high_water = lane.queued_events;
      }
      lane.queued_bytes += w.cost_bytes;
      lane.stats.enqueued++;
      lane.cv.notify_one();
    }
    void (*hook)(void*) = nullptr;
    void* hook_ctx = nullptr;
    {
      std::lock_guard<std::mutex> lk(st_m_);
      st_epoch_++;
      st_cv_.notify_one();
      hook = wake_hook_;
      hook_ctx = wake_hook_ctx_;
    }
    if (hook) hook(hook_ctx);
    return Post::OK;
  }

  /// Budget-EXEMPT post for a serial-domain DRAIN MARKER: a
  /// resume-domain marker's drop delivers USER unwinds, so it must never
  /// be shed inline on the producer thread (the same ban
  /// post_resume_item exists for). Never SHED,
  /// never REFUSED_FULL; REFUSED_CLOSED only. Soundness: the wake-bit
  /// protocol queues at most ONE marker per domain; running markers
  /// are bounded by the lane's effective drainers (+1 during the
  /// close/residue overlap), so the marker population is bounded by
  /// live domains × (effective_drainers + 2) worst-case — by
  /// configuration, never by traffic.
  Post post_resume_marker(size_t lane_idx, Work w) {
    return post_resume_item(lane_idx, std::move(w));
  }

  /// Budget-EXEMPT post for task-layer RESUME items (Flow): a coroutine
  /// item's `drop` is resume-into-cancellation (user unwinding), which
  /// must NEVER run inline on the producer thread — the forbidden
  /// pattern that budget shed would trigger. Exemption is safe under the
  /// core's resume contract: at most one outstanding resume per frame,
  /// so the resume population is bounded by live frames, not by traffic.
  /// REFUSED_CLOSED is still possible — the caller routes user drops
  /// through post_residue().
  Post post_resume_item(size_t lane_idx, Work w) {
    Lane& lane = lane_at(lane_idx);
    {
      std::lock_guard<std::mutex> lk(lane.m);
      if (lane.closed) {
        return Post::REFUSED_CLOSED;
      }
      const uint64_t seq = lane.next_seq++;
      if (w.deadline_ns != 0) {
        lane.edf.push(EdfItem{w.deadline_ns, seq, w});
      } else {
        lane.fifo.push_back(FifoItem{seq, w});
      }
      lane.queued_events++;
      if (lane.queued_events > lane.stats.high_water) {
        lane.stats.high_water = lane.queued_events;
      }
      lane.queued_bytes += w.cost_bytes;
      lane.stats.enqueued++;
      lane.cv.notify_one();
    }
    void (*hook)(void*) = nullptr;
    void* hook_ctx = nullptr;
    {
      std::lock_guard<std::mutex> lk(st_m_);
      st_epoch_++;
      st_cv_.notify_one();
      hook = wake_hook_;
      hook_ctx = wake_hook_ctx_;
    }
    if (hook) hook(hook_ctx);
    return Post::OK;
  }

  /// Deliver residue drops NOW on this (draining) thread — the
  /// delivery point for refused user-code drops. Safe from any thread
  /// that is entitled to run user unwinds (the facade's drain hook, the
  /// closer). Returns the number delivered.
  /// Residue drops are USER code (resume-into-cancellation unwinds) —
  /// they carry the callback-context marks exactly as worker drop paths
  /// do, uniformly
  /// for EVERY caller including the facade-less ~LaneRuntime closer.
  size_t drain_residue_now() {
    size_t n = 0;
    for (;;) {
      Work w;
      {
        std::lock_guard<std::mutex> lk(residue_m_);
        if (residue_.empty()) return n;
        w = residue_.front();
        residue_.pop_front();
      }
      if (w.drop) {
        uint8_t prev = 0;
        if (cfg_.ctx_enter) prev = cfg_.ctx_enter();
        w.drop(w.ctx);
        if (cfg_.ctx_exit) cfg_.ctx_exit(prev);
      }
      n++;
    }
  }

  /// Residue backlog (for the drain hook's remaining-work report).
  size_t residue_size() {
    std::lock_guard<std::mutex> lk(residue_m_);
    return residue_.size();
  }

  /// Shutdown-residue intake: trampoline-class callers hand refused
  /// USER-code drops here; the draining thread
  /// (a worker during drain, else the closer) delivers them.
  void post_residue(Work w) {
    std::lock_guard<std::mutex> lk(residue_m_);
    residue_.push_back(w);
  }

  /// Phase 1 of shutdown: bar new posts on every lane (one-way), wake
  /// everyone (notify_all — with wake-one, N-1 parked workers would
  /// otherwise learn of shutdown only via the belt).
  void close() {
    closed_.store(true, std::memory_order_release);
    for (auto& lp : lanes_) {
      std::lock_guard<std::mutex> lk(lp->m);
      lp->closed = true;
      lp->cv.notify_all();
    }
    std::lock_guard<std::mutex> lk(st_m_);
    st_epoch_++;
    st_cv_.notify_all();
  }

  /// Phase 2: drain everything to the drop path and join the workers.
  /// MT: workers drain their lanes as they observe closed; residue and
  /// any post-join stragglers drain on THIS (closer) thread. ST: the
  /// closer drains it all (closer == drainer, the core stop-drain shape).
  void join() {
    for (auto& t : workers_)
      if (t.joinable()) {
        AORTA_CHECK_NOT_REENTRANT("LanePool::join");
        t.join();
      }
    workers_.clear();
    for (auto& lp : lanes_) drain_closed_lane(*lp);
    drain_residue();
  }

  // ------------------------------------------------------------ ST mode
  /// Drain one item across the lanes under the WRR schedule. Returns
  /// false when nothing was runnable. Single-threaded owner only.
  bool drain_one() {
    for (size_t attempts = 0; attempts < lanes_.size(); attempts++) {
      size_t li = wrr_pick();
      if (run_one(*lanes_[li])) return true;
      // Lane empty: ADVANCE the cursor — retrying the same weighted lane
      // burns every attempt on it and strands backlog in later lanes
      // (found by the LaneRuntime post-through test; latent while the
      // earlier cases always had lane-0 traffic).
      wrr_cursor_ = (wrr_cursor_ + 1) % lanes_.size();
      wrr_spent_ = 0;
    }
    return false;
  }

  /// `drain_one`, restricted to lanes with NO dedicated workers
  /// (in MT mode the spin thread
  /// dispatches no callbacks on a lane a worker owns — an arbitrary-
  /// duration realtime-lane callback must not run at spin-thread
  /// scheduling). ST mode has no workers, so this IS drain_one.
  bool drain_one_unworked() {
    for (size_t attempts = 0; attempts < lanes_.size(); attempts++) {
      size_t li = wrr_pick();
      if (lanes_[li]->workers == 0 && run_one(*lanes_[li])) return true;
      wrr_cursor_ = (wrr_cursor_ + 1) % lanes_.size();
      wrr_spent_ = 0;
    }
    return false;
  }

  /// Drain until empty (bounded by `max_items`, 0 = unbounded).
  size_t drain_all(size_t max_items = 0) {
    size_t n = 0;
    while (drain_one()) {
      if (++n == max_items && max_items) break;
    }
    return n;
  }

  /// Block until work is available or `timeout` passes (ST owner's
  /// park). A REAL blocking wait on the pool-level condvar (bench
  /// finding: a polling wait puts the poll period into every cold
  /// wake's latency — 801 µs p50 on s100). post() bumps `st_epoch_` and
  /// signals under the same mutex; the wait is the standard predicate
  /// loop with the glibc-25847 belt as its timeout cadence.
  bool wait_for_work(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lk(st_m_);
    for (;;) {
      const uint64_t seen = st_epoch_;
      lk.unlock();
      if (any_work_or_closed()) {
        return any_work();
      }
      lk.lock();
      if (st_epoch_ != seen) continue;  // missed a post while checking
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) return false;
      st_cv_.wait_for(lk, std::min<std::chrono::milliseconds>(
                              cfg_.park_belt,
                              std::chrono::duration_cast<
                                  std::chrono::milliseconds>(deadline - now) +
                                  std::chrono::milliseconds(1)));
    }
  }

  /// Facade wake hook: invoked after every successful post so
  /// the embedding executor can wake its own primitive (executor Notify).
  void set_wake_hook(void (*hook)(void*), void* ctx) {
    std::lock_guard<std::mutex> lk(st_m_);
    wake_hook_ = hook;
    wake_hook_ctx_ = ctx;
  }

  LaneStats stats(size_t lane_idx) const {
    const Lane& lane = *lanes_[lane_idx];
    std::lock_guard<std::mutex> lk(lane.m);
    LaneStats out = lane.stats;
    out.queued_now = lane.queued_events;
    out.bytes_now = lane.queued_bytes;
    return out;
  }

 private:
  struct FifoItem {
    uint64_t seq;
    Work w;
  };
  struct EdfItem {
    uint64_t deadline_ns;
    uint64_t seq;
    Work w;
    /// Min-heap by (deadline, seq): same-deadline ties keep enqueue
    /// order — one producer's burst can never reorder.
    bool operator>(const EdfItem& o) const {
      if (deadline_ns != o.deadline_ns) return deadline_ns > o.deadline_ns;
      return seq > o.seq;
    }
  };

  struct Lane {
    explicit Lane(const LaneConfig& c) : cfg(c) {}
    const LaneConfig cfg;
    mutable std::mutex m;
    std::condition_variable cv;
    std::deque<FifoItem> fifo;
    std::priority_queue<EdfItem, std::vector<EdfItem>, std::greater<EdfItem>>
        edf;
    uint64_t next_seq = 1;
    size_t queued_events = 0;
    /// Serial-group members holding budget reservations.
    size_t group_reserved_events = 0;
    size_t group_reserved_bytes = 0;
    /// Dedicated workers pinned to this lane (set once at construction).
    uint32_t workers = 0;
    size_t queued_bytes = 0;
    uint32_t fifo_bypass_streak = 0;
    bool closed = false;
    LaneStats stats;
  };

  /// The one out-of-range clamp every lane-index entry point shares.
  Lane& lane_at(size_t lane_idx) {
    return *lanes_[lane_idx < lanes_.size() ? lane_idx : lanes_.size() - 1];
  }

  /// Enqueue under an ALREADY-HELD lane mutex (signal under
  /// the mutex + predicate re-check = no lost wakeups at protocol level).
  static void enqueue_locked(Lane& lane, const Work& w) {
    const uint64_t seq = lane.next_seq++;
    if (w.deadline_ns != 0) {
      lane.edf.push(EdfItem{w.deadline_ns, seq, w});
    } else {
      lane.fifo.push_back(FifoItem{seq, w});
    }
    lane.queued_events++;
    if (lane.queued_events > lane.stats.high_water) {
      lane.stats.high_water = lane.queued_events;
    }
    lane.queued_bytes += w.cost_bytes;
    lane.stats.enqueued++;
    lane.cv.notify_one();
  }


  static uint64_t mono_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  /// THE dequeue order: EDF head first, bounded by
  /// the K-bypass rule protecting the FIFO head; expired items route to
  /// drop with counters. Caller holds lane.m; returns the item to execute
  /// (already un-accounted) or false.
  bool pop_locked(Lane& lane, Work& out, bool& expired) {
    for (;;) {
      const bool have_edf = !lane.edf.empty();
      const bool have_fifo = !lane.fifo.empty();
      if (!have_edf && !have_fifo) return false;

      bool take_edf;
      if (!have_fifo) {
        take_edf = true;
      } else if (!have_edf || lane.cfg.edf_bypass_limit == 0) {
        take_edf = false;
      } else if (lane.fifo_bypass_streak >= lane.cfg.edf_bypass_limit) {
        take_edf = false;  // anti-starvation: FIFO head runs regardless
      } else {
        // EDF preference: urgent-first within the bound. FIFO items
        // whose turn arrives reset the streak.
        take_edf = true;
      }

      if (take_edf) {
        EdfItem it = lane.edf.top();
        lane.edf.pop();
        if (have_fifo) {
          lane.fifo_bypass_streak++;
          lane.stats.edf_bypasses++;
        }
        lane.queued_events--;
        lane.queued_bytes -= it.w.cost_bytes;
        if (it.deadline_ns < mono_ns()) {
          lane.stats.dropped_expired++;
          out = it.w;
          expired = true;
          return true;
        }
        out = it.w;
        expired = false;
        return true;
      }
      FifoItem it = lane.fifo.front();
      lane.fifo.pop_front();
      lane.fifo_bypass_streak = 0;
      lane.queued_events--;
      lane.queued_bytes -= it.w.cost_bytes;
      out = it.w;
      expired = false;
      return true;
    }
  }

  /// Run one item from `lane` (any mode). Returns false if none.
  /// Once the lane is CLOSED, items drain to DROP here too (shutdown
  /// phase 2; worker_loop alone was half the fix: the ST
  /// drain path also runs between a foreign-thread close() and the
  /// spin thread observing stop).
  bool run_one(Lane& lane) {
    Work w;
    bool expired = false;
    bool draining = false;
    {
      std::lock_guard<std::mutex> lk(lane.m);
      if (!pop_locked(lane, w, expired)) return false;
      draining = lane.closed;
    }
    if (expired || draining) {
      if (w.drop) w.drop(w.ctx);
      if (draining && !expired) {
        std::lock_guard<std::mutex> lk(lane.m);
        lane.stats.dropped_closed++;
      }
    } else {
      if (w.run) w.run(w.ctx);
      std::lock_guard<std::mutex> lk(lane.m);
      lane.stats.ran++;
    }
    return true;
  }

  void worker_loop(Lane& lane) {
    std::unique_lock<std::mutex> lk(lane.m);
    for (;;) {
      // Predicate loop: signal-under-mutex + re-check = no lost wakeup
      // at protocol level; the timed wait is the glibc-25847 belt only.
      lane.cv.wait_for(lk, cfg_.park_belt, [&] {
        return lane.queued_events > 0 || lane.closed;
      });
      Work w;
      bool expired = false;
      if (pop_locked(lane, w, expired)) {
        // Shutdown phase 2: once the lane is CLOSED,
        // queued items drain to DROP — running them would extend user
        // work past close() and drift from the core stop-drain contract.
        const bool draining = lane.closed;
        lk.unlock();
        if (expired || draining) {
          if (w.drop) {
            // Resume DROPS run user code
            // (resume-into-cancellation) — workers carry the marks
            // on this path too, so re-entry classification matches the
            // ST stop-drain exactly.
            uint8_t prev = 0;
            if (cfg_.ctx_enter) prev = cfg_.ctx_enter();
            w.drop(w.ctx);
            if (cfg_.ctx_exit) cfg_.ctx_exit(prev);
          }
        } else if (w.run) {
          // Workers carry the callback-context marks so re-entry
          // classification matches queue dispatch threads exactly.
          uint8_t prev = 0;
          if (cfg_.ctx_enter) prev = cfg_.ctx_enter();
          w.run(w.ctx);
          if (cfg_.ctx_exit) cfg_.ctx_exit(prev);
        }
        lk.lock();
        if (draining && !expired) {
          lane.stats.dropped_closed++;
        } else if (!expired) {
          lane.stats.ran++;
        }
        continue;
      }
      if (lane.closed) {
        // Lane empty + closed: this worker's drain duty is done.
        return;
      }
    }
  }

  /// After workers joined (or in ST mode): drain what's left to drop —
  /// the draining thread is THIS thread.
  void drain_closed_lane(Lane& lane) {
    for (;;) {
      Work w;
      bool expired = false;
      {
        std::lock_guard<std::mutex> lk(lane.m);
        if (!pop_locked(lane, w, expired)) return;
        lane.stats.dropped_closed++;
      }
      if (w.drop) w.drop(w.ctx);
    }
  }

  void drain_residue() {
    // One delivery path — the public marked variant (DRY).
    (void)drain_residue_now();
  }

  bool any_work() const { return scan_lanes(false); }
  bool any_work_or_closed() const { return scan_lanes(true); }

  /// The one lane scan behind any_work/any_work_or_closed.
  bool scan_lanes(bool count_closed) const {
    for (auto& lp : lanes_) {
      std::lock_guard<std::mutex> lk(lp->m);
      if (lp->queued_events > 0 || (count_closed && lp->closed)) return true;
    }
    return false;
  }

  size_t wrr_pick() {
    // The executor's proven WRR shape: classes drain at weights [8,4,2,1] per round.
    for (size_t scan = 0; scan < 2 * lanes_.size(); scan++) {
      size_t li = wrr_cursor_ % lanes_.size();
      uint32_t weight = li < cfg_.st_wrr_weights.size()
                            ? cfg_.st_wrr_weights[li]
                            : 1;
      if (wrr_spent_ < weight) {
        wrr_spent_++;
        return li;
      }
      wrr_cursor_ = (wrr_cursor_ + 1) % lanes_.size();
      wrr_spent_ = 0;
    }
    wrr_cursor_ = 0;
    wrr_spent_ = 1;
    return 0;
  }

  static void apply_policy(std::thread& t, const LanePolicy& p) {
#if defined(__linux__)
    if (p.sched_policy != SCHED_OTHER) {
      sched_param sp{};
      sp.sched_priority = p.sched_priority;
      // Best-effort: EPERM without rtprio/CAP_SYS_NICE is expected on
      // non-root deployments (the fleet runs as root). One-shot
      // diagnostics belong to the embedding layer.
      (void)pthread_setschedparam(t.native_handle(), p.sched_policy, &sp);
    }
#else
    (void)t;
    (void)p;  // policy is a documented no-op off Linux
#endif
  }

  PoolConfig cfg_;
  std::vector<std::unique_ptr<Lane>> lanes_;
  std::atomic<bool> closed_{false};
  std::vector<std::thread> workers_;
  std::mutex residue_m_;
  std::deque<Work> residue_;
  void (*wake_hook_)(void*) = nullptr;
  void* wake_hook_ctx_ = nullptr;
  // Pool-level ST park (real blocking wait; bench finding).
  std::mutex st_m_;
  std::condition_variable st_cv_;
  uint64_t st_epoch_ = 0;
  // ST WRR state (single drainer by contract; not thread-safe by design).
  size_t wrr_cursor_ = 0;
  uint32_t wrr_spent_ = 0;
};

/// SerialDomain occupancy mark: the SerialGroup whose drain slice
/// is executing on THIS thread (nullptr outside any slice). Diagnostics
/// only — the flow tier's canary and misuse diagnostics read it; never
/// enforcement. RAII save-and-restore, NOT set/clear: a nested delivery
/// (an inline resume inside a slice) must not corrupt the outer
/// mark.
inline thread_local const void* g_serial_slice_group = nullptr;

class SerialSliceMark {
 public:
  explicit SerialSliceMark(const void* group)
      : saved_(g_serial_slice_group) {
    g_serial_slice_group = group;
  }
  ~SerialSliceMark() { g_serial_slice_group = saved_; }
  SerialSliceMark(const SerialSliceMark&) = delete;
  SerialSliceMark& operator=(const SerialSliceMark&) = delete;

 private:
  const void* saved_;
};

#ifdef AORTA_LANES_TEST_SEAMS
/// Test seam: the drain-slice EPOCH — a process-unique id minted at
/// every drain()/drain_drop() entry and published thread-locally for
/// the slice's duration. The combinator ordering row asserts its three
/// events share ONE epoch (the same slice), which the group pointer
/// alone cannot prove. Seam test targets only.
inline std::atomic<uint64_t> g_serial_slice_epoch_counter{0};
inline thread_local uint64_t g_serial_slice_epoch = 0;

class SerialSliceEpochMark {
 public:
  SerialSliceEpochMark() : saved_(g_serial_slice_epoch) {
    g_serial_slice_epoch = g_serial_slice_epoch_counter.fetch_add(
                               1, std::memory_order_seq_cst) +
                           1;
  }
  ~SerialSliceEpochMark() { g_serial_slice_epoch = saved_; }
  SerialSliceEpochMark(const SerialSliceEpochMark&) = delete;
  SerialSliceEpochMark& operator=(const SerialSliceEpochMark&) = delete;

 private:
  uint64_t saved_;
};
#endif

/// Serial group — MutuallyExclusive callback-group semantics as a layer
/// ABOVE lanes: MPSC intake, single drain token, the wake-bit protocol
/// with the two-ordered-checks drain exit (backlog strand closed by the
/// budget path). The group's drain item is an ordinary lane work item, so the
/// group inherits its lane's class.
class SerialGroup {
 public:
  /// `batch_budget` bounds one drain slice (fairness across the lane).
  /// `resume_domain`: a TASK-tier
  /// serial domain — markers post through the budget-EXEMPT
  /// post_resume_marker intake (a domain marker's drop delivers user
  /// unwinds and must never shed inline on a producer), and members
  /// enter through post_resume_member (no reservation, no shed
  /// verdict; population bounded by live frames — at most one
  /// outstanding resume per frame). Domains carry ONLY resume members;
  /// callback groups keep the budget-governed intakes unchanged.
  SerialGroup(LanePool& pool, size_t lane, uint32_t batch_budget = 16,
              bool resume_domain = false)
      : pool_(pool),
        lane_(lane),
        budget_(batch_budget),
        resume_domain_(resume_domain) {}

  ~SerialGroup() {
    // Owner must stop posting before destruction; drain what remains so
    // no item leaks (drop path — mirrors pool residue semantics).
    for (;;) {
      Work w;
      {
        std::lock_guard<std::mutex> lk(m_);
        if (q_.empty()) break;
        w = q_.front();
        q_.pop_front();
      }
      if (w.group_reserved) {
        pool_.release_group_member(lane_, w.cost_bytes);
      }
      if (w.drop) w.drop(w.ctx);
    }
  }

  /// Post a FORWARDED CALLBACK member:
  /// budget-checked against the group's lane BEFORE consumption — a
  /// refusal NEVER consumes the item (the caller owns the overflow
  /// policy), and the member counts toward the lane's event budget so
  /// grouped subscriptions cannot accumulate unbounded memory outside
  /// the admission tier. Verdict truthfulness: OK here means the group
  /// owns the item (any later shed is the group's own drop delivery).
  Post post_callback_member(const Work& w) {
    if (!pool_.reserve_group_member(lane_, w.cost_bytes)) {
      return pool_.closed() ? Post::REFUSED_CLOSED : Post::REFUSED_FULL;
    }
    Work counted = w;
    counted.group_reserved = 1;
    (void)post(counted);  // consumes; reservation released at delivery
    return Post::OK;
  }

  /// Post into the group. Members run strictly serially, in FIFO order,
  /// with member-granularity deadline expiry (the
  /// drain replicates the lane's expiry check with the same counters).
  Post post(Work w) {
    {
      std::lock_guard<std::mutex> lk(m_);
      q_.push_back(w);
    }
    // wake-bit: submit = push + fetch_or(NOTIFIED); the submitter that
    // saw IDLE (no RUNNING, no NOTIFIED) enqueues the drain item.
    uint8_t prev = state_.fetch_or(NOTIFIED, std::memory_order_acq_rel);
    if (prev == 0) {
      return enqueue_drain();
    }
    return Post::OK;
  }

  /// Budget-EXEMPT member intake for RESUME-DOMAIN groups: no
  /// reservation, no shed verdict — the member population is bounded by
  /// live frames (one outstanding resume per frame), never by traffic.
  /// The marker rides post_resume_marker; REFUSED_CLOSED keeps the
  /// residue route (enqueue_drain). Precondition: resume_domain.
  Post post_resume_member(Work w) {
    {
      std::lock_guard<std::mutex> lk(m_);
      q_.push_back(w);
    }
    uint8_t prev = state_.fetch_or(NOTIFIED, std::memory_order_acq_rel);
    if (prev == 0) {
      // Verdict truthfulness: the member was
      // CONSUMED by the push above — OK means the group owns it, and
      // the marker's own fate (posted, or closed-pool → residue inside
      // enqueue_drain) is the group's internal business. Propagating a
      // marker REFUSED_CLOSED here would tell a contract-following
      // caller to re-deliver a Work the group already owns —
      // double-drop/double-free.
      (void)enqueue_drain();
    }
    return Post::OK;
  }

  uint64_t expired_drops() const {
    return expired_.load(std::memory_order_relaxed);
  }

#ifdef AORTA_LANES_TEST_SEAMS
  /// Test seams (single-TU gauntlet target
  /// only, fully excluded from release builds). The exit seam fires in
  /// drain_drop between the empty observation and the exit CAS — the
  /// deterministic window for the concurrent-late-producer row.
  static inline void (*drain_drop_exit_seam)(void*) = nullptr;
  static inline void* drain_drop_exit_seam_ctx = nullptr;
  /// Marker accounting: live count + high-water, and cell alloc/free
  /// pairing (single-delete pin).
  static inline std::atomic<int> dbg_markers_live{0};
  /// Drop-path proof: how many marker DROPS ran (drain_drop entries).
  static inline std::atomic<uint64_t> dbg_drain_drop_entries{0};
  static inline std::atomic<int> dbg_markers_high{0};
  static inline std::atomic<int> dbg_cells_alloc{0};
  static inline std::atomic<int> dbg_cells_freed{0};
  static void dbg_marker_inc() {
    int now = dbg_markers_live.fetch_add(1, std::memory_order_acq_rel) + 1;
    int hi = dbg_markers_high.load(std::memory_order_relaxed);
    while (now > hi &&
           !dbg_markers_high.compare_exchange_weak(hi, now)) {
    }
  }
  static void dbg_reset() {
    dbg_markers_live.store(0);
    dbg_markers_high.store(0);
    dbg_cells_alloc.store(0);
    dbg_cells_freed.store(0);
  }
  /// Watchdog escapes (bounded-step rule). dbg_force_running makes
  /// the NEXT exit CAS succeed (state := RUNNING exactly), so a broken
  /// protocol's marker chain terminates after a bounded number of seam
  /// firings — the violation is already recorded by the cell counters;
  /// dbg_force_idle is the generic cleanup escape.
  void dbg_force_running() {
    state_.store(RUNNING, std::memory_order_release);
  }
  void dbg_force_idle() { state_.store(0, std::memory_order_release); }
#endif

  /// The owner's handle, held WEAK (a strong self-ref would be an
  /// unfreeable cycle); markers lock it into owning refs.
  void bind_self(const std::shared_ptr<SerialGroup>& self) { self_ = self; }

 private:
  static constexpr uint8_t RUNNING = 1;
  static constexpr uint8_t NOTIFIED = 2;

  static void drain_run(void* ctx) {
    // The marker owns a heap shared_ptr cell — the group stays
    // alive until every queued marker is consumed, even if the owner
    // map dropped its reference (live-executor detach).
    auto* cell = static_cast<std::shared_ptr<SerialGroup>*>(ctx);
    (*cell)->drain();
#ifdef AORTA_LANES_TEST_SEAMS
    dbg_markers_live.fetch_sub(1, std::memory_order_acq_rel);
    dbg_cells_freed.fetch_add(1, std::memory_order_acq_rel);
#endif
    delete cell;
  }
  static void drain_drop(void* ctx) {
    // Pool is closing: the group's queued members drain via the pool's
    // residue path semantics — deliver drops here (the draining
    // thread is running us).
    //
    // FULL wake-bit protocol: BOTH halves of drain()'s discipline.
    // Entry exchange(RUNNING) clears NOTIFIED so a marker dropped
    // while never run can still take the exit CAS (without it the CAS
    // could never succeed and the re-enqueue would spin an infinite
    // marker chain through the residue); the two-ordered-exit-checks
    // CAS closes the concurrent-late-producer lost wakeup (the old
    // bare store(0) let a producer that pushed between the empty
    // observation and the store see NOTIFIED-still-set, post no
    // marker, and strand forever).
    auto* cell = static_cast<std::shared_ptr<SerialGroup>*>(ctx);
    SerialGroup* g = cell->get();
    g->state_.exchange(RUNNING, std::memory_order_acq_rel);
    // Drop deliveries run user unwinds — the slice mark covers
    // them too (a teardown unwind is domain-sanctioned execution).
#ifndef AORTA_LANES_BENCH_NO_SLICE_MARK
    SerialSliceMark slice_mark(g);
#endif
#ifdef AORTA_LANES_TEST_SEAMS
    SerialSliceEpochMark epoch_mark;
    // Test seam: the drop-path row must PROVE its delivery took
    // THIS path — the queued marker's drop — not the router's CLOSED
    // inline-cancel (which also carries a slice mark).
    dbg_drain_drop_entries.fetch_add(1, std::memory_order_seq_cst);
#endif
    for (;;) {
      Work w;
      {
        std::lock_guard<std::mutex> lk(g->m_);
        if (g->q_.empty()) break;
        w = g->q_.front();
        g->q_.pop_front();
      }
      if (w.group_reserved) {
        g->pool_.release_group_member(g->lane_, w.cost_bytes);
      }
      if (w.drop) w.drop(w.ctx);
    }
#ifdef AORTA_LANES_TEST_SEAMS
    if (drain_drop_exit_seam) drain_drop_exit_seam(drain_drop_exit_seam_ctx);
#endif
    uint8_t expect = RUNNING;
    if (!g->state_.compare_exchange_strong(expect, 0,
                                           std::memory_order_acq_rel)) {
      // NOTIFIED arrived after the empty observation: a late producer's
      // member sits in q_ with no marker of its own — re-enqueue (a
      // FRESH cell; on the closed pool that lands in the residue, the
      // existing marker route). This cell still deletes exactly once
      // below (single-delete pinned by the destructor-count row).
      g->enqueue_drain();
    }
#ifdef AORTA_LANES_TEST_SEAMS
    dbg_markers_live.fetch_sub(1, std::memory_order_acq_rel);
    dbg_cells_freed.fetch_add(1, std::memory_order_acq_rel);
#endif
    delete cell;
  }

  Post enqueue_drain() {
    Work d;
    d.run = drain_run;
    d.drop = drain_drop;
    d.ctx = new std::shared_ptr<SerialGroup>(self_or_alias());
#ifdef AORTA_LANES_TEST_SEAMS
    dbg_marker_inc();
    dbg_cells_alloc.fetch_add(1, std::memory_order_acq_rel);
#endif
    // Resume domains ride the budget-EXEMPT marker intake: a
    // domain marker's drop delivers user unwinds and must never shed
    // inline on the producer. Callback groups keep the budget-governed
    // post() unchanged.
    Post r = resume_domain_ ? pool_.post_resume_marker(lane_, d)
                            : pool_.post(lane_, d);
    if (r == Post::REFUSED_CLOSED) {
      // Closed pool: REFUSED_CLOSED does NOT consume — route the SAME
      // marker (same cell) through the pool residue so the members'
      // drops are delivered by the draining thread.
      pool_.post_residue(d);
    }
    return r;
  }

  /// Marker ownership: owner-bound groups (FacadeAttachment) hand
  /// out a REAL shared_ptr so queued markers keep the group alive past
  /// a live-executor detach; stack-owned groups (tests, whose dtor
  /// contract already demands post-quiesce) get a non-owning alias.
  std::shared_ptr<SerialGroup> self_or_alias() {
    if (auto sp = self_.lock()) {
      return sp;
    }
    return std::shared_ptr<SerialGroup>(this, [](SerialGroup*) {});
  }

  void drain() {
    // Drain entry = swap(RUNNING): clears NOTIFIED.
    state_.exchange(RUNNING, std::memory_order_acq_rel);
    // The slice occupancy mark (one TLS write per slice).
    // AORTA_LANES_BENCH_NO_SLICE_MARK (measurement knob): a bench-only
    // local_defines that compiles the
    // slice WITHOUT the mark so a with/without binary pair MEASURES its
    // cost instead of asserting it. Never defined by production
    // targets. The knob ALSO compiles out the mark's per-verb READER
    // (serial_canary_check, flow.hpp — with the TLS null
    // the canary would take the mismatch path with an atomic RMW at
    // every checkpoint, contaminating the pair); together the pair
    // isolates the whole mark feature as one variable. Routing
    // semantics are unaffected either way.
#ifndef AORTA_LANES_BENCH_NO_SLICE_MARK
    SerialSliceMark slice_mark(this);
#endif
#ifdef AORTA_LANES_TEST_SEAMS
    SerialSliceEpochMark epoch_mark;
#endif
    uint32_t ran = 0;
    for (;;) {
      // Two ordered exit checks (the backlog fix):
      // (1) budget path FIRST: batch stopped on budget with queue
      //     non-empty → re-enqueue and return (no NOTIFIED consult).
      if (ran >= budget_) {
        bool nonempty;
        {
          std::lock_guard<std::mutex> lk(m_);
          nonempty = !q_.empty();
        }
        if (nonempty) {
          enqueue_drain();
          return;
        }
      }
      Work w;
      bool have = false;
      {
        std::lock_guard<std::mutex> lk(m_);
        if (!q_.empty()) {
          w = q_.front();
          q_.pop_front();
          have = true;
        }
      }
      if (have) {
        if (w.group_reserved) {
          pool_.release_group_member(lane_, w.cost_bytes);
        }
        const uint64_t now = std::chrono::duration_cast<
                                 std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now()
                                     .time_since_epoch())
                                 .count();
        // Shutdown phase 2: once the pool has begun closing, remaining
        // members drain to DROP — running them would extend user work
        // past close(). run_one()/worker_loop() gate their pops the same
        // way, but a group drain already mid-flight on a worker when
        // close() lands on another thread passes through THIS loop only;
        // without the check it would keep running up to a full batch of
        // members the shutdown contract promises never run.
        const bool draining = pool_.closed();
        const bool expired = w.deadline_ns != 0 && w.deadline_ns < now;
        if (expired || draining) {
          if (expired) {
            expired_.fetch_add(1, std::memory_order_relaxed);
          }
          if (w.drop) w.drop(w.ctx);
        } else {
          if (w.run) w.run(w.ctx);
        }
        ran++;
        continue;
      }
      // (2) empty path: CAS(RUNNING → IDLE). Failure = NOTIFIED arrived
      //     after the empty observation → re-enqueue and return.
      uint8_t expect = RUNNING;
      if (state_.compare_exchange_strong(expect, 0,
                                         std::memory_order_acq_rel)) {
        return;  // parked
      }
      enqueue_drain();
      return;
    }
  }

  LanePool& pool_;
  const size_t lane_;
  std::weak_ptr<SerialGroup> self_;
  const uint32_t budget_;
  const bool resume_domain_ = false;  // budget-exempt intakes
  std::mutex m_;
  std::deque<Work> q_;
  std::atomic<uint8_t> state_{0};
  std::atomic<uint64_t> expired_{0};
};

}  // namespace lanes
}  // namespace aorta
