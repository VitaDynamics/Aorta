// AortaLanes gauntlet — every lane-pool protocol clause pinned by a
// test. The suite must stay TSan-clean.
#include "aorta/lanes.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using aorta::lanes::LaneConfig;
using aorta::lanes::LanePool;
using aorta::lanes::Post;
using aorta::lanes::PoolConfig;
using aorta::lanes::Work;

namespace {

uint64_t mono_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// A counting probe: records run/drop exactly-once accounting.
struct Probe {
  std::atomic<int> ran{0};
  std::atomic<int> dropped{0};
  std::vector<int> order;   // guarded by order_m
  std::mutex order_m;
};

struct Item {
  Probe* p;
  int tag;
};

void run_cb(void* ctx) {
  auto* it = static_cast<Item*>(ctx);
  {
    std::lock_guard<std::mutex> lk(it->p->order_m);
    it->p->order.push_back(it->tag);
  }
  it->p->ran.fetch_add(1, std::memory_order_relaxed);
  delete it;
}
void drop_cb(void* ctx) {
  auto* it = static_cast<Item*>(ctx);
  it->p->dropped.fetch_add(1, std::memory_order_relaxed);
  delete it;
}

Work make(Probe& p, int tag, uint64_t deadline_ns = 0, uint32_t bytes = 0) {
  Work w;
  w.run = run_cb;
  w.drop = drop_cb;
  w.ctx = new Item{&p, tag};
  w.deadline_ns = deadline_ns;
  w.cost_bytes = bytes;
  return w;
}

PoolConfig st_config(size_t nlanes = 1) {
  PoolConfig cfg;
  for (size_t i = 0; i < nlanes; i++) cfg.lanes.push_back(LaneConfig{});
  return cfg;  // no workers = single-threaded mode
}

// ---------------------------------------------------------------- basics

TEST(Lanes, StFifoOrderAndExactlyOnce) {
  LanePool pool(st_config());
  Probe p;
  for (int i = 0; i < 100; i++)
    ASSERT_EQ(pool.post(0, make(p, i)), Post::OK);
  EXPECT_EQ(pool.drain_all(), 100u);
  EXPECT_EQ(p.ran.load(), 100);
  EXPECT_EQ(p.dropped.load(), 0);
  for (int i = 0; i < 100; i++) EXPECT_EQ(p.order[i], i) << "FIFO order";
}

TEST(Lanes, EdfOrdersByDeadlineThenSeq) {
  LanePool pool(st_config());
  Probe p;
  uint64_t base = mono_ns() + 1'000'000'000ull;  // future: nothing expires
  // Same deadline → seq (enqueue) order preserved: tags 4,5,6.
  ASSERT_EQ(pool.post(0, make(p, 4, base + 500)), Post::OK);
  ASSERT_EQ(pool.post(0, make(p, 5, base + 500)), Post::OK);
  ASSERT_EQ(pool.post(0, make(p, 6, base + 500)), Post::OK);
  // Earlier deadlines jump ahead regardless of enqueue order.
  ASSERT_EQ(pool.post(0, make(p, 1, base + 100)), Post::OK);
  ASSERT_EQ(pool.post(0, make(p, 2, base + 200)), Post::OK);
  pool.drain_all();
  ASSERT_EQ(p.order.size(), 5u);
  EXPECT_EQ(p.order[0], 1);
  EXPECT_EQ(p.order[1], 2);
  EXPECT_EQ(p.order[2], 4);
  EXPECT_EQ(p.order[3], 5);
  EXPECT_EQ(p.order[4], 6);
}

TEST(Lanes, KBypassBoundProtectsFifoHead) {
  // Sustained deadline traffic may bypass the FIFO head at most K=8
  // consecutive times; then the FIFO head runs.
  PoolConfig cfg = st_config();
  cfg.lanes[0].edf_bypass_limit = 8;
  LanePool pool(cfg);
  Probe p;
  uint64_t base = mono_ns() + 1'000'000'000ull;
  ASSERT_EQ(pool.post(0, make(p, /*tag=*/999)), Post::OK);  // FIFO head
  for (int i = 0; i < 20; i++)
    ASSERT_EQ(pool.post(0, make(p, i, base + i)), Post::OK);  // EDF stream
  pool.drain_all();
  // The FIFO item must appear within the first K+1 executions.
  auto pos = std::find(p.order.begin(), p.order.end(), 999);
  ASSERT_NE(pos, p.order.end());
  EXPECT_LE(pos - p.order.begin(), 8) << "anti-starvation bound";
  EXPECT_EQ(pool.stats(0).edf_bypasses, 8u);
}

TEST(Lanes, ExpiredAtDequeueRoutesToDrop) {
  LanePool pool(st_config());
  Probe p;
  ASSERT_EQ(pool.post(0, make(p, 0, /*deadline=*/1 /*long past*/)),
            Post::OK);
  ASSERT_EQ(pool.post(0, make(p, 1)), Post::OK);
  pool.drain_all();
  EXPECT_EQ(p.dropped.load(), 1);
  EXPECT_EQ(p.ran.load(), 1);
  EXPECT_EQ(pool.stats(0).dropped_expired, 1u);
}

// ---------------------------------------------------------------- budgets

TEST(Lanes, EventBudgetSheds) {
  PoolConfig cfg = st_config();
  cfg.lanes[0].max_queued_events = 4;
  LanePool pool(cfg);
  Probe p;
  for (int i = 0; i < 4; i++)
    ASSERT_EQ(pool.post(0, make(p, i)), Post::OK);
  EXPECT_EQ(pool.post(0, make(p, 4)), Post::SHED_FULL);
  EXPECT_EQ(p.dropped.load(), 1) << "shed drop ran inline";
  EXPECT_EQ(pool.stats(0).shed_full, 1u);
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 4);
}

TEST(Lanes, ByteBudgetSheds) {
  PoolConfig cfg = st_config();
  cfg.lanes[0].max_queued_bytes = 1000;
  LanePool pool(cfg);
  Probe p;
  ASSERT_EQ(pool.post(0, make(p, 0, 0, 600)), Post::OK);
  EXPECT_EQ(pool.post(0, make(p, 1, 0, 600)), Post::SHED_FULL);
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 1);
  EXPECT_EQ(p.dropped.load(), 1);
}

// --------------------------------------------------------------- shutdown

TEST(Lanes, CloseBarsPostsAndDrainsQueuedToDrop) {
  LanePool pool(st_config());
  Probe p;
  for (int i = 0; i < 10; i++)
    ASSERT_EQ(pool.post(0, make(p, i)), Post::OK);
  pool.close();
  Work late = make(p, 99);
  EXPECT_EQ(pool.post(0, late), Post::REFUSED_CLOSED);
  pool.post_residue(late);  // trampoline-class caller hands it over
  pool.join();              // closer == drainer in ST mode
  EXPECT_EQ(p.ran.load(), 0);
  EXPECT_EQ(p.dropped.load(), 11) << "10 queued + 1 residue, all dropped";
  EXPECT_EQ(pool.stats(0).dropped_closed, 10u);
}

// Residue drops inside callback context: residue
// drops run USER code (resume-into-cancellation unwinds), so every
// residue delivery — facade drain hook, closer thread, ~LaneRuntime —
// must carry the callback-context marks exactly as worker drop paths
// do. The pool-level fix: drain_residue_now() wraps w.drop in
// the configured ctx hooks.
thread_local int g_ctx_depth = 0;
uint8_t ctx_enter_probe() {
  g_ctx_depth++;
  return static_cast<uint8_t>(g_ctx_depth - 1);
}
void ctx_exit_probe(uint8_t prev) { g_ctx_depth = prev; }

TEST(Lanes, ResidueDropsCarryCallbackContextMarks) {
  PoolConfig cfg = st_config();
  cfg.ctx_enter = &ctx_enter_probe;
  cfg.ctx_exit = &ctx_exit_probe;
  LanePool pool(cfg);
  static std::atomic<int> observed_depth{-1};
  observed_depth.store(-1);
  Work w{};
  w.drop = [](void*) { observed_depth.store(g_ctx_depth); };
  w.ctx = nullptr;
  pool.post_residue(w);
  EXPECT_EQ(pool.drain_residue_now(), 1u);
  EXPECT_EQ(observed_depth.load(), 1)
      << "a residue drop must execute INSIDE the callback-context "
         "marks (ctx_enter before, ctx_exit after)";
  EXPECT_EQ(g_ctx_depth, 0) << "mark must be restored after delivery";
  pool.close();
}

// The nested leg — a caller ALREADY inside a mark
// (e.g. a worker delivering a group drop that relocks into residue)
// must see save-and-restore semantics, not a clobber to zero. Guards
// against a future ctx_exit(0) regression.
TEST(Lanes, ResidueMarksNestUnderAnOuterMark) {
  PoolConfig cfg = st_config();
  cfg.ctx_enter = &ctx_enter_probe;
  cfg.ctx_exit = &ctx_exit_probe;
  LanePool pool(cfg);
  static std::atomic<int> observed_depth{-1};
  observed_depth.store(-1);
  const uint8_t outer = ctx_enter_probe();  // simulate an outer mark
  ASSERT_EQ(g_ctx_depth, 1);
  Work w{};
  w.drop = [](void*) { observed_depth.store(g_ctx_depth); };
  pool.post_residue(w);
  EXPECT_EQ(pool.drain_residue_now(), 1u);
  EXPECT_EQ(observed_depth.load(), 2) << "nested: outer mark + delivery mark";
  EXPECT_EQ(g_ctx_depth, 1) << "restore must return to the OUTER depth, "
                               "never clobber to zero";
  ctx_exit_probe(outer);
  EXPECT_EQ(g_ctx_depth, 0);
  pool.close();
}

TEST(Lanes, MtWorkersRunAndDrainOnClose) {
  PoolConfig cfg = st_config(2);
  cfg.workers_per_lane = {2, 1};
  Probe p;  // outlives the pool
  LanePool pool(cfg);
  for (int i = 0; i < 2000; i++)
    ASSERT_EQ(pool.post(i % 2, make(p, i)), Post::OK);
  // Give workers time, then shut down; everything resolves exactly once.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (p.ran.load() < 2000 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  pool.close();
  pool.join();
  EXPECT_EQ(p.ran.load() + p.dropped.load(), 2000) << "exactly-once";
  EXPECT_EQ(p.ran.load(), 2000) << "all ran (drained before close)";
}

TEST(Lanes, PostVsCloseHammerExactlyOnce) {
  // Producers race close(): every item resolves exactly once — run,
  // drop (drained), or REFUSED (producer routes to residue → drop).
  for (int round = 0; round < 20; round++) {
    PoolConfig cfg = st_config(1);
    cfg.workers_per_lane = {2};
    auto pool = std::make_unique<LanePool>(cfg);
    Probe p;
    std::atomic<int> posted{0}, refused{0};
    std::vector<std::thread> prods;
    for (int t = 0; t < 3; t++) {
      prods.emplace_back([&] {
        for (int i = 0; i < 500; i++) {
          Work w = make(p, i);
          Post r = pool->post(0, w);
          if (r == Post::REFUSED_CLOSED) {
            pool->post_residue(w);
            refused.fetch_add(1);
          }
          posted.fetch_add(1);
        }
      });
    }
    std::this_thread::sleep_for(std::chrono::microseconds(200 * round));
    pool->close();
    for (auto& t : prods) t.join();
    pool->join();
    EXPECT_EQ(p.ran.load() + p.dropped.load(), posted.load())
        << "round " << round << " refused=" << refused.load();
    pool.reset();
  }
}

TEST(Lanes, CreateDestroyStress) {
  // The #1 simple-pool defect class is shutdown races; create/destroy
  // in a tight loop under TSan is the
  // canonical detector. 20k CI-sized; the million-iteration soak runs in
  // the manual gauntlet (same body, env-scaled).
  int iters = 20000;
  if (const char* e = getenv("LANES_STRESS_ITERS")) iters = atoi(e);
  for (int i = 0; i < iters; i++) {
    PoolConfig cfg = st_config(1);
    cfg.workers_per_lane = {1};
    Probe p;  // MUST outlive the pool (workers may run callbacks in dtor)
    LanePool pool(cfg);
    pool.post(0, make(p, 0));
    // Immediate destruction: close+join via dtor; exactly-once holds.
    // (Alternating with a tiny sleep to vary interleavings.)
    if (i % 7 == 0) std::this_thread::yield();
  }
  SUCCEED();
}

// -------------------------------------------------------------- fairness

TEST(Lanes, StWrrKeepsBackgroundAlive) {
  // Sustained control-lane load must not starve background: with weights
  // [8,4] over 2 lanes, background gets ~1/3 of dequeues (the
  // executor's WRR fairness model).
  PoolConfig cfg = st_config(2);
  cfg.st_wrr_weights = {8, 4};
  LanePool pool(cfg);
  Probe p_ctl, p_bg;
  for (int i = 0; i < 240; i++) {
    ASSERT_EQ(pool.post(0, make(p_ctl, i)), Post::OK);
    ASSERT_EQ(pool.post(1, make(p_bg, i)), Post::OK);
  }
  // Drain only 120 items — under strict priority background would get 0.
  pool.drain_all(120);
  EXPECT_GT(p_bg.ran.load(), 20) << "background progressed under load";
  EXPECT_GT(p_ctl.ran.load(), p_bg.ran.load()) << "control still favored";
  pool.drain_all();
}

// --------------------------------------------------------------- ST wait

TEST(Lanes, WaitForWorkAndWakeHook) {
  LanePool pool(st_config());
  std::atomic<int> hooks{0};
  pool.set_wake_hook(
      [](void* ctx) {
        static_cast<std::atomic<int>*>(ctx)->fetch_add(1);
      },
      &hooks);
  EXPECT_FALSE(pool.wait_for_work(std::chrono::milliseconds(20)));
  Probe p;
  std::thread poster([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    pool.post(0, make(p, 1));
  });
  EXPECT_TRUE(pool.wait_for_work(std::chrono::milliseconds(500)));
  poster.join();
  EXPECT_EQ(hooks.load(), 1) << "facade wake hook fired per post";
  pool.drain_all();
}

}  // namespace

// ------------------------------------------------- serial groups (wake-bit)

TEST(SerialGroup, StrictSerialFifoUnderMtLane) {
  PoolConfig cfg = st_config(1);
  cfg.workers_per_lane = {3};  // 3 workers, yet the group runs serially
  Probe p;
  std::atomic<int> concurrent{0}, max_concurrent{0};
  struct GItem {
    Probe* p;
    std::atomic<int>* conc;
    std::atomic<int>* maxc;
    int tag;
  };
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0, /*budget=*/4);
  auto grun = [](void* ctx) {
    auto* it = static_cast<GItem*>(ctx);
    int c = it->conc->fetch_add(1) + 1;
    int m = it->maxc->load();
    while (c > m && !it->maxc->compare_exchange_weak(m, c)) {
    }
    {
      std::lock_guard<std::mutex> lk(it->p->order_m);
      it->p->order.push_back(it->tag);
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    it->conc->fetch_sub(1);
    it->p->ran.fetch_add(1);
    delete it;
  };
  auto gdrop = [](void* ctx) {
    auto* it = static_cast<GItem*>(ctx);
    it->p->dropped.fetch_add(1);
    delete it;
  };
  for (int i = 0; i < 200; i++) {
    Work w;
    w.run = grun;
    w.drop = gdrop;
    w.ctx = new GItem{&p, &concurrent, &max_concurrent, i};
    ASSERT_EQ(group.post(w), Post::OK);
  }
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (p.ran.load() + p.dropped.load() < 200 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(p.ran.load(), 200);
  EXPECT_EQ(max_concurrent.load(), 1) << "mutual exclusion held on MT lane";
  for (int i = 0; i < 200; i++) EXPECT_EQ(p.order[i], i) << "FIFO";
  // Quiesce the workers BEFORE the stack-allocated group/pool destruct:
  // the counter this test polls is bumped by the LAST member callback,
  // but drain()'s tail (second lock acquisition + exit CAS) can still be
  // running on a pool worker at that point — destroying `group` under it
  // is UB (PoolCloseMidDrainDropsRemaining-
  // Members already tears down this way).
  pool.close();
  pool.join();
}

TEST(SerialGroup, BacklogLargerThanBudgetNeverStrands) {
  // The backlog test: N >> budget items enqueued BEFORE the
  // first drain, ZERO mid-batch submits — every item must run.
  PoolConfig cfg = st_config(1);
  Probe p;
  LanePool pool(cfg);  // ST: we control drain timing exactly
  aorta::lanes::SerialGroup group(pool, 0, /*budget=*/4);
  for (int i = 0; i < 100; i++) ASSERT_EQ(group.post(make(p, i)), Post::OK);
  // Drain the lane repeatedly: each drain item runs a 4-item batch and
  // must RE-ENQUEUE itself (budget path) until the backlog is gone.
  size_t safety = 0;
  while (p.ran.load() < 100 && safety++ < 1000) pool.drain_all();
  EXPECT_EQ(p.ran.load(), 100) << "backlog fully drained, nothing stranded";
  EXPECT_EQ(p.dropped.load(), 0);
}

TEST(SerialGroup, MemberDeadlineExpiryAtDrain) {
  PoolConfig cfg = st_config(1);
  Probe p;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0);
  ASSERT_EQ(group.post(make(p, 0, /*deadline=*/1)), Post::OK);  // expired
  ASSERT_EQ(group.post(make(p, 1)), Post::OK);
  pool.drain_all();
  EXPECT_EQ(p.dropped.load(), 1);
  EXPECT_EQ(p.ran.load(), 1);
  EXPECT_EQ(group.expired_drops(), 1u);
}

TEST(SerialGroup, PoolCloseMidDrainDropsRemainingMembers) {
  // Regression: close() landing on another
  // thread while a group drain batch is mid-flight must route the
  // batch's REMAINING members to drop — running them extends user work
  // past shutdown, invisible to the facade's remaining-work accounting.
  PoolConfig cfg = st_config(1);
  cfg.workers_per_lane = {1};  // the drain marker runs on a worker
  Probe p;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0, /*budget=*/16);
  std::atomic<bool> first_started{false};
  std::atomic<bool> release_first{false};
  struct Gate {
    Probe* p;
    std::atomic<bool>* started;
    std::atomic<bool>* release;
  };
  auto blocking_run = [](void* ctx) {
    auto* g = static_cast<Gate*>(ctx);
    g->started->store(true);
    // Hold the drain mid-batch until the main thread has closed the pool.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!g->release->load() &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    g->p->ran.fetch_add(1);
    delete g;
  };
  auto gate_drop = [](void* ctx) {
    auto* g = static_cast<Gate*>(ctx);
    g->p->dropped.fetch_add(1);
    delete g;
  };
  Work first;
  first.run = blocking_run;
  first.drop = gate_drop;
  first.ctx = new Gate{&p, &first_started, &release_first};
  ASSERT_EQ(group.post(first), Post::OK);
  // These land in the group queue while the drain is blocked inside
  // member 0 (same batch: budget=16, and RUNNING is already set so no
  // second marker is minted).
  for (int i = 0; i < 8; i++) ASSERT_EQ(group.post(make(p, i + 1)), Post::OK);

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!first_started.load() &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(first_started.load()) << "worker entered member 0";
  pool.close();                // non-blocking: flips flags + notifies
  release_first.store(true);   // let the in-flight member finish
  pool.join();

  EXPECT_EQ(p.ran.load(), 1) << "only the already-in-flight member ran";
  EXPECT_EQ(p.dropped.load(), 8) << "post-close members dropped, not run";
}

TEST(SerialGroup, SubmitVsDrainExitRace) {
  // Hammer the wake-bit protocol's exit CAS: submitters race the drain's
  // park; every item must run exactly once, promptly.
  PoolConfig cfg = st_config(1);
  cfg.workers_per_lane = {2};
  Probe p;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0, /*budget=*/1);
  std::vector<std::thread> prods;
  std::atomic<int> posted{0};
  for (int t = 0; t < 3; t++) {
    prods.emplace_back([&] {
      for (int i = 0; i < 400; i++) {
        if (group.post(make(p, i)) == Post::OK) posted.fetch_add(1);
        if (i % 16 == 0) std::this_thread::yield();
      }
    });
  }
  for (auto& t : prods) t.join();
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (p.ran.load() < posted.load() &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(p.ran.load(), posted.load()) << "no lost wakeup, no strand";
  // Quiesce before `group`/`pool` destruct — drain()'s tail may still be
  // on a worker after the last `ran` bump (same UB window as
  // StrictSerialFifoUnderMtLane).
  pool.close();
  pool.join();
}

// ---- Flow pre-work: pool corrections ------------------------------

TEST(Lanes, ClosedLaneDrainsQueuedToDropInWorkers) {
  // Items still QUEUED when close() lands drain to
  // DROP on the workers, never run.
  PoolConfig cfg = st_config(1);
  cfg.workers_per_lane = {1};
  Probe p;
  LanePool pool(cfg);
  // A slow head item keeps the worker busy while we queue + close.
  std::atomic<bool> gate{false};
  std::atomic<bool> head_started{false};
  struct Slow {
    std::atomic<bool>* gate;
    std::atomic<bool>* started;
    Probe* p;
  };
  Work head;
  head.run = [](void* ctx) {
    auto* s = static_cast<Slow*>(ctx);
    s->started->store(true, std::memory_order_release);
    while (!s->gate->load(std::memory_order_acquire))
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    s->p->ran.fetch_add(1, std::memory_order_relaxed);
    delete s;
  };
  head.drop = [](void* ctx) { delete static_cast<Slow*>(ctx); };
  head.ctx = new Slow{&gate, &head_started, &p};
  ASSERT_EQ(pool.post(0, head), Post::OK);
  // Wait until the worker has actually DEQUEUED the head — a fixed sleep
  // races the worker under sanitizer CI.
  const auto head_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!head_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < head_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(head_started.load(std::memory_order_acquire)) << "head running";
  for (int i = 0; i < 5; i++) ASSERT_EQ(pool.post(0, make(p, i)), Post::OK);
  pool.close();
  gate.store(true, std::memory_order_release);
  pool.join();
  EXPECT_EQ(p.ran.load(), 1) << "only the in-flight head ran";
  EXPECT_EQ(p.dropped.load(), 5) << "queued items drained to drop";
}

TEST(Lanes, ResumePostIsBudgetExempt) {
  // Resume items bypass the budget (their drop is user
  // unwinding and must not run inline on the producer).
  PoolConfig cfg = st_config(1);
  cfg.lanes[0].max_queued_events = 2;
  LanePool pool(cfg);
  Probe p;
  ASSERT_EQ(pool.post(0, make(p, 0)), Post::OK);
  ASSERT_EQ(pool.post(0, make(p, 1)), Post::OK);
  EXPECT_EQ(pool.post(0, make(p, 2)), Post::SHED_FULL) << "plain post sheds";
  EXPECT_EQ(pool.post_resume_item(0, make(p, 3)), Post::OK)
      << "resume post is exempt from the budget";
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 3);
  EXPECT_EQ(p.dropped.load(), 1) << "only the shed plain post dropped";
  // Closed lane still refuses resume posts (caller owns the drop path).
  pool.close();
  Work late = make(p, 9);
  EXPECT_EQ(pool.post_resume_item(0, late), Post::REFUSED_CLOSED);
  pool.post_residue(late);
  pool.join();
}

TEST(Lanes, DrainReachesBacklogInLowLanesWhenHighLanesEmpty) {
  // WRR drain must not strand low-lane backlog behind empty weighted
  // lanes (LaneRuntime post-through regression).
  PoolConfig cfg = st_config(4);
  LanePool pool(cfg);
  Probe p;
  ASSERT_EQ(pool.post(2, make(p, 0)), Post::OK);
  ASSERT_EQ(pool.post(3, make(p, 1)), Post::OK);
  EXPECT_EQ(pool.drain_all(), 2u);
  EXPECT_EQ(p.ran.load(), 2);
}

TEST(Lanes, CallbackPostRefusalDoesNotConsume) {
  // post_callback_item returns
  // REFUSED_FULL with the item UNTOUCHED — the caller owns the policy.
  PoolConfig cfg = st_config(1);
  cfg.lanes[0].max_queued_events = 1;
  LanePool pool(cfg);
  Probe p;
  ASSERT_EQ(pool.post(0, make(p, 0)), Post::OK);
  Work w = make(p, 1);
  EXPECT_EQ(pool.post_callback_item(0, w), Post::REFUSED_FULL);
  EXPECT_EQ(p.dropped.load(), 0) << "refusal did NOT run the drop";
  EXPECT_EQ(pool.stats(0).shed_full, 0u)
      << "non-consuming refusal is NOT a shed (caller keeps the item)";
  // The caller can still deliver it (queue-path Drop policy = run drop itself).
  if (w.drop) w.drop(w.ctx);
  EXPECT_EQ(p.dropped.load(), 1);
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 1);
}

TEST(Lanes, GroupMemberRefusalNeverConsumes) {
  // A serial-group member post is
  // budget-checked BEFORE consumption — refusal hands the item back
  // (the old consume-then-shed protocol double-freed), and members
  // count toward the lane's event budget (no unbounded group queue).
  PoolConfig cfg = st_config(1);
  cfg.lanes[0].max_queued_events = 2;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0);
  Probe p;
  // Budget 2: the drain marker takes a lane slot when posted; members
  // take reservations. Fill: member 1 (reservation) + its drain marker
  // (lane slot) = budget 2 consumed.
  ASSERT_EQ(group.post_callback_member(make(p, 0)), Post::OK);
  Work w = make(p, 1);
  EXPECT_EQ(group.post_callback_member(w), Post::REFUSED_FULL)
      << "budget full refuses";
  EXPECT_EQ(p.dropped.load(), 0) << "refusal did NOT consume";
  // The caller still owns the refused item.
  if (w.drop) w.drop(w.ctx);
  EXPECT_EQ(p.dropped.load(), 1);
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 1) << "the accepted member ran";
  pool.close();
}

TEST(Lanes, DrainOneUnworkedNeverTouchesWorkerOwnedLanes) {
  // In MT mode the spin thread's
  // drain must not dispatch callbacks on a lane a dedicated worker
  // owns. Lane 0 has a worker; lane 1 does not. The test thread plays
  // the spin thread via drain_one_unworked(): it may only ever run
  // lane 1's item; lane 0's is the worker's.
  PoolConfig cfg = st_config(2);
  cfg.workers_per_lane = {1, 0};
  LanePool pool(cfg);
  const auto test_tid = std::this_thread::get_id();
  std::atomic<bool> covered_ran{false}, covered_on_test_thread{false};
  std::atomic<bool> uncovered_ran{false}, uncovered_on_test_thread{false};
  struct TidProbe {
    std::thread::id test_tid;
    std::atomic<bool>* ran;
    std::atomic<bool>* on_test_thread;
  };
  auto trun = [](void* ctx) {
    auto* t = static_cast<TidProbe*>(ctx);
    if (std::this_thread::get_id() == t->test_tid)
      t->on_test_thread->store(true);
    t->ran->store(true);
    delete t;
  };
  auto tdrop = [](void* ctx) { delete static_cast<TidProbe*>(ctx); };
  Work a;
  a.run = trun;
  a.drop = tdrop;
  a.ctx = new TidProbe{test_tid, &covered_ran, &covered_on_test_thread};
  ASSERT_EQ(pool.post(0, a), Post::OK);
  Work b;
  b.run = trun;
  b.drop = tdrop;
  b.ctx = new TidProbe{test_tid, &uncovered_ran, &uncovered_on_test_thread};
  ASSERT_EQ(pool.post(1, b), Post::OK);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!(covered_ran.load() && uncovered_ran.load()) &&
         std::chrono::steady_clock::now() < deadline) {
    pool.drain_one_unworked();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(covered_ran.load() && uncovered_ran.load());
  EXPECT_FALSE(covered_on_test_thread.load())
      << "worker-owned lane item must not run on the draining thread";
  EXPECT_TRUE(uncovered_on_test_thread.load())
      << "unworked lane item drains on the spin-thread path";
}

TEST(Lanes, GroupMemberByteBudgetRefusesAndCreditsBack) {
  // Members must count toward BOTH halves of the
  // lane's admission tier. Event budget alone admitted large-payload
  // grouped members without bound; the byte half now refuses (non-
  // consuming) and delivery credits the reservation back.
  PoolConfig cfg = st_config(1);
  cfg.lanes[0].max_queued_bytes = 100;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0);
  Probe p;
  Work big = make(p, 0);
  big.cost_bytes = 80;
  ASSERT_EQ(group.post_callback_member(big), Post::OK);
  Work over = make(p, 1);
  over.cost_bytes = 80;  // 80 reserved (+ marker) > 100 with another 80
  EXPECT_EQ(group.post_callback_member(over), Post::REFUSED_FULL)
      << "byte budget full refuses";
  EXPECT_EQ(p.dropped.load(), 0) << "refusal did NOT consume";
  if (over.drop) over.drop(over.ctx);
  EXPECT_EQ(p.dropped.load(), 1);
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 1) << "the accepted member ran";
  // Delivery released the byte reservation: the same payload fits again.
  Work again = make(p, 2);
  again.cost_bytes = 80;
  EXPECT_EQ(group.post_callback_member(again), Post::OK)
      << "released bytes credit back";
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 2);
  pool.close();
}

TEST(Lanes, MarkerShedWithReservedMembersDoesNotDeadlock) {
  // A group drain marker shed at post used to run
  // drain_drop INLINE UNDER lane.m — and drain_drop relocks the same
  // lane (release_group_member) = recursive-mutex deadlock. The shed
  // drop now runs outside the mutex; this test COMPLETING is the pin
  // (pre-fix it wedges here).
  PoolConfig cfg = st_config(1);
  cfg.lanes[0].max_queued_events = 2;
  LanePool pool(cfg);
  aorta::lanes::SerialGroup group(pool, 0);
  Probe p;
  ASSERT_EQ(pool.post(0, make(p, 0)), Post::OK);       // queued=1
  ASSERT_TRUE(pool.reserve_group_member(0));           // reserved=1
  ASSERT_EQ(pool.post(0, make(p, 1)), Post::OK);       // queued=2 (cap)
  Work member = make(p, 2);
  member.group_reserved = 1;
  // Member enters the group queue; the DRAIN MARKER post sheds on the
  // full lane → drain_drop delivers the member's drop + releases the
  // reservation (relocking the lane — outside the mutex now).
  (void)group.post(member);
  EXPECT_EQ(p.dropped.load(), 1) << "member drop delivered on marker shed";
  pool.drain_all();
  EXPECT_EQ(p.ran.load(), 2) << "fillers unaffected";
}

// ================================================================
// SerialDomain — the resume-domain lanes-layer
// gauntlet: budget-exempt marker/member intakes, drain_drop's FULL
// wake-bit protocol (entry exchange + CAS exit), deterministic seam
// rows, marker-cell single-delete, and the phase-split population
// bounds. All rows run in this single-TU target with
// AORTA_LANES_TEST_SEAMS compiled in.

using aorta::lanes::SerialGroup;

TEST(SerialDomainGauntlet, ResumeMemberExemptFromLaneBudgets) {
  SerialGroup::dbg_reset();
  PoolConfig cfg = st_config();
  cfg.lanes[0].max_queued_events = 1;  // one filler saturates the lane
  LanePool pool(cfg);
  Probe filler;
  ASSERT_EQ(pool.post(0, make(filler, 0)), Post::OK);
  // Budget-governed post on the full lane: SHED (baseline).
  Probe shed_probe;
  EXPECT_EQ(pool.post(0, make(shed_probe, 1)), Post::SHED_FULL);
  // Budget-exempt marker intake on the SAME full lane: OK, never shed.
  Probe marker_probe;
  EXPECT_EQ(pool.post_resume_marker(0, make(marker_probe, 2)), Post::OK);
  // Domain members on the full lane: OK too — population is bounded by
  // live frames, not by the lane budget.
  auto g = std::make_shared<SerialGroup>(pool, 0, 16,
                                         /*resume_domain=*/true);
  g->bind_self(g);
  Probe member;
  EXPECT_EQ(g->post_resume_member(make(member, 3)), Post::OK);
  pool.drain_all();
  EXPECT_EQ(member.ran.load(), 1) << "domain member ran";
  EXPECT_EQ(marker_probe.ran.load(), 1);
  EXPECT_EQ(shed_probe.dropped.load(), 1);
}

TEST(SerialDomainGauntlet, DropExitRaceLateProducerNotStranded) {
  // The lost-wakeup row, DETERMINISTIC via the
  // exit seam: a producer that lands between drain_drop's empty
  // observation and its wake-bit exit must not strand. The old bare
  // store(0) protocol lost this wakeup forever (the member stayed
  // queued with no marker); the CAS exit re-enqueues a fresh marker
  // through the residue.
  SerialGroup::dbg_reset();
  LanePool pool(st_config());
  auto g = std::make_shared<SerialGroup>(pool, 0, 16,
                                         /*resume_domain=*/true);
  g->bind_self(g);
  static Probe p;
  p.ran.store(0);
  p.dropped.store(0);
  ASSERT_EQ(g->post_resume_member(make(p, 0)), Post::OK);
  pool.close();
  struct SeamCtx {
    SerialGroup* g;
    std::atomic<bool> injected{false};
  };
  static SeamCtx sc;
  sc.g = g.get();
  sc.injected.store(false);
  SerialGroup::drain_drop_exit_seam = [](void* c) {
    auto* s = static_cast<SeamCtx*>(c);
    if (!s->injected.exchange(true)) {
      // The LATE producer: pushes to q_, sees the wake bits set →
      // posts no marker of its own — exactly the race window.
      (void)s->g->post_resume_member(make(p, 1));
    }
  };
  SerialGroup::drain_drop_exit_seam_ctx = &sc;
  pool.join();  // marker drops; seam injects; CAS-exit re-enqueues
  SerialGroup::drain_drop_exit_seam = nullptr;
  EXPECT_TRUE(sc.injected.load());
  EXPECT_EQ(p.dropped.load(), 2)
      << "BOTH members delivered — the late one via the re-enqueued "
         "marker (the old protocol stranded it forever)";
  EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(),
            SerialGroup::dbg_cells_freed.load())
      << "every marker cell deleted exactly once";
  EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(), 2)
      << "exactly one FRESH cell was minted by the CAS-exit re-enqueue";
}

TEST(SerialDomainGauntlet, MarkerDroppedWhileNotifiedTerminates) {
  // The entry-exchange row. Red TARGET: the
  // INTERMEDIATE protocol — CAS exit present but entry
  // exchange(RUNNING) missing — where a dropped-while-NOTIFIED marker
  // re-enqueues an infinite residue chain. (The ORIGINAL bare-store(0)
  // protocol terminates this row trivially; ITS red row is the
  // late-producer race above.)
  //
  // TRULY bounded watchdog (drain_residue_now()
  // itself loops to empty, so an in-call chain never returns): the
  // exit seam fires on EVERY chain link — after 8 links it forces the
  // wake-bit to RUNNING so the next CAS succeeds and the chain ENDS,
  // regardless of how broken the protocol is. The violation is then a
  // red cells_alloc assertion, never a hang.
  SerialGroup::dbg_reset();
  LanePool pool(st_config());
  auto g = std::make_shared<SerialGroup>(pool, 0, 16,
                                         /*resume_domain=*/true);
  g->bind_self(g);
  Probe p;
  ASSERT_EQ(g->post_resume_member(make(p, 0)), Post::OK);
  struct SeamCtx {
    SerialGroup* g;
    std::atomic<int> links{0};
  };
  static SeamCtx sc;
  sc.g = g.get();
  sc.links.store(0);
  SerialGroup::drain_drop_exit_seam = [](void* c) {
    auto* x = static_cast<SeamCtx*>(c);
    if (x->links.fetch_add(1, std::memory_order_acq_rel) + 1 >= 8) {
      x->g->dbg_force_running();  // bounded-step escape
    }
  };
  SerialGroup::drain_drop_exit_seam_ctx = &sc;
  pool.close();  // the queued marker will DROP, still NOTIFIED
  pool.join();   // chain length is seam-bounded even if the protocol
                 // regresses — join cannot hang
  SerialGroup::drain_drop_exit_seam = nullptr;
  EXPECT_EQ(p.dropped.load(), 1) << "member delivered through the drop";
  EXPECT_LE(SerialGroup::dbg_cells_alloc.load(), 4)
      << "entry exchange(RUNNING) missing: the dropped-while-NOTIFIED "
         "marker chain kept re-enqueuing (seam-bounded at 8 links)";
  EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(),
            SerialGroup::dbg_cells_freed.load());
}

// Verdict truthfulness on the closed+IDLE
// path — post_resume_member CONSUMES (the push precedes everything),
// so the verdict is OK even when the pool has closed and the marker
// itself rode the residue; a contract-following caller must NOT
// re-deliver. Exactly-once delivery is the proof.
TEST(SerialDomainGauntlet, ClosedIdleMemberConsumedOnceVerdictOk) {
  SerialGroup::dbg_reset();
  LanePool pool(st_config());
  auto g = std::make_shared<SerialGroup>(pool, 0, 16,
                                         /*resume_domain=*/true);
  g->bind_self(g);
  pool.close();  // group is IDLE, pool already closed
  Probe p;
  EXPECT_EQ(g->post_resume_member(make(p, 0)), Post::OK)
      << "the group OWNS the member (push happened) — REFUSED_CLOSED "
         "here would instruct a double-delivery";
  pool.join();  // residue marker delivers the member's drop
  EXPECT_EQ(p.dropped.load(), 1) << "delivered exactly once";
  EXPECT_EQ(p.ran.load(), 0);
  EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(),
            SerialGroup::dbg_cells_freed.load());
}

// The deterministic half: a REAL close-phase
// overlap — the OLD marker is still inside its drop tail (the exit
// seam window) when a late member forces a FRESH residue marker, so
// two markers are provably live at once (dbg_markers_high >= 2), and
// the teardown bound (<= 3) still holds.
TEST(SerialDomainGauntlet, TeardownOverlapTwoMarkersProvablyLive) {
  SerialGroup::dbg_reset();
  LanePool pool(st_config());
  auto g = std::make_shared<SerialGroup>(pool, 0, 16,
                                         /*resume_domain=*/true);
  g->bind_self(g);
  static Probe p;
  p.ran.store(0);
  p.dropped.store(0);
  ASSERT_EQ(g->post_resume_member(make(p, 0)), Post::OK);
  pool.close();
  struct SeamCtx {
    SerialGroup* g;
    std::atomic<bool> injected{false};
  };
  static SeamCtx sc;
  sc.g = g.get();
  sc.injected.store(false);
  SerialGroup::drain_drop_exit_seam = [](void* c) {
    auto* x = static_cast<SeamCtx*>(c);
    if (!x->injected.exchange(true)) {
      // Late member while the OLD marker is still inside its drop
      // tail: the exit CAS (which runs AFTER this seam returns) will
      // fail and mint a FRESH residue marker — at that mint the old
      // marker's live-decrement has not yet happened, so the
      // dbg_marker_inc high-water records 2 DETERMINISTICALLY.
      (void)x->g->post_resume_member(make(p, 1));
    }
  };
  SerialGroup::drain_drop_exit_seam_ctx = &sc;
  pool.join();
  SerialGroup::drain_drop_exit_seam = nullptr;
  EXPECT_TRUE(sc.injected.load());
  EXPECT_GE(SerialGroup::dbg_markers_high.load(), 2)
      << "PROOF of overlap: the fresh residue marker was minted while "
         "the old marker was still live (inside its drop tail)";
  EXPECT_LE(SerialGroup::dbg_markers_high.load(), 3)
      << "teardown population bound (effective_drainers + 2)";
  EXPECT_EQ(p.dropped.load(), 2) << "both members delivered";
  EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(),
            SerialGroup::dbg_cells_freed.load());
}

namespace {
// Hammer blocker member: its run PARKS until released — holding the
// domain's drain slice open so close() is guaranteed to land with
// backlog on EVERY config (cfg 2's workers could otherwise
// outrun the producer).
std::atomic<bool> g_blocker_started{false};
std::atomic<bool> g_blocker_release{false};
// Cross-thread rendezvous proof: set by a SECOND
// domain's residue-marker member, delivered on the MAIN thread while
// the blocker provably parks the FIRST domain's marker on the
// worker/drainer thread.
std::atomic<bool> g_overlap_proven{false};
void overlap_probe_drop(void*) {
  g_overlap_proven.store(
      g_blocker_started.load(std::memory_order_acquire) &&
          !g_blocker_release.load(std::memory_order_acquire),
      std::memory_order_release);
}
void blocker_run(void* ctx) {
  g_blocker_started.store(true, std::memory_order_release);
  while (!g_blocker_release.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
  drop_cb(ctx);  // account as dropped (delivered) via the probe
}
}  // namespace

TEST(SerialDomainGauntlet, MarkerPopulationBoundsAcrossConfigs) {
  // Phase-split bounds across THREE configs:
  //   cfg 0 — ST; cfg 1 — mixed MT, target lane UNWORKED;
  //   cfg 2 — literal ALL-worker MT ({1,1,1,1}), target lane 3.
  // Deterministic teardown realism:
  //   * a BLOCKER member parks the drain slice, so close() lands with
  //     guaranteed backlog on every config (asserted);
  //   * self-drain configs deliver EXACTLY 120 before the blocker
  //     (asserted ==120);
  //   * during teardown, the exit seam injects ONE late member per
  //     config — a residue marker is GUARANTEED to exist, and the main
  //     thread's residue loop must deliver it concurrently with the
  //     drop-drainer/worker deliveries (start-barrier ordered,
  //     asserted by the late member's delivery).
  for (int cfg_id = 0; cfg_id < 3; ++cfg_id) {
    SerialGroup::dbg_reset();
    g_blocker_started.store(false);
    g_blocker_release.store(false);
    PoolConfig cfg = st_config();
    if (cfg_id >= 1) {
      cfg.lanes.resize(4);
      for (int i = 1; i < 4; i++) cfg.lanes[i] = cfg.lanes[0];
      cfg.st_wrr_weights = {8, 4, 2, 1};
      cfg.workers_per_lane =
          (cfg_id == 1) ? std::vector<uint32_t>{1, 0, 0, 0}
                        : std::vector<uint32_t>{1, 1, 1, 1};
    }
    const size_t target_lane = (cfg_id >= 1) ? 3 : 0;
    const bool self_drain = (cfg_id <= 1);
    {
      LanePool pool(cfg);
      auto g = std::make_shared<SerialGroup>(pool, target_lane, 4,
                                             /*resume_domain=*/true);
      g->bind_self(g);
      static Probe p;
      p.ran.store(0);
      p.dropped.store(0);
      {
        std::lock_guard<std::mutex> lk(p.order_m);
        p.order.clear();
      }
      auto drain_step = [&]() {
        bool any = (cfg_id == 1) ? pool.drain_one_unworked()
                                 : pool.drain_one();
        if (!any) pool.wait_for_work(std::chrono::milliseconds(1));
        return any;
      };
      // Phase A (self-drain only): 120 exact deliveries.
      for (int i = 0; i < 120; i++) {
        (void)g->post_resume_member(make(p, i));
      }
      if (self_drain) {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (p.ran.load() < 120 &&
               std::chrono::steady_clock::now() < deadline) {
          (void)drain_step();
        }
        ASSERT_EQ(p.ran.load(), 120) << "cfg " << cfg_id;
      }
      // Phase B: the BLOCKER parks the drain slice, then 79 more
      // members queue behind it — guaranteed backlog on every config.
      Work blocker = make(p, 900);
      blocker.run = blocker_run;
      ASSERT_EQ(g->post_resume_member(blocker), Post::OK);
      for (int i = 0; i < 79; i++) {
        (void)g->post_resume_member(make(p, 200 + i));
      }
      std::thread pre_drainer;
      std::atomic<bool> pre_stop{false};
      if (self_drain) {
        pre_drainer = std::thread([&] {
          while (!pre_stop.load(std::memory_order_acquire)) {
            (void)drain_step();
          }
        });
      }
      auto bdead =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!g_blocker_started.load(std::memory_order_acquire) &&
             std::chrono::steady_clock::now() < bdead) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      ASSERT_TRUE(g_blocker_started.load()) << "cfg " << cfg_id;
      const int delivered_at_close = p.ran.load() + p.dropped.load();
      ASSERT_LT(delivered_at_close, 200)
          << "backlog GUARANTEED at close (cfg " << cfg_id << ")";
      const int live_high = SerialGroup::dbg_markers_high.load();
      pool.close();
      // ---- Cross-thread rendezvous: while the blocker
      // provably parks domain g1's marker MID-RUN on the worker/
      // drainer thread (started && !released), a SECOND domain posts a
      // member on the CLOSED pool — its marker lands in the residue by
      // construction — and THIS thread delivers it. Two REAL domain
      // markers execute concurrently on two threads, deterministically.
      {
        auto g2 = std::make_shared<SerialGroup>(pool, target_lane, 4,
                                                /*resume_domain=*/true);
        g2->bind_self(g2);
        g_overlap_proven.store(false);
        Work probe{};
        probe.drop = overlap_probe_drop;
        ASSERT_EQ(g2->post_resume_member(probe), Post::OK);
        auto rdead =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!g_overlap_proven.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < rdead) {
          (void)pool.drain_residue_now();
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        EXPECT_TRUE(g_overlap_proven.load())
            << "PROOF of cross-thread overlap: g2's residue marker "
               "delivered on the main thread while g1's marker was "
               "still parked mid-run on the worker/drainer (cfg "
            << cfg_id << ")";
      }
      // Teardown overlap machinery: seam-inject ONE late member (a
      // residue marker is then guaranteed), release the blocker, and
      // drain residue on THIS thread concurrently with the
      // drop-drainer/worker deliveries.
      struct SeamCtx {
        SerialGroup* g;
        std::atomic<bool> injected{false};
      };
      static SeamCtx sc;
      sc.g = g.get();
      sc.injected.store(false);
      SerialGroup::drain_drop_exit_seam = [](void* c) {
        auto* x = static_cast<SeamCtx*>(c);
        if (!x->injected.exchange(true)) {
          (void)x->g->post_resume_member(make(p, 999));
        }
      };
      SerialGroup::drain_drop_exit_seam_ctx = &sc;
      g_blocker_release.store(true, std::memory_order_release);
      std::atomic<bool> drop_started{false};
      std::thread drop_drainer;
      if (self_drain) {
        // pre_drainer keeps draining closed-lane DROPS; barrier on its
        // continued liveness is implicit (it set the blocker running).
        drop_started.store(true);
      } else {
        drop_started.store(true);  // cfg 2: pool workers deliver drops
      }
      auto tdead =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (p.ran.load() + p.dropped.load() < 201 &&
             std::chrono::steady_clock::now() < tdead) {
        (void)pool.drain_residue_now();  // concurrent residue drain
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
      if (self_drain) {
        pre_stop.store(true, std::memory_order_release);
        pre_drainer.join();
      }
      pool.join();
      SerialGroup::drain_drop_exit_seam = nullptr;
      EXPECT_TRUE(sc.injected.load())
          << "the teardown seam fired (cfg " << cfg_id << ")";
      EXPECT_EQ(p.ran.load() + p.dropped.load(), 201)
          << "every member (120 + blocker + 79 + the seam-injected "
             "late one) delivered exactly once (cfg " << cfg_id << ")";
      EXPECT_LE(live_high, 2)
          << "live-phase marker population ≤ effective_drainers + 1 "
             "(cfg " << cfg_id << ")";
      EXPECT_LE(SerialGroup::dbg_markers_high.load(), 3)
          << "teardown-phase population ≤ effective_drainers + 2 "
             "(cfg " << cfg_id << ")";
      EXPECT_EQ(SerialGroup::dbg_cells_alloc.load(),
                SerialGroup::dbg_cells_freed.load())
          << "cell single-delete (cfg " << cfg_id << ")";
      (void)drop_started;
    }
  }
}
