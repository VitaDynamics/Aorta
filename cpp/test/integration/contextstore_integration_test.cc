// C++ SDK ContextStore integration test.
//
// Drives the full FFI boundary: Node::CreateContextStore -> typed
// Set*/Get* round-trip -> Watch / WatchWithInitialSnapshot -> RAII
// teardown via WatchHandle / ContextStore destructors. Cross-language
// behaviour (Rust writer, C++ reader) is exercised in
// contextstore_cross_language_test.cc.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "aorta/contextstore.h"
#include "aorta/node.h"
#include "integration/network_test_fixture.h"
#include "throwing_callback_test_access.h"

namespace aorta {
namespace testing {

namespace {

// Destruction latch for teardown-completion signalling: a
// shared_ptr captured by a watch callback; when the callback State is
// freed, the last copy drops and the weak_ptr expires — a waitable
// signal that teardown finished, instead of a fixed sleep.
//
// The latch also records the thread that destroyed it (the destructor's
// thread for synchronous teardown, the reaper thread for deferred
// teardown) into a cell the TEST keeps alive, so the value is readable
// after the latch itself is freed. This discriminates sync-vs-reaper
// deterministically: a normal-thread destructor must destroy State on
// the destructor's thread; a REENTRANT destructor must defer it to the
// reaper thread.
struct StateLatch {
  std::shared_ptr<std::atomic<std::thread::id>> destroyed_on =
      std::make_shared<std::atomic<std::thread::id>>(std::thread::id());
  ~StateLatch() {
    destroyed_on->store(std::this_thread::get_id(), std::memory_order_seq_cst);
  }
};

bool WaitForLatchExpiry(const std::weak_ptr<StateLatch>& weak,
                        std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (weak.expired()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return weak.expired();
}

}  // namespace

class ContextStoreIntegrationTest : public NetworkTestFixture {
 protected:
  void SetUp() override {
    NetworkTestFixture::SetUp();
    auto node_result = CreateTestNode();
    ASSERT_TRUE(node_result.ok());
    node_ = node_result.value();
  }

  std::shared_ptr<Node> node_;
};

TEST_F(ContextStoreIntegrationTest, CreateAndShutdown) {
  auto store_result = node_->CreateContextStore();
  ASSERT_TRUE(store_result.ok()) << store_result.status().message();
  auto store = store_result.value();
  EXPECT_TRUE(store->Shutdown().ok());
}

TEST_F(ContextStoreIntegrationTest, SetGetBoolRoundTrip) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_bool/**").ok());

  ASSERT_TRUE(store->SetBool("system_cpp_bool/flag", true).ok());
  auto read = store->GetBool("system_cpp_bool/flag");
  ASSERT_TRUE(read.ok());
  EXPECT_TRUE(read.value().value);
  EXPECT_TRUE(read.value().is_fresh);
  EXPECT_EQ(read.value().sequence, 1u);
  EXPECT_EQ(read.value().source_instance_id.size(), 16u);
  EXPECT_FALSE(read.value().source_node.empty());
}

TEST_F(ContextStoreIntegrationTest, SetGetForEveryTypedKind) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_kinds/**").ok());

  ASSERT_TRUE(store->SetI64("system_cpp_kinds/i", -100).ok());
  ASSERT_TRUE(store->SetU64("system_cpp_kinds/u", 200).ok());
  ASSERT_TRUE(store->SetF64("system_cpp_kinds/f", 3.5).ok());
  ASSERT_TRUE(store->SetString("system_cpp_kinds/s", "hello").ok());

  EXPECT_EQ(store->GetI64("system_cpp_kinds/i").value().value, -100);
  EXPECT_EQ(store->GetU64("system_cpp_kinds/u").value().value, 200u);
  EXPECT_DOUBLE_EQ(store->GetF64("system_cpp_kinds/f").value().value, 3.5);
  EXPECT_EQ(store->GetString("system_cpp_kinds/s").value().value, "hello");
}

TEST_F(ContextStoreIntegrationTest, KindMismatchSurfacesAsError) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_mm/**").ok());
  ASSERT_TRUE(store->SetU64("system_cpp_mm/k", 1).ok());

  auto bad = store->GetString("system_cpp_mm/k");
  ASSERT_FALSE(bad.ok());
  EXPECT_EQ(bad.status().code(), AORTA_ERR_KIND_MISMATCH);
}

TEST_F(ContextStoreIntegrationTest, SetWithoutDeclareReturnsPrefixNotDeclared) {
  auto store = node_->CreateContextStore().value();
  auto status = store->SetBool("rcp_cpp_undecl/state", true);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_PREFIX_NOT_DECLARED);
}

TEST_F(ContextStoreIntegrationTest, WatchReceivesSelfSetEvent) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_watch/**").ok());

  std::atomic<int> live_count{0};
  std::mutex captured_mu;
  ContextChange captured;
  bool captured_ready = false;

  auto watch_result = store->Watch(
      "system_cpp_watch/**", [&](const ContextChange& c) {
        if (c.kind == ContextChangeKind::kLive) {
          {
            std::lock_guard<std::mutex> lock(captured_mu);
            captured = c;
            captured_ready = true;
          }
          live_count++;
        }
      });
  ASSERT_TRUE(watch_result.ok()) << watch_result.status().message();
  WatchHandle watch = std::move(watch_result.value());
  EXPECT_TRUE(watch.valid());

  ASSERT_TRUE(store->SetU64("system_cpp_watch/state", 42).ok());

  // Wait up to 2s for the event.
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (live_count.load() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(live_count.load(), 1);

  std::lock_guard<std::mutex> lock(captured_mu);
  ASSERT_TRUE(captured_ready);
  EXPECT_EQ(captured.kind, ContextChangeKind::kLive);
  EXPECT_EQ(captured.value_kind, ContextKind::kU64);
  EXPECT_EQ(captured.u64_value, 42u);
  EXPECT_TRUE(
      captured.key.find("system_cpp_watch/state") != std::string::npos);
}

TEST_F(ContextStoreIntegrationTest, WatchHandleResetStopsCallbacks) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_drop/**").ok());

  std::atomic<int> count{0};
  auto watch_result = store->Watch(
      "system_cpp_drop/**",
      [&](const ContextChange& c) {
        if (c.kind == ContextChangeKind::kLive) {
          count++;
        }
      });
  ASSERT_TRUE(watch_result.ok());
  WatchHandle watch = std::move(watch_result.value());

  ASSERT_TRUE(store->SetBool("system_cpp_drop/k", true).ok());

  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (count.load() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_EQ(count.load(), 1);

  // Reset awaits Zenoh undeclare so no further callback fires.
  EXPECT_TRUE(watch.Reset().ok());
  EXPECT_FALSE(watch.valid());

  ASSERT_TRUE(store->SetBool("system_cpp_drop/k", false).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(count.load(), 1);
}

TEST_F(ContextStoreIntegrationTest, WatchWithInitialSnapshotDeliversDoneMarker) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("rcp_cpp_snap/**").ok());
  ASSERT_TRUE(store->SetU64("rcp_cpp_snap/state", 7).ok());

  // Allow Zenoh routing to settle.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::mutex events_mu;
  std::vector<ContextChange> events;
  std::atomic<bool> snapshot_done{false};

  WatchSnapshotOptions opts;
  opts.snapshot_deadline = std::chrono::milliseconds(800);
  auto watch_result = store->WatchWithInitialSnapshot(
      "rcp_cpp_snap/**", opts, [&](const ContextChange& c) {
        std::lock_guard<std::mutex> lock(events_mu);
        events.push_back(c);
        if (c.kind == ContextChangeKind::kSnapshotDone) {
          snapshot_done.store(true);
        }
      });
  ASSERT_TRUE(watch_result.ok()) << watch_result.status().message();
  WatchHandle watch = std::move(watch_result.value());

  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!snapshot_done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(snapshot_done.load());

  std::lock_guard<std::mutex> lock(events_mu);
  bool saw_snapshot = false;
  bool saw_done = false;
  for (const auto& e : events) {
    if (e.kind == ContextChangeKind::kSnapshot &&
        e.key.find("rcp_cpp_snap/state") != std::string::npos) {
      saw_snapshot = true;
    }
    if (e.kind == ContextChangeKind::kSnapshotDone) {
      saw_done = true;
      ASSERT_TRUE(e.snapshot_done_reason.has_value());
    }
  }
  EXPECT_TRUE(saw_snapshot);
  EXPECT_TRUE(saw_done);
}

TEST_F(ContextStoreIntegrationTest, WatchHandleMoveAssignment) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_move/**").ok());
  auto first = store->Watch("system_cpp_move/**",
                            [](const ContextChange&) {})
                   .value();
  EXPECT_TRUE(first.valid());

  WatchHandle moved = std::move(first);
  EXPECT_TRUE(moved.valid());
  EXPECT_FALSE(first.valid());

  // Move-assign over a live handle: the previous handle must Reset.
  auto second = store->Watch("system_cpp_move/**",
                             [](const ContextChange&) {})
                    .value();
  moved = std::move(second);
  EXPECT_TRUE(moved.valid());
  EXPECT_FALSE(second.valid());
}

TEST_F(ContextStoreIntegrationTest, WatchRejectsNullCallback) {
  auto store = node_->CreateContextStore().value();
  auto result = store->Watch("system_cpp_null/**",
                             ContextStore::ChangeCallback{});
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST_F(ContextStoreIntegrationTest, OwnerLivelinessWatchSeesAliveAndGone) {
  // Owner store on a second in-process node (the fixture's CreateTestNode
  // can only create one node per test).
  auto owner_node = Node::Create("ctxE_cpp_liv_owner", "default");
  ASSERT_TRUE(owner_node.ok()) << owner_node.status().message();
  auto owner = owner_node.value()->CreateContextStore();
  ASSERT_TRUE(owner.ok()) << owner.status().message();

  // Watcher: a store on the fixture node that never declares a prefix.
  auto watcher = node_->CreateContextStore();
  ASSERT_TRUE(watcher.ok()) << watcher.status().message();

  std::mutex events_mu;
  std::atomic<bool> saw_alive{false};
  std::atomic<bool> saw_gone{false};
  std::string alive_node;
  std::string alive_instance;
  std::string gone_instance;

  auto watch_result = watcher.value()->WatchOwnerLiveliness(
      [&](const OwnerLivelinessEvent& e) {
        std::lock_guard<std::mutex> lock(events_mu);
        if (e.alive) {
          saw_alive.store(true);
          alive_node = e.node_name;
          alive_instance = e.source_instance_id;
        } else {
          saw_gone.store(true);
          gone_instance = e.source_instance_id;
        }
      });
  ASSERT_TRUE(watch_result.ok()) << watch_result.status().message();
  OwnerLivelinessWatchHandle watch = std::move(watch_result.value());
  EXPECT_TRUE(watch.valid());

  // No owner yet: nothing may fire before any declaration exists.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  {
    std::lock_guard<std::mutex> lock(events_mu);
    EXPECT_FALSE(saw_alive.load());
    EXPECT_FALSE(saw_gone.load());
  }

  // Owner declares -> alive event with the right identity.
  ASSERT_TRUE(owner.value()->DeclareOwnedPrefix("system_cpp_liv/**").ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!saw_alive.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(saw_alive.load()) << "no alive event after declare";
  {
    std::lock_guard<std::mutex> lock(events_mu);
    EXPECT_EQ(alive_node, "ctxE_cpp_liv_owner");
    EXPECT_EQ(alive_instance.size(), 16u);
  }

  // Owner shutdown -> gone event for the same instance.
  ASSERT_TRUE(owner.value()->Shutdown().ok());
  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!saw_gone.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(saw_gone.load()) << "no gone event after shutdown";
  {
    std::lock_guard<std::mutex> lock(events_mu);
    EXPECT_EQ(gone_instance, alive_instance);
  }

  // Reset awaits the undeclare: no callback may fire afterwards.
  EXPECT_TRUE(watch.Reset().ok());
  EXPECT_FALSE(watch.valid());
}

TEST_F(ContextStoreIntegrationTest,
         WatchMoveAssignLeaksIrrecoverableStateAndAdoptsRhs) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_mvab/**").ok());

  std::atomic<int> a_hits{0};
  std::atomic<int> b_hits{0};
  auto a = store->Watch("system_cpp_mvab/**",
                        [&](const ContextChange&) {
                          a_hits.fetch_add(1, std::memory_order_relaxed);
                        })
               .value();
  auto b = store->Watch("system_cpp_mvab/**",
                        [&](const ContextChange&) {
                          b_hits.fetch_add(1, std::memory_order_relaxed);
                        })
               .value();

  // Force a's teardown to fail: free the FFI watch behind the wrapper.
  ASSERT_EQ(aorta_ctx_watch_free(aorta::test_access::WatchCallbackTestAccess::
                                     RawHandle(a)),
            AORTA_OK);

  a = std::move(b);
  // The stale LHS state is leaked because callback drain was not
  // confirmed; the RHS is still adopted normally.
  EXPECT_TRUE(a.valid());
  EXPECT_FALSE(b.valid());

  ASSERT_TRUE(store->SetBool("system_cpp_mvab/k", true).ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (b_hits.load() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(b_hits.load(), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(a_hits.load(), 0) << "freed FFI watch must never deliver";

  EXPECT_TRUE(a.Reset().ok());
}

TEST_F(ContextStoreIntegrationTest,
       WatchResetReentrantRetainsAndSafeThreadRetrySucceeds) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_reset_retry/**").ok());

  std::atomic<int32_t> reset_code{AORTA_ERR_INTERNAL};
  std::atomic<bool> valid_after_reset{false};
  std::atomic<bool> attempted{false};
  auto target = std::make_unique<WatchHandle>(
      store->Watch("system_cpp_reset_retry/**", [](const ContextChange&) {})
          .value());
  auto trigger = store->Watch(
      "system_cpp_reset_retry/**", [&](const ContextChange&) {
        if (attempted.exchange(true)) {
          return;
        }
        const Status status = target->Reset();
        reset_code.store(status.code(), std::memory_order_release);
        valid_after_reset.store(target->valid(), std::memory_order_release);
      });
  ASSERT_TRUE(trigger.ok()) << trigger.status().message();

  ASSERT_TRUE(store->SetBool("system_cpp_reset_retry/k", true).ok());
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!attempted.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(attempted.load(std::memory_order_acquire));
  EXPECT_EQ(reset_code.load(std::memory_order_acquire), AORTA_ERR_REENTRANT);
  EXPECT_TRUE(valid_after_reset.load(std::memory_order_acquire));
  EXPECT_TRUE(target->valid());

  EXPECT_TRUE(target->Reset().ok());
  EXPECT_FALSE(target->valid());
}

TEST_F(ContextStoreIntegrationTest,
       WatchMoveAssignmentReentrantLeaksOldStateAndAdoptsRhs) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_move_reentrant/**").ok());

  auto old_state_latch = std::make_shared<StateLatch>();
  std::weak_ptr<StateLatch> old_state_weak = old_state_latch;
  WatchHandle lhs =
      store
          ->Watch("system_cpp_move_reentrant/**",
                  [latch = std::move(old_state_latch)](const ContextChange&) {})
          .value();
  const aorta_ctx_watch_t old_raw =
      aorta::test_access::WatchCallbackTestAccess::RawHandle(lhs);

  std::atomic<int> rhs_hits{0};
  WatchHandle rhs =
      store
          ->Watch("system_cpp_move_reentrant/**", [&](const ContextChange&) {
            rhs_hits.fetch_add(1, std::memory_order_relaxed);
          })
          .value();
  std::atomic<bool> moved{false};
  WatchHandle trigger =
      store
          ->Watch("system_cpp_move_reentrant/**", [&](const ContextChange&) {
            if (!moved.exchange(true)) {
              lhs = std::move(rhs);
            }
          })
          .value();

  ASSERT_TRUE(store->SetBool("system_cpp_move_reentrant/k", true).ok());
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!moved.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(moved.load(std::memory_order_acquire));
  EXPECT_TRUE(lhs.valid());
  EXPECT_FALSE(rhs.valid());
  EXPECT_FALSE(old_state_weak.expired());

  ASSERT_EQ(aorta_ctx_watch_free(old_raw), AORTA_OK);
  EXPECT_TRUE(lhs.Reset().ok());
  EXPECT_TRUE(trigger.Reset().ok());
}

TEST_F(ContextStoreIntegrationTest,
         WatchDestructorReentrantLeaksStateAndRetainsCoreWatch) {
  // Residual P1 #6: destroying a WatchHandle from INSIDE a callback
  // (a runtime context) hits REENTRANT — the FFI re-inserts the watch.
  // The destructor must leak the callback state and leave the core-side
  // watch registered. A detached retry would silently forfeit the frozen
  // observable semantics and make teardown completion timing-dependent.
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_reap/**").ok());

  std::atomic<int> a_hits{0};  // positive control: still-alive watch
  std::atomic<int> b_hits{0};
  std::atomic<bool> b_destroyed{false};

  // B lives on the heap so A's callback can destroy it. The latch
  // expires when B's callback State is freed (the reaper's last act);
  // `destroyed_on` records the destroying thread (must be the REAPER
  // thread, not the test thread — the deterministic sync-vs-reaper
  // discrimination).
  auto b_latch = std::make_shared<StateLatch>();
  auto b_destroyed_on = b_latch->destroyed_on;
  std::weak_ptr<StateLatch> b_latch_weak = b_latch;
  auto b_holder = std::make_unique<WatchHandle>(
      store
          ->Watch("system_cpp_reap/**",
                  [latch = std::move(b_latch), &b_hits](const ContextChange&) {
                    b_hits.fetch_add(1, std::memory_order_relaxed);
                  })
          .value());
  const aorta_ctx_watch_t b_raw =
      aorta::test_access::WatchCallbackTestAccess::RawHandle(*b_holder);

  auto watch_a = store->Watch("system_cpp_reap/**", [&](const ContextChange&) {
    a_hits.fetch_add(1, std::memory_order_relaxed);
    if (b_destroyed.exchange(true)) {
      return;
    }
    // Destroy B from inside this callback (runtime context): B's
    // destructor hits REENTRANT and defers to the reaper.
    b_holder.reset();
  });
  ASSERT_TRUE(watch_a.ok()) << watch_a.status().message();
  WatchHandle a = std::move(watch_a.value());

  ASSERT_TRUE(store->SetBool("system_cpp_reap/k", true).ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!b_destroyed.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(b_destroyed.load()) << "B was never destroyed in-callback";
  EXPECT_FALSE(b_latch_weak.expired())
      << "REENTRANT destruction must deliberately leak callback state";
  EXPECT_EQ(b_destroyed_on->load(), std::thread::id());
  const int hits_at_destroy = b_hits.load();
  const int a_baseline = a_hits.load();

  // Both the owned A and the deliberately leaked/re-inserted B remain live.
  ASSERT_TRUE(store->SetBool("system_cpp_reap/k", false).ok());
  ASSERT_TRUE(store->SetBool("system_cpp_reap/k", true).ok());
  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((a_hits.load() <= a_baseline || b_hits.load() <= hits_at_destroy) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(a_hits.load(), a_baseline)
      << "positive control: the live watch must receive post-teardown events";
  EXPECT_GT(b_hits.load(), hits_at_destroy)
      << "the re-inserted watch remains registered after destructor leakage";

  ASSERT_EQ(aorta_ctx_watch_free(b_raw), AORTA_OK);

  // a tears down normally at scope end.
}

TEST_F(ContextStoreIntegrationTest,
       WatchResetIrrecoverableErrorClearsHandleAndLeaksState) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_reset_error/**").ok());

  auto latch = std::make_shared<StateLatch>();
  std::weak_ptr<StateLatch> latch_weak = latch;
  WatchHandle watch =
      store
          ->Watch("system_cpp_reset_error/**",
                  [latch = std::move(latch)](const ContextChange&) {})
          .value();
  const aorta_ctx_watch_t raw =
      aorta::test_access::WatchCallbackTestAccess::RawHandle(watch);
  ASSERT_EQ(aorta_ctx_watch_free(raw), AORTA_OK);

  const Status status = watch.Reset();
  EXPECT_EQ(status.code(), AORTA_ERR_NOT_FOUND);
  EXPECT_FALSE(watch.valid());
  EXPECT_FALSE(latch_weak.expired());
  EXPECT_TRUE(watch.Reset().ok());
  EXPECT_EQ(aorta_ctx_watch_free(raw), AORTA_ERR_NOT_FOUND);
}

TEST_F(ContextStoreIntegrationTest,
       WatchResetWaitsForInFlightCallbackBeforeStateRelease) {
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_drain/**").ok());

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_started = false;
  bool allow_callback_return = false;
  auto latch = std::make_shared<StateLatch>();
  std::weak_ptr<StateLatch> latch_weak = latch;
  WatchHandle watch =
      store
          ->Watch("system_cpp_drain/**",
                  [&, latch = std::move(latch)](const ContextChange&) {
                    std::unique_lock<std::mutex> lock(mutex);
                    callback_started = true;
                    cv.notify_all();
                    cv.wait(lock, [&] { return allow_callback_return; });
                  })
          .value();

  std::promise<Status> set_promise;
  auto set_future = set_promise.get_future();
  std::thread set_thread(
      [&] { set_promise.set_value(store->SetBool("system_cpp_drain/k", true)); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&] { return callback_started; }));
  }

  std::promise<Status> reset_promise;
  auto reset_future = reset_promise.get_future();
  std::thread reset_thread([&] { reset_promise.set_value(watch.Reset()); });
  EXPECT_EQ(reset_future.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout);
  EXPECT_FALSE(latch_weak.expired());

  {
    std::lock_guard<std::mutex> lock(mutex);
    allow_callback_return = true;
  }
  cv.notify_all();
  ASSERT_EQ(reset_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(reset_future.get().ok());
  reset_thread.join();
  ASSERT_EQ(set_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(set_future.get().ok());
  set_thread.join();
  EXPECT_TRUE(latch_weak.expired());
  EXPECT_FALSE(watch.valid());
}

TEST_F(ContextStoreIntegrationTest,
         WatchDestructorOnNormalThreadTearsDownSynchronously) {
  // On a non-runtime thread the destructor must complete the
  // teardown SYNCHRONOUSLY — when it returns, the callback state is
  // already freed ON THE DESTRUCTOR'S THREAD and no callback can fire
  // afterwards (the documented contract; no reaper thread is spawned
  // for ordinary destruction). The thread-id check discriminates
  // sync-vs-reaper deterministically (an unconditional-reaper
  // destructor would destroy state on the reaper thread and fail the
  // equality even if it happened to finish before the assertion).
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_sync/**").ok());

  std::atomic<int> a_hits{0};  // positive control: still-alive watch
  std::atomic<int> b_hits{0};
  auto b_latch = std::make_shared<StateLatch>();
  auto b_destroyed_on = b_latch->destroyed_on;
  std::weak_ptr<StateLatch> b_latch_weak = b_latch;
  const std::thread::id test_thread = std::this_thread::get_id();
  WatchHandle a =
      store
          ->Watch("system_cpp_sync/**",
                  [&](const ContextChange&) {
                    a_hits.fetch_add(1, std::memory_order_relaxed);
                  })
          .value();
  {
    WatchHandle b =
        store
            ->Watch("system_cpp_sync/**",
                    [latch = std::move(b_latch), &b_hits](
                        const ContextChange&) {
                      b_hits.fetch_add(1, std::memory_order_relaxed);
                    })
            .value();
    // Prove both watches are live before tearing down.
    ASSERT_TRUE(store->SetBool("system_cpp_sync/k", true).ok());
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((a_hits.load() == 0 || b_hits.load() == 0) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GT(a_hits.load(), 0) << "watch A never received an event";
    ASSERT_GT(b_hits.load(), 0) << "watch B never received an event";
  }  // ~b on the main (non-runtime) thread; a stays alive for the
     // positive control

  // Teardown is COMPLETE when the destructor returns: state freed...
  EXPECT_TRUE(b_latch_weak.expired())
      << "callback state must be freed by the destructor (synchronous teardown)";
  // ...on the destructor's OWN thread (synchronous, not reaper).
  EXPECT_EQ(b_destroyed_on->load(), test_thread)
      << "synchronous teardown must destroy state on the destructor's thread";
  const int hits_at_destroy = b_hits.load();
  const int a_baseline = a_hits.load();

  // Positive control (replaces the fixed sleep): the still-alive watch
  // A MUST receive the post-teardown events, while the torn-down B must
  // not.
  ASSERT_TRUE(store->SetBool("system_cpp_sync/k", false).ok());
  ASSERT_TRUE(store->SetBool("system_cpp_sync/k", true).ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (a_hits.load() <= a_baseline &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(a_hits.load(), a_baseline)
      << "positive control: the live watch must receive post-teardown events";
  EXPECT_EQ(b_hits.load(), hits_at_destroy)
      << "no callback may fire after the destructor returned";
}

TEST_F(ContextStoreIntegrationTest,
         WatchDestructorRetainsStateWhenFreeCannotConfirmDrain) {
  // State is freed ONLY on AORTA_OK.
  // This test forces the free to return NOT_FOUND (the stale-handle
  // path — the exact state a retry would see after an earlier
  // panicking free removed the registry entry while a callback could
  // still be in flight; see the Rust-side panic-seam tests
  // ffi_watch_free_panic_after_remove_leaves_entry_gone). NOT_FOUND
  // proves nothing about drain, so the destructor must RETAIN State
  // (bounded leak + rate-limited log). Under ASan this pins that a
  // hypothetical late dispatch can never hit freed memory.
  auto store = node_->CreateContextStore().value();
  ASSERT_TRUE(store->DeclareOwnedPrefix("system_cpp_retain/**").ok());

  std::atomic<int> hits{0};
  auto latch = std::make_shared<StateLatch>();
  std::weak_ptr<StateLatch> latch_weak = latch;
  {
    WatchHandle w =
        store
            ->Watch("system_cpp_retain/**",
                    [latch = std::move(latch), &hits](const ContextChange&) {
                      hits.fetch_add(1, std::memory_order_relaxed);
                    })
            .value();
    // Force the free to return NOT_FOUND: free the FFI watch behind the
    // wrapper (the stale-handle misuse path).
    ASSERT_EQ(aorta_ctx_watch_free(
                  aorta::test_access::WatchCallbackTestAccess::RawHandle(w)),
              AORTA_OK);
    // Drop the test's latch copy so the callback's copy is the last
    // owner.
    latch.reset();
  }  // ~WatchHandle -> free -> NOT_FOUND -> State RETAINED

  EXPECT_FALSE(latch_weak.expired())
      << "State must be retained when the free cannot confirm drain "
         "(NOT_FOUND proves nothing about callbacks)";
}

TEST_F(ContextStoreIntegrationTest,
         OwnerLivelinessWatchMoveAssignAbandonsWhenTeardownFails) {
  auto owner_node = Node::Create("ctxE_cpp_liv_mvab", "default");
  ASSERT_TRUE(owner_node.ok()) << owner_node.status().message();
  auto owner = owner_node.value()->CreateContextStore();
  ASSERT_TRUE(owner.ok()) << owner.status().message();
  auto watcher = node_->CreateContextStore();
  ASSERT_TRUE(watcher.ok()) << watcher.status().message();

  std::atomic<int> a_hits{0};
  std::atomic<int> b_hits{0};
  auto a = watcher.value()
               ->WatchOwnerLiveliness([&](const OwnerLivelinessEvent&) {
                 a_hits.fetch_add(1, std::memory_order_relaxed);
               })
               .value();
  auto b = watcher.value()
               ->WatchOwnerLiveliness([&](const OwnerLivelinessEvent&) {
                 b_hits.fetch_add(1, std::memory_order_relaxed);
               })
               .value();

  ASSERT_EQ(
      aorta_ctx_owner_liveliness_watch_free(
          aorta::test_access::OwnerLivelinessWatchCallbackTestAccess::
              RawHandle(a)),
      AORTA_OK);

  a = std::move(b);
  EXPECT_TRUE(a.valid());
  EXPECT_TRUE(b.valid());

  // B's watch is still registered: the owner's token must reach it.
  ASSERT_TRUE(owner.value()->DeclareOwnedPrefix("system_cpp_liv_mvab/**").ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (b_hits.load() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(b_hits.load(), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(a_hits.load(), 0) << "freed FFI watch must never deliver";

  EXPECT_TRUE(b.Reset().ok());
  ASSERT_TRUE(owner.value()->Shutdown().ok());
}

TEST_F(ContextStoreIntegrationTest,
         OwnerLivelinessWatchDestructorInCallbackContextDefersTeardown) {
  auto owner_node = Node::Create("ctxE_cpp_liv_reap", "default");
  ASSERT_TRUE(owner_node.ok()) << owner_node.status().message();
  auto owner = owner_node.value()->CreateContextStore();
  ASSERT_TRUE(owner.ok()) << owner.status().message();
  auto watcher = node_->CreateContextStore();
  ASSERT_TRUE(watcher.ok()) << watcher.status().message();

  std::atomic<int> a_hits{0};  // positive control: still-alive watch
  std::atomic<int> b_hits{0};
  std::atomic<bool> b_destroyed{false};

  auto b_latch = std::make_shared<StateLatch>();
  auto b_destroyed_on = b_latch->destroyed_on;
  std::weak_ptr<StateLatch> b_latch_weak = b_latch;
  auto b_holder = std::make_unique<OwnerLivelinessWatchHandle>(
      watcher.value()
          ->WatchOwnerLiveliness(
              [latch = std::move(b_latch), &b_hits](const OwnerLivelinessEvent&) {
                b_hits.fetch_add(1, std::memory_order_relaxed);
              })
          .value());

  auto watch_a = watcher.value()->WatchOwnerLiveliness(
      [&](const OwnerLivelinessEvent&) {
        a_hits.fetch_add(1, std::memory_order_relaxed);
        if (b_destroyed.exchange(true)) {
          return;
        }
        b_holder.reset();  // ~OwnerLivelinessWatchHandle in callback context
      });
  ASSERT_TRUE(watch_a.ok()) << watch_a.status().message();
  OwnerLivelinessWatchHandle a = std::move(watch_a.value());

  // The owner declaring brings the token alive; both watches fire.
  ASSERT_TRUE(owner.value()->DeclareOwnedPrefix("system_cpp_liv_reap/**").ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!b_destroyed.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(b_destroyed.load()) << "B was never destroyed in-callback";
  // Wait for the reaper to complete the deferred free (state freed).
  ASSERT_TRUE(WaitForLatchExpiry(b_latch_weak, std::chrono::seconds(5)))
      << "reaper never freed B's callback state";
  // Deterministic discrimination: REENTRANT teardown must be deferred —
  // the state is destroyed on the REAPER thread, never the test thread.
  EXPECT_NE(b_destroyed_on->load(), std::this_thread::get_id())
      << "deferred teardown must destroy state off the test thread";
  const int hits_at_destroy = b_hits.load();
  const int a_baseline = a_hits.load();

  // Positive control: the still-alive watch A MUST receive the
  // post-teardown token withdrawal, while the torn-down B must not.
  ASSERT_TRUE(owner.value()->Shutdown().ok());
  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (a_hits.load() <= a_baseline &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(a_hits.load(), a_baseline)
      << "positive control: the live watch must receive the withdrawal event";
  EXPECT_EQ(b_hits.load(), hits_at_destroy)
      << "no callback may fire after the reaper completed teardown";
}

TEST_F(ContextStoreIntegrationTest,
         OwnerLivelinessWatchDestructorOnNormalThreadTearsDownSynchronously) {
  // Symmetric normal-thread synchronous-teardown contract for the
  // owner-liveliness watch (thread-id discrimination + positive
  // control).
  auto owner_node = Node::Create("ctxE_cpp_liv_sync", "default");
  ASSERT_TRUE(owner_node.ok()) << owner_node.status().message();
  auto owner = owner_node.value()->CreateContextStore();
  ASSERT_TRUE(owner.ok()) << owner.status().message();
  auto watcher = node_->CreateContextStore();
  ASSERT_TRUE(watcher.ok()) << watcher.status().message();

  std::atomic<int> a_hits{0};  // positive control: still-alive watch
  std::atomic<int> b_hits{0};
  auto b_latch = std::make_shared<StateLatch>();
  auto b_destroyed_on = b_latch->destroyed_on;
  std::weak_ptr<StateLatch> b_latch_weak = b_latch;
  const std::thread::id test_thread = std::this_thread::get_id();
  auto watch_a = watcher.value()->WatchOwnerLiveliness(
      [&](const OwnerLivelinessEvent&) {
        a_hits.fetch_add(1, std::memory_order_relaxed);
      });
  ASSERT_TRUE(watch_a.ok()) << watch_a.status().message();
  OwnerLivelinessWatchHandle a = std::move(watch_a.value());
  {
    OwnerLivelinessWatchHandle b =
        watcher.value()
            ->WatchOwnerLiveliness(
                [latch = std::move(b_latch), &b_hits](
                    const OwnerLivelinessEvent&) {
                  b_hits.fetch_add(1, std::memory_order_relaxed);
                })
            .value();
    // Prove both watches are live before tearing down.
    ASSERT_TRUE(owner.value()->DeclareOwnedPrefix("system_cpp_liv_sync/**").ok());
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((a_hits.load() == 0 || b_hits.load() == 0) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GT(a_hits.load(), 0) << "watch A never received an event";
    ASSERT_GT(b_hits.load(), 0) << "watch B never received an event";
  }  // ~b on the main (non-runtime) thread; a stays alive for the
     // positive control

  EXPECT_TRUE(b_latch_weak.expired())
      << "callback state must be freed by the destructor (synchronous teardown)";
  EXPECT_EQ(b_destroyed_on->load(), test_thread)
      << "synchronous teardown must destroy state on the destructor's thread";
  const int hits_at_destroy = b_hits.load();
  const int a_baseline = a_hits.load();

  // Positive control: owner shutdown must reach A but not B.
  ASSERT_TRUE(owner.value()->Shutdown().ok());
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (a_hits.load() <= a_baseline &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(a_hits.load(), a_baseline)
      << "positive control: the live watch must receive the withdrawal event";
  EXPECT_EQ(b_hits.load(), hits_at_destroy)
      << "no callback may fire after the destructor returned";
}

TEST_F(ContextStoreIntegrationTest, OwnerLivelinessWatchRejectsNullCallback) {
  auto store = node_->CreateContextStore().value();
  auto result = store->WatchOwnerLiveliness(
      ContextStore::OwnerLivelinessCallback{});
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

}  // namespace testing
}  // namespace aorta
