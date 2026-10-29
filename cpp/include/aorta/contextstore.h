#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <aorta.h>

#include "aorta/status.h"

namespace aorta {

class Node;

namespace test_access {
struct WatchCallbackTestAccess;
struct OwnerLivelinessWatchCallbackTestAccess;
}  // namespace test_access

/// Mirrors `aorta_core::contextstore_fb::ContextKind`.
enum class ContextKind : uint8_t {
  kNone = AORTA_CTX_KIND_NONE,
  kBool = AORTA_CTX_KIND_BOOL,
  kI64 = AORTA_CTX_KIND_I64,
  kU64 = AORTA_CTX_KIND_U64,
  kF64 = AORTA_CTX_KIND_F64,
  kString = AORTA_CTX_KIND_STRING,
};

/// Mirrors `aorta_core::contextstore::ContextChangeKind`.
enum class ContextChangeKind : uint8_t {
  kLive = AORTA_CTX_CHANGE_LIVE,
  kSnapshot = AORTA_CTX_CHANGE_SNAPSHOT,
  kSnapshotDone = AORTA_CTX_CHANGE_SNAPSHOT_DONE,
};

/// Mirrors `aorta_core::contextstore::SnapshotDoneReason`.
enum class SnapshotDoneReason : uint8_t {
  kQueryDrained = AORTA_CTX_SNAPSHOT_DONE_QUERY_DRAINED,
  kDeadlineElapsed = AORTA_CTX_SNAPSHOT_DONE_DEADLINE_ELAPSED,
};

/// Result of a typed `Get*`. Mirrors `ContextRead<T>` from the Rust API.
template <typename T>
struct ContextRead {
  T value;
  bool is_fresh = false;
  uint64_t stamp_ns = 0;
  uint64_t expires_at_ns = 0;
  std::string source_node;
  std::string source_instance_id;
  uint64_t sequence = 0;
};

/// Decoded change delivered to a watch callback. For
/// `kind == kSnapshotDone`, only `kind` and `snapshot_done_reason` are
/// populated; all other fields are defaulted/empty.
struct ContextChange {
  ContextChangeKind kind = ContextChangeKind::kLive;
  std::optional<SnapshotDoneReason> snapshot_done_reason;
  std::string key;
  ContextKind value_kind = ContextKind::kNone;
  bool bool_value = false;
  int64_t i64_value = 0;
  uint64_t u64_value = 0;
  double f64_value = 0.0;
  std::optional<std::string> string_value;
  uint64_t stamp_ns = 0;
  uint64_t expires_at_ns = 0;
  std::string source_node;
  std::string source_instance_id;
  uint64_t sequence = 0;
};

/// Options for `WatchWithInitialSnapshot`. Default deadline matches the
/// core SDK (1s).
struct WatchSnapshotOptions {
  std::chrono::milliseconds snapshot_deadline{1000};
};

/// One owner-liveliness event (mirrors `OwnerLivelinessEvent` from the
/// Rust core): an owning ContextStore became alive (`alive == true`,
/// token declared at its first `DeclareOwnedPrefix`) or went away
/// (`alive == false`, token withdrawn by shutdown / store drop /
/// process death). `source_instance_id` is the same id carried on every
/// context event and query reply, so consumers can correlate prefixes
/// to instances.
struct OwnerLivelinessEvent {
  std::string node_name;
  std::string source_instance_id;
  bool alive = false;
};

/// RAII wrapper for an active watch. The destructor calls
/// `aorta_ctx_watch_free`, which awaits Zenoh's undeclare so — WHEN it
/// reports AORTA_OK — no further callback fires after the destructor
/// returns (see the teardown bullet below for the non-OK contract).
/// Movable but not copyable.
///
/// **Teardown is fallible and safe from every context.**
///
/// - From a non-runtime thread, `Reset()` / the destructor free the
///   watch synchronously; no callback fires after they return WHEN the
///   FFI reports AORTA_OK. On ANY non-OK result the drain is
///   UNCONFIRMED — a callback may still fire — and the wrapper retains
///   the callback state (bounded leak) so captured objects stay alive;
///   treat a failed Reset/teardown as "the callback may still run" and
///   do not free anything the callback captures.
/// - From a runtime callback context (e.g. destroying a *different*
///   watch's handle inside a callback, or an executor task), the FFI
///   reports `AORTA_ERR_REENTRANT` and re-inserts the watch. The
///   **destructor** then hands ownership to a detached reaper thread
///   that retries the free from a non-runtime context; the callback
///   state stays alive until the free succeeds, so a registered
///   callback can never point at freed memory. Explicit `Reset()` in a
///   callback context returns the error instead and must be retried
///   from outside (the std::promise pattern below).
/// - Move-assignment onto a live handle whose teardown fails (e.g.
///   REENTRANT, or a handle the FFI already freed) ABANDONS the move:
///   both objects keep their watches and state, and teardown must be
///   retried explicitly. The moved-from object keeps its watch (it is
///   never left half-moved).
///
/// The supported pattern for deterministic teardown from a callback is
/// to signal completion (e.g. set a `std::promise`) and tear down the
/// watch from the outside:
///
/// ```cpp
/// std::promise<void> done;
/// auto watch = store->WatchWithInitialSnapshot(prefix, opts,
///     [&](const ContextChange& c) {
///       if (c.kind == ContextChangeKind::kSnapshotDone) {
///         done.set_value();
///       }
///     }).value();
/// done.get_future().wait();
/// // watch goes out of scope here — destructor handles teardown
/// ```
class WatchHandle;

// Dispatcher is `noexcept`. See subscriber.h
// header note for rationale; same try { ... } catch (...) pattern
// here.
extern "C" void aorta_cpp_on_ctx_change(const aorta_ctx_change_t* change,
                                        void* user_data) noexcept;

class WatchHandle {
 public:
  WatchHandle();
  ~WatchHandle();
  WatchHandle(WatchHandle&& other) noexcept;
  WatchHandle& operator=(WatchHandle&& other) noexcept;
  WatchHandle(const WatchHandle&) = delete;
  WatchHandle& operator=(const WatchHandle&) = delete;

  /// True while this wrapper retains a retryable handle/state pair.
  ///
  /// `Reset()` returning `AORTA_ERR_REENTRANT` leaves this true. Any other
  /// non-OK result clears the handle and deliberately leaks callback state,
  /// so this returns false even though callback drain was not confirmed.
  bool valid() const;

  /// Synchronously stop the watch.
  ///
  /// Result contract:
  /// - OK: clears the handle and destroys callback state.
  /// - REENTRANT: retains both unchanged; retry from a safe thread.
  /// - Any other non-OK: clears the handle and permanently leaks callback
  ///   state because callback drain is unconfirmed. Later calls are the
  ///   idempotent no-op returning OK.
  ///
  /// Move assignment first applies this teardown to its LHS. On any non-OK
  /// result it leaks the old state, forfeits retryability, and adopts the RHS.
  /// Destruction likewise leaks state on any non-OK result. Call `Reset()`
  /// from a safe thread before move assignment when reclaiming the old watch
  /// matters.
  Status Reset();

 private:
  friend class ContextStore;
  friend void aorta_cpp_on_ctx_change(const aorta_ctx_change_t* change,
                                      void* user_data) noexcept;
  friend struct ::aorta::test_access::WatchCallbackTestAccess;

  // Definition is trivial (a single `std::function`); it lives here so
  // test code in `aorta::test_access` can instantiate one to drive the
  // `aorta_cpp_on_ctx_change` dispatcher directly.
  struct State {
    std::function<void(const ContextChange&)> callback;
  };

  WatchHandle(aorta_ctx_watch_t handle, std::unique_ptr<State> state);

  aorta_ctx_watch_t handle_;
  std::unique_ptr<State> state_;
};

/// Trampoline for owner-liveliness watch callbacks. `noexcept` with the
/// same try/catch discipline as `aorta_cpp_on_ctx_change`.
extern "C" void aorta_cpp_on_ctx_owner_liveliness(
    const aorta_ctx_owner_liveliness_event_t* event, void* user_data) noexcept;

/// RAII wrapper for an active owner-liveliness watch. Teardown is fallible:
/// the destructor defers REENTRANT teardown to a detached
/// reaper thread; move-assignment abandons when teardown of the target
/// fails; explicit `Reset()` from a callback context returns the error
/// and must be retried from outside — signal completion from the
/// callback and tear down from the outside, per the `WatchHandle`
/// pattern).
class OwnerLivelinessWatchHandle {
 public:
  OwnerLivelinessWatchHandle();
  ~OwnerLivelinessWatchHandle();
  OwnerLivelinessWatchHandle(OwnerLivelinessWatchHandle&& other) noexcept;
  OwnerLivelinessWatchHandle& operator=(OwnerLivelinessWatchHandle&& other) noexcept;
  OwnerLivelinessWatchHandle(const OwnerLivelinessWatchHandle&) = delete;
  OwnerLivelinessWatchHandle& operator=(const OwnerLivelinessWatchHandle&) = delete;

  /// True if this handle currently owns a live watch.
  bool valid() const;

  /// Synchronously stop the watch. Equivalent to letting the destructor
  /// run, but returns the FFI status. Idempotent.
  Status Reset();

 private:
  friend class ContextStore;
  friend void aorta_cpp_on_ctx_owner_liveliness(
      const aorta_ctx_owner_liveliness_event_t* event, void* user_data) noexcept;
  friend struct ::aorta::test_access::OwnerLivelinessWatchCallbackTestAccess;

  struct State {
    std::function<void(const OwnerLivelinessEvent&)> callback;
  };

  OwnerLivelinessWatchHandle(aorta_ctx_owner_liveliness_watch_t handle,
                             std::unique_ptr<State> state);

  aorta_ctx_owner_liveliness_watch_t handle_;
  std::unique_ptr<State> state_;
};

/// C++ wrapper around `aorta_core::contextstore::ContextStore`.
///
/// Lifecycle:
/// - Construct via `Create(node)` (or `Node::CreateContextStore`).
/// - Use `DeclareOwnedPrefix` before any `Set*` (the ownership
///   bootstrap contract).
/// - Destructor (and explicit `Shutdown`) await Zenoh undeclare on
///   every declared Queryable + Subscriber and tear down all
///   in-flight watches owned by the store.
///
/// Threading: ContextStore is internally synchronised via the Rust
/// core's `Arc<Inner>`. All methods are safe to call from multiple
/// threads. Watch callbacks may fire on arbitrary Zenoh worker
/// threads and possibly concurrently across keys; callers are
/// responsible for synchronising any state captured by the callback.
class ContextStore {
 public:
  using ChangeCallback = std::function<void(const ContextChange&)>;
  using OwnerLivelinessCallback = std::function<void(const OwnerLivelinessEvent&)>;

  static StatusOr<std::shared_ptr<ContextStore>> Create(
      std::shared_ptr<Node> node);

  ~ContextStore();
  ContextStore(const ContextStore&) = delete;
  ContextStore& operator=(const ContextStore&) = delete;
  ContextStore(ContextStore&&) = delete;
  ContextStore& operator=(ContextStore&&) = delete;

  Status Shutdown();
  Status DeclareOwnedPrefix(std::string_view prefix);

  Status SetBool(std::string_view key, bool value, uint64_t expires_at_ns = 0);
  Status SetI64(std::string_view key, int64_t value,
                uint64_t expires_at_ns = 0);
  Status SetU64(std::string_view key, uint64_t value,
                uint64_t expires_at_ns = 0);
  Status SetF64(std::string_view key, double value, uint64_t expires_at_ns = 0);
  Status SetString(std::string_view key, std::string_view value,
                   uint64_t expires_at_ns = 0);

  StatusOr<ContextRead<bool>> GetBool(std::string_view key);
  StatusOr<ContextRead<int64_t>> GetI64(std::string_view key);
  StatusOr<ContextRead<uint64_t>> GetU64(std::string_view key);
  StatusOr<ContextRead<double>> GetF64(std::string_view key);
  StatusOr<ContextRead<std::string>> GetString(std::string_view key);

  /// Subscribe to live updates under `prefix`.
  StatusOr<WatchHandle> Watch(std::string_view prefix,
                              ChangeCallback callback);

  /// Subscribe to live updates plus an initial snapshot of the current
  /// owner state. The SDK delivers `kSnapshot` events for
  /// every reply, then a single `kSnapshotDone` marker carrying the
  /// reason, after which only `kLive` events arrive.
  StatusOr<WatchHandle> WatchWithInitialSnapshot(
      std::string_view prefix, const WatchSnapshotOptions& options,
      ChangeCallback callback);

  /// Watch the liveliness of ContextStore owners in this store's group:
  /// current owners are replayed as `alive == true` events,
  /// then live join/leave events follow. Callbacks may fire on arbitrary
  /// Zenoh worker threads; synchronise any captured state.
  StatusOr<OwnerLivelinessWatchHandle> WatchOwnerLiveliness(
      OwnerLivelinessCallback callback);

 private:
  ContextStore(aorta_ctx_t handle, std::shared_ptr<Node> node);

  aorta_ctx_t handle_;
  std::shared_ptr<Node> node_;  // keep node alive while store exists
};

}  // namespace aorta
