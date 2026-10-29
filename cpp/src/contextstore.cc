#include "aorta/contextstore.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <aorta.h>

#include "aorta/convenience.h"
#include "aorta/detail/reentrancy_check.h"
#include "aorta/node.h"
#include "internal/common.h"

namespace aorta {

namespace {

// Rate-limited counter used by `aorta_cpp_on_ctx_change` to log user
// callback exceptions without flooding the log on a sustained throw.
std::atomic<uint64_t> g_last_ctx_exception_log_ns{0};

std::string SvToString(const aorta_string_view_t& view) {
  if (view.data == nullptr || view.len == 0) {
    return std::string();
  }
  return std::string(reinterpret_cast<const char*>(view.data), view.len);
}

ContextChange ConvertChange(const aorta_ctx_change_t* c) {
  ContextChange change;
  change.kind = static_cast<ContextChangeKind>(c->kind);
  if (change.kind == ContextChangeKind::kSnapshotDone) {
    if (c->snapshot_done_reason != AORTA_CTX_SNAPSHOT_DONE_NONE) {
      change.snapshot_done_reason =
          static_cast<SnapshotDoneReason>(c->snapshot_done_reason);
    }
  }
  change.key = SvToString(c->key);
  change.value_kind = static_cast<ContextKind>(c->value_kind);
  change.bool_value = c->bool_value;
  change.i64_value = c->i64_value;
  change.u64_value = c->u64_value;
  change.f64_value = c->f64_value;
  if (c->value_kind == AORTA_CTX_KIND_STRING) {
    change.string_value = SvToString(c->string_value);
  }
  change.stamp_ns = c->stamp_ns;
  change.expires_at_ns = c->expires_at_ns;
  change.source_node = SvToString(c->source_node);
  change.source_instance_id = SvToString(c->source_instance_id);
  change.sequence = c->sequence;
  return change;
}

// Rate-limited warning for the deliberate State retention on an
// unexpected teardown error: ffi_guard can surface
// AORTA_ERR_INTERNAL after the watch was already removed from the Rust
// registry, where the Rust side only best-effort-drops the watch and a
// callback may still hit user_data. Retaining State (a bounded leak) is
// safer than a use-after-free; log it once per interval so a sustained
// failure cannot flood the log.
void MaybeLogRetainedWatchState(const char* what) {
  static std::atomic<uint64_t> g_last_retained_log_ns{0};
  uint64_t now_ns = internal::NowNs();
  uint64_t last = g_last_retained_log_ns.load(std::memory_order_relaxed);
  if (now_ns - last < internal::LogIntervalNs()) {
    return;
  }
  if (!g_last_retained_log_ns.compare_exchange_weak(
          last, now_ns, std::memory_order_relaxed)) {
    return;
  }
  internal::LogWarnf("aorta_sdk",
                     "watch teardown failed with an unexpected error; "
                     "callback state retained (bounded leak): %s",
                     what);
}

template <typename T>
void FillReadCommon(ContextRead<T>& read, const aorta_ctx_read_t& src) {
  read.is_fresh = src.is_fresh;
  read.stamp_ns = src.stamp_ns;
  read.expires_at_ns = src.expires_at_ns;
  if (src.source_node != nullptr) {
    read.source_node = src.source_node;
  }
  if (src.source_instance_id != nullptr) {
    read.source_instance_id = src.source_instance_id;
  }
  read.sequence = src.sequence;
}

}  // namespace

// ---------------------------------------------------------------------------
// WatchHandle
// ---------------------------------------------------------------------------
// `WatchHandle::State` is defined in the header (single `std::function`
// wrapper) so that test code in `aorta::testing` can construct one
// directly to exercise the dispatcher in isolation.

extern "C" void aorta_cpp_on_ctx_change(const aorta_ctx_change_t* change,
                                        void* user_data) noexcept {
  // Mirrors the `aorta_cpp_on_msg` pattern in subscriber.cc: catch any
  // exception thrown by the user callback so it never unwinds through
  // the C ABI boundary back into Rust (UB / abort). Rate-limit the
  // log via internal::MaybeLogCallbackException so a sustained throw
  // doesn't flood logs.
  try {
    auto* state = static_cast<WatchHandle::State*>(user_data);
    if (state == nullptr || !state->callback) {
      return;
    }
    ContextChange c = ConvertChange(change);
    state->callback(c);
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    std::fprintf(stderr, "Aorta fatal (from contextstore callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    internal::MaybeLogCallbackException("contextstore",
                                        g_last_ctx_exception_log_ns);
  }
}

extern "C" void aorta_cpp_on_ctx_owner_liveliness(
    const aorta_ctx_owner_liveliness_event_t* event,
    void* user_data) noexcept {
  // Same discipline as `aorta_cpp_on_ctx_change`: catch any exception
  // thrown by the user callback so it never unwinds through the C ABI
  // boundary back into Rust (UB / abort).
  try {
    auto* state = static_cast<OwnerLivelinessWatchHandle::State*>(user_data);
    if (state == nullptr || !state->callback) {
      return;
    }
    OwnerLivelinessEvent e;
    e.node_name = SvToString(event->node_name);
    e.source_instance_id = SvToString(event->source_instance_id);
    e.alive = event->alive;
    state->callback(e);
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    std::fprintf(stderr, "Aorta fatal (from contextstore liveliness callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    internal::MaybeLogCallbackException("contextstore",
                                        g_last_ctx_exception_log_ns);
  }
}

// ---------------------------------------------------------------------------
// OwnerLivelinessWatchHandle
// ---------------------------------------------------------------------------

OwnerLivelinessWatchHandle::OwnerLivelinessWatchHandle()
    : handle_(0), state_(nullptr) {}

OwnerLivelinessWatchHandle::OwnerLivelinessWatchHandle(
    aorta_ctx_owner_liveliness_watch_t handle, std::unique_ptr<State> state)
    : handle_(handle), state_(std::move(state)) {}

OwnerLivelinessWatchHandle::OwnerLivelinessWatchHandle(
    OwnerLivelinessWatchHandle&& other) noexcept
    : handle_(other.handle_), state_(std::move(other.state_)) {
  other.handle_ = 0;
}

OwnerLivelinessWatchHandle& OwnerLivelinessWatchHandle::operator=(
    OwnerLivelinessWatchHandle&& other) noexcept {
  if (this != &other) {
    // The current watch (if any) is being replaced by an explicit move.
    // Teardown is fallible: on REENTRANT the FFI re-inserted the watch
    // and may still deliver callbacks into our state; on NOT_FOUND the
    // handle is stale. If teardown fails, the move is ABANDONED — both
    // objects keep their watches and callback state intact, and the old
    // watch must be torn down via Reset() from a non-runtime thread (or
    // its destructor's reaper). Overwriting handle_/state_ here would
    // free state the FFI still references (use-after-free).
    if (handle_ != 0 && !Reset().ok()) {
      return *this;
    }
    handle_ = other.handle_;
    state_ = std::move(other.state_);
    other.handle_ = 0;
  }
  return *this;
}

OwnerLivelinessWatchHandle::~OwnerLivelinessWatchHandle() {
  if (handle_ == 0) {
    return;
  }
  aorta_ctx_owner_liveliness_watch_t handle = handle_;
  std::unique_ptr<State> state = std::move(state_);
  handle_ = 0;

  // Normal path: SYNCHRONOUS teardown — the documented contract. From a
  // non-runtime thread the FFI free awaits the undeclare and the
  // in-flight callbacks, so after this destructor returns no callback
  // can fire (and no reaper thread is spawned per ordinary
  // destruction).
  // reentrancy: F (REENTRANT is handled by the reaper branch below)
  aorta_result_t rc = aorta_ctx_owner_liveliness_watch_free(handle);
  if (rc == AORTA_OK) {
    return;  // `state` drops here — the FFI confirmed no callbacks.
  }
  // Closed form: State is freed ONLY on AORTA_OK.
  // NOT_FOUND proves nothing about callback drain — the callback
  // closure holds user_data directly and dispatch never consults the
  // registry, so a stale handle (e.g. one whose Rust-side watch was
  // dropped by an earlier panicking free) can still have a callback in
  // flight. Retain State on EVERYTHING else (bounded leak + rate-limited
  // log).
  if (rc == AORTA_ERR_REENTRANT) {
    // Destructor ran inside the runtime (e.g. destroying another
    // watch's handle from a callback, or from an executor task): the
    // FFI re-inserted the watch and may still deliver callbacks into
    // `state`. Hand ownership to a detached reaper thread that retries
    // the free from a non-runtime context; `state` stays alive until
    // the free succeeds (no UAF, no busy-loop on the runtime thread).
    std::thread reaper([handle, state = std::move(state)]() mutable {
      for (;;) {
        // reentrancy: F (reaper runs on a fresh non-runtime thread)
        aorta_result_t r = aorta_ctx_owner_liveliness_watch_free(handle);
        if (r == AORTA_OK) {
          return;  // the FFI confirmed no callbacks — safe to free
        }
        if (r == AORTA_ERR_REENTRANT) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        // NOT_FOUND and any other error: retain State deliberately
        // (closed form — see the synchronous path).
        MaybeLogRetainedWatchState("owner-liveliness watch");
        (void)state.release();
        return;
      }
    });
    reaper.detach();
    return;
  }
  // Unexpected error on the synchronous attempt: same deliberate
  // retention (see the reaper branch).
  MaybeLogRetainedWatchState("owner-liveliness watch");
  (void)state.release();
}

bool OwnerLivelinessWatchHandle::valid() const {
  return handle_ != 0 && state_ != nullptr;
}

Status OwnerLivelinessWatchHandle::Reset() {
  if (handle_ == 0) {
    state_.reset();
    return Status::Ok();
  }
  // aorta_ctx_owner_liveliness_watch_free awaits the Zenoh undeclare so
  // no further callback can fire after this returns. Only THEN is it
  // safe to drop the heap-allocated callback state (state_).
  //
  // On error the FFI has re-inserted the watch and may still
  // deliver callbacks — keep handle_ and state_ (see WatchHandle::Reset).
  // reentrancy: F (REENTRANT surfaces as a Status the caller retries)
  aorta_result_t rc = aorta_ctx_owner_liveliness_watch_free(handle_);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  handle_ = 0;
  state_.reset();
  return Status::Ok();
}

WatchHandle::WatchHandle() : handle_(0), state_(nullptr) {}

WatchHandle::WatchHandle(aorta_ctx_watch_t handle, std::unique_ptr<State> state)
    : handle_(handle), state_(std::move(state)) {}

WatchHandle::WatchHandle(WatchHandle&& other) noexcept
    : handle_(other.handle_), state_(std::move(other.state_)) {
  other.handle_ = 0;
}

WatchHandle& WatchHandle::operator=(WatchHandle&& other) noexcept {
  if (this != &other) {
    if (handle_ != 0) {
      const Status status = Reset();
      if (!status.ok() && status.code() == AORTA_ERR_REENTRANT) {
        handle_ = 0;
        (void)state_.release();
      }
    }
    handle_ = other.handle_;
    state_ = std::move(other.state_);
    other.handle_ = 0;
  }
  return *this;
}

WatchHandle::~WatchHandle() {
  if (handle_ == 0) {
    return;
  }
  aorta_ctx_watch_t handle = handle_;
  std::unique_ptr<State> state = std::move(state_);
  handle_ = 0;

  AORTA_CHECK_NOT_REENTRANT("WatchHandle::~WatchHandle");
  aorta_result_t rc = aorta_ctx_watch_free(handle);
  if (rc == AORTA_OK) {
    return;
  }
  (void)state.release();
  if (rc != AORTA_ERR_REENTRANT) {
    static std::atomic<bool> warned{false};
    detail::HandleReentrantHit(
        "WatchHandle::~WatchHandle could not confirm callback drain; "
        "leaking callback state",
        warned);
  }
}

bool WatchHandle::valid() const { return handle_ != 0 && state_ != nullptr; }

Status WatchHandle::Reset() {
  if (handle_ == 0) {
    state_.reset();
    return Status::Ok();
  }
  AORTA_CHECK_NOT_REENTRANT("WatchHandle::Reset");
  aorta_result_t rc = aorta_ctx_watch_free(handle_);
  if (rc == AORTA_ERR_REENTRANT) {
    return Status::FromResult(rc);
  }
  handle_ = 0;
  if (rc == AORTA_OK) {
    state_.reset();
    return Status::Ok();
  }
  (void)state_.release();
  static std::atomic<bool> warned{false};
  detail::HandleReentrantHit(
      "WatchHandle::Reset could not confirm callback drain; leaking "
      "callback state",
      warned);
  return Status::FromResult(rc);
}

// ---------------------------------------------------------------------------
// ContextStore
// ---------------------------------------------------------------------------

ContextStore::ContextStore(aorta_ctx_t handle, std::shared_ptr<Node> node)
    : handle_(handle), node_(std::move(node)) {}

ContextStore::~ContextStore() {
  if (handle_ != 0) {
    // aorta_ctx_free internally awaits ContextStore::shutdown — no
    // late callback can fire on a declared-prefix subscriber after
    // this point.
    // reentrancy: F (spawn-and-detach free — OK from a callback)
    (void)aorta_ctx_free(handle_);
    handle_ = 0;
  }
}

StatusOr<std::shared_ptr<ContextStore>> ContextStore::Create(
    std::shared_ptr<Node> node) {
  if (!node) {
    return StatusOr<std::shared_ptr<ContextStore>>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "node is null"));
  }
  aorta_ctx_t handle = 0;
  aorta_result_t rc = aorta_ctx_new(node->handle(), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<ContextStore>>(Status::FromResult(rc));
  }
  // shared_ptr aliasing: ContextStore ctor is private, use new.
  std::shared_ptr<ContextStore> store(new ContextStore(handle, std::move(node)));
  return StatusOr<std::shared_ptr<ContextStore>>(std::move(store));
}

Status ContextStore::Shutdown() {
  if (handle_ == 0) {
    return Status::Ok();
  }
  AORTA_CHECK_NOT_REENTRANT("ContextStore::Shutdown");
  aorta_result_t rc = aorta_ctx_shutdown(handle_);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return Status::Ok();
}

Status ContextStore::DeclareOwnedPrefix(std::string_view prefix) {
  std::string p(prefix);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::DeclareOwnedPrefix");
  aorta_result_t rc = aorta_ctx_declare_owned_prefix(handle_, p.c_str());
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return Status::Ok();
}

Status ContextStore::SetBool(std::string_view key, bool value,
                             uint64_t expires_at_ns) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::SetBool");
  aorta_result_t rc =
      aorta_ctx_set_bool(handle_, k.c_str(), value, expires_at_ns);
  return rc == AORTA_OK ? Status::Ok() : Status::FromResult(rc);
}

Status ContextStore::SetI64(std::string_view key, int64_t value,
                            uint64_t expires_at_ns) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::SetI64");
  aorta_result_t rc =
      aorta_ctx_set_i64(handle_, k.c_str(), value, expires_at_ns);
  return rc == AORTA_OK ? Status::Ok() : Status::FromResult(rc);
}

Status ContextStore::SetU64(std::string_view key, uint64_t value,
                            uint64_t expires_at_ns) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::SetU64");
  aorta_result_t rc =
      aorta_ctx_set_u64(handle_, k.c_str(), value, expires_at_ns);
  return rc == AORTA_OK ? Status::Ok() : Status::FromResult(rc);
}

Status ContextStore::SetF64(std::string_view key, double value,
                            uint64_t expires_at_ns) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::SetF64");
  aorta_result_t rc =
      aorta_ctx_set_f64(handle_, k.c_str(), value, expires_at_ns);
  return rc == AORTA_OK ? Status::Ok() : Status::FromResult(rc);
}

Status ContextStore::SetString(std::string_view key, std::string_view value,
                               uint64_t expires_at_ns) {
  std::string k(key);
  std::string v(value);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::SetString");
  aorta_result_t rc =
      aorta_ctx_set_string(handle_, k.c_str(), v.c_str(), expires_at_ns);
  return rc == AORTA_OK ? Status::Ok() : Status::FromResult(rc);
}

namespace {

template <typename Read>
StatusOr<Read> CopyOutAndFree(aorta_ctx_read_t* raw, aorta_result_t rc, Read&& tmpl) {
  if (rc != AORTA_OK) {
    aorta_ctx_read_free(raw);
    return StatusOr<Read>(Status::FromResult(rc));
  }
  Read read = std::move(tmpl);
  FillReadCommon(read, *raw);
  aorta_ctx_read_free(raw);
  return StatusOr<Read>(std::move(read));
}

}  // namespace

StatusOr<ContextRead<bool>> ContextStore::GetBool(std::string_view key) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::GetBool");
  aorta_ctx_read_t raw{};
  aorta_result_t rc = aorta_ctx_get_bool(handle_, k.c_str(), &raw);
  ContextRead<bool> tmpl;
  tmpl.value = raw.bool_value;
  return CopyOutAndFree(&raw, rc, std::move(tmpl));
}

StatusOr<ContextRead<int64_t>> ContextStore::GetI64(std::string_view key) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::GetI64");
  aorta_ctx_read_t raw{};
  aorta_result_t rc = aorta_ctx_get_i64(handle_, k.c_str(), &raw);
  ContextRead<int64_t> tmpl;
  tmpl.value = raw.i64_value;
  return CopyOutAndFree(&raw, rc, std::move(tmpl));
}

StatusOr<ContextRead<uint64_t>> ContextStore::GetU64(std::string_view key) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::GetU64");
  aorta_ctx_read_t raw{};
  aorta_result_t rc = aorta_ctx_get_u64(handle_, k.c_str(), &raw);
  ContextRead<uint64_t> tmpl;
  tmpl.value = raw.u64_value;
  return CopyOutAndFree(&raw, rc, std::move(tmpl));
}

StatusOr<ContextRead<double>> ContextStore::GetF64(std::string_view key) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::GetF64");
  aorta_ctx_read_t raw{};
  aorta_result_t rc = aorta_ctx_get_f64(handle_, k.c_str(), &raw);
  ContextRead<double> tmpl;
  tmpl.value = raw.f64_value;
  return CopyOutAndFree(&raw, rc, std::move(tmpl));
}

StatusOr<ContextRead<std::string>> ContextStore::GetString(
    std::string_view key) {
  std::string k(key);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::GetString");
  aorta_ctx_read_t raw{};
  aorta_result_t rc = aorta_ctx_get_string(handle_, k.c_str(), &raw);
  ContextRead<std::string> tmpl;
  if (rc == AORTA_OK && raw.string_value != nullptr) {
    tmpl.value = raw.string_value;
  }
  return CopyOutAndFree(&raw, rc, std::move(tmpl));
}

StatusOr<WatchHandle> ContextStore::Watch(std::string_view prefix,
                                          ChangeCallback callback) {
  if (!callback) {
    return StatusOr<WatchHandle>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "callback is null"));
  }
  auto state = std::make_unique<WatchHandle::State>();
  state->callback = std::move(callback);
  std::string p(prefix);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::Watch");
  aorta_ctx_watch_t handle = 0;
  aorta_result_t rc =
      aorta_ctx_watch(handle_, p.c_str(), &aorta_cpp_on_ctx_change,
                      static_cast<void*>(state.get()), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<WatchHandle>(Status::FromResult(rc));
  }
  return StatusOr<WatchHandle>(WatchHandle(handle, std::move(state)));
}

StatusOr<OwnerLivelinessWatchHandle> ContextStore::WatchOwnerLiveliness(
    OwnerLivelinessCallback callback) {
  if (!callback) {
    return StatusOr<OwnerLivelinessWatchHandle>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "callback is null"));
  }
  auto state = std::make_unique<OwnerLivelinessWatchHandle::State>();
  state->callback = std::move(callback);
  aorta_ctx_owner_liveliness_watch_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("ContextStore::WatchOwnerLiveliness");
  aorta_result_t rc = aorta_ctx_owner_liveliness_watch(
      handle_, &aorta_cpp_on_ctx_owner_liveliness, static_cast<void*>(state.get()),
      &handle);
  if (rc != AORTA_OK) {
    return StatusOr<OwnerLivelinessWatchHandle>(Status::FromResult(rc));
  }
  return StatusOr<OwnerLivelinessWatchHandle>(
      OwnerLivelinessWatchHandle(handle, std::move(state)));
}

StatusOr<std::shared_ptr<ContextStore>> Node::CreateContextStore() {
  return ContextStore::Create(shared_from_this());
}

StatusOr<WatchHandle> ContextStore::WatchWithInitialSnapshot(
    std::string_view prefix, const WatchSnapshotOptions& options,
    ChangeCallback callback) {
  if (!callback) {
    return StatusOr<WatchHandle>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "callback is null"));
  }
  auto state = std::make_unique<WatchHandle::State>();
  state->callback = std::move(callback);
  std::string p(prefix);
  AORTA_CHECK_NOT_REENTRANT("ContextStore::WatchWithInitialSnapshot");
  aorta_ctx_watch_t handle = 0;
  uint64_t deadline_ms =
      static_cast<uint64_t>(options.snapshot_deadline.count());
  aorta_result_t rc = aorta_ctx_watch_with_snapshot(
      handle_, p.c_str(), deadline_ms, &aorta_cpp_on_ctx_change,
      static_cast<void*>(state.get()), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<WatchHandle>(Status::FromResult(rc));
  }
  return StatusOr<WatchHandle>(WatchHandle(handle, std::move(state)));
}

}  // namespace aorta
