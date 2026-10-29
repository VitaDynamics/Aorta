#pragma once
// Aorta C++ tracing wrapper (Step 11) — thin, header-only, over the core C-ABI.
//
// Independent of telemetry/logging. The DISABLED path is an INLINE relaxed read
// of cached gate bytes — ZERO FFI call (the opposite of AORTA_LOG, whose builder
// ctor crosses the FFI on every disabled call site). Timing uses
// std::chrono::steady_clock (monotonic). Records go through the C-ABI only when
// the (scope, level) gate passes.

#include <chrono>
#include <cstdint>

// extern "C" prototypes for the trace C-ABI. Declared here until cbindgen
// regenerates them into aorta.h (duplicate identical extern "C" decls are
// harmless). See core/src/ffi/c_api/trace_ffi.rs.
extern "C" {
uint16_t aorta_trace_scope(const char* name);
void aorta_trace_set_enabled(uint8_t on);
uint8_t aorta_trace_is_enabled(void);
void aorta_trace_set_scope_enabled(uint16_t scope, uint8_t on);
uint8_t aorta_trace_enabled(uint16_t scope, uint8_t level);
int32_t aorta_trace_record(uint16_t scope, const char* name, uint8_t level,
                           uint64_t latency_ns);
int32_t aorta_trace_record_event(uint16_t scope, const char* name,
                                 uint8_t event_type, uint8_t level,
                                 uint64_t value);
int32_t aorta_trace_record_stage(uint8_t stage_id, uint64_t latency_ns);
int32_t aorta_trace_init_from_env(void);
uint64_t aorta_trace_user_metric_count(uint16_t scope, const char* name);
const void* aorta_trace_metric(uint16_t scope, const char* name, uint8_t event_type);
void aorta_trace_record_by_handle(const void* metric, uint64_t value);
const uint8_t* aorta_trace_master_gate(void);
const uint8_t* aorta_trace_scope_gate(uint16_t scope);
// Phase-2: per-event spans / instants / counters (the waterfall ring path).
uint16_t aorta_trace_intern_event(const char* name);
uint64_t aorta_trace_span_begin(uint16_t scope, uint16_t event_id, uint8_t level,
                                uint8_t flags);
uint64_t aorta_trace_span_end(uint64_t token);
void aorta_trace_record_instant(uint16_t scope, uint16_t event_id, uint8_t level);
void aorta_trace_record_counter(uint16_t scope, uint16_t event_id, uint8_t level,
                                uint64_t value);
void aorta_trace_set_context(const uint8_t* trace_id, uint64_t parent_span_id);
void aorta_trace_clear_context(void);
uint8_t aorta_trace_current_context(uint8_t* out_trace_id, uint64_t* out_span_id);
void aorta_trace_set_thread_time_enabled(uint8_t on);
uint64_t aorta_trace_monotonic_ns(void);
void aorta_trace_record_stage_at(uint8_t stage_id, const char* name,
                                 uint64_t start_mono_ns, const uint8_t* trace_id,
                                 uint64_t span_id, uint64_t parent_span_id);
void aorta_trace_record_stage_under(uint8_t stage_id, const char* name,
                                    uint64_t start_mono_ns,
                                    const uint8_t* trace_id,
                                    uint64_t parent_span_id);
int32_t aorta_trace_init(void);
void aorta_trace_shutdown(void);
}

namespace aorta {
namespace trace {

enum class Level : uint8_t { Trace = 0, Debug = 1, Info = 2, Warn = 3, Error = 4 };

// Built-in pipeline stage ids — MUST mirror the Rust core's
// `trace::StageId` numbering (the C ABI ships the raw value).
enum class StageId : uint8_t {
  kServiceClientCall = 0,
  kServiceRequestReceivedToDispatched = 1,
  kServiceReceivedToReply = 2,
  kActionGoalSentToAccepted = 3,
  kActionAcceptedToExecutionStarted = 4,
  kActionAcceptedToFinished = 5,
  kActionFeedbackInterval = 6,
  kServiceNetworkEstimate = 7,
  kServiceQueueWait = 8,
  kServiceUserCallback = 9,
  kActionUserCallback = 10,
  kActionResultWait = 11,
  kActionCancelRequestedToCanceled = 12,
  kServiceReplySend = 13,
  kActionGoalAdmission = 14,
  kActionGetResultQuery = 15,
  kTopicPublish = 16,
  kTopicCallback = 17,
  kSchedQueueWait = 18,
  kSchedGroupWait = 19,
  kSchedCallback = 20,
  kSchedRelayWait = 21,
  kActionGoalLifetime = 22,
};

inline uint8_t load_byte_relaxed(const uint8_t* p) {
  return __atomic_load_n(p, __ATOMIC_RELAXED);
}

inline void set_enabled(bool on) { aorta_trace_set_enabled(on ? 1 : 0); }
inline bool is_enabled() { return aorta_trace_is_enabled() != 0; }

class Scope;  // fwd-decl for Metric's friend factory

// A pre-resolved metric for the LOCK-FREE high-volume hot path. It resolves
// `(scope, name)` to a stable histogram handle ONCE (cold) and caches it with the
// inline gate bytes, so each `record()` is an inline gate read + one FFI call
// straight into the histogram — NO per-event string conversion, global lock, or
// allocation (which the by-name `Scope::record` path incurs). Construct via
// `Scope::metric(...)` and reuse (e.g. a `static` local or a member).
class Metric {
 public:
  // Inline gate: master on AND level >= the scope's threshold. A few relaxed
  // byte loads, NO FFI call — the disabled-path check.
  inline bool enabled() const {
    if (master_ == nullptr || load_byte_relaxed(master_) == 0) return false;
    if (handle_ == nullptr || gate_ == nullptr) return false;
    return static_cast<uint8_t>(level_) >= load_byte_relaxed(gate_);
  }

  // Record one value (a latency ns for a span/record metric, or a counter
  // value). Inline-gated; a disabled metric is a relaxed gate read with no call.
  inline void record(uint64_t value) const {
    if (enabled()) aorta_trace_record_by_handle(handle_, value);
  }

  // Record WITHOUT re-checking the gate — for callers that already gated (e.g.
  // the dual-tier span/instant/counter macros, where the enclosing site decided
  // to record). A NULL handle is a no-op.
  inline void record_unchecked(uint64_t value) const {
    if (handle_ != nullptr) aorta_trace_record_by_handle(handle_, value);
  }

  bool valid() const { return handle_ != nullptr; }

 private:
  friend class Scope;
  Metric(const void* handle, const uint8_t* master, const uint8_t* gate, Level level)
      : handle_(handle), master_(master), gate_(gate), level_(level) {}

  const void* handle_;
  const uint8_t* master_;
  const uint8_t* gate_;
  Level level_;
};

// A cached scope: interns the scope name once and caches the master + per-scope
// gate-byte pointers, so the hot-path `enabled()` check is a few INLINE relaxed
// byte loads with NO FFI call. Construct once (e.g. a `static` local) and reuse.
class Scope {
 public:
  explicit Scope(const char* name)
      : handle_(aorta_trace_scope(name)),
        master_(aorta_trace_master_gate()),
        gate_(aorta_trace_scope_gate(handle_)) {}

  uint16_t handle() const { return handle_; }

  void set_enabled(bool on) const {
    aorta_trace_set_scope_enabled(handle_, on ? 1 : 0);
  }

  // Inline near-zero gate: master on AND level >= scope threshold. No FFI call.
  inline bool enabled(Level level) const {
    if (master_ == nullptr || load_byte_relaxed(master_) == 0) return false;
    if (gate_ == nullptr) return false;
    return static_cast<uint8_t>(level) >= load_byte_relaxed(gate_);
  }

  // BY-NAME record. Inline-gated (no FFI when disabled), but when ENABLED it
  // resolves the metric by `name` on EVERY call: the C-ABI converts the name to a
  // Rust String (alloc), takes a global lock, and does a map lookup. Use this only
  // for DYNAMIC (runtime-computed) names or low-rate calls. For hot paths use the
  // AORTA_TRACE_RECORD macro (caches the handle per call-site) or Scope::metric().
  void record(const char* name, Level level, uint64_t latency_ns) const {
    if (enabled(level)) {
      aorta_trace_record(handle_, name, static_cast<uint8_t>(level), latency_ns);
    }
  }

  // EVENT-TYPE axis: a point INSTANT (occurrence) and a COUNTER value. Both
  // gate-first (inline, no FFI when disabled), then record by name via the C-ABI.
  // Same by-name caveat as record(): prefer AORTA_TRACE_INSTANT / _COUNTER (or
  // Scope::metric) at high rates.
  void instant(const char* name, Level level) const {
    if (enabled(level)) {
      aorta_trace_record_event(handle_, name, /*Instant=*/1,
                               static_cast<uint8_t>(level), 1);
    }
  }
  void counter(const char* name, Level level, uint64_t value) const {
    if (enabled(level)) {
      aorta_trace_record_event(handle_, name, /*Counter=*/2,
                               static_cast<uint8_t>(level), value);
    }
  }

  // Recorded sample count for `name` under this scope (`0` if never recorded).
  // Read-back into the real engine — for tests/diagnostics, NOT the hot path.
  uint64_t recorded_count(const char* name) const {
    return aorta_trace_user_metric_count(handle_, name);
  }

  // Resolve a metric ONCE for the lock-free high-volume path. Reuse the returned
  // Metric (e.g. a `static` local) and call `.record(value)` per event. Prefer
  // this over `record()`/`instant()`/`counter()` for high event rates — those go
  // by name through the FFI on every call (string + lock + lookup). `event_type`:
  // 0=span/latency, 1=instant, 2=counter (export label only).
  Metric metric(const char* name, Level level, uint8_t event_type = 0) const {
    return Metric(aorta_trace_metric(handle_, name, event_type), master_, gate_, level);
  }

 private:
  uint16_t handle_;
  const uint8_t* master_;
  const uint8_t* gate_;
};

// RAII span timer over a pre-resolved Metric: captures the monotonic start in
// the ctor IFF enabled, and records the elapsed latency into the cached handle on
// destruction (lock-free, no per-span string/lock/alloc). A disabled timer reads
// no clock and makes no FFI call. Construct via the AORTA_TRACE_SCOPE macro,
// which caches the Metric per call-site.
class ScopeTimer {
 public:
  explicit ScopeTimer(const Metric& metric)
      : metric_(&metric), active_(metric.enabled()) {
    if (active_) start_ = std::chrono::steady_clock::now();
  }

  ~ScopeTimer() {
    // Re-check the gate at drop: if tracing was turned off mid-span, skip the
    // clock read and the record entirely (matches the Rust record_stage_elapsed
    // disabled-path contract).
    if (!active_ || !metric_->enabled()) return;
    const auto end = std::chrono::steady_clock::now();
    const auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count();
    metric_->record(static_cast<uint64_t>(ns < 0 ? 0 : ns));
  }

  ScopeTimer(const ScopeTimer&) = delete;
  ScopeTimer& operator=(const ScopeTimer&) = delete;

 private:
  const Metric* metric_;
  bool active_;
  std::chrono::steady_clock::time_point start_;
};

// RAII WATERFALL span (Phase-2): on construction (if the gate passes) it opens a
// begin/end span in the core ring via the C-ABI; on destruction it closes it AND
// feeds the same duration into the aggregate-tier `Metric` — ONE site, BOTH
// outputs (timeline waterfall + p50/p95/p99 histogram). The core timestamps the
// span (begin at open, end at close), so the SDK does no clock read itself.
// Tier-gated via `Metric::enabled()` (master + scope level); a disabled span is
// one inline gate read and an inert token. Construct via AORTA_TRACE_SPAN.
class Span {
 public:
  Span(uint16_t scope, uint16_t event_id, Level level, const Metric& metric)
      : metric_(&metric) {
    if (metric.enabled()) {
      token_ = aorta_trace_span_begin(scope, event_id, static_cast<uint8_t>(level), 0);
    }
  }

  ~Span() {
    if (token_ != 0) {
      const uint64_t dur = aorta_trace_span_end(token_);
      metric_->record_unchecked(dur);
    }
  }

  Span(const Span&) = delete;
  Span& operator=(const Span&) = delete;

 private:
  const Metric* metric_;
  uint64_t token_ = 0;
};

// Install / clear inbound trace context for cross-process propagation (RPC /
// action). `trace_id` is 16 bytes. Off the hot path (per inbound message).
inline void set_context(const uint8_t* trace_id, uint64_t parent_span_id) {
  aorta_trace_set_context(trace_id, parent_span_id);
}
inline void clear_context() { aorta_trace_clear_context(); }

// A trace identity that can be carried across threads: the 16-byte trace id
// plus the span to parent new work under. `valid == false` means "no trace
// active" (the id fields are zeroed). Produced by `current_context()` and the
// request/goal accessors (e.g. `ServiceRequest::TraceContext()`); consumed by
// `ContextGuard`.
struct Context {
  uint8_t trace_id[16] = {};
  uint64_t span_id = 0;
  bool valid = false;
};

// Read the calling thread's active outbound context (the innermost open span,
// falling back to an installed inbound context). For handing a trace to a
// worker thread prefer the request/goal accessors — they identify the REQUEST
// rather than whatever span happens to be open on the capturing thread.
inline Context current_context() {
  Context ctx;
  ctx.valid = aorta_trace_current_context(ctx.trace_id, &ctx.span_id) != 0;
  return ctx;
}

// RAII inbound-context scope for DEFERRED work — the C++ analog of the Rust
// core's `adopt_remote_context`: capture a `Context` from the request/goal in
// the callback, move the work to a worker thread, and wrap it in a
// ContextGuard there so AORTA_TRACE_SPAN spans recorded inside stay attached
// to the request's trace. Destruction clears the thread's inbound context
// (deferred by the core while spans are still open, matching the Rust
// contract). Guards do NOT save/restore a previous context — one inbound
// context per thread at a time. An invalid `Context` makes the whole guard a
// no-op (it neither installs nor clears).
class ContextGuard {
 public:
  ContextGuard(const uint8_t* trace_id, uint64_t parent_span_id)
      : installed_(true) {
    aorta_trace_set_context(trace_id, parent_span_id);
  }
  explicit ContextGuard(const Context& ctx) : installed_(ctx.valid) {
    if (installed_) aorta_trace_set_context(ctx.trace_id, ctx.span_id);
  }
  ~ContextGuard() {
    if (installed_) aorta_trace_clear_context();
  }

  ContextGuard(const ContextGuard&) = delete;
  ContextGuard& operator=(const ContextGuard&) = delete;

 private:
  bool installed_;
};

// The tracing engine's monotonic clock (ns) — the time base every stage/span
// record uses. For SDK-side interval measurement that is later recorded via
// record_stage_at / record_stage_under.
inline uint64_t monotonic_ns() { return aorta_trace_monotonic_ns(); }

// Record built-in pipeline stage `stage` (`start_mono_ns` → now) with an
// EXPLICIT pre-minted span identity (the C++ mirror of the core's
// `record_stage_elapsed_linked`) — e.g. the action `goal_sent_to_accepted`
// span whose id already rode the wire. Feeds BOTH tiers. `name` is the
// primitive (service/action) name. No-op when tracing is disabled.
inline void record_stage_at(StageId stage, const char* name,
                            uint64_t start_mono_ns, const uint8_t* trace_id,
                            uint64_t span_id, uint64_t parent_span_id) {
  aorta_trace_record_stage_at(static_cast<uint8_t>(stage), name, start_mono_ns,
                              trace_id, span_id, parent_span_id);
}

// Record built-in pipeline stage `stage` (`start_mono_ns` → now) as a fresh
// CHILD of `(trace_id, parent_span_id)` (the C++ mirror of the core's
// `record_stage_elapsed_under`) — e.g. `action.result_wait` around a wait
// loop, parented to the goal's `goal_sent` span. A NULL `trace_id` falls back
// to the calling thread's local context. No-op when tracing is disabled.
inline void record_stage_under(StageId stage, const char* name,
                               uint64_t start_mono_ns, const uint8_t* trace_id,
                               uint64_t parent_span_id) {
  aorta_trace_record_stage_under(static_cast<uint8_t>(stage), name,
                                 start_mono_ns, trace_id, parent_span_id);
}

// Bootstrap / shutdown the Phase-2 tracer (drain thread + file sink) from the
// environment (`AORTA_TRACE`, `AORTA_TRACE_DIR`, ...). Idempotent.
inline void init() { aorta_trace_init(); }
inline void shutdown() { aorta_trace_shutdown(); }

}  // namespace trace
}  // namespace aorta

#define AORTA_TRACE_CONCAT_(a, b) a##b
#define AORTA_TRACE_CONCAT(a, b) AORTA_TRACE_CONCAT_(a, b)

// These macros are the recommended C++ instrumentation surface: they mirror the
// Rust `aorta_trace_*!` macros — write the metric NAME as a literal, and the
// metric handle is resolved ONCE and cached in a per-call-site `static`, so every
// event after the first is the lock-free by-handle path (no FFI string, global
// lock, or allocation). Disabled cost is an inline gate read. The argument
// expression is evaluated only when enabled (gate-before-eval).
//
// Use the `Scope::record/instant/counter(name, ...)` methods directly only for
// DYNAMIC (runtime-computed) metric names, which cannot be call-site-cached.

// Time the enclosing block. `scope_obj` is an `aorta::trace::Scope` (cached once,
// e.g. a `static` local). The metric handle is cached per call-site; steady-state
// disabled cost is one inline gate read. NOTE: because the RAII timer must be
// declared unconditionally, this resolves (interns) the metric on the FIRST time
// the line is reached even if tracing is off then — a one-time per-call-site cost,
// unlike the strictly gate-first RECORD/INSTANT/COUNTER macros below.
//
//   static aorta::trace::Scope s("app.planner");
//   AORTA_TRACE_SCOPE(s, aorta::trace::Level::Info, "replan");
#define AORTA_TRACE_SCOPE(scope_obj, level, name)                              \
  static ::aorta::trace::Metric AORTA_TRACE_CONCAT(_aorta_trace_metric_,       \
                                                   __LINE__) =                 \
      (scope_obj).metric((name), (level), /*Span=*/0);                         \
  ::aorta::trace::ScopeTimer AORTA_TRACE_CONCAT(_aorta_trace_timer_, __LINE__)(  \
      AORTA_TRACE_CONCAT(_aorta_trace_metric_, __LINE__))

// Record a pre-computed latency (ns) for `(scope, name)` at `level`. Gate-first:
// when disabled, nothing is interned and `value` is NOT evaluated. The handle is
// resolved + cached on the first ENABLED call.
#define AORTA_TRACE_RECORD(scope_obj, level, name, value)                      \
  do {                                                                         \
    if ((scope_obj).enabled((level))) {                                        \
      static ::aorta::trace::Metric AORTA_TRACE_CONCAT(_aorta_trace_m_,        \
                                                       __LINE__) =             \
          (scope_obj).metric((name), (level), /*Span=*/0);                     \
      AORTA_TRACE_CONCAT(_aorta_trace_m_, __LINE__).record((value));           \
    }                                                                          \
  } while (0)

// Time the enclosing block as a WATERFALL span AND an aggregate metric (Phase-2,
// the recommended span surface). Like AORTA_TRACE_SCOPE it must declare the RAII
// guard unconditionally, so the metric + interned event are resolved on the first
// time the line is reached (one-time per call-site); steady-state disabled cost is
// one inline gate read. Use AORTA_TRACE_SCOPE for histogram-only timing.
//
//   static aorta::trace::Scope s("aorta.service");
//   AORTA_TRACE_SPAN(s, aorta::trace::Level::Info, "client_call");
#define AORTA_TRACE_SPAN(scope_obj, level, name)                               \
  static ::aorta::trace::Metric AORTA_TRACE_CONCAT(_aorta_span_m_, __LINE__) = \
      (scope_obj).metric((name), (level), /*Span=*/0);                         \
  static const uint16_t AORTA_TRACE_CONCAT(_aorta_span_e_, __LINE__) =         \
      ::aorta_trace_intern_event((name));                                      \
  ::aorta::trace::Span AORTA_TRACE_CONCAT(_aorta_span_g_, __LINE__)(           \
      (scope_obj).handle(), AORTA_TRACE_CONCAT(_aorta_span_e_, __LINE__),      \
      (level), AORTA_TRACE_CONCAT(_aorta_span_m_, __LINE__))

// Record a point INSTANT (one occurrence) of `(scope, name)` at `level` — into
// BOTH the aggregate count (histogram) and the timeline (ring). Gate-first.
#define AORTA_TRACE_INSTANT(scope_obj, level, name)                           \
  do {                                                                         \
    if ((scope_obj).enabled((level))) {                                       \
      static ::aorta::trace::Metric AORTA_TRACE_CONCAT(_aorta_trace_m_,       \
                                                       __LINE__) =            \
          (scope_obj).metric((name), (level), /*Instant=*/1);                 \
      static const uint16_t AORTA_TRACE_CONCAT(_aorta_trace_e_, __LINE__) =   \
          ::aorta_trace_intern_event((name));                                 \
      AORTA_TRACE_CONCAT(_aorta_trace_m_, __LINE__).record_unchecked(1);      \
      ::aorta_trace_record_instant((scope_obj).handle(),                      \
          AORTA_TRACE_CONCAT(_aorta_trace_e_, __LINE__),                      \
          static_cast<uint8_t>(level));                                       \
    }                                                                          \
  } while (0)

// Record a COUNTER `value` for `(scope, name)` at `level` — into BOTH the
// aggregate distribution (histogram) and the timeline (ring). Gate-first;
// `value` is evaluated once, only when enabled.
#define AORTA_TRACE_COUNTER(scope_obj, level, name, value)                     \
  do {                                                                         \
    if ((scope_obj).enabled((level))) {                                        \
      static ::aorta::trace::Metric AORTA_TRACE_CONCAT(_aorta_trace_m_,        \
                                                       __LINE__) =             \
          (scope_obj).metric((name), (level), /*Counter=*/2);                  \
      static const uint16_t AORTA_TRACE_CONCAT(_aorta_trace_e_, __LINE__) =    \
          ::aorta_trace_intern_event((name));                                  \
      const uint64_t AORTA_TRACE_CONCAT(_aorta_trace_v_, __LINE__) = (value);  \
      AORTA_TRACE_CONCAT(_aorta_trace_m_, __LINE__)                            \
          .record_unchecked(AORTA_TRACE_CONCAT(_aorta_trace_v_, __LINE__));    \
      ::aorta_trace_record_counter((scope_obj).handle(),                       \
          AORTA_TRACE_CONCAT(_aorta_trace_e_, __LINE__),                       \
          static_cast<uint8_t>(level),                                         \
          AORTA_TRACE_CONCAT(_aorta_trace_v_, __LINE__));                      \
    }                                                                          \
  } while (0)
