//! Span / instant / counter recording — the per-event hot path.
//!
//! A timed span is emitted as a `SliceBegin`+`SliceEnd` pair sharing one
//! `span_id`; both records are written to the owning thread's ring
//! ([`super::ring`]) at **end** time (we have both timestamps then), so a span
//! costs two clock reads + two ring appends and never blocks. Nesting is tracked
//! by a thread-local open-span stack (RAII → LIFO), giving each child its
//! parent's `span_id`. Standalone `instant`/`counter` events are single records.
//!
//! # Gating contract (two layers, like `record_by_handle`)
//!
//! [`span_begin`] / [`instant`] / [`counter`] check only the **master** switch
//! ([`trace_enabled`]) — one relaxed load + predicted-not-taken branch when off.
//! The per-scope **level/tier** filter (`level >= scope_threshold`) is the
//! caller's job, done **inline at the call site against a cached gate byte**
//! (the SDK macros and the wired service/action sites do this *before* calling,
//! so neither the FFI nor a registry lookup is hit on the disabled path). Doing
//! the scope-gate check here would force a registry lookup per event; the cached
//! call-site gate is the load-bearing near-zero-off primitive.
//!
//! `span_end` deliberately does NOT re-check the master switch — if `span_begin`
//! opened a span while enabled, the completed span is always flushed even if
//! tracing was toggled off meanwhile.

use super::chunk::{ChunkEventType, RingRecord, FLAG_ASYNC};
use super::intern;
use super::ring::{self, ThreadRing};
use super::{trace_enabled, FixedHistogram};
use crate::time::monotonic_ns;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Opaque token returned by [`span_begin`] and passed back to [`span_end`].
/// `0` is the inert token (tracing was off at begin), making `span_end` a no-op.
pub type SpanToken = u64;

/// Active trace context for a thread. `trace_id` all-zero = no active trace.
#[derive(Clone, Copy)]
struct TraceContext {
    trace_id: [u8; 16],
    parent_span_id: u64,
    /// Set when the context came from inbound propagation (RPC/action), so a
    /// root span ending does NOT auto-clear it (the caller owns its lifetime).
    external: bool,
}

impl TraceContext {
    const EMPTY: TraceContext = TraceContext {
        trace_id: [0u8; 16],
        parent_span_id: 0,
        external: false,
    };
}

struct OpenSpan {
    span_id: u64,
    parent_span_id: u64,
    trace_id: [u8; 16],
    scope_id: u16,
    event_id: u16,
    level: u8,
    flags: u8,
    start_mono_ns: u64,
    start_thread_ns: u64,
}

struct ThreadState {
    ring: Arc<ThreadRing>,
    thread_iid: u16,
    ctx: TraceContext,
    open: Vec<OpenSpan>,
    /// `splitmix64` state for minting `trace_id`s. Seeded ONCE per thread from a
    /// crypto source (one `getrandom` via `Uuid::new_v4`, amortized over the
    /// thread's lifetime); thereafter each `trace_id` is a few ns of pure ALU.
    /// A `trace_id` needs high entropy + low collision, NOT crypto strength, so a
    /// per-thread PRNG is the right tool — calling `Uuid::new_v4` per root span
    /// is a ~800 ns (A78) `getrandom` on the hot path (regression fixed here).
    rng: u64,
}

impl ThreadState {
    fn init() -> Self {
        let (hi, lo) = uuid::Uuid::new_v4().as_u64_pair();
        ThreadState {
            ring: ring::register_thread_ring(),
            thread_iid: intern::register_current_thread(),
            ctx: TraceContext::EMPTY,
            open: Vec::with_capacity(8),
            // Mix both halves so two threads that draw correlated uuids still
            // diverge immediately.
            rng: hi ^ lo.rotate_left(32),
        }
    }

    /// Mint a fresh 128-bit `trace_id` from the per-thread PRNG (cheap, no
    /// syscall). Non-zero by construction (all-zero is the "no trace" sentinel).
    fn next_trace_id(&mut self) -> [u8; 16] {
        let a = splitmix64(&mut self.rng);
        let b = splitmix64(&mut self.rng);
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&a.to_le_bytes());
        id[8..].copy_from_slice(&b.to_le_bytes());
        if id == [0u8; 16] {
            id[0] = 1;
        }
        id
    }
}

/// `splitmix64` — a fast, well-distributed 64-bit PRNG step.
#[inline]
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        // Flush any still-open spans as completed-at-now, so a manual/FFI span
        // left open when the thread exits is not silently lost.
        // RAII spans are already closed by this point; this catches the rest.
        // Touches only `self.ring` + `crate::time` (both global, not thread-local
        // — safe during TLS destruction).
        if !self.open.is_empty() {
            let end = monotonic_ns();
            while let Some(span) = self.open.pop() {
                let begin = RingRecord {
                    trace_id: span.trace_id,
                    span_id: span.span_id,
                    parent_span_id: span.parent_span_id,
                    mono_ns: span.start_mono_ns,
                    thread_delta_ns: 0,
                    value: 0,
                    scope_id: span.scope_id,
                    event_id: span.event_id,
                    thread_id: self.thread_iid,
                    level: span.level,
                    event_type: ChunkEventType::SliceBegin as u8,
                    flags: span.flags | FLAG_ASYNC,
                };
                let end_rec = RingRecord {
                    mono_ns: end,
                    event_type: ChunkEventType::SliceEnd as u8,
                    ..begin
                };
                self.ring.push(&begin);
                self.ring.push(&end_rec);
            }
        }
        // Tell the drain the producer is gone so it can reclaim the ring once
        // fully consumed.
        self.ring.mark_orphaned();
    }
}

thread_local! {
    static STATE: RefCell<Option<ThreadState>> = const { RefCell::new(None) };
}

#[inline]
fn with_state<R>(f: impl FnOnce(&mut ThreadState) -> R) -> Option<R> {
    STATE
        .try_with(|cell| {
            let mut b = cell.borrow_mut();
            if b.is_none() {
                *b = Some(ThreadState::init());
            }
            f(b.as_mut().expect("trace thread state"))
        })
        // Main-thread TLS is destroyed before process-static destructors run.
        // Tracing is best-effort there; do not create replacement thread state
        // or register another destructor during the teardown pass.
        .ok()
}

/// Per-process salt mixed into span ids (low-entropy but unique within process;
/// only `trace_id` needs high entropy).
static NODE_SALT: OnceLock<u64> = OnceLock::new();
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(1);

fn node_salt() -> u64 {
    *NODE_SALT.get_or_init(|| {
        let (hi, _lo) = uuid::Uuid::new_v4().as_u64_pair();
        hi
    })
}

#[inline]
fn next_span_id() -> u64 {
    let c = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let id = c ^ node_salt();
    // span_id 0 is invalid; `c >= 1` so it is a safe non-zero fallback.
    if id == 0 {
        c
    } else {
        id
    }
}

/// Whether `thread_dur` (CPU-time-in-span) capture is on. Off by default; the
/// thread-CPU clock is a syscall, so this is opt-in via
/// `AORTA_TRACE_THREAD_TIME` (set by the tracer runtime init).
static THREAD_TIME: AtomicBool = AtomicBool::new(false);

/// Enable/disable `thread_dur` capture (cold path: config / runtime init).
pub fn set_thread_time_enabled(on: bool) {
    THREAD_TIME.store(on, Ordering::Relaxed);
}

/// Is `thread_dur` capture enabled?
#[inline]
pub fn thread_time_enabled() -> bool {
    THREAD_TIME.load(Ordering::Relaxed)
}

/// Begin a timed span. Returns a [`SpanToken`] to pass to [`span_end`] (or `0`
/// if tracing is off). `scope` is a scope handle ([`super::intern_scope`]);
/// `event_id` is an interned event name ([`super::intern::intern_event`]); both
/// should be cached at the call site. `level`/`flags` are stored on the records.
///
/// **Same-thread contract:** the open-span stack is thread-local, so `span_end`
/// MUST run on the same thread as `span_begin` (the RAII [`SpanGuard`] and the
/// macros guarantee this). Ending on a different thread finds no match and is a
/// no-op (the span is dropped, though a still-open span is flushed when its
/// thread exits — see `ThreadState::drop`). For work whose interval is known but
/// spans threads (async stages, tokio task migration), use [`emit_completed_span`]
/// instead, which takes explicit start/end timestamps and touches no stack.
#[inline]
pub fn span_begin(scope: u16, event_id: u16, level: u8, flags: u8) -> SpanToken {
    if !trace_enabled() {
        return 0;
    }
    let start_mono_ns = monotonic_ns();
    let start_thread_ns = if thread_time_enabled() {
        intern::thread_cpu_ns()
    } else {
        0
    };
    let span_id = next_span_id();
    with_state(|s| {
        // Establish / inherit the trace id.
        if s.open.is_empty() && s.ctx.trace_id == [0u8; 16] {
            s.ctx.trace_id = s.next_trace_id();
            s.ctx.external = false;
        }
        let parent_span_id = match s.open.last() {
            Some(top) => top.span_id,
            None => s.ctx.parent_span_id,
        };
        s.open.push(OpenSpan {
            span_id,
            parent_span_id,
            trace_id: s.ctx.trace_id,
            scope_id: scope,
            event_id,
            level,
            flags,
            start_mono_ns,
            start_thread_ns,
        });
        span_id
    })
    .unwrap_or(0)
}

/// End the span identified by `token`, flushing its begin+end records, and
/// return the span's wall duration in ns (so one site can also feed the
/// aggregate histogram without a third clock read — see [`SpanGuard`]). A `0`
/// token, or a token with no matching open span, returns `0` and records nothing.
#[inline]
pub fn span_end(token: SpanToken) -> u64 {
    if token == 0 {
        return 0;
    }
    let end_mono_ns = monotonic_ns();
    let end_thread_ns = if thread_time_enabled() {
        intern::thread_cpu_ns()
    } else {
        0
    };
    with_state(|s| {
        // Fast path: LIFO (RAII) → the span is on top. Otherwise search (a span
        // ended out of stack order, e.g. manual / async use) and mark it async.
        let pos = if s.open.last().map(|o| o.span_id) == Some(token) {
            Some(s.open.len() - 1)
        } else {
            s.open.iter().rposition(|o| o.span_id == token)
        };
        let Some(pos) = pos else {
            return 0;
        };
        let out_of_order = pos != s.open.len() - 1;
        let span = s.open.remove(pos);
        let duration_ns = end_mono_ns.saturating_sub(span.start_mono_ns);

        let thread_delta_ns =
            if span.start_thread_ns != 0 || (thread_time_enabled() && end_thread_ns != 0) {
                end_thread_ns.saturating_sub(span.start_thread_ns)
            } else {
                0
            };
        let mut flags = span.flags;
        if out_of_order {
            flags |= FLAG_ASYNC;
        }

        let begin = RingRecord {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            mono_ns: span.start_mono_ns,
            thread_delta_ns: 0,
            value: 0,
            scope_id: span.scope_id,
            event_id: span.event_id,
            thread_id: s.thread_iid,
            level: span.level,
            event_type: ChunkEventType::SliceBegin as u8,
            flags,
        };
        let end = RingRecord {
            mono_ns: end_mono_ns,
            thread_delta_ns,
            event_type: ChunkEventType::SliceEnd as u8,
            ..begin
        };
        s.ring.push(&begin);
        s.ring.push(&end);

        // A self-minted root trace ends when its stack empties — let the next
        // root mint a fresh trace_id. Externally-set context is left alone.
        if s.open.is_empty() && !s.ctx.external {
            s.ctx = TraceContext::EMPTY;
        }
        duration_ns
    })
    .unwrap_or(0)
}

/// RAII guard that closes a span on drop and feeds the SAME timing to BOTH the
/// waterfall ring (via [`span_end`]) and the aggregate-tier histogram — one
/// instrumentation site, two outputs, one pair of clock reads. Returned by the
/// `aorta_trace_span!` macro. Inert (`token == 0`) when the call site was gated
/// off, so dropping it does nothing.
pub struct SpanGuard {
    token: SpanToken,
    hist: Option<&'static FixedHistogram>,
}

impl SpanGuard {
    /// A live guard: closing it emits the ring begin/end pair and records the
    /// duration into `hist` (the aggregate p50/p95/p99 metric).
    pub fn new(token: SpanToken, hist: Option<&'static FixedHistogram>) -> Self {
        SpanGuard { token, hist }
    }

    /// An inert guard (call site gated off): dropping it does nothing.
    pub fn inert() -> Self {
        SpanGuard {
            token: 0,
            hist: None,
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        // Token 0 = inert OR a begin that raced a runtime disable (the macro's
        // gate passed but `span_begin` saw tracing off and returned 0). Either
        // way nothing was measured — recording `span_end(0) == 0` into the
        // histogram would inject bogus zero-latency samples into p50/p95/p99.
        if self.token == 0 {
            return;
        }
        let duration_ns = span_end(self.token);
        if let Some(h) = self.hist {
            h.record(duration_ns);
        }
    }
}

/// Emit a span for an **already-completed** interval `[start_mono_ns,
/// end_mono_ns]` — for instrumentation points (service/action stages) that
/// measured the interval themselves rather than wrapping a block. Unlike
/// [`span_begin`]/[`span_end`] this does NOT touch the thread-local open-span
/// stack, so it is safe to call from async tasks that migrate threads. The span
/// joins the active trace context if one is set, otherwise inherits an open span,
/// otherwise mints an ephemeral root. Master-gated (the caller applies the tier
/// gate). `value`/`thread_delta` are 0 for stage spans.
#[inline]
pub fn emit_completed_span(
    scope: u16,
    event_id: u16,
    level: u8,
    start_mono_ns: u64,
    end_mono_ns: u64,
    flags: u8,
) {
    if !trace_enabled() {
        return;
    }
    let span_id = next_span_id();
    let _ = with_state(|s| {
        let (trace_id, parent_span_id) = if s.ctx.trace_id != [0u8; 16] {
            let parent = match s.open.last() {
                Some(top) => top.span_id,
                None => s.ctx.parent_span_id,
            };
            (s.ctx.trace_id, parent)
        } else if let Some(top) = s.open.last() {
            (top.trace_id, top.span_id)
        } else {
            (s.next_trace_id(), 0)
        };
        let begin = RingRecord {
            trace_id,
            span_id,
            parent_span_id,
            mono_ns: start_mono_ns,
            thread_delta_ns: 0,
            value: 0,
            scope_id: scope,
            event_id,
            thread_id: s.thread_iid,
            level,
            event_type: ChunkEventType::SliceBegin as u8,
            flags,
        };
        let end = RingRecord {
            mono_ns: end_mono_ns.max(start_mono_ns),
            event_type: ChunkEventType::SliceEnd as u8,
            ..begin
        };
        s.ring.push(&begin);
        s.ring.push(&end);
    });
}

/// Record a point-in-time INSTANT event for `(scope, event)`.
#[inline]
pub fn instant(scope: u16, event_id: u16, level: u8) {
    emit_point(scope, event_id, level, ChunkEventType::Instant, 1);
}

/// Record a COUNTER sample `value` for `(scope, event)`.
#[inline]
pub fn counter(scope: u16, event_id: u16, level: u8, value: u64) {
    emit_point(scope, event_id, level, ChunkEventType::Counter, value);
}

#[inline]
fn emit_point(scope: u16, event_id: u16, level: u8, ty: ChunkEventType, value: u64) {
    if !trace_enabled() {
        return;
    }
    let mono_ns = monotonic_ns();
    let _ = with_state(|s| {
        let (trace_id, parent_span_id) = match s.open.last() {
            Some(top) => (top.trace_id, top.span_id),
            None => (s.ctx.trace_id, s.ctx.parent_span_id),
        };
        let rec = RingRecord {
            trace_id,
            span_id: 0,
            parent_span_id,
            mono_ns,
            thread_delta_ns: 0,
            value,
            scope_id: scope,
            event_id,
            thread_id: s.thread_iid,
            level,
            event_type: ty as u8,
            flags: 0,
        };
        s.ring.push(&rec);
    });
}

/// Install inbound trace context (CLIENT/SERVER propagation, action goal_id).
/// The active trace becomes `trace_id` with `parent_span_id` as the parent of
/// the next root span; it is NOT auto-cleared on span end.
pub fn set_trace_context(trace_id: [u8; 16], parent_span_id: u64) {
    let _ = with_state(|s| {
        s.ctx = TraceContext {
            trace_id,
            parent_span_id,
            external: true,
        };
    });
}

/// Clear any inbound trace context (end of a propagated scope).
pub fn clear_trace_context() {
    let _ = with_state(|s| {
        if s.open.is_empty() {
            s.ctx = TraceContext::EMPTY;
        } else {
            // Spans still open: only drop the external flag so it auto-clears
            // when they finish, rather than orphaning their trace mid-flight.
            s.ctx.external = false;
        }
    });
}

/// The active `(trace_id, span_id)` to inject into an outbound hop, or `None`
/// when there is no active trace. `span_id` is the innermost open span (the
/// causal parent of the remote work), falling back to the context parent.
pub fn current_outbound_context() -> Option<([u8; 16], u64)> {
    with_state(|s| {
        let trace_id = match s.open.last() {
            Some(top) => top.trace_id,
            None => s.ctx.trace_id,
        };
        if trace_id == [0u8; 16] {
            return None;
        }
        let span_id = match s.open.last() {
            Some(top) => top.span_id,
            None => s.ctx.parent_span_id,
        };
        Some((trace_id, span_id))
    })
    .flatten()
}

/// `TraceParent.flags` bit: the originator sampled this trace (always set today
/// — context is only injected when tracing is enabled at the call site).
pub const TRACEPARENT_SAMPLED: u8 = 0x01;

/// The trace identity that crosses a process boundary: the trace plus the
/// span on the SENDING side that is the causal parent of the remote work.
/// Wire layout (25 bytes): `16B trace_id + 8B span_id LE + 1B flags`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    pub span_id: u64,
    pub flags: u8,
}

/// Serialized [`TraceParent`] size on the wire.
pub const TRACEPARENT_WIRE_LEN: usize = 25;

impl TraceParent {
    /// Serialize to the 25-byte wire form.
    pub fn to_wire_bytes(&self) -> [u8; TRACEPARENT_WIRE_LEN] {
        let mut out = [0u8; TRACEPARENT_WIRE_LEN];
        out[..16].copy_from_slice(&self.trace_id);
        out[16..24].copy_from_slice(&self.span_id.to_le_bytes());
        out[24] = self.flags;
        out
    }

    /// Parse the 25-byte wire form. `None` on wrong length or an all-zero
    /// `trace_id` (the "no trace" sentinel — never a valid propagated trace).
    pub fn from_wire_bytes(bytes: &[u8]) -> Option<TraceParent> {
        if bytes.len() != TRACEPARENT_WIRE_LEN {
            return None;
        }
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&bytes[..16]);
        if trace_id == [0u8; 16] {
            return None;
        }
        let mut span = [0u8; 8];
        span.copy_from_slice(&bytes[16..24]);
        Some(TraceParent {
            trace_id,
            span_id: u64::from_le_bytes(span),
            flags: bytes[24],
        })
    }
}

/// Full identity for a span emitted with EXPLICIT ids (no thread-local context
/// lookup): used when the span id must exist before the span is recorded — the
/// client mints its RPC span id up front so the wire can carry it as the
/// server's parent, then records the span with the same id after the reply.
#[derive(Clone, Copy, Debug)]
pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: u64,
    pub parent_span_id: u64,
}

impl SpanLink {
    /// A child of this link: the SAME trace, a fresh span id, parented to this
    /// span. Used to chain sequential sub-phases of ONE logical operation into a
    /// single connected trace so a remote/UI consumer sees one waterfall — e.g.
    /// the scheduler's queue_wait → group_wait → callback, where each phase is a
    /// child of the previous (the converter draws a flow arrow per resolvable
    /// `parent_span_id`, so the parent must be an emitted span).
    #[inline]
    pub fn child(&self) -> SpanLink {
        SpanLink {
            trace_id: self.trace_id,
            span_id: next_span_id(),
            parent_span_id: self.span_id,
        }
    }
}

/// Mint a fresh process-unique span id (for explicit-id emission via
/// [`emit_completed_span_linked`]). Cheap: one relaxed counter increment.
#[inline]
pub fn mint_span_id() -> u64 {
    next_span_id()
}

/// Build the [`SpanLink`] for an outbound hop, minting the hop's span id up
/// front: joins the caller's open span / propagated context when present,
/// otherwise mints a fresh root trace. `None` when tracing is off. Does NOT
/// mutate the thread-local context — the caller owns the returned identity and
/// emits the span explicitly (async-migration safe).
pub fn outbound_link() -> Option<SpanLink> {
    if !trace_enabled() {
        return None;
    }
    with_state(|s| {
        let (trace_id, parent_span_id) = match s.open.last() {
            Some(top) => (top.trace_id, top.span_id),
            None if s.ctx.trace_id != [0u8; 16] => (s.ctx.trace_id, s.ctx.parent_span_id),
            None => (s.next_trace_id(), 0),
        };
        Some(SpanLink {
            trace_id,
            span_id: next_span_id(),
            parent_span_id,
        })
    })
    .flatten()
}

/// Build the per-request SERVER ROOT identity for an INBOUND hop: joins the
/// propagated caller context when present, otherwise mints a fresh root trace
/// (the request is still one coherent tree). The caller stores the link for
/// the request's lifetime, emits the ENCLOSING stage (e.g.
/// `received_to_reply` / `accepted_to_finished`) under exactly this identity,
/// and records the internal stages as its children
/// ([`crate::trace::record_stage_elapsed_under`]) — so a remote caller sees
/// ONE cross-process link to the root, with the server's internal structure
/// nested beneath it. `None` when tracing is off.
pub fn inbound_root_link(parent: Option<&TraceParent>) -> Option<SpanLink> {
    if !trace_enabled() {
        return None;
    }
    match parent {
        Some(tp) => Some(SpanLink {
            trace_id: tp.trace_id,
            span_id: next_span_id(),
            parent_span_id: tp.span_id,
        }),
        None => with_state(|s| SpanLink {
            trace_id: s.next_trace_id(),
            span_id: next_span_id(),
            parent_span_id: 0,
        }),
    }
}

/// Emit a completed-interval span with EXPLICIT identity (see [`SpanLink`]).
/// Like [`emit_completed_span`] it never touches the open-span stack, so it is
/// safe from async tasks that migrate threads; unlike it, the trace/span/parent
/// ids come from the caller (cross-process propagation), not thread state.
#[inline]
pub fn emit_completed_span_linked(
    scope: u16,
    event_id: u16,
    level: u8,
    start_mono_ns: u64,
    end_mono_ns: u64,
    flags: u8,
    link: SpanLink,
) {
    emit_completed_span_linked_with_thread_delta(
        scope,
        event_id,
        level,
        start_mono_ns,
        end_mono_ns,
        0,
        flags,
        link,
    );
}

/// Like [`emit_completed_span_linked`], but also records `thread_delta_ns` (the
/// CPU-time-in-span captured by the caller) on the end record → Chrome `tdur`.
/// Use only for a SYNCHRONOUS interval the caller timed on ONE thread (e.g. a
/// timer callback's `event.run()`); CPU time across an `.await` is meaningless.
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn emit_completed_span_linked_with_thread_delta(
    scope: u16,
    event_id: u16,
    level: u8,
    start_mono_ns: u64,
    end_mono_ns: u64,
    thread_delta_ns: u64,
    flags: u8,
    link: SpanLink,
) {
    if !trace_enabled() {
        return;
    }
    let _ = with_state(|s| {
        let begin = RingRecord {
            trace_id: link.trace_id,
            span_id: link.span_id,
            parent_span_id: link.parent_span_id,
            mono_ns: start_mono_ns,
            thread_delta_ns: 0,
            value: 0,
            scope_id: scope,
            event_id,
            thread_id: s.thread_iid,
            level,
            event_type: ChunkEventType::SliceBegin as u8,
            flags,
        };
        let end = RingRecord {
            mono_ns: end_mono_ns.max(start_mono_ns),
            thread_delta_ns,
            event_type: ChunkEventType::SliceEnd as u8,
            ..begin
        };
        s.ring.push(&begin);
        s.ring.push(&end);
    });
}

/// RAII adoption of a propagated [`TraceParent`] as the current thread's trace
/// context (server side, around a user handler invocation): spans opened inside
/// the guarded scope parent to the remote caller's span. Dropping the guard
/// clears the context (via [`clear_trace_context`], which defers if spans are
/// still open). Same-thread scope only — work the handler spawns elsewhere is
/// NOT covered (use explicit links there).
#[must_use]
pub struct AdoptedContext(());

impl Drop for AdoptedContext {
    fn drop(&mut self) {
        clear_trace_context();
    }
}

/// Install `parent` as the thread's trace context for the current scope.
pub fn adopt_remote_context(parent: &TraceParent) -> AdoptedContext {
    set_trace_context(parent.trace_id, parent.span_id);
    AdoptedContext(())
}

#[cfg(test)]
#[path = "../../test/trace_span_tests.rs"]
mod tests;
