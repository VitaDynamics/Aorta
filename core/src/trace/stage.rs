//! Trace stage taxonomy + lock-free per-stage latency recording.
//!
//! A *stage* is one segment of a service (RPC) or action (goal/feedback/result)
//! flow whose latency we aggregate. Each stage records into its own
//! [`FixedHistogram`] plus a tier-gated waterfall span. All timing is monotonic
//! (the caller passes a `latency_ns` derived from [`crate::time::monotonic_ns`]
//! or an `Instant` delta — never the wall clock).
//!
//! Recording is gated by the master switch ([`trace_enabled`]): when off, a
//! record is a single relaxed load + branch. Instrumentation sites SHOULD also
//! gate the latency computation behind `trace_enabled()` so the clock read is
//! skipped entirely when off.
//!
//! These histograms are tracing-OWNED; this module does not import
//! `crate::telemetry`.

use crate::trace::chunk::FLAG_ASYNC;
use crate::trace::{
    counter, emit_completed_span, emit_completed_span_linked,
    emit_completed_span_linked_with_thread_delta, gate_allows, instant, intern_event, intern_scope,
    level_value, scope_gate, trace_enabled, FixedHistogram, HistogramSnapshot, LogLevel, SpanLink,
};
use std::sync::atomic::AtomicU8;
use std::sync::OnceLock;

/// Identifier for a recorded stage. The `u8` discriminant is stable (it crosses
/// the C-ABI in Step 10) and indexes the per-stage histogram table.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageId {
    /// Client-side full service-call latency (`Client::call` enter → return).
    ServiceClientCall = 0,
    /// Server-side request received → dispatched to the handler (queue wait).
    ServiceRequestReceivedToDispatched = 1,
    /// Server-side request received → reply sent (server busy time).
    ServiceReceivedToReply = 2,
    /// Action goal sent → accepted (client send → accept observed).
    ActionGoalSentToAccepted = 3,
    /// Action accepted → execution started.
    ActionAcceptedToExecutionStarted = 4,
    /// Action accepted → terminal (finished/aborted/cancelled).
    ActionAcceptedToFinished = 5,
    /// Interval between consecutive feedback messages for a goal.
    ActionFeedbackInterval = 6,
    /// Client-side network round-trip estimate for a service call:
    /// `attempt elapsed − server busy duration` (both monotonic durations, so
    /// no cross-host clock subtraction). Derived from the reply-context
    /// attachment; recorded only when the server echoed one.
    ServiceNetworkEstimate = 7,
    /// Server-side executor queue wait: request enqueued → handler starts
    /// running. Executor-dispatch path only — direct dispatch
    /// has no queue; the manual-channel path is user-paced.
    ServiceQueueWait = 8,
    /// Server-side user handler invocation duration. Measures
    /// the SYNCHRONOUS handler call; work the handler spawns elsewhere (e.g.
    /// replying from a task) is not included — `received_to_reply` covers the
    /// full window.
    ServiceUserCallback = 9,
    /// Action execute-callback duration: handler future invoked → completed
    /// (the action analog of the queue wait is the existing
    /// `accepted_to_execution_started`).
    ActionUserCallback = 10,
    /// Client-observed result delivery: `wait_result` entry → result obtained
    /// (or timed out) — the client half of "terminal → client
    /// receives result". Recorded for every outcome.
    ActionResultWait = 11,
    /// Server-side cancel path: cancel request accepted → goal reaches the
    /// Canceled terminal state.
    ActionCancelRequestedToCanceled = 12,
    /// Server-side reply transmission: `reply()`/`reply_error()` entry → the
    /// zenoh send completed — splits "handler done" from "reply on the wire"
    /// inside `received_to_reply`'s tail.
    ServiceReplySend = 13,
    /// Server-side goal admission: send_goal request received → accept/reject
    /// reply ready (dedup + concurrency checks, status queryable declare,
    /// initial status publish). Covers REJECTED goals too — parented to the
    /// caller's goal_sent span, not the goal root (which rejects never mint).
    ActionGoalAdmission = 14,
    /// Server-side result delivery: get_result request received → response
    /// built (the server half of result delivery; the client
    /// half is `ActionResultWait`).
    ActionGetResultQuery = 15,
    /// Publisher-side publish: `publish()` entry → zenoh enqueue completed
    /// (serialize + shm copy + fire-and-forget put).
    TopicPublish = 16,
    /// Subscriber-side user callback duration for a delivered message.
    /// Parented to the publisher's `TopicPublish` span (pub → callback
    /// transit) when its traceparent rode the message attachment.
    TopicCallback = 17,
    /// Scheduler: event enqueue (construction) → dispatch start. Async span
    /// (the ingress→executor handoff may cross worker threads).
    SchedQueueWait = 18,
    /// Scheduler: callback-group lock-acquisition wait (MutuallyExclusive
    /// contention). Async span; absent for Reentrant groups (no lock).
    SchedGroupWait = 19,
    /// Scheduler: synchronous `event.run()` for a TIMER callback (subscribers
    /// and services already have their own callback span). Carries `thread_dur`.
    SchedCallback = 20,
    /// Scheduler: an event's age when it leaves the subscriber overflow-relay
    /// queue (construction → relay-dequeue). The relay is a SECOND queue before
    /// the executor, so this sojourn is otherwise silently folded into
    /// `SchedQueueWait` (which is measured construction → dispatch). Async span;
    /// emitted only on the overflow path so a slow callback backing the relay up
    /// is attributable, not mis-blamed on scheduler queue_wait.
    SchedRelayWait = 21,
    /// Client-side END-TO-END goal span: `send_goal` (request) → terminal result
    /// obtained. The single client slice covering the whole action, joined to
    /// the goal's trace under `goal_sent_to_accepted` (which nests at its head).
    /// Recorded at most ONCE per goal and ONLY on a terminal result — so unlike
    /// `ActionResultWait` (which also records timeouts/errors), its presence is
    /// a positive "the client received the result" signal. Emitted from BOTH the
    /// `get_result()` polling path and `wait_result()`.
    ActionGoalLifetime = 22,
}

/// Number of distinct stages.
pub const STAGE_COUNT: usize = 23;

impl StageId {
    /// Every stage, in discriminant order.
    pub const ALL: [StageId; STAGE_COUNT] = [
        StageId::ServiceClientCall,
        StageId::ServiceRequestReceivedToDispatched,
        StageId::ServiceReceivedToReply,
        StageId::ActionGoalSentToAccepted,
        StageId::ActionAcceptedToExecutionStarted,
        StageId::ActionAcceptedToFinished,
        StageId::ActionFeedbackInterval,
        StageId::ServiceNetworkEstimate,
        StageId::ServiceQueueWait,
        StageId::ServiceUserCallback,
        StageId::ActionUserCallback,
        StageId::ActionResultWait,
        StageId::ActionCancelRequestedToCanceled,
        StageId::ServiceReplySend,
        StageId::ActionGoalAdmission,
        StageId::ActionGetResultQuery,
        StageId::TopicPublish,
        StageId::TopicCallback,
        StageId::SchedQueueWait,
        StageId::SchedGroupWait,
        StageId::SchedCallback,
        StageId::SchedRelayWait,
        StageId::ActionGoalLifetime,
    ];

    /// Stable, dotted stage name for export/analysis.
    pub const fn name(self) -> &'static str {
        match self {
            StageId::ServiceClientCall => "service.client_call",
            StageId::ServiceRequestReceivedToDispatched => "service.request_received_to_dispatched",
            StageId::ServiceReceivedToReply => "service.received_to_reply",
            StageId::ActionGoalSentToAccepted => "action.goal_sent_to_accepted",
            StageId::ActionAcceptedToExecutionStarted => "action.accepted_to_execution_started",
            StageId::ActionAcceptedToFinished => "action.accepted_to_finished",
            StageId::ActionFeedbackInterval => "action.feedback_interval",
            StageId::ServiceNetworkEstimate => "service.network_estimate",
            StageId::ServiceQueueWait => "service.queue_wait",
            StageId::ServiceUserCallback => "service.user_callback",
            StageId::ActionUserCallback => "action.user_callback",
            StageId::ActionResultWait => "action.result_wait",
            StageId::ActionCancelRequestedToCanceled => "action.cancel_requested_to_canceled",
            StageId::ServiceReplySend => "service.reply_send",
            StageId::ActionGoalAdmission => "action.goal_admission",
            StageId::ActionGetResultQuery => "action.get_result_query",
            StageId::TopicPublish => "topic.publish",
            StageId::TopicCallback => "topic.callback",
            StageId::SchedQueueWait => "sched.queue_wait",
            StageId::SchedGroupWait => "sched.group_wait",
            StageId::SchedCallback => "sched.callback",
            StageId::SchedRelayWait => "sched.relay_wait",
            StageId::ActionGoalLifetime => "action.goal_lifetime",
        }
    }

    /// Reconstruct from a `u8` discriminant (for the C-ABI). `None` if unknown.
    pub const fn from_u8(v: u8) -> Option<StageId> {
        match v {
            0 => Some(StageId::ServiceClientCall),
            1 => Some(StageId::ServiceRequestReceivedToDispatched),
            2 => Some(StageId::ServiceReceivedToReply),
            3 => Some(StageId::ActionGoalSentToAccepted),
            4 => Some(StageId::ActionAcceptedToExecutionStarted),
            5 => Some(StageId::ActionAcceptedToFinished),
            6 => Some(StageId::ActionFeedbackInterval),
            7 => Some(StageId::ServiceNetworkEstimate),
            8 => Some(StageId::ServiceQueueWait),
            9 => Some(StageId::ServiceUserCallback),
            10 => Some(StageId::ActionUserCallback),
            11 => Some(StageId::ActionResultWait),
            12 => Some(StageId::ActionCancelRequestedToCanceled),
            13 => Some(StageId::ServiceReplySend),
            14 => Some(StageId::ActionGoalAdmission),
            15 => Some(StageId::ActionGetResultQuery),
            16 => Some(StageId::TopicPublish),
            17 => Some(StageId::TopicCallback),
            18 => Some(StageId::SchedQueueWait),
            19 => Some(StageId::SchedGroupWait),
            20 => Some(StageId::SchedCallback),
            21 => Some(StageId::SchedRelayWait),
            22 => Some(StageId::ActionGoalLifetime),
            _ => None,
        }
    }
}

/// One histogram per stage. `FixedHistogram::new()` is `const`, so no lazy init
/// is needed. Tracing-owned; never read by telemetry.
static STAGES: [FixedHistogram; STAGE_COUNT] = [const { FixedHistogram::new() }; STAGE_COUNT];

/// Record a stage latency in nanoseconds. Zero-overhead disabled path: a single
/// relaxed master-gate load + a predicted-not-taken branch.
#[inline]
pub fn record_stage(stage: StageId, latency_ns: u64) {
    if !trace_enabled() {
        return;
    }
    STAGES[stage as usize].record(latency_ns);
}

/// The scope under which the built-in service/action stage **waterfall spans**
/// are recorded. The aggregate histograms are always-on (master gate);
/// the per-stage spans are tier-gated under this scope, so `AORTA_TRACE=off` (or
/// silencing just this scope) drops the timeline spans without losing the
/// p50/p95/p99 aggregates.
pub const STAGE_SCOPE_NAME: &str = "aorta.stage";

struct StageTrace {
    scope: u16,
    gate: &'static AtomicU8,
    events: [u16; STAGE_COUNT],
}

static STAGE_TRACE: OnceLock<StageTrace> = OnceLock::new();

fn stage_trace() -> &'static StageTrace {
    STAGE_TRACE.get_or_init(|| {
        let scope = intern_scope(STAGE_SCOPE_NAME);
        let mut events = [0u16; STAGE_COUNT];
        for stage in StageId::ALL {
            events[stage as usize] = intern_event(stage.name());
        }
        StageTrace {
            scope,
            gate: scope_gate(scope),
            events,
        }
    })
}

/// Record `monotonic_ns() - start_mono_ns` for `stage`, reading the clock ONLY
/// when tracing is enabled. Use at an instrumentation END point that captured a
/// start timestamp earlier: if tracing was toggled OFF in between, the clock read
/// is skipped entirely (honouring the disabled-path contract) rather than reading
/// the clock and then discarding it inside [`record_stage`]. `start_mono_ns` must
/// come from [`crate::time::monotonic_ns`].
///
/// This also emits a **waterfall span** for the interval (one site, both
/// tiers — the always-on aggregate histogram plus the tier-gated timeline span),
/// so real service/action traffic shows up in `ui.perfetto.dev` with no extra
/// instrumentation. The span is gated by the `aorta.stage` scope at T1 (`Info`);
/// the histogram records whenever the master is on.
#[inline]
pub fn record_stage_elapsed(stage: StageId, start_mono_ns: u64) {
    if !trace_enabled() {
        return;
    }
    finish_stage(stage, start_mono_ns, None);
}

/// Like [`record_stage_elapsed`], but the waterfall span is emitted under a
/// **per-primitive** event interned via [`stage_event_for`] (e.g.
/// `service.received_to_reply:nav`), so a process hosting several
/// services/actions stays attributable on the timeline. The aggregate
/// histogram is still the global per-stage one — per-primitive percentiles
/// come from the waterfall offline, keeping the fixed histogram set bounded.
#[inline]
pub fn record_stage_elapsed_as(stage: StageId, start_mono_ns: u64, event: u16) {
    if !trace_enabled() {
        return;
    }
    finish_stage(stage, start_mono_ns, Some(event));
}

/// Like [`record_stage_elapsed_as`], but the waterfall span carries an EXPLICIT
/// identity (cross-process propagation): the caller pre-minted `link.span_id`
/// (so the wire could carry it as the remote side's parent) and the span must
/// be recorded under exactly that id. Used by the client for
/// `service.client_call` when a traceparent was injected on the query.
#[inline]
pub fn record_stage_elapsed_linked(
    stage: StageId,
    start_mono_ns: u64,
    event: u16,
    link: crate::trace::SpanLink,
) {
    if !trace_enabled() {
        return;
    }
    finish_stage_linked(stage, start_mono_ns, event, link);
}

/// Like [`record_stage_elapsed_as`], but the waterfall span is recorded as a
/// CHILD of `parent` (fresh span id, parented to `parent.span_id` on
/// `parent.trace_id`): server-side INTERNAL stages pass the per-request root
/// link so the request renders as one tree — a single cross-process link to
/// the root, internal structure nested beneath
/// ([`crate::trace::inbound_root_link`]). `None` falls back to the
/// local-context behavior of [`record_stage_elapsed_as`].
#[inline]
pub fn record_stage_elapsed_under(
    stage: StageId,
    start_mono_ns: u64,
    event: u16,
    parent: Option<&crate::trace::SpanLink>,
) {
    if !trace_enabled() {
        return;
    }
    match parent {
        Some(root) => finish_stage_linked(
            stage,
            start_mono_ns,
            event,
            crate::trace::SpanLink {
                trace_id: root.trace_id,
                span_id: crate::trace::mint_span_id(),
                parent_span_id: root.span_id,
            },
        ),
        None => finish_stage(stage, start_mono_ns, Some(event)),
    }
}

#[inline]
fn finish_stage(stage: StageId, start_mono_ns: u64, event: Option<u16>) {
    let end = crate::time::monotonic_ns();
    STAGES[stage as usize].record(end.saturating_sub(start_mono_ns));
    let st = stage_trace();
    if gate_allows(st.gate, LogLevel::Info) {
        emit_completed_span(
            st.scope,
            event.unwrap_or(st.events[stage as usize]),
            level_value(LogLevel::Info),
            start_mono_ns,
            end,
            0,
        );
    }
}

#[inline]
fn finish_stage_linked(
    stage: StageId,
    start_mono_ns: u64,
    event: u16,
    link: crate::trace::SpanLink,
) {
    let end = crate::time::monotonic_ns();
    STAGES[stage as usize].record(end.saturating_sub(start_mono_ns));
    let st = stage_trace();
    if gate_allows(st.gate, LogLevel::Info) {
        crate::trace::emit_completed_span_linked(
            st.scope,
            event,
            level_value(LogLevel::Info),
            start_mono_ns,
            end,
            0,
            link,
        );
    }
}

/// Intern the per-primitive waterfall event name for `stage` on `name`:
/// `"<stage-name>:<primitive-name>"`. Call ONCE at Service/Client/Action
/// construction (interning is bounded by the primitive count, not the call
/// rate) and feed the returned id to [`record_stage_elapsed_as`]. Interns
/// regardless of the gate so a runtime enable mid-flight needs no lazy path.
///
/// The name is canonicalized exactly like the key/routing layer
/// (`normalize_path`), so a client built with `/nav/` and a server built with
/// `nav` — one primitive on the wire — intern ONE event, not aliases that
/// would split the per-primitive timeline/percentiles. A name that fails to
/// normalize is interned raw (constructing that primitive will fail at the
/// routing layer anyway).
pub fn stage_event_for(stage: StageId, name: &str) -> u16 {
    let canonical = crate::key_expr::normalize_path(name).ok();
    let canonical = canonical.as_deref().unwrap_or(name);
    intern_event(&format!("{}:{}", stage.name(), canonical))
}

/// Intern a per-primitive LIFECYCLE-MARKER event (`"<kind>:<name>"`,
/// canonicalized exactly like [`stage_event_for`]) for waterfall-only markers
/// emitted via [`emit_stage_span_under`] (e.g. per-feedback publish/receive —
/// events that need timeline visibility but no aggregate histogram of their
/// own). Call once at primitive construction.
pub fn marker_event_for(kind: &str, name: &str) -> u16 {
    let canonical = crate::key_expr::normalize_path(name).ok();
    let canonical = canonical.as_deref().unwrap_or(name);
    intern_event(&format!("{}:{}", kind, canonical))
}

/// Emit a WATERFALL-ONLY span under the stage scope as a child of `parent`
/// — for lifecycle markers with no aggregate histogram (see
/// [`marker_event_for`]). Tier-gated exactly like the built-in stage spans.
#[inline]
pub fn emit_stage_span_under(
    event: u16,
    start_mono_ns: u64,
    end_mono_ns: u64,
    parent: &crate::trace::SpanLink,
) {
    if !trace_enabled() {
        return;
    }
    let st = stage_trace();
    if gate_allows(st.gate, LogLevel::Info) {
        crate::trace::emit_completed_span_linked(
            st.scope,
            event,
            level_value(LogLevel::Info),
            start_mono_ns,
            end_mono_ns,
            0,
            crate::trace::SpanLink {
                trace_id: parent.trace_id,
                span_id: crate::trace::mint_span_id(),
                parent_span_id: parent.span_id,
            },
        );
    }
}

/// Like [`emit_stage_span_under`], but the span is emitted at EXACTLY the
/// pre-minted `link` identity — for markers whose span id must exist before
/// emission because the wire carries it (e.g. the per-feedback publish span,
/// whose id rides the feedback attachment as the receive-marker's parent).
#[inline]
pub fn emit_stage_span_at(
    event: u16,
    start_mono_ns: u64,
    end_mono_ns: u64,
    link: crate::trace::SpanLink,
) {
    if !trace_enabled() {
        return;
    }
    let st = stage_trace();
    if gate_allows(st.gate, LogLevel::Info) {
        crate::trace::emit_completed_span_linked(
            st.scope,
            event,
            level_value(LogLevel::Info),
            start_mono_ns,
            end_mono_ns,
            0,
            link,
        );
    }
}

/// Snapshot one stage's histogram.
pub fn stage_snapshot(stage: StageId) -> HistogramSnapshot {
    STAGES[stage as usize].snapshot()
}

/// Snapshot every stage paired with its dotted name (for export/analysis).
pub fn all_stage_snapshots() -> Vec<(&'static str, HistogramSnapshot)> {
    StageId::ALL
        .iter()
        .map(|&s| (s.name(), stage_snapshot(s)))
        .collect()
}

// ---- Pub/Sub topic path ---------------------------------------------------
//
// The topic data path is the hottest in the system, so it is gated TWO ways:
//   - the aggregate per-stage HISTOGRAM records whenever the master switch is
//     on (like every other stage) — always-available publish/callback latency
//     percentiles with no waterfall cost;
//   - the per-message waterfall SPAN (and, on the publisher, the traceparent
//     it puts on the wire) is gated under a DEDICATED `aorta.topic` scope at
//     `Debug` level, so the default tier (`always` ⇒ `Info` floor) records
//     NOTHING on the topic path. Operators opt in with `AORTA_TRACE=standard`
//     / `verbose`, or target just this path with `AORTA_TRACE=aorta.topic=standard`
//     (any level at or below `Debug`). This keeps kHz topics off the ring and
//     out of the per-host capacity by default while leaving full per-message
//     recording one env var away.

/// Scope for the pub/sub topic waterfall spans — independent of `aorta.stage`
/// so operators can toggle the high-volume topic path on its own.
pub const TOPIC_SCOPE_NAME: &str = "aorta.topic";

/// One interned trace scope + its cached gate (this shape
/// existed as three identical structs — Topic/Sched/Flow — plus the
/// events-carrying StageTrace, which keeps its own type).
struct ScopeTrace {
    scope: u16,
    gate: &'static AtomicU8,
}

impl ScopeTrace {
    fn init(name: &str) -> Self {
        let scope = intern_scope(name);
        ScopeTrace {
            scope,
            gate: scope_gate(scope),
        }
    }
}

static TOPIC_TRACE: OnceLock<ScopeTrace> = OnceLock::new();

fn topic_trace() -> &'static ScopeTrace {
    TOPIC_TRACE.get_or_init(|| ScopeTrace::init(TOPIC_SCOPE_NAME))
}

/// Whether topic-path SPANS should be emitted now: master on AND the
/// `aorta.topic` scope admits `Debug`. The publisher checks this to decide
/// whether to mint a span id and put a traceparent on the wire; the subscriber
/// checks it to decide whether to decode the wire traceparent. The aggregate
/// histograms do NOT consult this (they follow the master switch only).
#[inline]
pub fn topic_scope_enabled() -> bool {
    trace_enabled() && gate_allows(topic_trace().gate, LogLevel::Debug)
}

/// Record a topic-path stage (`TopicPublish` / `TopicCallback`): the aggregate
/// histogram always (master-gated, reading the end clock here), plus the
/// waterfall span at the pre-built `link` when topic spans are enabled. `link`
/// is `Some` only when the caller saw [`topic_scope_enabled`] at the start of
/// the interval; passing `None` records the histogram alone. The span carries
/// the caller's EXPLICIT identity (publisher: the pre-minted id that rode the
/// wire; subscriber: the callback span parented to the publish span), so pub →
/// callback renders as one cross-process flow arrow.
#[inline]
pub fn record_topic_span(stage: StageId, start_mono_ns: u64, event: u16, link: Option<SpanLink>) {
    if !trace_enabled() {
        return;
    }
    let end = crate::time::monotonic_ns();
    STAGES[stage as usize].record(end.saturating_sub(start_mono_ns));
    if let Some(link) = link {
        let tt = topic_trace();
        // Re-check the gate at record time (it may have toggled off mid-interval);
        // matches the disabled-path contract of the stage helpers above.
        if gate_allows(tt.gate, LogLevel::Debug) {
            emit_completed_span_linked(
                tt.scope,
                event,
                level_value(LogLevel::Debug),
                start_mono_ns,
                end,
                0,
                link,
            );
        }
    }
}

// ---- Scheduler / executor path (self-exoneration & root-cause) -------------
//
// Same two-tier shape as the topic path: aggregate histograms follow the master
// switch; the per-event WATERFALL spans + the `sched.*` instants/counters gate
// under a dedicated `aorta.sched` scope at Debug, so the default tier records
// nothing on the scheduler path. `AORTA_TRACE=aorta.sched=standard` (or global
// `standard`/`verbose`) turns it on. queue_wait/group_wait are ASYNC spans
// (they span an `.await` that may cross worker threads); only the synchronous
// timer `SchedCallback` is a thread-lane span and the only one that carries
// `thread_dur`.

/// Scope for the scheduler-path waterfall spans / markers — independent of
/// `aorta.stage` and `aorta.topic` so the executor view toggles on its own.
pub const SCHED_SCOPE_NAME: &str = "aorta.sched";

static SCHED_TRACE: OnceLock<ScopeTrace> = OnceLock::new();

fn sched_trace() -> &'static ScopeTrace {
    SCHED_TRACE.get_or_init(|| ScopeTrace::init(SCHED_SCOPE_NAME))
}

/// Whether scheduler-path SPANS/instants should be emitted now: master on AND
/// the `aorta.sched` scope admits `Debug`. The executor checks this to decide
/// whether to mint span ids, maintain the waiter counters' span emission, etc.
/// The queue_wait/group_wait HISTOGRAMS do NOT consult this (master only).
#[inline]
pub fn sched_scope_enabled() -> bool {
    trace_enabled() && gate_allows(sched_trace().gate, LogLevel::Debug)
}

/// Record a scheduler WAIT stage (`SchedQueueWait` / `SchedGroupWait`): the
/// aggregate histogram always (master-gated), plus an ASYNC waterfall span at
/// `link` when `aorta.sched` is enabled. The caller passes BOTH endpoints in
/// the `monotonic_ns` epoch (queue_wait, whose start is a `std::time::Instant`,
/// reconstructs `start = end - duration`). `link` is `Some` only when the
/// caller saw [`sched_scope_enabled`]; `None` records the histogram alone.
/// `event` carries the per-group label for `SchedGroupWait`
/// (`sched.group_wait:<group>`).
#[inline]
pub fn record_sched_wait(
    stage: StageId,
    start_mono_ns: u64,
    end_mono_ns: u64,
    event: u16,
    link: Option<SpanLink>,
) {
    if !trace_enabled() {
        return;
    }
    STAGES[stage as usize].record(end_mono_ns.saturating_sub(start_mono_ns));
    if let Some(link) = link {
        let st = sched_trace();
        if gate_allows(st.gate, LogLevel::Debug) {
            emit_completed_span_linked(
                st.scope,
                event,
                level_value(LogLevel::Debug),
                start_mono_ns,
                end_mono_ns,
                FLAG_ASYNC,
                link,
            );
        }
    }
}

/// Record a `SchedCallback` (a synchronous timer `event.run()` the caller timed
/// on ONE thread): histogram + sync waterfall span carrying `thread_delta_ns`
/// (CPU-in-span → wall-vs-cpu). Fully `aorta.sched`-gated (incl. the histogram)
/// because reading thread-CPU is a syscall and timers already aggregate
/// elsewhere — pay it only when asked. No-op unless `aorta.sched` admits Debug.
#[inline]
pub fn record_sched_callback(
    start_mono_ns: u64,
    end_mono_ns: u64,
    thread_delta_ns: u64,
    event: u16,
    link: SpanLink,
) {
    let st = sched_trace();
    if !trace_enabled() || !gate_allows(st.gate, LogLevel::Debug) {
        return;
    }
    STAGES[StageId::SchedCallback as usize].record(end_mono_ns.saturating_sub(start_mono_ns));
    emit_completed_span_linked_with_thread_delta(
        st.scope,
        event,
        level_value(LogLevel::Debug),
        start_mono_ns,
        end_mono_ns,
        thread_delta_ns,
        0,
        link,
    );
}

/// Emit an `aorta.sched` INSTANT (e.g. `sched.reentrant_rejected:<op>`,
/// `sched.group_fallback:<handle>`, `sched.priority_inversion:<group>`) on the
/// calling thread — a first-class misuse/diagnosis marker (the trace record has
/// no per-span attributes, so these facts ride event names). `aorta.sched`-gated.
#[inline]
pub fn sched_instant(event: u16) {
    let st = sched_trace();
    if trace_enabled() && gate_allows(st.gate, LogLevel::Debug) {
        instant(st.scope, event, level_value(LogLevel::Debug));
    }
}

/// Emit an `aorta.sched` COUNTER sample (e.g. `sched.group_waiters:<group>`
/// queue depth) on the calling thread. `aorta.sched`-gated.
#[inline]
pub fn sched_counter(event: u16, value: u64) {
    let st = sched_trace();
    if trace_enabled() && gate_allows(st.gate, LogLevel::Debug) {
        counter(st.scope, event, level_value(LogLevel::Debug), value);
    }
}

// ---- Flow-path tracing -----------------------------

const FLOW_SCOPE_NAME: &str = "aorta.flow";

static FLOW_TRACE: OnceLock<ScopeTrace> = OnceLock::new();

fn flow_trace() -> &'static ScopeTrace {
    FLOW_TRACE.get_or_init(|| ScopeTrace::init(FLOW_SCOPE_NAME))
}

/// Emit an `aorta.flow` INSTANT (scope open/close/cancel, budget expiry —
/// facts ride event names; scope names are `'static` so per-name interning
/// is bounded). Gated exactly like `aorta.sched`.
#[inline]
pub fn flow_instant(event: u16) {
    let ft = flow_trace();
    if trace_enabled() && gate_allows(ft.gate, LogLevel::Debug) {
        instant(ft.scope, event, level_value(LogLevel::Debug));
    }
}

/// Emit an `aorta.flow` COUNTER sample (task spawn/finish/panic totals).
#[inline]
pub fn flow_counter(event: u16, value: u64) {
    let ft = flow_trace();
    if trace_enabled() && gate_allows(ft.gate, LogLevel::Debug) {
        counter(ft.scope, event, level_value(LogLevel::Debug), value);
    }
}

/// `true` when `aorta.flow` instants would be emitted — lets call sites skip
/// even the event-name interning when off.
#[inline]
pub fn flow_trace_active() -> bool {
    let ft = flow_trace();
    trace_enabled() && gate_allows(ft.gate, LogLevel::Debug)
}
