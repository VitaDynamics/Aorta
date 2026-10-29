use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Mutex, RwLock},
};

use zenoh::bytes::ZBytes;
use zenoh::handlers::{RingChannel, RingChannelHandler};
use zenoh::pubsub::Subscriber as ZenohSubscriber;
use zenoh::sample::Sample;

use crate::error::AortaResult;
use crate::event_queue::{
    CallbackGroupHandle, CallbackGroupType, Event, EventKind, EventPriority, EventQueueSender,
    InflightGuard,
};
use crate::key_expr::KeyExprBuilder;
use crate::log_prelude::*;
use crate::node::Node;
use crate::qos::{Priority, QoS};
use crate::schema::{infer_aorta_header_layout_from_bfbs, AortaHeaderLayout, SchemaHash};
use crate::schema_registry::{SchemaByKeyValue, SchemaRegistry};
use crate::telemetry::{RuntimeTelemetryGuard, TelemetryResourceGuard};

fn sample_into_payload(sample: zenoh::sample::Sample) -> ZBytes {
    let fields: zenoh::sample::SampleFields = sample.into();
    fields.payload
}

/// Run the user subscriber callback under the topic.callback trace bracket:
/// the always-on `topic.callback` histogram (master-gated), plus
/// — when topic spans are enabled (`cb_link` is `Some`) — the waterfall span at
/// `cb_link` (child of the publisher's `topic.publish` span) AND adoption of
/// that identity as the thread context, so user `aorta_trace_span!` calls and
/// any re-publish inside the callback nest under it. Drops (dedup/out-of-order)
/// never reach here, so only delivered messages are timed.
#[inline]
fn run_traced_callback(cb_link: Option<crate::trace::SpanLink>, event: u16, run: impl FnOnce()) {
    let start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
    {
        let _ctx = cb_link.map(|l| {
            crate::trace::adopt_remote_context(&crate::trace::TraceParent {
                trace_id: l.trace_id,
                span_id: l.span_id,
                flags: crate::trace::TRACEPARENT_SAMPLED,
            })
        });
        run();
    }
    if let Some(start) = start {
        crate::trace::record_topic_span(
            crate::trace::StageId::TopicCallback,
            start,
            event,
            cb_link,
        );
    }
}

/// Message metadata provided to subscriber callbacks
///
/// Contains metadata about the received message including QoS properties,
/// and timestamps.
#[derive(Debug, Clone)]
pub struct MessageContext {
    /// Topic on which the message was published
    pub topic: Arc<str>,
    /// Local receive timestamp (nanoseconds since epoch)
    pub receive_stamp_ns: i64,
    /// Subscriber-configured callback/event priority (derived from subscriber QoS priority).
    pub priority: Priority,
    /// Publisher-assigned timestamp (if available)
    pub publisher_stamp_ns: Option<i64>,
    /// Publisher node identifier.
    ///
    /// Core subscriber callback path does not parse payload header by default.
    /// SDK-level opt-in strategies may populate this field.
    pub publisher_node: Option<String>,
    /// Message sequence number.
    ///
    /// Core subscriber callback path does not parse payload header by default.
    /// SDK-level opt-in strategies may populate this field.
    pub sequence: Option<u64>,
    /// Source application-level namespace (e.g. the robot SN) parsed from the
    /// received key, for cross-namespace / fleet fan-out subscribers (`*`).
    ///
    /// Only populated when the subscriber was created with namespace-capture
    /// enabled (see `SubscriberOptions::capture_source_namespace`, set by the
    /// `*_for` cross-namespace creation methods). `None` for ordinary
    /// (non-namespaced) subscribers — the common on-robot case stays on the
    /// fast path. See
    /// `spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md` §4.3/§5.3.
    pub source_namespace: Option<String>,
}

/// Subscriber configuration options
///
/// Controls subscriber-side behavior including history retrieval and local buffering.
/// Note: Unlike DDS, Zenoh subscriber QoS does not create compatibility issues with
/// publishers. These settings only affect local behavior.
#[derive(Debug, Clone)]
pub struct SubscriberOptions {
    /// Callback group execution mode.
    /// Default: MutuallyExclusive
    pub callback_group: CallbackGroupType,

    /// Executor-scoped callback group handle.
    /// When set, this takes precedence over `callback_group`.
    /// Default: None
    pub callback_group_handle: Option<CallbackGroupHandle>,

    /// Local message queue depth for event processing.
    /// This controls the internal buffer size, not network-level history.
    ///
    /// Values of `0` are treated as a footgun and **silently clamped to
    /// the default 1024** — an unbounded queue lets a slow callback
    /// accumulate the entire incoming stream and exhaust process memory.
    /// If you genuinely need a different bound, set a deliberate value
    /// (consider `overflow_policy = Relay` for backpressure handling).
    ///
    /// Default: 1024
    pub history_depth: usize,

    /// Whether to prefetch schema metadata during subscriber initialization.
    /// This improves late-schema availability for header-strategy subscribers,
    /// at the cost of extra startup/network activity.
    /// Disable only when startup latency is more important than schema warmup.
    /// Default: true
    pub prefetch_schema_on_init: bool,

    /// Whether to parse `aorta_header` from payload bytes and populate header-derived
    /// fields in `MessageContext`.
    ///
    /// Keep this disabled for performance-critical subscribers. Enable only when
    /// subscriber-side logic needs generic header context (e.g. de-dup/reorder analysis).
    /// Default: false
    pub enable_header_context: bool,

    /// Optional precomputed Aorta header field layout hint for this subscriber topic.
    ///
    /// Typed SDK paths can provide this to avoid runtime schema lookups for
    /// header parsing strategy.
    /// Default: None
    pub header_layout_hint: Option<AortaHeaderLayout>,

    /// Whether to parse the source application-level namespace (e.g. robot SN)
    /// out of each received key and populate `MessageContext.source_namespace`.
    ///
    /// Set by the cross-namespace `*_for` creation methods; needed for fleet
    /// fan-out subscribers (namespace `*`) where each sample may originate from
    /// a different robot. Costs one per-sample key split, so it stays off for
    /// ordinary subscribers (the common on-robot fast path). Implied whenever
    /// `namespace` is set.
    /// Default: false
    pub capture_source_namespace: bool,

    /// Optional application-level namespace prefix (e.g. a robot SN, or `*` for
    /// fleet fan-out) for a cross-namespace cloud subscriber. When set, the
    /// subscription key is built as `{namespace}/aorta/{group}/pub/{topic}` and
    /// source-namespace capture is implied. `None` = ordinary subscriber.
    /// See `spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md` §5.
    pub namespace: Option<String>,

    /// Whether to include topic string in callback `MessageContext`.
    /// Enable for callbacks that need to read the topic from context.
    /// Default: false (fast path — skips per-message topic Arc::clone)
    pub include_topic_in_context: bool,

    /// Whether to capture local receive timestamp in callback `MessageContext`.
    /// Enable for callbacks that need to read the receive timestamp.
    /// Default: false (fast path — skips per-message `node.now()`)
    pub include_receive_timestamp: bool,

    /// Whether to include mapped QoS priority in callback `MessageContext`.
    /// Enable for callbacks that need to read the priority from context.
    /// Default: false (fast path — skips per-message priority mapping)
    pub include_priority_in_context: bool,

    /// Whether to include transport publisher timestamp in callback `MessageContext`.
    /// Enable for callbacks that need to read the publisher timestamp.
    /// Default: false (fast path — skips per-message timestamp extraction)
    pub include_publisher_timestamp: bool,

    /// Subscriber QoS for local callback/event behavior.
    /// The callback/event priority is derived from `qos.priority` and remains stable
    /// for the subscriber lifetime (no per-sample dynamic remapping).
    /// Default: `QoS::default()`
    pub qos: QoS,

    /// Whether to perform subscriber-side de-duplication using `(topic, publisher_node, sequence)`.
    /// Default: false
    pub enable_dedup: bool,

    /// Sliding window size (per source) for sequence de-duplication.
    /// Only effective when `enable_dedup` is true.
    /// Default: 1024
    pub dedup_window: usize,

    /// Whether to detect out-of-order sequence at subscriber side.
    /// Default: false
    pub enable_reorder_check: bool,

    /// Whether to drop out-of-order messages when reorder check is enabled.
    /// Default: false (analyze only, still deliver callbacks)
    pub drop_out_of_order: bool,

    /// Backpressure strategy when executor queue is full.
    /// - Drop: drop current event and emit rate-limited warning.
    /// - Relay: enqueue to a bounded overflow relay queue serviced by one relay task.
    ///
    /// Default: Drop
    pub overflow_policy: SubscriberOverflowPolicy,

    /// Bounded relay queue capacity used only when `overflow_policy == Relay`.
    /// Values <= 0 are normalized to 1.
    /// Default: 1024
    pub overflow_relay_capacity: usize,

    /// Per-topic in-flight receive cap (QoS & buffering redesign, Tier A).
    ///
    /// Bounds the number of events for *this* subscriber that may be queued on
    /// the shared executor at once. When the cap is reached, the newest event
    /// is dropped at admission (drop-newest) and `dropped_for_depth` is
    /// incremented — giving per-topic drop-isolation so a slow callback on one
    /// topic cannot fill the shared queue and starve other topics.
    ///
    /// `0` (the default) disables the gate entirely — byte-for-byte the
    /// historical behavior (the single shared 1024-slot executor queue is the
    /// only bound). Applies to `DeliveryMode::Executor`.
    ///
    /// For `DeliveryMode::InlinePull` this is the Zenoh `RingChannel` capacity
    /// (the keep-last-N depth); it must be `>= 1`.
    /// Default: 0
    pub receive_depth: usize,

    /// How received samples reach the consumer (Tier A vs Tier B-pull).
    /// Default: `Executor` (callback on the shared executor — today's model).
    pub delivery: DeliveryMode,

    /// Drop direction when `receive_depth` is exceeded.
    /// - `DropNewest` (default): reject the incoming event at the gate. The
    ///   only option for `DeliveryMode::Executor` (Tier A).
    /// - `DropOldest`: keep the newest N, evict the oldest. Requires
    ///   `DeliveryMode::InlinePull` (the native `RingChannel` KEEP_LAST).
    pub receive_overflow: ReceiveOverflow,

    /// Declarative subscriber→readiness bindings (Flow Concepts §6.5): feed a
    /// `WindowSync` input / satisfy a `ReadinessSet` condition on every
    /// arriving sample, on the RX path, for ANY delivery mode. Built via
    /// [`with_window_feed`](Self::with_window_feed) /
    /// [`with_readiness_satisfy`](Self::with_readiness_satisfy); indices are
    /// validated at declare time. Wires observe the stream — delivery is
    /// unaffected.
    pub readiness_wires: Vec<crate::flow::ReadinessWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriberOverflowPolicy {
    Drop,
    Relay,
}

/// How received samples are delivered to the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryMode {
    /// Callback dispatched on the node's shared executor (preserves callback
    /// groups, priority, and FIFO ordering). The default; pairs with the
    /// Tier A `receive_depth` in-flight cap.
    #[default]
    Executor,
    /// No callback: the subscriber owns a Zenoh-native `RingChannel(receive_depth)`
    /// and the application pulls samples via `recv`/`try_recv`/`recv_timeout`.
    /// Native KEEP_LAST / drop-oldest with structural per-topic isolation
    /// (Tier B-pull) — the latest-wins sensor idiom.
    InlinePull,
    /// Run the callback inline on the Zenoh RX thread (zero hop). The fastest
    /// and highest-footgun path; **deferred** (rejected at declare time) and
    /// never offered to Python.
    InlinePush,
}

/// Drop direction when a subscriber's `receive_depth` is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReceiveOverflow {
    /// Reject the newest event at the admission gate (Tier A; cheap).
    #[default]
    DropNewest,
    /// Evict the oldest, keep the newest N. Requires `DeliveryMode::InlinePull`
    /// (the native `RingChannel`).
    DropOldest,
}

const SUBSCRIBER_OVERFLOW_WARN_EVERY: u64 = 1024;
static SUBSCRIBER_OVERFLOW_DROP_COUNT: AtomicU64 = AtomicU64::new(0);

fn warn_subscriber_event_drop(reason: &str) {
    let dropped = SUBSCRIBER_OVERFLOW_DROP_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if dropped == 1 || dropped.is_multiple_of(SUBSCRIBER_OVERFLOW_WARN_EVERY) {
        aorta_core_warn!(
            "Aorta WARN: subscriber callback event dropped: {reason} (total_dropped={dropped})."
        );
    }
}

static TIER_A_SIZING_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// One-time-per-process notice about the Tier A (`receive_depth`) isolation
/// sizing rule.
///
/// Tier A bounds *this* subscriber's in-flight events, but the underlying shared
/// executor queue is a single fixed-capacity tokio mpsc that **cannot grow**.
/// Per-topic drop-isolation therefore holds only while the sum of all
/// `receive_depth`s on the executor stays within that capacity (default 1024);
/// beyond that, a cross-topic burst can fill the shared queue and one topic's
/// traffic can still shed another's. We surface this once (rather than silently)
/// so operators sizing many capped topics know the limit and the escape hatch:
/// a pull subscriber (`DeliveryMode::InlinePull`) gives **structural** isolation
/// (its own physical ring) independent of the shared-queue size.
fn warn_tier_a_isolation_sizing_once() {
    if TIER_A_SIZING_WARNED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        aorta_core_warn!(
            "Aorta NOTE: receive_depth gives per-topic drop-isolation only while the \
             SUM of receive_depths across this executor's subscribers stays within the shared \
             executor-queue capacity (default 1024) — the shared tokio queue cannot grow. For \
             structural isolation regardless of sizing, use a pull subscriber \
             (delivery=InlinePull)."
        );
    }
}

/// Header-derived context parsed from payload bytes.
///
/// This parser is intended for SDK-level opt-in strategies (e.g. de-dup/reorder).
/// Core subscriber callback path does not invoke it by default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedHeaderContext {
    pub publisher_node: Option<String>,
    pub sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriberRuntimeStats {
    pub duplicate_drop_count: u64,
    pub out_of_order_count: u64,
    pub out_of_order_drop_count: u64,
    pub executor_drop_count: u64,
    pub local_drop_count: u64,
    pub overflow_relay_enqueue_count: u64,
    pub overflow_relay_drop_count: u64,
    /// Events shed by the Tier A `receive_depth` gate (drop-newest). Sourced
    /// from the subscriber's own shed counter (`dropped_for_depth()`) when the
    /// snapshot is taken via `Subscriber::runtime_stats()`, so drop accounting
    /// is complete in one place. Always `0` when `receive_depth == 0` or for
    /// pull subscribers (whose drops happen natively inside the Zenoh ring).
    pub receive_depth_drop_count: u64,
    /// Events dropped at dequeue past their `FlowAdmission` deadline.
    /// Sourced from `Subscriber::dropped_for_deadline()` at snapshot,
    /// like `receive_depth_drop_count` above (telemetry v7 —
    /// the v5 changelog claimed this wire field before it existed).
    pub dropped_for_deadline: u64,
}

#[derive(Debug, Default)]
struct SubscriberRuntimeStatsInner {
    duplicate_drop_count: AtomicU64,
    out_of_order_count: AtomicU64,
    out_of_order_drop_count: AtomicU64,
    executor_drop_count: AtomicU64,
    local_drop_count: AtomicU64,
    overflow_relay_enqueue_count: AtomicU64,
    overflow_relay_drop_count: AtomicU64,
}

impl SubscriberRuntimeStatsInner {
    fn snapshot(&self) -> SubscriberRuntimeStats {
        SubscriberRuntimeStats {
            duplicate_drop_count: self.duplicate_drop_count.load(Ordering::Relaxed),
            out_of_order_count: self.out_of_order_count.load(Ordering::Relaxed),
            out_of_order_drop_count: self.out_of_order_drop_count.load(Ordering::Relaxed),
            executor_drop_count: self.executor_drop_count.load(Ordering::Relaxed),
            local_drop_count: self.local_drop_count.load(Ordering::Relaxed),
            overflow_relay_enqueue_count: self.overflow_relay_enqueue_count.load(Ordering::Relaxed),
            overflow_relay_drop_count: self.overflow_relay_drop_count.load(Ordering::Relaxed),
            // Filled by `Subscriber::runtime_stats()` — the authoritative
            // counters live on the Subscriber, not this inner block.
            receive_depth_drop_count: 0,
            dropped_for_deadline: 0,
        }
    }

    fn on_duplicate_drop(&self) {
        self.duplicate_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_out_of_order(&self) {
        self.out_of_order_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_out_of_order_drop(&self) {
        self.out_of_order_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_executor_drop(&self) {
        self.executor_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_local_drop(&self) {
        self.local_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_overflow_relay_enqueued(&self) {
        self.overflow_relay_enqueue_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn on_overflow_relay_drop(&self) {
        self.overflow_relay_drop_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Parse `aorta_header` fields from payload bytes.
///
/// Returns empty fields when payload is not a valid Aorta FlatBuffer root
/// or when `aorta_header`/fields are absent.
pub fn parse_header_context(payload: &[u8]) -> ParsedHeaderContext {
    parse_header_context_with_layout(payload, DEFAULT_AORTA_HEADER_LAYOUT)
}

fn parse_header_context_with_layout(
    payload: &[u8],
    layout: AortaHeaderLayout,
) -> ParsedHeaderContext {
    let (publisher_node, sequence) = parse_aorta_header_fields_with_layout(payload, layout);
    ParsedHeaderContext {
        publisher_node,
        sequence,
    }
}

impl Default for SubscriberOptions {
    fn default() -> Self {
        Self {
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            history_depth: 1024,
            prefetch_schema_on_init: true,
            enable_header_context: false,
            header_layout_hint: None,
            capture_source_namespace: false,
            namespace: None,
            include_topic_in_context: false,
            include_receive_timestamp: false,
            include_priority_in_context: false,
            include_publisher_timestamp: false,
            qos: QoS::default(),
            enable_dedup: false,
            dedup_window: 1024,
            enable_reorder_check: false,
            drop_out_of_order: false,
            overflow_policy: SubscriberOverflowPolicy::Drop,
            overflow_relay_capacity: 1024,
            receive_depth: 0,
            delivery: DeliveryMode::Executor,
            receive_overflow: ReceiveOverflow::DropNewest,
            readiness_wires: Vec::new(),
        }
    }
}

impl SubscriberOptions {
    /// Create new subscriber options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set callback group execution mode.
    pub fn with_callback_group(mut self, callback_group: CallbackGroupType) -> Self {
        self.callback_group = callback_group;
        self
    }

    /// Set executor-scoped callback group handle.
    pub fn with_callback_group_handle(
        mut self,
        callback_group_handle: CallbackGroupHandle,
    ) -> Self {
        self.callback_group_handle = Some(callback_group_handle);
        self
    }

    /// Set local message queue depth.
    ///
    /// Passing `0` is **silently clamped to the default 1024** to avoid
    /// the unbounded-queue OOM footgun. See the [`history_depth`] field
    /// doc for details.
    ///
    /// [`history_depth`]: SubscriberOptions::history_depth
    pub fn with_history_depth(mut self, depth: usize) -> Self {
        self.history_depth = depth;
        self
    }

    /// Set whether to prefetch schema metadata when subscriber starts.
    pub fn with_schema_prefetch_on_init(mut self, enable: bool) -> Self {
        self.prefetch_schema_on_init = enable;
        self
    }

    /// Set whether to parse payload `aorta_header` and fill header-derived context.
    pub fn with_header_context(mut self, enable: bool) -> Self {
        self.enable_header_context = enable;
        self
    }

    /// Set optional precomputed header layout hint for strategy parsing.
    pub fn with_header_layout_hint(mut self, layout: AortaHeaderLayout) -> Self {
        self.header_layout_hint = Some(layout);
        self
    }

    /// Set whether to parse the source application-level namespace from each
    /// received key into `MessageContext.source_namespace` (cross-namespace
    /// fan-out subscribers). See `capture_source_namespace`.
    pub fn with_source_namespace_capture(mut self, enable: bool) -> Self {
        self.capture_source_namespace = enable;
        self
    }

    /// Bind this subscriber to an application-level namespace (e.g. a robot SN,
    /// or `*` for fleet fan-out). The subscription key becomes
    /// `{namespace}/aorta/{group}/pub/{topic}` and source-namespace capture is
    /// implied. See `with_source_namespace_capture`.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set whether to include topic string in callback context.
    pub fn with_topic_in_context(mut self, include: bool) -> Self {
        self.include_topic_in_context = include;
        self
    }

    /// Set whether to capture receive timestamp in callback context.
    pub fn with_receive_timestamp(mut self, include: bool) -> Self {
        self.include_receive_timestamp = include;
        self
    }

    /// Set whether to include mapped QoS priority in callback context.
    pub fn with_priority_in_context(mut self, include: bool) -> Self {
        self.include_priority_in_context = include;
        self
    }

    /// Set whether to include transport publisher timestamp in callback context.
    pub fn with_publisher_timestamp(mut self, include: bool) -> Self {
        self.include_publisher_timestamp = include;
        self
    }

    /// Wire this subscriber as [`WindowSync`](crate::flow::WindowSync)
    /// input `input` (Flow Concepts §6.5), stamped with local receive time. Use
    /// [`with_window_feed_stamped`](Self::with_window_feed_stamped) to
    /// align on the transport publisher timestamp instead. The index is
    /// validated when the subscriber is declared.
    ///
    /// Design decision: the pull/callback delivery split is
    /// enforced at DECLARE time (runtime rejection with a named error),
    /// not by splitting `Subscriber` into two types. A type split would
    /// ripple through the C-ABI handle table, the Python mirrors, and
    /// every SDK wrapper — for a misuse that is already caught loudly on
    /// the very first declare (the cost lands once at startup, never
    /// per-sample).
    pub fn with_window_feed(
        self,
        window: &std::sync::Arc<crate::flow::WindowSync>,
        input: usize,
    ) -> Self {
        self.with_window_feed_stamped(window, input, crate::flow::WindowStamp::Receive)
    }

    /// [`with_window_feed`](Self::with_window_feed) with an explicit
    /// timestamp source: sensor fusion across publishers with a
    /// shared clock domain wants `WindowStamp::Publisher`.
    pub fn with_window_feed_stamped(
        mut self,
        window: &std::sync::Arc<crate::flow::WindowSync>,
        input: usize,
        stamp: crate::flow::WindowStamp,
    ) -> Self {
        self.readiness_wires
            .push(crate::flow::ReadinessWire::Window {
                window: std::sync::Arc::clone(window),
                input,
                stamp,
            });
        self
    }

    /// Wire this subscriber to satisfy `set` condition `condition` on every
    /// arriving sample (Flow Concepts §6.5) — the `Condition::MessageAvailable`
    /// carrier. The index is validated when the subscriber is declared.
    pub fn with_readiness_satisfy(
        mut self,
        set: &std::sync::Arc<crate::flow::ReadinessSet>,
        condition: usize,
    ) -> Self {
        self.readiness_wires
            .push(crate::flow::ReadinessWire::Satisfy {
                set: std::sync::Arc::clone(set),
                condition,
            });
        self
    }

    /// Set subscriber callback/event priority.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.qos.priority = priority;
        self
    }

    /// Set subscriber QoS (priority/reliability/durability/history).
    pub fn with_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Set whether to enable subscriber-side de-duplication.
    pub fn with_dedup(mut self, enable: bool) -> Self {
        self.enable_dedup = enable;
        self
    }

    /// Set per-source de-duplication window size.
    pub fn with_dedup_window(mut self, window: usize) -> Self {
        self.dedup_window = window.max(1);
        self
    }

    /// Set whether to enable out-of-order sequence detection.
    pub fn with_reorder_check(mut self, enable: bool) -> Self {
        self.enable_reorder_check = enable;
        self
    }

    /// Set whether to drop out-of-order messages after reorder detection.
    pub fn with_drop_out_of_order(mut self, drop: bool) -> Self {
        self.drop_out_of_order = drop;
        self
    }

    /// Set subscriber callback overflow policy.
    pub fn with_overflow_policy(mut self, policy: SubscriberOverflowPolicy) -> Self {
        self.overflow_policy = policy;
        self
    }

    /// Set overflow relay queue capacity used by `SubscriberOverflowPolicy::Relay`.
    pub fn with_overflow_relay_capacity(mut self, capacity: usize) -> Self {
        self.overflow_relay_capacity = capacity.max(1);
        self
    }

    /// Set the per-topic in-flight receive cap (Tier A). `0` disables the gate
    /// (default — today's behavior). See [`SubscriberOptions::receive_depth`].
    pub fn with_receive_depth(mut self, depth: usize) -> Self {
        self.receive_depth = depth;
        self
    }

    /// Set the delivery mode (executor callback vs native pull ring).
    pub fn with_delivery(mut self, delivery: DeliveryMode) -> Self {
        self.delivery = delivery;
        self
    }

    /// Set the drop direction for the `receive_depth` cap.
    pub fn with_receive_overflow(mut self, overflow: ReceiveOverflow) -> Self {
        self.receive_overflow = overflow;
        self
    }

    /// Sugar for a Zenoh-native keep-last-`depth` pull ring (Tier B-pull):
    /// sets `delivery = InlinePull`, `receive_overflow = DropOldest`, and
    /// `receive_depth = depth`. Consume via `Subscriber::recv*`.
    pub fn with_native_ring(mut self, depth: usize) -> Self {
        self.delivery = DeliveryMode::InlinePull;
        self.receive_overflow = ReceiveOverflow::DropOldest;
        self.receive_depth = depth;
        self
    }

    /// Declare-time validation of the buffering knobs (fail loud).
    ///
    /// - `DropOldest` requires `InlinePull` (Tier A is drop-newest only).
    /// - `InlinePush` is deferred and not yet implemented.
    /// - `InlinePull` requires `receive_depth >= 1` (the ring capacity).
    /// - `InlinePull` is incompatible with the callback-only relay overflow path.
    /// - `InlinePull` rejects the executor-only header-strategy options
    ///   (`enable_header_context` / `enable_dedup` / `enable_reorder_check`).
    fn validate_delivery(&self, has_callback: bool) -> AortaResult<()> {
        use crate::error::AortaError;
        // Readiness wires: out-of-range indices are wiring bugs —
        // rejected here at declare time, never silently dropped per sample.
        for wire in &self.readiness_wires {
            wire.validate()?;
        }
        match self.delivery {
            DeliveryMode::Executor => {
                if self.receive_overflow == ReceiveOverflow::DropOldest {
                    return Err(AortaError::invalid_argument(
                        "receive_overflow=DropOldest requires delivery=InlinePull \
                         (executor delivery is drop-newest only)",
                    ));
                }
            }
            DeliveryMode::InlinePull => {
                if has_callback {
                    return Err(AortaError::invalid_argument(
                        "delivery=InlinePull takes no callback; consume via Subscriber::recv \
                         (use subscribe_pull / Subscriber::new_pull)",
                    ));
                }
                if self.receive_depth == 0 {
                    return Err(AortaError::invalid_argument(
                        "delivery=InlinePull requires receive_depth >= 1 (the ring capacity)",
                    ));
                }
                // The overflow relay queue is an executor-path feature serviced
                // by a relay task; the native pull ring is keep-last/drop-oldest
                // and `new_pull` never creates or drains a relay. Accepting Relay
                // here would silently ignore it, so fail loud — matches
                // the doc contract above and the header-option rejection below.
                if self.overflow_policy == SubscriberOverflowPolicy::Relay {
                    return Err(AortaError::invalid_argument(
                        "delivery=InlinePull does not support overflow_policy=Relay (the relay \
                         queue is an executor-path feature; the native pull ring is \
                         keep-last/drop-oldest). Drain the ring promptly via Subscriber::recv, \
                         or use the executor delivery path if you need the relay queue",
                    ));
                }
                // Header-strategy options are executor-only: the pull path does
                // no AortaHeader parse / dedup / reorder (it stays zero-overhead),
                // so `PullSample.context` carries no publisher_node/sequence. Fail
                // loud rather than silently dropping the metadata the option
                // promised. The `include_*` context flags
                // (topic / timestamps / priority) DO work on the pull path.
                if self.enable_header_context || self.enable_dedup || self.enable_reorder_check {
                    return Err(AortaError::invalid_argument(
                        "delivery=InlinePull does not support enable_header_context / \
                         enable_dedup / enable_reorder_check (these parse the AortaHeader on the \
                         executor path only); pull samples expose topic/timestamps/priority via \
                         the include_* options, and you can dedup/reorder in your own recv loop",
                    ));
                }
            }
            DeliveryMode::InlinePush => {
                return Err(AortaError::invalid_argument(
                    "delivery=InlinePush is deferred and not yet implemented",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SourceSequenceState {
    last_sequence: Option<u64>,
    dedup_recent: VecDeque<u64>,
    dedup_set: HashSet<u64>,
}

#[derive(Debug)]
struct SubscriberStrategyConfig {
    enable_dedup: bool,
    dedup_window: usize,
    enable_reorder_check: bool,
    drop_out_of_order: bool,
}

#[derive(Debug, Default)]
struct SubscriberStrategyState {
    sources: HashMap<String, SourceSequenceState>,
    source_order: VecDeque<String>,
}

#[derive(Debug)]
struct SubscriberStrategy {
    cfg: SubscriberStrategyConfig,
    state: Mutex<SubscriberStrategyState>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StrategyDecision {
    drop_duplicate: bool,
    out_of_order: bool,
    drop_out_of_order: bool,
}

impl SubscriberStrategy {
    const MAX_SOURCE_ENTRIES: usize = 4096;

    fn new(options: &SubscriberOptions) -> Option<Arc<Self>> {
        if !options.enable_dedup && !options.enable_reorder_check {
            return None;
        }
        Some(Arc::new(Self {
            cfg: SubscriberStrategyConfig {
                enable_dedup: options.enable_dedup,
                dedup_window: options.dedup_window.max(1),
                enable_reorder_check: options.enable_reorder_check,
                drop_out_of_order: options.drop_out_of_order,
            },
            state: Mutex::new(SubscriberStrategyState::default()),
        }))
    }

    fn source_key(
        topic: &str,
        publisher_node: Option<&str>,
        source_identity: Option<&str>,
    ) -> Option<String> {
        if let Some(node) = publisher_node.filter(|value| !value.is_empty()) {
            return Some(format!("{topic}|node:{node}"));
        }
        source_identity
            .filter(|value| !value.is_empty())
            .map(|source| format!("{topic}|source:{source}"))
    }

    fn process(
        &self,
        topic: &str,
        parsed: &ParsedHeaderContext,
        source_identity: Option<&str>,
    ) -> StrategyDecision {
        let Some(sequence) = parsed.sequence else {
            return StrategyDecision::default();
        };
        let Some(source_key) =
            Self::source_key(topic, parsed.publisher_node.as_deref(), source_identity)
        else {
            // No stable source identifier available. Skip de-dup/reorder decisions to avoid
            // cross-publisher false positives caused by key collapsing.
            return StrategyDecision::default();
        };
        let mut state = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if !state.sources.contains_key(&source_key) {
            state.source_order.push_back(source_key.clone());
            if state.sources.len() >= Self::MAX_SOURCE_ENTRIES {
                if let Some(evict_key) = state.source_order.pop_front() {
                    state.sources.remove(&evict_key);
                }
            }
            state
                .sources
                .insert(source_key.clone(), SourceSequenceState::default());
        }

        let source_state = state
            .sources
            .get_mut(&source_key)
            .expect("source state must exist");

        let mut decision = StrategyDecision::default();

        if self.cfg.enable_dedup {
            if source_state.dedup_set.contains(&sequence) {
                decision.drop_duplicate = true;
            } else {
                source_state.dedup_set.insert(sequence);
                source_state.dedup_recent.push_back(sequence);
                while source_state.dedup_recent.len() > self.cfg.dedup_window {
                    if let Some(old) = source_state.dedup_recent.pop_front() {
                        source_state.dedup_set.remove(&old);
                    }
                }
            }
        }

        if self.cfg.enable_reorder_check {
            if let Some(last) = source_state.last_sequence {
                if sequence <= last {
                    decision.out_of_order = true;
                    decision.drop_out_of_order = self.cfg.drop_out_of_order;
                }
            }
            if source_state
                .last_sequence
                .map(|last| sequence > last)
                .unwrap_or(true)
            {
                source_state.last_sequence = Some(sequence);
            }
        }

        decision
    }
}

/// Shared context bundled into a single `Arc` to reduce per-message atomic
/// refcount operations in the Zenoh callback closure.  Instead of 5 independent
/// `Arc::clone` calls on every received sample, we perform exactly one.
struct SubscriberSharedCtx<F> {
    callback: F,
    /// Interned `topic.callback:<topic>` waterfall event.
    topic_callback_event: u16,
    callback_topic: Arc<str>,
    empty_context_topic: Arc<str>,
    static_context_template: Option<Arc<MessageContext>>,
    strategy: Option<Arc<SubscriberStrategy>>,
    runtime_stats: Option<Arc<SubscriberRuntimeStatsInner>>,
    header_layout_cache: Arc<HeaderLayoutCache>,
    schema_registry: Option<SchemaRegistry>,
    key_builder: Option<KeyExprBuilder>,
    runtime_handle: Option<tokio::runtime::Handle>,
}

#[derive(Default)]
struct HeaderLayoutRefreshState {
    in_flight_keys: HashSet<String>,
    last_attempt_at: HashMap<String, Instant>,
}

#[derive(Default)]
struct HeaderLayoutCache {
    by_schema_hash: RwLock<HashMap<SchemaHash, AortaHeaderLayout>>,
    by_topic_key: RwLock<HashMap<String, AortaHeaderLayout>>,
    refresh_state: Mutex<HeaderLayoutRefreshState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HeaderLayoutLookup {
    layout: Option<AortaHeaderLayout>,
    needs_refresh: bool,
}

impl HeaderLayoutCache {
    fn insert_for_schema(&self, hash: SchemaHash, layout: AortaHeaderLayout) {
        let mut guard = match self.by_schema_hash.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(hash, layout);
    }

    fn insert_for_topic(&self, topic_key: &str, layout: AortaHeaderLayout) {
        let mut guard = match self.by_topic_key.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(topic_key.to_string(), layout);
    }

    fn resolve(&self, schema_hash: Option<SchemaHash>, topic_key: &str) -> HeaderLayoutLookup {
        if let Some(hash) = schema_hash {
            let guard = match self.by_schema_hash.read() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(layout) = guard.get(&hash).copied() {
                return HeaderLayoutLookup {
                    layout: Some(layout),
                    needs_refresh: false,
                };
            }

            drop(guard);
            let topic_guard = match self.by_topic_key.read() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            return HeaderLayoutLookup {
                layout: topic_guard.get(topic_key).copied(),
                // Attachment hash exists but schema-hash cache misses:
                // treat as a refresh-worthy miss even if topic fallback exists.
                needs_refresh: true,
            };
        }

        let guard = match self.by_topic_key.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        HeaderLayoutLookup {
            layout: guard.get(topic_key).copied(),
            needs_refresh: false,
        }
    }

    fn try_begin_refresh(&self, key: &str) -> bool {
        let mut guard = match self.refresh_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let now = Instant::now();
        if guard.in_flight_keys.contains(key) {
            return false;
        }
        if let Some(last) = guard.last_attempt_at.get(key) {
            if now.duration_since(*last) < HEADER_LAYOUT_REFRESH_RETRY_INTERVAL {
                return false;
            }
        }
        guard.in_flight_keys.insert(key.to_string());
        guard.last_attempt_at.insert(key.to_string(), now);
        true
    }

    fn finish_refresh(&self, key: &str) {
        let mut guard = match self.refresh_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.in_flight_keys.remove(key);
    }
}

// Flow: decoding a C-ABI priority index (0..=4, out-of-range clamps
// to Normal) is the canonical `crate::event_queue::event_priority_from_index_lossy`
// (the canonical priority index table) — the duplicate that used to live here is gone.

/// Wire → dispatch vocabulary bridge: `qos::Priority` maps 1:1 onto
/// `EventPriority` (name-for-name, all 5 levels). Applied
/// at RX unless overridden via [`Subscriber::set_flow_priority`].
pub(crate) fn map_event_priority(priority: Priority) -> EventPriority {
    match priority {
        Priority::Realtime => EventPriority::Realtime,
        Priority::High => EventPriority::High,
        Priority::Normal => EventPriority::Normal,
        Priority::Low => EventPriority::Low,
        Priority::Background => EventPriority::Background,
    }
}

/// Context-building configuration captured for the `InlinePull` path so
/// `recv` can populate a `MessageContext` per the subscriber's options without
/// re-reading them on every pull.
struct PullCtx {
    callback_topic: Arc<str>,
    empty_context_topic: Arc<str>,
    include_topic_in_context: bool,
    include_receive_timestamp: bool,
    include_priority_in_context: bool,
    include_publisher_timestamp: bool,
    capture_source_namespace: bool,
    configured_priority: Priority,
    key_builder: Option<KeyExprBuilder>,
    node: Node,
}

/// A cheaply-cloneable drain handle for a `DeliveryMode::InlinePull` subscriber.
///
/// Holds the Zenoh `RingChannelHandler` (a `Weak` to the ring) plus the context
/// config — but **not** the Zenoh subscriber (the ring's sender side, which
/// lives in `Subscriber::inner`). This split is load-bearing for teardown: a
/// blocking `recv()` keeps only this handle alive, so `Subscriber::undeclare` /
/// the C-ABI `free` can reclaim the subscriber, drop the Zenoh subscriber's
/// callback (the ring's strong `Arc` + flume sender), and thereby **unblock**
/// the waiting `recv()` (which then sees the ring disconnected and returns an
/// error). Without the split, an infinite `recv()` would pin the subscriber and
/// `free` could never complete the undeclare.
/// a C-plane data-available callback (fired on the RX
/// thread AFTER the ring store; trampoline-class contract — the C++
/// awaiter only completes an arbiter branch in it).
#[derive(Clone, Copy)]
pub struct PullNotifyHook {
    pub func: unsafe extern "C" fn(*mut std::ffi::c_void),
    pub ctx: usize,
}

/// The single-awaiter C notify-hook slot: the
/// set-or-refuse + copy-out-then-fire mechanism previously existed in
/// three identical copies (pull subscriber, ReadinessSet, WindowSync).
#[derive(Default)]
pub(crate) struct NotifyHookSlot {
    slot: parking_lot::RwLock<Option<PullNotifyHook>>,
}

impl NotifyHookSlot {
    /// Single-awaiter contract: a DIFFERENT non-null hook over a live
    /// one is refused (two concurrent awaiters would silently strand each
    /// other); same-hook re-arm is idempotent. Returns false on refusal.
    pub(crate) fn set(&self, hook: Option<PullNotifyHook>) -> bool {
        let mut slot = self.slot.write();
        if let (Some(new), Some(old)) = (hook.as_ref(), slot.as_ref()) {
            if new.ctx != old.ctx || !std::ptr::fn_addr_eq(new.func, old.func) {
                return false;
            }
        }
        *slot = hook;
        true
    }

    /// Fire the C hook (COPIED OUT before invocation — holding the
    /// read guard across the call can deadlock when an inline-resume
    /// fallback re-enters `set(write)`).
    pub(crate) fn fire(&self) {
        let hook = { *self.slot.read() };
        if let Some(hook) = hook {
            // SAFETY: setter contract — (func, ctx) stay callable until
            // cleared/freed; stale arbiter ids no-op registry-side.
            unsafe { (hook.func)(hook.ctx as *mut std::ffi::c_void) };
        }
    }
}

#[derive(Clone)]
pub struct PullRecvHandle {
    handler: Arc<RingChannelHandler<Sample>>,
    /// sample-arrival wakeup for `recv_in` (the async
    /// pull). `Notify` carries a permit, so store-then-notify never
    /// loses a wakeup even when no waiter is registered yet.
    notify: Arc<tokio::sync::Notify>,
    /// the C-plane hook slot (`aorta_sub_set_notify`).
    c_hook: Arc<NotifyHookSlot>,
    /// One-way closure flag, published by WakeOnDrop BEFORE its wake
    /// (CI-caught race: the ring sender and the wake guard drop in
    /// unspecified order, so a woken waiter may still observe a
    /// connected-but-empty ring — this flag is the authoritative
    /// nothing-more-will-arrive signal for `recv_in`).
    closed: Arc<std::sync::atomic::AtomicBool>,
    /// Ring occupancy mirror for the non-popping probe.
    /// Saturating at the ring depth (drop-oldest evictions decrement via
    /// the RX closure when the ring was full — approximated by clamping
    /// on store; exactness is not required: the probe only gates a
    /// SELF-COMPLETE whose delivery path re-checks with a real pop).
    pending_count: Arc<std::sync::atomic::AtomicU64>,
    ctx: Arc<PullCtx>,
}

/// A sample pulled from a `DeliveryMode::InlinePull` subscriber.
pub struct PullSample {
    pub payload: ZBytes,
    pub context: MessageContext,
}

impl PullRecvHandle {
    fn map_err(err: impl std::fmt::Display, op: &str) -> crate::error::AortaError {
        crate::error::AortaError::internal(format!("pull {op} failed: {err}"))
    }

    fn note_pop(&self) {
        let _ = self.pending_count.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |v| Some(v.saturating_sub(1)),
        );
    }

    /// Block until the next sample (or the subscriber disconnects).
    pub fn recv(&self) -> AortaResult<PullSample> {
        let sample = self.handler.recv().map_err(|e| Self::map_err(e, "recv"))?;
        self.note_pop();
        Ok(self.build_pull_sample(sample))
    }

    /// Non-blocking pull. `Ok(None)` when the ring is currently empty.
    pub fn try_recv(&self) -> AortaResult<Option<PullSample>> {
        match self.handler.try_recv() {
            Ok(Some(sample)) => {
                self.note_pop();
                Ok(Some(self.build_pull_sample(sample)))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(Self::map_err(err, "try_recv")),
        }
    }

    /// Async pull under a Flow scope: waits for
    /// the next sample and completes `Err(Stopped)` when the scope's
    /// EFFECTIVE cancellation fires (shield-aware — a shielded cleanup
    /// consumer keeps receiving, the shielded-cleanup idiom). The `receive_depth`
    /// ring bound is unchanged (drop-oldest). Needs no executor: pure
    /// notify + ring.
    ///
    /// ORDERING: queued samples are drained BEFORE the
    /// cancellation check — an already-cancelled scope still receives
    /// what the ring holds (drain-friendly, deliberately unlike
    /// `call_in`'s entry checkpoint; new WAITS are what cancellation
    /// prevents).
    ///
    /// Multi-consumer: concurrent `recv_in` callers on
    /// cloned handles are supported via wake hand-off — each delivered
    /// sample re-notifies so a permit can never be collapsed away while
    /// data remains.
    pub async fn recv_in(&self, scope: &crate::flow::Scope) -> crate::flow::Outcome<PullSample> {
        loop {
            // Create the notified future BEFORE the emptiness probe.
            // `WakeOnDrop` (undeclare / session close) wakes with
            // `notify_waiters()`, which stores no permit — a close landing
            // between an empty `try_recv` and a later-created `notified()`
            // would be missed forever (parked task strands until scope
            // cancel). A `Notified` created before the `notify_waiters`
            // call completes even when polled after it, so this ordering
            // closes the window; same registered-before-check pattern as
            // action_client's hand-rolled result channel.
            let notified = self.notify.notified();
            match self.try_recv() {
                Ok(Some(sample)) => {
                    // Wake hand-off: if more samples remain, the next
                    // waiter must not depend on a permit our wake consumed
                    // (notify_one stores AT MOST one). Spurious wakes are
                    // absorbed by the try_recv(None) loop arm.
                    self.notify.notify_one();
                    return Ok(sample);
                }
                Ok(None) => {}
                Err(err) => {
                    return Err(crate::flow::Stopped::fault_with_code(
                        crate::ffi::c_api::aorta_result_code_of(&err),
                        format!("pull subscriber closed: {err}"),
                    ));
                }
            }
            // Empty AND closed = nothing more can arrive. Checked AFTER
            // the drain probe (queued samples still deliver) and
            // BEFORE parking: WakeOnDrop publishes the flag before its
            // wake, so a woken-then-empty waiter lands here instead of
            // re-parking against a dead producer.
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(crate::flow::Stopped::fault(
                    "pull subscriber closed: undeclared / session closed".to_string(),
                ));
            }
            tokio::select! {
                _ = notified => {}
                reason = (crate::flow::ambient::EffectiveCancelled {
                    cancelled: scope.token().cancelled(),
                }) => {
                    return Err(crate::flow::Stopped::cancelled(reason));
                }
            }
        }
    }

    /// set/clear the C-plane data-available hook.
    /// SINGLE-AWAITER contract: setting a non-null hook over
    /// a DIFFERENT non-null hook is refused — two concurrent `next()`
    /// awaiters on one subscriber would silently strand each other.
    /// Returns false on refusal.
    pub fn set_notify_hook(&self, hook: Option<PullNotifyHook>) -> bool {
        self.c_hook.set(hook)
    }

    /// NON-POPPING emptiness probe — the C++ next()
    /// recheck must not touch its coroutine frame (nor steal a sample it
    /// could not deliver); it probes and self-completes via handles only.
    pub fn has_pending(&self) -> bool {
        // The ring handler exposes no peek; try_recv would POP. A pulled
        // sample here would be LOST, so probe via the ring's len if
        // available — zenoh's RingChannelHandler has no len() either, so
        // the probe rides a counter: the RX store bumps it, pops consume
        // it. See `pending_count`.
        self.pending_count
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
    }

    /// Pull with a timeout. `Ok(None)` when the timeout elapses with no sample.
    pub fn recv_timeout(&self, timeout: Duration) -> AortaResult<Option<PullSample>> {
        match self.handler.recv_timeout(timeout) {
            Ok(Some(sample)) => {
                self.note_pop();
                Ok(Some(self.build_pull_sample(sample)))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(Self::map_err(err, "recv_timeout")),
        }
    }

    fn build_pull_sample(&self, sample: Sample) -> PullSample {
        let ctx = self.ctx.as_ref();
        let publisher_stamp_ns = if ctx.include_publisher_timestamp {
            // Shared NTP64→ns conversion — the raw `as_u64()` bit pattern
            // is NOT nanoseconds.
            crate::flow::readiness::publisher_ns_of(&sample).map(|ns| ns as i64)
        } else {
            None
        };
        let source_namespace = if ctx.capture_source_namespace {
            ctx.key_builder
                .as_ref()
                .and_then(|kb| kb.split_full_key(sample.key_expr().as_str()))
                .and_then(|(ns, _suffix)| ns)
        } else {
            None
        };
        let context = MessageContext {
            topic: if ctx.include_topic_in_context {
                Arc::clone(&ctx.callback_topic)
            } else {
                Arc::clone(&ctx.empty_context_topic)
            },
            receive_stamp_ns: if ctx.include_receive_timestamp {
                ctx.node.now().as_nanos()
            } else {
                0
            },
            priority: if ctx.include_priority_in_context {
                ctx.configured_priority
            } else {
                Priority::Normal
            },
            publisher_stamp_ns,
            // Header-derived fields are always None on the pull path: AortaHeader
            // parsing is executor-only and `enable_header_context` is rejected for
            // InlinePull at declare time (see `validate_delivery`), so this is a
            // contract, not silent loss.
            publisher_node: None,
            sequence: None,
            source_namespace,
        };
        PullSample {
            payload: sample_into_payload(sample),
            context,
        }
    }
}

/// Flow per-endpoint capabilities, dynamically
/// settable after creation (`Subscriber::set_flow_priority` /
/// `set_flow_admission`; C-ABI `aorta_subscriber_set_priority` /
/// `aorta_subscriber_set_admission`). All reads are single Relaxed loads on
/// the RX path; every capability is absent-by-default (zero cost when off).
/// Per-endpoint Flow counting-gate options (Flow Concepts §6.4) — the options
/// struct passed to [`Subscriber::set_flow_admission`]. `#[non_exhaustive]`:
/// construct via `Default` + field assignment or [`FlowAdmission::gated`].
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct FlowAdmission {
    /// In-flight cap (0 = ungated) — the settable form of `receive_depth`.
    pub max_in_flight: usize,
    /// Per-event deadline: expired events are dropped at dequeue.
    pub deadline: Option<std::time::Duration>,
}

impl FlowAdmission {
    /// Gate at `max_in_flight` with an optional per-event deadline.
    pub fn gated(max_in_flight: usize, deadline: Option<std::time::Duration>) -> Self {
        Self {
            max_in_flight,
            deadline,
        }
    }
}

/// NOTE on atomicity: the three fields update independently (each setter
/// stores Relaxed); a sample racing a reconfiguration may observe a MIXED
/// old/new policy for that one sample — each field is individually valid,
/// so the transient is benign (accepted; a snapshot-swap would buy strict
/// coherence at the cost of an allocation per reconfigure).
pub(crate) struct FlowCaps {
    /// `EventPriority` index override; `u8::MAX` = inherit the QoS mapping.
    pub(crate) priority_override: std::sync::atomic::AtomicU8,
    /// Per-event deadline in ns from arrival; 0 = none.
    pub(crate) deadline_ns: std::sync::atomic::AtomicU64,
    /// The in-flight cap — the SAME mechanism as
    /// `SubscriberOptions::receive_depth` (Tier A), made settable; 0 = ungated.
    pub(crate) max_in_flight: AtomicUsize,
    /// Events of THIS subscriber dropped at dequeue past their deadline.
    pub(crate) deadline_dropped: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[must_use = "dropping a Subscriber undeclares it and stops delivery; bind it (`let _sub = ...`) for the subscription lifetime"]
pub struct Subscriber {
    // Both Executor and Pull modes declare a `Subscriber<()>` (Pull provides the
    // ring via an explicit callback that forwards into the `RingChannelHandler`
    // stored in `pull`). Dropping this drops the ring's sender side, which
    // unblocks any waiting `PullRecvHandle::recv`.
    // Field order is load-bearing: `inner` MUST be declared before
    // `local_queue_shutdown` (and the other local-queue handles) so that
    // implicit `Subscriber` drop releases the Zenoh subscription first and
    // only then drops the shutdown sender, terminating the drain task.
    inner: ZenohSubscriber<()>,
    topic: String,
    runtime_stats: Option<Arc<SubscriberRuntimeStatsInner>>,
    inventory_guard: Option<TelemetryResourceGuard>,
    runtime_guard: Option<RuntimeTelemetryGuard>,
    /// Per-subscriber in-flight counter for the Tier A `receive_depth` gate.
    /// Shared (`Arc`) with every admitted event's `InflightGuard`, so it stays
    /// alive until the last in-flight event for this subscriber has run.
    sub_inflight: Arc<AtomicUsize>,
    /// Count of events shed by the `receive_depth` gate (drop-newest).
    dropped_for_depth: Arc<AtomicUsize>,
    /// The configured cap (`0` = gate disabled).
    receive_depth: usize,
    /// Flow dynamic capabilities (shared with the RX closure).
    flow_caps: Arc<FlowCaps>,
    /// Readiness wiring slot (shared with the RX closure): declare-time
    /// options and post-declare `bind_window`/`bind_readiness` converge here.
    readiness: Arc<crate::flow::readiness::ReadinessWiring>,
    /// `Some` only for `DeliveryMode::InlinePull` subscribers — the cloneable
    /// drain handle (ring `Weak` + context config), independent of `inner`.
    pull: Option<PullRecvHandle>,
    // JoinHandle for the local-queue draining task spawned by
    // `spawn_local_queue`, plus the explicit shutdown signal for it.
    //
    // Termination contract: the drain task consumes the
    // queue via `EventQueueConsumer` (see `EventQueue::into_parts`), which
    // holds NO sender — so dropping the last external sender clone (the
    // Zenoh callback closures) closes the backend and `pop()` returns
    // None. That sender-drop termination is only a FALLBACK: its timing
    // depends on when Zenoh releases the closures (an assumption an earlier
    // version got wrong), so deterministic termination is driven by
    // `local_queue_shutdown` being dropped or sent; `undeclare()` drops it
    // before awaiting the handle, and implicit
    // drop of the `Subscriber` (no `Drop` impl) terminates the task via
    // natural field drop. The 2s timeout + `abort()` in `undeclare()` is a
    // safety net only for user callbacks synchronously stuck inside
    // `event.run()` — abort takes effect only at the next `.await` point,
    // so such a callback may still run to completion past the
    // `undeclare()` return (Tokio's fundamental limit; cannot preempt
    // synchronous user code). Callers needing strict
    // no-callback-after-stop semantics must synchronize via their own
    // primitives inside the callback.
    local_queue_task: Option<tokio::task::JoinHandle<()>>,
    /// Explicit shutdown signal for the local-queue drain task: drop (or
    /// send) to make the task drain the remaining queued events and exit.
    /// Drop is the trigger — no message payload needed, hence `()`.
    /// `None` for pull subscribers (no local-queue task).
    local_queue_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// Cooperative stop flag for the drain task (set on `undeclare()`'s
    /// drain timeout): every dispatch path checks it before running each
    /// event (admission linearization point) and stops dispatching once
    /// set. `None` for pull subscribers.
    local_queue_stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// Identity sentinel for the drain task: the task holds the only strong
    /// reference, so this upgrades to `None` exactly when the task ended.
    /// `None` for pull subscribers (no local-queue task).
    local_queue_liveness: Option<std::sync::Weak<()>>,
}

impl Subscriber {
    pub async fn new<F>(node: &Node, topic: &str, callback: F) -> AortaResult<Self>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        Self::new_with_options(node, topic, callback, SubscriberOptions::default()).await
    }

    pub async fn new_with_options<F>(
        node: &Node,
        topic: &str,
        callback: F,
        options: SubscriberOptions,
    ) -> AortaResult<Self>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        // Fail loud on an inconsistent buffering config before touching Zenoh.
        // This callback constructor is the executor path, so a `callback` is
        // always present; `InlinePull`/`InlinePush` are rejected here and steered
        // to `new_pull` / a future push API.
        options.validate_delivery(/* has_callback = */ true)?;

        // Cross-namespace subscriber: prepend the application-level namespace
        // (e.g. {SN}/) ahead of aorta/{group}/. `topic_key_sub` permits a
        // wildcard namespace (observe surface), so `*` fan-out is allowed here.
        let key_builder = match &options.namespace {
            Some(ns) => node.key_builder().clone().with_namespace(ns)?,
            None => node.key_builder().clone(),
        };
        let key_expr = key_builder.topic_key_sub(topic)?;
        let key_expr: String = key_expr.into();
        let event_sender_cell = node.event_sender_cell();
        // The snapshot below is only valid for the executor attachment
        // it was taken from. Tagging it with the node's attachment generation
        // keeps the hot path lock-free (one relaxed load per event) while
        // guaranteeing that a node re-attached to a different executor never
        // keeps feeding the executor it left.
        let event_sender_generation = node.event_sender_generation_handle();
        // Generation BEFORE the cell read: an attach racing this snapshot then
        // tags a newer sender with an older generation, which the dispatch
        // path treats as stale and resolves through the cell — conservative,
        // never the other way round.
        let snapshot_generation = event_sender_generation.load(Ordering::Acquire);
        let cached_event_sender = event_sender_cell
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .map(|sender| CachedExecutorSender {
                sender,
                generation: snapshot_generation,
            });
        let local_queue = spawn_local_queue(options.history_depth);
        let local_sender = local_queue.sender;

        // Tier A per-subscriber in-flight cap state. `receive_depth == 0`
        // (the default) leaves the gate disabled: the closure below sees a
        // single `if receive_depth == 0` branch and is otherwise byte-for-byte
        // the historical dispatch. Both counters are `Arc`-shared with the
        // callback closure (and, for `sub_inflight`, with each admitted event's
        // `InflightGuard`).
        let receive_depth = options.receive_depth;
        if receive_depth > 0 {
            warn_tier_a_isolation_sizing_once();
        }
        let sub_inflight = Arc::new(AtomicUsize::new(0));
        let dropped_for_depth = Arc::new(AtomicUsize::new(0));
        let sub_inflight_cb = Arc::clone(&sub_inflight);
        let dropped_for_depth_cb = Arc::clone(&dropped_for_depth);
        let flow_caps = Arc::new(FlowCaps {
            priority_override: std::sync::atomic::AtomicU8::new(u8::MAX),
            deadline_ns: std::sync::atomic::AtomicU64::new(0),
            max_in_flight: AtomicUsize::new(receive_depth),
            deadline_dropped: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        });
        let flow_caps_cb = Arc::clone(&flow_caps);
        let readiness = Arc::new(crate::flow::readiness::ReadinessWiring::new(
            options.readiness_wires.clone(),
        ));
        let readiness_rx = Arc::clone(&readiness);
        let track_runtime_anomalies = node.runtime_metrics_telemetry().is_active();
        let runtime_stats =
            track_runtime_anomalies.then(|| Arc::new(SubscriberRuntimeStatsInner::default()));
        let overflow_policy = options.overflow_policy;
        let overflow_relay_sender = if options.overflow_policy == SubscriberOverflowPolicy::Relay {
            Some(spawn_overflow_relay_queue(
                Arc::clone(&event_sender_cell),
                options.overflow_relay_capacity,
                runtime_stats.clone(),
            ))
        } else {
            None
        };
        let enable_header_context = options.enable_header_context;
        let strategy = SubscriberStrategy::new(&options);
        let strategy_enabled = strategy.is_some();
        let callback_group = options.callback_group;
        let callback_group_handle = options.callback_group_handle;
        let callback_topic: Arc<str> = Arc::<str>::from(key_expr.clone());
        let empty_context_topic: Arc<str> = Arc::<str>::from("");
        let include_topic_in_context = options.include_topic_in_context;
        let include_receive_timestamp = options.include_receive_timestamp;
        let include_priority_in_context = options.include_priority_in_context;
        let include_publisher_timestamp = options.include_publisher_timestamp;
        let capture_source_namespace =
            options.capture_source_namespace || options.namespace.is_some();
        let parse_header_for_strategy =
            enable_header_context || options.enable_dedup || options.enable_reorder_check;
        let header_layout_cache = Arc::new(HeaderLayoutCache::default());
        if parse_header_for_strategy {
            if let Some(layout) = options.header_layout_hint {
                header_layout_cache.insert_for_topic(&key_expr, layout);
            }
        }
        // Cross-namespace subscribers cannot warm the cache here: the node's
        // SchemaRegistry holds the node's own (non-namespaced) key_builder, so a
        // schema_by_key query for a remote `{SN}/...` topic would resolve to the
        // wrong (local) key. Skip the warm-up — P0 relies on pre-shared schema
        // and/or an explicit header_layout_hint; runtime cross-namespace schema
        // resolution is P1. See Aorta_Cloud_Cross_Namespace_Addressing_Proposal §4.5/§6.
        if (options.prefetch_schema_on_init || parse_header_for_strategy)
            && options.namespace.is_none()
        {
            let registry = node.schema_registry().clone();
            let key_builder = node.key_builder().clone();
            let cache = if parse_header_for_strategy {
                Some(Arc::clone(&header_layout_cache))
            } else {
                None
            };
            warm_schema_cache_for_subscriber(&registry, &key_builder, &key_expr, cache);
        }
        let configured_priority = options.qos.priority;
        let node_for_context = node.clone();
        let schema_registry = Some(node.schema_registry().clone());
        let key_builder = Some(node.key_builder().clone());
        let runtime_handle = tokio::runtime::Handle::try_current().ok();
        let static_context_template = if !parse_header_for_strategy
            && !include_topic_in_context
            && !include_receive_timestamp
            && !include_priority_in_context
            && !include_publisher_timestamp
            && !capture_source_namespace
        {
            Some(Arc::new(MessageContext {
                topic: Arc::clone(&empty_context_topic),
                receive_stamp_ns: 0,
                priority: Priority::Normal,
                publisher_stamp_ns: None,
                publisher_node: None,
                sequence: None,
                source_namespace: None,
            }))
        } else {
            None
        };

        // Bundle 5 independently Arc-cloned values into a single shared context.
        // This reduces per-message atomic refcount operations from 5 to 1.
        let shared = Arc::new(SubscriberSharedCtx {
            callback,
            topic_callback_event: crate::trace::stage_event_for(
                crate::trace::StageId::TopicCallback,
                topic,
            ),
            callback_topic,
            empty_context_topic,
            static_context_template,
            strategy,
            runtime_stats: runtime_stats.clone(),
            header_layout_cache,
            schema_registry,
            key_builder,
            runtime_handle,
        });

        // Pub/sub is a live, volatile stream. Last-value / latched / late-joiner
        // snapshot semantics are ContextStore's job (the removed `request_history`
        // / `TransientLocal` path was a silent no-op).
        let sub_builder = node.session().inner().declare_subscriber(key_expr);

        let subscriber = sub_builder
            .callback(move |sample| {
                let shared = Arc::clone(&shared);

                let source_identity = if strategy_enabled {
                    sample.source_info().map(|source| {
                        let source_id = source.source_id();
                        format!("{}:{}", source_id.zid(), source_id.eid())
                    })
                } else {
                    None
                };
                let attachment_hash = if parse_header_for_strategy {
                    sample
                        .attachment()
                        .and_then(parse_attachment_hash_from_zbytes)
                } else {
                    None
                };
                let publisher_stamp_ns = if include_publisher_timestamp {
                    // Shared NTP64→ns conversion (see build_pull_sample).
                    crate::flow::readiness::publisher_ns_of(&sample).map(|ns| ns as i64)
                } else {
                    None
                };
                // Cross-namespace fan-out: parse the originating namespace from
                // the concrete received key (the subscription key may be `*`).
                // Off by default — one key split only when explicitly requested.
                let source_namespace = if capture_source_namespace {
                    shared
                        .key_builder
                        .as_ref()
                        .and_then(|kb| kb.split_full_key(sample.key_expr().as_str()))
                        .and_then(|(ns, _suffix)| ns)
                } else {
                    None
                };
                // Mint the topic.callback span identity, parented to
                // the publisher's `topic.publish` span when its traceparent rode
                // the attachment (pub → callback = the transit hop), else a fresh
                // root. Only when topic spans are enabled — the decode reads the
                // attachment here, before the sample is consumed below. `None`
                // leaves the always-on histogram as the only record.
                let cb_link = if crate::trace::topic_scope_enabled() {
                    let wire_parent = sample.attachment().and_then(|attachment| {
                        crate::attachment::decode(attachment.to_bytes().as_ref()).traceparent
                    });
                    crate::trace::inbound_root_link(wire_parent.as_ref())
                } else {
                    None
                };
                // Readiness wires: HLC stamp captured BEFORE the sample
                // is consumed below; unwired subscribers pay one load+branch.
                let readiness_publisher_ns = if !readiness_rx.is_empty() {
                    crate::flow::readiness::publisher_ns_of(&sample)
                } else {
                    None
                };
                let payload = sample_into_payload(sample);
                let payload_len = payload.len();
                if !readiness_rx.is_empty() {
                    // Dispatch on the RX thread, BEFORE the executor hop:
                    // waves/windows must not wait behind queue admission.
                    readiness_rx.dispatch(
                        &payload,
                        u64::try_from(node_for_context.now().as_nanos()).unwrap_or(0),
                        readiness_publisher_ns,
                    );
                }

                // Extract metadata for MessageContext
                let context_priority = if include_priority_in_context {
                    configured_priority
                } else {
                    Priority::Normal
                };
                let receive_stamp_ns = if include_receive_timestamp {
                    node_for_context.now().as_nanos()
                } else {
                    0
                };
                let shared_for_event = Arc::clone(&shared);

                // Flow: priority override (u8::MAX = inherit QoS)
                // and per-event deadline — single Relaxed loads.
                let flow_priority = match flow_caps_cb.priority_override.load(Ordering::Relaxed) {
                    u8::MAX => map_event_priority(configured_priority),
                    raw => crate::event_queue::event_priority_from_index_lossy(raw),
                };
                let flow_deadline = match flow_caps_cb.deadline_ns.load(Ordering::Relaxed) {
                    0 => None,
                    ns => Some(std::time::Instant::now() + std::time::Duration::from_nanos(ns)),
                };
                let event = Event::new_with_priority(
                    EventKind::SubscriptionData,
                    flow_priority,
                    move || {
                        let runtime_stats = shared_for_event.runtime_stats.as_deref();
                        if let Some(context) = shared_for_event.static_context_template.as_ref() {
                            run_traced_callback(
                                cb_link,
                                shared_for_event.topic_callback_event,
                                || (shared_for_event.callback)(&payload, context.as_ref()),
                            );
                            return;
                        }

                        // Schema resolution is intentionally excluded from callback hot path.
                        // If enabled, schema prefetch happens once during subscriber initialization.
                        let parsed = if parse_header_for_strategy {
                            let lookup = shared_for_event
                                .header_layout_cache
                                .resolve(attachment_hash, shared_for_event.callback_topic.as_ref());
                            if lookup.needs_refresh || lookup.layout.is_none() {
                                maybe_refresh_header_layout_on_miss(
                                    &shared_for_event.header_layout_cache,
                                    shared_for_event.schema_registry.as_ref(),
                                    shared_for_event.key_builder.as_ref(),
                                    shared_for_event.runtime_handle.as_ref(),
                                    shared_for_event.callback_topic.as_ref(),
                                    attachment_hash,
                                );
                            }
                            let layout = lookup.layout.unwrap_or(DEFAULT_AORTA_HEADER_LAYOUT);
                            parse_header_context_from_zbytes_with_layout(&payload, layout)
                        } else {
                            ParsedHeaderContext::default()
                        };

                        if let Some(strategy) = shared_for_event.strategy.as_ref() {
                            let decision = strategy.process(
                                shared_for_event.callback_topic.as_ref(),
                                &parsed,
                                source_identity.as_deref(),
                            );
                            if decision.out_of_order {
                                if let Some(runtime_stats) = runtime_stats {
                                    runtime_stats.on_out_of_order();
                                }
                            }
                            if decision.drop_duplicate {
                                if let Some(runtime_stats) = runtime_stats {
                                    runtime_stats.on_duplicate_drop();
                                }
                                return;
                            }
                            if decision.drop_out_of_order {
                                if let Some(runtime_stats) = runtime_stats {
                                    runtime_stats.on_out_of_order_drop();
                                }
                                return;
                            }
                        }

                        let publisher_node = if enable_header_context {
                            parsed.publisher_node.clone()
                        } else {
                            None
                        };
                        let sequence = if enable_header_context {
                            parsed.sequence
                        } else {
                            None
                        };

                        // Build MessageContext
                        let context = MessageContext {
                            topic: if include_topic_in_context {
                                Arc::clone(&shared_for_event.callback_topic)
                            } else {
                                Arc::clone(&shared_for_event.empty_context_topic)
                            },
                            receive_stamp_ns,
                            priority: context_priority,
                            publisher_stamp_ns,
                            publisher_node,
                            sequence,
                            source_namespace,
                        };

                        run_traced_callback(cb_link, shared_for_event.topic_callback_event, || {
                            (shared_for_event.callback)(&payload, &context)
                        });
                    },
                )
                .with_estimated_bytes(payload_len)
                .with_callback_group(callback_group, callback_group_handle);

                // Tier A `receive_depth` gate (drop-newest). Only active when
                // `receive_depth > 0`; otherwise this is a single predictable
                // branch and the event dispatches exactly as before. The
                // `fetch_update` CAS increments iff we are under the cap, so the
                // count is exact with no TOCTOU. The returned `InflightGuard`
                // rides the event and decrements on every drop path (run /
                // Full / Closed / panic).
                let event = event.with_deadline_counted(
                    flow_deadline,
                    std::sync::Arc::clone(&flow_caps_cb.deadline_dropped),
                );
                // The cap is read per-sample from FlowCaps (the settable form
                // of receive_depth — one mechanism, two configuration
                // surfaces).
                let receive_depth = flow_caps_cb.max_in_flight.load(Ordering::Relaxed);
                let event = if receive_depth == 0 {
                    event
                } else {
                    match sub_inflight_cb.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        (n < receive_depth).then_some(n + 1)
                    }) {
                        Ok(_) => event.with_inflight_guard(Some(InflightGuard::new(Arc::clone(
                            &sub_inflight_cb,
                        )))),
                        Err(_) => {
                            // At cap → shed the newest; never touch the channel.
                            // Same rate-limited visibility as the executor-Full
                            // and relay drop paths — a capped subscriber must
                            // not shed silently.
                            dropped_for_depth_cb.fetch_add(1, Ordering::Relaxed);
                            warn_subscriber_event_drop("receive_depth cap (Tier A)");
                            return;
                        }
                    }
                };

                dispatch_event_to_executor_or_local(
                    cached_event_sender.as_ref(),
                    &event_sender_generation,
                    &event_sender_cell,
                    &local_sender,
                    overflow_policy,
                    overflow_relay_sender.as_ref(),
                    shared.runtime_stats.as_deref(),
                    event,
                );
            })
            .await?;

        // Inventory mutation is intentionally limited to subscriber declaration/undeclare.
        // No telemetry work is added to the message callback hot path.
        let inventory_guard =
            match node
                .vital_telemetry()
                .register_subscriber(node.key_builder(), topic, None)
            {
                Ok(guard) => Some(guard),
                Err(err) => {
                    aorta_core_warn!(
                        "Aorta WARN: subscriber inventory telemetry disabled for '{}': {}",
                        topic,
                        err
                    );
                    None
                }
            };
        let runtime_guard = if let Some(runtime_stats) = runtime_stats.as_ref() {
            match node.runtime_metrics_telemetry().register_subscriber(
                topic,
                None,
                Arc::new({
                    let runtime_stats = Arc::clone(runtime_stats);
                    move || runtime_stats.snapshot()
                }),
            ) {
                Ok(guard) => Some(guard),
                Err(err) => {
                    aorta_core_warn!(
                        "Aorta WARN: subscriber runtime telemetry disabled for '{}': {}",
                        topic,
                        err
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            inner: subscriber,
            topic: topic.to_string(),
            runtime_stats,
            inventory_guard,
            runtime_guard,
            sub_inflight,
            dropped_for_depth,
            receive_depth,
            flow_caps,
            readiness,
            pull: None,
            local_queue_task: Some(local_queue.task),
            local_queue_shutdown: Some(local_queue.shutdown),
            local_queue_stop: Some(local_queue.stop),
            local_queue_liveness: Some(local_queue.liveness),
        })
    }

    /// Create a pull-mode (`DeliveryMode::InlinePull`) subscriber: the
    /// subscriber owns a Zenoh-native keep-last-`receive_depth` `RingChannel`
    /// and the application drains it via [`Subscriber::recv`] /
    /// [`Subscriber::try_recv`] / [`Subscriber::recv_timeout`]. No callback,
    /// no executor hop, structural per-topic isolation (Tier B-pull).
    pub async fn new_pull(
        node: &Node,
        topic: &str,
        mut options: SubscriberOptions,
    ) -> AortaResult<Self> {
        // Force pull semantics, then validate (no callback on this path).
        options.delivery = DeliveryMode::InlinePull;
        if options.receive_overflow == ReceiveOverflow::DropNewest {
            // Pull rings are inherently keep-last; normalize the default.
            options.receive_overflow = ReceiveOverflow::DropOldest;
        }
        options.validate_delivery(/* has_callback = */ false)?;

        let key_builder = match &options.namespace {
            Some(ns) => node.key_builder().clone().with_namespace(ns)?,
            None => node.key_builder().clone(),
        };
        let key_expr = key_builder.topic_key_sub(topic)?;
        let key_expr: String = key_expr.into();

        let capture_source_namespace =
            options.capture_source_namespace || options.namespace.is_some();
        let callback_topic: Arc<str> = Arc::<str>::from(key_expr.clone());
        let empty_context_topic: Arc<str> = Arc::<str>::from("");

        // Optionally warm the schema/header cache (parity with the callback
        // path's prefetch) — but pull mode has no header-strategy, so only the
        // plain prefetch matters and only for non-namespaced subscribers.
        if options.prefetch_schema_on_init && options.namespace.is_none() {
            let registry = node.schema_registry().clone();
            let kb = node.key_builder().clone();
            warm_schema_cache_for_subscriber(&registry, &kb, &key_expr, None);
        }

        let pull_ctx = Arc::new(PullCtx {
            callback_topic,
            empty_context_topic,
            include_topic_in_context: options.include_topic_in_context,
            include_receive_timestamp: options.include_receive_timestamp,
            include_priority_in_context: options.include_priority_in_context,
            include_publisher_timestamp: options.include_publisher_timestamp,
            capture_source_namespace,
            configured_priority: options.qos.priority,
            key_builder: Some(node.key_builder().clone()),
            node: node.clone(),
        });

        // Split the native keep-last ring: the `Callback` (which owns the ring's
        // strong `Arc` + flume sender) is moved into the Zenoh subscriber's
        // callback (`inner`), while the `RingChannelHandler` (a `Weak`) is kept in
        // a separately-`Arc`'d `PullRecvHandle`. Declaring with `.with(RingChannel)`
        // instead would bury the handler inside the subscriber, forcing `recv` to
        // pin the whole subscriber and blocking teardown of an idle infinite
        // `recv`. With the split, dropping `inner` on undeclare
        // drops the sender and unblocks any waiting `recv`.
        let (ring_cb, ring_handler) =
            zenoh::handlers::IntoHandler::into_handler(RingChannel::new(options.receive_depth));
        let recv_notify = Arc::new(tokio::sync::Notify::new());
        let c_hook: Arc<NotifyHookSlot> = Arc::new(NotifyHookSlot::default());
        // When zenoh drops this closure (undeclare / session
        // close), every parked waiter must wake to observe the
        // disconnect — otherwise a parked recv_in/next() strands until
        // scope cancel. The guard rides the closure's captures.
        struct WakeOnDrop {
            notify: Arc<tokio::sync::Notify>,
            c_hook: Arc<NotifyHookSlot>,
            closed: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for WakeOnDrop {
            fn drop(&mut self) {
                // Order matters (CI-caught race): the closure's OTHER
                // captures — including the ring's strong sender — drop in
                // unspecified order relative to this guard. A waiter woken
                // by the notify below could observe a still-connected,
                // EMPTY ring and re-park forever. The monotonic closed
                // flag, published BEFORE the wake, is what recv_in
                // consults so any post-wake emptiness reads as closure.
                self.closed
                    .store(true, std::sync::atomic::Ordering::Release);
                self.notify.notify_waiters();
                self.c_hook.fire();
            }
        }
        let recv_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let wake_guard = WakeOnDrop {
            notify: Arc::clone(&recv_notify),
            c_hook: Arc::clone(&c_hook),
            closed: Arc::clone(&recv_closed),
        };
        let pending_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let pending_rx = Arc::clone(&pending_count);
        let ring_depth = options.receive_depth as u64;
        let readiness = Arc::new(crate::flow::readiness::ReadinessWiring::new(
            options.readiness_wires.clone(),
        ));
        let readiness_rx = Arc::clone(&readiness);
        let node_for_wire = node.clone();
        let subscriber = node
            .session()
            .inner()
            .declare_subscriber(key_expr)
            .callback(move |sample| {
                let guard = &wake_guard;
                // Readiness wires: payload + HLC stamp captured before
                // the ring consumes the sample; unwired subscribers pay one
                // load + branch.
                let readiness_meta = if !readiness_rx.is_empty() {
                    Some((
                        sample.payload().clone(),
                        crate::flow::readiness::publisher_ns_of(&sample),
                    ))
                } else {
                    None
                };
                ring_cb.call(sample);
                // Probe mirror: clamp at depth (drop-oldest keeps the
                // ring at depth; the mirror must not drift above it).
                let _ = pending_rx.fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |v| Some(v.saturating_add(1).min(ring_depth)),
                );
                // Readiness wires dispatch AFTER the ring store: a waiter woken
                // by the wave/window observes the sample already present
                // (MessageAvailable semantics).
                if let Some((payload, publisher_ns)) = readiness_meta {
                    readiness_rx.dispatch(
                        &payload,
                        u64::try_from(node_for_wire.now().as_nanos()).unwrap_or(0),
                        publisher_ns,
                    );
                }
                // wake any async receiver AFTER the store (the
                // permit makes the pair race-free).
                guard.notify.notify_one();
                // the C-plane hook (trampoline-class by contract; the
                // copy-out-then-call semantics live in NotifyHookSlot).
                guard.c_hook.fire();
            })
            .await?;
        let pull_handle = PullRecvHandle {
            handler: Arc::new(ring_handler),
            notify: recv_notify,
            closed: recv_closed,
            c_hook,
            pending_count,
            ctx: pull_ctx,
        };

        let inventory_guard = node
            .vital_telemetry()
            .register_subscriber(node.key_builder(), topic, None)
            .ok();
        let track_runtime_anomalies = node.runtime_metrics_telemetry().is_active();
        let runtime_stats =
            track_runtime_anomalies.then(|| Arc::new(SubscriberRuntimeStatsInner::default()));
        let runtime_guard = if let Some(runtime_stats) = runtime_stats.as_ref() {
            node.runtime_metrics_telemetry()
                .register_subscriber(
                    topic,
                    None,
                    Arc::new({
                        let runtime_stats = Arc::clone(runtime_stats);
                        move || runtime_stats.snapshot()
                    }),
                )
                .ok()
        } else {
            None
        };

        Ok(Self {
            inner: subscriber,
            topic: topic.to_string(),
            runtime_stats,
            inventory_guard,
            runtime_guard,
            sub_inflight: Arc::new(AtomicUsize::new(0)),
            dropped_for_depth: Arc::new(AtomicUsize::new(0)),
            receive_depth: options.receive_depth,
            // Pull mode: the Zenoh ring does the buffering; Flow caps have
            // no executor path to act on — inert placeholder.
            flow_caps: Arc::new(FlowCaps {
                priority_override: std::sync::atomic::AtomicU8::new(u8::MAX),
                deadline_ns: std::sync::atomic::AtomicU64::new(0),
                max_in_flight: AtomicUsize::new(options.receive_depth),
                deadline_dropped: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            readiness,
            pull: Some(pull_handle),
            local_queue_task: None,
            local_queue_shutdown: None,
            local_queue_stop: None,
            local_queue_liveness: None,
        })
    }

    /// Bind this subscriber as [`WindowSync`](crate::flow::WindowSync) input
    /// `input` — the post-declare form of
    /// [`SubscriberOptions::with_window_feed`], any delivery mode.
    /// The index is validated here; the wire observes the stream without
    /// affecting delivery. Binds accumulate; they live as long as the
    /// subscriber.
    pub fn bind_window(
        &self,
        window: &Arc<crate::flow::WindowSync>,
        input: usize,
        stamp: crate::flow::WindowStamp,
    ) -> AortaResult<()> {
        self.readiness.bind(crate::flow::ReadinessWire::Window {
            window: Arc::clone(window),
            input,
            stamp,
        })
    }

    /// Bind this subscriber to satisfy `set` condition `condition` on every
    /// arriving sample — the post-declare form of
    /// [`SubscriberOptions::with_readiness_satisfy`].
    pub fn bind_readiness(
        &self,
        set: &Arc<crate::flow::ReadinessSet>,
        condition: usize,
    ) -> AortaResult<()> {
        self.readiness.bind(crate::flow::ReadinessWire::Satisfy {
            set: Arc::clone(set),
            condition,
        })
    }

    /// Remove the [`bind_window`](Self::bind_window) wire(s) feeding
    /// `window` input `input` (without an inverse, the
    /// subscriber's Arc clone kept feeding the window for its whole
    /// remaining lifetime once the owner freed its handle). Returns how
    /// many wires were removed (0 = nothing matched).
    pub fn unbind_window(&self, window: &Arc<crate::flow::WindowSync>, input: usize) -> usize {
        self.readiness.unbind_matching(|w| match w {
            crate::flow::ReadinessWire::Window {
                window: bound,
                input: bound_input,
                ..
            } => Arc::ptr_eq(bound, window) && *bound_input == input,
            _ => false,
        })
    }

    /// Remove the [`bind_readiness`](Self::bind_readiness) wire(s)
    /// satisfying `set` condition `condition`. Returns the removal count.
    pub fn unbind_readiness(
        &self,
        set: &Arc<crate::flow::ReadinessSet>,
        condition: usize,
    ) -> usize {
        self.readiness.unbind_matching(|w| match w {
            crate::flow::ReadinessWire::Satisfy {
                set: bound,
                condition: bound_condition,
            } => Arc::ptr_eq(bound, set) && *bound_condition == condition,
            _ => false,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Flow: override this subscriber's dispatch priority
    /// (`None` restores the QoS-derived mapping). Takes effect for samples
    /// arriving after the call. Callback-mode only: pull mode has no
    /// executor dispatch, so the call is rejected (the same rule the C-ABI
    /// enforces — one semantic, both surfaces).
    pub fn set_flow_priority(&self, priority: Option<EventPriority>) -> AortaResult<()> {
        if self.pull.is_some() {
            return Err(crate::error::AortaError::not_supported(
                "pull-mode subscribers have no executor dispatch: Flow priority does not apply",
            ));
        }
        // u8::MAX = the inherit-QoS sentinel; concrete levels encode via the
        // canonical priority index map.
        let raw = match priority {
            None => u8::MAX,
            Some(p) => crate::event_queue::priority_index(p),
        };
        self.flow_caps
            .priority_override
            .store(raw, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Flow: the per-endpoint counting gate —
    /// `max_in_flight` (0 = ungated; the settable form of `receive_depth`)
    /// and an optional per-event deadline (the scheduler drops the
    /// event at dequeue once it expires). Overflow direction and queueing
    /// stay creation-time options (`SubscriberOptions`).
    pub fn set_flow_admission(&self, admission: FlowAdmission) -> AortaResult<()> {
        if self.pull.is_some() {
            return Err(crate::error::AortaError::not_supported(
                "pull-mode subscribers buffer in the Zenoh ring: the Flow gate does not apply",
            ));
        }
        self.flow_caps.max_in_flight.store(
            admission.max_in_flight,
            std::sync::atomic::Ordering::Relaxed,
        );
        let ns = admission
            .deadline
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX).max(1))
            .unwrap_or(0);
        self.flow_caps
            .deadline_ns
            .store(ns, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Events of this subscriber dropped at dequeue past their
    /// `FlowAdmission` deadline (see also the executor-wide
    /// `dropped_expired`).
    pub fn dropped_for_deadline(&self) -> u64 {
        self.flow_caps
            .deadline_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn flow_caps(&self) -> Arc<FlowCaps> {
        Arc::clone(&self.flow_caps)
    }

    /// The configured Tier A in-flight cap (`0` = gate disabled). For
    /// `InlinePull` subscribers this is the ring capacity.
    pub fn receive_depth(&self) -> usize {
        // Pull mode: the ring capacity is fixed at creation (the Flow gate
        // does not apply; its setters are rejected) — report the immutable
        // configured value.
        if self.pull.is_some() {
            return self.receive_depth;
        }
        // Callback mode: reports the LIVE cap (set_flow_admission may have changed it); the
        // constructor seed only initializes the atomic
        // (a stale getter beside a dynamic capability API is a footgun).
        self.flow_caps
            .max_in_flight
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of events shed by the Tier A `receive_depth` gate (drop-newest).
    /// Always `0` when `receive_depth == 0` or for pull subscribers (whose
    /// drops happen natively inside the Zenoh ring).
    pub fn dropped_for_depth(&self) -> usize {
        self.dropped_for_depth.load(Ordering::Relaxed)
    }

    /// Current number of in-flight events counted against the Tier A cap.
    pub fn inflight(&self) -> usize {
        self.sub_inflight.load(Ordering::Relaxed)
    }

    /// `true` if this is a `DeliveryMode::InlinePull` subscriber.
    pub fn is_pull(&self) -> bool {
        self.pull.is_some()
    }

    /// A cloneable drain handle for a pull subscriber (`None` for callback
    /// subscribers). The handle holds only the ring `Weak` + context — NOT the
    /// Zenoh subscriber — so a thread blocked in `PullRecvHandle::recv` does not
    /// pin the subscriber, letting `undeclare`/`free` reclaim it and unblock the
    /// recv. The C-ABI `aorta_sub_recv` clones this out from
    /// under the registry lock before blocking.
    pub fn pull_recv_handle(&self) -> Option<PullRecvHandle> {
        self.pull.clone()
    }

    fn require_pull(&self) -> AortaResult<&PullRecvHandle> {
        self.pull.as_ref().ok_or_else(|| {
            crate::error::AortaError::invalid_argument(
                "recv is only valid on a pull (InlinePull) subscriber",
            )
        })
    }

    /// Block until the next sample is available (pull mode only).
    ///
    /// Returns `Err` for a non-pull subscriber or once the subscriber has been
    /// disconnected (e.g. undeclared/freed). This is a blocking call — invoke it
    /// from a dedicated consumer thread, mirroring rclpy's `wait_for_message`.
    pub fn recv(&self) -> AortaResult<PullSample> {
        self.require_pull()?.recv()
    }

    /// Non-blocking pull. `Ok(None)` when the ring is currently empty.
    pub fn try_recv(&self) -> AortaResult<Option<PullSample>> {
        self.require_pull()?.try_recv()
    }

    /// Pull with a timeout. `Ok(None)` when the timeout elapses with no sample.
    pub fn recv_timeout(&self, timeout: Duration) -> AortaResult<Option<PullSample>> {
        self.require_pull()?.recv_timeout(timeout)
    }

    /// Async pull under a Flow scope: `Err(Stopped)` on
    /// effective cancel; see [`PullRecvHandle::recv_in`].
    pub async fn recv_in(&self, scope: &crate::flow::Scope) -> crate::flow::Outcome<PullSample> {
        let handle = match self.require_pull() {
            Ok(h) => h,
            Err(err) => return Err(crate::flow::Stopped::fault_from_error(&err)),
        };
        handle.recv_in(scope).await
    }

    /// Identity sentinel for the local-queue drain task (test support):
    /// upgrades to `None` exactly when the drain task has ended. `None` for
    /// pull subscribers (no local-queue task).
    pub(crate) fn drain_task_liveness(&self) -> Option<std::sync::Weak<()>> {
        self.local_queue_liveness.clone()
    }

    pub fn runtime_stats(&self) -> SubscriberRuntimeStats {
        let mut stats = self
            .runtime_stats
            .as_ref()
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        stats.receive_depth_drop_count = self.dropped_for_depth() as u64;
        stats.dropped_for_deadline = self.dropped_for_deadline();
        stats
    }

    pub async fn undeclare(mut self) -> AortaResult<()> {
        if let Some(inventory_guard) = self.inventory_guard.take() {
            inventory_guard.release();
        }
        if let Some(runtime_guard) = self.runtime_guard.take() {
            runtime_guard.release();
        }
        // Zenoh's `undeclare` consumes the subscriber; partial-move `inner` out
        // of `mut self` (the outer `Subscriber` has no `Drop`, so this is fine —
        // `topic`/`local_queue_task` are used below). For a pull subscriber this
        // drops the ring's sender side, unblocking any waiting `recv`.
        self.inner.undeclare().await?;
        // Zenoh has now released the callback closures. With the queue
        // consumed via `EventQueueConsumer` (no self-held sender — see
        // `EventQueue::into_parts`), sender-drop would eventually close
        // the backend and `pop()` would return None — but that is only a
        // fallback whose timing depends on when Zenoh releases the
        // closures (an assumption an earlier version got wrong). Termination is
        // driven deterministically by the explicit shutdown signal: drop
        // it BEFORE awaiting the handle so the task drains the remaining
        // queued events and exits, making the happy path wait only for
        // that drain (no NEW callbacks fire after `undeclare()` returns).
        //
        // On timeout we `abort()` the JoinHandle, which is honest about
        // what Tokio actually gives us: `abort()` only takes effect at
        // the next `.await` point. If a user callback is mid-execution
        // inside `event.run()` (a fully synchronous call), abort cannot
        // preempt it — the callback will run to completion and the
        // abort lands when the loop next reaches an `.await` point
        // (parked in `pop()`). We accept this as a fundamental limit of
        // synchronous user callbacks: there is no way to interrupt
        // arbitrary user code from another task in safe Rust. The
        // trade-off vs. just dropping the JoinHandle (which would
        // silently detach the task) is that abort plus the stop flag
        // below terminate dispatch: once the flag is set, the admission
        // checks on both dispatch paths refuse any further event, and
        // abort cancels the task at its next await point.
        // NOTE: the shutdown drain itself has NO await point (see
        // `drain_local_queue`) — it runs in a single poll, so abort
        // cannot strand it mid-backlog; the admission checks are what
        // bound dispatch after the stop flag is set.
        if let Some(task) = self.local_queue_task.take() {
            // Must release the shutdown sender BEFORE awaiting, otherwise
            // the task can never observe the shutdown and the 2s timeout
            // would fire on every undeclare. Implicit drop
            // of the `Subscriber` (no `Drop` impl) terminates the task
            // the same way: this field drops naturally.
            drop(self.local_queue_shutdown.take());
            const LOCAL_QUEUE_DRAIN_TIMEOUT: std::time::Duration =
                std::time::Duration::from_secs(2);
            // `tokio::time::timeout` consumes the JoinHandle by value, so
            // hold an `AbortHandle` to fire on the timeout branch.
            let abort_handle = task.abort_handle();
            if tokio::time::timeout(LOCAL_QUEUE_DRAIN_TIMEOUT, task)
                .await
                .is_err()
            {
                // Force-stop: set the cooperative
                // stop flag FIRST — the dispatch loop checks it before every
                // event admission, so once the stuck callback returns, no
                // further events are dispatched. abort() alone only lands at
                // await points and cannot preempt the synchronous dispatch
                // loop; it remains as a backup for the task parked in `pop()`.
                if let Some(stop) = self.local_queue_stop.as_ref() {
                    stop.store(true, std::sync::atomic::Ordering::Release);
                }
                abort_handle.abort();
                aorta_core_warn!(
                    "Aorta WARN: subscriber '{}' local-queue task drain timed out after {:?}; aborted (a user callback already inside event.run() may still complete past this return — abort only takes effect at the next .await)",
                    self.topic,
                    LOCAL_QUEUE_DRAIN_TIMEOUT
                );
            }
        }
        Ok(())
    }
}

async fn cache_schema_by_key_value(registry: &SchemaRegistry, value: SchemaByKeyValue) {
    match value.kind() {
        crate::schema_registry::SchemaKind::Topic | crate::schema_registry::SchemaKind::Param => {
            registry
                .insert_local(value.hash(), value.bfbs().to_vec())
                .await;
        }
        crate::schema_registry::SchemaKind::Service => {}
    }
}

fn warm_schema_cache_for_subscriber(
    registry: &SchemaRegistry,
    key_builder: &KeyExprBuilder,
    full_key: &str,
    layout_cache: Option<Arc<HeaderLayoutCache>>,
) {
    let registry = registry.clone();
    let key_builder = key_builder.clone();
    let full_key = full_key.to_string();
    let layout_cache = layout_cache.clone();

    // Fetch schema only once at subscriber initialization time.
    // Runtime callback path must stay free of schema network lookups.
    crate::runtime::spawn_logged("subscriber_schema_prefetch", async move {
        if let Some(suffix) = key_builder.key_suffix_from_full_key(&full_key) {
            // Bounded retries are still initialization-stage work and avoid
            // moving schema lookup into the message callback hot path.
            const MAX_PREFETCH_ATTEMPTS: usize = 5;
            const PREFETCH_RETRY_INTERVAL: Duration = Duration::from_millis(200);

            for _ in 0..MAX_PREFETCH_ATTEMPTS {
                if let Ok(Some(value)) = registry.fetch_schema_by_key(&suffix).await {
                    if let Some(cache) = layout_cache.as_ref() {
                        if let Some(layout) = infer_aorta_header_layout_from_bfbs(value.bfbs()) {
                            cache.insert_for_schema(value.hash(), layout);
                            cache.insert_for_topic(&full_key, layout);
                        }
                    }
                    cache_schema_by_key_value(&registry, value).await;
                    break;
                }
                tokio::time::sleep(PREFETCH_RETRY_INTERVAL).await;
            }
        }
    });
}

const HEADER_LAYOUT_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(1);

fn maybe_refresh_header_layout_on_miss(
    cache: &Arc<HeaderLayoutCache>,
    registry: Option<&SchemaRegistry>,
    key_builder: Option<&KeyExprBuilder>,
    runtime_handle: Option<&tokio::runtime::Handle>,
    full_key: &str,
    schema_hash: Option<SchemaHash>,
) {
    let (Some(registry), Some(key_builder)) = (registry, key_builder) else {
        return;
    };
    let refresh_key = if let Some(hash) = schema_hash {
        format!("hash:{}", hash.to_hex())
    } else {
        format!("topic:{full_key}")
    };
    if !cache.try_begin_refresh(&refresh_key) {
        return;
    }
    let Some(runtime_handle) = runtime_handle.cloned() else {
        cache.finish_refresh(&refresh_key);
        return;
    };

    let cache = Arc::clone(cache);
    let registry = registry.clone();
    let key_builder = key_builder.clone();
    let topic_key = full_key.to_string();
    runtime_handle.spawn(async move {
        let refreshed =
            refresh_header_layout_from_registry(&registry, &key_builder, &topic_key, schema_hash)
                .await;
        if let Some((layout, resolved_hash)) = refreshed {
            if let Some(hash) = resolved_hash {
                cache.insert_for_schema(hash, layout);
            }
            cache.insert_for_topic(&topic_key, layout);
        }
        cache.finish_refresh(&refresh_key);
    });
}

async fn refresh_header_layout_from_registry(
    registry: &SchemaRegistry,
    key_builder: &KeyExprBuilder,
    full_key: &str,
    schema_hash: Option<SchemaHash>,
) -> Option<(AortaHeaderLayout, Option<SchemaHash>)> {
    if let Some(hash) = schema_hash {
        if let Ok(Some(bfbs)) = registry.fetch_schema(hash).await {
            if let Some(layout) = infer_aorta_header_layout_from_bfbs(&bfbs) {
                return Some((layout, Some(hash)));
            }
        }
    }

    let suffix = key_builder.key_suffix_from_full_key(full_key)?;
    let value = match registry.fetch_schema_by_key(&suffix).await {
        Ok(Some(value)) => value,
        _ => return None,
    };
    let layout = infer_aorta_header_layout_from_bfbs(value.bfbs())?;
    Some((layout, Some(value.hash())))
}

fn parse_attachment_hash(bytes: &[u8]) -> Option<SchemaHash> {
    // Composite-aware: accepts both the legacy bare 64-hex form and
    // the versioned TLV form, so a future trace-carrying publisher stays
    // rolling-upgrade safe against this subscriber.
    crate::attachment::decode(bytes).schema_hash
}

fn parse_attachment_hash_from_zbytes(attachment: &ZBytes) -> Option<SchemaHash> {
    let mut slices = attachment.slices();
    let first = slices.next()?;
    if slices.next().is_none() {
        return parse_attachment_hash(first);
    }
    let bytes = attachment.to_bytes();
    parse_attachment_hash(bytes.as_ref())
}

fn parse_header_context_from_zbytes_with_layout(
    payload: &ZBytes,
    layout: AortaHeaderLayout,
) -> ParsedHeaderContext {
    let mut slices = payload.slices();
    let Some(first) = slices.next() else {
        return ParsedHeaderContext::default();
    };
    if slices.next().is_none() {
        return parse_header_context_with_layout(first, layout);
    }
    let bytes = payload.to_bytes();
    parse_header_context_with_layout(bytes.as_ref(), layout)
}

const DEFAULT_AORTA_HEADER_LAYOUT: AortaHeaderLayout = AortaHeaderLayout {
    root_aorta_header_field_id: 0,
    header_publisher_node_field_id: 1,
    header_sequence_field_id: 2,
};

/// Fuzzing entry point for the hand-rolled AortaHeader vtable walker:
/// this code decodes header fields directly from raw
/// payload bytes controlled by any peer on the mesh. Invariant under fuzz:
/// never panic, never read out of bounds, on ANY input and ANY layout.
#[doc(hidden)]
pub fn fuzz_parse_aorta_header_fields(
    payload: &[u8],
    layout: crate::schema::AortaHeaderLayout,
) -> (Option<String>, Option<u64>) {
    parse_aorta_header_fields_with_layout(payload, layout)
}

fn parse_aorta_header_fields(payload: &[u8]) -> (Option<String>, Option<u64>) {
    parse_aorta_header_fields_with_layout(payload, DEFAULT_AORTA_HEADER_LAYOUT)
}

fn parse_aorta_header_fields_with_layout(
    payload: &[u8],
    layout: AortaHeaderLayout,
) -> (Option<String>, Option<u64>) {
    // FlatBuffers root offset (u32) + at least table soffset (i32).
    if payload.len() < 8 {
        return (None, None);
    }

    let Some(root_pos) = read_u32_le(payload, 0).map(|value| value as usize) else {
        return (None, None);
    };
    let Some(root_vtable_pos) = table_vtable_pos(payload, root_pos) else {
        return (None, None);
    };
    let Some(header_uoffset_pos) = table_field_pos(
        payload,
        root_pos,
        root_vtable_pos,
        layout.root_aorta_header_field_id,
    ) else {
        return (None, None);
    };
    let Some(header_pos) = follow_uoffset(payload, header_uoffset_pos) else {
        return (None, None);
    };
    let Some(header_vtable_pos) = table_vtable_pos(payload, header_pos) else {
        return (None, None);
    };

    let publisher_node = table_field_pos(
        payload,
        header_pos,
        header_vtable_pos,
        layout.header_publisher_node_field_id,
    )
    .and_then(|field_pos| parse_string_at_uoffset(payload, field_pos));

    let sequence = table_field_pos(
        payload,
        header_pos,
        header_vtable_pos,
        layout.header_sequence_field_id,
    )
    .and_then(|field_pos| read_u64_le(payload, field_pos));

    (publisher_node, sequence)
}

fn table_vtable_pos(payload: &[u8], table_pos: usize) -> Option<usize> {
    let soffset = read_i32_le(payload, table_pos)? as isize;
    let vtable_pos_signed = table_pos as isize - soffset;
    if vtable_pos_signed < 0 {
        return None;
    }
    let vtable_pos = vtable_pos_signed as usize;
    let vtable_len = read_u16_le(payload, vtable_pos)? as usize;
    if vtable_len < 4 {
        return None;
    }
    let _obj_len = read_u16_le(payload, vtable_pos + 2)?;
    if vtable_pos.checked_add(vtable_len)? > payload.len() {
        return None;
    }
    Some(vtable_pos)
}

fn table_field_pos(
    payload: &[u8],
    table_pos: usize,
    vtable_pos: usize,
    field_index: usize,
) -> Option<usize> {
    // field_index is the FlatBuffers field id used to index vtable entries.
    // For schemas without explicit `(id: N)`, this matches declaration order.
    let vtable_len = read_u16_le(payload, vtable_pos)? as usize;
    let entry_pos = vtable_pos.checked_add(4 + field_index * 2)?;
    if entry_pos.checked_add(2)? > vtable_pos.checked_add(vtable_len)? {
        return None;
    }
    let field_offset = read_u16_le(payload, entry_pos)? as usize;
    if field_offset == 0 {
        return None;
    }
    let field_pos = table_pos.checked_add(field_offset)?;
    if field_pos >= payload.len() {
        return None;
    }
    Some(field_pos)
}

fn follow_uoffset(payload: &[u8], pos: usize) -> Option<usize> {
    let offset = read_u32_le(payload, pos)? as usize;
    let target = pos.checked_add(offset)?;
    if target >= payload.len() {
        return None;
    }
    Some(target)
}

fn parse_string_at_uoffset(payload: &[u8], pos: usize) -> Option<String> {
    let str_pos = follow_uoffset(payload, pos)?;
    let str_len = read_u32_le(payload, str_pos)? as usize;
    let content_start = str_pos.checked_add(4)?;
    let content_end = content_start.checked_add(str_len)?;
    let content = payload.get(content_start..content_end)?;
    std::str::from_utf8(content)
        .ok()
        .map(|value| value.to_string())
}

fn read_u16_le(payload: &[u8], pos: usize) -> Option<u16> {
    let bytes = payload.get(pos..pos.checked_add(2)?)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32_le(payload: &[u8], pos: usize) -> Option<u32> {
    let bytes = payload.get(pos..pos.checked_add(4)?)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_i32_le(payload: &[u8], pos: usize) -> Option<i32> {
    let bytes = payload.get(pos..pos.checked_add(4)?)?;
    Some(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64_le(payload: &[u8], pos: usize) -> Option<u64> {
    let bytes = payload.get(pos..pos.checked_add(8)?)?;
    Some(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Default capacity used when the caller passes `0` (the historical
/// "unbounded" sentinel, kept in sync with `SubscriberOptions::default`).
const DEFAULT_LOCAL_QUEUE_CAPACITY: usize = 1024;

/// Dispatch a single queued event under the stop-flag admission check.
/// Shared by the pop branch and the shutdown drain so both dispatch paths
/// enforce the same admission linearization point:
/// the stop flag is checked AFTER the event is taken but BEFORE
/// it runs — a post-run check is TOCTOU (the flag can be set between the
/// check and the next event's dispatch, with no scheduling point in
/// between when the queue is non-empty). Once the flag is set, no further
/// event is admitted for dispatch. The unavoidable window between this
/// check and `run()` is the linearization point itself; the guarantee is
/// "no NEW event admitted after the stop flag is set", NOT "a running
/// event gets interrupted". One atomic load per event, no scheduling
/// round-trip. Returns `false` when the caller must stop dispatching.
fn dispatch_queued_event(
    event: crate::event_queue::Event,
    stop: &std::sync::atomic::AtomicBool,
) -> bool {
    if stop.load(std::sync::atomic::Ordering::Acquire) {
        return false;
    }
    event.run();
    true
}

/// Drain the local queue after the shutdown signal has been observed:
/// run every queued event. The loop body is deliberately synchronous —
/// `try_pop()` and `event.run()` have NO await point, so the whole drain
/// runs in a single poll and a pending `abort()` cannot land between
/// events and strand part of the backlog; dispatch is bounded by the stop
/// flag's admission check (via [`dispatch_queued_event`]), not by abort.
/// Events that arrive while the shutdown branch runs are also picked up
/// here.
async fn drain_local_queue(
    queue: &mut crate::event_queue::EventQueueConsumer,
    stop: &std::sync::atomic::AtomicBool,
) {
    while let Some(event) = queue.try_pop() {
        if !dispatch_queued_event(event, stop) {
            break;
        }
    }
}

/// Handle set for a subscriber's local-queue drain task.
pub(crate) struct LocalQueueDrainHandle {
    /// Cloneable queue sender handed to the dispatch closures.
    pub(crate) sender: crate::event_queue::EventQueueSender,
    /// The drain task itself (await on shutdown, abort as backup).
    pub(crate) task: tokio::task::JoinHandle<()>,
    /// Graceful shutdown signal: drop (or send) to drain and exit.
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    /// Cooperative stop flag, checked after every dispatched callback: once
    /// set, the loop stops dispatching and exits. Set by `undeclare()` on
    /// its drain timeout (abort alone cannot preempt the synchronous
    /// dispatch loop).
    pub(crate) stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Identity sentinel for the drain task: the task holds the only strong
    /// reference, so this upgrades to `None` exactly when the task has
    /// ended (used for identity-attributed leak detection in tests).
    pub(crate) liveness: std::sync::Weak<()>,
}

fn spawn_local_queue(capacity: usize) -> LocalQueueDrainHandle {
    // `0` historically meant "unbounded" but that's an OOM footgun under
    // slow callbacks. Clamp to the default 1024 to match the documented
    // safe behavior (see SubscriberOptions::history_depth doc).
    let effective_capacity = if capacity == 0 {
        DEFAULT_LOCAL_QUEUE_CAPACITY
    } else {
        capacity
    };
    let (sender, mut consumer) =
        crate::event_queue::EventQueue::new(effective_capacity).into_parts();
    // Termination contract: the queue is consumed via
    // `EventQueueConsumer` (no self-held sender — see
    // `EventQueue::into_parts`), so sender-drop would eventually close
    // the backend and `pop()` would return None. That is only a
    // fallback, though: its timing depends on when the Zenoh callback
    // closures happen to drop their sender clones (the assumption that
    // broke an earlier version), so termination is driven deterministically by the
    // explicit shutdown signal — the returned oneshot sender is dropped
    // (or sent) by the `Subscriber` owner, which makes the `select!`
    // below take the shutdown branch — drain the already-queued events,
    // then exit. The returned `JoinHandle` lets `undeclare()` await
    // termination and abort as a safety net for the case where a slow
    // user callback is still mid-execution when the subscriber is
    // dropped.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_task = std::sync::Arc::clone(&stop);
    let liveness_arc = std::sync::Arc::new(());
    let liveness = std::sync::Arc::downgrade(&liveness_arc);
    let handle = tokio::spawn(async move {
        // `biased` is an intent declaration, not a correctness
        // requirement: queued events take priority over the shutdown
        // branch while any remain. It is NOT what bounds `shutdown_rx`
        // to a single poll — that is the unconditional `break` at the
        // end of the shutdown branch: once the branch runs, the loop is
        // exited and the receiver is never polled again.
        loop {
            tokio::select! {
                biased;
                event = consumer.pop() => match event {
                    Some(event) => {
                        if !dispatch_queued_event(event, &stop_task) {
                            break;
                        }
                    }
                    None => break,
                },
                _ = &mut shutdown_rx => {
                    // Shutdown signal (send or sender drop both resolve the
                    // oneshot): drain what is queued and exit. `biased`
                    // guarantees queued events are preferred over the
                    // shutdown branch while any remain; `queue.pop()` is
                    // cancel-safe (tokio mpsc) and the `try_pop()` loop
                    // below picks up events that arrive while the shutdown
                    // branch runs.
                    drain_local_queue(&mut consumer, &stop_task).await;
                    break;
                }
            };
        }
        drop(liveness_arc);
    });
    LocalQueueDrainHandle {
        sender,
        task: handle,
        shutdown: shutdown_tx,
        stop,
        liveness,
    }
}

fn spawn_overflow_relay_queue(
    event_sender_cell: Arc<std::sync::RwLock<Option<EventQueueSender>>>,
    capacity: usize,
    runtime_stats: Option<Arc<SubscriberRuntimeStatsInner>>,
) -> crate::event_queue::EventQueueSender {
    let (sender, _task_handle) =
        spawn_overflow_relay_queue_with_handle(event_sender_cell, capacity, runtime_stats);
    sender
}

fn spawn_overflow_relay_queue_with_handle(
    event_sender_cell: Arc<std::sync::RwLock<Option<EventQueueSender>>>,
    capacity: usize,
    runtime_stats: Option<Arc<SubscriberRuntimeStatsInner>>,
) -> (
    crate::event_queue::EventQueueSender,
    tokio::task::JoinHandle<()>,
) {
    let (relay_tx, mut relay_rx) = tokio::sync::mpsc::channel::<Event>(capacity.max(1));
    let sender = crate::event_queue::EventQueueSender::from_bounded_channel(relay_tx);
    let task_handle = tokio::spawn(async move {
        while let Some(event) = relay_rx.recv().await {
            // The event has now left the OVERFLOW RELAY queue (a second queue
            // before the executor). Record its pre-executor age as a distinct
            // `sched.relay_wait` so this sojourn is not silently folded into the
            // scheduler's `queue_wait` (construction → dispatch).
            crate::executor::record_relay_wait(&event);
            let Some(sender) = event_sender_cell
                .read()
                .ok()
                .and_then(|guard| guard.as_ref().cloned())
            else {
                if let Some(runtime_stats) = runtime_stats.as_ref() {
                    runtime_stats.on_overflow_relay_drop();
                }
                warn_subscriber_event_drop("executor sender unavailable during overflow relay");
                continue;
            };
            if sender.send(event).await.is_err() {
                if let Some(runtime_stats) = runtime_stats.as_ref() {
                    runtime_stats.on_overflow_relay_drop();
                }
                warn_subscriber_event_drop("executor sender closed during overflow relay");
            }
        }
    });
    (sender, task_handle)
}

/// A construction-time snapshot of the node's executor ingress, tagged with
/// the attachment generation it was taken from. The tag is what
/// keeps the snapshot honest: one relaxed load per event decides whether it
/// still describes the node's current executor.
struct CachedExecutorSender {
    sender: EventQueueSender,
    generation: u64,
}

fn dispatch_event_to_executor_or_local(
    cached_sender: Option<&CachedExecutorSender>,
    event_sender_generation: &AtomicU64,
    event_sender_cell: &Arc<std::sync::RwLock<Option<EventQueueSender>>>,
    local_sender: &EventQueueSender,
    overflow_policy: SubscriberOverflowPolicy,
    overflow_relay_sender: Option<&EventQueueSender>,
    runtime_stats: Option<&SubscriberRuntimeStatsInner>,
    event: Event,
) {
    let mut event = event;
    // The snapshot is usable only while the node is still on the attachment it
    // was taken from; otherwise fall through to the cell, which is the single
    // source of truth for "which executor owns this node right now".
    if let Some(cached) =
        cached_sender.filter(|c| event_sender_generation.load(Ordering::Relaxed) == c.generation)
    {
        match try_dispatch_to_executor_sender(
            &cached.sender,
            overflow_policy,
            overflow_relay_sender,
            runtime_stats,
            event,
        ) {
            Ok(()) => return,
            Err(next_event) => event = next_event,
        }
    }

    if let Some(sender) = event_sender_cell
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().cloned())
    {
        match try_dispatch_to_executor_sender(
            &sender,
            overflow_policy,
            overflow_relay_sender,
            runtime_stats,
            event,
        ) {
            Ok(()) => return,
            Err(next_event) => event = next_event,
        }
    }

    // No executor is attached to this node, or sender has closed; use local queue fallback.
    if let Err(err) = local_sender.try_send(event) {
        match err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                if let Some(runtime_stats) = runtime_stats {
                    runtime_stats.on_local_drop();
                }
                warn_subscriber_event_drop("local subscriber queue full")
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                if let Some(runtime_stats) = runtime_stats {
                    runtime_stats.on_local_drop();
                }
                warn_subscriber_event_drop("local subscriber queue closed")
            }
        }
    }
}

fn try_dispatch_to_executor_sender(
    sender: &EventQueueSender,
    overflow_policy: SubscriberOverflowPolicy,
    overflow_relay_sender: Option<&EventQueueSender>,
    runtime_stats: Option<&SubscriberRuntimeStatsInner>,
    event: Event,
) -> Result<(), Event> {
    match sender.try_send(event) {
        Ok(_) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
            match overflow_policy {
                SubscriberOverflowPolicy::Drop => {
                    if let Some(runtime_stats) = runtime_stats {
                        runtime_stats.on_executor_drop();
                    }
                    warn_subscriber_event_drop("executor queue full")
                }
                SubscriberOverflowPolicy::Relay => {
                    if let Some(relay_sender) = overflow_relay_sender {
                        match relay_sender.try_send(event) {
                            Ok(()) => {
                                if let Some(runtime_stats) = runtime_stats {
                                    runtime_stats.on_overflow_relay_enqueued();
                                }
                            }
                            Err(err) => match err {
                                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                    if let Some(runtime_stats) = runtime_stats {
                                        runtime_stats.on_overflow_relay_drop();
                                    }
                                    warn_subscriber_event_drop("overflow relay queue full")
                                }
                                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                    if let Some(runtime_stats) = runtime_stats {
                                        runtime_stats.on_overflow_relay_drop();
                                    }
                                    warn_subscriber_event_drop("overflow relay queue closed")
                                }
                            },
                        }
                    } else {
                        if let Some(runtime_stats) = runtime_stats {
                            runtime_stats.on_executor_drop();
                        }
                        warn_subscriber_event_drop(
                            "overflow relay policy selected without relay queue",
                        );
                    }
                }
            }
            Ok(())
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(event)) => Err(event),
    }
}

#[cfg(test)]
#[path = "../test/subscriber_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/subscriber_advanced_tests.rs"]
mod advanced_tests;
