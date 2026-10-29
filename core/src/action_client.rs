use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{timeout, Instant};
use uuid::Uuid;
use zenoh::pubsub::Subscriber as ZenohSubscriber;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionPublicStatus, ActionReplyCode,
    ActionSendGoalResponse, ActionWire,
};
use crate::action_server::{
    fetch_action_info, DEFAULT_ACTION_CONTROL_TIMEOUT_MS, DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
};
use crate::error::{AortaError, AortaResult};
use crate::key_expr::{normalize_path, KeyExprBuilder};
use crate::log_prelude::*;
use crate::node::Node;
use crate::schema::TypedSubscribeMessage;
use crate::session::Session;
use crate::transport::{reply_error_text, reply_is_zenoh_query_timeout};
use crate::ZBytes;

/// Bound on the one-shot liveliness seed query at wait_result start.
/// Only delays entering the wait loop once per call;
/// status samples arriving meanwhile are buffered by the already-declared
/// status subscriber.
const ACTION_ALIVE_SEED_TIMEOUT: Duration = Duration::from_secs(2);

const ACTION_MULTI_REPLY_CHECK_WINDOW_MS: u64 = 25;
const ACTION_SEND_GOAL_RETRY_POLL_MS: u64 = 25;
const ACTION_SEND_GOAL_RETRY_STEP_MS: u64 = 250;

// Send-goal retry previously had a fixed-interval poll
// (`ACTION_SEND_GOAL_RETRY_POLL_MS`). When N action clients race to
// claim the same goal slot they all wake on the same period and
// stampede the server (thundering herd). ±25% jitter sourced from
// wall-clock nanoseconds spreads the wakeups across a ~12 ms window
// per 25 ms poll, which is cheap, dep-free, and good enough for
// herd avoidance (no crypto claim). `jitter_with` is factored out
// so the unit test can drive it deterministically without depending
// on the host clock.
const ACTION_SEND_GOAL_RETRY_JITTER_PCT: u32 = 25;

fn jitter_with(base: Duration, jitter_pct: u32, entropy: u64) -> Duration {
    let base_ms = base.as_millis() as u64;
    let span_ms = base_ms.saturating_mul(jitter_pct as u64) / 100;
    if span_ms == 0 {
        return base;
    }
    let range = 2 * span_ms + 1;
    let offset_ms = entropy % range;
    let result_ms = base_ms.saturating_sub(span_ms).saturating_add(offset_ms);
    Duration::from_millis(result_ms)
}

fn jittered_retry_poll(base: Duration) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    jitter_with(base, ACTION_SEND_GOAL_RETRY_JITTER_PCT, entropy)
}

/// Default capacity of the action feedback queue. Sized for ~10 Hz feedback
/// at modest payload sizes; tuneable via [`ActionClientOptions::feedback_capacity`].
pub const DEFAULT_ACTION_FEEDBACK_CAPACITY: usize = 256;

/// Behavior when the bounded feedback queue fills before the consumer drains it.
///
/// The previous implementation used an unbounded
/// channel which could exhaust memory under a slow / paused consumer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FeedbackOverflowPolicy {
    /// Drop the oldest queued sample to make room for the new one.
    ///
    /// Use for high-frequency progress streams (e.g. odometry, percent-done)
    /// where the consumer cares about the latest state more than completeness.
    /// Defaults are tuned for this case.
    #[default]
    DropOldest,
    /// Drop the newly-arriving sample, keeping the existing queue intact.
    ///
    /// Use for ordered status streams (e.g. discrete state transitions) where
    /// a gap mid-stream is more confusing than truncation at the tail.
    DropNew,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionClientOptions {
    pub control_timeout: Duration,
    pub info_timeout: Duration,
    /// Maximum number of feedback samples buffered between Zenoh callback
    /// thread and consumer task. See [`DEFAULT_ACTION_FEEDBACK_CAPACITY`].
    pub feedback_capacity: usize,
    /// Policy applied when the feedback queue is full at send time.
    pub feedback_overflow_policy: FeedbackOverflowPolicy,
    /// Optional application-level namespace prefix (e.g. a robot SN) for a
    /// cross-namespace cloud action client. When set, every action key is built
    /// as `{namespace}/aorta/{group}/act/...`. A wildcard namespace is rejected
    /// on the goal/cancel/get_result (request/reply) surfaces — an action must
    /// target a single robot. `None` = ordinary action client. See
    /// `spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md` §5.
    pub namespace: Option<String>,
}

impl Default for ActionClientOptions {
    fn default() -> Self {
        Self {
            control_timeout: Duration::from_millis(DEFAULT_ACTION_CONTROL_TIMEOUT_MS),
            info_timeout: Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
            feedback_capacity: DEFAULT_ACTION_FEEDBACK_CAPACITY,
            feedback_overflow_policy: FeedbackOverflowPolicy::default(),
            namespace: None,
        }
    }
}

pub struct ActionClient<TAction: ActionWire> {
    inner: Arc<ActionClientInner<TAction>>,
}

impl<TAction: ActionWire> Clone for ActionClient<TAction> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<TAction: ActionWire> ActionClient<TAction> {
    /// Infallible constructor. Panics only if `action_name` is malformed or
    /// `options.namespace` is set to a malformed namespace — use
    /// [`ActionClient::try_new`] when either comes from untrusted input (the
    /// cross-namespace `*_for` SDK methods do). The default options carry no
    /// namespace, and a codegen `ACTION_NAME` is always valid, so this never
    /// panics for the ordinary on-robot path.
    pub fn new(node: &Node, action_name: &str, options: ActionClientOptions) -> Self {
        Self::try_new(node, action_name, options)
            .expect("ActionClient::new: invalid action_name or namespace in options; use try_new")
    }

    /// Fallible constructor that normalizes `action_name`, validates
    /// `options.namespace` (if any) and binds the action client to
    /// `{namespace}/aorta/{group}/act/...`. The action name is a runtime
    /// routing identity decoupled from the schema, mirroring topic/service.
    pub fn try_new(
        node: &Node,
        action_name: &str,
        options: ActionClientOptions,
    ) -> AortaResult<Self> {
        let action_name = normalize_path(action_name)?;
        let key_builder = match &options.namespace {
            Some(ns) => {
                let kb = node.key_builder().clone().with_namespace(ns)?;
                // Fail fast: an action targets a single robot, so reject a
                // wildcard namespace now (the goal/cancel/get_result surfaces
                // use NamespaceMode::Reject) rather than on first send_goal.
                kb.action_send_goal_key(&action_name)?;
                kb
            }
            None => node.key_builder().clone(),
        };
        Ok(Self {
            inner: Arc::new(ActionClientInner {
                session: node.session().clone(),
                key_builder,
                ev_goal_sent_to_accepted: crate::trace::stage_event_for(
                    crate::trace::StageId::ActionGoalSentToAccepted,
                    &action_name,
                ),
                ev_result_wait: crate::trace::stage_event_for(
                    crate::trace::StageId::ActionResultWait,
                    &action_name,
                ),
                ev_goal_lifetime: crate::trace::stage_event_for(
                    crate::trace::StageId::ActionGoalLifetime,
                    &action_name,
                ),
                ev_feedback_recv: crate::trace::marker_event_for(
                    "action.feedback_recv",
                    &action_name,
                ),
                action_name,
                options,
                goal_streams: parking_lot::Mutex::new(GoalStreamRegistry::default()),
                _marker: PhantomData,
            }),
        })
    }

    pub fn action_name(&self) -> &str {
        &self.inner.action_name
    }

    /// Mint a fresh goal id in the canonical format `send_goal_with_id`
    /// accepts (hyphenated UUID v4) — the same generator `send_goal` uses
    /// internally. Pre-generate one to pair `feedback_stream_for` with
    /// `send_goal_with_id` (subscribe-before-send). Matches the
    /// C++ SDK's `GenerateGoalId`.
    pub fn generate_goal_id() -> String {
        Uuid::new_v4().hyphenated().to_string()
    }

    pub async fn send_goal(&self, goal: &TAction::GoalMsg<'_>) -> AortaResult<GoalHandle<TAction>> {
        let goal_id = Self::generate_goal_id();
        self.send_goal_with_id(&goal_id, goal).await
    }

    pub async fn send_goal_with_id(
        &self,
        goal_id: &str,
        goal: &TAction::GoalMsg<'_>,
    ) -> AortaResult<GoalHandle<TAction>> {
        let request = TAction::encode_send_goal_request(goal_id, goal)?;
        let key: String = self
            .inner
            .key_builder
            .action_send_goal_key(&self.inner.action_name)?
            .into();
        let deadline = Instant::now() + self.inner.options.control_timeout;
        let retry_step = Duration::from_millis(ACTION_SEND_GOAL_RETRY_STEP_MS);
        let retry_poll = Duration::from_millis(ACTION_SEND_GOAL_RETRY_POLL_MS);
        let mut payload = None;
        let request = ZBytes::from(request);
        // Tracing: capture the monotonic send time once (gated; no clock read
        // when off) for the client-observed goal-sent -> accepted latency.
        let sent_mono_ns = if crate::trace::trace_enabled() {
            Some(crate::time::monotonic_ns())
        } else {
            None
        };
        // Pre-mint the goal_sent_to_accepted span identity so the send_goal
        // query can carry it as the parent of the server's goal lifecycle
        // spans (cross-process trace propagation); the span itself is recorded below, on
        // accept, under the SAME id.
        let link = sent_mono_ns.and_then(|_| crate::trace::outbound_link());
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            // PER-ATTEMPT: a send_goal issued inside a
            // budgeted flow task stamps the ambient cap's CURRENT
            // remaining window as the GOAL budget — a goal accepted on a
            // retry carries the shrunken value, not the pre-backoff one
            // (absent = unlimited (un-stamped behavior)).
            // `AORTA_RPC_DEADLINE=0` disables the stamp — the same
            // fleet-wide rollback lever the plain-RPC path honors
            // (this surface was once ungated, so
            // the documented rollback silently did nothing for actions).
            let deadline_budget_ns = crate::flow::ambient::budget_cap_remaining_ns()
                .filter(|_| crate::client::rpc_deadline_stamping_enabled());
            let request_attachment = if link.is_none() && deadline_budget_ns.is_none() {
                None
            } else {
                crate::attachment::encode(&crate::attachment::AttachmentParts {
                    schema_hash: None,
                    traceparent: link.map(|l| crate::trace::TraceParent {
                        trace_id: l.trace_id,
                        span_id: l.span_id,
                        flags: crate::trace::TRACEPARENT_SAMPLED,
                    }),
                    reply_busy_ns: None,
                    deadline_budget_ns,
                })
            };

            let remaining = deadline.saturating_duration_since(now);
            let attempt_timeout = remaining.min(retry_step);
            if let Some(reply) = self
                .inner
                .query_optional_bytes_with_payload_attached(
                    &key,
                    Some(request.clone()),
                    request_attachment.as_deref(),
                    attempt_timeout,
                )
                .await?
            {
                payload = Some(reply);
                break;
            }

            if Instant::now() >= deadline {
                break;
            }
            // Jitter the poll interval per-iteration so concurrent
            // clients don't synchronize their wake-ups against the same
            // 25 ms period and stampede the action server.
            let sleep_for = jittered_retry_poll(retry_poll)
                .min(deadline.saturating_duration_since(Instant::now()));
            tokio::time::sleep(sleep_for).await;
        }

        let payload = payload.ok_or_else(|| {
            AortaError::timeout(format!(
                "action '{}' query '{}' timed out after {:?}",
                self.inner.action_name, key, self.inner.options.control_timeout
            ))
        })?;
        let response = TAction::decode_send_goal_response(payload.to_bytes().as_ref())?;
        if response.goal_id != goal_id {
            return Err(AortaError::internal(format!(
                "action '{}' send_goal reply goal_id mismatch: expected '{}', got '{}'",
                self.inner.action_name, goal_id, response.goal_id
            )));
        }
        if !response.accepted {
            return Err(map_send_goal_rejection(&self.inner.action_name, &response));
        }

        // Tracing: goal sent -> accepted (client-observed; monotonic, gated). The
        // clock is read only if tracing is still on (re-checked in the helper).
        // Recorded under the pre-minted identity when the query carried one, so
        // the server's goal lifecycle spans parent to THIS span.
        if let Some(sent) = sent_mono_ns {
            match link {
                Some(l) => crate::trace::record_stage_elapsed_linked(
                    crate::trace::StageId::ActionGoalSentToAccepted,
                    sent,
                    self.inner.ev_goal_sent_to_accepted,
                    l,
                ),
                None => crate::trace::record_stage_elapsed_as(
                    crate::trace::StageId::ActionGoalSentToAccepted,
                    sent,
                    self.inner.ev_goal_sent_to_accepted,
                ),
            }
        }

        Ok(GoalHandle {
            client: self.clone(),
            goal_id: response.goal_id,
            goal_secret: response.goal_secret,
            server_epoch: response.server_epoch,
            trace_link: link,
            sent_mono_ns,
            lifetime_recorded: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn info(&self) -> AortaResult<Option<crate::action::ActionInfo>> {
        fetch_action_info(
            &self.inner.session,
            &self.inner.key_builder,
            &self.inner.action_name,
            self.inner.options.info_timeout,
        )
        .await
    }

    pub async fn query_status(&self, goal_id: &str) -> AortaResult<Option<ActionPublicStatus>> {
        self.query_status_with_timeouts(
            goal_id,
            Some(self.inner.options.info_timeout),
            Some(self.inner.options.info_timeout),
        )
        .await
    }

    async fn query_status_with_timeouts(
        &self,
        goal_id: &str,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
    ) -> AortaResult<Option<ActionPublicStatus>> {
        let key: String = self
            .inner
            .key_builder
            .action_status_key(&self.inner.action_name, goal_id)?
            .into();
        match self
            .inner
            .query_optional_snapshot_bytes(&key, zenoh_timeout, recv_timeout)
            .await?
        {
            Some(payload) => TAction::decode_public_status(payload.to_bytes().as_ref()).map(Some),
            None => Ok(None),
        }
    }

    pub async fn cancel(
        &self,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<ActionCancelGoalResponse> {
        let request = TAction::encode_cancel_goal_request(goal_id, goal_secret)?;
        let key: String = self
            .inner
            .key_builder
            .action_cancel_goal_key(&self.inner.action_name)?
            .into();
        let payload = self
            .inner
            .query_bytes(
                &key,
                ZBytes::from(request),
                self.inner.options.control_timeout,
            )
            .await?;
        let response = TAction::decode_cancel_goal_response(payload.to_bytes().as_ref())?;
        if response.goal_id != goal_id {
            return Err(AortaError::internal(format!(
                "action '{}' cancel reply goal_id mismatch: expected '{}', got '{}'",
                self.inner.action_name, goal_id, response.goal_id
            )));
        }
        if !response.accepted {
            return Err(map_cancel_rejection(&self.inner.action_name, &response));
        }
        Ok(response)
    }

    pub async fn get_result(
        &self,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        self.get_result_with_timeouts(
            goal_id,
            goal_secret,
            Some(self.inner.options.control_timeout),
            Some(self.inner.options.control_timeout),
        )
        .await
    }

    async fn get_result_with_timeouts(
        &self,
        goal_id: &str,
        goal_secret: &str,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        let request = TAction::encode_get_result_request(goal_id, goal_secret)?;
        let key: String = self
            .inner
            .key_builder
            .action_get_result_key_pub(&self.inner.action_name)?
            .into();
        let payload = self
            .inner
            .query_bytes_custom(&key, ZBytes::from(request), zenoh_timeout, recv_timeout)
            .await?;
        let response = TAction::decode_get_result_response(payload.to_bytes().as_ref())?;
        // Every result path (get_result / wait_result, handle or raw)
        // funnels through here, so this is the single point where the client
        // conclusively learns "this goal publishes no more feedback": a
        // terminal status, or a NotFound/GoalExpired rejection (the goal is
        // gone — nothing can publish for it again). Close any goal-scoped
        // feedback streams so their `recv()` drains buffered samples and then
        // returns `None` instead of parking forever. Unauthorized says
        // nothing about the goal, so it does not close.
        let no_more_feedback = response.goal_id == goal_id
            && (response.status.is_terminal()
                || matches!(
                    response.code,
                    ActionReplyCode::NotFound | ActionReplyCode::GoalExpired
                ));
        if no_more_feedback {
            self.inner.close_goal_streams(goal_id);
        }
        validate_get_result_response(&self.inner.action_name, goal_id, response)
    }

    /// Open a goal-scoped feedback stream for `goal_id` without a
    /// [`GoalHandle`] — pair it with [`send_goal_with_id`] and subscribe
    /// BEFORE sending, so feedback the server publishes as its first handler
    /// action can never race the subscription (a plain
    /// `send_goal` + `GoalHandle::feedback_stream` leaves a window where
    /// early samples are lost, since topic subscriptions do not replay).
    ///
    /// Like `GoalHandle::feedback_stream`, the stream closes itself once this
    /// client observes the goal's terminal result: `recv()`
    /// drains any buffered samples and then returns `None`.
    ///
    /// [`send_goal_with_id`]: ActionClient::send_goal_with_id
    pub async fn feedback_stream_for(
        &self,
        goal_id: &str,
    ) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        self.feedback_stream_filtered(Some(goal_id), None).await
    }

    pub async fn feedback_stream_all(&self) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        self.feedback_stream_filtered(None, None).await
    }

    async fn feedback_stream_filtered(
        &self,
        goal_id_filter: Option<&str>,
        trace_link: Option<crate::trace::SpanLink>,
    ) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        let key: String = self
            .inner
            .key_builder
            .action_feedback_key_pub(&self.inner.action_name)?
            .into();
        let (tx, rx) = bounded_feedback::channel(
            self.inner.options.feedback_capacity,
            self.inner.options.feedback_overflow_policy,
        );
        // Goal-scoped streams register a close signal keyed by goal_id
        // so the terminal observation in `get_result_with_timeouts` can end
        // them. Registration also prunes entries whose streams are already
        // gone (Weak), so the map is bounded by live streams plus goals still
        // awaiting their result — it never grows with completed-goal count.
        // The terminal check and the registration share ONE lock acquisition,
        // so a concurrent terminal observation can never slip between them:
        // either it ran first (this stream is born closed) or it will find
        // this registration and close it.
        if let Some(goal_id) = goal_id_filter {
            let mut registry = self.inner.goal_streams.lock();
            if registry.is_recently_terminal(goal_id) {
                // Terminal already observed for this goal: no feedback can
                // arrive anymore, so skip the zenoh subscriber entirely and
                // hand back a born-closed stream (recv() returns None after
                // draining nothing) instead of one that parks forever.
                drop(registry);
                drop(tx);
                return Ok(ActionFeedbackStream {
                    subscriber: None,
                    receiver: rx,
                });
            }
            registry.closers.retain(|_, entries| {
                entries.retain(|closer| !closer.is_dead());
                !entries.is_empty()
            });
            registry
                .closers
                .entry(goal_id.to_string())
                .or_default()
                .push(tx.closer());
        }
        let expected_goal_id = goal_id_filter.map(ToOwned::to_owned);
        let action_name = self.inner.action_name.clone();
        let ev_feedback_recv = self.inner.ev_feedback_recv;

        let subscriber = self
            .inner
            .session
            .inner()
            .declare_subscriber(key.clone())
            .callback(move |sample| {
                let fields: zenoh::sample::SampleFields = sample.into();
                if fields.payload.is_empty() {
                    tx.send(Err(AortaError::invalid_argument(format!(
                        "action '{}' feedback sample on '{}' is empty",
                        action_name, key
                    ))));
                    return;
                }

                let payload = fields.payload.to_bytes().to_vec();
                if let Some(expected_goal_id) = expected_goal_id.as_deref() {
                    match TAction::decode_feedback_goal_id(&payload) {
                        Ok(goal_id) => {
                            if goal_id != expected_goal_id {
                                return;
                            }
                        }
                        Err(err) => {
                            tx.send(Err(err));
                            return;
                        }
                    }
                }

                // Feedback trace propagation: the per-feedback
                // traceparent rides the attachment, so the receive marker
                // parents to the SERVER's feedback_pub span — pub → recv
                // renders as the per-message transit hop. Legacy publishers
                // (no attachment) fall back to a PARENTLESS marker on the
                // goal's trace when the stream is goal-scoped (no arrow —
                // the goal_sent span is not the marker's cause).
                if crate::trace::trace_enabled() {
                    let wire_parent = fields.attachment.as_ref().and_then(|attachment| {
                        crate::attachment::decode(attachment.to_bytes().as_ref()).traceparent
                    });
                    let parent = match (wire_parent, trace_link.as_ref()) {
                        (Some(tp), _) => Some(crate::trace::SpanLink {
                            trace_id: tp.trace_id,
                            span_id: tp.span_id,
                            parent_span_id: 0,
                        }),
                        (None, Some(link)) => Some(crate::trace::SpanLink {
                            trace_id: link.trace_id,
                            span_id: 0,
                            parent_span_id: 0,
                        }),
                        (None, None) => None,
                    };
                    if let Some(parent) = parent {
                        let now = crate::time::monotonic_ns();
                        crate::trace::emit_stage_span_under(ev_feedback_recv, now, now, &parent);
                    }
                }
                tx.send(Ok(ActionFeedbackSample::<TAction>::new(payload)));
            })
            .await?;

        Ok(ActionFeedbackStream {
            subscriber: Some(subscriber),
            receiver: rx,
        })
    }
}

struct ActionClientInner<TAction: ActionWire> {
    session: Session,
    key_builder: KeyExprBuilder,
    action_name: String,
    /// Interned per-action waterfall event
    /// (`action.goal_sent_to_accepted:<action>`), minted once at build.
    ev_goal_sent_to_accepted: u16,
    ev_result_wait: u16,
    /// Interned per-action event for the client end-to-end goal-lifetime span
    /// (`action.goal_lifetime:<action>`), minted once at build.
    ev_goal_lifetime: u16,
    ev_feedback_recv: u16,
    options: ActionClientOptions,
    /// Goal-scoped feedback stream state for the end-of-stream contract
    /// (open-stream close signals + a bounded memory of observed terminals).
    /// `feedback_stream_all` never touches this. One lock covers both halves
    /// so terminal observation and stream registration are atomic w.r.t.
    /// each other.
    goal_streams: parking_lot::Mutex<GoalStreamRegistry>,
    _marker: PhantomData<TAction>,
}

/// How many recently-terminal goal ids the client remembers so a goal-scoped
/// stream opened AFTER (or racing) the terminal observation is born closed
/// instead of parking forever. Bounded FIFO: once evicted, a late-opened
/// stream for that goal behaves like any subscription with no publisher —
/// the memory is a race-window guard, not a permanent archive (an unbounded
/// set would reintroduce the completed-goal growth issue).
const RECENT_TERMINAL_GOALS_CAP: usize = 128;

#[derive(Default)]
struct GoalStreamRegistry {
    /// `goal_id` → close signals for every open stream filtered to it.
    /// Registered by `feedback_stream_filtered`, pruned when streams are
    /// gone (Weak), consumed by `close_goal_streams` on terminal.
    closers: HashMap<String, Vec<bounded_feedback::Closer>>,
    /// FIFO of goal ids whose terminal was observed, capped at
    /// [`RECENT_TERMINAL_GOALS_CAP`].
    recent_terminals: std::collections::VecDeque<String>,
}

impl GoalStreamRegistry {
    fn is_recently_terminal(&self, goal_id: &str) -> bool {
        self.recent_terminals.iter().any(|g| g == goal_id)
    }

    fn record_terminal(&mut self, goal_id: &str) {
        if self.is_recently_terminal(goal_id) {
            return;
        }
        if self.recent_terminals.len() >= RECENT_TERMINAL_GOALS_CAP {
            self.recent_terminals.pop_front();
        }
        self.recent_terminals.push_back(goal_id.to_string());
    }
}

impl<TAction: ActionWire> ActionClientInner<TAction> {
    /// Close every goal-scoped feedback stream registered for
    /// `goal_id`, and remember the terminal so a stream opened after (or
    /// racing) this observation is born closed. Idempotent.
    fn close_goal_streams(&self, goal_id: &str) {
        let closers = {
            let mut registry = self.goal_streams.lock();
            registry.record_terminal(goal_id);
            registry.closers.remove(goal_id)
        };
        if let Some(closers) = closers {
            for closer in closers {
                closer.close();
            }
        }
    }

    async fn query_bytes(
        &self,
        key: &str,
        payload: ZBytes,
        timeout_duration: Duration,
    ) -> AortaResult<ZBytes> {
        self.query_optional_bytes_with_payload(key, Some(payload), timeout_duration)
            .await?
            .ok_or_else(|| {
                AortaError::timeout(format!(
                    "action '{}' query '{}' timed out after {:?}",
                    self.action_name, key, timeout_duration
                ))
            })
    }

    async fn query_bytes_custom(
        &self,
        key: &str,
        payload: ZBytes,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
    ) -> AortaResult<ZBytes> {
        self.query_optional_bytes_with_payload_custom(
            key,
            Some(payload),
            zenoh_timeout,
            recv_timeout,
        )
        .await?
        .ok_or_else(|| match recv_timeout {
            Some(timeout_duration) => AortaError::timeout(format!(
                "action '{}' query '{}' timed out after {:?}",
                self.action_name, key, timeout_duration
            )),
            None => AortaError::timeout(format!(
                "action '{}' query '{}' returned no reply",
                self.action_name, key
            )),
        })
    }

    async fn query_optional_bytes(
        &self,
        key: &str,
        timeout_duration: Duration,
    ) -> AortaResult<Option<ZBytes>> {
        self.query_optional_bytes_with_payload(key, None, timeout_duration)
            .await
    }

    async fn query_optional_bytes_with_payload(
        &self,
        key: &str,
        payload: Option<ZBytes>,
        timeout_duration: Duration,
    ) -> AortaResult<Option<ZBytes>> {
        self.query_optional_bytes_with_payload_custom(
            key,
            payload,
            Some(timeout_duration),
            Some(timeout_duration),
        )
        .await
    }

    /// Like [`Self::query_optional_bytes_with_payload`], but with a query
    /// attachment (the traceparent rides the attachment on action control
    /// hops — the trace-context-aware query helper).
    async fn query_optional_bytes_with_payload_attached(
        &self,
        key: &str,
        payload: Option<ZBytes>,
        attachment: Option<&[u8]>,
        timeout_duration: Duration,
    ) -> AortaResult<Option<ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            payload,
            attachment,
            Some(timeout_duration),
            Some(timeout_duration),
            ReplyErrPolicy::Internal,
        )
        .await
    }

    async fn query_optional_bytes_with_payload_custom(
        &self,
        key: &str,
        payload: Option<ZBytes>,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
    ) -> AortaResult<Option<ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            payload,
            None,
            zenoh_timeout,
            recv_timeout,
            ReplyErrPolicy::Internal,
        )
        .await
    }

    async fn query_optional_snapshot_bytes(
        &self,
        key: &str,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
    ) -> AortaResult<Option<ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            None,
            None,
            zenoh_timeout,
            recv_timeout,
            ReplyErrPolicy::Absent,
        )
        .await
    }

    async fn query_optional_bytes_inner(
        &self,
        key: &str,
        payload: Option<ZBytes>,
        attachment: Option<&[u8]>,
        zenoh_timeout: Option<Duration>,
        recv_timeout: Option<Duration>,
        reply_err_policy: ReplyErrPolicy,
    ) -> AortaResult<Option<ZBytes>> {
        let builder = self
            .session
            .inner()
            .get(key)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None);
        let replies = match (
            payload,
            effective_zenoh_timeout(zenoh_timeout, recv_timeout),
            attachment,
        ) {
            (Some(payload), Some(timeout_duration), Some(attachment)) => {
                builder
                    .payload(payload)
                    .timeout(timeout_duration)
                    .attachment(attachment)
                    .await?
            }
            (Some(payload), Some(timeout_duration), None) => {
                builder.payload(payload).timeout(timeout_duration).await?
            }
            (Some(payload), None, Some(attachment)) => {
                builder.payload(payload).attachment(attachment).await?
            }
            (Some(payload), None, None) => builder.payload(payload).await?,
            (None, Some(timeout_duration), Some(attachment)) => {
                builder
                    .timeout(timeout_duration)
                    .attachment(attachment)
                    .await?
            }
            (None, Some(timeout_duration), None) => builder.timeout(timeout_duration).await?,
            (None, None, Some(attachment)) => builder.attachment(attachment).await?,
            (None, None, None) => builder.await?,
        };

        let first = match recv_timeout {
            Some(timeout_duration) => match timeout(timeout_duration, replies.recv_async()).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(_)) | Err(_) => return Ok(None),
            },
            None => match replies.recv_async().await {
                Ok(reply) => reply,
                Err(_) => return Ok(None),
            },
        };

        // A synthesized Zenoh query-timeout reply is the dual-guard firing
        // (client session and/or router `QueryCleanup` timer — see
        // `effective_zenoh_timeout` and `reply_error_is_zenoh_query_timeout`),
        // not a server answer. Surface it as the recv guard's `Ok(None)`
        // (`send_goal` retries within its deadline; one-shot callers map to
        // `AortaError::Timeout`) BEFORE the multi-reply check below — under
        // load both timers can land their `Err("Timeout")` replies in the
        // channel before the recv guard fires, and a second one must never be
        // mistaken for a `ConcurrentConflict`.
        if reply_is_zenoh_query_timeout(&first) {
            return Ok(None);
        }

        let check_window = recv_timeout
            .map(|timeout_duration| {
                timeout_duration.min(Duration::from_millis(ACTION_MULTI_REPLY_CHECK_WINDOW_MS))
            })
            .unwrap_or_else(|| Duration::from_millis(ACTION_MULTI_REPLY_CHECK_WINDOW_MS));
        // Multi-reply guard: a genuine second reply means two servers answered
        // the same goal. A synthesized timeout reply is the dual-guard, not a
        // conflict — skip it so the held-query timeout path never trips this.
        if !check_window.is_zero()
            && matches!(
                timeout(check_window, replies.recv_async()).await,
                Ok(Ok(second)) if !reply_is_zenoh_query_timeout(&second)
            )
        {
            return Err(AortaError::concurrent_conflict(format!(
                "action '{}' query '{}' returned multiple replies; deployment must enforce one active server per {{group,action}}",
                self.action_name, key
            )));
        }

        match first.result() {
            Ok(sample) => Ok(Some(sample.payload().clone())),
            Err(err) => {
                // `first` is a non-timeout reply error here (synthesized Zenoh
                // query-timeout replies already returned `Ok(None)` above), so
                // it is a genuine provider fault — surface it per policy.
                match reply_err_policy {
                    ReplyErrPolicy::Internal => Err(AortaError::internal(format!(
                        "action '{}' query '{}' returned error: {}",
                        self.action_name,
                        key,
                        reply_error_text(err)
                    ))),
                    ReplyErrPolicy::Absent => Ok(None),
                }
            }
        }
    }
}

#[derive(Copy, Clone)]
enum ReplyErrPolicy {
    /// Surface a Zenoh `reply_err` as `AortaError::Internal`. Use for control-path RPCs
    /// (`send_goal`, `cancel`, `get_result`) where a queryable returning an error is a real fault.
    Internal,
    /// Surface a Zenoh `reply_err` as `Ok(None)`. Use for snapshot-style queries
    /// (e.g. `query_status`) where the server-side telemetry queryable emits `reply_err`
    /// to mean "no current snapshot" — see `core/src/telemetry/queryable.rs`.
    Absent,
}

pub struct ActionFeedbackSample<TAction: ActionWire>
where
    <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    payload: Vec<u8>,
    _marker: PhantomData<fn() -> TAction>,
}

impl<TAction: ActionWire> ActionFeedbackSample<TAction>
where
    <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            _marker: PhantomData,
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn decode(
        &self,
    ) -> AortaResult<
        <<TAction as crate::action::ActionSchema>::Feedback as TypedSubscribeMessage>::Root<'_>,
    > {
        <TAction as crate::action::ActionSchema>::Feedback::decode_typed_root(&self.payload)
    }
}

pub struct ActionFeedbackStream<TAction: ActionWire>
where
    <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    subscriber: Option<ZenohSubscriber<()>>,
    receiver: bounded_feedback::Receiver<AortaResult<ActionFeedbackSample<TAction>>>,
}

impl<TAction: ActionWire> ActionFeedbackStream<TAction>
where
    <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    pub async fn recv(&mut self) -> Option<AortaResult<ActionFeedbackSample<TAction>>> {
        self.receiver.recv().await
    }

    /// Total number of feedback samples dropped due to queue overflow since
    /// this stream was created. Surfaced for diagnostics; consumers can poll
    /// it to detect that they are falling behind the producer.
    pub fn dropped_count(&self) -> u64 {
        self.receiver.dropped_count()
    }

    pub async fn close(mut self) -> AortaResult<()> {
        if let Some(subscriber) = self.subscriber.take() {
            subscriber.undeclare().await?;
        }
        Ok(())
    }
}

pub struct GoalHandle<TAction: ActionWire> {
    client: ActionClient<TAction>,
    goal_id: String,
    goal_secret: String,
    server_epoch: String,
    /// The goal's client-side trace identity (the `goal_sent_to_accepted`
    /// span) — `result_wait`, the goal-lifetime span, and feedback-receive
    /// markers join the goal's trace through it. `None` for persisted handles
    /// (the original trace belongs to another process lifetime).
    trace_link: Option<crate::trace::SpanLink>,
    /// Monotonic `send_goal` timestamp (the request instant) — the START of the
    /// `action.goal_lifetime` span. `None` when tracing was off at send (no
    /// clock read) or for persisted handles; the lifetime span is then skipped.
    sent_mono_ns: Option<u64>,
    /// One-shot guard so the goal-lifetime span is emitted AT MOST once per
    /// goal even across handle clones (an owned clone in a background result
    /// watcher) or repeated terminal `get_result()` calls. `Arc` so clones
    /// share the same flag.
    lifetime_recorded: Arc<AtomicBool>,
}

// Hand-written so the derive does not impose `TAction: Clone`; every field is
// independently cloneable. Cloning a handle yields a second view of the SAME
// goal (shared `ActionClient` Arc + copied identity/trace), letting the SDK
// move an owned handle into a background result watcher without consuming the
// caller's handle.
impl<TAction: ActionWire> Clone for GoalHandle<TAction> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            goal_id: self.goal_id.clone(),
            goal_secret: self.goal_secret.clone(),
            server_epoch: self.server_epoch.clone(),
            trace_link: self.trace_link,
            sent_mono_ns: self.sent_mono_ns,
            lifetime_recorded: Arc::clone(&self.lifetime_recorded),
        }
    }
}

impl<TAction: ActionWire> GoalHandle<TAction> {
    pub fn from_persisted(
        client: &ActionClient<TAction>,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<Self> {
        crate::action::goal_id_to_key(goal_id)?;
        if goal_secret.trim().is_empty() {
            return Err(AortaError::invalid_argument(
                "persisted action goal_secret must not be empty",
            ));
        }
        Ok(Self {
            client: client.clone(),
            goal_id: goal_id.to_string(),
            goal_secret: goal_secret.to_string(),
            server_epoch: String::new(),
            trace_link: None,
            // The original send happened in another process lifetime, so there
            // is no client-side request instant to anchor a lifetime span to.
            sent_mono_ns: None,
            lifetime_recorded: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }

    pub fn goal_secret(&self) -> &str {
        &self.goal_secret
    }

    pub fn server_epoch(&self) -> &str {
        &self.server_epoch
    }

    pub async fn query_status(&self) -> AortaResult<Option<ActionPublicStatus>> {
        self.client.query_status(&self.goal_id).await
    }

    pub async fn cancel(&self) -> AortaResult<ActionCancelGoalResponse> {
        self.client.cancel(&self.goal_id, &self.goal_secret).await
    }

    pub async fn get_result(
        &self,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        let result = self
            .client
            .get_result(&self.goal_id, &self.goal_secret)
            .await;
        // Close the client goal-lifetime span the first time a poll observes a
        // TERMINAL result — a non-terminal ("still executing") reply is just a
        // poll, not the end of the goal.
        if let Ok(response) = &result {
            if response.status.is_terminal() {
                self.record_goal_lifetime();
            }
        }
        result
    }

    /// Emit the client-side end-to-end **goal-lifetime** span
    /// (`action.goal_lifetime`): `send_goal` (request) → this terminal result,
    /// joined to the goal's trace under the `goal_sent_to_accepted` span (which
    /// nests at its head). The single client slice spanning the whole action.
    /// Anchored at the EXACT `send_goal` instant so the short handshake span
    /// nests inside it rather than clamping it. Recorded at most once per goal
    /// (shared across clones) and only on a terminal result; a no-op when
    /// tracing was off at send (`sent_mono_ns` is `None`).
    fn record_goal_lifetime(&self) {
        let Some(start) = self.sent_mono_ns else {
            return;
        };
        // Claim the single emission up front (idempotent across clones / repeat
        // terminal polls); the helper re-checks the master gate and skips the
        // clock read if tracing toggled off in between.
        if self.lifetime_recorded.swap(true, Ordering::Relaxed) {
            return;
        }
        crate::trace::record_stage_elapsed_under(
            crate::trace::StageId::ActionGoalLifetime,
            start,
            self.client.inner.ev_goal_lifetime,
            self.trace_link.as_ref(),
        );
    }

    pub async fn feedback_stream(&self) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as crate::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        self.client
            .feedback_stream_filtered(Some(&self.goal_id), self.trace_link)
            .await
    }

    pub async fn wait_result(
        &self,
        timeout_duration: Option<Duration>,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        // Tracing (result-delivery stage, client half): wait entry -> result obtained,
        // recorded for every outcome and joined to the goal's trace as a
        // child of the goal_sent_to_accepted span.
        let wait_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
        let result = self.wait_result_inner(timeout_duration).await;
        if let Some(start) = wait_start {
            crate::trace::record_stage_elapsed_under(
                crate::trace::StageId::ActionResultWait,
                start,
                self.client.inner.ev_result_wait,
                self.trace_link.as_ref(),
            );
        }
        // `wait_result_inner` returns `Ok` only once the goal is terminal, so a
        // success closes the end-to-end goal-lifetime span (request → result).
        if result.is_ok() {
            self.record_goal_lifetime();
        }
        result
    }

    async fn wait_result_inner(
        &self,
        timeout_duration: Option<Duration>,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        let status_key: String = self
            .client
            .inner
            .key_builder
            .action_status_key(&self.client.inner.action_name, &self.goal_id)?
            .into();
        let deadline = timeout_duration.map(|duration| Instant::now() + duration);
        let uses_persisted_epoch = self.server_epoch.is_empty();

        if uses_persisted_epoch {
            let initial_result_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
            let initial_result = self
                .client
                .get_result_with_timeouts(
                    &self.goal_id,
                    &self.goal_secret,
                    initial_result_timeout,
                    initial_result_timeout,
                )
                .await?;
            if initial_result.status.is_terminal() {
                return Ok(initial_result);
            }
        }

        let initial_query_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
        if let Some(status) = self
            .client
            .query_status_with_timeouts(&self.goal_id, initial_query_timeout, initial_query_timeout)
            .await?
        {
            if server_epoch_mismatch(&self.server_epoch, &status.server_epoch) {
                return Err(AortaError::not_found(format!(
                    "action '{}' goal '{}' belongs to previous server epoch '{}', current status epoch is '{}'",
                    self.client.inner.action_name, self.goal_id, self.server_epoch, status.server_epoch
                )));
            }
            if status.status.is_terminal() {
                let result_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
                return self
                    .client
                    .get_result_with_timeouts(
                        &self.goal_id,
                        &self.goal_secret,
                        None,
                        result_timeout,
                    )
                    .await;
            }
        }

        let subscriber = self
            .client
            .inner
            .session
            .inner()
            .declare_subscriber(status_key)
            .await?;

        // Watch the server's alive token (declared per (action, node)
        // by the action server) so a server dying mid-goal surfaces as an
        // error instead of an infinite wait — and a bounded wait fails fast
        // instead of burning its whole budget. Declared BEFORE the seed query
        // so no PUT/DELETE can fall between them. Any failure here only
        // disables death detection (legacy behavior), never the wait itself.
        let liveliness_watch = match self
            .client
            .inner
            .key_builder
            .action_owner_query(&self.client.inner.action_name)
        {
            Ok(pattern) => {
                match self
                    .client
                    .inner
                    .session
                    .inner()
                    .liveliness()
                    .declare_subscriber(pattern.clone())
                    .await
                {
                    Ok(sub) => Some((sub, pattern)),
                    Err(err) => {
                        aorta_core_warn!(
                            "Aorta WARN: action '{}' liveliness watch failed ({}); \
                             wait_result server-death detection disabled",
                            self.client.inner.action_name,
                            err
                        );
                        None
                    }
                }
            }
            Err(err) => {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' alive-token pattern invalid ({}); \
                     wait_result server-death detection disabled",
                    self.client.inner.action_name,
                    err
                );
                None
            }
        };
        let result = async {
            let mut alive: HashSet<String> = HashSet::new();
            // A seed that ran with its full window and succeeded is the only
            // one whose EMPTY result may be trusted as "no server alive"; a
            // deadline-truncated or failed seed is inconclusive (a bounded
            // wait_result must not exceed its budget here,
            // and a too-short window returning empty proves nothing).
            let mut alive_seed_conclusive = false;
            if let Some((_, pattern)) = &liveliness_watch {
                let remaining = remaining_wait_result_timeout(deadline, &self.goal_id)?;
                let (seed_timeout, seed_window_full) = alive_seed_window(remaining);
                match crate::liveliness::liveliness_get(
                    self.client.inner.session.inner(),
                    pattern,
                    seed_timeout,
                )
                .await
                {
                    Ok(keys) => {
                        alive.extend(keys);
                        alive_seed_conclusive = seed_window_full;
                    }
                    Err(err) => {
                        aorta_core_warn!(
                            "Aorta WARN: action '{}' alive-token seed query failed ({}); \
                             continuing without initial liveliness state",
                            self.client.inner.action_name,
                            err
                        );
                    }
                }
            }

            let recheck_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
            let mut recheck_found = false;
            if let Some(status) = self
                .client
                .query_status_with_timeouts(
                    &self.goal_id,
                    recheck_timeout,
                    recheck_timeout,
                )
                .await?
            {
                recheck_found = true;
                if server_epoch_mismatch(&self.server_epoch, &status.server_epoch) {
                    return Err(AortaError::not_found(format!(
                        "action '{}' goal '{}' belongs to previous server epoch '{}', current status epoch is '{}'",
                        self.client.inner.action_name, self.goal_id, self.server_epoch, status.server_epoch
                    )));
                }
                if status.status.is_terminal() {
                    let result_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
                    return self
                        .client
                        .get_result_with_timeouts(
                            &self.goal_id,
                            &self.goal_secret,
                            None,
                            result_timeout,
                        )
                        .await;
                }
            }

            // Server died before this wait started: no alive token AND no
            // status reply. (A live pre-liveliness server still answers the
            // status query, so a mixed-version fleet degrades to the legacy
            // wait instead of erroring here. Only a full-window, successful
            // seed can prove the token absence.)
            if alive_seed_conclusive && alive.is_empty() && !recheck_found {
                return Err(AortaError::not_found(format!(
                    "action '{}' server is not alive (no liveliness token and no status reply); \
                     goal '{}' cannot complete",
                    self.client.inner.action_name, self.goal_id
                )));
            }

            let deadline_sleep = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(deadline_sleep);
            let mut watch_open = liveliness_watch.is_some();

            loop {
                tokio::select! {
                    received = subscriber.recv_async() => {
                        let sample = received.map_err(|_| {
                            AortaError::not_found(format!(
                                "action '{}' wait_result subscriber closed for goal '{}'",
                                self.client.inner.action_name, self.goal_id
                            ))
                        })?;

                        let fields: zenoh::sample::SampleFields = sample.into();
                        if fields.payload.is_empty() {
                            return Err(AortaError::not_found(format!(
                                "action '{}' status for goal '{}' was removed before result retrieval",
                                self.client.inner.action_name, self.goal_id
                            )));
                        }
                        let status = TAction::decode_public_status(fields.payload.to_bytes().as_ref())?;
                        if server_epoch_mismatch(&self.server_epoch, &status.server_epoch) {
                            return Err(AortaError::not_found(format!(
                                "action '{}' goal '{}' belongs to previous server epoch '{}', current status epoch is '{}'",
                                self.client.inner.action_name, self.goal_id, self.server_epoch, status.server_epoch
                            )));
                        }
                        if status.status.is_terminal() {
                            let result_timeout = remaining_wait_result_timeout(deadline, &self.goal_id)?;
                            return self
                                .client
                                .get_result_with_timeouts(
                                    &self.goal_id,
                                    &self.goal_secret,
                                    None,
                                    result_timeout,
                                )
                                .await;
                        }
                    }
                    event = async {
                        match &liveliness_watch {
                            Some((sub, _)) => sub.recv_async().await.map_err(|_| ()),
                            None => std::future::pending().await,
                        }
                    }, if watch_open => {
                        match event {
                            Ok(sample) => {
                                let key = sample.key_expr().as_str().to_string();
                                match sample.kind() {
                                    zenoh::sample::SampleKind::Put => {
                                        alive.insert(key);
                                    }
                                    zenoh::sample::SampleKind::Delete => {
                                        alive.remove(&key);
                                        if alive.is_empty() {
                                            return Err(AortaError::not_found(format!(
                                                "action '{}' server is no longer alive (liveliness lost); \
                                                 goal '{}' cannot complete",
                                                self.client.inner.action_name, self.goal_id
                                            )));
                                        }
                                    }
                                }
                            }
                            Err(()) => {
                                // Watch channel closed — degrade to the legacy
                                // (liveliness-free) wait.
                                watch_open = false;
                            }
                        }
                    }
                    () = &mut deadline_sleep => {
                        return Err(AortaError::timeout(format!(
                            "timed out waiting for terminal action status '{}'",
                            self.goal_id
                        )));
                    }
                }
            }
        }
        .await;

        let _ = subscriber.undeclare().await;
        if let Some((sub, _)) = liveliness_watch {
            let _ = sub.undeclare().await;
        }
        result
    }
}

impl<TAction: ActionWire + Send + Sync + 'static> GoalHandle<TAction> {
    /// Tie the goal's SERVER-side lifetime to a value: the returned guard
    /// fires a detached best-effort cancel when dropped un-disarmed. Opt-in
    /// sugar — scope cancellation alone stops only the LOCAL wait; the goal
    /// keeps running server-side unless someone cancels it. See
    /// [`GoalGuard`].
    pub fn into_guard(self) -> GoalGuard<TAction> {
        GoalGuard { handle: Some(self) }
    }
}

/// RAII cancel-on-drop for an accepted goal (Flow sugar).
///
/// Dropping the guard spawns a DETACHED fire-and-forget cancel on the
/// current tokio runtime (failures are logged, never panicked). This is
/// deliberately opt-in: auto-cancel is a policy choice — plenty of flows
/// hand a running goal over to another supervisor instead. Call
/// [`disarm`](Self::disarm) to release the goal without cancelling, or
/// [`cancel`](Self::cancel) for an awaited, checked cancel.
///
/// Dropped OUTSIDE a tokio runtime the guard cannot spawn: it logs a WARN
/// and skips the cancel (prefer dropping guards inside the runtime, e.g.
/// from scoped tasks).
pub struct GoalGuard<TAction: ActionWire>
where
    TAction: Send + Sync + 'static,
{
    handle: Option<GoalHandle<TAction>>,
}

impl<TAction: ActionWire + Send + Sync + 'static> GoalGuard<TAction> {
    /// Borrow the guarded handle (feedback/result composition).
    pub fn handle(&self) -> &GoalHandle<TAction> {
        self.handle
            .as_ref()
            .expect("GoalGuard invariant: handle present until disarm/drop")
    }

    /// Release the goal WITHOUT cancelling (hand-over shape).
    pub fn disarm(mut self) -> GoalHandle<TAction> {
        self.handle
            .take()
            .expect("GoalGuard invariant: handle present until disarm/drop")
    }

    /// Awaited, checked cancel (consumes the guard).
    pub async fn cancel(mut self) -> AortaResult<ActionCancelGoalResponse> {
        let handle = self
            .handle
            .take()
            .expect("GoalGuard invariant: handle present until disarm/drop");
        handle.cancel().await
    }
}

impl<TAction: ActionWire + Send + Sync + 'static> Drop for GoalGuard<TAction>
where
    TAction: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    if let Err(err) = handle.cancel().await {
                        crate::aorta_core_warn!(
                            "Aorta WARN: GoalGuard drop-cancel failed for goal '{}': {err}",
                            handle.goal_id()
                        );
                    }
                });
            }
            Err(_) => {
                crate::aorta_core_warn!(
                    "Aorta WARN: GoalGuard for goal '{}' dropped outside a tokio \
                     runtime — cancel skipped (disarm() or cancel().await instead)",
                    handle.goal_id()
                );
            }
        }
    }
}

/// Cap the wait_result liveliness seed query by the caller's remaining wait
/// budget: a bounded `wait_result` must never exceed its
/// timeout inside the seed. Returns `(timeout, window_full)` — when the
/// window had to be truncated below [`ACTION_ALIVE_SEED_TIMEOUT`], an empty
/// seed result is inconclusive and must not trigger the immediate
/// server-not-alive error.
fn alive_seed_window(remaining: Option<Duration>) -> (Duration, bool) {
    match remaining {
        Some(remaining) if remaining < ACTION_ALIVE_SEED_TIMEOUT => (remaining, false),
        _ => (ACTION_ALIVE_SEED_TIMEOUT, true),
    }
}

#[cfg(test)]
mod alive_seed_window_tests {
    use super::{alive_seed_window, ACTION_ALIVE_SEED_TIMEOUT};
    use std::time::Duration;

    #[test]
    fn unbounded_wait_gets_full_conclusive_window() {
        assert_eq!(alive_seed_window(None), (ACTION_ALIVE_SEED_TIMEOUT, true));
    }

    #[test]
    fn large_budget_gets_full_conclusive_window() {
        assert_eq!(
            alive_seed_window(Some(Duration::from_secs(60))),
            (ACTION_ALIVE_SEED_TIMEOUT, true)
        );
    }

    #[test]
    fn small_budget_is_capped_and_inconclusive() {
        // wait_result(Some(100ms)) must not block ~2s in the
        // liveliness seed, and its (possibly empty) result must not be
        // treated as proof the server is gone.
        let budget = Duration::from_millis(100);
        assert_eq!(alive_seed_window(Some(budget)), (budget, false));
    }

    #[test]
    fn exact_budget_boundary_is_full_window() {
        assert_eq!(
            alive_seed_window(Some(ACTION_ALIVE_SEED_TIMEOUT)),
            (ACTION_ALIVE_SEED_TIMEOUT, true)
        );
    }
}

fn map_send_goal_rejection(action_name: &str, response: &ActionSendGoalResponse) -> AortaError {
    let detail = format!(
        "action '{}' send_goal rejected: code={:?}, status={:?}, message={}",
        action_name, response.code, response.status, response.message
    );
    match response.code {
        ActionReplyCode::Busy => AortaError::busy(detail),
        ActionReplyCode::Conflict => AortaError::concurrent_conflict(detail),
        ActionReplyCode::GoalExpired => AortaError::not_found(detail),
        _ => AortaError::invalid_argument(detail),
    }
}

fn map_cancel_rejection(action_name: &str, response: &ActionCancelGoalResponse) -> AortaError {
    let detail = format!(
        "action '{}' cancel rejected: code={:?}, status={:?}, message={}",
        action_name, response.code, response.status, response.message
    );
    match response.code {
        ActionReplyCode::NotFound => AortaError::not_found(detail),
        ActionReplyCode::Unauthorized | ActionReplyCode::AlreadyTerminal => {
            AortaError::invalid_argument(detail)
        }
        _ => AortaError::invalid_argument(detail),
    }
}

fn validate_get_result_response<TFeedbackData, TResult>(
    action_name: &str,
    goal_id: &str,
    response: ActionGetResultResponse<TFeedbackData, TResult>,
) -> AortaResult<ActionGetResultResponse<TFeedbackData, TResult>> {
    if response.goal_id != goal_id {
        return Err(AortaError::internal(format!(
            "action '{}' get_result reply goal_id mismatch: expected '{}', got '{}'",
            action_name, goal_id, response.goal_id
        )));
    }
    match response.code {
        ActionReplyCode::Unauthorized => Err(AortaError::invalid_argument(format!(
            "action '{}' get_result rejected: code={:?}, status={:?}, message={}",
            action_name, response.code, response.status, response.message
        ))),
        ActionReplyCode::NotFound | ActionReplyCode::GoalExpired => {
            Err(AortaError::not_found(format!(
                "action '{}' get_result rejected: code={:?}, status={:?}, message={}",
                action_name, response.code, response.status, response.message
            )))
        }
        _ => Ok(response),
    }
}

fn remaining_wait_result_timeout(
    deadline: Option<Instant>,
    goal_id: &str,
) -> AortaResult<Option<Duration>> {
    match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AortaError::timeout(format!(
                    "timed out waiting for terminal action status '{}'",
                    goal_id
                )));
            }
            Ok(Some(remaining))
        }
        None => Ok(None),
    }
}

/// Zenoh `.get().timeout()` value for the action dual-guard.
///
/// When an application-side `recv_timeout` tokio guard exists it must own the
/// caller result, so the Zenoh router timeout is set to OUTLAST it by
/// [`ZENOH_QUERY_TIMEOUT_GRACE`](crate::client::ZENOH_QUERY_TIMEOUT_GRACE). This
/// closes two gaps:
///   * **Missing router bound** — `recv_timeout=Some` with `zenoh_timeout=None`
///     (the `wait_result` terminal `get_result`) would otherwise default
///     `ext_timeout` to `queries_default_timeout` (600 s on robots) and leak a
///     pending query on every hop once the tokio guard fires and drops
///     `replies`; dropping the receiver does NOT cancel the query in the pinned
///     Zenoh fork (`cancel_query` is unstable, local-only, never notifies the
///     router). Bounding it here is the resource-leak half of the dual-guard.
///   * **Equal-timer race** — keeping the Zenoh timer strictly longer than the
///     tokio guard makes the app guard fire first, so the router's synthesized
///     `Err("Timeout")` reply normally never wins (`reply_error_is_zenoh_query_timeout`
///     remains the backstop if jitter lets it win).
///
/// With no app-side guard (`recv_timeout=None`) the explicit `zenoh_timeout` is
/// honored as-is: there is no race, and `None` keeps a genuinely unbounded
/// `wait_result(None)` query open until the reply arrives.
fn effective_zenoh_timeout(
    zenoh_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
) -> Option<Duration> {
    match recv_timeout {
        Some(recv) => Some(recv + crate::client::ZENOH_QUERY_TIMEOUT_GRACE),
        None => zenoh_timeout,
    }
}

fn server_epoch_mismatch(expected: &str, observed: &str) -> bool {
    !expected.is_empty() && expected != observed
}

/// Bounded SPSC feedback queue with a configurable overflow policy.
///
/// Backs [`ActionFeedbackStream`]. The previous
/// implementation used `mpsc::unbounded_channel`, which let a slow or
/// stalled consumer balloon memory under high-frequency feedback.
///
/// Hand-rolled instead of layered on `tokio::sync::mpsc` because:
/// - tokio mpsc has no built-in drop-oldest semantics; the producer can't
///   reach the receiver to evict.
/// - tokio broadcast supports drop-oldest but requires `T: Clone`, which
///   forces every error / payload type through `Clone` for no other reason.
///
/// Producer (Zenoh sample callback) runs on the session worker thread and
/// is synchronous; consumer is async. The send path takes the queue lock
/// briefly and never blocks. The recv path waits on a `Notify`.
mod bounded_feedback {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::sync::Notify;

    use super::FeedbackOverflowPolicy;

    pub fn channel<T>(capacity: usize, policy: FeedbackOverflowPolicy) -> (Sender<T>, Receiver<T>) {
        // Capacity == 0 would be a footgun (every send drops). Clamp to 1
        // so the user gets at-least-one-buffered semantics no matter what
        // they pass.
        let capacity = capacity.max(1);
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
        });
        (
            Sender {
                inner: inner.clone(),
                policy,
            },
            Receiver { inner },
        )
    }

    struct Inner<T> {
        queue: Mutex<VecDeque<T>>,
        capacity: usize,
        notify: Notify,
        closed: AtomicBool,
        dropped: AtomicU64,
    }

    /// Type-erased close entry point so a [`Closer`] can be stored without
    /// the channel's item type (the per-goal registry holds closers for
    /// streams of one action, but erasure keeps it independent of `T`).
    trait CloseSignal: Send + Sync {
        fn close_now(&self);
    }

    impl<T: Send> CloseSignal for Inner<T> {
        fn close_now(&self) {
            // Same protocol as Sender/Receiver drop: mark closed, then wake
            // every parked receiver so it observes the flag. `recv` drains
            // the queue before honoring `closed`, so buffered samples are
            // still delivered (drain-then-None).
            self.closed.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    /// External close handle for a channel (end-of-stream on
    /// terminal). Holds only a `Weak`, so a registry of closers never keeps
    /// a dropped stream's queue alive.
    pub struct Closer(std::sync::Weak<dyn CloseSignal>);

    impl Closer {
        /// Close the channel if it is still alive: further `send`s are
        /// refused (counted as dropped) and `recv` returns `None` once the
        /// buffered items are drained. Idempotent.
        pub fn close(&self) {
            if let Some(inner) = self.0.upgrade() {
                inner.close_now();
            }
        }

        /// True once every channel endpoint is gone — the registry uses this
        /// to prune dead entries.
        pub fn is_dead(&self) -> bool {
            self.0.strong_count() == 0
        }
    }

    impl<T: Send + 'static> Sender<T> {
        pub fn closer(&self) -> Closer {
            let erased: Arc<dyn CloseSignal> = self.inner.clone();
            Closer(Arc::downgrade(&erased))
        }
    }

    pub struct Sender<T> {
        inner: Arc<Inner<T>>,
        policy: FeedbackOverflowPolicy,
    }

    pub struct Receiver<T> {
        inner: Arc<Inner<T>>,
    }

    impl<T> Sender<T> {
        /// Push `item`. On overflow applies the configured policy; on a
        /// dropped receiver silently no-ops (the producer can't recover
        /// here — the Zenoh callback doesn't propagate errors). Returns
        /// `true` iff `item` ended up in the queue.
        pub fn send(&self, item: T) -> bool {
            if self.inner.closed.load(Ordering::Acquire) {
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            let stored = {
                let mut queue = self.inner.queue.lock();
                if queue.len() >= self.inner.capacity {
                    match self.policy {
                        FeedbackOverflowPolicy::DropOldest => {
                            queue.pop_front();
                            queue.push_back(item);
                            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                            true
                        }
                        FeedbackOverflowPolicy::DropNew => {
                            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                            false
                        }
                    }
                } else {
                    queue.push_back(item);
                    true
                }
            };
            // notify_one even on DropNew so a parked receiver still wakes
            // and observes the previously-buffered tail; lost wakeups
            // would leave it parked indefinitely.
            self.inner.notify.notify_one();
            stored
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.inner.closed.store(true, Ordering::Release);
            self.inner.notify.notify_waiters();
        }
    }

    impl<T> Receiver<T> {
        pub async fn recv(&mut self) -> Option<T> {
            loop {
                // Register the notification BEFORE checking the queue so a
                // sender that pushes between the pop and the await still
                // delivers a permit (Notify::notified holds permits across
                // notify_one calls only if registered first).
                let notified = self.inner.notify.notified();
                {
                    let mut queue = self.inner.queue.lock();
                    if let Some(item) = queue.pop_front() {
                        return Some(item);
                    }
                    if self.inner.closed.load(Ordering::Acquire) {
                        return None;
                    }
                }
                notified.await;
            }
        }

        pub fn dropped_count(&self) -> u64 {
            self.inner.dropped.load(Ordering::Relaxed)
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            // Closes from the consumer side too so that any future Sender
            // call short-circuits via the closed flag instead of wasting a
            // lock + push.
            self.inner.closed.store(true, Ordering::Release);
            self.inner.notify.notify_waiters();
        }
    }

    // Test-only accessor so `send_after_receiver_drop_is_silent_noop`
    // can read the counter without holding a Receiver.
    #[cfg(test)]
    impl<T> Sender<T> {
        pub fn dropped_count_for_test(&self) -> u64 {
            self.inner.dropped.load(Ordering::Relaxed)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn closer_drains_buffered_then_returns_none() {
            let (tx, mut rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            assert!(tx.send(1));
            assert!(tx.send(2));
            let closer = tx.closer();
            closer.close();
            // Drain-then-None: buffered items survive the close.
            assert_eq!(rx.recv().await, Some(1));
            assert_eq!(rx.recv().await, Some(2));
            assert_eq!(rx.recv().await, None);
            // Post-close sends are refused and counted.
            assert!(!tx.send(3));
            assert_eq!(rx.dropped_count(), 1);
        }

        #[tokio::test]
        async fn closer_wakes_parked_receiver() {
            let (tx, mut rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            let closer = tx.closer();
            let recv_task = tokio::spawn(async move { rx.recv().await });
            tokio::task::yield_now().await;
            closer.close();
            let received = tokio::time::timeout(std::time::Duration::from_secs(2), recv_task)
                .await
                .expect("closer must wake a parked receiver")
                .expect("recv task must not panic");
            assert_eq!(received, None);
        }

        #[tokio::test]
        async fn closer_is_dead_after_both_endpoints_drop() {
            let (tx, rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            let closer = tx.closer();
            assert!(!closer.is_dead());
            drop(tx);
            drop(rx);
            assert!(closer.is_dead());
            closer.close(); // must be a safe no-op
        }

        #[tokio::test]
        async fn send_recv_under_capacity_preserves_order() {
            let (tx, mut rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            for i in 0..3 {
                assert!(tx.send(i));
            }
            for expected in 0..3 {
                assert_eq!(rx.recv().await, Some(expected));
            }
            assert_eq!(rx.dropped_count(), 0);
        }

        #[tokio::test]
        async fn drop_oldest_keeps_newest_n_items() {
            let (tx, mut rx) = channel::<u32>(3, FeedbackOverflowPolicy::DropOldest);
            for i in 0..5 {
                let stored = tx.send(i);
                // Once the queue fills, every subsequent send still stores
                // the new item (it just evicted one).
                assert!(stored, "DropOldest must always store the new item");
            }
            // Capacity 3, sent 0..=4 → queue holds 2,3,4
            assert_eq!(rx.recv().await, Some(2));
            assert_eq!(rx.recv().await, Some(3));
            assert_eq!(rx.recv().await, Some(4));
            assert_eq!(rx.dropped_count(), 2);
        }

        #[tokio::test]
        async fn drop_new_keeps_oldest_n_items() {
            let (tx, mut rx) = channel::<u32>(3, FeedbackOverflowPolicy::DropNew);
            let mut stored_count = 0u32;
            for i in 0..5 {
                if tx.send(i) {
                    stored_count += 1;
                }
            }
            assert_eq!(stored_count, 3);
            // Queue holds 0,1,2; later items rejected
            assert_eq!(rx.recv().await, Some(0));
            assert_eq!(rx.recv().await, Some(1));
            assert_eq!(rx.recv().await, Some(2));
            assert_eq!(rx.dropped_count(), 2);
        }

        #[tokio::test]
        async fn recv_returns_none_after_sender_drop() {
            let (tx, mut rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            tx.send(7);
            drop(tx);
            // Buffered items still drain first.
            assert_eq!(rx.recv().await, Some(7));
            assert_eq!(rx.recv().await, None);
        }

        #[tokio::test]
        async fn recv_wakes_on_send() {
            let (tx, mut rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            let send_task = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                tx.send(42);
            });
            assert_eq!(rx.recv().await, Some(42));
            send_task.await.unwrap();
        }

        #[tokio::test]
        async fn capacity_zero_is_clamped_to_one() {
            let (tx, mut rx) = channel::<u32>(0, FeedbackOverflowPolicy::DropOldest);
            assert!(tx.send(1));
            assert!(tx.send(2)); // evicts 1
            assert_eq!(rx.recv().await, Some(2));
            assert_eq!(rx.dropped_count(), 1);
        }

        #[tokio::test]
        async fn send_after_receiver_drop_is_silent_noop() {
            let (tx, rx) = channel::<u32>(4, FeedbackOverflowPolicy::DropOldest);
            drop(rx);
            // Must not panic; must not store (channel is closed).
            assert!(!tx.send(1));
            assert!(tx.dropped_count_for_test() >= 1);
        }
    }
}

#[cfg(test)]
mod jitter_tests {
    use super::*;

    // Jitter must spread the polling interval across
    // [base - jitter_pct%, base + jitter_pct%] in a way that
    // varies across iterations. The factored `jitter_with(base,
    // pct, entropy)` lets us drive entropy deterministically so
    // the test doesn't depend on the host clock.

    #[test]
    fn jitter_with_zero_pct_returns_base_unchanged() {
        let base = Duration::from_millis(25);
        for entropy in [0u64, 1, 1234, u64::MAX] {
            assert_eq!(jitter_with(base, 0, entropy), base);
        }
    }

    #[test]
    fn jitter_with_returns_value_within_window() {
        let base = Duration::from_millis(100);
        let span_ms = (100 * 25) / 100; // ±25 ms window
        let lo = base - Duration::from_millis(span_ms);
        let hi = base + Duration::from_millis(span_ms);
        // Sample 256 entropy values; every result must land inside
        // the documented window.
        for entropy in 0u64..256 {
            let got = jitter_with(base, 25, entropy);
            assert!(
                got >= lo && got <= hi,
                "entropy={entropy} produced {got:?} outside [{lo:?}, {hi:?}]",
            );
        }
    }

    #[test]
    fn jitter_with_distinct_entropy_produces_distinct_intervals() {
        // The whole point of jitter is that two concurrent retry
        // loops on different threads (different entropy seeds)
        // don't sleep for identical durations. Pick two seeds far
        // enough apart that the modulo can't collapse them.
        let base = Duration::from_millis(25);
        let a = jitter_with(base, 25, 0);
        let b = jitter_with(base, 25, 5);
        assert_ne!(
            a, b,
            "entropy 0 and 5 produced identical jitter — modulo collision should not happen at this scale",
        );
    }

    #[test]
    fn jittered_retry_poll_stays_within_window_against_wall_clock() {
        // Live-clock smoke test: the function must not panic and
        // must always land in the documented window even when its
        // entropy comes from the system clock.
        let base = Duration::from_millis(25);
        let span = Duration::from_millis((25 * ACTION_SEND_GOAL_RETRY_JITTER_PCT as u64) / 100);
        for _ in 0..64 {
            let got = jittered_retry_poll(base);
            assert!(got >= base - span && got <= base + span);
        }
    }
}

#[cfg(test)]
mod timeout_policy_tests {
    use super::*;
    use crate::client::ZENOH_QUERY_TIMEOUT_GRACE;

    // Dual-guard policy: whenever an application-side recv guard exists,
    // the Zenoh router timeout must OUTLAST it by exactly the grace — so the app
    // guard owns the caller result and the router still reaps the pending query
    // promptly (no 600s leak).
    #[test]
    fn recv_guard_present_sets_zenoh_timeout_to_recv_plus_grace() {
        let recv = Duration::from_secs(5);
        for zenoh_hint in [
            None,
            Some(Duration::from_secs(1)),
            Some(Duration::from_secs(99)),
        ] {
            assert_eq!(
                effective_zenoh_timeout(zenoh_hint, Some(recv)),
                Some(recv + ZENOH_QUERY_TIMEOUT_GRACE),
                "recv guard must dominate (zenoh_hint={zenoh_hint:?})"
            );
        }
    }

    // The wait_result terminal get_result passes zenoh_timeout=None with a recv
    // deadline; before the fix this defaulted ext_timeout to the 600s session
    // default and leaked. It must now be bounded at recv + grace.
    #[test]
    fn wait_result_terminal_none_zenoh_timeout_is_bounded_not_leaked() {
        let recv = Duration::from_secs(3);
        assert_eq!(
            effective_zenoh_timeout(None, Some(recv)),
            Some(recv + ZENOH_QUERY_TIMEOUT_GRACE),
        );
    }

    // No app-side guard: honor the explicit zenoh_timeout as-is (no race to
    // avoid), and `None` keeps a genuinely unbounded wait_result(None) open.
    #[test]
    fn no_recv_guard_honors_explicit_zenoh_timeout() {
        assert_eq!(effective_zenoh_timeout(None, None), None);
        let z = Duration::from_secs(2);
        assert_eq!(effective_zenoh_timeout(Some(z), None), Some(z));
    }
}
