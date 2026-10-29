use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Mutex as TokioMutex, Notify, RwLock as TokioRwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionInfo, ActionPublicStatus,
    ActionReplyCode, ActionSendGoalResponse, ActionStatus, ActionWire, DEFAULT_ACTION_DEDUP_TTL_MS,
    DEFAULT_ACTION_RESULT_TTL_MS,
};
use crate::error::{AortaError, AortaResult};
use crate::key_expr::{normalize_path, KeyExprBuilder};
use crate::log_prelude::*;
use crate::node::Node;
use crate::schema::SchemaHash;
use crate::session::Session;
use crate::snapshot_queryable::{
    spawn_single_key_snapshot_queryable, spawn_snapshot_queryable_with_override,
};
use crate::time::unix_time_ns;
use crate::transport::{
    reply_error_is_absent_snapshot, reply_error_is_zenoh_query_timeout, reply_error_text,
};
use crate::ZBytes;

/// Fetch the action BFBS, validating the advertised content hash.
///
/// Two sources, in order:
///
/// 1. `act/info/<name>?schema=bfbs` — the hash-free path. It is the only one
///    that works where the global `aorta/sys/schema/**` namespace is denied
///    (the EDU ACL).
/// 2. `aorta/sys/schema/<hash>` — the pre-PR path, kept as a fallback so a
///    current client keeps working against a pre-PR action server. Such a
///    server ignores query parameters and answers `?schema=bfbs` with its
///    ActionInfo payload, so step 1 finds no hash match there. Without this
///    fallback every `aorta action *` subcommand, `aorta-introspect` and
///    `aorta-tui` would break for the whole of a staged rollout.
///
/// Both sources are content-validated against `info.schema_hash`, so neither
/// can inject a schema the action server did not advertise.
pub async fn fetch_action_schema_bfbs(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    info: &ActionInfo,
    timeout_duration: Duration,
) -> AortaResult<Vec<u8>> {
    let key = key_builder.action_info_key(&info.action_name)?;
    let replies = session
        .get(format!(
            "{key}?{ACTION_SCHEMA_QUERY_PARAM}={ACTION_SCHEMA_QUERY_VALUE}"
        ))
        .timeout(timeout_duration)
        .await?;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let bytes = sample.payload().to_bytes();
            if SchemaHash::from_bfbs_bytes(&bytes) == info.schema_hash {
                return Ok(bytes.to_vec());
            }
        }
    }
    crate::schema_resolve::fetch_bfbs_by_hash(session, &info.schema_hash, timeout_duration)
        .await
        .map_err(|err| {
            AortaError::not_found(format!(
                "matching action schema not found on {key} and not resolvable by hash {}: {err}",
                info.schema_hash.to_hex()
            ))
        })
}

/// Query parameter that makes the action info key serve the action BFBS
/// instead of ActionInfo. Written by [`fetch_action_schema_bfbs`], read by the
/// info queryable; keeping both on one constant pair stops the selector from
/// drifting between the two sides.
const ACTION_SCHEMA_QUERY_PARAM: &str = "schema";
const ACTION_SCHEMA_QUERY_VALUE: &str = "bfbs";

/// Serve ActionInfo on the action info key, plus the action BFBS to a
/// `?schema=bfbs` query.
///
/// This is the shared snapshot queryable with one parameter override, not a
/// second implementation: the stop / undeclare / absent-reply contract lives
/// in `snapshot_queryable` only.
pub(crate) async fn spawn_action_info_queryable(
    session: Session,
    key: String,
    payload: Arc<TokioRwLock<Option<ZBytes>>>,
    schema: ZBytes,
    stop_rx: oneshot::Receiver<()>,
) -> AortaResult<JoinHandle<()>> {
    spawn_snapshot_queryable_with_override(
        session,
        key,
        payload,
        Some((ACTION_SCHEMA_QUERY_PARAM, ACTION_SCHEMA_QUERY_VALUE, schema)),
        stop_rx,
    )
    .await
}

pub const DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_ACTION_CONTROL_TIMEOUT_MS: u64 = 10_000;
const ACTION_PROVIDER_CHECK_WINDOW_MS: u64 = 25;
/// How long `ensure_single_action_provider` waits out a predecessor's teardown
/// before failing closed. Teardown is bounded by the queryable tasks noticing
/// their stop signal and undeclaring; seconds is generous, and the wait is only
/// ever paid when a teardown is actually in flight.
const ACTION_PROVIDER_TEARDOWN_WAIT_MS: u64 = 3_000;
const ACTION_PROVIDER_TEARDOWN_POLL_MS: u64 = 25;
const ACTION_SEND_GOAL_READY_TIMEOUT_MS: u64 = 1_000;
const ACTION_SEND_GOAL_READY_POLL_MS: u64 = 25;
const ACTION_DUPLICATE_MESSAGE: &str = "duplicate accepted goal_id";
const ACTION_BUSY_MESSAGE: &str = "action server reached max_concurrency";
const ACTION_GOAL_EXPIRED_MESSAGE: &str = "goal result expired; use a new goal_id";
const ACTION_HANDLER_WITHOUT_RESULT_MESSAGE: &str =
    "handler returned Ok without setting terminal state";
const ACTION_HANDLER_PANIC_MESSAGE: &str = "handler panicked";
const ACTION_CONTEXT_DROPPED_MESSAGE: &str =
    "action execution context dropped during server shutdown";

type BoxedActionFuture = Pin<Box<dyn Future<Output = AortaResult<()>> + Send + 'static>>;
type BoxedActionHandler<TAction> = Arc<
    dyn Fn(RetainedActionGoal<TAction>, ActionExecutionContext<TAction>) -> BoxedActionFuture
        + Send
        + Sync,
>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionServerOptions {
    pub supports_cancel: bool,
    pub max_concurrency: usize,
    pub result_ttl: Duration,
    pub dedup_ttl: Duration,
}

impl Default for ActionServerOptions {
    fn default() -> Self {
        Self {
            supports_cancel: true,
            max_concurrency: 1,
            result_ttl: Duration::from_millis(DEFAULT_ACTION_RESULT_TTL_MS),
            dedup_ttl: Duration::from_millis(DEFAULT_ACTION_DEDUP_TTL_MS),
        }
    }
}

impl ActionServerOptions {
    pub fn validate(&self) -> AortaResult<()> {
        if self.max_concurrency == 0 {
            return Err(AortaError::invalid_argument(
                "action max_concurrency must be greater than zero",
            ));
        }
        if self.dedup_ttl < self.result_ttl {
            return Err(AortaError::invalid_argument(format!(
                "action dedup_ttl ({:?}) must be >= result_ttl ({:?})",
                self.dedup_ttl, self.result_ttl
            )));
        }
        let _ = u32::try_from(self.max_concurrency).map_err(|_| {
            AortaError::invalid_argument(format!(
                "action max_concurrency {} exceeds u32::MAX",
                self.max_concurrency
            ))
        })?;
        let _ = duration_to_millis_u64(self.result_ttl)?;
        let _ = duration_to_millis_u64(self.dedup_ttl)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RetainedActionGoal<TAction: ActionWire + Send + Sync + 'static> {
    goal_id: String,
    request_payload: Vec<u8>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire + Send + Sync + 'static> RetainedActionGoal<TAction> {
    fn new(goal_id: String, request_payload: Vec<u8>) -> Self {
        Self {
            goal_id,
            request_payload,
            _marker: PhantomData,
        }
    }

    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }

    pub fn payload(&self) -> &[u8] {
        &self.request_payload
    }

    pub fn decode(
        &self,
    ) -> AortaResult<<<TAction as crate::action::ActionSchema>::Goal as crate::schema::TypedSubscribeMessage>::Root<'_>>
    where
        <TAction as crate::action::ActionSchema>::Goal: crate::schema::TypedSubscribeMessage,
    {
        TAction::decode_goal_from_send_goal_request(&self.request_payload)
    }
}

pub struct ActionExecutionContext<TAction: ActionWire + Send + Sync + 'static> {
    server: Arc<ActionServerInner<TAction>>,
    goal: Arc<GoalRecord<TAction>>,
}

impl<TAction: ActionWire + Send + Sync + 'static> Clone for ActionExecutionContext<TAction> {
    fn clone(&self) -> Self {
        Self {
            server: Arc::clone(&self.server),
            goal: Arc::clone(&self.goal),
        }
    }
}

impl<TAction: ActionWire + Send + Sync + 'static> ActionExecutionContext<TAction> {
    pub fn goal_id(&self) -> &str {
        &self.goal.goal_id
    }

    /// The traceparent the CALLER propagated on the send_goal request, if any.
    /// The built-in goal lifecycle stage spans join it automatically;
    /// an execute callback that wants its own spans on the same trace can
    /// adopt it around synchronous sections via
    /// [`crate::trace::adopt_remote_context`].
    pub fn inbound_trace(&self) -> Option<crate::trace::TraceParent> {
        self.goal.inbound_trace
    }

    pub async fn publish_feedback(&self, data: &TAction::FeedbackDataMsg<'_>) -> AortaResult<()> {
        self.publish_feedback_with_message(data, None).await
    }

    pub async fn publish_feedback_with_message(
        &self,
        data: &TAction::FeedbackDataMsg<'_>,
        message: Option<&str>,
    ) -> AortaResult<()> {
        let feedback_snapshot = TAction::clone_feedback_data_owned(data)?;
        let (seq, status, feedback_interval_ns) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                return Ok(());
            }
            state.last_feedback_seq = state.last_feedback_seq.saturating_add(1);
            state.feedback_snapshot = feedback_snapshot;
            // Tracing: feedback interval (monotonic, gated). Read the clock only
            // when enabled; record the delta from the previous feedback.
            let interval = if crate::trace::trace_enabled() {
                let now = crate::time::monotonic_ns();
                let prev = state.last_feedback_mono_ns;
                state.last_feedback_mono_ns = now;
                (prev != 0).then(|| now.saturating_sub(prev))
            } else {
                // Reset the baseline while disabled so that, on re-enable, the next
                // feedback re-establishes the interval (and does not report a bogus
                // interval spanning the disabled window).
                state.last_feedback_mono_ns = 0;
                None
            };
            (state.last_feedback_seq, state.status, interval)
        };
        if let Some(interval) = feedback_interval_ns {
            crate::trace::record_stage(crate::trace::StageId::ActionFeedbackInterval, interval);
        }

        // Feedback trace propagation: one small slice per
        // feedback publish under the goal root, with the span id PRE-MINTED
        // so the feedback attachment carries it as the receive-marker's
        // parent — pub → recv renders as the per-message transit hop.
        let fb_link = match (self.goal.goal_root.as_ref(), crate::trace::trace_enabled()) {
            (Some(root), true) => Some(crate::trace::SpanLink {
                trace_id: root.trace_id,
                span_id: crate::trace::mint_span_id(),
                parent_span_id: root.span_id,
            }),
            _ => None,
        };
        let fb_start = fb_link.is_some().then(crate::time::monotonic_ns);
        let fb_attachment = fb_link.and_then(|l| {
            crate::attachment::encode(&crate::attachment::AttachmentParts {
                schema_hash: None,
                traceparent: Some(crate::trace::TraceParent {
                    trace_id: l.trace_id,
                    span_id: l.span_id,
                    flags: crate::trace::TRACEPARENT_SAMPLED,
                }),
                reply_busy_ns: None,
                deadline_budget_ns: None,
            })
        });
        let stamp_ns = unix_time_ns();
        let feedback_payload =
            TAction::encode_feedback(&self.goal.goal_id, seq, stamp_ns, status, data, message)?;
        self.server
            .publish_feedback_payload(feedback_payload, fb_attachment)
            .await?;
        let result = self.goal.publish_status(&self.server).await;
        if let (Some(link), Some(start)) = (fb_link, fb_start) {
            crate::trace::emit_stage_span_at(
                self.server.ev_feedback_pub,
                start,
                crate::time::monotonic_ns(),
                link,
            );
        }
        result
    }

    pub async fn succeed(&self, result: &TAction::ResultMsg<'_>) -> AortaResult<()> {
        let result_payload = TAction::clone_result_owned(result)?;
        self.finish_terminal(
            ActionStatus::Succeeded,
            ActionReplyCode::Succeeded,
            "",
            Some(result_payload),
        )
        .await
    }

    pub async fn abort(&self, code: ActionReplyCode, message: &str) -> AortaResult<()> {
        self.finish_terminal(ActionStatus::Aborted, code, message, None)
            .await
    }

    /// An action IS a Scope — the goal's scope, rooted at
    /// acceptance. `scope().is_cancelled()` mirrors [`Self::is_cancel_requested`];
    /// spawn goal sub-activities on it so a cancel request (or a handler
    /// fail-fast) reaches them structurally.
    pub fn scope(&self) -> &crate::flow::scope::ActionScope {
        &self.goal.scope
    }

    pub fn is_cancel_requested(&self) -> bool {
        // An action IS a Scope: `is_cancel_requested()` IS `scope.is_cancelled()`. The legacy
        // AtomicBool stays as the wire-facing signal; the scope token adds
        // the structural view (a handler-side `scope().cancel()` also counts).
        self.goal.cancel_requested.load(Ordering::Relaxed) || self.goal.scope.is_cancelled()
    }

    pub async fn cancelled(&self) {
        let scope_cancelled = self.goal.scope.cancelled();
        let legacy = async {
            loop {
                let notified = self.goal.cancel_notify.notified();
                if self.goal.cancel_requested.load(Ordering::Relaxed) {
                    return;
                }
                notified.await;
            }
        };
        tokio::select! {
            _ = scope_cancelled => {}
            _ = legacy => {}
        }
    }

    pub async fn cancel(&self, code: ActionReplyCode, message: &str) -> AortaResult<()> {
        self.finish_terminal(ActionStatus::Canceled, code, message, None)
            .await
    }

    async fn mark_executing(&self) -> AortaResult<()> {
        let (should_publish, accept_mono) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                (false, None)
            } else if state.started_stamp_ns == 0 {
                if state.status == ActionStatus::Accepted {
                    state.status = ActionStatus::Executing;
                }
                state.started_stamp_ns = unix_time_ns();
                (true, state.accepted_mono_ns)
            } else {
                (false, None)
            }
        };
        if should_publish {
            // Tracing: accept -> execution started (monotonic, gated; the clock
            // is read only if tracing is still on).
            if let Some(accept_mono) = accept_mono {
                crate::trace::record_stage_elapsed_under(
                    crate::trace::StageId::ActionAcceptedToExecutionStarted,
                    accept_mono,
                    self.server.ev_accepted_to_execution_started,
                    self.goal.goal_root.as_ref(),
                );
            }
            self.goal.publish_status(&self.server).await?;
        }
        Ok(())
    }

    async fn is_terminal(&self) -> bool {
        self.goal.state.lock().await.status.is_terminal()
    }

    async fn finish_terminal(
        &self,
        status: ActionStatus,
        code: ActionReplyCode,
        message: &str,
        result_payload: Option<TAction::OwnedResultMsg>,
    ) -> AortaResult<()> {
        let (became_terminal, accept_mono) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                (false, None)
            } else {
                if state.started_stamp_ns == 0 {
                    state.started_stamp_ns = unix_time_ns();
                }
                state.status = status;
                state.code = code;
                state.message = message.to_string();
                state.finished_stamp_ns = unix_time_ns();
                state.result_payload = result_payload;
                (true, state.accepted_mono_ns)
            }
        };
        if became_terminal {
            // Tracing: accept -> terminal (full goal latency; monotonic, gated;
            // the clock is read only if tracing is still on).
            if let Some(accept_mono) = accept_mono {
                match self.goal.goal_root {
                    Some(root) => crate::trace::record_stage_elapsed_linked(
                        crate::trace::StageId::ActionAcceptedToFinished,
                        accept_mono,
                        self.server.ev_accepted_to_finished,
                        root,
                    ),
                    None => crate::trace::record_stage_elapsed_as(
                        crate::trace::StageId::ActionAcceptedToFinished,
                        accept_mono,
                        self.server.ev_accepted_to_finished,
                    ),
                }
            }
            // Cancel-path stage: cancel request accepted -> Canceled terminal,
            // child of the goal root.
            if status == ActionStatus::Canceled {
                let cancel_mono = self.goal.cancel_requested_mono_ns.load(Ordering::Relaxed);
                if cancel_mono != 0 {
                    crate::trace::record_stage_elapsed_under(
                        crate::trace::StageId::ActionCancelRequestedToCanceled,
                        cancel_mono,
                        self.server.ev_cancel_to_canceled,
                        self.goal.goal_root.as_ref(),
                    );
                }
            }
            self.server.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            self.goal.publish_status(&self.server).await?;
            self.server
                .spawn_terminal_state_cleanup(Arc::clone(&self.goal));
        }
        Ok(())
    }
}

#[must_use = "dropping an ActionServer stops it; bind it for the serving lifetime"]
pub struct ActionServer<TAction: ActionWire + Send + Sync + 'static> {
    inner: Arc<ActionServerInner<TAction>>,
}

impl<TAction: ActionWire + Send + Sync + 'static> ActionServer<TAction> {
    pub async fn serve_info_only(
        node: &Node,
        action_name: &str,
        options: ActionServerOptions,
    ) -> AortaResult<Self> {
        let inner = ActionServerInner::new(node, action_name, options).await?;
        Ok(Self { inner })
    }

    pub async fn serve_with_handler<F, Fut>(
        node: &Node,
        action_name: &str,
        options: ActionServerOptions,
        handler: F,
    ) -> AortaResult<Self>
    where
        F: Fn(RetainedActionGoal<TAction>, ActionExecutionContext<TAction>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = AortaResult<()>> + Send + 'static,
    {
        let inner = ActionServerInner::new(node, action_name, options).await?;
        inner
            .start_send_goal_queryable(Arc::new(move |goal, ctx| Box::pin(handler(goal, ctx))))
            .await?;
        if inner.options.supports_cancel {
            inner.start_cancel_goal_queryable().await?;
        }
        inner.start_get_result_queryable().await?;
        Ok(Self { inner })
    }

    pub fn action_name(&self) -> &str {
        &self.inner.action_name
    }

    pub fn info_key(&self) -> &str {
        &self.inner.info_key
    }

    pub fn info(&self) -> &ActionInfo {
        &self.inner.info
    }

    pub async fn stop_and_join(self, timeout_duration: Duration) {
        let handler_tasks = self.inner.request_stop().await;
        self.inner
            .join_queryables(handler_tasks, timeout_duration)
            .await;
    }

    pub fn stop(&mut self) {
        self.inner.request_stop_nowait();
    }
}

impl<TAction: ActionWire + Send + Sync + 'static> Drop for ActionServer<TAction> {
    fn drop(&mut self) {
        self.stop();
    }
}

struct ActionServerInner<TAction: ActionWire + Send + Sync + 'static> {
    node: Node,
    action_name: String,
    /// 'static scope label for goal scopes — leaked ONCE per
    /// server (bounded by server count; per-goal names would leak per goal).
    goal_scope_label: &'static str,
    /// Set by `request_stop_nowait` BEFORE the queryable stop
    /// signals — `handle_send_goal` consults it so a goal registered while
    /// stop is in flight is born cancel-signalled (belt-and-suspenders; the
    /// `join_queryables` second scan is the race-free backstop).
    stopping: std::sync::atomic::AtomicBool,
    /// Interned per-action waterfall events (`<stage>:<action>`), minted once.
    ev_accepted_to_execution_started: u16,
    ev_accepted_to_finished: u16,
    ev_user_callback: u16,
    ev_feedback_pub: u16,
    ev_cancel_to_canceled: u16,
    ev_goal_admission: u16,
    ev_get_result_query: u16,
    info_key: String,
    send_goal_key: String,
    cancel_goal_key: String,
    get_result_key: String,
    feedback_key: String,
    info: ActionInfo,
    options: ActionServerOptions,
    info_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    info_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    info_task: TokioMutex<Option<JoinHandle<()>>>,
    send_goal_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    send_goal_task: TokioMutex<Option<JoinHandle<()>>>,
    cancel_goal_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    cancel_goal_task: TokioMutex<Option<JoinHandle<()>>>,
    get_result_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    get_result_task: TokioMutex<Option<JoinHandle<()>>>,
    goals: TokioMutex<HashMap<String, Arc<GoalRecord<TAction>>>>,
    expired_goals: TokioMutex<HashMap<String, ExpiredGoalRecord>>,
    admission_lock: TokioMutex<()>,
    active_goal_count: AtomicUsize,
    /// Per-(action, node) alive token. Clients watch
    /// it during `wait_result` so a server dying mid-goal surfaces as an
    /// error instead of an infinite wait. Dropped on the first stop request
    /// (std Mutex: taken from sync `request_stop_nowait`) so a gracefully
    /// stopped server also reads as gone even while wedged handler tasks
    /// keep the inner Arc alive.
    owner_token: std::sync::Mutex<Option<Arc<crate::liveliness::LivelinessManager>>>,
}

impl<TAction: ActionWire + Send + Sync + 'static> ActionServerInner<TAction> {
    async fn new(
        node: &Node,
        action_name: &str,
        options: ActionServerOptions,
    ) -> AortaResult<Arc<Self>> {
        options.validate()?;

        let action_name = normalize_path(action_name)?;
        let info_key: String = node.key_builder().action_info_key(&action_name)?.into();
        let send_goal_key: String = node
            .key_builder()
            .action_send_goal_key(&action_name)?
            .into();
        let cancel_goal_key: String = node
            .key_builder()
            .action_cancel_goal_key(&action_name)?
            .into();
        let get_result_key: String = node
            .key_builder()
            .action_get_result_key_pub(&action_name)?
            .into();
        let feedback_key: String = node
            .key_builder()
            .action_feedback_key_pub(&action_name)?
            .into();

        ensure_single_action_provider(
            node.session(),
            node.key_builder(),
            &action_name,
            Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
        )
        .await?;

        let schema_bfbs = TAction::schema_bfbs().to_vec();
        if schema_bfbs.is_empty() {
            return Err(AortaError::invalid_argument("action schema_bfbs is empty"));
        }
        let schema_hash = SchemaHash::from_bfbs_bytes(&schema_bfbs);
        node.schema_registry()
            .ensure_schema(schema_hash, schema_bfbs.clone())
            .await?;

        let info = ActionInfo {
            action_name: action_name.clone(),
            schema_hash,
            supports_cancel: options.supports_cancel,
            max_concurrency: u32::try_from(options.max_concurrency).expect("validated"),
            result_ttl_ms: duration_to_millis_u64(options.result_ttl)?,
            dedup_ttl_ms: duration_to_millis_u64(options.dedup_ttl)?,
            server_node_name: node.name().to_string(),
            server_epoch: Uuid::new_v4().hyphenated().to_string(),
        };
        let info_payload = Arc::new(TokioRwLock::new(Some(ZBytes::from(info.encode()?))));
        let (info_stop_tx, info_stop_rx) = oneshot::channel();
        let info_task = spawn_action_info_queryable(
            node.session().clone(),
            info_key.clone(),
            Arc::clone(&info_payload),
            ZBytes::from(schema_bfbs),
            info_stop_rx,
        )
        .await?;

        // Per-action waterfall events (timeline attribution in multi-action
        // processes); interned once per server.
        let ev_accepted_to_execution_started = crate::trace::stage_event_for(
            crate::trace::StageId::ActionAcceptedToExecutionStarted,
            &action_name,
        );
        let ev_user_callback =
            crate::trace::stage_event_for(crate::trace::StageId::ActionUserCallback, &action_name);
        let ev_feedback_pub = crate::trace::marker_event_for("action.feedback_pub", &action_name);
        let ev_cancel_to_canceled = crate::trace::stage_event_for(
            crate::trace::StageId::ActionCancelRequestedToCanceled,
            &action_name,
        );
        let ev_accepted_to_finished = crate::trace::stage_event_for(
            crate::trace::StageId::ActionAcceptedToFinished,
            &action_name,
        );
        let ev_goal_admission =
            crate::trace::stage_event_for(crate::trace::StageId::ActionGoalAdmission, &action_name);
        let ev_get_result_query = crate::trace::stage_event_for(
            crate::trace::StageId::ActionGetResultQuery,
            &action_name,
        );

        let goal_scope_label: &'static str =
            Box::leak(format!("action:{action_name}").into_boxed_str());

        // Declare the per-(action, node) alive token. A failed
        // declaration only disables client-side death detection — the server
        // still works — mirroring the service-side claim_service_owner
        // degradation.
        let owner_token = match node.key_builder().action_owner_key(&action_name, node.id()) {
            Ok(key) => {
                match crate::liveliness::LivelinessManager::declare_key(
                    node.session().inner(),
                    &key,
                )
                .await
                {
                    Ok(token) => Some(token),
                    Err(err) => {
                        aorta_core_warn!(
                            "Aorta WARN: action '{}' alive token declaration failed: {}; \
                             client-side server-death detection disabled",
                            action_name,
                            err
                        );
                        None
                    }
                }
            }
            Err(err) => {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' alive-token key invalid: {}; \
                     client-side server-death detection disabled",
                    action_name,
                    err
                );
                None
            }
        };

        let inner = Arc::new(Self {
            node: node.clone(),
            action_name,
            goal_scope_label,
            stopping: std::sync::atomic::AtomicBool::new(false),
            ev_accepted_to_execution_started,
            ev_accepted_to_finished,
            ev_user_callback,
            ev_feedback_pub,
            ev_cancel_to_canceled,
            ev_goal_admission,
            ev_get_result_query,
            info_key,
            send_goal_key,
            cancel_goal_key,
            get_result_key,
            feedback_key,
            info,
            options,
            info_payload,
            info_stop_tx: TokioMutex::new(Some(info_stop_tx)),
            info_task: TokioMutex::new(Some(info_task)),
            send_goal_stop_tx: TokioMutex::new(None),
            send_goal_task: TokioMutex::new(None),
            cancel_goal_stop_tx: TokioMutex::new(None),
            cancel_goal_task: TokioMutex::new(None),
            get_result_stop_tx: TokioMutex::new(None),
            get_result_task: TokioMutex::new(None),
            goals: TokioMutex::new(HashMap::new()),
            expired_goals: TokioMutex::new(HashMap::new()),
            admission_lock: TokioMutex::new(()),
            active_goal_count: AtomicUsize::new(0),
            owner_token: std::sync::Mutex::new(owner_token),
        });
        Ok(inner)
    }

    async fn start_send_goal_queryable(
        self: &Arc<Self>,
        handler: BoxedActionHandler<TAction>,
    ) -> AortaResult<()> {
        let mut stop_guard = self.send_goal_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.send_goal_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let send_goal_key = self.send_goal_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                // The caller's traceparent rides the send_goal query
                // attachment; parsed only when tracing is enabled (the stage
                // records that would join it are gated the same way).
                let decoded_attachment = query
                    .attachment()
                    .map(|attachment| crate::attachment::decode(attachment.to_bytes().as_ref()));
                let inbound_trace = if crate::trace::trace_enabled() {
                    decoded_attachment.as_ref().and_then(|p| p.traceparent)
                } else {
                    None
                };
                // the caller's relative goal budget (decoded
                // unconditionally — a delivery semantic, not tracing).
                // `AORTA_RPC_DEADLINE=0` disables the seed — the server
                // half of the same rollback lever the plain-RPC drop gate
                // honors (the seed was once ungated,
                // so a fleet-wide rollback left goal scopes still
                // expiring on stale wire budgets).
                let wire_budget_ns = decoded_attachment
                    .as_ref()
                    .and_then(|p| p.deadline_budget_ns)
                    .filter(|_| crate::service::rpc_deadline_drop_enabled());
                // Goal admission (request received -> accept/reject reply
                // ready), parented to the CALLER's goal_sent span so rejected
                // goals show up in the caller's trace too (rejects never mint
                // a goal root). Gated; clock read only when tracing is on.
                let admission_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
                let reply_payload = this
                    .handle_send_goal(
                        request.to_bytes().to_vec(),
                        Arc::clone(&handler),
                        inbound_trace,
                        wire_budget_ns,
                    )
                    .await;
                if let Some(start) = admission_start {
                    let parent = inbound_trace.map(|tp| crate::trace::SpanLink {
                        trace_id: tp.trace_id,
                        span_id: tp.span_id,
                        parent_span_id: 0,
                    });
                    crate::trace::record_stage_elapsed_under(
                        crate::trace::StageId::ActionGoalAdmission,
                        start,
                        this.ev_goal_admission,
                        parent.as_ref(),
                    );
                }
                match reply_payload {
                    Ok(payload) => {
                        if let Err(err) = query.reply(&send_goal_key, payload).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' send_goal reply failed: {}",
                                this.action_name,
                                err
                            );
                        }
                    }
                    Err(err) => {
                        if let Err(reply_err) = query.reply_err(err.to_string()).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' send_goal error reply failed: {}",
                                this.action_name,
                                reply_err
                            );
                        }
                    }
                }
            }

            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' send_goal queryable undeclare failed: {}",
                    this.action_name,
                    err
                );
            }
        });

        *stop_guard = Some(stop_tx);
        *self.send_goal_task.lock().await = Some(task);
        drop(stop_guard);

        if let Err(err) = self.wait_for_send_goal_queryable_ready().await {
            let handler_tasks = self.request_stop().await;
            self.join_queryables(
                handler_tasks,
                Duration::from_millis(ACTION_SEND_GOAL_READY_TIMEOUT_MS),
            )
            .await;
            return Err(err);
        }

        Ok(())
    }

    async fn wait_for_send_goal_queryable_ready(&self) -> AortaResult<()> {
        let timeout_duration = Duration::from_millis(ACTION_SEND_GOAL_READY_TIMEOUT_MS);
        let poll_duration = Duration::from_millis(ACTION_SEND_GOAL_READY_POLL_MS);
        let started = tokio::time::Instant::now();

        loop {
            let replies = self
                .node
                .session()
                .inner()
                .get(self.send_goal_key.clone())
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .timeout(poll_duration)
                .await?;

            if matches!(
                timeout(poll_duration, replies.recv_async()).await,
                Ok(Ok(_))
            ) {
                return Ok(());
            }

            if started.elapsed() >= timeout_duration {
                return Err(AortaError::timeout(format!(
                    "action '{}' send_goal queryable did not become ready after {:?}",
                    self.action_name, timeout_duration
                )));
            }

            tokio::time::sleep(poll_duration).await;
        }
    }

    async fn start_cancel_goal_queryable(self: &Arc<Self>) -> AortaResult<()> {
        let mut stop_guard = self.cancel_goal_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.cancel_goal_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let cancel_goal_key = self.cancel_goal_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                let reply_payload = this.handle_cancel_goal(request.to_bytes().to_vec()).await;
                match reply_payload {
                    Ok(payload) => {
                        if let Err(err) = query.reply(&cancel_goal_key, payload).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' cancel_goal reply failed: {}",
                                this.action_name,
                                err
                            );
                        }
                    }
                    Err(err) => {
                        if let Err(reply_err) = query.reply_err(err.to_string()).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' cancel_goal error reply failed: {}",
                                this.action_name,
                                reply_err
                            );
                        }
                    }
                }
            }

            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' cancel_goal queryable undeclare failed: {}",
                    this.action_name,
                    err
                );
            }
        });

        *stop_guard = Some(stop_tx);
        *self.cancel_goal_task.lock().await = Some(task);
        Ok(())
    }

    async fn start_get_result_queryable(self: &Arc<Self>) -> AortaResult<()> {
        let mut stop_guard = self.get_result_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.get_result_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let get_result_key = self.get_result_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                let reply_payload = this.handle_get_result(request.to_bytes().to_vec()).await;
                match reply_payload {
                    Ok(payload) => {
                        if let Err(err) = query.reply(&get_result_key, payload).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' get_result reply failed: {}",
                                this.action_name,
                                err
                            );
                        }
                    }
                    Err(err) => {
                        if let Err(reply_err) = query.reply_err(err.to_string()).await {
                            aorta_core_warn!(
                                "Aorta WARN: action '{}' get_result error reply failed: {}",
                                this.action_name,
                                reply_err
                            );
                        }
                    }
                }
            }

            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' get_result queryable undeclare failed: {}",
                    this.action_name,
                    err
                );
            }
        });

        *stop_guard = Some(stop_tx);
        *self.get_result_task.lock().await = Some(task);
        Ok(())
    }

    async fn handle_send_goal(
        self: &Arc<Self>,
        request_payload: Vec<u8>,
        handler: BoxedActionHandler<TAction>,
        inbound_trace: Option<crate::trace::TraceParent>,
        wire_budget_ns: Option<u64>,
    ) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action send_goal request payload is empty",
            ));
        }

        let goal_id = TAction::decode_send_goal_request_goal_id(&request_payload)?;
        let _ = self
            .node
            .key_builder()
            .action_status_key(&self.action_name, &goal_id)?;
        let payload_hash = action_payload_hash(&request_payload);
        let _admission_guard = self.admission_lock.lock().await;

        let maybe_existing = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };
        if let Some(existing) = maybe_existing {
            let state = existing.state.lock().await;
            let response = if existing.request_payload_hash == payload_hash {
                ActionSendGoalResponse {
                    accepted: true,
                    goal_id,
                    goal_secret: existing.goal_secret.clone(),
                    code: ActionReplyCode::DuplicateAccepted,
                    status: state.status,
                    message: ACTION_DUPLICATE_MESSAGE.to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::PayloadConflict,
                    status: state.status,
                    message: "goal_id already exists with different payload".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            };
            return TAction::encode_send_goal_response(&response);
        }

        if let Some(expired) = self.find_expired_goal(&goal_id).await {
            let response = if expired.payload_hash == payload_hash {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::GoalExpired,
                    status: expired.status,
                    message: ACTION_GOAL_EXPIRED_MESSAGE.to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::PayloadConflict,
                    status: expired.status,
                    message: "goal_id already exists with different payload".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            };
            return TAction::encode_send_goal_response(&response);
        }

        if self.current_active_goal_count().await >= self.options.max_concurrency {
            let response = ActionSendGoalResponse {
                accepted: false,
                goal_id,
                goal_secret: String::new(),
                code: ActionReplyCode::Busy,
                status: ActionStatus::Unknown,
                message: ACTION_BUSY_MESSAGE.to_string(),
                server_epoch: self.info.server_epoch.clone(),
            };
            return TAction::encode_send_goal_response(&response);
        }

        let accepted_stamp_ns = unix_time_ns();
        let status_key: String = self
            .node
            .key_builder()
            .action_status_key(&self.action_name, &goal_id)?
            .into();
        let goal_secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let initial_status = ActionPublicStatus {
            goal_id: goal_id.clone(),
            status: ActionStatus::Accepted,
            code: ActionReplyCode::Ok,
            accepted_stamp_ns,
            started_stamp_ns: 0,
            finished_stamp_ns: 0,
            last_feedback_seq: 0,
            server_epoch: self.info.server_epoch.clone(),
        };
        let initial_status_payload = TAction::encode_public_status(&initial_status)?;
        let status_payload = Arc::new(TokioRwLock::new(Some(ZBytes::from(
            initial_status_payload.clone(),
        ))));
        let goal = Arc::new(GoalRecord {
            goal_id: goal_id.clone(),
            goal_secret: goal_secret.clone(),
            // The goal roots a Scope; the caller's wire-stamped
            // budget seeds it (re-anchored at receipt, a
            // cooperative narrowing hint; absent = unlimited (un-stamped behavior)).
            scope: crate::flow::scope::ActionScope::for_goal(
                self.goal_scope_label,
                wire_budget_ns
                    .map(crate::flow::Budget::from_wire_relative_ns)
                    .unwrap_or_else(crate::flow::Budget::unlimited),
            ),
            request_payload: request_payload.clone(),
            request_payload_hash: payload_hash.clone(),
            status_key,
            status_payload,
            status_stop_tx: TokioMutex::new(None),
            status_task: TokioMutex::new(None),
            handler_task: TokioMutex::new(None),
            handler_abort: std::sync::Mutex::new(None),
            state: TokioMutex::new(GoalState {
                status: ActionStatus::Accepted,
                code: ActionReplyCode::Ok,
                message: String::new(),
                accepted_stamp_ns,
                started_stamp_ns: 0,
                finished_stamp_ns: 0,
                last_feedback_seq: 0,
                // Tracing: monotonic accept time, captured only when enabled.
                accepted_mono_ns: crate::trace::trace_enabled().then(crate::time::monotonic_ns),
                last_feedback_mono_ns: 0,
                feedback_snapshot: TAction::OwnedFeedbackDataMsg::default(),
                result_payload: None,
            }),
            cancel_requested: AtomicBool::new(false),
            cancel_notify: Notify::new(),
            cancel_requested_mono_ns: AtomicU64::new(0),
            inbound_trace,
            // Per-goal SERVER ROOT (trace hierarchy): one cross-process link to
            // the caller, lifecycle stages nested beneath it.
            goal_root: crate::trace::inbound_root_link(inbound_trace.as_ref()),
            _marker: PhantomData,
        });

        {
            let mut goals = self.goals.lock().await;
            goals.insert(goal_id.clone(), Arc::clone(&goal));
            self.active_goal_count.fetch_add(1, Ordering::AcqRel);
        }

        if let Err(err) = goal
            .start_status_queryable(self.node.session().clone())
            .await
        {
            let _ = self.remove_goal(&goal_id).await;
            self.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            return Err(err);
        }
        if let Err(err) = goal.publish_status(self).await {
            let _ = self.remove_goal(&goal_id).await;
            self.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            goal.request_stop().await;
            return Err(err);
        }

        let response = ActionSendGoalResponse {
            accepted: true,
            goal_id: goal_id.clone(),
            goal_secret,
            code: ActionReplyCode::Ok,
            status: ActionStatus::Accepted,
            message: String::new(),
            server_epoch: self.info.server_epoch.clone(),
        };
        let response_payload = TAction::encode_send_goal_response(&response)?;

        let handler_task = self.spawn_goal_handler(Arc::clone(&goal), handler);
        *goal.handler_task.lock().await = Some(handler_task);
        // Belt-and-suspenders: a send_goal racing request_stop may
        // register AFTER the stop snapshot — light its cancel signal at
        // birth so the handler's FIRST poll observes it. (The second scan
        // in join_queryables remains the race-free backstop.)
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            Self::fire_stop_cancel(&goal);
        }
        Ok(response_payload)
    }

    async fn handle_cancel_goal(
        self: &Arc<Self>,
        request_payload: Vec<u8>,
    ) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action cancel_goal request payload is empty",
            ));
        }

        let (goal_id, goal_secret) = TAction::decode_cancel_goal_request(&request_payload)?;
        if !self.options.supports_cancel {
            let response = ActionCancelGoalResponse {
                accepted: false,
                goal_id,
                code: ActionReplyCode::Conflict,
                status: ActionStatus::Unknown,
                message: "action server does not support cancel".to_string(),
            };
            return TAction::encode_cancel_goal_response(&response);
        }
        let goal = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };

        let response = if let Some(goal) = goal {
            let mut should_publish = false;
            let response = {
                let mut state = goal.state.lock().await;
                if goal.goal_secret != goal_secret {
                    ActionCancelGoalResponse {
                        accepted: false,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::Unauthorized,
                        status: state.status,
                        message: "goal_secret does not match".to_string(),
                    }
                } else if state.status.is_terminal() {
                    ActionCancelGoalResponse {
                        accepted: false,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::AlreadyTerminal,
                        status: state.status,
                        message: "goal is already terminal".to_string(),
                    }
                } else {
                    if state.status != ActionStatus::Canceling {
                        state.status = ActionStatus::Canceling;
                        should_publish = true;
                    }
                    ActionCancelGoalResponse {
                        accepted: true,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::Ok,
                        status: ActionStatus::Canceling,
                        message: String::new(),
                    }
                }
            };

            if response.accepted {
                // The wire cancel fires the goal scope's token FIRST,
                // cascading to every sub-activity spawned on it. Token before
                // notify: a handler waking from `cancelled().await` must
                // already observe `scope().is_cancelled()` (the contract the
                // goal-scope cancel cascade test asserts — notify-first raced it).
                goal.scope.cancel(crate::flow::CancelReason::Explicit);
                goal.cancel_requested.store(true, Ordering::Relaxed);
                // First accepted cancel anchors the cancel->canceled stage
                // (gated: no clock read when tracing is off).
                if crate::trace::trace_enabled() {
                    let now = crate::time::monotonic_ns().max(1);
                    let _ = goal.cancel_requested_mono_ns.compare_exchange(
                        0,
                        now,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
                goal.cancel_notify.notify_waiters();
            }
            if should_publish {
                goal.publish_status(self).await?;
            }
            response
        } else {
            ActionCancelGoalResponse {
                accepted: false,
                goal_id,
                code: ActionReplyCode::NotFound,
                status: ActionStatus::Unknown,
                message: "goal_id not found".to_string(),
            }
        };

        TAction::encode_cancel_goal_response(&response)
    }

    async fn handle_get_result(self: &Arc<Self>, request_payload: Vec<u8>) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action get_result request payload is empty",
            ));
        }

        // Result delivery, server half (request received -> response built),
        // child of the goal root when the goal is live (no root for
        // expired/unknown goals — local-context fallback). Gated.
        let query_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);

        let (goal_id, goal_secret) = TAction::decode_get_result_request(&request_payload)?;
        let goal = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };
        let goal_root = goal.as_ref().and_then(|goal| goal.goal_root);

        let response = if let Some(goal) = goal {
            let state = goal.state.lock().await;
            if goal.goal_secret != goal_secret {
                ActionGetResultResponse {
                    goal_id,
                    status: state.status,
                    accepted_stamp_ns: state.accepted_stamp_ns,
                    started_stamp_ns: state.started_stamp_ns,
                    finished_stamp_ns: state.finished_stamp_ns,
                    last_feedback_seq: state.last_feedback_seq,
                    feedback_snapshot: state.feedback_snapshot.clone(),
                    result: None,
                    code: ActionReplyCode::Unauthorized,
                    message: "goal_secret does not match".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionGetResultResponse {
                    goal_id,
                    status: state.status,
                    accepted_stamp_ns: state.accepted_stamp_ns,
                    started_stamp_ns: state.started_stamp_ns,
                    finished_stamp_ns: state.finished_stamp_ns,
                    last_feedback_seq: state.last_feedback_seq,
                    feedback_snapshot: state.feedback_snapshot.clone(),
                    result: state.result_payload.clone(),
                    code: state.code,
                    message: state.message.clone(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            }
        } else {
            match self.find_expired_goal(&goal_id).await {
                Some(expired) => ActionGetResultResponse {
                    goal_id,
                    status: expired.status,
                    accepted_stamp_ns: expired.accepted_stamp_ns,
                    started_stamp_ns: expired.started_stamp_ns,
                    finished_stamp_ns: expired.finished_stamp_ns,
                    last_feedback_seq: expired.last_feedback_seq,
                    feedback_snapshot: TAction::OwnedFeedbackDataMsg::default(),
                    result: None,
                    code: ActionReplyCode::GoalExpired,
                    message: ACTION_GOAL_EXPIRED_MESSAGE.to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                },
                None => ActionGetResultResponse {
                    goal_id,
                    status: ActionStatus::Unknown,
                    accepted_stamp_ns: 0,
                    started_stamp_ns: 0,
                    finished_stamp_ns: 0,
                    last_feedback_seq: 0,
                    feedback_snapshot: TAction::OwnedFeedbackDataMsg::default(),
                    result: None,
                    code: ActionReplyCode::NotFound,
                    message: "goal_id not found".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                },
            }
        };

        let encoded = TAction::encode_get_result_response(&response);
        if let Some(start) = query_start {
            crate::trace::record_stage_elapsed_under(
                crate::trace::StageId::ActionGetResultQuery,
                start,
                self.ev_get_result_query,
                goal_root.as_ref(),
            );
        }
        encoded
    }

    /// Spawn the outer driver task for a goal handler. Returns the
    /// `JoinHandle` so the caller can record it on the `GoalRecord` —
    /// `join_queryables` later drains it on shutdown so the user does
    /// not observe handler callbacks firing after `ActionServer::stop`.
    fn spawn_goal_handler(
        self: &Arc<Self>,
        goal: Arc<GoalRecord<TAction>>,
        handler: BoxedActionHandler<TAction>,
    ) -> JoinHandle<()> {
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let goal_view = RetainedActionGoal::<TAction>::new(
                goal.goal_id.clone(),
                goal.request_payload.clone(),
            );
            let ctx = ActionExecutionContext {
                server: Arc::clone(&server),
                goal: Arc::clone(&goal),
            };
            if let Err(err) = ctx.mark_executing().await {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' failed to transition goal '{}' to EXECUTING: {}",
                    server.action_name,
                    goal.goal_id,
                    err
                );
                let _ = ctx
                    .abort(ActionReplyCode::HandlerError, &err.to_string())
                    .await;
                return;
            }

            let handler_ctx = ctx.clone();
            // Handler-duration stage: action execute-callback duration (invocation ->
            // future completion, panic-isolated inner task included), child
            // of the goal root.
            let cb_start = goal.goal_root.is_some().then(crate::time::monotonic_ns);
            let inner = tokio::spawn(async move { handler(goal_view, handler_ctx).await });
            *goal.handler_abort.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(inner.abort_handle());
            let result = inner.await;
            if let Some(start) = cb_start {
                crate::trace::record_stage_elapsed_under(
                    crate::trace::StageId::ActionUserCallback,
                    start,
                    server.ev_user_callback,
                    goal.goal_root.as_ref(),
                );
            }
            match result {
                Ok(Ok(())) => {
                    if !ctx.is_terminal().await {
                        let _ = ctx
                            .abort(
                                ActionReplyCode::HandlerCompletedWithoutResult,
                                ACTION_HANDLER_WITHOUT_RESULT_MESSAGE,
                            )
                            .await;
                    }
                }
                Ok(Err(err)) => {
                    // Fail-fast: a failed handler cancels any
                    // sub-activities still running on the goal scope.
                    goal.scope.cancel(crate::flow::CancelReason::ChildFailed);
                    let _ = ctx
                        .abort(ActionReplyCode::HandlerError, &err.to_string())
                        .await;
                }
                Err(_) => {
                    goal.scope.cancel(crate::flow::CancelReason::ChildFailed);
                    let _ = ctx
                        .abort(ActionReplyCode::HandlerPanic, ACTION_HANDLER_PANIC_MESSAGE)
                        .await;
                }
            }
            // Scope lifecycle: the goal (a Scope) does not close until every
            // child finishes or cancels. A handler that reached terminal
            // with children still running left stragglers — cancel them
            // (post-terminal work is unreachable by the client) and drain.
            // No children (every pre-Flow handler) → both steps are no-ops,
            // preserving driver semantics byte-for-byte (invariant 5).
            if goal.scope.live_children() > 0 {
                if !goal.scope.is_cancelled() {
                    aorta_core_warn!(
                        "Aorta WARN: action '{}' goal '{}' reached terminal with {} \
                         sub-task(s) still running; cancelling them",
                        server.action_name,
                        goal.goal_id,
                        goal.scope.live_children()
                    );
                    goal.scope.cancel(crate::flow::CancelReason::Shutdown);
                }
                goal.scope.close().await;
            }
        })
    }

    async fn find_expired_goal(&self, goal_id: &str) -> Option<ExpiredGoalRecord> {
        self.expired_goals.lock().await.get(goal_id).cloned()
    }

    async fn current_active_goal_count(&self) -> usize {
        let goal_records = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };

        let mut count = 0usize;
        for goal in goal_records {
            if !goal.state.lock().await.status.is_terminal() {
                count += 1;
            }
        }
        count
    }

    fn spawn_terminal_state_cleanup(self: &Arc<Self>, goal: Arc<GoalRecord<TAction>>) {
        let goal_id = goal.goal_id.clone();
        let result_ttl = self.options.result_ttl;
        let dedup_ttl = self.options.dedup_ttl;
        let weak_server = Arc::downgrade(self);

        crate::runtime::spawn_logged("action_terminal_state_cleanup", async move {
            tokio::time::sleep(result_ttl).await;

            let Some(server) = weak_server.upgrade() else {
                return;
            };
            let Some(goal) = server.remove_goal(&goal_id).await else {
                return;
            };

            let expired = goal.expired_record().await;
            server
                .expired_goals
                .lock()
                .await
                .insert(goal_id.clone(), expired);

            if let Err(err) = goal.expire_state(server.node.session()).await {
                aorta_core_warn!(
                    "Aorta WARN: action '{}' failed to expire terminal goal '{}': {}",
                    server.action_name,
                    goal_id,
                    err
                );
            }

            drop(goal);
            drop(server);

            let dedup_delay = dedup_ttl.saturating_sub(result_ttl);
            if dedup_delay.is_zero() {
                if let Some(server) = weak_server.upgrade() {
                    server.expired_goals.lock().await.remove(&goal_id);
                }
                return;
            }

            tokio::time::sleep(dedup_delay).await;
            if let Some(server) = weak_server.upgrade() {
                server.expired_goals.lock().await.remove(&goal_id);
            }
        });
    }

    async fn publish_feedback_payload(
        &self,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> AortaResult<()> {
        let builder = self.node.session().inner().put(&self.feedback_key, payload);
        match attachment {
            Some(attachment) => builder.attachment(attachment).await?,
            None => builder.await?,
        }
        Ok(())
    }

    async fn publish_status_payload(
        &self,
        goal: &GoalRecord<TAction>,
        payload: Vec<u8>,
    ) -> AortaResult<()> {
        {
            let mut guard = goal.status_payload.write().await;
            *guard = Some(ZBytes::from(payload.clone()));
        }
        self.node
            .session()
            .inner()
            .put(&goal.status_key, payload)
            .await?;
        Ok(())
    }

    async fn remove_goal(&self, goal_id: &str) -> Option<Arc<GoalRecord<TAction>>> {
        self.goals.lock().await.remove(goal_id)
    }

    /// Stops the server. Returns a snapshot of all in-flight handler
    /// driver task handles so `join_queryables` can drain them later;
    /// taking the snapshot here, *before* the `abort` calls below
    /// schedule `spawn_terminal_state_cleanup`, decouples the handler
    /// drain from `self.goals` membership. Without this snapshot a
    /// short `result_ttl` could let the cleanup task remove a goal
    /// from `self.goals` before `join_queryables` walks the map,
    /// silently skipping the handler drain. The `JoinHandle`s in the
    /// returned Vec are moved out of
    /// the per-goal slot so the per-goal `handler_task` is left as
    /// `None` afterward.
    async fn request_stop(self: &Arc<Self>) -> Vec<(JoinHandle<()>, Arc<GoalRecord<TAction>>)> {
        self.request_stop_nowait();

        let goal_records = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };

        let mut handler_tasks = Vec::with_capacity(goal_records.len());
        for goal in &goal_records {
            if let Some(task) = goal.handler_task.lock().await.take() {
                handler_tasks.push((task, Arc::clone(goal)));
            }
        }

        for goal in goal_records {
            let is_terminal = goal.state.lock().await.status.is_terminal();
            if !is_terminal {
                // Cancel signal FIRST (both observation paths),
                // so the handler starts unwinding while the abort
                // bookkeeping below proceeds.
                Self::fire_stop_cancel(&goal);
                let ctx = ActionExecutionContext {
                    server: Arc::clone(self),
                    goal: Arc::clone(&goal),
                };
                let _ = ctx
                    .abort(
                        ActionReplyCode::ContextDropped,
                        ACTION_CONTEXT_DROPPED_MESSAGE,
                    )
                    .await;
            }
            goal.request_stop().await;
        }

        handler_tasks
    }

    /// The STOP-side cancel signal, firing BOTH
    /// observation paths — the legacy poll (`is_cancel_requested`) and the
    /// structural one (`scope().is_cancelled()` / `cancelled().await`) —
    /// exactly like the wire-cancel path (see `handle_cancel_goal`). A
    /// cooperatively-written handler unwinds promptly instead of eating
    /// the full join timeout on every server stop.
    fn fire_stop_cancel(goal: &GoalRecord<TAction>) {
        // Token before notify — same ordering as the wire-cancel path:
        // any waiter waking from `cancelled().await` must already observe
        // `scope().is_cancelled()`.
        goal.scope.cancel(crate::flow::CancelReason::Shutdown);
        goal.cancel_requested.store(true, Ordering::Relaxed);
        if crate::trace::trace_enabled() {
            let now = crate::time::monotonic_ns().max(1);
            let _ = goal.cancel_requested_mono_ns.compare_exchange(
                0,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        goal.cancel_notify.notify_waiters();
    }

    fn request_stop_nowait(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
        // Drop the alive token first: a stopping server must read as gone to
        // clients watching it in wait_result, even if wedged handler
        // tasks keep this inner alive past the stop request.
        if let Ok(mut token) = self.owner_token.lock() {
            token.take();
        }
        if let Ok(mut guard) = self.info_payload.try_write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.info_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.send_goal_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.cancel_goal_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.get_result_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
    }

    /// Drains the queryable tasks plus the per-goal status tasks plus
    /// any handler driver tasks. The `handler_tasks` parameter carries
    /// the snapshot taken by `request_stop` (taken early to dodge the
    /// `spawn_terminal_state_cleanup` removal race documented there);
    /// we also rescan `self.goals` for *late* `handler_task` entries
    /// AFTER `send_goal_task` has been awaited, because a `send_goal`
    /// query already in flight when `request_stop_nowait` fires can
    /// still run `handle_send_goal` to completion and insert a new
    /// `handler_task` after the initial snapshot. Once `send_goal_task`
    /// has exited (verified by `wait_for_task` returning), no further
    /// inserts can occur, so the second snapshot is race-free.
    async fn join_queryables(
        &self,
        mut handler_tasks: Vec<(JoinHandle<()>, Arc<GoalRecord<TAction>>)>,
        timeout_duration: Duration,
    ) {
        if let Some(task) = self.info_task.lock().await.take() {
            wait_for_task(task, timeout_duration, &self.action_name, "info").await;
        }
        if let Some(task) = self.send_goal_task.lock().await.take() {
            wait_for_task(task, timeout_duration, &self.action_name, "send_goal").await;
        }
        if let Some(task) = self.cancel_goal_task.lock().await.take() {
            wait_for_task(task, timeout_duration, &self.action_name, "cancel_goal").await;
        }
        if let Some(task) = self.get_result_task.lock().await.take() {
            wait_for_task(task, timeout_duration, &self.action_name, "get_result").await;
        }

        let goal_records = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };
        // Second pass: catch any `handler_task` inserted between the
        // `request_stop` snapshot and `send_goal_task` actually exiting.
        // Safe at this point because `send_goal_task` is the only
        // producer of new handler tasks and it has already been drained
        // above.
        for goal in &goal_records {
            if let Some(task) = goal.handler_task.lock().await.take() {
                // A LATE goal (send_goal raced request_stop) must
                // get the same cancel signal the snapshot loop fired —
                // otherwise its cooperative handler eats the join timeout.
                if !goal.state.lock().await.status.is_terminal() {
                    Self::fire_stop_cancel(goal);
                }
                handler_tasks.push((task, Arc::clone(goal)));
            }
        }
        for goal in goal_records {
            if let Some(task) = goal.status_task.lock().await.take() {
                wait_for_task(task, timeout_duration, &self.action_name, "status").await;
            }
        }

        for (task, goal) in handler_tasks {
            wait_for_handler_task(task, goal, timeout_duration, &self.action_name).await;
        }
    }
}

struct GoalRecord<TAction: ActionWire + Send + Sync + 'static> {
    goal_id: String,
    goal_secret: String,
    /// An action IS a Scope. Rooted at the goal (the per-goal
    /// cancel state fires its token; the caller's wire-stamped goal budget
    /// seeds it). `ActionExecutionContext::scope` hands it to
    /// the handler.
    scope: crate::flow::scope::ActionScope,
    request_payload: Vec<u8>,
    request_payload_hash: String,
    status_key: String,
    status_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    status_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    status_task: TokioMutex<Option<JoinHandle<()>>>,
    // The outer task that drives the user handler from `spawn_goal_handler`.
    // Tracked so `ActionServerInner::join_queryables` can drain it on
    // shutdown alongside the queryable tasks; without this the handler
    // task would survive past `ActionServer::stop` and the user could
    // observe callbacks firing after they thought the server had stopped.
    // Closes the fire-and-forget gap — the inner `tokio::spawn(...).await` at
    // `spawn_goal_handler` line ~1042 is already panic-isolated; only
    // the outer driver needs the handle.
    handler_task: TokioMutex<Option<JoinHandle<()>>>,
    /// Abort handle for the INNER handler task —
    /// aborting only the outer driver on stop-timeout DETACHES the inner
    /// task (drop-detach), leaving it running past shutdown.
    handler_abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    state: TokioMutex<GoalState<TAction>>,
    cancel_requested: AtomicBool,
    cancel_notify: Notify,
    /// Monotonic time the FIRST accepted cancel request landed (0 = none) —
    /// anchors the `cancel_requested_to_canceled` stage.
    cancel_requested_mono_ns: AtomicU64,
    /// Trace context propagated on the send_goal query attachment, when
    /// the caller injected one and tracing was enabled at accept. Immutable
    /// for the goal's lifetime.
    inbound_trace: Option<crate::trace::TraceParent>,
    /// Per-goal SERVER ROOT span identity: `accepted_to_finished` (the goal's
    /// full server window) is emitted under exactly this id (parented to the
    /// caller's send_goal span when propagated), and the other lifecycle
    /// stages record as its children — one cross-process link per goal.
    goal_root: Option<crate::trace::SpanLink>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire + Send + Sync + 'static> GoalRecord<TAction> {
    async fn start_status_queryable(&self, session: Session) -> AortaResult<()> {
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = spawn_single_key_snapshot_queryable(
            session,
            self.status_key.clone(),
            Arc::clone(&self.status_payload),
            stop_rx,
        )
        .await?;
        *self.status_stop_tx.lock().await = Some(stop_tx);
        *self.status_task.lock().await = Some(task);
        Ok(())
    }

    async fn public_status(&self, server: &ActionServerInner<TAction>) -> ActionPublicStatus {
        let state = self.state.lock().await;
        ActionPublicStatus {
            goal_id: self.goal_id.clone(),
            status: state.status,
            code: state.code,
            accepted_stamp_ns: state.accepted_stamp_ns,
            started_stamp_ns: state.started_stamp_ns,
            finished_stamp_ns: state.finished_stamp_ns,
            last_feedback_seq: state.last_feedback_seq,
            server_epoch: server.info.server_epoch.clone(),
        }
    }

    async fn publish_status(&self, server: &ActionServerInner<TAction>) -> AortaResult<()> {
        let status = self.public_status(server).await;
        let payload = TAction::encode_public_status(&status)?;
        server.publish_status_payload(self, payload).await
    }

    async fn request_stop(&self) {
        if let Some(stop_tx) = self.status_stop_tx.lock().await.take() {
            let _ = stop_tx.send(());
        }
        let mut payload = self.status_payload.write().await;
        *payload = None;
    }

    async fn expired_record(&self) -> ExpiredGoalRecord {
        let state = self.state.lock().await;
        ExpiredGoalRecord {
            goal_id: self.goal_id.clone(),
            payload_hash: self.request_payload_hash.clone(),
            status: state.status,
            accepted_stamp_ns: state.accepted_stamp_ns,
            started_stamp_ns: state.started_stamp_ns,
            finished_stamp_ns: state.finished_stamp_ns,
            last_feedback_seq: state.last_feedback_seq,
        }
    }

    async fn expire_state(&self, session: &Session) -> AortaResult<()> {
        session.inner().delete(&self.status_key).await?;
        self.request_stop().await;
        Ok(())
    }
}

struct GoalState<TAction: ActionWire + Send + Sync + 'static> {
    status: ActionStatus,
    code: ActionReplyCode,
    message: String,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    /// Monotonic accept time (ns) for tracing's action stage latencies. `None`
    /// when tracing was disabled at accept (no clock read) — not a `0` sentinel,
    /// since `monotonic_ns()` can legitimately read 0 on its first call. Separate
    /// from the wall-clock `accepted_stamp_ns` kept for the status/telemetry
    /// contract.
    accepted_mono_ns: Option<u64>,
    /// Monotonic time (ns) of the last feedback publish, for the feedback-interval
    /// stage. `0` until the first feedback under tracing.
    last_feedback_mono_ns: u64,
    feedback_snapshot: TAction::OwnedFeedbackDataMsg,
    result_payload: Option<TAction::OwnedResultMsg>,
}

#[derive(Clone)]
struct ExpiredGoalRecord {
    goal_id: String,
    payload_hash: String,
    status: ActionStatus,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
}

fn action_payload_hash(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    format!("sha256:{:x}", hasher.finalize())
}

async fn wait_for_task(
    task: JoinHandle<()>,
    timeout_duration: Duration,
    action_name: &str,
    surface: &str,
) {
    // Hold an `AbortHandle` separately from the JoinHandle we pass to
    // `tokio::time::timeout`, because `timeout` consumes the future by
    // value. On timeout we MUST call `abort()` — simply dropping the
    // `JoinHandle` only detaches the task, leaving it to keep running
    // past server shutdown and undermining the stop-and-join contract
    // (the same hazard as on the subscriber `undeclare` path).
    let abort_handle = task.abort_handle();
    if tokio::time::timeout(timeout_duration, task).await.is_err() {
        abort_handle.abort();
        aorta_core_warn!(
            "Aorta WARN: action '{}' {} stop timed out after {:?}; aborted",
            action_name,
            surface,
            timeout_duration
        );
    }
}

/// Outcome of one probe of an action's info key.
///
/// `Absent` and `TearingDown` are NOT the same thing, which is why this exists:
/// both mean "no usable ActionInfo right now", but only `Absent` means the
/// action surface is free. See [`probe_action_info`].
pub(crate) enum ActionInfoProbe {
    Found(Box<ActionInfo>),
    /// Nobody answered: no server holds this action.
    Absent,
    /// A snapshot queryable answered with the absent-snapshot marker. The
    /// server that owns it is TEARING DOWN, not gone: `request_stop_nowait`
    /// clears `info_payload` and signals the four queryable tasks, but those
    /// tasks undeclare asynchronously — a `send_goal` query already in flight
    /// still runs to completion, and `handle_send_goal` does not reject a
    /// stopping server. Startup cannot produce this state (`serve` fills
    /// `info_payload` BEFORE declaring the queryable), so the marker is
    /// unambiguously teardown.
    TearingDown,
}

/// One-shot probe of the action info key. `fetch_action_info` is the
/// caller-facing wrapper; admission (`ensure_single_action_provider`) needs the
/// `Absent` / `TearingDown` distinction this preserves.
pub(crate) async fn probe_action_info(
    session: &Session,
    key_builder: &KeyExprBuilder,
    action_name: &str,
    timeout_duration: Duration,
) -> AortaResult<ActionInfoProbe> {
    let info_key: String = key_builder.action_info_key(action_name)?.into();
    let replies = session
        .inner()
        .get(&info_key)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(timeout_duration)
        .await?;

    let reply = match timeout(timeout_duration, replies.recv_async()).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) | Err(_) => return Ok(ActionInfoProbe::Absent),
    };

    let check_window = timeout_duration.min(Duration::from_millis(ACTION_PROVIDER_CHECK_WINDOW_MS));
    if !check_window.is_zero()
        && matches!(timeout(check_window, replies.recv_async()).await, Ok(Ok(_)))
    {
        return Err(AortaError::concurrent_conflict(format!(
            "action '{}' returned multiple ActionInfo replies; deployment must enforce one active server per {{group,action}}",
            action_name
        )));
    }

    match reply.result() {
        Ok(sample) => ActionInfo::decode(sample.payload().to_bytes().as_ref())
            .map(|info| ActionInfoProbe::Found(Box::new(info))),
        // zenoh's client-side query timeout arrives as a synthesized reply
        // error racing the tokio recv timeout above; both mean "no server
        // answered in time", so both map to `Absent`.
        Err(err) if reply_error_is_zenoh_query_timeout(err) => Ok(ActionInfoProbe::Absent),
        // The absent-snapshot marker is not a fault — it is a live queryable
        // reporting an empty payload slot, i.e. a server on its way down.
        // Treating it as an error made `ensure_single_action_provider` — and
        // therefore `ActionServer::serve` — fail outright whenever a
        // just-stopped server's info queryable had not finished undeclaring,
        // which is exactly what a process that starts one action server after
        // another does. Same class as the status-key race, which was fixed on the
        // client side via `ReplyErrPolicy::Absent`; the info path was left
        // behind.
        Err(err) if reply_error_is_absent_snapshot(err) => Ok(ActionInfoProbe::TearingDown),
        Err(err) => Err(AortaError::internal(format!(
            "action '{}' info query returned error: {}",
            action_name,
            reply_error_text(err)
        ))),
    }
}

/// The action's published `ActionInfo`, or `None` when no usable info is
/// available — nobody answered, or the only answer came from a server that is
/// tearing down. Callers that must not race a teardown (server admission) use
/// [`probe_action_info`] instead.
pub async fn fetch_action_info(
    session: &Session,
    key_builder: &KeyExprBuilder,
    action_name: &str,
    timeout_duration: Duration,
) -> AortaResult<Option<ActionInfo>> {
    match probe_action_info(session, key_builder, action_name, timeout_duration).await? {
        ActionInfoProbe::Found(info) => Ok(Some(*info)),
        ActionInfoProbe::Absent | ActionInfoProbe::TearingDown => Ok(None),
    }
}

/// Admission check for a new server on `action_name`.
///
/// A `TearingDown` probe is neither "free" nor a permanent conflict: the old
/// server's info payload is already cleared, but its `send_goal` queryable can
/// still be declared and `handle_send_goal` does not reject a stopping server.
/// Admitting a replacement in that window would let one `QueryTarget::All`
/// goal reach BOTH providers, breaking the single-active-provider contract
/// (one active server per group and action) after handlers may already have
/// started side effects. So we WAIT for the teardown to
/// finish — the queryable disappearing is the signal — and fail closed if it
/// never does.
pub(crate) async fn ensure_single_action_provider(
    session: &Session,
    key_builder: &KeyExprBuilder,
    action_name: &str,
    timeout_duration: Duration,
) -> AortaResult<()> {
    let teardown_deadline =
        tokio::time::Instant::now() + Duration::from_millis(ACTION_PROVIDER_TEARDOWN_WAIT_MS);
    loop {
        match probe_action_info(session, key_builder, action_name, timeout_duration).await? {
            ActionInfoProbe::Absent => return Ok(()),
            ActionInfoProbe::Found(existing) => {
                return Err(AortaError::concurrent_conflict(format!(
                    "action '{}' already has an active server on node '{}' (epoch={})",
                    action_name, existing.server_node_name, existing.server_epoch
                )));
            }
            ActionInfoProbe::TearingDown => {
                if tokio::time::Instant::now() >= teardown_deadline {
                    return Err(AortaError::concurrent_conflict(format!(
                        "action '{}' still has a server tearing down after {}ms; its control \
                         queryables may still accept goals, so a replacement was not admitted",
                        action_name, ACTION_PROVIDER_TEARDOWN_WAIT_MS
                    )));
                }
                tokio::time::sleep(Duration::from_millis(ACTION_PROVIDER_TEARDOWN_POLL_MS)).await;
            }
        }
    }
}

fn duration_to_millis_u64(value: Duration) -> AortaResult<u64> {
    u64::try_from(value.as_millis()).map_err(|_| {
        AortaError::invalid_argument(format!(
            "duration {:?} exceeds supported action millisecond range",
            value
        ))
    })
}

/// The STOP-path join for a goal HANDLER task.
/// Like [`wait_for_task`], but a timeout must RECLAIM, not detach: abort
/// the outer driver (as before) **and** the inner handler task (dropping
/// the driver's JoinHandle only detaches it), then cancel the goal scope
/// (idempotent) and bounded-await its close so every sub-task spawned on
/// `ctx.scope()` is accounted before stop returns.
async fn wait_for_handler_task<TAction: ActionWire + Send + Sync + 'static>(
    task: JoinHandle<()>,
    goal: Arc<GoalRecord<TAction>>,
    timeout_duration: Duration,
    action_name: &str,
) {
    let abort_handle = task.abort_handle();
    let timed_out = tokio::time::timeout(timeout_duration, task).await.is_err();
    if timed_out {
        abort_handle.abort();
        if let Some(inner) = goal
            .handler_abort
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            inner.abort();
        }
    }
    // Both arms: the goal scope must not
    // outlive server stop even when the handler task ended WITHOUT the
    // timeout (e.g. aborted elsewhere during stop) — its sub-tasks were
    // previously abandoned on that interleaving.
    goal.scope.cancel(crate::flow::CancelReason::Shutdown);
    let drained = tokio::time::timeout(Duration::from_millis(500), goal.scope.close())
        .await
        .is_ok();
    if timed_out || !drained {
        aorta_core_warn!(
            "Aorta WARN: action '{}' handler stop {}; goal scope {}",
            action_name,
            if timed_out {
                format!("timed out after {timeout_duration:?}; aborted (inner task included)")
            } else {
                "completed but left goal-scope children".to_string()
            },
            if drained {
                "drained"
            } else {
                "still has children after 500ms bounded close"
            }
        );
    }
}
