use std::marker::PhantomData;
use std::time::Duration;

use aorta_core::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionInfo, ActionPublicStatus, ActionWire,
};
use aorta_core::action_client::{
    ActionClient as CoreActionClient, ActionFeedbackSample as CoreActionFeedbackSample,
    ActionFeedbackStream as CoreActionFeedbackStream, GoalGuard as CoreGoalGuard,
    GoalHandle as CoreGoalHandle,
};
pub use aorta_core::action_client::{
    ActionClientOptions, FeedbackOverflowPolicy, DEFAULT_ACTION_FEEDBACK_CAPACITY,
};
use aorta_core::action_server::{
    fetch_action_info as fetch_action_info_core, ActionServer as CoreActionServer,
    DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
};
pub use aorta_core::action_server::{
    ActionExecutionContext, ActionServerOptions, RetainedActionGoal,
};
use aorta_core::Target;

use crate::TypedSubscribeMessage;
use crate::{AortaResult, Node};

/// Action **server** for a typed action schema `TAction`: serves
/// long-running, cancellable goals with progress feedback.
///
/// Created via [`serve`](Self::serve) (with a goal handler) or
/// [`serve_info_only`](Self::serve_info_only) (declares the action without
/// executing goals — probing/placeholder). At most one active server per
/// `{group, action}` is allowed.
///
/// Dropping an `ActionServer` stops it; bind it for the serving lifetime.
#[must_use = "dropping an ActionServer stops it; bind it for the serving lifetime"]
pub struct ActionServer<TAction: ActionWire + Send + Sync + 'static> {
    inner: CoreActionServer<TAction>,
}

impl<TAction: ActionWire + Send + Sync + 'static> ActionServer<TAction> {
    /// Declare the action's info/liveliness only — no execution handler.
    /// Useful for probes or for an orchestrator that reserves the action
    /// name without running goals.
    pub async fn serve_info_only(
        node: &Node,
        action_name: &str,
        options: ActionServerOptions,
    ) -> AortaResult<Self> {
        let inner = CoreActionServer::serve_info_only(node.inner(), action_name, options).await?;
        Ok(Self { inner })
    }

    /// Serve the action with a goal handler.
    ///
    /// `handler` receives the retained goal (see
    /// [`RetainedActionGoal`]) and an [`ActionExecutionContext`]. Terminal
    /// semantics: the **first** terminal call (`succeed` / `abort` /
    /// `cancel`) wins and later ones are no-ops; a handler that returns
    /// without setting a terminal state (or returns `Err`, or panics) is
    /// auto-aborted by the server. Cancellation is cooperative — poll
    /// `ctx.is_cancel_requested()` or await `ctx.cancelled()` in the long
    /// task loop.
    ///
    /// ```ignore
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let server = ActionServer::<ExecuteTaskAction>::serve(
    ///     node,
    ///     ExecuteTaskAction::ACTION_NAME,
    ///     ActionServerOptions::default(),
    ///     |goal, ctx: ActionExecutionContext<ExecuteTaskAction>| async move {
    ///         let view = goal.decode()?;
    ///         let _ = view.source().unwrap_or_default();
    ///         ctx.succeed(&ExecuteTaskResultMsg { /* ... */ }).await
    ///     },
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See `rust/examples/cli_execute_task_server.rs` for the full example.
    pub async fn serve<F, Fut>(
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
        Fut: std::future::Future<Output = AortaResult<()>> + Send + 'static,
    {
        let inner =
            CoreActionServer::serve_with_handler(node.inner(), action_name, options, handler)
                .await?;
        Ok(Self { inner })
    }

    /// Static discovery: query whether an active server exists for
    /// `action_name` and return its [`ActionInfo`]. Uses the default query
    /// timeout ([`DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS`]).
    pub async fn query_info(node: &Node, action_name: &str) -> AortaResult<Option<ActionInfo>> {
        Self::query_info_with_timeout(
            node,
            action_name,
            Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
        )
        .await
    }

    /// [`query_info`](Self::query_info) with an explicit timeout.
    pub async fn query_info_with_timeout(
        node: &Node,
        action_name: &str,
        timeout: Duration,
    ) -> AortaResult<Option<ActionInfo>> {
        fetch_action_info_core(
            node.inner().session(),
            node.inner().key_builder(),
            action_name,
            timeout,
        )
        .await
    }

    /// The action name this server is bound to.
    pub fn action_name(&self) -> &str {
        self.inner.action_name()
    }

    /// This server's declared [`ActionInfo`].
    pub fn info(&self) -> &ActionInfo {
        self.inner.info()
    }

    /// Stop serving and wait for the background task to exit, bounded by
    /// `timeout`. Consumes the server.
    pub async fn stop_and_join(self, timeout: Duration) {
        self.inner.stop_and_join(timeout).await;
    }

    /// Request the server to stop (non-blocking).
    pub fn stop(&mut self) {
        self.inner.stop();
    }
}

/// Action **client** for a typed action schema `TAction`: sends goals to an
/// action server and tracks them via [`GoalHandle`].
///
/// Created directly (not through [`Node`]):
///
/// ```ignore
/// use aorta_sdk::prelude::*;
///
/// # async fn demo(node: &Node) -> AortaResult<()> {
/// let client = ActionClient::<ExecuteTaskAction>::new(
///     node,
///     ExecuteTaskAction::ACTION_NAME,
///     ActionClientOptions::default(),
/// );
/// let handle = client.send_goal(&ExecuteTaskGoalMsg { /* ... */ }).await?;
/// let result = handle.wait_result(Some(std::time::Duration::from_secs(10))).await?;
/// # Ok(())
/// # }
/// ```
///
/// See `rust/examples/cli_execute_task_client.rs` for the full example.
pub struct ActionClient<TAction: ActionWire> {
    inner: CoreActionClient<TAction>,
}

impl<TAction: ActionWire> ActionClient<TAction> {
    /// # Panics
    ///
    /// Panics on a malformed `action_name`. Prefer [`ActionClient::try_new`],
    /// which returns the error instead — matching the C++ (`StatusOr`) and
    /// Python (raises `AortaError`) SDKs for identical bad input.
    pub fn new(node: &Node, action_name: &str, options: ActionClientOptions) -> Self {
        Self {
            inner: CoreActionClient::new(node.inner(), action_name, options),
        }
    }

    /// Fallible constructor: returns `InvalidArgument` on a malformed
    /// `action_name` instead of panicking.
    pub fn try_new(
        node: &Node,
        action_name: &str,
        options: ActionClientOptions,
    ) -> AortaResult<Self> {
        Ok(Self {
            inner: CoreActionClient::try_new(node.inner(), action_name, options)?,
        })
    }

    /// Create an action client targeting a specific robot namespace (e.g. a
    /// robot SN) for cross-namespace cloud addressing. Every action key is
    /// built as `{namespace}/aorta/{group}/act/...`. A wildcard namespace is
    /// rejected — an action must target a single robot. See the Cloud
    /// Cross-Namespace Addressing Proposal §5.
    pub fn new_for(
        node: &Node,
        namespace: Target,
        action_name: &str,
        mut options: ActionClientOptions,
    ) -> AortaResult<Self> {
        options.namespace = Some(namespace.as_str().to_string());
        Ok(Self {
            inner: CoreActionClient::try_new(node.inner(), action_name, options)?,
        })
    }

    /// The action name this client targets.
    pub fn action_name(&self) -> &str {
        self.inner.action_name()
    }

    /// Send a typed goal and get a [`GoalHandle`] for tracking it.
    ///
    /// Failure modes: with no reachable server, the client retries until
    /// its control timeout and returns `Timeout`; a reachable server that
    /// is at `max_concurrency` rejects with `Busy`; conflicting goal ids,
    /// malformed payloads, and transport failures map to their own
    /// [`AortaError`](crate::AortaError) variants.
    pub async fn send_goal(&self, goal: &TAction::GoalMsg<'_>) -> AortaResult<GoalHandle<TAction>> {
        Ok(GoalHandle {
            inner: self.inner.send_goal(goal).await?,
            _marker: PhantomData,
        })
    }

    /// Mint a fresh goal id in the canonical format
    /// [`send_goal_with_id`](Self::send_goal_with_id) accepts (hyphenated
    /// UUID v4). Pre-generate one to pair
    /// [`feedback_stream_for`](Self::feedback_stream_for) with
    /// `send_goal_with_id` (subscribe-before-send). Matches the C++ SDK's
    /// `GenerateGoalId`.
    pub fn generate_goal_id() -> String {
        CoreActionClient::<TAction>::generate_goal_id()
    }

    /// Send a goal with a caller-chosen `goal_id` (idempotent/replayable
    /// sends).
    pub async fn send_goal_with_id(
        &self,
        goal_id: &str,
        goal: &TAction::GoalMsg<'_>,
    ) -> AortaResult<GoalHandle<TAction>> {
        Ok(GoalHandle {
            inner: self.inner.send_goal_with_id(goal_id, goal).await?,
            _marker: PhantomData,
        })
    }

    /// Query whether an active server exists for this action and return its
    /// [`ActionInfo`].
    pub async fn info(&self) -> AortaResult<Option<ActionInfo>> {
        self.inner.info().await
    }

    /// Query the public status of a goal by id (out-of-band — no
    /// [`GoalHandle`] needed).
    pub async fn query_status(&self, goal_id: &str) -> AortaResult<Option<ActionPublicStatus>> {
        self.inner.query_status(goal_id).await
    }

    /// Request cancellation of a goal by id + secret (out-of-band).
    /// Resolves once the request is acknowledged, not when the goal
    /// reaches a terminal state.
    pub async fn cancel(
        &self,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<ActionCancelGoalResponse> {
        self.inner.cancel(goal_id, goal_secret).await
    }

    /// Fetch a goal's terminal result by id + secret (out-of-band, e.g.
    /// after a process restart).
    pub async fn get_result(
        &self,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        self.inner.get_result(goal_id, goal_secret).await
    }

    /// Open a goal-scoped feedback stream for `goal_id` **without** a
    /// [`GoalHandle`] — pair it with [`send_goal_with_id`] and subscribe
    /// *before* sending, so feedback the server publishes as its first
    /// handler action (e.g. a business "accepted" frame) can never race the
    /// subscription. A plain `send_goal` + `GoalHandle::feedback_stream`
    /// leaves a window where such early samples are lost, since topic
    /// subscriptions do not replay.
    ///
    /// Like [`GoalHandle::feedback_stream`], the stream ends itself once
    /// this client observes the goal's terminal result (via `get_result` /
    /// `wait_result`): `recv()` drains buffered samples, then returns
    /// `None`.
    ///
    /// [`send_goal_with_id`]: ActionClient::send_goal_with_id
    pub async fn feedback_stream_for(
        &self,
        goal_id: &str,
    ) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        Ok(ActionFeedbackStream {
            inner: self.inner.feedback_stream_for(goal_id).await?,
            _marker: PhantomData,
        })
    }

    /// Subscribe to the feedback stream of **all** goals of this action
    /// (not limited to one goal). Never auto-closes; it is not goal-scoped.
    pub async fn feedback_stream_all(&self) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        Ok(ActionFeedbackStream {
            inner: self.inner.feedback_stream_all().await?,
            _marker: PhantomData,
        })
    }
}

/// One feedback sample of an action, delivered by
/// [`ActionFeedbackStream::recv`]. The payload is owned (copied out of the
/// transport buffer) and can be decoded via [`decode`](Self::decode).
pub struct ActionFeedbackSample<TAction: ActionWire>
where
    <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    inner: CoreActionFeedbackSample<TAction>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire> ActionFeedbackSample<TAction>
where
    <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    /// The raw (still-encoded) feedback payload bytes.
    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    /// Decode the feedback into its typed root view.
    pub fn decode(
        &self,
    ) -> AortaResult<
        <<TAction as aorta_core::action::ActionSchema>::Feedback as TypedSubscribeMessage>::Root<
            '_,
        >,
    > {
        self.inner.decode()
    }
}

/// Async stream of action feedback samples, obtained from
/// [`GoalHandle::feedback_stream`] (one goal) or
/// [`ActionClient::feedback_stream_all`] (all goals of an action).
///
/// The stream is queue-backed; samples are dropped (and counted) when the
/// consumer falls behind — poll [`dropped_count`](Self::dropped_count).
pub struct ActionFeedbackStream<TAction: ActionWire>
where
    <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    inner: CoreActionFeedbackStream<TAction>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire> ActionFeedbackStream<TAction>
where
    <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
{
    /// Await the next feedback sample. Returns `None` when the stream is
    /// closed. Errors are stream-level (envelope/goal-id validation,
    /// transport); typed payload decode errors surface on the sample's
    /// [`decode`](ActionFeedbackSample::decode) instead.
    pub async fn recv(&mut self) -> Option<AortaResult<ActionFeedbackSample<TAction>>> {
        self.inner.recv().await.map(|result| {
            result.map(|sample| ActionFeedbackSample {
                inner: sample,
                _marker: PhantomData,
            })
        })
    }

    /// Total number of feedback samples dropped due to queue overflow since
    /// this stream was created. Surfaced for diagnostics; consumers can poll
    /// it to detect that they are falling behind the producer.
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped_count()
    }

    /// Close the stream and release its subscription, consuming it.
    pub async fn close(self) -> AortaResult<()> {
        self.inner.close().await
    }
}

/// A handle to one in-flight goal, returned by
/// [`ActionClient::send_goal`].
///
/// Holds the goal's identity (`goal_id`, `goal_secret`, `server_epoch`)
/// and provides status queries, cancellation, feedback streaming, and
/// terminal-result waits. The SDK handle itself is **not `Clone`** — for
/// an independently owned observation point, use
/// [`observe_result`](Self::observe_result) or
/// [`GoalGuard::handle`].
///
/// # Lifetime semantics
///
/// Dropping a plain `GoalHandle` **never cancels** the goal — cancellation
/// is opt-in via [`into_guard`](Self::into_guard). Keep a handle (or a
/// [`ResultObserver`]) alive as long as you want to observe the goal; the
/// server keeps executing regardless.
pub struct GoalHandle<TAction: ActionWire> {
    inner: CoreGoalHandle<TAction>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire> GoalHandle<TAction> {
    /// Restore a handle to a previously persisted goal (e.g. after a
    /// restart) from `goal_id` + `goal_secret`.
    pub fn from_persisted(
        client: &ActionClient<TAction>,
        goal_id: &str,
        goal_secret: &str,
    ) -> AortaResult<Self> {
        Ok(Self {
            inner: CoreGoalHandle::from_persisted(&client.inner, goal_id, goal_secret)?,
            _marker: PhantomData,
        })
    }

    /// The goal's id.
    pub fn goal_id(&self) -> &str {
        self.inner.goal_id()
    }

    /// The goal's secret (needed for out-of-band operations).
    pub fn goal_secret(&self) -> &str {
        self.inner.goal_secret()
    }

    /// The server epoch this goal was accepted in (used to detect server
    /// restarts).
    pub fn server_epoch(&self) -> &str {
        self.inner.server_epoch()
    }

    /// Query the goal's current public status.
    pub async fn query_status(&self) -> AortaResult<Option<ActionPublicStatus>> {
        self.inner.query_status().await
    }

    /// Request cancellation of this goal. Cooperative: the server decides
    /// when to honor it. The future resolves once the cancellation request
    /// is **acknowledged** (the goal enters `Canceling`), not when the goal
    /// reaches a terminal state — confirm the final outcome via
    /// [`wait_result`](Self::wait_result) or [`query_status`](Self::query_status).
    pub async fn cancel(&self) -> AortaResult<ActionCancelGoalResponse> {
        self.inner.cancel().await
    }

    /// Fetch the goal's terminal result (if already terminal).
    pub async fn get_result(
        &self,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        self.inner.get_result().await
    }

    /// Open the feedback stream of this goal.
    pub async fn feedback_stream(&self) -> AortaResult<ActionFeedbackStream<TAction>>
    where
        <TAction as aorta_core::action::ActionSchema>::Feedback: TypedSubscribeMessage,
        TAction: 'static,
    {
        Ok(ActionFeedbackStream {
            inner: self.inner.feedback_stream().await?,
            _marker: PhantomData,
        })
    }

    /// Wait (optionally bounded) for the goal's terminal result, consuming
    /// the observer created internally. `None` means no caller-imposed
    /// deadline — the wait still ends early with an error if the server
    /// dies, transport fails, or the result cannot be decoded.
    pub async fn wait_result(
        &self,
        timeout: Option<Duration>,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        // One-liner over the observer: block the current task on the terminal
        // result. Same event-driven mechanism as before; the extra handle clone
        // is an Arc bump + a few String copies, negligible beside the network
        // round-trip.
        self.observe_result(timeout).recv().await
    }

    /// Cheap, lazy view of this goal's terminal result. Internally holds
    /// its own copy of the goal identity (an `Arc` bump + a few `String`
    /// copies) — no background thread, no executor slot, and no zenoh
    /// subscriber until [`ResultObserver::recv`] is awaited. Use it to
    /// decouple *where* you await the result from where the goal was sent
    /// (e.g. move it into a `tokio::select!` arm or hand it to another
    /// task).
    pub fn observe_result(&self, timeout: Option<Duration>) -> ResultObserver<TAction> {
        ResultObserver {
            inner: self.inner.clone(),
            timeout,
            _marker: PhantomData,
        }
    }

    /// Fire-and-forget terminal-result notification. Spawns a detached task on
    /// the current tokio runtime that awaits the result and invokes `cb` exactly
    /// once with the outcome (`Ok` terminal result, or `Err` on timeout /
    /// transport / decode failure). Returns immediately without blocking the
    /// caller. Must be called from within a tokio runtime context.
    pub fn on_result<F>(&self, timeout: Option<Duration>, cb: F)
    where
        F: FnOnce(
                AortaResult<
                    ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>,
                >,
            ) + Send
            + 'static,
        TAction: Send + Sync + 'static,
        TAction::OwnedFeedbackDataMsg: Send,
        TAction::OwnedResultMsg: Send,
    {
        let observer = self.observe_result(timeout);
        tokio::spawn(async move {
            cb(observer.recv().await);
        });
    }
}

impl<TAction: ActionWire + Send + Sync + 'static> GoalHandle<TAction> {
    /// Arm cancel-on-drop: wrap this handle in a [`GoalGuard`]. Auto-cancel is
    /// opt-in policy — a plain `GoalHandle` drop never cancels the goal.
    pub fn into_guard(self) -> GoalGuard<TAction> {
        GoalGuard {
            inner: self.inner.into_guard(),
        }
    }
}

/// Cancel-on-drop wrapper around a [`GoalHandle`], created by
/// [`GoalHandle::into_guard`]. Dropping an armed guard spawns a detached
/// best-effort cancel on the ambient tokio runtime (WARN-and-skip outside
/// one); [`disarm`](Self::disarm) hands the goal back without cancelling and
/// [`cancel`](Self::cancel) is the awaited, checked form. The drop semantics
/// live in the core guard; this is a thin SDK view over it.
pub struct GoalGuard<TAction: ActionWire>
where
    TAction: Send + Sync + 'static,
{
    inner: CoreGoalGuard<TAction>,
}

impl<TAction: ActionWire + Send + Sync + 'static> GoalGuard<TAction> {
    /// The guarded goal's id.
    pub fn goal_id(&self) -> &str {
        self.inner.handle().goal_id()
    }

    /// Cheap owned view of the guarded handle (an `Arc` bump + identity copy)
    /// for feedback/result composition while the guard stays armed.
    pub fn handle(&self) -> GoalHandle<TAction> {
        GoalHandle {
            inner: self.inner.handle().clone(),
            _marker: PhantomData,
        }
    }

    /// Release the goal WITHOUT cancelling (hand-over shape).
    pub fn disarm(self) -> GoalHandle<TAction> {
        GoalHandle {
            inner: self.inner.disarm(),
            _marker: PhantomData,
        }
    }

    /// Awaited, checked cancel (consumes the guard). Resolves once the
    /// cancellation request is acknowledged — confirm the terminal outcome
    /// via the returned response / a subsequent status query.
    pub async fn cancel(self) -> AortaResult<ActionCancelGoalResponse> {
        self.inner.cancel().await
    }
}

/// Lazy, owned view of a goal's terminal result, created by
/// [`GoalHandle::observe_result`]. Holds its own clone of the goal handle, so it
/// is independent of the originating handle's lifetime and can be moved into
/// another task or `select!` arm. Does no work until [`recv`](Self::recv) is
/// awaited.
pub struct ResultObserver<TAction: ActionWire> {
    inner: CoreGoalHandle<TAction>,
    timeout: Option<Duration>,
    _marker: PhantomData<TAction>,
}

impl<TAction: ActionWire> ResultObserver<TAction> {
    /// Await the goal's terminal result, consuming the observer. Backed by the
    /// same event-driven `wait_result` mechanism (status-key subscription +
    /// terminal `get_result`).
    pub async fn recv(
        self,
    ) -> AortaResult<ActionGetResultResponse<TAction::OwnedFeedbackDataMsg, TAction::OwnedResultMsg>>
    {
        self.inner.wait_result(self.timeout).await
    }
}
