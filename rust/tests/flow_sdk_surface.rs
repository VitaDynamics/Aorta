//! SDK-surface acceptance: an app depending ONLY on `aorta_sdk`
//! (prelude included) can name every Flow concept — no `aorta_core` import.
//!
//! Beyond nameability, this file carries the SDK-layer BEHAVIORAL coverage
//! for the Flow surface: `Client::call_in`,
//! `Node::root_scope`, `Node::watch`, `Node::poll_fd_in`, and the
//! `GoalHandle::into_guard` / `GoalGuard` lifecycle. Everything runs
//! in-process (same-session loopback) — no external peer. The one genuinely
//! peer-dependent path (a guard's drop-cancel round-tripping to a live action
//! server) is covered up to the wire-encode boundary; see `guard_probe`.

use std::time::{Duration, Instant};

use aorta_sdk::prelude::*;

/// Per-process unique resource names: `bazel test //...` runs many session-
/// opening test binaries concurrently on one host, and zenoh scouting can
/// join them into a single mesh.
fn unique_name(base: &str) -> String {
    format!("{base}_p{}", std::process::id())
}

// `Cancelled` is nameable in user signatures (F3).
#[allow(dead_code)]
struct HoldsCancelled {
    wait: Option<aorta_sdk::Cancelled>,
}

#[test]
fn flow_types_are_reachable_from_the_prelude() {
    // EventPriority is constructible (F1).
    let priority = EventPriority::High;
    assert_ne!(format!("{priority:?}"), "");

    // Core Flow vocabulary is nameable via the prelude (F2).
    let _reason: CancelReason = CancelReason::Explicit;
    let _budget: Budget = Budget::unlimited();
    let _opts: ScopeOpts = ScopeOpts::default();
    let _cond: Condition = Condition::MessageAvailable;
    fn _takes_scope(_s: &Scope) {}
    fn _takes_handle(_h: TaskHandle<()>) {}
    fn _takes_watch(_w: &Watch) {}

    // Return/field types of public APIs are NAMEABLE, not merely reachable
    // (surface-diet burr fix): `Scope::begin_external_child`,
    // `WindowSync::stats`, `Watch::snapshot`, `CancelReason::Unknown`,
    // `SubscriberOptions::readiness_wires` (a pub field, so its
    // type must be nameable, not merely "constructible but unusable").
    fn _takes_external_child(_c: ExternalChild) {}
    fn _takes_wires(_w: Vec<ReadinessWire>) {}
    // The sweep extends beyond Flow: subscriber/schema surface too.
    fn _takes_header_layout(_l: Option<AortaHeaderLayout>) {}
    fn _takes_runtime_stats(_s: SubscriberRuntimeStats) {}
    fn _takes_notify_hook(_h: Option<PullNotifyHook>) {}
    // Full recursive sweep: the error payload is
    // prelude-level; the event/executor PLUMBING types are crate-root
    // exports only (reachable via queue()/push_event/lane_hooks()/
    // Timer::new — nameable, but not advertised in the golden path).
    fn _takes_transport_error(_e: TransportError) {}
    fn _takes_event(_e: aorta_sdk::Event) {}
    fn _takes_event_kind(_k: aorta_sdk::EventKind) {}
    fn _takes_event_queue(_q: std::sync::Arc<aorta_sdk::EventQueue>) {}
    fn _takes_queue_opts(_o: aorta_sdk::EventQueueOptions) {}
    fn _takes_queue_sender(_s: aorta_sdk::EventQueueSender) {}
    fn _takes_inflight_guard(_g: aorta_sdk::InflightGuard) {}
    fn _takes_multi_ingress(_o: aorta_sdk::MultiIngressOptions) {}
    fn _takes_lane_hook_slot(_s: &aorta_sdk::LaneHookSlot) {}
    fn _takes_lane_hooks(_h: aorta_sdk::LaneHooks) {}
    fn _takes_forward_outcome(_o: aorta_sdk::ForwardOutcome) {}
    fn _takes_window_stats(_s: WindowStats) {}
    fn _takes_snapshot(_p: std::sync::Arc<SeqPayload>) {}
    fn _extracts_raw(reason: CancelReason) -> Option<RawCancelReason> {
        match reason {
            CancelReason::Unknown(raw) => Some(raw),
            _ => None,
        }
    }
    assert!(_extracts_raw(CancelReason::Explicit).is_none());

    // Combinator fns exist as prelude items.
    let _ = when_all::<()>;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_poll_fd_is_a_real_method() {
    use std::os::unix::io::{FromRawFd, OwnedFd};
    let node = Node::new("wp1_pollfd", "default").await.expect("node");
    let mut fds = [0i32; 2];
    // SAFETY: plain pipe(2); read end ownership transferred once below.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: transfers ownership of the read end exactly once.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let watch = node.poll_fd(read_end).await.expect("poll_fd");
    assert!(watch.load().is_none());
    // SAFETY: closes the write end we still own.
    unsafe { libc::close(fds[1]) };
    node.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// Client::call_in — the scoped RPC path.
// ---------------------------------------------------------------------------

/// Happy path: `call_in` under a budgeted scope round-trips against a
/// same-session echo service and yields the reply payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_call_in_round_trips_on_a_local_echo_service() {
    let node = Node::new("flow_surface_call_in", "default")
        .await
        .expect("node");
    let service_name = unique_name("flow_surface_echo");
    let _service = node
        .create_service(&service_name, |req| {
            let payload = req.payload().clone();
            tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .await
        .expect("service");

    let client = node.create_client(&service_name);
    // Bounded readiness gate instead of a magic discovery sleep; resolves in
    // milliseconds on a same-session queryable.
    let _ = client.wait_for_matching(Duration::from_secs(10)).await;

    let scope = node
        .root_scope()
        .child(ScopeOpts::named("call-in").budget(Duration::from_secs(10)));
    // Retry only guards a slow-CI matching race; the echo is idempotent.
    let deadline = Instant::now() + Duration::from_secs(10);
    let reply = loop {
        match client.call_in(&scope, b"ping".to_vec()).await {
            Ok(reply) => break reply,
            Err(stopped) => {
                assert!(
                    Instant::now() < deadline,
                    "call_in never round-tripped: {stopped}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    assert_eq!(reply.to_bytes().as_ref(), b"ping");

    node.close().await.expect("close");
}

/// A cancelled scope refuses `call_in` IMMEDIATELY (checkpoint, before the
/// wire) and reports the scope's own cancel reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_in_refuses_a_cancelled_scope_immediately() {
    let node = Node::new("flow_surface_call_in_cancel", "default")
        .await
        .expect("node");
    // Deliberately no provider anywhere: refusal must come from the scope,
    // not from the transport.
    let client = node.create_client(&unique_name("flow_surface_no_provider"));

    let scope = node.root_scope().child(ScopeOpts::named("doomed"));
    scope.cancel(CancelReason::Explicit);

    let stopped = client
        .call_in(&scope, b"never".to_vec())
        .await
        .expect_err("cancelled scope must refuse the call");
    assert_eq!(stopped, Stopped::cancelled(CancelReason::Explicit));
    assert!(stopped.is_cancel());
    assert!(!stopped.is_deadline());

    node.close().await.expect("close");
}

/// Budget expiry on the wire maps to the Flow vocabulary:
/// `Stopped::cancelled(DeadlineExceeded)` — not a fault, not an AortaError.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_in_maps_budget_expiry_to_a_deadline_stop() {
    let node = Node::new("flow_surface_call_in_deadline", "default")
        .await
        .expect("node");
    // No provider: the call cannot succeed, and both terminal paths (closed
    // reply channel or budget expiry) surface as AortaError::Timeout, which
    // call_in must map to a deadline Stop.
    let client = node.create_client(&unique_name("flow_surface_no_provider_ddl"));

    let scope = node
        .root_scope()
        .child(ScopeOpts::named("tiny-budget").budget(Duration::from_millis(200)));
    let stopped = client
        .call_in(&scope, b"late".to_vec())
        .await
        .expect_err("no provider + budget must stop the call");
    assert!(
        stopped.is_deadline(),
        "expected DeadlineExceeded, got: {stopped}"
    );

    node.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// Node::root_scope — the node-lifetime Flow root.
// ---------------------------------------------------------------------------

/// `root_scope()` hands out the SAME live root on every call, parents real
/// children, and `Node::close()` cancels the whole tree with `Shutdown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_root_scope_parents_children_and_close_cancels_it() {
    let node = Node::new("flow_surface_rootscope", "default")
        .await
        .expect("node");
    let root = node.root_scope();
    assert!(!root.is_cancelled());
    let again = node.root_scope();

    // A child spawned under the root runs to completion while the node lives.
    let child = root.child(ScopeOpts::named("root-child"));
    let task = child.spawn(async { 41 + 1 });
    assert_eq!(task.join().await, Ok(42));

    node.close().await.expect("close");
    assert!(
        root.is_cancelled(),
        "close() must cancel the node root scope"
    );
    assert_eq!(root.cancel_reason(), Some(CancelReason::Shutdown));
    // Both handles observe the cancellation (same root, not a fresh scope)…
    assert!(again.is_cancelled());
    // …and it cascades to derived children.
    assert!(child.is_cancelled(), "root cancel must cascade to children");
    assert_eq!(child.cancel_reason(), Some(CancelReason::Shutdown));
}

// ---------------------------------------------------------------------------
// Node::watch — the depth-1 latest-value cell.
// ---------------------------------------------------------------------------

/// A same-session publish lands in the watch: `load()` returns the LATEST
/// payload and the seq/stored counters advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_watch_surfaces_the_latest_published_value() {
    let node = Node::new("flow_surface_watch", "default")
        .await
        .expect("node");
    let topic = unique_name("flow_surface_watch_topic");
    let watch = node.watch(&topic).await.expect("watch");
    assert!(watch.load().is_none(), "fresh watch must be empty");
    assert_eq!(watch.stored_total(), 0);

    let publisher = node.create_publisher_raw(&topic).await.expect("publisher");
    // Bounded readiness gate; publish is fire-and-forget, so the loop below
    // re-sends in short waves rather than sleeping a magic discovery delay.
    let _ = publisher.wait_for_matching(Duration::from_secs(10)).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = watch.seq();
    let value = loop {
        assert!(
            Instant::now() < deadline,
            "watch never observed a same-session publish"
        );
        publisher.publish(b"latest".to_vec()).expect("publish");
        seen = tokio::time::timeout(Duration::from_millis(100), watch.changed(seen))
            .await
            .unwrap_or(seen);
        if let Some(value) = watch.load() {
            break value;
        }
    };
    assert_eq!(value.to_bytes().as_ref(), b"latest");
    assert!(watch.seq() >= 1);
    assert!(watch.stored_total() >= 1);

    node.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// Node::poll_fd_in — fd draining under an EXPLICIT scope.
// ---------------------------------------------------------------------------

/// `poll_fd_in` drains written bytes into the watch, and cancelling the
/// owning scope stops the reader (later writes are no longer observed).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_poll_fd_in_drains_bytes_and_stops_with_its_scope() {
    use std::os::unix::io::{FromRawFd, OwnedFd};
    let node = Node::new("flow_surface_pollfd_in", "default")
        .await
        .expect("node");
    let scope = node.root_scope().child(ScopeOpts::named("fd-reader"));

    let mut fds = [0i32; 2];
    // SAFETY: plain pipe(2); read end ownership transferred once below.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: transfers ownership of the read end exactly once.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let watch = node.poll_fd_in(read_end, &scope).await.expect("poll_fd_in");
    assert!(watch.load().is_none());

    // Bytes written to the fd surface as the watch's latest value.
    let payload = b"fd-bytes";
    let seen = watch.seq();
    // SAFETY: write(2) of a live local buffer to the pipe end we still own.
    let wrote = unsafe { libc::write(fds[1], payload.as_ptr().cast(), payload.len()) };
    assert_eq!(wrote, payload.len() as isize);
    let observed = tokio::time::timeout(Duration::from_secs(5), watch.changed(seen))
        .await
        .expect("fd bytes must reach the watch");
    let snap = watch.snapshot().expect("stored value");
    assert_eq!(snap.seq, observed);
    assert_eq!(snap.payload.to_bytes().as_ref(), payload);

    // The reader is accounted by — and cancelled with — the EXPLICIT
    // scope. On cancel it exits promptly and closes the read end it owns, so
    // the write side deterministically reports EPIPE once the reader is gone
    // (bounded poll — the exit is prompt but not atomic with cancel()).
    scope.cancel(CancelReason::Explicit);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // SAFETY: write(2) of a live local buffer to the pipe end we still own.
        let wrote = unsafe { libc::write(fds[1], payload.as_ptr().cast(), payload.len()) };
        if wrote < 0 {
            let err = std::io::Error::last_os_error();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EPIPE),
                "expected EPIPE from the closed read end, got: {err}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fd reader kept the pipe open after scope cancel"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // With the reader gone the watch is frozen — no further stores.
    let final_seq = watch.seq();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(watch.seq(), final_seq, "no stores after the reader exited");

    // SAFETY: closes the write end we still own.
    unsafe { libc::close(fds[1]) };
    node.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// GoalHandle::into_guard / GoalGuard — cancel-on-drop lifecycle (Flow Concepts §6.2).
//
// A FULL exercise (drop-cancel round-tripping to a server that aborts the
// goal) needs a live action server speaking a real wire schema; that is
// integration-tested elsewhere. Here the SDK-layer semantics are pinned
// without a peer: `guard_probe` counts `encode_cancel_goal_request` calls —
// the first synchronous step of `GoalHandle::cancel()` — so the tests can
// PROVE whether a guard transition spawned the detached cancel or not.
// ---------------------------------------------------------------------------

mod guard_probe {
    use aorta_sdk::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Number of `encode_cancel_goal_request` calls observed process-wide.
    /// `GoalHandle::cancel()` hits this synchronously before any network op,
    /// so it is the observable drop-cancel boundary without a live server.
    pub static CANCEL_ENCODES: AtomicUsize = AtomicUsize::new(0);

    pub fn cancel_encodes() -> usize {
        CANCEL_ENCODES.load(Ordering::SeqCst)
    }

    fn not_exercised<T>(what: &str) -> AortaResult<T> {
        Err(AortaError::internal(format!(
            "guard probe action: {what} needs a live action server; not exercised here"
        )))
    }

    pub struct ProbeSchema;

    impl SchemaMeta for ProbeSchema {
        fn schema_bfbs() -> &'static [u8] {
            b"flow-sdk-surface-guard-probe"
        }

        fn root_type() -> Option<&'static str> {
            Some("flow.surface.GuardProbe")
        }
    }

    // Required so `decode_goal_from_send_goal_request`'s trait-mandated
    // `Self::Goal: TypedSubscribeMessage` bound is satisfiable on a concrete
    // impl (trivially-false where-clauses are rejected, rust#48214). The
    // decode itself is never exercised — no goal is ever sent.
    impl TypedSubscribeMessage for ProbeSchema {
        type Root<'a> = &'a [u8];

        fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
            Ok(payload)
        }
    }

    pub struct ProbeAction;

    impl ActionSchema for ProbeAction {
        type Goal = ProbeSchema;
        type Result = ProbeSchema;
        type FeedbackData = ProbeSchema;
        type SendGoalRequest = ProbeSchema;
        type SendGoalResponse = ProbeSchema;
        type CancelGoalRequest = ProbeSchema;
        type CancelGoalResponse = ProbeSchema;
        type Feedback = ProbeSchema;
        type PublicStatus = ProbeSchema;
        type GetResultRequest = ProbeSchema;
        type GetResultResponse = ProbeSchema;

        const ACTION_NAME: &'static str = "flow_surface_guard_probe";
    }

    impl ActionWire for ProbeAction {
        type GoalMsg<'a> = ();
        type OwnedFeedbackDataMsg = ();
        type OwnedResultMsg = ();
        type ResultMsg<'a> = ();
        type FeedbackDataMsg<'a> = ();

        fn encode_send_goal_request(
            _goal_id: &str,
            _goal: &Self::GoalMsg<'_>,
        ) -> AortaResult<Vec<u8>> {
            not_exercised("encode_send_goal_request")
        }

        fn decode_send_goal_request_goal_id(_payload: &[u8]) -> AortaResult<String> {
            not_exercised("decode_send_goal_request_goal_id")
        }

        fn decode_goal_from_send_goal_request<'a>(
            _payload: &'a [u8],
        ) -> AortaResult<<Self::Goal as TypedSubscribeMessage>::Root<'a>>
        where
            Self::Goal: TypedSubscribeMessage,
        {
            not_exercised("decode_goal_from_send_goal_request")
        }

        fn encode_send_goal_response(_response: &ActionSendGoalResponse) -> AortaResult<Vec<u8>> {
            not_exercised("encode_send_goal_response")
        }

        fn decode_send_goal_response(_payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
            not_exercised("decode_send_goal_response")
        }

        fn encode_cancel_goal_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>> {
            CANCEL_ENCODES.fetch_add(1, Ordering::SeqCst);
            Ok(format!("{goal_id}|{goal_secret}").into_bytes())
        }

        fn decode_cancel_goal_request(_payload: &[u8]) -> AortaResult<(String, String)> {
            not_exercised("decode_cancel_goal_request")
        }

        fn encode_cancel_goal_response(
            _response: &ActionCancelGoalResponse,
        ) -> AortaResult<Vec<u8>> {
            not_exercised("encode_cancel_goal_response")
        }

        fn decode_cancel_goal_response(_payload: &[u8]) -> AortaResult<ActionCancelGoalResponse> {
            not_exercised("decode_cancel_goal_response")
        }

        fn encode_get_result_request(_goal_id: &str, _goal_secret: &str) -> AortaResult<Vec<u8>> {
            not_exercised("encode_get_result_request")
        }

        fn decode_get_result_request(_payload: &[u8]) -> AortaResult<(String, String)> {
            not_exercised("decode_get_result_request")
        }

        fn clone_feedback_data_owned(
            _data: &Self::FeedbackDataMsg<'_>,
        ) -> AortaResult<Self::OwnedFeedbackDataMsg> {
            Ok(())
        }

        fn clone_result_owned(_result: &Self::ResultMsg<'_>) -> AortaResult<Self::OwnedResultMsg> {
            Ok(())
        }

        fn encode_get_result_response(
            _response: &ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>,
        ) -> AortaResult<Vec<u8>> {
            not_exercised("encode_get_result_response")
        }

        fn decode_get_result_response(
            _payload: &[u8],
        ) -> AortaResult<ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>>
        {
            not_exercised("decode_get_result_response")
        }

        fn encode_feedback(
            _goal_id: &str,
            _seq: u64,
            _stamp_ns: u64,
            _status: ActionStatus,
            _data: &Self::FeedbackDataMsg<'_>,
            _message: Option<&str>,
        ) -> AortaResult<Vec<u8>> {
            not_exercised("encode_feedback")
        }

        fn decode_feedback_goal_id(_payload: &[u8]) -> AortaResult<String> {
            not_exercised("decode_feedback_goal_id")
        }

        fn encode_feedback_data(_data: &Self::FeedbackDataMsg<'_>) -> AortaResult<Vec<u8>> {
            not_exercised("encode_feedback_data")
        }

        fn encode_result(_result: &Self::ResultMsg<'_>) -> AortaResult<Vec<u8>> {
            not_exercised("encode_result")
        }

        fn encode_public_status(_status: &ActionPublicStatus) -> AortaResult<Vec<u8>> {
            not_exercised("encode_public_status")
        }

        fn decode_public_status(_payload: &[u8]) -> AortaResult<ActionPublicStatus> {
            not_exercised("decode_public_status")
        }
    }
}

/// One sequential test (the probe counter is process-global) covering the
/// guard lifecycle: persisted-handle validation, identity through
/// `into_guard`/`handle()`, `disarm` (no cancel), armed drop INSIDE a runtime
/// (spawns the detached cancel), and armed drop OUTSIDE a runtime
/// (WARN-and-skip, no cancel, no panic).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goal_guard_into_guard_disarm_and_drop_semantics() {
    use guard_probe::{cancel_encodes, ProbeAction};

    const GOAL_ID: &str = "6f9619ff-8b86-d011-b42d-00cf4fc964ff";
    const SECRET: &str = "persisted-secret";

    let node = Node::new("flow_surface_guard", "default")
        .await
        .expect("node");
    let client = ActionClient::<ProbeAction>::new(
        &node,
        ProbeAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    // from_persisted validation (the no-server entry point into GoalHandle).
    assert!(
        GoalHandle::from_persisted(&client, "not-a-uuid", SECRET).is_err(),
        "goal_id must be a UUID"
    );
    assert!(
        GoalHandle::from_persisted(&client, GOAL_ID, "  ").is_err(),
        "goal_secret must not be blank"
    );

    // into_guard preserves goal identity; handle() is a usable owned view
    // while the guard stays armed (and dropping that VIEW never cancels).
    let handle = GoalHandle::from_persisted(&client, GOAL_ID, SECRET).expect("persisted handle");
    let guard = handle.into_guard();
    assert_eq!(guard.goal_id(), GOAL_ID);
    let view = guard.handle();
    assert_eq!(view.goal_id(), GOAL_ID);
    assert_eq!(view.goal_secret(), SECRET);
    drop(view);

    // disarm() hands the goal back WITHOUT cancelling.
    let handle = guard.disarm();
    assert_eq!(handle.goal_id(), GOAL_ID);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(cancel_encodes(), 0, "disarm must not spawn a cancel");

    // Dropping an ARMED guard inside a tokio runtime spawns the detached
    // best-effort cancel (observed at the encode boundary; the wire attempt
    // then fails harmlessly — there is no server).
    drop(handle.into_guard());
    let deadline = Instant::now() + Duration::from_secs(5);
    while cancel_encodes() < 1 {
        assert!(
            Instant::now() < deadline,
            "armed guard drop never spawned the cancel"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(cancel_encodes(), 1);

    // Dropping an ARMED guard OUTSIDE a tokio runtime cannot spawn: the
    // documented behavior is WARN-and-skip — no cancel, no panic.
    let guard = GoalHandle::from_persisted(&client, GOAL_ID, SECRET)
        .expect("persisted handle")
        .into_guard();
    std::thread::spawn(move || drop(guard))
        .join()
        .expect("outside-runtime drop must not panic");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cancel_encodes(),
        1,
        "outside-runtime drop must skip the cancel"
    );

    node.close().await.expect("close");
}
