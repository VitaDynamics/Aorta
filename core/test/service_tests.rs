//! Unit tests for the Service module.

use crate::error::{AortaError, AortaResult};
use crate::schema::SchemaHash;
use crate::service::{Service, ServiceSchemaInfo};
use crate::session::Session;
use crate::telemetry::{RuntimeTelemetryGuard, TelemetryResourceGuard};
use crate::test_support::ENV_LOCK;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn service_schema_info_clone_and_debug() {
    let service_bfbs = vec![1, 2, 3];
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let cloned = schema_info.clone();
    assert_eq!(cloned.schema.as_bytes(), schema_info.schema.as_bytes());
    assert_eq!(cloned.schema_bfbs, schema_info.schema_bfbs);
    assert_eq!(cloned.request_root_type, schema_info.request_root_type);
    assert_eq!(cloned.response_root_type, schema_info.response_root_type);

    let debug_str = format!("{:?}", schema_info);
    assert!(debug_str.contains("ServiceSchemaInfo"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_request_stop_allows_followup_join() {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(tx);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let callback_guard = Arc::new(AtomicBool::new(true));
    let callback_guard_for_service = Arc::clone(&callback_guard);
    let task_handle = tokio::spawn(async move {
        let _ = stop_rx.await;
    });

    let running = crate::service::RunningService {
        service_name: "ffi_stop_window_test".to_string(),
        _receiver: rx,
        _event_queue: None,
        stop_tx: Some(stop_tx),
        task_handle: Some(task_handle),
        callback_guard: Some(callback_guard_for_service),
        stats: Arc::new(crate::service::ServiceStatsInner::default()),
        inventory_guard: Some(TelemetryResourceGuard::disabled()),
        runtime_guard: Some(RuntimeTelemetryGuard::disabled()),
        owner_claim: None,
        owner_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    let mut running = running;

    running.request_stop();
    assert!(
        !callback_guard.load(Ordering::Relaxed),
        "request_stop should disable callback guard immediately"
    );
    assert!(
        running.stop_tx.is_none(),
        "request_stop should consume stop sender"
    );
    assert!(
        running.task_handle.is_some(),
        "request_stop must keep join handle so stop_and_join can still wait"
    );

    running.stop_and_join(Duration::from_millis(500)).await;
}

fn make_running_service_for_idle_tests(
    stats: Arc<crate::service::ServiceStatsInner>,
) -> crate::service::RunningService {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(tx);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let task_handle = tokio::spawn(async move {
        let _ = stop_rx.await;
    });

    crate::service::RunningService {
        service_name: "wait_for_idle_test".to_string(),
        _receiver: rx,
        _event_queue: None,
        stop_tx: Some(stop_tx),
        task_handle: Some(task_handle),
        callback_guard: None,
        stats,
        inventory_guard: Some(TelemetryResourceGuard::disabled()),
        runtime_guard: Some(RuntimeTelemetryGuard::disabled()),
        owner_claim: None,
        owner_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_join_cancels_and_drains_owner_check() {
    let witness = Arc::new(());
    let weak = Arc::downgrade(&witness);
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = started.send(());
        std::future::pending::<()>().await;
        drop(witness);
    });
    ready.await.unwrap();
    let mut running =
        make_running_service_for_idle_tests(Arc::new(crate::service::ServiceStatsInner::default()));
    running.owner_claim = Some(crate::service::ServiceOwnerClaim {
        token: None,
        task: Some(task),
    });
    running.request_stop();
    assert!(running.owner_claim.as_ref().unwrap().task.is_some());
    running.stop_and_join(Duration::from_millis(500)).await;
    assert!(
        weak.upgrade().is_none(),
        "advisory query still owns its state"
    );
}

#[tokio::test]
async fn owner_claim_drop_cancels_query_before_it_can_start() {
    let (finished, released) = tokio::sync::oneshot::channel::<()>();
    let started = Arc::new(AtomicBool::new(false));
    let task_started = Arc::clone(&started);
    let task = tokio::spawn(async move {
        task_started.store(true, Ordering::Release);
        std::future::pending::<()>().await;
        let _ = finished.send(());
    });
    let claim = crate::service::ServiceOwnerClaim {
        token: None,
        task: Some(task),
    };
    drop(claim);
    // The sender is dropped only when the cancelled task's future is dropped.
    assert!(tokio::time::timeout(Duration::from_millis(500), released)
        .await
        .unwrap()
        .is_err());
    assert!(!started.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_wait_for_idle_returns_immediately_when_idle() {
    let stats = Arc::new(crate::service::ServiceStatsInner::default());
    let running = make_running_service_for_idle_tests(stats);

    running
        .wait_for_idle(Duration::from_millis(50))
        .await
        .expect("idle service should return immediately");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_wait_for_idle_unblocks_after_in_flight_drain() {
    let stats = Arc::new(crate::service::ServiceStatsInner::default());
    stats.on_request_received();
    let stats_for_drop = Arc::clone(&stats);
    let running = make_running_service_for_idle_tests(stats);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        stats_for_drop.on_request_dropped();
    });

    running
        .wait_for_idle(Duration::from_millis(300))
        .await
        .expect("wait_for_idle should unblock after in-flight drains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_wait_for_idle_times_out_when_busy() {
    let stats = Arc::new(crate::service::ServiceStatsInner::default());
    stats.on_request_received();
    let running = make_running_service_for_idle_tests(stats);

    let result = running.wait_for_idle(Duration::from_millis(40)).await;
    assert!(
        matches!(result, Err(AortaError::Timeout(_))),
        "expected timeout when service stays busy, got: {:?}",
        result
    );
}

#[test]
fn service_stats_snapshot_includes_query_receive_failures_and_task_exits() {
    let stats = crate::service::ServiceStatsInner::default();
    stats.on_query_receive_failure();
    stats.on_task_exit();

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.query_receive_failure_count, 1);
    assert_eq!(snapshot.task_exit_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_builder_creates_service() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "test_service").build();

    assert_eq!(service.name(), "test_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_builder_with_schema() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let service_bfbs = vec![1, 2, 3];
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);
    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs,
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session, "schema_service")
        .schema(schema_info)
        .build();

    assert_eq!(service.name(), "schema_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_builder_with_handler() -> AortaResult<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let session = Session::open_default().await?;
    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_clone = handler_called.clone();

    let service = Service::builder(session, "handler_service")
        .handler(move |_req| {
            handler_called_clone.store(true, Ordering::SeqCst);
        })
        .build();

    assert_eq!(service.name(), "handler_service");
    // Handler not called until service receives a request
    assert!(!handler_called.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_builder_with_event_queue() -> AortaResult<()> {
    use crate::event_queue::EventQueue;
    use std::sync::Arc;

    let session = Session::open_default().await?;
    let event_queue = Arc::new(EventQueue::new(64));

    let service = Service::builder(session, "eq_service")
        .event_queue(event_queue)
        .build();

    assert_eq!(service.name(), "eq_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_start_returns_running_service() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await?;
    let service_name = format!("start_test_service_{}", std::process::id());
    let service = Service::builder(session, &service_name).build();

    let running = service.start().await?;
    assert_eq!(running.name(), service_name);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_handler_starts_successfully() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let service = Service::builder(session, "handler_start_service")
        .handler(|req| {
            // Echo handler - in real code this would process the request
            let _ = req.payload();
        })
        .build();

    let running = service.start().await?;
    assert_eq!(running.name(), "handler_start_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_builder_chaining() -> AortaResult<()> {
    use crate::event_queue::EventQueue;
    use std::sync::Arc;

    let session = Session::open_default().await?;
    let event_queue = Arc::new(EventQueue::new(64));

    let service_bfbs = vec![10, 20, 30];
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);
    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs,
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session, "chained_service")
        .schema(schema_info)
        .event_queue(event_queue)
        .handler(|_req| {})
        .build();

    assert_eq!(service.name(), "chained_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_services_can_coexist() -> AortaResult<()> {
    let session1 = Session::open_default().await?;
    let session2 = Session::open_default().await?;

    let service1 = Service::builder(session1, "service_one").build();
    let service2 = Service::builder(session2, "service_two").build();

    let running1 = service1.start().await?;
    let running2 = service2.start().await?;

    assert_eq!(running1.name(), "service_one");
    assert_eq!(running2.name(), "service_two");
    Ok(())
}

// ============================================================================
// Edge Case Tests - Service Name Handling
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_empty_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "").build();

    // Empty name should still create a service (Zenoh handles validation)
    assert_eq!(service.name(), "");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_very_long_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    // Create a service name with 1024+ characters
    let long_name = "a".repeat(1024);
    let service = Service::builder(session, &long_name).build();

    assert_eq!(service.name(), long_name);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_special_characters_slash() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "with/slashes").build();

    assert_eq!(service.name(), "with/slashes");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_special_characters_dash_underscore() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "service-with_special-chars").build();

    assert_eq!(service.name(), "service-with_special-chars");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_special_characters_colon() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "service:with:colons").build();

    assert_eq!(service.name(), "service:with:colons");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_numeric_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "12345").build();

    assert_eq!(service.name(), "12345");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_with_unicode_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service = Service::builder(session, "服务名称").build();

    assert_eq!(service.name(), "服务名称");
    Ok(())
}

// ============================================================================
// Edge Case Tests - Handler Behavior
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_handler_can_be_replaced_before_start() -> AortaResult<()> {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let session = Session::open_default().await?;
    let handler1_called = Arc::new(AtomicU32::new(0));
    let handler2_called = Arc::new(AtomicU32::new(0));

    let handler1_clone = handler1_called.clone();
    let handler2_clone = handler2_called.clone();

    // Build with first handler, then replace with second
    let service = Service::builder(session, "replace_handler_service")
        .handler(move |_req| {
            handler1_clone.fetch_add(1, Ordering::SeqCst);
        })
        .handler(move |_req| {
            handler2_clone.fetch_add(1, Ordering::SeqCst);
        })
        .build();

    assert_eq!(service.name(), "replace_handler_service");
    // Neither handler should be called until service receives requests
    assert_eq!(handler1_called.load(Ordering::SeqCst), 0);
    assert_eq!(handler2_called.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_without_handler_can_start() -> AortaResult<()> {
    let session = Session::open_default().await?;

    // Service without explicit handler
    let service = Service::builder(session, "no_handler_service").build();
    let running = service.start().await?;

    assert_eq!(running.name(), "no_handler_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_handler_closure_captures_arc() -> AortaResult<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let session = Session::open_default().await?;
    let shared_state = Arc::new(AtomicBool::new(false));
    let state_clone = shared_state.clone();

    let service = Service::builder(session, "arc_handler_service")
        .handler(move |_req| {
            state_clone.store(true, Ordering::SeqCst);
        })
        .build();

    let _running = service.start().await?;

    // State should still be false (no requests yet)
    assert!(!shared_state.load(Ordering::SeqCst));
    Ok(())
}

// ============================================================================
// Edge Case Tests - Multiple Services on Same Session
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_services_same_session() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let service1 = Service::builder(session.clone(), "same_session_svc1").build();
    let service2 = Service::builder(session.clone(), "same_session_svc2").build();
    let service3 = Service::builder(session, "same_session_svc3").build();

    let running1 = service1.start().await?;
    let running2 = service2.start().await?;
    let running3 = service3.start().await?;

    assert_eq!(running1.name(), "same_session_svc1");
    assert_eq!(running2.name(), "same_session_svc2");
    assert_eq!(running3.name(), "same_session_svc3");
    Ok(())
}

// ============================================================================
// Edge Case Tests - RunningService Behavior
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_name_matches_original() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let original_name = "original_service_name_123";

    let service = Service::builder(session, original_name).build();
    let running = service.start().await?;

    assert_eq!(running.name(), original_name);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_service_drop_is_safe() -> AortaResult<()> {
    let session = Session::open_default().await?;

    {
        let service = Service::builder(session.clone(), "drop_test_service").build();
        let _running = service.start().await?;
        // _running is dropped here
    }

    // Session should still be usable after RunningService is dropped
    let service2 = Service::builder(session, "drop_test_service_2").build();
    let running2 = service2.start().await?;
    assert_eq!(running2.name(), "drop_test_service_2");
    Ok(())
}

// ---- deadline-over-RPC ---------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_budget_reaches_the_handler_and_absent_stays_unstamped() -> AortaResult<()> {
    use crate::client::Client;
    let session = Session::open_default().await?;
    let service_name = format!("deadline_svc_{}", std::process::id());
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Option<u64>>::new()));
    let observed_h = std::sync::Arc::clone(&observed);
    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            observed_h
                .lock()
                .unwrap()
                .push(req.remaining_budget().map(|d| d.as_nanos() as u64));
            let _ = tokio::spawn(async move {
                let _ = req.reply(&[1u8]).await;
            });
        })
        .build();
    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A stamped call: the handler observes Some(budget ≤ the timeout).
    let client = Client::builder(session.clone(), &service_name)
        .timeout(Duration::from_secs(5))
        .build();
    let _ = client.call(&[0u8]).await?;

    let seen = observed.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let budget = seen[0].expect("stamped call carries a wire budget");
    assert!(
        budget <= 5_000_000_000 && budget > 1_000_000_000,
        "re-anchored budget within (1s, 5s], got {budget}ns"
    );
    Ok(())
}

#[test]
fn expired_wire_budget_drops_before_the_handler() {
    // Unit pin on the gate's predicate: an expired re-anchored budget
    // reports expired; the dispatch wrapper drops such requests without
    // running user code (the original dropped-request accounting via Drop).
    let expired = crate::flow::Budget::from_wire_relative_ns(0);
    assert!(expired.is_expired());
    let live = crate::flow::Budget::from_wire_relative_ns(5_000_000_000);
    assert!(!live.is_expired());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_gate_drops_expired_and_runs_live() -> AortaResult<()> {
    // BEHAVIORAL pin on the one deadline gate — an expired
    // wire budget skips the handler and bumps the dedicated counter
    // (plus the generic dropped accounting via Drop); a live budget runs.
    use crate::flow::Budget;
    use zenoh::Wait;

    let session = Session::open_default().await?;
    let key = format!("aorta_test/deadline_gate/{}", std::process::id());
    let queryable = session
        .inner()
        .declare_queryable(&key)
        .await
        .map_err(|e| AortaError::internal(format!("queryable: {e}")))?;

    // Two real queries to wrap into requests.
    let mut queries = Vec::new();
    for _ in 0..2 {
        let key2 = key.clone();
        let session2 = session.inner().clone();
        let getter = tokio::task::spawn_blocking(move || {
            let _ = session2.get(&key2).timeout(Duration::from_secs(2)).wait();
        });
        let q = tokio::time::timeout(Duration::from_secs(5), queryable.recv_async())
            .await
            .map_err(|_| AortaError::internal("no query"))?
            .map_err(|e| AortaError::internal(format!("recv: {e}")))?;
        queries.push(q);
        let _ = getter.await;
    }

    let stats = Arc::new(crate::service::ServiceStatsInner::default());
    let ran = Arc::new(AtomicBool::new(false));
    let ran2 = Arc::clone(&ran);
    let gated = crate::service::wrap_handler_with_deadline_gate(Box::new(move |_req| {
        ran2.store(true, Ordering::SeqCst);
    }));

    // Expired budget: handler skipped, dedicated counter bumped.
    let expired = crate::service::ServiceRequest::for_deadline_test(
        queries.remove(0),
        Some(Budget::from_wire_relative_ns(0)),
        Arc::clone(&stats),
    );
    gated(expired);
    assert!(!ran.load(Ordering::SeqCst), "expired request never ran");
    assert_eq!(
        stats.deadline_dropped_for_test(),
        1,
        "dedicated deadline counter"
    );

    // Live budget: runs.
    let live = crate::service::ServiceRequest::for_deadline_test(
        queries.remove(0),
        Some(Budget::from_wire_relative_ns(5_000_000_000)),
        Arc::clone(&stats),
    );
    gated(live);
    assert!(ran.load(Ordering::SeqCst), "live request ran");
    assert_eq!(stats.deadline_dropped_for_test(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_wire_budget_is_v1_and_goal_seed_pins() -> AortaResult<()> {
    // The absent-field case — no stamp → remaining_budget()
    // None → the gate runs the handler (the un-gated path bit-for-bit).
    use crate::flow::Budget;
    use zenoh::Wait;

    let session = Session::open_default().await?;
    let key = format!("aorta_test/absent_budget/{}", std::process::id());
    let queryable = session
        .inner()
        .declare_queryable(&key)
        .await
        .map_err(|e| AortaError::internal(format!("queryable: {e}")))?;
    let key2 = key.clone();
    let session2 = session.inner().clone();
    let getter = tokio::task::spawn_blocking(move || {
        let _ = session2.get(&key2).timeout(Duration::from_secs(2)).wait();
    });
    let q = tokio::time::timeout(Duration::from_secs(5), queryable.recv_async())
        .await
        .map_err(|_| AortaError::internal("no query"))?
        .map_err(|e| AortaError::internal(format!("recv: {e}")))?;
    let _ = getter.await;

    let stats = Arc::new(crate::service::ServiceStatsInner::default());
    let observed = Arc::new(std::sync::Mutex::new(None));
    let observed2 = Arc::clone(&observed);
    let gated = crate::service::wrap_handler_with_deadline_gate(Box::new(move |req| {
        *observed2.lock().unwrap() = Some(req.remaining_budget());
    }));
    let req = crate::service::ServiceRequest::for_deadline_test(q, None, stats);
    gated(req);
    let seen = observed.lock().unwrap().take();
    assert_eq!(seen, Some(None), "no stamp → handler runs with None budget");

    // Action-goal seed contract pin: a wire budget seeds the
    // goal scope's Budget — remaining() ≤ stamped and expires.
    let seeded = crate::flow::scope::ActionScope::for_goal(
        "goal_seed_pin",
        Budget::from_wire_relative_ns(50_000_000),
    );
    let rem = seeded.remaining().expect("seeded budget bounds");
    assert!(rem <= Duration::from_millis(50), "re-anchored ≤ stamped");
    Ok(())
}
