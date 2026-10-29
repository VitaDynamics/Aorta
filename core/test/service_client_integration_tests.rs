use crate::client::Client;
use crate::error::{AortaError, AortaResult};
use crate::event_queue::{Event, EventKind, EventQueue};
use crate::schema::SchemaHash;
use crate::service::{RpcOverflowPolicy, Service};
use crate::session::Session;
use crate::test_support::{schema_attach_never_guard, EnvVarGuard, ENV_LOCK};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn unique_service(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

async fn wait_for_service_ready(
    client: &Client,
    service_name: &str,
    timeout: Duration,
) -> AortaResult<()> {
    let started = tokio::time::Instant::now();
    let mut last_err: Option<AortaError> = None;
    while started.elapsed() < timeout {
        match client.call(&[0xEE]).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(120)).await;
            }
        }
    }
    Err(AortaError::timeout(format!(
        "service readiness probe timed out after {:?}: service='{}' last_err={}",
        timeout,
        service_name,
        last_err
            .map(|e| format!("{:?}", e))
            .unwrap_or_else(|| "unknown".to_string())
    )))
}

async fn spawn_double_reply_queryable(
    session: &Session,
    key_expr: String,
) -> AortaResult<tokio::task::JoinHandle<()>> {
    let queryable = session.inner().declare_queryable(key_expr.clone()).await?;
    Ok(tokio::spawn(async move {
        if let Ok(query) = queryable.recv_async().await {
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            let _ = query.reply(&key_expr, payload.clone()).await;
            let _ = query.reply(&key_expr, payload).await;
        }
        let _ = queryable.undeclare().await;
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_basic_echo() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("echo_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();
    let request = vec![1, 2, 3, 4, 5];
    let response = client.call(&request).await?;

    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    let client_stats = client.stats();
    assert_eq!(client_stats.call_count, 1);
    assert_eq!(client_stats.success_count, 1);
    assert_eq!(client_stats.timeout_count, 0);
    assert_eq!(client_stats.concurrent_conflict_count, 0);
    assert_eq!(client_stats.error_count, 0);
    assert!(client_stats.last_call_started_ns > 0);
    assert!(client_stats.last_call_finished_ns >= client_stats.last_call_started_ns);

    running_service
        .wait_for_idle(Duration::from_millis(500))
        .await?;
    let service_stats = running_service.stats();
    assert_eq!(service_stats.request_received_count, 1);
    assert_eq!(service_stats.request_dispatched_count, 1);
    assert_eq!(service_stats.reply_ok_count, 1);
    assert_eq!(service_stats.reply_error_count, 0);
    assert_eq!(service_stats.reply_send_failure_count, 0);
    assert_eq!(service_stats.request_dropped_count, 0);
    assert_eq!(service_stats.query_receive_failure_count, 0);
    assert_eq!(service_stats.task_exit_count, 0);
    assert_eq!(service_stats.in_flight_requests, 0);
    assert!(service_stats.last_request_ns > 0);
    assert!(service_stats.last_reply_ns > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_zbytes_roundtrip() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("echo_zbytes_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().clone();
            let _ = tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();
    let request = vec![9, 8, 7, 6, 5];
    let response = client.call(request.clone()).await?;

    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_with_event_queue() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("queue_service");

    let request_received = Arc::new(AtomicBool::new(false));
    let request_received_clone = request_received.clone();

    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            request_received_clone.store(true, Ordering::SeqCst);
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();
    let request = vec![10, 20, 30];
    let response = client.call(&request).await?;

    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    assert!(request_received.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_detects_multiple_replies_when_strict_mode_enabled() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _strict_guard = EnvVarGuard::set("AORTA_RPC_ENFORCE_SINGLE_PROVIDER", "1");
    let _window_guard = EnvVarGuard::set("AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS", "500");

    let session = Session::open_default().await?;
    let service_name = unique_service("strict_multi_provider_service");
    let key_expr =
        crate::key_expr::key_builder_from_group_env().service_key_server(&service_name)?;
    let key_expr: String = key_expr.into();
    // Use a raw queryable that replies twice for one query to deterministically
    // trigger strict multi-reply detection without discovery race.
    let query_task = spawn_double_reply_queryable(&session, key_expr).await?;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1500)
        .build();
    let result = client.call(&[1, 2, 3]).await;
    match result {
        Err(AortaError::ConcurrentConflict(msg)) => {
            assert!(msg.contains("multiple replies"));
        }
        other => panic!("expected ConcurrentConflict, got {:?}", other),
    }

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = query_task.await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_explicit_non_strict_overrides_env_strict() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _strict_guard = EnvVarGuard::set("AORTA_RPC_ENFORCE_SINGLE_PROVIDER", "1");
    let _window_guard = EnvVarGuard::set("AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS", "500");

    let session = Session::open_default().await?;
    let service_name = unique_service("strict_override_false_service");
    let key_expr =
        crate::key_expr::key_builder_from_group_env().service_key_server(&service_name)?;
    let key_expr: String = key_expr.into();
    let query_task = spawn_double_reply_queryable(&session, key_expr).await?;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1500)
        .enforce_single_provider(false)
        .build();
    let response = client.call(&[1, 2, 3]).await?;
    assert_eq!(response.to_bytes().as_ref(), &[1, 2, 3]);

    let client_stats = client.stats();
    assert_eq!(client_stats.call_count, 1);
    assert_eq!(client_stats.success_count, 1);
    assert_eq!(client_stats.timeout_count, 0);
    assert_eq!(client_stats.concurrent_conflict_count, 0);
    assert_eq!(client_stats.error_count, 0);

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = query_task.await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_explicit_strict_overrides_env_non_strict() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _strict_guard = EnvVarGuard::set("AORTA_RPC_ENFORCE_SINGLE_PROVIDER", "0");
    let _window_guard = EnvVarGuard::set("AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS", "500");

    let session = Session::open_default().await?;
    let service_name = unique_service("strict_override_true_service");
    let key_expr =
        crate::key_expr::key_builder_from_group_env().service_key_server(&service_name)?;
    let key_expr: String = key_expr.into();
    let query_task = spawn_double_reply_queryable(&session, key_expr).await?;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1500)
        .enforce_single_provider(true)
        .build();
    let result = client.call(&[1, 2, 3]).await;
    match result {
        Err(AortaError::ConcurrentConflict(msg)) => {
            assert!(msg.contains("multiple replies"));
        }
        other => panic!("expected ConcurrentConflict, got {:?}", other),
    }

    let client_stats = client.stats();
    assert_eq!(client_stats.call_count, 1);
    assert_eq!(client_stats.success_count, 0);
    assert_eq!(client_stats.timeout_count, 0);
    assert_eq!(client_stats.concurrent_conflict_count, 1);
    assert_eq!(client_stats.error_count, 0);

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = query_task.await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_timeout_on_no_response() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("slow_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|_req| {})
        .build();

    let running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(100)
        .build();

    let request = vec![1, 2, 3];
    let result = client.call(&request).await;
    match result {
        Err(AortaError::Timeout(msg)) => {
            assert!(msg.contains("timed out"));
        }
        other => panic!("expected Timeout error, got {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(120)).await;

    let client_stats = client.stats();
    assert_eq!(client_stats.call_count, 1);
    assert_eq!(client_stats.success_count, 0);
    assert_eq!(client_stats.timeout_count, 1);
    assert_eq!(client_stats.concurrent_conflict_count, 0);
    assert_eq!(client_stats.error_count, 0);
    assert!(client_stats.last_call_started_ns > 0);
    assert!(client_stats.last_call_finished_ns >= client_stats.last_call_started_ns);

    running_service
        .wait_for_idle(Duration::from_millis(500))
        .await?;
    let service_stats = running_service.stats();
    // Retries are OFF by default, so a single logical `call()` must
    // reach the (possibly non-idempotent) provider EXACTLY ONCE even though it
    // times out. Before the fix the retry loop re-sent the request within the
    // deadline window, invoking the handler 2+ times — silent duplicate
    // execution. This assertion locks the fail-safe single-invocation guarantee.
    assert_eq!(
        service_stats.request_received_count, 1,
        "default (no-retry) RPC must invoke the provider exactly once per call()"
    );
    assert_eq!(
        service_stats.request_dispatched_count,
        service_stats.request_received_count
    );
    assert_eq!(service_stats.reply_ok_count, 0);
    assert_eq!(service_stats.reply_error_count, 0);
    assert_eq!(service_stats.request_dropped_count, 1);
    assert_eq!(service_stats.in_flight_requests, 0);
    assert!(service_stats.last_request_ns > 0);
    assert_eq!(service_stats.last_reply_ns, 0);
    Ok(())
}

/// Opt-in: a caller that has proven its service is idempotent-safe can
/// re-enable the retry loop via `ClientBuilder::allow_retry(true)`. With a
/// non-replying handler and a deadline several backoff windows wide, the
/// provider should then be re-invoked more than once before the call times out
/// — the inverse of the default fail-safe behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_allow_retry_opt_in_resends_request() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("retry_optin_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|_req| {})
        .build();

    let running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Deadline (300ms) spans several retry backoff windows (50ms + 75ms + …),
    // so an opted-in client must re-send before giving up.
    let client = Client::builder(session, &service_name)
        .timeout_ms(300)
        .allow_retry(true)
        .build();

    let request = vec![1, 2, 3];
    let result = client.call(&request).await;
    match result {
        Err(AortaError::Timeout(msg)) => assert!(msg.contains("timed out")),
        other => panic!("expected Timeout error, got {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(120)).await;

    running_service
        .wait_for_idle(Duration::from_millis(500))
        .await?;
    let service_stats = running_service.stats();
    assert!(
        service_stats.request_received_count >= 2,
        "allow_retry(true) must re-send the request before the deadline (got {})",
        service_stats.request_received_count
    );
    assert_eq!(
        service_stats.request_dispatched_count,
        service_stats.request_received_count
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_multiple_sequential_calls() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let session = Session::open_default().await?;
    let service_name = unique_service("multi_call_service");

    let request_count = Arc::new(tokio::sync::Mutex::new(0));
    let request_count_clone = request_count.clone();

    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            let count = request_count_clone.clone();
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let mut c = count.lock().await;
                *c += 1;
                drop(c);
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();

    for i in 0..3 {
        let request = vec![i; 5];
        let response = client.call(&request).await?;
        assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    }

    let count = request_count.lock().await;
    assert_eq!(*count, 3, "Should have processed 3 requests");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_returns_busy_when_executor_queue_is_full() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let config = zenoh::Config::from_json5(
        r#"
{
  mode: "peer",
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )?;
    let session = Session::open(config).await?;
    let service_name = unique_service("busy_on_executor_full_service");

    let event_queue = Arc::new(EventQueue::new(1));
    event_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill executor queue");

    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_for_service = Arc::clone(&handler_called);

    let service = Service::builder(session.clone(), &service_name)
        .event_queue(event_queue)
        .handler(move |_req| {
            handler_called_for_service.store(true, Ordering::SeqCst);
        })
        .build();

    let running_service = service.start().await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(800)
        .build();

    let result = client.call(&[1, 2, 3]).await;
    match result {
        Err(AortaError::Internal(msg)) => {
            assert!(
                msg.contains("busy"),
                "expected busy error payload, got: {}",
                msg
            );
        }
        other => panic!("expected Internal busy error, got {:?}", other),
    }

    assert!(
        !handler_called.load(Ordering::SeqCst),
        "service handler must not run when busy fast-fail triggers"
    );

    running_service
        .wait_for_idle(Duration::from_millis(500))
        .await?;
    let service_stats = running_service.stats();
    assert_eq!(service_stats.request_received_count, 1);
    assert_eq!(service_stats.request_dispatched_count, 0);
    assert_eq!(service_stats.reply_ok_count, 0);
    assert_eq!(service_stats.reply_error_count, 1);
    assert_eq!(service_stats.reply_send_failure_count, 0);
    assert_eq!(service_stats.request_dropped_count, 0);
    assert_eq!(service_stats.query_receive_failure_count, 0);
    assert_eq!(service_stats.task_exit_count, 0);
    assert_eq!(service_stats.in_flight_requests, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_block_timeout_when_executor_queue_stays_full() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let config = zenoh::Config::from_json5(
        r#"
{
  mode: "peer",
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )?;
    let session = Session::open(config).await?;
    let service_name = unique_service("busy_block_timeout_service");

    let event_queue = Arc::new(EventQueue::new(1));
    event_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill executor queue");

    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_for_service = Arc::clone(&handler_called);

    let service = Service::builder(session.clone(), &service_name)
        .event_queue(event_queue)
        .rpc_overflow_policy(RpcOverflowPolicy::Block)
        .rpc_overflow_block_timeout(Duration::from_millis(120))
        .handler(move |_req| {
            handler_called_for_service.store(true, Ordering::SeqCst);
        })
        .build();

    let running_service = service.start().await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1500)
        .build();

    let started = Instant::now();
    let result = client.call(&[1, 2, 3]).await;
    let elapsed = started.elapsed();
    match result {
        Err(AortaError::Internal(msg)) => {
            assert!(
                msg.contains("busy"),
                "expected busy error payload, got: {}",
                msg
            );
        }
        other => panic!("expected Internal busy error, got {:?}", other),
    }
    assert!(
        elapsed >= Duration::from_millis(80),
        "block timeout path should wait before failing, elapsed={:?}",
        elapsed
    );

    assert!(
        !handler_called.load(Ordering::SeqCst),
        "service handler must not run when block timeout expires"
    );

    running_service
        .wait_for_idle(Duration::from_millis(500))
        .await?;
    let service_stats = running_service.stats();
    assert_eq!(service_stats.request_received_count, 1);
    assert_eq!(service_stats.request_dispatched_count, 0);
    assert_eq!(service_stats.reply_ok_count, 0);
    assert_eq!(service_stats.reply_error_count, 1);
    assert_eq!(service_stats.reply_send_failure_count, 0);
    assert_eq!(service_stats.request_dropped_count, 0);
    assert_eq!(service_stats.query_receive_failure_count, 0);
    assert_eq!(service_stats.task_exit_count, 0);
    assert_eq!(service_stats.in_flight_requests, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_custom_timeout_ms() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("custom_timeout_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1000)
        .build();

    let request = vec![1, 2, 3, 4];
    let response = client.call(&request).await?;

    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_schema_info() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("schema_service");

    let service_hash = SchemaHash::from_bfbs_bytes(&[1, 2, 3]);

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .service_schema(service_hash)
        .build();

    let request = vec![100, 101, 102];
    let response = client.call(&request).await?;

    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_transformation() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("transform_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let transformed: Vec<u8> = payload.iter().map(|b| b.wrapping_add(1)).collect();
                let _ = req.reply(&transformed).await;
            });
        })
        .build();

    let _running_service = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();

    let request = vec![1, 2, 3, 4, 5];
    let response = client.call(&request).await?;
    let expected: Vec<u8> = request.iter().map(|b| b.wrapping_add(1)).collect();

    assert_eq!(response.to_bytes().as_ref(), expected.as_slice());
    Ok(())
}

// ============================================================================
// Advanced Integration Tests - Large Messages
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_large_message_64kb() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("large_msg_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    let client = Client::builder(session, &service_name)
        .timeout_ms(10000)
        .build();
    wait_for_service_ready(&client, &service_name, Duration::from_secs(6)).await?;

    let request: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    let response = client.call(&request).await?;

    assert_eq!(response.len(), 65536);
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_empty_request() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("empty_req_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();
    let response = client.call(&[]).await?;

    assert!(response.is_empty());
    Ok(())
}

// ============================================================================
// Advanced Integration Tests - Concurrency
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_client_multiple_concurrent_clients() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("concurrent_service");
    let request_count = Arc::new(AtomicU32::new(0));
    let count_clone = request_count.clone();

    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut handles = vec![];
    for i in 0..5 {
        let session_clone = session.clone();
        let service_name = service_name.clone();
        let handle = tokio::spawn(async move {
            let client = Client::builder(session_clone, &service_name).build();
            let request = vec![i as u8; 10];
            client.call(&request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.expect("task panicked");
        assert!(result.is_ok(), "call failed: {:?}", result.err());
    }

    assert_eq!(request_count.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_high_frequency_calls() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");
    let _normalize_case_guard = EnvVarGuard::remove("AORTA_KEY_NORMALIZE_CASE");

    let session = Session::open_default().await?;
    let service_name = unique_service("highfreq_service");
    let key_builder = crate::key_expr::KeyExprBuilder::new("default")?;
    let request_count = Arc::new(AtomicU32::new(0));
    let count_clone = request_count.clone();

    let service = Service::builder(session.clone(), &service_name)
        .key_builder(key_builder.clone())
        .handler(move |req| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .key_builder(key_builder)
        .build();
    wait_for_service_ready(&client, &service_name, Duration::from_secs(6)).await?;
    let baseline = request_count.load(Ordering::SeqCst);

    for i in 0u8..20 {
        let request = vec![i; 5];
        let response = client.call(&request).await?;
        assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    }

    assert_eq!(request_count.load(Ordering::SeqCst) - baseline, 20);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_variable_size_messages() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("varsize_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();

    let sizes = [10, 100, 1024, 4096, 8192];
    for size in sizes {
        let request: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let response = client.call(&request).await?;
        assert_eq!(response.len(), size, "Size mismatch for {} bytes", size);
        assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    }

    Ok(())
}

// ============================================================================
// Advanced Integration Tests - Error Handling
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_reply_error() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("error_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let _ = tokio::spawn(async move {
                let _ = req.reply_error("intentional error").await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();
    let result = client.call(&[1, 2, 3]).await;
    match result {
        Err(AortaError::Internal(msg)) => {
            assert!(msg.contains("returned error"));
        }
        other => panic!("expected Internal error, got {:?}", other),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_call_with_timeout_override() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("delay_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(50)
        .build();

    let result1 = client.call(&[1, 2, 3]).await;
    match result1 {
        Err(AortaError::Timeout(msg)) => {
            assert!(msg.contains("timed out"));
        }
        other => panic!("expected Timeout error, got {:?}", other),
    }

    let result2 = client.call_with_timeout(&[1, 2, 3], 1000).await;
    assert!(result2.is_ok(), "Expected success with 1000ms override");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_schema_registration() -> AortaResult<()> {
    use crate::key_expr::KeyExprBuilder;
    use crate::schema::SchemaHash;
    use crate::schema_registry::SchemaRegistry;
    use crate::service::ServiceSchemaInfo;

    let session = Session::open_default().await?;
    let service_name = unique_service("test_schema_service");
    let key_builder = KeyExprBuilder::new("test_group")?;
    let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());

    schema_registry.start_schema_queryables().await?;

    let service_bfbs = b"mock_service_schema_bfbs".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session.clone(), &service_name)
        .schema(schema_info)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let fetched = schema_registry
        .fetch_schema_by_key_service(&service_name)
        .await?;

    assert!(fetched.is_some(), "Schema should be registered");
    let schema = fetched.unwrap();
    assert_eq!(schema.service_hash, service_hash);
    assert_eq!(schema.service_bfbs, service_bfbs);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_schema_discovery() -> AortaResult<()> {
    use crate::key_expr::KeyExprBuilder;
    use crate::schema::SchemaHash;
    use crate::schema_registry::SchemaRegistry;
    use crate::service::ServiceSchemaInfo;

    let session = Session::open_default().await?;
    let service_name = unique_service("discovery_test_service");
    let key_builder = KeyExprBuilder::new("test_group")?;
    let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());

    schema_registry.start_schema_queryables().await?;

    let service_bfbs = b"client_test_service_bfbs".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session.clone(), &service_name)
        .schema(schema_info)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name)
        .service_schema(service_hash)
        .schema_registry(schema_registry)
        .validate_schema(true)
        .build();

    let request = vec![1, 2, 3, 4];
    let response = client.call(&request).await?;
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_schema_conflict_warn_keeps_rpc_available() -> AortaResult<()> {
    use crate::key_expr::KeyExprBuilder;
    use crate::schema::SchemaHash;
    use crate::schema_registry::SchemaRegistry;
    use crate::service::ServiceSchemaInfo;

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _conflict_guard = EnvVarGuard::set("AORTA_SCHEMA_CONFLICT_ACTION", "warn");

    let session = Session::open_default().await?;
    let group = format!("conflict_test_group_p{}", std::process::id());
    let key_builder = KeyExprBuilder::new(&group)?;
    let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());
    let service_name = unique_service("conflict_service_warn");

    schema_registry.start_schema_queryables().await?;

    let service_bfbs_v1 = b"service_v1_bfbs".to_vec();
    let service_hash_v1 = SchemaHash::from_bfbs_bytes(&service_bfbs_v1);

    let schema_info_v1 = ServiceSchemaInfo {
        schema: service_hash_v1,
        schema_bfbs: service_bfbs_v1.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let service1 = Service::builder(session.clone(), &service_name)
        .schema(schema_info_v1)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let mut running1 = service1.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let service_bfbs_v2 = b"service_v2_different_bfbs".to_vec();
    let service_hash_v2 = SchemaHash::from_bfbs_bytes(&service_bfbs_v2);

    let schema_info_v2 = ServiceSchemaInfo {
        schema: service_hash_v2,
        schema_bfbs: service_bfbs_v2.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let service2 = Service::builder(session.clone(), &service_name)
        .schema(schema_info_v2.clone())
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let mut running2 = service2.start().await?;

    let fetched = schema_registry
        .fetch_schema_by_key_service(&service_name)
        .await?
        .expect("service schema");
    assert_eq!(fetched.service_hash, schema_info_v2.schema);

    running1.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1000)
        .build();
    let response = client.call(&[9, 8, 7]).await?;
    assert_eq!(response.to_bytes().as_ref(), &[9, 8, 7]);

    running2.stop();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_schema_conflict_reject_blocks_second_service_start() -> AortaResult<()> {
    use crate::key_expr::KeyExprBuilder;
    use crate::schema::SchemaHash;
    use crate::schema_registry::SchemaRegistry;
    use crate::service::ServiceSchemaInfo;

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _conflict_guard = EnvVarGuard::set("AORTA_SCHEMA_CONFLICT_ACTION", "reject");

    let session = Session::open_default().await?;
    let group = format!("conflict_reject_group_p{}", std::process::id());
    let key_builder = KeyExprBuilder::new(&group)?;
    let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());
    let service_name = unique_service("conflict_service_reject");

    schema_registry.start_schema_queryables().await?;

    let service_bfbs_v1 = b"service_reject_v1_bfbs".to_vec();
    let service_hash_v1 = SchemaHash::from_bfbs_bytes(&service_bfbs_v1);

    let schema_info_v1 = ServiceSchemaInfo {
        schema: service_hash_v1,
        schema_bfbs: service_bfbs_v1.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let service1 = Service::builder(session.clone(), &service_name)
        .schema(schema_info_v1)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let mut running1 = service1.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let service_bfbs_v2 = b"service_reject_v2_bfbs".to_vec();
    let service_hash_v2 = SchemaHash::from_bfbs_bytes(&service_bfbs_v2);

    let schema_info_v2 = ServiceSchemaInfo {
        schema: service_hash_v2,
        schema_bfbs: service_bfbs_v2,
        request_root_type: None,
        response_root_type: None,
    };

    let service2 = Service::builder(session.clone(), &service_name)
        .schema(schema_info_v2)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let start_result = service2.start().await;
    match start_result {
        Err(AortaError::SchemaConflict(_)) => {}
        Err(other) => panic!("expected SchemaConflict, got {:?}", other),
        Ok(_) => panic!("expected reject to fail second service start"),
    }

    let client = Client::builder(session, &service_name)
        .timeout_ms(1000)
        .build();
    let response = client.call(&[1, 2, 3]).await?;
    assert_eq!(response.to_bytes().as_ref(), &[1, 2, 3]);

    running1.stop();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_schema_validation_mismatch() -> AortaResult<()> {
    use crate::key_expr::KeyExprBuilder;
    use crate::schema::SchemaHash;
    use crate::schema_registry::SchemaRegistry;
    use crate::service::ServiceSchemaInfo;

    let session = Session::open_default().await?;
    let service_name = unique_service("validation_service");
    let key_builder = KeyExprBuilder::new("validation_test_group")?;
    let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());

    schema_registry.start_schema_queryables().await?;

    let service_bfbs = b"validation_service_bfbs".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs,
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session.clone(), &service_name)
        .schema(schema_info)
        .schema_registry(schema_registry.clone())
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let wrong_hash = SchemaHash::from_bfbs_bytes(b"wrong_schema_hash");

    let client = Client::builder(session, &service_name)
        .service_schema(wrong_hash)
        .schema_registry(schema_registry)
        .validate_schema(true)
        .build();

    let result = client.call(&[1, 2, 3]).await;
    assert!(
        result.is_ok(),
        "Expected schema validation to log mismatch but allow call"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_response_schema_attachment_mismatch_warns_but_allows_call() -> AortaResult<()> {
    use crate::service::ServiceSchemaInfo;

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let session = Session::open_default().await?;
    let service_name = unique_service("resp_attach_mismatch");

    let service_bfbs = b"response_attach_service_schema".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);
    let schema_info = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs,
        request_root_type: None,
        response_root_type: None,
    };

    let service = Service::builder(session.clone(), &service_name)
        .schema(schema_info)
        .default_schema_attach_policy(crate::schema::SchemaAttachPolicy::Always)
        .schema_attach_policy(crate::schema::SchemaAttachPolicy::Always)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let wrong_service_hash = SchemaHash::from_bfbs_bytes(b"wrong_service_schema");
    let client = Client::builder(session, &service_name)
        .default_schema_attach_policy(crate::schema::SchemaAttachPolicy::Always)
        .schema_attach_policy(crate::schema::SchemaAttachPolicy::Always)
        .service_schema(wrong_service_hash)
        .timeout_ms(1500)
        .build();

    let response = client.call(&[7, 7, 7]).await?;
    assert_eq!(response.to_bytes().as_ref(), &[7, 7, 7]);

    Ok(())
}

/// Regression for the dual-guard `.timeout()` leak fix (observed
/// in production): `Client::call()` sets `.timeout(remaining + grace)` on the
/// Zenoh get so a provider that receives a query but never finalizes it does
/// not leave a pending query parked on every router hop for
/// `queries_default_timeout` (600s on robots, 10s in the test router).
///
/// Coverage note: the router-side cleanup *instant* is not observable through
/// Aorta's public API — the tokio guard returns at the deadline either way, the
/// provider is never notified of the reap (`finalize_pending_query` only sends
/// `ResponseFinal` toward the querier), and a reply to a reaped query is
/// fire-and-forget. So this pins (1) the dual-guard MECHANISM the fix relies on
/// — a `get(...).timeout(T)` against a provider holding the query open is
/// finalized by the router at ~`T`, far below the 10s session default, on a
/// kept-open receiver; and (2) the caller contract — the held-open provider
/// still yields a prompt `Timeout`, is invoked, and the request is left *held*
/// (`request_dropped_count == 0`) so only the query timeout can reap it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_call_bounds_zenoh_query_timeout() -> AortaResult<()> {
    use crate::key_expr::key_builder_from_group_env;
    use crate::service::ServiceRequest;
    use zenoh::query::ConsolidationMode;

    let session = Session::open_default().await?;
    let service_name = unique_service("held_query_service");

    let held: Arc<std::sync::Mutex<Vec<ServiceRequest>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_for_handler = Arc::clone(&held);
    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            held_for_handler.lock().expect("held lock").push(req);
        })
        .build();
    let running_service = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // (1) MECHANISM: a raw get mirroring what `Client::call()` issues. Keep the
    // receiver open and measure when the router finalizes the held-open query.
    let client_key: String = key_builder_from_group_env()
        .service_key_client(&service_name)?
        .into();
    let probe_timeout = Duration::from_millis(300);
    let probe = session
        .inner()
        .get(&client_key)
        .timeout(probe_timeout)
        .consolidation(ConsolidationMode::None)
        .await?;
    let started = Instant::now();
    while probe.recv_async().await.is_ok() {}
    let mechanism_elapsed = started.elapsed();
    assert!(
        mechanism_elapsed >= Duration::from_millis(150)
            && mechanism_elapsed < Duration::from_secs(3),
        "router must reap the held-open query at ~`.timeout()` ({:?}), not the \
         10s session default; got {:?}",
        probe_timeout,
        mechanism_elapsed
    );

    // (2) CALLER CONTRACT: Aorta `Client::call()` against the same held-open
    // provider returns a prompt Timeout (the grace keeps the app guard first).
    let client = Client::builder(session, &service_name)
        .timeout_ms(300)
        .build();
    let started = Instant::now();
    let result = client.call(&[1, 2, 3]).await;
    let call_elapsed = started.elapsed();
    match result {
        Err(AortaError::Timeout(msg)) => assert!(msg.contains("timed out")),
        other => panic!("expected Timeout error, got {:?}", other),
    }
    assert!(
        call_elapsed < Duration::from_secs(3),
        "call must return at its own deadline, got {:?}",
        call_elapsed
    );
    assert_eq!(client.stats().timeout_count, 1);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let svc = running_service.stats();
    assert!(
        svc.request_received_count >= 1,
        "provider must have received the query (got {})",
        svc.request_received_count
    );
    assert_eq!(svc.reply_ok_count, 0);
    assert_eq!(
        svc.request_dropped_count, 0,
        "requests are stashed (held open), not dropped — only the query \
         timeout can reap them"
    );

    held.lock().expect("held lock").clear();
    Ok(())
}

/// Regression: in strict single-provider mode the
/// extra-reply probe waits up to `multi_reply_check_window` AFTER the first
/// reply. When that window is configured larger than `ZENOH_QUERY_TIMEOUT_GRACE`
/// (250 ms) and the first reply lands late, the client's own
/// `.timeout(remaining + grace)` synthesizes an `Err("Timeout")` reply that
/// arrives INSIDE the probe window. It must be ignored — it is not a second
/// provider — so a single slow provider must NOT surface as `ConcurrentConflict`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_strict_mode_ignores_synthesized_timeout_in_check_window() -> AortaResult<()>
{
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let _strict_guard = EnvVarGuard::set("AORTA_RPC_ENFORCE_SINGLE_PROVIDER", "1");
    // Window (1000 ms) >> grace (250 ms): the synthesized timeout (at the
    // 1000 ms deadline + 250 ms grace) lands inside the strict probe window.
    let _window_guard = EnvVarGuard::set("AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS", "1000");

    let session = Session::open_default().await?;
    let service_name = unique_service("strict_late_single_provider");
    let key_expr: String = crate::key_expr::key_builder_from_group_env()
        .service_key_server(&service_name)?
        .into();

    // ONE provider that replies once, late (700 ms): within the 1000 ms call
    // deadline, but late enough that the strict window (1000 ms after the first
    // reply) outlasts the synthesized timeout at deadline + grace (1250 ms).
    let queryable = session.inner().declare_queryable(key_expr.clone()).await?;
    let provider = tokio::spawn(async move {
        if let Ok(query) = queryable.recv_async().await {
            tokio::time::sleep(Duration::from_millis(700)).await;
            let _ = query.reply(&key_expr, vec![0xA5]).await;
            // HOLD the query open past the probe window: dropping it would send
            // a ResponseFinal and finalize the query immediately, so no timeout
            // would ever be synthesized. Keeping it pending lets the client's
            // own `.timeout()` synthesize the `Err("Timeout")` this test targets.
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        let _ = queryable.undeclare().await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let client = Client::builder(session, &service_name)
        .timeout_ms(1000)
        .build();
    let result = client.call(&[1, 2, 3]).await;

    match result {
        Ok(payload) => assert_eq!(payload.to_bytes().as_ref(), &[0xA5]),
        other => panic!(
            "single late provider must not be a ConcurrentConflict, got {:?}",
            other
        ),
    }
    let stats = client.stats();
    assert_eq!(stats.concurrent_conflict_count, 0);
    assert_eq!(stats.success_count, 1);

    provider.abort();
    let _ = provider.await;
    Ok(())
}
