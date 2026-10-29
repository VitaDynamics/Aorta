//! Integration tests for SERVICE-side trace instrumentation (Step 6).
//!
//! Stands up a real in-process Zenoh service + client and asserts that a service
//! call records into the tracing-owned `ServiceClientCall` histogram when tracing
//! is enabled. The disabled-path no-op is covered by the Step-5 unit test
//! (`record_stage_disabled_is_noop`) plus the `if trace_enabled()` guard at the
//! call site, so this integration test asserts the WIRING (a real call reaches
//! the histogram) with a concurrency-robust `delta >= 1` check.
//!
//! Run filtered + single-threaded for an exact picture:
//! `cargo test -p aorta_core --lib trace_service_tests -- --test-threads=1`.

use crate::client::Client;
use crate::error::AortaResult;
use crate::service::Service;
use crate::session::Session;
use crate::trace::{set_trace_enabled, stage_snapshot, StageId};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn unique_service(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_call_records_client_call_latency_when_enabled() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = unique_service("trace_echo_service");

    let service = Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().clone();
            let _ = tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .build();
    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();

    // Serialize the gate toggle + snapshot window against every other trace test
    // (the suite-wide lock). Held across the `.await` below — sound because the
    // test body runs via `block_on`, which does not require a `Send` future (only
    // `tokio::spawn`ed tasks do, and none here hold the guard).
    let _trace_guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
    crate::trace::set_scope_enabled(stage_scope, true);
    let client_before = stage_snapshot(StageId::ServiceClientCall).count;
    let server_before = stage_snapshot(StageId::ServiceReceivedToReply).count;
    let net_before = stage_snapshot(StageId::ServiceNetworkEstimate).count;

    let request = vec![1u8, 2, 3, 4];
    let response = client.call(&request).await?;
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    // Client-side counters are recorded synchronously inside `call()` — safe
    // to sample immediately. The SERVER-side counter is recorded on the
    // server's own task right after the reply send and can land a beat after
    // the client returns (CI flake on slow runners: before=0, after=0) — it
    // is sampled below, AFTER the drain-retry loop has seen the server's
    // waterfall records (`finish_stage` records the histogram before it
    // emits the ring records, so seen-records ⇒ counted-histogram).
    let client_after = stage_snapshot(StageId::ServiceClientCall).count;
    let net_after = stage_snapshot(StageId::ServiceNetworkEstimate).count;

    // Waterfall tier: both stage spans must be attributed to THIS service by
    // name (multi-service processes stay tellable apart on the timeline).
    let ev_client_call = crate::trace::stage_event_for(StageId::ServiceClientCall, &service_name);
    let ev_received_to_reply =
        crate::trace::stage_event_for(StageId::ServiceReceivedToReply, &service_name);
    let ev_dispatched =
        crate::trace::stage_event_for(StageId::ServiceRequestReceivedToDispatched, &service_name);
    let ev_user_callback =
        crate::trace::stage_event_for(StageId::ServiceUserCallback, &service_name);
    let ev_reply_send = crate::trace::stage_event_for(StageId::ServiceReplySend, &service_name);
    // The server records received_to_reply on its own task right after the
    // reply send, which can land a beat after the client returns — drain with
    // a bounded retry instead of once.
    let mut recs = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have_client = recs.iter().any(|r| r.event_id == ev_client_call);
        let have_server = recs.iter().any(|r| r.event_id == ev_received_to_reply);
        let have_cb = recs.iter().any(|r| r.event_id == ev_user_callback);
        let have_send = recs.iter().any(|r| r.event_id == ev_reply_send);
        if (have_client && have_server && have_cb && have_send)
            || tokio::time::Instant::now() > deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let server_after = stage_snapshot(StageId::ServiceReceivedToReply).count;
    set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    let begin = |r: &&crate::trace::RingRecord| {
        ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
    };
    let client_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_client_call)
        .expect("client call must emit a service.client_call:<service> waterfall slice");
    let server_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_received_to_reply)
        .expect("server reply must emit a service.received_to_reply:<service> waterfall slice");
    let dispatched_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_dispatched)
        .expect("server must emit a received_to_dispatched waterfall slice");
    let callback_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_user_callback)
        .expect("server must emit a user_callback waterfall slice");

    // Cross-process propagation, exercised over the REAL wire (the query /
    // reply attachments — the only channel between the client call task and
    // the service receive loop). Hierarchy: received_to_reply is the SERVER
    // ROOT (the one cross-process link); internal stages nest beneath it.
    assert_ne!(client_begin.trace_id, [0u8; 16]);
    assert_eq!(
        server_begin.trace_id, client_begin.trace_id,
        "server root must ride the client's trace (traceparent on the query attachment)"
    );
    assert_eq!(
        server_begin.parent_span_id, client_begin.span_id,
        "server root (received_to_reply) must parent to the client_call span"
    );
    assert_eq!(dispatched_begin.trace_id, client_begin.trace_id);
    assert_eq!(
        dispatched_begin.parent_span_id, server_begin.span_id,
        "internal stages must nest under the server root, not the remote client span"
    );
    assert_eq!(callback_begin.trace_id, client_begin.trace_id);
    assert_eq!(
        callback_begin.parent_span_id, server_begin.span_id,
        "user_callback must nest under the server root"
    );
    let reply_send_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_reply_send)
        .expect("the reply transmission must emit a service.reply_send waterfall slice");
    assert_eq!(reply_send_begin.trace_id, client_begin.trace_id);
    assert_eq!(
        reply_send_begin.parent_span_id, server_begin.span_id,
        "reply_send must nest under the server root"
    );

    // Reply context: the server echoed its busy duration, so the client
    // derived a network round-trip estimate.
    assert!(
        net_after > net_before,
        "reply context must yield a ServiceNetworkEstimate sample \
         (before={net_before}, after={net_after})"
    );

    assert!(
        client_after > client_before,
        "an enabled service call must record ServiceClientCall latency \
         (before={client_before}, after={client_after})"
    );
    assert!(
        server_after > server_before,
        "the server reply must record ServiceReceivedToReply latency \
         (before={server_before}, after={server_after})"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_work_joins_the_request_trace_via_trace_context() -> AortaResult<()> {
    use std::sync::{Arc, Mutex};

    let session = Session::open_default().await?;
    let service_name = unique_service("trace_deferred_ctx_service");

    // Captured by the handler: the context it hands to a detached worker
    // thread (the deferred-work pattern under test). The worker is a plain OS
    // thread — no executor, no inherited thread-locals — so the ONLY way its
    // span can join the request's trace is the explicit adopt below.
    let captured: Arc<Mutex<Option<crate::trace::TraceParent>>> = Arc::new(Mutex::new(None));
    let captured_in_handler = Arc::clone(&captured);
    let worker_scope = crate::trace::intern_scope("test.deferred");
    let ev_worker = crate::trace::intern_event("deferred_work");

    let service = Service::builder(session.clone(), &service_name)
        .handler(move |req| {
            let ctx = req.trace_context();
            *captured_in_handler.lock().unwrap() = ctx;
            let worker = std::thread::spawn(move || {
                if let Some(ctx) = ctx {
                    let _adopted = crate::trace::adopt_remote_context(&ctx);
                    let token = crate::trace::span_begin(worker_scope, ev_worker, 2, 0);
                    let _ = crate::trace::span_end(token);
                }
            });
            worker.join().expect("worker thread");
            let payload = req.payload().clone();
            let _ = tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .build();
    let _running = service.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = Client::builder(session, &service_name).build();

    let _trace_guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
    crate::trace::set_scope_enabled(stage_scope, true);
    crate::trace::set_scope_enabled(worker_scope, true);

    let request = vec![9u8, 8, 7];
    let response = client.call(&request).await?;
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    let ev_received_to_reply =
        crate::trace::stage_event_for(StageId::ServiceReceivedToReply, &service_name);
    let mut recs = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have_root = recs.iter().any(|r| r.event_id == ev_received_to_reply);
        let have_worker = recs.iter().any(|r| r.event_id == ev_worker);
        if (have_root && have_worker) || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    let begin = |r: &&crate::trace::RingRecord| {
        ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
    };
    let root_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_received_to_reply)
        .expect("server must emit the received_to_reply root slice");
    let worker_begin = recs
        .iter()
        .filter(begin)
        .find(|r| r.event_id == ev_worker)
        .expect("the deferred worker must emit its span");

    let ctx = captured
        .lock()
        .unwrap()
        .expect("handler must observe a trace context while tracing is enabled");
    assert_eq!(ctx.trace_id, root_begin.trace_id);
    assert_eq!(
        ctx.span_id, root_begin.span_id,
        "trace_context must hand out the SERVER ROOT span id"
    );
    assert_eq!(worker_begin.trace_id, root_begin.trace_id);
    assert_eq!(
        worker_begin.parent_span_id, root_begin.span_id,
        "the deferred-thread span must nest under the request's server root"
    );
    Ok(())
}
