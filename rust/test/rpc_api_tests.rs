use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::CallbackGroupType;
use crate::ServiceSchemaInfo;
use crate::{
    AortaError, AortaResult, ClientOptions, HeaderInjector, HeaderValues, Node, RpcOverflowPolicy,
    SchemaAttachPolicy, SchemaMeta, ServiceOptions, TypedPublishMessage, TypedSubscribeMessage,
};
use aorta_core::schema::SchemaHash;
use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};

struct TypedRpcRequestSchema;
impl SchemaMeta for TypedRpcRequestSchema {
    fn schema_bfbs() -> &'static [u8] {
        b"typed_rpc_request_schema"
    }

    fn root_type() -> Option<&'static str> {
        Some("sdk.rpc.TypedRpcRequest")
    }
}

struct TypedRpcResponseSchema;
impl SchemaMeta for TypedRpcResponseSchema {
    fn schema_bfbs() -> &'static [u8] {
        b"typed_rpc_request_schema"
    }

    fn root_type() -> Option<&'static str> {
        Some("sdk.rpc.TypedRpcResponse")
    }
}

struct TypedRpcSchemaWithoutRoot;
impl SchemaMeta for TypedRpcSchemaWithoutRoot {
    fn schema_bfbs() -> &'static [u8] {
        b"typed_rpc_schema_without_root"
    }
}

struct TypedRpcHeader;

impl<'a> Follow<'a> for TypedRpcHeader {
    type Inner = TypedRpcHeader;

    unsafe fn follow(_buf: &'a [u8], _loc: usize) -> Self::Inner {
        TypedRpcHeader
    }
}

#[derive(Default)]
struct TypedRpcRequestMsg {
    bytes: Vec<u8>,
}

#[derive(Default)]
struct TypedRpcResponseMsg {
    bytes: Vec<u8>,
}

impl HeaderInjector for TypedRpcRequestSchema {
    type Header<'a> = TypedRpcHeader;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        _values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>> {
        let start = builder.start_table();
        let end = builder.end_table(start);
        WIPOffset::new(end.value())
    }
}

impl HeaderInjector for TypedRpcResponseSchema {
    type Header<'a> = TypedRpcHeader;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        _values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>> {
        let start = builder.start_table();
        let end = builder.end_table(start);
        WIPOffset::new(end.value())
    }
}

impl TypedPublishMessage for TypedRpcRequestSchema {
    type Msg<'a> = TypedRpcRequestMsg;
    type Root<'a> = flatbuffers::Vector<'a, u8>;

    fn build_typed_root<'a>(
        builder: &mut FlatBufferBuilder<'a>,
        _header: WIPOffset<Self::Header<'a>>,
        msg: &Self::Msg<'_>,
    ) -> WIPOffset<Self::Root<'a>> {
        builder.create_vector(msg.bytes.as_slice())
    }
}

impl TypedPublishMessage for TypedRpcResponseSchema {
    type Msg<'a> = TypedRpcResponseMsg;
    type Root<'a> = flatbuffers::Vector<'a, u8>;

    fn build_typed_root<'a>(
        builder: &mut FlatBufferBuilder<'a>,
        _header: WIPOffset<Self::Header<'a>>,
        msg: &Self::Msg<'_>,
    ) -> WIPOffset<Self::Root<'a>> {
        builder.create_vector(msg.bytes.as_slice())
    }
}

impl TypedSubscribeMessage for TypedRpcRequestSchema {
    type Root<'a> = flatbuffers::Vector<'a, u8>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
        flatbuffers::root::<flatbuffers::Vector<'a, u8>>(payload).map_err(|err| {
            AortaError::invalid_argument(format!("typed rpc request decode failed: {err}"))
        })
    }
}

impl TypedSubscribeMessage for TypedRpcResponseSchema {
    type Root<'a> = flatbuffers::Vector<'a, u8>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
        flatbuffers::root::<flatbuffers::Vector<'a, u8>>(payload).map_err(|err| {
            AortaError::invalid_argument(format!("typed rpc response decode failed: {err}"))
        })
    }
}

fn unique_service(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sdk_rpc/p{}/{}_{}", std::process::id(), base, id)
}

fn build_typed_rpc_payload<Message>(msg: &Message::Msg<'_>) -> Vec<u8>
where
    Message: TypedPublishMessage,
{
    let mut builder = FlatBufferBuilder::new();
    let header = Message::build_header(
        &mut builder,
        &HeaderValues {
            publish_stamp_ns: 0,
            publisher_node: "test",
            sequence: 0,
        },
    );
    let root = Message::build_typed_root(&mut builder, header, msg);
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[test]
fn client_options_default_values() {
    let options = ClientOptions::default();
    assert_eq!(options.timeout, Duration::from_millis(5000));
    assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Inherit);
    assert!(options.service_schema.is_none());
    assert!(options.request_root_type.is_none());
    assert!(options.response_root_type.is_none());
    assert!(!options.validate_schema);
    assert!(options.enforce_single_provider.is_none());
    assert!(options.multi_reply_check_window.is_none());
    assert!(options.allow_retry.is_none());
}

#[test]
fn client_options_builder_sets_allow_retry() {
    assert_eq!(
        ClientOptions::new().with_allow_retry(true).allow_retry,
        Some(true)
    );
    assert_eq!(
        ClientOptions::new().with_allow_retry(false).allow_retry,
        Some(false)
    );
}

#[test]
fn client_options_builder_sets_service_root_types() {
    let options = ClientOptions::new()
        .with_request_root_type("sdk.rpc.TypedRpcRequest")
        .with_response_root_type("sdk.rpc.TypedRpcResponse");
    assert_eq!(
        options.request_root_type.as_deref(),
        Some("sdk.rpc.TypedRpcRequest")
    );
    assert_eq!(
        options.response_root_type.as_deref(),
        Some("sdk.rpc.TypedRpcResponse")
    );
}

#[test]
fn service_options_default_values() {
    let options = ServiceOptions::default();
    assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Never);
    assert_eq!(options.callback_group, CallbackGroupType::MutuallyExclusive);
    assert!(options.callback_group_handle.is_none());
    assert_eq!(options.rpc_overflow_policy, RpcOverflowPolicy::FastFailBusy);
    assert_eq!(options.rpc_overflow_block_timeout, Duration::from_millis(0));
    assert!(options.schema.is_none());
}

#[test]
fn service_options_builder_updates_callback_group_fields() {
    let options = ServiceOptions::new()
        .with_callback_group(CallbackGroupType::Reentrant)
        .with_callback_group_handle(7)
        .with_rpc_overflow_policy(RpcOverflowPolicy::Block)
        .with_rpc_overflow_block_timeout(Duration::from_millis(12));
    assert_eq!(options.callback_group, CallbackGroupType::Reentrant);
    assert_eq!(options.callback_group_handle, Some(7));
    assert_eq!(options.rpc_overflow_policy, RpcOverflowPolicy::Block);
    assert_eq!(
        options.rpc_overflow_block_timeout,
        Duration::from_millis(12)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_roundtrip_via_sdk_node_api() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_client", "sdk_test").await?;
    let service_name = unique_service("echo");

    let mut service = service_node
        .create_service_with_options(
            &service_name,
            |request| {
                let payload = request.payload().clone();
                tokio::spawn(async move {
                    let _ = request.reply(payload).await;
                });
            },
            ServiceOptions::new().with_schema_attach_policy(SchemaAttachPolicy::Never),
        )
        .await?;

    let client = client_node.create_client_with_options(
        &service_name,
        ClientOptions::new()
            .with_timeout_ms(2000)
            .with_schema_attach_policy(SchemaAttachPolicy::Never),
    );

    // Readiness gate instead of a fixed sleep: on a loaded CI runner 150 ms
    // is not always enough for the queryable declaration to propagate, and
    // the first call then burns its whole 2 s budget.
    assert!(
        client.wait_for_matching(Duration::from_secs(10)).await?,
        "service provider not discovered within 10s"
    );

    let request = b"hello sdk rpc".to_vec();
    let response = client.call(request.clone()).await?;
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    let client_stats = client.stats();
    assert_eq!(client_stats.call_count, 1);
    assert_eq!(client_stats.success_count, 1);
    assert_eq!(client_stats.timeout_count, 0);
    assert_eq!(client_stats.concurrent_conflict_count, 0);
    assert_eq!(client_stats.error_count, 0);
    assert!(client_stats.last_call_started_ns > 0);
    assert!(client_stats.last_call_finished_ns >= client_stats.last_call_started_ns);

    // The handler replies from a SPAWNED task: the client's call() returns
    // when the reply arrives over the wire, which races the server-side
    // bookkeeping (request_received -> request_dispatched -> mark_reply_ok)
    // that completes on the executor thread after the send. On a heavily
    // loaded CI runner the executor can lag far enough that an immediate
    // sample sees the request received but not yet dispatched/replied (this
    // flaked on the GitHub-hosted runner with request_dispatched_count == 0).
    // Poll on the terminal counter (reply_ok_count, written last) with a
    // generous deadline so the whole pipeline has settled before we assert —
    // the same race class the core trace tests handle with a drain-retry.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let service_stats = loop {
        let stats = service.stats();
        if stats.reply_ok_count >= 1 || tokio::time::Instant::now() > deadline {
            break stats;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(service_stats.request_received_count, 1);
    assert_eq!(service_stats.request_dispatched_count, 1);
    assert_eq!(service_stats.reply_ok_count, 1);
    assert_eq!(service_stats.reply_error_count, 0);
    assert_eq!(service_stats.reply_send_failure_count, 0);
    assert_eq!(service_stats.request_dropped_count, 0);
    assert_eq!(service_stats.in_flight_requests, 0);
    assert!(service_stats.last_request_ns > 0);
    assert!(service_stats.last_reply_ns > 0);

    service.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

/// The per-client `allow_retry` opt-in must thread through
/// `create_client_with_options` into the core builder. With a non-replying
/// service, the SDK default (no-retry) invokes the provider exactly once,
/// while `with_allow_retry(true)` re-sends before the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_client_allow_retry_option_threads_through() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_retry_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_retry_client", "sdk_test").await?;

    // --- default: no retry => provider invoked exactly once ---
    let no_retry_name = unique_service("retry_default");
    let mut svc_default = service_node
        .create_service_with_options(
            &no_retry_name,
            |_request| {},
            ServiceOptions::new().with_schema_attach_policy(SchemaAttachPolicy::Never),
        )
        .await?;
    let client_default = client_node.create_client_with_options(
        &no_retry_name,
        ClientOptions::new()
            .with_timeout_ms(150)
            .with_schema_attach_policy(SchemaAttachPolicy::Never),
    );
    // Route-discovery barrier (2026-07-14: the fixed 150ms sleep raced the
    // querier's route table under CI load — the measured call then timed
    // out WITHOUT ever reaching the provider, the documented
    // call-before-route-known window). Warm-up calls (each itself a
    // Timeout: the provider never replies) probe until the provider proves
    // routable, then the counter must SETTLE (a warm-up query can still be
    // in flight when its call returns) before the baseline is taken; the
    // measured call asserts on the DELTA from that baseline.
    let route_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = client_default.call(b"warmup".to_vec()).await;
        if svc_default.stats().request_received_count > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < route_deadline,
            "provider never became routable within 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut baseline = svc_default.stats().request_received_count;
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let now = svc_default.stats().request_received_count;
        if now == baseline {
            break;
        }
        baseline = now;
    }
    match client_default.call(b"x".to_vec()).await {
        Err(AortaError::Timeout(_)) => {}
        other => panic!("expected Timeout, got {:?}", other),
    }
    // A would-be (forbidden) re-send fires inside the 150ms call window;
    // 300ms bounds it before the exactly-once check.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        svc_default.stats().request_received_count,
        baseline + 1,
        "SDK default (no-retry) must invoke the provider exactly once"
    );

    // --- opt-in: allow_retry(true) re-sends within the deadline ---
    let retry_name = unique_service("retry_optin");
    let mut svc_retry = service_node
        .create_service_with_options(
            &retry_name,
            |_request| {},
            ServiceOptions::new().with_schema_attach_policy(SchemaAttachPolicy::Never),
        )
        .await?;
    let client_retry = client_node.create_client_with_options(
        &retry_name,
        ClientOptions::new()
            .with_timeout_ms(300)
            .with_allow_retry(true)
            .with_schema_attach_policy(SchemaAttachPolicy::Never),
    );
    // Same route-discovery barrier + settle as the no-retry half.
    let route_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = client_retry.call(b"warmup".to_vec()).await;
        if svc_retry.stats().request_received_count > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < route_deadline,
            "provider never became routable within 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut baseline = svc_retry.stats().request_received_count;
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let now = svc_retry.stats().request_received_count;
        if now == baseline {
            break;
        }
        baseline = now;
    }
    match client_retry.call(b"x".to_vec()).await {
        Err(AortaError::Timeout(_)) => {}
        other => panic!("expected Timeout, got {:?}", other),
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        svc_retry.stats().request_received_count >= baseline + 2,
        "SDK with_allow_retry(true) must re-send (got {} over baseline {})",
        svc_retry.stats().request_received_count,
        baseline
    );

    svc_default.stop();
    svc_retry.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_client_builds_request_and_decodes_response() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_client_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_typed_client", "sdk_test").await?;
    let service_name = unique_service("typed_client_roundtrip");

    let mut service = service_node
        .create_service_typed_with_options::<TypedRpcRequestSchema, TypedRpcResponseSchema, _>(
            &service_name,
            |request| {
                let payload = request.payload().clone();
                let response = crate::with_contiguous_bytes(&payload, |bytes| {
                    let decoded =
                        TypedRpcRequestSchema::decode_typed_root(bytes).expect("decode request");
                    let echoed = decoded
                        .iter()
                        .map(|b| b.wrapping_add(1))
                        .collect::<Vec<_>>();
                    build_typed_rpc_payload::<TypedRpcResponseSchema>(&TypedRpcResponseMsg {
                        bytes: echoed,
                    })
                });
                tokio::spawn(async move {
                    let _ = request.reply(response).await;
                });
            },
            ServiceOptions::new().with_schema_attach_policy(SchemaAttachPolicy::Never),
        )
        .await?;

    let client = client_node
        .create_client_typed::<TypedRpcRequestSchema, TypedRpcResponseSchema>(&service_name)?;
    // Readiness gate instead of a fixed sleep.
    assert!(
        client
            .raw()
            .wait_for_matching(Duration::from_secs(10))
            .await?,
        "service provider not discovered within 10s"
    );
    let response = client
        .call(|msg| {
            msg.bytes = vec![1, 2, 3];
        })
        .await?;

    let decoded = response.decode()?;
    let collected = decoded.iter().collect::<Vec<_>>();
    assert_eq!(collected, vec![2, 3, 4]);
    assert!(!response.payload().is_empty());

    service.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_service_view_decodes_request_and_replies_typed() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_view_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_typed_view_client", "sdk_test").await?;
    let service_name = unique_service("typed_view");

    let mut service = service_node
        .create_service_typed_view::<TypedRpcRequestSchema, TypedRpcResponseSchema, _>(
            &service_name,
            |request, reply| {
                assert_eq!(reply.caller_namespace(), None);
                let request_bytes = request.iter().collect::<Vec<_>>();
                tokio::spawn(async move {
                    let _ = reply
                        .reply(|msg| {
                            msg.bytes = request_bytes
                                .into_iter()
                                .map(|value| value.saturating_mul(2))
                                .collect();
                        })
                        .await;
                });
            },
        )
        .await?;

    let client = client_node
        .create_client_with_options(&service_name, ClientOptions::new().with_timeout_ms(2000));

    // Readiness gate instead of a fixed sleep.
    assert!(
        client.wait_for_matching(Duration::from_secs(10)).await?,
        "service provider not discovered within 10s"
    );

    let request = build_typed_rpc_payload::<TypedRpcRequestSchema>(&TypedRpcRequestMsg {
        bytes: vec![4, 5, 6],
    });
    let response = client.call(request).await?;
    let response_bytes = crate::with_contiguous_bytes(&response, |bytes| bytes.to_vec());
    let decoded = TypedRpcResponseSchema::decode_typed_root(&response_bytes)?;
    assert_eq!(decoded.iter().collect::<Vec<_>>(), vec![8, 10, 12]);

    service.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_service_retained_supports_async_reply() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_retained_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_typed_retained_client", "sdk_test").await?;
    let service_name = unique_service("typed_retained");

    let mut service = service_node
        .create_service_typed_retained::<TypedRpcRequestSchema, TypedRpcResponseSchema, _>(
            &service_name,
            |request| {
                assert_eq!(request.caller_namespace(), None);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let decoded = request.decode().expect("decode retained request");
                    let response_bytes = decoded.iter().rev().collect::<Vec<_>>();
                    let _ = request
                        .reply(|msg| {
                            msg.bytes = response_bytes;
                        })
                        .await;
                });
            },
        )
        .await?;

    let client = client_node
        .create_client_with_options(&service_name, ClientOptions::new().with_timeout_ms(2000));

    // Readiness gate instead of a fixed sleep.
    assert!(
        client.wait_for_matching(Duration::from_secs(10)).await?,
        "service provider not discovered within 10s"
    );

    let request = build_typed_rpc_payload::<TypedRpcRequestSchema>(&TypedRpcRequestMsg {
        bytes: vec![7, 8, 9],
    });
    let response = client.call(request).await?;
    let response_bytes = crate::with_contiguous_bytes(&response, |bytes| bytes.to_vec());
    let decoded = TypedRpcResponseSchema::decode_typed_root(&response_bytes)?;
    assert_eq!(decoded.iter().collect::<Vec<_>>(), vec![9, 8, 7]);

    service.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_client_timeout_returns_timeout_error() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_timeout_service", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_timeout_client", "sdk_test").await?;
    let service_name = unique_service("timeout");

    let mut service = service_node
        .create_service_with_options(
            &service_name,
            |_request| {},
            ServiceOptions::new().with_schema_attach_policy(SchemaAttachPolicy::Never),
        )
        .await?;

    let client = client_node.create_client_with_options(
        &service_name,
        ClientOptions::new()
            .with_timeout_ms(120)
            .with_schema_attach_policy(SchemaAttachPolicy::Never),
    );

    // Wait for Zenoh routing convergence. Use a generous sleep (matching core
    // integration tests) to avoid flakes when the queryable declaration hasn't
    // propagated yet.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Issue a probe call with a longer timeout to confirm the service queryable
    // is reachable before running the actual tight-timeout test.
    {
        let probe_client = client_node.create_client_with_options(
            &service_name,
            ClientOptions::new()
                .with_timeout_ms(2000)
                .with_schema_attach_policy(SchemaAttachPolicy::Never),
        );
        // The handler never replies, so this will time out — but the service
        // will have received at least one request, confirming discovery.
        let _ = probe_client.call(b"discovery probe").await;
        assert!(
            service.stats().request_received_count >= 1,
            "service should be discoverable after probe"
        );
    }

    let result = client.call(b"hello timeout").await;
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

    let service_stats = service.stats();
    // request_received_count may be > 1 because of the discovery probe and
    // potential client retries within the timeout window.
    assert!(
        service_stats.request_received_count >= 2,
        "service should have received probe + at least one test request, got {}",
        service_stats.request_received_count,
    );
    assert_eq!(
        service_stats.request_dispatched_count,
        service_stats.request_received_count
    );
    assert_eq!(service_stats.reply_ok_count, 0);
    assert_eq!(service_stats.reply_error_count, 0);
    assert!(service_stats.request_dropped_count >= 1);
    assert_eq!(service_stats.in_flight_requests, 0);
    assert!(service_stats.last_request_ns > 0);
    assert_eq!(service_stats.last_reply_ns, 0);

    service.stop();
    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_options_schema_registers_schema_by_key() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_schema_service", "sdk_test").await?;
    let service_name = unique_service("schema_registry");

    let service_bfbs = b"sdk_rpc_service_schema".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let schema = ServiceSchemaInfo {
        schema: service_hash,
        schema_bfbs: service_bfbs.clone(),
        request_root_type: None,
        response_root_type: None,
    };

    let mut service = service_node
        .create_service_with_options(
            &service_name,
            |_request| {},
            ServiceOptions::new()
                .with_schema_attach_policy(SchemaAttachPolicy::Always)
                .with_schema(schema),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(150)).await;

    let discovered = service_node
        .inner()
        .schema_registry()
        .fetch_schema_by_key_service(&service_name)
        .await?
        .expect("service schema should be registered");
    assert_eq!(discovered.service_hash, service_hash);
    assert_eq!(discovered.service_bfbs, service_bfbs);

    service.stop();
    service_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_service_registers_schema_by_key_automatically() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_service", "sdk_test").await?;
    let service_name = unique_service("typed_schema_registry");

    let mut service = service_node
        .create_service_typed::<TypedRpcRequestSchema, TypedRpcResponseSchema, _>(
            &service_name,
            |_request| {},
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(150)).await;

    let discovered = service_node
        .inner()
        .schema_registry()
        .fetch_schema_by_key_service(&service_name)
        .await?
        .expect("typed service schema should be registered");
    assert_eq!(
        discovered.service_hash,
        SchemaHash::from_bfbs_bytes(TypedRpcRequestSchema::schema_bfbs())
    );
    assert_eq!(
        discovered.service_bfbs,
        TypedRpcRequestSchema::schema_bfbs()
    );
    assert_eq!(
        discovered.request_root_type.as_deref(),
        TypedRpcRequestSchema::root_type()
    );
    assert_eq!(
        discovered.response_root_type.as_deref(),
        TypedRpcResponseSchema::root_type()
    );

    service.stop();
    service_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_service_rejects_schema_hash_mismatch() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_mismatch", "sdk_test").await?;
    let service_name = unique_service("typed_schema_mismatch");

    let options = ServiceOptions::new().with_schema(ServiceSchemaInfo {
        schema: SchemaHash::from_bfbs_bytes(b"other_request"),
        schema_bfbs: b"other_request".to_vec(),
        request_root_type: None,
        response_root_type: None,
    });

    let result = service_node
        .create_service_typed_with_options::<TypedRpcRequestSchema, TypedRpcResponseSchema, _>(
            &service_name,
            |_request| {},
            options,
        )
        .await;

    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("typed service schema hash does not match"));
        }
        Ok(_) => panic!("expected InvalidArgument, got Ok"),
        Err(other) => panic!("expected InvalidArgument, got {}", other),
    }

    service_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_service_requires_root_type_metadata() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_typed_missing_root", "sdk_test").await?;
    let service_name = unique_service("typed_missing_root");

    let result = service_node
        .create_service_typed::<TypedRpcSchemaWithoutRoot, TypedRpcSchemaWithoutRoot, _>(
            &service_name,
            |_request| {},
        )
        .await;

    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("must declare root_type"));
        }
        Ok(_) => panic!("expected InvalidArgument, got Ok"),
        Err(other) => panic!("expected InvalidArgument, got {}", other),
    }

    service_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_client_detects_multiple_replies() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_strict_provider", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_strict_client", "sdk_test").await?;
    let service_name = unique_service("strict_conflict");
    let key_expr = client_node
        .inner()
        .key_builder()
        .service_key_server(&service_name)?;
    let key_expr: String = key_expr.into();

    let queryable = service_node
        .inner()
        .session()
        .inner()
        .declare_queryable(key_expr.clone())
        .await?;
    let query_task = tokio::spawn(async move {
        if let Ok(query) = queryable.recv_async().await {
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            let _ = query.reply(&key_expr, payload.clone()).await;
            let _ = query.reply(&key_expr, payload).await;
        }
        let _ = queryable.undeclare().await;
    });

    let client = client_node.create_client_with_options(
        &service_name,
        ClientOptions::new()
            .with_timeout_ms(3000)
            .with_schema_attach_policy(SchemaAttachPolicy::Never)
            .with_enforce_single_provider(true)
            .with_multi_reply_check_window_ms(500),
    );

    tokio::time::sleep(Duration::from_millis(250)).await;

    let result = client.call(vec![1, 2, 3]).await;
    match result {
        Err(AortaError::ConcurrentConflict(msg)) => {
            assert!(msg.contains("multiple replies"));
        }
        other => panic!("expected ConcurrentConflict, got {:?}", other),
    }

    let stats = client.stats();
    assert_eq!(stats.call_count, 1);
    assert_eq!(stats.success_count, 0);
    assert_eq!(stats.timeout_count, 0);
    assert_eq!(stats.concurrent_conflict_count, 1);
    assert_eq!(stats.error_count, 0);

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = query_task.await;

    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_disable_strict_allows_first_reply() -> AortaResult<()> {
    let service_node = Node::new("sdk_rpc_non_strict_provider", "sdk_test").await?;
    let client_node = Node::new("sdk_rpc_non_strict_client", "sdk_test").await?;
    let service_name = unique_service("strict_override");
    let key_expr = client_node
        .inner()
        .key_builder()
        .service_key_server(&service_name)?;
    let key_expr: String = key_expr.into();

    let queryable = service_node
        .inner()
        .session()
        .inner()
        .declare_queryable(key_expr.clone())
        .await?;
    let query_task = tokio::spawn(async move {
        if let Ok(query) = queryable.recv_async().await {
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            let _ = query.reply(&key_expr, payload.clone()).await;
            let _ = query.reply(&key_expr, payload).await;
        }
        let _ = queryable.undeclare().await;
    });

    let client = client_node.create_client_with_options(
        &service_name,
        ClientOptions::new()
            .with_timeout_ms(3000)
            .with_schema_attach_policy(SchemaAttachPolicy::Never)
            .with_enforce_single_provider(false)
            .with_multi_reply_check_window_ms(500),
    );

    // Wait for queryable declaration propagation to reduce routing-convergence flakes.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let request = vec![4, 5, 6];
    let response = client.call(request.clone()).await?;
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());

    let stats = client.stats();
    assert_eq!(stats.call_count, 1);
    assert_eq!(stats.success_count, 1);
    assert_eq!(stats.timeout_count, 0);
    assert_eq!(stats.concurrent_conflict_count, 0);
    assert_eq!(stats.error_count, 0);

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = query_task.await;

    service_node.close().await?;
    client_node.close().await?;
    Ok(())
}
