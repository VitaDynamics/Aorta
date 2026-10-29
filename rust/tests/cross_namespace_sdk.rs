//! Rust SDK surface tests for the cross-namespace `_for` consumer APIs
//! (Cloud Cross-Namespace Addressing Proposal §5). These exercise the SDK
//! wrappers and their fail-fast namespace
//! rules without needing a peer robot — node construction uses the in-process
//! local test router (`AORTA_TEST_LOCAL_ROUTER`).

use aorta_sdk::{Node, ServiceOptions, Target};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_client_for_rejects_wildcard_namespace() {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
    let node = Node::new("xns_sdk_client", "default")
        .await
        .expect("node create");

    // A concrete robot namespace is accepted.
    assert!(
        node.create_client_for(Target::namespace("R1"), "system/power_cycle")
            .is_ok(),
        "concrete-namespace client should build"
    );

    // A wildcard namespace must be rejected: request/reply targets one robot
    // (QueryTarget::All + first-reply-wins would otherwise broadcast the call).
    assert!(
        node.create_client_for(Target::namespace("*"), "system/power_cycle")
            .is_err(),
        "wildcard-namespace service client should be rejected"
    );
    assert!(
        node.create_client_for(Target::namespace("**"), "system/power_cycle")
            .is_err(),
        "wildcard-namespace service client should be rejected"
    );

    let _ = node.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_subscriber_for_accepts_concrete_and_wildcard() {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
    let node = Node::new("xns_sdk_sub", "default")
        .await
        .expect("node create");

    // Observe surface: both a single robot and fleet fan-out are valid.
    let concrete = node
        .create_subscriber_for(Target::namespace("R1"), "demo/state", |_data, _ctx| {})
        .await;
    assert!(
        concrete.is_ok(),
        "concrete-namespace subscriber should build"
    );

    let fanout = node
        .create_subscriber_for(Target::namespace("*"), "demo/state", |_data, _ctx| {})
        .await;
    assert!(fanout.is_ok(), "wildcard-namespace subscriber should build");

    // A malformed namespace (segment equal to the reserved key root) is rejected.
    let bad = node
        .create_subscriber_for(Target::namespace("aorta"), "demo/state", |_data, _ctx| {})
        .await;
    assert!(bad.is_err(), "namespace segment 'aorta' should be rejected");

    let _ = node.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_all_namespaces_option_reaches_sdk_handler_surface() {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
    let node = Node::new("xns_sdk_service", "default")
        .await
        .expect("node create");

    let mut service = node
        .create_service_with_options(
            "demo/config/get",
            |request| {
                let _: Option<&str> = request.caller_namespace();
            },
            ServiceOptions::new().with_serve_all_namespaces(true),
        )
        .await
        .expect("all-namespaces SDK service should build");

    service.stop();
    drop(service);
    let _ = node.close().await;
}
