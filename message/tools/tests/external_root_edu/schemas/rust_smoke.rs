// EDU external-root fixture — Rust G3 cells: message, service
// and action, each BUILT and RUN against the real SDK.
//
// Everything here comes from targets `aorta_external_schema_package` generated
// in THIS package out of `src/*.fbs`: the flatc modules, the Rust schema-meta,
// the BFBS-driven typed wrapper crates and the root facade `:edu_msgs_rs`.
// Nothing is hand written, and the schemas never entered Aorta's own registry.
//
// The dependency set is deliberately the EDU one — the facade crate plus
// `//rust:aorta_sdk` — so this file is exactly what a user's own `rust_test`
// looks like (`flatbuffers::FlatBufferBuilder` arrives through the SDK
// prelude; a user has no access to Aorta's private `@aorta_crates`).
use aorta_sdk::prelude::*;

use edu_msgs::edu_demo_job_msg::owned as job_owned;
use edu_msgs::edu_demo_job_msg::schema::EduDemoJobGoalSchema;
use edu_msgs::edu_demo_job_msg::typed::EduDemoJobGoalMsg;
use edu_msgs::edu_demo_job_msg::EduDemoJobAction;
use edu_msgs::edu_demo_service_msg::edu_demo_units_generated::edu::demo::EduDemoUnit;
use edu_msgs::edu_demo_service_msg::schema::{
    EduDemoServiceRequestSchema, EduDemoServiceResponseSchema,
};
use edu_msgs::edu_demo_service_msg::typed::{
    EduDemoServiceRequestMsg, EduDemoServiceResponseMsg,
};
use edu_msgs::edu_demo_text_msg::schema::EduDemoTextSchema;
use edu_msgs::edu_demo_text_msg::typed::EduDemoTextMsg;

fn header_values(node: &str) -> HeaderValues<'_> {
    HeaderValues {
        publish_stamp_ns: 1,
        publisher_node: node,
        sequence: 2,
    }
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

// ── message ─────────────────────────────────────────────────────────────────

#[test]
fn message_cell_typed_publish_subscribe_round_trip() {
    let mut builder = FlatBufferBuilder::new();
    let header = <EduDemoTextSchema as HeaderInjector>::build_header(
        &mut builder,
        &header_values("edu_rust_smoke"),
    );
    let msg = EduDemoTextMsg {
        text: Some("hello external root".to_string()),
        count: Some(7),
        ..Default::default()
    };
    let root =
        <EduDemoTextSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);
    let payload = builder.finished_data().to_vec();

    let decoded = <EduDemoTextSchema as TypedSubscribeMessage>::decode_typed_root(&payload)
        .expect("EduDemoText must decode through the typed subscribe path");
    assert_eq!(decoded.text(), Some("hello external root"));
    assert_eq!(decoded.count(), 7);
    assert!(
        decoded.aorta_header().is_some(),
        "the typed publish path must inject the aorta header"
    );
}

#[test]
fn message_cell_schema_meta_is_complete() {
    assert!(is_hex64(&EduDemoTextSchema::schema_hash().to_hex()));
    assert_eq!(
        EduDemoTextSchema::root_type(),
        Some("edu.demo.EduDemoText")
    );
    assert!(!EduDemoTextSchema::schema_bfbs().is_empty());
    assert_eq!(EduDemoTextSchema::fixed_size(), None);
}

// ── service ─────────────────────────────────────────────────────────────────

/// Compile-time proof that both markers satisfy the SDK's typed-RPC bounds,
/// plus the run-time contract `TypedClient` is built on: request and response
/// are two root types of ONE schema, so they MUST hash the same.
fn assert_typed_rpc_pair<Request, Response>()
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage,
{
    assert_eq!(
        Request::schema_hash(),
        Response::schema_hash(),
        "a typed service's request and response must share one schema identity"
    );
    assert!(Request::root_type().is_some());
    assert!(Response::root_type().is_some());
    assert_ne!(Request::root_type(), Response::root_type());
}

#[test]
fn service_cell_request_and_response_share_one_schema_identity() {
    assert_typed_rpc_pair::<EduDemoServiceRequestSchema, EduDemoServiceResponseSchema>();
    assert_eq!(
        EduDemoServiceRequestSchema::root_type(),
        Some("edu.demo.EduDemoServiceRequest")
    );
    assert_eq!(
        EduDemoServiceResponseSchema::root_type(),
        Some("edu.demo.EduDemoServiceResponse")
    );
    assert_eq!(
        EduDemoServiceRequestSchema::schema_bfbs(),
        EduDemoServiceResponseSchema::schema_bfbs(),
        "both root types must be served by the SAME bfbs artifact"
    );
}

#[test]
fn service_cell_request_response_round_trip_uses_the_include_only_enum() {
    let mut request_builder = FlatBufferBuilder::new();
    let request_header = <EduDemoServiceRequestSchema as HeaderInjector>::build_header(
        &mut request_builder,
        &header_values("edu_rust_smoke"),
    );
    // `EduDemoUnit` lives in the include-only `edu_demo_units.fbs` — a
    // registered item with no root type. Naming it here is the proof that an
    // include-only schema really reaches the Rust module closure (its
    // `edu_demo_units_generated` module is copied crate-local next to the
    // wrapper), not merely the C++/Python ones.
    let request = EduDemoServiceRequestMsg {
        message: Some("ping".to_string()),
        unit: Some(EduDemoUnit::SECONDS),
        ..Default::default()
    };
    let request_root = <EduDemoServiceRequestSchema as TypedPublishMessage>::build_typed_root(
        &mut request_builder,
        request_header,
        &request,
    );
    request_builder.finish(request_root, None);
    let request_payload = request_builder.finished_data().to_vec();
    let decoded_request =
        <EduDemoServiceRequestSchema as TypedSubscribeMessage>::decode_typed_root(&request_payload)
            .expect("EduDemoServiceRequest must decode");
    assert_eq!(decoded_request.message(), Some("ping"));
    assert_eq!(decoded_request.unit(), EduDemoUnit::SECONDS);

    let mut response_builder = FlatBufferBuilder::new();
    let response_header = <EduDemoServiceResponseSchema as HeaderInjector>::build_header(
        &mut response_builder,
        &header_values("edu_rust_smoke"),
    );
    let response = EduDemoServiceResponseMsg {
        echoed_message: Some("ping".to_string()),
        unit: Some(EduDemoUnit::SECONDS),
        ..Default::default()
    };
    let response_root = <EduDemoServiceResponseSchema as TypedPublishMessage>::build_typed_root(
        &mut response_builder,
        response_header,
        &response,
    );
    response_builder.finish(response_root, None);
    let response_payload = response_builder.finished_data().to_vec();
    let decoded_response = <EduDemoServiceResponseSchema as TypedSubscribeMessage>::decode_typed_root(
        &response_payload,
    )
    .expect("EduDemoServiceResponse must decode");
    assert_eq!(decoded_response.echoed_message(), Some("ping"));
}

// ── action ──────────────────────────────────────────────────────────────────

#[test]
fn action_cell_send_goal_request_round_trips() {
    assert_eq!(EduDemoJobAction::ACTION_NAME, "edu/demo/job");

    let goal = EduDemoJobGoalMsg {
        target: Some("dock".to_string()),
        repeats: Some(3),
        ..Default::default()
    };
    let payload = <EduDemoJobAction as ActionWire>::encode_send_goal_request("goal-1", &goal)
        .expect("encode_send_goal_request should succeed");
    assert_eq!(
        <EduDemoJobAction as ActionWire>::decode_send_goal_request_goal_id(&payload)
            .expect("goal id must decode"),
        "goal-1"
    );
    let decoded = <EduDemoJobAction as ActionWire>::decode_goal_from_send_goal_request(&payload)
        .expect("goal must decode out of the SendGoalRequest wrapper");
    assert_eq!(decoded.target(), Some("dock"));
    assert_eq!(decoded.repeats(), 3);
}

#[test]
fn action_cell_get_result_response_round_trips() {
    let response = ActionGetResultResponse {
        goal_id: "goal-1".to_string(),
        status: ActionStatus::Succeeded,
        accepted_stamp_ns: 1,
        started_stamp_ns: 2,
        finished_stamp_ns: 3,
        last_feedback_seq: 5,
        feedback_snapshot: job_owned::EduDemoJobFeedbackDataMsg {
            progress_pct: Some(50),
        },
        result: Some(job_owned::EduDemoJobResultMsg {
            duration_ms: Some(1234),
            completed: Some(3),
        }),
        code: ActionReplyCode::Succeeded,
        message: "ok".to_string(),
        server_epoch: "epoch-1".to_string(),
    };

    let payload = <EduDemoJobAction as ActionWire>::encode_get_result_response(&response)
        .expect("encode_get_result_response should succeed");
    let decoded = <EduDemoJobAction as ActionWire>::decode_get_result_response(&payload)
        .expect("decode_get_result_response should succeed");
    assert_eq!(decoded.goal_id, "goal-1");
    assert_eq!(decoded.status, ActionStatus::Succeeded);
    let result = decoded.result.expect("result present");
    assert_eq!(result.duration_ms, Some(1234));
    assert_eq!(result.completed, Some(3));
    assert_eq!(decoded.feedback_snapshot.progress_pct, Some(50));
}

#[test]
fn action_cell_aggregate_identity_is_the_goal_schema() {
    // ActionSchema::schema_hash() defaults to the Goal marker's hash, and the
    // Goal marker's bfbs is the ACTION AGGREGATE's bfbs (gen_action_wrapper
    // expands the minimal schema before flatc compiles it).
    assert_eq!(
        <EduDemoJobAction as ActionSchema>::schema_hash(),
        EduDemoJobGoalSchema::schema_hash()
    );
    assert!(is_hex64(&EduDemoJobGoalSchema::schema_hash().to_hex()));
    assert_eq!(
        EduDemoJobGoalSchema::root_type(),
        Some("edu.demo.EduDemoJobGoal")
    );
}
