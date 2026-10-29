use crate::action::{
    goal_id_to_key, goal_key_to_id, resolve_action_root_types_from_bfbs, ActionInfo,
    ActionReplyCode, ActionRootType, ActionSchema, ActionStatus, DEFAULT_ACTION_DEDUP_TTL_MS,
    DEFAULT_ACTION_RESULT_TTL_MS,
};
use crate::schema::{SchemaHash, SchemaMeta};

#[test]
fn goal_id_to_key_strips_dashes_and_roundtrips() {
    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let goal_key = goal_id_to_key(goal_id).expect("goal key");
    assert_eq!(goal_key, "550e8400e29b41d4a716446655440000");

    let roundtrip = goal_key_to_id(&goal_key).expect("goal id");
    assert_eq!(roundtrip, goal_id);
}

#[test]
fn goal_key_to_id_rejects_invalid_length() {
    assert!(goal_key_to_id("1234").is_err());
}

// Tiny synthetic bfbs builders used to exercise
// `resolve_action_root_types_from_bfbs` without requiring a real action
// .fbs in this test crate. Each test constructs a schema object list
// directly via flatbuffers and serializes a `reflection.Schema` root.
fn build_synthetic_action_bfbs(table_names: &[&str]) -> Vec<u8> {
    use flatbuffers_reflection::reflection;
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

    let objects: Vec<_> = table_names
        .iter()
        .map(|name| {
            let name_off = builder.create_string(name);
            let empty_fields =
                builder.create_vector::<flatbuffers::ForwardsUOffset<reflection::Field>>(&[]);
            let args = reflection::ObjectArgs {
                name: Some(name_off),
                fields: Some(empty_fields),
                is_struct: false,
                minalign: 1,
                bytesize: 0,
                attributes: None,
                documentation: None,
                declaration_file: None,
            };
            reflection::Object::create(&mut builder, &args)
        })
        .collect();
    let objects_vec = builder.create_vector(&objects);
    let empty_enums = builder.create_vector::<flatbuffers::ForwardsUOffset<reflection::Enum>>(&[]);

    let schema_args = reflection::SchemaArgs {
        objects: Some(objects_vec),
        enums: Some(empty_enums),
        file_ident: None,
        file_ext: None,
        root_table: None,
        services: None,
        advanced_features: reflection::AdvancedFeatures::empty(),
        fbs_files: None,
    };
    let schema = reflection::Schema::create(&mut builder, &schema_args);
    builder.finish(schema, Some("BFBS"));
    builder.finished_data().to_vec()
}

const ALL_EIGHT_RCP_TABLES: &[&str] = &[
    "aorta.action.demo.ExecuteTaskSendGoalRequest",
    "aorta.action.demo.ExecuteTaskSendGoalResponse",
    "aorta.action.demo.ExecuteTaskCancelGoalRequest",
    "aorta.action.demo.ExecuteTaskCancelGoalResponse",
    "aorta.action.demo.ExecuteTaskFeedback",
    "aorta.action.demo.ExecuteTaskPublicStatus",
    "aorta.action.demo.ExecuteTaskGetResultRequest",
    "aorta.action.demo.ExecuteTaskGetResultResponse",
];

#[test]
fn resolve_action_root_types_from_bfbs_finds_all_eight_surfaces() {
    let bfbs = build_synthetic_action_bfbs(ALL_EIGHT_RCP_TABLES);
    let names = resolve_action_root_types_from_bfbs(&bfbs).expect("resolve");
    assert_eq!(
        names.for_surface(ActionRootType::SendGoalRequest),
        "aorta.action.demo.ExecuteTaskSendGoalRequest"
    );
    assert_eq!(
        names.for_surface(ActionRootType::Feedback),
        "aorta.action.demo.ExecuteTaskFeedback"
    );
    assert_eq!(
        names.for_surface(ActionRootType::GetResultResponse),
        "aorta.action.demo.ExecuteTaskGetResultResponse"
    );
}

#[test]
fn resolve_action_root_types_from_bfbs_ignores_action_common_namespace() {
    // The shared `aorta.action.common.*` tables ride along inside every
    // action bfbs. They must be excluded from the surface match so they
    // don't ambiguously collide with the action-specific surfaces.
    let mut tables: Vec<&str> = ALL_EIGHT_RCP_TABLES.to_vec();
    tables.push("aorta.action.common.ActionReplyCode");
    tables.push("aorta.action.common.ActionStatus");
    tables.push("aorta.action.common.ActionInfo");
    let bfbs = build_synthetic_action_bfbs(&tables);
    let names = resolve_action_root_types_from_bfbs(&bfbs).expect("resolve");
    assert_eq!(
        names.for_surface(ActionRootType::SendGoalRequest),
        "aorta.action.demo.ExecuteTaskSendGoalRequest"
    );
}

#[test]
fn resolve_action_root_types_from_bfbs_supports_non_ascii_namespace() {
    // The whole point of Option B: action names (and therefore the
    // .fbs `namespace` declaration projected from them) can use the
    // relaxed keyexpr character set. Verify the bfbs resolver doesn't
    // re-impose an ASCII-only constraint.
    let tables = [
        "aorta.action.导航.FollowPathSendGoalRequest",
        "aorta.action.导航.FollowPathSendGoalResponse",
        "aorta.action.导航.FollowPathCancelGoalRequest",
        "aorta.action.导航.FollowPathCancelGoalResponse",
        "aorta.action.导航.FollowPathFeedback",
        "aorta.action.导航.FollowPathPublicStatus",
        "aorta.action.导航.FollowPathGetResultRequest",
        "aorta.action.导航.FollowPathGetResultResponse",
    ];
    let bfbs = build_synthetic_action_bfbs(&tables);
    let names = resolve_action_root_types_from_bfbs(&bfbs).expect("resolve");
    assert_eq!(
        names.for_surface(ActionRootType::Feedback),
        "aorta.action.导航.FollowPathFeedback"
    );
}

#[test]
fn resolve_action_root_types_from_bfbs_supports_hyphenated_prefix() {
    // Hyphens in the .fbs *table prefix* are not actually possible
    // (FlatBuffers identifier syntax forbids them in table names), but
    // hyphens in the *Zenoh-side action_name* are fine — the bfbs
    // tables themselves still follow FlatBuffers syntax. This test
    // verifies the resolver doesn't reject otherwise-valid bfbs that
    // happens to coexist with a hyphenated action_name on the wire.
    // (Resolver inputs: bfbs only — it doesn't even see action_name.)
    let bfbs = build_synthetic_action_bfbs(ALL_EIGHT_RCP_TABLES);
    let _ = resolve_action_root_types_from_bfbs(&bfbs).expect("resolve");
}

#[test]
fn resolve_action_root_types_from_bfbs_rejects_missing_surface() {
    // Drop SendGoalRequest from the set.
    let tables: Vec<&str> = ALL_EIGHT_RCP_TABLES
        .iter()
        .copied()
        .filter(|name| !name.ends_with("SendGoalRequest"))
        .collect();
    let bfbs = build_synthetic_action_bfbs(&tables);
    let err = resolve_action_root_types_from_bfbs(&bfbs).expect_err("missing");
    let msg = err.to_string();
    assert!(
        msg.contains("SendGoalRequest"),
        "error must mention the missing surface: {msg}"
    );
}

#[test]
fn resolve_action_root_types_from_bfbs_rejects_duplicate_surface() {
    let mut tables: Vec<&str> = ALL_EIGHT_RCP_TABLES.to_vec();
    tables.push("aorta.action.demo.ExtraExecuteTaskFeedback");
    let bfbs = build_synthetic_action_bfbs(&tables);
    let err = resolve_action_root_types_from_bfbs(&bfbs).expect_err("duplicate");
    let msg = err.to_string();
    assert!(
        msg.contains("multiple tables") && msg.contains("Feedback"),
        "error must explain the surface clash: {msg}"
    );
}

#[test]
fn resolve_action_root_types_from_bfbs_rejects_empty_bfbs() {
    let err = resolve_action_root_types_from_bfbs(&[]).expect_err("empty");
    assert!(err.to_string().contains("empty"));
}

#[test]
fn action_status_terminal_classification_matches_spec() {
    assert!(!ActionStatus::Accepted.is_terminal());
    assert!(!ActionStatus::Executing.is_terminal());
    assert!(!ActionStatus::Canceling.is_terminal());
    assert!(ActionStatus::Succeeded.is_terminal());
    assert!(ActionStatus::Aborted.is_terminal());
    assert!(ActionStatus::Canceled.is_terminal());
}

#[test]
fn action_reply_code_wire_values_match_schema() {
    assert_eq!(ActionReplyCode::Ok as i16, 1);
    assert_eq!(ActionReplyCode::Busy as i16, 200);
    assert_eq!(ActionReplyCode::HandlerPanic as i16, 301);
    assert_eq!(ActionReplyCode::Conflict as i16, 400);
}

#[test]
fn action_status_try_from_wire_value_matches_schema() {
    assert_eq!(
        ActionStatus::try_from(1i8).expect("accepted"),
        ActionStatus::Accepted
    );
    assert_eq!(
        ActionStatus::try_from(6i8).expect("canceled"),
        ActionStatus::Canceled
    );
    assert!(ActionStatus::try_from(99i8).is_err());
}

#[test]
fn action_reply_code_try_from_wire_value_matches_schema() {
    assert_eq!(
        ActionReplyCode::try_from(1i16).expect("ok"),
        ActionReplyCode::Ok
    );
    assert_eq!(
        ActionReplyCode::try_from(205i16).expect("goal expired"),
        ActionReplyCode::GoalExpired
    );
    assert!(ActionReplyCode::try_from(999i16).is_err());
}

#[test]
fn action_info_roundtrips_via_flatbuffer() {
    let info = ActionInfo {
        action_name: "demo/execute_task".to_string(),
        schema_hash: SchemaHash::from_bfbs_bytes(b"execute-task-bfbs"),
        supports_cancel: true,
        max_concurrency: 4,
        result_ttl_ms: 15_000,
        dedup_ttl_ms: 90_000,
        server_node_name: "planner_server".to_string(),
        server_epoch: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };

    let encoded = info.encode().expect("encode action info");
    let decoded = ActionInfo::decode(&encoded).expect("decode action info");
    assert_eq!(decoded, info);
}

#[test]
fn action_info_decode_uses_flatbuffer_ttl_defaults() {
    let info = ActionInfo {
        action_name: "demo/execute_task".to_string(),
        schema_hash: SchemaHash::from_bfbs_bytes(b"execute-task-bfbs"),
        supports_cancel: true,
        max_concurrency: 1,
        result_ttl_ms: DEFAULT_ACTION_RESULT_TTL_MS,
        dedup_ttl_ms: DEFAULT_ACTION_DEDUP_TTL_MS,
        server_node_name: "planner_server".to_string(),
        server_epoch: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };

    let encoded = info.encode().expect("encode action info");
    let decoded = ActionInfo::decode(&encoded).expect("decode action info");
    assert!(decoded.supports_cancel);
    assert_eq!(decoded.result_ttl_ms, DEFAULT_ACTION_RESULT_TTL_MS);
    assert_eq!(decoded.dedup_ttl_ms, DEFAULT_ACTION_DEDUP_TTL_MS);
}

#[test]
fn action_info_encode_rejects_zero_max_concurrency() {
    let info = ActionInfo {
        action_name: "demo/execute_task".to_string(),
        schema_hash: SchemaHash::from_bfbs_bytes(b"execute-task-bfbs"),
        supports_cancel: true,
        max_concurrency: 0,
        result_ttl_ms: DEFAULT_ACTION_RESULT_TTL_MS,
        dedup_ttl_ms: DEFAULT_ACTION_DEDUP_TTL_MS,
        server_node_name: "planner_server".to_string(),
        server_epoch: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };

    assert!(info.encode().is_err());
}

#[test]
fn action_info_encode_rejects_invalid_ttl_order() {
    let info = ActionInfo {
        action_name: "demo/execute_task".to_string(),
        schema_hash: SchemaHash::from_bfbs_bytes(b"execute-task-bfbs"),
        supports_cancel: true,
        max_concurrency: 1,
        result_ttl_ms: 60_000,
        dedup_ttl_ms: 59_999,
        server_node_name: "planner_server".to_string(),
        server_epoch: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };

    assert!(info.encode().is_err());
}

// Generated typed wrapper crates emit one `SchemaMeta` impl per surface,
// each returning the fully-qualified table name from `root_type()`.
// The macro below mirrors that codegen shape so the trait default can
// dispatch through `Self::<Surface>::root_type()` instead of deriving
// from `ACTION_NAME`.
macro_rules! impl_schema_meta_with_root_type {
    ($ty:ident, $root_type:expr) => {
        struct $ty;
        impl SchemaMeta for $ty {
            fn schema_bfbs() -> &'static [u8] {
                b"dummy-action-bfbs"
            }
            fn root_type() -> Option<&'static str> {
                Some($root_type)
            }
        }
    };
}

impl_schema_meta_with_root_type!(GoalSchema, "aorta.action.demo.ExecuteTaskGoal");
impl_schema_meta_with_root_type!(ResultSchema, "aorta.action.demo.ExecuteTaskResult");
impl_schema_meta_with_root_type!(
    FeedbackDataSchema,
    "aorta.action.demo.ExecuteTaskFeedbackData"
);
impl_schema_meta_with_root_type!(
    SendGoalRequestSchema,
    "aorta.action.demo.ExecuteTaskSendGoalRequest"
);
impl_schema_meta_with_root_type!(
    SendGoalResponseSchema,
    "aorta.action.demo.ExecuteTaskSendGoalResponse"
);
impl_schema_meta_with_root_type!(
    CancelGoalRequestSchema,
    "aorta.action.demo.ExecuteTaskCancelGoalRequest"
);
impl_schema_meta_with_root_type!(
    CancelGoalResponseSchema,
    "aorta.action.demo.ExecuteTaskCancelGoalResponse"
);
impl_schema_meta_with_root_type!(FeedbackSchema, "aorta.action.demo.ExecuteTaskFeedback");
impl_schema_meta_with_root_type!(
    PublicStatusSchema,
    "aorta.action.demo.ExecuteTaskPublicStatus"
);
impl_schema_meta_with_root_type!(
    GetResultRequestSchema,
    "aorta.action.demo.ExecuteTaskGetResultRequest"
);
impl_schema_meta_with_root_type!(
    GetResultResponseSchema,
    "aorta.action.demo.ExecuteTaskGetResultResponse"
);

struct ExecuteTaskAction;

impl ActionSchema for ExecuteTaskAction {
    type Goal = GoalSchema;
    type Result = ResultSchema;
    type FeedbackData = FeedbackDataSchema;
    type SendGoalRequest = SendGoalRequestSchema;
    type SendGoalResponse = SendGoalResponseSchema;
    type CancelGoalRequest = CancelGoalRequestSchema;
    type CancelGoalResponse = CancelGoalResponseSchema;
    type Feedback = FeedbackSchema;
    type PublicStatus = PublicStatusSchema;
    type GetResultRequest = GetResultRequestSchema;
    type GetResultResponse = GetResultResponseSchema;

    const ACTION_NAME: &'static str = "demo/execute_task";
}

#[test]
fn action_schema_root_type_dispatches_via_associated_schema_meta() {
    // The new default no longer depends on ACTION_NAME parsing — each
    // surface comes from the corresponding associated `SchemaMeta::
    // root_type()`. Exercise every surface to catch dispatch bugs.
    for (surface, expected) in [
        (
            ActionRootType::SendGoalRequest,
            "aorta.action.demo.ExecuteTaskSendGoalRequest",
        ),
        (
            ActionRootType::SendGoalResponse,
            "aorta.action.demo.ExecuteTaskSendGoalResponse",
        ),
        (
            ActionRootType::CancelGoalRequest,
            "aorta.action.demo.ExecuteTaskCancelGoalRequest",
        ),
        (
            ActionRootType::CancelGoalResponse,
            "aorta.action.demo.ExecuteTaskCancelGoalResponse",
        ),
        (
            ActionRootType::Feedback,
            "aorta.action.demo.ExecuteTaskFeedback",
        ),
        (
            ActionRootType::PublicStatus,
            "aorta.action.demo.ExecuteTaskPublicStatus",
        ),
        (
            ActionRootType::GetResultRequest,
            "aorta.action.demo.ExecuteTaskGetResultRequest",
        ),
        (
            ActionRootType::GetResultResponse,
            "aorta.action.demo.ExecuteTaskGetResultResponse",
        ),
    ] {
        assert_eq!(
            ExecuteTaskAction::root_type_for(surface),
            expected,
            "surface {:?}",
            surface
        );
    }
    assert_eq!(
        ExecuteTaskAction::schema_hash(),
        SchemaHash::from_bfbs_bytes(b"dummy-action-bfbs")
    );
}
