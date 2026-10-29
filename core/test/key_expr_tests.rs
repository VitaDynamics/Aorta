use super::{
    key_builder_from_group_env, normalize_path, normalize_path_with_wildcards, KeyExprBuilder,
};
use crate::test_support::{EnvVarGuard, ENV_LOCK};

// =====================================================================
// Trimming and slash handling
// =====================================================================

#[test]
fn normalize_path_strips_leading_and_trailing_slashes() {
    let path = normalize_path("/camera/image/").expect("normalize");
    assert_eq!(path, "camera/image");
}

#[test]
fn normalize_path_rejects_empty_middle_segment() {
    // `//` in the middle was historically collapsed silently — that hid
    // typos. Post-relaxation the middle empty segment is a hard error.
    assert!(normalize_path("camera//image").is_err());
    assert!(normalize_path("/camera//image/").is_err());
}

#[test]
fn normalize_path_rejects_dot_segments() {
    assert!(normalize_path("foo/./bar").is_err());
    assert!(normalize_path("foo/../bar").is_err());
}

// =====================================================================
// Character set — accepted
// =====================================================================

#[test]
fn normalize_path_accepts_uppercase_verbatim() {
    // No case folding under the relaxed rules — `Camera/Image` stays
    // `Camera/Image` byte-for-byte. Routes to a distinct Zenoh key from
    // lowercase `camera/image`.
    assert_eq!(
        normalize_path("Camera/Image").expect("normalize"),
        "Camera/Image"
    );
}

#[test]
fn normalize_path_accepts_hyphen() {
    // Zenoh ecosystem keys often use kebab-case (e.g. `zenoh-rs-pub`).
    assert_eq!(
        normalize_path("zenoh-rs-pub").expect("normalize"),
        "zenoh-rs-pub"
    );
}

#[test]
fn normalize_path_accepts_leading_underscore() {
    // ROS2 hidden-topic convention `_action_namespace`.
    assert_eq!(
        normalize_path("_action_namespace").expect("normalize"),
        "_action_namespace"
    );
}

#[test]
fn normalize_path_accepts_leading_digit() {
    // ROS2 forbids leading digits but Zenoh allows them; we follow the
    // Zenoh side to permit `1080p/stream` / `3d_lidar` style.
    assert_eq!(
        normalize_path("1080p/stream").expect("normalize"),
        "1080p/stream"
    );
}

#[test]
fn normalize_path_accepts_unicode_segments() {
    // Chinese segments (after NFC) flow through verbatim.
    assert_eq!(normalize_path("相机/前置").expect("normalize"), "相机/前置");
}

#[test]
fn normalize_path_nfc_collapses_decomposed_form() {
    // U+0065 (e) + U+0301 (combining acute) → NFC → U+00E9 (é).
    // Validator must normalize so the byte string handed to Zenoh is
    // identical for both inputs.
    let composed = normalize_path("café").expect("nfc composed");
    let decomposed = normalize_path("cafe\u{0301}").expect("nfc decomposed");
    assert_eq!(composed, decomposed);
    assert_eq!(composed, "café");
}

// =====================================================================
// Character set — rejected (Zenoh reserved + control + whitespace)
// =====================================================================

#[test]
fn normalize_path_rejects_zenoh_reserved_chars() {
    assert!(normalize_path("foo#bar").is_err()); // Zenoh-reserved
    assert!(normalize_path("foo$bar").is_err()); // chunk-wildcard prefix; deferred
    assert!(normalize_path("foo?bar").is_err()); // Zenoh-reserved
}

#[test]
fn normalize_path_rejects_control_chars() {
    assert!(normalize_path("foo\x00bar").is_err());
    assert!(normalize_path("foo\nbar").is_err());
}

#[test]
fn normalize_path_rejects_inline_wildcard_on_publish_side() {
    // `*` only valid as a *whole* segment on subscribe-side.
    assert!(normalize_path("cam*").is_err());
    assert!(normalize_path("*").is_err());
    assert!(normalize_path("**").is_err());
}

#[test]
fn normalize_path_with_wildcards_accepts_whole_segment_glob() {
    assert_eq!(
        normalize_path_with_wildcards("*/cmd").expect("normalize"),
        "*/cmd"
    );
    assert_eq!(
        normalize_path_with_wildcards("cam/**").expect("normalize"),
        "cam/**"
    );
    // Even on subscribe-side, inline `$` / `*` mid-segment is not yet
    // supported (deferred).
    assert!(normalize_path_with_wildcards("cam*").is_err());
}

#[test]
fn normalize_path_rejects_empty() {
    assert!(normalize_path("").is_err());
    assert!(normalize_path("   ").is_err());
}

// =====================================================================
// Length caps
// =====================================================================

#[test]
fn normalize_path_rejects_segment_exceeding_max_len() {
    // Cap is 128 UTF-8 bytes.
    assert!(normalize_path(&"a".repeat(129)).is_err());
    assert!(normalize_path(&"a".repeat(128)).is_ok());
}

#[test]
fn normalize_path_rejects_suffix_exceeding_max_len() {
    // Cap is 512 UTF-8 bytes.
    let seg = "a".repeat(100);
    let too_long = std::iter::repeat_n(seg.as_str(), 6)
        .collect::<Vec<_>>()
        .join("/");
    // 6×100 + 5 slashes = 605, exceeds 512
    assert!(normalize_path(&too_long).is_err());
}

// =====================================================================
// KeyExprBuilder API split — publish vs subscribe
// =====================================================================

#[test]
fn topic_key_pub_rejects_wildcards() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.topic_key_pub("*/cmd").is_err());
}

#[test]
fn topic_key_sub_accepts_wildcards() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.topic_key_sub("*/cmd").expect("sub key").into();
    assert_eq!(key, "aorta/default/pub/*/cmd");
}

#[test]
fn telemetry_key_sub_builds_sys_telemetry_pattern() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .telemetry_key_sub("ctx/owner_conflict")
        .expect("telemetry key")
        .into();
    assert_eq!(key, "aorta/default/sys/telemetry/ctx/owner_conflict");
    let wild: String = builder.telemetry_key_sub("**").expect("wild").into();
    assert_eq!(wild, "aorta/default/sys/telemetry/**");
}

#[test]
fn rpc_record_key_sub_builds_sys_record_srv_pattern() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .rpc_record_key_sub("**")
        .expect("rpc record key")
        .into();
    assert_eq!(key, "aorta/default/sys/record/srv/**");
}

#[test]
fn service_key_server_rejects_wildcards() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.service_key_server("nav/*").is_err());
}

#[test]
fn service_key_server_all_namespaces_builds_fleet_queryable() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .service_key_server_all_namespaces("demo/config/get")
        .expect("all-namespaces service key")
        .into();
    assert_eq!(key, "**/aorta/default/srv/demo/config/get");
}

#[test]
fn service_key_server_all_namespaces_rejects_wildcard_service_name() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.service_key_server_all_namespaces("demo/*").is_err());
}

#[test]
fn service_key_server_all_namespaces_rejects_app_namespace() {
    let builder = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("R1")
        .expect("namespace");
    assert!(builder
        .service_key_server_all_namespaces("demo/config/get")
        .is_err());
}

#[test]
fn service_key_client_accepts_wildcards() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .service_key_client("nav/**")
        .expect("client key")
        .into();
    assert_eq!(key, "aorta/default/srv/nav/**");
}

#[test]
fn service_owner_key_and_query_have_expected_shape() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .service_owner_key("nav/plan", "abc123")
        .expect("owner key")
        .into();
    assert_eq!(key, "aorta/default/sys/svc_owner/nav/plan/abc123");

    // The query's trailing `*` matches exactly the node-id segment, so it never
    // bleeds into a sibling service sharing a prefix.
    let query: String = builder
        .service_owner_query("nav/plan")
        .expect("owner query")
        .into();
    assert_eq!(query, "aorta/default/sys/svc_owner/nav/plan/*");
}

#[test]
fn action_owner_key_and_query_have_expected_shape() {
    // The action server's alive token mirrors the
    // service owner token layout.
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .action_owner_key("nav/goto", "abc123")
        .expect("owner key")
        .into();
    assert_eq!(key, "aorta/default/sys/act_owner/nav/goto/abc123");

    let query: String = builder
        .action_owner_query("nav/goto")
        .expect("owner query")
        .into();
    assert_eq!(query, "aorta/default/sys/act_owner/nav/goto/*");
}

#[test]
fn service_owner_key_node_id_extracts_last_segment() {
    assert_eq!(
        super::service_owner_key_node_id("aorta/default/sys/svc_owner/nav/plan/abc123"),
        Some("abc123")
    );
    assert_eq!(
        super::service_owner_key_node_id("aorta/default/sys/svc_owner/echo/n1"),
        Some("n1")
    );
    assert_eq!(super::service_owner_key_node_id(""), None);
}

#[test]
fn service_owner_key_rejects_wildcard_service() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.service_owner_key("nav/*", "n1").is_err());
}

// =====================================================================
// Existing publish-side builder behavior
// =====================================================================

#[test]
fn key_suffix_from_full_key_works() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_from_full_key("aorta/default/pub/camera/image")
        .expect("suffix");
    assert_eq!(suffix, "pub/camera/image");
}

#[test]
fn key_suffix_from_full_key_rejects_other_group() {
    let builder = KeyExprBuilder::new("alpha").expect("builder");
    assert!(builder
        .key_suffix_from_full_key("aorta/beta/pub/camera/image")
        .is_none());
}

#[test]
fn key_suffix_for_param_normalizes_segments() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_for_param("/node/", "/param/name/")
        .expect("suffix");
    assert_eq!(suffix, "prm/node/param/name");
}

#[test]
fn key_suffix_for_service_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_for_service("/nav/get_plan/")
        .expect("suffix");
    assert_eq!(suffix, "srv/nav/get_plan");
}

#[test]
fn param_key_owner_builds_full_key() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .param_key_owner("/node/", "/param/")
        .expect("param key")
        .into();
    assert_eq!(key, "aorta/default/prm/node/param");
}

#[test]
fn group_rejects_multi_segment() {
    assert!(KeyExprBuilder::new("fleet/alpha").is_err());
}

#[test]
fn group_rejects_exceeds_max_len() {
    // Group cap stays at 32 UTF-8 bytes.
    let group = "a".repeat(33);
    assert!(KeyExprBuilder::new(group).is_err());
}

#[test]
fn group_accepts_max_len_boundary() {
    let group = "a".repeat(32);
    let builder = KeyExprBuilder::new(group).expect("builder");
    assert_eq!(builder.group().len(), 32);
}

// =====================================================================
// Environment integration
// =====================================================================

#[test]
fn key_builder_from_group_env_uses_default_when_env_missing() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let builder = key_builder_from_group_env();
    assert_eq!(builder.group(), "default");
}

#[test]
fn key_builder_rejects_invalid_group_segment() {
    // `:` is not in the allowed char set; reliable rejection regardless of
    // relaxation rules. Validated by constructing the builder directly —
    // writing AORTA_GROUP=invalid:group into the shared process env while
    // TestEnvGuard's AORTA_TEST_ALLOW_ENV=1 is live would let parallel
    // non-locked tests read the polluted group and fail key validation
    // (the aorta-291 cascade flake). KeyExprBuilder::new validates the
    // same segment rules as the env→builder path.
    //
    // Coverage intentionally dropped: the contract that
    // `key_builder_from_group_env()` PANICS when the env holds an invalid
    // group is now untested — covering it requires writing the process
    // env, which is exactly the pollution source this test was rewritten
    // to avoid (a subprocess-based test could cover it; not done here).
    assert!(
        KeyExprBuilder::new("invalid:group").is_err(),
        "expected rejection of invalid group"
    );
}

// =====================================================================
// Schema / action surface tests (unchanged behavior — kept for coverage)
// =====================================================================

#[test]
fn schema_key_formats_hash() {
    let mut raw = [0u8; 32];
    raw[31] = 0x2b;
    raw[30] = 0x1a;
    let hash = crate::schema::SchemaHash::from_bytes(raw);
    let key: String = KeyExprBuilder::schema_key(&hash)
        .expect("schema key")
        .into();
    assert_eq!(
        key,
        "aorta/sys/schema/0000000000000000000000000000000000000000000000000000000000001a2b"
    );
}

#[test]
fn schema_by_key_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .schema_by_key("/pub/camera/image/")
        .expect("schema_by_key")
        .into();
    assert_eq!(key, "aorta/default/sys/schema_by_key/pub/camera/image");
}

#[test]
fn action_send_goal_key_builds_full_key() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .action_send_goal_key("/demo/execute_task/")
        .expect("action key")
        .into();
    assert_eq!(key, "aorta/default/act/send_goal/demo/execute_task");
}

#[test]
fn action_status_key_uses_goal_key_encoding() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .action_status_key("demo/execute_task", "550e8400-e29b-41d4-a716-446655440000")
        .expect("status key")
        .into();
    assert_eq!(
        key,
        "aorta/default/act/status/demo/execute_task/550e8400e29b41d4a716446655440000"
    );
}

#[test]
fn action_status_prefix_builds_full_key_prefix() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let prefix = builder
        .action_status_prefix("nav/follow_path")
        .expect("status prefix");
    assert_eq!(prefix, "aorta/default/act/status/nav/follow_path");
}

// `normalize_action_path` was removed: action
// names no longer derive FlatBuffers root types via path projection, so
// they follow the same relaxed rules as topic/service. The runtime now
// resolves root types from the compiled bfbs — see
// `core/test/action_tests.rs::resolve_action_root_types_from_bfbs_*`.

// =====================================================================
// Application-level namespace (Cloud Cross-Namespace Addressing Proposal §4)
// =====================================================================

#[test]
fn with_namespace_prefixes_concrete_namespace() {
    let builder = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("R1")
        .expect("namespace");
    assert_eq!(builder.namespace(), Some("R1"));

    // observe side
    let sub: String = builder.topic_key_sub("demo/state").expect("sub").into();
    assert_eq!(sub, "R1/aorta/default/pub/demo/state");

    // request/reply side (concrete namespace is fine)
    let srv: String = builder
        .service_key_client("system/power_cycle")
        .expect("srv")
        .into();
    assert_eq!(srv, "R1/aorta/default/srv/system/power_cycle");
}

#[test]
fn with_namespace_accepts_multi_segment() {
    let builder = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("region/site/R1")
        .expect("namespace");
    let sub: String = builder.topic_key_sub("demo/state").expect("sub").into();
    assert_eq!(sub, "region/site/R1/aorta/default/pub/demo/state");
}

#[test]
fn with_namespace_rejects_aorta_segment() {
    // `aorta` is the reverse-parse anchor; a namespace segment equal
    // to it would make `split_full_key` ambiguous.
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.clone().with_namespace("aorta").is_err());
    assert!(builder.with_namespace("region/aorta/R1").is_err());
}

#[test]
fn with_namespace_rejects_invalid_chars() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.clone().with_namespace("R1#bad").is_err());
    assert!(builder.with_namespace("").is_err());
}

#[test]
fn wildcard_namespace_allowed_on_observe_side() {
    let builder = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("*")
        .expect("namespace");

    let topic: String = builder.topic_key_sub("demo/state").expect("topic").into();
    assert_eq!(topic, "*/aorta/default/pub/demo/state");

    let fb: String = builder
        .action_feedback_key_sub("demo/execute_task")
        .expect("feedback")
        .into();
    assert_eq!(fb, "*/aorta/default/act/feedback/demo/execute_task");

    let status = builder
        .action_status_prefix("demo/execute_task")
        .expect("status prefix");
    assert_eq!(status, "*/aorta/default/act/status/demo/execute_task");
}

#[test]
fn wildcard_namespace_rejected_on_request_reply_side() {
    // service / action goal·cancel·get_result target exactly one robot:
    // `QueryTarget::All` + first-reply-wins makes a wildcard a fleet-wide
    // fan-out, which is unsafe for non-idempotent RPC.
    let builder = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("*")
        .expect("namespace");

    assert!(builder.service_key_client("system/power_cycle").is_err());
    assert!(builder.action_send_goal_key("demo/execute_task").is_err());
    assert!(builder.action_cancel_goal_key("demo/execute_task").is_err());
    assert!(builder
        .action_get_result_key_query("demo/execute_task")
        .is_err());

    // `**` is likewise rejected on the request/reply side.
    let b2 = KeyExprBuilder::new("default")
        .expect("builder")
        .with_namespace("**")
        .expect("namespace");
    assert!(b2.service_key_client("nav/plan").is_err());
}

#[test]
fn namespace_none_is_byte_identical() {
    // Regression guard: without a namespace, every surface produces exactly
    // the pre-feature key.
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(builder.namespace(), None);
    let sub: String = builder.topic_key_sub("demo/state").expect("sub").into();
    assert_eq!(sub, "aorta/default/pub/demo/state");
    let srv: String = builder
        .service_key_client("system/power_cycle")
        .expect("srv")
        .into();
    assert_eq!(srv, "aorta/default/srv/system/power_cycle");
}

// =====================================================================
// split_full_key — namespace-aware reverse parse
// =====================================================================

#[test]
fn split_full_key_extracts_namespace_and_suffix() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.split_full_key("R1/aorta/default/pub/demo/state"),
        Some((Some("R1".to_string()), "pub/demo/state".to_string()))
    );
}

#[test]
fn split_full_key_handles_multi_segment_namespace() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.split_full_key("region/site/R1/aorta/default/pub/demo/state"),
        Some((
            Some("region/site/R1".to_string()),
            "pub/demo/state".to_string()
        ))
    );
}

#[test]
fn split_full_key_no_namespace_returns_none_namespace() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.split_full_key("aorta/default/pub/demo/state"),
        Some((None, "pub/demo/state".to_string()))
    );
}

#[test]
fn split_full_key_rejects_foreign_group() {
    let builder = KeyExprBuilder::new("alpha").expect("builder");
    assert!(builder
        .split_full_key("R1/aorta/beta/pub/demo/state")
        .is_none());
    assert!(builder.split_full_key("aorta/beta/pub/x").is_none());
}

#[test]
fn key_suffix_from_full_key_still_works_with_namespace() {
    // Back-compat shim delegates to split_full_key.
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.key_suffix_from_full_key("R1/aorta/default/pub/camera/image"),
        Some("pub/camera/image".to_string())
    );
}
