use crate::key_expr::KeyExprBuilder;

#[test]
fn context_key_builds_full_key() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .context_key("system/emergency_stop_active")
        .expect("ctx key")
        .into();
    assert_eq!(key, "aorta/default/ctx/system/emergency_stop_active");
}

#[test]
fn context_key_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.context_key("/demo/state/").expect("ctx key").into();
    assert_eq!(key, "aorta/default/ctx/demo/state");
}

#[test]
fn context_key_rejects_empty_middle_segment() {
    // Post-relaxation: `//` is a hard error rather than silently collapsed.
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.context_key("demo//state").is_err());
}

#[test]
fn context_key_accepts_uppercase_verbatim() {
    // Post-relaxation: case is preserved byte-for-byte (no fold, no
    // reject). `demo/State` and `demo/state` are distinct context keys —
    // org-level conventions are now a style concern, not a middleware
    // policy.
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.context_key("demo/State").expect("ctx key").into();
    assert_eq!(key, "aorta/default/ctx/demo/State");
}

#[test]
fn context_key_rejects_empty() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.context_key("").is_err());
    assert!(builder.context_key("   ").is_err());
    assert!(builder.context_key("//").is_err());
}

#[test]
fn context_key_rejects_dot_segments() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.context_key("demo/./state").is_err());
    assert!(builder.context_key("demo/../state").is_err());
}

#[test]
fn context_prefix_allows_double_star() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .context_prefix("demo/**")
        .expect("ctx prefix")
        .into();
    assert_eq!(key, "aorta/default/ctx/demo/**");
}

#[test]
fn context_prefix_allows_single_star() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .context_prefix("function/*/status")
        .expect("ctx prefix")
        .into();
    assert_eq!(key, "aorta/default/ctx/function/*/status");
}

#[test]
fn context_prefix_allows_global_double_star() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.context_prefix("**").expect("ctx prefix").into();
    assert_eq!(key, "aorta/default/ctx/**");
}

#[test]
fn context_prefix_accepts_uppercase_outside_wildcard() {
    // Same relaxation as context_key: case is preserved literally.
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.context_prefix("Rcp/**").expect("ctx prefix").into();
    assert_eq!(key, "aorta/default/ctx/Rcp/**");
}

#[test]
fn context_prefix_rejects_empty() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.context_prefix("").is_err());
    assert!(builder.context_prefix("   ").is_err());
}

#[test]
fn context_key_includes_match_for_context_prefix() {
    // `context_prefix("demo/**")` should match `context_key("demo/state")` per
    // Zenoh keyexpr semantics.
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder.context_key("demo/state").expect("ctx key");
    let prefix = builder.context_prefix("demo/**").expect("ctx prefix");
    assert!(
        prefix.includes(&key),
        "prefix {} should include {}",
        prefix,
        key
    );
}

#[test]
fn key_suffix_for_context_returns_relative_path() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_for_context("/demo/state")
        .expect("suffix");
    assert_eq!(suffix, "ctx/demo/state");
}

#[test]
fn key_suffix_for_context_rejects_empty_middle_segment() {
    // Mirror of context_key_rejects_empty_middle_segment.
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder.key_suffix_for_context("/demo//state").is_err());
}

#[test]
fn ctx_owner_conflict_telemetry_key_uses_fixed_path() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder
        .ctx_owner_conflict_telemetry_key()
        .expect("telemetry key");
    assert_eq!(key, "aorta/default/sys/telemetry/vital/ctx/owner_conflict");
}

#[test]
fn ctx_owner_conflict_telemetry_key_uses_group() {
    let builder = KeyExprBuilder::new("alpha").expect("builder");
    let key = builder
        .ctx_owner_conflict_telemetry_key()
        .expect("telemetry key");
    assert_eq!(key, "aorta/alpha/sys/telemetry/vital/ctx/owner_conflict");
}

#[test]
fn telemetry_vital_snapshot_key_is_tiered() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.telemetry_vital_snapshot_key("node-1").expect("key"),
        "aorta/default/sys/telemetry/vital/snapshot/node-1"
    );
}

#[test]
fn telemetry_bulk_runtime_key_is_tiered() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert_eq!(
        builder.telemetry_bulk_runtime_key("node-1").expect("key"),
        "aorta/default/sys/telemetry/bulk/runtime/node-1"
    );
}

#[test]
fn ctx_owner_liveliness_key_builds_full_key() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .ctx_owner_liveliness_key("estimator", "0123456789abcdef")
        .expect("token key")
        .into();
    assert_eq!(
        key,
        "aorta/default/sys/alive/ctx/estimator/0123456789abcdef"
    );
}

#[test]
fn ctx_owner_liveliness_key_uses_group() {
    let builder = KeyExprBuilder::new("alpha").expect("builder");
    let key: String = builder
        .ctx_owner_liveliness_key("estimator", "0123456789abcdef")
        .expect("token key")
        .into();
    assert_eq!(key, "aorta/alpha/sys/alive/ctx/estimator/0123456789abcdef");
}

#[test]
fn ctx_owner_liveliness_key_rejects_wildcards_and_empty_segments() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    assert!(builder
        .ctx_owner_liveliness_key("node/*", "0123456789abcdef")
        .is_err());
    assert!(builder
        .ctx_owner_liveliness_key("node", "0123/4567")
        .is_err());
    assert!(builder
        .ctx_owner_liveliness_key("", "0123456789abcdef")
        .is_err());
    assert!(builder.ctx_owner_liveliness_key("node", "").is_err());
}

#[test]
fn ctx_owner_liveliness_query_covers_all_tokens_in_group() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let query: String = builder.ctx_owner_liveliness_query().expect("query").into();
    assert_eq!(query, "aorta/default/sys/alive/ctx/**");
    let query_ke = builder.ctx_owner_liveliness_query().expect("query");
    let token = builder
        .ctx_owner_liveliness_key("estimator", "0123456789abcdef")
        .expect("token key");
    assert!(
        query_ke.includes(&token),
        "query {} should include token {}",
        query,
        token
    );
}
