use std::time::Duration;

use crate::contextstore::ContextStore;
use crate::contextstore_fb::ContextKind;
use crate::error::{AortaError, AortaResult};
use crate::node::Node;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_bool() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_bool", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_bool/**").await?;
    store.set_bool("system_sg_bool/flag", true).await?;
    let r = store.get_bool("system_sg_bool/flag").await?;
    assert!(r.value);
    assert!(r.is_fresh);
    assert_eq!(r.expires_at_ns, 0);
    assert_eq!(r.source_node, "ctxB_sg_bool");
    assert_eq!(r.source_instance_id, store.source_instance_id());
    assert_eq!(r.sequence, 1);
    assert!(r.stamp_ns > 0);

    store.set_bool("system_sg_bool/flag", false).await?;
    let r2 = store.get_bool("system_sg_bool/flag").await?;
    assert!(!r2.value);
    assert!(r2.stamp_ns >= r.stamp_ns);
    assert_eq!(r2.sequence, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_i64() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_i64", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_i64/**").await?;
    store.set_i64("system_sg_i64/value", -42).await?;
    let r = store.get_i64("system_sg_i64/value").await?;
    assert_eq!(r.value, -42);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_u64() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_u64", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_u64/**").await?;
    store.set_u64("system_sg_u64/state", 7).await?;
    let r = store.get_u64("system_sg_u64/state").await?;
    assert_eq!(r.value, 7);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_f64() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_f64", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_f64/**").await?;
    store.set_f64("system_sg_f64/temp", 36.5).await?;
    let r = store.get_f64("system_sg_f64/temp").await?;
    assert!((r.value - 36.5).abs() < 1e-9);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_string() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_str", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_sg_str/**").await?;
    store
        .set_string("rcp_sg_str/request_id", "task-abc")
        .await?;
    let r = store.get_string("rcp_sg_str/request_id").await?;
    assert_eq!(r.value, "task-abc");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_empty_string() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_str_empty", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_sg_str_empty/**").await?;
    store.set_string("rcp_sg_str_empty/request_id", "").await?;
    let r = store.get_string("rcp_sg_str_empty/request_id").await?;
    assert_eq!(r.value, "");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kind_mismatch_on_get() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_mismatch", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_mismatch/**").await?;
    store.set_u64("system_sg_mismatch/key", 5).await?;
    let result = store.get_string("system_sg_mismatch/key").await;
    match result {
        Err(AortaError::KindMismatch(msg)) => {
            assert!(msg.contains("expected String"));
            assert!(msg.contains("got U64"));
        }
        other => panic!("expected KindMismatch, got {:?}", other),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_key_returns_not_found_after_deadline() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_missing", "default").await?;
    let store = ContextStore::new(&node);
    // No declare; nobody owns 'phantom_sg/**'.
    let result = store
        .get_u64_with_deadline("phantom_sg/key", Duration::from_millis(300))
        .await;
    assert!(matches!(result, Err(AortaError::NotFound(_))));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expires_at_ns_marks_value_stale() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_expiry", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_expiry/**").await?;
    // Expiry already in the past.
    store
        .set_u64_with_expiry("system_sg_expiry/key", 9, 1)
        .await?;
    let r = store.get_u64("system_sg_expiry/key").await?;
    assert_eq!(r.value, 9);
    assert!(!r.is_fresh, "value past expires_at_ns should be stale");
    assert_eq!(r.expires_at_ns, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expires_at_ns_zero_means_no_expiry() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_no_expiry", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_no_expiry/**").await?;
    store.set_u64("system_sg_no_expiry/key", 1).await?;
    let r = store.get_u64("system_sg_no_expiry/key").await?;
    assert!(r.is_fresh);
    assert_eq!(r.expires_at_ns, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expires_at_ns_far_future_keeps_value_fresh() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_far", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_far/**").await?;
    let far = u64::MAX / 2;
    store
        .set_u64_with_expiry("system_sg_far/key", 7, far)
        .await?;
    let r = store.get_u64("system_sg_far/key").await?;
    assert!(r.is_fresh);
    assert_eq!(r.expires_at_ns, far);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_carries_source_identity() -> AortaResult<()> {
    let node = Node::new("ctxB_sg_meta", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_sg_meta/**").await?;
    store.set_bool("system_sg_meta/flag", true).await?;
    let r = store.get_bool("system_sg_meta/flag").await?;
    assert_eq!(r.source_node, "ctxB_sg_meta");
    assert_eq!(r.source_instance_id.len(), 16);
    assert_eq!(r.source_instance_id, store.source_instance_id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_prefix_with_explicit_key_works() -> AortaResult<()> {
    // Declaring an exact key as the prefix should also allow set on
    // exactly that key.
    let node = Node::new("ctxB_sg_exact", "default").await?;
    let store = ContextStore::new(&node);
    store
        .declare_owned_prefix("system_sg_exact/single_key")
        .await?;
    store.set_bool("system_sg_exact/single_key", true).await?;
    let result = store.set_bool("system_sg_exact/other_key", true).await;
    assert!(matches!(result, Err(AortaError::PrefixNotDeclared(_))));
    Ok(())
}

#[test]
fn context_kind_name_is_stable() {
    // Just guards the Display strings used in KindMismatch error
    // messages so changes here force test review.
    assert_eq!(ContextKind::Bool.name(), "Bool");
    assert_eq!(ContextKind::I64.name(), "I64");
    assert_eq!(ContextKind::U64.name(), "U64");
    assert_eq!(ContextKind::F64.name(), "F64");
    assert_eq!(ContextKind::String.name(), "String");
    assert_eq!(ContextKind::None.name(), "None");
}
