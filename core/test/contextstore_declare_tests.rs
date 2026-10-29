use std::time::Duration;

use crate::contextstore::ContextStore;
use crate::error::{AortaError, AortaResult};
use crate::node::Node;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_single_prefix_succeeds() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_ok", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_a/**").await?;
    assert_eq!(store.declared_prefixes(), vec!["rcp_a/**".to_string()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_same_prefix_twice_is_idempotent() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_idem", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_b/**").await?;
    store.declare_owned_prefix("rcp_b/**").await?;
    assert_eq!(store.declared_prefixes().len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_multiple_prefixes_accumulate() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_multi", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_c/**").await?;
    store.declare_owned_prefix("dag_c/**").await?;
    let prefixes = store.declared_prefixes();
    assert_eq!(prefixes.len(), 2);
    assert!(prefixes.contains(&"rcp_c/**".to_string()));
    assert!(prefixes.contains(&"dag_c/**".to_string()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_without_declare_returns_prefix_not_declared() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_undecl", "default").await?;
    let store = ContextStore::new(&node);
    let result = store.set_u64("rcp_d/state", 0).await;
    match result {
        Err(AortaError::PrefixNotDeclared(msg)) => {
            assert!(
                msg.contains("rcp_d/state"),
                "error message should reference key, got: {}",
                msg
            );
        }
        other => panic!("expected PrefixNotDeclared, got {:?}", other),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_under_unrelated_declared_prefix_returns_prefix_not_declared() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_unrel", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_e/**").await?;
    let result = store.set_u64("dag_e/state", 0).await;
    assert!(matches!(result, Err(AortaError::PrefixNotDeclared(_))));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_resolves_only_after_subscriber_attached() -> AortaResult<()> {
    // After declare returns, the typed-API set must succeed immediately —
    // bootstrap contract: declare resolves only after Queryable AND
    // Subscriber are fully attached.
    let node = Node::new("ctxB_decl_bootstrap", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_f/**").await?;
    // Immediate set must succeed; no race window.
    store.set_u64("rcp_f/state", 1).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_tears_down_queryable() -> AortaResult<()> {
    // After explicit shutdown, the declared Queryable must no longer
    // reply. shutdown awaits Zenoh's undeclare so the hand-off is
    // deterministic — unlike Drop, which only schedules undeclare in
    // the background.
    let node = Node::new("ctxB_decl_drop", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_g/**").await?;
    store.set_u64("rcp_g/state", 9).await?;
    let r = store.get_u64("rcp_g/state").await?;
    assert_eq!(r.value, 9);

    store.shutdown().await?;
    drop(store);

    let probe = ContextStore::new(&node);
    let result = probe
        .get_u64_with_deadline("rcp_g/state", Duration::from_millis(500))
        .await;
    assert!(
        matches!(result, Err(AortaError::NotFound(_))),
        "expected NotFound after shutdown, got {:?}",
        result
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_clears_declarations() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_shutdown", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_sd/**").await?;
    store.declare_owned_prefix("dag_sd/**").await?;
    assert_eq!(store.declared_prefixes().len(), 2);
    store.shutdown().await?;
    assert!(store.declared_prefixes().is_empty());
    // After shutdown the typed-API set must reject the once-covered key.
    let result = store.set_u64("rcp_sd/state", 0).await;
    assert!(matches!(result, Err(AortaError::PrefixNotDeclared(_))));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_rejects_prefixes_that_overlap_existing_declarations() -> AortaResult<()> {
    // Two declarations on the SAME store where one includes the other
    // would make every cached key under the narrower prefix replied
    // by both Queryables — duplicating broad gets / snapshots. Reject
    // at declare time so this can never happen.
    let node = Node::new("ctxB_decl_overlap", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_overlap_a/**").await?;
    let nested = store.declare_owned_prefix("rcp_overlap_a/inner/**").await;
    assert!(
        matches!(nested, Err(AortaError::InvalidArgument(_))),
        "nested-under-existing should be rejected, got {:?}",
        nested
    );
    let sibling = store.declare_owned_prefix("rcp_overlap_b/**").await;
    assert!(sibling.is_ok(), "disjoint sibling is fine");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_prefixes_do_not_emit_duplicate_queryable_replies() -> AortaResult<()> {
    use std::time::Duration;
    use zenoh::query::{ConsolidationMode, QueryTarget};

    // Each declared prefix installs its own Queryable. Without per-prefix
    // scoping in handle_query, every Queryable callback would walk the
    // full cache and reply for keys belonging to OTHER prefixes too,
    // producing duplicates on broad gets. With scoping, each cached key
    // is replied at most once.
    let node = Node::new("ctxB_decl_dup", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_dup_a/**").await?;
    store.declare_owned_prefix("rcp_dup_b/**").await?;
    store.set_u64("rcp_dup_a/x", 1).await?;
    store.set_u64("rcp_dup_b/y", 2).await?;

    let replies = node
        .session()
        .inner()
        .get("aorta/default/ctx/**")
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(Duration::from_millis(500))
        .await
        .expect("zenoh get");

    let mut keys: Vec<String> = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let k = sample.key_expr().as_str().to_string();
            // Filter to just the keys this test owns; other tests may
            // have leaked Queryables on the shared local router.
            if k.starts_with("aorta/default/ctx/rcp_dup_") {
                keys.push(k);
            }
        }
    }

    keys.sort();
    assert_eq!(
        keys,
        vec![
            "aorta/default/ctx/rcp_dup_a/x".to_string(),
            "aorta/default/ctx/rcp_dup_b/y".to_string(),
        ],
        "wildcard get should see each owned key exactly once, got: {:?}",
        keys
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn is_key_declared_reflects_declarations() -> AortaResult<()> {
    let node = Node::new("ctxB_decl_check", "default").await?;
    let store = ContextStore::new(&node);
    assert!(!store.is_key_declared("rcp_h/state")?);
    store.declare_owned_prefix("rcp_h/**").await?;
    assert!(store.is_key_declared("rcp_h/state")?);
    assert!(store.is_key_declared("rcp_h/sub/key")?);
    assert!(!store.is_key_declared("dag_h/state")?);
    Ok(())
}
