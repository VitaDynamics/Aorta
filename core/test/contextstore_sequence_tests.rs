use crate::contextstore::ContextStore;
use crate::error::AortaResult;
use crate::node::Node;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_increments_per_set_on_same_key() -> AortaResult<()> {
    let node = Node::new("ctxB_seq_per_key", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_seq_pk/**").await?;

    store.set_u64("system_seq_pk/counter", 0).await?;
    let r1 = store.get_u64("system_seq_pk/counter").await?;
    assert_eq!(r1.sequence, 1);

    store.set_u64("system_seq_pk/counter", 1).await?;
    let r2 = store.get_u64("system_seq_pk/counter").await?;
    assert_eq!(r2.sequence, 2);

    store.set_u64("system_seq_pk/counter", 2).await?;
    let r3 = store.get_u64("system_seq_pk/counter").await?;
    assert_eq!(r3.sequence, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_independent_across_keys() -> AortaResult<()> {
    let node = Node::new("ctxB_seq_indep", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_seq_ind/**").await?;

    store.set_u64("system_seq_ind/a", 0).await?;
    store.set_u64("system_seq_ind/a", 0).await?;
    store.set_u64("system_seq_ind/a", 0).await?;
    store.set_u64("system_seq_ind/b", 0).await?;

    let ra = store.get_u64("system_seq_ind/a").await?;
    let rb = store.get_u64("system_seq_ind/b").await?;
    assert_eq!(ra.sequence, 3);
    assert_eq!(rb.sequence, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_resets_on_context_store_recreation() -> AortaResult<()> {
    // Sequence is per (writer process, key) and resets when
    // the writer restarts. Recreating ContextStore on the same Node models
    // a writer restart. We use shutdown to fully tear down store1's
    // Queryable so store2's get reliably reaches store2's owner Queryable
    // — without explicit shutdown the old store's Zenoh undeclare is a
    // background task and the get can race against a stale reply.
    let node = Node::new("ctxB_seq_reset", "default").await?;
    let store1 = ContextStore::new(&node);
    store1.declare_owned_prefix("system_seq_reset/**").await?;
    store1.set_u64("system_seq_reset/key", 0).await?;
    store1.set_u64("system_seq_reset/key", 0).await?;
    let r_old = store1.get_u64("system_seq_reset/key").await?;
    assert_eq!(r_old.sequence, 2);
    let old_instance = store1.source_instance_id().to_string();
    store1.shutdown().await?;
    drop(store1);

    let store2 = ContextStore::new(&node);
    assert_ne!(
        store2.source_instance_id(),
        old_instance.as_str(),
        "fresh ContextStore must mint a new instance id"
    );
    store2.declare_owned_prefix("system_seq_reset/**").await?;
    store2.set_u64("system_seq_reset/key", 0).await?;
    let r_new = store2.get_u64("system_seq_reset/key").await?;
    assert_eq!(r_new.sequence, 1);
    assert_eq!(r_new.source_instance_id, store2.source_instance_id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_context_stores_have_distinct_instance_ids() -> AortaResult<()> {
    let node = Node::new("ctxB_seq_distinct", "default").await?;
    let store1 = ContextStore::new(&node);
    let store2 = ContextStore::new(&node);
    assert_ne!(store1.source_instance_id(), store2.source_instance_id());
    assert_eq!(store1.source_instance_id().len(), 16);
    assert_eq!(store2.source_instance_id().len(), 16);
    // Both share the same node name.
    assert_eq!(store1.source_node(), store2.source_node());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_starts_at_one_for_first_set() -> AortaResult<()> {
    // Per spec: sequence is per-(writer, key). First set on a key should
    // produce sequence = 1, NOT 0 (so that 0 cleanly means "uninitialized"
    // for any consumer that wants to use it as a sentinel).
    let node = Node::new("ctxB_seq_one", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_seq_one/**").await?;
    store.set_bool("system_seq_one/flag", true).await?;
    let r = store.get_bool("system_seq_one/flag").await?;
    assert_eq!(r.sequence, 1);
    Ok(())
}
