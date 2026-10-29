//! Unit tests for the pure parts of `core::capture` — selector → keyexpr
//! resolution across namespaces, and `RawKeyExpr` group-root validation. The
//! live subscriber + ctx snapshot paths are covered by integration tests.

use super::CaptureSelector;
use crate::key_expr::KeyExprBuilder;

fn kb() -> KeyExprBuilder {
    KeyExprBuilder::new("default").expect("builder")
}

#[test]
fn selector_resolves_each_namespace() {
    let kb = kb();
    assert_eq!(
        CaptureSelector::Topic("camera/**".into())
            .resolve(&kb)
            .unwrap(),
        "aorta/default/pub/camera/**"
    );
    assert_eq!(
        CaptureSelector::Context("**".into()).resolve(&kb).unwrap(),
        "aorta/default/ctx/**"
    );
    assert_eq!(
        CaptureSelector::Telemetry("ctx/owner_conflict".into())
            .resolve(&kb)
            .unwrap(),
        "aorta/default/sys/telemetry/ctx/owner_conflict"
    );
    assert_eq!(
        CaptureSelector::RpcRecord("**".into())
            .resolve(&kb)
            .unwrap(),
        "aorta/default/sys/record/srv/**"
    );
}

#[test]
fn raw_keyexpr_must_be_rooted_at_group() {
    let kb = kb();
    // Inside the group: accepted verbatim.
    let ok = CaptureSelector::RawKeyExpr("aorta/default/sys/custom/**".into())
        .resolve(&kb)
        .unwrap();
    assert_eq!(ok, "aorta/default/sys/custom/**");

    // Wrong group / outside root: rejected.
    assert!(CaptureSelector::RawKeyExpr("aorta/other/pub/**".into())
        .resolve(&kb)
        .is_err());
    assert!(CaptureSelector::RawKeyExpr("not/aorta/**".into())
        .resolve(&kb)
        .is_err());
}

#[test]
fn topic_wildcards_are_preserved() {
    let kb = kb();
    assert_eq!(
        CaptureSelector::Topic("**".into()).resolve(&kb).unwrap(),
        "aorta/default/pub/**"
    );
}

/// End-to-end: a wildcard topic capture receives a live publish with the
/// concrete key, key suffix, and raw payload — and (default attach policy)
/// no schema hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn capture_subscriber_receives_live_topic_publish() {
    use super::{CaptureEvent, CaptureKind};
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("cap_pub", "cap_group").await.unwrap();
    let sub_node = Node::new("cap_sub", "cap_group").await.unwrap();

    #[derive(Clone)]
    struct Got {
        key: String,
        suffix: String,
        payload: Vec<u8>,
        had_schema_hash: bool,
        recv_nonzero: bool,
        kind_live: bool,
    }
    let got: Arc<Mutex<Option<Got>>> = Arc::new(Mutex::new(None));
    let got_cb = Arc::clone(&got);

    // Specific topic selector so we capture only our publish, not the node's
    // own background `/log` traffic that a broad `**` would also match.
    let _sub = sub_node
        .create_capture_subscriber(
            &[CaptureSelector::Topic("cap/chatter".into())],
            move |event| {
                if let CaptureEvent::Sample { kind, sample } = event {
                    *got_cb.lock().unwrap() = Some(Got {
                        key: sample.key.to_string(),
                        suffix: sample.key_suffix.to_string(),
                        payload: sample.payload.to_vec(),
                        had_schema_hash: sample.schema_hash.is_some(),
                        recv_nonzero: sample.recv_time_ns > 0,
                        kind_live: *kind == CaptureKind::Live,
                    });
                }
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher("cap/chatter").await.unwrap();
    pub_handle.publish(b"hello capture".to_vec()).unwrap();

    let waited = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if got.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "capture should receive the live publish");

    let got = got.lock().unwrap().clone().expect("captured sample");
    // Group-relative assertions (the concrete group prefix is environment-
    // dependent; the suffix is the stable, group-relative identity).
    assert_eq!(got.suffix, "pub/cap/chatter");
    assert!(
        got.key.ends_with("/pub/cap/chatter"),
        "concrete key should end with the topic path, got {}",
        got.key
    );
    assert!(got.key.starts_with("aorta/"), "got {}", got.key);
    assert_eq!(got.payload, b"hello capture");
    assert!(got.kind_live, "live publish should be CaptureKind::Live");
    assert!(got.recv_nonzero, "recv_time_ns should be stamped");
    assert!(
        !got.had_schema_hash,
        "default attach policy is never → no schema hash on the wire"
    );
}

/// End-to-end: `capture_context_snapshot` replays an active owner's current ctx
/// values as `Snapshot` samples, then emits exactly one `SnapshotDone`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_context_snapshot_replays_owner_state_then_done() {
    use super::{CaptureEvent, CaptureKind, SnapshotDoneReason};
    use crate::contextstore::ContextStore;
    use crate::node::Node;
    use crate::test_support::ENV_LOCK;
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");

    let owner_node = Node::new("capsnap_owner", "capsnap_grp").await.unwrap();
    let reader_node = Node::new("capsnap_reader", "capsnap_grp").await.unwrap();

    let owner = ContextStore::new(&owner_node);
    owner.declare_owned_prefix("capsnap/**").await.unwrap();
    owner.set_u64("capsnap/k1", 1).await.unwrap();
    owner.set_u64("capsnap/k2", 2).await.unwrap();
    owner.set_u64("capsnap/k3", 3).await.unwrap();

    // Let routing settle so the reader's GET sees the owner's queryables.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    #[derive(Default)]
    struct Collected {
        snapshot_keys: std::collections::BTreeSet<String>,
        snapshot_payload_nonempty: bool,
        saw_live: bool,
        done_reason: Option<SnapshotDoneReason>,
        done_count: usize,
    }
    let collected: Arc<Mutex<Collected>> = Arc::new(Mutex::new(Collected::default()));
    let cb = Arc::clone(&collected);

    reader_node
        .capture_context_snapshot(
            "capsnap/**",
            tokio::time::Duration::from_secs(2),
            move |event| {
                let mut c = cb.lock().unwrap();
                match event {
                    CaptureEvent::Sample { kind, sample } => match kind {
                        CaptureKind::Snapshot => {
                            c.snapshot_keys.insert(sample.key.to_string());
                            if !sample.payload.is_empty() {
                                c.snapshot_payload_nonempty = true;
                            }
                        }
                        CaptureKind::Live => c.saw_live = true,
                    },
                    CaptureEvent::SnapshotDone { reason } => {
                        c.done_reason = Some(*reason);
                        c.done_count += 1;
                    }
                }
            },
        )
        .await
        .unwrap();

    // The async fn completes only after delivering SnapshotDone, so everything
    // is collected by now — no wait loop needed.
    let c = collected.lock().unwrap();
    assert_eq!(c.done_count, 1, "exactly one SnapshotDone");
    assert_eq!(
        c.done_reason,
        Some(SnapshotDoneReason::QueryDrained),
        "active owner should drain cleanly"
    );
    assert!(!c.saw_live, "snapshot must not deliver Live samples");
    assert!(
        c.snapshot_payload_nonempty,
        "ctx values carry payload bytes"
    );
    assert_eq!(
        c.snapshot_keys.len(),
        3,
        "owner set 3 ctx keys, got {:?}",
        c.snapshot_keys
    );

    owner.shutdown().await.unwrap();
}
