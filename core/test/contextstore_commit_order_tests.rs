//! Commit-first cache ordering tests for ContextStore.
//!
//! The bug under test: `set_inner` allocated `sequence` outside any
//! per-key ordering, wrote the owner cache before the Zenoh put
//! completed, and ROLLED BACK the cache on put failure — while the
//! owner Queryable reads the cache uncoordinated. Consequences:
//!
//! 1. A GET racing a failing put can serve a value that is then
//!    retracted (phantom).
//! 2. Two concurrent sets on the same key where one fails can clobber
//!    the other's committed value (the rollback restores a stale
//!    'previous').
//! 3. Sequence commit order can invert under preemption.
//!
//! The fix: per key, {allocate sequence -> commit cache} under one
//! per-key ordering; the cache is monotonic and NEVER rolled back; the
//! wire put proceeds after commit. A failed put leaves the cache ahead
//! of watchers (the healable direction — `get`/snapshot/watch
//! re-creation converge on the committed value), never behind.
//!
//! Failure injection: a real `session.put` error is only reachable by
//! closing the session, which also kills the owner Queryable — so the
//! cache ordering cannot be observed through a real transport fault.
//! These tests use the `#[cfg(test)]` put gate (`TestPutGate`) which
//! models the identical failure class (transport error after the cache
//! commit) with deterministic in-flight windows.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::contextstore::{ContextStore, TestPutGate, TestPutRule, TestPutWindow};
use crate::contextstore_fb::root_as_context_value;
use crate::error::{AortaError, AortaResult};
use crate::node::Node;

/// Build a rule matching a put whose u64 value equals `expected`.
fn matches_u64(expected: u64) -> crate::contextstore::PutMatcher {
    Arc::new(move |_key, payload| {
        root_as_context_value(payload)
            .map(|v| v.u64_value() == expected)
            .unwrap_or(false)
    })
}

/// Build a rule matching a put whose u64 value is one of `expected`.
fn matches_u64_any(expected: &[u64]) -> crate::contextstore::PutMatcher {
    let expected = expected.to_vec();
    Arc::new(move |_key, payload| {
        root_as_context_value(payload)
            .map(|v| expected.contains(&v.u64_value()))
            .unwrap_or(false)
    })
}

/// Create a rendezvous window plus the test-side channels: the put
/// signals `entered` when it reaches the window and waits on `release`.
fn make_window() -> (TestPutWindow, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    (
        TestPutWindow {
            entered: entered_tx,
            release: release_rx,
        },
        entered_rx,
        release_tx,
    )
}

/// A factory that pops windows from a shared queue, so each matching
/// put parks in its own rendezvous.
fn window_factory(
    queue: &Arc<Mutex<VecDeque<TestPutWindow>>>,
) -> crate::contextstore::PutWindowFactory {
    let queue = Arc::clone(queue);
    Arc::new(move || {
        queue
            .lock()
            .unwrap()
            .pop_front()
            .expect("test put window queue exhausted")
    })
}

/// Wait for a oneshot with a bound so a broken interleaving fails the
/// test instead of hanging it.
async fn await_entered(rx: oneshot::Receiver<()>, what: &str) {
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {}", what))
        .expect("put window sender dropped without signalling");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_during_failed_put_serves_committed_value() -> AortaResult<()> {
    // Red scenario (a): a GET racing a failing put must keep serving the
    // committed value after the put fails — never a value that is then
    // retracted. With the rollback bug the cache snaps back to the
    // baseline (0) once the put fails, so the second get observes the
    // phantom retraction.
    let node = Node::new("ctxC_commit_phantom", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_ph/**").await?;
    store.set_u64("system_commit_ph/key", 0).await?;
    assert_eq!(store.get_u64("system_commit_ph/key").await?.value, 0);

    let (window, entered_rx, release_tx) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window])));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: matches_u64(1),
            fail: true,
            pre_commit: None,
            post_commit: Some(window_factory(&queue)),
        }],
    });

    // Set 1 commits the cache, then parks with the wire put in flight.
    let store_set = store.clone();
    let set_task = tokio::spawn(async move { store_set.set_u64("system_commit_ph/key", 1).await });
    await_entered(entered_rx, "put 1 to reach the post-commit window").await;

    // The value is committed and served while the put is outstanding.
    let during = store.get_u64("system_commit_ph/key").await?;
    assert_eq!(during.value, 1);
    assert_eq!(during.sequence, 2);

    // The put fails. The committed value must NOT be retracted.
    release_tx.send(()).unwrap();
    let set_result = set_task.await.expect("set task panicked");
    assert!(
        matches!(set_result, Err(AortaError::Transport(_))),
        "failed put must surface a transport error, got {:?}",
        set_result
    );

    let after = store.get_u64("system_commit_ph/key").await?;
    assert_eq!(
        after.value, 1,
        "failed put must not roll back the committed value (phantom retraction)"
    );
    assert_eq!(
        after.sequence, 2,
        "sequence must stay with the committed value"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sets_failing_first_commit_keeps_successor() -> AortaResult<()> {
    // Completion order 1: the FAILING set commits first, then the
    // successful set commits on top. Both old and fixed code end with
    // the successor — this guards the invariant for this order.
    let node = Node::new("ctxC_commit_ab1", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_a1/**").await?;
    store.set_u64("system_commit_a1/key", 0).await?;

    let (window, entered_rx, release_tx) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window])));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: matches_u64(1),
            fail: true,
            pre_commit: None,
            post_commit: Some(window_factory(&queue)),
        }],
    });

    // A (value 1) commits and parks with the put in flight.
    let store_set_a = store.clone();
    let set_a = tokio::spawn(async move { store_set_a.set_u64("system_commit_a1/key", 1).await });
    await_entered(entered_rx, "set A to reach the post-commit window").await;

    // B (value 2) commits and completes while A is still in flight.
    let store_set_b = store.clone();
    let set_b = tokio::spawn(async move { store_set_b.set_u64("system_commit_a1/key", 2).await });
    set_b.await.expect("set B task panicked")?;

    let mid = store.get_u64("system_commit_a1/key").await?;
    assert_eq!(mid.value, 2);
    assert_eq!(mid.sequence, 3);

    // A's put fails; the successor's committed value must survive.
    release_tx.send(()).unwrap();
    let set_a_result = set_a.await.expect("set A task panicked");
    assert!(matches!(set_a_result, Err(AortaError::Transport(_))));

    let final_r = store.get_u64("system_commit_a1/key").await?;
    assert_eq!(
        final_r.value, 2,
        "failed first commit must not clobber the successor"
    );
    assert_eq!(final_r.sequence, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sets_failing_last_commit_keeps_last_committed() -> AortaResult<()> {
    // Red scenario (b): completion order 2 — the successful set commits
    // first, then the FAILING set commits last. The rollback bug
    // restores the failed set's stale 'previous' (the earlier
    // committed value 2), resurrecting it over the last committed
    // value (1). The fix keeps the last committed value.
    let node = Node::new("ctxC_commit_ab2", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_a2/**").await?;
    store.set_u64("system_commit_a2/key", 0).await?;

    // B (value 2) succeeds first, completely.
    store.set_u64("system_commit_a2/key", 2).await?;

    let (window, entered_rx, release_tx) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window])));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: matches_u64(1),
            fail: true,
            pre_commit: None,
            post_commit: Some(window_factory(&queue)),
        }],
    });

    // A (value 1) commits last (sequence 3) and parks in flight.
    let store_set_a = store.clone();
    let set_a = tokio::spawn(async move { store_set_a.set_u64("system_commit_a2/key", 1).await });
    await_entered(entered_rx, "set A to reach the post-commit window").await;

    let mid = store.get_u64("system_commit_a2/key").await?;
    assert_eq!(
        mid.value, 1,
        "committed value must be served while the put is in flight"
    );
    assert_eq!(mid.sequence, 3);

    // A's put fails. The last committed value (1, seq 3) must survive —
    // the earlier committed value (2, seq 2) must NOT be resurrected.
    release_tx.send(()).unwrap();
    let set_a_result = set_a.await.expect("set A task panicked");
    assert!(matches!(set_a_result, Err(AortaError::Transport(_))));

    let final_r = store.get_u64("system_commit_a2/key").await?;
    assert_eq!(
        final_r.value, 1,
        "failed last commit must not resurrect the earlier committed value"
    );
    assert_eq!(final_r.sequence, 3, "seq order must equal commit order");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_failure_release_a_first_keeps_last_committed() -> AortaResult<()> {
    // Red scenario: both concurrent sets fail. Release order A then B.
    // The rollback bug walks the cache back through each stale
    // 'previous' (0, then 1), resurrecting the older value; the fix
    // keeps the last committed value (2, seq 3).
    let node = Node::new("ctxC_commit_2fail1", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_f1/**").await?;
    store.set_u64("system_commit_f1/key", 0).await?;

    let (window_a, entered_a, release_a) = make_window();
    let (window_b, entered_b, release_b) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window_a, window_b])));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: matches_u64_any(&[1, 2]),
            fail: true,
            pre_commit: None,
            post_commit: Some(window_factory(&queue)),
        }],
    });

    let store_set_a = store.clone();
    let set_a = tokio::spawn(async move { store_set_a.set_u64("system_commit_f1/key", 1).await });
    await_entered(entered_a, "set A to reach the post-commit window").await;
    let store_set_b = store.clone();
    let set_b = tokio::spawn(async move { store_set_b.set_u64("system_commit_f1/key", 2).await });
    await_entered(entered_b, "set B to reach the post-commit window").await;

    let mid = store.get_u64("system_commit_f1/key").await?;
    assert_eq!(mid.value, 2);
    assert_eq!(mid.sequence, 3);

    release_a.send(()).unwrap();
    let set_a_result = set_a.await.expect("set A task panicked");
    assert!(matches!(set_a_result, Err(AortaError::Transport(_))));
    release_b.send(()).unwrap();
    let set_b_result = set_b.await.expect("set B task panicked");
    assert!(matches!(set_b_result, Err(AortaError::Transport(_))));

    let final_r = store.get_u64("system_commit_f1/key").await?;
    assert_eq!(
        final_r.value, 2,
        "double failure must never resurrect an older committed value"
    );
    assert_eq!(final_r.sequence, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_failure_release_b_first_keeps_last_committed() -> AortaResult<()> {
    // Red scenario: both concurrent sets fail, release order B then A.
    // The rollback bug ends with the BASELINE value (0) — the oldest
    // value of all; the fix keeps the last committed value (2, seq 3).
    let node = Node::new("ctxC_commit_2fail2", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_f2/**").await?;
    store.set_u64("system_commit_f2/key", 0).await?;

    let (window_a, entered_a, release_a) = make_window();
    let (window_b, entered_b, release_b) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window_a, window_b])));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: matches_u64_any(&[1, 2]),
            fail: true,
            pre_commit: None,
            post_commit: Some(window_factory(&queue)),
        }],
    });

    let store_set_a = store.clone();
    let set_a = tokio::spawn(async move { store_set_a.set_u64("system_commit_f2/key", 1).await });
    await_entered(entered_a, "set A to reach the post-commit window").await;
    let store_set_b = store.clone();
    let set_b = tokio::spawn(async move { store_set_b.set_u64("system_commit_f2/key", 2).await });
    await_entered(entered_b, "set B to reach the post-commit window").await;

    release_b.send(()).unwrap();
    let set_b_result = set_b.await.expect("set B task panicked");
    assert!(matches!(set_b_result, Err(AortaError::Transport(_))));
    release_a.send(()).unwrap();
    let set_a_result = set_a.await.expect("set A task panicked");
    assert!(matches!(set_a_result, Err(AortaError::Transport(_))));

    let final_r = store.get_u64("system_commit_f2/key").await?;
    assert_eq!(
        final_r.value, 2,
        "double failure must never resurrect an older committed value"
    );
    assert_eq!(final_r.sequence, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_commit_order_holds_under_preemption() -> AortaResult<()> {
    // Red scenario (c): A starts first but is preempted before
    // committing; B commits in between. Without a per-key ordering the
    // sequence order inverts vs the commit order: A allocated seq 2 at
    // its start, B allocates seq 3 and commits, then A commits LAST
    // with the LOWER sequence — the cache ends with seq 2 while seq 3
    // was already committed (and its put already went out). With the
    // fix {allocate sequence -> commit cache} is one per-key critical
    // section, so the last commit carries the max sequence: A commits
    // with seq 3.
    //
    // Both A and B park at the pre-commit point; the test releases B
    // first, then A, so A's commit is strictly the last commit in both
    // code versions (no scheduling race). The red discriminator is the
    // sequence: the last commit must carry the maximum allocated
    // sequence (3), which only the per-key ordering provides.
    let node = Node::new("ctxC_commit_seq", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_commit_sq/**").await?;
    store.set_u64("system_commit_sq/key", 0).await?;

    // Both puts on this key park at the pre-commit point (matched by
    // key; the payload is not encoded yet at that point). Neither put
    // fails — the gate only controls the interleaving.
    let (window_a, entered_a, release_a) = make_window();
    let (window_b, _entered_b, release_b) = make_window();
    let queue = Arc::new(Mutex::new(VecDeque::from([window_a, window_b])));
    let key_matches: crate::contextstore::PutMatcher =
        Arc::new(|key, _| key.ends_with("system_commit_sq/key"));
    store.test_arm_put_gate(TestPutGate {
        rules: vec![TestPutRule {
            matches: key_matches,
            fail: false,
            pre_commit: Some(window_factory(&queue)),
            post_commit: None,
        }],
    });

    // A starts first and parks before committing.
    let store_set_a = store.clone();
    let set_a = tokio::spawn(async move { store_set_a.set_u64("system_commit_sq/key", 1).await });
    await_entered(entered_a, "set A to reach the pre-commit window").await;

    // B starts while A is preempted and parks too.
    let store_set_b = store.clone();
    let set_b = tokio::spawn(async move { store_set_b.set_u64("system_commit_sq/key", 2).await });

    // B commits first and completes; then A commits last. With the fix
    // A's commit carries seq 3 (allocation happens inside the per-key
    // section, after B's); without it A carries the seq 2 it allocated
    // before the preemption — the sequence order inverts vs the commit
    // order.
    release_b.send(()).unwrap();
    set_b.await.expect("set B task panicked")?;
    release_a.send(()).unwrap();
    set_a.await.expect("set A task panicked")?;

    let final_r = store.get_u64("system_commit_sq/key").await?;
    assert_eq!(
        final_r.value, 1,
        "the last commit must be the final cache value"
    );
    assert_eq!(
        final_r.sequence, 3,
        "seq order must equal commit order: the last commit must carry the max sequence"
    );
    Ok(())
}
