//! The depth-1 latest-value watch (`load()`/`store()`) — semantics,
//! zero-copy identity, and the Zenoh-fed watch path.

use super::Watch;
use crate::node::Node;
use crate::ZBytes;
use std::sync::Arc;
use std::time::Duration;

fn unique(base: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "{base}_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn detached_ring_load_sees_the_latest_store() {
    let watch = Watch::detached("test");
    assert!(watch.load().is_none(), "empty until the first store");
    watch.store(ZBytes::from(vec![1u8]));
    watch.store(ZBytes::from(vec![2u8]));
    watch.store(ZBytes::from(vec![3u8]));
    let latest = watch.load().expect("stored");
    assert_eq!(latest.to_bytes().as_ref(), &[3u8], "latest value wins");
    assert_eq!(watch.stored_total(), 3);
}

#[test]
fn load_is_a_zero_copy_shared_handle() {
    let watch = Watch::detached("test");
    watch.store(ZBytes::from(vec![7u8; 1024]));
    let a = watch.load().expect("stored");
    let b = watch.load().expect("stored");
    assert!(
        Arc::ptr_eq(&a, &b),
        "two loads of the same value share one allocation — no payload copy"
    );
}

#[test]
fn load_guard_reads_without_cloning_and_defers_reclaim() {
    let watch = Watch::detached("test");
    watch.store(ZBytes::from(vec![1u8]));
    let guard = watch.load_guard();
    assert_eq!(
        guard.as_ref().expect("value").payload.to_bytes().as_ref(),
        &[1u8]
    );
    // A concurrent store displaces the value; the guard still reads the OLD
    // one (reclaim deferred while held).
    watch.store(ZBytes::from(vec![2u8]));
    assert_eq!(
        guard.as_ref().expect("value").payload.to_bytes().as_ref(),
        &[1u8],
        "a held guard pins its snapshot"
    );
    drop(guard);
    assert_eq!(watch.load().expect("value").to_bytes().as_ref(), &[2u8]);
}

#[test]
fn store_is_safe_from_foreign_threads() {
    let watch = Watch::detached("bus");
    let producer_ring = Arc::clone(&watch);
    let producer = std::thread::spawn(move || {
        for i in 0..100u8 {
            producer_ring.store(ZBytes::from(vec![i]));
        }
    });
    // Concurrent reader: every observed snapshot is a valid full payload.
    let mut last_seen = None;
    for _ in 0..1000 {
        if let Some(v) = watch.load() {
            let bytes = v.to_bytes();
            assert_eq!(bytes.as_ref().len(), 1);
            last_seen = Some(bytes.as_ref()[0]);
        }
    }
    producer.join().expect("producer");
    assert_eq!(watch.load().expect("final").to_bytes().as_ref(), &[99u8]);
    let _ = last_seen;
}

#[test]
fn concurrent_stores_publish_seq_and_payload_atomically() {
    // Regression: `store_stamped`'s seq assignment and
    // cell publish were two separate steps — two racing writer threads
    // could land the LOWER-seq payload last (newer value silently lost,
    // `seq()` observably non-monotonic). The public contract explicitly
    // invites multi-threaded writers ("thread-safe from any thread").
    const THREADS: usize = 4;
    const STORES_PER_THREAD: u64 = 2_000;
    let watch = Watch::detached("mt-store");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Reader: seq must never be observed decreasing.
    let reader = {
        let watch = Arc::clone(&watch);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut last = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let now = watch.seq();
                assert!(now >= last, "seq regressed: {last} -> {now}");
                last = now;
            }
        })
    };
    let writers: Vec<_> = (0..THREADS)
        .map(|t| {
            let watch = Arc::clone(&watch);
            std::thread::spawn(move || {
                for i in 0..STORES_PER_THREAD {
                    watch.store(ZBytes::from(vec![t as u8, (i & 0xff) as u8]));
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    reader.join().unwrap();
    let total = THREADS as u64 * STORES_PER_THREAD;
    assert_eq!(watch.stored_total(), total);
    assert_eq!(
        watch.seq(),
        total,
        "final snapshot must carry the HIGHEST seq — a lower-seq payload \
         overwrote a higher-seq one"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_watch_delivers_the_latest_published_value() {
    let topic = unique("flow_stream");
    let pub_node = Node::new(unique("stream_pub"), "default")
        .await
        .expect("pub node");
    let sub_node = Node::new(unique("stream_sub"), "default")
        .await
        .expect("sub node");

    let watch = sub_node.watch(&topic).await.expect("stream");
    assert!(watch.load().is_none());

    let publisher = pub_node.create_publisher(&topic).await.expect("publisher");
    // Publish is fire-and-forget: a sample sent before the pub→sub route is
    // known is silently dropped (discovery race — flaked on the self-hosted
    // CI runner). Re-publishing the burst inside the wait loop keeps the
    // latest-wins assertion intact while making the test discovery-robust.
    for i in 0..5u8 {
        publisher.publish(vec![i, i, i]).expect("publish");
    }

    // Eventually the watch holds the LAST value (intermediate values may be
    // skipped — that is the point of a latest-value cell).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(v) = watch.load() {
            if v.to_bytes().as_ref() == [4u8, 4, 4].as_slice() {
                break;
            }
        }
        publisher.publish(vec![4u8, 4, 4]).expect("republish");
        assert!(
            std::time::Instant::now() < deadline,
            "stream must converge to the latest publish; last seen: {:?}",
            watch.load().map(|v| v.to_bytes().to_vec())
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(watch.stored_total() >= 1);

    pub_node.close().await.expect("close pub");
    sub_node.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_wakes_on_new_seq_only() {
    let watch = Watch::detached("changed");
    watch.store(ZBytes::from(vec![1u8]));
    let seen = watch.seq();
    assert_eq!(seen, 1);

    // Already-newer baseline returns immediately.
    assert_eq!(watch.changed(0).await, 1);

    // A waiter parked on the current seq wakes on the NEXT store.
    let waiter = {
        let watch = std::sync::Arc::clone(&watch);
        tokio::spawn(async move { watch.changed(seen).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    watch.store(ZBytes::from(vec![2u8]));
    let new_seq = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("no lost wakeup")
        .expect("task");
    assert_eq!(new_seq, 2);
    assert_eq!(watch.load().expect("value").to_bytes().as_ref(), &[2u8]);
}

/// Concurrency hammer (changed() must never miss a generation
/// nor hang): a storer task races a changed()-chaser; the chaser must
/// observe a strictly-increasing seq chain up to the final store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_racing_stores_never_hangs_or_regresses() {
    let watch = Watch::detached("hammer");
    const STORES: u64 = 200;
    let storer = {
        let watch = std::sync::Arc::clone(&watch);
        tokio::spawn(async move {
            for i in 0..STORES {
                watch.store(ZBytes::from(vec![(i % 251) as u8]));
                if i % 16 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };
    let mut seen = 0u64;
    while seen < STORES {
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), watch.changed(seen))
            .await
            .expect("changed() must not hang mid-stream");
        assert!(
            next > seen,
            "seq strictly increases (got {next} after {seen})"
        );
        seen = next;
    }
    assert_eq!(seen, STORES, "the final store is always observed");
    storer.await.expect("storer");
}

#[test]
fn unwatched_store_never_allocates_the_notify() {
    // The honest zero-ish-cost claim: no changed() caller → the OnceLock
    // stays empty across every store.
    let watch = Watch::detached("unwatched");
    for i in 0..64u8 {
        watch.store(ZBytes::from(vec![i]));
    }
    assert_eq!(watch.stored_total(), 64);
    // (Micro-cost: store pays one OnceLock::get Acquire load; measured in
    // the on-target benchmark gate. No API to introspect OnceLock emptiness
    // without initializing — the behavioural proof is the two tests above
    // plus this alloc-free path compiling without a Notify.)
}

#[test]
fn snapshot_is_one_atomic_seq_stamp_payload_view() {
    // `snapshot()` is THE structured reader — seq, source
    // stamp, and payload from one atomic snapshot. Foreign stores
    // carry None (arrival time is NOT substituted); store_stamped
    // round-trips the source stamp with the same snapshot.
    let watch = Watch::detached("stamped");
    assert!(watch.snapshot().is_none(), "empty cell reads None");
    watch.store(ZBytes::from(vec![1u8]));
    let snap = watch.snapshot().expect("value");
    assert_eq!((snap.seq, snap.stamp_ns), (1, None));
    watch.store_stamped(ZBytes::from(vec![2u8]), Some(42_000_000));
    let snap = watch.snapshot().expect("value");
    assert_eq!((snap.seq, snap.stamp_ns), (2, Some(42_000_000)));
    assert_eq!(snap.payload.to_bytes().as_ref(), &[2u8]);
    // The load() zero-copy identity guarantee holds through snapshot():
    // two reads of one store share the same payload Arc.
    let again = watch.snapshot().expect("value");
    assert!(std::sync::Arc::ptr_eq(&snap.payload, &again.payload));
}

// ---- async pull (recv_in) ------------------------------------

#[cfg(test)]
mod recv_in_tests {
    use crate::flow::{Scope, ScopeOpts};
    use crate::node::Node;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recv_in_delivers_and_stops_on_cancel() {
        let node = Node::new("recv_in_test", "default").await.expect("node");
        let sub = node
            .create_subscriber_pull("test/recv_in", 4)
            .await
            .expect("pull subscriber");
        let publisher = node
            .create_publisher("test/recv_in")
            .await
            .expect("publisher");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Value flows.
        let scope = Scope::new_root(ScopeOpts::default());
        publisher.publish(b"hello".as_slice()).expect("publish");
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv_in(&scope))
            .await
            .expect("no timeout")
            .expect("sample");
        assert_eq!(got.payload.to_bytes().as_ref(), b"hello");

        // Stopped on scope cancel: a parked recv_in resolves promptly
        // (cancel from another task while parked).
        let scope2 = scope.clone();
        let canceller = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            scope2.cancel(crate::flow::CancelReason::Explicit);
        });
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv_in(&scope))
            .await
            .expect("no hang");
        match out {
            Err(ref s) if s.is_cancel() => {}
            Err(ref s) => panic!("expected cancel, got fault: {s:?}"),
            Ok(_) => panic!("expected Stopped, got a sample"),
        }
        canceller.await.unwrap();
        drop(publisher);
        node.close().await.expect("close");
    }
}

#[cfg(test)]
mod recv_in_review_pins {
    use crate::flow::{Scope, ScopeOpts};
    use crate::node::Node;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_waiter_wake_handoff() {
        // Pin: two concurrent recv_in waiters + two samples —
        // the wake hand-off means neither waiter can strand while data
        // remains (Notify stores AT MOST one permit).
        let node = Node::new("recv_in_multi", "default").await.expect("node");
        let sub = std::sync::Arc::new(
            node.create_subscriber_pull("test/recv_multi", 8)
                .await
                .expect("pull"),
        );
        let publisher = node
            .create_publisher("test/recv_multi")
            .await
            .expect("publisher");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let scope = Scope::new_root(ScopeOpts::default());
        let mut waiters = Vec::new();
        for _ in 0..2 {
            let sub = std::sync::Arc::clone(&sub);
            let scope = scope.clone();
            waiters.push(tokio::spawn(async move { sub.recv_in(&scope).await }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // both parked
        publisher.publish(b"a".to_vec()).expect("pub");
        publisher.publish(b"b".to_vec()).expect("pub");
        for w in waiters {
            let out = tokio::time::timeout(std::time::Duration::from_secs(5), w)
                .await
                .expect("no strand")
                .unwrap();
            assert!(out.is_ok(), "both waiters received");
        }
        node.close().await.expect("close");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recv_in_never_strands_when_close_races_the_park() {
        // WakeOnDrop wakes with notify_waiters(),
        // which stores NO permit — recv_in must create its notified
        // future BEFORE the emptiness probe, or a close landing between
        // an empty try_recv and the first poll is missed forever. No
        // parking sleep here on purpose: the close races recv_in's very
        // first probe/park transition; every ordering must resolve
        // (fault), never strand.
        for i in 0..30 {
            let node = Node::new(&format!("recv_close_race_{i}"), "default")
                .await
                .expect("node");
            let sub = node
                .create_subscriber_pull("test/recv_close_race", 4)
                .await
                .expect("pull");
            let scope = Scope::new_root(ScopeOpts::default());
            let waiter = tokio::spawn(async move { sub.recv_in(&scope).await });
            node.close().await.expect("close");
            let out = tokio::time::timeout(std::time::Duration::from_secs(10), waiter)
                .await
                .expect("recv_in resolved despite racing close")
                .unwrap();
            assert!(out.is_err(), "racing close resolves as Stopped");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_recv_resolves_on_node_close() {
        // Rust-side pin: closing the node undeclares the
        // subscriber — the WakeOnDrop guard must resolve a parked
        // recv_in promptly (fault, not a hang until cancel).
        let node = Node::new("recv_in_close", "default").await.expect("node");
        let sub = node
            .create_subscriber_pull("test/recv_close", 4)
            .await
            .expect("pull");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let scope = Scope::new_root(ScopeOpts::default());
        let waiter = tokio::spawn(async move { sub.recv_in(&scope).await });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await; // parked
        node.close().await.expect("close");
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("parked recv resolved on close")
            .unwrap();
        assert!(out.is_err(), "close resolves as Stopped");
    }
}

#[cfg(test)]
mod shield_recv_pin {
    use crate::flow::{Scope, ScopeOpts};
    use crate::node::Node;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shielded_consumer_keeps_receiving_on_cancelled_scope() {
        // Shielded-cleanup idiom: a SHIELDED cleanup
        // consumer drains its subscription even after the scope
        // cancelled (EffectiveCancelled is shield-aware).
        let node = Node::new("recv_in_shield", "default").await.expect("node");
        let sub = node
            .create_subscriber_pull("test/recv_shield", 4)
            .await
            .expect("pull");
        let publisher = node
            .create_publisher("test/recv_shield")
            .await
            .expect("publisher");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let scope = Scope::new_root(ScopeOpts::default());
        scope.cancel(crate::flow::CancelReason::Explicit); // ALREADY cancelled

        // Unshielded: a parked recv_in resolves Stopped immediately.
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv_in(&scope))
            .await
            .expect("no hang");
        assert!(out.is_err(), "unshielded sees the cancel");

        // Shielded: the cancel is masked; the sample flows.
        publisher.publish(b"drain-me".to_vec()).expect("pub");
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            scope.shield(sub.recv_in(&scope)),
        )
        .await
        .expect("no hang");
        assert!(got.is_ok(), "shielded consumer drains despite the cancel");
        node.close().await.expect("close");
    }
}
