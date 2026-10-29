//! ContextStore aggregator mirror — the two sanctioned callback variants
//! plus the mandatory heal pattern.
//!
//! An aggregator consumes a context prefix on behalf of a consumer that
//! needs a latest-value mirror. This example shows, against a synthetic
//! in-process owner so it runs standalone:
//!
//! 1. **In-callback bounded apply** — the watch callback performs
//!    bounded in-process work (a hashmap update under a mutex). Legal:
//!    the reentrancy contract forbids blocking Aorta FFI re-entry from a
//!    watch callback, not bounded in-process work.
//! 2. **External drain** — the callback does only bounded work (enqueue
//!    into a bounded channel); a dedicated task applies the events.
//!    Use this variant when per-event work is unbounded, takes contended
//!    locks, or must call Aorta APIs. A deliberately slow drain
//!    (simulating unbounded work) overflows the bounded queue: the
//!    overflow events are dropped and counted.
//! 3. **Mandatory heal policy** — the SAME mirror and admission
//!    gate are then fed by a `watch_with_initial_snapshot` re-creation
//!    loop on a bounded schedule, so the events the drain dropped (and
//!    any transport loss) converge on the
//!    owner state: the snapshot replay delivers the current values and
//!    the gate admits their higher sequences. Duplicate `Snapshot`
//!    events after each re-creation are absorbed by the per-(instance,
//!    key) admission gate.
//!
//! All variants sit behind a per-(`source_instance_id`, key) admission
//! gate: only strictly increasing sequences from a given
//! writer pass, so duplicates are idempotent and a writer change
//! (restart) is seen as a fresh gate, never as a reorder.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aorta_sdk::contextstore::{ContextChange, ContextStore, WatchSnapshotOptions};
use aorta_sdk::{AortaResult, Node};

/// Per-(instance, key) admission gate: admits only strictly
/// increasing sequences from a given writer. Equal or lower sequences —
/// duplicate snapshots after a watch re-creation, or a replayed live
/// event — are dropped.
#[derive(Default)]
struct AdmissionGate {
    last_seq: HashMap<(String, String), u64>,
}

impl AdmissionGate {
    fn admit(&mut self, change: &ContextChange) -> bool {
        let key = (change.source_instance_id.clone(), change.key.clone());
        let last = self.last_seq.entry(key).or_insert(0);
        if change.sequence > *last {
            *last = change.sequence;
            true
        } else {
            false
        }
    }
}

/// The aggregator's mirror: key -> (stamp_ns, u64 value, source_instance_id).
type Mirror = Arc<Mutex<HashMap<String, (u64, u64, String)>>>;

fn apply(gate: &mut AdmissionGate, mirror: &Mirror, change: &ContextChange) {
    if gate.admit(change) {
        mirror.lock().unwrap().insert(
            change.key.clone(),
            (
                change.stamp_ns,
                change.u64_value,
                change.source_instance_id.clone(),
            ),
        );
    }
}

/// Variant 1: in-callback bounded apply. The callback does O(1) work —
/// one gate check plus one hashmap insert — and never awaits or calls
/// back into Aorta.
async fn run_in_callback_aggregator(
    store: &ContextStore,
    prefix: &str,
    duration: Duration,
) -> AortaResult<()> {
    let mirror: Mirror = Arc::new(Mutex::new(HashMap::new()));
    let gate = Arc::new(Mutex::new(AdmissionGate::default()));
    let mirror_cb = Arc::clone(&mirror);
    let gate_cb = Arc::clone(&gate);
    let watch = store
        .watch(prefix, move |change| {
            apply(&mut gate_cb.lock().unwrap(), &mirror_cb, &change);
        })
        .await?;
    tokio::time::sleep(duration).await;
    watch.shutdown().await?;
    print_mirror("in-callback bounded apply", &mirror);
    Ok(())
}

/// Variant 2: external drain. The callback only enqueues (bounded work);
/// a dedicated task applies events, so unbounded work never runs on the
/// Zenoh worker thread. A deliberately slow drain (the unbounded work
/// this variant exists for) overflows the bounded queue: overflow events
/// are dropped and counted. Returns the mirror + gate so variant 3 can
/// heal the SAME state, and the number of dropped events.
async fn run_external_drain_aggregator(
    store: &ContextStore,
    prefix: &str,
    duration: Duration,
) -> AortaResult<(Mirror, Arc<Mutex<AdmissionGate>>, u64)> {
    let mirror: Mirror = Arc::new(Mutex::new(HashMap::new()));
    let mirror_drain = Arc::clone(&mirror);
    let gate = Arc::new(Mutex::new(AdmissionGate::default()));
    let gate_drain = Arc::clone(&gate);
    // Deliberately tiny: with the writer at ~5 put/s and the drain at
    // ~3.3 events/s, the queue overflows within the drain window.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ContextChange>(2);
    let dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let dropped_cb = Arc::clone(&dropped);
    // The drain task is where unbounded work belongs. It stops when the
    // watch is shut down: shutdown() drops the subscriber, which drops
    // the callback closure and the sender it owns.
    let drain = tokio::spawn(async move {
        while let Some(change) = rx.recv().await {
            // Simulated slow consumer: the unbounded per-event work that
            // motivated the external-drain variant. This is what makes
            // the bounded queue overflow.
            tokio::time::sleep(Duration::from_millis(300)).await;
            apply(&mut gate_drain.lock().unwrap(), &mirror_drain, &change);
        }
    });
    let watch = store
        .watch(prefix, move |change| {
            // Bounded work only. Overflow drops the event and counts it:
            // that dropped state is exactly what the heal loop (variant
            // 3, same mirror + gate) converges afterwards.
            if tx.try_send(change).is_err() {
                dropped_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        })
        .await?;
    tokio::time::sleep(duration).await;
    watch.shutdown().await?;
    drain.await.expect("drain task panicked");
    let dropped_count = dropped.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "external drain: {} events dropped to queue overflow",
        dropped_count
    );
    print_mirror("external drain (before heal)", &mirror);
    Ok((mirror, gate, dropped_count))
}

/// Variant 3: mandatory heal policy. Re-creates
/// `watch_with_initial_snapshot` on a bounded schedule — the period is
/// the consumer's own freshness bound (frozen per consumer at cutover;
/// here 500 ms for the demo) — feeding the SAME mirror and admission
/// gate the external-drain variant returned, so the events it dropped
/// to queue overflow (and any transport loss) converge on the owner
/// state. Duplicate `Snapshot` events after each re-creation are
/// absorbed by the gate; `SnapshotDone` markers are ignored.
async fn run_healing_aggregator(
    store: &ContextStore,
    prefix: &str,
    period: Duration,
    duration: Duration,
    mirror: Mirror,
    gate: Arc<Mutex<AdmissionGate>>,
) -> AortaResult<()> {
    let start = tokio::time::Instant::now();
    let mut re_creations = 0u32;
    while start.elapsed() < duration {
        let mirror_cb = Arc::clone(&mirror);
        let gate_cb = Arc::clone(&gate);
        let watch = store
            .watch_with_initial_snapshot(prefix, WatchSnapshotOptions::default(), move |change| {
                if change.is_snapshot_done() {
                    return;
                }
                apply(&mut gate_cb.lock().unwrap(), &mirror_cb, &change);
            })
            .await?;
        // Hold the watch for one heal period, then re-create. The fresh
        // snapshot converges any loss the live stream accumulated.
        tokio::time::sleep(period).await;
        watch.shutdown().await?;
        re_creations += 1;
    }
    println!(
        "heal pattern: {} watch re-creations over {:?} (period {:?})",
        re_creations, duration, period
    );
    print_mirror("external drain (after heal)", &mirror);
    Ok(())
}

fn print_mirror(label: &str, mirror: &Mirror) {
    let guard = mirror.lock().unwrap();
    println!("--- mirror after {} ---", label);
    for (key, (stamp_ns, value, instance)) in guard.iter() {
        println!(
            "  {} = {} (stamp {} from {})",
            key, value, stamp_ns, instance
        );
    }
}

/// Synthetic owner: declares a prefix and writes on a ticker so the
/// example runs standalone with live events flowing during every
/// aggregator window. Real deployments use the same ContextStore owner
/// API on the writer side (edge-triggered).
async fn run_synthetic_owner(
    store: &ContextStore,
    prefix: &str,
    duration: Duration,
) -> AortaResult<()> {
    store.declare_owned_prefix(prefix).await?;
    let start = tokio::time::Instant::now();
    let mut i = 0u64;
    while start.elapsed() < duration {
        store
            .set_u64(&format!("fsm/input/owner{}", i % 2), i)
            .await?;
        i += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let node = Node::new("ctx_aggregator_mirror", "default").await?;
        let store = node.create_context_store();

        let owner_store = store.clone();
        let owner = tokio::spawn(async move {
            run_synthetic_owner(&owner_store, "fsm/input/**", Duration::from_millis(3_700)).await
        });

        // Give the first write a moment to propagate.
        tokio::time::sleep(Duration::from_millis(300)).await;

        run_in_callback_aggregator(&store, "fsm/input/**", Duration::from_millis(800)).await?;
        let (drain_mirror, drain_gate, dropped) =
            run_external_drain_aggregator(&store, "fsm/input/**", Duration::from_millis(2_000))
                .await?;
        assert!(
            dropped > 0,
            "the slow-consumer drain must overflow the bounded queue for the heal demo"
        );
        // The heal loop feeds the SAME mirror + gate the drain used: the
        // dropped state must converge on the owner state.
        run_healing_aggregator(
            &store,
            "fsm/input/**",
            Duration::from_millis(500),
            Duration::from_millis(1_800),
            drain_mirror,
            drain_gate,
        )
        .await?;

        owner.await.expect("synthetic owner panicked")?;
        Ok(())
    })
}
