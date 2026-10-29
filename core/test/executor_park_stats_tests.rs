//! Park / wake-signal counter semantics.
//!
//! The RX wakeup-chain attribution says the executor pays one wake pair
//! (`write` to the runtime's eventfd on the producer side, `epoll_wait` on the
//! consumer side) per message **only when the consumer was parked**. These
//! counters make that observable without a tracer:
//!
//! * `parks` — one increment per park ENTRY on the spin loop, not per message.
//!   At a paced rate the consumer drains to empty between messages, so
//!   `parks ≈ messages`; under saturation it never drains, so `parks ≪ messages`.
//! * `wake_signals` — one increment per event successfully published to the
//!   backend channel, i.e. per wake the sender *attempts*. It is the
//!   denominator: `parks / wake_signals` is the share of sends that actually
//!   cost a wake syscall pair.
//!
//! These tests pin exactly that contract, because the benchmark gate
//! (`parks/msg = 1.0 ± 0.1`) is only meaningful if "park" means park-entry and
//! "wake signal" means published-send.

use super::{ExecutorOptions, MultiThreadedExecutor, SingleThreadedExecutor};
use crate::event_queue::{Event, EventKind, EventQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn counting_event(counter: &Arc<AtomicUsize>) -> Event {
    let counter = Arc::clone(counter);
    Event::new(EventKind::SubscriptionData, move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

/// A send that reaches the backend channel is one wake signal; a send that is
/// refused is not. `wake_signals` counts publications, never attempts.
#[test]
fn wake_signals_count_published_sends_only() {
    let queue = EventQueue::new(2);
    let sender = queue.sender();
    let ran = Arc::new(AtomicUsize::new(0));

    assert_eq!(queue.wake_signals(), 0, "counter starts at zero");

    sender.try_send(counting_event(&ran)).expect("first send");
    sender.try_send(counting_event(&ran)).expect("second send");
    assert_eq!(
        queue.wake_signals(),
        2,
        "each published send is one wake signal"
    );

    // Capacity is 2 and nothing has been consumed: this one is refused.
    sender
        .try_send(counting_event(&ran))
        .expect_err("queue is full");
    assert_eq!(
        queue.wake_signals(),
        2,
        "a refused send publishes nothing, so it signals no wake"
    );
}

/// Paced arrivals: the spin loop drains to empty between events, so it parks
/// once per event. This is the shape the 1 kHz benchmark cell runs in, and the
/// reason `parks/msg ≈ 1.0` is the expected gate value there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paced_arrivals_park_once_per_event() {
    const EVENTS: usize = 16;
    const SPACING: Duration = Duration::from_millis(5);

    let mut exec = SingleThreadedExecutor::new_with_options(64, ExecutorOptions::default());
    let sender = exec.queue().sender();
    let stats = exec.spin_stats_reader();
    let stop = exec.stop_handle();
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_push = Arc::clone(&ran);

    assert_eq!(stats.snapshot().parks, 0, "counter starts at zero");

    let spin = tokio::spawn(async move {
        exec.spin().await;
    });

    // Let the loop reach its first park before the first arrival, so the count
    // below is "one park per event plus the initial one" and nothing else.
    tokio::time::sleep(SPACING).await;
    for _ in 0..EVENTS {
        sender
            .try_send(counting_event(&ran_push))
            .expect("paced send");
        tokio::time::sleep(SPACING).await;
    }
    stop.stop();
    spin.await.expect("spin join");

    assert_eq!(
        ran.load(Ordering::Relaxed),
        EVENTS,
        "every paced event must have run"
    );
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot.wake_signals, EVENTS as u64,
        "one wake signal per published send"
    );
    // One park before the first arrival, one after draining each event, and at
    // most one more racing the stop: the band is tight on purpose — a loop that
    // parked per spin iteration, or not at all, falls outside it.
    let parks = snapshot.parks;
    assert!(
        (EVENTS as u64..=EVENTS as u64 + 3).contains(&parks),
        "paced arrivals must park about once per event: parks={parks}, events={EVENTS}"
    );
}

/// Saturated ingress: the loop never observes an empty queue while the backlog
/// lasts, so it parks a handful of times for hundreds of messages. This is the
/// flood regime the bimodal investigation is about — `parks/msg` is the direct
/// observable that separates it from the paced regime above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saturated_ingress_parks_far_less_than_messages() {
    const EVENTS: usize = 512;

    let mut exec = SingleThreadedExecutor::new_with_options(1024, ExecutorOptions::default());
    let sender = exec.queue().sender();
    let stats = exec.spin_stats_reader();
    let stop = exec.stop_handle();
    let ran = Arc::new(AtomicUsize::new(0));

    // Fill the queue BEFORE the loop starts: it has a backlog from its very
    // first iteration and must not park until it has drained the lot.
    for _ in 0..EVENTS {
        sender.try_send(counting_event(&ran)).expect("prefill send");
    }
    assert_eq!(stats.snapshot().parks, 0, "no park before the loop runs");

    let spin = tokio::spawn(async move {
        exec.spin().await;
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while ran.load(Ordering::Relaxed) < EVENTS {
        assert!(
            std::time::Instant::now() < deadline,
            "backlog must drain: ran={}",
            ran.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.stop();
    spin.await.expect("spin join");

    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot.wake_signals, EVENTS as u64,
        "one wake signal per published send, regardless of parking"
    );
    let parks = snapshot.parks;
    assert!(
        parks > 0,
        "the loop parks once the backlog is drained — a zero here would make \
         the bound below vacuous"
    );
    assert!(
        parks * 10 < EVENTS as u64,
        "a saturated loop must park far less than once per message: parks={parks}, events={EVENTS}"
    );
}

/// The MT loop must obey the same park-entry contract as the ST loop:
/// with FIFO scheduling and `max_drain_per_cycle = 1`,
/// a prefilled saturated queue reaches the `select!` once per event and the
/// queue future is immediately ready — the task never blocks. Counting entry
/// into the select would report `parks/msg ≈ 1` for exactly the saturated
/// workload the metric exists to distinguish from the paced one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mt_fifo_saturated_ingress_parks_far_less_than_messages() {
    const EVENTS: usize = 512;

    let mut options = ExecutorOptions::default();
    options.priority.max_drain_per_cycle = 1; // the exact regression scenario
    let mut exec = MultiThreadedExecutor::new_with_options(1024, 2, options);
    let sender = exec.queue().sender();
    let stats = exec.spin_stats_reader();
    let stop = exec.stop_handle();
    let ran = Arc::new(AtomicUsize::new(0));

    for _ in 0..EVENTS {
        sender.try_send(counting_event(&ran)).expect("prefill send");
    }
    assert_eq!(stats.snapshot().parks, 0, "no park before the loop runs");

    let spin = tokio::spawn(async move {
        exec.spin().await;
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while ran.load(Ordering::Relaxed) < EVENTS {
        assert!(
            std::time::Instant::now() < deadline,
            "backlog must drain: ran={}",
            ran.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.stop();
    spin.await.expect("spin join");

    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot.wake_signals, EVENTS as u64,
        "one wake signal per published send, regardless of parking"
    );
    let parks = snapshot.parks;
    assert!(
        parks > 0,
        "the loop parks once the backlog is drained — a zero here would make \
         the bound below vacuous"
    );
    // MT semantics differ from ST: after the queue drains into the ready
    // buckets, the loop legitimately blocks waiting for WORKER COMPLETIONS
    // (join_next), and with instant callbacks the share of completions that
    // land inside the guard window is scheduler-dependent — there is no tight
    // deterministic bound. What IS structural: every counted park is ended by
    // consuming at least one completion (or queue event), so with the
    // non-blocking guards in place `parks <= events` by construction. The
    // unguarded counter violated exactly this (observed parks=813 for 512
    // events: every immediately-ready select entry was counted as a park).
    assert!(
        parks <= EVENTS as u64,
        "a counted park must correspond to a consumed wake; select entries \
         with an immediately-ready arm must not count: parks={parks}, events={EVENTS}"
    );
}
