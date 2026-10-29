//! Flow readiness sets, approximate-time `WindowSync`, and
//! `FdReadable` — including the acceptance gate: the
//! mechanism demonstrably replaces an application poll loop.

use super::{ReadinessSet, WindowSync};
use crate::ZBytes;
use std::sync::Arc;
use std::time::Duration;

fn payload(tag: u8) -> Arc<ZBytes> {
    Arc::new(ZBytes::from(vec![tag]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_set_fires_per_completed_wave() {
    let set = ReadinessSet::new(3).expect("set");
    let baseline = set.waves();
    let waiter = {
        let set = Arc::clone(&set);
        tokio::spawn(async move { set.wait_wave_after(baseline).await })
    };
    set.satisfy(0);
    set.satisfy(1);
    assert_eq!(set.pending_mask(), 0b100);
    set.satisfy(2); // completes the wave
    let wave = tokio::time::timeout(Duration::from_secs(30), waiter)
        .await
        .expect("wave wakes the waiter")
        .expect("task");
    assert_eq!(wave, 1);
    // The mask reset for the next wave.
    assert_eq!(set.pending_mask(), 0b111);
    set.satisfy(2);
    set.satisfy(1);
    set.satisfy(0);
    assert_eq!(set.waves(), 2, "recurring waves keep firing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn window_sync_emits_only_within_slop() {
    let window = WindowSync::new(3, Duration::from_millis(10));
    // Misaligned: input 0's sample is 50ms older than the others.
    window.feed(0, 1_000_000, payload(0));
    window.feed(1, 51_000_000, payload(1));
    window.feed(2, 52_000_000, payload(2));
    assert!(
        window.try_aligned().is_none(),
        "spread > slop must not emit"
    );

    // A fresh input-0 sample within slop completes the window.
    window.feed(0, 53_000_000, payload(9));
    let set = tokio::time::timeout(Duration::from_secs(30), window.aligned())
        .await
        .expect("aligned set");
    assert_eq!(set.len(), 3);
    let timestamps: Vec<u64> = set.iter().map(|(ts, _)| *ts).collect();
    assert_eq!(timestamps, vec![53_000_000, 51_000_000, 52_000_000]);
    assert_eq!(
        set[0].1.to_bytes().as_ref(),
        &[9u8],
        "latest replaced older"
    );
}

/// The acceptance shape: the readiness mechanism
/// replaces the hand-rolled poll loop. Three sensor feeds fire at different
/// rates; the fusion consumer AWAITS aligned sets — it never spins, never
/// sleeps, never polls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fusion_gate_replaces_the_application_poll_loop() {
    let window = WindowSync::new(3, Duration::from_millis(5));

    // Producers: camera/lidar/imu at different cadences, timestamped on a
    // shared clock.
    let start = std::time::Instant::now();
    let producers: Vec<_> = (0..3usize)
        .map(|input| {
            let window = Arc::clone(&window);
            tokio::spawn(async move {
                for _ in 0..20 {
                    let ts = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    window.feed(input, ts, payload(input as u8));
                    tokio::time::sleep(Duration::from_millis(2 + input as u64)).await;
                }
            })
        })
        .collect();

    // The consumer: awaits aligned sets — zero polling.
    let mut fused = 0usize;
    while fused < 3 {
        let set = tokio::time::timeout(Duration::from_secs(30), window.aligned())
            .await
            .expect("an aligned set within the produce window");
        assert_eq!(set.len(), 3);
        let min = set.iter().map(|(ts, _)| *ts).min().expect("ts");
        let max = set.iter().map(|(ts, _)| *ts).max().expect("ts");
        assert!(max - min <= 5_000_000, "aligned within slop");
        fused += 1;
    }
    for p in producers {
        p.await.expect("producer");
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_fd_drains_a_pipe_into_the_ring() {
    use std::io::Write;
    use std::os::unix::io::{FromRawFd, OwnedFd};

    let mut fds = [0i32; 2];
    // SAFETY: plain pipe(2); both ends owned below.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // Non-blocking read end for the AsyncFd loop.
    // SAFETY: fds[0] is the freshly created read end.
    unsafe {
        let flags = libc::fcntl(fds[0], libc::F_GETFL);
        libc::fcntl(fds[0], libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    // SAFETY: transfers ownership of the read end exactly once.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: transfers ownership of the write end exactly once.
    let mut write_end = unsafe { std::fs::File::from_raw_fd(fds[1]) };

    let fd_scope = crate::flow::Scope::new_root(crate::flow::ScopeOpts::named("poll-fd-test"));
    let ring = super::poll_fd_into_watch(read_end, &fd_scope)
        .await
        .expect("poll_fd");
    write_end.write_all(&[1, 2, 3]).expect("write");
    write_end.flush().expect("flush");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = ring.load() {
            assert_eq!(v.to_bytes().as_ref(), &[1u8, 2, 3]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "fd data reaches the ring"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Regression: a burst larger than the 4096-byte read buffer must
/// be fully drained — clearing readiness after a PARTIAL read would strand
/// the tail with no further edge (tokio AsyncFd is edge-triggered).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_fd_drains_bursts_larger_than_the_read_buffer() {
    use std::io::Write;
    use std::os::unix::io::{FromRawFd, OwnedFd};

    let mut fds = [0i32; 2];
    // SAFETY: plain pipe(2); both ends owned below.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: fds[0] is the freshly created read end.
    unsafe {
        let flags = libc::fcntl(fds[0], libc::F_GETFL);
        libc::fcntl(fds[0], libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    // SAFETY: transfers ownership of the read end exactly once.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: transfers ownership of the write end exactly once.
    let mut write_end = unsafe { std::fs::File::from_raw_fd(fds[1]) };

    let fd_scope = crate::flow::Scope::new_root(crate::flow::ScopeOpts::named("poll-fd-test"));
    let ring = super::poll_fd_into_watch(read_end, &fd_scope)
        .await
        .expect("poll_fd");

    // One 10-KiB burst (single writev → one readiness edge), then EOF so
    // the tail chunk is unambiguous.
    let burst: Vec<u8> = (0..10_240u32).map(|i| (i % 251) as u8).collect();
    write_end.write_all(&burst).expect("write");
    drop(write_end);

    // The ring is latest-value; after a full drain the LAST chunk (10240 %
    // 4096 = 2048 bytes, ending in burst's final byte) must be observable.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = ring.load() {
            let bytes = v.to_bytes();
            if bytes.len() == 2048 && bytes.last() == burst.last() {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the full burst must drain (got {:?} bytes latest)",
            ring.load().map(|v| v.to_bytes().len())
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ring.stored_total() >= 3, "burst split into >= 3 chunks");
}

#[test]
fn readiness_set_bounds_and_named_conditions() {
    // AortaResult error paths name the 1..=64 bound.
    let err = ReadinessSet::new(0)
        .map(|_| ())
        .expect_err("zero conditions");
    assert!(err.to_string().contains("1..=64"), "got: {err}");
    let err = ReadinessSet::new(65)
        .map(|_| ())
        .expect_err("too many conditions");
    assert!(err.to_string().contains("1..=64"), "got: {err}");

    // Condition is consumed by a constructor; indices are slice positions.
    let set = ReadinessSet::with_conditions(&[
        super::Condition::MessageAvailable,
        super::Condition::Periodic,
    ])
    .expect("set");
    assert_eq!(set.condition(0), Some(super::Condition::MessageAvailable));
    assert_eq!(set.condition(1), Some(super::Condition::Periodic));
    assert_eq!(set.condition(2), None);
    set.satisfy(0);
    assert_eq!(set.pending_mask(), 0b10, "bit 1 (Periodic) still missing");
    set.satisfy(1);
    assert_eq!(set.waves(), 1, "positional satisfy completes the wave");
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "at least one input")]
fn window_sync_rejects_zero_inputs_in_debug() {
    let _ = WindowSync::new(0, Duration::from_millis(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn window_sync_bounds_the_emitted_queue() {
    // A lagging consumer drops the OLDEST set, counted, never unbounded.
    let window = WindowSync::with_capacity(1, Duration::from_millis(1), 2);
    for i in 0..5u8 {
        window.feed(0, 1_000 * u64::from(i), payload(i));
    }
    let stats = window.stats();
    assert_eq!(stats.emitted, 5, "single-input sets always align");
    assert_eq!(stats.dropped_sets, 3, "depth 2 kept the newest two");
    // The two remaining are the NEWEST (oldest dropped first).
    let first = window.try_aligned().expect("set");
    assert_eq!(first[0].1.to_bytes().as_ref(), &[3u8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn window_sync_warns_when_inputs_never_align() {
    // Gross, consecutive misalignment (mixed clock domains) is loud.
    let window = WindowSync::new(2, Duration::from_micros(1));
    // Input 0 "unix epoch ns"-scale, input 1 "monotonic"-scale: never within
    // slop; every full set is a gross miss.
    for i in 0..10u64 {
        window.feed(0, 1_700_000_000_000_000_000 + i, payload(0));
        window.feed(1, 1_000_000 + i, payload(1));
    }
    let stats = window.stats();
    assert_eq!(stats.emitted, 0, "nothing aligned");
    assert_eq!(stats.fed, vec![10, 10]);
    assert!(stats.last_ts[0] > stats.last_ts[1]);
    // The detector itself is machine-assertable — 10
    // consecutive gross misses crossed the 8-miss threshold exactly once.
    assert!(
        stats.never_aligns_warned,
        "the one-time never-aligns WARN fired"
    );
}

fn test_pipe() -> (std::os::unix::io::OwnedFd, std::fs::File) {
    use std::os::unix::io::{FromRawFd, OwnedFd};
    let mut fds = [0i32; 2];
    // SAFETY: plain pipe(2); both ends owned below.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: fds[0] is the freshly created read end.
    unsafe {
        let flags = libc::fcntl(fds[0], libc::F_GETFL);
        libc::fcntl(fds[0], libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    // SAFETY: transfers ownership of each end exactly once.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: as above.
    let write_end = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    (read_end, write_end)
}

// ---------------------------------------------------------------------------
// Declarative wiring: subscriber → WindowSync / ReadinessSet
// ---------------------------------------------------------------------------

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
fn window_and_set_expose_their_capacity() {
    // Bind-time validation needs both surfaces to report their width.
    let window = WindowSync::new(3, Duration::from_millis(1));
    assert_eq!(window.inputs(), 3);
    let set = ReadinessSet::new(2).expect("set");
    assert_eq!(set.count(), 2);
}

#[test]
fn satisfier_closure_satisfies_its_index() {
    // Satisfier sugar: a validated closure factory for timer/join/any-source wiring.
    let set = ReadinessSet::new(2).expect("set");
    let s0 = set.satisfier(0).expect("in range");
    let s1 = set.satisfier(1).expect("in range");
    assert!(
        set.satisfier(2).is_err(),
        "out-of-range satisfier must be refused at build time"
    );
    s0();
    assert_eq!(set.pending_mask(), 0b10);
    s1();
    assert_eq!(set.waves(), 1, "the two satisfiers complete the wave");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_wires_periodic_condition_via_satisfier() {
    use crate::node::Node;

    // `Condition::Periodic` wiring is just the satisfier closure handed
    // to a core timer — no bespoke timer-readiness surface.
    let mut exec = crate::executor::SingleThreadedExecutor::new(256);
    let node = Node::new(unique("readiness_timer"), "default")
        .await
        .expect("node");
    exec.add_node(node.clone());
    let set = ReadinessSet::with_conditions(&[super::Condition::Periodic]).expect("set");
    let baseline = set.waves();
    let _timer = node
        .create_timer(
            Duration::from_millis(10),
            set.satisfier(0).expect("in range"),
        )
        .await
        .expect("timer");
    let stop = exec.stop_handle();
    let spin = tokio::spawn(async move { exec.spin().await });
    let wave = tokio::time::timeout(Duration::from_secs(30), set.wait_wave_after(baseline))
        .await
        .expect("timer tick completes the 1-bit wave");
    assert!(wave > baseline);
    stop.stop();
    let _ = spin.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_builder_feeds_window_and_satisfies_set() {
    use crate::node::Node;
    use crate::subscriber::SubscriberOptions;

    let node = Node::new(unique("readiness_wire"), "default")
        .await
        .expect("node");
    let window = WindowSync::new(2, Duration::from_secs(1));
    let set = ReadinessSet::new(1).expect("set");
    let topic_a = unique("test/readiness_a");
    let topic_b = unique("test/readiness_b");

    let _sub_a = node
        .create_subscriber_with_options(
            &topic_a,
            |_, _| {},
            SubscriberOptions::new().with_window_feed(&window, 0),
        )
        .await
        .expect("sub a");
    let _sub_b = node
        .create_subscriber_with_options(
            &topic_b,
            |_, _| {},
            SubscriberOptions::new()
                .with_window_feed(&window, 1)
                .with_readiness_satisfy(&set, 0),
        )
        .await
        .expect("sub b");

    let pub_a = node.create_publisher(&topic_a).await.expect("pub a");
    let pub_b = node.create_publisher(&topic_b).await.expect("pub b");

    // Republish on a cadence until aligned: subscription matching is
    // asynchronous, so early publishes may precede the route.
    let aligned = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            pub_a.publish(vec![0xAAu8]).expect("pub");
            pub_b.publish(vec![0xBBu8]).expect("pub");
            if let Some(set) = window.try_aligned() {
                return set;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("builder-wired inputs align");
    assert_eq!(aligned.len(), 2);
    assert_eq!(aligned[0].1.to_bytes().as_ref(), &[0xAAu8]);
    assert_eq!(aligned[1].1.to_bytes().as_ref(), &[0xBBu8]);
    assert!(set.waves() >= 1, "satisfy wire completed the 1-bit wave");
    let stats = window.stats();
    assert!(stats.fed[0] >= 1 && stats.fed[1] >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_rejects_out_of_range_readiness_wire() {
    use crate::node::Node;
    use crate::subscriber::SubscriberOptions;

    let node = Node::new(unique("readiness_oor"), "default")
        .await
        .expect("node");
    let window = WindowSync::new(2, Duration::from_millis(5));
    let err = node
        .create_subscriber_with_options(
            &unique("test/readiness_oor"),
            |_, _| {},
            SubscriberOptions::new().with_window_feed(&window, 2),
        )
        .await
        .map(|_| ())
        .expect_err("input 2 of a 2-input window is a wiring bug");
    assert!(
        err.to_string().contains("out of range"),
        "declare-time rejection names the bug: {err}"
    );

    let set = ReadinessSet::new(1).expect("set");
    let err = node
        .create_subscriber_with_options(
            &unique("test/readiness_oor2"),
            |_, _| {},
            SubscriberOptions::new().with_readiness_satisfy(&set, 1),
        )
        .await
        .map(|_| ())
        .expect_err("condition 1 of a 1-condition set is a wiring bug");
    assert!(err.to_string().contains("out of range"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_subscriber_binds_readiness_post_declare() {
    use crate::node::Node;

    let node = Node::new(unique("readiness_pull"), "default")
        .await
        .expect("node");
    let topic = unique("test/readiness_pull");
    let sub = node
        .create_subscriber_pull(&topic, 4)
        .await
        .expect("pull subscriber");

    let window = WindowSync::new(1, Duration::from_millis(5));
    let set = ReadinessSet::new(1).expect("set");
    sub.bind_window(&window, 0, super::WindowStamp::Receive)
        .expect("bind window");
    sub.bind_readiness(&set, 0).expect("bind set");
    assert!(
        sub.bind_window(&window, 1, super::WindowStamp::Receive)
            .is_err(),
        "post-declare bind validates the index too"
    );

    let publisher = node.create_publisher(&topic).await.expect("publisher");
    let aligned = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            publisher.publish(vec![0x5Au8]).expect("pub");
            if let Some(set) = window.try_aligned() {
                return set;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("pull-mode wire feeds the window");
    assert_eq!(aligned[0].1.to_bytes().as_ref(), &[0x5Au8]);
    assert!(set.waves() >= 1);
    // The wire OBSERVES the stream — the pull surface still delivers.
    let sample = sub.try_recv().expect("try_recv");
    assert!(sample.is_some(), "sample still reaches the ring");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbind_stops_the_wire_and_reports_the_removal() {
    // Bind's inverse — after unbind, arriving samples stop
    // feeding the window/set; a second unbind reports nothing matched.
    use crate::node::Node;

    let node = Node::new(unique("readiness_unbind"), "default")
        .await
        .expect("node");
    let topic = unique("test/readiness_unbind");
    let sub = node
        .create_subscriber_pull(&topic, 8)
        .await
        .expect("pull subscriber");

    let window = WindowSync::new(1, Duration::from_millis(5));
    let set = ReadinessSet::new(1).expect("set");
    sub.bind_window(&window, 0, super::WindowStamp::Receive)
        .expect("bind window");
    sub.bind_readiness(&set, 0).expect("bind set");

    let publisher = node.create_publisher(&topic).await.expect("publisher");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            publisher.publish(vec![0x11u8]).expect("pub");
            if window.stats().fed[0] > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("wire feeds before unbind");

    assert_eq!(sub.unbind_window(&window, 0), 1, "one window wire removed");
    assert_eq!(sub.unbind_readiness(&set, 0), 1, "one set wire removed");
    assert_eq!(sub.unbind_window(&window, 0), 0, "second unbind: nothing");

    let fed_before = window.stats().fed[0];
    let waves_before = set.waves();
    for _ in 0..5 {
        publisher.publish(vec![0x22u8]).expect("pub");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        window.stats().fed[0],
        fed_before,
        "unbound wire feeds nothing"
    );
    assert_eq!(set.waves(), waves_before, "unbound set sees no waves");
    // Delivery is unaffected — the pull surface still receives.
    assert!(sub.try_recv().expect("try_recv").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_fd_reader_exits_promptly_on_scope_cancel() {
    // The reader is scope-bound — cancellation stops it well
    // before the 500ms ring-drop liveness tick, and close() accounts it.
    let (read_end, _write_end) = test_pipe();
    let scope = crate::flow::Scope::new_root(crate::flow::ScopeOpts::named("poll-fd-cancel"));
    let ring = super::poll_fd_into_watch(read_end, &scope)
        .await
        .expect("poll_fd");
    let _keep_ring = ring; // ring stays alive: cancel is the ONLY exit
    let started = std::time::Instant::now();
    scope.cancel(crate::flow::CancelReason::Shutdown);
    scope.close().await; // returns only once the reader released its child
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "reader must exit on cancel, not on the liveness tick: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_fd_refuses_a_closing_scope() {
    let (read_end, _write_end) = test_pipe();
    let scope = crate::flow::Scope::new_root(crate::flow::ScopeOpts::named("poll-fd-closed"));
    scope.close().await;
    let err = super::poll_fd_into_watch(read_end, &scope)
        .await
        .expect_err("closing scope must refuse the reader");
    assert!(err.to_string().contains("closing"));
}

#[test]
fn hlc_to_unix_ns_converts_the_ntp64_fixed_point_to_real_nanoseconds() {
    // Regression: three RX paths used
    // `get_time().as_u64()` — the raw NTP64 bit pattern
    // (`seconds << 32 | frac`), ~4.295x off — where the field contract
    // says nanoseconds. Pin the unit so a revert turns red.
    let wall = Duration::new(1_767_225_600, 123_456_789); // 2026-01-01T00:00:00.123456789Z
    let ntp64 = zenoh::time::NTP64::from(wall);
    let ns = super::hlc_to_unix_ns(ntp64);
    let expected = wall.as_nanos() as u64;
    assert!(
        ns.abs_diff(expected) <= 4,
        "NTP64 round-trip must be nanoseconds (2^-32 s fraction rounding \
         allows a few ns): got {ns}, expected {expected}"
    );
    // The exact regression shape: the raw bit pattern is ~2^32/1e9 larger.
    let raw = ntp64.as_u64();
    assert_ne!(ns, raw, "as_u64() is NOT nanoseconds");
    assert!(
        raw / ns >= 4,
        "raw fixed-point should be ~4.295x the ns value (got {raw} vs {ns})"
    );
}
