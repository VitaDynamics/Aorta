//! Aorta Flow demo — the Rust SDK surface end-to-end (concepts:
//! `spec/Aorta_Flow_Concepts.md`).
//!
//! Single process, one node (same-session loopback pub/sub + service):
//!   1. Structured concurrency: a `Scope` tree, `spawn`, `when_all`,
//!      fail-fast isolation via a supervised scope, `offload`.
//!   2. Cancellation + budget: RT-safe polls, `call_in` interrupted by a
//!      scope cancel, deadline observation.
//!   3. Streaming: `Node::watch` — the depth-1 latest-value `Watch`.
//!   4. Readiness: `WindowSync` multi-input fusion with zero polling.
//!
//! Run: `cargo run -p aorta_sdk --features examples --example flow_demo`

use aorta_sdk::{
    when_all, CancelReason, Condition, Node, ReadinessSet, ScopeOpts, SubscriberOptions, Watch,
    WindowSync,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aorta Flow demo (Rust) ===");
    let node = Node::new("flow_demo", "default").await?;

    // ------------------------------------------------------------------
    // 1. Structured concurrency: the node's root scope parents a request
    //    scope; children run concurrently; when_all collects in order.
    // ------------------------------------------------------------------
    let root = node.root_scope();
    let request = root.child(ScopeOpts::named("demo-request").budget(Duration::from_secs(5)));
    let sensors = ["camera", "lidar", "imu"];
    let handles: Vec<_> = sensors
        .iter()
        .map(|name| {
            let name = *name;
            request.spawn(async move {
                // A "sensor warm-up": CPU-ish work goes to the offload pool,
                // never onto the async workers.
                let sum = aorta_sdk::offload(move || {
                    (0..100_000u64).sum::<u64>().wrapping_add(name.len() as u64)
                })
                .await;
                format!("{name}: ready ({sum})")
            })
        })
        .collect();
    for line in when_all(handles).await? {
        println!("  [scope] {line}");
    }
    println!(
        "  [scope] budget remaining: {:?}",
        request.remaining().map(|d| d.as_millis())
    );

    // Fail-fast is confined by a supervised scope: the panicking probe
    // cancels nothing outside itself.
    let probe_scope = request.child(ScopeOpts::named("supervised-probe").supervised());
    let probe: aorta_sdk::TaskHandle<()> =
        probe_scope.spawn(async { panic!("probe fault (contained)") });
    assert!(probe.join().await.is_err());
    assert!(!request.is_cancelled());
    println!("  [scope] supervised probe failed WITHOUT cancelling the request scope");

    // ------------------------------------------------------------------
    // 2. Cancellable RPC: an echo service on the same node; call_in
    //    inherits the scope budget; a cancel interrupts a pending call.
    // ------------------------------------------------------------------
    let service_name = "flow_demo_echo";
    let _service = node
        .create_service(service_name, |req| {
            let payload = req.payload().to_bytes().into_owned();
            tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .await?;
    // Demo-only discovery pause: production code should use the client's
    // matching-wait (WaitForMatching) instead of a magic sleep.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let client = node.create_client(service_name);
    let reply = client.call_in(&request, b"ping".to_vec()).await?;
    println!(
        "  [rpc]   call_in echo: {:?}",
        String::from_utf8_lossy(reply.to_bytes().as_ref())
    );

    // A cancelled scope refuses further calls immediately.
    let doomed = request.child(ScopeOpts::default());
    doomed.cancel(CancelReason::Explicit);
    let refused = client.call_in(&doomed, b"never".to_vec()).await;
    println!(
        "  [rpc]   cancelled-scope call refused: {}",
        refused.expect_err("must be refused")
    );

    // ------------------------------------------------------------------
    // 3. Streaming: the depth-1 latest-value watch — load() is a zero-copy
    //    shared handle of the LATEST publish; stale values are skipped.
    // ------------------------------------------------------------------
    let watch = node.watch("flow_demo_state").await?;
    let publisher = node.create_publisher_raw("flow_demo_state").await?;
    tokio::time::sleep(Duration::from_millis(300)).await; // local discovery
                                                          // AWAIT changes — no sleep-poll loop (Watch::changed is lost-wakeup-free;
                                                          // publish is fire-and-forget so the burst is re-sent until observed).
    let mut seen = watch.seq();
    let overall = std::time::Instant::now() + Duration::from_secs(10);
    let latest = loop {
        assert!(
            std::time::Instant::now() < overall,
            "stream never converged — is the local session healthy?"
        );
        for i in 0..5u8 {
            publisher.publish(vec![i])?;
        }
        seen = tokio::time::timeout(Duration::from_millis(500), watch.changed(seen))
            .await
            .unwrap_or(seen);
        if let Some(snap) = watch.snapshot() {
            if snap.payload.to_bytes().as_ref() == [4u8] {
                break snap.payload.clone();
            }
            seen = snap.seq;
        }
    };
    println!(
        "  [watch]  latest-value load(): {:?} (stored_total={})",
        latest.to_bytes().as_ref(),
        watch.stored_total()
    );

    // A foreign producer (e.g. a CAN thread) stores without zenoh:
    let bus: Arc<Watch> = Watch::detached("battery-bus");
    let producer = {
        let bus = Arc::clone(&bus);
        std::thread::spawn(move || bus.store(aorta_sdk::ZBytes::from(vec![88u8])))
    };
    producer.join().expect("producer");
    println!(
        "  [watch]  foreign-producer store observed: {:?}",
        bus.load().expect("stored").to_bytes().as_ref()
    );

    // ------------------------------------------------------------------
    // 4. Readiness: approximate-time fusion — each subscriber DECLARES its
    //    window input at build time (Flow Concepts §6.5; no hand-written
    //    feed glue) and the consumer AWAITS aligned sets. No poll loop anywhere.
    // ------------------------------------------------------------------
    let window = WindowSync::new(2, Duration::from_millis(50));
    let fresh = ReadinessSet::with_conditions(&[Condition::MessageAvailable])?;
    let _cam_sub = node
        .create_subscriber_with_options(
            "flow_demo_cam",
            |_, _| {},
            SubscriberOptions::new().with_window_feed(&window, 0),
        )
        .await?;
    let _imu_sub = node
        .create_subscriber_with_options(
            "flow_demo_imu",
            |_, _| {},
            SubscriberOptions::new()
                .with_window_feed(&window, 1)
                .with_readiness_satisfy(&fresh, 0),
        )
        .await?;
    // The producers are SCOPE CHILDREN (spawn_with — no manual clone), so
    // the same structured teardown that ends the request ends them.
    let cam_pub = node.create_publisher_raw("flow_demo_cam").await?;
    let imu_pub = node.create_publisher_raw("flow_demo_imu").await?;
    let fusion = request.child(ScopeOpts::named("fusion"));
    let feed = fusion.spawn_with(move |scope| async move {
        while !scope.is_cancelled() {
            let _ = cam_pub.publish(vec![1u8]);
            let _ = imu_pub.publish(vec![2u8]);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let set = tokio::time::timeout(Duration::from_secs(5), window.aligned()).await?;
    let spread_ns =
        set.iter().map(|(ts, _)| *ts).max().unwrap() - set.iter().map(|(ts, _)| *ts).min().unwrap();
    println!(
        "  [sync]  aligned {} builder-wired inputs within {}µs — no feed glue, no poll loop",
        set.len(),
        spread_ns / 1_000
    );
    println!(
        "  [sync]  message-available waves observed: {}",
        fresh.waves()
    );
    fusion.cancel(CancelReason::Shutdown);
    feed.await.ok();

    // Teardown: one named call cancels the whole request tree (fusion
    // included) and awaits the drain.
    request.shutdown(CancelReason::Shutdown).await;
    node.close().await?;
    println!("=== done ===");
    Ok(())
}
