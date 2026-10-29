//! Integration tests for PUB/SUB topic-path trace instrumentation.
//!
//! Stands up a real in-process Zenoh publisher + subscriber and asserts the
//! two-tier contract:
//!   - the aggregate `topic.publish` / `topic.callback` HISTOGRAMS record
//!     whenever the master switch is on (even at the default `always`/`Info`
//!     tier), so latency percentiles are always available; but
//!   - the per-message waterfall SPANS (and the traceparent on the wire) are
//!     gated under the dedicated `aorta.topic` scope at `Debug`, so the default
//!     tier records NOTHING on the topic path — full recording is one
//!     `AORTA_TRACE=aorta.topic=standard` (or `standard`/`verbose`) away.
//!
//! Run filtered single-threaded for an exact picture:
//! `cargo test -p aorta_core --lib trace_pubsub_tests -- --test-threads=1`.

use crate::error::AortaResult;
use crate::node::Node;
use crate::trace::{
    intern_scope, set_scope_enabled, set_scope_threshold, set_trace_enabled, stage_event_for,
    stage_snapshot, StageId, TOPIC_SCOPE_NAME,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

fn unique_topic(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

/// At the default `always` tier (the `Info` floor), a publish + delivery records
/// the always-on histograms but emits NO topic waterfall spans and puts NO
/// traceparent on the wire — the byte-identical legacy data path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_path_records_histograms_but_no_spans_at_default_tier() -> AortaResult<()> {
    let group = format!("tp_default_{}", std::process::id());
    let pub_node = Node::new("trace_topic_pub", &group).await?;
    let sub_node = Node::new("trace_topic_sub", &group).await?;
    let topic = unique_topic("trace_topic_default");

    let (tx, rx) = mpsc::channel::<()>();
    let _sub = sub_node
        .create_subscriber(&topic, move |_data, _ctx| {
            let _ = tx.send(());
        })
        .await?;
    let publisher = pub_node.create_publisher(&topic).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let _trace_guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    // Simulate the default `always` tier: the `aorta.topic` scope sits at the
    // `Info` floor, which filters the `Debug`-level topic spans.
    let topic_scope = intern_scope(TOPIC_SCOPE_NAME);
    set_scope_threshold(
        topic_scope,
        crate::trace::level_value(crate::trace::LogLevel::Info),
    );

    let ev_publish = stage_event_for(StageId::TopicPublish, &topic);
    let ev_callback = stage_event_for(StageId::TopicCallback, &topic);
    let pub_before = stage_snapshot(StageId::TopicPublish).count;
    let cb_before = stage_snapshot(StageId::TopicCallback).count;

    // Publish until a delivery lands (subscriber routing can lag setup).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut delivered = false;
    while tokio::time::Instant::now() < deadline {
        publisher.publish(vec![1u8, 2, 3])?;
        if rx.recv_timeout(Duration::from_millis(100)).is_ok() {
            delivered = true;
            break;
        }
    }
    assert!(delivered, "subscriber must receive at least one message");
    // Let the always-on callback histogram record on the dispatch thread.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);

    // Histograms recorded (always-on, master-gated)...
    assert!(
        stage_snapshot(StageId::TopicPublish).count > pub_before,
        "topic.publish histogram must record at the default tier"
    );
    assert!(
        stage_snapshot(StageId::TopicCallback).count > cb_before,
        "topic.callback histogram must record at the default tier"
    );
    // ...but NO waterfall spans for this topic (Debug filtered by the Info floor).
    assert!(
        !recs.iter().any(|r| r.event_id == ev_publish),
        "no topic.publish span may be emitted at the default `always`/Info tier"
    );
    assert!(
        !recs.iter().any(|r| r.event_id == ev_callback),
        "no topic.callback span may be emitted at the default `always`/Info tier"
    );
    Ok(())
}

/// With the `aorta.topic` scope enabled (operator opt-in), every message gets a
/// `topic.publish` span on the publisher and a `topic.callback` span on the
/// subscriber, and the callback parents to the publish span across the wire —
/// one cross-process flow arrow per message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_path_full_recording_links_callback_to_publish_when_enabled() -> AortaResult<()> {
    let group = format!("tp_full_{}", std::process::id());
    let pub_node = Node::new("trace_topic_pub2", &group).await?;
    let sub_node = Node::new("trace_topic_sub2", &group).await?;
    let topic = unique_topic("trace_topic_full");

    let (tx, rx) = mpsc::channel::<()>();
    let _sub = sub_node
        .create_subscriber(&topic, move |_data, _ctx| {
            let _ = tx.send(());
        })
        .await?;
    let publisher = pub_node.create_publisher(&topic).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let _trace_guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    // Operator opt-in: enable the dedicated topic scope (admits Debug spans).
    let topic_scope = intern_scope(TOPIC_SCOPE_NAME);
    set_scope_enabled(topic_scope, true);

    let ev_publish = stage_event_for(StageId::TopicPublish, &topic);
    let ev_callback = stage_event_for(StageId::TopicCallback, &topic);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut delivered = false;
    while tokio::time::Instant::now() < deadline {
        publisher.publish(vec![9u8, 9, 9])?;
        if rx.recv_timeout(Duration::from_millis(100)).is_ok() {
            delivered = true;
            break;
        }
    }
    assert!(delivered, "subscriber must receive at least one message");

    // Drain with a bounded retry: the callback span records on the subscriber's
    // dispatch thread, which can land a beat after the client sees the message.
    let mut recs = Vec::new();
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have_pub = recs.iter().any(|r| r.event_id == ev_publish);
        let have_cb = recs.iter().any(|r| r.event_id == ev_callback);
        if (have_pub && have_cb) || tokio::time::Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    let is_begin = |r: &&crate::trace::RingRecord| {
        ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
    };
    let callback_begin = recs
        .iter()
        .filter(is_begin)
        .find(|r| r.event_id == ev_callback)
        .expect("subscriber must emit a topic.callback:<topic> span when enabled");

    // Cross-process linkage over the real wire (the publish
    // attachment is the only channel): the callback span parents to the publish
    // span — one flow arrow pub → callback. Routing lag can make the loop fire
    // several publishes (each a topic.publish span) before the first delivery,
    // so pair the delivered callback to ITS publish by span id rather than
    // taking the first publish: a match also proves the wire
    // traceparent was present (a legacy fresh-root callback has parent 0, which
    // no publish span carries).
    let publish_begin = recs
        .iter()
        .filter(is_begin)
        .find(|r| r.event_id == ev_publish && r.span_id == callback_begin.parent_span_id)
        .expect(
            "the callback's parent must be one of the topic.publish spans \
             (pub → callback transit hop, traceparent on the message attachment)",
        );
    assert_ne!(publish_begin.trace_id, [0u8; 16]);
    assert_eq!(
        callback_begin.trace_id, publish_begin.trace_id,
        "the callback span must ride the publisher's trace"
    );
    Ok(())
}
