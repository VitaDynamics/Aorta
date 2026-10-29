use super::{
    dispatch_event_to_executor_or_local, parse_aorta_header_fields,
    parse_aorta_header_fields_with_layout, parse_attachment_hash,
    parse_attachment_hash_from_zbytes, parse_header_context,
    parse_header_context_from_zbytes_with_layout, sample_into_payload, spawn_local_queue,
    spawn_overflow_relay_queue, spawn_overflow_relay_queue_with_handle, DeliveryMode,
    HeaderLayoutCache, ParsedHeaderContext, ReceiveOverflow, SubscriberOptions,
    SubscriberOverflowPolicy, SubscriberRuntimeStatsInner, SubscriberStrategy,
};
use crate::schema::{AortaHeaderLayout, SchemaHash};
use crate::ZBytes;
use flatbuffers::{FlatBufferBuilder, Table, TableUnfinishedWIPOffset, WIPOffset};
use zenoh::key_expr::KeyExpr;
use zenoh::sample::SampleBuilder;

#[test]
fn parse_attachment_hash_accepts_64_hex() {
    let hex = b"000000000000000000000000000000000000000000000000000000000000002a";
    let hash = parse_attachment_hash(hex).expect("hash");
    assert_eq!(
        hash.to_hex(),
        "000000000000000000000000000000000000000000000000000000000000002a"
    );
}

#[test]
fn parse_attachment_hash_rejects_wrong_len() {
    assert!(parse_attachment_hash(&[1, 2, 3]).is_none());
}

#[test]
fn parse_attachment_hash_rejects_uppercase() {
    let hex = b"00000000000000000000000000000000000000000000000000000000000000AB";
    assert!(parse_attachment_hash(hex).is_none());
}

#[test]
fn parse_attachment_hash_rejects_non_hex() {
    let mut value = b"0000000000000000000000000000000000000000000000000000000000000000".to_vec();
    value[63] = b'g';
    assert!(parse_attachment_hash(&value).is_none());
}

#[test]
fn parse_attachment_hash_from_zbytes_single_slice_accepts_hash() {
    let hex = b"000000000000000000000000000000000000000000000000000000000000002a";
    let attachment = ZBytes::from(hex.to_vec());
    let hash = parse_attachment_hash_from_zbytes(&attachment).expect("hash");
    assert_eq!(
        hash.to_hex(),
        "000000000000000000000000000000000000000000000000000000000000002a"
    );
}

#[test]
fn parse_attachment_hash_from_zbytes_multi_slice_accepts_hash() {
    let hex = b"000000000000000000000000000000000000000000000000000000000000002a";
    let mut writer = ZBytes::writer();
    writer.append(ZBytes::from(hex[..32].to_vec()));
    writer.append(ZBytes::from(hex[32..].to_vec()));
    let attachment = writer.finish();
    let hash = parse_attachment_hash_from_zbytes(&attachment).expect("hash");
    assert_eq!(
        hash.to_hex(),
        "000000000000000000000000000000000000000000000000000000000000002a"
    );
}

#[test]
fn sample_into_payload_moves_owned_payload_without_getter_clone() {
    let key_expr = KeyExpr::try_from("aorta/test/subscriber").expect("key expr");
    let sample: zenoh::sample::Sample = SampleBuilder::put(key_expr, vec![1u8, 2, 3, 4]).into();
    let payload = sample_into_payload(sample);
    assert_eq!(payload.to_bytes().as_ref(), &[1u8, 2, 3, 4]);
}

#[test]
fn subscriber_options_with_header_layout_hint_sets_hint() {
    let layout = AortaHeaderLayout {
        root_aorta_header_field_id: 4,
        header_publisher_node_field_id: 7,
        header_sequence_field_id: 9,
    };
    let options = SubscriberOptions::new().with_header_layout_hint(layout);
    assert_eq!(options.header_layout_hint, Some(layout));
}

#[test]
fn header_layout_cache_try_begin_refresh_is_single_flight_and_throttled() {
    let cache = HeaderLayoutCache::default();
    assert!(cache.try_begin_refresh("topic:/demo/chatter"));
    assert!(
        !cache.try_begin_refresh("topic:/demo/chatter"),
        "same refresh key must be single-flight while in-flight"
    );
    cache.finish_refresh("topic:/demo/chatter");
    assert!(
        !cache.try_begin_refresh("topic:/demo/chatter"),
        "refresh retry should be throttled right after previous attempt"
    );
}

#[test]
fn header_layout_cache_resolve_schema_hit_does_not_request_refresh() {
    let cache = HeaderLayoutCache::default();
    let hash = SchemaHash::from_hex_str(
        "000000000000000000000000000000000000000000000000000000000000002a",
    )
    .expect("hash");
    let layout = AortaHeaderLayout {
        root_aorta_header_field_id: 4,
        header_publisher_node_field_id: 7,
        header_sequence_field_id: 9,
    };
    cache.insert_for_schema(hash, layout);

    let lookup = cache.resolve(Some(hash), "aorta/default/pub/demo/chatter");
    assert_eq!(lookup.layout, Some(layout));
    assert!(
        !lookup.needs_refresh,
        "schema-hash cache hit should not schedule refresh"
    );
}

#[test]
fn header_layout_cache_resolve_unknown_schema_hash_uses_topic_fallback_and_requests_refresh() {
    let cache = HeaderLayoutCache::default();
    let known_hash = SchemaHash::from_hex_str(
        "000000000000000000000000000000000000000000000000000000000000002a",
    )
    .expect("known hash");
    let unknown_hash = SchemaHash::from_hex_str(
        "000000000000000000000000000000000000000000000000000000000000002b",
    )
    .expect("unknown hash");
    let topic = "aorta/default/pub/demo/chatter";
    let layout = AortaHeaderLayout {
        root_aorta_header_field_id: 5,
        header_publisher_node_field_id: 8,
        header_sequence_field_id: 13,
    };
    cache.insert_for_schema(known_hash, layout);
    cache.insert_for_topic(topic, layout);

    let lookup = cache.resolve(Some(unknown_hash), topic);
    assert_eq!(lookup.layout, Some(layout));
    assert!(
        lookup.needs_refresh,
        "unknown schema hash must trigger refresh even with topic fallback"
    );
}

#[test]
fn header_layout_cache_resolve_unknown_schema_hash_without_topic_fallback_requests_refresh() {
    let cache = HeaderLayoutCache::default();
    let hash = SchemaHash::from_hex_str(
        "0000000000000000000000000000000000000000000000000000000000000030",
    )
    .expect("hash");

    let lookup = cache.resolve(Some(hash), "aorta/default/pub/demo/chatter");
    assert!(
        lookup.layout.is_none(),
        "without topic fallback, lookup should have no cached layout"
    );
    assert!(
        lookup.needs_refresh,
        "unknown schema hash should be treated as cache miss"
    );
}

#[test]
fn header_layout_cache_resolve_without_schema_hash_uses_topic_fallback_without_refresh() {
    let cache = HeaderLayoutCache::default();
    let topic = "aorta/default/pub/demo/chatter";
    let layout = AortaHeaderLayout {
        root_aorta_header_field_id: 2,
        header_publisher_node_field_id: 3,
        header_sequence_field_id: 4,
    };
    cache.insert_for_topic(topic, layout);

    let lookup = cache.resolve(None, topic);
    assert_eq!(lookup.layout, Some(layout));
    assert!(
        !lookup.needs_refresh,
        "missing attachment hash should use topic fallback without refresh"
    );
}

struct TestAortaHeader<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for TestAortaHeader<'a> {
    type Inner = TestAortaHeader<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            // SAFETY: FlatBuffer table location is provided by builder output.
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

struct TestAortaHeaderBuilder<'a: 'b, 'b> {
    fbb_: &'b mut FlatBufferBuilder<'a>,
    start_: WIPOffset<TableUnfinishedWIPOffset>,
}

impl<'a: 'b, 'b> TestAortaHeaderBuilder<'a, 'b> {
    fn new(fbb: &'b mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        Self {
            fbb_: fbb,
            start_: start,
        }
    }

    fn add_publisher_node(&mut self, publisher_node: WIPOffset<&'a str>) {
        self.fbb_
            .push_slot_always::<WIPOffset<_>>(6, publisher_node);
    }

    fn add_sequence(&mut self, sequence: u64) {
        self.fbb_.push_slot::<u64>(8, sequence, 0);
    }

    fn finish(self) -> WIPOffset<TestAortaHeader<'a>> {
        let o = self.fbb_.end_table(self.start_);
        WIPOffset::new(o.value())
    }
}

struct TestMessage<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for TestMessage<'a> {
    type Inner = TestMessage<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            // SAFETY: FlatBuffer table location is provided by builder output.
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

struct TestMessageBuilder<'a: 'b, 'b> {
    fbb_: &'b mut FlatBufferBuilder<'a>,
    start_: WIPOffset<TableUnfinishedWIPOffset>,
}

impl<'a: 'b, 'b> TestMessageBuilder<'a, 'b> {
    fn new(fbb: &'b mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        Self {
            fbb_: fbb,
            start_: start,
        }
    }

    fn add_aorta_header(&mut self, header: WIPOffset<TestAortaHeader<'a>>) {
        self.fbb_.push_slot_always::<WIPOffset<_>>(4, header);
    }

    fn add_text(&mut self, text: WIPOffset<&'a str>) {
        self.fbb_.push_slot_always::<WIPOffset<_>>(6, text);
    }

    fn finish(self) -> WIPOffset<TestMessage<'a>> {
        let o = self.fbb_.end_table(self.start_);
        WIPOffset::new(o.value())
    }
}

fn build_payload_with_header(publisher_node: &str, sequence: u64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let node = builder.create_string(publisher_node);
    let text = builder.create_string("hello");

    let mut header_builder = TestAortaHeaderBuilder::new(&mut builder);
    header_builder.add_publisher_node(node);
    header_builder.add_sequence(sequence);
    let header = header_builder.finish();

    let mut msg_builder = TestMessageBuilder::new(&mut builder);
    msg_builder.add_aorta_header(header);
    msg_builder.add_text(text);
    let msg = msg_builder.finish();
    builder.finish(msg, None);

    builder.finished_data().to_vec()
}

struct TestMessageHeaderId4Builder<'a: 'b, 'b> {
    fbb_: &'b mut FlatBufferBuilder<'a>,
    start_: WIPOffset<TableUnfinishedWIPOffset>,
}

impl<'a: 'b, 'b> TestMessageHeaderId4Builder<'a, 'b> {
    fn new(fbb: &'b mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        Self {
            fbb_: fbb,
            start_: start,
        }
    }

    // Field id 4 => VOffset = 4 + 2 * 4 = 12.
    fn add_aorta_header(&mut self, header: WIPOffset<TestAortaHeader<'a>>) {
        self.fbb_.push_slot_always::<WIPOffset<_>>(12, header);
    }

    // Keep text at field id 0 for this synthetic test message.
    fn add_text(&mut self, text: WIPOffset<&'a str>) {
        self.fbb_.push_slot_always::<WIPOffset<_>>(4, text);
    }

    fn finish(self) -> WIPOffset<TestMessage<'a>> {
        let o = self.fbb_.end_table(self.start_);
        WIPOffset::new(o.value())
    }
}

fn build_payload_with_header_id4(publisher_node: &str, sequence: u64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let node = builder.create_string(publisher_node);
    let text = builder.create_string("hello");

    let mut header_builder = TestAortaHeaderBuilder::new(&mut builder);
    header_builder.add_publisher_node(node);
    header_builder.add_sequence(sequence);
    let header = header_builder.finish();

    let mut msg_builder = TestMessageHeaderId4Builder::new(&mut builder);
    msg_builder.add_aorta_header(header);
    msg_builder.add_text(text);
    let msg = msg_builder.finish();
    builder.finish(msg, None);

    builder.finished_data().to_vec()
}

fn build_payload_without_header() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let text = builder.create_string("hello");

    let mut msg_builder = TestMessageBuilder::new(&mut builder);
    msg_builder.add_text(text);
    let msg = msg_builder.finish();
    builder.finish(msg, None);

    builder.finished_data().to_vec()
}

#[test]
fn parse_aorta_header_fields_extracts_node_and_sequence() {
    let payload = build_payload_with_header("node_alpha", 42);
    let (publisher_node, sequence) = parse_aorta_header_fields(&payload);

    assert_eq!(publisher_node.as_deref(), Some("node_alpha"));
    assert_eq!(sequence, Some(42));
}

#[test]
fn parse_aorta_header_fields_returns_none_without_header() {
    let payload = build_payload_without_header();
    let (publisher_node, sequence) = parse_aorta_header_fields(&payload);

    assert!(publisher_node.is_none());
    assert!(sequence.is_none());
}

#[test]
fn parse_aorta_header_fields_handles_invalid_payload() {
    let payload = vec![1, 2, 3, 4, 5];
    let (publisher_node, sequence) = parse_aorta_header_fields(&payload);

    assert!(publisher_node.is_none());
    assert!(sequence.is_none());
}

#[test]
fn parse_aorta_header_fields_with_layout_supports_nonzero_root_header_id() {
    let payload = build_payload_with_header_id4("node_gamma", 123);

    // Legacy fallback assumes root field id=0, so this should not extract header.
    let legacy = parse_aorta_header_fields(&payload);
    assert_eq!(legacy, (None, None));

    let layout = AortaHeaderLayout {
        root_aorta_header_field_id: 4,
        header_publisher_node_field_id: 1,
        header_sequence_field_id: 2,
    };
    let parsed = parse_aorta_header_fields_with_layout(&payload, layout);
    assert_eq!(parsed.0.as_deref(), Some("node_gamma"));
    assert_eq!(parsed.1, Some(123));
}

#[test]
fn parse_header_context_extracts_node_and_sequence() {
    let payload = build_payload_with_header("node_beta", 7);
    let parsed = parse_header_context(&payload);
    assert_eq!(parsed.publisher_node.as_deref(), Some("node_beta"));
    assert_eq!(parsed.sequence, Some(7));
}

#[test]
fn parse_header_context_from_zbytes_single_slice_extracts_node_and_sequence() {
    let payload = build_payload_with_header("node_delta", 88);
    let zbytes = ZBytes::from(payload);
    let parsed =
        parse_header_context_from_zbytes_with_layout(&zbytes, super::DEFAULT_AORTA_HEADER_LAYOUT);
    assert_eq!(parsed.publisher_node.as_deref(), Some("node_delta"));
    assert_eq!(parsed.sequence, Some(88));
}

#[test]
fn parse_header_context_from_zbytes_multi_slice_extracts_node_and_sequence() {
    let payload = build_payload_with_header("node_epsilon", 99);
    let split = payload.len() / 2;
    let mut writer = ZBytes::writer();
    writer.append(ZBytes::from(payload[..split].to_vec()));
    writer.append(ZBytes::from(payload[split..].to_vec()));
    let zbytes = writer.finish();
    let parsed =
        parse_header_context_from_zbytes_with_layout(&zbytes, super::DEFAULT_AORTA_HEADER_LAYOUT);
    assert_eq!(parsed.publisher_node.as_deref(), Some("node_epsilon"));
    assert_eq!(parsed.sequence, Some(99));
}

#[test]
fn subscriber_strategy_dedup_window_drops_duplicate() {
    let options = SubscriberOptions::new()
        .with_dedup(true)
        .with_dedup_window(2);
    let strategy = SubscriberStrategy::new(&options).expect("strategy");

    let parsed = ParsedHeaderContext {
        publisher_node: Some("node_a".to_string()),
        sequence: Some(1),
    };
    let parsed_dup = ParsedHeaderContext {
        publisher_node: Some("node_a".to_string()),
        sequence: Some(1),
    };
    let parsed_new = ParsedHeaderContext {
        publisher_node: Some("node_a".to_string()),
        sequence: Some(2),
    };

    let d1 = strategy.process("/a", &parsed, None);
    let d2 = strategy.process("/a", &parsed_dup, None);
    let d3 = strategy.process("/a", &parsed_new, None);

    assert!(!d1.drop_duplicate);
    assert!(d2.drop_duplicate);
    assert!(!d3.drop_duplicate);
}

#[test]
fn subscriber_strategy_reorder_drop_marks_and_drops() {
    let options = SubscriberOptions::new()
        .with_reorder_check(true)
        .with_drop_out_of_order(true);
    let strategy = SubscriberStrategy::new(&options).expect("strategy");

    let parsed_seq_2 = ParsedHeaderContext {
        publisher_node: Some("node_r".to_string()),
        sequence: Some(2),
    };
    let parsed_seq_1 = ParsedHeaderContext {
        publisher_node: Some("node_r".to_string()),
        sequence: Some(1),
    };
    let parsed_seq_3 = ParsedHeaderContext {
        publisher_node: Some("node_r".to_string()),
        sequence: Some(3),
    };

    let d1 = strategy.process("/r", &parsed_seq_2, None);
    let d2 = strategy.process("/r", &parsed_seq_1, None);
    let d3 = strategy.process("/r", &parsed_seq_3, None);

    assert!(!d1.out_of_order);
    assert!(d2.out_of_order);
    assert!(d2.drop_out_of_order);
    assert!(!d3.out_of_order);
}

#[test]
fn subscriber_strategy_uses_source_identity_when_publisher_node_missing() {
    let options = SubscriberOptions::new()
        .with_dedup(true)
        .with_dedup_window(8);
    let strategy = SubscriberStrategy::new(&options).expect("strategy");
    let parsed = ParsedHeaderContext {
        publisher_node: None,
        sequence: Some(42),
    };

    let first = strategy.process("/source_identity", &parsed, Some("zid_a:1"));
    let duplicate_same_source = strategy.process("/source_identity", &parsed, Some("zid_a:1"));
    let same_sequence_other_source = strategy.process("/source_identity", &parsed, Some("zid_b:1"));

    assert!(!first.drop_duplicate);
    assert!(duplicate_same_source.drop_duplicate);
    assert!(!same_sequence_other_source.drop_duplicate);
}

#[test]
fn subscriber_strategy_skips_dedup_when_source_is_unknown() {
    let options = SubscriberOptions::new()
        .with_dedup(true)
        .with_dedup_window(8);
    let strategy = SubscriberStrategy::new(&options).expect("strategy");
    let parsed = ParsedHeaderContext {
        publisher_node: None,
        sequence: Some(7),
    };

    let first = strategy.process("/unknown_source", &parsed, None);
    let second = strategy.process("/unknown_source", &parsed, None);

    assert!(!first.drop_duplicate);
    assert!(
        !second.drop_duplicate,
        "unknown source should not be collapsed into a shared dedup bucket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dispatch_event_overflow_drop_policy_discards_event() {
    use crate::event_queue::{Event, EventKind, EventQueue};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    let mut executor_queue = EventQueue::new(1);
    executor_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill queue");
    let event_sender_cell = Arc::new(RwLock::new(Some(executor_queue.sender())));
    let local_sender = spawn_local_queue(8).sender;
    let observed = Arc::new(AtomicUsize::new(0));

    let observed_event = Arc::clone(&observed);
    dispatch_event_to_executor_or_local(
        None,
        // No cached snapshot here, so the attachment generation is irrelevant.
        &std::sync::atomic::AtomicU64::new(0),
        &event_sender_cell,
        &local_sender,
        SubscriberOverflowPolicy::Drop,
        None,
        None,
        Event::new(EventKind::SubscriptionData, move || {
            observed_event.fetch_add(1, Ordering::SeqCst);
        }),
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    assert_eq!(
        observed.load(Ordering::SeqCst),
        0,
        "drop policy must not execute callback through local queue"
    );

    let queued = executor_queue
        .pop_timeout(tokio::time::Duration::from_millis(100))
        .await
        .expect("first event");
    queued.run();

    let forwarded = executor_queue
        .pop_timeout(tokio::time::Duration::from_millis(100))
        .await;
    assert!(
        forwarded.is_none(),
        "drop policy should not keep overflow event when executor queue is full"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dispatch_event_overflow_relay_policy_keeps_executor_path() {
    use crate::event_queue::{Event, EventKind, EventQueue};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    let mut executor_queue = EventQueue::new(1);
    executor_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill queue");
    let event_sender_cell = Arc::new(RwLock::new(Some(executor_queue.sender())));
    let local_sender = spawn_local_queue(8).sender;
    let overflow_relay_sender = spawn_overflow_relay_queue(Arc::clone(&event_sender_cell), 8, None);
    let observed = Arc::new(AtomicUsize::new(0));

    let observed_event = Arc::clone(&observed);
    dispatch_event_to_executor_or_local(
        None,
        // No cached snapshot here, so the attachment generation is irrelevant.
        &std::sync::atomic::AtomicU64::new(0),
        &event_sender_cell,
        &local_sender,
        SubscriberOverflowPolicy::Relay,
        Some(&overflow_relay_sender),
        None,
        Event::new(EventKind::SubscriptionData, move || {
            observed_event.fetch_add(1, Ordering::SeqCst);
        }),
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    assert_eq!(
        observed.load(Ordering::SeqCst),
        0,
        "overflow path must not execute callback via local queue when executor sender exists"
    );

    let queued = executor_queue
        .pop_timeout(tokio::time::Duration::from_millis(100))
        .await
        .expect("first event");
    queued.run();

    let queued = executor_queue
        .pop_timeout(tokio::time::Duration::from_millis(100))
        .await
        .expect("forwarded event");
    queued.run();

    assert_eq!(observed.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn overflow_relay_task_exits_after_sender_drop() {
    use std::sync::{Arc, RwLock};

    let event_sender_cell = Arc::new(RwLock::new(None));
    let (relay_sender, relay_task) =
        spawn_overflow_relay_queue_with_handle(Arc::clone(&event_sender_cell), 8, None);

    drop(relay_sender);

    let joined = tokio::time::timeout(tokio::time::Duration::from_millis(200), relay_task)
        .await
        .expect("relay task should exit after all senders are dropped");
    assert!(joined.is_ok(), "relay task should not panic");
}

#[test]
fn subscriber_runtime_stats_snapshot_reads_atomic_counters() {
    let stats = SubscriberRuntimeStatsInner::default();
    stats.on_duplicate_drop();
    stats.on_out_of_order();
    stats.on_out_of_order_drop();
    stats.on_executor_drop();
    stats.on_local_drop();
    stats.on_overflow_relay_enqueued();
    stats.on_overflow_relay_drop();

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.duplicate_drop_count, 1);
    assert_eq!(snapshot.out_of_order_count, 1);
    assert_eq!(snapshot.out_of_order_drop_count, 1);
    assert_eq!(snapshot.executor_drop_count, 1);
    assert_eq!(snapshot.local_drop_count, 1);
    assert_eq!(snapshot.overflow_relay_enqueue_count, 1);
    assert_eq!(snapshot.overflow_relay_drop_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dispatch_event_updates_runtime_stats_for_drop_and_local_fallback() {
    use crate::event_queue::{Event, EventKind, EventQueue};
    use std::sync::{Arc, RwLock};

    let stats = SubscriberRuntimeStatsInner::default();

    let executor_queue = EventQueue::new(1);
    executor_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill queue");
    let event_sender_cell = Arc::new(RwLock::new(Some(executor_queue.sender())));
    let local_sender = spawn_local_queue(8).sender;

    dispatch_event_to_executor_or_local(
        None,
        // No cached snapshot here, so the attachment generation is irrelevant.
        &std::sync::atomic::AtomicU64::new(0),
        &event_sender_cell,
        &local_sender,
        SubscriberOverflowPolicy::Drop,
        None,
        Some(&stats),
        Event::new(EventKind::SubscriptionData, || {}),
    );

    assert_eq!(stats.snapshot().executor_drop_count, 1);

    *event_sender_cell.write().expect("write lock") = None;
    let closed_local_sender = {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        crate::event_queue::EventQueueSender::from_bounded_channel(tx)
    };

    dispatch_event_to_executor_or_local(
        None,
        // No cached snapshot here, so the attachment generation is irrelevant.
        &std::sync::atomic::AtomicU64::new(0),
        &event_sender_cell,
        &closed_local_sender,
        SubscriberOverflowPolicy::Drop,
        None,
        Some(&stats),
        Event::new(EventKind::SubscriptionData, || {}),
    );

    assert_eq!(stats.snapshot().local_drop_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn overflow_relay_updates_runtime_stats_for_enqueue_and_drop() {
    use crate::event_queue::{Event, EventKind, EventQueue};
    use std::sync::{Arc, RwLock};

    let runtime_stats = Arc::new(SubscriberRuntimeStatsInner::default());
    let executor_queue = EventQueue::new(1);
    executor_queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("fill queue");
    let event_sender_cell = Arc::new(RwLock::new(Some(executor_queue.sender())));
    let overflow_relay_sender = spawn_overflow_relay_queue(
        Arc::clone(&event_sender_cell),
        1,
        Some(Arc::clone(&runtime_stats)),
    );

    let send_result = super::try_dispatch_to_executor_sender(
        &executor_queue.sender(),
        SubscriberOverflowPolicy::Relay,
        Some(&overflow_relay_sender),
        Some(runtime_stats.as_ref()),
        Event::new(EventKind::SubscriptionData, || {}),
    );
    assert!(send_result.is_ok());

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    assert_eq!(runtime_stats.snapshot().overflow_relay_enqueue_count, 1);

    let event_sender_cell_without_executor = Arc::new(RwLock::new(None));
    let overflow_drop_sender = spawn_overflow_relay_queue(
        Arc::clone(&event_sender_cell_without_executor),
        1,
        Some(Arc::clone(&runtime_stats)),
    );
    let _ = overflow_drop_sender.try_send(Event::new(EventKind::SubscriptionData, || {}));
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    assert_eq!(runtime_stats.snapshot().overflow_relay_drop_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_message_context_integration() {
    use crate::node::Node;
    use crate::qos::Priority;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    // Create two nodes
    let pub_node = Node::new("test_pub", "test_group").await.unwrap();
    let sub_node = Node::new("test_sub", "test_group").await.unwrap();

    let topic = "/test/message_context";

    // Shared state to capture MessageContext
    let received_context = Arc::new(Mutex::new(None));
    let received_context_clone = Arc::clone(&received_context);

    // Create subscriber with context fields enabled (no longer the default)
    let options = SubscriberOptions::new()
        .with_topic_in_context(true)
        .with_receive_timestamp(true)
        .with_priority_in_context(true)
        .with_publisher_timestamp(true);
    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |payload, context| {
                println!("Received: {} bytes", payload.len());
                println!("  Topic: {}", context.topic);
                println!("  Priority: {:?}", context.priority);
                println!("  Receive timestamp: {}", context.receive_stamp_ns);
                println!("  Publisher timestamp: {:?}", context.publisher_stamp_ns);

                // Store context for verification
                *received_context_clone.lock().unwrap() = Some(context.clone());
            },
            options,
        )
        .await
        .unwrap();

    // Give subscriber time to declare
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Create publisher and publish message
    let pub_handle = pub_node.create_publisher(topic).await.unwrap();
    let test_data = b"Hello MessageContext!";
    pub_handle.publish(test_data.to_vec()).unwrap();

    // Wait for message delivery
    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if received_context.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(wait_result.is_ok(), "MessageContext should be received");

    // Verify MessageContext was received
    let context = received_context.lock().unwrap();
    assert!(context.is_some(), "MessageContext should be received");

    if let Some(ctx) = context.as_ref() {
        // Verify topic matches
        assert!(ctx.topic.contains(topic), "Topic should match");

        // Verify receive timestamp is set
        assert!(ctx.receive_stamp_ns > 0, "Receive timestamp should be set");

        // Verify priority is set (default is Normal)
        assert_eq!(
            ctx.priority,
            Priority::Normal,
            "Default priority should be Normal"
        );

        // Publisher timestamp might be available (depends on Zenoh timestamp)
        println!(
            "Publisher timestamp present: {}",
            ctx.publisher_stamp_ns.is_some()
        );

        // Raw bytes publish path has no embedded AortaHeader.
        assert!(
            ctx.publisher_node.is_none(),
            "Raw publish should not include publisher node"
        );
        assert!(
            ctx.sequence.is_none(),
            "Raw publish should not include sequence"
        );
    }

    // Cleanup
    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_message_context_does_not_parse_header_by_default() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_ctx_default", "test_group")
        .await
        .unwrap();
    let sub_node = Node::new("test_sub_ctx_default", "test_group")
        .await
        .unwrap();
    let topic = "/test/message_context/default_no_header_parse";

    let received_context = Arc::new(Mutex::new(None));
    let received_context_clone = Arc::clone(&received_context);

    let _sub = sub_node
        .create_subscriber(topic, move |_payload, context| {
            *received_context_clone.lock().unwrap() = Some(context.clone());
        })
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();
    let payload_with_header = build_payload_with_header("embedded_header_node", 777);
    pub_handle.publish(payload_with_header).unwrap();

    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if received_context.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(wait_result.is_ok(), "MessageContext should be received");

    let context = received_context.lock().unwrap();
    let ctx = context.as_ref().expect("context");
    assert!(
        ctx.publisher_node.is_none(),
        "Core default path should not parse publisher_node from payload"
    );
    assert!(
        ctx.sequence.is_none(),
        "Core default path should not parse sequence from payload"
    );

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_message_context_parses_header_when_enabled() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_ctx_enabled", "test_group")
        .await
        .unwrap();
    let sub_node = Node::new("test_sub_ctx_enabled", "test_group")
        .await
        .unwrap();
    let topic = "/test/message_context/header_parse_enabled";

    let received_context = Arc::new(Mutex::new(None));
    let received_context_clone = Arc::clone(&received_context);

    let options = SubscriberOptions::new().with_header_context(true);
    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |_payload, context| {
                *received_context_clone.lock().unwrap() = Some(context.clone());
            },
            options,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();
    let payload_with_header = build_payload_with_header("embedded_header_node", 777);
    pub_handle.publish(payload_with_header).unwrap();

    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if received_context.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(wait_result.is_ok(), "MessageContext should be received");

    let context = received_context.lock().unwrap();
    let ctx = context.as_ref().expect("context");
    assert_eq!(
        ctx.publisher_node.as_deref(),
        Some("embedded_header_node"),
        "Header context should be parsed when enabled"
    );
    assert_eq!(
        ctx.sequence,
        Some(777),
        "Header sequence should be parsed when enabled"
    );

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_message_context_dedup_drops_duplicate_sequence_when_enabled() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_ctx_dedup", "test_group").await.unwrap();
    let sub_node = Node::new("test_sub_ctx_dedup", "test_group").await.unwrap();
    let topic = "/test/message_context/dedup";

    let received_sequences = Arc::new(Mutex::new(Vec::<u64>::new()));
    let received_sequences_clone = Arc::clone(&received_sequences);

    let options = SubscriberOptions::new()
        .with_header_context(true)
        .with_dedup(true)
        .with_dedup_window(16);
    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |_payload, context| {
                if let Some(seq) = context.sequence {
                    received_sequences_clone.lock().unwrap().push(seq);
                }
            },
            options,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();
    let payload_seq_10 = build_payload_with_header("dedup_node", 10);
    let payload_seq_10_dup = build_payload_with_header("dedup_node", 10);
    let payload_seq_11 = build_payload_with_header("dedup_node", 11);
    pub_handle.publish(payload_seq_10).unwrap();
    pub_handle.publish(payload_seq_10_dup).unwrap();
    pub_handle.publish(payload_seq_11).unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
    let got = received_sequences.lock().unwrap().clone();
    assert_eq!(got, vec![10, 11], "Duplicate sequence should be dropped");

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dedup_can_work_without_exposing_header_context() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_ctx_dedup_hidden", "test_group")
        .await
        .unwrap();
    let sub_node = Node::new("test_sub_ctx_dedup_hidden", "test_group")
        .await
        .unwrap();
    let topic = "/test/message_context/dedup_hidden_ctx";

    let received_sequence_ctx = Arc::new(Mutex::new(Vec::<Option<u64>>::new()));
    let received_sequence_ctx_clone = Arc::clone(&received_sequence_ctx);
    let handshake_seen = Arc::new(AtomicBool::new(false));
    let handshake_seen_clone = Arc::clone(&handshake_seen);

    let options = SubscriberOptions::new()
        .with_dedup(true)
        .with_dedup_window(16)
        .with_header_context(false);
    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |payload, context| {
                let bytes = payload.to_bytes();
                if bytes.as_ref() == b"warmup" {
                    handshake_seen_clone.store(true, Ordering::Relaxed);
                    return;
                }
                received_sequence_ctx_clone
                    .lock()
                    .unwrap()
                    .push(context.sequence);
                assert!(
                    context.publisher_node.is_none(),
                    "Header context should remain hidden when not enabled"
                );
            },
            options,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();

    let handshake_ready = tokio::time::timeout(tokio::time::Duration::from_secs(3), async {
        loop {
            pub_handle.publish(b"warmup".to_vec()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            if handshake_seen.load(Ordering::Relaxed) {
                break;
            }
        }
    })
    .await;
    assert!(
        handshake_ready.is_ok(),
        "Subscriber should receive warmup message before dedup hidden context test"
    );

    pub_handle
        .publish(build_payload_with_header("dedup_hidden_node", 20))
        .unwrap();
    pub_handle
        .publish(build_payload_with_header("dedup_hidden_node", 20))
        .unwrap();
    pub_handle
        .publish(build_payload_with_header("dedup_hidden_node", 21))
        .unwrap();

    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if received_sequence_ctx.lock().unwrap().as_slice() == [None, None] {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        wait_result.is_ok(),
        "Timed out waiting for dedup hidden context handling"
    );
    let got = received_sequence_ctx.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![None, None],
        "Dedup should still work while sequence remains hidden in context"
    );

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_message_context_reorder_drop_out_of_order_when_enabled() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_ctx_reorder", "test_group")
        .await
        .unwrap();
    let sub_node = Node::new("test_sub_ctx_reorder", "test_group")
        .await
        .unwrap();
    let topic = "/test/message_context/reorder";

    let received_sequences = Arc::new(Mutex::new(Vec::<u64>::new()));
    let received_sequences_clone = Arc::clone(&received_sequences);
    let handshake_seen = Arc::new(AtomicBool::new(false));
    let handshake_seen_clone = Arc::clone(&handshake_seen);

    let options = SubscriberOptions::new()
        .with_header_context(true)
        .with_reorder_check(true)
        .with_drop_out_of_order(true);
    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |payload, context| {
                let bytes = payload.to_bytes();
                if bytes.as_ref() == b"warmup" {
                    handshake_seen_clone.store(true, Ordering::Relaxed);
                    return;
                }
                if let Some(seq) = context.sequence {
                    received_sequences_clone.lock().unwrap().push(seq);
                }
            },
            options,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();

    let handshake_ready = tokio::time::timeout(tokio::time::Duration::from_secs(3), async {
        loop {
            pub_handle.publish(b"warmup".to_vec()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            if handshake_seen.load(Ordering::Relaxed) {
                break;
            }
        }
    })
    .await;
    assert!(
        handshake_ready.is_ok(),
        "Subscriber should receive warmup message before reorder test"
    );

    let payload_seq_2 = build_payload_with_header("reorder_node", 2);
    let payload_seq_1 = build_payload_with_header("reorder_node", 1);
    let payload_seq_3 = build_payload_with_header("reorder_node", 3);
    pub_handle.publish(payload_seq_2).unwrap();
    pub_handle.publish(payload_seq_1).unwrap();
    pub_handle.publish(payload_seq_3).unwrap();

    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if received_sequences.lock().unwrap().as_slice() == [2, 3] {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        wait_result.is_ok(),
        "Timed out waiting for out-of-order sequence handling"
    );
    let got = received_sequences.lock().unwrap().clone();
    assert_eq!(got, vec![2, 3], "Out-of-order sequence should be dropped");

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[test]
fn test_subscriber_options_default() {
    let options = SubscriberOptions::default();
    assert_eq!(options.history_depth, 1024);
    assert!(options.prefetch_schema_on_init);
    assert!(!options.enable_header_context);
    assert!(!options.include_topic_in_context);
    assert!(!options.include_receive_timestamp);
    assert!(!options.include_priority_in_context);
    assert!(!options.include_publisher_timestamp);
    assert_eq!(options.qos.priority, crate::qos::Priority::Normal);
    assert!(!options.enable_dedup);
    assert_eq!(options.dedup_window, 1024);
    assert!(!options.enable_reorder_check);
    assert!(!options.drop_out_of_order);
    assert_eq!(options.overflow_policy, SubscriberOverflowPolicy::Drop);
    assert_eq!(options.overflow_relay_capacity, 1024);
}

#[test]
fn test_subscriber_options_schema_prefetch_builder() {
    let options = SubscriberOptions::new().with_schema_prefetch_on_init(true);
    assert!(options.prefetch_schema_on_init);
    assert!(!options.enable_header_context);
    assert!(!options.include_topic_in_context);
    assert!(!options.include_receive_timestamp);
    assert!(!options.include_priority_in_context);
    assert!(!options.include_publisher_timestamp);
    assert_eq!(options.qos.priority, crate::qos::Priority::Normal);
    assert!(!options.enable_dedup);
    assert_eq!(options.dedup_window, 1024);
    assert!(!options.enable_reorder_check);
    assert!(!options.drop_out_of_order);
    assert_eq!(options.overflow_policy, SubscriberOverflowPolicy::Drop);
    assert_eq!(options.overflow_relay_capacity, 1024);
}

#[test]
fn test_subscriber_options_builder() {
    let options = SubscriberOptions::new()
        .with_history_depth(2048)
        .with_header_context(true)
        .with_topic_in_context(false)
        .with_receive_timestamp(false)
        .with_priority_in_context(false)
        .with_publisher_timestamp(false)
        .with_priority(crate::qos::Priority::High)
        .with_dedup(true)
        .with_dedup_window(8)
        .with_reorder_check(true)
        .with_drop_out_of_order(true)
        .with_overflow_policy(SubscriberOverflowPolicy::Relay)
        .with_overflow_relay_capacity(32);

    assert_eq!(options.history_depth, 2048);
    assert!(options.prefetch_schema_on_init);
    assert!(options.enable_header_context);
    assert!(!options.include_topic_in_context);
    assert!(!options.include_receive_timestamp);
    assert!(!options.include_priority_in_context);
    assert!(!options.include_publisher_timestamp);
    assert_eq!(options.qos.priority, crate::qos::Priority::High);
    assert!(options.enable_dedup);
    assert_eq!(options.dedup_window, 8);
    assert!(options.enable_reorder_check);
    assert!(options.drop_out_of_order);
    assert_eq!(options.overflow_policy, SubscriberOverflowPolicy::Relay);
    assert_eq!(options.overflow_relay_capacity, 32);
}

#[test]
fn test_subscriber_options_with_qos_sets_priority() {
    let options = SubscriberOptions::new().with_qos(crate::qos::QoS::bulk_transfer());
    assert_eq!(options.qos.priority, crate::qos::Priority::Background);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_subscriber_with_options() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    // Create nodes
    let pub_node = Node::new("test_pub_opts", "test_group").await.unwrap();
    let sub_node = Node::new("test_sub_opts", "test_group").await.unwrap();

    let topic = "/test/subscriber_options";

    // Track received messages
    let received_count = Arc::new(Mutex::new(0u32));
    let received_count_clone = Arc::clone(&received_count);
    let handshake_seen = Arc::new(AtomicBool::new(false));
    let handshake_seen_clone = Arc::clone(&handshake_seen);

    // Create subscriber with custom options
    let options = SubscriberOptions::new().with_history_depth(512);

    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |payload, _context| {
                handshake_seen_clone.store(true, Ordering::Relaxed);
                let bytes = payload.to_bytes();
                if bytes.as_ref().starts_with(b"Message ") {
                    *received_count_clone.lock().unwrap() += 1;
                }
            },
            options,
        )
        .await
        .unwrap();

    // Give subscriber time to declare
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Create publisher and publish messages
    let pub_handle = pub_node.create_publisher(topic).await.unwrap();

    let handshake_ready = tokio::time::timeout(tokio::time::Duration::from_secs(3), async {
        loop {
            pub_handle.publish(b"warmup".to_vec()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            if handshake_seen.load(Ordering::Relaxed) {
                break;
            }
        }
    })
    .await;
    assert!(
        handshake_ready.is_ok(),
        "Subscriber should receive warmup message before options burst test"
    );

    for i in 0..5 {
        pub_handle
            .publish(format!("Message {}", i).as_bytes().to_vec())
            .unwrap();
    }

    // Wait for messages
    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        loop {
            if *received_count.lock().unwrap() >= 5 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(wait_result.is_ok(), "Should receive all 5 messages");

    // Verify messages received
    let count = *received_count.lock().unwrap();
    assert_eq!(count, 5, "Should receive all 5 messages");

    // Cleanup
    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

// `with_history_depth(0)` historically meant "unbounded" but was changed
// to silently clamp to the default 1024 — see
// `SubscriberOptions::history_depth` doc for the OOM rationale. This test
// confirms that passing 0 still delivers all messages under low load
// (which now exercises the clamped 1024 path, not a true unbounded queue).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_subscriber_history_depth_zero_falls_back_to_default() {
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let pub_node = Node::new("test_pub_unbounded", "test_group").await.unwrap();
    let sub_node = Node::new("test_sub_unbounded", "test_group").await.unwrap();

    let topic = "/test/unbounded_queue";

    let received_count = Arc::new(Mutex::new(0u32));
    let received_count_clone = Arc::clone(&received_count);
    let handshake_seen = Arc::new(AtomicBool::new(false));
    let handshake_seen_clone = Arc::clone(&handshake_seen);

    let options = SubscriberOptions::new().with_history_depth(0);

    let _sub = sub_node
        .create_subscriber_with_options(
            topic,
            move |payload, _context| {
                handshake_seen_clone.store(true, Ordering::Relaxed);
                let bytes = payload.to_bytes();
                if bytes.as_ref().starts_with(b"Message ") {
                    *received_count_clone.lock().unwrap() += 1;
                }
            },
            options,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let pub_handle = pub_node.create_publisher(topic).await.unwrap();

    // Warmup handshake: make sure discovery/subscription attachment is ready
    // before burst verification, so this test focuses on the
    // history_depth=0 → 1024 clamped-fallback queue behavior.
    let handshake_ready = tokio::time::timeout(tokio::time::Duration::from_secs(3), async {
        loop {
            pub_handle.publish(b"warmup".to_vec()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            if handshake_seen.load(Ordering::Relaxed) {
                break;
            }
        }
    })
    .await;
    assert!(
        handshake_ready.is_ok(),
        "Subscriber should receive warmup message before burst test"
    );

    for i in 0..100 {
        pub_handle
            .publish(format!("Message {}", i).as_bytes().to_vec())
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
    }

    let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(15), async {
        loop {
            if *received_count.lock().unwrap() >= 100 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    let final_count = *received_count.lock().unwrap();
    assert!(
        wait_result.is_ok(),
        "history_depth=0 → 1024 fallback should deliver all 100 paced messages (got {})",
        final_count
    );

    let count = final_count;
    assert_eq!(
        count, 100,
        "history_depth=0 → 1024 fallback should deliver all 100 paced messages"
    );

    pub_node.close().await.unwrap();
    sub_node.close().await.unwrap();
    drop(_env_guard);
}

#[test]
fn shared_ctx_bundles_arc_fields_into_single_allocation() {
    use super::SubscriberSharedCtx;
    use std::sync::Arc;

    let callback = |_payload: &zenoh::bytes::ZBytes, _ctx: &super::MessageContext| {};
    let callback_topic: Arc<str> = Arc::from("aorta/default/topic/test/shared");
    let empty_context_topic: Arc<str> = Arc::from("");

    let shared = Arc::new(SubscriberSharedCtx {
        callback,
        topic_callback_event: 0,
        callback_topic: Arc::clone(&callback_topic),
        empty_context_topic: Arc::clone(&empty_context_topic),
        static_context_template: None,
        strategy: None,
        runtime_stats: Some(Arc::new(SubscriberRuntimeStatsInner::default())),
        header_layout_cache: Arc::new(super::HeaderLayoutCache::default()),
        schema_registry: None,
        key_builder: None,
        runtime_handle: None,
    });

    // Verify single Arc::clone gives access to all fields.
    let cloned = Arc::clone(&shared);
    assert_eq!(
        cloned.callback_topic.as_ref(),
        "aorta/default/topic/test/shared"
    );
    assert_eq!(cloned.empty_context_topic.as_ref(), "");
    assert!(cloned.static_context_template.is_none());
    assert!(cloned.strategy.is_none());

    // Confirm inner Arc strong counts: callback_topic should have exactly 2 refs
    // (original + inside shared). Before this change, the outer closure would
    // have cloned callback_topic independently on every received message.
    assert_eq!(Arc::strong_count(&callback_topic), 2); // original + inside shared
    assert_eq!(Arc::strong_count(&empty_context_topic), 2); // original + inside shared
    drop(cloned);
    drop(shared);
    // After dropping shared context, only original references remain.
    assert_eq!(Arc::strong_count(&callback_topic), 1);
    assert_eq!(Arc::strong_count(&empty_context_topic), 1);
}

#[test]
fn default_options_enable_static_context_template_fast_path() {
    // With all four context fields defaulting to false and no header/strategy flags,
    // the static_context_template should be Some — enabling the zero-overhead fast path
    // that skips per-message MessageContext construction entirely.
    let options = SubscriberOptions::default();

    // Replicate the gate logic from Subscriber::new_with_options
    let parse_header_for_strategy =
        options.enable_header_context || options.enable_dedup || options.enable_reorder_check;
    let should_have_template = !parse_header_for_strategy
        && !options.include_topic_in_context
        && !options.include_receive_timestamp
        && !options.include_priority_in_context
        && !options.include_publisher_timestamp;

    assert!(
        should_have_template,
        "Default SubscriberOptions should enable static_context_template fast path"
    );
}

#[test]
fn enabling_any_context_field_disables_static_context_template() {
    // Each individual flag should disable the fast path when enabled.
    let combos: Vec<(&str, SubscriberOptions)> = vec![
        (
            "include_topic_in_context",
            SubscriberOptions::new().with_topic_in_context(true),
        ),
        (
            "include_receive_timestamp",
            SubscriberOptions::new().with_receive_timestamp(true),
        ),
        (
            "include_priority_in_context",
            SubscriberOptions::new().with_priority_in_context(true),
        ),
        (
            "include_publisher_timestamp",
            SubscriberOptions::new().with_publisher_timestamp(true),
        ),
        (
            "enable_header_context",
            SubscriberOptions::new().with_header_context(true),
        ),
        ("enable_dedup", SubscriberOptions::new().with_dedup(true)),
        (
            "enable_reorder_check",
            SubscriberOptions::new().with_reorder_check(true),
        ),
    ];

    for (label, opts) in combos {
        let parse_header_for_strategy =
            opts.enable_header_context || opts.enable_dedup || opts.enable_reorder_check;
        let should_have_template = !parse_header_for_strategy
            && !opts.include_topic_in_context
            && !opts.include_receive_timestamp
            && !opts.include_priority_in_context
            && !opts.include_publisher_timestamp;

        assert!(
            !should_have_template,
            "Enabling {label} should disable static_context_template fast path"
        );
    }
}

// ---- QoS & buffering redesign: SubscriberOptions defaults, builders, validation ----

#[test]
fn buffering_defaults_are_backward_compatible() {
    let opts = SubscriberOptions::default();
    assert_eq!(
        opts.receive_depth, 0,
        "default gate is disabled (today's behavior)"
    );
    assert_eq!(opts.delivery, DeliveryMode::Executor);
    assert_eq!(opts.receive_overflow, ReceiveOverflow::DropNewest);
}

#[test]
fn with_receive_depth_sets_the_cap() {
    let opts = SubscriberOptions::new().with_receive_depth(32);
    assert_eq!(opts.receive_depth, 32);
    assert_eq!(opts.delivery, DeliveryMode::Executor);
}

#[test]
fn with_native_ring_is_pull_drop_oldest_sugar() {
    let opts = SubscriberOptions::new().with_native_ring(8);
    assert_eq!(opts.delivery, DeliveryMode::InlinePull);
    assert_eq!(opts.receive_overflow, ReceiveOverflow::DropOldest);
    assert_eq!(opts.receive_depth, 8);
}

#[test]
fn validate_executor_with_receive_depth_ok() {
    let opts = SubscriberOptions::new().with_receive_depth(16);
    assert!(opts.validate_delivery(true).is_ok());
}

#[test]
fn validate_drop_oldest_requires_inline_pull() {
    let opts = SubscriberOptions::new()
        .with_receive_depth(16)
        .with_receive_overflow(ReceiveOverflow::DropOldest);
    let err = opts.validate_delivery(true).unwrap_err();
    assert!(err.to_string().contains("DropOldest"), "got: {err}");
}

#[test]
fn validate_inline_pull_rejects_callback() {
    let opts = SubscriberOptions::new().with_native_ring(8);
    let err = opts
        .validate_delivery(/* has_callback = */ true)
        .unwrap_err();
    assert!(err.to_string().contains("InlinePull"), "got: {err}");
}

#[test]
fn validate_inline_pull_requires_nonzero_depth() {
    let opts = SubscriberOptions::new().with_delivery(DeliveryMode::InlinePull);
    let err = opts.validate_delivery(false).unwrap_err();
    assert!(err.to_string().contains("receive_depth"), "got: {err}");
}

#[test]
fn validate_inline_pull_ok_with_depth_no_callback() {
    let opts = SubscriberOptions::new().with_native_ring(4);
    assert!(opts.validate_delivery(false).is_ok());
}

#[test]
fn validate_inline_pull_rejects_header_strategy_options() {
    // Header parse / dedup / reorder are executor-only; the pull
    // path doesn't honor them, so accepting them would silently drop metadata.
    for opts in [
        SubscriberOptions::new()
            .with_native_ring(4)
            .with_header_context(true),
        SubscriberOptions::new()
            .with_native_ring(4)
            .with_dedup(true),
        SubscriberOptions::new()
            .with_native_ring(4)
            .with_reorder_check(true),
    ] {
        let err = opts.validate_delivery(false).unwrap_err();
        assert!(err.to_string().contains("InlinePull"), "got: {err}");
    }
}

#[test]
fn validate_inline_pull_rejects_relay_overflow() {
    // The overflow relay queue is executor-only; the native pull ring
    // never creates/drains it, so accepting overflow_policy=Relay would silently
    // ignore the caller's backpressure intent. Reject it (fail loud), matching
    // the documented contract.
    let opts = SubscriberOptions::new()
        .with_native_ring(4)
        .with_overflow_policy(SubscriberOverflowPolicy::Relay);
    let err = opts.validate_delivery(false).unwrap_err();
    assert!(err.to_string().contains("Relay"), "got: {err}");
    // The default (Drop) overflow policy stays valid on the pull path.
    let ok = SubscriberOptions::new()
        .with_native_ring(4)
        .with_overflow_policy(SubscriberOverflowPolicy::Drop);
    assert!(ok.validate_delivery(false).is_ok());
}

#[test]
fn validate_inline_pull_ok_with_include_context_flags() {
    // The include_* context flags (topic/timestamps/priority) ARE supported.
    let opts = SubscriberOptions::new()
        .with_native_ring(4)
        .with_topic_in_context(true)
        .with_receive_timestamp(true)
        .with_priority_in_context(true)
        .with_publisher_timestamp(true);
    assert!(opts.validate_delivery(false).is_ok());
}

#[test]
fn validate_inline_push_is_deferred() {
    let opts = SubscriberOptions::new().with_delivery(DeliveryMode::InlinePush);
    let err = opts.validate_delivery(false).unwrap_err();
    assert!(err.to_string().contains("InlinePush"), "got: {err}");
}

#[test]
fn map_event_priority_pins_the_qos_chain() {
    // The QoS→EventPriority half of the QoS → lane-class chain
    // (the class half is pinned FFI-side by qos_class_pin).
    use crate::event_queue::EventPriority;
    use crate::qos::Priority;
    assert_eq!(
        crate::subscriber::map_event_priority(Priority::Realtime),
        EventPriority::Realtime
    );
    assert_eq!(
        crate::subscriber::map_event_priority(Priority::High),
        EventPriority::High
    );
    assert_eq!(
        crate::subscriber::map_event_priority(Priority::Normal),
        EventPriority::Normal
    );
    assert_eq!(
        crate::subscriber::map_event_priority(Priority::Low),
        EventPriority::Low
    );
    assert_eq!(
        crate::subscriber::map_event_priority(Priority::Background),
        EventPriority::Background
    );
}

// ---- Regression tests: local-queue drain task shutdown ----
//
// Root cause (measured): `spawn_local_queue` moves the whole `EventQueue`
// into the drain task, and `EventQueue` self-holds one `EventQueueSender`
// (`core/src/event_queue.rs`), so `receiver.recv()` never returns None and
// the task parked forever. `Subscriber::undeclare()` then burned its full
// 2s timeout on every call (and C++ subscriber destructors paid it
// synchronously); implicit `Subscriber` drop leaked the task entirely.
// Fix: the task now terminates via an explicit oneshot shutdown signal
// whose sender is held by the `Subscriber` (drop = trigger).

// T1 (core regression): dropping the shutdown sender must terminate the
// drain task within 100ms. Pre-fix this was impossible — the task never
// exits (the probe above failed at the 100ms timeout).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn local_queue_drain_task_exits_promptly_after_shutdown() {
    let local_queue = spawn_local_queue(8);
    let (_sender, handle, shutdown_tx, _stop, _liveness) = (
        local_queue.sender,
        local_queue.task,
        local_queue.shutdown,
        local_queue.stop,
        local_queue.liveness,
    );
    let started = tokio::time::Instant::now();
    drop(shutdown_tx);
    let joined = tokio::time::timeout(tokio::time::Duration::from_millis(100), handle).await;
    let elapsed = started.elapsed();
    assert!(
        joined.is_ok(),
        "drain task did not exit within 100ms of the shutdown signal \
         (elapsed {:?})",
        elapsed
    );
    eprintln!("T1 drain-task shutdown latency: {:?}", elapsed);
}

// T2a (no event loss on shutdown — deterministic red/green): the drain
// logic (extracted as `drain_local_queue`) must run every queued event.
// Red/green check: emptying `drain_local_queue`'s body (deleting the drain
// loop) makes this test fail.
//
// Scope: helper behavior in isolation. The stop-flag admission checks on
// both dispatch paths are pinned by the stop-flag admission tests
// below. The shutdown branch's CALL to `drain_local_queue` has NO
// automated coverage: with the entry yield removed, `biased` guarantees
// the pop branch wins whenever the queue is non-empty, so the shutdown
// branch is only entered in a poll where the queue was empty — the drain
// can only ever pick up events pushed by another thread within that same
// poll, after pop() returned Pending and before try_pop (a sub-microsecond
// window, not deterministically constructible without test hooks). This
// is accepted; the call is guarded by code review plus
// test_subscriber_undeclare_timeout_stops_backlogged_callback (measured:
// deleting the call in the select! leaves all local_queue* 6 tests and
// test_subscriber_* 4 tests green).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn local_queue_drain_runs_all_queued_events() {
    use crate::event_queue::{Event, EventKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const N: usize = 16;
    let (sender, mut queue) = crate::event_queue::EventQueue::new(N).into_parts();
    let executed = Arc::new(AtomicUsize::new(0));
    for _ in 0..N {
        let executed = Arc::clone(&executed);
        sender
            .try_send(Event::new(EventKind::Timer, move || {
                executed.fetch_add(1, Ordering::SeqCst);
            }))
            .expect("queue has capacity");
    }
    super::drain_local_queue(&mut queue, &std::sync::atomic::AtomicBool::new(false)).await;
    assert_eq!(
        executed.load(Ordering::SeqCst),
        N,
        "drain_local_queue must run every queued event exactly once"
    );
}

// T2b (no event loss on shutdown — end-to-end contract): events that are
// already queued when the shutdown signal is dropped must all still be
// executed. First event's callback blocks synchronously, so the remaining
// N-1 events are queued (and the signal dropped) while the drain task is
// mid-callback — the biased pop branch then consumes them before the
// shutdown branch breaks. This pins the "close must not drop queued
// events" contract (it stays green even without the drain loop, because
// the pop branch covers this case; the drain loop covers events arriving
// during the shutdown branch, see T2a).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_queue_shutdown_preserves_queued_events() {
    use crate::event_queue::{Event, EventKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const N: usize = 16;
    let local_queue = spawn_local_queue(N);
    let (sender, handle, shutdown_tx, _stop, _liveness) = (
        local_queue.sender,
        local_queue.task,
        local_queue.shutdown,
        local_queue.stop,
        local_queue.liveness,
    );
    let executed = Arc::new(AtomicUsize::new(0));

    let executed_first = Arc::clone(&executed);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            executed_first.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("queue has capacity");
    for _ in 1..N {
        let executed = Arc::clone(&executed);
        sender
            .try_send(Event::new(EventKind::Timer, move || {
                executed.fetch_add(1, Ordering::SeqCst);
            }))
            .expect("queue has capacity");
    }
    // Drop the signal while the drain task is still blocked inside the first
    // callback: the other N-1 events are "in flight" at close time.
    drop(shutdown_tx);

    let joined = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "drain task did not exit after shutdown signal"
    );
    assert_eq!(
        executed.load(Ordering::SeqCst),
        N,
        "all events queued before shutdown must be executed (drain contract)"
    );
}

// ---- Cooperative stop flag ----

// Regression (deterministic repro): a callback blocked synchronously
// (E1) followed by a queued backlog (E2). abort() alone cannot preempt the
// synchronous dispatch loop — it only lands at await points, and a Ready
// future's `.await` does not yield to the runtime — so after E1 is released
// the loop goes straight back into `queue.pop()`, E2 is immediately Ready,
// and biased runs it. The dispatch loop must therefore check a cooperative
// stop flag after every callback: once the subscriber is force-stopped
// (undeclare's drain timeout: set the flag, then abort), no NEW backlogged
// callback may start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_queue_force_stop_prevents_backlogged_callback_after_blocked_callback() {
    use crate::event_queue::{Event, EventKind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let local_queue = spawn_local_queue(8);
    let (sender, handle, shutdown_tx, stop, _liveness) = (
        local_queue.sender,
        local_queue.task,
        local_queue.shutdown,
        local_queue.stop,
        local_queue.liveness,
    );
    let latch = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicUsize::new(0));

    // E1: blocks synchronously on the latch (uninterruptible by abort).
    let latch1 = Arc::clone(&latch);
    let ran1 = Arc::clone(&ran);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran1.fetch_add(1, Ordering::SeqCst);
            while !latch1.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }))
        .expect("queue has capacity");
    // E2: backlogged behind E1 — must NOT start after the force-stop.
    let ran2 = Arc::clone(&ran);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran2.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("queue has capacity");

    // Wait for E1 to start, then force-stop exactly like undeclare's drain
    // timeout path: drop the shutdown signal, then abort the task.
    let started = tokio::time::Instant::now();
    while ran.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() - started < tokio::time::Duration::from_secs(1),
            "E1 did not start"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }
    drop(shutdown_tx);
    // Exactly like undeclare()'s drain-timeout path: set the cooperative
    // stop flag first, then abort as backup.
    stop.store(true, std::sync::atomic::Ordering::Release);
    handle.abort();

    // Release E1: the dispatch loop must not start the backlogged E2.
    latch.store(true, Ordering::Release);
    let joined = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;
    assert!(joined.is_ok(), "drain task must exit after force-stop");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "abort must prevent a new backlogged callback from starting"
    );
}

// Regression (pop path — assembly form): the
// stop flag must be checked BEFORE each event's dispatch admission, not
// after — a post-run check is TOCTOU: the flag can be set between the
// check and the next event's dispatch with no scheduling point in between
// when the queue is non-empty. Deterministic construction via the
// callbacks themselves (the rendezvous needs no scheduler help): E1's
// callback sets the stop flag at the exact instant E1 finishes and E2 is
// not yet admitted; the loop then takes E2 and the admission check must
// refuse it. Deleting the admission check turns this red (E2 runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_queue_stop_flag_admission_check_between_backlog_events() {
    use crate::event_queue::{Event, EventKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let local_queue = spawn_local_queue(8);
    let (sender, handle, shutdown_tx, stop, _liveness) = (
        local_queue.sender,
        local_queue.task,
        local_queue.shutdown,
        local_queue.stop,
        local_queue.liveness,
    );
    let ran = Arc::new(AtomicUsize::new(0));

    // E1: runs, then sets the stop flag — the callback itself is the
    // rendezvous at the TOCTOU instant (E1 done, E2 not yet admitted).
    let ran1 = Arc::clone(&ran);
    let stop_cb = Arc::clone(&stop);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran1.fetch_add(1, Ordering::SeqCst);
            stop_cb.store(true, Ordering::Release);
        }))
        .expect("queue has capacity");
    // E2: backlogged behind E1 — must NOT be admitted after the flag is set.
    let ran2 = Arc::clone(&ran);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran2.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("queue has capacity");

    drop(shutdown_tx);
    let joined = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "drain task must exit after the stop flag is set"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "the stop flag must be checked before each event's dispatch admission \
         (set between E1 and E2; E2 must not run)"
    );
}

// Regression (shutdown-drain path): the drain
// loop must apply the same admission check per event. Deterministic
// construction (the callback is the rendezvous): E1 is queued first and
// sets the stop flag when it runs; E2 is queued behind it and must be
// refused by the admission check. Deleting the admission check inside
// `drain_local_queue` turns this red (E2 runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn local_queue_drain_stop_flag_admission_check_between_events() {
    use crate::event_queue::{Event, EventKind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let (sender, mut queue) = crate::event_queue::EventQueue::new(8).into_parts();
    let stop = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicUsize::new(0));

    let ran1 = Arc::clone(&ran);
    let stop_cb = Arc::clone(&stop);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran1.fetch_add(1, Ordering::SeqCst);
            stop_cb.store(true, Ordering::Release);
        }))
        .expect("queue has capacity");
    let ran2 = Arc::clone(&ran);
    sender
        .try_send(Event::new(EventKind::Timer, move || {
            ran2.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("queue has capacity");

    super::drain_local_queue(&mut queue, &stop).await;
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "the shutdown drain must check the stop flag before each event's \
         dispatch admission (E2 must not run)"
    );
}
