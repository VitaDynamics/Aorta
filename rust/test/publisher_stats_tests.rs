use crate::header::{HeaderInjector, HeaderValues};
use crate::publisher::force_publish_error_for_test;
use crate::PublisherOptions;
use crate::{AortaResult, Node, SchemaMeta};
use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct DummyMessage;

impl SchemaMeta for DummyMessage {
    fn schema_bfbs() -> &'static [u8] {
        b"dummy_aorta_header_schema"
    }
}

struct DummyHeader;
struct DummyRoot;

impl<'a> Follow<'a> for DummyHeader {
    type Inner = DummyHeader;

    unsafe fn follow(_buf: &'a [u8], _loc: usize) -> Self::Inner {
        DummyHeader
    }
}

impl<'a> Follow<'a> for DummyRoot {
    type Inner = DummyRoot;

    unsafe fn follow(_buf: &'a [u8], _loc: usize) -> Self::Inner {
        DummyRoot
    }
}

impl HeaderInjector for DummyMessage {
    type Header<'a> = DummyHeader;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        _values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>> {
        let start = builder.start_table();
        let end = builder.end_table(start);
        WIPOffset::new(end.value())
    }
}

fn finish_root(builder: &mut FlatBufferBuilder<'_>) {
    let start = builder.start_table();
    let end = builder.end_table(start);
    let root = WIPOffset::<DummyRoot>::new(end.value());
    builder.finish(root, None);
}

fn unique_topic(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sdk/test/p{}/{}_{}", std::process::id(), base, id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_stats_counts_publish_calls() -> AortaResult<()> {
    let pub_node = Node::new("stats_pub", "sdk_test").await?;
    let topic = unique_topic("stats");

    let publisher = pub_node.create_publisher::<DummyMessage>(&topic).await?;

    publisher.publish_bytes(vec![1u8, 2u8, 3u8])?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
        let _ = publisher.publish(|builder, _header| {
            finish_root(builder);
        });
    })?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let stats = publisher.stats();
    assert_eq!(stats.publish_count, 4);
    assert!(stats.last_publish_ns > 0);
    assert_eq!(stats.send_error_count, 0);
    assert_eq!(stats.reentrant_count, 1);
    assert_eq!(stats.tls_builder_publish_count, 2);
    assert_eq!(stats.fresh_builder_publish_count, 1);
    assert_eq!(stats.mutex_builder_publish_count, 0);

    pub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_stats_oversize_increments() -> AortaResult<()> {
    let node = Node::new("stats_oversize", "sdk_test").await?;
    let topic = unique_topic("oversize");

    let options = PublisherOptions::default().with_max_message_size(32);
    let publisher = node
        .create_publisher_with_options::<DummyMessage>(&topic, options)
        .await?;

    publisher.publish(|builder, _header| {
        let data = vec![0u8; 256];
        let vec = builder.create_vector(&data);
        builder.finish(vec, None);
    })?;

    let stats = publisher.stats();
    assert!(stats.oversize_count >= 1);
    assert!(stats.last_oversize_size >= 32);
    assert!(stats.max_capacity_seen >= stats.last_oversize_size);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn publisher_stats_default_thread_local_path_is_counted() -> AortaResult<()> {
    let node = Node::new("stats_tls_path", "sdk_test").await?;
    let topic = unique_topic("tls_path");
    let publisher = node.create_publisher::<DummyMessage>(&topic).await?;

    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    let stats = publisher.stats();
    assert_eq!(stats.tls_builder_publish_count, 2);
    assert_eq!(stats.mutex_builder_publish_count, 0);
    assert_eq!(stats.fresh_builder_publish_count, 0);
    assert_eq!(stats.mutex_mode_switch_count, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn publisher_stats_one_megabyte_capacity_stays_on_tls_path_by_default() -> AortaResult<()> {
    let node = Node::new("stats_tls_one_meg", "sdk_test").await?;
    let topic = unique_topic("tls_one_meg");
    let options = PublisherOptions::default().with_max_message_size(1024 * 1024);
    let publisher = node
        .create_publisher_with_options::<DummyMessage>(&topic, options)
        .await?;

    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    let stats = publisher.stats();
    assert_eq!(stats.tls_builder_publish_count, 2);
    assert_eq!(stats.mutex_builder_publish_count, 0);
    assert_eq!(stats.fresh_builder_publish_count, 0);
    assert_eq!(stats.mutex_mode_switch_count, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn publisher_stats_large_capacity_switches_to_mutex_path() -> AortaResult<()> {
    let node = Node::new("stats_mutex_path", "sdk_test").await?;
    let topic = unique_topic("mutex_path");
    let options = PublisherOptions::default().with_max_message_size(3 * 1024 * 1024);
    let publisher = node
        .create_publisher_with_options::<DummyMessage>(&topic, options)
        .await?;

    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    let stats = publisher.stats();
    assert_eq!(stats.tls_builder_publish_count, 0);
    assert_eq!(stats.mutex_builder_publish_count, 2);
    assert_eq!(stats.fresh_builder_publish_count, 0);
    assert_eq!(stats.mutex_mode_switch_count, 1);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_stats_send_error_increments() -> AortaResult<()> {
    let node = Node::new("stats_send_error", "sdk_test").await?;
    let topic = unique_topic("send_error");
    let publisher = node.create_publisher::<DummyMessage>(&topic).await?;

    force_publish_error_for_test();
    let result = publisher.publish_bytes(vec![9u8, 9u8, 9u8]);
    assert!(result.is_err());

    let stats = publisher.stats();
    assert_eq!(stats.send_error_count, 1);
    assert_eq!(stats.publish_count, 1);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn publisher_stats_no_queue_len_field() -> AortaResult<()> {
    let node = Node::new("stats_no_queue", "sdk_test").await?;
    let topic = unique_topic("no_queue");
    let publisher = node.create_publisher::<DummyMessage>(&topic).await?;

    let count = 16u32;
    for _ in 0..count {
        publisher.publish_bytes(vec![1u8, 2u8, 3u8])?;
    }

    let stats = publisher.stats();
    assert_eq!(stats.publish_count, count as u64);

    publisher.undeclare().await?;
    node.close().await?;
    Ok(())
}
