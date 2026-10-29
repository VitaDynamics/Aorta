use crate::header::{HeaderInjector, HeaderValues};
use crate::{AortaResult, Node, SchemaMeta, TypedPublishMessage};
use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct DummyMessage;

impl SchemaMeta for DummyMessage {
    fn schema_bfbs() -> &'static [u8] {
        b"dummy_aorta_header_schema"
    }
}

struct DummyHeader;
struct DummyRoot;
#[derive(Default)]
struct DummyTypedMsg {
    marker: Option<u32>,
}

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

impl TypedPublishMessage for DummyMessage {
    type Msg<'a> = DummyTypedMsg;
    type Root<'a> = DummyRoot;

    fn build_typed_root<'a>(
        builder: &mut FlatBufferBuilder<'a>,
        _header: WIPOffset<Self::Header<'a>>,
        _msg: &Self::Msg<'_>,
    ) -> WIPOffset<Self::Root<'a>> {
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
async fn publish_bytes_and_builders_deliver() -> AortaResult<()> {
    let pub_node = Node::new("sdk_pub", "sdk_test").await?;
    let sub_node = Node::new("sdk_sub", "sdk_test").await?;
    let topic = unique_topic("publish");

    let received = Arc::new(AtomicU32::new(0));
    let recv_clone = Arc::clone(&received);
    let handshake_seen = Arc::new(AtomicBool::new(false));
    let handshake_seen_clone = Arc::clone(&handshake_seen);

    let _sub = sub_node
        .create_subscriber(&topic, move |data, _ctx| {
            let bytes = data.to_bytes();
            if bytes.as_ref() == b"warmup" {
                handshake_seen_clone.store(true, Ordering::Relaxed);
            } else {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await?;

    let publisher = pub_node.create_publisher::<DummyMessage>(&topic).await?;

    let handshake_ready = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            publisher.publish_bytes(b"warmup".as_slice())?;
            tokio::time::sleep(Duration::from_millis(30)).await;
            if handshake_seen.load(Ordering::Relaxed) {
                break;
            }
        }
        Ok::<(), crate::AortaError>(())
    })
    .await;
    assert!(
        matches!(handshake_ready, Ok(Ok(()))),
        "subscriber did not receive warmup message in time"
    );

    publisher.publish_bytes(&[1u8, 2u8, 3u8][..])?;
    let owned = vec![4u8, 5u8, 6u8];
    publisher.publish_bytes(owned)?;
    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    publisher.publish(|builder, _header| {
        finish_root(builder);
    })?;

    publisher.publish_typed(|msg| {
        msg.marker = Some(7);
    })?;
    publisher.publish_typed_msg(&DummyTypedMsg { marker: Some(9) })?;

    let wait_result = tokio::time::timeout(Duration::from_millis(800), async {
        while received.load(Ordering::Relaxed) < 6 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    assert!(wait_result.is_ok(), "did not receive all messages in time");
    assert!(received.load(Ordering::Relaxed) >= 6);

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}
