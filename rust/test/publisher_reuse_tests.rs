use crate::header::{HeaderInjector, HeaderValues};
use crate::{AortaResult, Node, PublisherOptions, SchemaMeta};
use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_works_with_default_builder_reuse() -> AortaResult<()> {
    let node = Node::new("reuse_policy_node", "sdk_test").await?;

    let options = PublisherOptions::default();
    let publisher = node
        .create_publisher_with_options::<DummyMessage>("/reuse/default", options)
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish multiple times to exercise TLS builder reuse path.
    for _ in 0..3 {
        publisher.publish(|builder, _header| {
            finish_root(builder);
        })?;
    }

    node.close().await?;
    Ok(())
}
