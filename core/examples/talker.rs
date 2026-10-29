use std::time::Duration;

use aorta_core::log_prelude::*;
use aorta_core::node::Node;
use aorta_core::schema::SchemaMeta;
use flatbuffers::FlatBufferBuilder;

mod aorta_base_generated {
    #![allow(dead_code)]
    #![allow(unused_imports)]
    #![allow(mismatched_lifetime_syntaxes)]
    include!(concat!(env!("OUT_DIR"), "/aorta_base_generated.rs"));
}

mod demo_generated {
    #![allow(dead_code)]
    #![allow(unused_imports)]
    #![allow(mismatched_lifetime_syntaxes)]
    include!(concat!(env!("OUT_DIR"), "/demo_chat_generated.rs"));
}

struct DemoChatSchema;

impl SchemaMeta for DemoChatSchema {
    fn schema_bfbs() -> &'static [u8] {
        include_bytes!(concat!(env!("OUT_DIR"), "/demo_chat.bfbs"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "talker".to_string();
    let mut group = "default".to_string();
    let mut topic = "/demo/chatter".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                if let Some(value) = args.next() {
                    node_name = value;
                }
            }
            "--group" => {
                if let Some(value) = args.next() {
                    group = value;
                }
            }
            "--topic" => {
                if let Some(value) = args.next() {
                    topic = value;
                }
            }
            _ => {}
        }
    }

    let node = Node::new(node_name, group).await?;
    aorta_core_info!(
        "talker node={} group={} topic={}",
        node.name(),
        node.group(),
        topic
    );

    let publisher = node
        .create_publisher_with_schema::<DemoChatSchema>(&topic)
        .await?;
    if let Some(hash) = publisher.schema_hash() {
        aorta_core_info!("schema registered hash={}", hash.to_hex());
    }

    let mut counter: u64 = 0;
    let mut sequence: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            _ = ticker.tick() => {
                let payload = format!("hello {} from {}", counter, node.name());
                let mut builder = FlatBufferBuilder::new();
                let node_name = builder.create_string(node.name());
                let text = builder.create_string(&payload);
                let header = aorta_base_generated::aorta::sys::AortaHeader::create(
                    &mut builder,
                    &aorta_base_generated::aorta::sys::AortaHeaderArgs {
                        publish_stamp_ns: node.now().as_nanos() as u64,
                        publisher_node: Some(node_name),
                        sequence,
                    },
                );
                let chat = demo_generated::demo::Chat::create(
                    &mut builder,
                    &demo_generated::demo::ChatArgs {
                        aorta_header: Some(header),
                        text: Some(text),
                    },
                );
                builder.finish(chat, None);

                publisher.publish(builder.finished_data().to_vec())?;
                counter = counter.wrapping_add(1);
                sequence = sequence.wrapping_add(1);
            }
        }
    }

    node.close().await?;
    Ok(())
}
