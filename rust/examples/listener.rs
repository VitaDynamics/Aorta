use aorta_sdk::log_prelude::*;

use aorta_sdk::{with_contiguous_bytes, Node, SubscriberOptions};
use cli_args::ExampleArgs;
use demo_chat_msg::demo;

mod cli_args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "listener".to_string();
    let mut group = "default".to_string();
    let mut topic = "/demo/chatter".to_string();
    let mut enable_header_context = false;
    let mut enable_dedup = false;
    let mut dedup_window: usize = 1024;
    let mut enable_reorder_check = false;
    let mut drop_out_of_order = false;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--name" => {
                if let Some(value) = args.next_value() {
                    node_name = value;
                }
            }
            "--group" => {
                if let Some(value) = args.next_value() {
                    group = value;
                }
            }
            "--topic" => {
                if let Some(value) = args.next_value() {
                    topic = value;
                }
            }
            "--header-context" => enable_header_context = true,
            "--dedup" => enable_dedup = true,
            "--dedup-window" => {
                if let Some(value) = args.next_value() {
                    if let Ok(parsed) = value.parse::<usize>() {
                        dedup_window = parsed.max(1);
                    }
                }
            }
            "--reorder-check" => enable_reorder_check = true,
            "--drop-out-of-order" => drop_out_of_order = true,
            _ => {}
        }
    }

    let node = Node::new(node_name, group).await?;
    aorta_info!(
        "sdk listener node={} group={} topic={}",
        node.name(),
        node.group(),
        topic
    );
    aorta_info!(
        "subscriber strategy: header_context={} dedup={} dedup_window={} reorder_check={} drop_out_of_order={}",
        enable_header_context,
        enable_dedup,
        dedup_window,
        enable_reorder_check,
        drop_out_of_order
    );

    let options = SubscriberOptions::new()
        .with_header_context(enable_header_context)
        .with_topic_in_context(true)
        .with_receive_timestamp(true)
        .with_priority_in_context(true)
        .with_publisher_timestamp(true)
        .with_dedup(enable_dedup)
        .with_dedup_window(dedup_window)
        .with_reorder_check(enable_reorder_check)
        .with_drop_out_of_order(drop_out_of_order);

    let _subscriber = node
        .create_subscriber_with_options(
            &topic,
            |payload, context| {
                aorta_info!(
                    "[{}] Received at priority {:?}, local_ts={}, pub_ts={:?}",
                    context.topic,
                    context.priority,
                    context.receive_stamp_ns,
                    context.publisher_stamp_ns
                );
                if let Some(seq) = context.sequence {
                    aorta_info!(
                        "  ctx header: seq={} node={}",
                        seq,
                        context.publisher_node.as_deref().unwrap_or("")
                    );
                }
                with_contiguous_bytes(payload, |payload_bytes| {
                    if let Ok(chat) = demo::root_as_chat(payload_bytes) {
                        let text = chat.text().unwrap_or("");
                        if let Some(header) = chat.aorta_header() {
                            aorta_info!(
                                "  seq={} node={} text={}",
                                header.sequence(),
                                header.publisher_node().unwrap_or(""),
                                text
                            );
                        } else {
                            aorta_info!("  text={}", text);
                        }
                    } else {
                        aorta_info!("  {} bytes (non-flatbuffer)", payload_bytes.len());
                    }
                });
            },
            options,
        )
        .await?;
    tokio::signal::ctrl_c().await?;
    node.close().await?;
    Ok(())
}
