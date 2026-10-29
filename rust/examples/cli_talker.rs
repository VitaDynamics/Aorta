use std::time::Duration;

use aorta_sdk::prelude::*;
use cli_args::ExampleArgs;
use demo_chat_msg::prelude::*;
use schema_attach::parse_schema_attach_policy;

mod cli_args;
mod schema_attach;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "cli_talker".to_string();
    let mut group = "default".to_string();
    let mut topic = "/demo/chatter".to_string();
    let mut schema_attach = "always".to_string();

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
            "--schema-attach" => {
                if let Some(value) = args.next_value() {
                    schema_attach = value;
                }
            }
            _ => {}
        }
    }

    let schema_attach_policy = parse_schema_attach_policy(&schema_attach).ok_or_else(|| {
        format!(
            "invalid --schema-attach value: {} (expected never|always|inherit)",
            schema_attach
        )
    })?;

    let node = Node::new(node_name, group).await?;
    aorta_info!(
        "cli_talker node={} group={} topic={} schema_attach={}",
        node.name(),
        node.group(),
        topic,
        schema_attach
    );

    let mut options = PublisherOptions::default();
    options.schema_attach_policy = schema_attach_policy;
    let publisher = node
        .create_publisher_with_options::<ChatSchema>(&topic, options)
        .await?;
    if let Some(hash) = publisher.schema_hash() {
        aorta_info!("schema registered hash={}", hash.to_hex());
    }

    let mut counter: u64 = 0;
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
                publisher.publish_typed(|msg: &mut ChatMsg| {
                    msg.text = Some(payload.clone());
                })?;
                counter = counter.wrapping_add(1);
            }
        }
    }

    node.close().await?;
    Ok(())
}
