use aorta_core::executor::SingleThreadedExecutor;
use aorta_core::log_prelude::*;
use aorta_core::node::Node;
use aorta_core::subscriber::SubscriberOptions;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "listener".to_string();
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
        "listener node={} group={} topic={}",
        node.name(),
        node.group(),
        topic
    );

    let mut executor = SingleThreadedExecutor::new(1024);
    executor.add_node(node.clone());
    let stop = executor.stop_handle();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stop.stop();
    });

    // Opt in to all context fields for diagnostic output.
    let sub_options = SubscriberOptions::new()
        .with_topic_in_context(true)
        .with_receive_timestamp(true)
        .with_priority_in_context(true)
        .with_publisher_timestamp(true);
    let _subscriber = node
        .create_subscriber_with_options(
            &topic,
            |payload, context| {
                let payload_bytes = payload.to_bytes();
                aorta_core_info!(
                    "[{}] priority={:?} recv_ts={} pub_ts={:?}",
                    context.topic,
                    context.priority,
                    context.receive_stamp_ns,
                    context.publisher_stamp_ns
                );
                if let Ok(chat) = demo_generated::demo::root_as_chat(payload_bytes.as_ref()) {
                    if let Some(text) = chat.text() {
                        aorta_core_info!("  text: {}", text);
                    } else {
                        aorta_core_info!("  chat without text");
                    }
                } else if let Ok(text) = std::str::from_utf8(payload_bytes.as_ref()) {
                    aorta_core_info!("  raw: {}", text);
                } else {
                    aorta_core_info!("  {} bytes", payload.len());
                }
            },
            sub_options,
        )
        .await?;

    executor.spin().await;
    node.close().await?;
    Ok(())
}
