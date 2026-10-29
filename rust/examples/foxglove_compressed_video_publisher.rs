use std::time::Duration;

use aorta_sdk::prelude::*;
use foxglove_compressed_video_msg::prelude::*;
use foxglove_compressed_video_msg::Time_generated::foxglove::Time as FoxgloveTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "foxglove_cv_publisher".to_string();
    let mut group = "default".to_string();
    let mut topic = "/foxglove/compressed_video".to_string();

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
    let publisher = node.create_publisher::<CompressedVideoSchema>(&topic).await?;

    if let Some(hash) = publisher.schema_hash() {
        aorta_info!("schema registered hash={}", hash.to_hex());
    }
    aorta_info!(
        "foxglove compressed video publisher ready: node={} group={} topic={}",
        node.name(),
        node.group(),
        topic
    );

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
                let now_ns = unix_time_ns();
                let frame_data = [0x00u8, 0x00, 0x00, 0x01, 0x09, 0x10, (sequence & 0xff) as u8];
                publisher.publish_typed(|msg: &mut CompressedVideoMsg| {
                    msg.timestamp = Some(FoxgloveTime::new(
                        (now_ns / 1_000_000_000) as u32,
                        (now_ns % 1_000_000_000) as u32,
                    ));
                    msg.frame_id = Some(format!("camera/{}", node.name()));
                    msg.format = Some("h264".to_string());
                    msg.data = Some(&frame_data);
                })?;
                if sequence % 10 == 0 {
                    aorta_info!(
                        "published foxglove CompressedVideo frame seq={} topic={}",
                        sequence,
                        topic
                    );
                }
                sequence = sequence.wrapping_add(1);
            }
        }
    }

    node.close().await?;
    Ok(())
}
