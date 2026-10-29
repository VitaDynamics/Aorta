use std::time::Duration;

use all_types_msg::demo;
use all_types_msg::prelude::*;
use all_types_msg::typed::{ChildMsg, MetaInfoMsg, PayloadUnionMsg};
use aorta_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "all_types_talker".to_string();
    let mut group = "default".to_string();
    let mut topic = "/demo/all_types".to_string();

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
    let publisher = node.create_publisher::<AllTypesSchema>(&topic).await?;
    aorta_info!(
        "all types talker ready: node={} group={} topic={}",
        node.name(),
        node.group(),
        topic
    );

    let mut sequence: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                let child = ChildMsg {
                    id: Some(100 + sequence as u32),
                    name: Some(format!("child-{}", sequence)),
                    pose: Some(demo::Vec3::new(4.0, 5.0, 6.0)),
                };
                publisher.publish_typed(|msg: &mut AllTypesMsg| {
                    msg.flag = Some(true);
                    msg.val_int8 = Some(-8);
                    msg.val_uint8 = Some(8);
                    msg.val_int16 = Some(-16);
                    msg.val_uint16 = Some(16);
                    msg.val_int32 = Some(-32);
                    msg.val_uint32 = Some(32);
                    msg.val_int64 = Some(-64);
                    msg.val_uint64 = Some(64 + sequence);
                    msg.val_float32 = Some(1.25);
                    msg.val_float64 = Some(2.5);
                    msg.text = Some(format!("all-types-{}", sequence));
                    msg.color = Some(demo::Color::RED);
                    msg.mode = Some(demo::Mode::RUN);
                    msg.pose = Some(demo::Vec3::new(1.0, 2.0, 3.0));
                    msg.child = Some(Box::new(child.clone()));
                    msg.meta = Some(Box::new(MetaInfoMsg {
                        tag: Some(format!("meta-{}", sequence)),
                        level: Some(9),
                    }));
                    msg.payload = Some(PayloadUnionMsg::Child(Box::new(child.clone())));
                    msg.bytes = Some(&[1, 2, 3, 4]);
                    msg.ints = Some(&[10, 20, 30]);
                    msg.floats = Some(&[0.5, 1.5]);
                    msg.names = Some(vec!["alice".to_string(), "bob".to_string()]);
                    msg.poses = Some(vec![demo::Vec3::new(0.0, 1.0, 2.0)]);
                    msg.children = Some(vec![child.clone()]);
                    msg.colors = Some(vec![demo::Color::GREEN, demo::Color::BLUE]);
                    msg.modes = Some(vec![demo::Mode::IDLE, demo::Mode::STOP]);
                })?;
                sequence = sequence.wrapping_add(1);
            }
        }
    }

    node.close().await?;
    Ok(())
}
