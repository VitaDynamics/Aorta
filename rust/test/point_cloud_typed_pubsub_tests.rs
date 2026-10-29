use aorta_sdk::{AortaError, AortaResult, Node, SubscriberOptions, TypedRetainedSample};
use foxglove_point_cloud_msg::PackedElementField_generated::foxglove::NumericType;
use foxglove_point_cloud_msg::prelude::*;
use foxglove_point_cloud_msg::typed::{
    PackedElementFieldMsg, PointCloudMsg, PoseMsg, QuaternionMsg, Vector3Msg,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn unique_topic(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sdk/test/n{}/{}_{}", std::process::id(), base, id)
}

fn build_point_cloud_msg<'a>(data: &'a [u8]) -> PointCloudMsg<'a> {
    PointCloudMsg {
        frame_id: Some("map".to_string()),
        pose: Some(Box::new(PoseMsg {
            position: Some(Box::new(Vector3Msg {
                x: Some(1.0),
                y: Some(2.0),
                z: Some(3.0),
            })),
            orientation: Some(Box::new(QuaternionMsg {
                x: Some(0.0),
                y: Some(0.0),
                z: Some(0.0),
                w: Some(1.0),
            })),
        })),
        point_stride: Some(16),
        fields: Some(vec![PackedElementFieldMsg {
            name: Some("x".to_string()),
            offset: Some(0),
            r#type: Some(NumericType::FLOAT32),
        }]),
        data: Some(data),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_cloud_typed_pubsub_roundtrip() -> AortaResult<()> {
    let pub_node = Node::new("sdk_point_cloud_pub", "sdk_test").await?;
    let sub_node = Node::new("sdk_point_cloud_sub", "sdk_test").await?;
    let topic = unique_topic("foxglove_point_cloud");

    let stored: Arc<Mutex<Option<TypedRetainedSample<PointCloudSchema>>>> =
        Arc::new(Mutex::new(None));
    let stored_clone = Arc::clone(&stored);

    let _sub = sub_node
        .create_subscriber_typed_retained_with_options::<PointCloudSchema, _>(
            &topic,
            move |sample| {
                let mut guard = stored_clone
                    .lock()
                    .expect("point cloud typed retained sample lock");
                *guard = Some(sample);
            },
            SubscriberOptions::new()
                .with_topic_in_context(true)
                .with_header_context(true),
        )
        .await?;

    let publisher = pub_node.create_publisher::<PointCloudSchema>(&topic).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let point_data = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let wait_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            {
                let guard = stored.lock().expect("point cloud typed retained sample lock");
                if guard.is_some() {
                    break;
                }
            }

            let msg = build_point_cloud_msg(&point_data);
            publisher.publish_typed_msg(&msg)?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<(), AortaError>(())
    })
    .await;

    assert!(
        wait_result.is_ok(),
        "point cloud typed subscriber did not receive message before timeout"
    );
    wait_result.expect("point cloud typed wait result")?;

    let sample = {
        let mut guard = stored.lock().expect("point cloud typed retained sample lock");
        guard
            .take()
            .expect("point cloud typed retained sample must exist")
    };

    assert!(
        sample.payload().len() > point_data.len(),
        "serialized point cloud payload should include metadata and headers"
    );
    assert!(
        sample.context().topic.as_ref().ends_with(topic.as_str()),
        "context topic '{}' should end with '{}'",
        sample.context().topic,
        topic
    );
    assert_eq!(
        sample.context().publisher_node.as_deref(),
        Some("sdk_point_cloud_pub")
    );

    let root = sample.decode()?;
    assert_eq!(root.frame_id(), Some("map"));
    assert_eq!(root.point_stride(), 16);
    assert!(root.aorta_header().is_some());

    let pose = root.pose().expect("point cloud pose");
    let position = pose.position().expect("point cloud pose position");
    assert_eq!(position.x(), 1.0);
    assert_eq!(position.y(), 2.0);
    assert_eq!(position.z(), 3.0);

    let orientation = pose.orientation().expect("point cloud pose orientation");
    assert_eq!(orientation.w(), 1.0);

    let fields = root.fields().expect("point cloud fields");
    assert_eq!(fields.len(), 1);
    let field = fields.get(0);
    assert_eq!(field.name(), Some("x"));
    assert_eq!(field.offset(), 0);
    assert_eq!(field.type_(), NumericType::FLOAT32);

    let data = root.data().expect("point cloud data");
    assert_eq!(data.len(), point_data.len());
    for (idx, expected) in point_data.iter().enumerate() {
        assert_eq!(data.get(idx), *expected);
    }

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}
