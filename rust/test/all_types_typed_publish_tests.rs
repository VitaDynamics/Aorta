use all_types_msg::demo;
use all_types_msg::prelude::*;
use all_types_msg::typed::{AllTypesMsg, ChildMsg, MetaInfoMsg, PayloadUnionMsg};
use aorta_sdk::{HeaderInjector, HeaderValues, TypedPublishMessage};
use flatbuffers::FlatBufferBuilder;

#[test]
fn all_types_typed_publish_builds_flatbuffer() {
    let mut builder = FlatBufferBuilder::new();
    let header_values = HeaderValues {
        publish_stamp_ns: 1,
        publisher_node: "rust_test",
        sequence: 2,
    };
    let header = <AllTypesSchema as HeaderInjector>::build_header(&mut builder, &header_values);

    let child = ChildMsg {
        id: Some(7),
        name: Some("child".to_string()),
        pose: Some(demo::Vec3::new(4.0, 5.0, 6.0)),
    };

    let msg = AllTypesMsg {
        flag: Some(true),
        val_int8: Some(-8),
        val_uint8: Some(8),
        val_int16: Some(-16),
        val_uint16: Some(16),
        val_int32: Some(-32),
        val_uint32: Some(32),
        val_int64: Some(-64),
        val_uint64: Some(64),
        val_float32: Some(1.25),
        val_float64: Some(2.5),
        text: Some("all types".to_string()),
        color: Some(demo::Color::RED),
        mode: Some(demo::Mode::RUN),
        pose: Some(demo::Vec3::new(1.0, 2.0, 3.0)),
        child: Some(Box::new(child.clone())),
        meta: Some(Box::new(MetaInfoMsg {
            tag: Some("meta".to_string()),
            level: Some(9),
        })),
        payload: Some(PayloadUnionMsg::Child(Box::new(child.clone()))),
        bytes: Some(&[1, 2, 3, 4]),
        ints: Some(&[10, 20, 30]),
        floats: Some(&[0.5, 1.5]),
        names: Some(vec!["alice".to_string(), "bob".to_string()]),
        poses: Some(vec![demo::Vec3::new(0.0, 1.0, 2.0)]),
        children: Some(vec![child]),
        colors: Some(vec![demo::Color::GREEN, demo::Color::BLUE]),
        modes: Some(vec![demo::Mode::IDLE, demo::Mode::STOP]),
        signed_bytes: None,
    };

    let root =
        <AllTypesSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    assert!(!builder.finished_data().is_empty());
}
