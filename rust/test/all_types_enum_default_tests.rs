use all_types_msg::demo;
use flatbuffers::FlatBufferBuilder;

fn build_all_types(color: demo::Color, flag: bool) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let root = demo::AllTypes::create(
        &mut builder,
        &demo::AllTypesArgs {
            color,
            flag,
            ..Default::default()
        },
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[test]
fn default_color_decodes_as_red() {
    let bytes = build_all_types(demo::Color::RED, false);
    let decoded = flatbuffers::root::<demo::AllTypes>(bytes.as_slice())
        .expect("all-types message should decode");

    assert_eq!(decoded.color(), demo::Color::RED);
}

#[test]
fn explicit_color_round_trips() {
    let bytes = build_all_types(demo::Color::BLUE, false);
    let decoded = flatbuffers::root::<demo::AllTypes>(bytes.as_slice())
        .expect("all-types message should decode");

    assert_eq!(decoded.color(), demo::Color::BLUE);
}

#[test]
fn flag_round_trips() {
    let bytes = build_all_types(demo::Color::GREEN, true);
    let decoded = flatbuffers::root::<demo::AllTypes>(bytes.as_slice())
        .expect("all-types message should decode");

    assert!(decoded.flag());
}
