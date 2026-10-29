use super::{create_schema_by_key, root_as_schema_by_key};
use flatbuffers::FlatBufferBuilder;

#[test]
fn schema_by_key_roundtrip_reads_fields() {
    let mut builder = FlatBufferBuilder::new();
    let hash_hex =
        builder.create_string("000000000000000000000000000000000000000000000000000000000000007b");
    let bfbs = builder.create_vector(&[1u8, 2u8, 3u8]);
    let topic_root_type = builder.create_string("demo.Foo");
    let root = create_schema_by_key(
        &mut builder,
        0,
        Some(hash_hex),
        Some(bfbs),
        None,
        None,
        None,
        None,
        Some(topic_root_type),
    );
    builder.finish(root, None);

    let bytes = builder.finished_data().to_vec();
    let parsed = root_as_schema_by_key(&bytes).expect("root");
    assert_eq!(parsed.kind(), 0);
    assert_eq!(
        parsed.hash_hex().expect("hash_hex"),
        "000000000000000000000000000000000000000000000000000000000000007b"
    );
    assert_eq!(parsed.bfbs().expect("bfbs"), &[1u8, 2u8, 3u8]);
    assert_eq!(parsed.topic_root_type(), Some("demo.Foo"));
}

#[test]
fn schema_by_key_legacy_buffer_has_no_topic_root_type() {
    let mut builder = FlatBufferBuilder::new();
    let hash_hex =
        builder.create_string("000000000000000000000000000000000000000000000000000000000000007b");
    let bfbs = builder.create_vector(&[1u8, 2u8, 3u8]);
    let root = create_schema_by_key(
        &mut builder,
        0,
        Some(hash_hex),
        Some(bfbs),
        None,
        None,
        None,
        None,
        None,
    );
    builder.finish(root, None);

    let parsed = root_as_schema_by_key(builder.finished_data()).expect("root");
    assert_eq!(parsed.topic_root_type(), None);
}
