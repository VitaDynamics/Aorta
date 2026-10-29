use super::{
    bfbs_contains_aorta_header, bfbs_root_type_name, infer_aorta_header_layout_from_bfbs,
    infer_service_root_types_from_bfbs, SchemaAttachPolicy, SchemaHash,
};
use crate::test_support::{allow_schema_env_guard, ENV_LOCK};
use flatbuffers::FlatBufferBuilder;
use flatbuffers_reflection::reflection;

#[test]
fn hash_is_deterministic() {
    let data = b"schema-binary";
    let first = SchemaHash::from_bfbs_bytes(data);
    let second = SchemaHash::from_bfbs_bytes(data);
    assert_eq!(first.as_bytes(), second.as_bytes());
}

#[test]
fn hash_hex_roundtrip_and_bytes() {
    let mut raw = [0u8; 32];
    raw[0] = 0x12;
    raw[1] = 0x34;
    raw[2] = 0x56;
    raw[3] = 0x78;
    raw[31] = 0xff;
    let hash = SchemaHash::from_bytes(raw);
    let hex = hash.to_hex();
    assert_eq!(hex.len(), 64);
    let restored = SchemaHash::from_hex_str(&hex).expect("hex");
    assert_eq!(restored.as_bytes(), hash.as_bytes());
}

#[test]
fn hash_hex_rejects_uppercase() {
    let upper = "00000000000000000000000000000000000000000000000000000000000000AB";
    assert!(SchemaHash::from_hex_str(upper).is_none());
}

#[test]
fn hash_hex_rejects_wrong_len() {
    assert!(SchemaHash::from_hex_str("abcd").is_none());
}

#[test]
fn hash_hex_rejects_non_hex() {
    let mut value = "0000000000000000000000000000000000000000000000000000000000000000".to_string();
    value.pop();
    value.push('g');
    assert!(SchemaHash::from_hex_str(&value).is_none());
}

#[test]
fn hash_hex_is_lowercase() {
    let mut raw = [0u8; 32];
    raw[0] = 0xab;
    let hash = SchemaHash::from_bytes(raw);
    let hex = hash.to_hex();
    assert_eq!(&hex[0..2], "ab");
}

#[test]
fn bfbs_header_probe_detects_field() {
    // Needle is null-terminated, so a bare substring without trailing NUL must
    // not match, while a properly terminated occurrence must.
    let bytes = b"prefix_aorta_header_suffix";
    assert!(!bfbs_contains_aorta_header(bytes));

    let bytes_nul = b"prefix\0aorta_header\0suffix";
    assert!(bfbs_contains_aorta_header(bytes_nul));
    assert!(!bfbs_contains_aorta_header(b"no_header_here"));
}

#[test]
fn schema_attach_policy_resolve_inherit() {
    assert_eq!(
        SchemaAttachPolicy::Inherit.resolve(SchemaAttachPolicy::Always),
        SchemaAttachPolicy::Always
    );
    assert_eq!(
        SchemaAttachPolicy::Inherit.resolve(SchemaAttachPolicy::Never),
        SchemaAttachPolicy::Never
    );
}

#[test]
fn schema_attach_policy_resolve_override() {
    assert_eq!(
        SchemaAttachPolicy::Always.resolve(SchemaAttachPolicy::Never),
        SchemaAttachPolicy::Always
    );
    assert_eq!(
        SchemaAttachPolicy::Never.resolve(SchemaAttachPolicy::Always),
        SchemaAttachPolicy::Never
    );
}

#[test]
fn schema_attach_policy_from_env_values() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();

    std::env::set_var("AORTA_SCHEMA_ATTACH", "always");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Always);

    std::env::set_var("AORTA_SCHEMA_ATTACH", "never");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);

    std::env::set_var("AORTA_SCHEMA_ATTACH", "unknown");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);

    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

fn build_test_service_bfbs(root_table_name: &str, object_names: &[&str]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let empty_fields = builder.create_vector::<flatbuffers::WIPOffset<reflection::Field>>(&[]);
    let mut objects = Vec::with_capacity(object_names.len());
    let mut root_table = None;

    for name in object_names {
        let name_offset = builder.create_string(name);
        let object = reflection::Object::create(
            &mut builder,
            &reflection::ObjectArgs {
                name: Some(name_offset),
                fields: Some(empty_fields),
                is_struct: false,
                ..Default::default()
            },
        );
        if *name == root_table_name {
            root_table = Some(object);
        }
        objects.push(object);
    }

    let objects = builder.create_vector(&objects);
    let enums = builder.create_vector::<flatbuffers::WIPOffset<reflection::Enum>>(&[]);
    let schema = reflection::Schema::create(
        &mut builder,
        &reflection::SchemaArgs {
            objects: Some(objects),
            enums: Some(enums),
            root_table,
            ..Default::default()
        },
    );
    builder.finish(schema, Some("BFBS"));
    builder.finished_data().to_vec()
}

fn build_test_header_layout_bfbs(
    root_header_field_id: u16,
    publisher_node_field_id: u16,
    sequence_field_id: u16,
    include_root_header_field: bool,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let root_name = builder.create_string("demo.VideoFrame");
    let header_name = builder.create_string("aorta.sys.AortaHeader");
    let field_name_header = builder.create_string("aorta_header");
    let field_name_node = builder.create_string("publisher_node");
    let field_name_sequence = builder.create_string("sequence");

    let header_type_obj = reflection::Type::create(
        &mut builder,
        &reflection::TypeArgs {
            base_type: reflection::BaseType::Obj,
            index: 1,
            ..Default::default()
        },
    );
    let node_type = reflection::Type::create(
        &mut builder,
        &reflection::TypeArgs {
            base_type: reflection::BaseType::String,
            ..Default::default()
        },
    );
    let sequence_type = reflection::Type::create(
        &mut builder,
        &reflection::TypeArgs {
            base_type: reflection::BaseType::ULong,
            ..Default::default()
        },
    );

    let mut root_fields = Vec::new();
    if include_root_header_field {
        root_fields.push(reflection::Field::create(
            &mut builder,
            &reflection::FieldArgs {
                name: Some(field_name_header),
                type_: Some(header_type_obj),
                id: root_header_field_id,
                ..Default::default()
            },
        ));
    }
    let root_fields = builder.create_vector(&root_fields);

    let header_publisher_node_field = reflection::Field::create(
        &mut builder,
        &reflection::FieldArgs {
            name: Some(field_name_node),
            type_: Some(node_type),
            id: publisher_node_field_id,
            ..Default::default()
        },
    );
    let header_sequence_field = reflection::Field::create(
        &mut builder,
        &reflection::FieldArgs {
            name: Some(field_name_sequence),
            type_: Some(sequence_type),
            id: sequence_field_id,
            ..Default::default()
        },
    );
    let header_fields =
        builder.create_vector(&[header_publisher_node_field, header_sequence_field]);

    let root_object = reflection::Object::create(
        &mut builder,
        &reflection::ObjectArgs {
            name: Some(root_name),
            fields: Some(root_fields),
            is_struct: false,
            ..Default::default()
        },
    );
    let header_object = reflection::Object::create(
        &mut builder,
        &reflection::ObjectArgs {
            name: Some(header_name),
            fields: Some(header_fields),
            is_struct: false,
            ..Default::default()
        },
    );

    let objects = builder.create_vector(&[root_object, header_object]);
    let enums = builder.create_vector::<flatbuffers::WIPOffset<reflection::Enum>>(&[]);
    let schema = reflection::Schema::create(
        &mut builder,
        &reflection::SchemaArgs {
            objects: Some(objects),
            enums: Some(enums),
            root_table: Some(root_object),
            ..Default::default()
        },
    );
    builder.finish(schema, Some("BFBS"));
    builder.finished_data().to_vec()
}

#[test]
fn bfbs_root_type_name_returns_root_table_name() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    assert_eq!(
        bfbs_root_type_name(&bfbs).as_deref(),
        Some("demo.EchoRequest")
    );
}

#[test]
fn infer_service_root_types_from_bfbs_fills_missing_pair() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    let (request, response) = infer_service_root_types_from_bfbs(&bfbs, None, None);
    assert_eq!(request.as_deref(), Some("demo.EchoRequest"));
    assert_eq!(response.as_deref(), Some("demo.EchoResponse"));
}

#[test]
fn infer_service_root_types_from_bfbs_fills_request_from_response() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    let (request, response) =
        infer_service_root_types_from_bfbs(&bfbs, None, Some("demo.EchoResponse"));
    assert_eq!(request.as_deref(), Some("demo.EchoRequest"));
    assert_eq!(response.as_deref(), Some("demo.EchoResponse"));
}

#[test]
fn infer_service_root_types_from_bfbs_preserves_explicit_values() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    let (request, response) = infer_service_root_types_from_bfbs(
        &bfbs,
        Some("demo.CustomRequest"),
        Some("demo.CustomResponse"),
    );
    assert_eq!(request.as_deref(), Some("demo.CustomRequest"));
    assert_eq!(response.as_deref(), Some("demo.CustomResponse"));
}

#[test]
fn infer_service_root_types_from_bfbs_handles_invalid_bfbs() {
    let (request, response) = infer_service_root_types_from_bfbs(&[1, 2, 3, 4], None, None);
    assert!(request.is_none());
    assert!(response.is_none());
}

#[test]
fn infer_aorta_header_layout_from_bfbs_supports_nonzero_field_ids() {
    let bfbs = build_test_header_layout_bfbs(4, 7, 9, true);
    let layout = infer_aorta_header_layout_from_bfbs(&bfbs).expect("layout");

    assert_eq!(layout.root_aorta_header_field_id, 4);
    assert_eq!(layout.header_publisher_node_field_id, 7);
    assert_eq!(layout.header_sequence_field_id, 9);
}

#[test]
fn infer_aorta_header_layout_from_bfbs_returns_none_without_root_header() {
    let bfbs = build_test_header_layout_bfbs(4, 7, 9, false);
    assert!(infer_aorta_header_layout_from_bfbs(&bfbs).is_none());
}
