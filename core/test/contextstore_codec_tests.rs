use super::{
    context_value_schema_hash, encode_value_bool, encode_value_f64, encode_value_i64,
    encode_value_string, encode_value_u64, root_as_context_value, ContextKind, ContextValueMeta,
    CONTEXT_VALUE_BFBS,
};

fn meta(seq: u64) -> ContextValueMeta {
    ContextValueMeta {
        stamp_ns: 1_700_000_000_000_000_000,
        expires_at_ns: 1_700_000_000_500_000_000,
        source_node: "rcp_node".to_string(),
        source_instance_id: "0123456789abcdef".to_string(),
        sequence: seq,
    }
}

#[test]
fn round_trip_bool() {
    let bytes = encode_value_bool(true, &meta(1));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::Bool);
    assert!(v.bool_value());
    assert_eq!(v.stamp_ns(), 1_700_000_000_000_000_000);
    assert_eq!(v.expires_at_ns(), 1_700_000_000_500_000_000);
    assert_eq!(v.source_node().expect("source_node"), "rcp_node");
    assert_eq!(
        v.source_instance_id().expect("instance"),
        "0123456789abcdef"
    );
    assert_eq!(v.sequence(), 1);
}

#[test]
fn round_trip_bool_false_default_path() {
    let bytes = encode_value_bool(false, &meta(2));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::Bool);
    assert!(!v.bool_value());
}

#[test]
fn round_trip_i64() {
    let bytes = encode_value_i64(-12345, &meta(3));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::I64);
    assert_eq!(v.i64_value(), -12345);
}

#[test]
fn round_trip_u64() {
    let bytes = encode_value_u64(42, &meta(4));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::U64);
    assert_eq!(v.u64_value(), 42);
}

#[test]
fn round_trip_u64_max_value() {
    let bytes = encode_value_u64(u64::MAX, &meta(5));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.u64_value(), u64::MAX);
}

#[test]
fn round_trip_f64() {
    // Use the actual PI constant rather than a 3.14159 literal — clippy
    // (correctness::approx_constant) flags the literal as a likely typo
    // for the named constant. The test value is arbitrary; PI works.
    let bytes = encode_value_f64(std::f64::consts::PI, &meta(6));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::F64);
    assert!((v.f64_value() - std::f64::consts::PI).abs() < 1e-9);
}

#[test]
fn round_trip_string() {
    let bytes = encode_value_string("RUNNING", &meta(7));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::String);
    assert_eq!(v.string_value().expect("string"), "RUNNING");
}

#[test]
fn round_trip_empty_string() {
    let bytes = encode_value_string("", &meta(8));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::String);
    assert_eq!(v.string_value().expect("string"), "");
}

#[test]
fn no_expiry_default_zero() {
    // Writers that omit expires_at_ns leave it at the FlatBuffers default (0)
    // which the spec defines as "no expiry".
    let m = ContextValueMeta {
        stamp_ns: 100,
        expires_at_ns: 0,
        source_node: "n".to_string(),
        source_instance_id: "i".to_string(),
        sequence: 1,
    };
    let bytes = encode_value_bool(true, &m);
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.expires_at_ns(), 0);
}

#[test]
fn root_as_returns_none_on_garbage() {
    assert!(root_as_context_value(&[]).is_none());
    assert!(root_as_context_value(&[0xff, 0xff, 0xff, 0xff]).is_none());
}

#[test]
fn kind_byte_is_stable() {
    assert_eq!(ContextKind::None.as_byte(), 0);
    assert_eq!(ContextKind::Bool.as_byte(), 1);
    assert_eq!(ContextKind::I64.as_byte(), 2);
    assert_eq!(ContextKind::U64.as_byte(), 3);
    assert_eq!(ContextKind::F64.as_byte(), 4);
    assert_eq!(ContextKind::String.as_byte(), 5);
}

#[test]
fn kind_from_byte_round_trips() {
    for kind in [
        ContextKind::None,
        ContextKind::Bool,
        ContextKind::I64,
        ContextKind::U64,
        ContextKind::F64,
        ContextKind::String,
    ] {
        let b = kind.as_byte();
        assert_eq!(ContextKind::from_byte(b), Some(kind));
    }
    assert_eq!(ContextKind::from_byte(99), None);
    assert_eq!(ContextKind::from_byte(-1), None);
}

#[test]
fn kind_typed_field_isolation() {
    // Encoding as Bool must NOT populate the i64/u64/f64/string slots.
    let bytes = encode_value_bool(true, &meta(1));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::Bool);
    assert_eq!(v.i64_value(), 0);
    assert_eq!(v.u64_value(), 0);
    assert_eq!(v.f64_value(), 0.0);
    assert!(v.string_value().is_none());
}

#[test]
fn string_typed_field_isolation() {
    let bytes = encode_value_string("hello", &meta(1));
    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind(), ContextKind::String);
    assert_eq!(v.string_value().expect("string"), "hello");
    // bool/i64/u64/f64 default to zero.
    assert!(!v.bool_value());
    assert_eq!(v.u64_value(), 0);
}

#[test]
fn round_trip_kind_none_explicit() {
    // Explicitly construct a ContextValue with kind = None to exercise the
    // wire path that should not write any typed value field.
    use flatbuffers::FlatBufferBuilder;

    let mut builder = FlatBufferBuilder::new();
    let node_off = builder.create_string("n");
    let instance_off = builder.create_string("i");
    let root = super::create_context_value(
        &mut builder,
        ContextKind::None,
        false,
        0,
        0,
        0.0,
        None,
        100,
        0,
        Some(node_off),
        Some(instance_off),
        7,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();

    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind_raw(), 0);
    assert_eq!(v.kind(), ContextKind::None);
    assert!(!v.bool_value());
    assert_eq!(v.i64_value(), 0);
    assert_eq!(v.u64_value(), 0);
    assert_eq!(v.f64_value(), 0.0);
    assert!(v.string_value().is_none());
    assert_eq!(v.stamp_ns(), 100);
    assert_eq!(v.sequence(), 7);
}

#[test]
fn unknown_kind_byte_does_not_panic() {
    // Hand-craft a buffer with an out-of-range discriminant. The decoder
    // should fold it to None via from_byte's fallback rather than panic.
    use flatbuffers::FlatBufferBuilder;

    let mut builder = FlatBufferBuilder::new();
    let node_off = builder.create_string("n");
    let instance_off = builder.create_string("i");
    let off = {
        let start = builder.start_table();
        builder.push_slot::<i8>(4, 99i8, 0);
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(20, node_off);
        builder.push_slot_always::<flatbuffers::WIPOffset<_>>(22, instance_off);
        let end = builder.end_table(start);
        flatbuffers::WIPOffset::<super::ContextValue>::new(end.value())
    };
    builder.finish(off, None);
    let bytes = builder.finished_data().to_vec();

    let v = root_as_context_value(&bytes).expect("decode");
    assert_eq!(v.kind_raw(), 99);
    assert_eq!(v.kind(), ContextKind::None);
}

#[test]
fn context_value_schema_hash_is_64_hex_and_stable() {
    let h = context_value_schema_hash();
    assert_eq!(h.to_hex().len(), 64);
    assert_eq!(
        h,
        crate::schema::SchemaHash::from_bfbs_bytes(CONTEXT_VALUE_BFBS)
    );
}

#[test]
fn committed_bfbs_describes_context_value_root() {
    // The committed bfbs must be the ContextValue reflection schema (it names
    // the root type), so a recorder embedding it produces a channel MCAP
    // readers can actually parse. A cheap structural check that the bytes are
    // the right schema, complementing the byte-exact drift test below.
    assert!(
        CONTEXT_VALUE_BFBS
            .windows(b"aorta.ctx.ContextValue".len())
            .any(|w| w == b"aorta.ctx.ContextValue"),
        "CONTEXT_VALUE_BFBS does not name aorta.ctx.ContextValue"
    );
}

#[cfg(has_fresh_bfbs)]
include!(concat!(env!("OUT_DIR"), "/contextstore_bfbs_fresh.rs"));

#[cfg(has_fresh_bfbs)]
#[test]
fn context_value_bfbs_constant_matches_compiled_schema() {
    assert_eq!(
        CONTEXT_VALUE_BFBS, CONTEXT_VALUE_BFBS_FRESH,
        "CONTEXT_VALUE_BFBS is stale; re-generate from \
         message/schemas/system/contextstore.fbs with //message:flatc"
    );
}
