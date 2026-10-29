// Example-fixture proof for the generated Rust i8-vector fast path.
//
// The generated Rust builder has a zero-copy fast path for `[ubyte]`/`[byte]`
// vector fields (`create_u8_vector_fast` / `create_i8_vector_fast`, both built
// on `FlatBufferBuilder::create_byte_string`). Its gate keys on the CANONICAL
// scalar keyword (`ubyte`/`byte`). Previously the wrapper's field types
// came from the regex text parser, which preserved the source's spelling —
// so a field declared `[uint8]`/`[int8]` (long form) silently missed the fast
// path and used the slower generic `builder.create_vector(value)`. The wrapper
// now takes field types from the compiled bfbs (`schema_reflection`), whose
// scalar keywords are always canonical, so `[uint8]`/`[int8]` fields now take
// the same fast path as `[ubyte]`/`[byte]` fields — a semantically-equivalent
// correction (identical `Vector<u8>`/`Vector<i8>` on the wire).
//
// The fixture field `demo.AllTypes.signed_bytes:[int8]` forces
// `create_i8_vector_fast`, including its unsafe i8-to-u8 reinterpretation.
// A full negative/positive i8 range proves that the generated builder and raw
// accessor preserve every bit across the sign boundary.

use aorta_sdk::{HeaderInjector, HeaderValues, TypedPublishMessage};
use flatbuffers::FlatBufferBuilder;
use all_types_msg::demo::root_as_all_types;
use all_types_msg::prelude::AllTypesSchema;
use all_types_msg::typed::AllTypesMsg;

#[test]
fn example_i8_vector_fast_roundtrip() {
    let mut builder = FlatBufferBuilder::new();
    let header_values = HeaderValues {
        publish_stamp_ns: 1,
        publisher_node: "rust_test",
        sequence: 2,
    };
    let header =
        <AllTypesSchema as HeaderInjector>::build_header(&mut builder, &header_values);

    // Span the full signed-8-bit range across the sign boundary — the exact
    // values the `create_i8_vector_fast` `as *const u8` reinterpret must
    // preserve bit-for-bit.
    let signed_bytes: [i8; 6] = [i8::MIN, -100, -1, 0, 1, i8::MAX];
    let msg = AllTypesMsg {
        signed_bytes: Some(&signed_bytes),
        ..Default::default()
    };

    let root =
        <AllTypesSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded =
        root_as_all_types(builder.finished_data()).expect("decode AllTypes root");
    let out = decoded.signed_bytes().expect("signed byte vector present");
    assert_eq!(
        out.len(),
        signed_bytes.len(),
        "signed-byte length must round-trip"
    );
    for (idx, expected) in signed_bytes.iter().enumerate() {
        assert_eq!(
            out.get(idx),
            *expected,
            "signed_bytes[{}] must round-trip through create_i8_vector_fast",
            idx,
        );
    }
    assert!(decoded.aorta_header().is_some(), "header must be present");
}
