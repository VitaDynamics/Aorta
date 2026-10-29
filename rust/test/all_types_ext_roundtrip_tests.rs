// Codegen golden: compile + round-trip proof for the type-support matrix rows
// all_types.fbs does NOT cover, on the Rust side:
//   - `[bool]` (`flags`)                    -- Rust vector_scalar path (`&[bool]`)
//   - union with a MEMBER ALIAS (`Chosen`)  -- 2C alias/target model
//   - qualified `root_type demo.ext.AllTypesExt` (loader root-type parse)
//
// Build through the public typed-publish API, then decode via flatc's own raw
// generated accessors and assert every field survives.
use all_types_ext_msg::demo;
use all_types_ext_msg::demo::ext::root_as_all_types_ext;
use all_types_ext_msg::prelude::*;
use all_types_ext_msg::typed::{AllTypesExtMsg, AltLeafMsg, AltPayloadMsg};
use aorta_sdk::{HeaderInjector, HeaderValues, TypedPublishMessage};
use flatbuffers::FlatBufferBuilder;

#[test]
fn all_types_ext_round_trips_bool_vector_and_union_alias() {
    let mut builder = FlatBufferBuilder::new();
    let header_values = HeaderValues {
        publish_stamp_ns: 1,
        publisher_node: "rust_test",
        sequence: 2,
    };
    let header = <AllTypesExtSchema as HeaderInjector>::build_header(&mut builder, &header_values);

    let msg = AllTypesExtMsg {
        // [bool]: a distinguishable, non-uniform pattern so a dropped bit or a
        // wrong element width reads back wrong.
        flags: Some(&[true, false, true, true, false]),
        toggle: Some(true),
        // union member alias: `Chosen: AltLeaf` -- constructed by the ALIAS.
        payload: Some(AltPayloadMsg::Chosen(Box::new(AltLeafMsg { id: Some(77) }))),
        levels: Some(vec![demo::ext::Level::ONE, demo::ext::Level::TWO]),
        names: Some(vec!["alice".to_string(), "bob".to_string()]),
    };

    let root =
        <AllTypesExtSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_all_types_ext(builder.finished_data()).expect("decode all_types_ext");

    let flags = decoded.flags().expect("flags present");
    assert_eq!(flags.len(), 5);
    assert!(flags.get(0));
    assert!(!flags.get(1));
    assert!(flags.get(2));
    assert!(flags.get(3));
    assert!(!flags.get(4));

    assert!(decoded.toggle());

    // union alias: flatc keys the discriminant + `_as_chosen()` accessor by the
    // ALIAS "Chosen", not the target bare name "AltLeaf".
    let leaf = decoded.payload_as_chosen().expect("payload is Chosen");
    assert_eq!(leaf.id(), 77);

    let levels = decoded.levels().expect("levels present");
    assert_eq!(levels.len(), 2);
    assert_eq!(levels.get(0), demo::ext::Level::ONE);
    assert_eq!(levels.get(1), demo::ext::Level::TWO);

    let names = decoded.names().expect("names present");
    assert_eq!(names.len(), 2);
    assert_eq!(names.get(0), "alice");
    assert_eq!(names.get(1), "bob");
}
