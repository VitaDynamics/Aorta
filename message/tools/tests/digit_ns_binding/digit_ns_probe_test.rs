// #375 regression: the fixture crate COMPILING is the proof, and this test is
// what keeps it compiling under `bazel test //... --build_tests_only` (no
// other test depends on the fixture). Every path below goes through a module
// flatc emitted as `p_05` for `namespace p05.probe;` -- the wrapper's
// top-level re-export, the prelude re-export, and the typed Root impl.
use aorta_core::schema::SchemaMeta;
use digit_ns_probe_rust_binding::p_05::probe::{DigitNsProbeData, DigitNsProbeDataArgs};
use digit_ns_probe_rust_binding::prelude::DigitNsProbeDataSchema;

#[test]
fn digit_boundary_namespace_paths_resolve() {
    let mut b = flatbuffers::FlatBufferBuilder::new();
    let root = DigitNsProbeData::create(&mut b, &DigitNsProbeDataArgs { value: 375 });
    b.finish(root, None);

    let decoded = flatbuffers::root::<DigitNsProbeData>(b.finished_data()).expect("decode");
    assert_eq!(decoded.value(), 375);

    // The prelude re-exports the same mangled module.
    let via_prelude = flatbuffers::root::<
        digit_ns_probe_rust_binding::prelude::p_05::probe::DigitNsProbeData,
    >(b.finished_data())
    .expect("decode via prelude");
    assert_eq!(via_prelude.value(), 375);

    // The ONLY place the unmangled `p05` spelling belongs: the schema FQN
    // string, which must keep the schema's own dotted spelling.
    assert_eq!(
        DigitNsProbeDataSchema::root_type(),
        Some("p05.probe.DigitNsProbeData")
    );
}
