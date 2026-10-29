use aorta_sdk::{HeaderInjector, HeaderValues, TypedPublishMessage};
use flatbuffers::FlatBufferBuilder;
use reporter_namespace_probe_msg::prelude::ReporterNamespaceProbeSchema;
use reporter_namespace_probe_msg::reporter_fixture::root_as_reporter_namespace_probe;
use reporter_namespace_probe_msg::typed::{ReporterNamespaceProbeMsg, Vector3Msg};
use reporter_namespace_probe_msg::xtree_common_generated::aorta::xtree::common::XtreePhase;

#[test]
fn three_namespace_trees_compile_and_round_trip() {
    let mut builder = FlatBufferBuilder::new();
    let header = <ReporterNamespaceProbeSchema as HeaderInjector>::build_header(
        &mut builder,
        &HeaderValues {
            publish_stamp_ns: 1,
            publisher_node: "fixture-test",
            sequence: 2,
        },
    );
    let msg = ReporterNamespaceProbeMsg {
        phase: Some(XtreePhase::TRACK),
        offset: Some(Box::new(Vector3Msg {
            x: Some(1.0),
            y: Some(2.0),
            z: Some(3.0),
        })),
        label: Some("reporter-fixture".to_string()),
    };
    let root = <ReporterNamespaceProbeSchema as TypedPublishMessage>::build_typed_root(
        &mut builder,
        header,
        &msg,
    );
    builder.finish(root, None);

    let decoded = root_as_reporter_namespace_probe(builder.finished_data())
        .expect("decode reporter fixture");
    assert_eq!(decoded.phase(), XtreePhase::TRACK);
    assert_eq!(decoded.label(), Some("reporter-fixture"));
    let offset = decoded.offset().expect("offset");
    assert_eq!(offset.x(), 1.0);
    assert_eq!(offset.z(), 3.0);
}
