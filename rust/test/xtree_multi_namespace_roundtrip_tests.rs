// Rust wrapper multi-namespace-tree module assembly.
//
// Compile + round-trip proof that a schema reaching BEYOND its own namespace
// tree + the aorta_base pair now produces a Rust wrapper that compiles and
// round-trips the cross-tree types. Pre-fix these wrappers failed to compile
// (E0412 / E0433 / E0659). Three cases:
//   1. xtree_foxglove -- cross-package nested foxglove tables (Vector3).
//   2. xtree_vln      -- bare-namespace root + aorta.sys + aorta.xtree.common.
//   3. xtree_service  -- service side, aorta.services.xvln + aorta.sys
//                        + aorta.services.common (status) + aorta.xtree.common.
//
// Each test USES the cross-tree types explicitly (constructs the typed Msg with
// them, then reads them back through flatc's raw generated accessors).
use aorta_sdk::{HeaderInjector, HeaderValues, TypedPublishMessage};
use flatbuffers::FlatBufferBuilder;

fn header_values() -> HeaderValues<'static> {
    HeaderValues {
        publish_stamp_ns: 1,
        publisher_node: "rust_test",
        sequence: 2,
    }
}

// Case 2: `xvln` (bare namespace root) reaching aorta.sys + aorta.xtree.common.
#[test]
fn xtree_vln_round_trips_cross_tree_enum() {
    use xtree_vln_msg::xtree_common_generated::aorta::xtree::common::XtreePhase;
    use xtree_vln_msg::prelude::XvlnPathSchema;
    use xtree_vln_msg::typed::XvlnPathMsg;
    use xtree_vln_msg::xvln::root_as_xvln_path;

    let mut builder = FlatBufferBuilder::new();
    let header = <XvlnPathSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = XvlnPathMsg {
        // Cross-tree enum from the THIRD namespace tree (aorta.xtree.common).
        phase: Some(XtreePhase::APPROACH),
    };
    let root =
        <XvlnPathSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_xvln_path(builder.finished_data()).expect("decode xvln_path");
    assert_eq!(decoded.phase(), XtreePhase::APPROACH);
}

// Case 3: service side, aorta.services.xvln reaching aorta.xtree.common + status.
#[test]
fn xtree_service_round_trips_cross_tree_enum_and_status() {
    use xtree_service_msg::xtree_common_generated::aorta::xtree::common::XtreePhase;
    use xtree_service_msg::prelude::{
        ControlFollowingRequestSchema, ControlFollowingResponseSchema,
    };
    use xtree_service_msg::schema_aorta::services::xvln::{
        root_as_control_following_request, ControlFollowingResponse,
    };
    use xtree_service_msg::status_generated::aorta::services::common::ServiceStatus;
    use xtree_service_msg::typed::{ControlFollowingRequestMsg, ControlFollowingResponseMsg};

    // Request: cross-tree enum (aorta.xtree.common).
    let mut rb = FlatBufferBuilder::new();
    let rh =
        <ControlFollowingRequestSchema as HeaderInjector>::build_header(&mut rb, &header_values());
    let req = ControlFollowingRequestMsg {
        phase: Some(XtreePhase::TRACK),
    };
    let rroot =
        <ControlFollowingRequestSchema as TypedPublishMessage>::build_typed_root(&mut rb, rh, &req);
    rb.finish(rroot, None);
    let dreq = root_as_control_following_request(rb.finished_data()).expect("decode request");
    assert_eq!(dreq.phase(), XtreePhase::TRACK);

    // Response: cross-tree status enum (aorta.services.common).
    let mut sb = FlatBufferBuilder::new();
    let sh =
        <ControlFollowingResponseSchema as HeaderInjector>::build_header(&mut sb, &header_values());
    let resp = ControlFollowingResponseMsg {
        status: Some(ServiceStatus::SUCCESS),
    };
    let sroot = <ControlFollowingResponseSchema as TypedPublishMessage>::build_typed_root(
        &mut sb, sh, &resp,
    );
    sb.finish(sroot, None);
    // ControlFollowingResponse is not the schema root_type, so decode via the
    // generic flatbuffers root (build_typed_root put it at the buffer root).
    let dresp =
        flatbuffers::root::<ControlFollowingResponse>(sb.finished_data()).expect("decode response");
    assert_eq!(dresp.status(), ServiceStatus::SUCCESS);
}

// Regression: TWO providers sharing a sub-namespace branch. `zzz`
// reaches aorta.xtree.common via BOTH xtree_common (XtreePhase) and
// xtree_common_extra (XtreeExtra) -- the shape a flat root glob mishandles
// (E0659 on the shared `xtree`). The recursive trie merges them.
#[test]
fn xtree_branch_round_trips_two_providers_same_subtree() {
    use xtree_branch_msg::xtree_common_extra_generated::aorta::xtree::common::XtreeExtra;
    use xtree_branch_msg::xtree_common_generated::aorta::xtree::common::XtreePhase;
    use xtree_branch_msg::prelude::ZzzBranchSchema;
    use xtree_branch_msg::typed::ZzzBranchMsg;
    use xtree_branch_msg::zzz::root_as_zzz_branch;

    let mut builder = FlatBufferBuilder::new();
    let header = <ZzzBranchSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = ZzzBranchMsg {
        phase: Some(XtreePhase::TRACK),
        extra: Some(XtreeExtra::HIGH),
    };
    let root =
        <ZzzBranchSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_zzz_branch(builder.finished_data()).expect("decode zzz_branch");
    assert_eq!(decoded.phase(), XtreePhase::TRACK);
    assert_eq!(decoded.extra(), XtreeExtra::HIGH);
}

// Regression: local namespace shares a DEPTH-2 prefix with a foreign
// tree. `aorta.branch.consumer` reaches `aorta.branch.common` (BranchPhase);
// the foreign `common` must merge into the consumer's OWN `pub mod branch`, not
// duplicate it (E0428). Compiling + round-tripping proves the merge.
#[test]
fn branch_consumer_round_trips_depth2_shared_prefix() {
    use branch_consumer_msg::branch_common_generated::aorta::branch::common::BranchPhase;
    use branch_consumer_msg::prelude::BranchConsumerSchema;
    use branch_consumer_msg::schema_aorta::branch::consumer::root_as_branch_consumer;
    use branch_consumer_msg::typed::BranchConsumerMsg;

    let mut builder = FlatBufferBuilder::new();
    let header =
        <BranchConsumerSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = BranchConsumerMsg {
        phase: Some(BranchPhase::CLOSED),
    };
    let root =
        <BranchConsumerSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_branch_consumer(builder.finished_data()).expect("decode branch_consumer");
    assert_eq!(decoded.phase(), BranchPhase::CLOSED);
}

// Regression: a foreign tree SHARING the local root AND a DISJOINT
// root reuse the same segment name (`xfoo`). aorta.services.collide reaches
// aorta.xfoo.common (shared -> nested `pub mod xfoo`) AND xfoo (disjoint, 2
// providers -> top-level `pub mod xfoo`). Pre-fix the loose idempotency check
// matched the nested module and skipped the disjoint synth (E0433).
#[test]
fn collide_consumer_round_trips_shared_and_disjoint_same_segment() {
    use collide_consumer_msg::collide_disjoint_a_generated::xfoo::CollideDisjA;
    use collide_consumer_msg::collide_disjoint_b_generated::xfoo::CollideDisjB;
    use collide_consumer_msg::collide_shared_generated::aorta::xfoo::common::CollideShared;
    use collide_consumer_msg::prelude::CollideConsumerSchema;
    use collide_consumer_msg::schema_aorta::services::collide::root_as_collide_consumer;
    use collide_consumer_msg::typed::CollideConsumerMsg;

    let mut builder = FlatBufferBuilder::new();
    let header =
        <CollideConsumerSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = CollideConsumerMsg {
        shared_phase: Some(CollideShared::A),
        disj_a: Some(CollideDisjA::X),
        disj_b: Some(CollideDisjB::Y),
    };
    let root = <CollideConsumerSchema as TypedPublishMessage>::build_typed_root(
        &mut builder,
        header,
        &msg,
    );
    builder.finish(root, None);

    let decoded = root_as_collide_consumer(builder.finished_data()).expect("decode collide");
    assert_eq!(decoded.shared_phase(), CollideShared::A);
    assert_eq!(decoded.disj_a(), CollideDisjA::X);
    assert_eq!(decoded.disj_b(), CollideDisjB::Y);
}

// Regression: THREE-LAYER same-segment. `xfoo` is BOTH a middle
// segment of the local namespace (aorta.xfoo.consumer -> flatc's nested
// `pub mod xfoo` at column 0) AND a disjoint root. The disjoint top-level synth
// must not be skipped by a column-0 idempotency anchor (E0433 pre-fix).
#[test]
fn xfoo_consumer_round_trips_three_layer_same_segment() {
    use xfoo_consumer_msg::collide_disjoint_a_generated::xfoo::CollideDisjA;
    use xfoo_consumer_msg::collide_disjoint_b_generated::xfoo::CollideDisjB;
    use xfoo_consumer_msg::collide_shared_generated::aorta::xfoo::common::CollideShared;
    use xfoo_consumer_msg::prelude::XfooConsumerSchema;
    use xfoo_consumer_msg::schema_aorta::xfoo::consumer::root_as_xfoo_consumer;
    use xfoo_consumer_msg::typed::XfooConsumerMsg;

    let mut builder = FlatBufferBuilder::new();
    let header =
        <XfooConsumerSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = XfooConsumerMsg {
        shared_phase: Some(CollideShared::A),
        disj_a: Some(CollideDisjA::X),
        disj_b: Some(CollideDisjB::Y),
    };
    let root =
        <XfooConsumerSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_xfoo_consumer(builder.finished_data()).expect("decode xfoo");
    assert_eq!(decoded.shared_phase(), CollideShared::A);
    assert_eq!(decoded.disj_a(), CollideDisjA::X);
    assert_eq!(decoded.disj_b(), CollideDisjB::Y);
}

// Regression: a foreign type in a MIXED-CASE namespace
// (ProbeCamel.HTTPThing). flatc snake_cases the module path to
// `probe_camel::httpthing`; the absolute rewrite must reproduce that exactly.
#[test]
fn camelcase_consumer_round_trips_snake_cased_namespace() {
    use camelcase_consumer_msg::camelcase_probe_generated::probe_camel::httpthing::CamelEnum;
    use camelcase_consumer_msg::prelude::CamelConsumerSchema;
    use camelcase_consumer_msg::schema_aorta::services::camel::root_as_camel_consumer;
    use camelcase_consumer_msg::typed::CamelConsumerMsg;

    let mut builder = FlatBufferBuilder::new();
    let header =
        <CamelConsumerSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = CamelConsumerMsg {
        probe: Some(CamelEnum::TWO),
    };
    let root =
        <CamelConsumerSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_camel_consumer(builder.finished_data()).expect("decode camel");
    assert_eq!(decoded.probe(), CamelEnum::TWO);
}

// Regression: a cross-crate `nested_flatbuffer` reference. flatc emits
// `payload_nested_flatbuffer()` returning a foreign type via a `super::…` path
// (missed by the rewrite pre-fix -> E0433). The rewrite now models the attribute
// target and rewrites it to an absolute path.
#[test]
fn nested_consumer_round_trips_cross_crate_nested_flatbuffer() {
    use nested_consumer_msg::nested_dep_generated::aorta::nestdep::common::{
        NestedRoot, NestedRootArgs,
    };
    use nested_consumer_msg::prelude::NestedConsumerSchema;
    use nested_consumer_msg::schema_aorta::nestdep::consumer::root_as_nested_consumer;
    use nested_consumer_msg::typed::NestedConsumerMsg;

    // Build a NestedRoot buffer (from the foreign nested_dep crate).
    let mut nb = FlatBufferBuilder::new();
    let label = nb.create_string("nested");
    let nr = NestedRoot::create(
        &mut nb,
        &NestedRootArgs {
            value: 42,
            label: Some(label),
        },
    );
    nb.finish(nr, None);
    let nested_bytes = nb.finished_data().to_vec();

    let mut builder = FlatBufferBuilder::new();
    let header =
        <NestedConsumerSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = NestedConsumerMsg {
        payload: Some(&nested_bytes),
    };
    let root =
        <NestedConsumerSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_nested_consumer(builder.finished_data()).expect("decode nested");
    let nested = decoded
        .payload_nested_flatbuffer()
        .expect("nested_flatbuffer present");
    assert_eq!(nested.value(), 42);
    assert_eq!(nested.label(), Some("nested"));
}

// Regression: a FOREIGN-ROOT-ONLY schema. `foreign_root_only.fbs`
// declares no local types; its `root_type` is the foreign
// aorta.nestdep.common.NestedRoot. flatc emits the `root_as_nested_root`
// helper set in that (multi-segment) namespace, referencing the foreign type by
// BARE name. The dep's single-level namespace re-export cannot resolve it via
// the glob -> pre-fix E0412/E0433. The rewrite now collects the foreign
// root_type and rewrites its same-namespace bare reference to the absolute
// `crate::nested_dep_generated::aorta::nestdep::common::NestedRoot`. This test
// round-trips a buffer THROUGH the rewritten accessor.
#[test]
fn foreign_root_only_round_trips_via_rewritten_root_accessor() {
    use foreign_root_only_msg::nested_dep_generated::aorta::nestdep::common::{
        NestedRoot, NestedRootArgs,
    };
    // `schema_aorta` == the wrapper's own flatc module; the `root_as_nested_root`
    // helper here is the one whose foreign-type reference was rewritten.
    use foreign_root_only_msg::schema_aorta::nestdep::common::root_as_nested_root;

    // Build a NestedRoot buffer via the foreign (dep) crate's builder.
    let mut builder = FlatBufferBuilder::new();
    let label = builder.create_string("froot");
    let nr = NestedRoot::create(
        &mut builder,
        &NestedRootArgs {
            value: 7,
            label: Some(label),
        },
    );
    builder.finish(nr, None);

    // Decode through the foreign-root-only wrapper's rewritten root accessor.
    let decoded = root_as_nested_root(builder.finished_data()).expect("decode foreign root");
    assert_eq!(decoded.value(), 7);
    assert_eq!(decoded.label(), Some("froot"));
}

// Regression (corruption case A): a foreign root `deep.ABCD` paired with
// `file_identifier "ABCD"`. flatc emits BOTH same-namespace bare type references
// (`ABCD` in the root_as_*/finish_* helpers -> must be rewritten) AND the string
// literal `pub const ABCD_IDENTIFIER: &str = "ABCD";` (the on-wire identifier ->
// must NOT be rewritten). An earlier whole-line rewrite corrupted the string;
// this asserts the identifier bytes are unchanged AND the type refs still resolve.
#[test]
fn abcd_froot_preserves_file_identifier_bytes() {
    use abcd_froot_msg::abcd_dep_generated::deep::{ABCDArgs, ABCD};
    use abcd_froot_msg::deep::{finish_abcd_buffer, root_as_abcd, ABCD_IDENTIFIER};

    // The file_identifier const must survive the same-namespace rewrite verbatim.
    assert_eq!(ABCD_IDENTIFIER, "ABCD");

    let mut builder = FlatBufferBuilder::new();
    let root = ABCD::create(&mut builder, &ABCDArgs { v: 5 });
    finish_abcd_buffer(&mut builder, root);
    let buf = builder.finished_data();

    // FlatBuffers stores the 4-byte file identifier at bytes [4..8]; corrupting
    // ABCD_IDENTIFIER (the whole-line-rewrite bug) would have written the wrong bytes here.
    assert_eq!(&buf[4..8], b"ABCD");
    assert!(flatbuffers::buffer_has_identifier(buf, "ABCD", false));

    // The rewritten root accessor (foreign type reference) still round-trips.
    let decoded = root_as_abcd(buf).expect("decode ABCD via rewritten root accessor");
    assert_eq!(decoded.v(), 5);
}

// Regression (corruption case B): a LOCAL enum variant `ABCD` sharing a
// bare name with a same-namespace FOREIGN type `ABCD`. flatc emits the variant
// DEFINITION `pub const ABCD: Self = Self(1);` (a value) in the same file as the
// foreign type reference. An earlier whole-line rewrite corrupted the variant
// definition into a syntax error; the compile of this fixture is itself the
// primary proof. This round-trip additionally exercises both the variant (as a
// value) and the rewritten foreign type reference.
#[test]
fn enum_shadow_round_trips_with_same_named_local_variant() {
    use enum_shadow_consumer_msg::prelude::EnumShadowSchema;
    use enum_shadow_consumer_msg::same::pkg::{root_as_enum_shadow, Local};
    use enum_shadow_consumer_msg::typed::{ABCDMsg, EnumShadowMsg};

    let mut builder = FlatBufferBuilder::new();
    let header = <EnumShadowSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = EnumShadowMsg {
        // Local enum variant named `ABCD` (a VALUE) -- must NOT be rewritten.
        choice: Some(Local::ABCD),
        // Same-namespace FOREIGN type `ABCD` (a TYPE) -- rewritten to absolute.
        nested: Some(Box::new(ABCDMsg { v: Some(99) })),
    };
    let root =
        <EnumShadowSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_enum_shadow(builder.finished_data()).expect("decode enum shadow");
    assert_eq!(decoded.choice(), Local::ABCD);
    assert_eq!(decoded.nested().expect("nested present").v(), 99);
}

// Regression: a DEEP same-namespace consumer referencing a foreign
// enum/struct/table in EVERY field shape flatc emits. Pre-fix the rewrite
// anchors missed the COMMA (`Vector<'a, T>`, `Array<'a, T, N>`), single-COLON
// (`pub field: T`, `x: T`) and BRACKET (`&[T; N]`) type positions, so the deep
// (depth-3) references dangled (E0412). This fixture COMPILING is the primary
// proof that all positions are now rewritten; the round-trip additionally
// exercises the direct-enum, direct-struct, vector<enum> and vector<struct>
// accessors at runtime.
#[test]
fn deepshadow_round_trips_all_foreign_field_shapes() {
    use deepshadow_consumer_msg::deepshadow_dep_generated::aorta::shadowdeep::common::{
        ShadowEnum, ShadowStruct,
    };
    use deepshadow_consumer_msg::prelude::DeepShadowSchema;
    use deepshadow_consumer_msg::schema_aorta::shadowdeep::common::root_as_deep_shadow;
    use deepshadow_consumer_msg::typed::DeepShadowMsg;

    let mut builder = FlatBufferBuilder::new();
    let header = <DeepShadowSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = DeepShadowMsg {
        de: Some(ShadowEnum::ON),       // direct enum: `-> T`, `: T`, default
        ds: Some(ShadowStruct::new(7)), // direct struct: `Option<&'a T>`
        dt: None,
        ve: Some(vec![ShadowEnum::NONE, ShadowEnum::ON]), // Vector<'a, T> (comma)
        vs: Some(vec![ShadowStruct::new(1), ShadowStruct::new(2)]), // Vector<'a, T>
        vt: None,
        sw: None, // the ShadowWrap fixed-array (`&[T; N]`) path is compile-verified
    };
    let root =
        <DeepShadowSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_deep_shadow(builder.finished_data()).expect("decode deep shadow");
    assert_eq!(decoded.de(), ShadowEnum::ON);
    assert_eq!(decoded.ds().expect("ds present").x(), 7);
    let ve = decoded.ve().expect("ve present");
    assert_eq!(ve.len(), 2);
    assert_eq!(ve.get(1), ShadowEnum::ON);
    let vs = decoded.vs().expect("vs present");
    assert_eq!(vs.len(), 2);
    assert_eq!(vs.get(1).x(), 2);
}

// Regression: a DEEP same-namespace foreign UNION. flatc emits the union
// discriminant `UShadow` in type positions AND value paths (all rewritten to
// absolute) AND inside the variant-name STRING LITERAL `"UShadow::AlphaLeaf"`
// (which the `::`-followed rewrite would corrupt without string-literal masking).
// This fixture compiling proves the type/value refs resolve; the round-trip
// exercises the rewritten union accessors at runtime.
#[test]
fn union_shadow_round_trips_deep_foreign_union() {
    use union_shadow_consumer_msg::prelude::UnionShadowSchema;
    use union_shadow_consumer_msg::schema_aorta::unionshadow::common::root_as_union_shadow;
    use union_shadow_consumer_msg::typed::{AlphaLeafMsg, UShadowMsg, UnionShadowMsg};
    use union_shadow_consumer_msg::union_dep_generated::aorta::unionshadow::common::UShadow;

    let mut builder = FlatBufferBuilder::new();
    let header =
        <UnionShadowSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = UnionShadowMsg {
        pick: Some(UShadowMsg::AlphaLeaf(Box::new(AlphaLeafMsg { a: Some(5) }))),
    };
    let root =
        <UnionShadowSchema as TypedPublishMessage>::build_typed_root(&mut builder, header, &msg);
    builder.finish(root, None);

    let decoded = root_as_union_shadow(builder.finished_data()).expect("decode union shadow");
    assert_eq!(decoded.pick_type(), UShadow::AlphaLeaf);
    assert_eq!(decoded.pick_as_alpha_leaf().expect("alpha present").a(), 5);
}

// Regression: a schema with a LOCAL `Marker` type in `mrlocal.inner` AND
// a foreign `root_type` (`mrforeign.deep.ForeignRoot`) in a DIFFERENT namespace.
// flatc emits TWO pub-mod nests in one file -- the foreign-root helpers in
// `mrforeign::deep` (referencing `ForeignRoot` BARE, the leaf that needed the
// injected alias) and the local `Marker` in `mrlocal::inner`. Round-trips through
// BOTH nests.
#[test]
fn mixedroot_round_trips_foreign_root_helpers_and_local_type() {
    use mixedroot_consumer_msg::mixedroot_consumer_generated::mrforeign::deep::{
        finish_foreign_root_buffer, root_as_foreign_root,
    };
    use mixedroot_consumer_msg::mixedroot_dep_generated::mrforeign::deep::{
        ForeignRoot, ForeignRootArgs,
    };
    use mixedroot_consumer_msg::mrlocal::inner::{Marker, MarkerArgs};

    // 1. Foreign root_type helpers (mrforeign::deep nest -- the leaf whose bare
    //    `ForeignRoot` references are resolved by the injected `use` alias).
    let mut builder = FlatBufferBuilder::new();
    let root = ForeignRoot::create(&mut builder, &ForeignRootArgs { payload: 42 });
    finish_foreign_root_buffer(&mut builder, root);
    let decoded = root_as_foreign_root(builder.finished_data()).expect("decode foreign root");
    assert_eq!(decoded.payload(), 42);

    // 2. Local type (mrlocal::inner nest).
    let mut mb = FlatBufferBuilder::new();
    let marker = Marker::create(&mut mb, &MarkerArgs { value: 7 });
    mb.finish(marker, None);
    let decoded_marker = flatbuffers::root::<Marker>(mb.finished_data()).expect("decode marker");
    assert_eq!(decoded_marker.value(), 7);

    // 3. Schema-meta FQN: the foreign root_type's SchemaMeta
    //    must report its OWN namespace (`mrforeign.deep.ForeignRoot`), NOT the
    //    local one (`mrlocal.inner.ForeignRoot`, the pre-fix wrong value) -- the
    //    root-type name is wire-relevant (schema discovery). The local Marker keeps
    //    its own (local) namespace.
    use aorta_sdk::SchemaMeta;
    use mixedroot_consumer_msg::schema::{ForeignRootSchema, MarkerSchema};
    assert_eq!(
        <ForeignRootSchema as SchemaMeta>::root_type(),
        Some("mrforeign.deep.ForeignRoot")
    );
    assert_eq!(
        <MarkerSchema as SchemaMeta>::root_type(),
        Some("mrlocal.inner.Marker")
    );
}

// Regression: a SHARED-PREFIX FORK. Local ns `sfork.leaf` + foreign root
// `sfork.mid.leaf` share the `sfork` prefix, so flatc emits TWO `pub mod leaf`
// nests (`sfork::mid::leaf` and `sfork::leaf`). The foreign-root alias must land in
// `sfork::mid::leaf` and the same-namespace `sfork.leaf.LocalDep` field alias in
// `sfork::leaf` -- a first-match leaf search injects the local alias into the wrong
// (first) `leaf`, leaving `sfork::leaf` dangling (pre-fix E0412). The full-path
// stack finder places each correctly; this round-trips through BOTH leaves.
#[test]
fn sharedfork_round_trips_shared_prefix_fork_leaves() {
    use sharedfork_consumer_msg::sfork::leaf::{LocalHolder, LocalHolderArgs};
    use sharedfork_consumer_msg::sharedfork_consumer_generated::sfork::mid::leaf::{
        finish_foreign_root_buffer, root_as_foreign_root,
    };
    use sharedfork_consumer_msg::sharedfork_dep_generated::sfork::leaf::{LocalDep, LocalDepArgs};
    use sharedfork_consumer_msg::sharedfork_dep_generated::sfork::mid::leaf::{
        ForeignRoot, ForeignRootArgs,
    };

    // Foreign root helpers (sfork::mid::leaf nest -- its alias leaf).
    let mut b = FlatBufferBuilder::new();
    let root = ForeignRoot::create(&mut b, &ForeignRootArgs { payload: 42 });
    finish_foreign_root_buffer(&mut b, root);
    let decoded = root_as_foreign_root(b.finished_data()).expect("decode foreign root");
    assert_eq!(decoded.payload(), 42);

    // Local LocalHolder holding a same-namespace foreign LocalDep (sfork::leaf nest).
    let mut mb = FlatBufferBuilder::new();
    let dep = LocalDep::create(&mut mb, &LocalDepArgs { d: 9 });
    let holder = LocalHolder::create(&mut mb, &LocalHolderArgs { dep: Some(dep) });
    mb.finish(holder, None);
    let decoded_holder =
        flatbuffers::root::<LocalHolder>(mb.finished_data()).expect("decode holder");
    assert_eq!(decoded_holder.dep().expect("dep present").d(), 9);
}

// Case 1: aorta.services.xtree nesting cross-package foxglove tables.
#[test]
fn xtree_foxglove_round_trips_cross_package_table() {
    use xtree_foxglove_msg::prelude::XtreeFoxgloveProbeSchema;
    use xtree_foxglove_msg::schema_aorta::services::xtree::root_as_xtree_foxglove_probe;
    use xtree_foxglove_msg::typed::{Vector3Msg, XtreeFoxgloveProbeMsg};

    let mut builder = FlatBufferBuilder::new();
    let header =
        <XtreeFoxgloveProbeSchema as HeaderInjector>::build_header(&mut builder, &header_values());
    let msg = XtreeFoxgloveProbeMsg {
        // Cross-package nested foxglove.Vector3 table (the E0412 site pre-fix).
        offset: Some(Box::new(Vector3Msg {
            x: Some(1.5),
            y: Some(-2.5),
            z: Some(3.5),
        })),
        cloud: None,
    };
    let root = <XtreeFoxgloveProbeSchema as TypedPublishMessage>::build_typed_root(
        &mut builder,
        header,
        &msg,
    );
    builder.finish(root, None);

    let decoded =
        root_as_xtree_foxglove_probe(builder.finished_data()).expect("decode xtree_foxglove");
    let offset = decoded.offset().expect("offset present");
    assert_eq!(offset.x(), 1.5);
    assert_eq!(offset.y(), -2.5);
    assert_eq!(offset.z(), 3.5);
}
