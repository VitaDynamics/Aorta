// Regression: the Tier-2 `mns_consumer` crate COMPILING is
// the proof. Its root-local types live in TWO namespaces (`p::alpha`, `p::beta`),
// and `p::beta::BetaThing` references the same-namespace foreign `p::beta::BetaDep`
// -- flatc emits it BARE in the `p::beta` nest. If the patcher computed every
// reference against the first local namespace (`p::alpha`), the `p::beta` alias
// would be misplaced and `BetaThing::dep` would fail to compile.
use mns_consumer_rust::mns_dep_generated::p::beta::{BetaDep, BetaDepArgs};
use mns_consumer_rust::p::alpha::{AlphaThing, AlphaThingArgs};
use mns_consumer_rust::p::beta::{BetaThing, BetaThingArgs};

#[test]
fn tier2_multi_namespace_both_nests_resolve() {
    let mut b = flatbuffers::FlatBufferBuilder::new();

    // p::alpha nest.
    let alpha = AlphaThing::create(&mut b, &AlphaThingArgs { a: 3 });
    b.finish(alpha, None);
    let da = flatbuffers::root::<AlphaThing>(b.finished_data()).expect("decode alpha");
    assert_eq!(da.a(), 3);

    // p::beta nest with a same-namespace foreign BetaDep (the ref that needed the
    // per-emission-namespace alias).
    let mut mb = flatbuffers::FlatBufferBuilder::new();
    let dep = BetaDep::create(&mut mb, &BetaDepArgs { v: 11 });
    let beta = BetaThing::create(&mut mb, &BetaThingArgs { dep: Some(dep) });
    mb.finish(beta, None);
    let db = flatbuffers::root::<BetaThing>(mb.finished_data()).expect("decode beta");
    assert_eq!(db.dep().expect("dep present").v(), 11);
}
