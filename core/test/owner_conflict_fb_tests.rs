use super::{encode_owner_conflict_event, root_as_owner_conflict_event};

#[test]
fn round_trip_owner_conflict_event() {
    let bytes = encode_owner_conflict_event(
        "ctx/demo/state",
        "rcp_node",
        "self_instance_xyz",
        "rcp_node",
        "foreign_instance_abc",
        1_700_000_000_111_111_111,
        42,
        1_700_000_000_222_222_222,
    );
    let evt = root_as_owner_conflict_event(&bytes).expect("decode");
    assert_eq!(evt.key().expect("key"), "ctx/demo/state");
    assert_eq!(evt.self_node().expect("self_node"), "rcp_node");
    assert_eq!(
        evt.self_instance_id().expect("self_instance"),
        "self_instance_xyz"
    );
    assert_eq!(evt.foreign_node().expect("foreign_node"), "rcp_node");
    assert_eq!(
        evt.foreign_instance_id().expect("foreign_instance"),
        "foreign_instance_abc"
    );
    assert_eq!(evt.foreign_stamp(), 1_700_000_000_111_111_111);
    assert_eq!(evt.foreign_seq(), 42);
    assert_eq!(evt.observed_at(), 1_700_000_000_222_222_222);
}

#[test]
fn root_as_returns_none_on_garbage() {
    assert!(root_as_owner_conflict_event(&[]).is_none());
    assert!(root_as_owner_conflict_event(&[0xff, 0xff, 0xff, 0xff]).is_none());
}
