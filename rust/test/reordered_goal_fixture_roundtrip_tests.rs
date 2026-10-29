use aorta_sdk::ActionWire;
use reordered_goal_probe_msg::prelude::ReorderedGoalProbeAction;
use reordered_goal_probe_msg::schema_aorta::action::example::ProbeKind;
use reordered_goal_probe_msg::typed::ReorderedGoalProbeGoalMsg;

#[test]
fn canonical_field_order_preserves_fixture_goal_values() {
    let samples = [-3, 0, 9];
    let goal = ReorderedGoalProbeGoalMsg {
        sequence: Some(1_234_567_890_123),
        kind: Some(ProbeKind::SECONDARY),
        label: Some("fixture-label".to_string()),
        samples: Some(&samples),
        enabled: Some(true),
    };

    let payload =
        <ReorderedGoalProbeAction as ActionWire>::encode_send_goal_request("goal-1", &goal)
            .expect("encode fixture goal");
    let root = <ReorderedGoalProbeAction as ActionWire>::decode_goal_from_send_goal_request(
        &payload,
    )
    .expect("decode fixture goal");

    assert_eq!(root.sequence(), 1_234_567_890_123);
    assert_eq!(root.kind(), ProbeKind::SECONDARY);
    assert_eq!(root.label(), Some("fixture-label"));
    let samples = root.samples().expect("samples");
    assert_eq!(samples.iter().collect::<Vec<_>>(), vec![-3, 0, 9]);
    assert!(root.enabled());
}
