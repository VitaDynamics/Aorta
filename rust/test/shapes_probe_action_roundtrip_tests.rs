// Golden action: compile + round-trip proof for the owned-root clone compile
// bombs -- a struct field (`Some(root.acc())` vs `Option<&T>`) and a `[struct]`
// field (`values.iter().collect()` yielding `Vec<&T>` into `Vec<T>`) in an
// action Result/FeedbackData.
//
// The GetResultResponse round-trip drives BOTH owned paths for the fixed
// shapes: encode borrows owned -> typed (`borrow_*_owned_as_typed`) and builds
// the buffer; decode reconstructs the owned Msg from the flatbuffer root via
// `clone_*_owned_from_root` -- the path that failed to compile pre-fix. Every
// struct / [struct] / string / [string] / table / [table] / union-with-alias /
// enum / [enum] field must survive the round-trip byte-for-byte.
use aorta_sdk::{ActionGetResultResponse, ActionReplyCode, ActionStatus, ActionWire};
use shapes_probe_msg::schema_aorta::action::example::{Grade, Point};
use shapes_probe_msg::{owned, ShapesProbeAction};

fn sentinel_result() -> owned::ShapesProbeResultMsg {
    owned::ShapesProbeResultMsg {
        grade: Some(Grade::HIGH),
        // struct field -- the `Some(root.acc())` compile bomb site.
        origin: Some(Point::new(1.0, 2.0, 3.0)),
        // [struct] field -- the `values.iter().collect()` compile bomb site.
        path: Some(vec![Point::new(4.0, 5.0, 6.0), Point::new(7.0, 8.0, 9.0)]),
        note: Some("done".to_string()),
        labels: Some(vec!["l1".to_string(), "l2".to_string()]),
        best: Some(Box::new(owned::LeafMsg {
            id: Some(11),
            label: Some("best-leaf".to_string()),
        })),
        leaves: Some(vec![
            owned::LeafMsg {
                id: Some(12),
                label: Some("leaf-a".to_string()),
            },
            owned::LeafMsg {
                id: Some(13),
                label: Some("leaf-b".to_string()),
            },
        ]),
        choice: Some(owned::ShapeChoiceMsg::Chosen(Box::new(owned::LeafMsg {
            id: Some(99),
            label: Some("chosen".to_string()),
        }))),
        grades: Some(vec![Grade::LOW, Grade::HIGH, Grade::UNKNOWN]),
        // optional scalar (`= null`) -- owned-root clone emits `root.score()`
        // directly (the accessor already returns `Option<i32>`); `Some(..)`
        // would double-wrap and fail to compile.
        score: Some(-7),
        // [bool] through the action owned-root clone: flatc's `[bool]` accessor
        // returns `Vector<'a, bool>` (no `.to_vec()`), so the emitter uses
        // `.iter().collect()` (the Rust side of the [bool] fix).
        flags: Some(vec![true, false, true]),
        // `required` fields: the owned-root clone reads a
        // non-Option accessor and wraps in `Some(...)`. Must be set (flatc rejects
        // a missing required field at buffer finish).
        // required struct: accessor returns `&Point` -> `Some(*root.req_point())`.
        req_point: Some(Point::new(30.0, 31.0, 32.0)),
        req_note: Some("req".to_string()),
        req_best: Some(Box::new(owned::LeafMsg {
            id: Some(21),
            label: Some("req-best".to_string()),
        })),
        req_scores: Some(vec![100, 200]),
        req_tags: Some(vec!["t1".to_string(), "t2".to_string()]),
        req_marks: Some(vec![Point::new(20.0, 21.0, 22.0)]),
        req_grades: Some(vec![Grade::HIGH, Grade::LOW]),
        req_leaves: Some(vec![owned::LeafMsg {
            id: Some(22),
            label: Some("req-leaf".to_string()),
        }]),
    }
}

fn sentinel_feedback() -> owned::ShapesProbeFeedbackDataMsg {
    owned::ShapesProbeFeedbackDataMsg {
        stage: Some(Grade::LOW),
        progress: Some(0.5),
        // struct + [struct] in FeedbackData -- same owned-root clone paths.
        cursor: Some(Point::new(10.0, 11.0, 12.0)),
        waypoints: Some(vec![Point::new(13.0, 14.0, 15.0)]),
    }
}

#[test]
fn shapes_probe_get_result_response_round_trips_struct_and_struct_vector() {
    let response = ActionGetResultResponse {
        goal_id: "goal-1".to_string(),
        status: ActionStatus::Succeeded,
        accepted_stamp_ns: 100,
        started_stamp_ns: 200,
        finished_stamp_ns: 300,
        last_feedback_seq: 5,
        feedback_snapshot: sentinel_feedback(),
        result: Some(sentinel_result()),
        code: ActionReplyCode::Succeeded,
        message: "ok".to_string(),
        server_epoch: "epoch-1".to_string(),
    };

    let payload = <ShapesProbeAction as ActionWire>::encode_get_result_response(&response)
        .expect("encode_get_result_response should succeed");
    let decoded = <ShapesProbeAction as ActionWire>::decode_get_result_response(&payload)
        .expect("decode_get_result_response should succeed");

    // envelope
    assert_eq!(decoded.goal_id, "goal-1");
    assert_eq!(decoded.status, ActionStatus::Succeeded);
    assert_eq!(decoded.code, ActionReplyCode::Succeeded);

    // Result -- reconstructed through clone_shapesproberesult_owned_from_root.
    let result = decoded.result.expect("result present");
    assert_eq!(result.grade, Some(Grade::HIGH));
    // struct: origin survives (was `Some(root.origin())` -> `.copied()`).
    assert_eq!(result.origin, Some(Point::new(1.0, 2.0, 3.0)));
    // [struct]: path survives (was `.iter().collect()` -> `.iter().copied().collect()`).
    assert_eq!(
        result.path,
        Some(vec![Point::new(4.0, 5.0, 6.0), Point::new(7.0, 8.0, 9.0)])
    );
    assert_eq!(result.note.as_deref(), Some("done"));
    assert_eq!(
        result.labels,
        Some(vec!["l1".to_string(), "l2".to_string()])
    );
    let best = result.best.expect("best present");
    assert_eq!(best.id, Some(11));
    assert_eq!(best.label.as_deref(), Some("best-leaf"));
    let leaves = result.leaves.expect("leaves present");
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].id, Some(12));
    assert_eq!(leaves[1].label.as_deref(), Some("leaf-b"));
    match result.choice.expect("choice present") {
        owned::ShapeChoiceMsg::Chosen(leaf) => {
            assert_eq!(leaf.id, Some(99));
            assert_eq!(leaf.label.as_deref(), Some("chosen"));
        }
        other => panic!("expected Chosen variant, got {other:?}"),
    }
    assert_eq!(
        result.grades,
        Some(vec![Grade::LOW, Grade::HIGH, Grade::UNKNOWN])
    );
    // optional scalar: a present value survives the owned-root clone.
    assert_eq!(result.score, Some(-7));
    // [bool] survives the owned-root clone (`.iter().collect()`).
    assert_eq!(result.flags, Some(vec![true, false, true]));
    // required fields (non-Option accessor -> `Some(...)`) survive.
    assert_eq!(result.req_point, Some(Point::new(30.0, 31.0, 32.0)));
    assert_eq!(result.req_note.as_deref(), Some("req"));
    assert_eq!(result.req_best.expect("req_best").id, Some(21));
    assert_eq!(result.req_scores, Some(vec![100, 200]));
    assert_eq!(
        result.req_tags,
        Some(vec!["t1".to_string(), "t2".to_string()])
    );
    assert_eq!(result.req_marks, Some(vec![Point::new(20.0, 21.0, 22.0)]));
    assert_eq!(result.req_grades, Some(vec![Grade::HIGH, Grade::LOW]));
    assert_eq!(
        result.req_leaves.expect("req_leaves")[0].label.as_deref(),
        Some("req-leaf")
    );

    // FeedbackData snapshot -- same owned-root clone paths for struct/[struct].
    let feedback = decoded.feedback_snapshot;
    assert_eq!(feedback.stage, Some(Grade::LOW));
    assert_eq!(feedback.cursor, Some(Point::new(10.0, 11.0, 12.0)));
    assert_eq!(feedback.waypoints, Some(vec![Point::new(13.0, 14.0, 15.0)]));
}

#[test]
fn shapes_probe_optional_scalar_absence_round_trips_as_none() {
    // The point of a `= null` scalar: absence is preserved as `None`, NOT
    // coerced to a default (0). owned-root clone reads the accessor's
    // `Option<i32>` straight through, so a `None` score stays `None`.
    let mut result = sentinel_result();
    result.score = None;
    result.flags = None; // an absent [bool] round-trips as None too.

    let response = ActionGetResultResponse {
        goal_id: "goal-2".to_string(),
        status: ActionStatus::Succeeded,
        accepted_stamp_ns: 1,
        started_stamp_ns: 2,
        finished_stamp_ns: 3,
        last_feedback_seq: 0,
        feedback_snapshot: sentinel_feedback(),
        result: Some(result),
        code: ActionReplyCode::Succeeded,
        message: "ok".to_string(),
        server_epoch: "epoch-2".to_string(),
    };

    let payload = <ShapesProbeAction as ActionWire>::encode_get_result_response(&response)
        .expect("encode_get_result_response should succeed");
    let decoded = <ShapesProbeAction as ActionWire>::decode_get_result_response(&payload)
        .expect("decode_get_result_response should succeed");

    let result = decoded.result.expect("result present");
    assert_eq!(result.score, None, "absent optional scalar must stay None");
    assert_eq!(result.flags, None, "absent [bool] must stay None");
    // A non-optional field on the same table is unaffected.
    assert_eq!(result.grade, Some(Grade::HIGH));
}
