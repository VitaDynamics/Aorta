//! Tests for the stage taxonomy + recording API (Step 5).
//!
//! Stage histograms are process-global statics, so these tests use count DELTAS
//! (before/after) and serialize via a guard. Run the combined trace suite with
//! `--test-threads=1` when validating multiple trace test files together.

use crate::trace::{
    record_stage, record_stage_elapsed, set_trace_enabled, stage_snapshot, StageId, STAGE_COUNT,
};

fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

#[test]
fn record_stage_disabled_is_noop() {
    guard(|| {
        set_trace_enabled(false);
        let before = stage_snapshot(StageId::ServiceClientCall).count;
        record_stage(StageId::ServiceClientCall, 1234);
        let after = stage_snapshot(StageId::ServiceClientCall).count;
        assert_eq!(before, after, "disabled record must be a no-op");
    });
}

#[test]
fn record_stage_enabled_populates_histogram() {
    guard(|| {
        set_trace_enabled(true);
        let before = stage_snapshot(StageId::ServiceReceivedToReply).count;
        record_stage(StageId::ServiceReceivedToReply, 5_000);
        record_stage(StageId::ServiceReceivedToReply, 6_000);
        let snap = stage_snapshot(StageId::ServiceReceivedToReply);
        assert_eq!(snap.count - before, 2);
        assert!(snap.max_ns >= 6_000);
    });
}

#[test]
fn records_route_to_correct_stage() {
    guard(|| {
        set_trace_enabled(true);
        let a_before = stage_snapshot(StageId::ActionGoalSentToAccepted).count;
        let b_before = stage_snapshot(StageId::ActionAcceptedToFinished).count;
        record_stage(StageId::ActionGoalSentToAccepted, 100);
        let a_after = stage_snapshot(StageId::ActionGoalSentToAccepted).count;
        let b_after = stage_snapshot(StageId::ActionAcceptedToFinished).count;
        assert_eq!(a_after - a_before, 1);
        assert_eq!(
            b_after - b_before,
            0,
            "a record must not leak into other stages"
        );
    });
}

#[test]
fn record_stage_elapsed_emits_histogram_and_waterfall_span() {
    use crate::trace::chunk::{ChunkEventType, RingRecord};
    guard(|| {
        set_trace_enabled(true);
        // Ensure the stage-span scope passes T1.
        let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
        crate::trace::set_scope_enabled(stage_scope, true);
        // Clear stragglers from prior tests.
        let mut scratch: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);

        let before = stage_snapshot(StageId::ServiceClientCall).count;
        let start = crate::time::monotonic_ns();
        record_stage_elapsed(StageId::ServiceClientCall, start);

        // Aggregate histogram recorded (always-on tier).
        assert_eq!(
            stage_snapshot(StageId::ServiceClientCall).count - before,
            1,
            "the aggregate histogram still records"
        );

        // Waterfall span recorded (T1 tier) under the aorta.stage scope.
        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        let ours: Vec<_> = recs
            .into_iter()
            .filter(|r| r.scope_id == stage_scope)
            .collect();
        assert_eq!(ours.len(), 2, "one begin + one end stage span");
        assert!(ours
            .iter()
            .any(|r| ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin));
        assert!(ours
            .iter()
            .any(|r| ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceEnd));
    });
}

#[test]
fn record_stage_elapsed_as_attributes_span_to_primitive() {
    use crate::trace::chunk::RingRecord;
    use crate::trace::{record_stage_elapsed_as, stage_event_for};
    guard(|| {
        set_trace_enabled(true);
        let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
        crate::trace::set_scope_enabled(stage_scope, true);
        let mut scratch: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);

        // Interning is deterministic (same name -> same id) and per-primitive
        // (different name -> different id, != the generic stage event).
        let ev_nav = stage_event_for(StageId::ServiceReceivedToReply, "nav_service");
        assert_eq!(
            ev_nav,
            stage_event_for(StageId::ServiceReceivedToReply, "nav_service")
        );
        let ev_em = stage_event_for(StageId::ServiceReceivedToReply, "software_em");
        assert_ne!(ev_nav, ev_em);
        let ev_generic = crate::trace::intern_event(StageId::ServiceReceivedToReply.name());
        assert_ne!(ev_nav, ev_generic);
        // Names are canonicalized like the routing layer: `/nav_service/` and
        // `nav_service` are ONE primitive on the wire, so they must intern ONE
        // event (no alias split of the per-primitive percentiles).
        assert_eq!(
            ev_nav,
            stage_event_for(StageId::ServiceReceivedToReply, "/nav_service/")
        );

        let before = stage_snapshot(StageId::ServiceReceivedToReply).count;
        record_stage_elapsed_as(
            StageId::ServiceReceivedToReply,
            crate::time::monotonic_ns(),
            ev_nav,
        );

        // Aggregate stays GLOBAL per stage (bounded histogram set).
        assert_eq!(
            stage_snapshot(StageId::ServiceReceivedToReply).count - before,
            1,
            "per-primitive attribution must not fork the aggregate histogram"
        );

        // Waterfall span carries the per-primitive event id.
        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        let ours: Vec<_> = recs
            .into_iter()
            .filter(|r| r.scope_id == stage_scope)
            .collect();
        assert_eq!(ours.len(), 2, "one begin + one end stage span");
        assert!(
            ours.iter().all(|r| r.event_id == ev_nav),
            "the span must be attributed to the per-primitive event"
        );
    });
}

#[test]
fn stage_span_suppressed_when_scope_off_but_histogram_kept() {
    use crate::trace::chunk::RingRecord;
    guard(|| {
        set_trace_enabled(true);
        let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
        crate::trace::set_scope_enabled(stage_scope, false); // silence the timeline tier
        let mut scratch: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);

        let before = stage_snapshot(StageId::ServiceReceivedToReply).count;
        record_stage_elapsed(StageId::ServiceReceivedToReply, crate::time::monotonic_ns());
        // Aggregate still recorded...
        assert_eq!(
            stage_snapshot(StageId::ServiceReceivedToReply).count - before,
            1
        );
        // ...but no waterfall span for the silenced scope.
        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        assert!(recs.iter().all(|r| r.scope_id != stage_scope));
    });
}

#[test]
fn stage_discriminants_names_and_from_u8_roundtrip() {
    for (i, s) in StageId::ALL.iter().enumerate() {
        assert_eq!(*s as u8, i as u8, "discriminant must equal index");
        assert_eq!(StageId::from_u8(i as u8), Some(*s));
        assert!(!s.name().is_empty());
    }
    assert_eq!(StageId::from_u8(STAGE_COUNT as u8), None);
    assert_eq!(StageId::ALL.len(), STAGE_COUNT);
}

#[test]
fn record_stage_elapsed_under_nests_beneath_the_parent_link() {
    use crate::trace::chunk::{ChunkEventType, RingRecord};
    use crate::trace::{record_stage_elapsed_under, stage_event_for, SpanLink};
    guard(|| {
        set_trace_enabled(true);
        let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
        crate::trace::set_scope_enabled(stage_scope, true);
        let mut scratch: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);

        let ev = stage_event_for(StageId::ServiceQueueWait, "under_service");
        // The per-request root: its SPAN id is the parent of internal stages
        // (its own parent — e.g. the remote caller's span — is NOT inherited).
        let root = SpanLink {
            trace_id: [3u8; 16],
            span_id: 99,
            parent_span_id: 7,
        };
        let before = stage_snapshot(StageId::ServiceQueueWait).count;
        record_stage_elapsed_under(
            StageId::ServiceQueueWait,
            crate::time::monotonic_ns(),
            ev,
            Some(&root),
        );
        assert_eq!(
            stage_snapshot(StageId::ServiceQueueWait).count - before,
            1,
            "aggregate histogram still records"
        );

        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        let ours: Vec<_> = recs
            .into_iter()
            .filter(|r| r.scope_id == stage_scope && r.event_id == ev)
            .collect();
        assert_eq!(ours.len(), 2, "one begin + one end stage span");
        for r in &ours {
            assert_eq!(r.trace_id, [3u8; 16], "child rides the root's trace");
            assert_eq!(r.parent_span_id, 99, "child parents to the ROOT span");
            assert_ne!(r.span_id, 0, "child minted its own id");
            assert_ne!(r.span_id, 99, "child id is NOT the root's");
        }
        assert_eq!(
            ChunkEventType::from_u8(ours[0].event_type),
            ChunkEventType::SliceBegin
        );

        // Without a parent it falls back to the local-context path.
        record_stage_elapsed_under(
            StageId::ServiceQueueWait,
            crate::time::monotonic_ns(),
            ev,
            None,
        );
        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        assert!(
            recs.iter()
                .any(|r| r.scope_id == stage_scope && r.trace_id != [3u8; 16]),
            "unparented stage span must not inherit the previous root"
        );
    });
}
