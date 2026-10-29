//! Span / instant / counter recording mechanics (`core/src/trace/span.rs`).
//!
//! These exercise the real process-global ring path, so they serialize through
//! the shared trace-test lock and isolate by using a unique scope per test
//! (filtering drained records by `scope_id`). They cover the **master**-gated
//! recording mechanics; per-scope tier gating is the caller's job and is covered
//! at the macro/SDK layer.

use super::{counter, instant, span_begin, span_end};
use crate::trace::chunk::{ChunkEventType, RingRecord};

/// Drain every ring and return only the records for `scope` (test isolation).
fn drain_for(scope: u16) -> Vec<RingRecord> {
    let mut out = Vec::new();
    crate::trace::ring::drain_all(&mut out);
    out.into_iter().filter(|r| r.scope_id == scope).collect()
}

/// Clear any stragglers left in the rings by a previous test on a pooled thread.
fn clear_rings() {
    let mut out = Vec::new();
    crate::trace::ring::drain_all(&mut out);
}

fn ty(r: &RingRecord) -> ChunkEventType {
    ChunkEventType::from_u8(r.event_type)
}

#[test]
fn span_emits_begin_end_pair_sharing_span_id() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.pair");
        let ev = crate::trace::intern_event("work");

        let token = span_begin(scope, ev, 2, 0);
        assert_ne!(token, 0, "enabled span_begin returns a live token");
        super::span_end(token);

        let recs = drain_for(scope);
        assert_eq!(recs.len(), 2, "one begin + one end");
        assert_eq!(ty(&recs[0]), ChunkEventType::SliceBegin);
        assert_eq!(ty(&recs[1]), ChunkEventType::SliceEnd);
        assert_eq!(recs[0].span_id, token);
        assert_eq!(recs[1].span_id, token);
        assert!(
            recs[1].mono_ns >= recs[0].mono_ns,
            "end is not before begin"
        );
        assert_eq!(recs[0].event_id, ev);
        // A self-minted root has a non-zero trace_id.
        assert_ne!(recs[0].trace_id, [0u8; 16]);
    });
}

#[test]
fn disabled_master_makes_span_a_noop() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(false);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.off");
        let ev = crate::trace::intern_event("work");
        let token = span_begin(scope, ev, 2, 0);
        assert_eq!(token, 0, "disabled span_begin returns the inert token");
        span_end(token); // no-op
        assert!(drain_for(scope).is_empty());
    });
}

#[test]
fn raced_off_guard_records_nothing_into_histogram() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        let scope = crate::trace::intern_scope("test.span.raceoff");
        let hist =
            crate::trace::register_user_metric(scope, "raced", crate::trace::EventType::Span);
        let before = hist.snapshot().count;
        // Simulate the enable/disable race: the macro's gate passed (so the
        // guard carries the histogram) but `span_begin` observed tracing off
        // and returned the inert token 0. Dropping the guard must NOT record
        // a bogus zero-latency sample.
        drop(crate::trace::SpanGuard::new(0, Some(hist)));
        assert_eq!(
            hist.snapshot().count,
            before,
            "a raced-off (token 0) guard must not record into the histogram"
        );
    });
}

#[test]
fn nested_spans_link_parent_and_share_trace() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.nested");
        let outer_ev = crate::trace::intern_event("outer");
        let inner_ev = crate::trace::intern_event("inner");

        let outer = span_begin(scope, outer_ev, 2, 0);
        let inner = span_begin(scope, inner_ev, 2, 0);
        span_end(inner);
        span_end(outer);

        let recs = drain_for(scope);
        assert_eq!(recs.len(), 4);
        let inner_begin = recs
            .iter()
            .find(|r| r.span_id == inner && ty(r) == ChunkEventType::SliceBegin)
            .unwrap();
        let outer_begin = recs
            .iter()
            .find(|r| r.span_id == outer && ty(r) == ChunkEventType::SliceBegin)
            .unwrap();
        // Inner's parent is the outer span.
        assert_eq!(inner_begin.parent_span_id, outer);
        // Root has no parent.
        assert_eq!(outer_begin.parent_span_id, 0);
        // Nested spans share one trace_id.
        assert_eq!(inner_begin.trace_id, outer_begin.trace_id);
    });
}

#[test]
fn sequential_roots_get_distinct_trace_ids() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.roots");
        let ev = crate::trace::intern_event("root");

        let a = span_begin(scope, ev, 2, 0);
        span_end(a);
        let b = span_begin(scope, ev, 2, 0);
        span_end(b);

        let recs = drain_for(scope);
        let tid_a = recs.iter().find(|r| r.span_id == a).unwrap().trace_id;
        let tid_b = recs.iter().find(|r| r.span_id == b).unwrap().trace_id;
        assert_ne!(tid_a, tid_b, "each top-level span is its own trace");
    });
}

#[test]
fn instant_and_counter_emit_single_records() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.point");
        let iev = crate::trace::intern_event("blip");
        let cev = crate::trace::intern_event("depth");

        instant(scope, iev, 2);
        counter(scope, cev, 2, 42);

        let recs = drain_for(scope);
        assert_eq!(recs.len(), 2);
        let inst = recs.iter().find(|r| r.event_id == iev).unwrap();
        let cnt = recs.iter().find(|r| r.event_id == cev).unwrap();
        assert_eq!(ty(inst), ChunkEventType::Instant);
        assert_eq!(inst.value, 1);
        assert_eq!(ty(cnt), ChunkEventType::Counter);
        assert_eq!(cnt.value, 42);
    });
}

#[test]
fn instant_inside_span_carries_enclosing_context() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.point_in_span");
        let sev = crate::trace::intern_event("span");
        let iev = crate::trace::intern_event("blip");

        let s = span_begin(scope, sev, 2, 0);
        instant(scope, iev, 2);
        span_end(s);

        let recs = drain_for(scope);
        let inst = recs.iter().find(|r| r.event_id == iev).unwrap();
        let begin = recs
            .iter()
            .find(|r| r.span_id == s && ty(r) == ChunkEventType::SliceBegin)
            .unwrap();
        // The instant nests under the open span (parent + shared trace).
        assert_eq!(inst.parent_span_id, s);
        assert_eq!(inst.trace_id, begin.trace_id);
    });
}

#[test]
fn external_context_sets_trace_id_and_parent() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.ctx");
        let ev = crate::trace::intern_event("server_handle");

        let injected = [0x11u8; 16];
        let injected_parent = 0xABCD_u64;
        super::set_trace_context(injected, injected_parent);
        let s = span_begin(scope, ev, 2, 0);
        span_end(s);
        super::clear_trace_context();

        let recs = drain_for(scope);
        let begin = recs
            .iter()
            .find(|r| r.span_id == s && ty(r) == ChunkEventType::SliceBegin)
            .unwrap();
        assert_eq!(begin.trace_id, injected, "uses inbound trace_id");
        assert_eq!(
            begin.parent_span_id, injected_parent,
            "links to inbound parent"
        );
    });
}

#[test]
fn out_of_order_end_marks_async() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.async");
        let a_ev = crate::trace::intern_event("a");
        let b_ev = crate::trace::intern_event("b");

        // End the OUTER span before the inner → non-LIFO use.
        let a = span_begin(scope, a_ev, 2, 0);
        let b = span_begin(scope, b_ev, 2, 0);
        span_end(a); // out of order
        span_end(b);

        let recs = drain_for(scope);
        let a_begin = recs
            .iter()
            .find(|r| r.span_id == a && ty(r) == ChunkEventType::SliceBegin)
            .unwrap();
        assert_eq!(
            a_begin.flags & crate::trace::chunk::FLAG_ASYNC,
            crate::trace::chunk::FLAG_ASYNC,
            "non-LIFO end is marked async"
        );
    });
}

#[test]
fn outbound_link_is_none_when_disabled() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(false);
        assert!(super::outbound_link().is_none());
    });
}

#[test]
fn outbound_link_mints_a_fresh_root_without_touching_thread_context() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        let link = super::outbound_link().expect("enabled");
        assert_ne!(link.trace_id, [0u8; 16], "root link mints a trace id");
        assert_ne!(link.span_id, 0);
        assert_eq!(link.parent_span_id, 0, "unanchored hop is a trace root");
        // The mint is caller-owned: thread context stays untouched, so a second
        // unanchored hop is its OWN root, and no context lingers.
        let second = super::outbound_link().expect("enabled");
        assert_ne!(second.trace_id, link.trace_id);
        assert_eq!(super::current_outbound_context(), None);
    });
}

#[test]
fn outbound_link_joins_the_open_span_as_parent() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.outlink");
        let ev = crate::trace::intern_event("caller");

        let token = span_begin(scope, ev, 2, 0);
        let link = super::outbound_link().expect("enabled");
        span_end(token);

        assert_eq!(
            link.parent_span_id, token,
            "the open span is the causal parent of the hop"
        );
        let recs = drain_for(scope);
        assert_eq!(
            link.trace_id, recs[0].trace_id,
            "the hop rides the open span's trace"
        );
    });
}

#[test]
fn emit_completed_span_linked_writes_the_explicit_identity() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.linked");
        let ev = crate::trace::intern_event("remote_stage");

        let link = super::SpanLink {
            trace_id: [7u8; 16],
            span_id: 42,
            parent_span_id: 41,
        };
        super::emit_completed_span_linked(scope, ev, 2, 100, 200, 0, link);

        let recs = drain_for(scope);
        assert_eq!(recs.len(), 2, "one begin + one end");
        for r in &recs {
            assert_eq!(r.trace_id, [7u8; 16]);
            assert_eq!(r.span_id, 42);
            assert_eq!(r.parent_span_id, 41);
        }
        assert_eq!(recs[0].mono_ns, 100);
        assert_eq!(recs[1].mono_ns, 200);
    });
}

#[test]
fn adopted_context_parents_spans_and_clears_on_drop() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        clear_rings();
        let scope = crate::trace::intern_scope("test.span.adopt");
        let ev = crate::trace::intern_event("handler_work");

        let parent = super::TraceParent {
            trace_id: [9u8; 16],
            span_id: 77,
            flags: super::TRACEPARENT_SAMPLED,
        };
        {
            let _ctx = super::adopt_remote_context(&parent);
            let token = span_begin(scope, ev, 2, 0);
            span_end(token);
        }

        let recs = drain_for(scope);
        assert_eq!(recs[0].trace_id, [9u8; 16], "span joins the remote trace");
        assert_eq!(recs[0].parent_span_id, 77, "remote span is the parent");
        assert_eq!(
            super::current_outbound_context(),
            None,
            "guard drop clears the adopted context"
        );
    });
}

#[test]
fn inbound_root_link_joins_parent_or_mints_fresh_root() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(false);
        assert!(super::inbound_root_link(None).is_none());

        crate::trace::set_trace_enabled(true);
        let tp = super::TraceParent {
            trace_id: [5u8; 16],
            span_id: 42,
            flags: super::TRACEPARENT_SAMPLED,
        };
        let root = super::inbound_root_link(Some(&tp)).expect("enabled");
        assert_eq!(root.trace_id, [5u8; 16], "root rides the caller's trace");
        assert_eq!(
            root.parent_span_id, 42,
            "caller's span is the root's parent"
        );
        assert_ne!(root.span_id, 0);
        assert_ne!(root.span_id, 42);

        let fresh = super::inbound_root_link(None).expect("enabled");
        assert_ne!(
            fresh.trace_id, [0u8; 16],
            "unpropagated request still gets a coherent tree"
        );
        assert_eq!(fresh.parent_span_id, 0, "fresh root has no parent");
        assert_ne!(fresh.trace_id, root.trace_id);
    });
}
