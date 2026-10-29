//! Tests for the trace CHUNK format (`core/src/trace/chunk.rs`).
//!
//! These are deliberately **version-insensitive** (no flatc / BFBS-byte
//! snapshot): they assert the fixed `Record` layout, the file identifier, and a
//! full build → verify → read-back roundtrip. A `.fbs` field change that is not
//! regenerated into the committed binding either fails to compile (the
//! `Record::new` arity / `ChunkArgs` fields change) or trips these asserts —
//! while staying robust to which flatc version a developer happens to have.

use super::{
    build_chunk, root_as_chunk, ChunkEventType, ChunkProvenance, RingRecord, CHUNK_FORMAT_VERSION,
    CHUNK_IDENTIFIER, FLAG_ASYNC,
};
use std::mem::{align_of, size_of};

// The drain rebases nothing the test can't predict, so the resolvers are simple
// deterministic functions of the global id.
fn prov() -> ChunkProvenance {
    ChunkProvenance {
        node_id: "node-a".to_string(),
        run_id: "run-1".to_string(),
        pid: 4242,
        process_name: "unit-test".to_string(),
        host_clock_domain_id: 7,
        seq: 3,
        dropped: 5,
    }
}

fn span(
    scope: u16,
    event: u16,
    thread: u16,
    span_id: u64,
    parent: u64,
    mono: u64,
    ty: ChunkEventType,
) -> RingRecord {
    RingRecord {
        trace_id: [0xAB; 16],
        span_id,
        parent_span_id: parent,
        mono_ns: mono,
        thread_delta_ns: 0,
        value: 0,
        scope_id: scope,
        event_id: event,
        thread_id: thread,
        level: 2, // info
        event_type: ty as u8,
        flags: 0,
    }
}

#[test]
fn record_struct_is_fixed_72_bytes() {
    // The hot-path POD and the FlatBuffers struct are memcpy-class & fixed.
    assert_eq!(
        size_of::<super::Record>(),
        72,
        "Record struct must be 72 bytes"
    );
    // flatc represents a struct as a `[u8; N]` newtype with endian-aware
    // accessors, so its alignment is 1 (FlatBuffers handles field alignment
    // inside the array and at vector placement).
    assert_eq!(align_of::<super::Record>(), 1);
    // Our ring POD is at least as small (it carries global ids, no padding games).
    assert!(size_of::<RingRecord>() <= 80);
}

#[test]
fn chunk_identifier_is_atrc() {
    assert_eq!(CHUNK_IDENTIFIER, "ATRC");
}

#[test]
fn roundtrip_preserves_provenance_and_records() {
    let recs = vec![
        span(10, 100, 1000, 1, 0, 5_000, ChunkEventType::SliceBegin),
        span(10, 100, 1000, 1, 0, 9_000, ChunkEventType::SliceEnd),
    ];
    let p = prov();
    let buf = build_chunk(
        &p,
        /*mono_epoch_ns*/ 5_000,
        /*wall_start_realtime_ns*/ 1_700_000_000_000_000_000,
        &recs,
        |s| format!("scope-{s}"),
        |e| format!("event-{e}"),
        |t| (t as u32, format!("thread-{t}")),
    );

    // Independently decodable + verified.
    let chunk = root_as_chunk(&buf).expect("valid chunk");
    assert_eq!(chunk.format_version(), CHUNK_FORMAT_VERSION);
    assert_eq!(chunk.pid(), 4242);
    assert_eq!(chunk.node_id(), Some("node-a"));
    assert_eq!(chunk.run_id(), Some("run-1"));
    assert_eq!(chunk.process_name(), Some("unit-test"));
    assert_eq!(chunk.host_clock_domain_id(), 7);
    assert_eq!(chunk.seq(), 3);
    assert_eq!(chunk.dropped(), 5);
    assert_eq!(chunk.mono_epoch_ns(), 5_000);

    let records = chunk.records().expect("records");
    assert_eq!(records.len(), 2);

    // delta_ns is rebased against mono_epoch_ns.
    assert_eq!(records.get(0).delta_ns(), 0);
    assert_eq!(records.get(1).delta_ns(), 4_000);

    // begin/end share the authoritative span_id and the chunk-local alias.
    assert_eq!(records.get(0).span_id(), 1);
    assert_eq!(records.get(1).span_id(), 1);
    assert_eq!(records.get(0).span_index(), records.get(1).span_index());
    assert_ne!(records.get(0).span_index(), 0);

    // iids resolve through the per-chunk tables.
    let scopes = chunk.scopes().expect("scopes");
    let events = chunk.events().expect("events");
    let threads = chunk.threads().expect("threads");
    let r0 = records.get(0);
    assert_eq!(scopes.get(r0.scope_iid() as usize), "scope-10");
    assert_eq!(events.get(r0.event_iid() as usize), "event-100");
    let te = threads.get(r0.thread_iid() as usize);
    assert_eq!(te.os_tid(), 1000);
    assert_eq!(te.name(), Some("thread-1000"));

    // trace_id preserved inline.
    let tid: Vec<u8> = r0.trace_id().iter().collect();
    assert_eq!(tid, vec![0xAB; 16]);
}

#[test]
fn interning_dedups_repeated_ids() {
    // Two scopes, three events, but scope 10 + thread 1 repeat → deduped.
    let recs = vec![
        span(10, 100, 1, 1, 0, 0, ChunkEventType::Instant),
        span(10, 101, 1, 2, 0, 0, ChunkEventType::Instant),
        span(20, 102, 1, 3, 0, 0, ChunkEventType::Instant),
    ];
    let p = prov();
    let buf = build_chunk(
        &p,
        0,
        0,
        &recs,
        |s| format!("s{s}"),
        |e| format!("e{e}"),
        |t| (t as u32, format!("t{t}")),
    );
    let chunk = root_as_chunk(&buf).unwrap();
    assert_eq!(chunk.scopes().unwrap().len(), 2, "scope 10 deduped");
    assert_eq!(chunk.events().unwrap().len(), 3);
    assert_eq!(chunk.threads().unwrap().len(), 1, "thread 1 deduped");
}

#[test]
fn parent_index_resolves_within_chunk_else_zero() {
    // span 1 is the parent of span 2 (in-chunk); span 3's parent (99) is NOT in
    // this chunk → parent_index 0 but parent_span_id preserved (resolve by id).
    let recs = vec![
        span(1, 1, 1, 1, 0, 0, ChunkEventType::SliceBegin),
        span(1, 2, 1, 2, 1, 0, ChunkEventType::SliceBegin),
        span(1, 3, 1, 3, 99, 0, ChunkEventType::SliceBegin),
    ];
    let p = prov();
    let buf = build_chunk(
        &p,
        0,
        0,
        &recs,
        |s| format!("s{s}"),
        |e| format!("e{e}"),
        |t| (t as u32, String::new()),
    );
    let chunk = root_as_chunk(&buf).unwrap();
    let r = chunk.records().unwrap();
    // child-of-1: parent_index points at span 1's index, parent_span_id == 1.
    assert_eq!(r.get(1).parent_span_id(), 1);
    assert_eq!(r.get(1).parent_index(), r.get(0).span_index());
    // orphan: parent not in chunk.
    assert_eq!(r.get(2).parent_span_id(), 99);
    assert_eq!(r.get(2).parent_index(), 0);
}

#[test]
fn parent_index_resolves_when_child_records_precede_parent() {
    // Real end-time emission appends the INNER span's records BEFORE the outer
    // parent's (the inner closes first). A single-pass parent_index resolution
    // would store 0 here; the pre-pass must resolve it.
    let recs = vec![
        // inner span (child of 1) emitted first
        span(1, 2, 1, 2, 1, 100, ChunkEventType::SliceBegin),
        span(1, 2, 1, 2, 1, 200, ChunkEventType::SliceEnd),
        // outer span (the parent) emitted after
        span(1, 1, 1, 1, 0, 50, ChunkEventType::SliceBegin),
        span(1, 1, 1, 1, 0, 300, ChunkEventType::SliceEnd),
    ];
    let p = prov();
    let buf = build_chunk(
        &p,
        0,
        0,
        &recs,
        |_| "s".into(),
        |_| "e".into(),
        |_| (1, String::new()),
    );
    let chunk = root_as_chunk(&buf).unwrap();
    let r = chunk.records().unwrap();
    let child_begin = r.get(0);
    let parent_begin = r.get(2);
    assert_eq!(child_begin.parent_span_id(), 1);
    assert_ne!(parent_begin.span_index(), 0);
    assert_eq!(
        child_begin.parent_index(),
        parent_begin.span_index(),
        "child's parent_index resolves to the later-emitted parent"
    );
}

#[test]
fn flags_and_event_type_survive_roundtrip() {
    let mut rec = span(1, 1, 1, 7, 0, 0, ChunkEventType::SliceEnd);
    rec.flags = FLAG_ASYNC;
    rec.value = 0;
    let p = prov();
    let buf = build_chunk(
        &p,
        0,
        0,
        &[rec],
        |_| "s".into(),
        |_| "e".into(),
        |_| (0, String::new()),
    );
    let chunk = root_as_chunk(&buf).unwrap();
    let r = chunk.records().unwrap().get(0);
    assert_eq!(r.flags() & FLAG_ASYNC, FLAG_ASYNC);
    assert_eq!(
        ChunkEventType::from_u8(r.event_type()),
        ChunkEventType::SliceEnd
    );
}

#[test]
fn counter_value_roundtrips() {
    let mut rec = span(1, 1, 1, 0, 0, 0, ChunkEventType::Counter);
    rec.value = 0xDEAD_BEEF_CAFE;
    let p = prov();
    let buf = build_chunk(
        &p,
        0,
        0,
        &[rec],
        |_| "s".into(),
        |_| "e".into(),
        |_| (0, String::new()),
    );
    let chunk = root_as_chunk(&buf).unwrap();
    assert_eq!(chunk.records().unwrap().get(0).value(), 0xDEAD_BEEF_CAFE);
}
