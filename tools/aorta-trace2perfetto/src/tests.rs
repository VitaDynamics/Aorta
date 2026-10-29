//! Converter tests — build CHUNKs in-memory via the shared core binding, convert,
//! and assert the Chrome-JSON shape (no disk / no flatc).

use super::*;
use aorta_core::trace::chunk::{build_chunk, ChunkEventType, ChunkProvenance, RingRecord};

fn prov(node: &str, pid: u32) -> ChunkProvenance {
    ChunkProvenance {
        node_id: node.to_string(),
        run_id: "run".to_string(),
        pid,
        process_name: "proc".to_string(),
        host_clock_domain_id: 1,
        seq: 0,
        dropped: 0,
    }
}

fn rec(
    scope: u16,
    event: u16,
    thread: u16,
    span_id: u64,
    parent: u64,
    mono: u64,
    ty: ChunkEventType,
) -> RingRecord {
    RingRecord {
        trace_id: [7u8; 16],
        span_id,
        parent_span_id: parent,
        mono_ns: mono,
        scope_id: scope,
        event_id: event,
        thread_id: thread,
        event_type: ty as u8,
        ..RingRecord::default()
    }
}

// Resolvers turn the in-record global ids into the chunk tables the converter
// reads back as cat / name / thread.
fn build(node: &str, pid: u32, recs: &[RingRecord], mono_epoch: u64, wall_start: u64) -> Vec<u8> {
    build_chunk(
        &prov(node, pid),
        mono_epoch,
        wall_start,
        recs,
        |s| format!("scope{s}"),
        |e| format!("event{e}"),
        |t| (100 + t as u32, format!("thread{t}")),
    )
}

fn parse(json: &str) -> serde_json::Value {
    serde_json::from_str(json).unwrap()
}

#[test]
fn single_span_becomes_complete_event_with_metadata() {
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("nodeA", 42, &recs, 1_000, 1_700_000_000_000_000_000);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let x: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
    assert_eq!(x.len(), 1, "one complete slice");
    assert_eq!(x[0]["name"], "event0");
    assert_eq!(x[0]["cat"], "scope0");
    // dur = (5000-1000) ns = 4 µs.
    assert!((x[0]["dur"].as_f64().unwrap() - 4.0).abs() < 1e-6);

    // Process + thread track labels emitted.
    let m: Vec<_> = events.iter().filter(|e| e["ph"] == "M").collect();
    assert!(m
        .iter()
        .any(|e| e["name"] == "process_name"
            && e["args"]["name"].as_str().unwrap().contains("nodeA")));
    assert!(m
        .iter()
        .any(|e| e["name"] == "thread_name" && e["args"]["name"] == "thread0"));
}

#[test]
fn end_only_chunk_still_surfaces_process_and_thread_metadata() {
    // A chunk whose only record is an unmatched SliceEnd (the matching begin
    // landed in a lost/earlier chunk) produces NO complete span — but its
    // process/thread naming must still surface. Regression for the DecodedTrace
    // naming-completeness fix: `decode_trace` returns process/thread labels for
    // every process/thread seen, so the converter can emit metadata even for
    // chunks that yield no spans/points. The original golden tests never covered
    // this case.
    let recs = vec![rec(0, 0, 3, 9, 0, 2_000, ChunkEventType::SliceEnd)]; // thread 3
    let chunk = build("lonelynode", 7, &recs, 1_000, 1_700_000_000_000_000_000);

    // decode_trace surfaces the labels even with zero spans/points.
    let decoded = decode_trace(std::slice::from_ref(&chunk)).unwrap();
    assert!(
        decoded.spans.is_empty(),
        "end-only record yields no paired span"
    );
    assert!(decoded.points.is_empty());
    assert!(
        decoded
            .process_labels
            .iter()
            .any(|(_, label)| label.contains("lonelynode")),
        "process label surfaced: {:?}",
        decoded.process_labels
    );
    assert!(
        decoded
            .thread_labels
            .iter()
            .any(|(_, _, label)| label == "thread3"),
        "thread label surfaced: {:?}",
        decoded.thread_labels
    );

    // ...and the converter still emits the process/thread metadata events.
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let m: Vec<_> = events.iter().filter(|e| e["ph"] == "M").collect();
    assert!(
        m.iter().any(|e| e["name"] == "process_name"
            && e["args"]["name"].as_str().unwrap().contains("lonelynode")),
        "process_name metadata preserved for span-less chunk"
    );
    assert!(
        m.iter()
            .any(|e| e["name"] == "thread_name" && e["args"]["name"] == "thread3"),
        "thread_name metadata preserved for span-less chunk"
    );
}

#[test]
fn instant_and_counter_map_to_i_and_c() {
    let mut counter = rec(1, 1, 0, 0, 0, 2_000, ChunkEventType::Counter);
    counter.value = 7;
    let recs = vec![rec(1, 2, 0, 0, 0, 1_000, ChunkEventType::Instant), counter];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let inst: Vec<_> = events.iter().filter(|e| e["ph"] == "i").collect();
    assert_eq!(inst.len(), 1);
    assert_eq!(inst[0]["s"], "t");

    let cnt: Vec<_> = events.iter().filter(|e| e["ph"] == "C").collect();
    assert_eq!(cnt.len(), 1);
    assert_eq!(cnt[0]["args"]["event1"], 7);
}

#[test]
fn cross_thread_parent_emits_flow_arrow() {
    // Parent span on thread 0; child span on thread 1 with parent link.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 1, 2, 1, 2_000, ChunkEventType::SliceBegin),
        rec(0, 0, 1, 2, 1, 3_000, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let starts: Vec<_> = events.iter().filter(|e| e["ph"] == "s").collect();
    let finishes: Vec<_> = events.iter().filter(|e| e["ph"] == "f").collect();
    assert_eq!(starts.len(), 1, "one flow start (parent→child)");
    assert_eq!(finishes.len(), 1, "one flow finish");
    assert_eq!(starts[0]["id"], finishes[0]["id"], "matched flow id");
    assert_eq!(finishes[0]["bp"], "e", "finish binds to enclosing slice");
}

#[test]
fn same_thread_parent_also_emits_flow() {
    // Nesting shows containment; the arrow still shows the causal hop —
    // EVERY resolvable parent link draws one, same-thread included, so
    // identical requests render identically regardless of which worker
    // thread happened to record each stage.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 2, 1, 2_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 2, 1, 3_000, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    assert_eq!(events.iter().filter(|e| e["ph"] == "s").count(), 1);
    assert_eq!(events.iter().filter(|e| e["ph"] == "f").count(), 1);
    assert_eq!(events.iter().filter(|e| e["ph"] == "X").count(), 2);
}

#[test]
fn per_process_anchor_ignores_chunk_anchor_jitter() {
    // Two chunks of ONE process whose own wall anchors disagree by 700 ns
    // (sampling jitter): intra-process spacing must follow MONOTONIC exactly
    // (the process anchor), not the per-chunk wall samples.
    let a = build(
        "n",
        1,
        &[
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
            rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
        ],
        0,
        1_000_000,
    );
    let b = build(
        "n",
        1,
        &[
            rec(0, 1, 0, 2, 0, 1_001_000, ChunkEventType::SliceBegin),
            rec(0, 1, 0, 2, 0, 1_002_000, ChunkEventType::SliceEnd),
        ],
        1_000_000,
        2_000_700, // projected from the process anchor: 2_000_000 → 700 ns skew
    );
    // A third chunk past the tolerance (a real wall step): keeps its OWN anchor.
    let c = build(
        "n",
        1,
        &[
            rec(0, 2, 0, 3, 0, 2_001_000, ChunkEventType::SliceBegin),
            rec(0, 2, 0, 3, 0, 2_002_000, ChunkEventType::SliceEnd),
        ],
        2_000_000,
        99_000_000_000,
    );
    let trace = convert_buffers(&[a, b, c]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let ts_of = |name: &str| {
        events
            .iter()
            .find(|e| e["ph"] == "X" && e["name"] == name)
            .unwrap()["ts"]
            .as_f64()
            .unwrap()
    };
    let (t0, t1, t2) = (ts_of("event0"), ts_of("event1"), ts_of("event2"));
    // mono distance 1_000_000 ns = exactly 1000 µs, jitter-free.
    assert!(
        (t1 - t0 - 1000.0).abs() < 1e-6,
        "process anchor must keep intra-process spacing mono-exact (got {})",
        t1 - t0
    );
    // The stepped chunk is anchored on its own wall clock, far away.
    assert!(
        t2 - t0 > 90_000_000.0,
        "wall-step chunk keeps its own anchor"
    );
}

#[test]
fn stage_slices_rehome_to_per_primitive_lanes() {
    use aorta_core::trace::chunk::build_chunk;
    // A root + a nesting child + an OVERHANGING child (handler replied
    // inline), all recorded on the same real thread. Expected: root + child
    // on lane 1, the overhanging child spills to lane 2 — never a nesting
    // violation; arrows drawn for both links.
    let recs = vec![
        rec(0, 0, 0, 10, 99, 1_000, ChunkEventType::SliceBegin), // root (remote parent)
        rec(0, 1, 0, 11, 10, 1_200, ChunkEventType::SliceBegin), // nests
        rec(0, 1, 0, 11, 10, 1_400, ChunkEventType::SliceEnd),
        rec(0, 2, 0, 12, 10, 1_500, ChunkEventType::SliceBegin), // overhangs
        rec(0, 0, 0, 10, 99, 5_000, ChunkEventType::SliceEnd),
        rec(0, 2, 0, 12, 10, 6_000, ChunkEventType::SliceEnd),
    ];
    let names = [
        "service.received_to_reply:echo",
        "service.queue_wait:echo",
        "service.user_callback:echo",
    ];
    let chunk = build_chunk(
        &prov("n", 1),
        1_000,
        0,
        &recs,
        |_| "aorta.stage".to_string(),
        |e| names[e as usize].to_string(),
        |t| (100 + t as u32, format!("thread{t}")),
    );
    // DEFAULT projection: all three stage slices stay on the REAL thread. The
    // overhanging user_callback is a tree CHILD of the root, so instead of
    // spilling it the overlap pass CLAMPS its tail to the root's end → it nests
    // cleanly (waterfall) on the same thread, with the trimmed tail recorded in
    // args.overhang_ns.
    let default_trace = convert_buffers(std::slice::from_ref(&chunk)).unwrap();
    let dv = parse(&default_trace.to_json().unwrap());
    let devents = dv["traceEvents"].as_array().unwrap();
    let dslice = |name: &str| {
        devents
            .iter()
            .find(|e| e["ph"] == "X" && e["name"] == name)
            .unwrap()
    };
    assert!(
        devents
            .iter()
            .filter(|e| e["ph"] == "X")
            .all(|e| e["tid"].as_i64().unwrap() == 100),
        "a tree child that overhangs its ancestor nests in place, not spilled"
    );
    let root = dslice("service.received_to_reply:echo");
    let overhang = dslice("service.user_callback:echo");
    // Clamped to the root's end (root [1000,5000] → 4 µs duration); the trimmed
    // 1000 ns tail (orig end 6000) is preserved in args.
    assert_eq!(
        overhang["ts"].as_f64().unwrap() + overhang["dur"].as_f64().unwrap(),
        root["ts"].as_f64().unwrap() + root["dur"].as_f64().unwrap(),
        "the overhanging child is clamped to its ancestor's end (nests)"
    );
    assert_eq!(
        overhang["args"]["overhang_ns"], 1000,
        "the trimmed tail is preserved in args.overhang_ns"
    );
    // Arrows still drawn for every link in the default projection too.
    assert_eq!(devents.iter().filter(|e| e["ph"] == "s").count(), 2);

    let trace = convert_buffers_with(
        &[chunk],
        ConvertOptions {
            stage_lanes: true,
            ..Default::default()
        },
    )
    .unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let slice = |name: &str| {
        events
            .iter()
            .find(|e| e["ph"] == "X" && e["name"] == name)
            .unwrap()
    };
    let root = slice("service.received_to_reply:echo");
    let nested = slice("service.queue_wait:echo");
    let overhang = slice("service.user_callback:echo");
    let base = 1_000_000_000i64;
    assert!(
        root["tid"].as_i64().unwrap() >= base,
        "root re-homed to a lane"
    );
    assert_eq!(
        root["tid"], nested["tid"],
        "a properly nesting child shares the root's lane"
    );
    assert_ne!(
        overhang["tid"], root["tid"],
        "an overhanging child must spill to its own lane (no nesting violation)"
    );
    assert!(overhang["tid"].as_i64().unwrap() >= base);

    let lane_names: Vec<&str> = events
        .iter()
        .filter(|e| e["ph"] == "M" && e["name"] == "thread_name")
        .filter(|e| e["tid"].as_i64().unwrap() >= base)
        .map(|e| e["args"]["name"].as_str().unwrap())
        .collect();
    assert!(lane_names.contains(&"echo [stages]"), "{lane_names:?}");
    assert!(lane_names.contains(&"echo [stages 2]"), "{lane_names:?}");
    assert_eq!(
        root["args"]["thread"], "thread0",
        "the original recording thread survives in args.thread"
    );

    // Both parent links draw arrows (root's own remote parent is unresolved).
    assert_eq!(events.iter().filter(|e| e["ph"] == "s").count(), 2);
    assert_eq!(events.iter().filter(|e| e["ph"] == "f").count(), 2);
}

#[test]
fn merges_multiple_processes_into_distinct_pids() {
    let a = build(
        "nodeA",
        1,
        &[
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
            rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
        ],
        1_000,
        10_000,
    );
    let b = build(
        "nodeB",
        1, // same real pid, different host
        &[
            rec(0, 0, 0, 9, 0, 1_000, ChunkEventType::SliceBegin),
            rec(0, 0, 0, 9, 0, 2_000, ChunkEventType::SliceEnd),
        ],
        1_000,
        10_000,
    );
    let trace = convert_buffers(&[a, b]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let x: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
    assert_eq!(x.len(), 2);
    let pids: std::collections::HashSet<i64> =
        x.iter().map(|e| e["pid"].as_i64().unwrap()).collect();
    assert_eq!(
        pids.len(),
        2,
        "same real pid on two hosts → two synthetic pids"
    );
}

#[test]
fn same_span_id_in_two_processes_does_not_mispair() {
    // Two processes (different nodes) emit a span with the SAME span_id (span ids
    // are only process-local). Pairing must key on process identity, so each
    // process gets its own correctly-paired slice — not one merged/overwritten
    // pair. Same trace_id (a propagated trace) makes it adversarial.
    let recs_a = vec![
        rec(0, 0, 0, 5, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 5, 0, 3_000, ChunkEventType::SliceEnd),
    ];
    let recs_b = vec![
        rec(0, 0, 0, 5, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 5, 0, 9_000, ChunkEventType::SliceEnd),
    ];
    let a = build("nodeA", 1, &recs_a, 1_000, 0);
    let b = build("nodeB", 2, &recs_b, 1_000, 0);
    let trace = convert_buffers(&[a, b]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let x: Vec<_> = v["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["ph"] == "X")
        .collect();
    assert_eq!(x.len(), 2, "both processes' spans survive (no overwrite)");
    let mut durs: Vec<f64> = x.iter().map(|e| e["dur"].as_f64().unwrap()).collect();
    durs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    // 2us and 8us — each paired within its own process.
    assert!((durs[0] - 2.0).abs() < 1e-6, "process A: 2us");
    assert!((durs[1] - 8.0).abs() < 1e-6, "process B: 8us");
}

#[test]
fn timestamps_are_relative_to_global_epoch() {
    // wall_start large; ts should start near 0 (global min subtracted).
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 1_700_000_000_000_000_000);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let x = v["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["ph"] == "X")
        .unwrap();
    assert!(x["ts"].as_f64().unwrap() < 1.0, "first event near t=0");
}

#[test]
fn wall_clock_emits_absolute_realtime_us_with_exact_dur() {
    // Same chunk as the epoch test: wall_start 1.7e18 ns, span [1_000, 2_000].
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 1_700_000_000_000_000_000);
    let trace = convert_buffers_with(
        &[chunk],
        ConvertOptions {
            wall_clock: true,
            ..Default::default()
        },
    )
    .unwrap();
    let v = parse(&trace.to_json().unwrap());
    let x = v["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["ph"] == "X")
        .unwrap();
    // Absolute realtime µs (≈ wall_start/1000), NOT the epoch-relative ~0.
    assert!(
        x["ts"].as_f64().unwrap() > 1.6e15,
        "ts must be absolute realtime µs, got {}",
        x["ts"]
    );
    // Duration stays exact (1 µs) despite the absolute-stamp f64 quantization.
    assert_eq!(x["dur"].as_f64().unwrap(), 1.0);
}

#[test]
fn unterminated_span_is_zero_width_not_dropped() {
    // Begin with no matching end (e.g. dropped end record).
    let recs = vec![rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin)];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let x: Vec<_> = v["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["ph"] == "X")
        .cloned()
        .collect();
    assert_eq!(x.len(), 1);
    assert_eq!(x[0]["dur"].as_f64().unwrap(), 0.0);
}

#[test]
fn convert_files_reads_zst_from_disk() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let bytes = build("n", 1, &recs, 1_000, 0);
    let compressed = zstd::stream::encode_all(&bytes[..], 1).unwrap();

    let dir = std::env::temp_dir().join(format!(
        "t2p-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.chunk.zst"),
        compressed,
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 1);
    let trace = convert_files(&files).unwrap();
    let v = parse(&trace.to_json().unwrap());
    assert_eq!(
        v["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["ph"] == "X")
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn convert_files_reads_atrc_container_and_survives_torn_tail() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    // Two frames (one complete span each, distinct span ids)...
    let mut file = Vec::new();
    for span_id in [1u64, 2u64] {
        let recs = vec![
            rec(
                0,
                0,
                0,
                span_id,
                0,
                1_000 * span_id,
                ChunkEventType::SliceBegin,
            ),
            rec(
                0,
                0,
                0,
                span_id,
                0,
                2_000 * span_id,
                ChunkEventType::SliceEnd,
            ),
        ];
        let bytes = build("n", 1, &recs, 1_000, 0);
        let z = zstd::stream::encode_all(&bytes[..], 1).unwrap();
        file.extend_from_slice(b"ATCF");
        file.push(1); // frame version
        file.extend_from_slice(&[0u8; 3]); // flags + reserved
        file.extend_from_slice(&(z.len() as u32).to_le_bytes());
        file.extend_from_slice(&z);
    }
    // ...plus a torn tail frame (crash mid-append).
    file.extend_from_slice(b"ATCF");
    file.push(1);
    file.extend_from_slice(&[0u8; 3]);
    file.extend_from_slice(&(999_999u32).to_le_bytes());
    file.extend_from_slice(&[0xAB; 8]);

    let dir = std::env::temp_dir().join(format!(
        "t2p-atrc-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.atrc"),
        file,
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 1);
    let trace = convert_files(&files).unwrap();
    let v = parse(&trace.to_json().unwrap());
    assert_eq!(
        v["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["ph"] == "X")
            .count(),
        2,
        "both complete frames decode; the torn tail is dropped"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn convert_files_recovers_crash_orphaned_active_container() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    // A writer crashed before rotation: the file on disk is still
    // `.atrc.active`. Its complete frames must be recoverable directly.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let bytes = build("n", 1, &recs, 1_000, 0);
    let z = zstd::stream::encode_all(&bytes[..], 1).unwrap();
    let mut file = Vec::new();
    file.extend_from_slice(b"ATCF");
    file.push(1);
    file.extend_from_slice(&[0u8; 3]);
    file.extend_from_slice(&(z.len() as u32).to_le_bytes());
    file.extend_from_slice(&z);

    let dir = std::env::temp_dir().join(format!(
        "t2p-active-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.atrc.active"),
        file,
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 1, "directory scan picks up the orphan .active");
    let trace = convert_files(&files).unwrap();
    let v = parse(&trace.to_json().unwrap());
    assert_eq!(
        v["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["ph"] == "X")
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn empty_active_crash_artifact_does_not_abort_conversion() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    // One valid legacy chunk + an empty .atrc.active (crashed before the
    // first frame header): the artifact must contribute zero chunks, not an
    // error that aborts the whole conversion.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let bytes = build("n", 1, &recs, 1_000, 0);
    let compressed = zstd::stream::encode_all(&bytes[..], 1).unwrap();

    let dir = std::env::temp_dir().join(format!(
        "t2p-empty-active-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.chunk.zst"),
        compressed,
    )
    .unwrap();
    std::fs::write(
        dir.join("n_proc_2_00000000000000000001_00000000.atrc.active"),
        b"",
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 2);
    let trace = convert_files(&files).unwrap();
    let v = parse(&trace.to_json().unwrap());
    assert_eq!(
        v["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["ph"] == "X")
            .count(),
        1,
        "the valid chunk converts; the empty artifact contributes nothing"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_file_is_skipped_not_fatal() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd),
    ];
    let bytes = build("n", 1, &recs, 1_000, 0);
    let compressed = zstd::stream::encode_all(&bytes[..], 1).unwrap();

    let dir = std::env::temp_dir().join(format!(
        "t2p-corrupt-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.chunk.zst"),
        compressed,
    )
    .unwrap();
    // Not zstd at all: a corrupt legacy file must be skipped with a warning,
    // not abort the whole directory conversion.
    std::fs::write(
        dir.join("n_proc_2_00000000000000000001_00000000.chunk.zst"),
        b"this is not zstd",
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 2);
    let trace = convert_files(&files).unwrap();
    let v = parse(&trace.to_json().unwrap());
    assert_eq!(
        v["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["ph"] == "X")
            .count(),
        1,
        "the valid file converts; the corrupt one is skipped"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn async_slice_renders_as_nestable_async_begin_end() {
    // A FLAG_ASYNC span (queue_wait/group_wait) renders as a Chrome
    // nestable-async pair ('b'/'e' correlated by a PROCESS-scoped id2.local),
    // NOT a thread-track 'X' — so Perfetto groups it by NAME onto one
    // depth-stacked async track UNDER THIS PROCESS (id2.local keeps it out of
    // the global "Legacy Events" group), and it never spills to "[overlap N]".
    let mut begin = rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin);
    begin.flags = aorta_core::trace::chunk::FLAG_ASYNC;
    let recs = vec![begin, rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd)];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    assert!(
        !events.iter().any(|e| e["ph"] == "X"),
        "an async span must NOT be emitted as a thread-track 'X' slice"
    );
    let b = events
        .iter()
        .find(|e| e["ph"] == "b")
        .expect("async begin event");
    let e = events
        .iter()
        .find(|e| e["ph"] == "e")
        .expect("async end event");
    let local = b["id2"]["local"]
        .as_str()
        .expect("process-scoped id2.local");
    assert!(!local.is_empty());
    assert_eq!(
        b["id2"], e["id2"],
        "begin/end correlate via a shared process-scoped id2"
    );
    assert!(
        b["id"].is_null(),
        "a bare global id must NOT be set (it would file the track globally)"
    );
    assert_eq!(b["name"], e["name"], "the async track is grouped by name");
}

#[test]
fn async_slices_share_one_track_and_never_spill() {
    // Two same-name async waits that CROSS in time ([1000,4000], [2000,5000]):
    // as nestable-async they both survive on the ONE name-grouped track (no 'X',
    // no overlap sub-lane), where Perfetto depth-stacks them.
    let mut b1 = rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin);
    let mut b2 = rec(0, 0, 0, 2, 0, 2_000, ChunkEventType::SliceBegin);
    b1.flags = aorta_core::trace::chunk::FLAG_ASYNC;
    b2.flags = aorta_core::trace::chunk::FLAG_ASYNC;
    let recs = vec![
        b1,
        b2,
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 2, 0, 5_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let begins: Vec<_> = events.iter().filter(|e| e["ph"] == "b").collect();
    assert_eq!(begins.len(), 2, "both async waits survive as begins");
    let overlap_base = 2_000_000_000i64;
    assert!(
        events
            .iter()
            .filter(|e| e["ph"] == "b" || e["ph"] == "e")
            .all(|e| e["tid"].as_i64().unwrap() < overlap_base),
        "async waits must never spill to an overlap sub-lane"
    );
    let ids: std::collections::HashSet<&str> = begins
        .iter()
        .map(|e| e["id2"]["local"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids.len(),
        2,
        "each async wait gets a distinct correlation id"
    );
}

#[test]
fn callback_carries_its_scheduler_wait_breakdown_in_args() {
    // A dispatch on ONE thread: queue_wait [1000,2000] (async) → group_wait
    // [2000,5000] (async, group "3") → sched.callback [5000,9000] (sync). The
    // converter can't draw an arrow from the callback to its async waits, so it
    // matches them by same-thread time-adjacency (group_wait.end == callback.ts,
    // queue_wait.end == group_wait.ts) and hangs the breakdown on the callback.
    use aorta_core::trace::chunk::{build_chunk, FLAG_ASYNC};
    let names = ["sched.queue_wait", "sched.group_wait:3", "sched.callback"];
    let mut qb = rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin);
    let mut gb = rec(0, 1, 0, 2, 0, 2_000, ChunkEventType::SliceBegin);
    qb.flags = FLAG_ASYNC;
    gb.flags = FLAG_ASYNC;
    let recs = vec![
        qb,
        rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd), // queue_wait = 1000ns
        gb,
        rec(0, 1, 0, 2, 0, 5_000, ChunkEventType::SliceEnd), // group_wait = 3000ns
        rec(0, 2, 0, 3, 0, 5_000, ChunkEventType::SliceBegin), // callback starts at group_wait.end
        rec(0, 2, 0, 3, 0, 9_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build_chunk(
        &prov("n", 1),
        1_000,
        0,
        &recs,
        |_| "aorta.sched".to_string(),
        |e| names[e as usize].to_string(),
        |t| (100 + t as u32, format!("thread{t}")),
    );
    let v = parse(&convert_buffers(&[chunk]).unwrap().to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let cb = events
        .iter()
        .find(|e| e["ph"] == "X" && e["name"] == "sched.callback")
        .expect("the callback renders as a sync slice");
    assert_eq!(cb["args"]["queue_wait_ns"], 1000, "queue_wait attributed");
    assert_eq!(cb["args"]["group_wait_ns"], 3000, "group_wait attributed");
    assert_eq!(cb["args"]["group"], "3", "the group label rides along");
    assert_eq!(
        cb["args"]["sched_overhead_ns"], 4000,
        "total scheduler overhead before the callback"
    );
}

#[test]
fn crossing_slices_on_one_thread_spill_to_an_overlap_sub_lane() {
    // Two slices on the SAME OS thread that partially overlap (cross): A
    // [1000,4000], B [2000,5000] — B starts inside A but ends after it, so
    // they are neither nested nor disjoint. Perfetto would DROP one; the
    // default projection must instead spill the crosser to a sub-lane so both
    // survive on distinct tracks.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 1, 0, 2, 0, 2_000, ChunkEventType::SliceBegin),
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
        rec(0, 1, 0, 2, 0, 5_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    // Default projection (no stage lanes).
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();

    let xs: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
    assert_eq!(
        xs.len(),
        2,
        "both crossing slices must survive (none dropped)"
    );
    let tids: Vec<i64> = xs.iter().map(|e| e["tid"].as_i64().unwrap()).collect();
    assert_ne!(
        tids[0], tids[1],
        "the crosser must spill to a different track"
    );
    let overlap_base = 2_000_000_000i64;
    let spilled = xs
        .iter()
        .find(|e| e["tid"].as_i64().unwrap() >= overlap_base)
        .expect("one slice re-homed to an overlap sub-lane");
    assert_eq!(
        spilled["args"]["thread"], "thread0",
        "the original OS thread survives in args.thread"
    );
    let lane_names: Vec<&str> = events
        .iter()
        .filter(|e| e["ph"] == "M" && e["name"] == "thread_name")
        .filter(|e| e["tid"].as_i64().unwrap() >= overlap_base)
        .map(|e| e["args"]["name"].as_str().unwrap())
        .collect();
    assert!(
        lane_names.iter().any(|n| n.contains("[overlap")),
        "sub-lane labeled as an overlap spill: {lane_names:?}"
    );
}

#[test]
fn nested_slices_on_one_thread_are_not_spilled() {
    // A properly nesting child ([2000,3000] inside [1000,4000]) is a valid
    // stack — it must stay on the real thread, untouched.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(0, 1, 0, 2, 0, 2_000, ChunkEventType::SliceBegin),
        rec(0, 1, 0, 2, 0, 3_000, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let xs: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
    assert_eq!(xs.len(), 2);
    assert_eq!(
        xs[0]["tid"], xs[1]["tid"],
        "nested slices share the one real thread (no spill)"
    );
    let overlap_base = 2_000_000_000i64;
    assert!(
        xs.iter().all(|e| e["tid"].as_i64().unwrap() < overlap_base),
        "no overlap sub-lane created for a clean nest"
    );
}

// ── decode_trace: the shared, presentation-neutral decode API ──────────────

#[test]
fn decode_trace_pairs_begin_end_into_one_span_with_duration_and_parent() {
    // A root span [1000,5000] and a nested child [2000,3000] with parent=root.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        rec(1, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin),
        rec(1, 1, 0, 2, 1, 3_000, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("nodeA", 7, &recs, 1_000, 1_700_000_000_000_000_000);
    let decoded = decode_trace(&[chunk]).unwrap();

    assert_eq!(decoded.spans.len(), 2, "two begin/end pairs → two spans");
    let root = decoded.spans.iter().find(|s| s.name == "event0").unwrap();
    let child = decoded.spans.iter().find(|s| s.name == "event1").unwrap();
    // dur = end-start, in ns (absolute realtime, anchor applied).
    assert_eq!(root.duration_ns(), 4_000, "root span = 4us");
    assert_eq!(child.duration_ns(), 1_000, "child span = 1us");
    assert_eq!(root.scope, "scope0", "scope flows through as cat");
    assert_eq!(child.scope, "scope1");
    // Parent link resolves by span_id.
    assert_eq!(child.parent_span_id, root.span_id);
    assert_eq!(root.parent_span_id, 0, "root has no parent");
    // Labels are owned and present.
    assert!(root.process_label.contains("nodeA"));
    assert_eq!(root.thread_label, "thread0");
    assert!(!root.is_async && !child.is_async);
    // epoch is the earliest realtime; root starts at the epoch → relative 0.
    assert_eq!(root.start_ns, decoded.epoch_ns);
}

#[test]
fn decode_trace_instant_and_counter_become_points_not_spans() {
    let mut counter = rec(2, 3, 0, 0, 0, 2_000, ChunkEventType::Counter);
    counter.value = 42;
    let recs = vec![rec(1, 1, 0, 0, 0, 1_000, ChunkEventType::Instant), counter];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let decoded = decode_trace(&[chunk]).unwrap();

    assert!(decoded.spans.is_empty(), "no spans, only points");
    assert_eq!(decoded.points.len(), 2);
    let inst = decoded
        .points
        .iter()
        .find(|p| p.kind == ChunkEventType::Instant)
        .unwrap();
    assert_eq!(inst.name, "event1");
    let cnt = decoded
        .points
        .iter()
        .find(|p| p.kind == ChunkEventType::Counter)
        .unwrap();
    assert_eq!(cnt.value, 42, "counter value carried");
}

#[test]
fn decode_trace_segregates_async_spans() {
    // One sync span + one FLAG_ASYNC span: the async flag must survive so a
    // viewer can segregate it onto its own async section.
    let mut async_begin = rec(0, 0, 0, 2, 0, 1_500, ChunkEventType::SliceBegin);
    async_begin.flags = aorta_core::trace::chunk::FLAG_ASYNC;
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin),
        async_begin,
        rec(0, 0, 0, 2, 0, 2_500, ChunkEventType::SliceEnd),
        rec(0, 0, 0, 1, 0, 3_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let decoded = decode_trace(&[chunk]).unwrap();
    assert_eq!(decoded.spans.len(), 2);
    assert_eq!(
        decoded.spans.iter().filter(|s| s.is_async).count(),
        1,
        "exactly one span is flagged async"
    );
    assert_eq!(
        decoded.spans.iter().filter(|s| !s.is_async).count(),
        1,
        "exactly one span is sync"
    );
}

#[test]
fn decode_trace_unterminated_begin_is_zero_width_not_dropped() {
    // A begin with no matching end (dropped end / torn frame) → a zero-width
    // span kept as a marker, not silently dropped.
    let recs = vec![rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin)];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let decoded = decode_trace(&[chunk]).unwrap();
    assert_eq!(decoded.spans.len(), 1);
    assert_eq!(
        decoded.spans[0].duration_ns(),
        0,
        "unterminated begin → zero-width marker"
    );
    assert_eq!(decoded.spans[0].start_ns, decoded.spans[0].end_ns);
}

#[test]
fn decode_trace_files_round_trips_through_atrc_container() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);

    // Frame two complete chunks into an ATCF container, then append a torn
    // tail (crash signature): decode_trace_files must recover both spans and
    // tolerate the truncated tail — proving it uses the SAME reader path as
    // the converter.
    let mut file = Vec::new();
    for span_id in [1u64, 2u64] {
        let recs = vec![
            rec(
                0,
                0,
                0,
                span_id,
                0,
                1_000 * span_id,
                ChunkEventType::SliceBegin,
            ),
            rec(
                0,
                0,
                0,
                span_id,
                0,
                3_000 * span_id,
                ChunkEventType::SliceEnd,
            ),
        ];
        let bytes = build("n", 1, &recs, 1_000, 0);
        let z = zstd::stream::encode_all(&bytes[..], 1).unwrap();
        file.extend_from_slice(b"ATCF");
        file.push(1);
        file.extend_from_slice(&[0u8; 3]);
        file.extend_from_slice(&(z.len() as u32).to_le_bytes());
        file.extend_from_slice(&z);
    }
    // Torn tail.
    file.extend_from_slice(b"ATCF");
    file.push(1);
    file.extend_from_slice(&[0u8; 3]);
    file.extend_from_slice(&(999_999u32).to_le_bytes());
    file.extend_from_slice(&[0xAB; 8]);

    let dir = std::env::temp_dir().join(format!(
        "t2p-decode-atrc-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("n_proc_1_00000000000000000000_00000000.atrc"),
        file,
    )
    .unwrap();

    let files = collect_inputs(std::slice::from_ref(&dir)).unwrap();
    assert_eq!(files.len(), 1);
    let decoded = decode_trace_files(&files).unwrap();
    assert_eq!(
        decoded.spans.len(),
        2,
        "both complete frames decode; torn tail dropped"
    );
    let mut durs: Vec<u64> = decoded.spans.iter().map(|s| s.duration_ns()).collect();
    durs.sort_unstable();
    // span1: 3000-1000=2000ns ; span2: 6000-2000=4000ns
    assert_eq!(durs, vec![2_000, 4_000]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tree_child_overhang_nests_in_place_by_clamping() {
    // A tree CHILD ([2000,5000], span 2, parent 1) that overhangs its parent
    // ([1000,4000], span 1) must NOT spill — it nests in place on the same
    // thread (waterfall), clamped to the parent's end, with the trimmed tail in
    // args.overhang_ns. Contrast crossing_slices_on_one_thread_spill_* where
    // the two slices are UNRELATED (parent 0) and do spill.
    let recs = vec![
        rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin), // parent
        rec(0, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin), // child, parent=1
        rec(0, 0, 0, 1, 0, 4_000, ChunkEventType::SliceEnd),
        rec(0, 1, 0, 2, 1, 5_000, ChunkEventType::SliceEnd),
    ];
    let chunk = build("n", 1, &recs, 1_000, 0);
    let trace = convert_buffers(&[chunk]).unwrap();
    let v = parse(&trace.to_json().unwrap());
    let events = v["traceEvents"].as_array().unwrap();
    let xs: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
    assert_eq!(xs.len(), 2, "both slices survive");
    assert!(
        xs.iter().all(|e| e["tid"].as_i64().unwrap() == 100),
        "the child nests on the real thread (no overlap sub-lane)"
    );
    let parent = xs.iter().find(|e| e["name"] == "event0").unwrap();
    let child = xs.iter().find(|e| e["name"] == "event1").unwrap();
    assert_eq!(
        child["ts"].as_f64().unwrap() + child["dur"].as_f64().unwrap(),
        parent["ts"].as_f64().unwrap() + parent["dur"].as_f64().unwrap(),
        "child clamped to the parent's end so it nests"
    );
    assert_eq!(
        child["args"]["overhang_ns"], 1000,
        "the clamped tail (5000-4000 ns) is preserved"
    );
}
