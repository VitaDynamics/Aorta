//! Convert Aorta trace **CHUNK** files → a `ui.perfetto.dev`-loadable trace.
//!
//! On-robot the canonical format is the compact FlatBuffers CHUNK
//! (`aorta_core::trace::chunk`); the Perfetto transcode runs **off-device**
//! here. The output is **Chrome Trace Event JSON** — fully supported by
//! `ui.perfetto.dev` and `trace_processor`, and dependency-free (no robot-side
//! protobuf). Native Perfetto-proto is a future alternate backend.
//!
//! The converter MERGES every chunk from every process and host:
//! - each `(node, pid)` becomes one Perfetto process track group;
//! - by DEFAULT every SYNCHRONOUS slice renders on its real OS thread row; with
//!   [`ConvertOptions::stage_lanes`] the built-in `aorta.stage` lifecycle
//!   slices re-home onto synthetic per-primitive **stage lanes**
//!   (`<name> [stages]`, overlap spills to numbered lanes; the original
//!   thread is kept in `args.thread`) — the stages record on whatever async
//!   worker ran them, so the lane projection makes identical requests render
//!   identically. Both projections read the SAME chunks; re-render any time;
//! - `FLAG_ASYNC` slices (the scheduler `queue_wait` / `group_wait` waits, which
//!   overlap by nature) render as Chrome **nestable-async** slices instead:
//!   Perfetto groups them by NAME into one depth-stacked track (all
//!   `sched.queue_wait` together; each `sched.group_wait:<group>` its own — the
//!   per-group contention view), so they never spill into per-thread lanes;
//! - every resolvable `parent_span_id` link between SYNC slices becomes a **flow
//!   arrow** — same-thread links included (nesting shows containment, the arrow
//!   shows the causal hop and keeps interleaved requests tellable apart on
//!   shared thread rows). Links that touch an async slice are NOT drawn: an
//!   async slice sits on a name-grouped async track, not the thread row its
//!   (pid,tid) names, so the arrow would mis-bind to whatever sync slice shares
//!   that row — the async track grouping conveys the relationship instead;
//! - each callback (`sched.callback` / `topic.callback:*`) carries its own
//!   scheduler-wait breakdown in args (`queue_wait_ns`, `group_wait_ns`,
//!   `group`, `sched_overhead_ns`), matched from the same-thread async waits at
//!   its dispatch — click a callback to read how long it spent queued / blocked
//!   on its group lock, without chasing the wait slices on the async tracks;
//! - instants and counters become Perfetto instant / counter tracks;
//! - `thread_delta_ns` (when captured) becomes Chrome `tdur` (CPU-in-span).
//!
//! Event timestamps are mapped to a common timeline with ONE affine anchor
//! `(mono_epoch_ns ↔ wall_start_realtime_ns)` PER PROCESS (its earliest
//! chunk): monotonic is the only truth within a process, so re-anchoring at
//! every chunk would inject µs-level wall-sampling jitter BETWEEN slices of
//! one process (enough to reorder same-instant spans). A chunk whose own
//! anchor disagrees with the process anchor by more than
//! [`MAX_PROCESS_ANCHOR_SKEW_NS`] (a wall step / pid reuse) falls back to its
//! own. PTP-synced REALTIME joins hosts; a global epoch is
//! subtracted so `ts` keeps sub-µs precision.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aorta_core::trace::chunk::{root_as_chunk, ChunkEventType, FLAG_ASYNC};
use serde::Serialize;

/// A single Chrome Trace Event. Optional fields are omitted when absent so the
/// JSON stays compact and valid for each phase.
#[derive(Serialize)]
struct TraceEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cat: Option<String>,
    ph: &'static str,
    ts: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tdur: Option<f64>,
    pid: i64,
    tid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    /// PROCESS-scoped async id (`{"local": "<id>"}`). A bare `id` makes Perfetto
    /// file async ('b'/'e') slices under a GLOBAL "Legacy Events" group; a local
    /// id2 keeps them inside their own process track group.
    #[serde(skip_serializing_if = "Option::is_none")]
    id2: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bp: Option<&'static str>,
    /// Instant scope ("t" = thread).
    #[serde(skip_serializing_if = "Option::is_none")]
    s: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<serde_json::Value>,
}

/// The top-level Chrome trace document.
#[derive(Serialize)]
pub struct ChromeTrace {
    #[serde(rename = "traceEvents")]
    trace_events: Vec<TraceEvent>,
    #[serde(rename = "displayTimeUnit")]
    display_time_unit: &'static str,
}

impl ChromeTrace {
    /// Number of emitted events (for tests / summaries).
    pub fn len(&self) -> usize {
        self.trace_events.len()
    }
    pub fn is_empty(&self) -> bool {
        self.trace_events.is_empty()
    }
    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

// --- internal decoded forms (decoupled from the FlatBuffers lifetime) ---

struct DecodedRecord {
    trace_id: [u8; 16],
    span_id: u64,
    parent_span_id: u64,
    realtime_ns: u64,
    thread_delta_ns: u64,
    value: u64,
    synth_pid: i64,
    tid: i64,
    name: String,
    cat: String,
    event_type: ChunkEventType,
    is_async: bool,
}

// --- public, presentation-neutral decode API (shared by the converter + the
// offline trace TUI; one decode + anchor + begin/end pairing path, DRY) ---

/// A paired span (a `SliceBegin`+`SliceEnd` sharing a `span_id`, or an
/// unterminated begin → zero-width) on the common per-process timeline.
///
/// Timestamps are **absolute** realtime nanoseconds (the process affine anchor
/// already applied); subtract [`DecodedTrace::epoch_ns`] for a relative view.
/// `pid`/`tid` are the synthetic, dense Chrome-style ids ([`Naming`]) that keep
/// same-pid processes on different hosts distinct; `process_label`/`thread_label`
/// are their human names. FlatBuffers accessors borrow the chunk buffer, so all
/// strings/ints here are OWNED — safe to keep after the buffers drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSpan {
    pub trace_id: [u8; 16],
    pub span_id: u64,
    pub parent_span_id: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    /// CPU-on-thread time inside the span when captured (`0` = not captured).
    pub thread_delta_ns: u64,
    /// The recording scope (`cat` in Chrome terms), e.g. `aorta.stage`.
    pub scope: String,
    pub name: String,
    pub pid: i64,
    pub tid: i64,
    pub process_label: String,
    pub thread_label: String,
    /// `true` for `FLAG_ASYNC` spans (scheduler `queue_wait`/`group_wait` waits
    /// that overlap by nature); a viewer must segregate these onto their own
    /// async section, NOT a per-thread call-stack row.
    pub is_async: bool,
}

impl TraceSpan {
    /// Wall-to-wall duration; `0` for an unterminated begin (zero-width marker).
    pub fn duration_ns(&self) -> u64 {
        self.end_ns.saturating_sub(self.start_ns)
    }
}

/// A non-span point event (an instant or a counter sample) on the common
/// timeline. Owned strings/ints (see [`TraceSpan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracePoint {
    pub kind: ChunkEventType,
    pub ts_ns: u64,
    pub scope: String,
    pub name: String,
    pub value: u64,
    pub pid: i64,
    pub tid: i64,
    pub process_label: String,
    pub thread_label: String,
}

/// The presentation-neutral result of decoding a set of CHUNK buffers: every
/// span paired and anchored on the common timeline, every point event, and the
/// global epoch (the earliest realtime instant). Both the Perfetto converter
/// and the offline trace TUI build their views from THIS — one decode path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedTrace {
    pub spans: Vec<TraceSpan>,
    pub points: Vec<TracePoint>,
    /// Earliest realtime across all records (`0` if empty); a viewer subtracts
    /// this to render a timeline that starts near zero.
    pub epoch_ns: u64,
    /// Per-process labels `(synth_pid, process_label)` for EVERY process seen
    /// during decode — including processes whose chunks carry only Flow/end-only
    /// records or zero records (which yield no spans/points). The converter emits
    /// process-name metadata from this so naming is not lost for such chunks.
    pub process_labels: Vec<(i64, String)>,
    /// Per-thread labels `(synth_pid, tid, thread_label)` for EVERY thread seen
    /// during decode (same rationale as `process_labels`).
    pub thread_labels: Vec<(i64, i64, String)>,
}

impl DecodedTrace {
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty() && self.points.is_empty()
    }
}

#[derive(Default)]
struct Naming {
    /// (node, real_pid) -> synthetic Chrome pid (dense, hosts kept distinct).
    pid_map: HashMap<(String, u32), i64>,
    next_pid: i64,
    /// synth_pid -> "process @ node (pid N)".
    process_names: HashMap<i64, String>,
    /// (synth_pid, tid) -> thread name.
    thread_names: HashMap<(i64, i64), String>,
}

impl Naming {
    fn synth_pid(&mut self, node: &str, pid: u32, process_name: &str) -> i64 {
        if let Some(&p) = self.pid_map.get(&(node.to_string(), pid)) {
            return p;
        }
        self.next_pid += 1;
        let p = self.next_pid;
        self.pid_map.insert((node.to_string(), pid), p);
        self.process_names
            .insert(p, format!("{process_name} @ {node} (pid {pid})"));
        p
    }
}

/// Read a trace file into decoded CHUNK buffers: `.atrc` containers yield one
/// buffer per frame (`aorta_core::trace::sink::decode_container` — a truncated
/// crash tail is dropped with a warning, complete frames survive); legacy
/// `*.chunk.zst` yields one; uncompressed `*.chunk` is taken as-is.
pub fn read_chunk_file(path: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
    let raw = std::fs::read(path)?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    // `.atrc.active` = a crash-orphaned (or live) container that was never
    // finalized: its complete frames are exactly the salvageable data, so it
    // decodes like a finalized container (the torn tail, if any, is dropped).
    let zst_like =
        name.ends_with(".chunk.zst") || name.ends_with(".atrc") || name.ends_with(".atrc.active");
    if zst_like {
        let (chunks, truncated) = aorta_core::trace::sink::decode_container(&raw)?;
        if truncated {
            eprintln!(
                "warning: {} has a truncated tail frame (crash signature); kept {} complete chunk(s)",
                path.display(),
                chunks.len()
            );
        }
        Ok(chunks)
    } else {
        Ok(vec![raw])
    }
}

/// Find all `*.atrc` (incl. crash-orphaned `*.atrc.active`) / `*.chunk.zst` /
/// `*.chunk` files under the given inputs
/// (a file is taken as-is; a directory is scanned non-recursively).
pub fn collect_inputs(inputs: &[PathBuf]) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for input in inputs {
        if input.is_dir() {
            for entry in std::fs::read_dir(input)? {
                let p = entry?.path();
                if is_chunk(&p) {
                    out.push(p);
                }
            }
        } else if input.is_file() {
            out.push(input.clone());
        }
    }
    out.sort();
    Ok(out)
}

fn is_chunk(p: &Path) -> bool {
    match p.file_name().and_then(|n| n.to_str()) {
        Some(n) => {
            n.ends_with(".atrc")
                || n.ends_with(".atrc.active")
                || n.ends_with(".chunk.zst")
                || n.ends_with(".chunk")
        }
        None => false,
    }
}

/// Largest tolerated disagreement between a chunk's own wall anchor and the
/// per-process anchor before the chunk falls back to its own (a real wall
/// step — NTP/RTC — or a pid reused across boots).
pub const MAX_PROCESS_ANCHOR_SKEW_NS: u64 = 1_000_000_000;

/// Converter presentation options.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConvertOptions {
    /// Re-home `aorta.stage` slices onto per-primitive stage lanes (see
    /// module docs). Default OFF: slices stay on their real OS threads.
    pub stage_lanes: bool,
    /// Emit ABSOLUTE wall-clock timestamps (realtime µs since the Unix epoch)
    /// instead of subtracting the trace's global epoch. Perfetto then shows the
    /// real calendar time of every slice (e.g. `2026-07-04 08:36:…` UTC) rather
    /// than a `1970-01-01`-relative offset. Default OFF because f64 µs quantizes
    /// to ~250 ns at 2026 magnitudes; slice *durations* stay exact regardless
    /// (they are computed from the raw ns delta, not the two absolute stamps).
    pub wall_clock: bool,
}

/// Scope whose slices re-home onto stage lanes (see module docs).
const LANE_SCOPE: &str = "aorta.stage";
/// Synthetic stage-lane tids live far above real OS tids (and sort after them).
const LANE_TID_BASE: i64 = 1_000_000_000;
/// Synthetic overlap-spill sub-lane tids (see the overlap pass). A separate
/// base from [`LANE_TID_BASE`] so the two re-homings never collide.
const OVERLAP_TID_BASE: i64 = 2_000_000_000;

/// The per-primitive lane group key: the `:name` suffix interned by
/// `stage_event_for` / `marker_event_for`; a name without a suffix groups by
/// its full name.
fn lane_suffix(name: &str) -> &str {
    name.rsplit_once(':').map(|(_, sfx)| sfx).unwrap_or(name)
}

/// Convert decoded (uncompressed) chunk buffers → a Chrome trace (default
/// presentation: real threads, no stage lanes).
pub fn convert_buffers(chunks: &[Vec<u8>]) -> anyhow::Result<ChromeTrace> {
    convert_buffers_with(chunks, ConvertOptions::default())
}

/// Decode + anchor + pair a set of (uncompressed) chunk buffers into the
/// presentation-neutral [`DecodedTrace`]. This is the ONE place the per-process
/// affine anchor math and the begin/end pairing live; both the Perfetto
/// converter ([`convert_buffers_with`]) and the offline trace TUI build their
/// views on top of this — so the two can never diverge in how a span's start /
/// end / duration / parent is computed (DRY, mirroring the chunk producer ↔
/// converter "one binding" rule).
pub fn decode_trace(chunks: &[Vec<u8>]) -> anyhow::Result<DecodedTrace> {
    let mut naming = Naming::default();

    // ---- pass 1: one wall↔mono anchor per process (earliest chunk wins) ----
    let mut anchors: HashMap<i64, (u64, u64)> = HashMap::new(); // synth_pid -> (mono, wall)
    for buf in chunks {
        let chunk = root_as_chunk(buf).map_err(|e| anyhow::anyhow!("invalid trace chunk: {e}"))?;
        let node = chunk.node_id().unwrap_or("node").to_string();
        let process_name = chunk.process_name().unwrap_or("process").to_string();
        let synth_pid = naming.synth_pid(&node, chunk.pid(), &process_name);
        let cand = (chunk.mono_epoch_ns(), chunk.wall_start_realtime_ns());
        let entry = anchors.entry(synth_pid).or_insert(cand);
        if cand.0 < entry.0 {
            *entry = cand;
        }
    }

    // ---- pass 2: decode every record onto the common timeline ----
    let mut decoded: Vec<DecodedRecord> = Vec::new();
    let mut min_realtime = u64::MAX;

    for buf in chunks {
        let chunk = root_as_chunk(buf).map_err(|e| anyhow::anyhow!("invalid trace chunk: {e}"))?;
        let node = chunk.node_id().unwrap_or("node").to_string();
        let process_name = chunk.process_name().unwrap_or("process").to_string();
        let real_pid = chunk.pid();
        let synth_pid = naming.synth_pid(&node, real_pid, &process_name);

        // Use the PROCESS anchor unless this chunk's own anchor disagrees
        // beyond the tolerance (wall step / pid reuse) — see module docs.
        let (a_mono, a_wall) = anchors[&synth_pid];
        let projected = a_wall.saturating_add(chunk.mono_epoch_ns().saturating_sub(a_mono));
        let (eff_mono, eff_wall) =
            if projected.abs_diff(chunk.wall_start_realtime_ns()) <= MAX_PROCESS_ANCHOR_SKEW_NS {
                (a_mono, a_wall)
            } else {
                (chunk.mono_epoch_ns(), chunk.wall_start_realtime_ns())
            };

        let scopes = chunk.scopes();
        let events = chunk.events();
        let threads = chunk.threads();
        let records = match chunk.records() {
            Some(r) => r,
            None => continue,
        };
        for i in 0..records.len() {
            let r = records.get(i);
            let (os_tid, tname) = match threads {
                Some(t) if (r.thread_iid() as usize) < t.len() => {
                    let te = t.get(r.thread_iid() as usize);
                    (te.os_tid() as i64, te.name().unwrap_or("").to_string())
                }
                _ => (0, String::new()),
            };
            naming
                .thread_names
                .entry((synth_pid, os_tid))
                .or_insert_with(|| {
                    if tname.is_empty() {
                        format!("tid {os_tid}")
                    } else {
                        tname.clone()
                    }
                });
            let cat = scopes
                .filter(|s| (r.scope_iid() as usize) < s.len())
                .map(|s| s.get(r.scope_iid() as usize).to_string())
                .unwrap_or_default();
            let name = events
                .filter(|e| (r.event_iid() as usize) < e.len())
                .map(|e| e.get(r.event_iid() as usize).to_string())
                .unwrap_or_default();
            let mut trace_id = [0u8; 16];
            let arr = r.trace_id();
            for (k, slot) in trace_id.iter_mut().enumerate() {
                *slot = arr.get(k);
            }
            let abs_mono = chunk.mono_epoch_ns().saturating_add(r.delta_ns());
            let realtime_ns = eff_wall.saturating_add(abs_mono.saturating_sub(eff_mono));
            min_realtime = min_realtime.min(realtime_ns);
            decoded.push(DecodedRecord {
                trace_id,
                span_id: r.span_id(),
                parent_span_id: r.parent_span_id(),
                realtime_ns,
                thread_delta_ns: r.thread_delta_ns(),
                value: r.value(),
                synth_pid,
                tid: os_tid,
                name,
                cat,
                event_type: ChunkEventType::from_u8(r.event_type()),
                is_async: r.flags() & FLAG_ASYNC != 0,
            });
        }
    }

    let epoch = if min_realtime == u64::MAX {
        0
    } else {
        min_realtime
    };

    // Label lookups: every span/point carries its OWNED process/thread label so
    // a consumer (the TUI) needs no Naming at all, and the converter rebuilds
    // its track-metadata maps from these.
    let label_of = |pid: i64, tid: i64| -> (String, String) {
        let proc = naming.process_names.get(&pid).cloned().unwrap_or_default();
        let thr = naming
            .thread_names
            .get(&(pid, tid))
            .cloned()
            .unwrap_or_else(|| format!("tid {tid}"));
        (proc, thr)
    };

    // ---- pair begins/ends; instants & counters become points ----
    struct Begin {
        ts: u64,
        name: String,
        cat: String,
        pid: i64,
        tid: i64,
        trace_id: [u8; 16],
        span_id: u64,
        parent: u64,
        thread_delta: u64,
        is_async: bool,
    }
    // Begin/end are ALWAYS within one process, so pair by (synth_pid, trace_id,
    // span_id): span_id is only process-local (counter^salt), so a bare
    // (trace_id, span_id) key would mis-pair if two processes in one propagated
    // trace collided.
    type PairKey = (i64, [u8; 16], u64);
    let mut begins: HashMap<PairKey, Begin> = HashMap::new();
    let mut ends: HashMap<PairKey, (u64, u64)> = HashMap::new(); // ts, thread_delta
    let mut points: Vec<TracePoint> = Vec::new();

    for d in &decoded {
        match d.event_type {
            ChunkEventType::SliceBegin => {
                begins.insert(
                    (d.synth_pid, d.trace_id, d.span_id),
                    Begin {
                        ts: d.realtime_ns,
                        name: d.name.clone(),
                        cat: d.cat.clone(),
                        pid: d.synth_pid,
                        tid: d.tid,
                        trace_id: d.trace_id,
                        span_id: d.span_id,
                        parent: d.parent_span_id,
                        thread_delta: d.thread_delta_ns,
                        is_async: d.is_async,
                    },
                );
            }
            ChunkEventType::SliceEnd => {
                ends.insert(
                    (d.synth_pid, d.trace_id, d.span_id),
                    (d.realtime_ns, d.thread_delta_ns),
                );
            }
            ChunkEventType::Instant | ChunkEventType::Counter => {
                let (process_label, thread_label) = label_of(d.synth_pid, d.tid);
                points.push(TracePoint {
                    kind: d.event_type,
                    ts_ns: d.realtime_ns,
                    scope: d.cat.clone(),
                    name: d.name.clone(),
                    value: d.value,
                    pid: d.synth_pid,
                    tid: d.tid,
                    process_label,
                    thread_label,
                });
            }
            ChunkEventType::Flow => {}
        }
    }

    // ---- materialize spans (unterminated begin → zero-width marker) ----
    // Sorted by (pid, tid, start, span_id) so the output order is deterministic
    // and independent of HashMap iteration (the converter re-sorts where it
    // needs lane-stable order, so this does not affect the Chrome JSON).
    let mut spans: Vec<TraceSpan> = Vec::with_capacity(begins.len());
    for (key, b) in begins {
        let (end, thread_delta) = match ends.get(&key) {
            // `thread_delta` rides the END record (CPU time accumulated over the
            // span); use the END record's value verbatim (matching the original
            // Chrome-JSON behavior — do NOT fall back to the begin's value).
            Some(&(end_ts, td)) => (end_ts.max(b.ts), td),
            // Unterminated begin → zero-width marker; only the begin's value exists.
            None => (b.ts, b.thread_delta),
        };
        let (process_label, thread_label) = label_of(b.pid, b.tid);
        spans.push(TraceSpan {
            trace_id: b.trace_id,
            span_id: b.span_id,
            parent_span_id: b.parent,
            start_ns: b.ts,
            end_ns: end,
            thread_delta_ns: thread_delta,
            scope: b.cat,
            name: b.name,
            pid: b.pid,
            tid: b.tid,
            process_label,
            thread_label,
            is_async: b.is_async,
        });
    }
    spans.sort_by(|a, b| {
        a.pid
            .cmp(&b.pid)
            .then(a.tid.cmp(&b.tid))
            .then(a.start_ns.cmp(&b.start_ns))
            .then(b.end_ns.cmp(&a.end_ns))
            .then(a.span_id.cmp(&b.span_id))
    });
    points.sort_by(|a, b| {
        a.pid
            .cmp(&b.pid)
            .then(a.tid.cmp(&b.tid))
            .then(a.ts_ns.cmp(&b.ts_ns))
    });

    // Surface the COMPLETE naming accumulated during decode (process/thread
    // labels for every process/thread seen — including Flow/end-only/zero-record
    // chunks that produce no spans/points). Sorted for deterministic output.
    let mut process_labels: Vec<(i64, String)> = naming.process_names.into_iter().collect();
    process_labels.sort();
    let mut thread_labels: Vec<(i64, i64, String)> = naming
        .thread_names
        .into_iter()
        .map(|((pid, tid), label)| (pid, tid, label))
        .collect();
    thread_labels.sort();

    Ok(DecodedTrace {
        spans,
        points,
        epoch_ns: epoch,
        process_labels,
        thread_labels,
    })
}

/// Convert decoded (uncompressed) chunk buffers → a Chrome trace.
pub fn convert_buffers_with(
    chunks: &[Vec<u8>],
    opts: ConvertOptions,
) -> anyhow::Result<ChromeTrace> {
    // ONE decode + anchor + pairing path, shared with the TUI.
    let decoded = decode_trace(chunks)?;
    // Relative timeline subtracts the global epoch to keep sub-µs precision in
    // f64 µs; `--wall-clock` keeps the absolute realtime so Perfetto renders the
    // real calendar time (slice durations stay exact either way — see below).
    let epoch = if opts.wall_clock { 0 } else { decoded.epoch_ns };
    let us = |ns: u64| (ns.saturating_sub(epoch)) as f64 / 1000.0;

    // Seed the track-metadata maps from the COMPLETE naming decode surfaced —
    // covering processes/threads whose chunks carry only Flow/end-only/zero
    // records (which yield no spans/points), so their metadata is not lost (the
    // lane projection below inserts SYNTHETIC lane/overlap tids into `thread_names`).
    let mut naming = Naming::default();
    for (pid, label) in &decoded.process_labels {
        naming
            .process_names
            .entry(*pid)
            .or_insert_with(|| label.clone());
    }
    for (pid, tid, label) in &decoded.thread_labels {
        naming
            .thread_names
            .entry((*pid, *tid))
            .or_insert_with(|| label.clone());
    }

    let mut events_out: Vec<TraceEvent> = Vec::new();

    // ---- instants & counters stay on real threads ----
    for p in &decoded.points {
        match p.kind {
            ChunkEventType::Instant => {
                events_out.push(TraceEvent {
                    name: Some(p.name.clone()),
                    cat: Some(p.scope.clone()),
                    ph: "i",
                    ts: us(p.ts_ns),
                    dur: None,
                    tdur: None,
                    pid: p.pid,
                    tid: p.tid,
                    id: None,
                    id2: None,
                    bp: None,
                    s: Some("t"),
                    args: None,
                });
            }
            ChunkEventType::Counter => {
                let mut map = serde_json::Map::new();
                map.insert(p.name.clone(), serde_json::json!(p.value));
                events_out.push(TraceEvent {
                    name: Some(p.name.clone()),
                    cat: Some(p.scope.clone()),
                    ph: "C",
                    ts: us(p.ts_ns),
                    dur: None,
                    tdur: None,
                    pid: p.pid,
                    tid: p.tid,
                    id: None,
                    id2: None,
                    bp: None,
                    s: None,
                    args: Some(serde_json::Value::Object(map)),
                });
            }
            _ => {}
        }
    }

    // ---- materialize slices from the shared spans ----
    struct SliceOut {
        ts: u64,
        end: u64,
        tdur: Option<f64>,
        name: String,
        cat: String,
        pid: i64,
        tid: i64,
        trace_id: [u8; 16],
        span_id: u64,
        parent: u64,
        is_async: bool,
        /// Original thread label when re-homed onto a lane (→ `args.thread`).
        orig_thread: Option<String>,
        /// Nanoseconds clamped off the END so a tree child nests cleanly under
        /// an ancestor it slightly overhangs (→ `args.overhang_ns`; 0 = none).
        overhang_ns: u64,
    }
    let mut slices: Vec<SliceOut> = Vec::with_capacity(decoded.spans.len());
    for s in &decoded.spans {
        slices.push(SliceOut {
            ts: s.start_ns,
            end: s.end_ns,
            tdur: (s.thread_delta_ns > 0).then(|| s.thread_delta_ns as f64 / 1000.0),
            name: s.name.clone(),
            cat: s.scope.clone(),
            pid: s.pid,
            tid: s.tid,
            trace_id: s.trace_id,
            span_id: s.span_id,
            parent: s.parent_span_id,
            is_async: s.is_async,
            orig_thread: None,
            overhang_ns: 0,
        });
    }

    // ---- stage lanes (opt-in): re-home `aorta.stage` slices ----
    let mut groups: HashMap<(i64, String), Vec<usize>> = HashMap::new();
    for (i, sl) in slices.iter().enumerate() {
        if opts.stage_lanes && sl.cat == LANE_SCOPE {
            groups
                .entry((sl.pid, lane_suffix(&sl.name).to_string()))
                .or_default()
                .push(i);
        }
    }
    let mut group_keys: Vec<_> = groups.keys().cloned().collect();
    group_keys.sort(); // deterministic lane numbering
    let mut lane_counter: HashMap<i64, i64> = HashMap::new();
    for key in group_keys {
        let mut idxs = groups.remove(&key).expect("group key");
        // Start ascending; at equal starts the LONGER slice first, so a root
        // and a child anchored at the same instant nest root-outermost.
        idxs.sort_by(|&a, &b| {
            slices[a]
                .ts
                .cmp(&slices[b].ts)
                .then(slices[b].end.cmp(&slices[a].end))
        });
        // Greedy lane allocation: a slice goes to the first lane where it
        // either starts after everything open has closed, or nests strictly
        // inside the innermost still-open interval. Anything else (e.g. a
        // user_callback overhanging its root because the handler replied
        // inline) spills to the next lane — never a nesting violation.
        let mut lane_stacks: Vec<Vec<(u64, u64)>> = Vec::new();
        let mut lane_tids: Vec<i64> = Vec::new();
        for &i in &idxs {
            let (ts, end) = (slices[i].ts, slices[i].end);
            let mut placed = None;
            for (k, stack) in lane_stacks.iter_mut().enumerate() {
                while let Some(&(_, top_end)) = stack.last() {
                    if top_end <= ts {
                        stack.pop();
                    } else {
                        break;
                    }
                }
                let fits = match stack.last() {
                    None => true,
                    Some(&(_, top_end)) => end <= top_end,
                };
                if fits {
                    stack.push((ts, end));
                    placed = Some(k);
                    break;
                }
            }
            let k = match placed {
                Some(k) => k,
                None => {
                    let counter = lane_counter.entry(slices[i].pid).or_insert(0);
                    *counter += 1;
                    let tid = LANE_TID_BASE + *counter;
                    let label = if lane_stacks.is_empty() {
                        format!("{} [stages]", key.1)
                    } else {
                        format!("{} [stages {}]", key.1, lane_stacks.len() + 1)
                    };
                    naming.thread_names.insert((slices[i].pid, tid), label);
                    lane_stacks.push(vec![(ts, end)]);
                    lane_tids.push(tid);
                    lane_stacks.len() - 1
                }
            };
            slices[i].orig_thread = naming
                .thread_names
                .get(&(slices[i].pid, slices[i].tid))
                .cloned();
            slices[i].tid = lane_tids[k];
        }
    }

    // ---- make every real-thread track a valid Perfetto stack ----
    // Perfetto's per-track model DROPS complete ('X') slices that partially
    // overlap (cross — neither nested nor disjoint) another on the same track.
    // Aorta stage spans are independently-emitted completed intervals, not a
    // call stack, so two kinds of crossing arise on a shared OS worker thread:
    //
    //   1. A tree CHILD that slightly overhangs its ancestor — e.g.
    //      `action.user_callback` ends a few hundred µs after
    //      `action.accepted_to_finished`, because the goal's terminal time is
    //      recorded when the handler calls `succeed()` but the callback span
    //      only closes once the handler RETURNS. These ARE parent/child and
    //      should render as a nested waterfall; we CLAMP the child's end to the
    //      ancestor's so it nests in place (the trimmed tail → `args.overhang_ns`).
    //   2. Two UNRELATED slices (different subtrees) that tokio happened to
    //      schedule onto one worker thread and that genuinely run concurrently
    //      — e.g. `goal_admission` vs `accepted_to_execution_started`. These are
    //      not nestable; spill the crosser to a numbered sub-lane
    //      ("<thread> [overlap N]", original thread kept in `args.thread`).
    //
    // Lane 0 keeps the real OS thread, so a track with no crossing is left
    // byte-identical. Runs in BOTH projections (after the stage-lane re-homing),
    // since non-stage slices stay on real threads even under `--stage-lanes`.
    {
        // `desc` is a (possibly transitive) tree descendant of `anc` when its
        // parent chain reaches `anc`'s span (both on the same track / trace).
        // A free fn (not a closure) so its `&slices` borrow is per-call and
        // doesn't conflict with the in-loop mutation of `slices[i].end`.
        fn is_descendant_of(
            slices: &[SliceOut],
            by_span: &HashMap<([u8; 16], u64), usize>,
            desc: usize,
            anc: usize,
            max_depth: usize,
        ) -> bool {
            let tid = slices[anc].trace_id;
            if slices[desc].trace_id != tid {
                return false;
            }
            let anc_span = slices[anc].span_id;
            let mut cur = slices[desc].parent;
            for _ in 0..max_depth {
                if cur == 0 {
                    return false;
                }
                if cur == anc_span {
                    return true;
                }
                match by_span.get(&(tid, cur)) {
                    Some(&p) => cur = slices[p].parent,
                    None => return false,
                }
            }
            false
        }

        let mut by_track: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
        for (i, sl) in slices.iter().enumerate() {
            // Async WAIT spans (queue_wait/group_wait) overlap by nature and are
            // emitted below as Chrome nestable-async slices — Perfetto groups
            // those by NAME into one depth-stacked track, so they must NOT go
            // through the per-thread overlap spill (which is what produced the
            // many "<thread> [overlap N]" lanes).
            if sl.is_async {
                continue;
            }
            by_track.entry((sl.pid, sl.tid)).or_default().push(i);
        }
        let mut track_keys: Vec<_> = by_track.keys().cloned().collect();
        track_keys.sort();
        let mut overlap_counter: HashMap<i64, i64> = HashMap::new();
        let depth_cap = slices.len();
        for key in track_keys {
            let mut idxs = by_track.remove(&key).expect("track key");
            if idxs.len() < 2 {
                continue; // a lone slice can never cross
            }
            // Same-trace span_id -> idx, so we can test tree ancestry below.
            let by_span: HashMap<([u8; 16], u64), usize> = idxs
                .iter()
                .map(|&i| ((slices[i].trace_id, slices[i].span_id), i))
                .collect();
            // Start ascending; at equal starts the LONGER first, so an
            // enclosing slice opens before a slice it strictly contains.
            idxs.sort_by(|&a, &b| {
                slices[a]
                    .ts
                    .cmp(&slices[b].ts)
                    .then(slices[b].end.cmp(&slices[a].end))
            });
            // Each lane is a stack of (end, slice_idx) for the open intervals.
            let mut lane_stacks: Vec<Vec<(u64, usize)>> = Vec::new();
            let mut lane_tids: Vec<i64> = Vec::new();
            for &i in &idxs {
                let (ts, mut end) = (slices[i].ts, slices[i].end);
                let mut placed = None;
                for (k, stack) in lane_stacks.iter_mut().enumerate() {
                    while let Some(&(top_end, _)) = stack.last() {
                        if top_end <= ts {
                            stack.pop();
                        } else {
                            break;
                        }
                    }
                    match stack.last() {
                        // Lane idle, or this slice nests strictly inside the
                        // innermost open one → it fits as-is.
                        None => {
                            stack.push((end, i));
                            placed = Some(k);
                            break;
                        }
                        Some(&(top_end, _)) if end <= top_end => {
                            stack.push((end, i));
                            placed = Some(k);
                            break;
                        }
                        // Overhangs the innermost open slice: if it's a tree
                        // descendant of it, clamp the overhang so it nests here
                        // (waterfall); otherwise it's unrelated → try next lane.
                        Some(&(top_end, top_i))
                            if is_descendant_of(&slices, &by_span, i, top_i, depth_cap) =>
                        {
                            slices[i].overhang_ns = end - top_end;
                            end = top_end;
                            slices[i].end = end;
                            stack.push((end, i));
                            placed = Some(k);
                            break;
                        }
                        Some(_) => {}
                    }
                }
                let k = match placed {
                    Some(k) => k,
                    None => {
                        let tid = if lane_stacks.is_empty() {
                            key.1 // lane 0 keeps the real OS thread
                        } else {
                            let counter = overlap_counter.entry(key.0).or_insert(0);
                            *counter += 1;
                            let tid = OVERLAP_TID_BASE + *counter;
                            let base = naming
                                .thread_names
                                .get(&(key.0, key.1))
                                .cloned()
                                .unwrap_or_else(|| format!("tid {}", key.1));
                            naming.thread_names.insert(
                                (key.0, tid),
                                format!("{} [overlap {}]", base, lane_stacks.len() + 1),
                            );
                            tid
                        };
                        lane_stacks.push(vec![(end, i)]);
                        lane_tids.push(tid);
                        lane_stacks.len() - 1
                    }
                };
                if lane_tids[k] != slices[i].tid {
                    // Preserve a stage-lane's orig_thread; otherwise record the
                    // real OS thread this slice spilled off of.
                    if slices[i].orig_thread.is_none() {
                        slices[i].orig_thread = naming.thread_names.get(&(key.0, key.1)).cloned();
                    }
                    slices[i].tid = lane_tids[k];
                }
            }
        }
    }

    // ---- span locations (FINAL, post-lane) for flow arrows ----
    // Keyed by (trace_id, span_id): a parent link resolves ACROSS processes.
    let mut span_loc: HashMap<([u8; 16], u64), (i64, i64, u64)> = HashMap::new();
    for sl in &slices {
        span_loc.insert((sl.trace_id, sl.span_id), (sl.pid, sl.tid, sl.ts));
    }
    // Async slices live on name-grouped async tracks, NOT on the thread row
    // their (pid,tid) names — so a flow endpoint that lands there would bind to
    // whatever sync slice sits on that thread at that ts (the wrong slice). Skip
    // any flow that touches an async span; the async track grouping conveys the
    // queue_wait → group_wait → callback relationship without the mis-bound
    // arrow storm. Sync↔sync links (e.g. topic.publish → topic.callback) keep
    // their flow arrows.
    let async_spans: std::collections::HashSet<([u8; 16], u64)> = slices
        .iter()
        .filter(|sl| sl.is_async)
        .map(|sl| (sl.trace_id, sl.span_id))
        .collect();

    // ---- per-callback scheduler-wait breakdown (→ args on the callback) ----
    // A dispatch runs sequentially on ONE worker thread: queue_wait → (lock)
    // group_wait → the callback, so `group_wait.end == callback.start` and
    // `queue_wait.end ≈ group_wait.start` to within a couple of monotonic-read
    // ns. The async waits render on their own (un-addressable) tracks, so we
    // can't draw an arrow from a callback to ITS waits — instead we match each
    // callback (`sched.callback` for timers, `topic.callback:*` for
    // subscribers) to the same-thread waits whose ends sit at its dispatch, and
    // hang their durations on the callback as args. Click any callback to read
    // how long it spent queued / blocked on its group lock. Trace-agnostic, so
    // it works even though the topic.callback and the sched waits are separate
    // traces. The window rejects a far (different-dispatch) wait — e.g. a
    // Reentrant timer that has queue_wait but no group_wait.
    const ASSOC_WINDOW_NS: u64 = 200_000; // 200µs ≫ read jitter, ≪ dispatch spacing
    let mut q_by_track: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    let mut g_by_track: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    for (i, sl) in slices.iter().enumerate() {
        if !sl.is_async {
            continue;
        }
        if sl.name == "sched.queue_wait" {
            q_by_track.entry((sl.pid, sl.tid)).or_default().push(i);
        } else if sl.name.starts_with("sched.group_wait") {
            g_by_track.entry((sl.pid, sl.tid)).or_default().push(i);
        }
    }
    // callback slice idx -> (queue_wait_ns?, (group_wait_ns, group_label)?)
    let nearest_end = |idxs: Option<&Vec<usize>>, target: u64| -> Option<usize> {
        idxs?
            .iter()
            .copied()
            .map(|j| (j, (slices[j].end as i128 - target as i128).unsigned_abs()))
            .filter(|&(_, d)| d as u64 <= ASSOC_WINDOW_NS)
            .min_by_key(|&(_, d)| d)
            .map(|(j, _)| j)
    };
    let mut assoc: HashMap<usize, (Option<u64>, Option<(u64, String)>)> = HashMap::new();
    for (i, sl) in slices.iter().enumerate() {
        let is_cb = sl.name.starts_with("sched.callback") || sl.name.starts_with("topic.callback");
        if !is_cb {
            continue;
        }
        let g = nearest_end(g_by_track.get(&(sl.pid, sl.tid)), sl.ts);
        let group = g.map(|j| {
            (
                slices[j].end - slices[j].ts,
                lane_suffix(&slices[j].name).to_string(),
            )
        });
        let q_target = g.map(|j| slices[j].ts).unwrap_or(sl.ts);
        let q = nearest_end(q_by_track.get(&(sl.pid, sl.tid)), q_target);
        let queue = q.map(|j| slices[j].end - slices[j].ts);
        if queue.is_some() || group.is_some() {
            assoc.insert(i, (queue, group));
        }
    }

    // ---- emit slices + a flow arrow for resolvable SYNC parent links ----
    let mut flow_seq: u64 = 0;
    let mut async_seq: u64 = 0;
    let mut flows: Vec<TraceEvent> = Vec::new();
    for (slice_idx, sl) in slices.iter().enumerate() {
        let mut args = serde_json::Map::new();
        if let Some(orig) = &sl.orig_thread {
            args.insert("thread".to_string(), serde_json::json!(orig));
        }
        if sl.overhang_ns > 0 {
            // Tail trimmed so this child nests under an ancestor it slightly
            // overhung; the true end is `ts + dur + overhang_ns`.
            args.insert("overhang_ns".to_string(), serde_json::json!(sl.overhang_ns));
        }
        // Scheduler-wait breakdown for a callback (see `assoc` above).
        if let Some((queue_ns, group)) = assoc.get(&slice_idx) {
            let mut sched = 0u64;
            if let Some(q) = queue_ns {
                args.insert("queue_wait_ns".to_string(), serde_json::json!(q));
                sched += q;
            }
            if let Some((g_ns, label)) = group {
                args.insert("group_wait_ns".to_string(), serde_json::json!(g_ns));
                args.insert("group".to_string(), serde_json::json!(label));
                sched += g_ns;
            }
            args.insert("sched_overhead_ns".to_string(), serde_json::json!(sched));
        }
        let args = if args.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(args))
        };
        if sl.is_async {
            // Nestable-async begin/end: Perfetto groups these by NAME into ONE
            // process-scoped track and depth-stacks overlapping ones — so all
            // `sched.queue_wait` share a track and each `sched.group_wait:<g>`
            // gets its own (the per-group contention view). `id` only
            // correlates THIS slice's begin/end, so it is unique per slice.
            // PROCESS-scoped via id2.local so Perfetto files the track under
            // this process (a bare id lands it in a GLOBAL "Legacy Events"
            // group). The id only correlates this slice's begin/end.
            let id2 = serde_json::json!({ "local": format!("a{async_seq}") });
            async_seq += 1;
            events_out.push(TraceEvent {
                name: Some(sl.name.clone()),
                cat: Some(sl.cat.clone()),
                ph: "b",
                ts: us(sl.ts),
                dur: None,
                tdur: None,
                pid: sl.pid,
                tid: sl.tid,
                id: None,
                id2: Some(id2.clone()),
                bp: None,
                s: None,
                args,
            });
            events_out.push(TraceEvent {
                name: Some(sl.name.clone()),
                cat: Some(sl.cat.clone()),
                ph: "e",
                ts: us(sl.end),
                dur: None,
                tdur: None,
                pid: sl.pid,
                tid: sl.tid,
                id: None,
                id2: Some(id2),
                bp: None,
                s: None,
                args: None,
            });
        } else {
            events_out.push(TraceEvent {
                name: Some(sl.name.clone()),
                cat: Some(sl.cat.clone()),
                ph: "X",
                ts: us(sl.ts),
                // Duration from the raw ns delta (NOT `us(end) - us(ts)`): in
                // `--wall-clock` mode the two absolute stamps each quantize to
                // ~250 ns in f64, but the delta is small and stays exact.
                dur: Some(sl.end.saturating_sub(sl.ts) as f64 / 1000.0),
                tdur: sl.tdur,
                pid: sl.pid,
                tid: sl.tid,
                id: None,
                id2: None,
                bp: None,
                s: None,
                args,
            });
        }

        // Flow arrow for a SYNC parent link only (see `async_spans` above).
        if sl.parent != 0 && !sl.is_async && !async_spans.contains(&(sl.trace_id, sl.parent)) {
            if let Some(&(ppid, ptid, pts)) = span_loc.get(&(sl.trace_id, sl.parent)) {
                let id = format!("f{flow_seq}");
                flow_seq += 1;
                flows.push(flow_event("s", &id, us(pts), ppid, ptid, None));
                flows.push(flow_event("f", &id, us(sl.ts), sl.pid, sl.tid, Some("e")));
            }
        }
    }
    events_out.extend(flows);

    // ---- track metadata LAST (lane labels are known only now) ----
    for (pid, name) in &naming.process_names {
        events_out.push(meta_event("process_name", *pid, 0, name));
    }
    for ((pid, tid), name) in &naming.thread_names {
        events_out.push(meta_event("thread_name", *pid, *tid, name));
    }

    Ok(ChromeTrace {
        trace_events: events_out,
        display_time_unit: "ns",
    })
}

/// Read + decode a set of trace files into the shared [`DecodedTrace`] (the
/// offline TUI's entry point). Mirrors [`convert_files`]: a file that fails to
/// read/decode is SKIPPED with a warning so one corrupt file never aborts the
/// whole directory; uses the SAME reader path ([`read_chunk_file`] →
/// `decode_container`) and the SAME decode/pairing ([`decode_trace`]) as the
/// converter.
pub fn decode_trace_files(paths: &[PathBuf]) -> anyhow::Result<DecodedTrace> {
    let mut buffers = Vec::with_capacity(paths.len());
    for p in paths {
        match read_chunk_file(p) {
            Ok(chunks) => buffers.extend(chunks),
            Err(err) => eprintln!("warning: skipping {}: {err}", p.display()),
        }
    }
    decode_trace(&buffers)
}

/// Read + decode + convert a set of trace files (containers and/or legacy).
/// A file that fails to read/decode is SKIPPED with a warning — one corrupt
/// file must not abort the conversion of an entire trace directory.
pub fn convert_files(paths: &[PathBuf]) -> anyhow::Result<ChromeTrace> {
    convert_files_with(paths, ConvertOptions::default())
}

/// [`convert_files`] with explicit presentation options.
pub fn convert_files_with(paths: &[PathBuf], opts: ConvertOptions) -> anyhow::Result<ChromeTrace> {
    let mut buffers = Vec::with_capacity(paths.len());
    for p in paths {
        match read_chunk_file(p) {
            Ok(chunks) => buffers.extend(chunks),
            Err(err) => eprintln!("warning: skipping {}: {err}", p.display()),
        }
    }
    convert_buffers_with(&buffers, opts)
}

fn meta_event(name: &'static str, pid: i64, tid: i64, value: &str) -> TraceEvent {
    TraceEvent {
        name: Some(name.to_string()),
        cat: None,
        ph: "M",
        ts: 0.0,
        dur: None,
        tdur: None,
        pid,
        tid,
        id: None,
        id2: None,
        bp: None,
        s: None,
        args: Some(serde_json::json!({ "name": value })),
    }
}

fn flow_event(
    ph: &'static str,
    id: &str,
    ts: f64,
    pid: i64,
    tid: i64,
    bp: Option<&'static str>,
) -> TraceEvent {
    TraceEvent {
        name: Some("link".to_string()),
        cat: Some("flow".to_string()),
        ph,
        ts,
        dur: None,
        tdur: None,
        pid,
        tid,
        id: Some(id.to_string()),
        id2: None,
        bp,
        s: None,
        args: None,
    }
}

#[cfg(test)]
mod tests;
