//! Trace CHUNK: the canonical compact record format.
//!
//! This module wraps the committed FlatBuffers binding
//! ([`chunk_generated`]) and provides:
//!
//! - [`RingRecord`] — the fixed POD the hot path appends to a per-thread ring
//!   ([`super::ring`]). It carries **global** interning ids (scope handle,
//!   event-name id, thread id) and an **absolute** monotonic timestamp; the
//!   drain remaps these to chunk-local `iid`s and a chunk-relative `delta_ns`
//!   when it assembles a chunk.
//! - [`ChunkEventType`] — the five Perfetto TrackEvent primitives as a `u8`.
//! - [`build_chunk`] — the drain-side assembler: a slice of [`RingRecord`] plus
//!   provenance + name resolvers → a finished, independently-decodable
//!   FlatBuffers `Chunk` buffer (the interning **reset boundary**).
//!
//! The hot path never touches FlatBuffers; all chunk assembly is off the hot
//! path in the drain thread. The off-device `tools/aorta-trace2perfetto`
//! consumes the same binding via the re-exports here (one schema, one binding —
//! DRY across producer and converter).

#[path = "chunk_generated.rs"]
mod chunk_generated;

use std::collections::HashMap;

pub use chunk_generated::aorta::trace::{
    root_as_chunk, root_as_chunk_with_opts, Chunk, ChunkArgs, Record, ThreadEntry, ThreadEntryArgs,
    CHUNK_IDENTIFIER,
};

/// On-disk format version. Bumped on an incompatible `Chunk`/`Record` change;
/// decoders reject an unknown MAJOR version and skip unknown `feature_flags`
/// bits gracefully (forward-compat).
pub const CHUNK_FORMAT_VERSION: u16 = 1;

/// The five Perfetto TrackEvent primitives, encoded in [`RingRecord::event_type`]
/// / `Record.event_type`. A timed span is a `SliceBegin`+`SliceEnd` pair sharing
/// a `span_id`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkEventType {
    SliceBegin = 0,
    SliceEnd = 1,
    Instant = 2,
    Counter = 3,
    Flow = 4,
}

impl ChunkEventType {
    /// Reconstruct from the stored `u8` (unknown → `Instant`, the inert default).
    pub const fn from_u8(v: u8) -> ChunkEventType {
        match v {
            0 => ChunkEventType::SliceBegin,
            1 => ChunkEventType::SliceEnd,
            3 => ChunkEventType::Counter,
            4 => ChunkEventType::Flow,
            _ => ChunkEventType::Instant,
        }
    }

    /// Stable lowercase label (for the converter / debugging).
    pub const fn as_str(self) -> &'static str {
        match self {
            ChunkEventType::SliceBegin => "slice_begin",
            ChunkEventType::SliceEnd => "slice_end",
            ChunkEventType::Instant => "instant",
            ChunkEventType::Counter => "counter",
            ChunkEventType::Flow => "flow",
        }
    }
}

/// `flags.bit0`: span begin/end may cross threads (tokio migration) → the
/// converter emits it on a `span_id`-keyed async track, not a thread track.
pub const FLAG_ASYNC: u8 = 0b0000_0001;
/// `flags.bit1`: FLOW direction — set = this end is the *source* (out), clear =
/// the *sink* (in). Only meaningful when `event_type == Flow`.
pub const FLAG_FLOW_OUT: u8 = 0b0000_0010;

/// The fixed POD the hot path appends to a per-thread ring. `#[repr(C)]` + all
/// scalars → trivially `Copy`/memcpy. Interning ids are **global** here (the
/// process-wide scope/event/thread registries); the drain remaps them to
/// chunk-local `iid`s. `mono_ns` is an absolute [`crate::time::monotonic_ns`]
/// reading; the drain rebases it to `delta_ns` against the chunk epoch.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingRecord {
    pub trace_id: [u8; 16],
    pub span_id: u64,
    pub parent_span_id: u64,
    pub mono_ns: u64,
    pub thread_delta_ns: u64,
    pub value: u64,
    pub scope_id: u16,
    pub event_id: u16,
    pub thread_id: u16,
    pub level: u8,
    pub event_type: u8,
    pub flags: u8,
}

impl Default for RingRecord {
    fn default() -> Self {
        // A zeroed record is inert: trace_id all-zero is "invalid", so a
        // stale ring slot never decodes as a real event.
        RingRecord {
            trace_id: [0u8; 16],
            span_id: 0,
            parent_span_id: 0,
            mono_ns: 0,
            thread_delta_ns: 0,
            value: 0,
            scope_id: 0,
            event_id: 0,
            thread_id: 0,
            level: 0,
            event_type: ChunkEventType::Instant as u8,
            flags: 0,
        }
    }
}

/// Per-chunk provenance — lets a window/process be retrieved without decoding
/// (mirrored in the filename). Borrowed strings; the drain owns the storage.
#[derive(Debug, Clone)]
pub struct ChunkProvenance {
    pub node_id: String,
    pub run_id: String,
    pub pid: u32,
    pub process_name: String,
    pub host_clock_domain_id: u32,
    pub seq: u64,
    /// Records dropped (ring full) before this chunk was assembled.
    pub dropped: u64,
}

/// Build a finished, independently-decodable FlatBuffers `Chunk` from `records`.
///
/// `mono_epoch_ns` is the chunk's monotonic epoch (typically the min `mono_ns`
/// in the batch); `wall_start_realtime_ns` is the wall-clock instant that epoch
/// maps to (the per-chunk affine anchor for offline cross-host alignment).
/// The name resolvers turn the global interning ids in each [`RingRecord`] into
/// the chunk-local string/thread tables; passing them as closures keeps this
/// assembler unit-testable without the global registries.
#[allow(clippy::too_many_arguments)]
pub fn build_chunk(
    prov: &ChunkProvenance,
    mono_epoch_ns: u64,
    wall_start_realtime_ns: u64,
    records: &[RingRecord],
    mut scope_name: impl FnMut(u16) -> String,
    mut event_name: impl FnMut(u16) -> String,
    mut thread_info: impl FnMut(u16) -> (u32, String),
) -> Vec<u8> {
    // First pass: assign chunk-local iids in first-seen order, building the
    // string/thread tables. `span_index`/`parent_index` alias `span_id` the same
    // way (chunk-local compression aliases; `span_id` stays authoritative).
    let mut scope_map: HashMap<u16, u16> = HashMap::new();
    let mut event_map: HashMap<u16, u16> = HashMap::new();
    let mut thread_map: HashMap<u16, u16> = HashMap::new();
    let mut span_map: HashMap<u64, u16> = HashMap::new();
    let mut scopes: Vec<String> = Vec::new();
    let mut events: Vec<String> = Vec::new();
    let mut threads: Vec<(u32, String)> = Vec::new();

    let intern = |map: &mut HashMap<u16, u16>, list_len: &mut u16, id: u16| -> u16 {
        if let Some(&iid) = map.get(&id) {
            iid
        } else {
            let iid = *list_len;
            map.insert(id, iid);
            *list_len += 1;
            iid
        }
    };
    let mut scopes_len = 0u16;
    let mut events_len = 0u16;
    let mut threads_len = 0u16;

    // Reserve span_index 0 as "none" so parent_index 0 reliably means "parent not
    // in this chunk" (resolve by span_id instead). Real spans start at 1.
    //
    // PRE-PASS: assign a span_index to EVERY distinct span_id first, so a
    // `parent_index` resolves even when the parent's records appear LATER in the
    // batch. With end-time span emission an inner (child) span closes — and so is
    // appended — before its outer parent, so a single combined pass would always
    // see the child before the parent and store parent_index = 0.
    let mut next_span_index: u16 = 1;
    for r in records {
        if r.span_id != 0 {
            span_map.entry(r.span_id).or_insert_with(|| {
                let i = next_span_index;
                next_span_index = next_span_index.saturating_add(1);
                i
            });
        }
    }

    struct Remapped {
        scope_iid: u16,
        event_iid: u16,
        thread_iid: u16,
        span_index: u16,
        parent_index: u16,
    }
    let mut remapped: Vec<Remapped> = Vec::with_capacity(records.len());
    for r in records {
        let scope_iid = intern(&mut scope_map, &mut scopes_len, r.scope_id);
        if scope_iid as usize == scopes.len() {
            scopes.push(scope_name(r.scope_id));
        }
        let event_iid = intern(&mut event_map, &mut events_len, r.event_id);
        if event_iid as usize == events.len() {
            events.push(event_name(r.event_id));
        }
        let thread_iid = intern(&mut thread_map, &mut threads_len, r.thread_id);
        if thread_iid as usize == threads.len() {
            threads.push(thread_info(r.thread_id));
        }
        let span_index = span_map.get(&r.span_id).copied().unwrap_or(0);
        let parent_index = if r.parent_span_id == 0 {
            0
        } else {
            span_map.get(&r.parent_span_id).copied().unwrap_or(0)
        };
        remapped.push(Remapped {
            scope_iid,
            event_iid,
            thread_iid,
            span_index,
            parent_index,
        });
    }

    // Second pass: serialize. All child objects are created before the Chunk
    // table (FlatBuffers builds bottom-up).
    let mut fbb = flatbuffers::FlatBufferBuilder::with_capacity(64 + records.len() * 80);

    let scope_offs: Vec<_> = scopes.iter().map(|s| fbb.create_string(s)).collect();
    let scopes_vec = fbb.create_vector(&scope_offs);
    let event_offs: Vec<_> = events.iter().map(|s| fbb.create_string(s)).collect();
    let events_vec = fbb.create_vector(&event_offs);
    let level_labels = ["trace", "debug", "info", "warn", "error"];
    let level_offs: Vec<_> = level_labels.iter().map(|s| fbb.create_string(s)).collect();
    let levels_vec = fbb.create_vector(&level_offs);

    let thread_offs: Vec<_> = threads
        .iter()
        .map(|(os_tid, name)| {
            let name_off = fbb.create_string(name);
            ThreadEntry::create(
                &mut fbb,
                &ThreadEntryArgs {
                    os_tid: *os_tid,
                    name: Some(name_off),
                },
            )
        })
        .collect();
    let threads_vec = fbb.create_vector(&thread_offs);

    let record_structs: Vec<Record> = records
        .iter()
        .zip(remapped.iter())
        .map(|(r, m)| {
            Record::new(
                &r.trace_id,
                r.span_id,
                r.parent_span_id,
                r.mono_ns.saturating_sub(mono_epoch_ns),
                r.thread_delta_ns,
                r.value,
                m.scope_iid,
                m.event_iid,
                m.thread_iid,
                m.span_index,
                m.parent_index,
                r.level,
                r.event_type,
                r.flags,
            )
        })
        .collect();
    let records_vec = fbb.create_vector(&record_structs);

    let node_id = fbb.create_string(&prov.node_id);
    let run_id = fbb.create_string(&prov.run_id);
    let process_name = fbb.create_string(&prov.process_name);

    let chunk = Chunk::create(
        &mut fbb,
        &ChunkArgs {
            format_version: CHUNK_FORMAT_VERSION,
            feature_flags: 0,
            node_id: Some(node_id),
            run_id: Some(run_id),
            pid: prov.pid,
            process_name: Some(process_name),
            host_clock_domain_id: prov.host_clock_domain_id,
            mono_epoch_ns,
            wall_start_realtime_ns,
            seq: prov.seq,
            dropped: prov.dropped,
            scopes: Some(scopes_vec),
            events: Some(events_vec),
            levels: Some(levels_vec),
            threads: Some(threads_vec),
            records: Some(records_vec),
        },
    );
    chunk_generated::aorta::trace::finish_chunk_buffer(&mut fbb, chunk);
    fbb.finished_data().to_vec()
}

#[cfg(test)]
#[path = "../../test/trace_chunk_tests.rs"]
mod tests;
