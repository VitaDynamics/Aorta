//! Pass-1 planning (MCAP Player Proposal §6.1, §6.2, §6.3 static parts, §6.5).
//!
//! One linear walk per snapshot through the §6.7 reader adapter produces
//! everything the rest of the run needs: channels, schemas, recorder metadata,
//! the effective timeline, the per-topic selection bits and the verified safe
//! prefix. What is deliberately *not* produced is anything that grows with the
//! input length — chunk metadata is streamed, message-index records are
//! skipped, and pass 2 re-walks the same snapshot and recomputes per-message
//! effective times and selection predicates deterministically. Retained state
//! (channels, schemas, per-segment scalars) runs under a checked byte budget so
//! an adversarial channel/schema flood fails loudly instead of growing.
//!
//! Validation performed here, all hard errors:
//!
//! - **Format** (§6.1): profile `aorta`; `message_encoding` ∈ {`flatbuffer`
//!   (schema_id ≠ 0), `aorta_raw_bytes` (schema_id 0)}; schema `encoding`
//!   `flatbuffer`.
//! - **Channel consistency** (§6.1): `topic` == metadata `aorta_key`, and
//!   `key_suffix` is the group-relative suffix of `topic`.
//! - **Schema integrity** (§6.2): SHA-256 recomputed over each Schema record's
//!   bfbs (canonical [`SchemaHash::from_bfbs_bytes`]) must equal the channel's
//!   recorded `schema_hash`.
//! - **Segment sets** (§6.1): one group, one `recording_id`, at most one
//!   id-less input, unique + contiguous indices (unless `--allow-gaps`), no
//!   overlapping unrelated time ranges, and only a *final* truncated segment.
//! - **Topic kinds** (§6.3, static half): typed-only / raw-only /
//!   mixed → error / multi-schema × {strict, latest}.

use crate::reader::{
    Flow, McapWalker, ReadError, ReaderLimits, RecordEvent, RecordSink, SnapshotSource, WalkReport,
};
use crate::snapshot::Cancelled;
use crate::timeline::{TimelineAccumulator, Window};
use crate::validate::ValidatedLimits;
use crate::{PlayerConfig, SchemaPolicy, SkippedChannels, TruncationReport};
use anyhow::{anyhow, bail, Result};
use aorta_core::schema::SchemaHash;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(all(test, feature = "fault-injection"))]
pub(crate) static TEST_PLAN_STALL: std::sync::Mutex<Option<crate::live_tests::ThreadStall>> =
    std::sync::Mutex::new(None);

/// Checked ceiling on retained pass-1 planning state (MCAP Player Proposal
/// §6.1).
pub const DEFAULT_PLANNING_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// One input to pass 1: the pass-0 private snapshot plus the path it came from
/// (errors name the *source*, not the anonymous copy).
pub struct PlanInput<'a> {
    pub source: PathBuf,
    pub reader: &'a dyn SnapshotSource,
}

/// `--topics` / `--exclude`, matched against the *recorded* keys before any
/// `--group` remap (MCAP Player Proposal §6.5 step 3, §6.9).
///
/// One definition, two consumers: pass 1 computes the selection bits with it,
/// and pass 2 re-derives them from the same filter rather than carrying a
/// per-channel table across the passes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicFilter {
    topics: Vec<String>,
    exclude: Vec<String>,
}

impl TopicFilter {
    pub fn new(topics: Vec<String>, exclude: Vec<String>) -> Self {
        Self { topics, exclude }
    }

    /// A filter matches a topic name, a key suffix or a full key — exactly.
    pub fn accepts(&self, key: &str, key_suffix: &str, topic_suffix: &str) -> bool {
        let matches = |filters: &[String]| {
            filters
                .iter()
                .any(|f| f == key || f == key_suffix || f == topic_suffix)
        };
        if !self.topics.is_empty() && !matches(&self.topics) {
            return false;
        }
        !matches(&self.exclude)
    }
}

/// Everything pass-1 planning needs from the validated config.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanOptions {
    pub topics: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_policy: SchemaPolicy,
    pub allow_gaps: bool,
    pub start: Duration,
    pub duration: Option<Duration>,
    pub reader_limits: ReaderLimits,
    pub planning_budget_bytes: u64,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            exclude: Vec::new(),
            schema_policy: SchemaPolicy::Strict,
            allow_gaps: false,
            start: Duration::ZERO,
            duration: None,
            reader_limits: ReaderLimits::default(),
            planning_budget_bytes: DEFAULT_PLANNING_BUDGET_BYTES,
        }
    }
}

impl PlanOptions {
    pub fn from_config(config: &PlayerConfig, limits: &ValidatedLimits) -> Self {
        Self {
            topics: config.topics.clone(),
            exclude: config.exclude.clone(),
            schema_policy: config.schema_policy,
            allow_gaps: config.allow_gaps,
            start: limits.start,
            duration: limits.duration,
            reader_limits: ReaderLimits {
                max_chunk_bytes: limits.max_chunk_bytes,
                max_message_bytes: limits.max_message_bytes,
            },
            planning_budget_bytes: DEFAULT_PLANNING_BUDGET_BYTES,
        }
    }
}

/// `aorta.recorder.start` identity of one input (MCAP Player Proposal §2,
/// §6.1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentIdentity {
    pub group: Option<String>,
    pub started_at_ns: Option<u64>,
    pub recording_id: Option<String>,
    pub segment_index: Option<u64>,
    pub segment_continues: Option<bool>,
}

/// One planned segment, in play order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPlan {
    pub source: PathBuf,
    /// Index into the caller's input slice — pass 2 re-walks the same snapshot.
    pub input_index: usize,
    pub identity: SegmentIdentity,
    /// End offset of the MCAP Player Proposal §6.1 verified safe prefix; pass 2
    /// replays within it.
    pub safe_prefix_end: u64,
    pub truncated: bool,
    pub messages: u64,
    /// Effective time of this segment's first / last message.
    pub t_first: Option<u64>,
    pub t_last: Option<u64>,
    /// Raw recorded range, retained separately for cross-recording overlap
    /// validation; effective times are clamped for scheduling.
    pub raw_t_first: Option<u64>,
    pub raw_t_last: Option<u64>,
}

/// What a planned topic replays as (MCAP Player Proposal §6.3 topic kinds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicKind {
    /// Schema-less replay through one `aorta_raw_bytes` publisher.
    Raw,
    /// One typed publisher carrying this bfbs (`schema_attach_policy: Never`).
    Typed {
        schema_hash: String,
        schema_name: String,
        bfbs: Vec<u8>,
    },
}

/// A topic with ≥1 finally publishable message (MCAP Player Proposal §6.1
/// publish plan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTopic {
    /// The recorded full key, e.g. `aorta/robot/pub/sensor/imu`.
    pub key: String,
    /// The group-relative suffix, e.g. `pub/sensor/imu`.
    pub key_suffix: String,
    /// What `create_publisher_with_options` takes: the suffix without `pub/`.
    pub topic_suffix: String,
    pub kind: TopicKind,
    /// Finally publishable messages (window ∩ filters).
    pub messages: u64,
}

/// Effective-timeline scalars of the whole ordered segment set (MCAP Player
/// Proposal §6.5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlannedTimeline {
    /// Effective time of the first message of the set, all channels.
    pub t_first: Option<u64>,
    /// Effective time of the last message of the set.
    pub t_last: Option<u64>,
    /// `[t_first + S, t_first + S + D]`; `None` when the set has no messages.
    pub window: Option<Window>,
    /// Effective time of the first *selected* message — the pacing anchor.
    pub t0: Option<u64>,
    pub total_messages: u64,
    pub selected_messages: u64,
    pub clamped_messages: u64,
}

/// The pass-1 product: everything pass 2 and the publisher map consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPlan {
    /// Recorded group (`aorta.group`, else the first channel key's group).
    pub recorded_group: Option<String>,
    pub segments: Vec<SegmentPlan>,
    /// Publish plan: only topics with ≥1 finally publishable message.
    pub topics: Vec<PlannedTopic>,
    pub timeline: PlannedTimeline,
    pub skipped_channels: SkippedChannels,
    pub truncation: TruncationReport,
    /// Retained planning state, checked against the MCAP Player Proposal §6.1
    /// budget.
    pub retained_bytes: u64,
}

impl PlaybackPlan {
    /// Empty final selection → zero-count `Completed`, no publishers, no
    /// preflight (MCAP Player Proposal §6.1/§6.5). `--loop` + empty exits after
    /// one pass.
    pub fn is_empty_selection(&self) -> bool {
        self.topics.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Wire constants (MCAP Player Proposal §2 — exactly what
// `tools/aorta-record/src/writer.rs` emits)
// ---------------------------------------------------------------------------

const PROFILE_AORTA: &str = "aorta";
const ENCODING_FLATBUFFER: &str = "flatbuffer";
const ENCODING_RAW: &str = "aorta_raw_bytes";
/// The literal the recorder stores as a schema-less channel's `schema_hash`.
const RAW_SCHEMA_MARKER: &str = "raw";
const KEY_ROOT: &str = "aorta/";
const PUB_PREFIX: &str = "pub/";
const CTX_PREFIX: &str = "ctx/";
const SYS_PREFIX: &str = "sys/";
const START_METADATA: &str = "aorta.recorder.start";
const END_METADATA: &str = "aorta.recorder.end";
/// Flat per-entry overhead charged to the MCAP Player Proposal §6.1 planning
/// budget on top of the bytes actually stored — maps, `String` headers and
/// `Vec` headers are real memory even when the payload is short.
const ENTRY_OVERHEAD_BYTES: u64 = 128;

fn path_bytes(path: &Path) -> u64 {
    size_of::<PathBuf>() as u64 + path.to_string_lossy().len() as u64
}

fn identity_bytes(identity: &SegmentIdentity) -> u64 {
    size_of::<SegmentIdentity>() as u64
        + identity
            .group
            .as_ref()
            .map_or(0, |value| value.len() as u64)
        + identity
            .recording_id
            .as_ref()
            .map_or(0, |value| value.len() as u64)
}

fn topic_kind_bytes(kind: &TopicKind) -> u64 {
    size_of::<TopicKind>() as u64
        + match kind {
            TopicKind::Raw => 0,
            TopicKind::Typed {
                schema_hash,
                schema_name,
                bfbs,
            } => schema_hash.len() as u64 + schema_name.len() as u64 + bfbs.len() as u64,
        }
}

// ---------------------------------------------------------------------------
// Segment ordering & set validation (MCAP Player Proposal §6.1)
// ---------------------------------------------------------------------------

/// Order the inputs into one coherent segment set (MCAP Player Proposal §6.1).
///
/// The identities come from a cheap pre-scan that stops at the
/// `aorta.recorder.start` record, so ordering is settled *before* the timeline
/// walk — the effective-timeline accumulator has to see the segments in play
/// order, not in argv order.
pub fn order_segments(
    sources: &[PathBuf],
    identities: &[SegmentIdentity],
    allow_gaps: bool,
) -> Result<Vec<usize>> {
    if sources.len() != identities.len() {
        bail!(
            "internal error: {} inputs but {} identities",
            sources.len(),
            identities.len()
        );
    }
    if sources.is_empty() {
        bail!("no inputs to plan");
    }

    // One recorded group per run.
    let mut groups: BTreeMap<&str, &Path> = BTreeMap::new();
    for (index, identity) in identities.iter().enumerate() {
        if let Some(group) = identity.group.as_deref() {
            groups.entry(group).or_insert(sources[index].as_path());
        }
    }
    if groups.len() > 1 {
        bail!(
            "inputs mix recorded groups ({}) — one run replays one group (MCAP Player Proposal §6.1)",
            describe(&groups)
        );
    }

    // One recording identity per run, plus at most one id-less input.
    let mut sets: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    let mut idless: Vec<usize> = Vec::new();
    for (index, identity) in identities.iter().enumerate() {
        match identity.recording_id.as_deref() {
            Some(id) => sets.entry(id).or_default().push(index),
            None => idless.push(index),
        }
    }
    if sets.len() > 1 {
        let named: BTreeMap<&str, &Path> = sets
            .iter()
            .map(|(id, members)| (*id, sources[members[0]].as_path()))
            .collect();
        bail!(
            "inputs mix recording ids ({}) — cross-recording interleave is not supported \
             (MCAP Player Proposal §6.1, §10)",
            describe(&named)
        );
    }
    if idless.len() > 1 {
        bail!(
            "{} inputs carry no aorta.recording_id ({}) — at most one id-less file per \
             invocation (MCAP Player Proposal §6.1)",
            idless.len(),
            idless
                .iter()
                .map(|i| format!("'{}'", sources[*i].display()))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let mut ordered: Vec<usize> = Vec::with_capacity(sources.len());
    if let Some((recording_id, members)) = sets.into_iter().next() {
        let mut members = members;
        members.sort_by_key(|index| (identities[*index].segment_index.unwrap_or(0), *index));

        let mut seen: BTreeSet<u64> = BTreeSet::new();
        let mut duplicates: Vec<u64> = Vec::new();
        for index in &members {
            let segment_index = identities[*index].segment_index.ok_or_else(|| {
                anyhow!(
                    "'{}' carries aorta.recording_id '{}' without aorta.segment.index \
                     (MCAP Player Proposal §6.1)",
                    sources[*index].display(),
                    recording_id
                )
            })?;
            if !seen.insert(segment_index) {
                duplicates.push(segment_index);
            }
        }
        let lowest = *seen.iter().next().expect("non-empty set");
        let highest = *seen.iter().next_back().expect("non-empty set");
        let missing: Vec<u64> = (lowest..=highest).filter(|i| !seen.contains(i)).collect();

        if !duplicates.is_empty() || !missing.is_empty() {
            let detail = format!(
                "recording '{}' has duplicate segment indices [{}] and missing indices [{}] \
                 (present: {}..={})",
                recording_id,
                join_numbers(&duplicates),
                join_numbers(&missing),
                lowest,
                highest
            );
            if allow_gaps {
                tracing::warn!("aorta-play: {} — playing anyway (--allow-gaps)", detail);
            } else {
                bail!("{detail} — pass --allow-gaps to play it anyway (MCAP Player Proposal §6.1)");
            }
        }
        ordered.extend(members);
    }

    // The id-less input (if any) is placed by its recorded start time.
    if let Some(index) = idless.first().copied() {
        let started = identities[index].started_at_ns.unwrap_or(0);
        let position = ordered
            .iter()
            .position(|other| identities[*other].started_at_ns.unwrap_or(0) > started)
            .unwrap_or(ordered.len());
        ordered.insert(position, index);
    }

    Ok(ordered)
}

fn describe(entries: &BTreeMap<&str, &Path>) -> String {
    entries
        .iter()
        .map(|(value, source)| format!("'{}' in '{}'", value, source.display()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_numbers(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Pass-1 entry point
// ---------------------------------------------------------------------------

/// Run pass 1 over every snapshot and produce the playback plan.
pub fn build_plan(
    inputs: &[PlanInput<'_>],
    options: &PlanOptions,
    cancel: &AtomicBool,
) -> Result<PlaybackPlan> {
    #[cfg(all(test, feature = "fault-injection"))]
    {
        let stall = TEST_PLAN_STALL.lock().expect("plan stall").take();
        if let Some(stall) = stall {
            stall.wait();
        }
    }
    let sources: Vec<PathBuf> = inputs.iter().map(|input| input.source.clone()).collect();

    // Pre-scan: the recorder writes `aorta.recorder.start` as the first record
    // after the header, so this stops within the first few hundred bytes.
    let mut identities = Vec::with_capacity(inputs.len());
    for input in inputs {
        check_cancelled(cancel)?;
        identities.push(scan_identity(input, options.reader_limits, cancel)?);
    }

    let order = order_segments(&sources, &identities, options.allow_gaps)?;

    let mut planner = Planner::new(options, cancel);
    planner.charge(
        sources.iter().map(|source| path_bytes(source)).sum::<u64>()
            + identities.iter().map(identity_bytes).sum::<u64>()
            + (order.len() * size_of::<usize>()) as u64,
    )?;
    for input_index in order {
        check_cancelled(cancel)?;
        let input = &inputs[input_index];
        let mut walker = McapWalker::open(input.reader, options.reader_limits)
            .map_err(|err| plan_error(&input.source, err))?;
        planner.begin_segment(
            input.source.clone(),
            input_index,
            identities[input_index].clone(),
        )?;
        let report = walker
            .walk_with_stop(&mut planner, cancel)
            .map_err(|err| plan_error(&input.source, err))?;
        if planner.cancelled || report.stopped_by_sink || cancel.load(Ordering::SeqCst) {
            return Err(anyhow!(Cancelled));
        }
        planner.finish_segment(report)?;
    }

    planner.finish()
}

fn check_cancelled(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::SeqCst) {
        return Err(anyhow!(Cancelled));
    }
    Ok(())
}

/// A sink error already names its channel/topic; a reader error names the file.
fn plan_error(source: &Path, err: ReadError) -> anyhow::Error {
    match err {
        ReadError::Sink(inner) => inner,
        other => anyhow!("'{}': {}", source.display(), other),
    }
}

// ---------------------------------------------------------------------------
// Identity pre-scan
// ---------------------------------------------------------------------------

fn scan_identity(
    input: &PlanInput<'_>,
    limits: ReaderLimits,
    cancel: &AtomicBool,
) -> Result<SegmentIdentity> {
    let mut walker =
        McapWalker::open(input.reader, limits).map_err(|err| plan_error(&input.source, err))?;
    let mut sink = IdentitySink {
        source: input.source.clone(),
        identity: SegmentIdentity::default(),
    };
    walker
        .walk_with_stop(&mut sink, cancel)
        .map_err(|err| plan_error(&input.source, err))?;
    check_cancelled(cancel)?;
    Ok(sink.identity)
}

struct IdentitySink {
    source: PathBuf,
    identity: SegmentIdentity,
}

impl RecordSink for IdentitySink {
    fn on_record(&mut self, record: &RecordEvent<'_>) -> Result<Flow> {
        match record {
            RecordEvent::Metadata { name, entries } if name == START_METADATA => {
                self.identity = parse_identity(&self.source, entries)?;
                Ok(Flow::Stop)
            }
            // The start metadata precedes every channel and message the
            // recorder writes, so anything else bounds the pre-scan.
            RecordEvent::Channel { .. } | RecordEvent::Message { .. } => Ok(Flow::Stop),
            _ => Ok(Flow::Continue),
        }
    }
}

fn parse_identity(source: &Path, entries: &BTreeMap<String, String>) -> Result<SegmentIdentity> {
    Ok(SegmentIdentity {
        group: entries.get("aorta.group").cloned(),
        started_at_ns: parse_u64(source, entries, "aorta.started_at_ns")?,
        recording_id: entries.get("aorta.recording_id").cloned(),
        segment_index: parse_u64(source, entries, "aorta.segment.index")?,
        segment_continues: entries
            .get("aorta.segment.continues")
            .map(|value| value == "true"),
    })
}

fn parse_u64(source: &Path, entries: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>> {
    entries
        .get(key)
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                anyhow!(
                    "'{}': {} metadata key '{}' is not a u64 ('{}')",
                    source.display(),
                    START_METADATA,
                    key,
                    value
                )
            })
        })
        .transpose()
}

// ---------------------------------------------------------------------------
// Retained pass-1 state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ScannedSchema {
    name: String,
    hash: String,
    bfbs: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ScannedChannel {
    id: u16,
    topic: String,
    key_suffix: String,
    topic_suffix: String,
    message_encoding: String,
    schema_hash: String,
    typed: bool,
    /// `pub/**` — the only primitive v1 replays (MCAP Player Proposal §6.10).
    replayable: bool,
    /// `replayable` *and* accepted by `--topics` / `--exclude`.
    selected: bool,
    messages: u64,
    publishable: u64,
    last_log_time: Option<u64>,
    last_seq: u64,
}

#[derive(Debug, Clone)]
struct TopicChannel {
    source: PathBuf,
    id: u16,
    typed: bool,
    message_encoding: String,
    schema_hash: String,
    last_log_time: Option<u64>,
    last_seq: u64,
}

#[derive(Debug, Clone)]
struct TopicAggregate {
    key: String,
    key_suffix: String,
    topic_suffix: String,
    publishable: u64,
    channels: Vec<TopicChannel>,
}

#[derive(Debug)]
struct SegmentScan {
    source: PathBuf,
    input_index: usize,
    identity: SegmentIdentity,
    schemas: BTreeMap<u16, ScannedSchema>,
    channels: BTreeMap<u16, ScannedChannel>,
    group: Option<String>,
    ended: bool,
    messages: u64,
    t_first: Option<u64>,
    t_last: Option<u64>,
    raw_t_first: Option<u64>,
    raw_t_last: Option<u64>,
}

// ---------------------------------------------------------------------------
// The pass-1 sink
// ---------------------------------------------------------------------------

struct Planner<'a> {
    options: &'a PlanOptions,
    filter: TopicFilter,
    cancel: &'a AtomicBool,
    cancelled: bool,

    // Cross-segment state — the effective timeline spans the ordered set.
    timeline: TimelineAccumulator,
    window: Option<Window>,
    t0: Option<u64>,
    selected_messages: u64,
    message_seq: u64,
    topics: BTreeMap<String, TopicAggregate>,
    /// Deduplicated bfbs catalogue, keyed by recomputed hash.
    schemas: BTreeMap<String, ScannedSchema>,
    skipped: SkippedChannels,
    recorded_group: Option<String>,
    retained_bytes: u64,
    segments: Vec<SegmentPlan>,
    truncation: TruncationReport,

    current: Option<SegmentScan>,
}

impl<'a> Planner<'a> {
    fn new(options: &'a PlanOptions, cancel: &'a AtomicBool) -> Self {
        Self {
            options,
            filter: TopicFilter::new(options.topics.clone(), options.exclude.clone()),
            cancel,
            cancelled: false,
            timeline: TimelineAccumulator::new(),
            window: None,
            t0: None,
            selected_messages: 0,
            message_seq: 0,
            topics: BTreeMap::new(),
            schemas: BTreeMap::new(),
            skipped: SkippedChannels::default(),
            recorded_group: None,
            retained_bytes: 0,
            segments: Vec::new(),
            // Exact until a torn *chunk* makes it unknowable (MCAP Player
            // Proposal §6.1).
            truncation: TruncationReport {
                discarded_tail_messages: Some(0),
                ..TruncationReport::default()
            },
            current: None,
        }
    }

    fn begin_segment(
        &mut self,
        source: PathBuf,
        input_index: usize,
        identity: SegmentIdentity,
    ) -> Result<()> {
        self.charge(
            size_of::<SegmentScan>() as u64
                + path_bytes(&source)
                + identity_bytes(&identity)
                + identity
                    .group
                    .as_ref()
                    .map_or(0, |group| group.len() as u64),
        )?;
        let group = identity.group.clone();
        self.current = Some(SegmentScan {
            source,
            input_index,
            identity,
            schemas: BTreeMap::new(),
            channels: BTreeMap::new(),
            group,
            ended: false,
            messages: 0,
            t_first: None,
            t_last: None,
            raw_t_first: None,
            raw_t_last: None,
        });
        Ok(())
    }

    fn segment(&mut self) -> &mut SegmentScan {
        self.current
            .as_mut()
            .expect("a record can only arrive inside a segment walk")
    }

    fn source(&self) -> PathBuf {
        self.current
            .as_ref()
            .map(|segment| segment.source.clone())
            .unwrap_or_default()
    }

    /// Charge retained planning state against the MCAP Player Proposal §6.1
    /// byte budget.
    fn charge(&mut self, bytes: u64) -> Result<()> {
        self.retained_bytes = self.retained_bytes.saturating_add(bytes);
        if self.retained_bytes > self.options.planning_budget_bytes {
            bail!(
                "'{}': retained pass-1 planning state grew past the {}-byte budget \
                 ({} bytes retained) — refusing to let channel/schema counts drive memory \
                 (MCAP Player Proposal §6.1)",
                self.source().display(),
                self.options.planning_budget_bytes,
                self.retained_bytes
            );
        }
        Ok(())
    }

    // -- record handlers ---------------------------------------------------

    fn on_header(&mut self, profile: &str) -> Result<()> {
        if profile != PROFILE_AORTA {
            bail!(
                "'{}': MCAP profile is '{}', expected '{}' — this is not an Aorta recording \
                 (MCAP Player Proposal §6.1)",
                self.source().display(),
                profile,
                PROFILE_AORTA
            );
        }
        Ok(())
    }

    fn on_schema(&mut self, id: u16, name: &str, encoding: &str, data: &[u8]) -> Result<()> {
        if encoding != ENCODING_FLATBUFFER {
            bail!(
                "'{}': schema {} ('{}') declares encoding '{}', expected '{}' (MCAP Player Proposal §6.1)",
                self.source().display(),
                id,
                name,
                encoding,
                ENCODING_FLATBUFFER
            );
        }
        // MCAP Player Proposal §6.2 — the hash is recomputed here, never
        // trusted from the file.
        let hash = SchemaHash::from_bfbs_bytes(data).to_hex();
        self.charge(
            ENTRY_OVERHEAD_BYTES
                + size_of::<ScannedSchema>() as u64
                + name.len() as u64
                + hash.len() as u64
                + data.len() as u64,
        )?;
        let schema = ScannedSchema {
            name: name.to_string(),
            hash,
            bfbs: data.to_vec(),
        };
        self.segment().schemas.insert(id, schema);
        Ok(())
    }

    fn on_channel(
        &mut self,
        id: u16,
        schema_id: u16,
        topic: &str,
        message_encoding: &str,
        metadata: &BTreeMap<String, String>,
    ) -> Result<()> {
        let source = self.source();

        // MCAP Player Proposal §6.1 channel consistency: topic == aorta_key,
        // key_suffix is the group-relative suffix of topic.
        let aorta_key = metadata.get("aorta_key").ok_or_else(|| {
            anyhow!(
                "'{}': channel {} ('{}') has no aorta_key metadata (MCAP Player Proposal §6.1)",
                source.display(),
                id,
                topic
            )
        })?;
        if aorta_key != topic {
            bail!(
                "'{}': channel {} topic '{}' disagrees with its aorta_key metadata '{}' \
                 (MCAP Player Proposal §6.1)",
                source.display(),
                id,
                topic,
                aorta_key
            );
        }
        let key_suffix = metadata.get("key_suffix").ok_or_else(|| {
            anyhow!(
                "'{}': channel {} ('{}') has no key_suffix metadata (MCAP Player Proposal §6.1)",
                source.display(),
                id,
                topic
            )
        })?;
        let (group, suffix) = split_key(topic).ok_or_else(|| {
            anyhow!(
                "'{}': channel {} topic '{}' is not an aorta/{{group}}/{{suffix}} key \
                 (MCAP Player Proposal §6.1)",
                source.display(),
                id,
                topic
            )
        })?;
        if suffix != key_suffix {
            bail!(
                "'{}': channel {} key_suffix metadata '{}' is not the group-relative suffix \
                 of topic '{}' (expected '{}', MCAP Player Proposal §6.1)",
                source.display(),
                id,
                key_suffix,
                topic,
                suffix
            );
        }

        // One group per file; the metadata value wins when both are present.
        {
            let segment = self.segment();
            match segment.group.as_deref() {
                Some(known) if known != group => bail!(
                    "'{}': channel {} key '{}' is in group '{}' but the file's group is '{}' \
                     (MCAP Player Proposal §6.1)",
                    source.display(),
                    id,
                    topic,
                    group,
                    known
                ),
                Some(_) => {}
                None => segment.group = Some(group.to_string()),
            }
        }
        if self.recorded_group.is_none() {
            self.charge(ENTRY_OVERHEAD_BYTES + group.len() as u64)?;
            self.recorded_group = Some(group.to_string());
        }

        // MCAP Player Proposal §6.1 format validation + §6.2 schema integrity.
        let recorded_hash = metadata.get("schema_hash").map(String::as_str);
        let (typed, schema_hash) = match message_encoding {
            ENCODING_FLATBUFFER => {
                if schema_id == 0 {
                    bail!(
                        "'{}': channel {} ('{}') declares '{}' but carries no schema \
                         (schema_id 0, MCAP Player Proposal §6.1)",
                        source.display(),
                        id,
                        topic,
                        ENCODING_FLATBUFFER
                    );
                }
                let schema = self
                    .segment()
                    .schemas
                    .get(&schema_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "'{}': channel {} ('{}') references unknown schema {} (MCAP Player Proposal §6.1)",
                            source.display(),
                            id,
                            topic,
                            schema_id
                        )
                    })?;
                let recorded = recorded_hash.ok_or_else(|| {
                    anyhow!(
                        "'{}': typed channel {} ('{}') has no schema_hash metadata (MCAP Player Proposal §6.2)",
                        source.display(),
                        id,
                        topic
                    )
                })?;
                if !is_canonical_schema_hash(recorded) {
                    bail!(
                        "'{}': channel {} ('{}') records schema_hash '{}', which is not 64 \
                         lowercase hex characters (MCAP Player Proposal §6.2)",
                        source.display(),
                        id,
                        topic,
                        recorded
                    );
                }
                if recorded != schema.hash {
                    bail!(
                        "'{}': channel {} ('{}') records schema_hash '{}' but its bfbs hashes \
                         to '{}' (MCAP Player Proposal §6.2)",
                        source.display(),
                        id,
                        topic,
                        recorded,
                        schema.hash
                    );
                }
                let hash = schema.hash.clone();
                if !self.schemas.contains_key(&hash) {
                    self.charge(
                        ENTRY_OVERHEAD_BYTES
                            + schema.name.len() as u64
                            + hash.len() as u64
                            + hash.len() as u64
                            + schema.bfbs.len() as u64,
                    )?;
                    self.schemas.insert(hash.clone(), schema);
                }
                (true, hash)
            }
            ENCODING_RAW => {
                if schema_id != 0 {
                    bail!(
                        "'{}': channel {} ('{}') declares '{}' but references schema {} \
                         (raw channels carry schema_id 0, MCAP Player Proposal §6.1)",
                        source.display(),
                        id,
                        topic,
                        ENCODING_RAW,
                        schema_id
                    );
                }
                (false, RAW_SCHEMA_MARKER.to_string())
            }
            other => bail!(
                "'{}': channel {} ('{}') declares message_encoding '{}', expected '{}' or '{}' \
                 (MCAP Player Proposal §6.1)",
                source.display(),
                id,
                topic,
                other,
                ENCODING_FLATBUFFER,
                ENCODING_RAW
            ),
        };

        // MCAP Player Proposal §6.10 — v1 replays `pub/**` only; everything
        // else is counted.
        let replayable = key_suffix.starts_with(PUB_PREFIX);
        if !replayable {
            self.skipped.total += 1;
            if key_suffix.starts_with(CTX_PREFIX) {
                self.skipped.ctx += 1;
            } else if key_suffix.starts_with(SYS_PREFIX) {
                self.skipped.sys += 1;
            }
        }
        let topic_suffix = key_suffix
            .strip_prefix(PUB_PREFIX)
            .unwrap_or(key_suffix)
            .to_string();
        // MCAP Player Proposal §6.5 step 3 — filters decide publishing only,
        // and they are matched against the *recorded* keys, before any
        // `--group` remap.
        let selected = replayable && self.passes_filters(topic, key_suffix, &topic_suffix);

        self.charge(
            ENTRY_OVERHEAD_BYTES
                + topic.len() as u64
                + key_suffix.len() as u64
                + topic_suffix.len() as u64
                + message_encoding.len() as u64
                + schema_hash.len() as u64,
        )?;
        self.charge(size_of::<ScannedChannel>() as u64)?;
        self.segment().channels.insert(
            id,
            ScannedChannel {
                id,
                topic: topic.to_string(),
                key_suffix: key_suffix.to_string(),
                topic_suffix,
                message_encoding: message_encoding.to_string(),
                schema_hash,
                typed,
                replayable,
                selected,
                messages: 0,
                publishable: 0,
                last_log_time: None,
                last_seq: 0,
            },
        );
        Ok(())
    }

    fn passes_filters(&self, key: &str, key_suffix: &str, topic_suffix: &str) -> bool {
        self.filter.accepts(key, key_suffix, topic_suffix)
    }

    fn on_message(&mut self, channel_id: u16, log_time: u64) -> Result<()> {
        // Step 1 (MCAP Player Proposal §6.5): every channel, before filtering,
        // feeds the timeline.
        let segment = self.segment();
        segment.raw_t_first = Some(
            segment
                .raw_t_first
                .map_or(log_time, |value| value.min(log_time)),
        );
        segment.raw_t_last = Some(
            segment
                .raw_t_last
                .map_or(log_time, |value| value.max(log_time)),
        );
        let effective = self.timeline.push(log_time);
        // Step 2: the window is anchored on the first effective time.
        if self.window.is_none() {
            self.window = Some(Window::resolve(
                effective,
                self.options.start,
                self.options.duration,
            )?);
        }
        let in_window = self.window.expect("resolved above").contains(effective);
        self.message_seq += 1;
        let seq = self.message_seq;

        let source = self.source();
        let segment = self.segment();
        segment.messages += 1;
        if segment.t_first.is_none() {
            segment.t_first = Some(effective);
        }
        segment.t_last = Some(effective);

        let channel = segment.channels.get_mut(&channel_id).ok_or_else(|| {
            anyhow!(
                "'{}': message references unknown channel {} (MCAP Player Proposal §6.1)",
                source.display(),
                channel_id
            )
        })?;
        channel.messages += 1;
        channel.last_log_time = Some(log_time);
        channel.last_seq = seq;
        if channel.selected && in_window {
            channel.publishable += 1;
            self.selected_messages += 1;
            if self.t0.is_none() {
                self.t0 = Some(effective);
            }
        }
        Ok(())
    }

    fn on_metadata(&mut self, name: &str, entries: &BTreeMap<String, String>) -> Result<()> {
        match name {
            START_METADATA => {
                let source = self.source();
                let identity = parse_identity(&source, entries)?;
                self.charge(
                    ENTRY_OVERHEAD_BYTES
                        + identity.group.as_ref().map_or(0, |g| g.len() as u64)
                        + identity
                            .recording_id
                            .as_ref()
                            .map_or(0, |id| id.len() as u64),
                )?;
                if let Some(group) = identity.group.clone() {
                    if self.recorded_group.is_none() {
                        self.charge(ENTRY_OVERHEAD_BYTES + group.len() as u64)?;
                        self.recorded_group = Some(group.clone());
                    }
                    self.segment().group = Some(group);
                }
                self.segment().identity = identity;
            }
            END_METADATA => self.segment().ended = true,
            _ => {}
        }
        Ok(())
    }

    // -- segment / run completion -----------------------------------------

    fn finish_segment(&mut self, report: WalkReport) -> Result<()> {
        let segment = self.current.take().expect("a segment walk was started");

        let aggregate_charge: u64 = segment
            .channels
            .values()
            .filter(|channel| channel.replayable && !self.topics.contains_key(&channel.topic))
            .map(|channel| {
                ENTRY_OVERHEAD_BYTES
                    + size_of::<TopicAggregate>() as u64
                    + channel.topic.len() as u64
                    + channel.topic.len() as u64
                    + channel.key_suffix.len() as u64
                    + channel.topic_suffix.len() as u64
            })
            .sum();
        self.charge(aggregate_charge)?;

        if report.truncated {
            self.charge(ENTRY_OVERHEAD_BYTES + path_bytes(&segment.source))?;
            self.truncation
                .truncated_inputs
                .push(segment.source.clone());
        }
        self.truncation.discarded_tail_chunks = self
            .truncation
            .discarded_tail_chunks
            .saturating_add(report.discarded_tail_chunks);
        self.truncation.discarded_tail_messages = match (
            self.truncation.discarded_tail_messages,
            report.discarded_tail_messages,
        ) {
            (Some(known), Some(more)) => Some(known.saturating_add(more)),
            // One `unknown` segment makes the total unknown (MCAP Player
            // Proposal §6.1).
            _ => None,
        };

        // Fold the segment's channels into the cross-segment topic map.
        for channel in segment.channels.values() {
            if !channel.replayable {
                continue;
            }
            self.charge(
                ENTRY_OVERHEAD_BYTES
                    + size_of::<TopicChannel>() as u64
                    + path_bytes(&segment.source)
                    + channel.message_encoding.len() as u64
                    + channel.schema_hash.len() as u64,
            )?;
            let aggregate =
                self.topics
                    .entry(channel.topic.clone())
                    .or_insert_with(|| TopicAggregate {
                        key: channel.topic.clone(),
                        key_suffix: channel.key_suffix.clone(),
                        topic_suffix: channel.topic_suffix.clone(),
                        publishable: 0,
                        channels: Vec::new(),
                    });
            aggregate.publishable += channel.publishable;
            aggregate.channels.push(TopicChannel {
                source: segment.source.clone(),
                id: channel.id,
                typed: channel.typed,
                message_encoding: channel.message_encoding.clone(),
                schema_hash: channel.schema_hash.clone(),
                last_log_time: channel.last_log_time,
                last_seq: channel.last_seq,
            });
        }

        self.segments.push(SegmentPlan {
            source: segment.source.clone(),
            input_index: segment.input_index,
            identity: segment.identity.clone(),
            safe_prefix_end: report.safe_prefix_end,
            truncated: report.truncated,
            messages: segment.messages,
            t_first: segment.t_first,
            t_last: segment.t_last,
            raw_t_first: segment.raw_t_first,
            raw_t_last: segment.raw_t_last,
        });
        self.charge(
            size_of::<SegmentPlan>() as u64
                + path_bytes(&segment.source)
                + identity_bytes(&segment.identity),
        )?;
        Ok(())
    }

    fn finish(mut self) -> Result<PlaybackPlan> {
        // Only the final segment may be torn (MCAP Player Proposal §6.1).
        if let Some((_, interior)) = self.segments.split_last() {
            if let Some(segment) = interior.iter().find(|segment| segment.truncated) {
                bail!(
                    "'{}' is truncated but is not the final segment — only the last segment of \
                     a rotated set may be torn (MCAP Player Proposal §6.1)",
                    segment.source.display()
                );
            }
        }
        // Unrelated inputs (the id-less file next to a rotated set) must not
        // overlap in recorded time (MCAP Player Proposal §6.1).
        for pair in self.segments.windows(2) {
            let (previous, next) = (&pair[0], &pair[1]);
            if previous.identity.recording_id == next.identity.recording_id {
                continue;
            }
            if let (Some(previous_end), Some(next_start)) = (previous.raw_t_last, next.raw_t_first)
            {
                if next_start < previous_end {
                    bail!(
                        "'{}' and '{}' belong to different recordings and their time ranges \
                         overlap (MCAP Player Proposal §6.1)",
                        previous.source.display(),
                        next.source.display()
                    );
                }
            }
        }

        // MCAP Player Proposal §6.1 publish plan: only topics with ≥1 finally
        // publishable message are classified, so an excluded or out-of-window
        // topic can neither create a publisher nor fail startup.
        let mut topics = Vec::new();
        let mut topic_charges = Vec::new();
        for aggregate in self.topics.values() {
            if aggregate.publishable == 0 {
                continue;
            }
            let kind = classify_topic(aggregate, self.options.schema_policy, &self.schemas)?;
            let charge = size_of::<PlannedTopic>() as u64
                + aggregate.key.len() as u64
                + aggregate.key_suffix.len() as u64
                + aggregate.topic_suffix.len() as u64
                + topic_kind_bytes(&kind);
            topics.push(PlannedTopic {
                key: aggregate.key.clone(),
                key_suffix: aggregate.key_suffix.clone(),
                topic_suffix: aggregate.topic_suffix.clone(),
                kind,
                messages: aggregate.publishable,
            });
            topic_charges.push(charge);
        }
        self.charge(
            (topic_charges.len() as u64) * (ENTRY_OVERHEAD_BYTES + size_of::<u64>() as u64),
        )?;
        for charge in topic_charges {
            self.charge(charge)?;
        }

        if self.skipped.total > 0 {
            tracing::info!(
                "aorta-play: skipped {} non-pub channels (ctx: {}, sys: {}) — v1 replays \
                 pub/** only",
                self.skipped.total,
                self.skipped.ctx,
                self.skipped.sys
            );
        }

        Ok(PlaybackPlan {
            recorded_group: self.recorded_group.take(),
            segments: std::mem::take(&mut self.segments),
            topics,
            timeline: PlannedTimeline {
                t_first: self.timeline.t_first(),
                t_last: self.timeline.t_last(),
                window: self.window,
                t0: self.t0,
                total_messages: self.timeline.messages(),
                selected_messages: self.selected_messages,
                clamped_messages: self.timeline.clamped(),
            },
            skipped_channels: self.skipped.clone(),
            truncation: std::mem::take(&mut self.truncation),
            retained_bytes: self.retained_bytes,
        })
    }
}

impl RecordSink for Planner<'_> {
    fn on_record(&mut self, record: &RecordEvent<'_>) -> Result<Flow> {
        // Cancellation lands mid-scan (MCAP Player Proposal §6.7): the walk is
        // the only thing between the token and a multi-gigabyte file.
        if self.cancel.load(Ordering::SeqCst) {
            self.cancelled = true;
            return Ok(Flow::Stop);
        }
        match record {
            RecordEvent::Header { profile, .. } => self.on_header(profile)?,
            RecordEvent::Schema {
                id,
                name,
                encoding,
                data,
            } => self.on_schema(*id, name, encoding, data)?,
            RecordEvent::Channel {
                id,
                schema_id,
                topic,
                message_encoding,
                metadata,
            } => self.on_channel(*id, *schema_id, topic, message_encoding, metadata)?,
            RecordEvent::Message {
                channel_id,
                log_time,
                ..
            } => self.on_message(*channel_id, *log_time)?,
            RecordEvent::Metadata { name, entries } => self.on_metadata(name, entries)?,
        }
        Ok(Flow::Continue)
    }
}

// ---------------------------------------------------------------------------
// Topic classification (MCAP Player Proposal §6.3, static half)
// ---------------------------------------------------------------------------

fn classify_topic(
    aggregate: &TopicAggregate,
    policy: SchemaPolicy,
    schemas: &BTreeMap<String, ScannedSchema>,
) -> Result<TopicKind> {
    let typed: Vec<&TopicChannel> = aggregate.channels.iter().filter(|c| c.typed).collect();
    let raw: Vec<&TopicChannel> = aggregate.channels.iter().filter(|c| !c.typed).collect();

    if !typed.is_empty() && !raw.is_empty() {
        bail!(
            "topic '{}' mixes raw and typed channels ({}) — one transport key owns one by-key \
             schema slot, so v1 fails closed (MCAP Player Proposal §6.3, §10)",
            aggregate.key,
            describe_channels(&aggregate.channels)
        );
    }
    if typed.is_empty() {
        return Ok(TopicKind::Raw);
    }

    let hashes: BTreeSet<&str> = typed.iter().map(|c| c.schema_hash.as_str()).collect();
    if hashes.len() > 1 && policy == SchemaPolicy::Strict {
        bail!(
            "topic '{}' carries {} typed schemas ({}) — pass --schema-policy latest to replay \
             all typed bytes through the newest channel's schema (MCAP Player Proposal §6.3)",
            aggregate.key,
            hashes.len(),
            describe_channels(&aggregate.channels)
        );
    }

    // `latest` (and the single-schema case): the channel whose last message is
    // newest, with the canonical record-stream position as the tie-break.
    let newest = typed
        .iter()
        .max_by_key(|channel| (channel.last_log_time.unwrap_or(0), channel.last_seq))
        .expect("non-empty");
    let schema = schemas.get(&newest.schema_hash).ok_or_else(|| {
        anyhow!(
            "topic '{}' references schema '{}', which no Schema record defined",
            aggregate.key,
            newest.schema_hash
        )
    })?;
    Ok(TopicKind::Typed {
        schema_hash: schema.hash.clone(),
        schema_name: schema.name.clone(),
        bfbs: schema.bfbs.clone(),
    })
}

fn describe_channels(channels: &[TopicChannel]) -> String {
    channels
        .iter()
        .map(|channel| {
            format!(
                "{}#{} message_encoding={} schema_hash={}",
                channel.source.display(),
                channel.id,
                channel.message_encoding,
                channel.schema_hash
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Key helpers (SDK Common Concepts §3 key format:
// `aorta/{group}/{primitive}/{category}/…`)
// ---------------------------------------------------------------------------

/// The group-relative suffix of a recorded key, falling back to the key itself
/// for anything that is not `aorta/{group}/{suffix}` (pass 1 has already
/// rejected those, so pass 2 only needs a total function).
pub(crate) fn key_suffix_of(topic: &str) -> &str {
    split_key(topic).map_or(topic, |(_, suffix)| suffix)
}

/// The `pub/`-relative topic suffix — i.e. what
/// `create_publisher_with_options` takes. `None` for non-`pub/**` keys, which
/// v1 does not replay (MCAP Player Proposal §6.10).
pub(crate) fn topic_suffix_of(key_suffix: &str) -> Option<&str> {
    key_suffix.strip_prefix(PUB_PREFIX)
}

/// Split `aorta/{group}/{suffix}` into its group and its group-relative suffix.
fn split_key(topic: &str) -> Option<(&str, &str)> {
    let rest = topic.strip_prefix(KEY_ROOT)?;
    let (group, suffix) = rest.split_once('/')?;
    if group.is_empty() || suffix.is_empty() {
        return None;
    }
    Some((group, suffix))
}

/// The canonical schema-hash form: SHA-256, 64 lowercase hex characters.
fn is_canonical_schema_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{self, FixtureBuilder, FixtureOptions, SchemaSpec};
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;

    const IMU: &str = "aorta/robot/pub/sensor/imu";
    const LIDAR: &str = "aorta/robot/pub/sensor/lidar";
    const BFBS_A: &[u8] = b"bfbs-imu-v1";
    const BFBS_B: &[u8] = b"bfbs-imu-v2";

    // -- harness -----------------------------------------------------------

    fn plan_files(files: &[(&str, Vec<u8>)], options: &PlanOptions) -> Result<PlaybackPlan> {
        let cancel = AtomicBool::new(false);
        plan_files_cancellable(files, options, &cancel)
    }

    fn plan_files_cancellable(
        files: &[(&str, Vec<u8>)],
        options: &PlanOptions,
        cancel: &AtomicBool,
    ) -> Result<PlaybackPlan> {
        let inputs: Vec<PlanInput<'_>> = files
            .iter()
            .map(|(name, bytes)| PlanInput {
                source: PathBuf::from(*name),
                reader: bytes,
            })
            .collect();
        build_plan(&inputs, options, cancel)
    }

    fn plan_one(bytes: Vec<u8>, options: &PlanOptions) -> Result<PlaybackPlan> {
        plan_files(&[("a.mcap", bytes)], options)
    }

    /// One typed topic, three in-order messages.
    fn simple() -> Vec<u8> {
        FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, 1_000, b"m0")
            .message(0, 2_000, b"m1")
            .message(0, 3_000, b"m2")
            .build()
    }

    fn segment(recording_id: &str, index: u64, base_ns: u64) -> Vec<u8> {
        FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", base_ns, Some((recording_id, index, true)))
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, base_ns, b"m0")
            .message(0, base_ns + 1_000, b"m1")
            .build()
    }

    // -- format validation (MCAP Player Proposal §6.1) ---------------------

    #[test]
    fn rejects_a_non_aorta_profile() {
        let mut bytes = simple();
        // The profile string lives in the Header record, right after the magic.
        let header = fixture::find_record(&bytes, mcap::records::op::HEADER);
        let at = header.body() + 4; // 4-byte length prefix, then "aorta"
        bytes[at..at + 5].copy_from_slice(b"ros2x"[..5].as_ref());
        fixture::fix_data_section_crc(&mut bytes);

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("profile"), "got: {err}");
    }

    #[test]
    fn rejects_an_unknown_message_encoding() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.message_encoding = "protobuf".to_string();
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("protobuf"), "got: {err}");
    }

    #[test]
    fn rejects_a_typed_channel_without_a_schema() {
        let mut spec = fixture::raw_channel_spec(IMU);
        spec.message_encoding = "flatbuffer".to_string();
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("schema"), "got: {err}");
    }

    #[test]
    fn rejects_a_raw_channel_carrying_a_schema() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.message_encoding = "aorta_raw_bytes".to_string();
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("aorta_raw_bytes"), "got: {err}");
    }

    #[test]
    fn rejects_a_schema_whose_encoding_is_not_flatbuffer() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.schema = Some(SchemaSpec {
            name: "aorta.sensor.Imu".to_string(),
            encoding: "jsonschema".to_string(),
            data: BFBS_A.to_vec(),
        });
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("jsonschema"), "got: {err}");
    }

    // -- channel consistency (MCAP Player Proposal §6.1) -------------------

    #[test]
    fn rejects_a_channel_whose_topic_and_aorta_key_disagree() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.metadata
            .insert("aorta_key".to_string(), LIDAR.to_string());
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("aorta_key") && text.contains(IMU),
            "got: {text}"
        );
    }

    #[test]
    fn rejects_a_channel_whose_key_suffix_is_not_the_group_relative_suffix() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.metadata
            .insert("key_suffix".to_string(), "pub/sensor/other".to_string());
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("key_suffix"), "got: {err}");
    }

    #[test]
    fn rejects_a_channel_without_the_recorder_metadata() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.metadata = BTreeMap::new();
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("aorta_key"), "got: {err}");
    }

    // -- schema integrity (MCAP Player Proposal §6.2) ----------------------

    #[test]
    fn rejects_a_channel_whose_schema_hash_does_not_match_the_bfbs() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.metadata
            .insert("schema_hash".to_string(), fixture::schema_hash_hex(BFBS_B));
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains(IMU) && text.contains(&fixture::schema_hash_hex(BFBS_A)),
            "the error must name the channel and both hashes, got: {text}"
        );
    }

    #[test]
    fn rejects_a_schema_hash_that_is_not_64_lowercase_hex() {
        let mut spec = fixture::typed_channel_spec(IMU, "aorta.sensor.Imu", BFBS_A);
        spec.metadata.insert(
            "schema_hash".to_string(),
            fixture::schema_hash_hex(BFBS_A).to_uppercase(),
        );
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .channel(spec)
            .message(0, 1_000, b"m0")
            .build();

        assert!(plan_one(bytes, &PlanOptions::default()).is_err());
    }

    // -- topic plan (MCAP Player Proposal §6.3) ----------------------------

    #[test]
    fn plans_a_typed_only_topic_with_its_bfbs() {
        let plan = plan_one(simple(), &PlanOptions::default()).expect("plan");
        assert_eq!(plan.topics.len(), 1);
        let planned = &plan.topics[0];
        assert_eq!(planned.key, IMU);
        assert_eq!(planned.key_suffix, "pub/sensor/imu");
        assert_eq!(planned.topic_suffix, "sensor/imu");
        assert_eq!(planned.messages, 3);
        assert_eq!(
            planned.kind,
            TopicKind::Typed {
                schema_hash: fixture::schema_hash_hex(BFBS_A),
                schema_name: "aorta.sensor.Imu".to_string(),
                bfbs: BFBS_A.to_vec(),
            }
        );
        assert_eq!(plan.recorded_group.as_deref(), Some("robot"));
    }

    #[test]
    fn plans_a_raw_only_topic_without_a_schema() {
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .raw_channel(LIDAR)
            .message(0, 1_000, b"blob")
            .build();
        let plan = plan_one(bytes, &PlanOptions::default()).expect("plan");
        assert_eq!(plan.topics.len(), 1);
        assert_eq!(plan.topics[0].kind, TopicKind::Raw);
        assert_eq!(plan.topics[0].topic_suffix, "sensor/lidar");
    }

    #[test]
    fn rejects_a_topic_mixing_raw_and_typed_channels() {
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .raw_channel(IMU)
            .message(0, 1_000, b"typed")
            .message(1, 2_000, b"raw")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains(IMU) && text.contains("aorta_raw_bytes") && text.contains("flatbuffer"),
            "the error must list the offending channels, got: {text}"
        );
    }

    #[test]
    fn strict_rejects_a_topic_carrying_two_typed_schemas() {
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_B)
            .message(0, 1_000, b"v1")
            .message(1, 2_000, b"v2")
            .build();

        let err = plan_one(bytes, &PlanOptions::default()).unwrap_err();
        let text = err.to_string();
        assert!(text.contains(IMU) && text.contains("latest"), "got: {text}");
    }

    #[test]
    fn latest_picks_the_schema_of_the_channel_with_the_newest_last_log_time() {
        for options in [FixtureOptions::default(), FixtureOptions::unchunked()] {
            // Channel 0 (BFBS_A) is declared first but its last message is
            // older, so `latest` must pick channel 1's schema.
            let bytes = FixtureBuilder::new(options.clone())
                .recorder_start("robot", 1_000, None)
                .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
                .typed_channel(IMU, "aorta.sensor.Imu", BFBS_B)
                .message(1, 1_000, b"v2")
                .message(0, 2_000, b"v1")
                .flush_after(1)
                .message(1, 3_000, b"v2")
                .build();

            let plan = plan_one(
                bytes,
                &PlanOptions {
                    schema_policy: SchemaPolicy::Latest,
                    ..PlanOptions::default()
                },
            )
            .expect("latest replays through the newest schema");
            assert_eq!(plan.topics.len(), 1, "{options:?}");
            assert_eq!(
                plan.topics[0].kind,
                TopicKind::Typed {
                    schema_hash: fixture::schema_hash_hex(BFBS_B),
                    schema_name: "aorta.sensor.Imu".to_string(),
                    bfbs: BFBS_B.to_vec(),
                },
                "{options:?}"
            );
            // All typed bytes replay through that one publisher.
            assert_eq!(plan.topics[0].messages, 3, "{options:?}");
        }
    }

    #[test]
    fn skips_and_counts_non_pub_channels() {
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .typed_channel(
                "aorta/robot/ctx/system/estop",
                "aorta.ctx.ContextValue",
                b"bfbs-ctx",
            )
            .typed_channel(
                "aorta/robot/sys/telemetry/vital/snapshot/n1",
                "aorta.telemetry.VitalSnapshot",
                b"bfbs-vital",
            )
            .raw_channel("aorta/robot/srv/demo/echo")
            .message(0, 1_000, b"m0")
            .message(1, 1_100, b"ctx")
            .message(2, 1_200, b"sys")
            .message(3, 1_300, b"srv")
            .build();

        let plan = plan_one(bytes, &PlanOptions::default()).expect("plan");
        assert_eq!(plan.topics.len(), 1, "only pub/** is replayed");
        assert_eq!(plan.topics[0].key, IMU);
        assert_eq!(plan.skipped_channels.total, 3);
        assert_eq!(plan.skipped_channels.ctx, 1);
        assert_eq!(plan.skipped_channels.sys, 1);
        // Skipped messages still feed the effective timeline (MCAP Player
        // Proposal §6.5).
        assert_eq!(plan.timeline.total_messages, 4);
        assert_eq!(plan.timeline.selected_messages, 1);
    }

    // -- effective timeline, windowing & filtering -------------------------
    // (MCAP Player Proposal §6.5)

    #[test]
    fn the_effective_timeline_clamps_across_a_chunk_boundary() {
        // The MCAP Player Proposal §6.5 required fixture: non-monotone log_time
        // across two chunks.
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, 100, b"m0")
            .message(0, 200, b"m1")
            .flush_after(1)
            .message(0, 150, b"m2")
            .message(0, 90, b"m3")
            .build();

        let plan = plan_one(bytes, &PlanOptions::default()).expect("plan");
        assert_eq!(plan.timeline.t_first, Some(100));
        assert_eq!(
            plan.timeline.t_last,
            Some(200),
            "clamped to the running max"
        );
        assert_eq!(plan.timeline.clamped_messages, 2);
        assert_eq!(plan.timeline.total_messages, 4);
        assert_eq!(plan.timeline.selected_messages, 4);
    }

    #[test]
    fn windowing_operates_on_effective_times_not_raw_log_times() {
        // Effective: 100, 100, 100, 300. A window starting 150 ns after t_first
        // therefore selects only the last message — the clamped ones are at 100.
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, 100, b"m0")
            .message(0, 90, b"m1")
            .message(0, 95, b"m2")
            .message(0, 300, b"m3")
            .build();

        let plan = plan_one(
            bytes,
            &PlanOptions {
                start: Duration::from_nanos(150),
                ..PlanOptions::default()
            },
        )
        .expect("plan");
        assert_eq!(
            plan.timeline.window,
            Some(Window {
                start: 250,
                end: None
            })
        );
        assert_eq!(plan.timeline.selected_messages, 1);
        assert_eq!(plan.timeline.t0, Some(300));
        assert_eq!(plan.timeline.clamped_messages, 2, "clamping is unaffected");
        assert_eq!(plan.topics[0].messages, 1);
    }

    #[test]
    fn a_duration_closes_the_window_inclusively() {
        let plan = plan_one(
            simple(),
            &PlanOptions {
                duration: Some(Duration::from_nanos(1_000)),
                ..PlanOptions::default()
            },
        )
        .expect("plan");
        assert_eq!(
            plan.timeline.window,
            Some(Window {
                start: 1_000,
                end: Some(2_000)
            })
        );
        assert_eq!(plan.timeline.selected_messages, 2);
    }

    #[test]
    fn a_topic_filter_never_perturbs_the_clamp_state() {
        let build = || {
            FixtureBuilder::new(FixtureOptions::default())
                .recorder_start("robot", 1_000, None)
                .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
                .typed_channel(LIDAR, "aorta.sensor.Lidar", b"bfbs-lidar")
                .message(1, 100, b"lidar-late")
                .message(0, 90, b"imu-clamped")
                .message(0, 300, b"imu")
                .build()
        };

        let unfiltered = plan_one(build(), &PlanOptions::default()).expect("plan");
        let filtered = plan_one(
            build(),
            &PlanOptions {
                topics: vec![IMU.to_string()],
                ..PlanOptions::default()
            },
        )
        .expect("plan");

        assert_eq!(unfiltered.timeline.clamped_messages, 1);
        assert_eq!(
            filtered.timeline.clamped_messages, unfiltered.timeline.clamped_messages,
            "filtering decides publishing only (MCAP Player Proposal §6.5 step 3)"
        );
        assert_eq!(filtered.timeline.t_first, unfiltered.timeline.t_first);
        assert_eq!(filtered.timeline.t_last, unfiltered.timeline.t_last);
        // The filtered-out lidar message still paces the remainder: the first
        // published message is the *clamped* imu one, at effective time 100.
        assert_eq!(filtered.timeline.t0, Some(100));
        assert_eq!(filtered.topics.len(), 1);
        assert_eq!(filtered.topics[0].key, IMU);
    }

    #[test]
    fn filters_match_the_full_key_the_suffix_or_the_topic_name() {
        for filter in [IMU, "pub/sensor/imu", "sensor/imu"] {
            let plan = plan_one(
                simple(),
                &PlanOptions {
                    topics: vec![filter.to_string()],
                    ..PlanOptions::default()
                },
            )
            .expect("plan");
            assert_eq!(plan.topics.len(), 1, "filter '{filter}'");
        }
        let plan = plan_one(
            simple(),
            &PlanOptions {
                topics: vec!["imu".to_string()],
                ..PlanOptions::default()
            },
        )
        .expect("plan");
        assert!(
            plan.is_empty_selection(),
            "matching is exact, not a substring"
        );
    }

    #[test]
    fn exclude_is_applied_after_topics() {
        let plan = plan_one(
            simple(),
            &PlanOptions {
                topics: vec![IMU.to_string()],
                exclude: vec!["sensor/imu".to_string()],
                ..PlanOptions::default()
            },
        )
        .expect("plan");
        assert!(plan.is_empty_selection());
        assert_eq!(
            plan.timeline.total_messages, 3,
            "excluded messages still feed the timeline"
        );
        assert_eq!(plan.timeline.t0, None);
    }

    #[test]
    fn the_publish_plan_holds_only_finally_publishable_topics() {
        // `lidar` is entirely outside the window, `imu` is not: only `imu` may
        // create a publisher, and `lidar` cannot fail startup.
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .typed_channel(LIDAR, "aorta.sensor.Lidar", b"bfbs-lidar")
            .message(1, 100, b"early")
            .message(0, 5_000, b"imu")
            .build();

        let plan = plan_one(
            bytes,
            &PlanOptions {
                start: Duration::from_nanos(1_000),
                ..PlanOptions::default()
            },
        )
        .expect("plan");
        assert_eq!(plan.topics.len(), 1);
        assert_eq!(plan.topics[0].key, IMU);
        assert_eq!(plan.timeline.total_messages, 2);
        assert_eq!(plan.timeline.selected_messages, 1);
    }

    #[test]
    fn a_mixed_topic_outside_the_selection_cannot_fail_startup() {
        // The mixed raw+typed topic is filtered out, so MCAP Player Proposal
        // §6.3 never classifies it and the run plans the remaining topic (§6.1
        // publish plan).
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(LIDAR, "aorta.sensor.Lidar", b"bfbs-lidar")
            .raw_channel(LIDAR)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, 1_000, b"typed")
            .message(1, 1_100, b"raw")
            .message(2, 1_200, b"imu")
            .build();

        let plan = plan_one(
            bytes,
            &PlanOptions {
                exclude: vec![LIDAR.to_string()],
                ..PlanOptions::default()
            },
        )
        .expect("an excluded topic is never classified");
        assert_eq!(plan.topics.len(), 1);
        assert_eq!(plan.topics[0].key, IMU);
    }

    #[test]
    fn an_empty_selection_produces_a_zero_topic_plan_with_a_live_timeline() {
        let plan = plan_one(
            simple(),
            &PlanOptions {
                start: Duration::from_secs(3_600),
                ..PlanOptions::default()
            },
        )
        .expect("an empty selection is not an error");
        assert!(plan.is_empty_selection());
        assert_eq!(plan.timeline.selected_messages, 0);
        assert_eq!(plan.timeline.t0, None);
        assert_eq!(plan.timeline.total_messages, 3);
    }

    // -- segment sets (MCAP Player Proposal §6.1) --------------------------

    #[test]
    fn orders_a_rotated_set_by_segment_index_not_by_input_order() {
        let files = [
            ("s2.mcap", segment("rec-1", 2, 3_000)),
            ("s0.mcap", segment("rec-1", 0, 1_000)),
            ("s1.mcap", segment("rec-1", 1, 2_000)),
        ];
        let plan = plan_files(&files, &PlanOptions::default()).expect("contiguous set");
        let order: Vec<_> = plan
            .segments
            .iter()
            .map(|s| s.identity.segment_index.unwrap())
            .collect();
        assert_eq!(order, vec![0, 1, 2]);
        assert_eq!(plan.timeline.t_first, Some(1_000));
        assert_eq!(plan.timeline.t_last, Some(4_000));
        assert_eq!(plan.timeline.total_messages, 6);
        assert_eq!(plan.timeline.clamped_messages, 0);
    }

    #[test]
    fn rejects_a_gap_in_the_segment_indices_unless_allow_gaps() {
        let files = [
            ("s0.mcap", segment("rec-1", 0, 1_000)),
            ("s2.mcap", segment("rec-1", 2, 3_000)),
        ];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("--allow-gaps"), "got: {err}");

        let plan = plan_files(
            &files,
            &PlanOptions {
                allow_gaps: true,
                ..PlanOptions::default()
            },
        )
        .expect("--allow-gaps warns and plays");
        assert_eq!(plan.segments.len(), 2);
    }

    #[test]
    fn rejects_duplicate_segment_indices_unless_allow_gaps() {
        let files = [
            ("a.mcap", segment("rec-1", 0, 1_000)),
            ("b.mcap", segment("rec-1", 0, 1_000)),
        ];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got: {err}");
        assert!(plan_files(
            &files,
            &PlanOptions {
                allow_gaps: true,
                ..PlanOptions::default()
            }
        )
        .is_ok());
    }

    #[test]
    fn rejects_more_than_one_input_without_a_recording_id() {
        let files = [("a.mcap", simple()), ("b.mcap", simple())];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("recording_id"), "got: {err}");
    }

    #[test]
    fn rejects_mixed_recording_ids() {
        let files = [
            ("a.mcap", segment("rec-1", 0, 1_000)),
            ("b.mcap", segment("rec-2", 0, 1_000)),
        ];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("rec-2"), "got: {err}");
    }

    #[test]
    fn rejects_mixed_groups() {
        let other = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("other", 2_000, Some(("rec-1", 1, false)))
            .typed_channel("aorta/other/pub/sensor/imu", "aorta.sensor.Imu", BFBS_A)
            .message(0, 2_000, b"m0")
            .build();
        let files = [("a.mcap", segment("rec-1", 0, 1_000)), ("b.mcap", other)];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("group"), "got: {err}");
    }

    #[test]
    fn rejects_a_truncated_interior_segment() {
        let files = [
            (
                "s0.mcap",
                fixture::strip_end_magic(&segment("rec-1", 0, 1_000)),
            ),
            ("s1.mcap", segment("rec-1", 1, 2_000)),
        ];
        let err = plan_files(&files, &PlanOptions::default()).unwrap_err();
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    #[test]
    fn accepts_a_truncated_final_segment_and_reports_it() {
        let files = [
            ("s0.mcap", segment("rec-1", 0, 1_000)),
            (
                "s1.mcap",
                fixture::strip_end_magic(&segment("rec-1", 1, 2_000)),
            ),
        ];
        let plan = plan_files(&files, &PlanOptions::default())
            .expect("only the final segment may be truncated");
        assert_eq!(
            plan.truncation.truncated_inputs,
            vec![PathBuf::from("s1.mcap")]
        );
        assert_eq!(plan.segments.len(), 2);
        assert!(plan.segments[1].truncated);
        assert!(!plan.segments[0].truncated);
        // A stripped end magic discards nothing: the count stays exact.
        assert_eq!(plan.truncation.discarded_tail_chunks, 0);
        assert_eq!(plan.truncation.discarded_tail_messages, Some(0));
        assert_eq!(plan.timeline.total_messages, 4);
    }

    #[test]
    fn rejects_unrelated_segment_time_overlap() {
        let first = segment("recording", 0, 100);
        let second = simple();
        let err = plan_files(
            &[("first.mcap", first), ("second.mcap", second)],
            &PlanOptions::default(),
        )
        .expect_err("raw segment ranges overlap even when effective time is clamped");
        assert!(err.to_string().contains("overlap"), "got: {err}");
    }

    #[test]
    fn a_torn_final_chunk_reports_an_unknown_discarded_message_count() {
        // A torn prevalidated chunk yields no messages to count and its message
        // index was never written (MCAP Player Proposal §2, §6.1) — `unknown`,
        // not zero.
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A)
            .message(0, 1_000, b"m0")
            .flush_after(0)
            .message(0, 2_000, b"m1")
            .build();
        let chunks: Vec<_> = fixture::top_level_records(&bytes)
            .into_iter()
            .filter(|span| span.opcode == mcap::records::op::CHUNK)
            .collect();
        assert_eq!(chunks.len(), 2, "the fixture must have two chunks");
        let second = fixture::chunk_fields(&bytes, chunks[1]);
        let torn = fixture::truncate_at(&bytes, second.records_offset + 4);

        let plan = plan_one(torn, &PlanOptions::default()).expect("a torn tail is recoverable");
        assert_eq!(plan.truncation.discarded_tail_chunks, 1);
        assert_eq!(plan.truncation.discarded_tail_messages, None);
        assert_eq!(plan.timeline.total_messages, 1);
        assert_eq!(plan.topics.len(), 1);
        assert_eq!(plan.topics[0].messages, 1);
    }

    // -- planning byte budget (MCAP Player Proposal §6.1) ------------------

    #[test]
    fn a_channel_flood_exceeds_the_planning_budget_and_names_the_snapshot() {
        let mut builder =
            FixtureBuilder::new(FixtureOptions::default()).recorder_start("robot", 1_000, None);
        for index in 0..512 {
            let topic = format!("aorta/robot/pub/flood/topic-{index:08}");
            builder = builder.channel(fixture::typed_channel_spec(
                &topic,
                "aorta.flood.Item",
                format!("bfbs-{index:08}").as_bytes(),
            ));
        }
        let bytes = builder.message(0, 1_000, b"m0").build();

        let err = plan_files(
            &[("flood.mcap", bytes)],
            &PlanOptions {
                planning_budget_bytes: 8 * 1024,
                ..PlanOptions::default()
            },
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("flood.mcap") && text.contains("budget"),
            "the error must name the snapshot, got: {text}"
        );
    }

    #[test]
    fn many_tiny_chunks_keep_the_retained_planning_state_constant() {
        let build = |chunks: usize| {
            let mut builder = FixtureBuilder::new(FixtureOptions::default())
                .recorder_start("robot", 1_000, None)
                .typed_channel(IMU, "aorta.sensor.Imu", BFBS_A);
            for index in 0..chunks {
                builder = builder
                    .message(0, 1_000 + index as u64, b"m")
                    .flush_after(index);
            }
            builder.build()
        };

        let small = plan_one(build(4), &PlanOptions::default()).expect("plan");
        let large = plan_one(build(400), &PlanOptions::default()).expect("plan");
        assert_eq!(
            small.retained_bytes, large.retained_bytes,
            "chunk metadata is streamed, never retained (MCAP Player Proposal §6.1)"
        );
        assert_eq!(large.timeline.total_messages, 400);
    }

    #[test]
    fn planning_retained_bytes_include_topic_map_and_aggregate_clones() {
        let build = |extra: usize| {
            let topic = format!("aorta/robot/pub/{}", "x".repeat(32 + extra));
            FixtureBuilder::new(FixtureOptions::default())
                .recorder_start("robot", 1_000, None)
                .typed_channel(&topic, "aorta.sensor.Imu", BFBS_A)
                .message(0, 1_000, b"m0")
                .build()
        };

        let small = plan_one(build(0), &PlanOptions::default()).expect("small plan");
        let large = plan_one(build(4_096), &PlanOptions::default()).expect("large plan");
        let delta = large.retained_bytes - small.retained_bytes;
        assert!(
            delta >= 10 * 4_096,
            "topic retained-state growth must include both map and aggregate keys; delta={delta}"
        );
    }

    // -- cancellation (MCAP Player Proposal §6.7) --------------------------

    #[test]
    fn planning_stops_at_the_cancellation_flag() {
        let cancel = AtomicBool::new(true);
        let err = plan_files_cancellable(&[("a.mcap", simple())], &PlanOptions::default(), &cancel)
            .unwrap_err();
        assert!(
            crate::snapshot::is_cancelled(&err),
            "cancellation must surface as the Cancelled marker, got: {err}"
        );
        assert!(cancel.load(Ordering::SeqCst));
    }
}
