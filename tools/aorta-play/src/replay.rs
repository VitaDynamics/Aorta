//! Pass 2 — the timed replay (MCAP Player Proposal §6.5, §6.7).
//!
//! Two halves, deliberately on opposite sides of a bounded queue:
//!
//! - a **dedicated reader OS thread** re-walks each segment snapshot in play
//!   order through the same §6.7 adapter pass 1 used, recomputing the effective
//!   timeline, the window and the topic-selection predicates deterministically
//!   (§6.1: chunk metadata is streamed, never retained, so pass 2 derives
//!   rather than remembers). It follows the recorder's writer-thread contract
//!   (`tools/aorta-record/src/writer_thread.rs`): a bounded queue, sliced sends
//!   that re-check an atomic stop flag, terminal [`PlayEvent::Eof`] /
//!   [`PlayEvent::Error`] events, receiver-close as a second unblocking path,
//!   and a join deadline with detach-and-warn;
//! - an **async scheduler driver** that paces messages onto absolute targets
//!   ([`crate::timeline::Scheduler`]), publishing verbatim bytes and counting
//!   late / published / failed publishes. The shutdown token is checked on
//!   *every* iteration — including runs of already-due (clamped) messages where
//!   `sleep_until` is instantly ready and never yields.
//!
//! The queue admits up to [`QUEUE_CAPACITY`] messages **or** `--buffer-bytes`
//! of payload, whichever binds first; `--max-message-bytes ≤ --buffer-bytes`
//! (§6.12) guarantees every accepted message can eventually be enqueued.

use crate::plan::{PlaybackPlan, TopicFilter};
use crate::reader::{Flow, McapWalker, ReadError, ReaderLimits, RecordEvent, RecordSink};
use crate::snapshot::Snapshot;
use crate::timeline::{Clock, Scheduler, Step, TimelineAccumulator, Window};
use crate::{ProgressHandle, RunSummary};
use anyhow::{anyhow, Result};
use aorta_core::publisher::Publisher;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "fault-injection")]
use std::sync::Once;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Message slots in the pass-2 queue (MCAP Player Proposal §6.7).
pub(crate) const QUEUE_CAPACITY: usize = 1024;
/// How long a blocked send waits before re-checking the stop flag.
const SEND_SLICE: Duration = Duration::from_millis(2);
/// Publish errors are counted always, logged at this cadence (MCAP Player
/// Proposal §6.6).
const PUBLISH_ERROR_LOG_INTERVAL: u64 = 256;

#[cfg(all(test, feature = "fault-injection"))]
static TEST_READER_PANIC: AtomicBool = AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

/// One owned message on its way from the reader thread to the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlayMessage {
    /// Index into the publisher map.
    pub topic: usize,
    pub effective_time: u64,
    pub payload: Vec<u8>,
}

/// The reader thread's event stream. `Eof` and `Error` are terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlayEvent {
    Message(PlayMessage),
    /// One full walk of the segment set finished; `--loop` starts another.
    PassEnd,
    /// No more passes — the reader thread is done.
    Eof,
    /// Mid-replay failure (mid-file corruption, snapshot inconsistency).
    Error(String),
}

impl PlayEvent {
    fn payload_len(&self) -> u64 {
        match self {
            Self::Message(message) => message.payload.len() as u64,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendOutcome {
    Sent,
    /// Stop flag set, or the receiver is gone: unwind the walk.
    Stopped,
}

/// Sender half: bounded by slots *and* bytes, sliced so the stop flag stays
/// reachable while the queue is full.
struct QueueTx {
    tx: mpsc::Sender<PlayEvent>,
    in_flight: Arc<AtomicU64>,
    budget: u64,
    stop: Arc<AtomicBool>,
}

impl QueueTx {
    fn send(&self, event: PlayEvent) -> SendOutcome {
        let bytes = event.payload_len();
        let mut event = event;
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return SendOutcome::Stopped;
            }
            let in_flight = self.in_flight.load(Ordering::SeqCst);
            // The byte budget only blocks while something is already queued —
            // `--max-message-bytes ≤ --buffer-bytes` (MCAP Player Proposal
            // §6.12) then guarantees every accepted message is eventually
            // admissible.
            if in_flight > 0 && in_flight.saturating_add(bytes) > self.budget {
                std::thread::sleep(SEND_SLICE);
                continue;
            }
            self.in_flight.fetch_add(bytes, Ordering::SeqCst);
            match self.tx.try_send(event) {
                Ok(()) => return SendOutcome::Sent,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    self.in_flight.fetch_sub(bytes, Ordering::SeqCst);
                    event = returned;
                    std::thread::sleep(SEND_SLICE);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.in_flight.fetch_sub(bytes, Ordering::SeqCst);
                    return SendOutcome::Stopped;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reader thread
// ---------------------------------------------------------------------------

/// Everything the reader thread owns for the whole run (including the snapshot
/// fds — pass 2 reads the same private copies pass 1 verified).
pub(crate) struct ReplayInputs {
    pub snapshots: Vec<Snapshot>,
    pub segments: Vec<ReplaySegment>,
    pub filter: TopicFilter,
    pub start: Duration,
    pub duration: Option<Duration>,
    pub reader_limits: ReaderLimits,
    /// Recorded topic key → publisher index.
    pub topic_index: BTreeMap<String, usize>,
    pub loop_playback: bool,
}

/// One segment in play order, as pass 1 planned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplaySegment {
    pub source: PathBuf,
    pub input_index: usize,
    /// MCAP Player Proposal §6.1 verified safe prefix — pass 2 must reproduce
    /// it exactly.
    pub safe_prefix_end: u64,
}

impl ReplayInputs {
    /// Build the pass-2 inputs from the pass-1 plan.
    pub(crate) fn from_plan(
        plan: &PlaybackPlan,
        snapshots: Vec<Snapshot>,
        filter: TopicFilter,
        start: Duration,
        duration: Option<Duration>,
        reader_limits: ReaderLimits,
        loop_playback: bool,
    ) -> Self {
        let topic_index = plan
            .topics
            .iter()
            .enumerate()
            .map(|(index, topic)| (topic.key.clone(), index))
            .collect();
        let segments = plan
            .segments
            .iter()
            .map(|segment| ReplaySegment {
                source: segment.source.clone(),
                input_index: segment.input_index,
                safe_prefix_end: segment.safe_prefix_end,
            })
            .collect();
        Self {
            snapshots,
            segments,
            filter,
            start,
            duration,
            reader_limits,
            topic_index,
            loop_playback,
        }
    }
}

/// Handle to the managed reader thread (MCAP Player Proposal §6.7 writer-thread
/// contract).
pub(crate) struct ReaderThread {
    rx: mpsc::Receiver<PlayEvent>,
    stop: Arc<AtomicBool>,
    in_flight: Arc<AtomicU64>,
    /// Resolves when the thread exits — a channel, not a `join()`, so the async
    /// side can bound the wait and detach a wedged thread.
    finished: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Debug)]
enum ReaderThreadError {
    Panicked(String),
    Vanished,
}

impl std::fmt::Display for ReaderThreadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Panicked(message) => write!(f, "pass-2 reader thread panicked: {message}"),
            Self::Vanished => {
                f.write_str("pass-2 reader thread vanished before its terminal event")
            }
        }
    }
}

impl std::error::Error for ReaderThreadError {}

impl ReaderThread {
    pub(crate) fn spawn(inputs: ReplayInputs, buffer_bytes: u64) -> Result<Self> {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let stop = Arc::new(AtomicBool::new(false));
        let in_flight = Arc::new(AtomicU64::new(0));
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

        let queue = QueueTx {
            tx,
            in_flight: in_flight.clone(),
            budget: buffer_bytes,
            stop: stop.clone(),
        };
        // Detached on purpose: the join is deadline-bounded on the async side
        // and a wedged thread dies with the process (MCAP Player Proposal
        // §6.7).
        std::thread::Builder::new()
            .name("aorta-play-reader".to_string())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reader_loop(inputs, &queue);
                }));
                if let Err(payload) = result {
                    let message = payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic payload".to_string());
                    queue.send(PlayEvent::Error(
                        ReaderThreadError::Panicked(message).to_string(),
                    ));
                }
                let _ = finished_tx.send(());
            })
            .map_err(|err| anyhow!("cannot spawn aorta-play reader thread: {err}"))?;

        Ok(Self {
            rx,
            stop,
            in_flight,
            finished: finished_rx,
        })
    }

    /// Next event, releasing the message's byte budget as it is handed over.
    pub(crate) async fn recv(&mut self) -> Option<PlayEvent> {
        let event = self.rx.recv().await?;
        self.in_flight
            .fetch_sub(event.payload_len(), Ordering::SeqCst);
        Some(event)
    }

    pub(crate) fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Stop and wait for the thread, bounded by `deadline`. `false` means the
    /// thread was detached at the deadline (residual, MCAP Player Proposal
    /// §6.7).
    pub(crate) async fn stop_and_join(self, deadline: Duration) -> bool {
        let Self {
            rx, stop, finished, ..
        } = self;
        stop.store(true, Ordering::SeqCst);
        // Receiver close is the second unblocking path: a sender parked in a
        // sliced retry sees `Closed` even if it missed the flag.
        drop(rx);
        matches!(tokio::time::timeout(deadline, finished).await, Ok(Ok(())))
    }
}

fn reader_loop(inputs: ReplayInputs, queue: &QueueTx) {
    #[cfg(all(test, feature = "fault-injection"))]
    if TEST_READER_PANIC.swap(false, Ordering::SeqCst) {
        panic!("test reader panic");
    }
    loop {
        match replay_pass(&inputs, queue) {
            Ok(PassEnd::Completed) => {}
            Ok(PassEnd::Stopped) => return,
            Err(err) => {
                queue.send(PlayEvent::Error(format!("{err:#}")));
                return;
            }
        }
        if !inputs.loop_playback {
            break;
        }
        if queue.send(PlayEvent::PassEnd) == SendOutcome::Stopped {
            return;
        }
    }
    queue.send(PlayEvent::Eof);
}

enum PassEnd {
    Completed,
    Stopped,
}

/// One full walk of the ordered segment set.
fn replay_pass(inputs: &ReplayInputs, queue: &QueueTx) -> Result<PassEnd> {
    let mut timeline = TimelineAccumulator::new();
    let mut window: Option<Window> = None;

    for segment in &inputs.segments {
        let snapshot = inputs.snapshots.get(segment.input_index).ok_or_else(|| {
            anyhow!(
                "internal error: segment '{}' references snapshot {} of {}",
                segment.source.display(),
                segment.input_index,
                inputs.snapshots.len()
            )
        })?;
        let mut walker = McapWalker::open(&snapshot.file, inputs.reader_limits)
            .map_err(|err| walk_error(&segment.source, err))?;
        let mut sink = ReplaySink {
            inputs,
            queue,
            timeline: &mut timeline,
            window: &mut window,
            channels: BTreeMap::new(),
            stopped: false,
            past_window: false,
        };
        let report = walker
            .walk_with_stop(&mut sink, &queue.stop)
            .map_err(|err| walk_error(&segment.source, err))?;
        let (stopped, past_window) = (sink.stopped, sink.past_window);

        if stopped {
            return Ok(PassEnd::Stopped);
        }
        if past_window {
            // The effective timeline is non-decreasing, so nothing later can
            // fall back inside `[start, end]` (MCAP Player Proposal §6.5).
            break;
        }
        // Pass 2 must replay strictly within the pass-1 verified boundaries
        // (MCAP Player Proposal §6.1). Same snapshot, same adapter, same limits
        // — a different prefix means the snapshot changed under us.
        if report.safe_prefix_end != segment.safe_prefix_end {
            return Err(anyhow!(
                "'{}': the verified safe prefix moved between passes ({} → {}) — the private \
                 snapshot is inconsistent (MCAP Player Proposal §6.1)",
                segment.source.display(),
                segment.safe_prefix_end,
                report.safe_prefix_end
            ));
        }
    }
    Ok(PassEnd::Completed)
}

fn walk_error(source: &std::path::Path, err: ReadError) -> anyhow::Error {
    match err {
        ReadError::Sink(inner) => inner,
        other => anyhow!("'{}': {}", source.display(), other),
    }
}

struct ReplaySink<'a> {
    inputs: &'a ReplayInputs,
    queue: &'a QueueTx,
    timeline: &'a mut TimelineAccumulator,
    window: &'a mut Option<Window>,
    /// Channel id → publisher index, `None` for anything not replayed.
    channels: BTreeMap<u16, Option<usize>>,
    stopped: bool,
    past_window: bool,
}

impl RecordSink for ReplaySink<'_> {
    fn on_record(&mut self, record: &RecordEvent<'_>) -> Result<Flow> {
        let stop_flag = &self.queue.stop;
        if stop_flag.load(Ordering::SeqCst) {
            self.stopped = true;
            return Ok(Flow::Stop);
        }
        match record {
            RecordEvent::Channel {
                id,
                topic,
                metadata,
                ..
            } => {
                let key_suffix = metadata
                    .get("key_suffix")
                    .cloned()
                    .unwrap_or_else(|| crate::plan::key_suffix_of(topic).to_string());
                let target = crate::plan::topic_suffix_of(&key_suffix)
                    .filter(|topic_suffix| {
                        self.inputs.filter.accepts(topic, &key_suffix, topic_suffix)
                    })
                    .and_then(|_| self.inputs.topic_index.get(topic.as_str()).copied());
                self.channels.insert(*id, target);
                Ok(Flow::Continue)
            }
            RecordEvent::Message {
                channel_id,
                log_time,
                data,
                ..
            } => {
                // Step 1 (MCAP Player Proposal §6.5): every channel feeds the
                // timeline, before filtering — pacing of the remainder is
                // filter-independent.
                let effective = self.timeline.push(*log_time);
                let window = match self.window {
                    Some(window) => *window,
                    None => {
                        let window =
                            Window::resolve(effective, self.inputs.start, self.inputs.duration)?;
                        *self.window = Some(window);
                        window
                    }
                };
                if window.end.is_some_and(|end| effective > end) {
                    self.past_window = true;
                    return Ok(Flow::Stop);
                }
                if !window.contains(effective) {
                    return Ok(Flow::Continue);
                }
                let Some(Some(topic)) = self.channels.get(channel_id).copied() else {
                    return Ok(Flow::Continue);
                };
                // The oversize-payload rejection already happened inside the
                // adapter, before this owned copy (MCAP Player Proposal §6.7).
                let message = PlayMessage {
                    topic,
                    effective_time: effective,
                    payload: data.to_vec(),
                };
                match self.queue.send(PlayEvent::Message(message)) {
                    SendOutcome::Sent => Ok(Flow::Continue),
                    SendOutcome::Stopped => {
                        self.stopped = true;
                        Ok(Flow::Stop)
                    }
                }
            }
            _ => Ok(Flow::Continue),
        }
    }
}

// ---------------------------------------------------------------------------
// Scheduler driver (async side)
// ---------------------------------------------------------------------------

/// One planned topic's publisher, in publisher-map order.
pub(crate) struct TopicPublisher {
    pub key: String,
    pub publisher: Publisher,
}

/// How the pass-2 driver ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayEnd {
    /// The reader thread reported `Eof`.
    Finished,
    /// The shutdown token fired (MCAP Player Proposal §6.7).
    Cancelled,
}

pub(crate) struct DriveOptions<'a> {
    pub publishers: &'a [TopicPublisher],
    pub scheduler: Scheduler,
    pub clock: &'a dyn Clock,
    pub token: &'a CancellationToken,
    pub progress: &'a ProgressHandle,
    /// Effective-time span of the selection, for the progress line.
    pub span_ns: u64,
}

/// Drive the timed replay until `Eof`, a mid-replay error, or cancellation.
pub(crate) async fn drive(
    reader: &mut ReaderThread,
    mut options: DriveOptions<'_>,
    summary: &mut RunSummary,
) -> Result<ReplayEnd> {
    let end = loop {
        // Checked on every iteration, including runs of already-due messages
        // that never yield (MCAP Player Proposal §6.7).
        if options.token.is_cancelled() {
            break ReplayEnd::Cancelled;
        }
        let event = tokio::select! {
            biased;
            _ = options.token.cancelled() => break ReplayEnd::Cancelled,
            event = reader.recv() => event,
        };
        let Some(event) = event else {
            // The thread vanished without a terminal event: treat as end of
            // stream rather than hanging.
            return Err(anyhow!(ReaderThreadError::Vanished));
        };
        match event {
            PlayEvent::Message(message) => {
                match publish_when_due(&mut options, &message, summary).await? {
                    Due::Published => {
                        options.progress.observe(
                            summary.published_messages,
                            message
                                .effective_time
                                .saturating_sub(options.scheduler.t0_ns()),
                            options.span_ns,
                        );
                    }
                    Due::Cancelled => break ReplayEnd::Cancelled,
                }
            }
            PlayEvent::PassEnd => {
                // `--loop`: the next pass re-derives `wall0` (MCAP Player
                // Proposal §6.5).
                options.scheduler.restart(options.clock);
                tracing::debug!(
                    published = summary.published_messages,
                    "aorta-play: starting another --loop pass"
                );
            }
            PlayEvent::Eof => break ReplayEnd::Finished,
            PlayEvent::Error(message) => return Err(anyhow!(message)),
        }
    };

    summary.late_messages = options.scheduler.late_messages();
    summary.max_lateness = options.scheduler.max_lateness();
    Ok(end)
}

enum Due {
    Published,
    Cancelled,
}

async fn publish_when_due(
    options: &mut DriveOptions<'_>,
    message: &PlayMessage,
    summary: &mut RunSummary,
) -> Result<Due> {
    loop {
        if options.token.is_cancelled() {
            return Ok(Due::Cancelled);
        }
        match options
            .scheduler
            .step(message.effective_time, options.clock)?
        {
            Step::Sleep(remaining) => {
                tokio::select! {
                    biased;
                    _ = options.token.cancelled() => return Ok(Due::Cancelled),
                    _ = tokio::time::sleep(remaining) => {}
                }
            }
            Step::Publish { .. } => {
                let Some(topic) = options.publishers.get(message.topic) else {
                    return Err(anyhow!(
                        "internal error: message references publisher {} of {}",
                        message.topic,
                        options.publishers.len()
                    ));
                };
                // Verbatim wire bytes (MCAP Player Proposal §6.11) —
                // fire-and-forget, success means enqueued (§6.4).
                test_wedge_at_publish_call();
                match topic.publisher.publish(message.payload.clone()) {
                    Ok(()) => {
                        summary.published_messages += 1;
                        *summary
                            .published_per_channel
                            .entry(topic.key.clone())
                            .or_insert(0) += 1;
                    }
                    Err(err) => {
                        summary.publish_errors += 1;
                        if summary.publish_errors == 1
                            || summary
                                .publish_errors
                                .is_multiple_of(PUBLISH_ERROR_LOG_INTERVAL)
                        {
                            tracing::warn!(
                                "aorta-play: publish failed on '{}' ({} error(s) so far): {}",
                                topic.key,
                                summary.publish_errors,
                                err
                            );
                        }
                    }
                }
                return Ok(Due::Published);
            }
        }
    }
}

/// Test-only process wedge at the synchronous publish call site. The child
/// signal tests use this hook to hold the scheduler in the one state where the
/// library cannot observe cancellation.
#[cfg(feature = "fault-injection")]
fn test_wedge_at_publish_call() {
    static WEDGED: Once = Once::new();
    if std::env::var("AORTA_PLAY_TEST_WEDGE").ok().as_deref() != Some("1") {
        return;
    }

    WEDGED.call_once(|| eprintln!("AORTA_PLAY_TEST_WEDGE_ARMED=1"));
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

#[cfg(not(feature = "fault-injection"))]
fn test_wedge_at_publish_call() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture;
    use crate::snapshot::{snapshot_inputs, StatvfsSpaceProbe};
    use std::path::Path;

    /// Scratch directory holding fixture inputs and their private snapshots.
    struct Scratch {
        dir: PathBuf,
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "aorta-play-replay-{}-{}",
                std::process::id(),
                tag
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("scratch dir");
            Self { dir }
        }

        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.dir.join(name);
            std::fs::write(&path, bytes).expect("fixture");
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn snapshots_of(paths: &[PathBuf], dir: &Path) -> Vec<Snapshot> {
        let temp = dir.join("snap");
        std::fs::create_dir_all(&temp).expect("temp dir");
        let stop = AtomicBool::new(false);
        snapshot_inputs(paths, &temp, &stop, &StatvfsSpaceProbe).expect("snapshots")
    }

    fn plan_of(snapshots: &[Snapshot], options: &crate::plan::PlanOptions) -> PlaybackPlan {
        let inputs: Vec<crate::plan::PlanInput<'_>> = snapshots
            .iter()
            .map(|snapshot| crate::plan::PlanInput {
                source: snapshot.source.clone(),
                reader: &snapshot.file,
            })
            .collect();
        let stop = AtomicBool::new(false);
        crate::plan::build_plan(&inputs, options, &stop).expect("plan")
    }

    /// Collect everything the reader thread produces for one run.
    async fn collect(mut reader: ReaderThread) -> Vec<PlayEvent> {
        let mut events = Vec::new();
        while let Some(event) = reader.recv().await {
            let terminal = matches!(event, PlayEvent::Eof | PlayEvent::Error(_));
            events.push(event);
            if terminal {
                break;
            }
        }
        assert!(
            reader.stop_and_join(Duration::from_secs(5)).await,
            "the reader thread must join well inside its deadline"
        );
        events
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reader_thread_replays_the_selected_messages_in_stream_order() {
        let scratch = Scratch::new("order");
        let input = scratch.write(
            "a.mcap",
            &fixture::simple_file(fixture::FixtureOptions::default()),
        );
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions::default();
        let plan = plan_of(&snapshots, &options);

        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            TopicFilter::default(),
            Duration::ZERO,
            None,
            options.reader_limits,
            false,
        );
        let events = collect(ReaderThread::spawn(inputs, 1 << 20).expect("reader thread")).await;

        let payloads: Vec<Vec<u8>> = events
            .iter()
            .filter_map(|event| match event {
                PlayEvent::Message(message) => Some(message.payload.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            payloads,
            vec![
                b"payload-0".to_vec(),
                b"payload-1".to_vec(),
                b"payload-2".to_vec()
            ],
            "verbatim bytes, in canonical record-stream order (MCAP Player Proposal §6.5, §6.11)"
        );
        assert_eq!(events.last(), Some(&PlayEvent::Eof), "terminal event");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_effective_timeline_is_recomputed_identically_in_pass_2() {
        // MCAP Player Proposal §6.5 worked example: file order 100, 90, 95 →
        // effective 100, 100, 100, replayed back-to-back.
        let scratch = Scratch::new("clamp");
        let bytes = fixture::FixtureBuilder::new(fixture::FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel("aorta/robot/pub/sensor/imu", "Imu", b"bfbs-imu")
            .message(0, 100, b"a")
            .message(0, 90, b"b")
            .message(0, 95, b"c")
            .build();
        let input = scratch.write("clamp.mcap", &bytes);
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions::default();
        let plan = plan_of(&snapshots, &options);
        assert_eq!(plan.timeline.clamped_messages, 2);

        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            TopicFilter::default(),
            Duration::ZERO,
            None,
            options.reader_limits,
            false,
        );
        let events = collect(ReaderThread::spawn(inputs, 1 << 20).expect("reader thread")).await;
        let times: Vec<u64> = events
            .iter()
            .filter_map(|event| match event {
                PlayEvent::Message(message) => Some(message.effective_time),
                _ => None,
            })
            .collect();
        assert_eq!(times, vec![100, 100, 100]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn excluded_topics_never_reach_the_queue_but_still_pace_the_rest() {
        let scratch = Scratch::new("filter");
        let bytes = fixture::FixtureBuilder::new(fixture::FixtureOptions::default())
            .recorder_start("robot", 1_000, None)
            .typed_channel("aorta/robot/pub/sensor/imu", "Imu", b"bfbs-imu")
            .typed_channel("aorta/robot/pub/sensor/lidar", "Lidar", b"bfbs-lidar")
            .message(0, 100, b"imu-0")
            .message(1, 200, b"lidar-0")
            .message(0, 300, b"imu-1")
            .build();
        let input = scratch.write("two.mcap", &bytes);
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions {
            topics: vec!["sensor/imu".to_string()],
            ..crate::plan::PlanOptions::default()
        };
        let plan = plan_of(&snapshots, &options);
        assert_eq!(plan.topics.len(), 1, "only the selected topic is planned");

        let filter = TopicFilter::new(options.topics.clone(), options.exclude.clone());
        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            filter,
            Duration::ZERO,
            None,
            options.reader_limits,
            false,
        );
        let events = collect(ReaderThread::spawn(inputs, 1 << 20).expect("reader thread")).await;
        let messages: Vec<&PlayMessage> = events
            .iter()
            .filter_map(|event| match event {
                PlayEvent::Message(message) => Some(message),
                _ => None,
            })
            .collect();
        assert_eq!(messages.len(), 2, "only the imu messages are publishable");
        assert!(messages.iter().all(|message| message.topic == 0));
        assert_eq!(
            messages[1].effective_time, 300,
            "the filtered-out lidar message still advanced the timeline"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loop_mode_emits_a_pass_boundary_between_walks() {
        let scratch = Scratch::new("loop");
        let input = scratch.write(
            "a.mcap",
            &fixture::simple_file(fixture::FixtureOptions::default()),
        );
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions::default();
        let plan = plan_of(&snapshots, &options);
        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            TopicFilter::default(),
            Duration::ZERO,
            None,
            options.reader_limits,
            true,
        );

        let mut reader = ReaderThread::spawn(inputs, 1 << 20).expect("reader thread");
        let mut messages = 0usize;
        let mut passes = 0usize;
        while passes < 2 {
            match reader.recv().await.expect("the loop keeps producing") {
                PlayEvent::Message(_) => messages += 1,
                PlayEvent::PassEnd => passes += 1,
                other => panic!("unexpected {other:?} in loop mode"),
            }
        }
        assert_eq!(messages, 6, "two full passes over a three-message fixture");
        assert!(
            reader.stop_and_join(Duration::from_secs(5)).await,
            "a looping reader stops on request (MCAP Player Proposal §6.7)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_queue_stays_stoppable() {
        // Byte budget of 1 with more than one message in flight: the sender
        // parks in its sliced retry, and the stop request must still land.
        let scratch = Scratch::new("backpressure");
        let input = scratch.write(
            "a.mcap",
            &fixture::simple_file(fixture::FixtureOptions::default()),
        );
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions::default();
        let plan = plan_of(&snapshots, &options);
        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            TopicFilter::default(),
            Duration::ZERO,
            None,
            options.reader_limits,
            true,
        );

        let mut reader = ReaderThread::spawn(inputs, 1).expect("reader thread");
        // One message fits (the budget only blocks while something is queued).
        assert!(matches!(reader.recv().await, Some(PlayEvent::Message(_))));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            reader.stop_and_join(Duration::from_secs(5)).await,
            "a sender blocked on the byte budget still observes the stop flag"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(feature = "fault-injection")]
    async fn reader_panics_are_terminal_errors() {
        let scratch = Scratch::new("panic");
        let input = scratch.write(
            "a.mcap",
            &fixture::simple_file(fixture::FixtureOptions::default()),
        );
        let snapshots = snapshots_of(&[input], &scratch.dir);
        let options = crate::plan::PlanOptions::default();
        let plan = plan_of(&snapshots, &options);
        let inputs = ReplayInputs::from_plan(
            &plan,
            snapshots,
            TopicFilter::default(),
            Duration::ZERO,
            None,
            options.reader_limits,
            false,
        );
        TEST_READER_PANIC.store(true, Ordering::SeqCst);
        let events = collect(ReaderThread::spawn(inputs, 1 << 20).expect("reader thread")).await;
        assert!(matches!(
            events.last(),
            Some(PlayEvent::Error(message)) if message.contains("pass-2 reader thread panicked")
        ));
    }
}
