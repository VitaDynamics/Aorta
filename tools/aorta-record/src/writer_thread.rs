//! Dedicated writer thread: owns the [`RotatingWriter`] so synchronous MCAP
//! I/O (disk writes, flush, rotation, retention) never runs on the async drain
//! task — a stalled disk backs up into the bounded command queue (visible as
//! `writer_queue` depth in the stats line) instead of stalling ingestion and
//! schema resolution.
//!
//! Contract with the drain loop:
//! - [`WriterThread::send_timeout`] applies backpressure in bounded slices, so
//!   stop signals stay reachable while the queue is full.
//! - `max_messages` is enforced HERE, exactly: past-cap writes are ignored, so
//!   the drain loop noticing the cap a few events late never over-writes.
//! - A write/flush/roll error terminates the thread; the drain loop observes
//!   this as send failures / [`WriterThread::is_stopped`], and the real error
//!   surfaces from [`finish`], which is itself deadline-bounded.
//!
//! [`finish`]: WriterThread::finish_with_deadline

use crate::config::{LogLevel, RecorderConfig};
use crate::rotation::{RotatingWriter, RotationOutcome};
use crate::writer::RecordMessage;
use crate::{log_roll, max_messages_reached};
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub(crate) enum WriterCmd {
    Write(Box<RecordMessage>),
    /// Periodic flush + rotation check (sent from the drain loop's flush tick;
    /// best-effort — a full queue skips the tick, batch flushing still applies).
    Tick,
    /// Test-only fault injection: park the writer thread until the barrier's
    /// sender side releases it, simulating hung disk I/O.
    #[cfg(test)]
    TestBlock(std::sync::mpsc::Receiver<()>),
}

/// Outcome of a deadline-sliced enqueue attempt (see [`WriterThread::send_timeout`]).
pub(crate) enum SendOutcome {
    Sent,
    /// The bounded queue stayed full for the whole slice; the message is handed
    /// back so the caller can re-check stop signals and retry (or drop it).
    Timeout(Box<RecordMessage>),
    /// The writer thread has stopped (I/O error); the error surfaces at finish.
    Stopped,
}

pub(crate) struct WriterThread {
    tx: mpsc::Sender<WriterCmd>,
    accepted: Arc<AtomicU64>,
    cap_reached: Arc<AtomicBool>,
    /// Resolves with the writer's final result (finish outcome or its first
    /// I/O error). Delivered via a channel — not a thread join — so the async
    /// side can bound how long it waits (`shutdown_timeout_ms`
    /// must also cover queue drain + finalize, and hung I/O must not hang the
    /// process on an unbounded `join()`).
    outcome: tokio::sync::oneshot::Receiver<Result<RotationOutcome>>,
}

impl WriterThread {
    /// Move `writer` onto a dedicated OS thread. `schema_miss` / `dropped` are
    /// the drain-side counters the writer snapshots into rotation metadata and
    /// the end-of-run summary (they were plain locals when writing was inline).
    pub(crate) fn spawn(
        writer: RotatingWriter,
        config: &RecorderConfig,
        schema_miss: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    ) -> Self {
        // Same bound as the ingress queue: worst-case buffered memory is one
        // extra queue of messages between drain and disk.
        let (tx, rx) = mpsc::channel::<WriterCmd>(config.queue_capacity);
        let accepted = Arc::new(AtomicU64::new(writer.accepted_messages()));
        let cap_reached = Arc::new(AtomicBool::new(max_messages_reached(
            writer.accepted_messages(),
            config.max_messages,
        )));

        let params = ThreadParams {
            batch_max_messages: config.batch_max_messages,
            batch_max_bytes: config.batch_max_bytes,
            max_messages: config.max_messages,
            log_level: config.log_level,
        };
        let thread_accepted = accepted.clone();
        let thread_cap = cap_reached.clone();
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        // The handle is intentionally detached: waiting happens on the outcome
        // channel with a deadline. If the writer is wedged in hung I/O past the
        // shutdown deadline, the process exits and takes the thread with it —
        // the deliberate forced-stop semantics (the MCAP is unfinalized then,
        // exactly as it would be under a hard kill).
        std::thread::Builder::new()
            .name("aorta-record-writer".to_string())
            .spawn(move || {
                let result = writer_loop(
                    writer,
                    rx,
                    params,
                    thread_accepted,
                    thread_cap,
                    schema_miss,
                    dropped,
                );
                let _ = outcome_tx.send(result);
            })
            .expect("spawn aorta-record writer thread");

        Self {
            tx,
            accepted,
            cap_reached,
            outcome: outcome_rx,
        }
    }

    /// Deadline-sliced enqueue: waits at most `slice` for queue space, handing
    /// the message back on timeout so the caller can re-check stop signals
    /// (Ctrl-C / duration / shutdown deadline) between slices — a full queue
    /// over a stuck-but-alive writer must not make the stop paths unreachable.
    pub(crate) async fn send_timeout(
        &self,
        message: Box<RecordMessage>,
        slice: std::time::Duration,
    ) -> SendOutcome {
        use tokio::sync::mpsc::error::SendTimeoutError;
        match self.tx.send_timeout(WriterCmd::Write(message), slice).await {
            Ok(()) => SendOutcome::Sent,
            Err(SendTimeoutError::Timeout(WriterCmd::Write(message))) => {
                SendOutcome::Timeout(message)
            }
            Err(SendTimeoutError::Timeout(_)) => unreachable!("only Write is sent here"),
            Err(SendTimeoutError::Closed(_)) => SendOutcome::Stopped,
        }
    }

    /// Best-effort periodic flush/roll signal; skipped when the queue is full
    /// (queued writes will batch-flush anyway).
    pub(crate) fn try_tick(&self) {
        let _ = self.tx.try_send(WriterCmd::Tick);
    }

    /// Test-only unbounded enqueue (production paths use [`Self::send_timeout`]
    /// so stop signals stay reachable under backpressure).
    #[cfg(test)]
    pub(crate) async fn write(&self, message: RecordMessage) -> Result<()> {
        self.tx
            .send(WriterCmd::Write(Box::new(message)))
            .await
            .map_err(|_| anyhow!("writer thread stopped; see the error reported at finish"))
    }

    /// Test-only fault injection: park the writer thread (simulated hung I/O)
    /// until the returned sender is used or dropped.
    #[cfg(test)]
    pub(crate) async fn test_block(&self) -> std::sync::mpsc::Sender<()> {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        self.tx
            .send(WriterCmd::TestBlock(release_rx))
            .await
            .expect("writer thread alive");
        release_tx
    }

    /// True once the writer thread has exited (I/O error): its receiver is
    /// gone. The drain loop polls this on the flush tick so an idle recording
    /// (no incoming events to fail a `write()` on) still notices a dead writer
    /// within one flush interval instead of hanging until Ctrl-C — the inline
    /// writer surfaced this via `writer.flush()?` on the same tick.
    pub(crate) fn is_stopped(&self) -> bool {
        self.tx.is_closed()
    }

    /// Messages accepted by the writer so far (across all segments).
    pub(crate) fn accepted_messages(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// True once `max_messages` has been reached (enforced writer-side).
    pub(crate) fn cap_reached(&self) -> bool {
        self.cap_reached.load(Ordering::Relaxed)
    }

    /// Commands currently queued toward the writer (live observability).
    pub(crate) fn queue_depth(&self) -> usize {
        self.tx.max_capacity().saturating_sub(self.tx.capacity())
    }

    /// Close the command channel and wait — bounded by `deadline` — for the
    /// writer to drain its queue and finalize the MCAP (final flush + end
    /// metadata + retention). Returns the writer's own error if it stopped
    /// early, or a forced-stop error if the deadline elapses first (hung or
    /// pathologically slow I/O); in that case the detached thread keeps
    /// draining in the background and dies with the process.
    pub(crate) async fn finish_with_deadline(
        self,
        deadline: tokio::time::Instant,
    ) -> Result<RotationOutcome> {
        let Self { tx, outcome, .. } = self;
        drop(tx);
        match tokio::time::timeout_at(deadline, outcome).await {
            Ok(Ok(result)) => result,
            Ok(Err(_recv)) => Err(anyhow!("writer thread panicked")),
            Err(_elapsed) => Err(anyhow!(
                "writer did not finish within the shutdown deadline; forced stop \
                 (queued messages may be lost and the MCAP may be unfinalized)"
            )),
        }
    }
}

struct ThreadParams {
    batch_max_messages: usize,
    batch_max_bytes: usize,
    max_messages: Option<u64>,
    log_level: LogLevel,
}

fn writer_loop(
    mut writer: RotatingWriter,
    mut rx: mpsc::Receiver<WriterCmd>,
    params: ThreadParams,
    accepted: Arc<AtomicU64>,
    cap_reached: Arc<AtomicBool>,
    schema_miss: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
) -> Result<RotationOutcome> {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            WriterCmd::Write(message) => {
                if cap_reached.load(Ordering::Relaxed) {
                    // Exact cap enforcement: the drain loop may enqueue a few
                    // events past the cap before it observes the flag.
                    continue;
                }
                writer.write_message(*message)?;
                accepted.store(writer.accepted_messages(), Ordering::Relaxed);
                if max_messages_reached(writer.accepted_messages(), params.max_messages) {
                    cap_reached.store(true, Ordering::Relaxed);
                    // Parity with the inline loop: no roll once stopping.
                    continue;
                }
                if writer.should_flush(params.batch_max_messages, params.batch_max_bytes) {
                    writer.flush()?;
                }
                if let Some(info) = writer.maybe_roll(
                    Instant::now(),
                    schema_miss.load(Ordering::Relaxed),
                    dropped.load(Ordering::Relaxed),
                )? {
                    log_roll(params.log_level, &info);
                }
            }
            WriterCmd::Tick => {
                writer.flush()?;
                if !cap_reached.load(Ordering::Relaxed) {
                    if let Some(info) = writer.maybe_roll(
                        Instant::now(),
                        schema_miss.load(Ordering::Relaxed),
                        dropped.load(Ordering::Relaxed),
                    )? {
                        log_roll(params.log_level, &info);
                    }
                }
            }
            #[cfg(test)]
            WriterCmd::TestBlock(barrier) => {
                // Simulated hung I/O: park until the test releases the barrier
                // (or drops it).
                let _ = barrier.recv();
            }
        }
    }

    writer.flush()?;
    writer.finish(
        schema_miss.load(Ordering::Relaxed),
        dropped.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::WriterThread;
    use crate::config::{build_config, CompressionKind, ConfigOverrides, FileConfig};
    use crate::rotation::RotatingWriter;
    use crate::writer::RecordMessage;
    use crate::SegmentFinalized;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    fn test_setup(
        tag: &str,
        max_messages: Option<u64>,
    ) -> (crate::config::RecorderConfig, std::path::PathBuf) {
        test_setup_with_capacity(tag, max_messages, None)
    }

    fn test_setup_with_capacity(
        tag: &str,
        max_messages: Option<u64>,
        queue_capacity: Option<usize>,
    ) -> (crate::config::RecorderConfig, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "aorta-record-wt-{}-{}.mcap",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("default".to_string());
        overrides.output = Some(path.to_string_lossy().to_string());
        overrides.compression = Some(CompressionKind::None);
        overrides.max_messages = max_messages;
        overrides.queue_capacity = queue_capacity;
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        (config, path)
    }

    /// Generous-deadline finish for happy-path tests.
    async fn finish_ok(wt: WriterThread) -> crate::rotation::RotationOutcome {
        wt.finish_with_deadline(tokio::time::Instant::now() + std::time::Duration::from_secs(30))
            .await
            .expect("finish")
    }

    fn message(i: u64) -> RecordMessage {
        RecordMessage {
            topic_key: "aorta/default/pub/demo/chatter".to_string(),
            key_suffix: "pub/demo/chatter".to_string(),
            schema: None,
            unresolved_schema_hash: None,
            payload: vec![i as u8; 4],
            log_time_ns: i,
            publish_time_ns: i,
        }
    }

    /// Happy path: writes flow through the thread, counters track, finish
    /// finalizes a readable MCAP with every message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_flow_through_thread_and_finish_finalizes() {
        let (config, path) = test_setup("flow", None);
        let writer = RotatingWriter::create(&config).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        for i in 0..5 {
            wt.write(message(i)).await.expect("write");
        }
        wt.try_tick(); // must be accepted (or skipped) without disturbing writes

        let outcome = finish_ok(wt).await;
        assert_eq!(outcome.accepted_messages, 5);

        let bytes = std::fs::read(&path).expect("read mcap");
        let count = mcap::MessageStream::new(&bytes)
            .expect("stream")
            .map(|m| m.expect("message"))
            .count();
        assert_eq!(count, 5, "all queued messages must reach the file");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_reports_each_finalized_segment_in_order() {
        let (mut config, path) = test_setup("segments", None);
        config.rotation.enabled = true;
        config.rotation.split = None;
        config.rotation.max_file_size_bytes = Some(1);
        config.rotation.name_template = Some("{stem}_{index}.{ext}".to_string());
        let seen = Arc::new(Mutex::new(Vec::<SegmentFinalized>::new()));
        let callback_seen = seen.clone();
        let callback = Arc::new(move |segment: SegmentFinalized| {
            callback_seen.lock().expect("seen lock").push(segment);
            Ok(())
        });
        let writer =
            RotatingWriter::create_with_segment_callback(&config, Some(callback)).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        wt.write(message(0)).await.expect("write first segment");
        wt.write(message(1)).await.expect("write final segment");
        let outcome = finish_ok(wt).await;
        assert_eq!(outcome.segment_count, 2);

        let seen = seen.lock().expect("seen lock");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].index, 0);
        assert_eq!(seen[1].index, 1);
        assert!(seen.iter().all(|segment| segment.path.exists()));
        assert!(seen.iter().all(|segment| segment.size_bytes > 0));
        assert_eq!(seen[0].recording_id, seen[1].recording_id);

        for segment in seen.iter() {
            let _ = std::fs::remove_file(&segment.path);
        }
        let _ = std::fs::remove_file(path);
    }

    /// `max_messages` is enforced writer-side and exactly: the drain loop may
    /// race a few extra events into the queue after the cap; they must be
    /// ignored, and `cap_reached()` must flip for the drain loop to observe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cap_is_enforced_writer_side_exactly() {
        let (config, path) = test_setup("cap", Some(2));
        let writer = RotatingWriter::create(&config).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        // Send more than the cap — simulating the drain loop racing past it.
        for i in 0..6 {
            wt.write(message(i)).await.expect("write");
        }

        let outcome = finish_ok(wt).await;
        assert_eq!(
            outcome.accepted_messages, 2,
            "cap must bound the file exactly"
        );

        let bytes = std::fs::read(&path).expect("read mcap");
        let count = mcap::MessageStream::new(&bytes)
            .expect("stream")
            .map(|m| m.expect("message"))
            .count();
        assert_eq!(count, 2);
        let _ = std::fs::remove_file(path);
    }

    /// The accepted counter and cap flag are observable from the async side
    /// (the drain loop's stop condition + stats line read them live).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn counters_are_observable_while_running() {
        let (config, path) = test_setup("counters", Some(3));
        let writer = RotatingWriter::create(&config).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        for i in 0..3 {
            wt.write(message(i)).await.expect("write");
        }
        // The thread processes asynchronously; poll briefly for the flag.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !wt.cap_reached() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(wt.cap_reached(), "cap flag must flip once max_messages hit");
        assert_eq!(wt.accepted_messages(), 3);

        let _ = finish_ok(wt).await;
        let _ = std::fs::remove_file(path);
    }

    /// Seeding: ctx-snapshot messages written BEFORE the thread spawns (the
    /// run() sequence) must count toward the cap from the start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_spawn_snapshot_writes_seed_the_counters() {
        let (config, path) = test_setup("seed", Some(2));
        let mut writer = RotatingWriter::create(&config).expect("writer");
        writer.write_message(message(0)).expect("snapshot write");
        writer.write_message(message(1)).expect("snapshot write");

        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        assert_eq!(wt.accepted_messages(), 2);
        assert!(
            wt.cap_reached(),
            "a snapshot that already reached the cap must start capped"
        );
        // Further writes are ignored.
        wt.write(message(2)).await.expect("send ok, write ignored");
        let outcome = finish_ok(wt).await;
        assert_eq!(outcome.accepted_messages, 2);
        let _ = std::fs::remove_file(path);
    }

    /// A stuck-but-alive writer (hung I/O) with a full queue must hand the
    /// message back on `send_timeout` so the drain loop can poll stop signals
    /// between slices — and deliver it once the writer unblocks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_timeout_hands_message_back_when_writer_is_stuck() {
        use super::SendOutcome;
        let (config, path) = test_setup_with_capacity("stuck", None, Some(1));
        let writer = RotatingWriter::create(&config).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        // Park the thread (simulated hung I/O), then fill the single-slot
        // queue: once write() returns, TestBlock is IN the thread and the
        // queued message occupies the only slot.
        let release = wt.test_block().await;
        wt.write(message(0)).await.expect("fill the queue");

        let outcome = wt
            .send_timeout(Box::new(message(1)), std::time::Duration::from_millis(50))
            .await;
        let handed_back = match outcome {
            SendOutcome::Timeout(m) => m,
            SendOutcome::Sent => panic!("queue was full; send must not succeed"),
            SendOutcome::Stopped => panic!("writer is alive; must not report stopped"),
        };
        assert_eq!(
            handed_back.log_time_ns, 1,
            "the same message is handed back"
        );

        // Unblock the writer: the retried send now lands and everything drains.
        drop(release);
        match wt
            .send_timeout(handed_back, std::time::Duration::from_secs(10))
            .await
        {
            SendOutcome::Sent => {}
            _ => panic!("send must succeed once the writer unblocks"),
        }
        let outcome = finish_ok(wt).await;
        assert_eq!(
            outcome.accepted_messages, 2,
            "no message lost across the stall"
        );
        let _ = std::fs::remove_file(path);
    }

    /// A hung writer must not hang shutdown: `finish_with_deadline` returns a
    /// forced-stop error once the deadline elapses instead of joining forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_deadline_bounds_a_hung_writer() {
        let (config, path) = test_setup("hung-finish", None);
        let writer = RotatingWriter::create(&config).expect("writer");
        let wt = WriterThread::spawn(
            writer,
            &config,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        let release = wt.test_block().await; // park the thread; keep the barrier held
        let err = wt
            .finish_with_deadline(
                tokio::time::Instant::now() + std::time::Duration::from_millis(200),
            )
            .await
            .expect_err("a hung writer must force-stop at the deadline");
        assert!(
            err.to_string().contains("shutdown deadline"),
            "unexpected error: {err}"
        );
        drop(release); // let the leaked thread exit before the test ends
        let _ = std::fs::remove_file(path);
    }

    /// The schema_miss / dropped counters shared with the drain loop must land
    /// in the end-of-run metadata exactly as the inline writer wrote them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_counters_reach_end_metadata() {
        let (config, path) = test_setup("meta", None);
        let writer = RotatingWriter::create(&config).expect("writer");
        let schema_miss = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let wt = WriterThread::spawn(writer, &config, schema_miss.clone(), dropped.clone());

        wt.write(message(0)).await.expect("write");
        schema_miss.store(7, Ordering::Relaxed);
        dropped.store(3, Ordering::Relaxed);
        let _ = finish_ok(wt).await;

        let bytes = std::fs::read(&path).expect("read mcap");
        let mut end_meta = None;
        for record in mcap::read::LinearReader::new(&bytes).expect("reader") {
            if let mcap::records::Record::Metadata(m) = record.expect("record") {
                if m.name == "aorta.recorder.end" {
                    end_meta = Some(m.metadata.clone());
                }
            }
        }
        let end_meta = end_meta.expect("end metadata present");
        assert_eq!(
            end_meta.get("aorta.schema_miss_count").map(String::as_str),
            Some("7")
        );
        assert_eq!(
            end_meta.get("aorta.dropped_messages").map(String::as_str),
            Some("3")
        );
        let _ = std::fs::remove_file(path);
    }
}
