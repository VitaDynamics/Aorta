//! Reader adapter over `mcap::sans_io::LinearReader` (MCAP Player Proposal §6.7
//! items 1–5) and the §6.1 file-opening / crash-recovery rules.
//!
//! The high-level `MessageStream` cannot prevalidate chunk CRCs, cannot check
//! the data-section CRC and has no record-length limit (§2), so both
//! player passes drive `sans_io::LinearReader` directly. That reader, however,
//! keeps its parser state and physical offsets private, skips its own size
//! checks whenever a stored CRC is zero, and hands attacker-controlled lengths
//! to `insert()` before its limits apply. This adapter supplies exactly what is
//! missing:
//!
//! 1. **Physical framing tracker** — the adapter parses the outer record
//!    envelopes (opcode + length) itself, straight from the snapshot, always
//!    running *ahead* of the bytes it feeds the reader. That is what defines
//!    the §6.1 verified safe prefix and the torn-tail classification.
//! 2. **`ReadRequest` cap** — every request is checked against the remaining
//!    verified bytes and the `--max-chunk-bytes` cap *before* `insert()`.
//! 3. **CRC-independent size checks** — outer chunk length, compression-name
//!    length and the declared compressed/uncompressed sizes are validated from
//!    the chunk header whether or not a CRC is stored.
//! 4. **Index/summary skip** — message-index and summary records are
//!    length-validated by the framer and never parsed into containers; ordering
//!    comes from the record stream (§6.5).
//! 5. **CRC-zero recalculate-and-compare** — where a stored CRC is zero (both
//!    mcap's "do not compare" sentinel *and* a legitimate CRC32 value), the
//!    adapter performs the comparison mcap skips: a zero-CRC chunk is fully
//!    decompressed and hashed *before* any of its records may be yielded, and
//!    the data section is hashed across the whole walk. Calculated ≠ 0 is a
//!    hard error, exactly like a nonzero mismatch.

use mcap::records::{op, Record};
use mcap::sans_io::linear_reader::{LinearReadEvent, LinearReader, LinearReaderOptions};
use mcap::{McapError, MAGIC};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::{atomic::AtomicUsize, Arc};

/// `opcode` (1) + `length` (8).
const OPCODE_LEN_SIZE: u64 = 9;
/// `message_start_time` + `message_end_time` + `uncompressed_size` +
/// `uncompressed_crc` + compression-name length + `compressed_size`.
const MIN_CHUNK_HEADER_SIZE: u64 = 8 + 8 + 8 + 4 + 4 + 8;
/// Slack above `--max-chunk-bytes` for envelope/header reads (MCAP Player
/// Proposal §6.7: the cap is "`--max-chunk-bytes` + fixed header slack"). A
/// chunk header is 40 bytes plus a compression name; 64 KiB is generous and
/// constant.
const READ_REQUEST_SLACK: u64 = 64 * 1024;
/// Stand-in CRC used to make mcap *report* a checksum it would otherwise only
/// compare. Both zero-CRC paths (chunk, `DataEnd`) feed this value in place of
/// the stored zero: mcap then either reports `calculated` in its mismatch error
/// — which is the number MCAP Player Proposal §6.1 needs — or accepts, which
/// means the calculated value equals this sentinel and is therefore not zero.
const CRC_PROBE_SENTINEL: u32 = 0xFFFF_FFFF;
const ZERO_CRC_HASH_BUFFER_BYTES: usize = 64 * 1024;
/// Typed payload identifying the adapter's *own* deliberate stop signal.
///
/// The identity must be this private type, never `ErrorKind` + display text: a
/// public [`SnapshotSource`] is free to return an `io::Error` with the same
/// kind and message as a genuine failure, and that must surface as
/// [`ReadError::Io`] — only errors the adapter itself minted may classify as
/// a stop.
#[derive(Debug)]
struct StopRequested;

impl std::fmt::Display for StopRequested {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("aorta-play stop requested")
    }
}

impl std::error::Error for StopRequested {}

fn stop_requested_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, StopRequested)
}

fn is_stop_requested_error(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<StopRequested>().is_some())
}

// ---------------------------------------------------------------------------
// Snapshot source
// ---------------------------------------------------------------------------

/// Random-access byte source: the pass-0 private snapshot (MCAP Player Proposal
/// §6.1), or a `Vec<u8>` in tests. Random access is what lets the framing
/// tracker run ahead of the sequential feed without disturbing it.
pub trait SnapshotSource {
    fn size(&self) -> std::io::Result<u64>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize>;
}

impl SnapshotSource for std::fs::File {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }
}

impl<T: SnapshotSource + ?Sized> SnapshotSource for &T {
    fn size(&self) -> std::io::Result<u64> {
        (**self).size()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        (**self).read_at(offset, buf)
    }
}

impl SnapshotSource for [u8] {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.len());
        let n = buf.len().min(self.len() - start);
        buf[..n].copy_from_slice(&self[start..start + n]);
        Ok(n)
    }
}

impl SnapshotSource for Vec<u8> {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.as_slice().read_at(offset, buf)
    }
}

/// Fill `buf` from `offset`, returning how many bytes were available.
fn read_at_full<S: SnapshotSource + ?Sized>(
    source: &S,
    offset: u64,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match source.read_at(offset + filled as u64, &mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Structured reader failures (MCAP Player Proposal §6.7). The recoverable torn
/// tail is *not* an error — it is reported in [`WalkReport`].
#[derive(Debug)]
pub enum ReadError {
    /// The snapshot is not an MCAP file (missing/short start magic).
    Format(String),
    /// Corruption strictly inside the verified framing — hard error (MCAP
    /// Player Proposal §6.1).
    Corruption(String),
    /// A declared size or a `ReadRequest` exceeded a configured bound (MCAP
    /// Player Proposal §6.7).
    Bound(String),
    /// The snapshot could not be read.
    Io(std::io::Error),
    /// The record sink failed.
    Sink(anyhow::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(msg) => write!(f, "not an MCAP snapshot: {msg}"),
            Self::Corruption(msg) => write!(f, "corrupt MCAP snapshot: {msg}"),
            Self::Bound(msg) => write!(f, "MCAP bound violation: {msg}"),
            Self::Io(err) => write!(f, "cannot read the MCAP snapshot: {err}"),
            Self::Sink(err) => write!(f, "record sink failed: {err}"),
        }
    }
}

impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Sink(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ReadError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl ReadError {
    pub fn is_format(&self) -> bool {
        matches!(self, Self::Format(_))
    }

    pub fn is_corruption(&self) -> bool {
        matches!(self, Self::Corruption(_))
    }

    pub fn is_bound(&self) -> bool {
        matches!(self, Self::Bound(_))
    }
}

// ---------------------------------------------------------------------------
// Limits, events, sink
// ---------------------------------------------------------------------------

/// The MCAP Player Proposal §6.7 byte bounds the adapter enforces, straight
/// from the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderLimits {
    /// `--max-chunk-bytes`: declared chunk/record size cap, also wired to the
    /// reader's `record_length_limit`.
    pub max_chunk_bytes: u64,
    /// `--max-message-bytes`: rejected *before* any owned message copy.
    pub max_message_bytes: u64,
}

impl Default for ReaderLimits {
    fn default() -> Self {
        Self {
            max_chunk_bytes: crate::DEFAULT_MAX_CHUNK_BYTES,
            max_message_bytes: crate::DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}

impl ReaderLimits {
    /// Largest `ReadRequest` the adapter will honour (MCAP Player Proposal §6.7
    /// item 2).
    pub fn read_request_cap(&self) -> u64 {
        self.max_chunk_bytes.saturating_add(READ_REQUEST_SLACK)
    }

    fn record_length_limit(&self) -> usize {
        usize::try_from(self.max_chunk_bytes).unwrap_or(usize::MAX)
    }
}

/// A decoded record from the canonical record stream (MCAP Player Proposal
/// §6.5). Small fields are owned (they are allocated by the parse either way);
/// payloads stay borrowed so the bounded-memory contract holds — the pass-2
/// owned copy is the consumer's, and oversize payloads never reach it.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordEvent<'a> {
    Header {
        profile: String,
        library: String,
    },
    Schema {
        id: u16,
        name: String,
        encoding: String,
        data: &'a [u8],
    },
    Channel {
        id: u16,
        schema_id: u16,
        topic: String,
        message_encoding: String,
        metadata: BTreeMap<String, String>,
    },
    Message {
        channel_id: u16,
        sequence: u32,
        log_time: u64,
        publish_time: u64,
        data: &'a [u8],
    },
    Metadata {
        name: String,
        entries: BTreeMap<String, String>,
    },
}

/// Whether the walk continues after a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    /// Stop the walk (cancellation, or the consumer has what it needs).
    Stop,
}

/// Receiver of the canonical record stream.
pub trait RecordSink {
    fn on_record(&mut self, record: &RecordEvent<'_>) -> anyhow::Result<Flow>;
}

/// Result of one linear walk over a snapshot (MCAP Player Proposal §6.1, §5
/// summary).
///
/// When `stopped_by_sink` is set the walk ended early (cancellation), so the
/// framing walk was not completed and the tail-classification fields describe
/// only what had been framed — they are not the file's final verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalkReport {
    /// The snapshot does not end with `Footer` + end magic: opened the MCAP
    /// Player Proposal §6.1 `IgnoreEndMagic` way and replayed from its verified
    /// prefix.
    pub truncated: bool,
    /// End offset of the verified safe prefix — authoritative for pass 2.
    pub safe_prefix_end: u64,
    /// Tail chunks discarded whole because they were not fully framed — exact.
    pub discarded_tail_chunks: u64,
    /// Tail messages discarded; `None` = `unknown` (a torn chunk yields no
    /// messages to count and its index was never written, MCAP Player Proposal
    /// §2/§6.1).
    pub discarded_tail_messages: Option<u64>,
    /// Records handed to the sink.
    pub records: u64,
    /// Message records handed to the sink.
    pub messages: u64,
    /// The sink asked to stop before the end of the verified prefix.
    pub stopped_by_sink: bool,
}

// ---------------------------------------------------------------------------
// ReadRequest planning (MCAP Player Proposal §6.7 item 2)
// ---------------------------------------------------------------------------

/// What the adapter will do with a `ReadRequest`, decided *before* `insert()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadPlan {
    /// Hand the reader exactly this many bytes.
    Supply(usize),
    /// Nothing verified is left: signal EOF instead of allocating.
    Eof,
}

/// Validate a `ReadRequest` before any allocation happens (MCAP Player Proposal
/// §6.7 item 2).
///
/// A request above the cap is a hard bound violation. A request beyond the
/// verified bytes is *not*: that is exactly what an EOF-torn tail looks like
/// (§6.1), so the adapter supplies what it has — never allocating for bytes
/// that cannot exist — and lets the shortfall surface as EOF.
pub(crate) fn plan_read(need: usize, available: u64, cap: u64) -> Result<ReadPlan, ReadError> {
    if need == 0 {
        return Err(ReadError::Corruption(
            "reader requested zero bytes — refusing to spin".to_string(),
        ));
    }
    if need as u64 > cap {
        return Err(ReadError::Bound(format!(
            "reader requested {need} bytes, above the {cap}-byte read cap \
             (--max-chunk-bytes + header slack)"
        )));
    }
    if available == 0 {
        return Ok(ReadPlan::Eof);
    }
    Ok(ReadPlan::Supply((need as u64).min(available) as usize))
}

// ---------------------------------------------------------------------------
// Walker
// ---------------------------------------------------------------------------

/// One linear, verifying walk over a snapshot. Pass 1 and pass 2 each build
/// one over the same snapshot and get the identical record stream (MCAP Player
/// Proposal §6.5).
pub struct McapWalker<S: SnapshotSource> {
    source: S,
    snapshot_len: u64,
    limits: ReaderLimits,
    reader: LinearReader,

    // Framing tracker (MCAP Player Proposal §6.7 item 1). `frame_end` is the
    // verified safe prefix.
    frame_cursor: u64,
    frame_end: u64,
    framing_done: bool,
    clean_end: bool,
    torn: bool,
    torn_opcode: Option<u8>,

    // DataEnd bookkeeping for the MCAP Player Proposal §6.1 zero-CRC
    // data-section rule.
    data_end_offset: Option<u64>,
    data_end_zero_crc: bool,

    #[cfg(test)]
    last_zero_crc_peak: Option<VerifierPeak>,

    // Sequential feed.
    feed_offset: u64,
    feed_eof: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VerifierPeak {
    hash_buffer_bytes: usize,
    decoder_input_bytes: usize,
}

#[cfg(test)]
impl VerifierPeak {
    fn total_bytes(self) -> usize {
        self.hash_buffer_bytes + self.decoder_input_bytes
    }
}

impl<S: SnapshotSource> McapWalker<S> {
    /// Open a snapshot: verify the start magic and prepare the reader with the
    /// three MCAP Player Proposal §6.7 options. End-magic classification is the
    /// framer's job, so the reader itself always runs in `IgnoreEndMagic` mode
    /// (§6.1) — the framer is strictly stronger, it checks the footer *and* the
    /// magic *and* that nothing follows.
    pub fn open(source: S, limits: ReaderLimits) -> Result<Self, ReadError> {
        let snapshot_len = source.size()?;
        if snapshot_len < MAGIC.len() as u64 {
            return Err(ReadError::Format(format!(
                "{snapshot_len} bytes is shorter than the 8-byte start magic"
            )));
        }
        let mut magic = [0u8; 8];
        read_at_full(&source, 0, &mut magic)?;
        if magic != *MAGIC {
            return Err(ReadError::Format("bad start magic".to_string()));
        }

        let reader = LinearReader::new_with_options(
            LinearReaderOptions::default()
                .with_skip_end_magic(true)
                .with_prevalidate_chunk_crcs(true)
                .with_validate_data_section_crc(true)
                .with_record_length_limit(limits.record_length_limit()),
        );

        Ok(Self {
            source,
            snapshot_len,
            limits,
            reader,
            frame_cursor: MAGIC.len() as u64,
            frame_end: MAGIC.len() as u64,
            framing_done: false,
            clean_end: false,
            torn: false,
            torn_opcode: None,
            data_end_offset: None,
            data_end_zero_crc: false,
            #[cfg(test)]
            last_zero_crc_peak: None,
            feed_offset: 0,
            feed_eof: false,
        })
    }

    #[cfg(test)]
    fn last_zero_crc_peak(&self) -> Option<VerifierPeak> {
        self.last_zero_crc_peak
    }

    /// Walk the verified prefix, handing every replay-relevant record to
    /// `sink` in canonical record-stream order (MCAP Player Proposal §6.5).
    pub fn walk(&mut self, sink: &mut dyn RecordSink) -> Result<WalkReport, ReadError> {
        self.walk_inner(sink, None)
    }

    /// Walk while polling a managed pass stop flag before every adapter event.
    pub fn walk_with_stop(
        &mut self,
        sink: &mut dyn RecordSink,
        stop: &AtomicBool,
    ) -> Result<WalkReport, ReadError> {
        self.walk_inner(sink, Some(stop))
    }

    fn walk_inner(
        &mut self,
        sink: &mut dyn RecordSink,
        stop: Option<&AtomicBool>,
    ) -> Result<WalkReport, ReadError> {
        let limits = self.limits;
        let mut records = 0u64;
        let mut messages = 0u64;
        let mut stopped_by_sink = false;

        loop {
            if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
                stopped_by_sink = true;
                break;
            }
            // The reader's event borrows its internal buffer, so every arm is
            // reduced to an owned `Step` before the borrow ends and the
            // adapter touches the reader again.
            let step = match self.reader.next_event() {
                None => Step::Done,
                Some(Err(err)) => Step::Failed(err),
                Some(Ok(LinearReadEvent::ReadRequest(need))) => Step::Feed(need),
                Some(Ok(LinearReadEvent::Record { .. }))
                    if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) =>
                {
                    Step::Stopped
                }
                Some(Ok(LinearReadEvent::Record { opcode, data })) => {
                    if opcode == op::DATA_END {
                        // mcap emits `DataEnd` only after the data-section CRC
                        // comparison passes. Under the zero-CRC probe that
                        // means calculated == sentinel != stored 0 — a
                        // collision that must fail closed (MCAP Player Proposal
                        // §6.7 item 5).
                        if self.data_end_zero_crc {
                            return Err(ReadError::Corruption(format!(
                                "data section stores CRC 0 but calculates \
                                 {CRC_PROBE_SENTINEL:#010x} (probe-sentinel \
                                 collision; MCAP Player Proposal §6.1 recalculate-and-compare)"
                            )));
                        }
                        // The data section is complete; the summary section
                        // holds nothing the player consumes (MCAP Player
                        // Proposal §6.5, §6.7 item 4).
                        Step::Done
                    } else {
                        match decode(opcode, data, &limits)? {
                            None => Step::Continue,
                            Some(event) => {
                                records += 1;
                                if matches!(event, RecordEvent::Message { .. }) {
                                    messages += 1;
                                }
                                match sink.on_record(&event).map_err(ReadError::Sink)? {
                                    Flow::Continue => Step::Continue,
                                    Flow::Stop => Step::Stopped,
                                }
                            }
                        }
                    }
                }
            };

            match step {
                Step::Continue => {}
                Step::Feed(need) => match self.feed(need, stop)? {
                    FrameStep::Continue => {}
                    FrameStep::Stopped => {
                        stopped_by_sink = true;
                        break;
                    }
                },
                Step::Done => break,
                Step::Stopped => {
                    stopped_by_sink = true;
                    break;
                }
                Step::Failed(err) => {
                    self.accept_or_fail(err)?;
                    break;
                }
            }
        }

        if !stopped_by_sink {
            // Finish the framing walk so the tail classification is complete:
            // the summary section is length-validated and skipped, never fed to
            // the reader and never parsed into containers (MCAP Player Proposal
            // §6.7 item 4).
            while !self.framing_done {
                match self.advance_frame(stop)? {
                    FrameStep::Continue => {}
                    FrameStep::Stopped => {
                        stopped_by_sink = true;
                        break;
                    }
                }
            }
        }

        let (discarded_tail_chunks, discarded_tail_messages) = match (self.torn, self.torn_opcode) {
            (true, Some(op::CHUNK)) => (1, None),
            (true, Some(op::MESSAGE)) => (0, Some(1)),
            _ => (0, Some(0)),
        };

        Ok(WalkReport {
            truncated: !self.clean_end,
            safe_prefix_end: self.frame_end,
            discarded_tail_chunks,
            discarded_tail_messages,
            records,
            messages,
            stopped_by_sink,
        })
    }

    /// Classify a reader error: only an EOF-torn tail is recoverable (MCAP
    /// Player Proposal §6.1).
    fn accept_or_fail(&mut self, err: McapError) -> Result<(), ReadError> {
        match err {
            // The MCAP Player Proposal §6.1 zero-`DataEnd`-CRC comparison,
            // performed by feeding the sentinel in place of the stored zero.
            McapError::BadDataCrc { saved, calculated }
                if saved == CRC_PROBE_SENTINEL && self.data_end_zero_crc =>
            {
                if calculated == 0 {
                    Ok(())
                } else {
                    Err(ReadError::Corruption(format!(
                        "data section stores CRC 0 but calculates {calculated:#010x} \
                         (MCAP Player Proposal §6.1 recalculate-and-compare)"
                    )))
                }
            }
            // The tail is torn *and* every verified byte has been fed: this is
            // the recoverable case, and only this one.
            McapError::UnexpectedEof if !self.clean_end && self.feed_eof => Ok(()),
            other => Err(ReadError::Corruption(format!(
                "{other} (verified prefix ends at offset {})",
                self.frame_end
            ))),
        }
    }

    /// Supply bytes for one `ReadRequest`, keeping framing ahead of the feed.
    fn feed(&mut self, need: usize, stop: Option<&AtomicBool>) -> Result<FrameStep, ReadError> {
        while !self.framing_done && self.frame_end.saturating_sub(self.feed_offset) < need as u64 {
            if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
                return Ok(FrameStep::Stopped);
            }
            if self.advance_frame(stop)? == FrameStep::Stopped {
                return Ok(FrameStep::Stopped);
            }
        }
        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(FrameStep::Stopped);
        }
        let available = self.frame_end.saturating_sub(self.feed_offset);
        match plan_read(need, available, self.limits.read_request_cap())? {
            ReadPlan::Eof => {
                self.reader.notify_read(0);
                self.feed_eof = true;
            }
            ReadPlan::Supply(count) => {
                let offset = self.feed_offset;
                let buffer = self.reader.insert(count);
                let filled = read_at_full(&self.source, offset, &mut buffer[..count])?;
                if filled < count {
                    return Err(ReadError::Corruption(
                        "the snapshot shrank while it was being read".to_string(),
                    ));
                }
                if self.data_end_zero_crc {
                    let crc_at = self
                        .data_end_offset
                        .expect("a zero DataEnd CRC implies a framed DataEnd record")
                        + OPCODE_LEN_SIZE;
                    patch_sentinel(buffer, offset, crc_at);
                }
                self.reader.notify_read(count);
                self.feed_offset += count as u64;
            }
        }
        Ok(FrameStep::Continue)
    }

    /// Frame the next outer record: bounds, chunk-header checks and the
    /// zero-CRC chunk comparison all happen here, i.e. strictly *before* any of
    /// that record's bytes reach the reader.
    fn advance_frame(&mut self, stop: Option<&AtomicBool>) -> Result<FrameStep, ReadError> {
        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(FrameStep::Stopped);
        }
        if self.framing_done {
            return Ok(FrameStep::Continue);
        }
        let start = self.frame_cursor;
        if start >= self.snapshot_len {
            self.framing_done = true;
            return Ok(FrameStep::Continue);
        }

        let mut envelope = [0u8; OPCODE_LEN_SIZE as usize];
        let filled = read_at_full(&self.source, start, &mut envelope)?;
        if filled < envelope.len() {
            // Not even a full envelope: the tail is torn mid-record.
            self.framing_done = true;
            self.torn = true;
            self.torn_opcode = (filled >= 1).then_some(envelope[0]);
            return Ok(FrameStep::Continue);
        }

        let opcode = envelope[0];
        let len = u64::from_le_bytes(envelope[1..].try_into().expect("8-byte length"));
        // Declared-size check first: an absurd length is a bound violation, not
        // a torn tail, even when it happens to run past the snapshot end.
        if len > self.limits.max_chunk_bytes {
            return Err(ReadError::Bound(format!(
                "record with opcode {opcode:#04x} at offset {start} declares {len} bytes, \
                 above --max-chunk-bytes ({})",
                self.limits.max_chunk_bytes
            )));
        }
        let end = start
            .checked_add(OPCODE_LEN_SIZE)
            .and_then(|body| body.checked_add(len))
            .ok_or_else(|| {
                ReadError::Corruption(format!(
                    "record with opcode {opcode:#04x} at offset {start} overflows the offset space"
                ))
            })?;
        if end > self.snapshot_len {
            self.framing_done = true;
            self.torn = true;
            self.torn_opcode = Some(opcode);
            return Ok(FrameStep::Continue);
        }

        match opcode {
            op::CHUNK => {
                if self.frame_chunk(start, len, stop)? == FrameStep::Stopped {
                    return Ok(FrameStep::Stopped);
                }
            }
            op::DATA_END => {
                if len < 4 {
                    return Err(ReadError::Corruption(format!(
                        "DataEnd record at offset {start} is {len} bytes, need 4"
                    )));
                }
                let mut crc = [0u8; 4];
                read_at_full(&self.source, start + OPCODE_LEN_SIZE, &mut crc)?;
                self.data_end_offset = Some(start);
                self.data_end_zero_crc = u32::from_le_bytes(crc) == 0;
            }
            _ => {}
        }

        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(FrameStep::Stopped);
        }

        self.frame_cursor = end;
        self.frame_end = end;

        if opcode == op::FOOTER {
            if self.data_end_offset.is_none() {
                return Err(ReadError::Corruption(
                    "Footer appeared before a valid DataEnd record".to_string(),
                ));
            }
            // Immediately after a Footer, fewer than eight bytes are a torn
            // end magic; exactly eight valid bytes are clean; every other
            // shape is hard corruption (MCAP Player Proposal §6.1).
            let remaining = self.snapshot_len.checked_sub(end).ok_or_else(|| {
                ReadError::Corruption("Footer extends beyond the snapshot".to_string())
            })?;
            if remaining < MAGIC.len() as u64 {
                self.torn = true;
                self.torn_opcode = Some(op::FOOTER);
            } else if remaining == MAGIC.len() as u64 {
                let mut magic = [0u8; 8];
                read_at_full(&self.source, end, &mut magic)?;
                if magic == *MAGIC {
                    self.frame_cursor = self.snapshot_len;
                    self.frame_end = self.snapshot_len;
                    self.clean_end = true;
                } else {
                    return Err(ReadError::Corruption(
                        "Footer is followed by an invalid end magic".to_string(),
                    ));
                }
            } else {
                return Err(ReadError::Corruption(
                    "Footer is followed by bytes beyond its single end magic".to_string(),
                ));
            }
            self.framing_done = true;
        }
        Ok(FrameStep::Continue)
    }

    /// CRC-independent chunk-header validation (MCAP Player Proposal
    /// §6.7 item 3) plus the §6.1 zero-CRC comparison (§6.7 item 5).
    fn frame_chunk(
        &mut self,
        start: u64,
        len: u64,
        stop: Option<&AtomicBool>,
    ) -> Result<FrameStep, ReadError> {
        if len < MIN_CHUNK_HEADER_SIZE {
            return Err(ReadError::Corruption(format!(
                "chunk at offset {start} is {len} bytes, shorter than its {MIN_CHUNK_HEADER_SIZE}-byte header"
            )));
        }
        let body = start + OPCODE_LEN_SIZE;
        let mut header = [0u8; MIN_CHUNK_HEADER_SIZE as usize];
        read_at_full(&self.source, body, &mut header)?;

        let uncompressed_size = u64::from_le_bytes(header[16..24].try_into().expect("u64"));
        let crc = u32::from_le_bytes(header[24..28].try_into().expect("u32"));
        let compression_len =
            u64::from(u32::from_le_bytes(header[28..32].try_into().expect("u32")));
        let header_len = MIN_CHUNK_HEADER_SIZE + compression_len;
        if header_len > len {
            return Err(ReadError::Bound(format!(
                "chunk at offset {start} declares a {compression_len}-byte compression name \
                 that does not fit in its {len}-byte record"
            )));
        }
        if uncompressed_size > self.limits.max_chunk_bytes {
            return Err(ReadError::Bound(format!(
                "chunk at offset {start} declares {uncompressed_size} decompressed bytes, \
                 above --max-chunk-bytes ({})",
                self.limits.max_chunk_bytes
            )));
        }
        let mut compressed = [0u8; 8];
        read_at_full(&self.source, body + 32 + compression_len, &mut compressed)?;
        let compressed_size = u64::from_le_bytes(compressed);
        if compressed_size > self.limits.max_chunk_bytes {
            return Err(ReadError::Bound(format!(
                "chunk at offset {start} declares {compressed_size} compressed bytes, \
                 above --max-chunk-bytes ({})",
                self.limits.max_chunk_bytes
            )));
        }
        let compressed_end = header_len.checked_add(compressed_size).ok_or_else(|| {
            ReadError::Corruption(format!(
                "chunk at offset {start} overflows compressed-size arithmetic"
            ))
        })?;
        if compressed_end > len {
            return Err(ReadError::Corruption(format!(
                "chunk at offset {start} declares {compressed_size} compressed bytes that do not \
                 fit in its {len}-byte record"
            )));
        }

        if crc == 0 {
            let compression_len = usize::try_from(compression_len).map_err(|_| {
                ReadError::Bound(format!(
                    "chunk at offset {start} has an unrepresentable compression name length"
                ))
            })?;
            let mut compression_bytes = vec![0u8; compression_len];
            read_at_full(&self.source, body + 32, &mut compression_bytes)?;
            let compression = String::from_utf8(compression_bytes).map_err(|_| {
                ReadError::Corruption(format!(
                    "chunk at offset {start} has a non-UTF-8 compression name"
                ))
            })?;
            let records_offset = body + header_len;
            if self.verify_zero_crc_chunk(
                start,
                &compression,
                uncompressed_size,
                records_offset,
                compressed_size,
                stop,
            )? == FrameStep::Stopped
            {
                return Ok(FrameStep::Stopped);
            }
        }
        Ok(FrameStep::Continue)
    }

    fn verify_zero_crc_chunk(
        &mut self,
        start: u64,
        compression: &str,
        uncompressed_size: u64,
        records_offset: u64,
        compressed_size: u64,
        stop: Option<&AtomicBool>,
    ) -> Result<FrameStep, ReadError> {
        let mut hasher = crc32fast::Hasher::new();
        let decoded_size;
        #[cfg(test)]
        let hash_buffer_bytes;
        #[cfg(test)]
        let mut decoder_input_bytes = 0usize;
        #[cfg(test)]
        let max_read_capacity = Arc::new(AtomicUsize::new(0));
        match compression {
            "" => {
                let mut source = SnapshotReader {
                    source: &self.source,
                    offset: records_offset,
                    remaining: compressed_size,
                    stop,
                    #[cfg(test)]
                    max_read_capacity: max_read_capacity.clone(),
                };
                let Some(result) =
                    hash_decompressed(&mut source, &mut hasher, uncompressed_size, stop)?
                else {
                    return Ok(FrameStep::Stopped);
                };
                decoded_size = result.total;
                #[cfg(test)]
                {
                    hash_buffer_bytes = result.hash_buffer_bytes;
                }
            }
            "zstd" | "lz4" => {
                let source = SnapshotReader {
                    source: &self.source,
                    offset: records_offset,
                    remaining: compressed_size,
                    stop,
                    #[cfg(test)]
                    max_read_capacity: max_read_capacity.clone(),
                };
                match compression {
                    "zstd" => {
                        let input =
                            BufReader::with_capacity(zstd::zstd_safe::DCtx::in_size(), source);
                        #[cfg(test)]
                        {
                            decoder_input_bytes = input.capacity();
                        }
                        let mut decoder =
                            zstd::stream::read::Decoder::with_buffer(input).map_err(|err| {
                                ReadError::Corruption(format!(
                                    "cannot initialize zstd verification for chunk at offset {start}: {err}"
                                ))
                            })?;
                        let Some(result) =
                            hash_decompressed(&mut decoder, &mut hasher, uncompressed_size, stop)?
                        else {
                            return Ok(FrameStep::Stopped);
                        };
                        decoded_size = result.total;
                        #[cfg(test)]
                        {
                            hash_buffer_bytes = result.hash_buffer_bytes;
                        }
                    }
                    "lz4" => {
                        let mut decoder = lz4::Decoder::new(source).map_err(|err| {
                            ReadError::Corruption(format!(
                                "cannot initialize lz4 verification for chunk at offset {start}: {err}"
                            ))
                        })?;
                        let Some(result) =
                            hash_decompressed(&mut decoder, &mut hasher, uncompressed_size, stop)?
                        else {
                            return Ok(FrameStep::Stopped);
                        };
                        decoded_size = result.total;
                        #[cfg(test)]
                        {
                            hash_buffer_bytes = result.hash_buffer_bytes;
                        }
                        #[cfg(test)]
                        let (source, finish_result) = decoder.finish();
                        #[cfg(not(test))]
                        let (_, finish_result) = decoder.finish();
                        #[cfg(test)]
                        {
                            decoder_input_bytes = source.max_read_capacity.load(Ordering::SeqCst);
                        }
                        finish_result.map_err(|err| {
                            ReadError::Corruption(format!(
                                "lz4 verification for chunk at offset {start} did not finish: {err}"
                            ))
                        })?;
                    }
                    _ => unreachable!(),
                }
            }
            other => {
                return Err(ReadError::Corruption(format!(
                "cannot verify zero-CRC chunk at offset {start}: unsupported compression '{other}'"
                )))
            }
        }
        #[cfg(test)]
        {
            self.last_zero_crc_peak = Some(VerifierPeak {
                hash_buffer_bytes,
                decoder_input_bytes,
            });
        }
        if decoded_size != uncompressed_size {
            return Err(ReadError::Corruption(format!(
                "zero-CRC chunk at offset {start} decompresses to {decoded_size} bytes, expected {uncompressed_size}"
            )));
        }
        let calculated = hasher.finalize();
        if calculated != 0 {
            return Err(ReadError::Corruption(format!(
                "chunk at offset {start} stores CRC 0 but calculates {calculated:#010x} \
                 (MCAP Player Proposal §6.1 recalculate-and-compare)"
            )));
        }
        Ok(FrameStep::Continue)
    }
}

struct HashResult {
    total: u64,
    #[cfg(test)]
    hash_buffer_bytes: usize,
}

fn hash_decompressed<R: Read>(
    source: &mut R,
    hasher: &mut crc32fast::Hasher,
    expected: u64,
    stop: Option<&AtomicBool>,
) -> Result<Option<HashResult>, ReadError> {
    let mut buffer = [0u8; ZERO_CRC_HASH_BUFFER_BYTES];
    let mut total = 0u64;
    loop {
        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(None);
        }
        let read = match source.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if is_stop_requested_error(&error) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(None);
        }
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > expected {
            return Err(ReadError::Bound(format!(
                "zero-CRC decompression exceeded its declared {expected}-byte size"
            )));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Some(HashResult {
        total,
        #[cfg(test)]
        hash_buffer_bytes: buffer.len(),
    }))
}

struct SnapshotReader<'source, 'stop, S: SnapshotSource> {
    source: &'source S,
    stop: Option<&'stop AtomicBool>,
    offset: u64,
    remaining: u64,
    #[cfg(test)]
    max_read_capacity: Arc<AtomicUsize>,
}

impl<S: SnapshotSource> Read for SnapshotReader<'_, '_, S> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Err(stop_requested_error());
        }
        #[cfg(test)]
        self.max_read_capacity
            .fetch_max(buffer.len(), Ordering::SeqCst);
        if self.remaining == 0 {
            return Ok(0);
        }
        let count = self.remaining.min(buffer.len() as u64) as usize;
        let read = self.source.read_at(self.offset, &mut buffer[..count])?;
        if self.stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Err(stop_requested_error());
        }
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "zero-CRC chunk ended before its declared compressed size",
            ));
        }
        self.offset += read as u64;
        self.remaining -= read as u64;
        Ok(read)
    }
}

enum Step {
    Continue,
    Feed(usize),
    Done,
    Stopped,
    Failed(McapError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameStep {
    Continue,
    Stopped,
}

/// Overwrite the four CRC bytes at absolute offset `crc_at` with the sentinel,
/// for whatever part of them falls inside `buffer` (which starts at `at`).
fn patch_sentinel(buffer: &mut [u8], at: u64, crc_at: u64) {
    for i in 0..4u64 {
        let absolute = crc_at + i;
        if absolute >= at && absolute < at + buffer.len() as u64 {
            buffer[(absolute - at) as usize] = 0xFF;
        }
    }
}

/// Decode one raw record. Index, summary, attachment and private records are
/// skipped (`Ok(None)`) — never parsed into containers (MCAP Player Proposal
/// §6.7 item 4).
fn decode<'a>(
    opcode: u8,
    data: &'a [u8],
    limits: &ReaderLimits,
) -> Result<Option<RecordEvent<'a>>, ReadError> {
    match opcode {
        op::HEADER | op::SCHEMA | op::CHANNEL | op::MESSAGE | op::METADATA => {}
        _ => return Ok(None),
    }

    if opcode == op::MESSAGE {
        // Reject before the parse hands out a payload the consumer would copy
        // (MCAP Player Proposal §6.7: rejected *before* the owned-message
        // copy).
        let payload = (data.len() as u64).saturating_sub(2 + 4 + 8 + 8);
        if payload > limits.max_message_bytes {
            return Err(ReadError::Bound(format!(
                "message payload of {payload} bytes exceeds --max-message-bytes ({})",
                limits.max_message_bytes
            )));
        }
    }

    let record = mcap::parse_record(opcode, data)
        .map_err(|err| ReadError::Corruption(format!("cannot parse record: {err}")))?;
    let event = match record {
        Record::Header(header) => RecordEvent::Header {
            profile: header.profile,
            library: header.library,
        },
        Record::Schema { header, data } => RecordEvent::Schema {
            id: header.id,
            name: header.name,
            encoding: header.encoding,
            data: borrowed(data)?,
        },
        Record::Channel(channel) => RecordEvent::Channel {
            id: channel.id,
            schema_id: channel.schema_id,
            topic: channel.topic,
            message_encoding: channel.message_encoding,
            metadata: channel.metadata,
        },
        Record::Message { header, data } => RecordEvent::Message {
            channel_id: header.channel_id,
            sequence: header.sequence,
            log_time: header.log_time,
            publish_time: header.publish_time,
            data: borrowed(data)?,
        },
        Record::Metadata(metadata) => RecordEvent::Metadata {
            name: metadata.name,
            entries: metadata.metadata,
        },
        _ => return Ok(None),
    };
    Ok(Some(event))
}

/// `parse_record` slices payloads straight out of the buffer it was given, so
/// an owned payload would mean the reader changed its contract.
fn borrowed(data: Cow<'_, [u8]>) -> Result<&[u8], ReadError> {
    match data {
        Cow::Borrowed(bytes) => Ok(bytes),
        Cow::Owned(_) => Err(ReadError::Corruption(
            "the MCAP parser returned an owned payload where a borrow was expected".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{self, FixtureBuilder, FixtureOptions};
    use mcap::Compression;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    // -- test sink ---------------------------------------------------------

    #[derive(Debug, Default)]
    struct Collector {
        header: Option<(String, String)>,
        schemas: Vec<(u16, String, String, Vec<u8>)>,
        channels: Vec<(u16, u16, String, String)>,
        messages: Vec<(u16, u64, Vec<u8>)>,
        metadata: Vec<(String, BTreeMap<String, String>)>,
        stop_after_records: Option<u64>,
        seen: u64,
    }

    impl Collector {
        fn log_times(&self) -> Vec<u64> {
            self.messages.iter().map(|(_, t, _)| *t).collect()
        }

        fn payloads(&self) -> Vec<Vec<u8>> {
            self.messages.iter().map(|(_, _, d)| d.clone()).collect()
        }
    }

    impl RecordSink for Collector {
        fn on_record(&mut self, record: &RecordEvent<'_>) -> anyhow::Result<Flow> {
            self.seen += 1;
            match record {
                RecordEvent::Header { profile, library } => {
                    self.header = Some((profile.clone(), library.clone()));
                }
                RecordEvent::Schema {
                    id,
                    name,
                    encoding,
                    data,
                } => self
                    .schemas
                    .push((*id, name.clone(), encoding.clone(), data.to_vec())),
                RecordEvent::Channel {
                    id,
                    schema_id,
                    topic,
                    message_encoding,
                    ..
                } => self
                    .channels
                    .push((*id, *schema_id, topic.clone(), message_encoding.clone())),
                RecordEvent::Message {
                    channel_id,
                    log_time,
                    data,
                    ..
                } => self.messages.push((*channel_id, *log_time, data.to_vec())),
                RecordEvent::Metadata { name, entries } => {
                    self.metadata.push((name.clone(), entries.clone()))
                }
            }
            if self.stop_after_records == Some(self.seen) {
                return Ok(Flow::Stop);
            }
            Ok(Flow::Continue)
        }
    }

    fn walk(bytes: &[u8]) -> Result<(WalkReport, Collector), ReadError> {
        walk_with(bytes, ReaderLimits::default(), Collector::default())
    }

    fn walk_with(
        bytes: &[u8],
        limits: ReaderLimits,
        mut collector: Collector,
    ) -> Result<(WalkReport, Collector), ReadError> {
        let mut walker = McapWalker::open(bytes.to_vec(), limits)?;
        let report = walker.walk(&mut collector)?;
        Ok((report, collector))
    }

    struct DelayedSource {
        bytes: Arc<Vec<u8>>,
        delay_after: u64,
        entered: Arc<AtomicBool>,
        delay: Duration,
    }

    impl SnapshotSource for DelayedSource {
        fn size(&self) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        fn read_at(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<usize> {
            if offset >= self.delay_after {
                self.entered.store(true, Ordering::SeqCst);
                thread::sleep(self.delay);
            }
            self.bytes.as_slice().read_at(offset, buffer)
        }
    }

    fn two_chunk_file(options: FixtureOptions) -> Vec<u8> {
        // Non-monotone `log_time` across the chunk boundary (MCAP Player
        // Proposal §6.5's required fixture): the stream order, not the
        // timestamps, is canonical.
        FixtureBuilder::new(options)
            .typed_channel("aorta/robot/pub/sensor/imu", "aorta.sensor.Imu", b"bfbs")
            .message(0, 100, b"m0")
            .message(0, 200, b"m1")
            .flush_after(1)
            .message(0, 150, b"m2")
            .message(0, 90, b"m3")
            .build()
    }

    // -- ReadRequest cap (MCAP Player Proposal §6.7 item 2) ----------------

    #[test]
    fn plan_read_rejects_a_request_above_the_cap_before_allocating() {
        let err = plan_read(4097, 1_000_000, 4096).unwrap_err();
        assert!(err.is_bound(), "got: {err}");
        assert!(err.to_string().contains("4097"), "got: {err}");
    }

    #[test]
    fn plan_read_never_allocates_beyond_the_remaining_snapshot() {
        // The torn-tail shape: the reader wants more than exists. The adapter
        // hands over what it has (never `need`) and lets EOF surface.
        assert_eq!(plan_read(4096, 10, 1 << 20).unwrap(), ReadPlan::Supply(10));
        assert_eq!(plan_read(8, 10, 1 << 20).unwrap(), ReadPlan::Supply(8));
    }

    #[test]
    fn plan_read_signals_eof_instead_of_allocating_when_nothing_is_verified() {
        assert_eq!(plan_read(9, 0, 1 << 20).unwrap(), ReadPlan::Eof);
        assert!(plan_read(0, 10, 1 << 20).unwrap_err().is_corruption());
    }

    // -- happy paths -------------------------------------------------------

    #[test]
    fn walks_an_unchunked_file_in_stream_order() {
        let bytes = fixture::simple_file(FixtureOptions::unchunked());
        let (report, collector) = walk(&bytes).expect("clean walk");

        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert_eq!(
            collector.payloads(),
            vec![
                b"payload-0".to_vec(),
                b"payload-1".to_vec(),
                b"payload-2".to_vec()
            ]
        );
        assert_eq!(
            collector.header.as_ref().map(|(p, _)| p.as_str()),
            Some("aorta")
        );
        assert_eq!(collector.schemas.len(), 1);
        assert_eq!(collector.schemas[0].3, b"bfbs-imu".to_vec());
        assert_eq!(collector.channels.len(), 1);
        assert_eq!(collector.channels[0].2, "aorta/robot/pub/sensor/imu");
        assert_eq!(collector.metadata.len(), 1);
        assert_eq!(collector.metadata[0].0, "aorta.recorder.start");
        assert!(!report.truncated);
        assert_eq!(report.safe_prefix_end, bytes.len() as u64);
        assert_eq!(report.messages, 3);
        assert_eq!(report.discarded_tail_chunks, 0);
        assert_eq!(report.discarded_tail_messages, Some(0));
    }

    #[test]
    fn walks_chunked_files_of_every_compression() {
        for options in [
            FixtureOptions::default(),
            FixtureOptions::compressed(Compression::Zstd),
            FixtureOptions::compressed(Compression::Lz4),
        ] {
            let bytes = fixture::simple_file(options.clone());
            let (report, collector) = walk(&bytes).expect("clean walk");
            assert_eq!(collector.log_times(), vec![100, 200, 300], "{options:?}");
            assert_eq!(collector.schemas.len(), 1, "{options:?}");
            assert_eq!(collector.channels.len(), 1, "{options:?}");
            assert!(!report.truncated, "{options:?}");
            assert_eq!(report.safe_prefix_end, bytes.len() as u64, "{options:?}");
        }
    }

    #[test]
    fn walks_a_file_mixing_typed_and_raw_channels() {
        let bytes = FixtureBuilder::new(FixtureOptions::default())
            .typed_channel("aorta/robot/pub/sensor/imu", "aorta.sensor.Imu", b"bfbs")
            .raw_channel("aorta/robot/pub/sensor/blob")
            .message(0, 10, b"typed")
            .message(1, 20, b"raw")
            .build();

        let (_, collector) = walk(&bytes).expect("clean walk");
        assert_eq!(collector.channels.len(), 2);
        assert_eq!(collector.channels[0].3, "flatbuffer");
        assert_eq!(collector.channels[1].3, "aorta_raw_bytes");
        assert_eq!(collector.channels[1].1, 0, "raw channels carry schema_id 0");
        assert_eq!(collector.schemas.len(), 1);
        assert_eq!(
            collector.payloads(),
            vec![b"typed".to_vec(), b"raw".to_vec()]
        );
    }

    #[test]
    fn walks_a_summary_less_file() {
        let bytes = fixture::simple_file(FixtureOptions {
            summary: false,
            ..FixtureOptions::default()
        });
        let (report, collector) = walk(&bytes).expect("clean walk");
        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert!(!report.truncated);
    }

    #[test]
    fn walks_a_minimal_file_with_no_channels() {
        let bytes = FixtureBuilder::new(FixtureOptions::default()).build();
        let (report, collector) = walk(&bytes).expect("clean walk");
        assert_eq!(report.messages, 0);
        assert!(collector.messages.is_empty());
        assert!(!report.truncated);
    }

    #[test]
    fn stops_when_the_sink_asks_to_stop() {
        let bytes = fixture::simple_file(FixtureOptions::default());
        let collector = Collector {
            stop_after_records: Some(4),
            ..Collector::default()
        };
        let (report, collector) =
            walk_with(&bytes, ReaderLimits::default(), collector).expect("walk");
        assert!(report.stopped_by_sink);
        assert_eq!(collector.seen, 4);
        assert!(collector.messages.len() < 3);
    }

    // -- format / opening --------------------------------------------------

    /// `McapWalker` holds a `LinearReader`, which is not `Debug`, so opening
    /// failures are unwrapped by hand.
    fn open_err(bytes: Vec<u8>) -> ReadError {
        match McapWalker::open(bytes, ReaderLimits::default()) {
            Err(err) => err,
            Ok(_) => panic!("expected the open to fail"),
        }
    }

    #[test]
    fn rejects_an_empty_snapshot() {
        let err = open_err(Vec::new());
        assert!(err.is_format(), "got: {err}");
    }

    #[test]
    fn rejects_a_snapshot_without_the_start_magic() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        bytes[3] ^= 0xFF;
        let err = open_err(bytes);
        assert!(err.is_format(), "got: {err}");
    }

    // -- crash recovery (MCAP Player Proposal §6.1) ------------------------

    #[test]
    fn a_missing_end_magic_is_recoverable_and_flagged_truncated() {
        let bytes = fixture::strip_end_magic(&fixture::simple_file(FixtureOptions::default()));
        let (report, collector) = walk(&bytes).expect("recoverable");
        assert!(report.truncated);
        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert_eq!(report.discarded_tail_chunks, 0);
        assert_eq!(report.discarded_tail_messages, Some(0));
    }

    #[test]
    fn a_missing_footer_is_recoverable_and_flagged_truncated() {
        let bytes = fixture::strip_footer(&fixture::simple_file(FixtureOptions::default()));
        let (report, collector) = walk(&bytes).expect("recoverable");
        assert!(report.truncated);
        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert_eq!(report.discarded_tail_messages, Some(0));
    }

    #[test]
    fn a_missing_data_end_before_clean_footer_is_hard_error() {
        let bytes = fixture::simple_file(FixtureOptions {
            data_section_crc: false,
            summary: false,
            ..FixtureOptions::default()
        });
        let data_end = fixture::find_record(&bytes, op::DATA_END);
        let mut missing = fixture::remove_span(&bytes, data_end);
        let footer = fixture::find_record(&missing, op::FOOTER);
        let crc = fixture::crc32(&missing[..footer.body() + 16]);
        fixture::put_u32(&mut missing, footer.body() + 16, crc);
        let err = walk(&missing).expect_err("a clean footer needs DataEnd");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn complete_bytes_after_footer_are_hard_error() {
        let bytes = fixture::simple_file(FixtureOptions::default());
        let footer = fixture::find_record(&bytes, op::FOOTER);
        let with_extra = fixture::splice(
            &bytes,
            footer.end(),
            &fixture::padding_record([0x11, 0x22, 0x33, 0x44]),
        );
        let err = walk(&with_extra).expect_err("complete bytes after Footer are corruption");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn complete_record_after_valid_end_magic_is_hard_error() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        bytes.extend_from_slice(&fixture::padding_record([0x51, 0x52, 0x53, 0x54]));
        let err = walk(&bytes).expect_err("bytes after the valid end magic are corruption");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn full_invalid_end_magic_is_hard_error() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        let start = bytes.len() - mcap::MAGIC.len();
        bytes[start..].fill(0xA5);
        let err = walk(&bytes).expect_err("a full invalid end magic is corruption");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn stopping_during_post_data_end_framing_joins_the_worker() {
        let base = fixture::simple_file(FixtureOptions::default());
        let data_end = fixture::find_record(&base, op::DATA_END);
        let footer = fixture::find_record(&base, op::FOOTER);
        let mut tail = Vec::new();
        for index in 0..1_000u32 {
            tail.extend_from_slice(&fixture::padding_record(index.to_le_bytes()));
        }
        let bytes = fixture::splice(&base, footer.offset, &tail);
        let entered = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker_entered = entered.clone();
        let source = DelayedSource {
            bytes: Arc::new(bytes),
            delay_after: (data_end.end()) as u64,
            entered: worker_entered,
            delay: Duration::from_millis(1),
        };
        let worker = thread::spawn(move || {
            let mut walker = McapWalker::open(source, ReaderLimits::default()).expect("open");
            walker.walk_with_stop(&mut Collector::default(), &worker_stop)
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            entered.load(Ordering::SeqCst),
            "post-DataEnd framing was not entered"
        );
        stop.store(true, Ordering::SeqCst);
        let joined_at = Instant::now();
        let report = worker
            .join()
            .expect("reader worker must not panic")
            .expect("walk");
        assert!(
            joined_at.elapsed() < Duration::from_millis(250),
            "worker did not stop inside the bounded framing interval"
        );
        assert!(report.stopped_by_sink);
    }

    #[test]
    fn stopping_during_zero_crc_verification_joins_the_worker() {
        let payload = vec![0x5A; 8 * 1024 * 1024];
        let base = FixtureBuilder::new(FixtureOptions {
            chunk_crcs: false,
            ..FixtureOptions::default()
        })
        .raw_channel("aorta/robot/pub/blob")
        .message(0, 100, &payload)
        .build();
        let bytes = fixture::make_chunk_crc_calculate_zero(&base);
        let chunk = fixture::first_chunk(&bytes);
        let entered = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker_entered = entered.clone();
        let source = DelayedSource {
            bytes: Arc::new(bytes),
            delay_after: chunk.records_offset as u64,
            entered: worker_entered,
            delay: Duration::from_millis(3),
        };
        let worker = thread::spawn(move || {
            let mut walker = McapWalker::open(source, ReaderLimits::default()).expect("open");
            walker.walk_with_stop(&mut Collector::default(), &worker_stop)
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            entered.load(Ordering::SeqCst),
            "zero-CRC verification was not entered"
        );
        stop.store(true, Ordering::SeqCst);
        let joined_at = Instant::now();
        let report = worker
            .join()
            .expect("reader worker must not panic")
            .expect("walk");
        assert!(
            joined_at.elapsed() < Duration::from_millis(250),
            "worker did not stop inside the bounded verification interval"
        );
        assert!(report.stopped_by_sink);
    }

    #[test]
    fn a_torn_unchunked_record_stops_at_the_verified_prefix() {
        let bytes = fixture::simple_file(FixtureOptions::unchunked());
        let last = *fixture::top_level_records(&bytes)
            .iter()
            .rfind(|span| span.opcode == op::MESSAGE)
            .expect("a message record");
        let torn = fixture::truncate_at(&bytes, last.offset + 12);

        let (report, collector) = walk(&torn).expect("torn tails are recoverable");
        assert!(report.truncated);
        assert_eq!(collector.log_times(), vec![100, 200]);
        assert_eq!(report.safe_prefix_end, last.offset as u64);
        assert_eq!(report.discarded_tail_chunks, 0);
        assert_eq!(report.discarded_tail_messages, Some(1));
    }

    #[test]
    fn a_torn_chunk_yields_none_of_its_messages_and_reports_unknown() {
        for options in [
            FixtureOptions::default(),
            FixtureOptions::compressed(Compression::Zstd),
            FixtureOptions::compressed(Compression::Lz4),
        ] {
            let bytes = two_chunk_file(options.clone());
            let chunks: Vec<_> = fixture::top_level_records(&bytes)
                .into_iter()
                .filter(|span| span.opcode == op::CHUNK)
                .collect();
            assert_eq!(chunks.len(), 2, "fixture must have two chunks: {options:?}");
            let second = fixture::chunk_fields(&bytes, chunks[1]);
            let torn = fixture::truncate_at(&bytes, second.records_offset + 4);

            let (report, collector) = walk(&torn).expect("torn tails are recoverable");
            assert!(report.truncated, "{options:?}");
            assert_eq!(collector.log_times(), vec![100, 200], "{options:?}");
            assert_eq!(
                report.safe_prefix_end, chunks[1].offset as u64,
                "{options:?}"
            );
            assert_eq!(report.discarded_tail_chunks, 1, "{options:?}");
            assert_eq!(report.discarded_tail_messages, None, "{options:?}");
        }
    }

    // -- corruption (MCAP Player Proposal §6.1, §7 item 3) -----------------

    #[test]
    fn a_fully_framed_chunk_with_a_bad_nonzero_crc_is_a_hard_error() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        let chunk = fixture::first_chunk(&bytes);
        assert_ne!(chunk.crc, 0, "fixture must store a real CRC");
        fixture::flip_byte(&mut bytes, chunk.records_offset + 2);
        fixture::fix_data_section_crc(&mut bytes);

        let err = walk(&bytes).expect_err("mid-file corruption is a hard error");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn a_bad_data_section_crc_is_a_hard_error() {
        let mut bytes = fixture::simple_file(FixtureOptions::unchunked());
        fixture::set_data_section_crc(&mut bytes, 0x1234_5678);
        let err = walk(&bytes).expect_err("a bad data-section CRC is a hard error");
        assert!(err.is_corruption(), "got: {err}");
    }

    // -- CRC-zero matrix (stored × calculated), MCAP Player Proposal §6.1 ---

    /// A source whose reads fail with an error byte-identical to the stop
    /// marker's kind + text, while the stop flag is *not* set.
    struct ForgedInterruptSource {
        bytes: Arc<Vec<u8>>,
        fail_after: u64,
    }

    impl SnapshotSource for ForgedInterruptSource {
        fn size(&self) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        fn read_at(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<usize> {
            if offset >= self.fail_after {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "aorta-play stop requested",
                ));
            }
            self.bytes.as_slice().read_at(offset, buffer)
        }
    }

    #[test]
    fn a_forged_interrupt_io_error_is_not_mistaken_for_stop() {
        // The deliberate stop marker must be a typed
        // identity, not kind + display text — a public `SnapshotSource` is
        // free to produce this exact pair as a genuine I/O failure, and the
        // walk must surface it as `ReadError::Io`, never as a successful
        // stopped walk.
        let bytes = fixture::make_chunk_crc_calculate_zero(&fixture::simple_file(
            FixtureOptions::default(),
        ));
        let chunk = fixture::first_chunk(&bytes);
        assert_eq!(chunk.crc, 0, "fixture must exercise the zero-CRC verifier");
        let fail_after = chunk.records_offset as u64;
        let source = ForgedInterruptSource {
            bytes: Arc::new(bytes),
            fail_after,
        };
        let mut walker = McapWalker::open(&source, ReaderLimits::default())
            .expect("open must precede the fault");
        let mut sink = Collector::default();
        let err = walker
            .walk(&mut sink)
            .expect_err("a genuine I/O failure must not be classified as stop");
        assert!(
            matches!(err, ReadError::Io(_)),
            "expected ReadError::Io, got: {err}"
        );
    }

    #[test]
    fn a_chunk_storing_zero_that_calculates_zero_is_accepted() {
        let bytes = fixture::make_chunk_crc_calculate_zero(&fixture::simple_file(
            FixtureOptions::default(),
        ));
        let chunk = fixture::first_chunk(&bytes);
        assert_eq!(chunk.crc, 0);
        let (report, collector) = walk(&bytes).expect("calculated zero must be accepted");
        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert!(!report.truncated);
    }

    #[test]
    fn a_chunk_zero_crc_probe_sentinel_collision_is_rejected() {
        let bytes = fixture::make_chunk_crc_calculate(
            &fixture::simple_file(FixtureOptions::default()),
            CRC_PROBE_SENTINEL,
        );
        let chunk = fixture::first_chunk(&bytes);
        assert_eq!(chunk.crc, 0);
        let records =
            &bytes[chunk.records_offset..chunk.records_offset + chunk.compressed_size as usize];
        assert_eq!(fixture::crc32(records), CRC_PROBE_SENTINEL);

        let err = walk(&bytes).expect_err("chunk sentinel collision must be rejected");
        assert!(err.is_corruption(), "got: {err}");
        assert!(err.to_string().contains("calculates"), "got: {err}");
    }

    #[test]
    fn a_chunk_storing_zero_that_calculates_nonzero_is_rejected() {
        let bytes = fixture::simple_file(FixtureOptions {
            chunk_crcs: false,
            ..FixtureOptions::default()
        });
        let chunk = fixture::first_chunk(&bytes);
        assert_eq!(chunk.crc, 0, "fixture must store the zero sentinel");
        let records =
            &bytes[chunk.records_offset..chunk.records_offset + chunk.compressed_size as usize];
        assert_ne!(fixture::crc32(records), 0, "fixture must calculate nonzero");

        let err = walk(&bytes).expect_err("calculated nonzero must be rejected");
        assert!(err.is_corruption(), "got: {err}");
        assert!(err.to_string().contains("calculates"), "got: {err}");
    }

    fn near_limit_incompressible_payload(len: usize) -> Vec<u8> {
        let mut state = 0x9E37_79B9u32;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect()
    }

    #[test]
    fn zero_crc_verifier_peak_is_measured_from_production_buffers() {
        const MAX_CHUNK_BYTES: u64 = 2 * 1024 * 1024;
        let payload = near_limit_incompressible_payload(1_750_000);
        for (compression, expected_decoder_input) in [
            (Compression::Zstd, zstd::zstd_safe::DCtx::in_size()),
            (Compression::Lz4, 32 * 1024),
        ] {
            let bytes = FixtureBuilder::new(FixtureOptions {
                compression: Some(compression),
                chunk_crcs: false,
                summary: false,
                ..FixtureOptions::default()
            })
            .raw_channel("aorta/robot/pub/blob")
            .message(0, 100, &payload)
            .build();
            let mut walker = McapWalker::open(
                bytes,
                ReaderLimits {
                    max_chunk_bytes: MAX_CHUNK_BYTES,
                    max_message_bytes: MAX_CHUNK_BYTES,
                },
            )
            .expect("open");
            let _ = walker
                .walk(&mut Collector::default())
                .expect_err("stored zero with a nonzero calculated CRC must fail");
            let peak = walker
                .last_zero_crc_peak()
                .expect("production verifier must report its live peak");
            assert_eq!(peak.hash_buffer_bytes, ZERO_CRC_HASH_BUFFER_BYTES);
            assert_eq!(
                peak.decoder_input_bytes, expected_decoder_input,
                "decoder input capacity for {compression:?}"
            );
            assert_eq!(
                peak.total_bytes(),
                ZERO_CRC_HASH_BUFFER_BYTES + expected_decoder_input
            );
        }
    }

    struct ByteAtATimeSource {
        bytes: Arc<Vec<u8>>,
        start: u64,
        entered: Arc<AtomicBool>,
        delay: Duration,
    }

    fn prepend_empty_compressed_frames(bytes: Vec<u8>, compression: Compression) -> Vec<u8> {
        const EMPTY_FRAME_COUNT: usize = 256;
        let chunk = fixture::first_chunk(&bytes);
        let original = bytes
            [chunk.records_offset..chunk.records_offset + chunk.compressed_size as usize]
            .to_vec();
        let mut prefix = Vec::new();
        match compression {
            Compression::Zstd => {
                let empty = zstd::stream::encode_all(std::io::empty(), 0).expect("zstd frame");
                for _ in 0..EMPTY_FRAME_COUNT {
                    prefix.extend_from_slice(&empty);
                }
            }
            Compression::Lz4 => {
                const SKIPPABLE_BYTES: usize = 2 * 1024;
                prefix.extend_from_slice(&0x184D2A50u32.to_le_bytes());
                prefix.extend_from_slice(&(SKIPPABLE_BYTES as u32).to_le_bytes());
                prefix.resize(prefix.len() + SKIPPABLE_BYTES, 0xA5);
            }
        }
        let mut payload = Vec::with_capacity(prefix.len() + original.len());
        payload.extend_from_slice(&prefix);
        payload.extend_from_slice(&original);
        let delta = payload.len() - original.len();
        let mut bytes = bytes;
        fixture::put_u64(
            &mut bytes,
            chunk.record.offset + 1,
            chunk.record.len + delta as u64,
        );
        fixture::put_u64(
            &mut bytes,
            chunk.compressed_size_offset,
            payload.len() as u64,
        );
        bytes.splice(
            chunk.records_offset..chunk.records_offset + original.len(),
            payload,
        );
        bytes
    }

    impl SnapshotSource for ByteAtATimeSource {
        fn size(&self) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        fn read_at(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<usize> {
            if offset >= self.start {
                self.entered.store(true, Ordering::SeqCst);
                thread::sleep(self.delay);
                let count = buffer.len().min(1);
                return self.bytes.as_slice().read_at(offset, &mut buffer[..count]);
            }
            self.bytes.as_slice().read_at(offset, buffer)
        }
    }

    fn stopping_during_compressed_zero_crc_verification_joins_worker(compression: Compression) {
        let payload = near_limit_incompressible_payload(1_500_000);
        let bytes = FixtureBuilder::new(FixtureOptions {
            compression: Some(compression),
            chunk_crcs: false,
            ..FixtureOptions::default()
        })
        .raw_channel("aorta/robot/pub/blob")
        .message(0, 100, &payload)
        .build();
        let bytes = prepend_empty_compressed_frames(bytes, compression);
        let chunk = fixture::first_chunk(&bytes);
        let entered = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let source = ByteAtATimeSource {
            bytes: Arc::new(bytes),
            start: chunk.records_offset as u64,
            entered: entered.clone(),
            delay: Duration::from_millis(1),
        };
        let worker = thread::spawn(move || {
            let mut walker = McapWalker::open(source, ReaderLimits::default()).expect("open");
            walker.walk_with_stop(&mut Collector::default(), &worker_stop)
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !entered.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            entered.load(Ordering::SeqCst),
            "compressed decoding was not entered"
        );
        thread::sleep(Duration::from_millis(10));
        stop.store(true, Ordering::SeqCst);
        let joined_at = Instant::now();
        let report = worker
            .join()
            .expect("reader worker must not panic")
            .expect("stop must be reported as a walk result");
        assert!(
            joined_at.elapsed() < Duration::from_millis(250),
            "{compression:?} worker did not stop inside the bounded decoder interval"
        );
        assert!(report.stopped_by_sink);
    }

    #[test]
    fn stopping_during_zstd_zero_crc_verification_joins_the_worker() {
        stopping_during_compressed_zero_crc_verification_joins_worker(Compression::Zstd);
    }

    #[test]
    fn stopping_during_lz4_zero_crc_verification_joins_the_worker() {
        stopping_during_compressed_zero_crc_verification_joins_worker(Compression::Lz4);
    }

    #[test]
    fn near_u64_compressed_size_is_rejected_without_overflow() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        let chunk = fixture::first_chunk(&bytes);
        fixture::put_u64(&mut bytes, chunk.compressed_size_offset, u64::MAX);
        let limits = ReaderLimits {
            max_chunk_bytes: u64::MAX,
            ..ReaderLimits::default()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            walk_with(&bytes, limits, Collector::default())
        }));
        assert!(result.is_ok(), "declared size arithmetic must not panic");
        let err = result
            .unwrap()
            .expect_err("the near-u64 record must be rejected");
        assert!(err.is_corruption() || err.is_bound(), "got: {err}");
    }

    #[test]
    fn corrupting_the_body_of_a_calculated_zero_chunk_is_rejected() {
        let mut bytes = fixture::make_chunk_crc_calculate_zero(&fixture::simple_file(
            FixtureOptions::default(),
        ));
        let chunk = fixture::first_chunk(&bytes);
        fixture::flip_byte(&mut bytes, chunk.records_offset + 3);
        fixture::fix_data_section_crc(&mut bytes);

        let err = walk(&bytes).expect_err("a corrupt body must be rejected");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn patching_a_nonzero_chunk_crc_to_zero_is_rejected() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        let chunk = fixture::first_chunk(&bytes);
        assert_ne!(chunk.crc, 0);
        fixture::put_u32(&mut bytes, chunk.crc_offset, 0);
        fixture::fix_data_section_crc(&mut bytes);

        let err = walk(&bytes).expect_err("a patched zero must not disable the check");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn a_zero_crc_chunk_with_an_oversize_declared_size_errors_before_decompression() {
        let mut bytes = fixture::simple_file(FixtureOptions {
            chunk_crcs: false,
            ..FixtureOptions::default()
        });
        let chunk = fixture::first_chunk(&bytes);
        fixture::put_u64(&mut bytes, chunk.uncompressed_size_offset, 1 << 40);
        fixture::fix_data_section_crc(&mut bytes);

        let limits = ReaderLimits {
            max_chunk_bytes: 1 << 20,
            ..ReaderLimits::default()
        };
        let err = walk_with(&bytes, limits, Collector::default())
            .expect_err("an oversize declared size must be rejected before decompression");
        assert!(err.is_bound(), "got: {err}");
    }

    #[test]
    fn a_data_section_storing_zero_that_calculates_zero_is_accepted() {
        let bytes = fixture::make_data_section_crc_calculate_zero(&fixture::simple_file(
            FixtureOptions::unchunked(),
        ));
        assert_eq!(fixture::data_section_crc(&bytes), 0);

        let (_, collector) = walk(&bytes).expect("calculated zero must be accepted");
        assert_eq!(collector.log_times(), vec![100, 200, 300]);
    }

    #[test]
    fn a_data_section_storing_zero_that_calculates_nonzero_is_rejected() {
        let bytes = fixture::simple_file(FixtureOptions {
            use_chunks: false,
            data_section_crc: false,
            ..FixtureOptions::default()
        });
        assert_ne!(fixture::data_section_crc(&bytes), 0);
        let err = walk(&bytes).expect_err("calculated nonzero must be rejected");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn a_data_section_storing_zero_that_calculates_the_probe_sentinel_is_rejected() {
        // mcap accepts the patched probe when the true CRC
        // happens to equal the sentinel — acceptance means calculated ==
        // 0xFFFF_FFFF != stored 0, which must fail closed (MCAP Player Proposal
        // §6.7 item 5).
        let bytes = fixture::make_data_section_crc_calculate(
            &fixture::simple_file(FixtureOptions {
                use_chunks: false,
                ..FixtureOptions::default()
            }),
            CRC_PROBE_SENTINEL,
        );
        assert_eq!(fixture::data_section_crc(&bytes), CRC_PROBE_SENTINEL);
        let err = walk(&bytes).expect_err("sentinel collision must be rejected");
        assert!(err.is_corruption(), "got: {err}");
    }

    #[test]
    fn patching_a_nonzero_data_section_crc_to_zero_is_rejected() {
        let mut bytes = fixture::simple_file(FixtureOptions::unchunked());
        assert_ne!(fixture::data_section_crc(&bytes), 0);
        fixture::set_data_section_crc(&mut bytes, 0);
        let err = walk(&bytes).expect_err("a patched zero must not disable the check");
        assert!(err.is_corruption(), "got: {err}");
    }

    // -- adapter bounds (MCAP Player Proposal §6.7) ------------------------

    #[test]
    fn a_malicious_compression_name_length_errors_before_any_insert() {
        let mut bytes = fixture::simple_file(FixtureOptions::default());
        let chunk = fixture::first_chunk(&bytes);
        fixture::put_u32(&mut bytes, chunk.compression_len_offset, u32::MAX);
        fixture::fix_data_section_crc(&mut bytes);

        let err = walk(&bytes).expect_err("the compression name must be bounded");
        assert!(err.is_bound(), "got: {err}");
    }

    #[test]
    fn an_oversize_declared_record_errors_before_reader_allocation() {
        let mut bytes = fixture::simple_file(FixtureOptions::unchunked());
        let message = fixture::find_record(&bytes, op::MESSAGE);
        fixture::put_u64(&mut bytes, message.offset + 1, 1 << 30);

        let limits = ReaderLimits {
            max_chunk_bytes: 64 * 1024,
            max_message_bytes: 1024,
        };
        let err = walk_with(&bytes, limits, Collector::default())
            .expect_err("an oversize declared record must be rejected");
        assert!(err.is_bound(), "got: {err}");
    }

    #[test]
    fn an_oversize_message_is_rejected_before_the_owned_copy() {
        let bytes = fixture::simple_file(FixtureOptions::unchunked());
        let limits = ReaderLimits {
            max_message_bytes: 4,
            ..ReaderLimits::default()
        };
        let err = walk_with(&bytes, limits, Collector::default())
            .expect_err("an oversize message must be rejected");
        assert!(err.is_bound(), "got: {err}");
        assert!(err.to_string().contains("max-message-bytes"), "got: {err}");
    }

    // -- indexes are never load-bearing ------------------------------------
    // (MCAP Player Proposal §6.5, §6.7 item 4)

    #[test]
    fn misleading_and_duplicated_message_indexes_never_affect_order() {
        let bytes = two_chunk_file(FixtureOptions::default());
        let index = fixture::find_record(&bytes, op::MESSAGE_INDEX);
        let mut duplicate = bytes[index.offset..index.end()].to_vec();
        // Garbage offsets and log times in the duplicated index record.
        for byte in duplicate.iter_mut().skip(fixture::OPCODE_LEN_SIZE + 6) {
            *byte = 0x7F;
        }
        let mut spliced = fixture::splice(&bytes, index.end(), &duplicate);
        fixture::fix_data_section_crc(&mut spliced);

        let (report, collector) = walk(&spliced).expect("indexes are skipped, not parsed");
        assert_eq!(collector.log_times(), vec![100, 200, 150, 90]);
        assert_eq!(report.messages, 4);
    }

    #[test]
    fn summary_records_are_length_validated_even_though_they_are_skipped() {
        let bytes = fixture::simple_file(FixtureOptions::default());
        // The repeated schema record in the summary section, after DataEnd.
        let data_end = fixture::find_record(&bytes, op::DATA_END);
        let summary = fixture::top_level_records(&bytes)
            .into_iter()
            .find(|span| span.offset > data_end.offset && span.opcode == op::SCHEMA)
            .expect("a repeated summary schema record");
        let mut corrupt = bytes.clone();
        fixture::put_u64(&mut corrupt, summary.offset + 1, 1 << 30);

        let limits = ReaderLimits {
            max_chunk_bytes: 64 * 1024,
            max_message_bytes: 1024,
        };
        let err = walk_with(&corrupt, limits, Collector::default())
            .expect_err("summary records are length-validated");
        assert!(err.is_bound(), "got: {err}");
    }

    #[test]
    fn walks_a_real_file_snapshot() {
        // The shape pass 1 / pass 2 use: the unlinked pass-0 copy (MCAP Player
        // Proposal §6.1).
        let bytes = fixture::simple_file(FixtureOptions::compressed(Compression::Zstd));
        let path = std::env::temp_dir().join(format!(
            "aorta-play-reader-{}-{:?}.mcap",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, &bytes).expect("write fixture file");
        let file = std::fs::File::open(&path).expect("open fixture file");

        let mut collector = Collector::default();
        let mut walker = McapWalker::open(&file, ReaderLimits::default()).expect("open");
        let report = walker.walk(&mut collector).expect("clean walk");
        let _ = std::fs::remove_file(&path);

        assert_eq!(collector.log_times(), vec![100, 200, 300]);
        assert!(!report.truncated);
        assert_eq!(report.safe_prefix_end, bytes.len() as u64);
    }

    #[test]
    fn non_monotone_log_times_keep_their_stream_order_across_chunks() {
        let bytes = two_chunk_file(FixtureOptions::compressed(Compression::Zstd));
        let (_, collector) = walk(&bytes).expect("clean walk");
        assert_eq!(collector.log_times(), vec![100, 200, 150, 90]);
        assert_eq!(
            collector.payloads(),
            vec![
                b"m0".to_vec(),
                b"m1".to_vec(),
                b"m2".to_vec(),
                b"m3".to_vec()
            ]
        );
    }
}
