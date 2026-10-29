//! File rotation / splitting for the MCAP recorder.
//!
//! `RotatingWriter` composes over the single-file [`McapRecorderWriter`]
//! primitive. When rotation is disabled it is a transparent single-segment
//! pass-through (byte-for-byte identical to the pre-rotation recorder); when
//! enabled it splits the stream into time-named segments and enforces optional
//! retention. See `spec/Aorta_Recorder_Rotation_Plan.md`.
//!
//! Segment self-containment (ctx-latch replay into each new segment) is a
//! follow-up phase and is intentionally NOT implemented here: segments after the
//! first do not yet re-emit the ctx snapshot.

use crate::config::{RecorderConfig, StorageConfig, TimestampZone};
use crate::writer::{McapRecorderWriter, RecordMessage, SegmentMeta};
use crate::{SegmentCallback, SegmentFinalized};
use anyhow::{anyhow, Context, Result};
use aorta_core::unix_time_ns;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Why a roll fired, recorded in the closed segment's end metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollReason {
    Time,
    Size,
}

impl RollReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RollReason::Time => "time",
            RollReason::Size => "size",
        }
    }
}

/// Returned when a roll occurs, for caller-side logging.
#[derive(Debug, Clone)]
pub struct RollInfo {
    pub reason: RollReason,
    pub segment: SegmentFinalized,
    pub deleted_segments: u64,
}

/// Aggregated result of a finished recording (across all segments).
#[derive(Debug, Clone)]
pub struct RotationOutcome {
    pub accepted_messages: u64,
    pub accepted_bytes: u64,
    pub flush_count: u64,
    pub segment_count: u64,
    pub deleted_segments: u64,
    pub final_file_size_bytes: u64,
    pub retained_bytes: u64,
}

/// Pure roll-decision predicate, split out for exhaustive unit testing.
///
/// An empty segment is never rolled. Size and time are independent triggers;
/// size wins only to make the recorded reason deterministic when both are due.
fn roll_decision(
    elapsed: Duration,
    split: Option<Duration>,
    msgs_since_roll: u64,
    part_size_bytes: Option<u64>,
    size_cap_bytes: Option<u64>,
) -> Option<RollReason> {
    if msgs_since_roll == 0 {
        return None; // never finalize an empty segment
    }
    if let (Some(size), Some(cap)) = (part_size_bytes, size_cap_bytes) {
        if size >= cap {
            return Some(RollReason::Size);
        }
    }
    if split.is_some_and(|period| elapsed >= period) {
        return Some(RollReason::Time);
    }
    None
}

/// Promote a finished `.part` to its final name, refusing to overwrite an
/// existing finalized segment. `std::fs::rename` *replaces* the destination on
/// Unix, so a bare rename would silently clobber a prior run's segment (or every
/// segment, under a `{ts}`-less template). We pre-check existence and fail loudly
/// instead; the residual check→rename race is bounded by the per-segment
/// `create_new` `.part` reservation and millisecond-unique names.
fn promote_segment(part_path: &Path, final_path: &Path) -> Result<()> {
    if final_path.exists() {
        return Err(anyhow!(
            "refusing to overwrite existing finalized segment '{}' (no-clobber); \
             a prior run or a name template without {{ts}} collided",
            final_path.display()
        ));
    }
    std::fs::rename(part_path, final_path).with_context(|| {
        format!(
            "failed to finalize segment '{}' -> '{}'",
            part_path.display(),
            final_path.display()
        )
    })
}

/// Render a segment-start timestamp as `YYYYMMDDThhmmss_fffZ` (UTC, millisecond
/// precision). Computed directly from a unix-nanosecond instant via a civil-date
/// conversion so no timezone/date dependency is required (Recorder Rotation
/// Plan §3.1).
pub fn format_segment_timestamp(unix_ns: u64, zone: TimestampZone) -> String {
    // P1 only renders UTC; `local` is rejected at config-build time. Treat any
    // value as UTC here so the function is total.
    let _ = zone;
    let (y, mo, d, h, mi, s, ms) = utc_civil_from_unix_ns(unix_ns);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}_{:03}Z",
        y, mo, d, h, mi, s, ms
    )
}

/// Convert unix nanoseconds to `(year, month, day, hour, min, sec, millis)` in
/// UTC. Uses Howard Hinnant's `civil_from_days` algorithm (public domain).
fn utc_civil_from_unix_ns(unix_ns: u64) -> (i64, u32, u32, u32, u32, u32, u32) {
    let total_secs = (unix_ns / 1_000_000_000) as i64;
    let millis = ((unix_ns / 1_000_000) % 1_000) as u32;

    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let hour = (secs_of_day / 3_600) as u32;
    let minute = ((secs_of_day % 3_600) / 60) as u32;
    let second = (secs_of_day % 60) as u32;

    // civil_from_days: days are signed, counted from 1970-01-01.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    (year, month, day, hour, minute, second, millis)
}

/// Expands a filename template into concrete segment paths.
#[derive(Debug, Clone)]
pub struct NameTemplate {
    parent: PathBuf,
    stem: String,
    ext: String,
    template: String,
    group: String,
    pid: u32,
    zone: TimestampZone,
}

impl NameTemplate {
    pub fn from_output(
        output: &Path,
        group: &str,
        zone: TimestampZone,
        template: Option<&str>,
    ) -> Self {
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let stem = output
            .file_stem()
            .and_then(OsStr::to_str)
            .unwrap_or("aorta")
            .to_string();
        let ext = output
            .extension()
            .and_then(OsStr::to_str)
            .unwrap_or("mcap")
            .to_string();
        let template = template
            .map(str::to_string)
            .unwrap_or_else(|| "{stem}_{ts}.{ext}".to_string());
        Self {
            parent,
            stem,
            ext,
            template,
            group: group.to_string(),
            pid: std::process::id(),
            zone,
        }
    }

    /// Returns `(final_path, part_path)` for a segment starting at `start_ns`.
    pub fn segment_paths(&self, start_ns: u64, index: u64) -> (PathBuf, PathBuf) {
        let ts = format_segment_timestamp(start_ns, self.zone);
        let name = self
            .template
            .replace("{ts}", &ts)
            .replace("{stem}", &self.stem)
            .replace("{ext}", &self.ext)
            .replace("{group}", &self.group)
            .replace("{pid}", &self.pid.to_string())
            .replace("{index}", &format!("{index:03}"));
        let final_path = self.parent.join(name);
        let mut part = final_path.clone().into_os_string();
        part.push(".part");
        (final_path, PathBuf::from(part))
    }
}

/// Single-file writer + rotation/splitting/retention policy.
pub struct RotatingWriter {
    inner: Option<McapRecorderWriter>,
    enabled: bool,

    // Parameters needed to open subsequent segments.
    group: String,
    rpc_record: bool,
    storage: StorageConfig,

    // Policy.
    split: Option<Duration>,
    size_cap_bytes: Option<u64>,
    max_files: Option<usize>,
    max_total_size_bytes: Option<u64>,

    // Naming + identity.
    naming: NameTemplate,
    recording_id: String,
    index: u64,
    segment_callback: Option<SegmentCallback>,

    // Active segment state.
    final_path: PathBuf,
    part_path: PathBuf,
    segment_start: Instant,
    msgs_since_roll: u64,

    // Retention bookkeeping (finalized segments this run produced).
    retained: VecDeque<(PathBuf, u64)>,
    retained_bytes: u64,

    // Running totals across finalized segments.
    finished_accepted_messages: u64,
    finished_accepted_bytes: u64,
    finished_flush_count: u64,
    segment_count: u64,
    deleted_segments: u64,
    last_segment_size_bytes: u64,
}

impl RotatingWriter {
    #[cfg(test)]
    pub fn create(config: &RecorderConfig) -> Result<Self> {
        Self::create_with_segment_callback(config, None)
    }

    pub fn create_with_segment_callback(
        config: &RecorderConfig,
        segment_callback: Option<SegmentCallback>,
    ) -> Result<Self> {
        let rotation = &config.rotation;

        if !rotation.enabled {
            // Legacy single-file path: literal output, truncating create, no
            // segment metadata — byte-for-byte identical to pre-rotation.
            let inner = McapRecorderWriter::create(
                &config.output,
                &config.group,
                config.rpc_record,
                &config.storage,
                None,
            )?;
            return Ok(Self::single_segment(config, inner, segment_callback));
        }

        let naming = NameTemplate::from_output(
            &config.output,
            &config.group,
            rotation.timestamp,
            rotation.name_template.as_deref(),
        );
        let start_ns = unix_time_ns();
        let recording_id = format!("{}-{}-{}", config.group, start_ns, std::process::id());
        let (final_path, part_path) = naming.segment_paths(start_ns, 0);

        let seg = SegmentMeta {
            recording_id: recording_id.clone(),
            start_ns,
            index: 0,
            continues: false,
        };
        let inner = McapRecorderWriter::create(
            &part_path,
            &config.group,
            config.rpc_record,
            &config.storage,
            Some(&seg),
        )?;

        Ok(Self {
            inner: Some(inner),
            enabled: true,
            group: config.group.clone(),
            rpc_record: config.rpc_record,
            storage: config.storage.clone(),
            split: rotation.split,
            size_cap_bytes: rotation.max_file_size_bytes,
            max_files: rotation.max_files,
            max_total_size_bytes: rotation.max_total_size_bytes,
            naming,
            recording_id,
            index: 0,
            segment_callback,
            final_path,
            part_path,
            segment_start: Instant::now(),
            msgs_since_roll: 0,
            retained: VecDeque::new(),
            retained_bytes: 0,
            finished_accepted_messages: 0,
            finished_accepted_bytes: 0,
            finished_flush_count: 0,
            segment_count: 0,
            deleted_segments: 0,
            last_segment_size_bytes: 0,
        })
    }

    fn single_segment(
        config: &RecorderConfig,
        inner: McapRecorderWriter,
        segment_callback: Option<SegmentCallback>,
    ) -> Self {
        let naming =
            NameTemplate::from_output(&config.output, &config.group, TimestampZone::Utc, None);
        Self {
            inner: Some(inner),
            enabled: false,
            group: config.group.clone(),
            rpc_record: config.rpc_record,
            storage: config.storage.clone(),
            split: config.rotation.split,
            size_cap_bytes: None,
            max_files: None,
            max_total_size_bytes: None,
            naming,
            recording_id: format!("{}-{}-{}", config.group, unix_time_ns(), std::process::id()),
            index: 0,
            segment_callback,
            final_path: config.output.clone(),
            part_path: config.output.clone(),
            segment_start: Instant::now(),
            msgs_since_roll: 0,
            retained: VecDeque::new(),
            retained_bytes: 0,
            finished_accepted_messages: 0,
            finished_accepted_bytes: 0,
            finished_flush_count: 0,
            segment_count: 0,
            deleted_segments: 0,
            last_segment_size_bytes: 0,
        }
    }

    pub fn write_message(&mut self, message: RecordMessage) -> Result<()> {
        let inner = self
            .inner
            .as_mut()
            .expect("active segment present during write");
        inner.write_message(message)?;
        self.msgs_since_roll = self.msgs_since_roll.saturating_add(1);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner
            .as_mut()
            .expect("active segment present during flush")
            .flush()
    }

    pub fn should_flush(&self, batch_max_messages: usize, batch_max_bytes: usize) -> bool {
        self.inner
            .as_ref()
            .map(|w| w.should_flush(batch_max_messages, batch_max_bytes))
            .unwrap_or(false)
    }

    /// Global accepted-message count across all segments (for `max_messages`).
    pub fn accepted_messages(&self) -> u64 {
        self.finished_accepted_messages
            + self
                .inner
                .as_ref()
                .map(|w| w.counters().accepted_messages)
                .unwrap_or(0)
    }

    /// Evaluate roll triggers; roll if due. Returns roll info for logging.
    /// `now` is an injected monotonic instant (the period is monotonic);
    /// the new segment's wall-clock start is sampled inside `roll`.
    pub fn maybe_roll(
        &mut self,
        now: Instant,
        schema_miss_count: u64,
        dropped_messages: u64,
    ) -> Result<Option<RollInfo>> {
        if !self.enabled {
            return Ok(None);
        }
        let elapsed = now.saturating_duration_since(self.segment_start);
        // Only stat the file when a size cap is configured.
        let part_size = if self.size_cap_bytes.is_some() {
            std::fs::metadata(&self.part_path).map(|m| m.len()).ok()
        } else {
            None
        };
        let Some(reason) = roll_decision(
            elapsed,
            self.split,
            self.msgs_since_roll,
            part_size,
            self.size_cap_bytes,
        ) else {
            // Empty-window reset: a full period elapsed with no data. Restart the
            // window so the next message begins a fresh period instead of being
            // rolled immediately into a stale one-message segment.
            if self.msgs_since_roll == 0 && self.split.is_some_and(|period| elapsed >= period) {
                self.segment_start = now;
            }
            return Ok(None);
        };
        let info = self.roll(reason, schema_miss_count, dropped_messages)?;
        Ok(Some(info))
    }

    fn roll(
        &mut self,
        reason: RollReason,
        schema_miss_count: u64,
        dropped_messages: u64,
    ) -> Result<RollInfo> {
        // Finalize the current segment (writes footer/summary).
        let inner = self.inner.take().expect("active segment present at roll");
        let counters = inner.finish(schema_miss_count, dropped_messages, Some(reason.as_str()))?;
        self.finished_accepted_messages = self
            .finished_accepted_messages
            .saturating_add(counters.accepted_messages);
        self.finished_accepted_bytes = self
            .finished_accepted_bytes
            .saturating_add(counters.accepted_bytes);
        self.finished_flush_count = self
            .finished_flush_count
            .saturating_add(counters.flush_count);
        self.segment_count = self.segment_count.saturating_add(1);

        // Promote .part -> final with no-clobber semantics: refuse to
        // overwrite an existing finalized segment (prior run / {ts}-less
        // template). On failure the .part remains a complete MCAP; we propagate
        // the error (recording fails) rather than lose data.
        promote_segment(&self.part_path, &self.final_path)?;
        let finalized_path = self.final_path.clone();
        let bytes = std::fs::metadata(&finalized_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let finalized_index = self.index;
        let segment = SegmentFinalized {
            recording_id: self.recording_id.clone(),
            index: finalized_index,
            path: finalized_path,
            size_bytes: bytes,
        };
        self.last_segment_size_bytes = segment.size_bytes;
        self.notify_segment(segment.clone())?;
        if self.segment_callback.is_none() {
            self.retained
                .push_back((segment.path.clone(), segment.size_bytes));
            self.retained_bytes = self.retained_bytes.saturating_add(segment.size_bytes);
        }
        let deleted = self.enforce_retention();

        // Open the next segment.
        let new_start_ns = unix_time_ns();
        self.index = self.index.saturating_add(1);
        let (final_path, part_path) = self.naming.segment_paths(new_start_ns, self.index);
        let seg = SegmentMeta {
            recording_id: self.recording_id.clone(),
            start_ns: new_start_ns,
            index: self.index,
            continues: true,
        };
        let new_inner = McapRecorderWriter::create(
            &part_path,
            &self.group,
            self.rpc_record,
            &self.storage,
            Some(&seg),
        )
        .with_context(|| format!("failed to open next segment '{}'", part_path.display()))?;
        self.inner = Some(new_inner);
        self.final_path = final_path;
        self.part_path = part_path;
        self.segment_start = Instant::now();
        self.msgs_since_roll = 0;

        Ok(RollInfo {
            reason,
            segment,
            deleted_segments: deleted,
        })
    }

    fn notify_segment(&self, segment: SegmentFinalized) -> Result<()> {
        if let Some(callback) = &self.segment_callback {
            callback(segment).context("segment finalized callback failed")?;
        }
        Ok(())
    }

    /// Enforce `max_files` / `max_total_size_bytes` by deleting the oldest
    /// finalized segments this run produced. Safe by construction: only paths in
    /// the in-run deque are deleted, never the active segment, never via glob.
    /// Best-effort: a deletion failure is counted but non-fatal.
    fn enforce_retention(&mut self) -> u64 {
        let mut deleted = 0u64;

        if let Some(max) = self.max_files {
            while self.retained.len() > max {
                if !self.pop_oldest() {
                    break;
                }
                deleted += 1;
            }
        }

        if let Some(cap) = self.max_total_size_bytes {
            // Keep at least one finalized segment regardless of the byte cap.
            while self.retained_bytes > cap && self.retained.len() > 1 {
                if !self.pop_oldest() {
                    break;
                }
                deleted += 1;
            }
        }

        self.deleted_segments = self.deleted_segments.saturating_add(deleted);
        deleted
    }

    /// Delete the oldest retained segment. Returns whether bookkeeping advanced
    /// (true even on a filesystem error, so retention cannot livelock).
    fn pop_oldest(&mut self) -> bool {
        let Some((path, bytes)) = self.retained.pop_front() else {
            return false;
        };
        self.retained_bytes = self.retained_bytes.saturating_sub(bytes);
        if let Err(err) = std::fs::remove_file(&path) {
            // Non-fatal: the cap is best-effort. Surface to stderr; the run
            // summary still reports counts.
            eprintln!(
                "WARN: rotation retention failed to delete '{}': {}",
                path.display(),
                err
            );
        }
        true
    }

    pub fn finish(
        mut self,
        schema_miss_count: u64,
        dropped_messages: u64,
    ) -> Result<RotationOutcome> {
        // Discard an empty trailing segment: if a roll opened a new
        // segment and the run then stopped before any message arrived, do NOT
        // finalize/retain that empty .part — otherwise retention (e.g.
        // max_files=1) would evict a real data segment in favour of the empty
        // one. Only applies once at least one segment already holds data; if this
        // is the sole segment we still emit it (parity with the single-file path
        // that always writes a file).
        if self.enabled && self.msgs_since_roll == 0 && self.segment_count > 0 {
            // Close the writer (releasing the file) before removing the .part.
            drop(self.inner.take());
            let _ = std::fs::remove_file(&self.part_path);
            return Ok(RotationOutcome {
                accepted_messages: self.finished_accepted_messages,
                accepted_bytes: self.finished_accepted_bytes,
                flush_count: self.finished_flush_count,
                segment_count: self.segment_count,
                deleted_segments: self.deleted_segments,
                final_file_size_bytes: self.last_segment_size_bytes,
                retained_bytes: self.retained_bytes,
            });
        }

        let reason = if self.enabled { Some("final") } else { None };
        let inner = self.inner.take().expect("active segment present at finish");
        let counters = inner.finish(schema_miss_count, dropped_messages, reason)?;
        self.finished_accepted_messages = self
            .finished_accepted_messages
            .saturating_add(counters.accepted_messages);
        self.finished_accepted_bytes = self
            .finished_accepted_bytes
            .saturating_add(counters.accepted_bytes);
        self.finished_flush_count = self
            .finished_flush_count
            .saturating_add(counters.flush_count);
        self.segment_count = self.segment_count.saturating_add(1);

        let final_file_size_bytes = if self.enabled {
            // No-clobber promotion, same as a mid-run roll.
            promote_segment(&self.part_path, &self.final_path)?;
            let bytes = std::fs::metadata(&self.final_path)
                .map(|m| m.len())
                .unwrap_or(0);
            let segment = SegmentFinalized {
                recording_id: self.recording_id.clone(),
                index: self.index,
                path: self.final_path.clone(),
                size_bytes: bytes,
            };
            self.last_segment_size_bytes = segment.size_bytes;
            self.notify_segment(segment.clone())?;
            if self.segment_callback.is_none() {
                self.retained
                    .push_back((segment.path.clone(), segment.size_bytes));
                self.retained_bytes = self.retained_bytes.saturating_add(bytes);
            }
            self.enforce_retention();
            bytes
        } else {
            let path = self.final_path.clone();
            let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            self.notify_segment(SegmentFinalized {
                recording_id: self.recording_id.clone(),
                index: 0,
                path: path.clone(),
                size_bytes,
            })?;
            size_bytes
        };

        Ok(RotationOutcome {
            accepted_messages: self.finished_accepted_messages,
            accepted_bytes: self.finished_accepted_bytes,
            flush_count: self.finished_flush_count,
            segment_count: self.segment_count,
            deleted_segments: self.deleted_segments,
            final_file_size_bytes,
            retained_bytes: self.retained_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_epoch_is_zeroed() {
        assert_eq!(
            format_segment_timestamp(0, TimestampZone::Utc),
            "19700101T000000_000Z"
        );
    }

    #[test]
    fn timestamp_extracts_seconds_and_millis() {
        // 1.234 s after epoch.
        assert_eq!(
            format_segment_timestamp(1_234_000_000, TimestampZone::Utc),
            "19700101T000001_234Z"
        );
    }

    #[test]
    fn timestamp_rolls_to_next_day() {
        // Exactly one day after epoch.
        assert_eq!(
            format_segment_timestamp(86_400 * 1_000_000_000, TimestampZone::Utc),
            "19700102T000000_000Z"
        );
    }

    #[test]
    fn timestamp_known_2023_value() {
        // 1_700_000_000 s = 2023-11-14T22:13:20Z.
        assert_eq!(
            format_segment_timestamp(1_700_000_000 * 1_000_000_000, TimestampZone::Utc),
            "20231114T221320_000Z"
        );
    }

    #[test]
    fn name_template_default_inserts_ts_before_ext() {
        let nt = NameTemplate::from_output(
            Path::new("/data/aorta.mcap"),
            "default",
            TimestampZone::Utc,
            None,
        );
        let (final_path, part_path) = nt.segment_paths(0, 0);
        assert_eq!(
            final_path,
            PathBuf::from("/data/aorta_19700101T000000_000Z.mcap")
        );
        assert_eq!(
            part_path,
            PathBuf::from("/data/aorta_19700101T000000_000Z.mcap.part")
        );
    }

    #[test]
    fn name_template_custom_placeholders() {
        let nt = NameTemplate::from_output(
            Path::new("/data/run.mcap"),
            "fleet",
            TimestampZone::Utc,
            Some("{group}-{stem}-{ts}.{ext}"),
        );
        let (final_path, _) = nt.segment_paths(0, 0);
        assert_eq!(
            final_path,
            PathBuf::from("/data/fleet-run-19700101T000000_000Z.mcap")
        );
    }

    #[test]
    fn name_template_formats_zero_padded_segment_index() {
        let nt = NameTemplate::from_output(
            Path::new("/data/auto_20260915_150433.mcap"),
            "default",
            TimestampZone::Utc,
            Some("{stem}_{index}.{ext}"),
        );
        let (final_path, part_path) = nt.segment_paths(0, 7);
        assert_eq!(
            final_path,
            PathBuf::from("/data/auto_20260915_150433_007.mcap")
        );
        assert_eq!(
            part_path,
            PathBuf::from("/data/auto_20260915_150433_007.mcap.part")
        );
    }

    #[test]
    fn name_template_handles_extensionless_output() {
        let nt = NameTemplate::from_output(Path::new("aorta"), "g", TimestampZone::Utc, None);
        let (final_path, _) = nt.segment_paths(0, 0);
        assert_eq!(
            final_path,
            PathBuf::from("./aorta_19700101T000000_000Z.mcap")
        );
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn size_trigger_has_no_time_floor() {
        assert_eq!(
            roll_decision(secs(30), None, 100, Some(9_999), Some(100)),
            Some(RollReason::Size)
        );
    }

    #[test]
    fn size_only_does_not_roll_under_cap_at_any_age() {
        assert_eq!(
            roll_decision(secs(3600), None, 100, Some(99), Some(100)),
            None
        );
    }

    #[test]
    fn roll_empty_segment_suppressed() {
        assert_eq!(
            roll_decision(secs(120), Some(secs(60)), 0, None, None),
            None
        );
    }

    #[test]
    fn roll_time_trigger_fires_at_period() {
        assert_eq!(
            roll_decision(secs(60), Some(secs(60)), 1, None, None),
            Some(RollReason::Time)
        );
    }

    #[test]
    fn roll_size_trigger_fires_before_period() {
        assert_eq!(
            roll_decision(secs(90), Some(secs(300)), 5, Some(200), Some(100)),
            Some(RollReason::Size)
        );
    }

    #[test]
    fn roll_size_under_cap_does_not_fire() {
        assert_eq!(
            roll_decision(secs(90), Some(secs(300)), 5, Some(50), Some(100)),
            None
        );
    }

    #[test]
    fn roll_size_beats_time_when_both_due() {
        assert_eq!(
            roll_decision(secs(300), Some(secs(300)), 5, Some(200), Some(100)),
            Some(RollReason::Size)
        );
    }

    // --- RotatingWriter integration (real files) ---------------------------
    //
    // Time rolls are forced by injecting a `now` far past the segment start. The
    // new segment's filename is still derived from the real wall clock at
    // millisecond precision, so a 2ms sleep before each forced roll keeps names
    // distinct. This is a test-only concern for such artificial time jumps.

    use crate::config::{
        build_config, CompressionKind, ConfigOverrides, FileConfig, RecorderConfig,
    };

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aorta-rot-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir temp");
        dir
    }

    fn rotating_config(dir: &Path, enabled: bool, max_files: Option<usize>) -> RecorderConfig {
        let mut o = ConfigOverrides::default();
        o.group = Some("default".to_string());
        o.output = Some(dir.join("rec.mcap").to_string_lossy().to_string());
        o.compression = Some(CompressionKind::None);
        o.rotate_enabled = Some(enabled);
        if enabled {
            o.split_minutes = Some(1);
            o.rotate_max_files = max_files;
        }
        build_config(Some(FileConfig::default()), o).expect("config")
    }

    fn msg(i: u64) -> RecordMessage {
        RecordMessage {
            topic_key: "aorta/default/pub/demo/x".to_string(),
            key_suffix: "pub/demo/x".to_string(),
            schema: None,
            unresolved_schema_hash: None,
            payload: vec![i as u8],
            log_time_ns: i + 1,
            publish_time_ns: i + 1,
        }
    }

    fn far_future() -> Instant {
        Instant::now() + Duration::from_secs(120)
    }

    fn list_mcap(dir: &Path) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
            .expect("readdir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(OsStr::to_str) == Some("mcap"))
            .collect();
        out.sort();
        out
    }

    fn has_part(dir: &Path) -> bool {
        std::fs::read_dir(dir)
            .expect("readdir")
            .filter_map(|e| e.ok())
            .any(|e| {
                e.path()
                    .extension()
                    .and_then(OsStr::to_str)
                    .map(|x| x == "part")
                    .unwrap_or(false)
            })
    }

    fn count_msgs(path: &Path) -> usize {
        let bytes = std::fs::read(path).expect("read mcap");
        let mut n = 0;
        for m in mcap::MessageStream::new(&bytes).expect("stream") {
            m.expect("decodable message");
            n += 1;
        }
        n
    }

    #[test]
    fn rotating_splits_into_two_segments() {
        let dir = temp_dir("split");
        let config = rotating_config(&dir, true, None);
        let mut w = RotatingWriter::create(&config).expect("create");

        w.write_message(msg(0)).expect("write 0");
        std::thread::sleep(Duration::from_millis(2));
        let info = w.maybe_roll(far_future(), 0, 0).expect("roll");
        assert!(
            matches!(info.map(|i| i.reason), Some(RollReason::Time)),
            "time trigger should fire past the period"
        );
        // Active segment is mid-write as a .part; first segment is finalized.
        assert!(has_part(&dir), "new active segment is a .part");
        assert_eq!(list_mcap(&dir).len(), 1, "one finalized segment after roll");

        w.write_message(msg(1)).expect("write 1");
        let outcome = w.finish(0, 0).expect("finish");

        assert_eq!(outcome.segment_count, 2, "two segments total");
        assert!(!has_part(&dir), "no leftover .part after finish");
        let files = list_mcap(&dir);
        assert_eq!(files.len(), 2, "two finalized .mcap files");
        let total: usize = files.iter().map(|f| count_msgs(f)).sum();
        assert_eq!(total, 2, "every message preserved across segments");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn callback_failure_preserves_finalized_segment_without_opening_next() {
        let dir = temp_dir("callback-failure");
        let config = rotating_config(&dir, true, None);
        let callback =
            std::sync::Arc::new(|_segment: SegmentFinalized| Err(anyhow!("ledger unavailable")));
        let mut w =
            RotatingWriter::create_with_segment_callback(&config, Some(callback)).expect("create");

        w.write_message(msg(0)).expect("write");
        let error = w
            .maybe_roll(far_future(), 0, 0)
            .expect_err("callback failure must stop recording");

        assert!(error
            .to_string()
            .contains("segment finalized callback failed"));
        assert_eq!(list_mcap(&dir).len(), 1, "completed segment is preserved");
        assert!(!has_part(&dir), "next segment is not opened after failure");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn callback_reclamation_preserves_summary_size_without_retention_ownership() {
        let dir = temp_dir("callback-reclaim");
        let config = rotating_config(&dir, true, None);
        let finalized_size = std::sync::Arc::new(std::sync::Mutex::new(0));
        let callback_size = finalized_size.clone();
        let callback = std::sync::Arc::new(move |segment: SegmentFinalized| {
            *callback_size.lock().expect("size lock") = segment.size_bytes;
            std::fs::remove_file(segment.path)?;
            Ok(())
        });
        let mut w =
            RotatingWriter::create_with_segment_callback(&config, Some(callback)).expect("create");

        w.write_message(msg(0)).expect("write");
        w.maybe_roll(far_future(), 0, 0)
            .expect("roll")
            .expect("should roll");
        let outcome = w.finish(0, 0).expect("finish empty trailing segment");

        let expected_size = *finalized_size.lock().expect("size lock");
        assert!(expected_size > 0);
        assert_eq!(outcome.final_file_size_bytes, expected_size);
        assert_eq!(outcome.retained_bytes, 0);
        assert!(list_mcap(&dir).is_empty(), "callback reclaimed the segment");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_does_not_roll_empty_segment() {
        let dir = temp_dir("empty");
        let config = rotating_config(&dir, true, None);
        let mut w = RotatingWriter::create(&config).expect("create");

        // No messages written: even far past the period, no roll.
        assert!(
            w.maybe_roll(far_future(), 0, 0).expect("roll").is_none(),
            "empty segment must never roll"
        );
        assert_eq!(list_mcap(&dir).len(), 0, "no finalized file yet");

        w.write_message(msg(0)).expect("write");
        let outcome = w.finish(0, 0).expect("finish");
        assert_eq!(outcome.segment_count, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_retention_keeps_only_max_files() {
        let dir = temp_dir("retain");
        let config = rotating_config(&dir, true, Some(1));
        let mut w = RotatingWriter::create(&config).expect("create");

        for i in 0..3u64 {
            w.write_message(msg(i)).expect("write");
            std::thread::sleep(Duration::from_millis(2));
            w.maybe_roll(far_future(), 0, 0)
                .expect("roll")
                .expect("should roll");
        }
        w.write_message(msg(99)).expect("write final");
        let outcome = w.finish(0, 0).expect("finish");

        assert_eq!(outcome.segment_count, 4, "3 rolls + final");
        assert_eq!(
            outcome.deleted_segments, 3,
            "max_files=1 deletes the 3 oldest"
        );
        let files = list_mcap(&dir);
        assert_eq!(files.len(), 1, "only the newest finalized segment remains");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotation_disabled_writes_single_literal_file() {
        let dir = temp_dir("off");
        let config = rotating_config(&dir, false, None);
        let mut w = RotatingWriter::create(&config).expect("create");

        w.write_message(msg(0)).expect("write");
        assert!(
            w.maybe_roll(Instant::now() + Duration::from_secs(9_999), 0, 0)
                .expect("roll")
                .is_none(),
            "disabled rotation never rolls"
        );
        let outcome = w.finish(0, 0).expect("finish");
        assert_eq!(outcome.segment_count, 1);

        let files = list_mcap(&dir);
        assert_eq!(files.len(), 1, "single literal file");
        assert_eq!(
            files[0].file_name().and_then(OsStr::to_str),
            Some("rec.mcap"),
            "literal name, no timestamp injected"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Build a rotating config with chunking disabled + no compression so that
    /// `fs::metadata(part)` tracks written bytes immediately (no chunk buffering)
    /// — needed for deterministic size-cap/byte-retention tests.
    fn rotating_config_nochunk(dir: &Path, split_minutes: u64) -> RecorderConfig {
        let mut o = ConfigOverrides::default();
        o.group = Some("default".to_string());
        o.output = Some(dir.join("rec.mcap").to_string_lossy().to_string());
        o.compression = Some(CompressionKind::None);
        o.chunking = Some(false);
        o.rotate_enabled = Some(true);
        o.split_minutes = Some(split_minutes);
        build_config(Some(FileConfig::default()), o).expect("config")
    }

    fn big_msg(i: u64) -> RecordMessage {
        RecordMessage {
            topic_key: "aorta/default/pub/demo/x".to_string(),
            key_suffix: "pub/demo/x".to_string(),
            schema: None,
            unresolved_schema_hash: None,
            payload: vec![(i & 0xff) as u8; 512],
            log_time_ns: i + 1,
            publish_time_ns: i + 1,
        }
    }

    fn read_named_metadata(path: &Path, name: &str) -> std::collections::BTreeMap<String, String> {
        let bytes = std::fs::read(path).expect("read mcap");
        for record in mcap::read::LinearReader::new(&bytes).expect("reader") {
            if let mcap::records::Record::Metadata(m) = record.expect("record") {
                if m.name == name {
                    return m.metadata.clone();
                }
            }
        }
        panic!("metadata '{}' not found in {}", name, path.display());
    }

    #[test]
    fn rotating_segment_metadata_is_emitted() {
        let dir = temp_dir("meta");
        let config = rotating_config(&dir, true, None);
        let mut w = RotatingWriter::create(&config).expect("create");

        w.write_message(msg(0)).expect("write 0");
        std::thread::sleep(Duration::from_millis(2));
        w.maybe_roll(far_future(), 0, 0)
            .expect("roll")
            .expect("rolled");
        w.write_message(msg(1)).expect("write 1");
        w.finish(0, 0).expect("finish");

        let files = list_mcap(&dir); // sorted by name == chronological
        assert_eq!(files.len(), 2);

        let s0 = read_named_metadata(&files[0], "aorta.recorder.start");
        let s1 = read_named_metadata(&files[1], "aorta.recorder.start");
        // recording_id is shared across all segments of one recording.
        let rid0 = s0.get("aorta.recording_id").expect("recording_id 0");
        let rid1 = s1.get("aorta.recording_id").expect("recording_id 1");
        assert_eq!(rid0, rid1, "recording_id shared across segments");
        assert!(!rid0.is_empty());
        // index increments; `continues` flips after the first segment.
        assert_eq!(s0.get("aorta.segment.index").map(String::as_str), Some("0"));
        assert_eq!(
            s0.get("aorta.segment.continues").map(String::as_str),
            Some("false")
        );
        assert_eq!(s1.get("aorta.segment.index").map(String::as_str), Some("1"));
        assert_eq!(
            s1.get("aorta.segment.continues").map(String::as_str),
            Some("true")
        );
        // Closed segment records why it rolled; the last records `final`.
        let e0 = read_named_metadata(&files[0], "aorta.recorder.end");
        assert_eq!(
            e0.get("aorta.rotation.reason").map(String::as_str),
            Some("time")
        );
        let e1 = read_named_metadata(&files[1], "aorta.recorder.end");
        assert_eq!(
            e1.get("aorta.rotation.reason").map(String::as_str),
            Some("final")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_path_emits_no_segment_metadata() {
        // Back-compat: the rotation-off path must not inject any of the new
        // segment metadata keys (the concrete, testable form of byte-for-byte).
        let dir = temp_dir("nometa");
        let config = rotating_config(&dir, false, None);
        let mut w = RotatingWriter::create(&config).expect("create");
        w.write_message(msg(0)).expect("write");
        w.finish(0, 0).expect("finish");

        let file = dir.join("rec.mcap");
        let start = read_named_metadata(&file, "aorta.recorder.start");
        assert!(!start.contains_key("aorta.recording_id"));
        assert!(!start.contains_key("aorta.segment.index"));
        assert!(!start.contains_key("aorta.segment.continues"));
        let end = read_named_metadata(&file, "aorta.recorder.end");
        assert!(!end.contains_key("aorta.rotation.reason"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_size_only_rolls_at_cap() {
        let dir = temp_dir("size");
        let mut config = rotating_config_nochunk(&dir, 5);
        config.rotation.split = None;
        config.rotation.max_file_size_bytes = Some(2048);
        let mut w = RotatingWriter::create(&config).expect("create");

        for i in 0..8u64 {
            w.write_message(big_msg(i)).expect("write"); // 8 * 512B = 4 KiB > cap
        }
        w.flush().expect("flush");
        std::thread::sleep(Duration::from_millis(2));

        let info = w.maybe_roll(Instant::now(), 0, 0).expect("roll");
        assert!(
            matches!(info.map(|i| i.reason), Some(RollReason::Size)),
            "size-only rotation should roll as soon as the cap is reached"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_total_size_retention_deletes_oldest() {
        let dir = temp_dir("total");
        let mut config = rotating_config_nochunk(&dir, 1); // 1-minute period
        config.rotation.max_files = None;
        config.rotation.max_total_size_bytes = Some(2048); // each segment > cap
        let mut w = RotatingWriter::create(&config).expect("create");

        for _seg in 0..3 {
            for i in 0..8u64 {
                w.write_message(big_msg(i)).expect("write");
            }
            std::thread::sleep(Duration::from_millis(2));
            w.maybe_roll(far_future(), 0, 0)
                .expect("roll")
                .expect("rolled");
        }
        for i in 0..8u64 {
            w.write_message(big_msg(i)).expect("write final");
        }
        let outcome = w.finish(0, 0).expect("finish");

        assert_eq!(outcome.segment_count, 4, "3 rolls + final");
        assert_eq!(
            outcome.deleted_segments, 3,
            "byte cap below one segment keeps only the newest"
        );
        assert_eq!(list_mcap(&dir).len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_promotion_refuses_to_clobber() {
        // A name template without {ts} makes every segment resolve to
        // the same final name. The first roll promotes cleanly; the second must
        // refuse to overwrite the already-finalized file rather than clobber it.
        let dir = temp_dir("clobber");
        let mut config = rotating_config(&dir, true, None);
        config.rotation.name_template = Some("{stem}.{ext}".to_string());
        let mut w = RotatingWriter::create(&config).expect("create");

        w.write_message(msg(0)).expect("write 0");
        w.maybe_roll(far_future(), 0, 0)
            .expect("first roll ok")
            .expect("rolled");
        w.write_message(msg(1)).expect("write 1");
        let res = w.maybe_roll(far_future(), 0, 0);
        assert!(
            res.is_err(),
            "second roll must refuse to overwrite the existing finalized segment"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_discards_empty_trailing_segment() {
        // A roll opens a new segment, the run stops with no further
        // message. The empty trailing segment must be discarded, not finalized
        // and retained — otherwise max_files=1 would evict the data segment.
        let dir = temp_dir("empty-trail");
        let config = rotating_config(&dir, true, Some(1));
        let mut w = RotatingWriter::create(&config).expect("create");

        w.write_message(msg(0)).expect("write"); // seg 0 holds data
        std::thread::sleep(Duration::from_millis(2));
        w.maybe_roll(far_future(), 0, 0)
            .expect("roll")
            .expect("rolled"); // opens an empty seg 1
        let outcome = w.finish(0, 0).expect("finish"); // no further messages

        assert_eq!(
            outcome.segment_count, 1,
            "empty trailing segment is not counted"
        );
        assert_eq!(
            outcome.deleted_segments, 0,
            "the data segment must not be evicted by an empty one"
        );
        let files = list_mcap(&dir);
        assert_eq!(files.len(), 1, "only the data segment survives");
        assert_eq!(count_msgs(&files[0]), 1, "surviving segment holds the data");
        assert!(!has_part(&dir), "empty .part removed, not left behind");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotating_resets_window_after_idle() {
        // After an idle gap longer than the period, the next message
        // must start a fresh window instead of rolling immediately into a stale
        // one-message segment.
        let dir = temp_dir("idle-window");
        let config = rotating_config(&dir, true, None); // 1-minute period
        let mut w = RotatingWriter::create(&config).expect("create");
        let base = Instant::now();

        // Idle 120s with no data: empty-suppressed AND the window resets to now.
        assert!(w
            .maybe_roll(base + Duration::from_secs(120), 0, 0)
            .expect("roll")
            .is_none());

        w.write_message(msg(0)).expect("write");
        // At +150s only 30s have elapsed since the reset (< 60s) → no roll.
        assert!(
            w.maybe_roll(base + Duration::from_secs(150), 0, 0)
                .expect("roll")
                .is_none(),
            "fresh window after idle: first message must not roll immediately"
        );
        // At +185s the fresh 65s window elapses → a normal time roll.
        std::thread::sleep(Duration::from_millis(2));
        let info = w
            .maybe_roll(base + Duration::from_secs(185), 0, 0)
            .expect("roll");
        assert!(
            matches!(info.map(|i| i.reason), Some(RollReason::Time)),
            "the reset window still rolls once its own period elapses"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
