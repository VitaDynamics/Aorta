//! Trace drain thread + rotating file sink + retention.
//!
//! A single background **drain thread** sweeps every per-thread ring
//! ([`super::ring::drain_all`]), accumulates records, and flushes them as a
//! compressed FlatBuffers **CHUNK** to a per-process rotating file. The hot path
//! never touches the disk or FlatBuffers; all of that is here, off the hot path.
//!
//! Chunks are framed and **appended** to a per-process `*.atrc` container file
//! (written `*.active`, finalized by atomic rename on rotation — size/age — or
//! clean shutdown), so sparse traffic does not explode into file-per-chunk.
//! Retention is a **daemon-free, per-host disk ring buffer**: on each rotation,
//! the writer sums the whole trace dir and unlinks the oldest finalized files
//! until total ≤ `AORTA_TRACE_MAX_BYTES` (default 2 GB, shared by every process
//! on the host); crash-orphaned `.atrc.active` files are salvaged by rename.
//! Filenames encode `<node>_<process>_<pid>_<wall_ts>_<seq>.atrc` (first frame's
//! provenance) so a time window / process can be retrieved by glob without
//! decoding.

use super::chunk::{self, ChunkProvenance, RingRecord};
use super::{intern, ring, scope};
use crate::time::{monotonic_ns, unix_time_ns};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Default per-host trace-storage cap (2 GB), shared by all processes' chunks.
pub const DEFAULT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Static provenance + storage policy for the file sink.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub node_id: String,
    pub run_id: String,
    pub process_name: String,
    pub pid: u32,
    pub host_clock_domain_id: u32,
    /// zstd level; 1 is the spec default (~5.4x at low CPU).
    pub compression_level: i32,
    /// Rotate the container file once it reaches this many bytes.
    pub file_max_bytes: u64,
    /// Rotate the container file once it has been open this long (bounds how
    /// long data sits in a `.active` file and how stale a crash orphan can be).
    pub file_max_age: Duration,
}

/// Default container-file rotation size (LTTng `--tracefile-size` analog).
/// Keep ≤ max_bytes/16 so file-granular FIFO eviction stays reasonably smooth.
pub const DEFAULT_FILE_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Default container-file rotation age.
pub const DEFAULT_FILE_MAX_AGE: Duration = Duration::from_secs(300);

/// Resolve the trace output directory from the environment, exactly as
/// [`SinkConfig::from_env`] does: honor `AORTA_TRACE_DIR`, else fall back to
/// `std::env::temp_dir()/aorta-trace`. The single source of truth for "where
/// traces live" — offline readers (e.g. the trace TUI) default to this so they
/// can never drift from where the writer actually persists chunks.
pub fn default_trace_dir() -> PathBuf {
    std::env::var_os("AORTA_TRACE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("aorta-trace"))
}

impl SinkConfig {
    /// Build from the environment (the same vars the CLI and SDKs honor):
    /// `AORTA_TRACE_DIR`, `AORTA_TRACE_MAX_BYTES`, `AORTA_NODE_ID`,
    /// `AORTA_TRACE_RUN_ID`, `AORTA_TRACE_FILE_MAX_BYTES`,
    /// `AORTA_TRACE_FILE_MAX_SECS`.
    pub fn from_env() -> SinkConfig {
        let dir = default_trace_dir();
        let max_bytes = std::env::var("AORTA_TRACE_MAX_BYTES")
            .ok()
            .and_then(|s| parse_size(&s))
            .unwrap_or(DEFAULT_MAX_BYTES);
        let node_id = std::env::var("AORTA_NODE_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(default_node_id);
        let run_id = std::env::var("AORTA_TRACE_RUN_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        let host_clock_domain_id = fnv1a(node_id.as_bytes());
        let file_max_bytes = std::env::var("AORTA_TRACE_FILE_MAX_BYTES")
            .ok()
            .and_then(|s| parse_size(&s))
            .unwrap_or(DEFAULT_FILE_MAX_BYTES);
        let file_max_age = std::env::var("AORTA_TRACE_FILE_MAX_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_FILE_MAX_AGE); // clamped at FileSink::new
        SinkConfig {
            dir,
            max_bytes,
            node_id,
            run_id,
            process_name: default_process_name(),
            pid: std::process::id(),
            host_clock_domain_id,
            compression_level: 1,
            file_max_bytes,
            file_max_age,
        }
    }

    fn provenance(&self, seq: u64, dropped: u64) -> ChunkProvenance {
        ChunkProvenance {
            node_id: self.node_id.clone(),
            run_id: self.run_id.clone(),
            pid: self.pid,
            process_name: self.process_name.clone(),
            host_clock_domain_id: self.host_clock_domain_id,
            seq,
            dropped,
        }
    }
}

// ── Container frame format ────────────────────────────────────────────────────
//
// A trace file (`*.atrc`) is a sequence of self-delimiting frames, each holding
// one zstd-compressed CHUNK (the CHUNK stays the independent decode unit — the
// CTF-packet / Perfetto-incremental-state analog). Appending a frame is one
// `write_all`, so a crash leaves at most one truncated tail frame; readers keep
// every complete frame (Perfetto's protobuf-concatenation property, made
// explicit). Legacy single-frame `*.chunk.zst` files remain readable.

/// Frame header magic ("Aorta Trace Container Frame").
pub const FRAME_MAGIC: [u8; 4] = *b"ATCF";
/// Container frame format version.
pub const FRAME_VERSION: u8 = 1;
/// Bytes: magic(4) + version(1) + flags(1) + reserved(2) + payload_len(4 LE).
pub const FRAME_HEADER_LEN: usize = 12;

fn encode_frame_header(payload_len: u32) -> [u8; FRAME_HEADER_LEN] {
    let mut h = [0u8; FRAME_HEADER_LEN];
    h[0..4].copy_from_slice(&FRAME_MAGIC);
    h[4] = FRAME_VERSION;
    // h[5] flags, h[6..8] reserved: zero.
    h[8..12].copy_from_slice(&payload_len.to_le_bytes());
    h
}

/// Split a container file into decompressed CHUNK buffers. Returns the chunks
/// plus whether a truncated/corrupt tail was dropped (a crash signature — the
/// complete frames before it are intact). Bytes NOT starting with the frame
/// magic are treated as one legacy whole-file zstd chunk.
pub fn decode_container(bytes: &[u8]) -> std::io::Result<(Vec<Vec<u8>>, bool)> {
    if bytes.len() < FRAME_MAGIC.len() {
        // A crash can leave a fresh `.atrc.active` empty or with a cut-off
        // magic (the header goes out in the same write_all as the payload).
        // No legacy file is ever this short (a zstd frame is >= 9 bytes), so
        // report "no complete chunks" — torn if any bytes exist — instead of
        // failing the whole conversion on a decompression error.
        return Ok((Vec::new(), !bytes.is_empty()));
    }
    if bytes[0..4] != FRAME_MAGIC {
        // Legacy `*.chunk.zst`: the whole file is one zstd frame.
        let chunk = zstd::stream::decode_all(bytes).map_err(std::io::Error::other)?;
        return Ok((vec![chunk], false));
    }
    let mut chunks = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        let rest = &bytes[off..];
        if rest.len() < FRAME_HEADER_LEN || rest[0..4] != FRAME_MAGIC || rest[4] != FRAME_VERSION {
            return Ok((chunks, true));
        }
        let len = u32::from_le_bytes([rest[8], rest[9], rest[10], rest[11]]) as usize;
        let Some(payload) = rest.get(FRAME_HEADER_LEN..FRAME_HEADER_LEN + len) else {
            return Ok((chunks, true));
        };
        match zstd::stream::decode_all(payload) {
            Ok(chunk) => chunks.push(chunk),
            Err(_) => return Ok((chunks, true)),
        }
        off += FRAME_HEADER_LEN + len;
    }
    Ok((chunks, false))
}

/// The currently-open container file frames are appended to.
struct ActiveFile {
    file: std::fs::File,
    active_path: PathBuf,
    final_path: PathBuf,
    bytes: u64,
    opened_at: Instant,
}

/// Appends compressed CHUNK frames into rotating container files in the trace
/// dir and enforces the per-host retention cap. Not internally synchronized;
/// the drain thread owns one.
pub struct FileSink {
    cfg: SinkConfig,
    seq: u64,
    active: Option<ActiveFile>,
    /// Records lost to the jam failsafe / failed appends, carried into the
    /// next successfully-written chunk's `dropped` count (ring drop-newest
    /// accounting, extended to the file layer).
    carried_dropped: u64,
}

impl FileSink {
    /// Create the sink, ensuring the trace dir exists. The container file is
    /// created lazily on the first frame (an idle process leaves no file).
    ///
    /// `file_max_age` is clamped to half the pid-reuse salvage backstop here —
    /// at the sink, not just in `SinkConfig::from_env` — so the "a genuine
    /// writer never looks older than the backstop" invariant holds for
    /// programmatic configs too.
    pub fn new(mut cfg: SinkConfig) -> std::io::Result<FileSink> {
        cfg.file_max_age = cfg.file_max_age.min(SALVAGE_LIVE_PID_AFTER / 2);
        std::fs::create_dir_all(&cfg.dir)?;
        Ok(FileSink {
            cfg,
            seq: 0,
            active: None,
            carried_dropped: 0,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    /// Build, compress, and append one chunk frame to the container file,
    /// rotating it on the size/age policy. Returns the path the frame went to
    /// (the `.active` container, or the finalized file if this append rotated).
    /// Resolvers read the global registries off the hot path.
    pub fn write_chunk(
        &mut self,
        records: &[RingRecord],
        dropped: u64,
        mono_epoch_ns: u64,
        wall_start_realtime_ns: u64,
    ) -> std::io::Result<PathBuf> {
        // Jam failsafe: `.active` lives OUTSIDE the retention cap, so a
        // persistently-failing rotation (appends fine, sync/rename not) must
        // not grow it without bound. Try once to clear the jam; if it stays
        // jammed, drop this frame WITH accounting (drop-newest, like the
        // rings) — the count surfaces in the next written chunk.
        let over_ceiling = self
            .active
            .as_ref()
            .is_some_and(|a| a.bytes >= self.cfg.file_max_bytes.saturating_mul(4));
        if over_ceiling && self.rotate().is_err() {
            self.carried_dropped = self
                .carried_dropped
                .saturating_add(records.len() as u64)
                .saturating_add(dropped);
            return Err(std::io::Error::other(
                "trace container rotation jammed; frame dropped",
            ));
        }

        let prov = self
            .cfg
            .provenance(self.seq, dropped.saturating_add(self.carried_dropped));
        let bytes = chunk::build_chunk(
            &prov,
            mono_epoch_ns,
            wall_start_realtime_ns,
            records,
            |id| scope::scope_name(id).unwrap_or_default(),
            intern::event_name,
            intern::thread_info,
        );
        let compressed = zstd::stream::encode_all(&bytes[..], self.cfg.compression_level)
            .map_err(std::io::Error::other)?;

        if self.active.is_none() {
            // Name the file by its FIRST frame's provenance (same 5-field
            // scheme as the legacy format, new extension).
            let fname = format!(
                "{}_{}_{}_{:020}_{:08}.atrc",
                sanitize(&self.cfg.node_id),
                sanitize(&self.cfg.process_name),
                self.cfg.pid,
                wall_start_realtime_ns,
                self.seq,
            );
            let final_path = self.cfg.dir.join(&fname);
            let active_path = self.cfg.dir.join(format!("{fname}.active"));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&active_path)?;
            self.active = Some(ActiveFile {
                file,
                active_path,
                final_path,
                bytes: 0,
                opened_at: Instant::now(),
            });
        }

        // One contiguous write per frame: a crash mid-write leaves a truncated
        // tail that `decode_container` detects and drops; prior frames survive
        // (page cache is kernel-owned — a process crash loses nothing written).
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + compressed.len());
        frame.extend_from_slice(&encode_frame_header(compressed.len() as u32));
        frame.extend_from_slice(&compressed);
        let active = self.active.as_mut().expect("active container file");
        use std::io::Write as _;
        if let Err(err) = active.file.write_all(&frame) {
            // The chunk (with the carried count baked in) never hit the disk:
            // absorb everything back so the NEXT chunk reports it.
            self.carried_dropped = self
                .carried_dropped
                .saturating_add(records.len() as u64)
                .saturating_add(dropped);
            return Err(err);
        }
        self.carried_dropped = 0;
        active.bytes += frame.len() as u64;
        self.seq = self.seq.wrapping_add(1);

        let rotate = active.bytes >= self.cfg.file_max_bytes
            || active.opened_at.elapsed() >= self.cfg.file_max_age;
        let written_to = if rotate {
            self.rotate()?
                .expect("rotate with an active file returns its final path")
        } else {
            self.active
                .as_ref()
                .expect("active container file")
                .active_path
                .clone()
        };
        Ok(written_to)
    }

    /// Age-based rotation hook for the drain loop: finalize the container if it
    /// has exceeded `file_max_age` even with no new frames arriving, so a live
    /// `.active` is never older than the rotation age (which is what lets the
    /// reaper treat much-older `.active` files as crash orphans).
    pub fn tick(&mut self) {
        let expired = self
            .active
            .as_ref()
            .is_some_and(|a| a.opened_at.elapsed() >= self.cfg.file_max_age);
        if expired {
            let _ = self.rotate();
        }
    }

    /// Finalize the active container: fsync, then atomically rename to the
    /// final name. Returns the finalized path (None if there was no active
    /// file). fsync here (not per frame) is deliberate: page cache already
    /// survives process crashes; fsync only buys power-loss durability, so we
    /// pay it once per rotation instead of once per second (eMMC endurance).
    fn rotate(&mut self) -> std::io::Result<Option<PathBuf>> {
        let Some(active) = self.active.take() else {
            return Ok(None);
        };
        // Keep ownership until every fallible step succeeds: on ENOSPC/EIO
        // the container must remain OUR `.active` (naturally retried by the
        // next size/age rotation, tick, or Drop) instead of becoming a
        // live-pid orphan that only the 24h backstop would reclaim.
        // Sweep on the error path too, so an over-cap dir can still free
        // space for the retry.
        if let Err(err) = active.file.sync_all() {
            self.active = Some(active);
            self.sweep();
            return Err(err);
        }
        if let Err(err) = std::fs::rename(&active.active_path, &active.final_path) {
            if err.kind() == std::io::ErrorKind::NotFound {
                // Lost ownership: something renamed/removed our `.active`
                // (e.g. a clock-step-confused salvager on an RTC-less boot,
                // or manual cleanup). The fd's frames live on under whatever
                // name the inode has now — but retrying this rename forever
                // would pin us to a container we no longer own (and, if the
                // renamed file is later evicted, append into an unlinked
                // inode for the rest of the process lifetime). Reset and
                // start a fresh container instead.
                drop(active);
                self.sweep();
                return Err(err);
            }
            self.active = Some(active);
            self.sweep();
            return Err(err);
        }
        let final_path = active.final_path.clone();
        drop(active);
        self.sweep();
        Ok(Some(final_path))
    }

    /// Per-host disk ring buffer: if finalized chunks exceed the cap, unlink the
    /// oldest (by the `wall_ts` filename field) until within budget. Tolerant of
    /// concurrent sweeps by other processes. Also reaps stale `*.active` files
    /// left by a crashed writer so they cannot leak storage outside the cap.
    pub fn sweep(&self) {
        self.reap_stale_active();
        let mut entries = match scan_chunks(&self.cfg.dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        let mut total: u64 = entries.iter().map(|e| e.size).sum();
        if total <= self.cfg.max_bytes {
            return;
        }
        // Oldest first.
        entries.sort_by_key(|e| (e.wall_ts, e.seq));
        for e in entries {
            if total <= self.cfg.max_bytes {
                break;
            }
            match std::fs::remove_file(&e.path) {
                // We freed it.
                Ok(()) => total = total.saturating_sub(e.size),
                // Another sweeper already removed it: it is gone, so it no longer
                // counts toward `total`. Subtract too, otherwise we would keep
                // deleting further (still-valid) files and over-evict.
                Err(ref err) if err.kind() == std::io::ErrorKind::NotFound => {
                    total = total.saturating_sub(e.size)
                }
                // Some other error (perms, etc.): leave it and stop trying it.
                Err(_) => {}
            }
        }
    }

    /// Handle crash-orphaned `*.active` files.
    ///
    /// - `*.atrc.active`: SALVAGE — rename to the final name once the writer
    ///   is provably gone, keyed on the **writer pid encoded in the filename**
    ///   (the trace dir is per-host local storage, so the writer is local by
    ///   design). A dead pid can never append again → salvage immediately
    ///   after a crash; a live pid is NEVER touched — age-based guesses would
    ///   mis-salvage a live writer running with a larger
    ///   `AORTA_TRACE_FILE_MAX_SECS` than ours. Unparseable names fall
    ///   back to a conservative fixed age. Complete frames are valid; a
    ///   truncated tail is dropped at read time.
    /// - legacy `*.chunk.zst.active`: delete after [`STALE_ACTIVE`] (the old
    ///   write+rename window was milliseconds; these hold one possibly-partial
    ///   chunk).
    fn reap_stale_active(&self) {
        let now = std::time::SystemTime::now();
        let entries = match std::fs::read_dir(&self.cfg.dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let age = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|mtime| now.duration_since(mtime).ok());
            let Some(age) = age else { continue };
            if name.ends_with(".atrc.active") {
                let ours = self.active.as_ref().is_some_and(|a| a.active_path == path);
                if ours {
                    continue;
                }
                let salvage = match active_writer_pid(name) {
                    // Dead writer: salvage immediately. Live pid + an mtime far
                    // beyond any permitted rotation age = PID REUSE (a genuine
                    // writer's container refreshes its mtime on every append
                    // and is tick-rotated within file_max_age, which from_env
                    // clamps to SALVAGE_LIVE_PID_AFTER/2) — without this
                    // fallback a reused pid would pin the orphan outside the
                    // retention cap forever.
                    Some(pid) => !pid_alive(pid) || age >= SALVAGE_LIVE_PID_AFTER,
                    None => age >= SALVAGE_UNKNOWN_WRITER_AFTER,
                };
                if salvage {
                    let final_path = path.with_extension(""); // strip ".active"
                    let _ = std::fs::rename(&path, &final_path);
                }
            } else if name.ends_with(".chunk.zst.active") && age >= STALE_ACTIVE {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

/// Parse the writer pid from `<node>_<proc>_<pid>_<wall_ts>_<seq>.atrc.active`
/// (right-anchored, same as [`parse_chunk_name`]).
fn active_writer_pid(name: &str) -> Option<u32> {
    let stem = name.strip_suffix(".atrc.active")?;
    let mut it = stem.rsplit('_');
    let _seq = it.next()?;
    let _wall_ts = it.next()?;
    it.next()?.parse().ok()
}

/// Whether `pid` is alive on THIS host **as a process** (`kill(pid, 0)`;
/// `EPERM` = alive but not ours). Pid 0 / unrepresentable values are treated
/// as alive (never probe process groups, never salvage on a parse artifact).
///
/// `kill(2)` succeeds for ANY live task id — including a recycled THREAD id
/// (the tid pool is the pid pool, and threads churn far faster than
/// processes). Verified on-robot: a dead writer's orphan stayed pinned for
/// the full 24 h backstop because its old pid had become another process's
/// zenoh rx THREAD. A writer is a process, so on Linux a positive kill probe
/// must also prove the task is a thread-group LEADER (`Tgid == pid`).
fn pid_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    if pid <= 0 {
        return true;
    }
    // SAFETY: signal 0 performs error checking only; no signal is delivered.
    let rc = unsafe { libc::kill(pid, 0) };
    let task_alive = rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
    task_alive && thread_group_leader(pid)
}

/// Is the live task `pid` a thread-group leader (a real process)? Unreadable
/// or unparsable `/proc` stays conservative (`true` = alive, never salvage on
/// uncertainty — the 24 h backstop still applies).
#[cfg(target_os = "linux")]
fn thread_group_leader(pid: libc::pid_t) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(status) => status
            .lines()
            .find_map(|line| line.strip_prefix("Tgid:"))
            .and_then(|v| v.trim().parse::<libc::pid_t>().ok())
            .is_none_or(|tgid| tgid == pid),
        Err(_) => true,
    }
}

/// Non-Linux (dev hosts): no cheap tid/pid distinction — keep the kill-probe
/// answer (the 24 h backstop covers reuse).
#[cfg(not(target_os = "linux"))]
fn thread_group_leader(_pid: libc::pid_t) -> bool {
    true
}

/// Fallback salvage age for `.atrc.active` files whose name does not parse to
/// a writer pid (foreign/hand-made files): conservative, well beyond any sane
/// `AORTA_TRACE_FILE_MAX_SECS`.
const SALVAGE_UNKNOWN_WRITER_AFTER: Duration = Duration::from_secs(3600);

/// Pid-reuse backstop: a `.atrc.active` whose named pid is alive but whose
/// mtime is this old cannot belong to that pid (live writers append/rotate
/// well within `file_max_age`, clamped to half of this) — the original writer
/// died and the OS recycled its pid. 24 h.
const SALVAGE_LIVE_PID_AFTER: Duration = Duration::from_secs(24 * 3600);

impl Drop for FileSink {
    /// Best-effort finalize so a clean shutdown (drain loop exit / atexit)
    /// leaves no `.active` behind. Crash paths are covered by the reaper's
    /// salvage rename instead.
    fn drop(&mut self) {
        let _ = self.rotate();
    }
}

/// A `*.active` file older than this is treated as a crash orphan and reaped.
/// Far larger than any real write+rename window (milliseconds).
const STALE_ACTIVE: Duration = Duration::from_secs(60);

struct ChunkFile {
    path: PathBuf,
    size: u64,
    wall_ts: u64,
    seq: u64,
}

/// List finalized `*.chunk.zst` files (skipping `*.active`) with parsed
/// provenance for retention ordering.
fn scan_chunks(dir: &Path) -> std::io::Result<Vec<ChunkFile>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        // Finalized container files + legacy single-chunk files; `.active`
        // files match neither suffix and are skipped.
        if !name.ends_with(".atrc") && !name.ends_with(".chunk.zst") {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        let (wall_ts, seq) = parse_chunk_name(name);
        out.push(ChunkFile {
            path,
            size,
            wall_ts,
            seq,
        });
    }
    Ok(out)
}

/// Extract `(wall_ts, seq)` from
/// `<node>_<proc>_<pid>_<wall_ts>_<seq>.{atrc|chunk.zst}`. Parsed from the
/// RIGHT so underscores in node/process names cannot confuse it (node/process
/// are also sanitized on write). Unparseable → `(0, 0)` (sorts oldest, so it
/// is evicted first — fail-safe for retention).
fn parse_chunk_name(name: &str) -> (u64, u64) {
    let stem = name
        .strip_suffix(".atrc")
        .or_else(|| name.strip_suffix(".chunk.zst"))
        .unwrap_or(name);
    let mut it = stem.rsplit('_');
    let seq = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let wall_ts = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (wall_ts, seq)
}

/// Replace filename-unsafe chars (incl. the `_` field separator) so the 5-field
/// chunk filename parses unambiguously.
fn sanitize(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "x".to_string()
    } else {
        cleaned
    }
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(p) = s.strip_suffix(['G', 'g']) {
        (p, 1024 * 1024 * 1024)
    } else if let Some(p) = s.strip_suffix(['M', 'm']) {
        (p, 1024 * 1024)
    } else if let Some(p) = s.strip_suffix(['K', 'k']) {
        (p, 1024)
    } else {
        (s, 1)
    };
    num.trim().parse::<u64>().ok().map(|n| n * mult)
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn default_node_id() -> String {
    // libc gethostname; fall back to a stable placeholder.
    let mut buf = [0u8; 256];
    // SAFETY: writes up to `buf.len()` bytes into `buf` and NUL-terminates.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        if let Ok(s) = std::str::from_utf8(&buf[..end]) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    "node".to_string()
}

fn default_process_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "process".to_string())
}

/// Drain-loop cadence + chunk sizing.
#[derive(Debug, Clone)]
pub struct DrainConfig {
    /// How often the drain thread sweeps the rings.
    pub interval: Duration,
    /// Flush a chunk once this many records have accumulated (bounds chunk size).
    pub chunk_max_records: usize,
    /// Flush a partial chunk after this long (bounds chunk latency).
    pub chunk_max_age: Duration,
}

impl Default for DrainConfig {
    fn default() -> Self {
        DrainConfig {
            interval: Duration::from_millis(5),
            chunk_max_records: 16384,
            chunk_max_age: Duration::from_secs(1),
        }
    }
}

impl DrainConfig {
    /// Build from the environment: `AORTA_TRACE_SWEEP_MS` (ring sweep cadence),
    /// `AORTA_TRACE_CHUNK_RECORDS` (chunk flush size),
    /// `AORTA_TRACE_CHUNK_AGE_MS` (chunk flush age = the crash-loss window for
    /// in-process data). Unset/unparsable fields keep the defaults.
    pub fn from_env() -> DrainConfig {
        let d = DrainConfig::default();
        let ms = |key: &str| {
            std::env::var(key)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .filter(|&v| v > 0)
                .map(Duration::from_millis)
        };
        DrainConfig {
            interval: ms("AORTA_TRACE_SWEEP_MS").unwrap_or(d.interval),
            chunk_max_records: std::env::var("AORTA_TRACE_CHUNK_RECORDS")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(d.chunk_max_records),
            chunk_max_age: ms("AORTA_TRACE_CHUNK_AGE_MS").unwrap_or(d.chunk_max_age),
        }
    }
}

/// Owns the drain thread. Dropping (or [`Tracer::shutdown`]) stops it after a
/// final flush, so a short-lived process still persists its tail chunk.
pub struct Tracer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Tracer {
    /// Spawn the drain thread for `sink`.
    pub fn start(sink: FileSink, cfg: DrainConfig) -> Tracer {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("aorta-trace-drain".to_string())
            .spawn(move || drain_loop(sink, cfg, stop_thread))
            .expect("spawn aorta-trace-drain");
        Tracer {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop the drain thread after a final flush (idempotent).
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Tracer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn drain_loop(mut sink: FileSink, cfg: DrainConfig, stop: Arc<AtomicBool>) {
    let mut buf: Vec<RingRecord> = Vec::with_capacity(cfg.chunk_max_records);
    let mut dropped: u64 = 0;
    let mut last_flush = Instant::now();
    loop {
        std::thread::sleep(cfg.interval);
        dropped += ring::drain_all(&mut buf);
        let stopping = stop.load(Ordering::Acquire);
        let flush_now = buf.len() >= cfg.chunk_max_records
            || (!buf.is_empty() && last_flush.elapsed() >= cfg.chunk_max_age)
            || (stopping && !buf.is_empty());
        if flush_now {
            flush(&mut sink, &buf, dropped);
            buf.clear();
            dropped = 0;
            last_flush = Instant::now();
        }
        // Age-rotate the container even when idle, so a live `.active` never
        // outlives the rotation age (the reaper's salvage threshold relies on
        // this).
        sink.tick();
        if stopping {
            // Drain to quiescence: producers can append between drains during
            // shutdown, so keep draining (with the accumulated dropped count)
            // until two consecutive empty passes — bounded so a runaway producer
            // cannot wedge exit.
            let mut empties = 0;
            while empties < 2 {
                buf.clear();
                dropped += ring::drain_all(&mut buf);
                if buf.is_empty() && dropped == 0 {
                    empties += 1;
                    std::thread::sleep(Duration::from_millis(1));
                } else {
                    empties = 0;
                    flush(&mut sink, &buf, dropped);
                    dropped = 0;
                }
            }
            break;
        }
    }
}

fn flush(sink: &mut FileSink, buf: &[RingRecord], dropped: u64) {
    if buf.is_empty() && dropped == 0 {
        return;
    }
    let mono_epoch_ns = buf
        .iter()
        .map(|r| r.mono_ns)
        .min()
        .unwrap_or_else(monotonic_ns);
    // Anchor the epoch to wall time: extrapolate the realtime instant that
    // corresponds to mono_epoch_ns from a single near-simultaneous sample.
    let mono_now = monotonic_ns();
    let wall_now = unix_time_ns();
    let wall_start = wall_now.saturating_sub(mono_now.saturating_sub(mono_epoch_ns));
    let _ = sink.write_chunk(buf, dropped, mono_epoch_ns, wall_start);
}

/// Process-global tracer (installed by the runtime init, `trace::runtime::init`).
static GLOBAL: OnceLock<Mutex<Option<Tracer>>> = OnceLock::new();

fn global() -> &'static Mutex<Option<Tracer>> {
    GLOBAL.get_or_init(|| Mutex::new(None))
}

/// Install the process-global drain thread (idempotent: a second call is a
/// no-op while one is already running).
pub fn install(sink: FileSink, cfg: DrainConfig) {
    let mut g = global().lock().expect("trace tracer lock");
    if g.is_some() {
        return;
    }
    *g = Some(Tracer::start(sink, cfg));
}

/// Shut down the process-global drain thread, flushing the tail (idempotent).
pub fn shutdown_global() {
    if let Some(mut t) = global().lock().expect("trace tracer lock").take() {
        t.shutdown();
    }
}

#[cfg(test)]
#[path = "../../test/trace_sink_tests.rs"]
mod tests;
