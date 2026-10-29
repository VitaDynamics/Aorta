//! File sink + container framing + retention + drain-thread tests
//! (`core/src/trace/sink.rs`).

use super::{decode_container, DrainConfig, FileSink, SinkConfig, Tracer};
use crate::trace::chunk::{root_as_chunk, ChunkEventType, RingRecord};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static UNIQ: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = UNIQ.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!(
        "aorta-trace-test-{}-{}-{}",
        tag,
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(dir: PathBuf, max_bytes: u64) -> SinkConfig {
    SinkConfig {
        dir,
        max_bytes,
        node_id: "nodeA".to_string(),
        run_id: "run1".to_string(),
        process_name: "proc".to_string(),
        pid: 123,
        host_clock_domain_id: 9,
        compression_level: 1,
        file_max_bytes: super::DEFAULT_FILE_MAX_BYTES,
        file_max_age: super::DEFAULT_FILE_MAX_AGE,
    }
}

fn point(scope: u16, event: u16, mono: u64) -> RingRecord {
    RingRecord {
        scope_id: scope,
        event_id: event,
        mono_ns: mono,
        value: 1,
        event_type: ChunkEventType::Instant as u8,
        ..RingRecord::default()
    }
}

/// Decode every chunk in a container (or legacy) file.
fn read_chunks(path: &Path) -> (Vec<Vec<u8>>, bool) {
    let raw = std::fs::read(path).unwrap();
    decode_container(&raw).unwrap()
}

/// The single finalized `.atrc` file in `dir`.
fn sole_atrc(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".atrc"))
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one finalized .atrc");
    found.pop().unwrap()
}

#[test]
fn frames_append_into_one_container_until_finalized() {
    let dir = temp_dir("container");
    let mut sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    let scope = crate::trace::intern_scope("sink.test.scope");
    let ev = crate::trace::intern_event("sink.test.event");

    // Three flushes → three frames appended to ONE .active container.
    for i in 0..3u64 {
        let recs = vec![point(scope, ev, 1000 + i), point(scope, ev, 2000 + i)];
        let path = sink
            .write_chunk(&recs, i, 1000 + i, 1_700_000_000_000_000_000 + i)
            .unwrap();
        assert!(
            path.to_string_lossy().ends_with(".atrc.active"),
            "frames append to the active container, got {path:?}"
        );
    }
    // Clean shutdown finalizes (atomic rename, no .active left behind).
    drop(sink);
    let final_path = sole_atrc(&dir);
    assert!(!final_path.with_extension("atrc.active").exists());

    let (chunks, truncated) = read_chunks(&final_path);
    assert!(!truncated);
    assert_eq!(chunks.len(), 3, "one frame per flush");
    // Chunks decode independently with their own provenance + interning.
    for (i, raw) in chunks.iter().enumerate() {
        let chunk = root_as_chunk(raw).unwrap();
        assert_eq!(chunk.node_id(), Some("nodeA"));
        assert_eq!(chunk.pid(), 123);
        assert_eq!(chunk.seq(), i as u64);
        assert_eq!(chunk.dropped(), i as u64);
        assert_eq!(chunk.records().unwrap().len(), 2);
        let r0 = chunk.records().unwrap().get(0);
        assert_eq!(
            chunk.scopes().unwrap().get(r0.scope_iid() as usize),
            "sink.test.scope"
        );
        assert_eq!(
            chunk.events().unwrap().get(r0.event_iid() as usize),
            "sink.test.event"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rotation_by_size_starts_a_new_container() {
    let dir = temp_dir("rotate-size");
    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_bytes = 1; // every frame exceeds the budget → rotate per frame
    let mut sink = FileSink::new(config).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");

    let p1 = sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 10).unwrap();
    let p2 = sink.write_chunk(&[point(scope, ev, 2)], 0, 2, 20).unwrap();
    // Rotation happened inline: write_chunk returned the FINALIZED paths.
    assert!(p1.to_string_lossy().ends_with(".atrc"), "{p1:?}");
    assert!(p2.to_string_lossy().ends_with(".atrc"), "{p2:?}");
    assert_ne!(p1, p2);
    assert_eq!(read_chunks(&p1).0.len(), 1);
    assert_eq!(read_chunks(&p2).0.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tick_rotates_an_aged_container_without_new_frames() {
    let dir = temp_dir("rotate-age");
    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_age = Duration::from_millis(30);
    let mut sink = FileSink::new(config).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");

    let p = sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 10).unwrap();
    assert!(p.to_string_lossy().ends_with(".atrc.active"));
    sink.tick(); // too young: no-op
    assert!(p.exists());
    std::thread::sleep(Duration::from_millis(40));
    sink.tick(); // aged out: finalize even though no new frame arrived
    assert!(!p.exists(), "active container finalized by tick");
    assert_eq!(read_chunks(&sole_atrc(&dir)).0.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_rotation_keeps_the_active_container_and_retries() {
    let dir = temp_dir("rotate-fail");
    std::fs::create_dir_all(&dir).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");
    // Probe the real frame size, then pick a rotation size just below it:
    // every frame triggers a rotation attempt, while two frames stay under
    // the 4x jam ceiling — exercising the retained-state retry path, not the
    // drop-with-accounting failsafe (separate test).
    let probe_dir = temp_dir("rotate-fail-probe");
    let mut probe = FileSink::new(cfg(probe_dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    let probe_path = probe.write_chunk(&[point(scope, ev, 1)], 0, 1, 42).unwrap();
    let frame_len = std::fs::metadata(&probe_path).unwrap().len();
    drop(probe);
    let _ = std::fs::remove_dir_all(&probe_dir);

    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_bytes = frame_len.saturating_sub(16).max(1);
    let mut sink = FileSink::new(config).unwrap();

    // Block the rename target: the container is named by its FIRST frame's
    // (wall_ts=42, seq=0), so pre-create a DIRECTORY at that final path.
    let final_path = dir.join("nodeA_proc_123_00000000000000000042_00000000.atrc");
    std::fs::create_dir(&final_path).unwrap();

    // Two writes: both append fine, both fail to rotate — and the SAME
    // .active must keep accumulating frames (state retained on error).
    assert!(sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 42).is_err());
    assert!(sink.write_chunk(&[point(scope, ev, 2)], 0, 2, 99).is_err());
    let active_path = dir.join("nodeA_proc_123_00000000000000000042_00000000.atrc.active");
    assert!(active_path.exists(), "container stays ours as .active");

    // Unblock and write once more: the retry finalizes ALL THREE frames.
    std::fs::remove_dir(&final_path).unwrap();
    let p = sink.write_chunk(&[point(scope, ev, 3)], 0, 3, 7).unwrap();
    assert_eq!(p, final_path);
    assert!(!active_path.exists(), "no orphan left behind");
    let (chunks, truncated) = read_chunks(&final_path);
    assert!(!truncated);
    assert_eq!(chunks.len(), 3, "no frame was lost across failed rotations");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn jammed_rotation_drops_frames_with_accounting_instead_of_growing_unbounded() {
    let dir = temp_dir("jam");
    std::fs::create_dir_all(&dir).unwrap();
    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_bytes = 1; // ceiling = 4 bytes: any one frame exceeds it
    let mut sink = FileSink::new(config).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");

    // Jam the rename target.
    let final_path = dir.join("nodeA_proc_123_00000000000000000042_00000000.atrc");
    std::fs::create_dir(&final_path).unwrap();

    // Frame 1 appends, rotation jams (state retained).
    assert!(sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 42).is_err());
    // Frame 2: over the ceiling, jam persists -> frame DROPPED with
    // accounting, .active did not grow.
    let active_path = dir.join("nodeA_proc_123_00000000000000000042_00000000.atrc.active");
    let size_before = std::fs::metadata(&active_path).unwrap().len();
    assert!(sink.write_chunk(&[point(scope, ev, 2)], 0, 2, 50).is_err());
    assert_eq!(
        std::fs::metadata(&active_path).unwrap().len(),
        size_before,
        "a jammed container must not keep growing"
    );

    // Unjam: the next write clears the jam (finalizing frame 1), then writes
    // its own frame whose chunk REPORTS the dropped one.
    std::fs::remove_dir(&final_path).unwrap();
    sink.write_chunk(&[point(scope, ev, 3)], 0, 3, 77).unwrap();
    drop(sink);

    let (chunks1, _) = read_chunks(&final_path);
    assert_eq!(chunks1.len(), 1);
    assert_eq!(root_as_chunk(&chunks1[0]).unwrap().dropped(), 0);
    let second = dir.join("nodeA_proc_123_00000000000000000077_00000001.atrc");
    let (chunks2, _) = read_chunks(&second);
    assert_eq!(chunks2.len(), 1);
    assert_eq!(
        root_as_chunk(&chunks2[0]).unwrap().dropped(),
        1,
        "the jam-dropped frame is accounted in the next written chunk"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn losing_the_active_file_resets_to_a_fresh_container() {
    let dir = temp_dir("lost-ownership");
    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_age = Duration::from_millis(30);
    let mut sink = FileSink::new(config).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");

    let p = sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 10).unwrap();
    assert!(p.to_string_lossy().ends_with(".atrc.active"));
    // Someone takes our .active away (clock-step-confused salvager, manual rm).
    std::fs::remove_file(&p).unwrap();
    std::thread::sleep(Duration::from_millis(40));
    sink.tick(); // rotate -> rename ENOENT -> ownership reset (no retry loop)

    // New frames flow into a FRESH container, which finalizes normally.
    let p2 = sink.write_chunk(&[point(scope, ev, 2)], 0, 2, 20).unwrap();
    assert!(p2.to_string_lossy().ends_with(".atrc.active"));
    assert_ne!(p, p2, "a fresh container, not the lost one");
    drop(sink);
    let finals: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|q| q.to_string_lossy().ends_with(".atrc"))
        .collect();
    assert_eq!(finals.len(), 1, "the fresh container finalized");
    assert_eq!(read_chunks(&finals[0]).0.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn truncated_tail_is_dropped_but_prior_frames_survive() {
    let dir = temp_dir("trunc");
    let mut sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");
    sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 10).unwrap();
    sink.write_chunk(&[point(scope, ev, 2)], 0, 2, 20).unwrap();
    drop(sink);
    let path = sole_atrc(&dir);

    // Simulate a crash mid-append: a frame header promising more bytes than
    // exist (the kill landed inside write_all).
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"ATCF");
    bytes.push(super::FRAME_VERSION);
    bytes.extend_from_slice(&[0u8; 3]);
    bytes.extend_from_slice(&(1_000_000u32).to_le_bytes());
    bytes.extend_from_slice(&[0xAB; 16]); // far short of the promised payload
    let (chunks, truncated) = decode_container(&bytes).unwrap();
    assert!(truncated, "the torn tail frame must be reported");
    assert_eq!(chunks.len(), 2, "complete frames before the tear survive");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_single_chunk_files_still_decode() {
    let dir = temp_dir("legacy");
    std::fs::create_dir_all(&dir).unwrap();
    // A legacy `.chunk.zst` is one bare zstd frame with no container header.
    let payload = b"legacy chunk payload bytes";
    let compressed = zstd::stream::encode_all(&payload[..], 1).unwrap();
    let (chunks, truncated) = decode_container(&compressed).unwrap();
    assert!(!truncated);
    assert_eq!(chunks, vec![payload.to_vec()]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn filename_encodes_provenance_for_glob_retrieval() {
    let dir = temp_dir("name");
    let mut config = cfg(dir.clone(), super::DEFAULT_MAX_BYTES);
    config.file_max_bytes = 1; // finalize per frame so the name is observable
    let mut sink = FileSink::new(config).unwrap();
    let scope = crate::trace::intern_scope("s");
    let ev = crate::trace::intern_event("e");
    let path = sink.write_chunk(&[point(scope, ev, 1)], 0, 1, 42).unwrap();
    let name = path.file_name().unwrap().to_string_lossy();
    // <node>_<proc>_<pid>_<wall_ts>_<seq>.atrc — wall_ts/seq of the FIRST frame.
    assert!(name.starts_with("nodeA_proc_123_"));
    let stem = name.strip_suffix(".atrc").unwrap();
    let fields: Vec<&str> = stem.split('_').collect();
    assert_eq!(fields.len(), 5, "exactly five underscore fields");
    assert_eq!(fields[3].parse::<u64>().unwrap(), 42, "wall_ts field");
    assert_eq!(fields[4].parse::<u64>().unwrap(), 0, "seq field");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn retention_evicts_oldest_until_under_cap_across_formats() {
    let dir = temp_dir("retain");
    std::fs::create_dir_all(&dir).unwrap();
    // Mixed fleet: 5 legacy chunks + 5 containers, 1000 bytes each, ascending
    // wall_ts (legacy older).
    for seq in 0u64..5 {
        let name = format!("nodeA_proc_123_{:020}_{:08}.chunk.zst", 1000 + seq, seq);
        std::fs::write(dir.join(name), vec![0u8; 1000]).unwrap();
    }
    for seq in 5u64..10 {
        let name = format!("nodeA_proc_123_{:020}_{:08}.atrc", 1000 + seq, seq);
        std::fs::write(dir.join(name), vec![0u8; 1000]).unwrap();
    }
    // Cap = 3500 bytes → at most 3 files (3000) may remain.
    let sink = FileSink::new(cfg(dir.clone(), 3500)).unwrap();
    sink.sweep();

    let mut remaining: Vec<u64> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            let stem = n
                .strip_suffix(".chunk.zst")
                .or_else(|| n.strip_suffix(".atrc"))?
                .to_string();
            stem.rsplit('_').next()?.parse::<u64>().ok()
        })
        .collect();
    remaining.sort_unstable();
    // The survivors are the NEWEST regardless of format.
    assert_eq!(remaining, vec![7, 8, 9]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sweep_never_touches_live_writer_active_files() {
    let dir = temp_dir("active");
    std::fs::create_dir_all(&dir).unwrap();
    let our_pid = std::process::id();
    // A fresh legacy .active + a LIVE-writer container .active (named with a
    // pid that is alive — ours) + one finalized file; cap tiny.
    std::fs::write(
        dir.join("nodeA_proc_123_00000000000000000001_00000000.chunk.zst.active"),
        vec![0u8; 10_000],
    )
    .unwrap();
    let live = dir.join(format!(
        "nodeA_proc_{our_pid}_00000000000000000002_00000001.atrc.active"
    ));
    std::fs::write(&live, vec![0u8; 10_000]).unwrap();
    backdate(&live, 2 * 3600); // age below the pid-reuse backstop is irrelevant
    std::fs::write(
        dir.join("nodeA_proc_123_00000000000000000003_00000002.atrc"),
        vec![0u8; 100],
    )
    .unwrap();
    let sink = FileSink::new(cfg(dir.clone(), 50)).unwrap();
    sink.sweep();
    // Fresh legacy .active: below the age threshold. Live-writer container
    // .active: its pid is alive, so it is never counted, removed, or salvaged
    // — even a day old (a writer may run a larger rotation age than ours).
    assert!(dir
        .join("nodeA_proc_123_00000000000000000001_00000000.chunk.zst.active")
        .exists());
    assert!(
        live.exists(),
        "live writer's container must not be salvaged"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dead_writer_container_is_salvaged_immediately() {
    let dir = temp_dir("salvage");
    std::fs::create_dir_all(&dir).unwrap();
    // 99_999_999 exceeds pid_max on Linux and macOS → provably dead writer.
    let orphan = dir.join("nodeA_proc_99999999_00000000000000000005_00000000.atrc.active");
    std::fs::write(&orphan, b"frames-from-a-crashed-process").unwrap();
    // No backdating: a dead pid can never append again, salvage is immediate.

    let sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    sink.sweep();

    assert!(!orphan.exists(), "orphan .active was salvaged");
    let salvaged = dir.join("nodeA_proc_99999999_00000000000000000005_00000000.atrc");
    assert!(salvaged.exists(), "renamed to a finalized container");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pid_reuse_backstop_salvages_day_old_live_pid_container() {
    let dir = temp_dir("salvage-reuse");
    std::fs::create_dir_all(&dir).unwrap();
    // The named pid is ALIVE (ours — standing in for an OS-recycled pid), but
    // the mtime is a day old: no genuine writer can look like this (appends
    // refresh mtime; tick rotates within file_max_age <= backstop/2).
    let our_pid = std::process::id();
    let orphan = dir.join(format!(
        "nodeA_proc_{our_pid}_00000000000000000006_00000000.atrc.active"
    ));
    std::fs::write(&orphan, b"frames-from-a-crashed-then-recycled-pid").unwrap();
    backdate(&orphan, 25 * 3600);

    let sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    sink.sweep();

    assert!(!orphan.exists(), "pid-reuse backstop salvaged the orphan");
    assert!(dir
        .join(format!(
            "nodeA_proc_{our_pid}_00000000000000000006_00000000.atrc"
        ))
        .exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn short_crash_artifacts_decode_to_zero_chunks_not_errors() {
    // Crash before the first frame header completed: empty or sub-magic file.
    let (chunks, truncated) = decode_container(b"").unwrap();
    assert!(chunks.is_empty());
    assert!(!truncated, "an empty file lost nothing");
    let (chunks, truncated) = decode_container(b"AT").unwrap();
    assert!(chunks.is_empty());
    assert!(truncated, "a cut-off magic is a torn artifact");
}

#[test]
fn unparseable_active_name_salvaged_only_after_fallback_age() {
    let dir = temp_dir("salvage-unknown");
    std::fs::create_dir_all(&dir).unwrap();
    let fresh = dir.join("garbage-no-pid-fields.atrc.active");
    std::fs::write(&fresh, b"x").unwrap();
    let old = dir.join("old-garbage-no-pid-fields.atrc.active");
    std::fs::write(&old, b"x").unwrap();
    backdate(&old, 2 * 3600); // beyond SALVAGE_UNKNOWN_WRITER_AFTER

    let sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
    sink.sweep();

    assert!(fresh.exists(), "fresh unparseable .active is left alone");
    assert!(!old.exists(), "hour-old unparseable .active is salvaged");
    assert!(dir.join("old-garbage-no-pid-fields.atrc").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Set a file's mtime `secs` into the past (libc utimes; test-only).
fn backdate(path: &Path, secs: i64) {
    use std::os::unix::ffi::OsStrExt as _;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let t = libc::timeval {
        tv_sec: now - secs,
        tv_usec: 0,
    };
    let times = [t, t];
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: valid NUL-terminated path + a 2-element timeval array.
    let rc = unsafe { libc::utimes(cpath.as_ptr(), times.as_ptr()) };
    assert_eq!(rc, 0, "utimes failed");
}

#[test]
fn drain_thread_persists_spans_and_flushes_on_shutdown() {
    crate::trace_test_support::guarded(|| {
        crate::trace::set_trace_enabled(true);
        // Clear any stragglers from prior tests on pooled threads.
        let mut scratch = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);

        let dir = temp_dir("drain");
        let sink = FileSink::new(cfg(dir.clone(), super::DEFAULT_MAX_BYTES)).unwrap();
        let mut tracer = Tracer::start(
            sink,
            DrainConfig {
                interval: std::time::Duration::from_millis(2),
                chunk_max_records: 1_000_000,
                chunk_max_age: std::time::Duration::from_millis(10),
            },
        );

        let scope = crate::trace::intern_scope("sink.drain.scope");
        let ev = crate::trace::intern_event("sink.drain.span");
        for _ in 0..5 {
            let t = crate::trace::span_begin(scope, ev, 2, 0);
            crate::trace::span_end(t);
        }
        // shutdown() does a final drain + flush + join; dropping the sink in
        // the drain thread finalizes the container.
        tracer.shutdown();

        // Find our records across whatever container(s) were written.
        let mut found = 0usize;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if !path.to_string_lossy().ends_with(".atrc") {
                continue;
            }
            let (chunks, truncated) = read_chunks(&path);
            assert!(!truncated);
            for raw in &chunks {
                let chunk = root_as_chunk(raw).unwrap();
                let scopes = chunk.scopes().unwrap();
                let recs = chunk.records().unwrap();
                for i in 0..recs.len() {
                    let r = recs.get(i);
                    if scopes.get(r.scope_iid() as usize) == "sink.drain.scope" {
                        found += 1;
                    }
                }
            }
        }
        // 5 spans → 5 begin + 5 end records.
        assert_eq!(found, 10, "all span records reached disk");
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// `kill(pid, 0)` succeeds for ANY live task — including a THREAD id from the
/// shared tid/pid pool. A dead writer whose pid was recycled as some process's
/// thread must still salvage immediately (found on-robot: the orphan sat
/// pinned behind the 24 h backstop because tid 3453 had become a zenoh rx
/// thread of another process).
#[test]
#[cfg(target_os = "linux")]
fn a_recycled_thread_id_does_not_count_as_a_live_writer() {
    let (tid_tx, tid_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        // SAFETY: gettid takes no arguments and returns the calling thread id.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) } as u32;
        tid_tx.send(tid).unwrap();
        let _ = done_rx.recv(); // hold the tid alive across the assertions
    });
    let tid = tid_rx.recv().unwrap();
    assert_ne!(tid, std::process::id(), "spawned thread is not the leader");
    assert!(
        super::pid_alive(std::process::id()),
        "our own pid is a live PROCESS"
    );
    assert!(
        !super::pid_alive(tid),
        "a live THREAD id is not a live writer process — it must not pin a \
         dead writer's orphan"
    );
    drop(done_tx);
    holder.join().unwrap();
}
