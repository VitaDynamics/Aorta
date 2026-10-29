//! Pass 0 — private immutable snapshot (MCAP Player Proposal §6.1).
//!
//! A recording that is still being written has the same footer-less signature
//! as a crashed one (§2), and a file-backed mmap of a user-writable file
//! is UB the moment the writer touches it. Pass 0 therefore takes a real byte
//! snapshot: capture the source identity, stream-copy the bytes into a private
//! temp file that is unlinked immediately after creation (fd-backed, vanishes
//! on process death), then re-check the identity. Any change is a hard error.
//! Every later pass reads only these private copies.

use anyhow::{anyhow, Context, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(feature = "fault-injection")]
use std::time::Duration;

/// Fixed copy block. Cancellation is checked once per block (MCAP Player
/// Proposal §6.1), so this also bounds the in-thread cancellation latency of
/// pass 0.
pub(crate) const COPY_BLOCK_BYTES: usize = 1024 * 1024;

#[cfg(all(test, feature = "fault-injection"))]
pub(crate) static TEST_COPY_STALL: std::sync::Mutex<Option<crate::live_tests::ThreadStall>> =
    std::sync::Mutex::new(None);

/// Marker error for a cancelled pass-0 copy, so the caller can map it to
/// `Termination::Interrupted` instead of a failure (MCAP Player Proposal §6.6).
#[derive(Debug)]
pub(crate) struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("snapshot cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// True when `err` is the cancellation marker produced by pass 0.
pub(crate) fn is_cancelled(err: &anyhow::Error) -> bool {
    err.downcast_ref::<Cancelled>().is_some()
}

/// Source-file identity captured before and after the copy (MCAP Player
/// Proposal §6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
}

impl FileIdentity {
    pub(crate) fn of(path: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let meta = fs::metadata(path)
            .with_context(|| format!("cannot stat input '{}'", path.display()))?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.size(),
            mtime_sec: meta.mtime(),
            mtime_nsec: meta.mtime_nsec(),
        })
    }
}

/// One private, unlinked copy of an input file.
#[derive(Debug)]
pub(crate) struct Snapshot {
    pub source: PathBuf,
    /// The unlinked copy. Pass 1 and pass 2 read the *same* fd (MCAP Player
    /// Proposal §6.1).
    pub file: File,
    pub len: u64,
}

/// Free-space probe, injected so the MCAP Player Proposal §6.1 up-front check
/// is testable without filling a real filesystem.
pub(crate) trait SpaceProbe {
    fn available_bytes(&self, dir: &Path) -> Result<u64>;
}

/// Production probe: `statvfs` over the snapshot temp directory.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StatvfsSpaceProbe;

impl SpaceProbe for StatvfsSpaceProbe {
    // `f_bavail` / `f_frsize` are `u64` on Linux but narrower on macOS, so the
    // widening casts are load-bearing on one platform and a no-op on the other.
    #[allow(clippy::unnecessary_cast)]
    fn available_bytes(&self, dir: &Path) -> Result<u64> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(dir.as_os_str().as_bytes())
            .with_context(|| format!("temp path '{}' contains a NUL byte", dir.display()))?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `path` is a valid NUL-terminated C string and `stat` is a
        // properly aligned, owned `statvfs` the callee only writes into.
        let rc = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("statvfs('{}') failed", dir.display()));
        }
        let block_size = if stat.f_frsize != 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };
        Ok((stat.f_bavail as u64).saturating_mul(block_size))
    }
}

/// Expand the CLI inputs into an ordered file list (MCAP Player Proposal §5):
/// plain files are taken as given, directories contribute their
/// **non-recursive** `*.mcap` entries in sorted order.
pub(crate) fn collect_input_files(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if inputs.is_empty() {
        return Err(anyhow!(
            "no input given: pass one or more .mcap files or directories"
        ));
    }

    let mut collected: Vec<PathBuf> = Vec::new();
    for input in inputs {
        let meta = fs::metadata(input)
            .with_context(|| format!("cannot read input '{}'", input.display()))?;
        if meta.is_dir() {
            let mut found: Vec<PathBuf> = Vec::new();
            let entries = fs::read_dir(input)
                .with_context(|| format!("cannot list directory '{}'", input.display()))?;
            for entry in entries {
                let entry = entry
                    .with_context(|| format!("cannot list directory '{}'", input.display()))?;
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|ext| ext == "mcap") {
                    found.push(path);
                }
            }
            if found.is_empty() {
                return Err(anyhow!(
                    "no *.mcap files in directory '{}' (the directory glob is non-recursive)",
                    input.display()
                ));
            }
            // Deterministic order; segment-set ordering by `segment.index` is a
            // pass-1 concern (MCAP Player Proposal §6.1) and overrides this
            // later.
            found.sort();
            collected.extend(found);
        } else {
            collected.push(input.clone());
        }
    }

    // Drop repeats of the same underlying file while preserving first-seen
    // order — replaying one segment twice would corrupt the timeline.
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut unique = Vec::with_capacity(collected.len());
    for path in collected {
        let key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if seen.insert(key) {
            unique.push(path);
        }
    }
    Ok(unique)
}

/// Up-front temp-space check (MCAP Player Proposal §6.1). The error names the
/// required size.
pub(crate) fn ensure_free_space(
    temp_dir: &Path,
    required_bytes: u64,
    probe: &dyn SpaceProbe,
) -> Result<()> {
    let available = probe.available_bytes(temp_dir)?;
    if available < required_bytes {
        return Err(anyhow!(
            "insufficient free space in '{}': the pass-0 private snapshot needs {} bytes, \
             {} bytes available",
            temp_dir.display(),
            required_bytes,
            available
        ));
    }
    Ok(())
}

/// Snapshot every input file sequentially, after checking that the temp
/// filesystem can hold all of them (MCAP Player Proposal §6.1).
pub(crate) fn snapshot_inputs(
    files: &[PathBuf],
    temp_dir: &Path,
    cancel: &AtomicBool,
    probe: &dyn SpaceProbe,
) -> Result<Vec<Snapshot>> {
    ensure_private_temp_dir(temp_dir)?;

    let mut required: u64 = 0;
    for file in files {
        let identity = FileIdentity::of(file)?;
        required = required.checked_add(identity.size).ok_or_else(|| {
            anyhow!("total input size overflows u64 — refusing to snapshot this input set")
        })?;
    }
    ensure_free_space(temp_dir, required, probe)?;

    let mut snapshots = Vec::with_capacity(files.len());
    for file in files {
        snapshots.push(snapshot_file(file, temp_dir, cancel)?);
    }
    Ok(snapshots)
}

fn snapshot_file(source: &Path, temp_dir: &Path, cancel: &AtomicBool) -> Result<Snapshot> {
    snapshot_file_with_hook(source, temp_dir, cancel, &mut |_| {})
}

/// `on_block` runs after each copied block; tests use it to mutate the source
/// mid-copy and prove the identity re-check fires.
fn snapshot_file_with_hook(
    source: &Path,
    temp_dir: &Path,
    cancel: &AtomicBool,
    on_block: &mut dyn FnMut(u64),
) -> Result<Snapshot> {
    ensure_private_temp_dir(temp_dir)?;
    let before = FileIdentity::of(source)?;

    let mut reader =
        File::open(source).with_context(|| format!("cannot open input '{}'", source.display()))?;
    let mut writer = create_private_temp_file(temp_dir)?;

    let mut buffer = vec![0u8; COPY_BLOCK_BYTES];
    let mut copied: u64 = 0;
    let mut remaining = before.size;
    while remaining > 0 {
        // One cancellation check per block (MCAP Player Proposal §6.1/§6.7).
        if cancel.load(Ordering::SeqCst) {
            return Err(anyhow!(Cancelled));
        }
        let want = remaining.min(COPY_BLOCK_BYTES as u64) as usize;
        let read = reader
            .read(&mut buffer[..want])
            .with_context(|| format!("cannot read input '{}'", source.display()))?;
        if read == 0 {
            // The source shrank under us; the identity re-check below reports it.
            break;
        }
        writer
            .write_all(&buffer[..read])
            .context("cannot write the private snapshot copy")?;
        copied += read as u64;
        remaining -= read as u64;
        on_block(copied);
        #[cfg(all(test, feature = "fault-injection"))]
        {
            let stall = TEST_COPY_STALL.lock().expect("copy stall").take();
            if let Some(stall) = stall {
                stall.wait();
            }
        }
        #[cfg(feature = "fault-injection")]
        {
            // Test-only delay makes an append race deterministic.
            if let Ok(raw) = std::env::var("AORTA_PLAY_TEST_SNAPSHOT_DELAY_MS") {
                if let Ok(ms) = raw.parse::<u64>() {
                    std::thread::sleep(Duration::from_millis(ms));
                }
            }
        }
    }
    writer
        .flush()
        .context("cannot flush the private snapshot copy")?;

    // Re-check identity *after* the copy: a still-running recorder is the whole
    // reason pass 0 exists (MCAP Player Proposal §2, §6.1).
    let after = FileIdentity::of(source)?;
    if after != before || copied != before.size {
        return Err(anyhow!(
            "'{}': input changed during snapshot — is the recorder still writing this file?",
            source.display()
        ));
    }

    writer
        .seek(SeekFrom::Start(0))
        .context("cannot rewind the private snapshot copy")?;
    Ok(Snapshot {
        source: source.to_path_buf(),
        file: writer,
        len: copied,
    })
}

/// Create a temp file and unlink it immediately, so the copy lives only behind
/// the returned fd and vanishes on process death (MCAP Player Proposal §6.1).
fn create_private_temp_file(temp_dir: &Path) -> Result<File> {
    static SEQ: AtomicU64 = AtomicU64::new(0);

    for _ in 0..16 {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = temp_dir.join(format!(
            "aorta-play-snapshot-{}-{}.tmp",
            std::process::id(),
            seq
        ));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        match options.open(&path) {
            Ok(file) => {
                fs::remove_file(&path).with_context(|| {
                    format!(
                        "cannot unlink the private snapshot copy '{}'",
                        path.display()
                    )
                })?;
                return Ok(file);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "cannot create a private snapshot copy in '{}'",
                        temp_dir.display()
                    )
                })
            }
        }
    }
    Err(anyhow!(
        "cannot create a private snapshot copy in '{}': name collisions exhausted",
        temp_dir.display()
    ))
}

fn ensure_private_temp_dir(temp_dir: &Path) -> Result<()> {
    match fs::create_dir(temp_dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("cannot create snapshot temp dir '{}'", temp_dir.display())
            })
        }
    }
    #[cfg(unix)]
    fs::set_permissions(temp_dir, fs::Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "cannot make snapshot temp dir '{}' private",
            temp_dir.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Self-cleaning scratch directory (no dev-dependency on `tempfile`).
    struct TestDir(PathBuf);

    impl TestDir {
        fn new(tag: &str) -> Self {
            let seq = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "aorta-play-test-{}-{}-{}",
                std::process::id(),
                tag,
                seq
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create test dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn child(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct FakeProbe(u64);

    impl SpaceProbe for FakeProbe {
        fn available_bytes(&self, _dir: &Path) -> Result<u64> {
            Ok(self.0)
        }
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write fixture");
    }

    fn dir_entry_count(dir: &Path) -> usize {
        fs::read_dir(dir).expect("read dir").count()
    }

    #[test]
    fn collect_input_files_globs_directory_non_recursively() {
        let dir = TestDir::new("glob");
        write_file(&dir.child("b.mcap"), b"b");
        write_file(&dir.child("a.mcap"), b"a");
        write_file(&dir.child("notes.txt"), b"x");
        fs::create_dir_all(dir.child("nested")).expect("nested");
        write_file(&dir.child("nested/c.mcap"), b"c");

        let files = collect_input_files(&[dir.path().to_path_buf()]).expect("glob");
        assert_eq!(
            files,
            vec![dir.child("a.mcap"), dir.child("b.mcap")],
            "sorted, non-recursive, *.mcap only"
        );
    }

    #[test]
    fn collect_input_files_takes_explicit_files_in_order_and_dedups() {
        let dir = TestDir::new("explicit");
        write_file(&dir.child("z.mcap"), b"z");
        write_file(&dir.child("a.mcap"), b"a");

        let files = collect_input_files(&[
            dir.child("z.mcap"),
            dir.child("a.mcap"),
            dir.child("z.mcap"),
        ])
        .expect("explicit files");
        assert_eq!(files, vec![dir.child("z.mcap"), dir.child("a.mcap")]);
    }

    #[test]
    fn collect_input_files_rejects_missing_input_and_empty_list() {
        let dir = TestDir::new("missing");
        assert!(collect_input_files(&[]).is_err());
        let err = collect_input_files(&[dir.child("nope.mcap")]).unwrap_err();
        assert!(err.to_string().contains("nope.mcap"), "got: {err}");
    }

    #[test]
    fn collect_input_files_rejects_directory_without_mcap_files() {
        let dir = TestDir::new("emptydir");
        write_file(&dir.child("readme.md"), b"x");
        let err = collect_input_files(&[dir.path().to_path_buf()]).unwrap_err();
        assert!(err.to_string().contains(".mcap"), "got: {err}");
    }

    #[test]
    fn snapshot_is_readable_from_the_unlinked_fd_and_leaves_no_temp_files() {
        let src_dir = TestDir::new("copy-src");
        let tmp_dir = TestDir::new("copy-tmp");
        let payload: Vec<u8> = (0..(COPY_BLOCK_BYTES * 2 + 7))
            .map(|i| (i % 251) as u8)
            .collect();
        let source = src_dir.child("input.mcap");
        write_file(&source, &payload);

        let cancel = AtomicBool::new(false);
        let mut snapshot = snapshot_file(&source, tmp_dir.path(), &cancel).expect("snapshot");
        assert_eq!(snapshot.len, payload.len() as u64);

        let mut copied = Vec::new();
        snapshot.file.seek(SeekFrom::Start(0)).expect("seek");
        snapshot.file.read_to_end(&mut copied).expect("read back");
        assert_eq!(copied, payload, "unlinked fd must hold the exact bytes");
        assert_eq!(
            dir_entry_count(tmp_dir.path()),
            0,
            "the temp file must be unlinked immediately after creation"
        );
        assert_eq!(
            fs::metadata(tmp_dir.path())
                .expect("temp directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "snapshot directory must be private"
        );
        assert_eq!(
            snapshot
                .file
                .metadata()
                .expect("snapshot metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "snapshot file must be private"
        );
    }

    #[test]
    fn snapshot_rejects_a_source_that_changes_during_the_copy() {
        let src_dir = TestDir::new("mutate-src");
        let tmp_dir = TestDir::new("mutate-tmp");
        let source = src_dir.child("input.mcap");
        write_file(&source, &vec![7u8; COPY_BLOCK_BYTES * 3]);

        let cancel = AtomicBool::new(false);
        let mut mutated = false;
        let err = snapshot_file_with_hook(&source, tmp_dir.path(), &cancel, &mut |_copied| {
            if !mutated {
                mutated = true;
                // Simulate a still-running recorder appending to the file.
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&source)
                    .expect("reopen source");
                file.write_all(&[9u8; 4096]).expect("append");
                file.flush().expect("flush");
            }
        })
        .expect_err("a changing source must be a hard error");
        let text = err.to_string();
        assert!(
            text.contains("input changed during snapshot")
                && text.contains("is the recorder still writing this file?"),
            "MCAP Player Proposal §6.1 message expected, got: {text}"
        );
        assert_eq!(
            dir_entry_count(tmp_dir.path()),
            0,
            "no temp file may survive the failed copy"
        );
    }

    #[test]
    fn snapshot_observes_cancellation_between_blocks() {
        let src_dir = TestDir::new("cancel-src");
        let tmp_dir = TestDir::new("cancel-tmp");
        let source = src_dir.child("input.mcap");
        write_file(&source, &vec![1u8; COPY_BLOCK_BYTES * 3]);

        let cancel = AtomicBool::new(false);
        let err = snapshot_file_with_hook(&source, tmp_dir.path(), &cancel, &mut |_copied| {
            cancel.store(true, Ordering::SeqCst);
        })
        .expect_err("cancellation must stop the copy");
        assert!(
            is_cancelled(&err),
            "expected the Cancelled marker, got: {err}"
        );
    }

    #[test]
    fn snapshot_of_an_empty_file_succeeds() {
        let src_dir = TestDir::new("empty-src");
        let tmp_dir = TestDir::new("empty-tmp");
        let source = src_dir.child("input.mcap");
        write_file(&source, b"");

        let cancel = AtomicBool::new(false);
        let snapshot = snapshot_file(&source, tmp_dir.path(), &cancel).expect("snapshot");
        assert_eq!(snapshot.len, 0);
    }

    #[test]
    fn ensure_free_space_rejects_and_names_the_required_size() {
        let dir = TestDir::new("space");
        let err = ensure_free_space(dir.path(), 4096, &FakeProbe(4095)).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("4096") && text.contains("4095"),
            "error must name required and available sizes, got: {text}"
        );
        assert!(ensure_free_space(dir.path(), 4096, &FakeProbe(4096)).is_ok());
    }

    #[test]
    fn statvfs_probe_reports_a_plausible_value_for_the_temp_dir() {
        let dir = TestDir::new("statvfs");
        let available = StatvfsSpaceProbe
            .available_bytes(dir.path())
            .expect("statvfs must succeed on an existing directory");
        assert!(available > 0, "temp filesystem reported zero free bytes");
        assert!(StatvfsSpaceProbe
            .available_bytes(&dir.child("does-not-exist"))
            .is_err());
    }

    #[test]
    fn snapshot_inputs_checks_space_before_copying_anything() {
        let src_dir = TestDir::new("space-src");
        let tmp_dir = TestDir::new("space-tmp");
        let a = src_dir.child("a.mcap");
        let b = src_dir.child("b.mcap");
        write_file(&a, &[0u8; 1024]);
        write_file(&b, &[0u8; 1024]);

        let cancel = AtomicBool::new(false);
        let err = snapshot_inputs(
            &[a.clone(), b.clone()],
            tmp_dir.path(),
            &cancel,
            &FakeProbe(2047),
        )
        .unwrap_err();
        assert!(err.to_string().contains("2048"), "got: {err}");
        assert_eq!(dir_entry_count(tmp_dir.path()), 0);

        let snapshots = snapshot_inputs(&[a, b], tmp_dir.path(), &cancel, &FakeProbe(4096))
            .expect("enough space");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].len, 1024);
        assert_eq!(snapshots[1].len, 1024);
    }
}
