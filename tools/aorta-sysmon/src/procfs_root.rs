//! Filesystem root for `/proc` and `/sys` reads.
//!
//! Production code uses `ProcfsRoot::system()`, which points at `/proc` and
//! `/sys`. Unit tests pass `ProcfsRoot::fixture(<path>)` so each collector can
//! be exercised against the redacted captures under
//! `tools/aorta-sysmon/testdata/`.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ProcfsRoot {
    proc: PathBuf,
    sys: PathBuf,
}

impl ProcfsRoot {
    pub fn system() -> Self {
        Self {
            proc: PathBuf::from("/proc"),
            sys: PathBuf::from("/sys"),
        }
    }

    /// Root that points at a fixture directory laid out as
    /// `<root>/proc/...` and `<root>/sys/...`. Missing trees are
    /// silently treated as "file absent" by the collectors.
    pub fn fixture(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            proc: root.join("proc"),
            sys: root.join("sys"),
        }
    }

    pub fn proc(&self) -> &Path {
        &self.proc
    }

    pub fn sys(&self) -> &Path {
        &self.sys
    }

    pub fn proc_path(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.proc.join(rel)
    }

    pub fn sys_path(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.sys.join(rel)
    }
}

impl Default for ProcfsRoot {
    fn default() -> Self {
        Self::system()
    }
}
