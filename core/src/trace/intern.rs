//! Process-global interning for the record path.
//!
//! The hot path stores small **global** ids in each [`super::chunk::RingRecord`]
//! (event-name id, thread id); the drain resolves them to the per-chunk string /
//! thread tables. These registries are cold (touched once per distinct name /
//! once per thread) and never on the per-event hot path — a call site caches the
//! id it gets back.
//!
//! Scopes have their own richer registry ([`super::scope`]); this module covers
//! the two interning axes the record path adds: event names and OS threads.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// A minimal name→`u16` interner: idempotent `intern`, reverse `name` lookup.
/// Ids are 0-based and dense (they index a chunk-local table directly). Capacity
/// is `u16::MAX` distinct names, ample for event names.
struct StringInterner {
    inner: RwLock<StringInternerInner>,
}

struct StringInternerInner {
    by_name: HashMap<String, u16>,
    names: Vec<String>,
}

/// The reserved overflow id — NEVER allocated; reverse lookups
/// resolve it to [`INTERN_OVERFLOW_NAME`].
pub(crate) const INTERN_OVERFLOW_SENTINEL: u16 = u16::MAX;
pub(crate) const INTERN_OVERFLOW_NAME: &str = "<intern-overflow>";

/// One-shot PER INTERNER overflow WARN — tracing degrades, the process
/// lives (the caller passes its own sticky flag; a shared static would let
/// the first interner's overflow swallow the other's warning).
fn warn_intern_overflow(kind: &str, warned: &std::sync::atomic::AtomicBool) {
    if !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
        crate::aorta_core_warn!(
            "Aorta WARN: trace {} interner exhausted its u16 id space — further \
             names attribute to the '<intern-overflow>' bucket (tracing degrades; \
             the process continues). Dynamic per-entity names are the usual cause",
            kind
        );
    }
}

impl StringInterner {
    fn new() -> Self {
        StringInterner {
            inner: RwLock::new(StringInternerInner {
                by_name: HashMap::new(),
                names: Vec::new(),
            }),
        }
    }

    fn intern(&self, name: &str) -> u16 {
        {
            let inner = self.inner.read().expect("trace interner poisoned");
            if let Some(&id) = inner.by_name.get(name) {
                return id;
            }
        }
        let mut inner = self.inner.write().expect("trace interner poisoned");
        if let Some(&id) = inner.by_name.get(name) {
            return id;
        }
        // Tracing must NEVER abort the host — the
        // id space caps at u16::MAX - 1 and `u16::MAX` is the reserved
        // overflow sentinel (never allocated; reverse lookups special-case
        // it). Overflowing names all attribute to the sentinel bucket.
        static EVENT_WARNED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        let Ok(id) = u16::try_from(inner.names.len()) else {
            warn_intern_overflow("event-name", &EVENT_WARNED);
            return INTERN_OVERFLOW_SENTINEL;
        };
        if id == INTERN_OVERFLOW_SENTINEL {
            warn_intern_overflow("event-name", &EVENT_WARNED);
            return INTERN_OVERFLOW_SENTINEL;
        }
        inner.by_name.insert(name.to_string(), id);
        inner.names.push(name.to_string());
        id
    }

    fn name(&self, id: u16) -> Option<String> {
        if id == INTERN_OVERFLOW_SENTINEL {
            return Some(INTERN_OVERFLOW_NAME.to_string());
        }
        self.inner
            .read()
            .expect("trace interner poisoned")
            .names
            .get(id as usize)
            .cloned()
    }
}

static EVENTS: OnceLock<StringInterner> = OnceLock::new();

fn events() -> &'static StringInterner {
    EVENTS.get_or_init(StringInterner::new)
}

/// Intern an event name (the slice/instant/counter label) → stable dense id.
/// Cache the result at the call site; do not call per event.
#[inline]
pub fn intern_event(name: &str) -> u16 {
    events().intern(name)
}

/// Event name for an id (drain-side, building the chunk `events` table).
pub fn event_name(id: u16) -> String {
    events().name(id).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Thread registry: a global thread id -> (os_tid, thread_name).
// ---------------------------------------------------------------------------

struct ThreadInfo {
    os_tid: u32,
    name: String,
}

static THREADS: OnceLock<RwLock<Vec<ThreadInfo>>> = OnceLock::new();

fn threads() -> &'static RwLock<Vec<ThreadInfo>> {
    THREADS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register the calling thread once and return its global thread id. Cold: call
/// from per-thread init and cache the result thread-locally.
pub fn register_current_thread() -> u16 {
    let tid = os_tid();
    let name = std::thread::current()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("tid-{tid}"));
    let mut threads = threads().write().expect("trace thread registry poisoned");
    // Never abort — overflow attributes to the sentinel bucket.
    static THREAD_WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let Ok(id) = u16::try_from(threads.len()) else {
        warn_intern_overflow("thread", &THREAD_WARNED);
        return INTERN_OVERFLOW_SENTINEL;
    };
    if id == INTERN_OVERFLOW_SENTINEL {
        warn_intern_overflow("thread", &THREAD_WARNED);
        return INTERN_OVERFLOW_SENTINEL;
    }
    threads.push(ThreadInfo { os_tid: tid, name });
    id
}

/// `(os_tid, name)` for a global thread id (drain-side, building `threads`).
pub fn thread_info(id: u16) -> (u32, String) {
    if id == INTERN_OVERFLOW_SENTINEL {
        return (0, INTERN_OVERFLOW_NAME.to_string());
    }
    threads()
        .read()
        .expect("trace thread registry poisoned")
        .get(id as usize)
        .map(|t| (t.os_tid, t.name.clone()))
        .unwrap_or((0, String::new()))
}

/// The kernel thread id of the calling thread (Perfetto's `tid`). Identity is
/// `(pid, os_tid)` across chunks/hosts.
pub fn os_tid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `gettid` takes no args and cannot fail.
        (unsafe { libc::syscall(libc::SYS_gettid) }) as u32
    }
    #[cfg(target_os = "macos")]
    {
        let mut tid: u64 = 0;
        // SAFETY: writes the calling thread's id into `tid`; 0 = current thread.
        unsafe {
            libc::pthread_threadid_np(0, &mut tid);
        }
        tid as u32
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

/// CPU time consumed by the calling thread, in ns — `CLOCK_THREAD_CPUTIME_ID`.
/// A **syscall** (not vDSO): ~636 ns (A78) / ~1037 ns (A55), so this is only
/// read when `thread_dur` capture is enabled (env `AORTA_TRACE_THREAD_TIME`).
pub fn thread_cpu_ns() -> u64 {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid timespec; the clock id is a documented constant.
        unsafe {
            libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
        }
        (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

#[cfg(test)]
mod overflow_tests {
    use super::*;

    #[test]
    fn event_interner_overflow_degrades_to_the_sentinel() {
        // A LOCAL interner instance (the global
        // one is process-wide — filling it would poison sibling tests).
        let interner = StringInterner::new();
        let mut last = 0u16;
        for i in 0..70_000u32 {
            last = interner.intern(&format!("overflow-test-{i}"));
        }
        assert_eq!(
            last, INTERN_OVERFLOW_SENTINEL,
            "past the cap every intern returns the reserved sentinel"
        );
        assert_eq!(
            interner.name(INTERN_OVERFLOW_SENTINEL).as_deref(),
            Some(INTERN_OVERFLOW_NAME),
            "the sentinel reverse-resolves to the overflow bucket name"
        );
        // The last REAL id (65534) still resolves to its own name.
        assert_eq!(
            interner.name(u16::MAX - 1).as_deref(),
            Some("overflow-test-65534")
        );
    }
}
