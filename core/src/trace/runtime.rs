//! Tracer bootstrap: turn the recording engine on, spawn the drain
//! thread + file sink, and arrange flush-on-exit — all from the environment.
//!
//! This is where the tracing **default flips to on-at-T1**: a
//! process that calls [`init`] with `AORTA_TRACE` unset records the `always`
//! tier continuously; `AORTA_TRACE=off` disables it; `standard`/`verbose` lower
//! the tier floor. The static master gate stays default-OFF (so isolated unit
//! tests are unaffected) — the flip lives here, at the process entry point.
//!
//! Env vars (shared by the SDKs and the CLI):
//! - `AORTA_TRACE` — `off` | `always` | `standard` | `verbose` | `scope=level,…`
//!   (unset ⇒ `always`).
//! - `AORTA_TRACE_DIR`, `AORTA_TRACE_MAX_BYTES` — storage (see [`super::sink`]).
//! - `AORTA_TRACE_FILE_MAX_BYTES`, `AORTA_TRACE_FILE_MAX_SECS` — container-file
//!   rotation (defaults 8M / 300s).
//! - `AORTA_TRACE_SWEEP_MS`, `AORTA_TRACE_CHUNK_RECORDS`,
//!   `AORTA_TRACE_CHUNK_AGE_MS` — drain cadence / chunk sizing (defaults
//!   5 / 16384 / 1000; the chunk age is the in-process crash-loss window).
//! - `AORTA_TRACE_THREAD_TIME` — capture `thread_dur` (off by default; syscall).
//! - `AORTA_TRACE_AUTO_INIT` — `0`/`false` keeps [`init`] manual: the
//!   `Session::open` hook ([`auto_init`]) becomes a no-op, for embedders that
//!   don't want a background thread / disk writes they didn't ask for (mirrors
//!   `AORTA_LOG_AUTO_INIT`). Unset ⇒ enabled.
//!
//! **fork() contract:** the drain thread does not survive `fork`. The
//! fork+exec pattern (spawn a utility child that `exec`s immediately) is fine;
//! a child that keeps running this process image must not touch tracing
//! between `fork` and `exec`, and a child that wants tracing after a bare
//! `fork` is NOT supported (re-exec instead — the inherited `INITED` latch,
//! rings, and pid-stamped sink files all belong to the parent).

use super::sink::{self, DrainConfig, FileSink, SinkConfig};
use super::{scope, span, trace_enabled};
use std::sync::atomic::{AtomicBool, Ordering};

static INITED: AtomicBool = AtomicBool::new(false);
static ATEXIT_REGISTERED: AtomicBool = AtomicBool::new(false);

const ENV_TRACE_AUTO_INIT: &str = "AORTA_TRACE_AUTO_INIT";

/// Whether [`init`] has run (explicitly or via [`auto_init`]).
pub fn is_initialized() -> bool {
    INITED.load(Ordering::Acquire)
}

/// Process-entry hook for the SDK path: called by the first (and every)
/// `Session::open`; runs [`init`] unless the embedder kept manual control via
/// `AORTA_TRACE_AUTO_INIT=0`. Idempotent — an explicit earlier [`init`] (or
/// C-ABI `aorta_trace_init`) wins and this becomes a no-op.
pub fn auto_init() {
    if auto_init_enabled() {
        init();
    }
}

/// Read the `AORTA_TRACE_AUTO_INIT` decision from the environment. Uncached:
/// sessions open rarely, and rereading keeps tests and late `setenv` callers
/// honest.
///
/// In hermetic test builds (`cfg(test)` without `AORTA_TEST_ALLOW_ENV` —
/// see `runtime_env`) env reads are disabled, so the opt-out could never be
/// expressed; auto-init therefore defaults **off** there, keeping the
/// session-opening test suites from flipping the global gate / spawning the
/// sink mid-run. Production (non-test) builds always read the env.
pub fn auto_init_enabled() -> bool {
    if !crate::runtime_env::reads_enabled() {
        return false;
    }
    auto_init_enabled_value(crate::runtime_env::var(ENV_TRACE_AUTO_INIT).as_deref())
}

/// Pure decision: unset or unparsable ⇒ enabled (fail open, matching
/// `AORTA_LOG_AUTO_INIT`); only an explicit false form opts out.
pub fn auto_init_enabled_value(raw: Option<&str>) -> bool {
    match raw {
        Some(raw) => crate::logging::parse_bool(raw).unwrap_or(true),
        None => true,
    }
}

/// Initialize tracing from the environment and, if enabled, spawn the drain
/// thread + file sink. Idempotent: a second call is a no-op. Call once at
/// process startup (binaries / SDK init / `aorta_trace_init`).
pub fn init() {
    if INITED.swap(true, Ordering::AcqRel) {
        return;
    }
    configure_from_env();
    span::set_thread_time_enabled(env_bool("AORTA_TRACE_THREAD_TIME"));

    if trace_enabled() {
        match FileSink::new(SinkConfig::from_env()) {
            Ok(file_sink) => {
                sink::install(file_sink, DrainConfig::from_env());
                register_atexit();
            }
            Err(e) => {
                // Never fail process startup over tracing: degrade to gate-only
                // (histograms still record; spans accumulate in rings but are not
                // persisted). Surface it once.
                eprintln!("aorta: trace sink disabled (cannot open trace dir): {e}");
            }
        }
    }
}

/// Apply the `AORTA_TRACE` directive; unset/empty ⇒ default `always`
/// (master on, T1 floor).
fn configure_from_env() {
    match std::env::var("AORTA_TRACE") {
        Ok(s) if !s.trim().is_empty() => scope::apply_spec(&s),
        _ => scope::apply_spec("always"),
    }
}

/// Flush + stop the drain thread (idempotent). Call at clean shutdown; also
/// registered via `atexit` so a one-shot process persists its tail chunk.
pub fn shutdown() {
    sink::shutdown_global();
}

fn register_atexit() {
    if ATEXIT_REGISTERED.swap(true, Ordering::AcqRel) {
        return;
    }
    // SAFETY: `atexit_flush` is an `extern "C"` fn with no args that only calls
    // the idempotent `shutdown`; registering it is sound.
    unsafe {
        libc::atexit(atexit_flush);
    }
}

extern "C" fn atexit_flush() {
    shutdown();
}

fn env_bool(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        })
        .unwrap_or(false)
}
