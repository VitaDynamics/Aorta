//! Wrapper-side signal ownership (MCAP Player Proposal §6.7 "signal owner").
//!
//! **This module is for the wrappers only** — `aorta-play`'s `main` and
//! `aorta-cli play` share it. The library itself installs no handlers and never
//! exits the process (§6.6); [`crate::run_with_shutdown`] never calls in
//! here.
//!
//! Two properties the naive designs do not have:
//!
//! - **Scheduler-independent.** `tokio::signal::ctrl_c()` is explicitly
//!   prohibited: once its future is first polled Tokio installs a process-wide
//!   handler that permanently replaces Unix's default "SIGINT terminates"
//!   behaviour, and a runtime wedged inside a synchronous transport
//!   `.wait()` — the one documented unbounded state (§6.7) — may never
//!   poll the signal task again. The escape must live in the OS handler.
//! - **Coalescing-proof.** `signal-hook`'s delivery state is a single flag, so
//!   two close-spaced SIGINTs can surface as *one* iterator event; an
//!   iterator-local "second press" counter would silently lose the only finite
//!   escape. The forced exit therefore lives in the handler itself.
//!
//! Registration order is the crate's documented conditional-shutdown pattern
//! (`signal-hook` 0.3.18 `flag.rs`):
//!
//! 1. [`register_conditional_shutdown`] with an initially **false** flag — a
//!    SIGINT arriving while the flag is set terminates from inside the handler
//!    via `signal_hook::low_level::exit`, i.e. bare `_exit(130)`.
//!    `std::process::exit` is prohibited on this path: it runs `atexit`
//!    handlers that may synchronize with already-wedged threads, which would
//!    unbound the "hard" bound.
//! 2. [`flag::register`] on the same flag, which **arms** it on the first
//!    delivery. Every later delivery therefore exits inside the handler,
//!    whatever the runtime is doing.
//!
//! A dedicated iterator thread exists only to translate the first delivery into
//! [`CancellationToken::cancel`] for a graceful teardown. It plays no part in
//! the forced exit.

use anyhow::{Context, Result};
use signal_hook::consts::signal::SIGINT;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(feature = "fault-injection")]
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Direct-exit code of the forced path (MCAP Player Proposal §6.7). Defined
/// here rather than via the `RunOutcome` mapping, which cannot execute while
/// the library is wedged.
pub const FORCED_EXIT_CODE: i32 = 130;

#[cfg(feature = "fault-injection")]
const TEST_BLOCK_ITERATOR_ENV: &str = "AORTA_PLAY_TEST_BLOCK_SIGNAL_ITERATOR";

/// Installed handler state, kept alive for the process's lifetime.
pub struct SignalOwner {
    armed: Arc<AtomicBool>,
    token: CancellationToken,
}

impl SignalOwner {
    /// The flag the OS handler consults: `true` once the first SIGINT armed it.
    pub fn armed(&self) -> bool {
        self.armed.load(Ordering::SeqCst)
    }

    /// The token the first delivery cancels.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

/// Install the signal owner **before** entering the engine and return the
/// shutdown token to hand to [`crate::run_with_shutdown`].
///
/// Call once per process; a second call would register a second iterator
/// thread and a second conditional shutdown for the same signal.
pub fn install(token: CancellationToken) -> Result<SignalOwner> {
    let armed = Arc::new(AtomicBool::new(false));

    // 1. Forced exit first, still disarmed.
    signal_hook::flag::register_conditional_shutdown(SIGINT, FORCED_EXIT_CODE, armed.clone())
        .context("failed to register the SIGINT conditional shutdown")?;
    // 2. Arming action second — order matters (see the module docs).
    signal_hook::flag::register(SIGINT, armed.clone())
        .context("failed to register the SIGINT arming action")?;

    // 3. Graceful path: one thread whose only job is the first cancel.
    let mut signals = signal_hook::iterator::Signals::new([SIGINT])
        .context("failed to create the SIGINT iterator")?;
    // Test-only hook for the MCAP Player Proposal §7 item 5
    // two-handler-delivery child test. It keeps the iterator from draining the
    // first delivery so the visible arm acknowledgement proves that the OS
    // handler, not the iterator, owns the forced-exit decision.
    #[cfg(feature = "fault-injection")]
    let block_iterator = std::env::var(TEST_BLOCK_ITERATOR_ENV).ok().as_deref() == Some("1");
    let iterator_token = token.clone();
    std::thread::Builder::new()
        .name("aorta-play-signals".to_string())
        .spawn(move || {
            #[cfg(feature = "fault-injection")]
            if block_iterator {
                eprintln!("AORTA_PLAY_TEST_SIGNAL_ITERATOR_BLOCKED=1");
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                }
            }
            for _signal in signals.forever() {
                // Idempotent: a coalesced or repeated delivery just cancels an
                // already-cancelled token. The forced exit is the handler's.
                iterator_token.cancel();
            }
        })
        .context("failed to spawn the SIGINT iterator thread")?;

    #[cfg(feature = "fault-injection")]
    if block_iterator {
        // Test-only acknowledgement watcher. Printing is deliberately outside
        // the async-signal handler; the handler only flips the atomic flag.
        let acknowledgement = armed.clone();
        std::thread::Builder::new()
            .name("aorta-play-signal-ack".to_string())
            .spawn(move || {
                while !acknowledgement.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                eprintln!("AORTA_PLAY_TEST_SIGNAL_ARMED=1");
            })
            .context("failed to spawn the SIGINT test acknowledgement thread")?;
    }

    Ok(SignalOwner { armed, token })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One delivery must arm the handler *and* cancel the token. A second
    /// delivery would `_exit(130)` from inside the handler and take the test
    /// harness with it, so this test raises exactly once — the forced path is
    /// proved by the MCAP Player Proposal §7 item 5 child-process tests.
    #[test]
    fn the_first_delivery_arms_the_handler_and_cancels_the_token() {
        let token = CancellationToken::new();
        let owner = install(token.clone()).expect("install");
        assert!(
            !owner.armed(),
            "the flag starts disarmed (MCAP Player Proposal §6.7)"
        );
        assert!(!token.is_cancelled());

        // SAFETY: `raise` on SIGINT with the handlers above installed.
        unsafe { libc::raise(SIGINT) };

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !token.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            token.is_cancelled(),
            "the iterator thread translates the first delivery into a cancel"
        );
        assert!(
            owner.armed(),
            "the first delivery arms the conditional shutdown, so any further \
             SIGINT _exit(130)s from inside the OS handler"
        );
    }
}
