//! `AORTA_GROUP` process ownership, restoration and taint (MCAP Player Proposal
//! §6.9, §6.7).
//!
//! `Node` ignores its `group` argument and reads `AORTA_GROUP` from the process
//! environment (`node.rs:184`, `key_expr.rs:778`), so replaying into a group the
//! ambient environment does not name means **the player owns the variable** for
//! the lifetime of its Node:
//!
//! - the effective group is `--group` > the recorded `aorta.group` (ambient
//!   `AORTA_GROUP` is *not* a source — it only produces an info line when it
//!   disagrees);
//! - the prior value is saved before Node construction and restored on **every**
//!   return path where teardown completed (success, startup error, mid-replay
//!   error, cancellation);
//! - if the teardown deadline expired the variable is deliberately **not**
//!   restored — live core tasks may still read it — and the process is marked
//!   *Aorta-tainted*: no further Node/player construction is supported in it,
//!   so the next [`crate::run_with_shutdown`] in the same process fails closed
//!   and an embedding caller must exit.
//!
//! Concurrent player runs, or concurrent Node construction, in one process are
//! unsupported (§6.9): save/restore cannot make concurrent mutation of a
//! process-global variable safe.

use anyhow::{anyhow, bail, Result};
use std::sync::atomic::{AtomicBool, Ordering};

/// The variable `Node` reads its group from.
pub const ENV_AORTA_GROUP: &str = "AORTA_GROUP";

static TAINTED: AtomicBool = AtomicBool::new(false);

/// True once a teardown deadline expired in this process (MCAP Player Proposal
/// §6.7). No further player run is supported afterwards.
pub fn is_tainted() -> bool {
    TAINTED.load(Ordering::SeqCst)
}

/// Mark the process Aorta-tainted. Irreversible outside tests.
pub(crate) fn mark_tainted() {
    TAINTED.store(true, Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn clear_taint_for_test() {
    TAINTED.store(false, Ordering::SeqCst);
}

/// Env mutation is process-global: every test that touches `AORTA_GROUP` (or
/// any other Aorta env gate) serializes on [`EnvLock`]. The crate's gate also
/// runs with `--test-threads=1`, so this is a belt-and-braces guard rather
/// than a contended lock.
///
/// It is a spin-and-sleep flag rather than a `Mutex<()>` on purpose: async
/// tests hold it across `await` points, which a `MutexGuard` may not be.
#[cfg(test)]
pub(crate) struct EnvLock;

#[cfg(test)]
static ENV_LOCKED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
impl EnvLock {
    pub(crate) fn acquire() -> Self {
        while ENV_LOCKED
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Self
    }
}

#[cfg(test)]
impl Drop for EnvLock {
    fn drop(&mut self) {
        ENV_LOCKED.store(false, Ordering::SeqCst);
    }
}

/// Fail closed when a previous run left the process tainted (MCAP Player
/// Proposal §6.7).
pub(crate) fn ensure_not_tainted() -> Result<()> {
    if is_tainted() {
        bail!(
            "a previous aorta-play run in this process exceeded its teardown deadline; the \
             process is Aorta-tainted (live core tasks may still hold the session and \
             {ENV_AORTA_GROUP} was deliberately left as-is) — no further player run is \
             supported here, the caller must exit (MCAP Player Proposal §6.7, §6.9)"
        );
    }
    Ok(())
}

/// Resolve the group to replay into: `--group` wins over the recorded
/// `aorta.group`; neither present is a hard error (MCAP Player Proposal §6.9).
pub(crate) fn effective_group(flag: Option<&str>, recorded: Option<&str>) -> Result<String> {
    if let Some(flag) = flag {
        let flag = flag.trim();
        if flag.is_empty() {
            bail!("--group is empty; pass a real group name (MCAP Player Proposal §6.9)");
        }
        return Ok(flag.to_string());
    }
    let recorded = recorded
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "the recording carries no group (no aorta.group metadata and no channel key to \
                 fall back on) — pass --group to say where to replay it (MCAP Player Proposal §6.9)"
            )
        })?;
    Ok(recorded.to_string())
}

/// Owns `AORTA_GROUP` for the lifetime of the player's Node (MCAP Player
/// Proposal §6.9).
#[derive(Debug)]
pub(crate) struct GroupGuard {
    prior: Option<String>,
    group: String,
    /// While true, the prior value is restored on `restore()`/drop.
    armed: bool,
}

impl GroupGuard {
    /// Save the prior value and install `group`. Called *before* Node
    /// construction, since the Node reads the variable while constructing.
    pub(crate) fn acquire(group: &str) -> Self {
        let prior = std::env::var(ENV_AORTA_GROUP).ok();
        if let Some(ambient) = prior.as_deref() {
            if ambient.trim() != group {
                tracing::info!(
                    "aorta-play: ambient {}='{}' differs from the replay group '{}' — the \
                     player owns the variable until teardown completes (MCAP Player Proposal §6.9)",
                    ENV_AORTA_GROUP,
                    ambient,
                    group
                );
            }
        }
        std::env::set_var(ENV_AORTA_GROUP, group);
        Self {
            prior,
            group: group.to_string(),
            armed: true,
        }
    }

    pub(crate) fn group(&self) -> &str {
        &self.group
    }

    /// Teardown completed: put the environment back exactly as it was.
    pub(crate) fn restore(mut self) {
        self.restore_now();
    }

    /// Teardown deadline expired: leave the variable alone and taint the
    /// process (MCAP Player Proposal §6.7, §6.9).
    pub(crate) fn abandon(mut self) {
        self.armed = false;
        mark_tainted();
        tracing::warn!(
            "aorta-play: teardown deadline expired — leaving {}='{}' in place (live core tasks \
             may still read it) and marking the process Aorta-tainted (MCAP Player Proposal §6.7, §6.9)",
            ENV_AORTA_GROUP,
            self.group
        );
    }

    fn restore_now(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        match self.prior.take() {
            Some(prior) => std::env::set_var(ENV_AORTA_GROUP, prior),
            None => std::env::remove_var(ENV_AORTA_GROUP),
        }
    }
}

impl Drop for GroupGuard {
    /// Safety net for panics and early returns: an armed guard restores. The
    /// explicit `restore()` / `abandon()` calls are what the contract is
    /// written in terms of.
    fn drop(&mut self) {
        self.restore_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_wins_over_the_recorded_group() {
        assert_eq!(
            effective_group(Some("live"), Some("robot")).unwrap(),
            "live"
        );
        assert_eq!(effective_group(None, Some("robot")).unwrap(), "robot");
    }

    #[test]
    fn ambient_group_is_ignored_when_recorded_metadata_is_available() {
        let _lock = EnvLock::acquire();
        std::env::set_var(ENV_AORTA_GROUP, "ambient");
        assert_eq!(effective_group(None, Some("recorded")).unwrap(), "recorded");
        std::env::remove_var(ENV_AORTA_GROUP);
    }

    #[test]
    fn neither_source_is_a_hard_error() {
        let err = effective_group(None, None).unwrap_err();
        assert!(err.to_string().contains("--group"), "got: {err}");
        assert!(effective_group(Some("  "), Some("robot")).is_err());
    }

    #[test]
    fn the_guard_installs_and_restores_the_prior_value() {
        let _lock = EnvLock::acquire();
        std::env::set_var(ENV_AORTA_GROUP, "before");
        let guard = GroupGuard::acquire("replay");
        assert_eq!(std::env::var(ENV_AORTA_GROUP).unwrap(), "replay");
        guard.restore();
        assert_eq!(std::env::var(ENV_AORTA_GROUP).unwrap(), "before");
        std::env::remove_var(ENV_AORTA_GROUP);
    }

    #[test]
    fn an_absent_prior_value_is_removed_again() {
        let _lock = EnvLock::acquire();
        std::env::remove_var(ENV_AORTA_GROUP);
        let guard = GroupGuard::acquire("replay");
        assert_eq!(std::env::var(ENV_AORTA_GROUP).unwrap(), "replay");
        guard.restore();
        assert!(std::env::var(ENV_AORTA_GROUP).is_err());
    }

    #[test]
    fn abandoning_the_guard_leaves_the_group_and_taints_the_process() {
        let _lock = EnvLock::acquire();
        std::env::set_var(ENV_AORTA_GROUP, "before");
        let guard = GroupGuard::acquire("replay");
        guard.abandon();
        assert_eq!(
            std::env::var(ENV_AORTA_GROUP).unwrap(),
            "replay",
            "an expired teardown must not restore the variable (MCAP Player Proposal §6.9)"
        );
        assert!(is_tainted());
        assert!(ensure_not_tainted().is_err());
        clear_taint_for_test();
        std::env::remove_var(ENV_AORTA_GROUP);
    }

    #[test]
    fn dropping_an_armed_guard_still_restores() {
        let _lock = EnvLock::acquire();
        std::env::set_var(ENV_AORTA_GROUP, "before");
        {
            let _guard = GroupGuard::acquire("replay");
        }
        assert_eq!(std::env::var(ENV_AORTA_GROUP).unwrap(), "before");
        std::env::remove_var(ENV_AORTA_GROUP);
    }
}
