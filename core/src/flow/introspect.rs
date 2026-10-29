//! Live scope-tree introspection : the operator's answer
//! to "which scopes exist, what are they waiting on?" — the missing tool
//! when a `close()` hangs (see the close() watchdog, which names ONE scope;
//! this names them all).
//!
//! Lazily activated: until [`enable`] is called, `Scope` creation pays ONE
//! `OnceLock::get()` load (deliberately: never `get_or_init` on that
//! path) and registers nothing. Enable it in debug builds, tools, or on
//! demand; a registered scope costs one `Weak` in a global list, pruned on
//! every [`dump`].

use std::sync::{Mutex, OnceLock, Weak};
use std::time::Duration;

use super::scope::ScopeInner;
use aorta_sync::CancelReason;

static REGISTRY: OnceLock<Mutex<Vec<Weak<ScopeInner>>>> = OnceLock::new();

/// One scope's live snapshot (flat list — parent edges are a recorded
/// follow-up; names + counts already unblock hang debugging).
#[derive(Clone, Debug)]
pub struct ScopeInfo {
    pub name: &'static str,
    pub live_children: u64,
    /// Stragglers detached by a grace-expired shutdown, not yet
    /// self-resolved.
    pub detached: u64,
    pub cancelled: Option<CancelReason>,
    pub budget_remaining: Option<Duration>,
    pub closing: bool,
    pub user_handles: usize,
}

/// Turn registration ON (idempotent). Scopes created BEFORE enabling are
/// not visible — enable early (tools do this at startup).
pub fn enable() {
    let _ = REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
}

/// `true` when the registry is active.
pub fn enabled() -> bool {
    REGISTRY.get().is_some()
}

pub(crate) fn register(inner: &std::sync::Arc<ScopeInner>) {
    // Review-pinned: `get()` only — the disabled path is one load, no init.
    if let Some(registry) = REGISTRY.get() {
        registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(std::sync::Arc::downgrade(inner));
    }
}

/// Snapshot every live scope (dead weak entries are pruned in passing).
pub fn dump() -> Vec<ScopeInfo> {
    let Some(registry) = REGISTRY.get() else {
        return Vec::new();
    };
    let mut list = registry.lock().unwrap_or_else(|p| p.into_inner());
    let mut out = Vec::new();
    list.retain(|weak| {
        let Some(inner) = weak.upgrade() else {
            return false; // prune
        };
        out.push(inner.snapshot());
        true
    });
    out
}
