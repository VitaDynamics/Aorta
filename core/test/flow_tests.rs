//! Flow core primitives: `Budget` semantics, the `CancelToken` async
//! surface on a real runtime, and the explicit reentrancy detector.
//! (The cancel tree's structural/concurrency proofs live in `aorta_sync`.)

use super::{Budget, CancelReason, CancelToken};
use std::time::Duration;

// ---- Budget ----------------------------------------------------------------

#[test]
fn unlimited_budget_never_expires_and_has_no_wire_form() {
    let budget = Budget::unlimited();
    assert_eq!(budget.remaining(), None);
    assert!(!budget.is_expired());
    assert_eq!(budget.wire_relative_ns(), None);
}

#[test]
fn deadline_budget_counts_down_and_expires() {
    let budget = Budget::with_deadline(Duration::from_millis(20));
    let first = budget.remaining().expect("has a deadline");
    assert!(first <= Duration::from_millis(20));
    assert!(!budget.is_expired());
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(budget.remaining(), Some(Duration::ZERO));
    assert!(budget.is_expired());
}

#[test]
fn narrow_takes_the_minimum_and_never_widens() {
    let parent = Budget::with_deadline(Duration::from_millis(10));
    let narrowed = parent.narrow(Duration::from_secs(60));
    assert!(
        narrowed.remaining().expect("deadline") <= Duration::from_millis(10),
        "a child may narrow but never widen"
    );
    let tighter = parent.narrow(Duration::from_millis(1));
    assert!(tighter.remaining().expect("deadline") <= Duration::from_millis(1));
}

#[test]
fn narrow_on_unlimited_adopts_the_child_deadline() {
    let narrowed = Budget::unlimited().narrow(Duration::from_millis(50));
    let rem = narrowed.remaining().expect("child deadline applies");
    assert!(rem <= Duration::from_millis(50));
}

#[test]
fn wire_relative_ns_round_trips_as_a_relative_deadline() {
    let budget = Budget::with_deadline(Duration::from_secs(2));
    let ns = budget.wire_relative_ns().expect("finite");
    assert!(ns <= 2_000_000_000);
    let received = Budget::from_wire_relative_ns(ns);
    let rem = received.remaining().expect("finite");
    assert!(rem <= Duration::from_secs(2));
    assert!(!received.is_expired());
}

#[test]
fn expired_budget_wire_form_is_zero() {
    let budget = Budget::with_deadline(Duration::ZERO);
    assert!(budget.is_expired());
    assert_eq!(budget.wire_relative_ns(), Some(0));
}

// ---- CancelToken on a runtime ----------------------------------------------

#[tokio::test]
async fn cancelled_future_resolves_across_tasks() {
    let root = CancelToken::new_root();
    let child = root.child();
    let waiter = tokio::spawn(async move { child.cancelled().await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    root.cancel(CancelReason::Shutdown);
    let reason = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("cancel must wake the waiter")
        .expect("task");
    assert_eq!(reason, CancelReason::Shutdown);
}

#[tokio::test]
async fn cancelled_select_arm_fires_on_cancel() {
    let token = CancelToken::new_root();
    let token2 = token.clone();
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = token2.cancelled() => "cancelled",
            _ = tokio::time::sleep(Duration::from_secs(30)) => "timeout",
        }
    });
    token.cancel(CancelReason::Explicit);
    let outcome = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("select must resolve promptly")
        .expect("task");
    assert_eq!(outcome, "cancelled");
}

#[test]
fn is_cancelled_polls_lock_free_from_foreign_threads() {
    // The RT-safe poll contract: observable from any std thread.
    let token = CancelToken::new_root();
    let observer = token.clone();
    let seen = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while !observer.is_cancelled() {
            if start.elapsed() > Duration::from_secs(5) {
                return false;
            }
            std::thread::yield_now();
        }
        true
    });
    std::thread::sleep(Duration::from_millis(5));
    token.cancel(CancelReason::Explicit);
    assert!(seen.join().expect("observer thread"));
}

// ---- Explicit reentrancy detector -------------------------------------------

#[test]
fn callback_context_guard_makes_block_on_reentrancy_explicit_without_a_panic() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Count panics via a hook: the explicit detector must return the
    // reentrancy error WITHOUT constructing (and catching) a Tokio panic.
    let _env_lock = crate::test_support::ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let panics = Arc::new(AtomicUsize::new(0));
    let hook_panics = Arc::clone(&panics);
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |_| {
        hook_panics.fetch_add(1, Ordering::SeqCst);
    }));

    let result = {
        let _cb = super::reentrancy::CallbackContextGuard::enter();
        crate::ffi_block_on!(async { 7 })
    };
    std::panic::set_hook(prev_hook);

    assert!(
        result.is_err(),
        "inside a callback context a blocking call must classify reentrant"
    );
    assert_eq!(
        panics.load(Ordering::SeqCst),
        0,
        "the explicit detector must not rely on a constructed panic"
    );
}

#[test]
fn block_on_guard_makes_nested_ffi_block_on_reentrant() {
    // Model of the executor-spin shape: a plain C thread parks inside an
    // aorta block_on; a callback dispatched on that thread re-enters.
    let result = crate::ffi_block_on!(async {
        // Inside the outer block_on: a nested blocking call on this thread
        // must be refused explicitly.
        crate::ffi_block_on!(async { 1 }).is_err()
    });
    assert_eq!(
        result.ok(),
        Some(true),
        "nested ffi_block_on on the same thread must be reentrant"
    );
}

#[test]
fn block_on_guard_resets_after_the_call() {
    assert!(crate::ffi_block_on!(async { 1 }).is_ok());
    // Sequential (non-nested) calls on the same thread stay legal.
    assert!(crate::ffi_block_on!(async { 2 }).is_ok());
}
