use std::future::Future;

/// Spawn a fire-and-forget Tokio task whose panic is logged instead of
/// being silently swallowed.
///
/// Use only when the task genuinely does not need lifecycle management
/// (e.g. a one-shot Zenoh undeclare cleanup, a long-lived background
/// flusher whose lifetime matches the process). For tasks that must be
/// joined or aborted on shutdown, use `tokio::spawn` directly and retain
/// the `JoinHandle` — see `ContextStore::WatchHandle::shutdown` for the
/// canonical pattern.
///
/// Implementation: the future runs in its own Tokio task; a thin
/// supervisor task awaits the inner `JoinHandle` and emits a structured
/// `tracing::error!` on panic (target `aorta::task`, field `task =
/// <name>`). Cancellation (the supervisor being dropped before completion)
/// is logged at debug — fire-and-forget by definition does not propagate
/// cancellation, but a future Drop-on-shutdown path would benefit from
/// the trace.
pub fn spawn_logged<F>(name: &'static str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let _ = spawn_logged_task(name, fut);
}

/// Like [`spawn_logged`], but returns the supervisor's `JoinHandle` for
/// workers that are joined on shutdown. The panic is logged the moment it
/// happens — not only if/when someone joins — because most fleet nodes never
/// call `stop()`, so an unlogged worker panic would otherwise silently kill
/// that subsystem forever. Joining the returned handle
/// completes when the worker completes; note that aborting it does NOT abort
/// the inner worker (the telemetry runtimes only join, never abort).
pub(crate) fn spawn_logged_task<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = tokio::spawn(fut);
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => {}
            Err(err) if err.is_panic() => {
                let panic = err.into_panic();
                let msg = panic_payload_string(&panic);
                tracing::error!(
                    target: "aorta::task",
                    task = name,
                    "fire-and-forget task panicked: {}",
                    msg
                );
            }
            Err(err) => {
                tracing::debug!(
                    target: "aorta::task",
                    task = name,
                    "fire-and-forget task ended unexpectedly: {}",
                    err
                );
            }
        }
    })
}

/// Best-effort extraction of a panic payload's message. Shared by the
/// fire-and-forget task supervisor and the event-dispatch isolation in
/// `event_queue` so panic logging stays single-source.
pub(crate) fn panic_payload_string(panic: &Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn happy_path_completes_silently() {
        let notify = Arc::new(Notify::new());
        let n = notify.clone();
        spawn_logged("happy", async move {
            n.notify_one();
        });
        notify.notified().await;
    }

    #[tokio::test]
    async fn string_panic_is_caught_and_logged() {
        spawn_logged("string_panic", async {
            panic!("intentional panic");
        });
        // Yield enough times for the inner spawn + supervisor to run to
        // completion. The supervisor must observe the panic via JoinError
        // rather than crashing the runtime.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn non_string_panic_is_caught() {
        spawn_logged("non_string_panic", async {
            std::panic::panic_any(42u32);
        });
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn joinable_variant_logs_panic_and_join_completes() {
        // A shutdown join must complete normally after
        // a worker panic (the panic is logged at occurrence time by the
        // supervisor, not surfaced as a JoinError at stop time).
        let handle = spawn_logged_task("joinable_panic", async {
            panic!("intentional worker panic");
        });
        handle.await.expect("supervisor join must succeed");
    }

    #[tokio::test]
    async fn joinable_variant_happy_path_joins() {
        let notify = Arc::new(Notify::new());
        let n = notify.clone();
        let handle = spawn_logged_task("joinable_happy", async move {
            n.notify_one();
        });
        notify.notified().await;
        handle.await.expect("supervisor join must succeed");
    }

    #[tokio::test]
    async fn payload_string_handles_known_types() {
        let s: Box<dyn std::any::Any + Send + 'static> = Box::new("static str panic");
        assert_eq!(panic_payload_string(&s), "static str panic");

        let s: Box<dyn std::any::Any + Send + 'static> = Box::new("owned panic".to_string());
        assert_eq!(panic_payload_string(&s), "owned panic");

        let s: Box<dyn std::any::Any + Send + 'static> = Box::new(7i64);
        assert_eq!(panic_payload_string(&s), "<non-string panic payload>");
    }
}
