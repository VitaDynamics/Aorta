use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::runtime::spawn_logged;

#[derive(Debug, Clone)]
struct CapturedEvent {
    level: Level,
    target: String,
    message: String,
    task: Option<String>,
}

#[derive(Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl CaptureLayer {
    fn new(events: Arc<Mutex<Vec<CapturedEvent>>>) -> Self {
        Self { events }
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let metadata = event.metadata();
        let captured = CapturedEvent {
            level: *metadata.level(),
            target: metadata.target().to_string(),
            message: visitor.message,
            task: visitor.task,
        };
        if let Ok(mut guard) = self.events.lock() {
            guard.push(captured);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
    task: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{:?}", value),
            "task" => self.task = Some(format!("{:?}", value).trim_matches('"').to_string()),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_string(),
            "task" => self.task = Some(value.to_string()),
            _ => {}
        }
    }
}

fn install_capture() -> (
    Arc<Mutex<Vec<CapturedEvent>>>,
    tracing::subscriber::DefaultGuard,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer::new(events.clone());
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (events, guard)
}

async fn wait_for_supervisor() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn spawn_logged_emits_error_event_on_string_panic() {
    let (events, _guard) = install_capture();

    spawn_logged("string_panic_site", async {
        panic!("deliberate string panic");
    });
    wait_for_supervisor().await;

    let captured = events.lock().unwrap();
    let matches: Vec<_> = captured
        .iter()
        .filter(|e| e.target == "aorta::task" && e.level == Level::ERROR)
        .collect();
    assert!(
        !matches.is_empty(),
        "expected at least one ERROR event at target=aorta::task, got: {:?}",
        *captured
    );
    let evt = &matches[0];
    assert_eq!(evt.task.as_deref(), Some("string_panic_site"));
    assert!(
        evt.message.contains("deliberate string panic"),
        "unexpected message: {}",
        evt.message
    );
}

#[tokio::test]
async fn spawn_logged_emits_error_event_on_non_string_panic() {
    let (events, _guard) = install_capture();

    spawn_logged("non_string_panic_site", async {
        std::panic::panic_any(0xdeadbeefu32);
    });
    wait_for_supervisor().await;

    let captured = events.lock().unwrap();
    let matches: Vec<_> = captured
        .iter()
        .filter(|e| e.target == "aorta::task" && e.level == Level::ERROR)
        .collect();
    assert!(!matches.is_empty());
    let evt = &matches[0];
    assert_eq!(evt.task.as_deref(), Some("non_string_panic_site"));
    assert!(
        evt.message.contains("<non-string panic payload>"),
        "expected fallback message, got: {}",
        evt.message
    );
}

#[tokio::test]
async fn spawn_logged_happy_path_emits_no_error_event() {
    let (events, _guard) = install_capture();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    spawn_logged("happy_site", async move {
        let _ = tx.send(());
    });
    let _ = rx.await;
    wait_for_supervisor().await;

    let captured = events.lock().unwrap();
    let unexpected: Vec<_> = captured
        .iter()
        .filter(|e| e.target == "aorta::task" && e.level == Level::ERROR)
        .collect();
    assert!(
        unexpected.is_empty(),
        "expected no error events on happy path, got: {:?}",
        unexpected
    );
}

// ── The process-wide panic hook mirrors panics into
// the log plane. The hook fires on the panicking thread, so a same-thread
// caught panic is observable through the thread-local capture subscriber. ──

#[test]
fn panic_hook_mirrors_panic_into_log_plane() {
    let (events, _guard) = install_capture();
    crate::logging::install_panic_hook_once();

    let result = std::panic::catch_unwind(|| {
        panic!("hook-test deliberate panic");
    });
    assert!(
        result.is_err(),
        "panic must still propagate to catch_unwind"
    );

    let captured = events.lock().unwrap();
    let matches: Vec<_> = captured
        .iter()
        .filter(|e| e.target == "aorta::panic" && e.level == Level::ERROR)
        .collect();
    assert!(
        !matches.is_empty(),
        "expected an ERROR event at target=aorta::panic, got: {:?}",
        *captured
    );
    assert!(
        matches[0].message.contains("hook-test deliberate panic"),
        "unexpected message: {}",
        matches[0].message
    );
}

#[test]
fn panic_hook_install_is_idempotent() {
    crate::logging::install_panic_hook_once();
    crate::logging::install_panic_hook_once();
    // A caught panic after double-install must not abort or recurse.
    assert!(std::panic::catch_unwind(|| panic!("idempotency probe")).is_err());
}
