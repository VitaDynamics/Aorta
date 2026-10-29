//! Raw capture (`CaptureSubscriber`): a low-level, recorder-oriented "snoop"
//! subscription over any aorta namespace. Unlike the typed
//! [`crate::subscriber::Subscriber`], it performs NO header parsing, NO
//! dedup/reorder strategy, and NO schema typing — every received sample is
//! delivered verbatim, in arrival order, with its concrete key + raw payload +
//! attachment metadata (schema hash, traceparent) + timestamps.
//!
//! It is a deliberate observer escape hatch, not a normal consumption path.
//! Schema resolution is OFF this delivery path (the callback only carries the
//! opportunistic `schema_hash`; resolve lazily via [`crate::schema_resolve`]).
//! There is no internal queue: delivery is synchronous on the Zenoh callback
//! thread, so a slow callback backpressures the link — the consumer owns its own
//! buffering/drop policy.

use std::sync::Arc;
use std::time::Duration;

use zenoh::pubsub::Subscriber;

use crate::error::{AortaError, AortaResult, TransportError};
use crate::key_expr::KeyExprBuilder;
use crate::schema::SchemaHash;
use crate::time::unix_time_ns;
use crate::trace::TraceParent;

/// A single captured message. All borrows are valid ONLY for the callback's
/// duration — copy what you keep.
#[derive(Debug)]
pub struct CapturedSample<'a> {
    /// Concrete received keyexpr, e.g. `aorta/default/pub/camera/front`.
    pub key: &'a str,
    /// Aorta key suffix relative to the group root (e.g. `pub/camera/front`,
    /// `ctx/...`). Empty when the key is outside the configured group.
    pub key_suffix: &'a str,
    /// Raw, contiguous payload bytes (FlatBuffers for topic/ctx; opaque for sys/*).
    pub payload: &'a [u8],
    /// Schema hash from the attachment, if present. OPPORTUNISTIC: usually
    /// `None` (default `SchemaAttachPolicy` is *never*); resolve by key instead.
    pub schema_hash: Option<SchemaHash>,
    /// Traceparent from the attachment (publisher `TopicPublish` span), if present.
    pub traceparent: Option<TraceParent>,
    /// Publisher-assigned timestamp (Zenoh sample timestamp), if available.
    pub publish_time_ns: Option<u64>,
    /// Local receive timestamp (unix ns) stamped at ingress.
    pub recv_time_ns: u64,
}

/// Whether a delivered sample is a live put/publish or a ctx-snapshot reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureKind {
    Live,
    Snapshot,
}

/// Why a context-snapshot phase ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotDoneReason {
    /// The query reply stream closed before the deadline.
    QueryDrained,
    /// The deadline elapsed (replies may be incomplete).
    DeadlineElapsed,
}

/// One delivery to a capture callback.
#[derive(Debug)]
pub enum CaptureEvent<'a> {
    /// A captured message; `kind` distinguishes a snapshot reply from a live put.
    Sample {
        kind: CaptureKind,
        sample: CapturedSample<'a>,
    },
    /// Emitted exactly once at the end of a [`capture_context_snapshot`] phase.
    /// Never emitted by a plain live [`CaptureSubscriber`].
    SnapshotDone { reason: SnapshotDoneReason },
}

/// What to capture. Resolved through [`KeyExprBuilder`] so no caller hand-writes
/// `aorta/{group}/…`. Wildcards (`*`, `**`) are allowed in every variant.
#[derive(Clone, Debug)]
pub enum CaptureSelector {
    /// Topic namespace: `pub/<pattern>` (e.g. `**`, `camera/**`).
    Topic(String),
    /// Context namespace: `ctx/<pattern>`.
    Context(String),
    /// Telemetry stream: `sys/telemetry/<pattern>`.
    Telemetry(String),
    /// RPC record-mirror stream: `sys/record/srv/<pattern>` (producer is a
    /// separate workstream; raw-only until then).
    RpcRecord(String),
    /// Escape hatch: a full aorta keyexpr already rooted at `aorta/{group}/…`,
    /// validated against the group root.
    RawKeyExpr(String),
}

impl CaptureSelector {
    /// Resolve to a concrete keyexpr string under `key_builder`'s group.
    fn resolve(&self, key_builder: &KeyExprBuilder) -> AortaResult<String> {
        Ok(match self {
            CaptureSelector::Topic(p) => key_builder.topic_key_sub(p)?.into(),
            CaptureSelector::Context(p) => key_builder.context_prefix(p)?.into(),
            CaptureSelector::Telemetry(p) => key_builder.telemetry_key_sub(p)?.into(),
            CaptureSelector::RpcRecord(p) => key_builder.rpc_record_key_sub(p)?.into(),
            CaptureSelector::RawKeyExpr(k) => {
                let root = format!("aorta/{}/", key_builder.group());
                if !k.starts_with(&root) {
                    return Err(AortaError::invalid_argument(format!(
                        "raw capture keyexpr must be rooted at '{}': {}",
                        root, k
                    )));
                }
                k.clone()
            }
        })
    }
}

/// A live raw-capture subscription over one or more selectors. Holds the
/// underlying Zenoh subscribers; dropping it undeclares them.
pub struct CaptureSubscriber {
    _subscribers: Vec<Subscriber<()>>,
}

impl CaptureSubscriber {
    /// Create a capture subscriber. The callback runs on the Zenoh callback
    /// thread — keep it short and non-blocking (push to your own queue/ring).
    /// Multiple selectors create multiple subscribers under one handle; there is
    /// no cross-selector ordering guarantee.
    pub async fn new<F>(
        session: &zenoh::Session,
        key_builder: &KeyExprBuilder,
        selectors: &[CaptureSelector],
        callback: F,
    ) -> AortaResult<Self>
    where
        F: Fn(&CaptureEvent<'_>) + Send + Sync + 'static,
    {
        if selectors.is_empty() {
            return Err(AortaError::invalid_argument(
                "no capture selectors provided",
            ));
        }
        let callback = Arc::new(callback);
        let mut subscribers = Vec::with_capacity(selectors.len());
        for selector in selectors {
            let keyexpr = selector.resolve(key_builder)?;
            let cb = Arc::clone(&callback);
            let kb = key_builder.clone();
            let ke_for_err = keyexpr.clone();
            let subscriber = session
                .declare_subscriber(keyexpr)
                .callback(move |sample| deliver(&kb, &sample, CaptureKind::Live, cb.as_ref()))
                .await
                .map_err(|err| {
                    AortaError::transport(TransportError::backend(format!(
                        "capture declare_subscriber failed for '{}': {}",
                        ke_for_err, err
                    )))
                })?;
            subscribers.push(subscriber);
        }
        Ok(Self {
            _subscribers: subscribers,
        })
    }
}

/// Wildcard GET over `ctx/<pattern>`: each current value is delivered as a
/// `CaptureEvent::Sample { kind: Snapshot, .. }` (raw bytes, byte-identical to a
/// live ctx put), terminated by exactly one `CaptureEvent::SnapshotDone`.
///
/// No-loss recipe: declare the live `CaptureSubscriber` FIRST, then
/// call this, then drain queued live events after `SnapshotDone`.
pub async fn capture_context_snapshot<F>(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    pattern: &str,
    deadline: Duration,
    callback: F,
) -> AortaResult<()>
where
    F: Fn(&CaptureEvent<'_>) + Send + Sync + 'static,
{
    let selector: String = key_builder.context_prefix(pattern)?.into();
    // Single deadline budget across get + drain: stamp `started` BEFORE the get
    // so the drain loop's `remaining` reflects time the get already consumed
    // (otherwise worst-case wall-clock is ~2×deadline).
    let started = std::time::Instant::now();
    // QueryTarget::All so the snapshot gathers the current value from every ctx
    // owner queryable, not just the best-matching one.
    let replies = match tokio::time::timeout(
        deadline,
        session
            .get(&selector)
            .target(zenoh::query::QueryTarget::All)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .timeout(deadline),
    )
    .await
    {
        Ok(Ok(replies)) => replies,
        // Contract: exactly one SnapshotDone is always delivered, even when the
        // get itself fails — emit it (incomplete = DeadlineElapsed) before
        // surfacing the failure via the return value.
        Ok(Err(err)) => {
            callback(&CaptureEvent::SnapshotDone {
                reason: SnapshotDoneReason::DeadlineElapsed,
            });
            return Err(AortaError::transport(TransportError::backend(format!(
                "ctx snapshot get '{}' failed: {}",
                selector, err
            ))));
        }
        Err(_) => {
            callback(&CaptureEvent::SnapshotDone {
                reason: SnapshotDoneReason::DeadlineElapsed,
            });
            return Err(AortaError::timeout(format!(
                "ctx snapshot '{}' timed out",
                selector
            )));
        }
    };

    // Default reason; only the clean-drain path overrides it.
    let mut reason = SnapshotDoneReason::DeadlineElapsed;
    loop {
        let elapsed = started.elapsed();
        if elapsed >= deadline {
            break; // reason stays DeadlineElapsed
        }
        let remaining = deadline.saturating_sub(elapsed);
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => {
                if let Ok(sample) = reply.into_result() {
                    deliver(key_builder, &sample, CaptureKind::Snapshot, &callback);
                }
            }
            // Reply stream closed: the snapshot drained cleanly.
            Ok(Err(_)) => {
                reason = SnapshotDoneReason::QueryDrained;
                break;
            }
            // Deadline hit mid-wait: reason stays DeadlineElapsed.
            Err(_) => break,
        }
    }
    callback(&CaptureEvent::SnapshotDone { reason });
    Ok(())
}

/// Build a `CapturedSample` from a Zenoh sample and hand it to `cb`. All borrows
/// live only for this call (the locals below).
fn deliver<F>(
    key_builder: &KeyExprBuilder,
    sample: &zenoh::sample::Sample,
    kind: CaptureKind,
    cb: &F,
) where
    F: Fn(&CaptureEvent<'_>) + ?Sized,
{
    let full_key = sample.key_expr().as_str();
    let key_suffix = key_builder
        .key_suffix_from_full_key(full_key)
        .unwrap_or_default();
    let parts = sample
        .attachment()
        .map(|a| crate::attachment::decode(a.to_bytes().as_ref()))
        .unwrap_or_default();
    let payload = sample.payload().to_bytes();
    let event = CaptureEvent::Sample {
        kind,
        sample: CapturedSample {
            key: full_key,
            key_suffix: &key_suffix,
            payload: payload.as_ref(),
            schema_hash: parts.schema_hash,
            traceparent: parts.traceparent,
            publish_time_ns: sample.timestamp().map(|ts| ts.get_time().as_nanos()),
            recv_time_ns: unix_time_ns(),
        },
    };
    cb(&event);
}

#[cfg(test)]
#[path = "../test/capture_tests.rs"]
mod tests;
