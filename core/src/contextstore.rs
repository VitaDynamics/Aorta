//! ContextStore SDK V1 — single-process core.
//!
//! Implements the ContextStore V1 design. This file provides the
//! single-process behaviour: explicit `declare_owned_prefix`, typed
//! `set` / `get` / `watch`, owner-side latest-value cache, and the
//! per-process `source_instance_id`. Cross-process conflict detection
//! and the `watch_with_initial_snapshot` helper land in a later phase.
//!
//! The module is intentionally async-only and operates against the
//! existing `aorta_core::Session`. The owner-side Queryable replies from
//! the in-memory cache; the internal Subscriber installed by
//! `declare_owned_prefix` is a no-op stub today and will host the
//! conflict-detection logic.

use std::collections::{HashMap, VecDeque};
use std::future::IntoFuture;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use zenoh::key_expr::KeyExpr;
use zenoh::pubsub::Subscriber;
use zenoh::qos::CongestionControl;
use zenoh::query::{ConsolidationMode, Query, QueryTarget, Queryable};

use crate::contextstore_fb::{
    self, encode_value_bool, encode_value_f64, encode_value_i64, encode_value_string,
    encode_value_u64, root_as_context_value, ContextKind, ContextValueMeta,
};
use crate::error::{AortaError, AortaResult};
use crate::key_expr::KeyExprBuilder;
use crate::liveliness::LivelinessManager;
use crate::log_prelude::*;
use crate::node::Node;
use crate::owner_conflict_fb::encode_owner_conflict_event;
use crate::session::Session;

/// Default deadline for `get_*` when no explicit deadline is provided.
pub const DEFAULT_GET_DEADLINE: Duration = Duration::from_millis(2_000);

/// Default deadline for `watch_with_initial_snapshot` when the caller does
/// not override `WatchSnapshotOptions::snapshot_deadline`.
pub const DEFAULT_SNAPSHOT_DEADLINE: Duration = Duration::from_millis(1_000);

/// Result of a typed `get_*` call.
#[derive(Clone, Debug)]
pub struct ContextRead<T> {
    pub value: T,
    pub is_fresh: bool,
    pub stamp_ns: u64,
    pub expires_at_ns: u64,
    pub source_node: String,
    pub source_instance_id: String,
    pub sequence: u64,
}

/// Discriminator for events delivered by `watch` and
/// `watch_with_initial_snapshot`.
///
/// `Live` events come from the Zenoh subscriber; `Snapshot` events are
/// replies to the wildcard `get` issued at watch startup; `SnapshotDone`
/// is a single marker that closes the snapshot phase. After
/// `SnapshotDone` the watcher only sees `Live` events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextChangeKind {
    Live,
    Snapshot,
    SnapshotDone,
}

/// Reason carried by a `SnapshotDone` event. There is
/// deliberately no `NoOwner` reason — in a manifest-less system there is
/// no reliable way to prove that no owner exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotDoneReason {
    /// Zenoh's reply stream closed normally.
    QueryDrained,
    /// `WatchSnapshotOptions::snapshot_deadline` elapsed before the
    /// reply stream drained.
    DeadlineElapsed,
}

/// Options for `watch_with_initial_snapshot`.
#[derive(Clone, Debug)]
pub struct WatchSnapshotOptions {
    pub snapshot_deadline: Duration,
}

impl Default for WatchSnapshotOptions {
    fn default() -> Self {
        Self {
            snapshot_deadline: DEFAULT_SNAPSHOT_DEADLINE,
        }
    }
}

/// Decoded context update delivered to a watcher.
///
/// For `kind == SnapshotDone`, the event carries `snapshot_done_reason`
/// and the value / metadata fields are placeholders (empty strings, zero
/// numerics). For `kind == Live | Snapshot`, all fields are populated
/// from the wire `ContextValue`.
#[derive(Clone, Debug)]
pub struct ContextChange {
    pub kind: ContextChangeKind,
    pub key: String,
    pub value_kind: ContextKind,
    pub bool_value: bool,
    pub i64_value: i64,
    pub u64_value: u64,
    pub f64_value: f64,
    pub string_value: Option<String>,
    pub stamp_ns: u64,
    pub expires_at_ns: u64,
    pub source_node: String,
    pub source_instance_id: String,
    pub sequence: u64,
    pub snapshot_done_reason: Option<SnapshotDoneReason>,
}

impl ContextChange {
    /// Build a `Live` or `Snapshot` event by decoding `sample.payload()`.
    /// Returns `None` if the payload is not a valid `ContextValue` —
    /// callers should silently drop in that case.
    fn from_sample(sample: &zenoh::sample::Sample, kind: ContextChangeKind) -> Option<Self> {
        debug_assert!(
            kind == ContextChangeKind::Live || kind == ContextChangeKind::Snapshot,
            "ContextChange::from_sample only builds Live/Snapshot events"
        );
        let payload_bytes = sample.payload().to_bytes();
        let parsed = root_as_context_value(&payload_bytes)?;
        Some(Self {
            kind,
            key: sample.key_expr().as_str().to_string(),
            value_kind: parsed.kind(),
            bool_value: parsed.bool_value(),
            i64_value: parsed.i64_value(),
            u64_value: parsed.u64_value(),
            f64_value: parsed.f64_value(),
            string_value: parsed.string_value().map(|s| s.to_string()),
            stamp_ns: parsed.stamp_ns(),
            expires_at_ns: parsed.expires_at_ns(),
            source_node: parsed.source_node().unwrap_or("").to_string(),
            source_instance_id: parsed.source_instance_id().unwrap_or("").to_string(),
            sequence: parsed.sequence(),
            snapshot_done_reason: None,
        })
    }

    fn snapshot_done(reason: SnapshotDoneReason) -> Self {
        Self {
            kind: ContextChangeKind::SnapshotDone,
            key: String::new(),
            value_kind: ContextKind::None,
            bool_value: false,
            i64_value: 0,
            u64_value: 0,
            f64_value: 0.0,
            string_value: None,
            stamp_ns: 0,
            expires_at_ns: 0,
            source_node: String::new(),
            source_instance_id: String::new(),
            sequence: 0,
            snapshot_done_reason: Some(reason),
        }
    }

    pub fn is_snapshot_done(&self) -> bool {
        self.kind == ContextChangeKind::SnapshotDone
    }
}

/// Test-only undeclare-failure injection (P2-a): when set, the NEXT
/// `WatchHandle::shutdown` / `OwnerLivelinessHandle::shutdown` treats
/// the undeclare as failed (one-shot), deterministically exercising the
/// 'drain unconfirmed' repeat-error path.
#[cfg(test)]
pub(crate) static TEST_FAIL_NEXT_UNDECLARE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Handle returned by `watch` and `watch_with_initial_snapshot`. Dropping
/// the handle aborts the in-flight snapshot task and lets Zenoh's
/// background `undeclare` eventually run on the underlying subscriber.
///
/// **Drop is best-effort.** A subscriber callback may still fire briefly
/// after `drop` returns because the Zenoh undeclare is asynchronous —
/// any captured `user_data` (callback-side state) must therefore outlive
/// `drop` in callers that care about determinism. For deterministic
/// teardown — required by the C ABI where the subscriber may capture
/// raw pointers into caller-owned memory — call
/// [`WatchHandle::shutdown`] before dropping the handle.
#[must_use = "dropping a WatchHandle stops the watch; bind it for the watch lifetime"]
pub struct WatchHandle {
    subscriber: Mutex<Option<Subscriber<()>>>,
    snapshot_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set when an undeclare failed after the subscriber was taken
    /// (P2-a): the drain is UNCONFIRMED — callbacks may still be in
    /// flight — so repeat `shutdown()` calls must keep returning the
    /// error instead of a false Ok. Carries the original error message.
    drain_unconfirmed: Mutex<Option<String>>,
}

impl WatchHandle {
    fn live_only(subscriber: Subscriber<()>) -> Self {
        Self {
            subscriber: Mutex::new(Some(subscriber)),
            snapshot_task: Mutex::new(None),
            drain_unconfirmed: Mutex::new(None),
        }
    }

    fn with_snapshot(
        subscriber: Subscriber<()>,
        snapshot_task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            subscriber: Mutex::new(Some(subscriber)),
            snapshot_task: Mutex::new(Some(snapshot_task)),
            drain_unconfirmed: Mutex::new(None),
        }
    }

    /// Synchronously tear down the watch:
    /// 1. Abort the snapshot task (no further snapshot/SnapshotDone
    ///    events will fire).
    /// 2. Await Zenoh's `undeclare` on the subscriber so no live event
    ///    callback can fire after this future resolves.
    ///
    /// Callers crossing FFI boundaries (or any caller whose closure
    /// captures pointers/state with bounded lifetime) MUST call
    /// `shutdown` before dropping the handle. Plain Drop only schedules
    /// undeclare in the background and a callback may still execute
    /// briefly after Drop returns.
    ///
    /// # Re-entrant teardown is NOT supported
    ///
    /// Calling `shutdown` (or `aorta_ctx_watch_free`, or the C++
    /// `WatchHandle::Reset()`) from *inside* the watch's own callback
    /// is unsupported. The shutdown waits for the in-flight callback
    /// to return; if that callback is the very thing waiting for
    /// shutdown, you get a self-deadlock on the snapshot task path
    /// (or a "Cannot start a runtime from within a runtime" panic
    /// when crossing the C ABI). This is the async analogue of
    /// `delete this` — same rule applies: signal completion from the
    /// callback (e.g. via a channel or `Notify`) and call shutdown
    /// from outside the callback.
    pub async fn shutdown(&self) -> AortaResult<()> {
        // tokio's abort() only *requests* cancellation; if the task is
        // synchronously inside the user callback (cb(change)) the
        // signal only fires at the next await. Awaiting the JoinHandle
        // blocks until the task actually exits, including after any
        // in-flight callback returns. Without this the C ABI
        // `aorta_ctx_watch_free` could return while a snapshot
        // callback is still touching `user_data`, breaking the
        // documented "no callback fires after free" guarantee.
        if let Some(task) = self.snapshot_task.lock().unwrap().take() {
            task.abort();
            // JoinError::Cancelled is the expected outcome; ignore it.
            let _ = task.await;
        }
        let subscriber = self.subscriber.lock().unwrap().take();
        if let Some(s) = subscriber {
            // wait_callbacks(): prevents a use-after-free — the
            // C ABI `aorta_ctx_watch_free` / C++ `WatchHandle::Reset`
            // free callback state immediately after shutdown returns.
            //
            // Propagate the undeclare error instead
            // of swallowing it. zenoh 1.9.0's undeclare returns early
            // WITHOUT waiting the callback sync group when the undeclare
            // itself fails, so "shutdown returned" only means "callbacks
            // drained" when the undeclare SUCCEEDED. The C ABI free
            // paths map this error to a non-OK result and the C++ side
            // retains callback state unless it sees AORTA_OK — the
            // single invariant the wrapper teardown relies on.
            //
            // P2-a: the subscriber was already take()n, so a second
            // shutdown() would see None and return a false Ok. Persist
            // the failed state so repeat shutdown() keeps returning the
            // error (the drain can never be confirmed afterwards).
            #[cfg(test)]
            if TEST_FAIL_NEXT_UNDECLARE.swap(false, std::sync::atomic::Ordering::SeqCst) {
                *self.drain_unconfirmed.lock().unwrap() =
                    Some("test-injected undeclare failure".to_string());
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "WatchHandle::shutdown: subscriber undeclare failed: {}",
                        "test-injected undeclare failure"
                    )),
                ));
            }
            if let Err(err) = s.undeclare().wait_callbacks().await {
                *self.drain_unconfirmed.lock().unwrap() = Some(err.to_string());
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "WatchHandle::shutdown: subscriber undeclare failed: {}",
                        err
                    )),
                ));
            }
            Ok(())
        } else if let Some(msg) = self.drain_unconfirmed.lock().unwrap().clone() {
            // P2-a: a previous shutdown's undeclare failed — callback
            // drain was never confirmed. Keep returning the error.
            Err(AortaError::transport(
                crate::error::TransportError::backend(format!(
                    "WatchHandle::shutdown: previous undeclare failed (drain unconfirmed): {}",
                    msg
                )),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        if let Some(task) = self.snapshot_task.lock().unwrap().take() {
            task.abort();
        }
        // Subscriber drop happens implicitly when `subscriber` Mutex is
        // dropped. Zenoh's undeclare is scheduled in the background; for
        // deterministic teardown call `shutdown().await` first.
    }
}

/// Event delivered by [`ContextStore::watch_ctx_owner_liveliness`]: one
/// owning ContextStore became alive (`alive == true`, token declared at
/// its first `declare_owned_prefix`) or went away (`alive == false`,
/// token withdrawn by shutdown / store drop / process death). The
/// `source_instance_id` is the store's per-process identity — the same
/// id carried on every context event and query reply — so consumers
/// can correlate prefixes to instances.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerLivelinessEvent {
    pub node_name: String,
    pub source_instance_id: String,
    pub alive: bool,
}

/// Handle returned by [`ContextStore::watch_ctx_owner_liveliness`].
/// Dropping the handle lets Zenoh's background `undeclare` eventually
/// run; for deterministic teardown (no callback after return — required
/// by the C ABI) call [`OwnerLivelinessHandle::shutdown`] first.
#[must_use = "dropping an OwnerLivelinessHandle stops the liveliness watch; bind it for the watch lifetime"]
pub struct OwnerLivelinessHandle {
    subscriber: Mutex<Option<Subscriber<()>>>,
    /// P2-a: see `WatchHandle::drain_unconfirmed`.
    drain_unconfirmed: Mutex<Option<String>>,
}

impl OwnerLivelinessHandle {
    /// Synchronously tear down the watch: await Zenoh's `undeclare` on
    /// the liveliness subscriber so no callback can fire after this
    /// future resolves.
    pub async fn shutdown(&self) -> AortaResult<()> {
        let subscriber = self.subscriber.lock().unwrap().take();
        if let Some(s) = subscriber {
            // wait_callbacks(): the pinned zenoh 1.9.0 defaults
            // wait_callbacks=false, so without this an in-flight
            // callback can still run AFTER shutdown returns; the C++
            // / C-ABI free paths free callback state immediately after,
            // which would be a use-after-free.
            //
            // Propagate the undeclare error — see
            // WatchHandle::shutdown for the full rationale. AORTA_OK
            // from the C ABI free now means "undeclare succeeded AND
            // callbacks drained".
            //
            // P2-a: persist the failed state so repeat shutdown() keeps
            // returning the error (see WatchHandle::shutdown).
            #[cfg(test)]
            if TEST_FAIL_NEXT_UNDECLARE.swap(false, std::sync::atomic::Ordering::SeqCst) {
                *self.drain_unconfirmed.lock().unwrap() =
                    Some("test-injected undeclare failure".to_string());
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "OwnerLivelinessHandle::shutdown: subscriber undeclare failed: {}",
                        "test-injected undeclare failure"
                    )),
                ));
            }
            if let Err(err) = s.undeclare().wait_callbacks().await {
                *self.drain_unconfirmed.lock().unwrap() = Some(err.to_string());
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "OwnerLivelinessHandle::shutdown: subscriber undeclare failed: {}",
                        err
                    )),
                ));
            }
            Ok(())
        } else if let Some(msg) = self.drain_unconfirmed.lock().unwrap().clone() {
            Err(AortaError::transport(crate::error::TransportError::backend(
                format!(
                    "OwnerLivelinessHandle::shutdown: previous undeclare failed (drain unconfirmed): {}",
                    msg
                ),
            )))
        } else {
            Ok(())
        }
    }
}

/// Parse a context owner-liveliness token key into `(node_name,
/// source_instance_id)`. Returns `None` for keys that do not carry
/// exactly the two expected trailing segments (e.g. a foreign raw
/// token that does not follow the store shape).
fn parse_ctx_owner_token(key: &str, prefix: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix(prefix)?;
    let mut segments = rest.split('/');
    let node_name = segments.next()?;
    let instance = segments.next()?;
    if node_name.is_empty() || instance.is_empty() || segments.next().is_some() {
        return None;
    }
    Some((node_name.to_string(), instance.to_string()))
}

/// ContextStore primitive. One instance per `Node` writer; cheap to clone
/// (the underlying state is shared via `Arc`).
///
/// # Lifecycle
///
/// Dropping a `ContextStore` only schedules Zenoh's `undeclare` in the
/// background — Drop is RAII best-effort. For a deterministic teardown,
/// e.g. before the same writer process re-creates a store on the same
/// prefix, or in tests that immediately re-issue queries, call
/// [`ContextStore::shutdown`] explicitly. Production owner shutdown
/// paths should call `shutdown` so rolling deploys observe a clean
/// hand-off rather than a stale Queryable racing against the new owner.
#[derive(Clone)]
pub struct ContextStore {
    inner: Arc<Inner>,
}

/// Test-only fault-injection rule for `set_inner`'s wire put (compiled
/// out of production builds). Lets tests reproduce deterministic put
/// failure and in-flight windows so the commit/rollback ordering can be
/// exercised without a real transport fault. A real `session.put` error
/// is only reachable by closing the session, which also kills the
/// owner Queryable — so the cache ordering itself cannot be observed
/// through a real failure; this gate models the identical failure class
/// (transport error after the cache commit).
#[cfg(test)]
pub(crate) type PutMatcher = Arc<dyn Fn(&str, &[u8]) -> bool + Send + Sync>;

#[cfg(test)]
pub(crate) type PutWindowFactory = Arc<dyn Fn() -> TestPutWindow + Send + Sync>;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestPutRule {
    /// Selects which puts the rule applies to, by (full key, payload).
    pub(crate) matches: PutMatcher,
    /// Whether a matched put's post-commit outcome is a transport error
    /// (`true`) or a success that skips the wire put (`false`).
    pub(crate) fail: bool,
    /// Optional window before the cache commit (after sequence
    /// allocation): parks the put so tests can force preemption between
    /// sequence allocation and commit. Fires before the payload is
    /// encoded (the per-key ordering section owns encoding), so the
    /// `matches` predicate receives an empty payload at this point and
    /// must discriminate by key.
    pub(crate) pre_commit: Option<PutWindowFactory>,
    /// Optional window after the cache commit, before the put outcome:
    /// parks the put so tests can observe the committed cache while the
    /// wire put is still in flight.
    pub(crate) post_commit: Option<PutWindowFactory>,
}

/// One rendezvous window: `entered` fires when the put reaches the
/// window; the put then awaits `release` before proceeding.
#[cfg(test)]
pub(crate) struct TestPutWindow {
    pub(crate) entered: tokio::sync::oneshot::Sender<()>,
    pub(crate) release: tokio::sync::oneshot::Receiver<()>,
}

/// A set of [`TestPutRule`]s armed on a store. The first matching rule
/// applies.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestPutGate {
    pub(crate) rules: Vec<TestPutRule>,
}

/// Per-key ordering state for the owner cache. The guarding
/// `tokio::sync::Mutex` serializes {sequence allocation -> cache
/// commit} per key, so the per-key sequence order equals the commit
/// order; `seq` lives under the same lock, so no separate seq mutex is
/// needed. Entries are created lazily on first `set` and retained for
/// the store lifetime (mirroring the old per-key seq map).
#[derive(Default)]
struct PerKeyState {
    seq: u64,
}

struct Inner {
    session: Session,
    keys: KeyExprBuilder,
    source_node: String,
    source_instance_id: String,
    /// Pre-computed key for the conflict telemetry stream. Owned String
    /// because Zenoh `put` accepts `&str` directly and we don't need a
    /// borrowed `KeyExpr<'static>`.
    conflict_telemetry_key: String,
    declared: RwLock<Vec<DeclaredPrefix>>,
    cache: RwLock<HashMap<String, Vec<u8>>>,
    key_states: Mutex<HashMap<String, Arc<tokio::sync::Mutex<PerKeyState>>>>,
    /// Serializes `declare_owned_prefix` against `shutdown` (and against
    /// each other): held by declare across precheck -> install ->
    /// rollback and by shutdown from entry, so the two are linearized.
    /// This closes (a) the ghost-token window — a failed declare
    /// withdraws the token it created, and no concurrent declare can
    /// have succeeded meanwhile — and (b) the declare/shutdown race
    /// where shutdown could withdraw a just-created token before the
    /// prefix install, leaving a prefix without a token.
    lifecycle: tokio::sync::Mutex<()>,
    /// Owner-liveliness token, declared once at the FIRST successful
    /// `declare_owned_prefix` and withdrawn on `shutdown` / store drop.
    /// `None` until the first declaration. Guarded by an
    /// async mutex; the lifecycle mutex above makes the first-declare a
    /// serialized transaction (exactly one token per store).
    liveliness_token: tokio::sync::Mutex<Option<Arc<LivelinessManager>>>,
    /// Test-only put fault-injection gate; `None` in production builds.
    #[cfg(test)]
    test_put_gate: Mutex<Option<TestPutGate>>,
    /// Test-only fault injection: when set, the next
    /// `declare_owned_prefix` on this store fails the Queryable /
    /// Subscriber declaration (consumed one-shot). Exercises the
    /// post-token rollback paths deterministically.
    #[cfg(test)]
    test_fail_queryable_declare: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    test_fail_subscriber_declare: std::sync::atomic::AtomicBool,
}

/// Snapshot-phase state for a single `watch_with_initial_snapshot`
/// session. Each watcher owns one of these via Arc; the Zenoh
/// subscriber callback consults it to decide whether to deliver a Live
/// event directly or buffer it until `SnapshotDone` flips the phase.
enum SnapshotPhase {
    /// Snapshot task has not yet emitted `SnapshotDone`. Live events
    /// land in the queue; `deliver_snapshot_done` drains the queue
    /// while still holding the phase lock so any subscriber callback
    /// blocked on the lock is delivered after the buffered ones.
    Buffering(VecDeque<ContextChange>),
    /// `SnapshotDone` has been emitted; subscriber callbacks deliver
    /// directly.
    Done,
}

struct DeclaredPrefix {
    pattern: String,
    keyexpr: KeyExpr<'static>,
    // Held in `Mutex<Option<...>>` so `shutdown` can `take` and explicitly
    // `await` `undeclare`. Drop alone relies on Zenoh's eventual
    // background undeclare, which is fine for production but unreliable
    // for tests that immediately re-issue queries on the same prefix.
    queryable: Mutex<Option<Queryable<()>>>,
    subscriber: Mutex<Option<Subscriber<()>>>,
}

impl ContextStore {
    /// Create a new ContextStore for the given `Node`. Generates a fresh
    /// 16-hex-character `source_instance_id` per call so two ContextStore
    /// instances within the same process get distinct identities.
    pub fn new(node: &Node) -> Self {
        let source_node = node.name().to_string();
        let source_instance_id = generate_instance_id();
        let keys = node.key_builder().clone();
        let session = node.session().clone();
        // The telemetry key is `aorta/{group}/sys/telemetry/vital/ctx/owner_conflict`.
        // Construction can only fail if the node's group is malformed,
        // which `Node::new` would have already rejected.
        let conflict_telemetry_key = keys
            .ctx_owner_conflict_telemetry_key()
            .expect("ContextStore: invalid node group prevents owner_conflict telemetry key");

        aorta_core_info!(
            "ContextStore created: source_node={}, source_instance_id={}",
            source_node,
            source_instance_id
        );

        Self {
            inner: Arc::new(Inner {
                session,
                keys,
                source_node,
                source_instance_id,
                conflict_telemetry_key,
                declared: RwLock::new(Vec::new()),
                cache: RwLock::new(HashMap::new()),
                key_states: Mutex::new(HashMap::new()),
                lifecycle: tokio::sync::Mutex::new(()),
                liveliness_token: tokio::sync::Mutex::new(None),
                #[cfg(test)]
                test_put_gate: Mutex::new(None),
                #[cfg(test)]
                test_fail_queryable_declare: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                test_fail_subscriber_declare: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// The human-readable writer node name carried in `source_node`.
    pub fn source_node(&self) -> &str {
        &self.inner.source_node
    }

    /// The SDK-generated, per-process unique writer identity carried in
    /// `source_instance_id`. Conflict detection compares this,
    /// not `source_node`.
    pub fn source_instance_id(&self) -> &str {
        &self.inner.source_instance_id
    }

    /// Declare a context prefix this writer owns. Must be called before
    /// any `set` on a key under this prefix.
    ///
    /// The future returned by this method only resolves after both the
    /// owner-side Zenoh Queryable AND the internal Subscriber have been
    /// fully declared. This honours the bootstrap contract: "set called
    /// before declare resolves returns PrefixNotDeclared".
    ///
    /// Calling `declare_owned_prefix` twice with the same pattern is a
    /// no-op. Declarations cannot be revoked at runtime in V1 — they are
    /// torn down only when the `ContextStore` (and its last clone) is
    /// dropped.
    pub async fn declare_owned_prefix(&self, pattern: &str) -> AortaResult<()> {
        // The node name is embedded in the owner-liveliness token
        // key as a single segment. `Node::new` only rejects empty names,
        // so validate the owner identity at the ContextStore boundary
        // with a precise error naming the constraint — a multi-segment
        // name ('foo/bar'), whitespace, or a reserved character would
        // otherwise break the token key and the reader-side parse.
        let token_key = self
            .inner
            .keys
            .ctx_owner_liveliness_key(&self.inner.source_node, &self.inner.source_instance_id)
            .map_err(|err| {
                AortaError::invalid_argument(format!(
                    "ContextStore owner-liveliness identity invalid: node name '{}' must be a single key segment (no '/', no whitespace, no reserved characters '$' '#' '?', no wildcards): {}",
                    self.inner.source_node, err
                ))
            })?;

        // Serialized lifecycle: declare holds the mutex across
        // precheck -> install -> rollback so it is linearized against
        // `shutdown` and against other declares. The mid-flight
        // duplicate/overlap re-checks that the pre-fix code needed for
        // concurrent declares cannot occur anymore and are gone.
        let _lifecycle = self.inner.lifecycle.lock().await;
        let keyexpr = self.inner.keys.context_prefix(pattern)?;
        let keyexpr_str = keyexpr.as_str().to_string();

        // Idempotent re-declare of the SAME prefix is a no-op.
        // Overlapping declarations on the same store are rejected:
        // each Queryable's `handle_query` walks every cached key
        // covered by its own declared keyexpr, so two declarations
        // where one includes the other would emit duplicate replies
        // for keys covered by both. Operators wanting to own a wide
        // prefix should declare one wide prefix; wanting to split
        // ownership across modules means declaring disjoint prefixes
        // on disjoint stores.
        {
            let declared = self.inner.declared.read().unwrap();
            if declared.iter().any(|d| d.keyexpr.as_str() == keyexpr_str) {
                return Ok(());
            }
            for d in declared.iter() {
                if d.keyexpr.includes(&keyexpr) || keyexpr.includes(&d.keyexpr) {
                    return Err(AortaError::invalid_argument(format!(
                        "context prefix '{}' overlaps existing declared prefix '{}'; declared prefixes on a single ContextStore must be disjoint",
                        pattern, d.pattern
                    )));
                }
            }
        }

        // FIRST declaration on this store: declare the owner-liveliness
        // token (`aorta/{group}/sys/alive/ctx/{node}/{instance}`).
        // This is the head of a serialized transaction: every
        // later failure in this call withdraws the token IT created,
        // so a failed declare never leaves a ghost token — the
        // store can never report alive with zero successful prefixes.
        // Because the lifecycle mutex serializes declares, no concurrent
        // declare can have succeeded meanwhile, so the token in the slot
        // is always the one this call created.
        let mut token_guard = self.inner.liveliness_token.lock().await;
        let created_token = token_guard.is_none();
        if created_token {
            let token =
                LivelinessManager::declare_key(self.inner.session.inner(), &token_key).await?;
            *token_guard = Some(token);
        }
        drop(token_guard);

        // Test seam: fail the Queryable declaration (one-shot) so the
        // token rollback path is exercised deterministically.
        #[cfg(test)]
        if self
            .inner
            .test_fail_queryable_declare
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.rollback_created_token(created_token).await;
            return Err(AortaError::transport(
                crate::error::TransportError::backend(
                    "test-injected declare_queryable failure".to_string(),
                ),
            ));
        }

        // Install owner-side Queryable. Each query is dispatched to a
        // tokio task that walks the cache and replies with every entry
        // whose key intersects the query's key expression. The callback
        // captures *this* declared prefix's keyexpr so it only replies
        // for keys it actually owns — without this scoping, a store
        // declaring multiple prefixes would emit duplicate snapshot
        // replies on broad gets.
        //
        // The callback captures Weak<Inner>: a strong capture
        // would keep Inner — and with it the session and the
        // owner-liveliness token — alive forever via the
        // Inner -> session -> callback -> Inner cycle, so dropping the
        // last store clone would never withdraw the token.
        let qry_inner = Arc::downgrade(&self.inner);
        let qry_declared_ke = keyexpr.clone();
        let queryable = match self
            .inner
            .session
            .inner()
            .declare_queryable(keyexpr.clone())
            .callback(move |query| {
                let Some(inner) = qry_inner.upgrade() else {
                    return;
                };
                let declared_ke = qry_declared_ke.clone();
                crate::runtime::spawn_logged(
                    "ctx_query_handler",
                    handle_query(inner, query, declared_ke),
                );
            })
            .await
        {
            Ok(q) => q,
            Err(err) => {
                // Withdraw the token created by THIS call.
                self.rollback_created_token(created_token).await;
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "ContextStore declare_queryable failed for {}: {}",
                        keyexpr_str, err
                    )),
                ));
            }
        };

        // Test seam: fail the Subscriber declaration (one-shot) so the
        // queryable rollback + token withdrawal path is exercised
        // deterministically.
        #[cfg(test)]
        if self
            .inner
            .test_fail_subscriber_declare
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            if let Err(rb) = queryable.undeclare().await {
                aorta_core_warn!(
                    "ContextStore declare rollback: queryable.undeclare failed: {}",
                    rb
                );
            }
            self.rollback_created_token(created_token).await;
            return Err(AortaError::transport(
                crate::error::TransportError::backend(
                    "test-injected declare_subscriber failure".to_string(),
                ),
            ));
        }

        // Install internal Subscriber for conflict detection. The
        // callback decodes each sample, compares `source_instance_id`
        // against `self`, and on mismatch logs + emits an
        // `OwnerConflictEvent` telemetry put. Self-puts (matching
        // instance id) are silently ignored.
        //
        // If declare_subscriber fails after declare_queryable succeeded,
        // explicitly undeclare the Queryable so we never leave
        // half-installed state — and withdraw the token created by this
        // call. Weak<Inner> capture: see the Queryable comment
        // above.
        let conflict_inner = Arc::downgrade(&self.inner);
        let subscriber = match self
            .inner
            .session
            .inner()
            .declare_subscriber(keyexpr.clone())
            .callback(move |sample| {
                let Some(inner) = conflict_inner.upgrade() else {
                    return;
                };
                handle_self_subscriber_sample(&inner, sample);
            })
            .await
        {
            Ok(s) => s,
            Err(err) => {
                if let Err(rb) = queryable.undeclare().await {
                    aorta_core_warn!(
                        "ContextStore declare rollback: queryable.undeclare failed: {}",
                        rb
                    );
                }
                self.rollback_created_token(created_token).await;
                return Err(AortaError::transport(
                    crate::error::TransportError::backend(format!(
                        "ContextStore declare_subscriber failed for {}: {}",
                        keyexpr_str, err
                    )),
                ));
            }
        };

        // Install the declaration. The write lock still guards
        // concurrent readers of `declared` (e.g. `is_covered`).
        self.inner.declared.write().unwrap().push(DeclaredPrefix {
            pattern: pattern.to_string(),
            keyexpr,
            queryable: Mutex::new(Some(queryable)),
            subscriber: Mutex::new(Some(subscriber)),
        });

        Ok(())
    }

    pub async fn shutdown(&self) -> AortaResult<()> {
        // Serialized lifecycle: from entry, shutdown is
        // linearized against `declare_owned_prefix`, so it can never
        // interleave a token create with a prefix install (which would
        // leave a prefix without a token).
        let _lifecycle = self.inner.lifecycle.lock().await;
        let prefixes = {
            let mut declared = self.inner.declared.write().unwrap();
            std::mem::take(&mut *declared)
        };
        for prefix in prefixes {
            let q = prefix.queryable.lock().unwrap().take();
            let s = prefix.subscriber.lock().unwrap().take();
            if let Some(q) = q {
                if let Err(err) = q.undeclare().await {
                    aorta_core_warn!(
                        "ContextStore::shutdown: queryable.undeclare failed: {}",
                        err
                    );
                }
            }
            if let Some(s) = s {
                if let Err(err) = s.undeclare().await {
                    aorta_core_warn!(
                        "ContextStore::shutdown: subscriber.undeclare failed: {}",
                        err
                    );
                }
            }
        }
        self.inner.cache.write().unwrap().clear();
        self.inner.key_states.lock().unwrap().clear();
        // Withdraw the owner-liveliness token: liveliness subscribers
        // observe a Delete for this store's token. A later
        // `declare_owned_prefix` on this store declares a fresh token.
        self.inner.liveliness_token.lock().await.take();
        Ok(())
    }

    /// Returns true if `relative_key` falls under at least one declared
    /// owned prefix. Public for diagnostics; used internally by `set_*`.
    pub fn is_key_declared(&self, relative_key: &str) -> AortaResult<bool> {
        let keyexpr = self.inner.keys.context_key(relative_key)?;
        Ok(self.is_covered(&keyexpr))
    }

    pub async fn set_bool(&self, relative_key: &str, value: bool) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::Bool, 0, |meta| {
            encode_value_bool(value, meta)
        })
        .await
    }

    pub async fn set_bool_with_expiry(
        &self,
        relative_key: &str,
        value: bool,
        expires_at_ns: u64,
    ) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::Bool, expires_at_ns, |meta| {
            encode_value_bool(value, meta)
        })
        .await
    }

    pub async fn set_i64(&self, relative_key: &str, value: i64) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::I64, 0, |meta| {
            encode_value_i64(value, meta)
        })
        .await
    }

    pub async fn set_i64_with_expiry(
        &self,
        relative_key: &str,
        value: i64,
        expires_at_ns: u64,
    ) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::I64, expires_at_ns, |meta| {
            encode_value_i64(value, meta)
        })
        .await
    }

    pub async fn set_u64(&self, relative_key: &str, value: u64) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::U64, 0, |meta| {
            encode_value_u64(value, meta)
        })
        .await
    }

    pub async fn set_u64_with_expiry(
        &self,
        relative_key: &str,
        value: u64,
        expires_at_ns: u64,
    ) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::U64, expires_at_ns, |meta| {
            encode_value_u64(value, meta)
        })
        .await
    }

    pub async fn set_f64(&self, relative_key: &str, value: f64) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::F64, 0, |meta| {
            encode_value_f64(value, meta)
        })
        .await
    }

    pub async fn set_f64_with_expiry(
        &self,
        relative_key: &str,
        value: f64,
        expires_at_ns: u64,
    ) -> AortaResult<()> {
        self.set_inner(relative_key, ContextKind::F64, expires_at_ns, |meta| {
            encode_value_f64(value, meta)
        })
        .await
    }

    pub async fn set_string(&self, relative_key: &str, value: &str) -> AortaResult<()> {
        let owned = value.to_string();
        self.set_inner(relative_key, ContextKind::String, 0, move |meta| {
            encode_value_string(&owned, meta)
        })
        .await
    }

    pub async fn set_string_with_expiry(
        &self,
        relative_key: &str,
        value: &str,
        expires_at_ns: u64,
    ) -> AortaResult<()> {
        let owned = value.to_string();
        self.set_inner(
            relative_key,
            ContextKind::String,
            expires_at_ns,
            move |meta| encode_value_string(&owned, meta),
        )
        .await
    }

    async fn set_inner<F>(
        &self,
        relative_key: &str,
        _kind: ContextKind,
        expires_at_ns: u64,
        encode: F,
    ) -> AortaResult<()>
    where
        F: FnOnce(&ContextValueMeta) -> Vec<u8>,
    {
        // Step 1: synchronous validation BEFORE any Zenoh side-effect.
        let keyexpr = self.inner.keys.context_key(relative_key)?;
        let full_key: String = keyexpr.as_str().to_string();

        if !self.is_covered(&keyexpr) {
            return Err(AortaError::prefix_not_declared(format!(
                "set on '{}' but no declared owned prefix covers it",
                full_key
            )));
        }

        #[cfg(test)]
        {
            // Test gate: pre-commit point. Fires before the per-key
            // ordering section, so matching closures receive an empty
            // payload and must discriminate by key.
            let _ = self.test_gate_put(&full_key, &[], false).await;
        }

        // Step 2: per key, {allocate sequence -> commit cache} under ONE
        // ordering. Holding the per-key async mutex across sequence
        // allocation and the cache write makes sequence order == commit
        // order by construction; distinct keys still proceed
        // concurrently. The cache is monotonic and NEVER rolled back:
        // a failed/dropped put leaves the cache AHEAD of watchers
        // (the healable direction — `get`/snapshot/watch re-creation
        // converge on the committed value), never behind. Sequence
        // numbers consumed by failed puts are a documented gap
        // (consumers must not rely on contiguity).
        let key_state = {
            let mut map = self.inner.key_states.lock().unwrap();
            map.entry(full_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(PerKeyState::default())))
                .clone()
        };
        let payload = {
            let mut guard = key_state.lock().await;
            guard.seq = guard.seq.saturating_add(1);
            let seq = guard.seq;
            let stamp_ns = stamp_now_ns();
            let meta = ContextValueMeta {
                stamp_ns,
                expires_at_ns,
                source_node: self.inner.source_node.clone(),
                source_instance_id: self.inner.source_instance_id.clone(),
                sequence: seq,
            };
            let payload = encode(&meta);
            self.inner
                .cache
                .write()
                .unwrap()
                .insert(full_key.clone(), payload.clone());
            payload
        };

        // Step 3: actual put on the wire, after the cache commit.
        #[cfg(test)]
        if let Some(result) = self.test_gate_put(&full_key, &payload, true).await {
            return result;
        }
        // QoS: CongestionControl::Block — uniformly, no opt-out knob
        // The canonical context plane is low-rate and must
        // not silently drop under ordinary congestion; a writer that
        // cannot tolerate brief backpressure does not belong on it.
        //
        // Honest semantics (congestion drop): the pinned zenoh fork
        // bounds the wait (`wait_before_close`, default 5 s); on
        // overflow the message is dropped and the transport closed, and
        // `session.put` returns Ok regardless. Residual loss therefore
        // coincides with transport disruption and is healed reader-side
        // (bounded watch re-creation + TTL/heartbeat, item F), NOT by
        // this flag. The first-put-before-route-known race needs no
        // writer-side barrier either — reader snapshot queries converge
        // on the committed owner cache (first-put route race).
        let put_result = self
            .inner
            .session
            .inner()
            .put(keyexpr, payload)
            .congestion_control(CongestionControl::Block)
            .await;
        if let Err(err) = put_result {
            // The cache is intentionally NOT rolled back: the committed
            // value stays visible to owner-side reads (Queryable) and
            // snapshot queries. Contract: "A failed/dropped put leaves
            // the cache ahead of watchers — the direction
            // get/snapshot/watch-re-creation heal — never behind."
            return Err(AortaError::transport(
                crate::error::TransportError::backend(format!(
                    "ContextStore put failed for {}: {}",
                    full_key, err
                )),
            ));
        }

        Ok(())
    }

    /// Withdraw the owner-liveliness token created by THIS declare call.
    /// Only meaningful on the first-declare transaction; with
    /// the lifecycle mutex held, no concurrent declare can have
    /// succeeded meanwhile, so taking the slot is always correct when
    /// `created` is true — and never touches a token a previous declare
    /// created.
    async fn rollback_created_token(&self, created: bool) {
        if created {
            self.inner.liveliness_token.lock().await.take();
        }
    }

    fn is_covered(&self, keyexpr: &KeyExpr<'_>) -> bool {
        let declared = self.inner.declared.read().unwrap();
        declared.iter().any(|d| d.keyexpr.includes(keyexpr))
    }

    /// Arm the test-only put gate. See [`TestPutGate`]; compiled out of
    /// production builds.
    #[cfg(test)]
    pub(crate) fn test_arm_put_gate(&self, gate: TestPutGate) {
        *self.inner.test_put_gate.lock().unwrap() = Some(gate);
    }

    /// Apply the test-only put gate at the pre-commit point
    /// (`post_commit == false`: may park in the rule's pre-commit
    /// window, never decides the outcome) or the post-commit point
    /// (`post_commit == true`: parks in the post-commit window and
    /// decides the put outcome per the rule's `fail` flag). Returns
    /// `None` when no rule matches, or at the pre-commit point, meaning
    /// the caller continues to the real wire put.
    #[cfg(test)]
    async fn test_gate_put(
        &self,
        full_key: &str,
        payload: &[u8],
        post_commit: bool,
    ) -> Option<AortaResult<()>> {
        let rule = {
            let gate = self.inner.test_put_gate.lock().unwrap();
            gate.as_ref()
                .and_then(|g| g.rules.iter().find(|r| (r.matches)(full_key, payload)))
                .cloned()
        };
        let rule = rule?;
        let window = if post_commit {
            rule.post_commit.as_ref().map(|f| f())
        } else {
            rule.pre_commit.as_ref().map(|f| f())
        };
        if let Some(window) = window {
            let _ = window.entered.send(());
            let _ = window.release.await;
        }
        if !post_commit {
            // The pre-commit point only parks; the outcome is decided at
            // the post-commit point (or by the real put).
            return None;
        }
        if rule.fail {
            Some(Err(AortaError::transport(
                crate::error::TransportError::backend(format!(
                    "ContextStore put failed for {}: test-injected transport failure",
                    full_key
                )),
            )))
        } else {
            Some(Ok(()))
        }
    }

    /// Test-only: fail the next Queryable declaration on this store
    /// (one-shot), exercising the token rollback path deterministically.
    #[cfg(test)]
    pub(crate) fn test_fail_queryable_declare(&self) {
        self.inner
            .test_fail_queryable_declare
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: fail the next Subscriber declaration on this store
    /// (one-shot), exercising the queryable rollback + token withdrawal
    /// path deterministically.
    #[cfg(test)]
    pub(crate) fn test_fail_subscriber_declare(&self) {
        self.inner
            .test_fail_subscriber_declare
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub async fn get_bool(&self, relative_key: &str) -> AortaResult<ContextRead<bool>> {
        self.get_bool_with_deadline(relative_key, DEFAULT_GET_DEADLINE)
            .await
    }

    pub async fn get_bool_with_deadline(
        &self,
        relative_key: &str,
        deadline: Duration,
    ) -> AortaResult<ContextRead<bool>> {
        self.get_inner(
            relative_key,
            ContextKind::Bool,
            |v| v.bool_value(),
            deadline,
        )
        .await
    }

    pub async fn get_i64(&self, relative_key: &str) -> AortaResult<ContextRead<i64>> {
        self.get_i64_with_deadline(relative_key, DEFAULT_GET_DEADLINE)
            .await
    }

    pub async fn get_i64_with_deadline(
        &self,
        relative_key: &str,
        deadline: Duration,
    ) -> AortaResult<ContextRead<i64>> {
        self.get_inner(relative_key, ContextKind::I64, |v| v.i64_value(), deadline)
            .await
    }

    pub async fn get_u64(&self, relative_key: &str) -> AortaResult<ContextRead<u64>> {
        self.get_u64_with_deadline(relative_key, DEFAULT_GET_DEADLINE)
            .await
    }

    pub async fn get_u64_with_deadline(
        &self,
        relative_key: &str,
        deadline: Duration,
    ) -> AortaResult<ContextRead<u64>> {
        self.get_inner(relative_key, ContextKind::U64, |v| v.u64_value(), deadline)
            .await
    }

    pub async fn get_f64(&self, relative_key: &str) -> AortaResult<ContextRead<f64>> {
        self.get_f64_with_deadline(relative_key, DEFAULT_GET_DEADLINE)
            .await
    }

    pub async fn get_f64_with_deadline(
        &self,
        relative_key: &str,
        deadline: Duration,
    ) -> AortaResult<ContextRead<f64>> {
        self.get_inner(relative_key, ContextKind::F64, |v| v.f64_value(), deadline)
            .await
    }

    pub async fn get_string(&self, relative_key: &str) -> AortaResult<ContextRead<String>> {
        self.get_string_with_deadline(relative_key, DEFAULT_GET_DEADLINE)
            .await
    }

    pub async fn get_string_with_deadline(
        &self,
        relative_key: &str,
        deadline: Duration,
    ) -> AortaResult<ContextRead<String>> {
        self.get_inner(
            relative_key,
            ContextKind::String,
            |v| v.string_value().unwrap_or("").to_string(),
            deadline,
        )
        .await
    }

    async fn get_inner<T, F>(
        &self,
        relative_key: &str,
        expected_kind: ContextKind,
        extract: F,
        deadline: Duration,
    ) -> AortaResult<ContextRead<T>>
    where
        F: FnOnce(&contextstore_fb::ContextValue<'_>) -> T,
    {
        let keyexpr = self.inner.keys.context_key(relative_key)?;
        let full_key: String = keyexpr.as_str().to_string();

        // Propagate the deadline into the underlying Zenoh `get` so the
        // router-side query is cancelled when we stop caring; pair it
        // with a tokio-level timeout on `recv_async` so the caller has
        // a hard upper bound regardless of Zenoh's behaviour.
        let replies = self
            .inner
            .session
            .inner()
            .get(&keyexpr)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(deadline)
            .await
            .map_err(|err| {
                AortaError::transport(crate::error::TransportError::backend(format!(
                    "ContextStore get failed for {}: {}",
                    full_key, err
                )))
            })?;

        let reply = match tokio::time::timeout(deadline, replies.recv_async()).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                // recv_async returned Err: the reply stream drained
                // (Zenoh closed it) without any matching reply. With a
                // healthy network this means no Queryable currently
                // owns the key — treat as missing.
                return Err(AortaError::not_found(format!(
                    "context key '{}' has no live owner",
                    full_key
                )));
            }
            Err(_) => {
                // Local deadline elapsed before any reply arrived. This
                // is a transport timeout, not a definitive "missing".
                return Err(AortaError::timeout(format!(
                    "context get '{}' timed out after {:?}",
                    full_key, deadline
                )));
            }
        };

        let sample = reply.result().map_err(|e| {
            AortaError::internal(format!(
                "context get '{}' returned error reply: {}",
                full_key,
                crate::transport::reply_error_text(e)
            ))
        })?;

        let payload_bytes = sample.payload().to_bytes();
        let parsed = root_as_context_value(&payload_bytes).ok_or_else(|| {
            AortaError::internal(format!(
                "context get '{}' returned malformed ContextValue",
                full_key
            ))
        })?;

        if parsed.kind() != expected_kind {
            return Err(AortaError::kind_mismatch(format!(
                "context key '{}': expected {} got {}",
                full_key,
                expected_kind.name(),
                parsed.kind().name()
            )));
        }

        let value = extract(&parsed);
        let now_ns = stamp_now_ns();
        let expires_at_ns = parsed.expires_at_ns();
        let is_fresh = expires_at_ns == 0 || now_ns < expires_at_ns;

        Ok(ContextRead {
            value,
            is_fresh,
            stamp_ns: parsed.stamp_ns(),
            expires_at_ns,
            source_node: parsed.source_node().unwrap_or("").to_string(),
            source_instance_id: parsed.source_instance_id().unwrap_or("").to_string(),
            sequence: parsed.sequence(),
        })
    }

    /// Subscribe to context updates under `prefix`. The callback fires
    /// per Zenoh Sample; malformed payloads are dropped silently. Drop
    /// the returned `WatchHandle` to unsubscribe.
    pub async fn watch<F>(&self, prefix: &str, callback: F) -> AortaResult<WatchHandle>
    where
        F: Fn(ContextChange) + Send + Sync + 'static,
    {
        let keyexpr = self.inner.keys.context_prefix(prefix)?;
        let cb: Arc<dyn Fn(ContextChange) + Send + Sync> = Arc::new(callback);

        let subscriber = self
            .inner
            .session
            .inner()
            .declare_subscriber(keyexpr)
            .callback(move |sample| {
                if let Some(change) = ContextChange::from_sample(&sample, ContextChangeKind::Live) {
                    cb(change);
                }
            })
            .await
            .map_err(|err| {
                AortaError::transport(crate::error::TransportError::backend(format!(
                    "ContextStore watch declare_subscriber failed: {}",
                    err
                )))
            })?;

        Ok(WatchHandle::live_only(subscriber))
    }

    /// Subscribe to a context prefix and replay the current owner state
    /// before live events:
    ///
    /// 1. Install a Zenoh subscriber on the prefix.
    /// 2. Issue a wildcard `get` on the same prefix.
    /// 3. Each get reply is delivered as `ContextChangeKind::Snapshot`.
    /// 4. After the reply stream closes (`QueryDrained`) or the
    ///    deadline elapses (`DeadlineElapsed`) the SDK delivers a
    ///    single `ContextChangeKind::SnapshotDone` carrying the reason.
    /// 5. Live events arriving during the snapshot phase are buffered
    ///    and delivered after `SnapshotDone`, in arrival order.
    /// 6. Replies arriving after the deadline marker are discarded —
    ///    they are NOT promoted to live events.
    ///
    /// If the caller needs strict per-key completeness after
    /// `SnapshotDone`, issue an explicit `get_*` for each missing key.
    ///
    /// # Threading
    ///
    /// The buffered-event drain runs while holding an internal mutex
    /// that the live subscriber callback also acquires to enqueue. A
    /// slow user `callback` therefore blocks live deliveries while the
    /// drain is in progress; keep the callback fast and detach long
    /// work to a separate task. The serialised drain is intentional —
    /// it preserves the spec's "buffered events before new live"
    /// ordering at the cost of a brief mutual-exclusion window.
    pub async fn watch_with_initial_snapshot<F>(
        &self,
        prefix: &str,
        options: WatchSnapshotOptions,
        callback: F,
    ) -> AortaResult<WatchHandle>
    where
        F: Fn(ContextChange) + Send + Sync + 'static,
    {
        let keyexpr = self.inner.keys.context_prefix(prefix)?;
        let cb: Arc<dyn Fn(ContextChange) + Send + Sync> = Arc::new(callback);

        let phase: Arc<Mutex<SnapshotPhase>> =
            Arc::new(Mutex::new(SnapshotPhase::Buffering(VecDeque::new())));

        // Install the live subscriber first so any put that arrives
        // during the initial get is captured (either delivered after
        // SnapshotDone if still in Buffering, or directly if Done).
        let phase_for_sub = Arc::clone(&phase);
        let cb_for_sub = Arc::clone(&cb);
        let subscriber = self
            .inner
            .session
            .inner()
            .declare_subscriber(keyexpr.clone())
            .callback(move |sample| {
                let change = match ContextChange::from_sample(&sample, ContextChangeKind::Live) {
                    Some(c) => c,
                    None => return,
                };
                let mut guard = phase_for_sub.lock().unwrap();
                match &mut *guard {
                    SnapshotPhase::Buffering(q) => q.push_back(change),
                    SnapshotPhase::Done => {
                        drop(guard);
                        cb_for_sub(change);
                    }
                }
            })
            .await
            .map_err(|err| {
                AortaError::transport(crate::error::TransportError::backend(format!(
                    "ContextStore watch_with_initial_snapshot subscribe failed: {}",
                    err
                )))
            })?;

        // Spawn the snapshot task. It owns the get reply stream so the
        // stream is dropped (and Zenoh cleans it up) when the task
        // exits — including on abort from `WatchHandle::drop`.
        let session_for_snap = self.inner.session.clone();
        let cb_for_snap = Arc::clone(&cb);
        let phase_for_snap = Arc::clone(&phase);
        let snap_keyexpr = keyexpr;
        let deadline = options.snapshot_deadline;
        let snapshot_task = tokio::spawn(async move {
            run_snapshot_task(
                session_for_snap,
                snap_keyexpr,
                deadline,
                cb_for_snap,
                phase_for_snap,
            )
            .await;
        });

        Ok(WatchHandle::with_snapshot(subscriber, snapshot_task))
    }

    /// List the patterns this store has declared, in declaration order.
    /// Returned for diagnostics / tests; not part of the public V1 SDK
    /// surface beyond visibility.
    pub fn declared_prefixes(&self) -> Vec<String> {
        let declared = self.inner.declared.read().unwrap();
        declared.iter().map(|d| d.pattern.clone()).collect()
    }

    /// Watch the liveliness of ContextStore owners in this store's
    /// group: a thin passthrough over Zenoh's liveliness subscriber
    /// with `history(true)` — current tokens are replayed
    /// as `alive == true` events, then live join/leave events follow
    /// (`alive == false` on token withdrawal via shutdown / store drop
    /// / process death). No mapping keys, no merge protocol — consumers
    /// correlate prefixes to instances via `source_instance_id`, the
    /// same id on every context event and query reply.
    ///
    /// The callback fires on Zenoh worker threads; keep it fast and
    /// synchronise any captured state. Events whose token key does not
    /// carry exactly `{node_name}/{source_instance_id}` as its trailing
    /// segments are silently dropped. Call
    /// [`OwnerLivelinessHandle::shutdown`] for deterministic teardown;
    /// dropping the handle is best-effort.
    pub async fn watch_ctx_owner_liveliness<F>(
        &self,
        callback: F,
    ) -> AortaResult<OwnerLivelinessHandle>
    where
        F: Fn(OwnerLivelinessEvent) + Send + Sync + 'static,
    {
        let keyexpr = self.inner.keys.ctx_owner_liveliness_query()?;
        // Everything before the trailing `**` is the fixed token-key
        // prefix `aorta/{group}/sys/alive/ctx/`.
        let prefix = keyexpr.as_str().trim_end_matches("**").to_string();
        let cb: Arc<dyn Fn(OwnerLivelinessEvent) + Send + Sync> = Arc::new(callback);
        let subscriber = self
            .inner
            .session
            .inner()
            .liveliness()
            .declare_subscriber(keyexpr)
            .history(true)
            .callback(move |sample| {
                let alive = match sample.kind() {
                    zenoh::sample::SampleKind::Put => true,
                    zenoh::sample::SampleKind::Delete => false,
                };
                let Some((node_name, source_instance_id)) =
                    parse_ctx_owner_token(sample.key_expr().as_str(), &prefix)
                else {
                    return;
                };
                cb(OwnerLivelinessEvent {
                    node_name,
                    source_instance_id,
                    alive,
                });
            })
            .await
            .map_err(|err| {
                AortaError::transport(crate::error::TransportError::backend(format!(
                    "ContextStore watch_ctx_owner_liveliness declare_subscriber failed: {}",
                    err
                )))
            })?;

        Ok(OwnerLivelinessHandle {
            subscriber: Mutex::new(Some(subscriber)),
            drain_unconfirmed: Mutex::new(None),
        })
    }
}

/// Conflict-detection callback for the internal Subscriber installed by
/// `declare_owned_prefix`. Decodes the sample, compares
/// `source_instance_id` against `self`, and on mismatch logs and
/// schedules an `OwnerConflictEvent` telemetry put.
fn handle_self_subscriber_sample(inner: &Arc<Inner>, sample: zenoh::sample::Sample) {
    let payload_bytes = sample.payload().to_bytes();
    let parsed = match root_as_context_value(&payload_bytes) {
        Some(p) => p,
        None => return,
    };
    let foreign_instance_str = parsed.source_instance_id().unwrap_or("");
    if foreign_instance_str == inner.source_instance_id.as_str() {
        // Own put echoed back to us. Suppress.
        return;
    }
    let foreign_node = parsed.source_node().unwrap_or("").to_string();
    let foreign_instance = foreign_instance_str.to_string();
    let foreign_stamp = parsed.stamp_ns();
    let foreign_seq = parsed.sequence();
    let key = sample.key_expr().as_str().to_string();
    // Release the borrow into payload_bytes before subsequent allocs.
    let _ = parsed;
    drop(payload_bytes);

    aorta_core_warn!(
        "context.owner_conflict: key={} self_node={} self_instance={} foreign_node={} foreign_instance={} foreign_stamp={} foreign_seq={}",
        key,
        inner.source_node,
        inner.source_instance_id,
        foreign_node,
        foreign_instance,
        foreign_stamp,
        foreign_seq
    );

    let observed_at = stamp_now_ns();
    let event_bytes = encode_owner_conflict_event(
        &key,
        &inner.source_node,
        &inner.source_instance_id,
        &foreign_node,
        &foreign_instance,
        foreign_stamp,
        foreign_seq,
        observed_at,
    );

    let session = inner.session.clone();
    let telemetry_key = inner.conflict_telemetry_key.clone();
    tokio::spawn(async move {
        if let Err(err) = session.inner().put(telemetry_key, event_bytes).await {
            aorta_core_warn!("context.owner_conflict telemetry put failed: {}", err);
        }
    });
}

/// Snapshot-phase loop for `watch_with_initial_snapshot`. Owns the get
/// reply stream; exits as soon as a `SnapshotDone` is delivered, which
/// drops the stream and lets Zenoh clean up.
///
/// We deliberately do NOT pass `.timeout(deadline)` to Zenoh — the
/// deadline is enforced locally with `tokio::time::timeout` so we can
/// distinguish a clean reply-stream drain (`QueryDrained`) from a
/// local-side fire (`DeadlineElapsed`). The deadline covers BOTH the
/// `.get(...)` future itself and every subsequent `recv_async`, so a
/// stalled transport during get-issue still produces a
/// `DeadlineElapsed` rather than blocking the watcher forever.
async fn run_snapshot_task(
    session: Session,
    keyexpr: KeyExpr<'static>,
    deadline: Duration,
    cb: Arc<dyn Fn(ContextChange) + Send + Sync>,
    phase: Arc<Mutex<SnapshotPhase>>,
) {
    let start = tokio::time::Instant::now();

    let get_future = session
        .inner()
        .get(&keyexpr)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None);

    let replies = match tokio::time::timeout(deadline, get_future.into_future()).await {
        Ok(Ok(r)) => r,
        Ok(Err(err)) => {
            aorta_core_warn!("watch_with_initial_snapshot get failed: {}", err);
            deliver_snapshot_done(&cb, &phase, SnapshotDoneReason::QueryDrained);
            return;
        }
        Err(_) => {
            deliver_snapshot_done(&cb, &phase, SnapshotDoneReason::DeadlineElapsed);
            return;
        }
    };

    loop {
        let elapsed = start.elapsed();
        if elapsed >= deadline {
            deliver_snapshot_done(&cb, &phase, SnapshotDoneReason::DeadlineElapsed);
            return;
        }
        let remaining = deadline - elapsed;
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => {
                if let Ok(sample) = reply.result() {
                    if let Some(change) =
                        ContextChange::from_sample(sample, ContextChangeKind::Snapshot)
                    {
                        cb(change);
                    }
                }
            }
            Ok(Err(_)) => {
                deliver_snapshot_done(&cb, &phase, SnapshotDoneReason::QueryDrained);
                return;
            }
            Err(_) => {
                deliver_snapshot_done(&cb, &phase, SnapshotDoneReason::DeadlineElapsed);
                return;
            }
        }
    }
}

/// Emit `SnapshotDone` and drain any live events that were buffered
/// during the snapshot phase. The phase lock is held across the drain
/// so concurrent live events arriving on the subscriber callback wait
/// behind the buffered ones rather than slipping in front.
fn deliver_snapshot_done(
    cb: &Arc<dyn Fn(ContextChange) + Send + Sync>,
    phase: &Arc<Mutex<SnapshotPhase>>,
    reason: SnapshotDoneReason,
) {
    cb(ContextChange::snapshot_done(reason));
    let mut guard = phase.lock().unwrap();
    let buffered = match std::mem::replace(&mut *guard, SnapshotPhase::Done) {
        SnapshotPhase::Buffering(q) => q,
        SnapshotPhase::Done => VecDeque::new(),
    };
    for change in buffered {
        cb(change);
    }
    drop(guard);
}

async fn handle_query(inner: Arc<Inner>, query: Query, declared_ke: KeyExpr<'static>) {
    // Snapshot the cache under the read lock, then release before any await.
    // Two filters apply: (1) the cached key must fall under THIS
    // declaration's prefix — without this, a store with multiple declared
    // prefixes would emit duplicate replies because every Queryable's
    // callback walks the full cache; (2) the query's keyexpr must
    // intersect the cached key — Zenoh has already routed the query to
    // us based on declared_ke, but the actual query keyexpr can be a
    // narrower expression.
    let q_ke = query.key_expr().clone().into_owned();
    let entries: Vec<(String, Vec<u8>)> = {
        let cache = inner.cache.read().unwrap();
        cache
            .iter()
            .filter_map(|(k, v)| match KeyExpr::new(k.as_str()) {
                Ok(cached_ke) => {
                    if declared_ke.includes(&cached_ke) && q_ke.intersects(&cached_ke) {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                }
                Err(_) => None,
            })
            .collect()
    };

    for (key, payload) in entries {
        let cached_ke = match KeyExpr::new(key.as_str()) {
            Ok(ke) => ke.into_owned(),
            Err(_) => continue,
        };
        if let Err(err) = query.reply(cached_ke, payload).await {
            aorta_core_warn!(
                "ContextStore queryable reply failed for key '{}': {}",
                key,
                err
            );
        }
    }
}

fn generate_instance_id() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    id[..16].to_string()
}

fn stamp_now_ns() -> u64 {
    let ns = crate::time::now().as_nanos();
    if ns < 0 {
        0
    } else {
        ns as u64
    }
}
