use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use zenoh::bytes::ZBytes;
use zenoh::Wait;

use crate::error::{AortaError, AortaResult};
use crate::payload::AortaPayload;

/// Transport-facing publisher interface for Core publish path.
pub trait TransportPublisher: Send + Sync {
    fn publish_zbytes(&self, payload: ZBytes, attachment: Option<ZBytes>) -> AortaResult<()>;
    fn publish(&self, payload: AortaPayload, attachment: Option<ZBytes>) -> AortaResult<()>;
    fn undeclare(&self) -> AortaResult<()>;

    /// Resolve whether at least one matching subscriber currently exists for
    /// this publisher's key expression (Locality::Any).
    ///
    /// Returned as a boxed future so the trait stays object-safe behind
    /// `Arc<dyn TransportPublisher>`. Defaulted to `not_supported` so test
    /// doubles need no change; the zenoh-backed impl provides the real
    /// behavior. MUST be resolved via `.await` — never zenoh's sync `.wait()`,
    /// which routes through `tokio::task::block_in_place` and panics on a
    /// `current_thread` runtime (the SDK auto-executor is `new_current_thread`).
    fn matching_status(&self) -> Pin<Box<dyn Future<Output = AortaResult<bool>> + Send + '_>> {
        Box::pin(async { Err(AortaError::not_supported("matching_status")) })
    }
}

/// Zenoh-backed transport publisher adapter.
///
/// Uses `RwLock` so the publish hot path takes a shared read lock (essentially
/// an atomic increment/decrement pair — ~3-5 ns uncontended on ARM64) instead
/// of an exclusive `Mutex::lock()` (~15-25 ns).  Only `undeclare()` takes the
/// exclusive write lock.
#[derive(Debug)]
pub struct ZenohTransportPublisher {
    /// Holds the zenoh publisher. `None` after explicit undeclare. Held in an
    /// `Arc` so `matching_status` can clone it out from under the read guard and
    /// `.await` without holding a (`!Send`) `RwLockReadGuard` across the await.
    inner: RwLock<Option<Arc<zenoh::pubsub::Publisher<'static>>>>,
}

impl ZenohTransportPublisher {
    pub fn new(inner: zenoh::pubsub::Publisher<'static>) -> Self {
        Self {
            inner: RwLock::new(Some(Arc::new(inner))),
        }
    }
}

impl TransportPublisher for ZenohTransportPublisher {
    #[inline]
    fn publish_zbytes(&self, payload: ZBytes, attachment: Option<ZBytes>) -> AortaResult<()> {
        let guard = self
            .inner
            .read()
            .map_err(|_| AortaError::internal("publisher rwlock poisoned"))?;
        let publisher = guard
            .as_ref()
            .ok_or_else(|| AortaError::internal("publisher has been undeclared"))?;

        let builder = publisher.put(payload);
        let result = match attachment {
            Some(bytes) => builder.attachment(bytes).wait(),
            None => builder.wait(),
        };
        result.map_err(AortaError::from)
    }

    #[inline]
    fn publish(&self, payload: AortaPayload, attachment: Option<ZBytes>) -> AortaResult<()> {
        let zpayload = match payload {
            AortaPayload::Owned(owned) => ZBytes::from(owned.into_bytes()),
            AortaPayload::Shm(shm) => {
                return Err(AortaError::not_supported(format!(
                    "SHM payload publish not implemented (handle={}, len={})",
                    shm.handle(),
                    shm.len()
                )))
            }
        };
        self.publish_zbytes(zpayload, attachment)
    }

    fn undeclare(&self) -> AortaResult<()> {
        let publisher = {
            let mut guard = self
                .inner
                .write()
                .map_err(|_| AortaError::internal("publisher rwlock poisoned"))?;
            guard.take()
        };

        if let Some(arc) = publisher {
            // Common case: sole owner. If an in-flight `matching_status()`
            // future still holds a clone, `try_unwrap` fails and the zenoh
            // publisher is undeclared when the last `Arc` drops.
            if let Ok(publisher) = Arc::try_unwrap(arc) {
                publisher.undeclare().wait().map_err(AortaError::from)?;
            }
        }
        Ok(())
    }

    fn matching_status(&self) -> Pin<Box<dyn Future<Output = AortaResult<bool>> + Send + '_>> {
        // Clone the `Arc` out from under the read guard and drop the guard
        // BEFORE awaiting; resolved with `.await` (never `.wait()`).
        let publisher = match self.inner.read() {
            Ok(guard) => guard.as_ref().cloned(),
            Err(_) => {
                return Box::pin(async { Err(AortaError::internal("publisher rwlock poisoned")) })
            }
        };
        Box::pin(async move {
            let publisher =
                publisher.ok_or_else(|| AortaError::internal("publisher has been undeclared"))?;
            let status = publisher
                .matching_status()
                .await
                .map_err(AortaError::from)?;
            Ok(status.matching())
        })
    }
}

/// True when a query reply error is one zenoh synthesizes itself after the
/// query timeout elapses. There are two synthesis sites with different
/// encodings:
///
/// - client session: `ReplyError::new("Timeout", Encoding::ZENOH_STRING)`
///   (`zenoh/src/api/session.rs`) — encoding id 1;
/// - router `QueryCleanup` timer: payload `"Timeout"` with
///   `Encoding::default()` (`zenoh/src/net/routing/dispatcher/queries.rs`) —
///   encoding id 0 (`zenoh/bytes`), observed when the router's copy of the
///   query deadline fires before the client's.
///
/// Both arrive through the reply channel exactly like a remote `reply_err`,
/// so RPC-style callers must classify them as an attempt timeout (retryable,
/// or surfaced as `AortaError::Timeout`) rather than a provider fault
/// (`AortaError::Internal`).
///
/// We require the `"Timeout"` payload under one of those two encodings'
/// *base* form (`zenoh/bytes` or `zenoh/string`), NOT an exact `Encoding`
/// match. Comparing the base keeps a real provider `reply_err` that happens to
/// carry `"Timeout"` bytes under an application/custom encoding from being
/// misread as a synthesized timeout, while ignoring the `schema` suffix.
///
/// Why the base and not `Encoding::id()` or a full `==`:
/// - `Encoding::id()` is `#[zenoh_macros::internal]`, so it is unavailable in
///   the cargo builds (fuzz / miri / exported-workspace / clippy gate) — only
///   the Bazel build enables that feature.
/// - an exact `Encoding ==` additionally pins the `schema` field, which is not
///   stable across the wire round-trip (`None` vs `Some("")`); that schema
///   mismatch let a synthesized `"Timeout"` whose encoding base was right fall
///   through to `Internal` — the (rare, CI-only) flake on
///   `ffi_action_client_send_goal_maps_zenoh_timeout_to_timeout`, where a held
///   send_goal surfaced as a provider fault.
///
/// The cargo-public surface that exposes the base is `Display`: zenoh documents
/// the encoding string as MIME-like `type/subtype[;schema]`. We require the
/// base to be `zenoh/bytes` or `zenoh/string` AND the schema to be empty. The
/// wire instability is ONLY `None` vs `Some("")` (both render to an empty
/// schema, i.e. `zenoh/bytes` or `zenoh/bytes;`), so tolerating an empty schema
/// covers it; a NON-empty schema (e.g. a real provider's `reply_err("Timeout")`
/// under `zenoh/string;my-error`) is not a synthesized timeout and stays a
/// provider fault. The contract test
/// `zenoh_synthesized_query_timeout_reply_is_classified_as_timeout` pins these
/// forms so a fork change can't silently regress this.
pub fn reply_error_is_zenoh_query_timeout(err: &zenoh::query::ReplyError) -> bool {
    if err.payload().to_bytes().as_ref() != b"Timeout".as_slice() {
        return false;
    }
    let encoding = err.encoding().to_string();
    let (base, schema) = match encoding.split_once(';') {
        Some(pair) => pair,
        None => (encoding.as_str(), ""),
    };
    schema.is_empty() && (base == "zenoh/bytes" || base == "zenoh/string")
}

/// Whether a reply error is the "no current snapshot" marker that
/// [`crate::snapshot_queryable`] emits when its payload slot is absent or
/// empty. Snapshot-style queries (action info/status, telemetry) MUST read it
/// as absence, not as a provider fault: the queryable stays declared across the
/// whole window in which a server is starting up or tearing down, and answers
/// every query in that window with this marker.
///
/// The action CLIENT already gets this right for the status key via
/// `ReplyErrPolicy::Absent`; this helper is what lets the info path — shared by
/// `ActionClient::info`, the C-ABI client, and `ensure_single_action_provider`
/// on the server-startup path — agree with it.
pub fn reply_error_is_absent_snapshot(err: &zenoh::query::ReplyError) -> bool {
    err.payload().to_bytes().as_ref()
        == crate::snapshot_queryable::ABSENT_SNAPSHOT_MARKER.as_bytes()
}

/// Human-readable text of a reply error's payload. Aorta's own `reply_error`
/// (and the native-interop profile) carries a
/// UTF-8 message string in the payload — surface it as text instead of the
/// `ZBytes` debug hex dump. Lossy decode keeps non-UTF-8 foreign payloads
/// printable instead of failing. Consumed by the RPC client, action client/
/// server, ContextStore and the C-ABI action client; `pub` (re-exported at the
/// crate root) so the CLI tools' raw-query paths format reply errors the same
/// way instead of debug-dumping bytes.
pub fn reply_error_text(err: &zenoh::query::ReplyError) -> String {
    String::from_utf8_lossy(err.payload().to_bytes().as_ref()).into_owned()
}

/// Whether a received reply is a synthesized Zenoh query-timeout — i.e. its
/// result is an `Err` that [`reply_error_is_zenoh_query_timeout`] classifies as
/// the `"Timeout"` sentinel the client session and/or the router `QueryCleanup`
/// timer push into the reply channel when a `get()` times out.
///
/// Such a reply is a dual-guard timeout firing, never a server answer: callers
/// map it to the recv guard's "no reply" outcome and must NOT count it toward a
/// multi-reply `ConcurrentConflict` guard. Two `"Timeout"` replies for one query
/// mean both dual-guard sources fired, not that two servers answered — Aorta
/// queryables never reply a bare `"Timeout"`. Shared by the Rust action client
/// (`action_client.rs`), the C-ABI action client (`ffi/c_api/mod.rs`) and the
/// RPC client (`client.rs`) so the classification lives in one place.
pub(crate) fn reply_is_zenoh_query_timeout(reply: &zenoh::query::Reply) -> bool {
    reply
        .result()
        .err()
        .is_some_and(reply_error_is_zenoh_query_timeout)
}
