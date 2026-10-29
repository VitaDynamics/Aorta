//!
//! ## Flow C-ABI struct evolution policy
//!
//! Structs are size/offset-asserted (`abi_assert.rs` + the C++
//! `abi_layout_test`). INPUT structs (caller→library) are FROZEN at first
//! release and evolve via new `_v2` symbols plus an `AORTA_ABI_VERSION`
//! bump; they must not ship speculative or reserved fields — a field that
//! exists must work. OUTPUT structs (library→callback, always allocated by
//! the library) may grow by APPENDING fields together with an ABI bump:
//! old callbacks keep reading a valid prefix; new callers are protected by
//! the version gate. Callback struct pointers are valid only for the
//! duration of the invocation.
#![allow(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString};
use std::io::Write;
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Mutex as TokioMutex, Notify, RwLock as TokioRwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;
use uuid::Uuid;
use zenoh::pubsub::Subscriber as ZenohRawSubscriber;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::ffi::guard::{ffi_guard, ffi_guard_value, ffi_guard_void};

use crate::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionInfo, ActionPublicStatus,
    ActionReplyCode, ActionSendGoalResponse, ActionStatus,
};
use crate::action_server::{
    ensure_single_action_provider, fetch_action_info, spawn_action_info_queryable,
    DEFAULT_ACTION_CONTROL_TIMEOUT_MS, DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
};
use crate::client::{Client, ClientStats as CoreClientStats};
#[cfg(test)]
use crate::error::TransportError;
use crate::error::{AortaError, AortaResult};
use crate::event_queue::{CallbackGroupHandle, CallbackGroupType};
use crate::executor::{
    ExecutorAdmissionOptions, ExecutorClassPolicyOptions, ExecutorOptions, MultiThreadedExecutor,
    PrioritySchedulerOptions, SchedulerMode, SingleThreadedExecutor, SpinOutcome, StopHandle,
};
use crate::key_expr::normalize_path;
use crate::logging::{self, LogConfig, LogFormat, LogLevel};
use crate::node::Node;
use crate::publisher::{Publisher, PublisherOptions};
use crate::qos::{CongestionControl, Priority, QoS, Reliability};
use crate::runtime_env;
use crate::schema::{infer_aorta_header_layout_from_bfbs, SchemaAttachPolicy, SchemaHash};
use crate::service::{
    RunningService, Service, ServiceRequest, ServiceSchemaInfo, ServiceStats as CoreServiceStats,
};
use crate::snapshot_queryable::spawn_single_key_snapshot_queryable;
use crate::subscriber::{
    DeliveryMode, MessageContext, ReceiveOverflow, Subscriber, SubscriberOptions,
    SubscriberOverflowPolicy,
};
use crate::telemetry::schema::{
    NodeOptions as CoreNodeOptions, TelemetryNodeMetadata as CoreTelemetryNodeMetadata,
};
use crate::time::unix_time_ns;
use crate::timer::Timer;

const DEFAULT_EXECUTOR_QUEUE: usize = 1024;
const DEFAULT_MULTI_EXECUTOR_WORKERS: usize = 4;
const DEFAULT_ASYNC_EXECUTOR_IN_FLIGHT: usize = 32;
const DEFAULT_ACTION_MULTI_REPLY_CHECK_WINDOW_MS: u64 = 25;
const ENV_ASYNC_EXECUTOR_IN_FLIGHT: &str = "AORTA_ASYNC_EXECUTOR_MAX_IN_FLIGHT";

pub type aorta_result_t = i32;
pub type aorta_node_t = u64;
pub type aorta_pub_t = u64;
pub type aorta_sub_t = u64;
pub type aorta_exec_t = u64;
pub type aorta_sample_t = u64;
pub type aorta_timer_t = u64;
pub type aorta_service_t = u64;
pub type aorta_client_t = u64;
pub type aorta_action_client_t = u64;
pub type aorta_action_server_t = u64;
pub type aorta_action_context_t = u64;
pub type aorta_action_feedback_sub_t = u64;
pub type aorta_service_request_t = u64;
pub type aorta_callback_group_handle_t = u64;

/// Opaque stop-handle identifier. Obtained from an executor via
/// `aorta_executor_get_stop_handle`. A value of `0` is reserved as an invalid
/// handle (returned uninitialised; never allocated).
///
/// # Lifetime and double-free safety
///
/// Stop handles are u64 keys into a registry (see `Registry::stop_handles`).
/// Calling `aorta_stop_handle_free` twice on the same handle is safe and
/// returns `AORTA_ERR_NOT_FOUND` on the second call instead of triggering
/// use-after-free (which the previous `Box<StopHandle>` design exposed).
///
/// # Concurrency
///
/// `aorta_stop_handle_stop` performs a brief registry lookup (Mutex) to
/// resolve the handle to an `Arc<StopHandle>`. It is **no longer
/// async-signal-safe** (prior versions documented it as such). Callers in
/// signal handlers must instead post to a self-pipe / signalfd and call
/// `aorta_stop_handle_stop` from a normal thread.
pub type aorta_stop_handle_t = u64;

/// Sentinel for "no stop handle". Matches `Default::default()` on `u64`.
pub const AORTA_STOP_HANDLE_INVALID: aorta_stop_handle_t = 0;

pub const AORTA_OK: aorta_result_t = 0;
pub const AORTA_ERR_INVALID_ARGUMENT: aorta_result_t = -1;
pub const AORTA_ERR_NOT_FOUND: aorta_result_t = -2;
pub const AORTA_ERR_TIMEOUT: aorta_result_t = -3;
pub const AORTA_ERR_BUSY: aorta_result_t = -4;
pub const AORTA_ERR_INTERNAL: aorta_result_t = -5;
pub const AORTA_ERR_SCHEMA_CONFLICT: aorta_result_t = -7;
pub const AORTA_ERR_CONCURRENT_CONFLICT: aorta_result_t = -8;
pub const AORTA_ERR_ALREADY_REPLIED: aorta_result_t = -10;
pub const AORTA_ERR_PREFIX_NOT_DECLARED: aorta_result_t = -11;
pub const AORTA_ERR_DECLARATION_NOT_READY: aorta_result_t = -12;
pub const AORTA_ERR_KIND_MISMATCH: aorta_result_t = -13;
pub const AORTA_ERR_INSTANCE_ID_COLLISION: aorta_result_t = -14;
/// Returned when an FFI entry point is invoked from inside an Aorta
/// callback (subscriber `on_msg`, service handler, timer tick,
/// context-store watch change — action `execute` and `cancel_requested`
/// are NOT in this list: raw execute handlers run on a dedicated
/// per-goal thread and cancel-requested callbacks on the Flow offload
/// pool, where blocking FFI calls are legal; if a thread cannot be
/// created the job falls back to the offload pool, whose queue
/// inline-drains on the submitting thread on worker creation failure,
/// where the runtime backstop covers it). Aorta FFI is not reentrant from within callbacks — see
/// the REENTRANCY CONTRACT block in `cpp/include/aorta/aorta.h` for
/// the rules and recommended patterns.
pub const AORTA_ERR_REENTRANT: aorta_result_t = -15;
/// The operation was cancelled through a Flow cancel token.
pub const AORTA_ERR_CANCELLED: aorta_result_t = -16;
/// The caller's buffer is smaller than the value; `*out_len` reports the
/// required size (for example `aorta_watch_load`).
pub const AORTA_ERR_BUFFER_TOO_SMALL: aorta_result_t = -17;
pub const AORTA_OPTIONAL_BOOL_INHERIT: u8 = 0;
pub const AORTA_OPTIONAL_BOOL_FALSE: u8 = 1;
pub const AORTA_OPTIONAL_BOOL_TRUE: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum aorta_log_format_t {
    #[default]
    AORTA_LOG_FORMAT_TEXT = 0,
    AORTA_LOG_FORMAT_JSON = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum aorta_log_level_t {
    AORTA_LOG_LEVEL_TRACE = 0,
    AORTA_LOG_LEVEL_DEBUG = 1,
    AORTA_LOG_LEVEL_INFO = 2,
    #[default]
    AORTA_LOG_LEVEL_WARN = 3,
    AORTA_LOG_LEVEL_ERROR = 4,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_log_config_t {
    pub filter: *const c_char,
    pub file_path: *const c_char,
    pub format: aorta_log_format_t,
    pub stderr_enabled: u8,
}

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
    /// Single-slot thread-local cache for the most recently used publisher.
    /// Avoids locking the global publisher registry on every publish call.
    /// Stores a Weak reference so that aorta_publisher_free invalidation is
    /// automatic — the Weak simply fails to upgrade after the Arc is removed
    /// from the registry.
    static PUB_CACHE: std::cell::RefCell<Option<(aorta_pub_t, Weak<Publisher>)>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_last_error(message: impl Into<String>) {
    let mut msg = message.into();
    // Interior NUL bytes are valid UTF-8 and can arrive verbatim from a
    // foreign peer's reply_err payload. CString
    // rejects them, and discarding the whole message for one byte destroys
    // the diagnostic — escape them visibly instead so C/C++/Python callers
    // keep the rest of the text.
    if msg.as_bytes().contains(&0) {
        msg = msg.replace('\0', "\\x00");
    }
    let cstr = CString::new(msg).unwrap_or_else(|_| CString::new("invalid error message").unwrap());
    // TLS destruction precedes C/C++ static destruction on the main thread.
    // A late FFI call may therefore have no last-error slot; diagnostics are
    // best-effort during teardown and must not turn recovery into an abort.
    let _ = LAST_ERROR.try_with(|cell| {
        *cell.borrow_mut() = Some(cstr);
    });
}

fn aorta_err<T: Into<String>>(code: aorta_result_t, message: T) -> aorta_result_t {
    set_last_error(message.into());
    code
}

/// Returns `AORTA_ERR_REENTRANT` after writing a descriptive message
/// into the thread-local `LAST_ERROR` slot. Without this, callers
/// reading `aorta_last_error()` after a reentrant rejection would see
/// whatever string the previous failure on this thread happened to
/// leave behind, which is wildly misleading. See REENTRANCY CONTRACT
/// in `cpp/include/aorta/aorta.h` for the user-facing rules.
fn aorta_reentrant(op: &str) -> aorta_result_t {
    // Scheduler diagnosis: a blocking middleware op was attempted from inside a
    // running callback — the classic self-deadlock. Surface it on the timeline
    // (the single funnel for every REENTRANT rejection), tagged by the FFI
    // SUBSYSTEM it came from: each call site passes `module_path!()`, whose last
    // `::` segment (`client`, `contextstore`, `action_server`, …) names the
    // subsystem. This is a rare error path, so the per-call format!/intern is
    // irrelevant; the suffix can't ride `LAST_ERROR` (overwritten just below).
    let subsystem = op.rsplit("::").next().unwrap_or(op);
    crate::trace::sched_instant(crate::trace::intern_event(&format!(
        "sched.reentrant_rejected:{subsystem}"
    )));
    aorta_err(
        AORTA_ERR_REENTRANT,
        "FFI entry cannot be called from inside an Aorta callback that is currently running \
         on a Tokio worker thread. See REENTRANCY CONTRACT in <aorta.h> for the safe entry \
         points and the recommended deferral patterns.",
    )
}

/// The `aorta_result_t` code for an error WITHOUT consuming it or touching the
/// thread-local last-error (unlike [`map_aorta_error`]). Used where the code is
/// needed as data — e.g. carried in a watcher outcome alongside the message.
pub(crate) fn aorta_result_code_of(err: &AortaError) -> aorta_result_t {
    match err {
        AortaError::InvalidArgument(_) => AORTA_ERR_INVALID_ARGUMENT,
        AortaError::NotFound(_) => AORTA_ERR_NOT_FOUND,
        AortaError::Busy(_) | AortaError::ShmUnavailable(_) | AortaError::QuotaExceeded(_) => {
            AORTA_ERR_BUSY
        }
        AortaError::CapacityExceeded(_) | AortaError::NotSupported(_) => AORTA_ERR_INVALID_ARGUMENT,
        AortaError::SchemaConflict(_) => AORTA_ERR_SCHEMA_CONFLICT,
        AortaError::ConcurrentConflict(_) => AORTA_ERR_CONCURRENT_CONFLICT,
        AortaError::Timeout(_) => AORTA_ERR_TIMEOUT,
        AortaError::Cancelled(_) => AORTA_ERR_CANCELLED,
        AortaError::AlreadyReplied(_) => AORTA_ERR_ALREADY_REPLIED,
        AortaError::PrefixNotDeclared(_) => AORTA_ERR_PREFIX_NOT_DECLARED,
        AortaError::DeclarationNotReady(_) => AORTA_ERR_DECLARATION_NOT_READY,
        AortaError::KindMismatch(_) => AORTA_ERR_KIND_MISMATCH,
        AortaError::InstanceIdCollision(_) => AORTA_ERR_INSTANCE_ID_COLLISION,
        AortaError::Transport(_) | AortaError::Internal(_) => AORTA_ERR_INTERNAL,
    }
}

fn map_aorta_error(err: AortaError) -> aorta_result_t {
    match err {
        AortaError::InvalidArgument(msg) => aorta_err(AORTA_ERR_INVALID_ARGUMENT, msg),
        AortaError::NotFound(msg) => aorta_err(AORTA_ERR_NOT_FOUND, msg),
        AortaError::Busy(msg) => aorta_err(AORTA_ERR_BUSY, msg),
        AortaError::ShmUnavailable(msg) => aorta_err(AORTA_ERR_BUSY, msg),
        AortaError::QuotaExceeded(msg) => aorta_err(AORTA_ERR_BUSY, msg),
        AortaError::CapacityExceeded(msg) => aorta_err(AORTA_ERR_INVALID_ARGUMENT, msg),
        AortaError::NotSupported(msg) => aorta_err(AORTA_ERR_INVALID_ARGUMENT, msg),
        AortaError::SchemaConflict(msg) => aorta_err(AORTA_ERR_SCHEMA_CONFLICT, msg),
        AortaError::ConcurrentConflict(msg) => aorta_err(AORTA_ERR_CONCURRENT_CONFLICT, msg),
        AortaError::Timeout(msg) => aorta_err(AORTA_ERR_TIMEOUT, msg),
        AortaError::Cancelled(msg) => aorta_err(AORTA_ERR_CANCELLED, msg),
        AortaError::AlreadyReplied(msg) => aorta_err(AORTA_ERR_ALREADY_REPLIED, msg),
        AortaError::PrefixNotDeclared(msg) => aorta_err(AORTA_ERR_PREFIX_NOT_DECLARED, msg),
        AortaError::DeclarationNotReady(msg) => aorta_err(AORTA_ERR_DECLARATION_NOT_READY, msg),
        AortaError::KindMismatch(msg) => aorta_err(AORTA_ERR_KIND_MISMATCH, msg),
        AortaError::InstanceIdCollision(msg) => aorta_err(AORTA_ERR_INSTANCE_ID_COLLISION, msg),
        AortaError::Transport(err) => {
            aorta_err(AORTA_ERR_INTERNAL, format!("transport error: {}", err))
        }
        AortaError::Internal(msg) => aorta_err(AORTA_ERR_INTERNAL, msg),
    }
}

use crate::ffi::runtime_helper::{in_async_context, runtime};

#[derive(Default)]
struct Registry {
    next_id: AtomicU64,
    arbiters: Mutex<HashMap<u64, Arc<arbiter::ArbiterGroup>>>,
    nodes: Mutex<HashMap<u64, Node>>,
    publishers: Mutex<HashMap<u64, Arc<Publisher>>>,
    subscribers: Mutex<HashMap<u64, FfiSubscriberEntry>>,
    executors: Mutex<HashMap<u64, Arc<ExecutorEntry>>>,
    timers: Mutex<HashMap<u64, FfiTimerEntry>>,
    samples: Mutex<HashMap<u64, SampleEntry>>,
    services: Mutex<HashMap<u64, Arc<Mutex<Option<RunningService>>>>>,
    service_requests: Mutex<HashMap<u64, Arc<Mutex<Option<ServiceRequest>>>>>,
    clients: Mutex<HashMap<u64, Arc<Client>>>,
    action_clients: Mutex<HashMap<u64, Arc<FfiActionClient>>>,
    action_servers: Mutex<HashMap<u64, FfiActionServerHandle>>,
    action_contexts: Mutex<HashMap<u64, Arc<FfiActionContext>>>,
    action_feedback_subscribers: Mutex<HashMap<u64, FfiActionFeedbackSubEntry>>,
    action_feedback2_subscribers: Mutex<HashMap<u64, FfiActionFeedback2SubEntry>>,
    action_result_subscribers: Mutex<HashMap<u64, FfiActionResultSubEntry>>,
    context_stores: Mutex<HashMap<u64, crate::contextstore::ContextStore>>,
    context_watches: Mutex<HashMap<u64, crate::contextstore::WatchHandle>>,
    ctx_owner_liveliness_watches: Mutex<HashMap<u64, crate::contextstore::OwnerLivelinessHandle>>,
    /// Stop handles handed out by `aorta_executor_get_stop_handle`. Keyed by u64
    /// to allow safe double-free detection (returns `AORTA_ERR_NOT_FOUND` on
    /// the second free instead of UAF). See `aorta_stop_handle_t` doc.
    stop_handles: Mutex<HashMap<u64, Arc<StopHandle>>>,
    /// Raw capture subscribers (capture C-ABI).
    capture_subscribers: Mutex<HashMap<u64, capture::FfiCaptureEntry>>,
    /// Schema resolvers (SchemaResolver C-ABI).
    schema_resolvers: Mutex<HashMap<u64, Arc<crate::schema_resolve::SchemaResolver>>>,
    /// Flow cancel tokens.
    cancel_tokens: Mutex<HashMap<u64, crate::flow::CancelToken>>,
    /// Flow scopes.
    flow_scopes: Mutex<HashMap<u64, crate::flow::Scope>>,
    /// External (C-side) Flow tasks: scope accounting handles released on
    /// `aorta_task_finish`.
    flow_tasks: Mutex<HashMap<u64, crate::flow::scope::ExternalChild>>,
    /// Depth-1 latest-value watches.
    flow_watches: Mutex<HashMap<u64, Arc<crate::watch::Watch>>>,
    /// Approximate-time alignment windows.
    flow_windows: Mutex<HashMap<u64, Arc<crate::flow::WindowSync>>>,
    /// Readiness AND-join sets.
    flow_readiness: Mutex<HashMap<u64, Arc<crate::flow::ReadinessSet>>>,
    /// Popped aligned sets: (ts_ns, contiguous payload copy) per input —
    /// views handed to C stay valid until `aorta_aligned_set_free`.
    flow_aligned_sets: Mutex<HashMap<u64, AlignedSetEntry>>,
}

/// One popped aligned set at the C boundary: per-input
/// `(ts_ns, contiguous payload copy)`.
type AlignedSetEntry = Vec<(u64, Vec<u8>)>;

struct FfiSubscriberEntry {
    // A pull-mode `aorta_sub_recv` clones the subscriber's lightweight
    // `PullRecvHandle` out from under the registry lock (NOT the whole
    // subscriber) before blocking on `recv()`, so `free` can reclaim and
    // undeclare this `Subscriber` even while a thread is parked in `recv` —
    // dropping the Zenoh subscriber unblocks the recv.
    subscriber: Subscriber,
    // In-flight count of C++ dispatcher (`cb`) invocations: incremented in the
    // subscriber dispatch closure BEFORE calling `cb` and decremented after
    // (via `InFlightGuard`). `aorta_subscriber_free` drains this to zero before
    // returning, so the C++ destructor cannot free its CallbackState while a
    // dispatcher has entered the C++ dispatcher but not yet taken the C++
    // `in_flight_mutex` (the teardown residual window).
    in_flight: Arc<AtomicUsize>,
}

struct FfiTimerEntry {
    timer: Timer,
    // Cleared before timer destruction so queued callbacks can safely no-op
    // instead of dereferencing freed SDK callback state.
    callback_user_data: Arc<AtomicUsize>,
    // Same role as `FfiSubscriberEntry::in_flight`: brackets the C++ timer
    // dispatcher call so `aorta_timer_free` can drain in-flight ticks before
    // the C++ side frees its CallbackState.
    in_flight: Arc<AtomicUsize>,
}

/// RAII guard that decrements an in-flight counter on drop. Brackets the C++
/// dispatcher call in the subscriber/timer FFI dispatch closures so the
/// corresponding `aorta_*_free` can wait for in-flight callbacks to finish
/// before the C++ side frees its callback state. Borrows the counter (no Arc
/// clone) since the owning Arc outlives each dispatch.
pub(crate) struct InFlightGuard<'a>(pub(crate) &'a AtomicUsize);

impl<'a> InFlightGuard<'a> {
    /// Increment-and-guard in one step (the arbiter's producer-quiesce
    /// counter uses this shape — one RAII counting idiom).
    pub(crate) fn enter(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        InFlightGuard(counter)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Default bound for `drain_in_flight`, matched to the C++ destructor's
/// `kSubscriberDestructorWaitTimeout` / `kTimerDestructorWaitTimeout` (2s) so
/// the two layers' fallbacks line up.
/// Thread name of a raw action goal's dedicated execute thread.
const ACTION_EXECUTE_THREAD_NAME: &str = "aorta-action";
const FREE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Bounded busy-wait until `in_flight` reaches zero. Returns `true` if it
/// drained, `false` on timeout. Used by `aorta_subscriber_free` /
/// `aorta_timer_free` after they have stopped NEW dispatches, to let already
/// entered C++ dispatchers finish before the C++ destructor frees its state.
/// Bounded so a hung user callback cannot wedge teardown — on timeout the
/// caller returns and the C++ side's own wait+leak-on-timeout path takes over.
fn drain_in_flight(in_flight: &AtomicUsize, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while in_flight.load(Ordering::Acquire) != 0 {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

struct ExecutorEntry {
    exec: Mutex<ExecutorInstance>,
    /// Threading kind, readable WITHOUT the exec mutex (spin holds
    /// that for its whole run — attach must not touch it).
    is_multi: bool,
    stop: StopHandle,
    /// Cached ingress sender, cloned at construction. LOAD-BEARING for the
    /// Flow C-ABI: `aorta_executor_spin` holds `exec` for the WHOLE spin, so
    /// `aorta_executor_post_resume` / `aorta_scope_spawn` (legal from inside
    /// dispatched callbacks) must reach the reserved lane without
    /// touching that mutex — via this clone.
    sender: crate::event_queue::EventQueueSender,
}

enum ExecutorInstance {
    Single(SingleThreadedExecutor),
    Multi(MultiThreadedExecutor),
}

impl ExecutorInstance {
    fn queue_sender(&self) -> crate::event_queue::EventQueueSender {
        match self {
            ExecutorInstance::Single(e) => e.queue().sender(),
            ExecutorInstance::Multi(e) => e.queue().sender(),
        }
    }

    fn stop_handle(&self) -> StopHandle {
        match self {
            Self::Single(exec) => exec.stop_handle(),
            Self::Multi(exec) => exec.stop_handle(),
        }
    }

    /// Fallible attach — the C-ABI must SURFACE a refusal, not swallow it:
    /// a C++/Python caller that believes `add_node` succeeded
    /// would spin an executor that never receives an event.
    fn try_add_node(&mut self, node: Node) -> crate::error::AortaResult<()> {
        match self {
            Self::Single(exec) => exec.try_add_node(node),
            Self::Multi(exec) => exec.try_add_node(node),
        }
    }

    async fn spin(&mut self) {
        match self {
            Self::Single(exec) => exec.spin().await,
            Self::Multi(exec) => exec.spin().await,
        }
    }

    async fn spin_once(&mut self, timeout: Duration) -> SpinOutcome {
        match self {
            Self::Single(exec) => exec.spin_once(timeout).await,
            Self::Multi(exec) => exec.spin_once(timeout).await,
        }
    }

    fn create_callback_group(&self, group_type: CallbackGroupType) -> CallbackGroupHandle {
        match self {
            Self::Single(exec) => exec.create_callback_group(group_type),
            Self::Multi(exec) => exec.create_callback_group(group_type),
        }
    }

    fn remove_callback_group(&self, handle: CallbackGroupHandle) -> bool {
        match self {
            Self::Single(exec) => exec.remove_callback_group(handle),
            Self::Multi(exec) => exec.remove_callback_group(handle),
        }
    }

    fn admission_stats(&self) -> crate::event_queue::AdmissionStats {
        match self {
            Self::Single(exec) => exec.admission_stats(),
            Self::Multi(exec) => exec.admission_stats(),
        }
    }
}

struct SampleEntry {
    payload: Vec<u8>,
    topic: String,
    receive_stamp_ns: i64,
    priority: i32,
    publisher_node: Option<String>,
    sequence: u64,
}

/// Reply-error mapping policy for the FFI action client's shared query helper.
/// Mirrors `ReplyErrPolicy` in `action_client.rs` so the C ABI surfaces snapshot-style
/// absent state as `Ok(None)` instead of `AortaError::Internal`.
#[derive(Copy, Clone)]
enum FfiReplyErrPolicy {
    Internal,
    Absent,
}

struct FfiActionClient {
    session: crate::Session,
    key_builder: crate::KeyExprBuilder,
    action_name: String,
    control_timeout: Duration,
    info_timeout: Duration,
    /// The owning node's executor event-sender cell, captured at creation so
    /// result-push deliveries can ride the executor's
    /// reserved `CoroutineResume` lane when the node is attached to a spinning
    /// executor. The `Arc` keeps the cell alive independent of the node
    /// handle's lifetime; an unattached node reads as `None`.
    event_sender_cell:
        std::sync::Arc<std::sync::RwLock<Option<crate::event_queue::EventQueueSender>>>,
}

enum FfiActionServerHandle {
    InfoOnly(FfiActionInfoServer),
    Raw(Arc<FfiRawActionServer>),
}

struct FfiActionInfoServer {
    _node: Node,
    stop_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
struct FfiActionServerCallbacks {
    codec: aorta_action_server_codec_t,
    execute: aorta_action_execute_cb,
    cancel_requested: aorta_action_cancel_requested_cb,
    user_data: usize,
}

/// Admission gate for user callbacks of one raw action server.
///
/// Every dispatch of `execute` / `cancel_requested` holds a permit for the
/// whole user call. `close` (server free, or a failed constructor) refuses
/// new permits; `user_data_release` runs exactly once, when the gate is
/// closed and no permit is outstanding — so it can never race a callback
/// that is reading `user_data`. Neither `close` nor a permit drop blocks.
struct FfiCallbackGate {
    state: Mutex<FfiCallbackGateState>,
}

struct FfiCallbackGateState {
    closed: bool,
    in_flight: usize,
    /// `(release, user_data)`, taken by whoever observes closed + idle.
    release: Option<(unsafe extern "C" fn(*mut c_void), usize)>,
}

struct FfiCallbackPermit {
    gate: Arc<FfiCallbackGate>,
}

impl FfiCallbackGate {
    fn new(release: aorta_user_data_release_cb, user_data: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FfiCallbackGateState {
                closed: false,
                in_flight: 0,
                release: release.map(|release| (release, user_data)),
            }),
        })
    }

    /// `None` once the gate is closed: the caller must not invoke the
    /// user callback.
    fn enter(self: &Arc<Self>) -> Option<FfiCallbackPermit> {
        let mut state = self.state.lock();
        if state.closed {
            return None;
        }
        state.in_flight += 1;
        Some(FfiCallbackPermit {
            gate: Arc::clone(self),
        })
    }

    /// Idempotent. Releases `user_data` now if no callback is in flight,
    /// else when the last permit drops.
    fn close(&self) {
        let release = {
            let mut state = self.state.lock();
            state.closed = true;
            if state.in_flight == 0 {
                state.release.take()
            } else {
                None
            }
        };
        Self::run_release(release);
    }

    fn run_release(release: Option<(unsafe extern "C" fn(*mut c_void), usize)>) {
        if let Some((release, user_data)) = release {
            // SAFETY: `release` and `user_data` came from the caller of
            // `aorta_action_server_new_with_callbacks`, whose contract
            // hands both over for exactly one call; `take()` under the gate
            // lock guarantees this is that one call.
            ffi_guard_void(|| unsafe { release(user_data as *mut c_void) });
        }
    }
}

/// Closes the gate when dropped unless disarmed — keeps a constructor's
/// early returns and unwinds from leaving the gate open (and `user_data`
/// unreleased) while startup tasks still hold the server.
struct FfiCallbackGateCloseGuard {
    gate: Option<Arc<FfiCallbackGate>>,
}

impl FfiCallbackGateCloseGuard {
    fn new(gate: Arc<FfiCallbackGate>) -> Self {
        Self { gate: Some(gate) }
    }

    fn disarm(mut self) {
        self.gate = None;
    }
}

impl Drop for FfiCallbackGateCloseGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.close();
        }
    }
}

impl Drop for FfiCallbackGate {
    /// Backstop for paths that never reach `close` (a panic during
    /// construction). Permits hold the gate, so none is outstanding here.
    fn drop(&mut self) {
        Self::run_release(self.state.get_mut().release.take());
    }
}

impl Drop for FfiCallbackPermit {
    fn drop(&mut self) {
        let release = {
            let mut state = self.gate.state.lock();
            state.in_flight -= 1;
            if state.closed && state.in_flight == 0 {
                state.release.take()
            } else {
                None
            }
        };
        FfiCallbackGate::run_release(release);
    }
}

struct FfiRawActionServer {
    node: Node,
    action_name: String,
    info_key: String,
    send_goal_key: String,
    cancel_goal_key: String,
    get_result_key: String,
    feedback_key: String,
    info: ActionInfo,
    options: crate::action_server::ActionServerOptions,
    callbacks: FfiActionServerCallbacks,
    /// Gates every user-callback dispatch; see [`FfiCallbackGate`].
    callback_gate: Arc<FfiCallbackGate>,
    /// One permit per running execute handler, `max_concurrency` in all.
    /// Admission caps non-terminal goals; this caps handler threads,
    /// which can outlive their goal's terminal transition. A goal waiting
    /// for a slot stays ACCEPTED. Closed at free.
    execute_slots: Arc<tokio::sync::Semaphore>,
    info_payload: Arc<TokioRwLock<Option<crate::ZBytes>>>,
    info_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    info_task: TokioMutex<Option<JoinHandle<()>>>,
    send_goal_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    send_goal_task: TokioMutex<Option<JoinHandle<()>>>,
    cancel_goal_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    cancel_goal_task: TokioMutex<Option<JoinHandle<()>>>,
    get_result_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    get_result_task: TokioMutex<Option<JoinHandle<()>>>,
    goals: TokioMutex<HashMap<String, Arc<FfiRawGoalRecord>>>,
    expired_goals: TokioMutex<HashMap<String, FfiRawExpiredGoalRecord>>,
    context_handles: TokioMutex<Vec<u64>>,
    admission_lock: TokioMutex<()>,
    active_goal_count: AtomicUsize,
    /// Interned per-action waterfall stage/marker events (span hierarchy),
    /// minted once at server construction — the FFI mirror of the core
    /// `ActionServer` set, so both SDK paths emit converter-identical events.
    ev_accepted_to_execution_started: u16,
    ev_accepted_to_finished: u16,
    ev_user_callback: u16,
    ev_feedback_pub: u16,
    ev_cancel_to_canceled: u16,
    ev_goal_admission: u16,
    ev_get_result_query: u16,
}

struct FfiRawGoalRecord {
    goal_id: String,
    goal_secret: String,
    request_payload: Vec<u8>,
    request_payload_hash: String,
    status_key: String,
    status_payload: Arc<TokioRwLock<Option<crate::ZBytes>>>,
    status_stop_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    status_task: TokioMutex<Option<JoinHandle<()>>>,
    state: TokioMutex<FfiRawGoalState>,
    cancel_requested: AtomicBool,
    cancel_notify: Notify,
    /// First accepted cancel's monotonic stamp (0 = none) — anchors the
    /// `action.cancel_requested_to_canceled` stage.
    cancel_requested_mono_ns: AtomicU64,
    /// Traceparent propagated on the send_goal query attachment, when
    /// the caller injected one and tracing was enabled at receipt.
    inbound_trace: Option<crate::trace::TraceParent>,
    /// Per-goal SERVER ROOT (span hierarchy): `accepted_to_finished` is emitted
    /// under exactly this id (parented to the caller's goal_sent span);
    /// lifecycle stages nest beneath it. Immutable for the goal's lifetime.
    goal_root: Option<crate::trace::SpanLink>,
}

impl FfiRawGoalRecord {
    /// The trace context identifying THIS goal for deferred work: the goal
    /// root (preferred) or the caller's propagated span. Mirrors
    /// `ServiceRequest::trace_context`.
    fn trace_context(&self) -> Option<crate::trace::TraceParent> {
        if let Some(root) = self.goal_root {
            return Some(crate::trace::TraceParent {
                trace_id: root.trace_id,
                span_id: root.span_id,
                flags: self
                    .inbound_trace
                    .map_or(crate::trace::TRACEPARENT_SAMPLED, |t| t.flags),
            });
        }
        self.inbound_trace
    }
}

struct FfiRawGoalState {
    status: ActionStatus,
    code: ActionReplyCode,
    message: String,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    /// Tracing: monotonic accept time, captured only when tracing was enabled
    /// at accept (no clock read otherwise). Anchors the lifecycle stages.
    accepted_mono_ns: Option<u64>,
    /// Tracing: previous feedback publish's monotonic stamp (0 = none) for
    /// the `action.feedback_interval` aggregate.
    last_feedback_mono_ns: u64,
    feedback_snapshot: Vec<u8>,
    result_payload: Option<Vec<u8>>,
}

#[derive(Clone)]
struct FfiRawExpiredGoalRecord {
    payload_hash: String,
    status: ActionStatus,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
}

struct FfiActionContext {
    server: Weak<FfiRawActionServer>,
    goal: Arc<FfiRawGoalRecord>,
}

struct FfiActionFeedbackSubEntry {
    subscriber: ZenohRawSubscriber<()>,
    // Same role as `FfiSubscriberEntry::in_flight`: undeclare stops NEW
    // dispatches but an already-entered callback may still be running `cb` —
    // `aorta_action_feedback_sub_free` drains this to zero before returning so
    // the caller can free its callback state without a use-after-free.
    in_flight: Arc<AtomicUsize>,
}

/// How a feedback2 subscription delivers the C callback. Decided ONCE at
/// subscribe time (from whether the client's node had an executor attached),
/// so the callback is never invoked from two threads concurrently.
enum Feedback2Delivery {
    /// Executor-less fallback: a dedicated drain thread invokes the callback
    /// off the RX task; `free` joins it (or detaches on a reentrant free).
    Thread(std::thread::JoinHandle<()>),
    /// Drains dispatch as `CoroutineResume` events on the node
    /// executor's reserved lane (coalesced by a `WakeSlot`); `free` sets
    /// `cancelled` and drains `in_flight` instead of joining a thread.
    Lane {
        cancelled: Arc<std::sync::atomic::AtomicBool>,
        in_flight: Arc<AtomicUsize>,
    },
}

/// Registry entry for a bounded, off-RX action feedback subscription
/// (`aorta_action_client_subscribe_feedback2`). The raw zenoh subscriber's
/// inline RX callback only ENQUEUES into the bounded `queue` (applying the
/// overflow policy + a dropped counter); the C callback fires per
/// [`Feedback2Delivery`]. See `action_client.rs` for the queue + drain model.
struct FfiActionFeedback2SubEntry {
    subscriber: ZenohRawSubscriber<()>,
    queue: std::sync::Arc<crate::ffi::c_api::action_client::FfiFeedbackQueue>,
    delivery: Feedback2Delivery,
}

/// Registry entry for an event-driven action result subscription
/// (`aorta_action_client_subscribe_result`). The watcher runs as a task on the
/// shared FFI runtime; the terminal callback is delivered as a `CoroutineResume`
/// event on the node executor's reserved lane, or inline on the watcher task
/// when no executor is attached (the per-subscription
/// delivery thread is gone). See `action_client.rs` for the watcher and
/// delivery model.
struct FfiActionResultSubEntry {
    /// The watcher task. `free` aborts it AND awaits it, so the watcher's
    /// synchronous decode-hook calls (which touch the C callback state) have all
    /// finished before the caller frees that state — `abort()` alone is async
    /// and would not guarantee this.
    join: tokio::task::JoinHandle<()>,
    /// Set on free so a not-yet-started terminal callback is skipped before it
    /// touches the (possibly freed) C callback state — including a `CoroutineResume`
    /// lane event still queued behind a stopped/busy executor (a stale wake).
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    /// Drained to zero on free: waits for a terminal callback that is
    /// ACTIVELY running (on an executor dispatch or the watcher task) to finish.
    in_flight: Arc<AtomicUsize>,
}

struct OwnedFfiBytes {
    payload: crate::ZBytes,
    slices: Vec<aorta_bytes_slice_t>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry {
        next_id: AtomicU64::new(1),
        arbiters: Mutex::new(HashMap::new()),
        nodes: Mutex::new(HashMap::new()),
        publishers: Mutex::new(HashMap::new()),
        subscribers: Mutex::new(HashMap::new()),
        executors: Mutex::new(HashMap::new()),
        timers: Mutex::new(HashMap::new()),
        samples: Mutex::new(HashMap::new()),
        services: Mutex::new(HashMap::new()),
        service_requests: Mutex::new(HashMap::new()),
        clients: Mutex::new(HashMap::new()),
        action_clients: Mutex::new(HashMap::new()),
        action_servers: Mutex::new(HashMap::new()),
        action_contexts: Mutex::new(HashMap::new()),
        action_feedback_subscribers: Mutex::new(HashMap::new()),
        action_feedback2_subscribers: Mutex::new(HashMap::new()),
        action_result_subscribers: Mutex::new(HashMap::new()),
        context_stores: Mutex::new(HashMap::new()),
        context_watches: Mutex::new(HashMap::new()),
        ctx_owner_liveliness_watches: Mutex::new(HashMap::new()),
        stop_handles: Mutex::new(HashMap::new()),
        capture_subscribers: Mutex::new(HashMap::new()),
        schema_resolvers: Mutex::new(HashMap::new()),
        cancel_tokens: Mutex::new(HashMap::new()),
        flow_scopes: Mutex::new(HashMap::new()),
        flow_tasks: Mutex::new(HashMap::new()),
        flow_watches: Mutex::new(HashMap::new()),
        flow_windows: Mutex::new(HashMap::new()),
        flow_readiness: Mutex::new(HashMap::new()),
        flow_aligned_sets: Mutex::new(HashMap::new()),
    })
}

fn alloc_handle() -> u64 {
    alloc_handle_from(&registry().next_id)
}

// Handle 0 is the reserved invalid-sentinel
// (every `aorta_*_handle_t` defaults to 0 to flag "not yet
// allocated"). `fetch_add(Relaxed)` wraps after 2^64-1 increments —
// roughly 580,000 years at one million allocations per second, so
// the cosmic-ray bound is generous, but if a long-running process
// ever does cross the boundary the post-wrap value is 0, which
// would silently collide with the sentinel. Loop past zero so that
// stays impossible regardless of process lifetime.
fn alloc_handle_from(counter: &AtomicU64) -> u64 {
    loop {
        let id = counter.fetch_add(1, Ordering::Relaxed);
        if id != 0 {
            return id;
        }
    }
}

fn async_executor_in_flight_limit() -> usize {
    runtime_env::var(ENV_ASYNC_EXECUTOR_IN_FLIGHT)
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ASYNC_EXECUTOR_IN_FLIGHT)
        .max(1)
}

fn bytes_view_from_slices(slices: &[aorta_bytes_slice_t], total_len: usize) -> aorta_bytes_view_t {
    aorta_bytes_view_t {
        slices: if slices.is_empty() {
            ptr::null()
        } else {
            slices.as_ptr()
        },
        slice_count: slices.len(),
        total_len,
    }
}

fn ffi_slice_from_bytes(slice: &[u8]) -> aorta_bytes_slice_t {
    aorta_bytes_slice_t {
        data: slice.as_ptr(),
        len: slice.len(),
    }
}

fn build_slices_from_zbytes(payload: &crate::ZBytes, out: &mut Vec<aorta_bytes_slice_t>) {
    out.clear();
    for slice in payload.slices() {
        out.push(ffi_slice_from_bytes(slice));
    }
}

fn bytes_view_from_zbytes_borrowed(
    payload: &crate::ZBytes,
    single: &mut aorta_bytes_slice_t,
    multi: &mut Vec<aorta_bytes_slice_t>,
) -> aorta_bytes_view_t {
    multi.clear();
    let mut iter = payload.slices();
    let Some(first) = iter.next() else {
        return aorta_bytes_view_t::default();
    };

    let Some(second) = iter.next() else {
        *single = ffi_slice_from_bytes(first);
        return aorta_bytes_view_t {
            slices: ptr::addr_of!(*single),
            slice_count: 1,
            total_len: payload.len(),
        };
    };

    multi.push(ffi_slice_from_bytes(first));
    multi.push(ffi_slice_from_bytes(second));
    for slice in iter {
        multi.push(ffi_slice_from_bytes(slice));
    }
    bytes_view_from_slices(multi, payload.len())
}

fn owned_bytes_from_zbytes(payload: crate::ZBytes) -> aorta_owned_bytes_t {
    let mut owned = Box::new(OwnedFfiBytes {
        payload,
        slices: Vec::new(),
    });
    build_slices_from_zbytes(&owned.payload, &mut owned.slices);
    let view = bytes_view_from_slices(&owned.slices, owned.payload.len());

    aorta_owned_bytes_t {
        view,
        internal: Box::into_raw(owned) as *mut c_void,
    }
}

impl FfiActionClient {
    async fn query_bytes(
        &self,
        key: &str,
        payload: Option<crate::ZBytes>,
        timeout_duration: Duration,
    ) -> AortaResult<crate::ZBytes> {
        self.query_optional_bytes(key, payload, timeout_duration)
            .await?
            .ok_or_else(|| {
                AortaError::timeout(format!(
                    "action '{}' query '{}' timed out after {:?}",
                    self.action_name, key, timeout_duration
                ))
            })
    }

    async fn query_optional_bytes(
        &self,
        key: &str,
        payload: Option<crate::ZBytes>,
        timeout_duration: Duration,
    ) -> AortaResult<Option<crate::ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            payload,
            None,
            timeout_duration,
            FfiReplyErrPolicy::Internal,
        )
        .await
    }

    /// Like [`query_optional_bytes`], with a query attachment (the
    /// traceparent rides the attachment on action control queries).
    async fn query_optional_bytes_attached(
        &self,
        key: &str,
        payload: Option<crate::ZBytes>,
        attachment: Option<Vec<u8>>,
        timeout_duration: Duration,
    ) -> AortaResult<Option<crate::ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            payload,
            attachment,
            timeout_duration,
            FfiReplyErrPolicy::Internal,
        )
        .await
    }

    async fn query_optional_snapshot_bytes(
        &self,
        key: &str,
        timeout_duration: Duration,
    ) -> AortaResult<Option<crate::ZBytes>> {
        self.query_optional_bytes_inner(
            key,
            None,
            None,
            timeout_duration,
            FfiReplyErrPolicy::Absent,
        )
        .await
    }

    async fn query_optional_bytes_inner(
        &self,
        key: &str,
        payload: Option<crate::ZBytes>,
        attachment: Option<Vec<u8>>,
        timeout_duration: Duration,
        reply_err_policy: FfiReplyErrPolicy,
    ) -> AortaResult<Option<crate::ZBytes>> {
        let builder = self
            .session
            .inner()
            .get(key)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None);

        let replies = match (payload, attachment) {
            (Some(payload), Some(attachment)) => {
                builder
                    .payload(payload)
                    .attachment(attachment)
                    .timeout(timeout_duration)
                    .await?
            }
            (Some(payload), None) => builder.payload(payload).timeout(timeout_duration).await?,
            (None, Some(attachment)) => {
                builder
                    .attachment(attachment)
                    .timeout(timeout_duration)
                    .await?
            }
            (None, None) => builder.timeout(timeout_duration).await?,
        };

        let first = match tokio_timeout(timeout_duration, replies.recv_async()).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) | Err(_) => return Ok(None),
        };

        // A synthesized Zenoh query-timeout reply is the dual-guard firing
        // (client session and/or router `QueryCleanup` timer), not a server
        // answer. Surface it as the recv guard's `Ok(None)` (so `query_bytes`
        // maps to `AortaError::Timeout`) BEFORE the multi-reply check below —
        // under load both timers can land their `Err("Timeout")` replies in the
        // channel before the recv guard fires, and a second one must never be
        // mistaken for a `ConcurrentConflict`. Mirrors `query_optional_bytes_inner`
        // in `action_client.rs`.
        if crate::transport::reply_is_zenoh_query_timeout(&first) {
            return Ok(None);
        }

        let check_window = timeout_duration.min(Duration::from_millis(
            DEFAULT_ACTION_MULTI_REPLY_CHECK_WINDOW_MS,
        ));
        // Multi-reply guard: a genuine second reply means two servers answered
        // the same goal. A synthesized timeout reply is the dual-guard, not a
        // conflict — skip it so the held-query timeout path never trips this.
        if !check_window.is_zero()
            && matches!(
                tokio_timeout(check_window, replies.recv_async()).await,
                Ok(Ok(second)) if !crate::transport::reply_is_zenoh_query_timeout(&second)
            )
        {
            return Err(AortaError::concurrent_conflict(format!(
                "action '{}' query '{}' returned multiple replies; deployment must enforce one active server per {{group,action}}",
                self.action_name, key
            )));
        }

        match first.result() {
            Ok(sample) => Ok(Some(sample.payload().clone())),
            Err(err) => {
                // `first` is a non-timeout reply error here (synthesized Zenoh
                // query-timeout replies already returned `Ok(None)` above), so
                // it is a genuine provider fault — surface it per policy.
                match reply_err_policy {
                    FfiReplyErrPolicy::Internal => Err(AortaError::internal(format!(
                        "action '{}' query '{}' returned error: {}",
                        self.action_name,
                        key,
                        crate::transport::reply_error_text(err)
                    ))),
                    FfiReplyErrPolicy::Absent => Ok(None),
                }
            }
        }
    }

    fn send_goal_key(&self) -> AortaResult<String> {
        self.key_builder
            .action_send_goal_key(&self.action_name)
            .map(Into::into)
    }

    fn cancel_goal_key(&self) -> AortaResult<String> {
        self.key_builder
            .action_cancel_goal_key(&self.action_name)
            .map(Into::into)
    }

    fn get_result_key(&self) -> AortaResult<String> {
        // FfiActionClient: client-side query against a concrete action
        // server. Action name has no glob here, so the publish-side
        // (no-wildcard) validator is the right one.
        self.key_builder
            .action_get_result_key_pub(&self.action_name)
            .map(Into::into)
    }

    fn status_key(&self, goal_id: &str) -> AortaResult<String> {
        self.key_builder
            .action_status_key(&self.action_name, goal_id)
            .map(Into::into)
    }

    fn feedback_key(&self) -> AortaResult<String> {
        // FfiActionClient: client-side subscription to a concrete
        // action server's feedback publisher. Action name has no glob.
        self.key_builder
            .action_feedback_key_pub(&self.action_name)
            .map(Into::into)
    }
}

impl FfiRawActionServer {
    async fn new(
        node: &Node,
        action_name: String,
        schema_bfbs: Vec<u8>,
        options: crate::action_server::ActionServerOptions,
        callbacks: FfiActionServerCallbacks,
        callback_gate: Arc<FfiCallbackGate>,
    ) -> AortaResult<Arc<Self>> {
        ensure_single_action_provider(
            node.session(),
            node.key_builder(),
            &action_name,
            Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
        )
        .await?;

        let info_key: String = node.key_builder().action_info_key(&action_name)?.into();
        let send_goal_key: String = node
            .key_builder()
            .action_send_goal_key(&action_name)?
            .into();
        let cancel_goal_key: String = node
            .key_builder()
            .action_cancel_goal_key(&action_name)?
            .into();
        let get_result_key: String = node
            .key_builder()
            .action_get_result_key_pub(&action_name)?
            .into();
        let feedback_key: String = node
            .key_builder()
            .action_feedback_key_pub(&action_name)?
            .into();

        let schema_hash = SchemaHash::from_bfbs_bytes(&schema_bfbs);
        node.schema_registry()
            .ensure_schema(schema_hash, schema_bfbs.clone())
            .await?;

        let info = ActionInfo {
            action_name: action_name.clone(),
            schema_hash,
            supports_cancel: options.supports_cancel,
            max_concurrency: u32::try_from(options.max_concurrency).map_err(|_| {
                AortaError::invalid_argument("action max_concurrency exceeds u32 range")
            })?,
            result_ttl_ms: u64::try_from(options.result_ttl.as_millis()).map_err(|_| {
                AortaError::invalid_argument("action result_ttl exceeds supported range")
            })?,
            dedup_ttl_ms: u64::try_from(options.dedup_ttl.as_millis()).map_err(|_| {
                AortaError::invalid_argument("action dedup_ttl exceeds supported range")
            })?,
            server_node_name: node.name().to_string(),
            server_epoch: Uuid::new_v4().hyphenated().to_string(),
        };
        let info_payload = Arc::new(TokioRwLock::new(Some(crate::ZBytes::from(info.encode()?))));
        let (info_stop_tx, info_stop_rx) = oneshot::channel();
        let info_task = spawn_action_info_queryable(
            node.session().clone(),
            info_key.clone(),
            Arc::clone(&info_payload),
            crate::ZBytes::from(schema_bfbs),
            info_stop_rx,
        )
        .await?;

        // Tracing: intern the per-action stage/marker events once (identical
        // names to the core ActionServer's set — the converter cannot tell
        // the SDK paths apart).
        let ev_accepted_to_execution_started = crate::trace::stage_event_for(
            crate::trace::StageId::ActionAcceptedToExecutionStarted,
            &action_name,
        );
        let ev_accepted_to_finished = crate::trace::stage_event_for(
            crate::trace::StageId::ActionAcceptedToFinished,
            &action_name,
        );
        let ev_user_callback =
            crate::trace::stage_event_for(crate::trace::StageId::ActionUserCallback, &action_name);
        let ev_feedback_pub = crate::trace::marker_event_for("action.feedback_pub", &action_name);
        let ev_cancel_to_canceled = crate::trace::stage_event_for(
            crate::trace::StageId::ActionCancelRequestedToCanceled,
            &action_name,
        );
        let ev_goal_admission =
            crate::trace::stage_event_for(crate::trace::StageId::ActionGoalAdmission, &action_name);
        let execute_slots = Arc::new(tokio::sync::Semaphore::new(
            options
                .max_concurrency
                .min(tokio::sync::Semaphore::MAX_PERMITS),
        ));
        let ev_get_result_query = crate::trace::stage_event_for(
            crate::trace::StageId::ActionGetResultQuery,
            &action_name,
        );

        Ok(Arc::new(Self {
            node: node.clone(),
            action_name,
            info_key,
            send_goal_key,
            cancel_goal_key,
            get_result_key,
            feedback_key,
            info,
            options,
            callbacks,
            callback_gate,
            execute_slots,
            info_payload,
            info_stop_tx: TokioMutex::new(Some(info_stop_tx)),
            info_task: TokioMutex::new(Some(info_task)),
            send_goal_stop_tx: TokioMutex::new(None),
            send_goal_task: TokioMutex::new(None),
            cancel_goal_stop_tx: TokioMutex::new(None),
            cancel_goal_task: TokioMutex::new(None),
            get_result_stop_tx: TokioMutex::new(None),
            get_result_task: TokioMutex::new(None),
            goals: TokioMutex::new(HashMap::new()),
            expired_goals: TokioMutex::new(HashMap::new()),
            context_handles: TokioMutex::new(Vec::new()),
            admission_lock: TokioMutex::new(()),
            active_goal_count: AtomicUsize::new(0),
            ev_accepted_to_execution_started,
            ev_accepted_to_finished,
            ev_user_callback,
            ev_feedback_pub,
            ev_cancel_to_canceled,
            ev_goal_admission,
            ev_get_result_query,
        }))
    }

    async fn start(self: &Arc<Self>) -> AortaResult<()> {
        self.start_send_goal_queryable().await?;
        if self.options.supports_cancel {
            self.start_cancel_goal_queryable().await?;
        }
        self.start_get_result_queryable().await?;
        Ok(())
    }

    async fn start_send_goal_queryable(self: &Arc<Self>) -> AortaResult<()> {
        let mut stop_guard = self.send_goal_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.send_goal_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let send_goal_key = self.send_goal_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                // The caller's traceparent rides the send_goal query
                // attachment; parsed only when tracing is enabled (the stage
                // records that would join it are gated the same way).
                let inbound_trace = if crate::trace::trace_enabled() {
                    query.attachment().and_then(|attachment| {
                        crate::attachment::decode(attachment.to_bytes().as_ref()).traceparent
                    })
                } else {
                    None
                };
                // Goal admission (request received -> accept/reject reply
                // ready), parented to the CALLER's goal_sent span — rejected
                // goals show up in the caller's trace too (mirrors the core
                // ActionServer).
                let admission_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
                let reply_payload = this
                    .handle_send_goal(request.to_bytes().to_vec(), inbound_trace)
                    .await;
                if let Some(start) = admission_start {
                    let parent = inbound_trace.map(|tp| crate::trace::SpanLink {
                        trace_id: tp.trace_id,
                        span_id: tp.span_id,
                        parent_span_id: 0,
                    });
                    crate::trace::record_stage_elapsed_under(
                        crate::trace::StageId::ActionGoalAdmission,
                        start,
                        this.ev_goal_admission,
                        parent.as_ref(),
                    );
                }
                match reply_payload {
                    Ok(payload) => {
                        if let Err(err) = query.reply(&send_goal_key, payload).await {
                            set_last_error(format!(
                                "action '{}' send_goal reply failed: {}",
                                this.action_name, err
                            ));
                        }
                    }
                    Err(err) => {
                        if let Err(reply_err) = query.reply_err(err.to_string()).await {
                            set_last_error(format!(
                                "action '{}' send_goal error reply failed: {}",
                                this.action_name, reply_err
                            ));
                        }
                    }
                }
            }

            let _ = queryable.undeclare().await;
        });

        *stop_guard = Some(stop_tx);
        *self.send_goal_task.lock().await = Some(task);
        drop(stop_guard);
        self.wait_for_send_goal_queryable_ready().await
    }

    async fn wait_for_send_goal_queryable_ready(&self) -> AortaResult<()> {
        let timeout_duration = Duration::from_millis(1_000);
        let poll_duration = Duration::from_millis(25);
        let started = tokio::time::Instant::now();

        loop {
            let replies = self
                .node
                .session()
                .inner()
                .get(self.send_goal_key.clone())
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .timeout(poll_duration)
                .await?;

            if matches!(
                tokio_timeout(poll_duration, replies.recv_async()).await,
                Ok(Ok(_))
            ) {
                return Ok(());
            }
            if started.elapsed() >= timeout_duration {
                return Err(AortaError::timeout(format!(
                    "action '{}' send_goal queryable did not become ready after {:?}",
                    self.action_name, timeout_duration
                )));
            }
            tokio::time::sleep(poll_duration).await;
        }
    }

    async fn start_cancel_goal_queryable(self: &Arc<Self>) -> AortaResult<()> {
        let mut stop_guard = self.cancel_goal_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.cancel_goal_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let cancel_goal_key = self.cancel_goal_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                let reply_payload = this.handle_cancel_goal(request.to_bytes().to_vec()).await;
                match reply_payload {
                    Ok(payload) => {
                        let _ = query.reply(&cancel_goal_key, payload).await;
                    }
                    Err(err) => {
                        let _ = query.reply_err(err.to_string()).await;
                    }
                }
            }

            let _ = queryable.undeclare().await;
        });

        *stop_guard = Some(stop_tx);
        *self.cancel_goal_task.lock().await = Some(task);
        Ok(())
    }

    async fn start_get_result_queryable(self: &Arc<Self>) -> AortaResult<()> {
        let mut stop_guard = self.get_result_stop_tx.lock().await;
        if stop_guard.is_some() {
            return Ok(());
        }

        let queryable = self
            .node
            .session()
            .inner()
            .declare_queryable(self.get_result_key.clone())
            .await?;
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let this = Arc::clone(self);
        let get_result_key = self.get_result_key.clone();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(_) => break,
                    },
                };

                let request = query.payload().cloned().unwrap_or_default();
                let reply_payload = this.handle_get_result(request.to_bytes().to_vec()).await;
                match reply_payload {
                    Ok(payload) => {
                        let _ = query.reply(&get_result_key, payload).await;
                    }
                    Err(err) => {
                        let _ = query.reply_err(err.to_string()).await;
                    }
                }
            }

            let _ = queryable.undeclare().await;
        });

        *stop_guard = Some(stop_tx);
        *self.get_result_task.lock().await = Some(task);
        Ok(())
    }

    unsafe fn decode_send_goal_request_goal_id(&self, payload: &[u8]) -> AortaResult<String> {
        let mut slice = aorta_bytes_slice_t::default();
        let view = single_slice_bytes_view(payload, &mut slice);
        let mut goal_id = aorta_string_view_t::default();
        let cb = self
            .callbacks
            .codec
            .decode_send_goal_request_goal_id
            .ok_or_else(|| {
                AortaError::invalid_argument("decode_send_goal_request_goal_id is null")
            })?;
        let rc = cb(ptr::addr_of!(view), ptr::addr_of_mut!(goal_id));
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to decode action send_goal request goal_id",
            ));
        }
        string_view_to_string(goal_id, "goal_id")
    }

    unsafe fn decode_goal_secret_request(
        &self,
        payload: &[u8],
        cb: aorta_action_decode_goal_secret_cb,
    ) -> AortaResult<(String, String)> {
        let mut slice = aorta_bytes_slice_t::default();
        let view = single_slice_bytes_view(payload, &mut slice);
        let mut goal_id = aorta_string_view_t::default();
        let mut goal_secret = aorta_string_view_t::default();
        let cb =
            cb.ok_or_else(|| AortaError::invalid_argument("action decode callback is null"))?;
        let rc = cb(
            ptr::addr_of!(view),
            ptr::addr_of_mut!(goal_id),
            ptr::addr_of_mut!(goal_secret),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to decode action goal_secret request payload",
            ));
        }
        Ok((
            string_view_to_string(goal_id, "goal_id")?,
            string_view_to_string(goal_secret, "goal_secret")?,
        ))
    }

    unsafe fn encode_send_goal_response(
        &self,
        response: &ActionSendGoalResponse,
    ) -> AortaResult<Vec<u8>> {
        let cb = self
            .callbacks
            .codec
            .encode_send_goal_response
            .ok_or_else(|| AortaError::invalid_argument("encode_send_goal_response is null"))?;
        let mut owned = aorta_owned_bytes_view_t::default();
        let rc = cb(
            if response.accepted { 1 } else { 0 },
            string_view_from_str(&response.goal_id),
            string_view_from_str(&response.goal_secret),
            action_reply_code_to_i16(response.code),
            action_status_to_i8(response.status),
            string_view_from_str(&response.message),
            string_view_from_str(&response.server_epoch),
            ptr::addr_of_mut!(owned),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to encode action send_goal response",
            ));
        }
        Ok(
            zbytes_from_owned_ffi_view(ptr::addr_of!(owned), "send_goal response")?
                .to_bytes()
                .to_vec(),
        )
    }

    unsafe fn encode_cancel_goal_response(
        &self,
        response: &ActionCancelGoalResponse,
    ) -> AortaResult<Vec<u8>> {
        let cb = self
            .callbacks
            .codec
            .encode_cancel_goal_response
            .ok_or_else(|| AortaError::invalid_argument("encode_cancel_goal_response is null"))?;
        let mut owned = aorta_owned_bytes_view_t::default();
        let rc = cb(
            if response.accepted { 1 } else { 0 },
            string_view_from_str(&response.goal_id),
            action_reply_code_to_i16(response.code),
            action_status_to_i8(response.status),
            string_view_from_str(&response.message),
            ptr::addr_of_mut!(owned),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to encode action cancel_goal response",
            ));
        }
        Ok(
            zbytes_from_owned_ffi_view(ptr::addr_of!(owned), "cancel response")?
                .to_bytes()
                .to_vec(),
        )
    }

    unsafe fn encode_public_status(&self, status: &ActionPublicStatus) -> AortaResult<Vec<u8>> {
        let cb = self
            .callbacks
            .codec
            .encode_public_status
            .ok_or_else(|| AortaError::invalid_argument("encode_public_status is null"))?;
        let mut owned = aorta_owned_bytes_view_t::default();
        let rc = cb(
            string_view_from_str(&status.goal_id),
            action_status_to_i8(status.status),
            action_reply_code_to_i16(status.code),
            status.accepted_stamp_ns,
            status.started_stamp_ns,
            status.finished_stamp_ns,
            status.last_feedback_seq,
            string_view_from_str(&status.server_epoch),
            ptr::addr_of_mut!(owned),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to encode action public status",
            ));
        }
        Ok(
            zbytes_from_owned_ffi_view(ptr::addr_of!(owned), "public status")?
                .to_bytes()
                .to_vec(),
        )
    }

    unsafe fn encode_feedback(
        &self,
        goal_id: &str,
        seq: u64,
        stamp_ns: u64,
        status: ActionStatus,
        feedback_data: &[u8],
        message: &str,
    ) -> AortaResult<Vec<u8>> {
        let cb = self
            .callbacks
            .codec
            .encode_feedback
            .ok_or_else(|| AortaError::invalid_argument("encode_feedback is null"))?;
        let mut feedback_slice = aorta_bytes_slice_t::default();
        let feedback_view = single_slice_bytes_view(feedback_data, &mut feedback_slice);
        let mut owned = aorta_owned_bytes_view_t::default();
        let rc = cb(
            string_view_from_str(goal_id),
            seq,
            stamp_ns,
            action_status_to_i8(status),
            ptr::addr_of!(feedback_view),
            string_view_from_str(message),
            ptr::addr_of_mut!(owned),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal("failed to encode action feedback"));
        }
        Ok(
            zbytes_from_owned_ffi_view(ptr::addr_of!(owned), "feedback")?
                .to_bytes()
                .to_vec(),
        )
    }

    unsafe fn encode_get_result_response(
        &self,
        response: &ActionGetResultResponse<Vec<u8>, Vec<u8>>,
    ) -> AortaResult<Vec<u8>> {
        let cb = self
            .callbacks
            .codec
            .encode_get_result_response
            .ok_or_else(|| AortaError::invalid_argument("encode_get_result_response is null"))?;
        let mut feedback_slice = aorta_bytes_slice_t::default();
        let feedback_view =
            single_slice_bytes_view(&response.feedback_snapshot, &mut feedback_slice);
        let result_vec = response.result.clone().unwrap_or_default();
        let mut result_slice = aorta_bytes_slice_t::default();
        let result_view = single_slice_bytes_view(&result_vec, &mut result_slice);
        let mut owned = aorta_owned_bytes_view_t::default();
        let rc = cb(
            string_view_from_str(&response.goal_id),
            action_status_to_i8(response.status),
            response.accepted_stamp_ns,
            response.started_stamp_ns,
            response.finished_stamp_ns,
            response.last_feedback_seq,
            ptr::addr_of!(feedback_view),
            ptr::addr_of!(result_view),
            action_reply_code_to_i16(response.code),
            string_view_from_str(&response.message),
            string_view_from_str(&response.server_epoch),
            ptr::addr_of_mut!(owned),
        );
        if rc != AORTA_OK {
            return Err(AortaError::internal(
                "failed to encode action get_result response",
            ));
        }
        Ok(
            zbytes_from_owned_ffi_view(ptr::addr_of!(owned), "get_result response")?
                .to_bytes()
                .to_vec(),
        )
    }

    async fn handle_send_goal(
        self: &Arc<Self>,
        request_payload: Vec<u8>,
        inbound_trace: Option<crate::trace::TraceParent>,
    ) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action send_goal request payload is empty",
            ));
        }

        // SAFETY: invokes the codec `unsafe fn` which calls
        // `self.callbacks.codec.decode_send_goal_request`, a function pointer
        // installed by FFI caller through `aorta_action_server_codec_t`.
        // The codec contract: pure function over a borrowed `Vec<u8>`,
        // returns a fully-owned `String`, no thread/lifetime entanglement.
        let goal_id = unsafe { self.decode_send_goal_request_goal_id(&request_payload) }?;
        let _ = self
            .node
            .key_builder()
            .action_status_key(&self.action_name, &goal_id)?;
        let payload_hash = ffi_action_payload_hash(&request_payload);
        let _guard = self.admission_lock.lock().await;

        let maybe_existing = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };
        if let Some(existing) = maybe_existing {
            let state = existing.state.lock().await;
            let response = if existing.request_payload_hash == payload_hash {
                ActionSendGoalResponse {
                    accepted: true,
                    goal_id,
                    goal_secret: existing.goal_secret.clone(),
                    code: ActionReplyCode::DuplicateAccepted,
                    status: state.status,
                    message: "duplicate accepted goal_id".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::PayloadConflict,
                    status: state.status,
                    message: "goal_id already exists with different payload".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            };
            // SAFETY: invokes the codec `unsafe fn` which calls
            // `self.callbacks.codec.encode_send_goal_response`, an FFI-supplied
            // function pointer that owns a borrowed `&ActionSendGoalResponse`
            // and returns a fully-owned `Vec<u8>`.
            return unsafe { self.encode_send_goal_response(&response) };
        }

        if let Some(expired) = self.find_expired_goal(&goal_id).await {
            let response = if expired.payload_hash == payload_hash {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::GoalExpired,
                    status: expired.status,
                    message: "goal result expired; use a new goal_id".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionSendGoalResponse {
                    accepted: false,
                    goal_id,
                    goal_secret: String::new(),
                    code: ActionReplyCode::PayloadConflict,
                    status: expired.status,
                    message: "goal_id already exists with different payload".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            };
            // SAFETY: invokes the codec `unsafe fn` which calls
            // `self.callbacks.codec.encode_send_goal_response`, an FFI-supplied
            // function pointer that owns a borrowed `&ActionSendGoalResponse`
            // and returns a fully-owned `Vec<u8>`.
            return unsafe { self.encode_send_goal_response(&response) };
        }

        if self.current_active_goal_count().await >= self.options.max_concurrency {
            let response = ActionSendGoalResponse {
                accepted: false,
                goal_id,
                goal_secret: String::new(),
                code: ActionReplyCode::Busy,
                status: ActionStatus::Unknown,
                message: "action server reached max_concurrency".to_string(),
                server_epoch: self.info.server_epoch.clone(),
            };
            // SAFETY: invokes the codec `unsafe fn` which calls
            // `self.callbacks.codec.encode_send_goal_response`, an FFI-supplied
            // function pointer that owns a borrowed `&ActionSendGoalResponse`
            // and returns a fully-owned `Vec<u8>`.
            return unsafe { self.encode_send_goal_response(&response) };
        }

        let accepted_stamp_ns = unix_time_ns();
        let status_key: String = self
            .node
            .key_builder()
            .action_status_key(&self.action_name, &goal_id)?
            .into();
        let goal_secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let initial_status = ActionPublicStatus {
            goal_id: goal_id.clone(),
            status: ActionStatus::Accepted,
            code: ActionReplyCode::Ok,
            accepted_stamp_ns,
            started_stamp_ns: 0,
            finished_stamp_ns: 0,
            last_feedback_seq: 0,
            server_epoch: self.info.server_epoch.clone(),
        };
        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `self.callbacks.codec.encode_public_status` function pointer.
        // Borrows `&ActionPublicStatus`, returns owned `Vec<u8>`.
        let initial_status_payload = unsafe { self.encode_public_status(&initial_status) }?;
        let status_payload = Arc::new(TokioRwLock::new(Some(crate::ZBytes::from(
            initial_status_payload.clone(),
        ))));
        let goal = Arc::new(FfiRawGoalRecord {
            goal_id: goal_id.clone(),
            goal_secret: goal_secret.clone(),
            request_payload: request_payload.clone(),
            request_payload_hash: payload_hash,
            status_key,
            status_payload,
            status_stop_tx: TokioMutex::new(None),
            status_task: TokioMutex::new(None),
            state: TokioMutex::new(FfiRawGoalState {
                status: ActionStatus::Accepted,
                code: ActionReplyCode::Ok,
                message: String::new(),
                accepted_stamp_ns,
                started_stamp_ns: 0,
                finished_stamp_ns: 0,
                last_feedback_seq: 0,
                // Tracing: monotonic accept time, captured only when enabled.
                accepted_mono_ns: crate::trace::trace_enabled().then(crate::time::monotonic_ns),
                last_feedback_mono_ns: 0,
                feedback_snapshot: Vec::new(),
                result_payload: None,
            }),
            cancel_requested: AtomicBool::new(false),
            cancel_notify: Notify::new(),
            cancel_requested_mono_ns: AtomicU64::new(0),
            inbound_trace,
            // Per-goal SERVER ROOT (span hierarchy): one cross-process link to
            // the caller, lifecycle stages nested beneath it.
            goal_root: crate::trace::inbound_root_link(inbound_trace.as_ref()),
        });

        {
            let mut goals = self.goals.lock().await;
            goals.insert(goal_id.clone(), Arc::clone(&goal));
            self.active_goal_count.fetch_add(1, Ordering::AcqRel);
        }

        if let Err(err) = goal
            .start_status_queryable(self.node.session().clone())
            .await
        {
            let _ = self.remove_goal(&goal_id).await;
            self.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            return Err(err);
        }
        if let Err(err) = goal.publish_status(self).await {
            let _ = self.remove_goal(&goal_id).await;
            self.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            goal.request_stop().await;
            return Err(err);
        }

        let response = ActionSendGoalResponse {
            accepted: true,
            goal_id: goal_id.clone(),
            goal_secret,
            code: ActionReplyCode::Ok,
            status: ActionStatus::Accepted,
            message: String::new(),
            server_epoch: self.info.server_epoch.clone(),
        };
        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `self.callbacks.codec.encode_send_goal_response` function pointer.
        // Borrows `&ActionSendGoalResponse`, returns owned `Vec<u8>`.
        let response_payload = unsafe { self.encode_send_goal_response(&response) }?;
        self.spawn_goal_handler(goal);
        Ok(response_payload)
    }

    async fn handle_cancel_goal(
        self: &Arc<Self>,
        request_payload: Vec<u8>,
    ) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action cancel_goal request payload is empty",
            ));
        }

        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `decode_cancel_goal_request` function pointer; same contract as
        // the send_goal codec (borrowed `&Vec<u8>`, owned String pair out).
        let (goal_id, goal_secret) = unsafe {
            self.decode_goal_secret_request(
                &request_payload,
                self.callbacks.codec.decode_cancel_goal_request,
            )
        }?;
        if !self.options.supports_cancel {
            let response = ActionCancelGoalResponse {
                accepted: false,
                goal_id,
                code: ActionReplyCode::Conflict,
                status: ActionStatus::Unknown,
                message: "action server does not support cancel".to_string(),
            };
            // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
            // `encode_cancel_goal_response` function pointer; same contract
            // as the send_goal codec (borrowed response, owned bytes out).
            return unsafe { self.encode_cancel_goal_response(&response) };
        }
        let goal = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };

        let response = if let Some(goal) = goal {
            let mut should_publish = false;
            let response = {
                let mut state = goal.state.lock().await;
                if goal.goal_secret != goal_secret {
                    ActionCancelGoalResponse {
                        accepted: false,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::Unauthorized,
                        status: state.status,
                        message: "goal_secret does not match".to_string(),
                    }
                } else if state.status.is_terminal() {
                    ActionCancelGoalResponse {
                        accepted: false,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::AlreadyTerminal,
                        status: state.status,
                        message: "goal is already terminal".to_string(),
                    }
                } else {
                    if state.status != ActionStatus::Canceling {
                        state.status = ActionStatus::Canceling;
                        should_publish = true;
                    }
                    ActionCancelGoalResponse {
                        accepted: true,
                        goal_id: goal_id.clone(),
                        code: ActionReplyCode::Ok,
                        status: ActionStatus::Canceling,
                        message: String::new(),
                    }
                }
            };

            if response.accepted {
                goal.cancel_requested.store(true, Ordering::Relaxed);
                // First accepted cancel anchors the cancel->canceled stage
                // (gated: no clock read when tracing is off).
                if crate::trace::trace_enabled() {
                    let now = crate::time::monotonic_ns().max(1);
                    let _ = goal.cancel_requested_mono_ns.compare_exchange(
                        0,
                        now,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
                goal.cancel_notify.notify_waiters();
                // `should_publish` is set only by the first transition into
                // CANCELING, so the callback fires at most once per goal.
                if should_publish {
                    self.dispatch_cancel_requested(&goal.goal_id);
                }
            }
            if should_publish {
                goal.publish_status(self).await?;
            }
            response
        } else {
            ActionCancelGoalResponse {
                accepted: false,
                goal_id,
                code: ActionReplyCode::NotFound,
                status: ActionStatus::Unknown,
                message: "goal_id not found".to_string(),
            }
        };

        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `encode_cancel_goal_response` function pointer; same contract as
        // the other codec calls in this module.
        unsafe { self.encode_cancel_goal_response(&response) }
    }

    async fn handle_get_result(self: &Arc<Self>, request_payload: Vec<u8>) -> AortaResult<Vec<u8>> {
        if request_payload.is_empty() {
            return Err(AortaError::invalid_argument(
                "action get_result request payload is empty",
            ));
        }

        // Result delivery, server half (request received -> response built),
        // child of the goal root when the goal is live. Gated.
        let query_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);

        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `decode_get_result_request` function pointer; same contract as
        // the cancel-goal codec.
        let (goal_id, goal_secret) = unsafe {
            self.decode_goal_secret_request(
                &request_payload,
                self.callbacks.codec.decode_get_result_request,
            )
        }?;
        let goal = {
            let goals = self.goals.lock().await;
            goals.get(&goal_id).cloned()
        };
        let goal_root = goal.as_ref().and_then(|goal| goal.goal_root);

        let response = if let Some(goal) = goal {
            let state = goal.state.lock().await;
            if goal.goal_secret != goal_secret {
                ActionGetResultResponse {
                    goal_id,
                    status: state.status,
                    accepted_stamp_ns: state.accepted_stamp_ns,
                    started_stamp_ns: state.started_stamp_ns,
                    finished_stamp_ns: state.finished_stamp_ns,
                    last_feedback_seq: state.last_feedback_seq,
                    feedback_snapshot: state.feedback_snapshot.clone(),
                    result: None,
                    code: ActionReplyCode::Unauthorized,
                    message: "goal_secret does not match".to_string(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            } else {
                ActionGetResultResponse {
                    goal_id,
                    status: state.status,
                    accepted_stamp_ns: state.accepted_stamp_ns,
                    started_stamp_ns: state.started_stamp_ns,
                    finished_stamp_ns: state.finished_stamp_ns,
                    last_feedback_seq: state.last_feedback_seq,
                    feedback_snapshot: state.feedback_snapshot.clone(),
                    result: state.result_payload.clone(),
                    code: state.code,
                    message: state.message.clone(),
                    server_epoch: self.info.server_epoch.clone(),
                }
            }
        } else if let Some(expired) = self.find_expired_goal(&goal_id).await {
            ActionGetResultResponse {
                goal_id,
                status: expired.status,
                accepted_stamp_ns: expired.accepted_stamp_ns,
                started_stamp_ns: expired.started_stamp_ns,
                finished_stamp_ns: expired.finished_stamp_ns,
                last_feedback_seq: expired.last_feedback_seq,
                feedback_snapshot: Vec::new(),
                result: None,
                code: ActionReplyCode::GoalExpired,
                message: "goal result expired; use a new goal_id".to_string(),
                server_epoch: self.info.server_epoch.clone(),
            }
        } else {
            ActionGetResultResponse {
                goal_id,
                status: ActionStatus::Unknown,
                accepted_stamp_ns: 0,
                started_stamp_ns: 0,
                finished_stamp_ns: 0,
                last_feedback_seq: 0,
                feedback_snapshot: Vec::new(),
                result: None,
                code: ActionReplyCode::NotFound,
                message: "goal_id not found".to_string(),
                server_epoch: self.info.server_epoch.clone(),
            }
        };

        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `encode_get_result_response` function pointer; same contract as
        // the other codec calls in this module.
        let encoded = unsafe { self.encode_get_result_response(&response) };
        if let Some(start) = query_start {
            crate::trace::record_stage_elapsed_under(
                crate::trace::StageId::ActionGetResultQuery,
                start,
                self.ev_get_result_query,
                goal_root.as_ref(),
            );
        }
        encoded
    }

    fn spawn_goal_handler(self: &Arc<Self>, goal: Arc<FfiRawGoalRecord>) {
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let ctx_handle = alloc_handle();
            registry().action_contexts.lock().insert(
                ctx_handle,
                Arc::new(FfiActionContext {
                    server: Arc::downgrade(&server),
                    goal: Arc::clone(&goal),
                }),
            );
            server.context_handles.lock().await.push(ctx_handle);

            let ctx = FfiActionContext {
                server: Arc::downgrade(&server),
                goal: Arc::clone(&goal),
            };
            // Held by the dispatch thread until the handler returns; see
            // `execute_slots`. Closed only when the server is freed.
            let Ok(slot) = Arc::clone(&server.execute_slots).acquire_owned().await else {
                let _ = ctx
                    .abort(
                        ActionReplyCode::ContextDropped,
                        "action server stopped before the goal was dispatched",
                    )
                    .await;
                registry().action_contexts.lock().remove(&ctx_handle);
                server
                    .context_handles
                    .lock()
                    .await
                    .retain(|handle| *handle != ctx_handle);
                return;
            };
            if let Err(err) = ctx.mark_executing().await {
                let _ = ctx
                    .abort(ActionReplyCode::HandlerError, &err.to_string())
                    .await;
                registry().action_contexts.lock().remove(&ctx_handle);
                server
                    .context_handles
                    .lock()
                    .await
                    .retain(|handle| *handle != ctx_handle);
                return;
            }

            let request_payload = goal.request_payload.clone();
            let execute = server.callbacks.execute;
            let user_data = server.callbacks.user_data;
            let gate = Arc::clone(&server.callback_gate);
            // Pre-mint the user-callback span identity and adopt
            // it INSIDE the dispatch thread, so C/C++ AORTA_TRACE_SPAN
            // spans opened in the execute callback nest beneath it (the FFI
            // mirror of the service direct-dispatch path; the async core
            // ActionServer cannot adopt, but this dispatch is synchronous on
            // its own thread).
            let cb_link = goal.goal_root.map(|r| crate::trace::SpanLink {
                trace_id: r.trace_id,
                span_id: crate::trace::mint_span_id(),
                parent_span_id: r.span_id,
            });
            let cb_start = cb_link.is_some().then(crate::time::monotonic_ns);
            // Each goal's execute callback gets its own thread: a
            // synchronous handler may legally block until its goal is
            // terminal, so it must not occupy a worker of the shared,
            // CPU-sized offload pool (one long goal per worker would starve
            // every other offloaded job, including other goals' dispatch).
            // `execute_slots` caps these threads at `max_concurrency`, even
            // when handlers keep running after their goal is terminal. The
            // callback runs
            // off-runtime and MAY make blocking aorta FFI calls. Panics are
            // caught here to preserve the JoinError-shaped handling below.
            let run_result =
                crate::flow::offload::offload_dedicated(ACTION_EXECUTE_THREAD_NAME, move || {
                    let _slot = slot;
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                        // Held for the whole user call: `user_data` stays alive
                        // until it returns, even if the server is freed meanwhile.
                        let Some(_permit) = gate.enter() else {
                            return false;
                        };
                        let Some(execute) = execute else {
                            return true;
                        };
                        let mut slice = aorta_bytes_slice_t::default();
                        let view = single_slice_bytes_view(&request_payload, &mut slice);
                        let _adopted = cb_link.as_ref().map(|c| {
                            crate::trace::adopt_remote_context(&crate::trace::TraceParent {
                                trace_id: c.trace_id,
                                span_id: c.span_id,
                                flags: crate::trace::TRACEPARENT_SAMPLED,
                            })
                        });
                        // Action execute dispatch — wrap so any Rust-side panic
                        // raised in our marshaling code (above) cannot unwind out
                        // of the dispatch thread. This is one of the required
                        // guarded dispatch sites. The outer
                        // `catch_unwind` only sees panics that actually unwind —
                        // `ffi_guard_void` here closes the gap for any
                        // nested-runtime / drop-impl panic in our own code.
                        // SAFETY: dispatch site for the action-execute user
                        // callback. `view` is a freshly-built single-slice view
                        // pointing at the borrowed `request_payload` whose owner
                        // (this closure) outlives the dispatch. `ctx_handle` is a
                        // registry key, `user_data` is the caller's opaque pointer
                        // shipped through the FFI on action server registration.
                        // The `ffi_guard_void` wrapping is documented above.
                        ffi_guard_void(|| unsafe {
                            execute(ptr::addr_of!(view), ctx_handle, user_data as *mut c_void);
                        });
                        true
                    }))
                })
                .await;
            if let (Some(link), Some(start)) = (cb_link, cb_start) {
                crate::trace::record_stage_elapsed_linked(
                    crate::trace::StageId::ActionUserCallback,
                    start,
                    server.ev_user_callback,
                    link,
                );
            }

            match run_result {
                Ok(false) => {
                    let _ = ctx
                        .abort(
                            ActionReplyCode::ContextDropped,
                            "action server stopped before the goal was dispatched",
                        )
                        .await;
                }
                Ok(true) => {
                    if !ctx.is_terminal().await {
                        let _ = ctx
                            .abort(
                                ActionReplyCode::HandlerCompletedWithoutResult,
                                "handler returned without setting terminal state",
                            )
                            .await;
                    }
                }
                Err(_) => {
                    let _ = ctx
                        .abort(ActionReplyCode::HandlerPanic, "handler panicked")
                        .await;
                }
            }

            registry().action_contexts.lock().remove(&ctx_handle);
            server
                .context_handles
                .lock()
                .await
                .retain(|handle| *handle != ctx_handle);
        });
    }

    /// Hands the user's `cancel_requested` callback to the offload pool
    /// (High priority) without waiting for it. Skipped once the server is
    /// being freed (the callback gate is closed).
    fn dispatch_cancel_requested(&self, goal_id: &str) {
        let Some(cancel_requested) = self.callbacks.cancel_requested else {
            return;
        };
        let gate = Arc::clone(&self.callback_gate);
        let user_data = self.callbacks.user_data;
        let goal_id = goal_id.to_string();
        tokio::spawn(crate::flow::offload::offload_with_priority(
            move || {
                let Some(_permit) = gate.enter() else {
                    return;
                };
                let view = aorta_string_view_t {
                    data: goal_id.as_ptr(),
                    len: goal_id.len(),
                };
                // SAFETY: dispatch site for the user's cancel-requested
                // callback. `view` borrows `goal_id`, owned by this closure
                // for the duration of the call; `user_data` is the caller's
                // opaque pointer, kept alive by the held gate permit.
                ffi_guard_void(|| unsafe { cancel_requested(view, user_data as *mut c_void) });
            },
            crate::event_queue::EventPriority::High,
        ));
    }

    async fn find_expired_goal(&self, goal_id: &str) -> Option<FfiRawExpiredGoalRecord> {
        self.expired_goals.lock().await.get(goal_id).cloned()
    }

    async fn current_active_goal_count(&self) -> usize {
        let goal_records = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };

        let mut count = 0usize;
        for goal in goal_records {
            if !goal.state.lock().await.status.is_terminal() {
                count += 1;
            }
        }
        count
    }

    fn spawn_terminal_state_cleanup(self: &Arc<Self>, goal: Arc<FfiRawGoalRecord>) {
        let goal_id = goal.goal_id.clone();
        let result_ttl = self.options.result_ttl;
        let dedup_ttl = self.options.dedup_ttl;
        let weak_server = Arc::downgrade(self);

        tokio::spawn(async move {
            tokio::time::sleep(result_ttl).await;

            let Some(server) = weak_server.upgrade() else {
                return;
            };
            let Some(goal) = server.remove_goal(&goal_id).await else {
                return;
            };

            let expired = goal.expired_record().await;
            server
                .expired_goals
                .lock()
                .await
                .insert(goal_id.clone(), expired);

            let _ = goal.expire_state(server.node.session()).await;

            let dedup_delay = dedup_ttl.saturating_sub(result_ttl);
            if dedup_delay.is_zero() {
                server.expired_goals.lock().await.remove(&goal_id);
                return;
            }

            tokio::time::sleep(dedup_delay).await;
            if let Some(server) = weak_server.upgrade() {
                server.expired_goals.lock().await.remove(&goal_id);
            }
        });
    }

    async fn publish_feedback_payload(
        &self,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> AortaResult<()> {
        let builder = self.node.session().inner().put(&self.feedback_key, payload);
        match attachment {
            Some(attachment) => builder.attachment(attachment).await?,
            None => builder.await?,
        }
        Ok(())
    }

    async fn publish_status_payload(
        &self,
        goal: &FfiRawGoalRecord,
        payload: Vec<u8>,
    ) -> AortaResult<()> {
        {
            let mut guard = goal.status_payload.write().await;
            *guard = Some(crate::ZBytes::from(payload.clone()));
        }
        self.node
            .session()
            .inner()
            .put(&goal.status_key, payload)
            .await?;
        Ok(())
    }

    async fn remove_goal(&self, goal_id: &str) -> Option<Arc<FfiRawGoalRecord>> {
        self.goals.lock().await.remove(goal_id)
    }

    async fn request_stop(self: &Arc<Self>) {
        self.request_stop_nowait();

        let goals = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };
        for goal in goals {
            let is_terminal = goal.state.lock().await.status.is_terminal();
            let ctx = FfiActionContext {
                server: Arc::downgrade(self),
                goal: Arc::clone(&goal),
            };
            if !is_terminal {
                let _ = ctx
                    .abort(
                        ActionReplyCode::ContextDropped,
                        "action execution context dropped during server shutdown",
                    )
                    .await;
            }
            goal.request_stop().await;
        }
    }

    fn request_stop_nowait(&self) {
        if let Ok(mut guard) = self.info_payload.try_write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.info_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.send_goal_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.cancel_goal_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
        if let Ok(mut guard) = self.get_result_stop_tx.try_lock() {
            if let Some(stop_tx) = guard.take() {
                let _ = stop_tx.send(());
            }
        }
    }

    async fn join(self: &Arc<Self>, timeout_duration: Duration) {
        if let Some(task) = self.info_task.lock().await.take() {
            let _ = tokio::time::timeout(timeout_duration, task).await;
        }
        if let Some(task) = self.send_goal_task.lock().await.take() {
            let _ = tokio::time::timeout(timeout_duration, task).await;
        }
        if let Some(task) = self.cancel_goal_task.lock().await.take() {
            let _ = tokio::time::timeout(timeout_duration, task).await;
        }
        if let Some(task) = self.get_result_task.lock().await.take() {
            let _ = tokio::time::timeout(timeout_duration, task).await;
        }

        let goals = {
            let goals = self.goals.lock().await;
            goals.values().cloned().collect::<Vec<_>>()
        };
        for goal in goals {
            if let Some(task) = goal.status_task.lock().await.take() {
                let _ = tokio::time::timeout(timeout_duration, task).await;
            }
        }

        let handles = self
            .context_handles
            .lock()
            .await
            .drain(..)
            .collect::<Vec<_>>();
        let mut contexts = registry().action_contexts.lock();
        for handle in handles {
            contexts.remove(&handle);
        }
    }
}

impl FfiRawGoalRecord {
    async fn start_status_queryable(&self, session: crate::Session) -> AortaResult<()> {
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = spawn_single_key_snapshot_queryable(
            session,
            self.status_key.clone(),
            Arc::clone(&self.status_payload),
            stop_rx,
        )
        .await?;
        *self.status_stop_tx.lock().await = Some(stop_tx);
        *self.status_task.lock().await = Some(task);
        Ok(())
    }

    async fn public_status(&self, server: &FfiRawActionServer) -> ActionPublicStatus {
        let state = self.state.lock().await;
        ActionPublicStatus {
            goal_id: self.goal_id.clone(),
            status: state.status,
            code: state.code,
            accepted_stamp_ns: state.accepted_stamp_ns,
            started_stamp_ns: state.started_stamp_ns,
            finished_stamp_ns: state.finished_stamp_ns,
            last_feedback_seq: state.last_feedback_seq,
            server_epoch: server.info.server_epoch.clone(),
        }
    }

    async fn publish_status(&self, server: &FfiRawActionServer) -> AortaResult<()> {
        let status = self.public_status(server).await;
        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `encode_public_status` function pointer; borrows `&status`,
        // returns owned `Vec<u8>`.
        let payload = unsafe { server.encode_public_status(&status) }?;
        server.publish_status_payload(self, payload).await
    }

    async fn request_stop(&self) {
        if let Some(stop_tx) = self.status_stop_tx.lock().await.take() {
            let _ = stop_tx.send(());
        }
        let mut payload = self.status_payload.write().await;
        *payload = None;
    }

    async fn expired_record(&self) -> FfiRawExpiredGoalRecord {
        let state = self.state.lock().await;
        FfiRawExpiredGoalRecord {
            payload_hash: self.request_payload_hash.clone(),
            status: state.status,
            accepted_stamp_ns: state.accepted_stamp_ns,
            started_stamp_ns: state.started_stamp_ns,
            finished_stamp_ns: state.finished_stamp_ns,
            last_feedback_seq: state.last_feedback_seq,
        }
    }

    async fn expire_state(&self, session: &crate::Session) -> AortaResult<()> {
        session.inner().delete(&self.status_key).await?;
        self.request_stop().await;
        Ok(())
    }
}

impl FfiActionContext {
    async fn mark_executing(&self) -> AortaResult<()> {
        let Some(server) = self.server.upgrade() else {
            return Err(AortaError::internal("action server no longer exists"));
        };
        let (should_publish, accept_mono) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                (false, None)
            } else if state.started_stamp_ns == 0 {
                if state.status == ActionStatus::Accepted {
                    state.status = ActionStatus::Executing;
                }
                state.started_stamp_ns = unix_time_ns();
                (true, state.accepted_mono_ns)
            } else {
                (false, None)
            }
        };
        if should_publish {
            // Tracing: accept -> execution started (monotonic, gated; the clock
            // is read only if tracing is still on).
            if let Some(accept_mono) = accept_mono {
                crate::trace::record_stage_elapsed_under(
                    crate::trace::StageId::ActionAcceptedToExecutionStarted,
                    accept_mono,
                    server.ev_accepted_to_execution_started,
                    self.goal.goal_root.as_ref(),
                );
            }
            self.goal.publish_status(&server).await?;
        }
        Ok(())
    }

    async fn publish_feedback(&self, feedback_data: &[u8], message: &str) -> AortaResult<()> {
        let Some(server) = self.server.upgrade() else {
            return Err(AortaError::internal("action server no longer exists"));
        };
        let (seq, status, feedback_interval_ns) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                return Ok(());
            }
            state.last_feedback_seq = state.last_feedback_seq.saturating_add(1);
            state.feedback_snapshot = feedback_data.to_vec();
            // Tracing: consecutive-feedback interval (monotonic, gated). A
            // disabled window resets the anchor so re-enabling does not report
            // a bogus interval spanning it (mirrors the core ActionServer).
            let interval = if crate::trace::trace_enabled() {
                let now = crate::time::monotonic_ns();
                let prev = state.last_feedback_mono_ns;
                state.last_feedback_mono_ns = now;
                (prev != 0).then(|| now.saturating_sub(prev))
            } else {
                state.last_feedback_mono_ns = 0;
                None
            };
            (state.last_feedback_seq, state.status, interval)
        };
        if let Some(interval) = feedback_interval_ns {
            crate::trace::record_stage(crate::trace::StageId::ActionFeedbackInterval, interval);
        }

        // One small slice per feedback publish under the
        // goal root, span id PRE-MINTED so the feedback attachment carries it
        // as the receive-marker's parent (pub → recv = the transit hop).
        let fb_link = match (self.goal.goal_root.as_ref(), crate::trace::trace_enabled()) {
            (Some(root), true) => Some(crate::trace::SpanLink {
                trace_id: root.trace_id,
                span_id: crate::trace::mint_span_id(),
                parent_span_id: root.span_id,
            }),
            _ => None,
        };
        let fb_start = fb_link.is_some().then(crate::time::monotonic_ns);
        let fb_attachment = fb_link.and_then(|l| {
            crate::attachment::encode(&crate::attachment::AttachmentParts {
                schema_hash: None,
                traceparent: Some(crate::trace::TraceParent {
                    trace_id: l.trace_id,
                    span_id: l.span_id,
                    flags: crate::trace::TRACEPARENT_SAMPLED,
                }),
                reply_busy_ns: None,
                deadline_budget_ns: None,
            })
        });
        let stamp_ns = unix_time_ns();
        // SAFETY: invokes the codec `unsafe fn` over the FFI-supplied
        // `encode_feedback` function pointer; borrows the goal id, seq,
        // stamp, status, feedback bytes, and message; returns owned bytes.
        let feedback_payload = unsafe {
            server.encode_feedback(
                &self.goal.goal_id,
                seq,
                stamp_ns,
                status,
                feedback_data,
                message,
            )?
        };
        server
            .publish_feedback_payload(feedback_payload, fb_attachment)
            .await?;
        let result = self.goal.publish_status(&server).await;
        if let (Some(link), Some(start)) = (fb_link, fb_start) {
            crate::trace::emit_stage_span_at(
                server.ev_feedback_pub,
                start,
                crate::time::monotonic_ns(),
                link,
            );
        }
        result
    }

    async fn finish_terminal(
        &self,
        status: ActionStatus,
        code: ActionReplyCode,
        message: &str,
        result_payload: Option<Vec<u8>>,
    ) -> AortaResult<()> {
        let Some(server) = self.server.upgrade() else {
            return Err(AortaError::internal("action server no longer exists"));
        };
        let (became_terminal, accept_mono) = {
            let mut state = self.goal.state.lock().await;
            if state.status.is_terminal() {
                (false, None)
            } else {
                if state.started_stamp_ns == 0 {
                    state.started_stamp_ns = unix_time_ns();
                }
                state.status = status;
                state.code = code;
                state.message = message.to_string();
                state.finished_stamp_ns = unix_time_ns();
                state.result_payload = result_payload;
                (true, state.accepted_mono_ns)
            }
        };
        if became_terminal {
            // Tracing: accept -> terminal under the pre-minted goal ROOT (one
            // cross-process link to the caller; monotonic, gated).
            if let Some(accept_mono) = accept_mono {
                match self.goal.goal_root {
                    Some(root) => crate::trace::record_stage_elapsed_linked(
                        crate::trace::StageId::ActionAcceptedToFinished,
                        accept_mono,
                        server.ev_accepted_to_finished,
                        root,
                    ),
                    None => crate::trace::record_stage_elapsed_as(
                        crate::trace::StageId::ActionAcceptedToFinished,
                        accept_mono,
                        server.ev_accepted_to_finished,
                    ),
                }
            }
            // Cancel request accepted -> Canceled terminal,
            // child of the goal root.
            if status == ActionStatus::Canceled {
                let cancel_mono = self.goal.cancel_requested_mono_ns.load(Ordering::Relaxed);
                if cancel_mono != 0 {
                    crate::trace::record_stage_elapsed_under(
                        crate::trace::StageId::ActionCancelRequestedToCanceled,
                        cancel_mono,
                        server.ev_cancel_to_canceled,
                        self.goal.goal_root.as_ref(),
                    );
                }
            }
            server.active_goal_count.fetch_sub(1, Ordering::AcqRel);
            self.goal.publish_status(&server).await?;
            server.spawn_terminal_state_cleanup(Arc::clone(&self.goal));
        }
        Ok(())
    }

    async fn abort(&self, code: ActionReplyCode, message: &str) -> AortaResult<()> {
        self.finish_terminal(ActionStatus::Aborted, code, message, None)
            .await
    }

    async fn cancel(&self, code: ActionReplyCode, message: &str) -> AortaResult<()> {
        self.finish_terminal(ActionStatus::Canceled, code, message, None)
            .await
    }

    async fn succeed(&self, result_payload: Vec<u8>) -> AortaResult<()> {
        self.finish_terminal(
            ActionStatus::Succeeded,
            ActionReplyCode::Succeeded,
            "",
            Some(result_payload),
        )
        .await
    }

    async fn is_terminal(&self) -> bool {
        self.goal.state.lock().await.status.is_terminal()
    }
}

fn ffi_action_payload_hash(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    format!("sha256:{:x}", hasher.finalize())
}

struct ForeignOwnedSlice {
    data: *const u8,
    len: usize,
    release: aorta_release_bytes_fn,
    release_ctx: usize,
}

// SAFETY: The C++ caller guarantees that the data pointer remains valid and
// exclusively owned by Rust from the moment `aorta_publisher_publish_owned`
// is called until `release` is invoked. No other thread accesses the buffer
// during that window, so sending between threads is safe.
unsafe impl Send for ForeignOwnedSlice {}

impl AsRef<[u8]> for ForeignOwnedSlice {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: The C++ caller guarantees `data` points to a valid,
        // readable buffer of at least `len` bytes that remains alive until
        // `release` is called.
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

impl Drop for ForeignOwnedSlice {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            // SAFETY: Invokes the C++ release callback with the original
            // context, pointer, and length. This is the ownership hand-back
            // contract: after this call the C++ side may free the buffer.
            unsafe { release(self.release_ctx as *mut c_void, self.data, self.len) };
        }
    }
}

unsafe fn zbytes_from_ffi_view(
    view: *const aorta_bytes_view_t,
    field: &str,
) -> AortaResult<crate::ZBytes> {
    if view.is_null() {
        return Err(AortaError::invalid_argument(format!("{} is null", field)));
    }

    let view = &*view;
    if view.slice_count == 0 {
        if view.total_len != 0 {
            return Err(AortaError::invalid_argument(format!(
                "{} has zero slices but non-zero total_len ({})",
                field, view.total_len
            )));
        }
        return Ok(crate::ZBytes::new());
    }

    if view.slices.is_null() {
        return Err(AortaError::invalid_argument(format!(
            "{} slices pointer is null",
            field
        )));
    }

    let slices = std::slice::from_raw_parts(view.slices, view.slice_count);
    let mut writer = crate::ZBytes::writer();
    let mut total_len = 0usize;

    for (index, slice) in slices.iter().enumerate() {
        if slice.len > 0 && slice.data.is_null() {
            return Err(AortaError::invalid_argument(format!(
                "{} slice[{}] data pointer is null",
                field, index
            )));
        }

        let bytes = if slice.len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(slice.data, slice.len)
        };
        total_len = total_len.checked_add(slice.len).ok_or_else(|| {
            AortaError::invalid_argument(format!("{} total length overflow", field))
        })?;
        writer.write_all(bytes).map_err(|err| {
            AortaError::internal(format!("failed to convert {} into payload: {}", field, err))
        })?;
    }

    if total_len != view.total_len {
        return Err(AortaError::invalid_argument(format!(
            "{} total_len mismatch: expected {}, got {}",
            field, view.total_len, total_len
        )));
    }

    Ok(writer.finish())
}

unsafe fn zbytes_from_owned_ffi_view(
    view: *const aorta_owned_bytes_view_t,
    field: &str,
) -> AortaResult<crate::ZBytes> {
    if view.is_null() {
        return Err(AortaError::invalid_argument(format!("{} is null", field)));
    }

    let view = &*view;
    if view.slice_count == 0 {
        if view.total_len != 0 {
            return Err(AortaError::invalid_argument(format!(
                "{} has zero slices but non-zero total_len ({})",
                field, view.total_len
            )));
        }
        return Ok(crate::ZBytes::new());
    }

    if view.slices.is_null() {
        return Err(AortaError::invalid_argument(format!(
            "{} slices pointer is null",
            field
        )));
    }

    let slices = std::slice::from_raw_parts(view.slices, view.slice_count);

    // Fast path: single slice — skip ZBytes::writer() + loop overhead.
    if view.slice_count == 1 {
        let slice = &slices[0];
        if slice.len != view.total_len {
            return Err(AortaError::invalid_argument(format!(
                "{} total_len mismatch: expected {}, got {}",
                field, view.total_len, slice.len
            )));
        }
        if slice.len > 0 && slice.data.is_null() {
            return Err(AortaError::invalid_argument(format!(
                "{} slice[0] data pointer is null",
                field
            )));
        }
        if let Some(release) = slice.release {
            let owner = ForeignOwnedSlice {
                data: slice.data,
                len: slice.len,
                release: Some(release),
                release_ctx: slice.release_ctx as usize,
            };
            return Ok(crate::ZBytes::from(Bytes::from_owner(owner)));
        } else {
            let bytes = if slice.len == 0 {
                &[] as &[u8]
            } else {
                std::slice::from_raw_parts(slice.data, slice.len)
            };
            return Ok(crate::ZBytes::from(bytes.to_vec()));
        }
    }

    let mut writer = crate::ZBytes::writer();
    let mut total_len = 0usize;

    for (index, slice) in slices.iter().enumerate() {
        if slice.len > 0 && slice.data.is_null() {
            return Err(AortaError::invalid_argument(format!(
                "{} slice[{}] data pointer is null",
                field, index
            )));
        }
        total_len = total_len.checked_add(slice.len).ok_or_else(|| {
            AortaError::invalid_argument(format!("{} total length overflow", field))
        })?;

        if let Some(release) = slice.release {
            let owner = ForeignOwnedSlice {
                data: slice.data,
                len: slice.len,
                release: Some(release),
                release_ctx: slice.release_ctx as usize,
            };
            writer.append(crate::ZBytes::from(Bytes::from_owner(owner)));
        } else {
            let bytes = if slice.len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(slice.data, slice.len)
            };
            writer.write_all(bytes).map_err(|err| {
                AortaError::internal(format!("failed to convert {} into payload: {}", field, err))
            })?;
        }
    }

    if total_len != view.total_len {
        return Err(AortaError::invalid_argument(format!(
            "{} total_len mismatch: expected {}, got {}",
            field, view.total_len, total_len
        )));
    }

    Ok(writer.finish())
}

unsafe fn cstr_to_string(ptr: *const c_char, field: &str) -> AortaResult<String> {
    if ptr.is_null() {
        return Err(AortaError::invalid_argument(format!("{} is null", field)));
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|value| value.to_string())
        .map_err(|_| AortaError::invalid_argument(format!("{} is not valid utf-8", field)))
}

unsafe fn cstr_to_string_optional(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr)
        .to_str()
        .ok()
        .map(|value| value.to_string())
}

unsafe fn string_kv_map_from_ffi(
    entries: *const aorta_string_kv_t,
    len: usize,
    field: &str,
) -> AortaResult<BTreeMap<String, String>> {
    if entries.is_null() {
        if len == 0 {
            return Ok(BTreeMap::new());
        }
        return Err(AortaError::invalid_argument(format!(
            "{} is null but len is non-zero",
            field
        )));
    }

    let items = std::slice::from_raw_parts(entries, len);
    let mut values = BTreeMap::new();
    for (index, item) in items.iter().enumerate() {
        let key = cstr_to_string(item.key, &format!("{field}[{index}].key"))?;
        let value = cstr_to_string(item.value, &format!("{field}[{index}].value"))?;
        values.insert(key, value);
    }
    Ok(values)
}

fn default_ffi_node_telemetry_metadata() -> CoreTelemetryNodeMetadata {
    CoreTelemetryNodeMetadata {
        executable: None,
        language: Some("c".to_string()),
        sdk_name: Some("aorta_c_api".to_string()),
        sdk_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        labels: Default::default(),
        build_id: None,
        container_id: None,
    }
}

unsafe fn node_options_from_ffi(opts: *const aorta_node_options_t) -> AortaResult<CoreNodeOptions> {
    let mut telemetry = default_ffi_node_telemetry_metadata();
    let Some(opts) = opts.as_ref() else {
        return Ok(CoreNodeOptions { telemetry });
    };
    let Some(telemetry_opts) = opts.telemetry.as_ref() else {
        return Ok(CoreNodeOptions { telemetry });
    };

    if let Some(value) = cstr_to_string_optional(telemetry_opts.executable) {
        telemetry.executable = Some(value);
    }
    if let Some(value) = cstr_to_string_optional(telemetry_opts.language) {
        telemetry.language = Some(value);
    }
    if let Some(value) = cstr_to_string_optional(telemetry_opts.sdk_name) {
        telemetry.sdk_name = Some(value);
    }
    if let Some(value) = cstr_to_string_optional(telemetry_opts.sdk_version) {
        telemetry.sdk_version = Some(value);
    }
    if let Some(value) = cstr_to_string_optional(telemetry_opts.build_id) {
        telemetry.build_id = Some(value);
    }
    if let Some(value) = cstr_to_string_optional(telemetry_opts.container_id) {
        telemetry.container_id = Some(value);
    }
    telemetry.labels = string_kv_map_from_ffi(
        telemetry_opts.labels,
        telemetry_opts.labels_len,
        "telemetry.labels",
    )?;

    Ok(CoreNodeOptions { telemetry })
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_string_view_t {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_string_kv_t {
    pub key: *const c_char,
    pub value: *const c_char,
}

fn string_view_from_str(value: &str) -> aorta_string_view_t {
    aorta_string_view_t {
        data: value.as_ptr(),
        len: value.len(),
    }
}

fn string_view_from_optional(value: Option<&str>) -> aorta_string_view_t {
    match value {
        Some(content) => string_view_from_str(content),
        None => aorta_string_view_t::default(),
    }
}

unsafe fn string_view_to_string(view: aorta_string_view_t, field: &str) -> AortaResult<String> {
    if view.data.is_null() {
        if view.len == 0 {
            return Ok(String::new());
        }
        return Err(AortaError::invalid_argument(format!(
            "{} data is null but len is non-zero",
            field
        )));
    }
    let bytes = std::slice::from_raw_parts(view.data, view.len);
    std::str::from_utf8(bytes)
        .map(|value| value.to_string())
        .map_err(|_| AortaError::invalid_argument(format!("{} is not valid utf-8", field)))
}

unsafe fn string_view_to_string_optional(
    view: aorta_string_view_t,
    field: &str,
) -> AortaResult<Option<String>> {
    if view.data.is_null() {
        if view.len == 0 {
            return Ok(None);
        }
        return Err(AortaError::invalid_argument(format!(
            "{} data is null but len is non-zero",
            field
        )));
    }
    let bytes = std::slice::from_raw_parts(view.data, view.len);
    let value = std::str::from_utf8(bytes)
        .map_err(|_| AortaError::invalid_argument(format!("{} is not valid utf-8", field)))?;
    Ok(Some(value.to_string()))
}

#[repr(C)]
pub struct aorta_schema_hash_view_t {
    pub hex: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_bytes_slice_t {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_bytes_view_t {
    pub slices: *const aorta_bytes_slice_t,
    pub slice_count: usize,
    pub total_len: usize,
}

pub type aorta_release_bytes_fn = Option<unsafe extern "C" fn(*mut c_void, *const u8, usize)>;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_owned_bytes_slice_t {
    pub data: *const u8,
    pub len: usize,
    pub release: aorta_release_bytes_fn,
    pub release_ctx: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_owned_bytes_view_t {
    pub slices: *const aorta_owned_bytes_slice_t,
    pub slice_count: usize,
    pub total_len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_owned_bytes_t {
    pub view: aorta_bytes_view_t,
    pub internal: *mut c_void,
}

#[repr(C)]
pub struct aorta_msg_context_t {
    pub topic: aorta_string_view_t,
    pub receive_stamp_ns: i64,
    pub priority: i32,
    pub publisher_node: aorta_string_view_t,
    pub sequence: u64,
}

/// A sample pulled from a pull-mode (`InlinePull`) subscriber via
/// `aorta_sub_recv` / `aorta_sub_try_recv`.
///
/// `payload` and the string views inside `context` borrow memory owned by
/// `internal` (a boxed backing that keeps the `ZBytes` and context strings
/// alive). The caller MUST release it with `aorta_pull_sample_free` exactly
/// once; the views are valid until then.
///
/// (Named `aorta_pull_sample_t` to avoid colliding with the recording-side
/// `aorta_sample_t` handle in `c_api/sample.rs`.)
#[repr(C)]
pub struct aorta_pull_sample_t {
    pub payload: aorta_bytes_view_t,
    pub context: aorta_msg_context_t,
    /// Opaque owner of the backing storage; freed by `aorta_pull_sample_free`.
    pub internal: *mut c_void,
}

/// Backing storage for an [`aorta_pull_sample_t`]: owns the payload bytes, the
/// borrowed-slice storage the `payload` view points at (`single` inline for
/// the near-universal contiguous case, `slices` only allocated for fragmented
/// payloads — same shape as `bytes_view_from_zbytes_borrowed`, so a pull on
/// the hot drain path costs one Box, not Box + Vec), and the `MessageContext`
/// whose strings the `context` views point at.
struct PullSampleBacking {
    payload: crate::ZBytes,
    single: aorta_bytes_slice_t,
    slices: Vec<aorta_bytes_slice_t>,
    context: MessageContext,
}

fn sample_from_pull(sample: crate::subscriber::PullSample) -> aorta_pull_sample_t {
    let crate::subscriber::PullSample { payload, context } = sample;
    let mut backing = Box::new(PullSampleBacking {
        payload,
        single: aorta_bytes_slice_t::default(),
        slices: Vec::new(),
        context,
    });
    // Build the view against the Box-owned storage BEFORE into_raw — the heap
    // allocation (and thus `single`'s and the Vec buffer's addresses) is
    // stable across into_raw.
    let b = &mut *backing;
    let payload_view = bytes_view_from_zbytes_borrowed(&b.payload, &mut b.single, &mut b.slices);
    let ctx = build_msg_context(&backing.context);
    aorta_pull_sample_t {
        payload: payload_view,
        context: ctx,
        internal: Box::into_raw(backing) as *mut c_void,
    }
}

/// Release the backing storage of a sample produced by `aorta_sub_recv` /
/// `aorta_sub_try_recv`. Idempotent: nulls `internal` so a double free is a
/// no-op. After this returns, the sample's `payload`/`context` views dangle.
///
/// # Safety
/// `sample` must be null or point at a writable `aorta_pull_sample_t` previously
/// filled by `aorta_sub_recv`/`aorta_sub_try_recv` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_pull_sample_free(sample: *mut aorta_pull_sample_t) {
    // ffi_guard_void like every other *_free: nothing here panics today, but
    // this runs on every pull-subscriber teardown and a future Drop impl in
    // the backing must not unwind across the FFI boundary.
    crate::ffi::guard::ffi_guard_void(|| {
        if sample.is_null() {
            return;
        }
        // SAFETY: caller contract — `sample` points at a writable `aorta_pull_sample_t`.
        let sample = unsafe { &mut *sample };
        if !sample.internal.is_null() {
            // SAFETY: `internal` was produced by `Box::into_raw` in `sample_from_pull`.
            drop(unsafe { Box::from_raw(sample.internal as *mut PullSampleBacking) });
            sample.internal = ptr::null_mut();
        }
    });
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct aorta_publisher_stats_t {
    pub publish_count: u64,
    pub oversize_count: u64,
    pub last_oversize_size: usize,
    pub current_capacity: usize,
    pub max_capacity_seen: usize,
    pub last_publish_ns: u64,
    pub send_error_count: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct aorta_service_stats_t {
    pub request_received_count: u64,
    pub request_dispatched_count: u64,
    pub reply_ok_count: u64,
    pub reply_error_count: u64,
    pub reply_send_failure_count: u64,
    pub request_dropped_count: u64,
    pub query_receive_failure_count: u64,
    pub task_exit_count: u64,
    pub in_flight_requests: u64,
    pub last_request_ns: u64,
    pub last_reply_ns: u64,
    /// Deadline-expired queue drops — a SUBSET of `request_dropped_count`
    /// (appended for ABI stability; core has carried it all along, the
    /// conversion silently dropped it — caught by the Flow TCK).
    pub request_dropped_deadline_count: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct aorta_client_stats_t {
    pub call_count: u64,
    pub success_count: u64,
    pub timeout_count: u64,
    pub concurrent_conflict_count: u64,
    pub error_count: u64,
    pub retry_count: u64,
    pub last_call_started_ns: u64,
    pub last_call_finished_ns: u64,
    pub last_call_latency_ns: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct aorta_subscriber_runtime_stats_t {
    pub duplicate_drop_count: u64,
    pub out_of_order_count: u64,
    pub out_of_order_drop_count: u64,
    pub executor_drop_count: u64,
    pub local_drop_count: u64,
    pub overflow_relay_enqueue_count: u64,
    pub overflow_relay_drop_count: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_schema_attach_policy_t {
    AORTA_SCHEMA_ATTACH_INHERIT = 0,
    AORTA_SCHEMA_ATTACH_ALWAYS = 1,
    AORTA_SCHEMA_ATTACH_NEVER = 2,
}

#[repr(C)]
pub struct aorta_pub_options_t {
    pub schema_attach_policy: aorta_schema_attach_policy_t,
    /// Per-publisher congestion-control OVERRIDE as a raw wire byte — one of the
    /// `AORTA_PUB_CONGESTION_*` values (`0` = inherit `qos.congestion_control`,
    /// `1` = Drop, `2` = Block). Validated in `publisher_options_from_ffi`; `0`
    /// (the zero-initialized default) inherits the QoS value rather than forcing
    /// Drop. Note this is a *different* encoding from `aorta_qos_t.congestion_control`.
    pub congestion_control: u8,
    pub schema_bfbs: *const u8,
    pub schema_bfbs_len: usize,
    pub max_message_size: usize,
    /// Optional fully-qualified root table name matching `schema_bfbs`.
    /// Nullable; when non-null, this must point to a NUL-terminated UTF-8
    /// string borrowed for the duration of the call.
    pub schema_root_type: *const c_char,
}

#[repr(C)]
pub struct aorta_node_telemetry_options_t {
    pub executable: *const c_char,
    pub language: *const c_char,
    pub sdk_name: *const c_char,
    pub sdk_version: *const c_char,
    pub build_id: *const c_char,
    pub container_id: *const c_char,
    pub labels: *const aorta_string_kv_t,
    pub labels_len: usize,
}

#[repr(C)]
pub struct aorta_node_options_t {
    pub telemetry: *const aorta_node_telemetry_options_t,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_callback_group_t {
    AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE = 0,
    AORTA_CALLBACK_GROUP_REENTRANT = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_sub_overflow_policy_t {
    AORTA_SUB_OVERFLOW_DROP = 0,
    AORTA_SUB_OVERFLOW_RELAY = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_rpc_overflow_policy_t {
    AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY = 0,
    AORTA_RPC_OVERFLOW_BLOCK = 1,
}

/// Subscriber creation options.
///
/// **Layout-frozen** (this struct is mirrored on the
/// runtime-loaded Python surface, so new fields never grow it — it stays
/// byte-for-byte at its original 72-byte layout). The QoS & buffering
/// knobs live in the companion [`aorta_sub_buffering_t`],
/// passed to `aorta_subscriber_new_buffered`.
#[repr(C)]
pub struct aorta_sub_options_t {
    pub callback_group: aorta_callback_group_t,
    pub callback_group_handle: aorta_callback_group_handle_t,
    pub deadline_ns: i64,
    pub prefetch_schema_on_init: u8,
    pub enable_header_context: u8,
    pub include_topic_in_context: u8,
    pub include_receive_timestamp: u8,
    pub include_priority_in_context: u8,
    pub include_publisher_timestamp: u8,
    pub enable_dedup: u8,
    pub dedup_window: usize,
    pub enable_reorder_check: u8,
    pub drop_out_of_order: u8,
    pub overflow_policy: aorta_sub_overflow_policy_t,
    pub overflow_relay_capacity: usize,
    pub schema_bfbs: *const u8,
    pub schema_bfbs_len: usize,
}

/// QoS & buffering knobs, carried by the
/// dedicated `aorta_subscriber_new_buffered` constructor.
///
/// A new size-asserted struct behind a new symbol
/// (`aorta_sub_options_t` is layout-frozen, so the knobs must not grow it). A
/// zero-initialized struct — or a NULL pointer — reproduces today's behavior
/// exactly: `receive_depth = 0` disables the Tier A gate, `delivery_mode = 0`
/// is the executor/callback path, `receive_overflow = 0` is drop-newest.
#[repr(C)]
pub struct aorta_sub_buffering_t {
    /// Per-topic in-flight cap (Tier A) / watch capacity (Tier B-pull).
    /// `0` = disabled (today's behavior) for Executor delivery.
    pub receive_depth: u64,
    /// `0` = executor (callback), `1` = inline_pull (native watch), `2` = inline_push (deferred).
    pub delivery_mode: u32,
    /// `0` = drop_newest (Tier A), `1` = drop_oldest (requires inline_pull).
    pub receive_overflow: u32,
}

#[repr(C)]
pub struct aorta_service_options_t {
    pub schema_attach_policy: aorta_schema_attach_policy_t,
    pub callback_group: aorta_callback_group_t,
    pub callback_group_handle: aorta_callback_group_handle_t,
    pub deadline_ns: i64,
    pub service_schema_hash_hex: *const u8,
    pub service_schema_hash_len: usize,
    pub service_schema_bfbs: *const u8,
    pub service_schema_bfbs_len: usize,
    pub request_root_type: *const u8,
    pub request_root_type_len: usize,
    pub response_root_type: *const u8,
    pub response_root_type_len: usize,
    pub rpc_overflow_policy: aorta_rpc_overflow_policy_t,
    pub rpc_overflow_block_timeout_ms: u64,
}

#[repr(C)]
pub struct aorta_client_options_t {
    pub schema_attach_policy: aorta_schema_attach_policy_t,
    pub timeout_ms: u64,
    pub enforce_single_provider: u8,
    // Tri-state (0=inherit/env default, 1=false, 2=true). Placed adjacent to
    // `enforce_single_provider` so it occupies existing struct padding and the
    // C-ABI size stays 48 bytes. Default 0 => inherit core's fail-safe
    // no-retry default; zero-initialized callers stay safe.
    pub allow_retry: u8,
    pub multi_reply_check_window_ms: u64,
    pub service_schema_hash_hex: *const u8,
    pub service_schema_hash_len: usize,
}

#[repr(C)]
pub struct aorta_action_client_options_t {
    pub control_timeout_ms: u64,
    pub info_timeout_ms: u64,
}

#[repr(C)]
pub struct aorta_action_server_options_t {
    pub supports_cancel: u8,
    pub max_concurrency: usize,
    pub result_ttl_ms: u64,
    pub dedup_ttl_ms: u64,
}

pub type aorta_action_decode_goal_id_cb = Option<
    unsafe extern "C" fn(
        request: *const aorta_bytes_view_t,
        out_goal_id: *mut aorta_string_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_decode_goal_secret_cb = Option<
    unsafe extern "C" fn(
        request: *const aorta_bytes_view_t,
        out_goal_id: *mut aorta_string_view_t,
        out_goal_secret: *mut aorta_string_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_encode_send_goal_response_cb = Option<
    unsafe extern "C" fn(
        accepted: u8,
        goal_id: aorta_string_view_t,
        goal_secret: aorta_string_view_t,
        code: i16,
        status: i8,
        message: aorta_string_view_t,
        server_epoch: aorta_string_view_t,
        out: *mut aorta_owned_bytes_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_encode_cancel_goal_response_cb = Option<
    unsafe extern "C" fn(
        accepted: u8,
        goal_id: aorta_string_view_t,
        code: i16,
        status: i8,
        message: aorta_string_view_t,
        out: *mut aorta_owned_bytes_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_encode_public_status_cb = Option<
    unsafe extern "C" fn(
        goal_id: aorta_string_view_t,
        status: i8,
        code: i16,
        accepted_stamp_ns: u64,
        started_stamp_ns: u64,
        finished_stamp_ns: u64,
        last_feedback_seq: u64,
        server_epoch: aorta_string_view_t,
        out: *mut aorta_owned_bytes_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_encode_feedback_cb = Option<
    unsafe extern "C" fn(
        goal_id: aorta_string_view_t,
        seq: u64,
        stamp_ns: u64,
        status: i8,
        feedback_data: *const aorta_bytes_view_t,
        message: aorta_string_view_t,
        out: *mut aorta_owned_bytes_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_encode_get_result_response_cb = Option<
    unsafe extern "C" fn(
        goal_id: aorta_string_view_t,
        status: i8,
        accepted_stamp_ns: u64,
        started_stamp_ns: u64,
        finished_stamp_ns: u64,
        last_feedback_seq: u64,
        feedback_snapshot: *const aorta_bytes_view_t,
        result_payload: *const aorta_bytes_view_t,
        code: i16,
        message: aorta_string_view_t,
        server_epoch: aorta_string_view_t,
        out: *mut aorta_owned_bytes_view_t,
    ) -> aorta_result_t,
>;

pub type aorta_action_execute_cb = Option<
    unsafe extern "C" fn(
        request: *const aorta_bytes_view_t,
        ctx: aorta_action_context_t,
        user_data: *mut c_void,
    ),
>;

/// Notifies the server that a client's cancel request for `goal_id` was
/// accepted (the goal moved to CANCELING). Invoked at most once per goal,
/// on a Flow offload worker (blocking FFI calls are legal there, but the
/// callback should return promptly). `goal_id` is valid only for the
/// duration of the call. It may race with the goal's own terminal
/// transition, and may arrive before the execute handler has registered
/// the goal — handlers that track goals should check
/// `aorta_action_context_is_cancel_requested` after registering.
pub type aorta_action_cancel_requested_cb =
    Option<unsafe extern "C" fn(goal_id: aorta_string_view_t, user_data: *mut c_void)>;

/// Releases the `user_data` handed to a constructor that takes ownership
/// of it. Called exactly once, after which the library never touches
/// `user_data` again.
pub type aorta_user_data_release_cb = Option<unsafe extern "C" fn(user_data: *mut c_void)>;

/// Callback set for `aorta_action_server_new_with_callbacks` (input
/// struct; frozen at first release).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct aorta_action_server_callbacks_t {
    /// Required. Runs on a dedicated thread per goal (see
    /// `aorta_action_server_new_with_callbacks`).
    pub execute: aorta_action_execute_cb,
    /// Optional. Requires `supports_cancel`.
    pub cancel_requested: aorta_action_cancel_requested_cb,
    pub user_data: *mut c_void,
    /// Optional. Called exactly once, never while a callback is using
    /// `user_data`: after the server is freed and the last admitted
    /// callback has returned, or when construction fails (see
    /// `aorta_action_server_new_with_callbacks` for the timing).
    pub user_data_release: aorta_user_data_release_cb,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct aorta_action_server_codec_t {
    pub decode_send_goal_request_goal_id: aorta_action_decode_goal_id_cb,
    pub decode_cancel_goal_request: aorta_action_decode_goal_secret_cb,
    pub decode_get_result_request: aorta_action_decode_goal_secret_cb,
    pub encode_send_goal_response: aorta_action_encode_send_goal_response_cb,
    pub encode_cancel_goal_response: aorta_action_encode_cancel_goal_response_cb,
    pub encode_public_status: aorta_action_encode_public_status_cb,
    pub encode_feedback: aorta_action_encode_feedback_cb,
    pub encode_get_result_response: aorta_action_encode_get_result_response_cb,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_executor_type_t {
    AORTA_EXECUTOR_SINGLE = 0,
    AORTA_EXECUTOR_MULTI = 1,
    AORTA_EXECUTOR_CURRENT = 2,
    AORTA_EXECUTOR_ASYNC = 3,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_scheduler_mode_t {
    AORTA_SCHEDULER_FIFO = 0,
    AORTA_SCHEDULER_PRIORITY = 1,
}

#[repr(C)]
pub struct aorta_priority_scheduler_options_t {
    pub max_drain_per_cycle: u32,
    pub weights: [u8; 5],
    pub map_subscription_qos: u8,
}

#[repr(C)]
pub struct aorta_executor_admission_options_t {
    pub max_queued_events: usize,
    pub max_queued_bytes: usize,
}

#[repr(C)]
pub struct aorta_executor_options_t {
    pub scheduler_mode: aorta_scheduler_mode_t,
    pub priority: *const aorta_priority_scheduler_options_t,
    pub admission: *const aorta_executor_admission_options_t,
    pub class_policy: *const aorta_executor_class_policy_options_t,
}

#[repr(C)]
pub struct aorta_executor_admission_stats_t {
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub peak_queued_events: u64,
    pub peak_queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_queue_closed: u64,
    pub dropped_budget_events: u64,
    pub dropped_budget_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_timer_overflow_policy_t {
    AORTA_TIMER_OVERFLOW_COALESCE_LATEST = 0,
    AORTA_TIMER_OVERFLOW_BLOCK = 1,
}

#[repr(C)]
pub struct aorta_executor_class_policy_options_t {
    pub sub_overflow_policy: aorta_sub_overflow_policy_t,
    pub sub_overflow_relay_capacity: usize,
    pub rpc_overflow_policy: aorta_rpc_overflow_policy_t,
    pub rpc_overflow_block_timeout_ms: u64,
    pub timer_overflow_policy: aorta_timer_overflow_policy_t,
    pub timer_overflow_block_timeout_ms: u64,
}

#[repr(C)]
pub struct aorta_timer_options_t {
    pub callback_group: aorta_callback_group_t,
    pub callback_group_handle: aorta_callback_group_handle_t,
    pub auto_start: u8,
    pub invoke_on_missed: u8,
    pub overflow_policy: aorta_timer_overflow_policy_t,
    pub overflow_block_timeout_ms: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_priority_t {
    AORTA_PRIORITY_REALTIME = 0,
    AORTA_PRIORITY_HIGH = 1,
    AORTA_PRIORITY_NORMAL = 2,
    AORTA_PRIORITY_LOW = 3,
    AORTA_PRIORITY_BACKGROUND = 4,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_reliability_t {
    AORTA_RELIABILITY_BEST_EFFORT = 0,
    AORTA_RELIABILITY_RELIABLE = 1,
}

/// Canonical congestion-control values for `aorta_qos_t.congestion_control`.
/// Governs behavior when the publisher's egress queue is congested.
/// `DROP` matches the Core default; `BLOCK` applies backpressure for lossless
/// delivery on critical topics — previously only reachable from the Rust SDK.
///
/// These are raw `u8` constants rather than an enum: the QoS byte is a `u8` and
/// is always valid to load (the value is validated, an enum load would be
/// undefined behavior on an out-of-range discriminant).
pub const AORTA_CONGESTION_CONTROL_DROP: u8 = 0;
pub const AORTA_CONGESTION_CONTROL_BLOCK: u8 = 1;

/// Per-publisher congestion-control OVERRIDE encoding for the *separate*
/// `aorta_pub_options_t.congestion_control` byte — a faithful mirror of Rust
/// `PublisherOptions::congestion_control: Option<CongestionControl>`. This is a
/// distinct space from the `AORTA_CONGESTION_CONTROL_*` QoS byte above (note the
/// shifted values): `INHERIT` is `0` so a **zero-initialized** `aorta_pub_options_t`
/// — e.g. a caller that only sets `max_message_size` or a schema — leaves the
/// override unset and inherits `qos.congestion_control`, instead of silently
/// forcing `Drop` and dropping the backpressure a `Block` QoS asked for.
/// Follows the same `INHERIT = 0` sentinel convention as the sibling
/// `aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT`.
pub const AORTA_PUB_CONGESTION_INHERIT: u8 = 0;
pub const AORTA_PUB_CONGESTION_DROP: u8 = 1;
pub const AORTA_PUB_CONGESTION_BLOCK: u8 = 2;

/// Quality of Service for publishers and subscribers — a 1:1 mirror of the
/// Rust `QoS` struct. The redesign removed the `durability`/`history_depth`
/// fields (silent no-ops; latched state is `ContextStore`'s job) and folded the
/// `congestion_control` + `express` knobs in, so the struct now fully describes
/// QoS without companion accessors or a separate `*_with_express` ctor param.
#[repr(C)]
pub struct aorta_qos_t {
    pub priority: aorta_priority_t,
    pub reliability: aorta_reliability_t,
    /// One of `AORTA_CONGESTION_CONTROL_*` (`0` = Drop, `1` = Block).
    pub congestion_control: u8,
    /// `0` = batched (default), `1` = express (bypass Zenoh batching).
    pub express: u8,
}

/// Identifier for a canonical QoS profile. Use with [`aorta_qos_preset`], which
/// returns the full `aorta_qos_t` (priority/reliability/congestion_control/
/// express).
///
/// Eliminates the duplicate preset definitions
/// that previously lived in `cpp/include/aorta/qos.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum aorta_qos_preset_t {
    AORTA_QOS_PRESET_DEFAULT = 0,
    AORTA_QOS_PRESET_REALTIME_CONTROL = 1,
    AORTA_QOS_PRESET_SENSOR_DATA = 2,
    AORTA_QOS_PRESET_STATE_UPDATE = 3,
    AORTA_QOS_PRESET_BULK_TRANSFER = 4,
}

pub type aorta_timer_cb = Option<extern "C" fn(*mut c_void)>;
pub type aorta_on_msg_cb = Option<
    extern "C" fn(
        *const aorta_bytes_view_t,
        *const aorta_schema_hash_view_t,
        *const aorta_msg_context_t,
        *mut c_void,
    ),
>;

pub type aorta_service_callback_t =
    Option<extern "C" fn(*const aorta_bytes_view_t, aorta_service_request_t, *mut c_void)>;

fn qos_from_ffi(qos: *const aorta_qos_t) -> AortaResult<QoS> {
    if qos.is_null() {
        return Ok(QoS::default());
    }
    // SAFETY: `qos` was null-checked above; FFI caller contract on every
    // entry point that forwards a `*const aorta_qos_t` requires it to point
    // at a valid `aorta_qos_t` for the duration of the call.
    let qos = unsafe { &*qos };
    let priority = match qos.priority {
        aorta_priority_t::AORTA_PRIORITY_REALTIME => Priority::Realtime,
        aorta_priority_t::AORTA_PRIORITY_HIGH => Priority::High,
        aorta_priority_t::AORTA_PRIORITY_NORMAL => Priority::Normal,
        aorta_priority_t::AORTA_PRIORITY_LOW => Priority::Low,
        aorta_priority_t::AORTA_PRIORITY_BACKGROUND => Priority::Background,
    };
    let reliability = match qos.reliability {
        aorta_reliability_t::AORTA_RELIABILITY_BEST_EFFORT => Reliability::BestEffort,
        aorta_reliability_t::AORTA_RELIABILITY_RELIABLE => Reliability::Reliable,
    };
    let congestion_control = match qos.congestion_control {
        AORTA_CONGESTION_CONTROL_DROP => CongestionControl::Drop,
        AORTA_CONGESTION_CONTROL_BLOCK => CongestionControl::Block,
        other => {
            return Err(AortaError::invalid_argument(format!(
                "qos.congestion_control must be 0 (Drop) or 1 (Block), got {other}"
            )))
        }
    };
    let express = match qos.express {
        0 => false,
        1 => true,
        other => {
            return Err(AortaError::invalid_argument(format!(
                "qos.express must be 0 or 1, got {other}"
            )))
        }
    };
    Ok(QoS::new(priority, reliability)
        .with_congestion_control(congestion_control)
        .with_express(express))
}

/// Converts the high-level [`QoS`] into the C-ABI [`aorta_qos_t`] struct.
fn qos_to_ffi(qos: &QoS) -> aorta_qos_t {
    aorta_qos_t {
        priority: match qos.priority {
            Priority::Realtime => aorta_priority_t::AORTA_PRIORITY_REALTIME,
            Priority::High => aorta_priority_t::AORTA_PRIORITY_HIGH,
            Priority::Normal => aorta_priority_t::AORTA_PRIORITY_NORMAL,
            Priority::Low => aorta_priority_t::AORTA_PRIORITY_LOW,
            Priority::Background => aorta_priority_t::AORTA_PRIORITY_BACKGROUND,
        },
        reliability: match qos.reliability {
            Reliability::BestEffort => aorta_reliability_t::AORTA_RELIABILITY_BEST_EFFORT,
            Reliability::Reliable => aorta_reliability_t::AORTA_RELIABILITY_RELIABLE,
        },
        congestion_control: match qos.congestion_control {
            CongestionControl::Drop => AORTA_CONGESTION_CONTROL_DROP,
            CongestionControl::Block => AORTA_CONGESTION_CONTROL_BLOCK,
        },
        express: u8::from(qos.express),
    }
}

fn qos_for_preset(preset: aorta_qos_preset_t) -> QoS {
    // Read the discriminant via `preset as u32` and compare against
    // each variant's value rather than match-ing on the enum directly.
    // Two effects:
    //   * If the value bit-pattern is outside the declared variants
    //     (e.g. the C caller wrote `(aorta_qos_preset_t)99`), we land
    //     in the DEFAULT branch instead of hitting Rust's
    //     "exhaustive-match on invalid discriminant" UB.
    //   * The compiler can't optimize away the safety net assuming
    //     the discriminant is valid, because the if-else operates on
    //     `u32` rather than the enum value.
    // Theoretical UB-from-construction still exists at FFI parameter
    // binding (shared by every `#[repr(C)] enum` parameter in
    // `aorta_*_t` — `aorta_priority_t`, `aorta_executor_type_t`,
    // `aorta_callback_group_t`, etc.). Fixing all of these is a separate,
    // crate-wide change; this function stays consistent with the existing
    // convention.
    let disc = preset as u32;
    if disc == aorta_qos_preset_t::AORTA_QOS_PRESET_REALTIME_CONTROL as u32 {
        QoS::realtime_control()
    } else if disc == aorta_qos_preset_t::AORTA_QOS_PRESET_SENSOR_DATA as u32 {
        QoS::sensor_data()
    } else if disc == aorta_qos_preset_t::AORTA_QOS_PRESET_STATE_UPDATE as u32 {
        QoS::state_update()
    } else if disc == aorta_qos_preset_t::AORTA_QOS_PRESET_BULK_TRANSFER as u32 {
        QoS::bulk_transfer()
    } else {
        // AORTA_QOS_PRESET_DEFAULT == 0 lands here too, alongside any
        // unknown / out-of-range value the C caller may have supplied.
        QoS::default()
    }
}

/// Returns the canonical [`aorta_qos_t`] for a preset profile. Single
/// source of truth for QoS presets — C++ / Python SDK preset constructors
/// must call this rather than hard-coding the field values.
///
/// The returned struct fully describes the preset (priority / reliability /
/// congestion_control / express), so no companion accessors are needed.
///
/// Safe: no pointer arguments, no panic paths. The body uses a
/// defensive cast-to-`u32` + if-else (see [`qos_for_preset`]) so
/// invalid-discriminant values from the C side fall back to DEFAULT
/// rather than reaching Rust's "exhaustive-match on enum" UB sink.
/// `ffi_guard` is unnecessary here.
#[no_mangle]
pub extern "C" fn aorta_qos_preset(preset: aorta_qos_preset_t) -> aorta_qos_t {
    qos_to_ffi(&qos_for_preset(preset))
}

fn attach_policy_from_ffi(policy: aorta_schema_attach_policy_t) -> SchemaAttachPolicy {
    match policy {
        aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS => SchemaAttachPolicy::Always,
        aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER => SchemaAttachPolicy::Never,
        aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT => SchemaAttachPolicy::Inherit,
    }
}

fn callback_group_from_ffi(group: aorta_callback_group_t) -> CallbackGroupType {
    match group {
        aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE => {
            CallbackGroupType::MutuallyExclusive
        }
        aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT => CallbackGroupType::Reentrant,
    }
}

fn callback_group_handle_from_ffi(
    handle: aorta_callback_group_handle_t,
) -> Option<CallbackGroupHandle> {
    if handle == 0 {
        None
    } else {
        Some(handle)
    }
}

fn subscriber_overflow_policy_from_ffi(
    policy: aorta_sub_overflow_policy_t,
) -> SubscriberOverflowPolicy {
    match policy {
        aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_DROP => SubscriberOverflowPolicy::Drop,
        aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_RELAY => SubscriberOverflowPolicy::Relay,
    }
}

fn rpc_overflow_policy_from_ffi(
    policy: aorta_rpc_overflow_policy_t,
) -> crate::service::RpcOverflowPolicy {
    match policy {
        aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY => {
            crate::service::RpcOverflowPolicy::FastFailBusy
        }
        aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_BLOCK => {
            crate::service::RpcOverflowPolicy::Block
        }
    }
}

fn timer_overflow_policy_from_ffi(
    policy: aorta_timer_overflow_policy_t,
) -> crate::timer::TimerOverflowPolicy {
    match policy {
        aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_COALESCE_LATEST => {
            crate::timer::TimerOverflowPolicy::CoalesceLatest
        }
        aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_BLOCK => {
            crate::timer::TimerOverflowPolicy::Block
        }
    }
}

unsafe fn executor_options_from_ffi(
    opts: *const aorta_executor_options_t,
) -> AortaResult<ExecutorOptions> {
    if opts.is_null() {
        return Ok(ExecutorOptions::default());
    }

    let opts = &*opts;
    let mode = match opts.scheduler_mode {
        aorta_scheduler_mode_t::AORTA_SCHEDULER_FIFO => SchedulerMode::Fifo,
        aorta_scheduler_mode_t::AORTA_SCHEDULER_PRIORITY => SchedulerMode::Priority,
    };

    let mut priority = PrioritySchedulerOptions::default();
    if !opts.priority.is_null() {
        let ffi_priority = &*opts.priority;
        priority.max_drain_per_cycle = ffi_priority.max_drain_per_cycle;
        priority.weights = ffi_priority.weights;
        priority.map_subscription_qos = match ffi_priority.map_subscription_qos {
            0 => false,
            1 => true,
            other => {
                return Err(AortaError::invalid_argument(format!(
                    "priority.map_subscription_qos must be 0 or 1, got {}",
                    other
                )));
            }
        };
    }

    let mut admission = ExecutorAdmissionOptions::default();
    if !opts.admission.is_null() {
        let ffi_admission = &*opts.admission;
        admission.max_queued_events = ffi_admission.max_queued_events;
        admission.max_queued_bytes = ffi_admission.max_queued_bytes;
    }

    let mut class_policy = ExecutorClassPolicyOptions::default();
    if !opts.class_policy.is_null() {
        let ffi_class = &*opts.class_policy;
        class_policy.subscriber_overflow_policy =
            subscriber_overflow_policy_from_ffi(ffi_class.sub_overflow_policy);
        class_policy.subscriber_overflow_relay_capacity = ffi_class.sub_overflow_relay_capacity;
        class_policy.rpc_overflow_policy =
            rpc_overflow_policy_from_ffi(ffi_class.rpc_overflow_policy);
        class_policy.rpc_overflow_block_timeout =
            Duration::from_millis(ffi_class.rpc_overflow_block_timeout_ms);
        class_policy.timer_overflow_policy =
            timer_overflow_policy_from_ffi(ffi_class.timer_overflow_policy);
        class_policy.timer_overflow_block_timeout =
            Duration::from_millis(ffi_class.timer_overflow_block_timeout_ms);
    }

    Ok(ExecutorOptions {
        mode,
        priority,
        admission,
        class_policy,
        force_tokio_queue: false,
    })
}

fn optional_bool_from_ffi(value: u8, field: &str) -> AortaResult<Option<bool>> {
    match value {
        AORTA_OPTIONAL_BOOL_INHERIT => Ok(None),
        AORTA_OPTIONAL_BOOL_FALSE => Ok(Some(false)),
        AORTA_OPTIONAL_BOOL_TRUE => Ok(Some(true)),
        _ => Err(AortaError::invalid_argument(format!(
            "{} must be 0 (inherit), 1 (false), or 2 (true)",
            field
        ))),
    }
}

fn log_format_from_ffi(value: aorta_log_format_t) -> LogFormat {
    match value {
        aorta_log_format_t::AORTA_LOG_FORMAT_JSON => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FfiLogTarget {
    Core,
    Ffi,
    Zenoh,
    SdkGeneric,
}

fn ffi_log_target_from_scope(scope: &str) -> (FfiLogTarget, Option<&str>) {
    let normalized = scope.trim();
    match normalized {
        "" => (FfiLogTarget::SdkGeneric, Some("aorta_sdk")),
        "ffi" | "aorta_ffi" => (FfiLogTarget::Ffi, Some("aorta_ffi")),
        "core" | "aorta_core" => (FfiLogTarget::Core, Some("aorta_core")),
        "sdk" | "aorta_sdk" => (FfiLogTarget::SdkGeneric, Some("aorta_sdk")),
        "zenoh" => (FfiLogTarget::Zenoh, Some("zenoh")),
        other => (FfiLogTarget::SdkGeneric, Some(other)),
    }
}

fn ffi_log_target_name(target: FfiLogTarget) -> &'static str {
    match target {
        FfiLogTarget::Core => "aorta_core",
        FfiLogTarget::Ffi => "aorta_ffi",
        FfiLogTarget::Zenoh => "zenoh",
        FfiLogTarget::SdkGeneric => "aorta_sdk",
    }
}

fn log_level_from_ffi(level: aorta_log_level_t) -> LogLevel {
    match level {
        aorta_log_level_t::AORTA_LOG_LEVEL_TRACE => LogLevel::Trace,
        aorta_log_level_t::AORTA_LOG_LEVEL_DEBUG => LogLevel::Debug,
        aorta_log_level_t::AORTA_LOG_LEVEL_INFO => LogLevel::Info,
        aorta_log_level_t::AORTA_LOG_LEVEL_WARN => LogLevel::Warn,
        aorta_log_level_t::AORTA_LOG_LEVEL_ERROR => LogLevel::Error,
    }
}

fn emit_ffi_log(
    level: aorta_log_level_t,
    scope: &str,
    file: Option<&str>,
    line: Option<u32>,
    message: &str,
) {
    let (target, scope_override) = ffi_log_target_from_scope(scope);
    logging::log_at(
        log_level_from_ffi(level),
        ffi_log_target_name(target),
        scope_override,
        file,
        line,
        message,
    );
}

unsafe fn publisher_options_from_ffi(
    qos: *const aorta_qos_t,
    opts: *const aorta_pub_options_t,
) -> AortaResult<PublisherOptions> {
    let mut options = PublisherOptions {
        qos: qos_from_ffi(qos)?,
        ..PublisherOptions::default()
    };

    if opts.is_null() {
        return Ok(options);
    }

    let opts = &*opts;
    options.schema_attach_policy = attach_policy_from_ffi(opts.schema_attach_policy);
    // FFI callers carry the per-publisher congestion override on
    // `aorta_pub_options_t`. `INHERIT` (the zero-initialized default)
    // maps to `None` so Core resolves against `qos.congestion_control`; an
    // explicit Drop/Block overrides it. Without the inherit sentinel a caller
    // that zero-inits `opts` only to set `max_message_size` / a schema would
    // force Drop and lose the backpressure a `Block` QoS asked for.
    options.congestion_control = match opts.congestion_control {
        AORTA_PUB_CONGESTION_INHERIT => None,
        AORTA_PUB_CONGESTION_DROP => Some(CongestionControl::Drop),
        AORTA_PUB_CONGESTION_BLOCK => Some(CongestionControl::Block),
        other => {
            return Err(AortaError::invalid_argument(format!(
                "aorta_pub_options_t.congestion_control must be 0 (inherit), \
                 1 (Drop), or 2 (Block), got {other}"
            )))
        }
    };

    if !opts.schema_bfbs.is_null() && opts.schema_bfbs_len > 0 {
        let bytes = std::slice::from_raw_parts(opts.schema_bfbs, opts.schema_bfbs_len);
        options.schema_bfbs = Some(bytes.to_vec());
    }

    if opts.max_message_size > 0 {
        options.max_message_size = Some(opts.max_message_size);
    }

    if !opts.schema_root_type.is_null() {
        options.schema_root_type = Some(cstr_to_string(
            opts.schema_root_type,
            "aorta_pub_options_t.schema_root_type",
        )?);
    }

    Ok(options)
}

unsafe fn schema_hash_from_ffi(
    hex: *const u8,
    len: usize,
    field_name: &str,
) -> AortaResult<Option<SchemaHash>> {
    if hex.is_null() {
        if len == 0 {
            return Ok(None);
        }
        return Err(AortaError::invalid_argument(format!(
            "{} pointer is null but length is {}",
            field_name, len
        )));
    }
    if len == 0 {
        return Err(AortaError::invalid_argument(format!(
            "{} length is zero but pointer is non-null",
            field_name
        )));
    }
    let bytes = std::slice::from_raw_parts(hex, len);
    let hash = SchemaHash::from_hex_bytes(bytes).ok_or_else(|| {
        AortaError::invalid_argument(format!(
            "{} must be 64-byte lowercase schema hash hex",
            field_name
        ))
    })?;
    Ok(Some(hash))
}

unsafe fn schema_bfbs_from_ffi(
    bytes: *const u8,
    len: usize,
    field_name: &str,
) -> AortaResult<Option<Vec<u8>>> {
    if bytes.is_null() {
        if len == 0 {
            return Ok(None);
        }
        return Err(AortaError::invalid_argument(format!(
            "{} pointer is null but length is {}",
            field_name, len
        )));
    }
    if len == 0 {
        return Err(AortaError::invalid_argument(format!(
            "{} length is zero but pointer is non-null",
            field_name
        )));
    }
    Ok(Some(std::slice::from_raw_parts(bytes, len).to_vec()))
}

unsafe fn utf8_text_from_ffi(
    ptr: *const u8,
    len: usize,
    field_name: &str,
) -> AortaResult<Option<String>> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(None);
        }
        return Err(AortaError::invalid_argument(format!(
            "{} pointer is null but length is {}",
            field_name, len
        )));
    }
    if len == 0 {
        return Err(AortaError::invalid_argument(format!(
            "{} length is zero but pointer is non-null",
            field_name
        )));
    }
    let bytes = std::slice::from_raw_parts(ptr, len);
    let value = std::str::from_utf8(bytes).map_err(|err| {
        AortaError::invalid_argument(format!("{} must be valid UTF-8: {}", field_name, err))
    })?;
    Ok(Some(value.to_string()))
}

unsafe fn service_config_from_ffi(
    opts: *const aorta_service_options_t,
    node_default: SchemaAttachPolicy,
) -> AortaResult<(SchemaAttachPolicy, Option<ServiceSchemaInfo>)> {
    if opts.is_null() {
        let schema_attach_policy = SchemaAttachPolicy::Inherit.resolve(node_default);
        if matches!(schema_attach_policy, SchemaAttachPolicy::Always) {
            return Err(AortaError::invalid_argument(
                "service schema (hash or bfbs) is required when rpc schema attach policy is always",
            ));
        }
        return Ok((schema_attach_policy, None));
    }

    let opts = &*opts;
    let schema_attach_policy =
        attach_policy_from_ffi(opts.schema_attach_policy).resolve(node_default);
    let service_schema = schema_hash_from_ffi(
        opts.service_schema_hash_hex,
        opts.service_schema_hash_len,
        "service schema hash",
    )?;
    let service_bfbs = schema_bfbs_from_ffi(
        opts.service_schema_bfbs,
        opts.service_schema_bfbs_len,
        "service schema bfbs",
    )?;
    let request_root_type = utf8_text_from_ffi(
        opts.request_root_type,
        opts.request_root_type_len,
        "service request root type",
    )?;
    let response_root_type = utf8_text_from_ffi(
        opts.response_root_type,
        opts.response_root_type_len,
        "service response root type",
    )?;

    let schema_info = match (service_schema, service_bfbs) {
        (None, None) => None,
        (Some(schema), None) => Some(ServiceSchemaInfo {
            schema,
            schema_bfbs: Vec::new(),
            request_root_type,
            response_root_type,
        }),
        (None, Some(schema_bfbs)) => Some(ServiceSchemaInfo {
            schema: SchemaHash::from_bfbs_bytes(&schema_bfbs),
            schema_bfbs,
            request_root_type,
            response_root_type,
        }),
        (Some(schema), Some(schema_bfbs)) => {
            let schema_from_bfbs = SchemaHash::from_bfbs_bytes(&schema_bfbs);
            if schema_from_bfbs != schema {
                return Err(AortaError::invalid_argument(
                    "service schema hash does not match service schema bfbs",
                ));
            }
            Some(ServiceSchemaInfo {
                schema,
                schema_bfbs,
                request_root_type,
                response_root_type,
            })
        }
    };

    if matches!(schema_attach_policy, SchemaAttachPolicy::Always) && schema_info.is_none() {
        return Err(AortaError::invalid_argument(
            "service schema (hash or bfbs) is required when rpc schema attach policy is always",
        ));
    }

    Ok((schema_attach_policy, schema_info))
}

/// Tuple returned by [`client_config_from_ffi`]: connect timeout, attach
/// policy, optional schema hash, optional rpc-enforce-single-provider
/// override, optional rpc-multi-reply-check window. Aliased so the lint
/// `clippy::type_complexity` doesn't fire on the function signature.
type ClientConfigFromFfi = (
    Duration,
    SchemaAttachPolicy,
    Option<SchemaHash>,
    Option<bool>,
    Option<Duration>,
    Option<bool>,
);

unsafe fn client_config_from_ffi(
    opts: *const aorta_client_options_t,
    fallback_timeout_ms: u64,
    node_default: SchemaAttachPolicy,
) -> AortaResult<ClientConfigFromFfi> {
    if opts.is_null() {
        let schema_attach_policy = SchemaAttachPolicy::Inherit.resolve(node_default);
        if matches!(schema_attach_policy, SchemaAttachPolicy::Always) {
            return Err(AortaError::invalid_argument(
                "client service schema hash is required when rpc schema attach policy is always",
            ));
        }
        return Ok((
            Duration::from_millis(fallback_timeout_ms),
            schema_attach_policy,
            None,
            None,
            None,
            None,
        ));
    }

    let opts = &*opts;
    let timeout_ms = if opts.timeout_ms == 0 {
        fallback_timeout_ms
    } else {
        opts.timeout_ms
    };
    let enforce_single_provider = optional_bool_from_ffi(
        opts.enforce_single_provider,
        "client enforce_single_provider",
    )?;
    let allow_retry = optional_bool_from_ffi(opts.allow_retry, "client allow_retry")?;
    let multi_reply_check_window = if opts.multi_reply_check_window_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(opts.multi_reply_check_window_ms))
    };
    let schema_attach_policy =
        attach_policy_from_ffi(opts.schema_attach_policy).resolve(node_default);
    let service_schema = schema_hash_from_ffi(
        opts.service_schema_hash_hex,
        opts.service_schema_hash_len,
        "client service schema hash",
    )?;

    if matches!(schema_attach_policy, SchemaAttachPolicy::Always) && service_schema.is_none() {
        return Err(AortaError::invalid_argument(
            "client service schema hash is required when rpc schema attach policy is always",
        ));
    }

    Ok((
        Duration::from_millis(timeout_ms),
        schema_attach_policy,
        service_schema,
        enforce_single_provider,
        multi_reply_check_window,
        allow_retry,
    ))
}

unsafe fn action_client_config_from_ffi(
    opts: *const aorta_action_client_options_t,
) -> AortaResult<(Duration, Duration)> {
    if opts.is_null() {
        return Ok((
            Duration::from_millis(DEFAULT_ACTION_CONTROL_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
        ));
    }

    let opts = &*opts;
    let control_timeout_ms = if opts.control_timeout_ms == 0 {
        DEFAULT_ACTION_CONTROL_TIMEOUT_MS
    } else {
        opts.control_timeout_ms
    };
    let info_timeout_ms = if opts.info_timeout_ms == 0 {
        DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS
    } else {
        opts.info_timeout_ms
    };

    Ok((
        Duration::from_millis(control_timeout_ms),
        Duration::from_millis(info_timeout_ms),
    ))
}

unsafe fn action_server_config_from_ffi(
    opts: *const aorta_action_server_options_t,
) -> AortaResult<crate::action_server::ActionServerOptions> {
    if opts.is_null() {
        return Ok(crate::action_server::ActionServerOptions::default());
    }

    let opts = &*opts;
    let options = crate::action_server::ActionServerOptions {
        supports_cancel: opts.supports_cancel != 0,
        max_concurrency: opts.max_concurrency,
        result_ttl: Duration::from_millis(opts.result_ttl_ms),
        dedup_ttl: Duration::from_millis(opts.dedup_ttl_ms),
    };
    options.validate()?;
    Ok(options)
}

fn single_slice_bytes_view(payload: &[u8], slice: &mut aorta_bytes_slice_t) -> aorta_bytes_view_t {
    if payload.is_empty() {
        return aorta_bytes_view_t::default();
    }
    *slice = ffi_slice_from_bytes(payload);
    aorta_bytes_view_t {
        slices: ptr::addr_of!(*slice),
        slice_count: 1,
        total_len: payload.len(),
    }
}

fn action_status_to_i8(status: ActionStatus) -> i8 {
    status as i8
}

fn action_reply_code_to_i16(code: ActionReplyCode) -> i16 {
    code as i16
}

fn priority_to_ffi(priority: Priority) -> i32 {
    match priority {
        Priority::Realtime => aorta_priority_t::AORTA_PRIORITY_REALTIME as i32,
        Priority::High => aorta_priority_t::AORTA_PRIORITY_HIGH as i32,
        Priority::Normal => aorta_priority_t::AORTA_PRIORITY_NORMAL as i32,
        Priority::Low => aorta_priority_t::AORTA_PRIORITY_LOW as i32,
        Priority::Background => aorta_priority_t::AORTA_PRIORITY_BACKGROUND as i32,
    }
}

fn build_msg_context(context: &MessageContext) -> aorta_msg_context_t {
    aorta_msg_context_t {
        topic: string_view_from_str(context.topic.as_ref()),
        receive_stamp_ns: context.receive_stamp_ns,
        priority: priority_to_ffi(context.priority),
        publisher_node: string_view_from_optional(context.publisher_node.as_deref()),
        sequence: context.sequence.unwrap_or(0),
    }
}

fn publisher_stats_from_core(publisher: &Publisher) -> aorta_publisher_stats_t {
    let _ = publisher;
    aorta_publisher_stats_t::default()
}

fn service_stats_from_core(stats: CoreServiceStats) -> aorta_service_stats_t {
    aorta_service_stats_t {
        request_received_count: stats.request_received_count,
        request_dispatched_count: stats.request_dispatched_count,
        reply_ok_count: stats.reply_ok_count,
        reply_error_count: stats.reply_error_count,
        reply_send_failure_count: stats.reply_send_failure_count,
        request_dropped_count: stats.request_dropped_count,
        query_receive_failure_count: stats.query_receive_failure_count,
        task_exit_count: stats.task_exit_count,
        in_flight_requests: stats.in_flight_requests,
        last_request_ns: stats.last_request_ns,
        last_reply_ns: stats.last_reply_ns,
        request_dropped_deadline_count: stats.request_dropped_deadline_count,
    }
}

fn client_stats_from_core(stats: CoreClientStats) -> aorta_client_stats_t {
    aorta_client_stats_t {
        call_count: stats.call_count,
        success_count: stats.success_count,
        timeout_count: stats.timeout_count,
        concurrent_conflict_count: stats.concurrent_conflict_count,
        error_count: stats.error_count,
        retry_count: stats.retry_count,
        last_call_started_ns: stats.last_call_started_ns,
        last_call_finished_ns: stats.last_call_finished_ns,
        last_call_latency_ns: stats.last_call_latency_ns,
    }
}

fn executor_admission_stats_from_core(
    stats: crate::event_queue::AdmissionStats,
) -> aorta_executor_admission_stats_t {
    aorta_executor_admission_stats_t {
        queued_events: stats.queued_events,
        queued_bytes: stats.queued_bytes,
        peak_queued_events: stats.peak_queued_events,
        peak_queued_bytes: stats.peak_queued_bytes,
        dropped_queue_full: stats.dropped_queue_full,
        dropped_queue_closed: stats.dropped_queue_closed,
        dropped_budget_events: stats.dropped_budget_events,
        dropped_budget_bytes: stats.dropped_budget_bytes,
    }
}

#[no_mangle]
pub extern "C" fn aorta_last_error() -> *const c_char {
    ffi_guard_value(ptr::null(), || {
        LAST_ERROR
            .try_with(|cell| {
                cell.borrow()
                    .as_ref()
                    .map(|value| value.as_ptr())
                    .unwrap_or(ptr::null())
            })
            .unwrap_or(ptr::null())
    })
}

/// Monotonic C-ABI compatibility version of this `libaorta_core` build.
///
/// SDK loaders (Python `aorta._ffi.loader`, and any future C/C++ check) compare
/// this against their compiled-in expected value at load time and refuse to run
/// against a mismatched — stale or wrong-arch — shared library *before* any
/// other call. This catches the "right symbol names, wrong layout" case that a
/// symbol-presence probe cannot.
///
/// **Bump this on ANY breaking C-ABI change** (struct layout, function
/// signature, result-code semantics) in lockstep with `abi_assert.rs` and the
/// `libaorta_core` SO_NAME, in the same commit. Also bumped
/// for purely *additive* surface (a new `extern "C"` fn an SDK now requires), so
/// the version alone signals the minimum compatible `libaorta_core`.
///
/// History: 1→2 added `aorta_node_get_group` (for the Python SDK); 2→3 added
/// `aorta_service_request_reply_error` (Python SDK RPC); 3→4 added the
/// bridge fns `aorta_timer_is_running`, `aorta_timer_get_period_ns`,
/// `aorta_ctx_is_key_declared`, `aorta_ctx_declared_prefixes`, and
/// `aorta_subscriber_runtime_stats`; 4→5 added the route-readiness fns
/// `aorta_publisher_matching_status`, `aorta_publisher_wait_for_matching`,
/// `aorta_client_matching_status`, and `aorta_client_wait_for_matching`
/// (required by the C++ SDK matching wrappers); 5→6 added the raw-capture +
/// schema-resolver surface `aorta_capture_subscriber_new/free`,
/// `aorta_capture_context_snapshot`, `aorta_schema_resolver_new/free`,
/// `aorta_schema_resolver_resolve_by_hash/by_key`,
/// `aorta_schema_resolver_clear_key_cache`, and `aorta_resolved_schema_free`
/// (required by the C++ and Python capture/resolver SDK wrappers). 6→7 is the
/// QoS & buffering redesign — the first *layout-breaking* bump (prior ones were
/// purely additive): `aorta_qos_t` shrinks 24→12 B (dropped `durability` /
/// `history_depth`, folded in `congestion_control` + `express`), removing
/// `aorta_durability_t`; and the `aorta_subscriber_new2`,
/// `aorta_{publisher,subscriber}_new_with_express`, `aorta_qos_preset_express`,
/// `aorta_qos_preset_congestion` symbols and the `aorta_sub_options2_t` type
/// are removed. `aorta_sub_options_t` stays **layout-frozen at 72 B** (it
/// is Python-mirrored, so it never grows); the buffering
/// knobs ride the new 16-byte `aorta_sub_buffering_t` behind the new
/// `aorta_subscriber_new_buffered` constructor, alongside `aorta_sub_recv` /
/// `aorta_sub_try_recv` / `aorta_sub_dropped_count` / `aorta_pull_sample_free`.
/// A v6 peer would misread the reshaped `aorta_qos_t` and reference removed
/// symbols, so this MUST reject v6 peers.
// ABI history: 7 = pre-Flow (the last released generation). 8 = Aorta
// Flow — ONE release bump: the branch's interim iteration
// steps (arbiter spine, watch, the polish reshape of
// admission/resume-event, aorta_task_finish_failed, priority wiring
// + aorta_executor_post_resume_after) were collapsed into this single
// generation before release. The 7→8 surface: the arbiter surface
// (group/arm/complete/revoke + timer bridge + resolutions pin),
// lane-facade hooks (aorta_executor_attach/detach_lane_pool,
// aorta_lane_hooks_t w/ forward_fn incl. deadline/cost,
// aorta_executor_wake, callback-context enter/exit,
// update_lane_telemetry), create_callback_group_with_class,
// aorta_sub_set_notify + aorta_sub_pending, aorta_watch_*, task
// accounting, priority wiring (post_resume priority param,
// aorta_scope_priority), aorta_executor_post_resume_after,
// and the readiness surface: aorta_window_sync_* / aorta_aligned_set_*
// / aorta_readiness_set_* + aorta_subscriber_bind_{window,readiness},
// plus the action ctx bridge: aorta_action_client_send_goal_ctx /
// cancel_ctx / wait_result_ctx (one-shot, scope-aware, callback delivery),
// and the arbiter scope bridge aorta_scope_arm_arbiter (per-scope cancel
// branch behind next(sub, rt, scope) — recv_in parity), plus
// aorta_subscriber_unbind_{window,readiness} (bind's inverse).
// 8→9: SerialDomain — ONE additive symbol,
// aorta_executor_stop_flag_ptr (resolve-once RT-safe pointer to the
// executor's monotonic stop flag; the aorta_scope_cancelled_flag_ptr
// precedent), required as the arming edge of the C++ SDK's stop-bridge
// state machine (facade gate close sequenced before pool close). No
// struct or layout changes; bumped because the C++ SDK now REQUIRES the
// symbol (additive-surface rule above).
// 9→10: ContextStore owner liveliness — additive
// surface: aorta_ctx_owner_liveliness_watch / _free plus the new
// `aorta_ctx_owner_liveliness_event_t` struct (40 B; the only new
// layout — existing types untouched). Bumped because the C++ and Python
// SDKs now REQUIRE the symbols (additive-surface rule).
// 10→11: two additive changes shipped in one generation:
//  - C++ reentrancy guardrails: ONE additive symbol,
//    aorta_reentrant_context (exact thread-local reentrancy detector:
//    1 iff inside a dispatched callback or an aorta-owned block_on).
//    The C++ SDK now REQUIRES the symbol (additive-surface rule above).
//  - Topic schema root metadata: `aorta_pub_options_t` appends
//    `schema_root_type` at offset 32, growing from 32 B to 40 B. C++
//    and Python mirrors require this field so typed publishers can
//    register a non-declared root table through schema_by_key.
// 11→12: generic UUIDv7 generation adds `aorta_uuid_v7`, consumed by the
// C++ and Python SDKs; Rust calls the same Core implementation directly.
// 12→13: raw action servers gain `aorta_action_server_new_with_callbacks`
// and its new input struct `aorta_action_server_callbacks_t` (32 B):
// a server-level cancel-requested callback plus a `user_data` release
// hook that the library invokes exactly once, after the last callback
// has returned. Existing structs are untouched. Bumped because the C++
// SDK now REQUIRES the symbol (additive-surface rule).
pub const AORTA_ABI_VERSION: u32 = 13;

#[no_mangle]
pub extern "C" fn aorta_abi_version() -> u32 {
    AORTA_ABI_VERSION
}

/// Returns 1 iff a blocking Aorta operation on the current thread right
/// now would be refused as reentrant by the explicit detector (thread is
/// inside a dispatched Aorta callback or nested inside an aorta-owned
/// blocking entry). Exact, allocation-free, never initializes the
/// runtime. Returns 0 on plain threads, spawn_blocking threads, Flow
/// offload workers, unbound feedback2 delivery threads, and
/// foreign-runtime workers (the latter still rejected by the classifier
/// fallback on ffi_block_on! entries).
#[no_mangle]
pub extern "C" fn aorta_reentrant_context() -> u8 {
    crate::flow::reentrancy::explicitly_reentrant() as u8
}

pub mod arbiter;
pub mod log_ffi;
pub use log_ffi::*;

pub mod trace_ffi;
pub use trace_ffi::*;

pub mod node;
pub use node::*;
pub mod uuid_v7;
pub use uuid_v7::*;
pub mod publisher;
pub use publisher::*;
pub mod subscriber;
pub use subscriber::*;

pub mod executor;
pub use executor::*;
pub mod timer;
pub use timer::*;
pub mod sample;
pub use sample::*;
pub mod service;
pub use service::*;
pub mod client;
pub use client::*;
pub mod action_server;
pub use action_server::*;
pub mod action_client;
pub use action_client::*;
pub mod contextstore;
pub use contextstore::*;
pub mod capture;
pub use capture::*;
pub mod flow;
pub use flow::*;

#[cfg(test)]
mod tests;
