#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;

use aorta_core::ZBytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use aorta_core::error::{AortaError, AortaResult};
use aorta_core::node::Node as CoreNode;
use aorta_core::publisher::Publisher as CorePublisher;
use aorta_core::schema::{SchemaHash, SchemaMeta};

use crate::header::{HeaderInjector, HeaderValues, TypedPublishMessage};
use crate::log_prelude::*;

// ---------------------------------------------------------------------------
// Builder policy enums — SDK-local definitions
// ---------------------------------------------------------------------------
//
// These were removed from `aorta_core::publisher::PublisherOptions` in Phase
// 5.0 (core no longer exposes builder management knobs).  The Rust SDK keeps
// local copies because the SDK-level publisher still uses TLS / mutex / fresh
// builder selection internally.

/// Controls what happens when a published message exceeds the builder's
/// current capacity.
///
/// Currently only one policy exists; the builder always grows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuilderGrowthPolicy {
    /// Grow the builder and emit a warning log (default).
    GrowAndWarn,
}

/// Controls how the SDK reuses `FlatBufferBuilder` allocations across
/// successive [`publish`](crate::Publisher::publish) calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuilderReusePolicy {
    /// One builder per publisher, cached in thread-local storage (default).
    /// Falls back to `Mutex` when capacity exceeds the large-buffer
    /// threshold (`AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB`, default 2 MiB).
    ThreadLocal,
    /// One builder per publisher, guarded by a mutex (shared across threads).
    Mutex,
    /// No caching — allocate a fresh builder every publish.
    None,
}

// ---------------------------------------------------------------------------
// builder_from_vec — create a FlatBufferBuilder from a recycled Vec<u8>
// ---------------------------------------------------------------------------
//
// FlatBufferBuilder::from_vec() uses `DefaultAllocator::len()` (== Vec::len())
// as the backing-buffer size. RecyclableBuffer preserves that allocator length
// when it returns a Vec to the pool. The Vec capacity may be larger and is not
// a valid substitute: changing the allocator length to the capacity can shift
// FlatBuffers alignment and produce invalid tables when message shapes change.

/// Create a `FlatBufferBuilder` from a recycled `Vec<u8>`, exposing the
/// Vec's full capacity as usable buffer space to avoid the grow-downwards
/// loop.
fn builder_from_vec(mut vec: Vec<u8>) -> FlatBufferBuilder<'static> {
    if vec.is_empty() {
        vec.resize(vec.capacity(), 0);
    }
    FlatBufferBuilder::from_vec(vec)
}

// ---------------------------------------------------------------------------
// RecyclePool — bounded LIFO Vec<u8> buffer pool (mutex-guarded)
// ---------------------------------------------------------------------------
//
// When a published message is dropped by Zenoh (on any thread), its backing
// Vec<u8> is returned here instead of being freed.  The next publish pops a
// Vec from the pool and feeds it to builder_from_vec(), avoiding
// both malloc and the grow-downwards loop in steady state.
//
// Concurrency: fully MPMC.  Pops are NOT single-threaded — obtain_builder()
// runs concurrently from every publisher thread (TLS first-publish, the
// fresh-builder/reentrant path, and the post-publish cache refill), while
// Zenoh drop threads push concurrently.  An earlier implementation used an
// intrusive lock-free Treiber stack here under an assumed single-popper
// (MPSC) contract; under the real MPMC usage an untagged Treiber pop is
// ABA-unsound (two in-flight builders can alias one allocation).  A mutex
// around a preallocated Vec is the
// simple, provably correct replacement: the critical section is a Vec move
// (never allocates), so an uncontended lock/unlock is the same two atomic
// operations as the CAS loop it replaces, and LIFO order keeps the warmest
// buffer on top.

/// Buffers with less capacity than this are not worth pooling: they would
/// seed a builder with almost no usable space and force an immediate
/// grow-downwards reallocation on the next publish.
const MIN_RECYCLE_CAPACITY: usize = 16;

/// Maximum number of buffers to keep in the recycle pool.
/// Beyond this depth, returned buffers are freed immediately, preventing
/// unbounded RSS growth under backpressure.
const MAX_RECYCLE_POOL_DEPTH: usize = 8;

/// Reject buffers larger than this factor × base_capacity.
/// Prevents occasional oversized messages from permanently polluting the pool.
const RECYCLE_MAX_CAPACITY_FACTOR: usize = 4;

/// Bounded MPMC buffer pool: a mutex-guarded LIFO stack of recycled Vecs.
pub(crate) struct RecyclePool {
    /// LIFO stack of recycled buffers.  Preallocated to
    /// MAX_RECYCLE_POOL_DEPTH so push never allocates (and therefore never
    /// panics) inside the critical section.
    buffers: Mutex<Vec<Vec<u8>>>,
    /// Buffers larger than this are not pooled. Set to
    /// `base_capacity * RECYCLE_MAX_CAPACITY_FACTOR`.
    max_buffer_capacity: usize,
}

impl RecyclePool {
    pub(crate) fn new(base_capacity: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::with_capacity(MAX_RECYCLE_POOL_DEPTH)),
            max_buffer_capacity: base_capacity
                .saturating_mul(RECYCLE_MAX_CAPACITY_FACTOR)
                .max(MIN_RECYCLE_CAPACITY),
        }
    }

    /// Lock the pool, recovering from poison.
    ///
    /// The critical sections below only move Vecs (no user code, no
    /// allocation), so a poisoned lock means a panic elsewhere unwound
    /// through a guard — the pool contents are still structurally valid.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Vec<u8>>> {
        self.buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Push a `Vec<u8>` into the pool for later reuse.
    ///
    /// Buffers smaller than MIN_RECYCLE_CAPACITY, larger than the pool's
    /// capacity cap, or arriving while the pool is full are silently dropped
    /// (freed) instead of pooled.
    pub(crate) fn push(&self, buf: Vec<u8>) {
        if buf.capacity() < MIN_RECYCLE_CAPACITY {
            return; // not worth recycling
        }

        // Reject oversized buffers to prevent occasional large messages
        // from permanently inflating RSS.
        if buf.capacity() > self.max_buffer_capacity {
            return; // drop the oversized buffer
        }

        let mut buffers = self.lock();
        if buffers.len() < MAX_RECYCLE_POOL_DEPTH {
            buffers.push(buf);
        }
        // else: pool full — drop the buffer to bound RSS.
    }

    /// Pop a `Vec<u8>` from the pool, or return `None` if empty.
    ///
    /// The returned Vec retains the allocator length produced by
    /// `FlatBufferBuilder::collapse()` so it can be passed back to
    /// `FlatBufferBuilder::from_vec()` without changing alignment.
    pub(crate) fn pop(&self) -> Option<Vec<u8>> {
        self.lock().pop()
    }

    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        self.lock().len()
    }

    /// Current pool depth (for diagnostics / stats).
    pub(crate) fn approximate_depth(&self) -> usize {
        self.lock().len()
    }
}

// ---------------------------------------------------------------------------
// RecyclableBuffer — zero-copy owner for Bytes::from_owner()
// ---------------------------------------------------------------------------
//
// Wraps a (Vec<u8>, offset) pair.  Implements AsRef<[u8]> to expose
// only the valid FlatBuffer region [offset..].  On Drop, returns the
// Vec to the RecyclePool for reuse.

pub(crate) struct RecyclableBuffer {
    vec: Vec<u8>,
    offset: usize,
    pool: Arc<RecyclePool>,
}

impl AsRef<[u8]> for RecyclableBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.vec[self.offset..]
    }
}

impl Drop for RecyclableBuffer {
    fn drop(&mut self) {
        // FlatBufferBuilder::reset() clears the previously written tail before
        // reuse. collapse() transfers ownership, so perform the equivalent
        // clearing here before the Vec returns to the pool. Without this,
        // omitted/default fields can inherit stale vtable bytes.
        self.vec[self.offset..].fill(0);
        // Take ownership of the Vec and return it to the pool.
        let vec = std::mem::take(&mut self.vec);
        self.pool.push(vec);
    }
}

const DEFAULT_VAR_CAPACITY: usize = 4 * 1024;
// Keep 1 MiB-class periodic publishers on the lock-free TLS builder path by
// default; memory-sensitive deployments can still lower this via env.
const DEFAULT_TLS_LARGE_CAPACITY_THRESHOLD: usize = 2 * 1024 * 1024;

/// If the TLS builder's capacity exceeds `TLS_SHRINK_FACTOR × capacity_hint`,
/// replace it with a right-sized one. This reclaims RSS from occasional
/// oversized publishes (e.g. a 10MB calibration blob on a normally-256B topic).
/// Set conservatively high (8x) to avoid unnecessary shrink/realloc cycles
/// for normal FlatBufferBuilder growth patterns (which typically 2x).
const TLS_SHRINK_FACTOR: usize = 8;
const LOG_INTERVAL_NS: u64 = 1_000_000_000;
const ENV_INCLUDE_PUBLISHER_NODE_IN_HEADER: &str = "AORTA_HEADER_INCLUDE_PUBLISHER_NODE";

static NEXT_PUBLISHER_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
thread_local! {
    static FORCE_PUBLISH_ERROR: Cell<bool> = const { Cell::new(false) };
}

fn tls_large_capacity_threshold() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(
        || match std::env::var("AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB") {
            Ok(raw) => raw
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .map(|kb| kb * 1024)
                .unwrap_or(DEFAULT_TLS_LARGE_CAPACITY_THRESHOLD),
            Err(_) => DEFAULT_TLS_LARGE_CAPACITY_THRESHOLD,
        },
    )
}

fn include_publisher_node_in_header() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(
        || match std::env::var(ENV_INCLUDE_PUBLISHER_NODE_IN_HEADER) {
            Ok(raw) => {
                let value = raw.trim();
                !(value == "0"
                    || value.eq_ignore_ascii_case("false")
                    || value.eq_ignore_ascii_case("no")
                    || value.eq_ignore_ascii_case("off"))
            }
            Err(_) => true,
        },
    )
}

thread_local! {
    /// Combined thread-local state for all publishers accessed on this thread.
    /// Uses a Vec with linear scan instead of HashMap — for the typical case
    /// of 1-3 publishers per thread, linear scan is faster (no hashing, no
    /// bucket lookup, better cache locality).  Merging the builder and
    /// reentrancy maps into one TLS variable halves the TLS accesses per
    /// publish (2 instead of 4).
    static TLS_PUB_STATE: RefCell<Vec<TlsPublisherState>> = const { RefCell::new(Vec::new()) };
}

struct TlsPublisherState {
    publisher_id: u64,
    builder: Option<FlatBufferBuilder<'static>>,
    capacity_hint: usize,
    /// Nesting depth of in-flight publish calls on this thread for this publisher.
    publishing_depth: usize,
}

#[cfg(test)]
pub(crate) fn force_publish_error_for_test() {
    FORCE_PUBLISH_ERROR.with(|flag| flag.set(true));
}

#[cfg(test)]
fn take_force_publish_error() -> bool {
    FORCE_PUBLISH_ERROR.with(|flag| flag.replace(false))
}

// TlsBuilderSlot removed — merged into TlsPublisherState above.

struct ReentrancyGuard {
    publisher_id: u64,
    tls_active: bool,
}

impl ReentrancyGuard {
    /// Mark this publisher as currently publishing.  Returns `true` if already
    /// inside a publish call (reentrancy detected).
    fn enter(publisher_id: u64) -> (bool, Self) {
        match TLS_PUB_STATE.try_with(|tls| {
            let mut vec = tls.borrow_mut();
            // Linear scan — fast for the typical 1-3 entries.
            for slot in vec.iter_mut() {
                if slot.publisher_id == publisher_id {
                    let was = slot.publishing_depth > 0;
                    slot.publishing_depth = slot.publishing_depth.saturating_add(1);
                    return was;
                }
            }
            // First time seeing this publisher on this thread.
            vec.push(TlsPublisherState {
                publisher_id,
                builder: None,
                capacity_hint: 0, // will be set by publish_with_tls_builder
                publishing_depth: 1,
            });
            false
        }) {
            Ok(reentrant) => (
                reentrant,
                Self {
                    publisher_id,
                    tls_active: true,
                },
            ),
            // Static destructors can publish after main-thread TLS teardown.
            // The publish path will use a one-shot builder without caching.
            Err(_) => (
                false,
                Self {
                    publisher_id,
                    tls_active: false,
                },
            ),
        }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        if !self.tls_active {
            return;
        }
        let _ = TLS_PUB_STATE.try_with(|tls| {
            let mut vec = tls.borrow_mut();
            for slot in vec.iter_mut() {
                if slot.publisher_id == self.publisher_id {
                    slot.publishing_depth = slot.publishing_depth.saturating_sub(1);
                    return;
                }
            }
        });
    }
}

/// Snapshot of a publisher's runtime counters, taken by [`Publisher::stats`].
///
/// All counters are monotonic since the publisher was created; timing fields
/// are in nanoseconds. Used for diagnostics and tuning (see the `publisher_*
/// tests` for expectations).
#[derive(Clone, Debug, Default)]
pub struct PublisherStats {
    /// Total number of publish attempts (all paths).
    pub publish_count: u64,
    /// Number of publishes whose payload exceeded the builder capacity
    /// (triggered an auto-grow).
    pub oversize_count: u64,
    /// Number of re-entrant publishes (publish called from inside a
    /// subscriber callback of the same node, etc.).
    pub reentrant_count: u64,
    /// Publishes that used the thread-local cached builder.
    pub tls_builder_publish_count: u64,
    /// Publishes that used the mutex-guarded shared builder.
    pub mutex_builder_publish_count: u64,
    /// Publishes that allocated a fresh (non-cached) builder.
    pub fresh_builder_publish_count: u64,
    /// Times a recycled buffer was successfully reused from the pool.
    pub recycle_pool_hit_count: u64,
    /// Times the recycle pool was empty and a new buffer was allocated.
    pub recycle_pool_miss_count: u64,
    /// Times the builder strategy switched from TLS to mutex (large messages).
    pub mutex_mode_switch_count: u64,
    /// Size of the most recent oversize payload.
    pub last_oversize_size: usize,
    /// Current builder capacity in bytes.
    pub current_capacity: usize,
    /// Highest builder capacity ever seen.
    pub max_capacity_seen: usize,
    /// Nanoseconds of the last publish (0 if publish timestamps are off).
    pub last_publish_ns: u64,
    /// Publish attempts that failed at the transport layer.
    pub send_error_count: u64,
    /// Cumulative time spent building the FlatBuffer payload (ns).
    pub build_total_ns: u64,
    /// Longest single build (ns).
    pub build_max_ns: u64,
    /// Number of builds sampled into the build timings.
    pub build_sample_count: u64,
    /// Cumulative time spent handing the payload to Zenoh (ns).
    pub transport_total_ns: u64,
    /// Longest single transport hand-off (ns).
    pub transport_max_ns: u64,
    /// Number of transports sampled into the transport timings.
    pub transport_sample_count: u64,
}

struct PublisherStatsInner {
    publish_count: AtomicU64,
    oversize_count: AtomicU64,
    reentrant_count: AtomicU64,
    tls_builder_publish_count: AtomicU64,
    mutex_builder_publish_count: AtomicU64,
    fresh_builder_publish_count: AtomicU64,
    recycle_pool_hit_count: AtomicU64,
    recycle_pool_miss_count: AtomicU64,
    mutex_mode_switch_count: AtomicU64,
    last_oversize_size: AtomicUsize,
    current_capacity: AtomicUsize,
    max_capacity_seen: AtomicUsize,
    last_publish_ns: AtomicU64,
    send_error_count: AtomicU64,
    build_total_ns: AtomicU64,
    build_max_ns: AtomicU64,
    build_sample_count: AtomicU64,
    transport_total_ns: AtomicU64,
    transport_max_ns: AtomicU64,
    transport_sample_count: AtomicU64,
}

fn update_max_u64(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (value > current).then_some(value)
    });
}

impl PublisherStatsInner {
    fn new(initial_capacity: usize) -> Self {
        Self {
            publish_count: AtomicU64::new(0),
            oversize_count: AtomicU64::new(0),
            reentrant_count: AtomicU64::new(0),
            tls_builder_publish_count: AtomicU64::new(0),
            mutex_builder_publish_count: AtomicU64::new(0),
            fresh_builder_publish_count: AtomicU64::new(0),
            recycle_pool_hit_count: AtomicU64::new(0),
            recycle_pool_miss_count: AtomicU64::new(0),
            mutex_mode_switch_count: AtomicU64::new(0),
            last_oversize_size: AtomicUsize::new(0),
            current_capacity: AtomicUsize::new(initial_capacity),
            max_capacity_seen: AtomicUsize::new(initial_capacity),
            last_publish_ns: AtomicU64::new(0),
            send_error_count: AtomicU64::new(0),
            build_total_ns: AtomicU64::new(0),
            build_max_ns: AtomicU64::new(0),
            build_sample_count: AtomicU64::new(0),
            transport_total_ns: AtomicU64::new(0),
            transport_max_ns: AtomicU64::new(0),
            transport_sample_count: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> PublisherStats {
        PublisherStats {
            publish_count: self.publish_count.load(Ordering::Relaxed),
            oversize_count: self.oversize_count.load(Ordering::Relaxed),
            reentrant_count: self.reentrant_count.load(Ordering::Relaxed),
            tls_builder_publish_count: self.tls_builder_publish_count.load(Ordering::Relaxed),
            mutex_builder_publish_count: self.mutex_builder_publish_count.load(Ordering::Relaxed),
            fresh_builder_publish_count: self.fresh_builder_publish_count.load(Ordering::Relaxed),
            recycle_pool_hit_count: self.recycle_pool_hit_count.load(Ordering::Relaxed),
            recycle_pool_miss_count: self.recycle_pool_miss_count.load(Ordering::Relaxed),
            mutex_mode_switch_count: self.mutex_mode_switch_count.load(Ordering::Relaxed),
            last_oversize_size: self.last_oversize_size.load(Ordering::Relaxed),
            current_capacity: self.current_capacity.load(Ordering::Relaxed),
            max_capacity_seen: self.max_capacity_seen.load(Ordering::Relaxed),
            last_publish_ns: self.last_publish_ns.load(Ordering::Relaxed),
            send_error_count: self.send_error_count.load(Ordering::Relaxed),
            build_total_ns: self.build_total_ns.load(Ordering::Relaxed),
            build_max_ns: self.build_max_ns.load(Ordering::Relaxed),
            build_sample_count: self.build_sample_count.load(Ordering::Relaxed),
            transport_total_ns: self.transport_total_ns.load(Ordering::Relaxed),
            transport_max_ns: self.transport_max_ns.load(Ordering::Relaxed),
            transport_sample_count: self.transport_sample_count.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct BuilderConfig {
    base_capacity: usize,
    growth_policy: BuilderGrowthPolicy,
    reuse_policy: BuilderReusePolicy,
    include_publish_timestamp: bool,
}

/// A typed publisher for schema `M` on a fixed topic.
///
/// Created via [`Node::create_publisher`](crate::Node::create_publisher) or
/// [`Node::create_publisher_with_options`](crate::Node::create_publisher_with_options).
///
/// Every publish:
/// - injects an Aorta header (publish timestamp, publisher node, per-
///   publisher sequence) via `M::build_header`;
/// - builds the FlatBuffer payload with an internal (TLS/mutex/fresh)
///   builder and publishes it **synchronously, fire-and-forget** — there is
///   no `.await` on the hot path;
/// - recycles the backing buffer after Zenoh releases it (bounded pool),
///   making steady-state publishes zero-copy.
///
/// Publishers are not `Clone`; move one into the closure/task that
/// publishes, or share it behind an `Arc`. Re-entrant publishes — calling
/// `publish()` on the same `Publisher` from the same thread while an outer
/// `publish()` is still in flight, e.g. from inside its build/fill closure
/// — are detected and handled with a temporary builder.
///
/// `publish()` is synchronous and fire-and-forget (no `.await`, no
/// acknowledgement) — but under
/// [`CongestionControl::Block`](aorta_core::qos::CongestionControl::Block)
/// the transport may **block the calling thread** while backpressured.
/// `Drop` (the default) never blocks.
///
/// Set [`PublisherOptions::max_message_size`](aorta_core::publisher::PublisherOptions::max_message_size)
/// to the expected message size to avoid repeated builder growth. When it is
/// `None`, the initial builder capacity comes from the schema's
/// `fixed_size()` (fixed-size messages) or defaults to 4096 bytes
/// (variable-size messages).
pub struct Publisher<M: SchemaMeta + HeaderInjector> {
    inner: CorePublisher,
    node: CoreNode,
    publisher_node_name: String,
    include_publisher_node_in_header: bool,
    include_publish_timestamp: bool,
    sequence: AtomicU64,
    publisher_id: u64,
    base_capacity: usize,
    builder_growth_policy: BuilderGrowthPolicy,
    builder_reuse_policy: BuilderReusePolicy,
    use_mutex_builder: AtomicBool,
    mutex_builder: OnceLock<Mutex<FlatBufferBuilder<'static>>>,
    pool: Arc<RecyclePool>,
    stats: PublisherStatsInner,
    last_oversize_log_ns: AtomicU64,
    last_reentrant_log_ns: AtomicU64,
    _marker: PhantomData<M>,
}

impl<M: SchemaMeta + HeaderInjector> Publisher<M> {
    pub(crate) fn new(node: CoreNode, inner: CorePublisher, config: BuilderConfig) -> Self {
        let publisher_id = NEXT_PUBLISHER_ID.fetch_add(1, Ordering::Relaxed);
        let stats = PublisherStatsInner::new(config.base_capacity);
        let publisher_node_name = node.name().to_string();
        let include_publisher_node_in_header = include_publisher_node_in_header();
        Self {
            inner,
            node,
            publisher_node_name,
            include_publisher_node_in_header,
            include_publish_timestamp: config.include_publish_timestamp,
            sequence: AtomicU64::new(0),
            publisher_id,
            base_capacity: config.base_capacity,
            builder_growth_policy: config.growth_policy,
            builder_reuse_policy: config.reuse_policy,
            use_mutex_builder: AtomicBool::new(false),
            mutex_builder: OnceLock::new(),
            pool: Arc::new(RecyclePool::new(config.base_capacity)),
            stats,
            last_oversize_log_ns: AtomicU64::new(0),
            last_reentrant_log_ns: AtomicU64::new(0),
            _marker: PhantomData,
        }
    }

    pub(crate) fn builder_config_from_options(
        options: &aorta_core::publisher::PublisherOptions,
        topic: &str,
    ) -> BuilderConfig {
        let base_capacity = if let Some(size) = options.max_message_size {
            size
        } else if let Some(fixed) = M::fixed_size() {
            fixed
        } else {
            aorta_warn!(
                "Aorta WARN: publisher '{}' created without max_message_size; \
                 using default capacity {} bytes for variable-size message.",
                topic,
                DEFAULT_VAR_CAPACITY
            );
            DEFAULT_VAR_CAPACITY
        };

        BuilderConfig {
            base_capacity,
            growth_policy: BuilderGrowthPolicy::GrowAndWarn,
            reuse_policy: BuilderReusePolicy::ThreadLocal,
            include_publish_timestamp: options.include_publish_timestamp,
        }
    }

    /// The topic this publisher was created for.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// The schema hash attached to this publisher's declarations, if any.
    pub fn schema_hash(&self) -> Option<SchemaHash> {
        self.inner.schema_hash()
    }

    /// Access the underlying core publisher (advanced use).
    pub fn inner(&self) -> &CorePublisher {
        &self.inner
    }

    /// Snapshot of this publisher's runtime counters, see [`PublisherStats`].
    pub fn stats(&self) -> PublisherStats {
        self.stats.snapshot()
    }

    /// One-shot readiness check: is there at least one matching subscriber for
    /// this publisher's key expression right now?
    ///
    /// Reads local routing tables (no network round-trip) and is safe on any
    /// runtime flavor (resolved via `.await`, never zenoh's sync `.wait()`).
    pub async fn matching_status(&self) -> AortaResult<bool> {
        self.inner.matching_status().await
    }

    /// Wait (bounded by `timeout`) until a matching subscriber appears.
    ///
    /// Returns `Ok(true)` once a matching subscriber is visible, or `Ok(false)`
    /// if the timeout elapses first. Intended to gate the first publish on a
    /// fresh client so a put is not dropped before its route is known.
    pub async fn wait_for_matching(&self, timeout: std::time::Duration) -> AortaResult<bool> {
        self.inner.wait_for_matching(timeout).await
    }

    /// Synchronous fire-and-forget publish (auto-managed builder).
    ///
    /// Zero-copy path: the FlatBufferBuilder is collapsed in-place, its
    /// backing Vec wrapped in a `RecyclableBuffer` and published through
    /// `Bytes::from_owner` — no memcpy in steady state.
    ///
    /// Note: fire-and-forget means no acknowledgement, not "never blocks" —
    /// with [`CongestionControl::Block`](aorta_core::qos::CongestionControl::Block)
    /// the call can block while the transport is backpressured.
    pub fn publish<F>(&self, build: F) -> AortaResult<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        let (reentrant, _guard) = ReentrancyGuard::enter(self.publisher_id);
        if reentrant {
            self.stats.reentrant_count.fetch_add(1, Ordering::Relaxed);
            self.maybe_log_reentrant(self.node.now().as_nanos() as u64);
        }

        if reentrant {
            return self.publish_with_fresh_builder(build);
        }

        match self.builder_reuse_policy {
            BuilderReusePolicy::None => self.publish_with_fresh_builder(build),
            BuilderReusePolicy::Mutex => self.publish_with_mutex_builder(build),
            BuilderReusePolicy::ThreadLocal => {
                if self.use_mutex_builder.load(Ordering::Acquire) {
                    self.publish_with_mutex_builder(build)
                } else {
                    self.publish_with_tls_builder(build)
                }
            }
        }
    }

    /// Publish a typed message built by `fill` (see
    /// [`TypedPublishMessage`]).
    ///
    /// ```ignore
    /// publisher.publish_typed(|msg: &mut ChatMsg| {
    ///     msg.text = Some("hello".to_string());
    /// })?; // no .await — synchronous fire-and-forget
    /// ```
    pub fn publish_typed<'d, F>(&self, fill: F) -> AortaResult<()>
    where
        M: TypedPublishMessage,
        F: FnOnce(&mut M::Msg<'d>),
    {
        let mut msg = M::Msg::default();
        fill(&mut msg);
        self.publish_typed_msg(&msg)
    }

    /// Publish a pre-built typed message instance (by-reference form of
    /// [`publish_typed`](Self::publish_typed)).
    pub fn publish_typed_msg(&self, msg: &M::Msg<'_>) -> AortaResult<()>
    where
        M: TypedPublishMessage,
    {
        self.publish(|builder, header| {
            let root = M::build_typed_root(builder, header, msg);
            builder.finish(root, None);
        })
    }

    fn current_capacity(&self) -> usize {
        self.stats.current_capacity.load(Ordering::Relaxed)
    }

    /// Obtain a FlatBufferBuilder, preferring a recycled Vec from the pool.
    fn obtain_builder(&self, capacity: usize) -> FlatBufferBuilder<'static> {
        if let Some(vec) = self.pool.pop() {
            self.stats
                .recycle_pool_hit_count
                .fetch_add(1, Ordering::Relaxed);
            builder_from_vec(vec)
        } else {
            self.stats
                .recycle_pool_miss_count
                .fetch_add(1, Ordering::Relaxed);
            // Warm-allocate: use vec![0u8; capacity] instead of Vec::with_capacity
            // to force the OS to back the allocation with physical pages and bring
            // them into the TLB/cache.  This matches the C++ StagedAllocator's
            // Prewarm()/warm-allocate behavior (malloc + memset).
            builder_from_vec(vec![0u8; capacity])
        }
    }

    /// Publish using a fresh (non-cached) builder.  Used for reentrant calls
    /// and the `None` reuse policy.
    fn publish_with_fresh_builder<F>(&self, build: F) -> AortaResult<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        self.stats
            .fresh_builder_publish_count
            .fetch_add(1, Ordering::Relaxed);
        let capacity = self.current_capacity().max(self.base_capacity);
        let builder = self.obtain_builder(capacity);
        let payload_len = self.build_and_publish(builder, build)?;
        self.handle_oversize(payload_len, capacity);
        Ok(())
    }

    fn publish_with_mutex_builder<F>(&self, build: F) -> AortaResult<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        self.stats
            .mutex_builder_publish_count
            .fetch_add(1, Ordering::Relaxed);
        let capacity = self.current_capacity().max(self.base_capacity);
        let mutex = self
            .mutex_builder
            .get_or_init(|| Mutex::new(FlatBufferBuilder::with_capacity(capacity)));
        let mut guard = mutex.lock().expect("publisher mutex poisoned");

        // Take the cached builder out; we need ownership for collapse().
        // Replace with a dummy empty builder (zero-cost: no allocation).
        let builder = std::mem::replace(&mut *guard, FlatBufferBuilder::with_capacity(0));
        let (payload_len, returned_vec) = self.build_and_publish_returning_vec(builder, build)?;

        // Put a fresh builder (from the returned vec) back into the mutex slot.
        // This recycles the allocation for the next publish.
        if let Some(vec) = returned_vec {
            *guard = builder_from_vec(vec);
        } else {
            *guard = self.obtain_builder(capacity);
        }

        self.handle_oversize(payload_len, capacity);
        Ok(())
    }

    fn publish_with_tls_builder<F>(&self, build: F) -> AortaResult<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        let capacity = self.current_capacity().max(self.base_capacity);
        let threshold = tls_large_capacity_threshold();
        if capacity >= threshold {
            self.use_mutex_builder.store(true, Ordering::Release);
            self.stats
                .mutex_mode_switch_count
                .fetch_add(1, Ordering::Relaxed);
            return self.publish_with_mutex_builder(build);
        }
        self.stats
            .tls_builder_publish_count
            .fetch_add(1, Ordering::Relaxed);

        // Take builder from TLS (or create fresh from pool).
        let builder = TLS_PUB_STATE
            .try_with(|tls| {
                let mut vec = tls.borrow_mut();
                for slot in vec.iter_mut() {
                    if slot.publisher_id == self.publisher_id {
                        return slot.builder.take().unwrap_or_else(|| {
                            self.obtain_builder(slot.capacity_hint.max(capacity))
                        });
                    }
                }
                // Slot was already created by ReentrancyGuard::enter, but
                // just in case, create a fresh builder.
                self.obtain_builder(capacity)
            })
            .unwrap_or_else(|_| self.obtain_builder(capacity));

        let capacity_before = capacity;
        let (payload_len, returned_vec) = self.build_and_publish_returning_vec(builder, build)?;

        // Return a builder to TLS from the returned vec (or fresh from pool).
        if payload_len > threshold {
            // Graduated to mutex; don't return to TLS.
            self.switch_to_mutex_on_oversize(capacity);
        } else {
            // Check if the returned vec is oversized compared to steady-state.
            let should_shrink = TLS_PUB_STATE
                .try_with(|tls| {
                    let vec = tls.borrow();
                    for slot in vec.iter() {
                        if slot.publisher_id == self.publisher_id {
                            let hint = slot.capacity_hint.max(payload_len);
                            return hint > 0
                                && returned_vec.as_ref().is_some_and(|v| {
                                    v.capacity() > hint * TLS_SHRINK_FACTOR
                                        && v.capacity() > DEFAULT_VAR_CAPACITY
                                });
                        }
                    }
                    false
                })
                .unwrap_or(false);

            let new_builder = if should_shrink {
                // Drop the oversized vec and create a right-sized builder.
                drop(returned_vec);
                self.obtain_builder(payload_len)
            } else if let Some(vec) = returned_vec {
                builder_from_vec(vec)
            } else {
                self.obtain_builder(capacity)
            };

            let _ = TLS_PUB_STATE.try_with(|tls| {
                let mut vec = tls.borrow_mut();
                for slot in vec.iter_mut() {
                    if slot.publisher_id == self.publisher_id {
                        slot.builder = Some(new_builder);
                        if payload_len > slot.capacity_hint {
                            slot.capacity_hint = payload_len;
                        }
                        return;
                    }
                }
                // Shouldn't happen — ReentrancyGuard::enter already created the slot.
                vec.push(TlsPublisherState {
                    publisher_id: self.publisher_id,
                    builder: Some(new_builder),
                    capacity_hint: capacity,
                    publishing_depth: 0,
                });
            });
        }

        self.handle_oversize(payload_len, capacity_before);
        Ok(())
    }

    /// Core zero-copy publish path.
    ///
    /// Takes ownership of the builder, builds the message, collapses the
    /// builder to get the backing `Vec<u8>`, wraps it in a `RecyclableBuffer`
    /// → `Bytes::from_owner` → `ZBytes` and publishes.  Zero memcpy.
    ///
    /// The `RecyclableBuffer`'s Drop impl returns the Vec to the pool when
    /// Zenoh is done with the payload.
    fn build_and_publish<F>(
        &self,
        mut builder: FlatBufferBuilder<'_>,
        build: F,
    ) -> AortaResult<usize>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        let publish_stamp_ns = if self.include_publish_timestamp {
            self.node.now().as_nanos() as u64
        } else {
            0
        };
        self.stats.publish_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .last_publish_ns
            .store(publish_stamp_ns, Ordering::Relaxed);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let publisher_node = if self.include_publisher_node_in_header {
            self.publisher_node_name.as_str()
        } else {
            ""
        };
        let values = HeaderValues {
            publish_stamp_ns,
            publisher_node,
            sequence,
        };

        let header = M::build_header(&mut builder, &values);
        let build_start = Instant::now();
        build(&mut builder, header);

        // Zero-copy path: collapse the builder to get the backing Vec,
        // wrap in RecyclableBuffer, convert to Bytes, then to ZBytes.
        let (vec, offset) = builder.collapse();
        let payload_len = vec.len() - offset;
        let build_ns = build_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.stats
            .build_total_ns
            .fetch_add(build_ns, Ordering::Relaxed);
        self.stats
            .build_sample_count
            .fetch_add(1, Ordering::Relaxed);
        update_max_u64(&self.stats.build_max_ns, build_ns);

        #[cfg(test)]
        if take_force_publish_error() {
            // Return the vec to the pool even on simulated error.
            self.pool.push(vec);
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
            return Err(AortaError::internal("forced publish error"));
        }

        let rb = RecyclableBuffer {
            vec,
            offset,
            pool: Arc::clone(&self.pool),
        };
        let bytes = Bytes::from_owner(rb);
        let zbytes = ZBytes::from(bytes);
        let transport_start = Instant::now();
        if let Err(err) = self.inner.publish(zbytes) {
            let transport_ns = transport_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.stats
                .transport_total_ns
                .fetch_add(transport_ns, Ordering::Relaxed);
            self.stats
                .transport_sample_count
                .fetch_add(1, Ordering::Relaxed);
            update_max_u64(&self.stats.transport_max_ns, transport_ns);
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        let transport_ns = transport_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.stats
            .transport_total_ns
            .fetch_add(transport_ns, Ordering::Relaxed);
        self.stats
            .transport_sample_count
            .fetch_add(1, Ordering::Relaxed);
        update_max_u64(&self.stats.transport_max_ns, transport_ns);
        Ok(payload_len)
    }

    /// Like `build_and_publish`, but for mutex/TLS paths that want the Vec
    /// back (to reconstruct the cached builder) instead of pooling it.
    ///
    /// On success, the Vec is owned by Zenoh via RecyclableBuffer (will be
    /// pooled on drop), and we pop a fresh Vec from the pool for the caller.
    /// On error, returns the Vec directly.
    fn build_and_publish_returning_vec<F>(
        &self,
        mut builder: FlatBufferBuilder<'static>,
        build: F,
    ) -> AortaResult<(usize, Option<Vec<u8>>)>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
    {
        let publish_stamp_ns = if self.include_publish_timestamp {
            self.node.now().as_nanos() as u64
        } else {
            0
        };
        self.stats.publish_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .last_publish_ns
            .store(publish_stamp_ns, Ordering::Relaxed);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let publisher_node = if self.include_publisher_node_in_header {
            self.publisher_node_name.as_str()
        } else {
            ""
        };
        let values = HeaderValues {
            publish_stamp_ns,
            publisher_node,
            sequence,
        };

        let header = M::build_header(&mut builder, &values);
        let build_start = Instant::now();
        build(&mut builder, header);

        let (vec, offset) = builder.collapse();
        let payload_len = vec.len() - offset;
        let build_ns = build_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.stats
            .build_total_ns
            .fetch_add(build_ns, Ordering::Relaxed);
        self.stats
            .build_sample_count
            .fetch_add(1, Ordering::Relaxed);
        update_max_u64(&self.stats.build_max_ns, build_ns);

        #[cfg(test)]
        if take_force_publish_error() {
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
            // Return the vec to caller so the builder cache can be replenished.
            return Err(AortaError::internal("forced publish error"));
        }

        let rb = RecyclableBuffer {
            vec,
            offset,
            pool: Arc::clone(&self.pool),
        };
        let bytes = Bytes::from_owner(rb);
        let zbytes = ZBytes::from(bytes);
        let transport_start = Instant::now();
        if let Err(err) = self.inner.publish(zbytes) {
            let transport_ns = transport_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.stats
                .transport_total_ns
                .fetch_add(transport_ns, Ordering::Relaxed);
            self.stats
                .transport_sample_count
                .fetch_add(1, Ordering::Relaxed);
            update_max_u64(&self.stats.transport_max_ns, transport_ns);
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        let transport_ns = transport_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.stats
            .transport_total_ns
            .fetch_add(transport_ns, Ordering::Relaxed);
        self.stats
            .transport_sample_count
            .fetch_add(1, Ordering::Relaxed);
        update_max_u64(&self.stats.transport_max_ns, transport_ns);

        // The vec is now owned by RecyclableBuffer (inside Zenoh).
        // Pop a fresh vec from the pool for the caller to rebuild their cache.
        let recycled = self.pool.pop();
        Ok((payload_len, recycled))
    }

    fn switch_to_mutex_on_oversize(&self, capacity: usize) {
        if self
            .use_mutex_builder
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.stats
                .mutex_mode_switch_count
                .fetch_add(1, Ordering::Relaxed);
            self.mutex_builder
                .get_or_init(|| Mutex::new(FlatBufferBuilder::with_capacity(capacity)));
        }
    }

    fn handle_oversize(&self, payload_len: usize, capacity_before: usize) {
        if payload_len <= capacity_before {
            return;
        }

        self.stats.oversize_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .last_oversize_size
            .store(payload_len, Ordering::Relaxed);
        self.maybe_log_oversize(payload_len, capacity_before);

        self.update_capacity_stats(payload_len);
    }

    fn update_capacity_stats(&self, new_capacity: usize) {
        let mut current = self.stats.current_capacity.load(Ordering::Relaxed);
        while new_capacity > current {
            match self.stats.current_capacity.compare_exchange_weak(
                current,
                new_capacity,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(value) => current = value,
            }
        }

        let mut max_seen = self.stats.max_capacity_seen.load(Ordering::Relaxed);
        while new_capacity > max_seen {
            match self.stats.max_capacity_seen.compare_exchange_weak(
                max_seen,
                new_capacity,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(value) => max_seen = value,
            }
        }
    }

    fn maybe_log_oversize(&self, payload_len: usize, capacity_before: usize) {
        let now_ns = self.node.now().as_nanos() as u64;
        if self.should_log(now_ns, &self.last_oversize_log_ns) {
            aorta_warn!(
                "Aorta WARN: publisher '{}' payload {} bytes exceeded capacity {} bytes; \
                 auto-growing builder.",
                self.topic(),
                payload_len,
                capacity_before
            );
        }
    }

    fn maybe_log_reentrant(&self, now_ns: u64) {
        if self.should_log(now_ns, &self.last_reentrant_log_ns) {
            aorta_warn!(
                "Aorta WARN: publisher '{}' re-entrant publish detected; using temporary builder.",
                self.topic()
            );
        }
    }

    fn should_log(&self, now_ns: u64, last_log: &AtomicU64) -> bool {
        let last = last_log.load(Ordering::Relaxed);
        if now_ns.saturating_sub(last) < LOG_INTERVAL_NS {
            return false;
        }
        last_log
            .compare_exchange(last, now_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Synchronous fire-and-forget publish of raw bytes
    ///
    /// Publishes raw byte payload without header injection or FlatBuffers building.
    /// Useful for forwarding/bridging scenarios.
    ///
    /// # Arguments
    /// * `payload` - Raw bytes convertible to ZBytes (Vec, &[u8], Bytes, String, etc.).
    ///   For shared ownership without extra copy, pass `bytes::Bytes`.
    ///
    /// # Returns
    /// * `Ok(())` if publish task was spawned successfully
    pub fn publish_bytes<IntoZBytes>(&self, payload: IntoZBytes) -> AortaResult<()>
    where
        IntoZBytes: Into<ZBytes>,
    {
        let payload: ZBytes = payload.into();
        let now_ns = self.node.now().as_nanos() as u64;
        self.stats.publish_count.fetch_add(1, Ordering::Relaxed);
        self.stats.last_publish_ns.store(now_ns, Ordering::Relaxed);
        #[cfg(test)]
        if take_force_publish_error() {
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
            return Err(AortaError::internal("forced publish error"));
        }
        let result = self.inner.publish(payload);
        if result.is_err() {
            self.stats.send_error_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Undeclare this publisher, consuming it: unregisters the topic
    /// declaration from Zenoh. Call when the publisher is no longer needed
    /// (before [`Node::close`](crate::Node::close)).
    pub async fn undeclare(self) -> AortaResult<()> {
        self.inner.undeclare().await
    }
}

impl<M: SchemaMeta + HeaderInjector> std::fmt::Debug for Publisher<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("topic", &self.topic())
            .field("schema_hash", &self.schema_hash())
            .finish()
    }
}

pub(crate) fn ensure_schema_bfbs<M: SchemaMeta>(
    options: &mut aorta_core::publisher::PublisherOptions,
) -> AortaResult<()> {
    if options.schema_bfbs.is_none() {
        options.schema_bfbs = Some(M::schema_bfbs().to_vec());
        if options.schema_root_type.is_none() {
            options.schema_root_type = M::root_type().map(str::to_string);
        }
    }
    if options.schema_bfbs.is_none() {
        return Err(AortaError::invalid_argument("schema bfbs is required"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../test/publisher_tests.rs"]
mod tests;
