use crate::error::{AortaError, AortaResult};
use crate::event_queue::{
    AdmissionStatsReader, CallbackGroupHandle, CallbackGroupType, EventQueueSender,
};
use crate::executor::{CallbackGroupRegistry, ExecutorClassPolicyOptions};
use crate::key_expr::group_from_env_or_default;
use crate::key_expr::KeyExprBuilder;
use crate::liveliness::LivelinessManager;
use crate::log_prelude::*;
use crate::logging;
use crate::publisher::{Publisher, PublisherOptions};
use crate::runtime_env;
use crate::schema::{SchemaAttachPolicy, SchemaHash, SchemaMeta};
use crate::schema_registry::SchemaRegistry;
use crate::session::{format_runtime_config_snapshot, ConfigSource, Session};
use crate::subscriber::Subscriber;
use crate::telemetry::schema::{
    HostIdentity, NodeIdentityTelemetry, NodeOptions, TelemetryNodeMetadata,
};
use crate::telemetry::{RuntimeMetricsTelemetryPublisher, VitalSnapshotTelemetryPublisher};
use crate::time::{Clock, ClockMode, Time};
use crate::timer::{ExecutorTimerScheduler, Timer, TimerOptions};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::sync::OnceCell as TokioOnceCell;

#[derive(Debug, Clone)]
pub struct Node {
    name: String,
    id: String,
    group: String,
    session: Session,
    key_builder: KeyExprBuilder,
    clock: Clock,
    schema_registry: SchemaRegistry,
    schema_attach_policy: SchemaAttachPolicy,
    event_sender: Arc<RwLock<Option<EventQueueSender>>>,
    /// Serializes the whole ATTACHMENT TRANSITION — the claim or release of
    /// every executor-owned slot on this node (event sender, timer scheduler,
    /// callback-group registry, telemetry readers). `Executor::try_add_node`
    /// and `Executor::detach_nodes` each hold it across their ENTIRE sequence.
    /// Two consequences, both required for correctness:
    ///
    /// * the ownership check and the sender install are one atomic step, so
    ///   two executors racing to attach the same node cannot both observe it
    ///   as unowned and both claim it;
    /// * a predecessor's cleanup cannot interleave with a successor's attach,
    ///   so the node can never advertise a live sender while parts of that
    ///   executor's configuration have already been stripped.
    ///
    /// Lock order: this mutex is always taken BEFORE `event_sender`, never
    /// the other way round.
    attachment_txn: Arc<Mutex<()>>,
    /// Identity of the executor that most recently ATTACHED this node
    /// (`0` = never attached). Survives `detach_nodes()` on purpose: it is what
    /// distinguishes "the executor this node just left" from "some other
    /// executor" once the attachment generation has already moved on
    /// Live snapshot producers hold that executor's queue, so
    /// reattaching to it cannot split callbacks, while reattaching anywhere
    /// else can.
    last_attachment_owner: Arc<AtomicU64>,
    /// Live callback producers that snapshot the node's event sender at
    /// CONSTRUCTION and therefore cannot follow a re-attachment: timers
    /// (`create_timer*`) and services. Subscribers are not counted — they
    /// re-validate their snapshot against the attachment generation on every
    /// event. A handoff (detach + attach elsewhere) is refused while this is
    /// non-zero, because it would split one node's callbacks across two
    /// executors.
    snapshot_producers: Arc<AtomicUsize>,
    /// Monotonic counter bumped on EVERY executor attach/detach on this node.
    /// Callback producers that snapshot the event sender at
    /// construction tag the snapshot with the generation it came from and
    /// validate it with a single relaxed load per event, so a snapshot can
    /// never outlive the attachment it was taken from — which is what made an
    /// application's explicitly created executor silently inert.
    event_sender_generation: Arc<AtomicU64>,
    timer_scheduler: Arc<RwLock<Option<ExecutorTimerScheduler>>>,
    callback_group_registry: Arc<RwLock<Option<CallbackGroupRegistry>>>,
    executor_class_policy_defaults: Arc<RwLock<ExecutorClassPolicyOptions>>,
    identity: Arc<NodeIdentity>,
    options: NodeOptions,
    host_identity: HostIdentity,
    process_start_time_ns: u64,
    vital_telemetry: Arc<VitalSnapshotTelemetryPublisher>,
    runtime_metrics_telemetry: Arc<RuntimeMetricsTelemetryPublisher>,
    pub(crate) log_topic_forwarder: Arc<logging::LogTopicForwarder>,
    liveliness: Arc<Mutex<Option<Arc<LivelinessManager>>>>,
    liveliness_once: Arc<TokioOnceCell<()>>,
    /// The node's Flow root scope (`Node::root_scope`), created
    /// lazily; `close()` cancels it with `CancelReason::Shutdown`.
    root_scope: Arc<std::sync::OnceLock<crate::flow::Scope>>,
}

#[derive(Debug)]
struct NodeIdentity {
    key: String,
    released: AtomicBool,
}

impl NodeIdentity {
    fn register(name: &str, group: &str) -> AortaResult<Arc<Self>> {
        let key = format!("{}/{}", group, name);
        let registry = node_name_registry();
        let mut guard = registry.lock().expect("node name registry lock");
        if !guard.insert(key.clone()) {
            return Err(AortaError::invalid_argument(format!(
                "node name '{}' already exists in group '{}'",
                name, group
            )));
        }
        drop(guard);

        Ok(Arc::new(Self {
            key,
            released: AtomicBool::new(false),
        }))
    }

    fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let registry = node_name_registry();
        let mut guard = registry.lock().expect("node name registry lock");
        guard.remove(&self.key);
    }
}

impl Drop for NodeIdentity {
    fn drop(&mut self) {
        self.release();
    }
}

/// Owner id for a sender installed directly by a test with no executor object
/// behind it. Distinct from every `next_executor_id()` value (those count up
/// from 1), so a synthetic attachment can never be mistaken for a real
/// executor's reattach privilege.
#[cfg(test)]
pub(crate) const SYNTHETIC_TEST_OWNER: u64 = u64::MAX;

/// Keeps a node's live-snapshot-producer count accurate for as long as the
/// producer that snapshotted the event sender exists. Held by
/// [`crate::timer::Timer`] and [`crate::service::Service`].
#[derive(Debug)]
pub struct SnapshotProducerGuard {
    producers: Arc<AtomicUsize>,
}

impl Drop for SnapshotProducerGuard {
    fn drop(&mut self) {
        self.producers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn node_name_registry() -> &'static Mutex<HashSet<String>> {
    static REGISTRY: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashSet::new()))
}

impl Node {
    pub async fn new(name: impl Into<String>, group: impl AsRef<str>) -> AortaResult<Self> {
        Self::with_options(name, group, NodeOptions::default()).await
    }

    pub async fn with_options(
        name: impl Into<String>,
        group: impl AsRef<str>,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        let name = name.into();
        if logging::auto_init_enabled() {
            if let Err(err) = logging::init_from_env_with_node_once(&name) {
                aorta_core_warn!("Aorta WARN: logging auto init failed: {}", err);
            }
        }
        let resolved = crate::session::resolve_config_from_env_or_local_default().await?;
        Self::with_config_source_and_options(name, group, resolved.config, resolved.source, options)
            .await
    }

    pub async fn with_zenoh_config(
        name: impl Into<String>,
        group: impl AsRef<str>,
        config: zenoh::Config,
    ) -> AortaResult<Self> {
        Self::with_zenoh_config_and_options(name, group, config, NodeOptions::default()).await
    }

    pub async fn with_zenoh_config_and_options(
        name: impl Into<String>,
        group: impl AsRef<str>,
        config: zenoh::Config,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        Self::with_config_and_options(name, group, config, options).await
    }

    pub async fn with_zenoh_config_file(
        name: impl Into<String>,
        group: impl AsRef<str>,
        path: impl AsRef<std::path::Path>,
    ) -> AortaResult<Self> {
        Self::with_zenoh_config_file_and_options(name, group, path, NodeOptions::default()).await
    }

    pub async fn with_zenoh_config_file_and_options(
        name: impl Into<String>,
        group: impl AsRef<str>,
        path: impl AsRef<std::path::Path>,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        let config = zenoh::Config::from_file(path)?;
        Self::with_config_and_options(name, group, config, options).await
    }

    pub async fn with_config(
        name: impl Into<String>,
        group: impl AsRef<str>,
        config: zenoh::Config,
    ) -> AortaResult<Self> {
        Self::with_config_and_options(name, group, config, NodeOptions::default()).await
    }

    pub async fn with_config_and_options(
        name: impl Into<String>,
        group: impl AsRef<str>,
        config: zenoh::Config,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        Self::with_config_source_and_options(name, group, config, ConfigSource::Explicit, options)
            .await
    }

    /// Internal constructor that carries the true config provenance so the
    /// startup snapshot lines don't claim `source=explicit` for configs that
    /// were actually resolved from the environment or a silent fallback.
    async fn with_config_source_and_options(
        name: impl Into<String>,
        _group: impl AsRef<str>,
        config: zenoh::Config,
        source: ConfigSource,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(AortaError::invalid_argument("node name is empty"));
        }

        let raw_group = group_from_env_or_default();
        let key_builder = KeyExprBuilder::new(&raw_group)?;
        let group = key_builder.group().to_string();

        if logging::auto_init_enabled() {
            if let Err(err) = logging::init_from_env_with_node_once(&name) {
                aorta_core_warn!("Aorta WARN: logging auto init failed: {}", err);
            }
        }

        let config_snapshot = format_runtime_config_snapshot(&config, source);
        let identity = NodeIdentity::register(&name, &group)?;
        let session = Session::open_with_source(config, source).await?;
        let clock = Clock::new(ClockMode::System);
        let schema_registry = SchemaRegistry::new(session.clone(), key_builder.clone());
        let schema_attach_policy = SchemaAttachPolicy::from_env();
        let event_sender = Arc::new(RwLock::new(None));
        let attachment_txn = Arc::new(Mutex::new(()));
        let last_attachment_owner = Arc::new(AtomicU64::new(0));
        let snapshot_producers = Arc::new(AtomicUsize::new(0));
        let event_sender_generation = Arc::new(AtomicU64::new(0));
        let timer_scheduler = Arc::new(RwLock::new(None));
        let callback_group_registry = Arc::new(RwLock::new(None));
        let executor_class_policy_defaults =
            Arc::new(RwLock::new(ExecutorClassPolicyOptions::default()));
        let options = NodeOptions {
            telemetry: options.telemetry.normalized(),
        };
        let host_identity = crate::telemetry::resolve_host_identity();
        // Stable, fleet-meaningful node id derived from (host, group, name) with
        // an `AORTA_NODE_ID` override — same node across restarts.
        let id = crate::telemetry::identity::derive_node_id(&host_identity.host_id, &group, &name)?;
        let process_start_time_ns = crate::time::unix_time_ns();
        let liveliness = Arc::new(Mutex::new(None));
        let liveliness_once = Arc::new(TokioOnceCell::new());

        if let Err(err) = logging::set_default_context(&name, &group) {
            aorta_core_warn!("Aorta WARN: logging default context not set: {}", err);
        }

        aorta_core_info!(
            "Aorta INFO: node created: name={}, id={}, group={}, session_config={}",
            name,
            id,
            group,
            config_snapshot
        );

        if SchemaRegistry::queryable_enabled_from_env() {
            if let Err(err) = schema_registry.start_schema_queryables().await {
                aorta_core_warn!("Aorta WARN: schema queryable disabled: {}", err);
            }
        }

        let session_zid = session.zid();
        let base_identity_telemetry =
            crate::telemetry::identity::build_node_identity_telemetry_base(
                &id,
                &name,
                &group,
                &host_identity,
                process_start_time_ns,
                std::process::id(),
                &session_zid,
                &options.telemetry,
            );
        let vital_telemetry = Arc::new(VitalSnapshotTelemetryPublisher::new(
            session.clone(),
            key_builder.telemetry_vital_snapshot_key(&id)?,
            id.clone(),
            group.clone(),
            base_identity_telemetry,
        ));
        let runtime_metrics_telemetry = Arc::new(RuntimeMetricsTelemetryPublisher::new(
            session.clone(),
            key_builder.telemetry_bulk_runtime_key(&id)?,
            id.clone(),
            group.clone(),
        ));

        let node = Self {
            name,
            id,
            group,
            session,
            key_builder,
            clock,
            schema_registry,
            schema_attach_policy,
            event_sender,
            attachment_txn,
            last_attachment_owner,
            snapshot_producers,
            event_sender_generation,
            timer_scheduler,
            callback_group_registry,
            executor_class_policy_defaults,
            identity,
            options,
            host_identity,
            process_start_time_ns,
            vital_telemetry,
            runtime_metrics_telemetry,
            log_topic_forwarder: Arc::new(logging::LogTopicForwarder::default()),
            liveliness,
            root_scope: Arc::new(std::sync::OnceLock::new()),
            liveliness_once,
        };

        // Auto-declare liveliness token unless explicitly disabled
        if Self::liveliness_enabled_from_env() {
            if let Err(err) = node.declare_liveliness().await {
                aorta_core_warn!("Aorta WARN: liveliness token disabled: {}", err);
            }
        }

        if VitalSnapshotTelemetryPublisher::enabled_from_env() {
            // Register the merged VitalSnapshot schema (by-hash) so recorders
            // resolve it from the hash attached to each vital put. The schema
            // transitively covers the nested node_identity + resource_inventory
            // tables via `include`.
            node.register_telemetry_schema(
                crate::telemetry::fb::vital_snapshot_schema_hash(),
                crate::telemetry::fb::TELEMETRY_VITAL_SNAPSHOT_BFBS,
                "vital snapshot",
            )
            .await;
            if let Err(err) = node.vital_telemetry.start().await {
                aorta_core_warn!("Aorta WARN: vital telemetry disabled: {}", err);
            }
        }

        if RuntimeMetricsTelemetryPublisher::enabled_from_env() {
            node.register_telemetry_schema(
                crate::telemetry::fb::runtime_metrics_schema_hash(),
                crate::telemetry::fb::TELEMETRY_RUNTIME_METRICS_BFBS,
                "runtime metrics",
            )
            .await;
            if let Err(err) = node.runtime_metrics_telemetry.start().await {
                aorta_core_warn!("Aorta WARN: runtime telemetry disabled: {}", err);
            }
        }

        if let Err(err) = crate::logging::start_topic_forwarding(&node).await {
            aorta_core_warn!(
                "Aorta WARN: log topic forwarding start failed (will retry on next Node init): {}",
                err
            );
        }

        Ok(node)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub async fn declare_liveliness(&self) -> AortaResult<()> {
        self.liveliness_once
            .get_or_try_init(|| async {
                let manager = LivelinessManager::declare(
                    self.session.inner(),
                    &self.key_builder,
                    &self.name,
                    &self.id,
                )
                .await?;
                let mut guard = self.liveliness.lock().map_err(|e| {
                    AortaError::internal(format!("failed to lock liveliness: {}", e))
                })?;
                if guard.is_none() {
                    *guard = Some(manager);
                }
                Ok::<(), AortaError>(())
            })
            .await?;
        Ok(())
    }

    pub fn has_liveliness(&self) -> bool {
        self.liveliness
            .lock()
            .ok()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
    }

    fn liveliness_enabled_from_env() -> bool {
        match runtime_env::var("AORTA_LIVELINESS") {
            Some(val) => val != "0" && val.to_lowercase() != "false",
            None => true, // Default: enabled
        }
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub fn now(&self) -> Time {
        crate::time::now()
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn session_zid(&self) -> String {
        self.session.zid()
    }

    pub fn key_builder(&self) -> &KeyExprBuilder {
        &self.key_builder
    }

    pub fn schema_registry(&self) -> &SchemaRegistry {
        &self.schema_registry
    }

    pub fn schema_attach_policy(&self) -> SchemaAttachPolicy {
        self.schema_attach_policy
    }

    pub fn node_options(&self) -> &NodeOptions {
        &self.options
    }

    pub fn telemetry_metadata(&self) -> &TelemetryNodeMetadata {
        &self.options.telemetry
    }

    pub fn host_identity(&self) -> &HostIdentity {
        &self.host_identity
    }

    pub fn process_start_time_ns(&self) -> u64 {
        self.process_start_time_ns
    }

    pub fn build_node_identity_telemetry(&self) -> NodeIdentityTelemetry {
        crate::telemetry::build_node_identity_telemetry(self)
    }

    pub fn vital_telemetry(&self) -> Arc<VitalSnapshotTelemetryPublisher> {
        Arc::clone(&self.vital_telemetry)
    }

    pub fn vital_telemetry_key(&self) -> &str {
        self.vital_telemetry.key()
    }

    pub fn runtime_metrics_telemetry(&self) -> Arc<RuntimeMetricsTelemetryPublisher> {
        Arc::clone(&self.runtime_metrics_telemetry)
    }

    pub fn runtime_metrics_telemetry_key(&self) -> &str {
        self.runtime_metrics_telemetry.key()
    }

    pub async fn close(&self) -> AortaResult<()> {
        // Release the forwarder's Publisher -> Session ownership before
        // telemetry and transport teardown. Clones share the same join state.
        self.log_topic_forwarder.stop().await;
        // Drop liveliness token before closing session so undeclare can be propagated
        // while the session is still active.
        if let Ok(mut guard) = self.liveliness.lock() {
            *guard = None;
        } else {
            aorta_core_warn!("Aorta WARN: failed to lock liveliness during node close");
        }
        self.runtime_metrics_telemetry
            .clear_executor_admission_reader();
        self.runtime_metrics_telemetry.clear_executor_spin_reader();
        self.runtime_metrics_telemetry.clear_executor_flow_readers();
        self.vital_telemetry.clear_executor_spin_reader();
        self.vital_telemetry.clear_executor_error_reader();
        if let Err(err) = self.vital_telemetry.stop().await {
            aorta_core_warn!("Aorta WARN: vital telemetry shutdown failed: {}", err);
        }
        if let Err(err) = self.runtime_metrics_telemetry.stop().await {
            aorta_core_warn!("Aorta WARN: runtime telemetry shutdown failed: {}", err);
        }
        self.schema_registry.stop_schema_queryables().await;
        // Flow: shut down the node's root scope tree — cancel, then a
        // BOUNDED 500ms drain so cooperative children unwind
        // against a live transport; defiant children only delay close by
        // the window (bounded-teardown posture preserved).
        {
            // Force-init instead of peeking: a
            // clone's FIRST root_scope()/poll_fd() call racing this close()
            // could otherwise create a root scope the peek never saw — its
            // reader tasks would run on past node/session teardown.
            // With get_or_init both sides converge on ONE instance: either
            // we cancel the scope the racer got, or the racer gets the
            // (already-cancelled) scope we created here.
            let scope = self.root_scope();
            scope.cancel(crate::flow::CancelReason::Shutdown);
            // A BOUNDED drain window between the
            // cancel and transport loss — cooperative children get 500ms to
            // observe the token and unwind instead of failing mid-operation
            // against a dying session. (close() also walks the subtree,
            // which prunes the dead-child Weak registries in passing.) The
            // node keeps its bounded-teardown posture: on timeout we WARN
            // with the live count and proceed.
            if tokio::time::timeout(std::time::Duration::from_millis(500), scope.close())
                .await
                .is_err()
            {
                aorta_core_warn!(
                    "Aorta WARN: node close: flow root scope still has {} live subtree children after the 500ms drain window — proceeding with session close",
                    scope.inner.subtree_in_flight()
                );
            }
        }
        let close_result = self.session.close().await;
        crate::shm::cleanup_orphaned_segments();
        self.identity.release();
        close_result
    }

    /// Install `sender` as this node's executor ingress and return the new
    /// attachment generation. The generation is bumped AFTER the
    /// cell is written, so an observer that sees the new generation also sees
    /// the new sender.
    /// Install `sender` as this node's executor ingress, recording
    /// `owner_id` as the attaching executor, and return the new attachment
    /// generation. The generation is bumped AFTER the cell is written, so an
    /// observer that sees the new generation also sees the new sender.
    pub(crate) fn set_event_sender(&self, sender: EventQueueSender, owner_id: u64) -> u64 {
        if let Ok(mut guard) = self.event_sender.write() {
            *guard = Some(sender);
        }
        self.last_attachment_owner
            .store(owner_id, Ordering::Release);
        self.event_sender_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Identity of the executor that most recently attached this node; `0`
    /// when it has never been attached. Unlike
    /// [`Node::executor_attachment_generation`] this does NOT change on
    /// detach — see [`Node::last_attachment_owner`] (the field docs).
    pub fn last_attachment_owner(&self) -> u64 {
        self.last_attachment_owner.load(Ordering::Acquire)
    }

    pub(crate) fn clear_event_sender(&self) {
        if let Ok(mut guard) = self.event_sender.write() {
            *guard = None;
        }
        self.event_sender_generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Generation of this node's CURRENT executor attachment;
    /// `0` means no executor was ever attached. Bumped by every attach and
    /// every detach, so it also distinguishes "re-attached to the same
    /// executor" from "still the original attachment".
    pub fn executor_attachment_generation(&self) -> u64 {
        self.event_sender_generation.load(Ordering::Acquire)
    }

    pub(crate) fn event_sender_generation_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.event_sender_generation)
    }

    /// Enter this node's attachment transaction — see [`Node::attachment_txn`].
    ///
    /// Hold the returned guard across the WHOLE claim or release sequence.
    /// Recovers from poisoning: the critical section only moves handles
    /// between slots (no user code), so a lock poisoned by a panic elsewhere
    /// still protects structurally valid state.
    pub fn attachment_transaction(&self) -> std::sync::MutexGuard<'_, ()> {
        self.attachment_txn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Count of live callback producers that snapshot the event sender and so
    /// cannot follow a re-attachment (timers and services) — see
    /// [`Node::snapshot_producers`].
    pub fn live_snapshot_producer_count(&self) -> usize {
        self.snapshot_producers.load(Ordering::Acquire)
    }

    /// Register a producer that snapshots this node's event sender. The
    /// returned guard decrements on drop and must be stored FOR THE PRODUCER'S
    /// WHOLE LIFETIME.
    /// The node's executor sender for a producer that will SNAPSHOT it — a
    /// timer or a service — together with the guard that keeps
    /// [`Node::live_snapshot_producer_count`] accurate for that producer's
    /// lifetime. Store the guard inside the producer.
    ///
    /// Subscribers must NOT use this: they re-validate their snapshot against
    /// the attachment generation on every event and therefore follow the node
    /// across a handoff, so they must not block one.
    pub fn callback_event_sender_for_snapshot_producer(
        &self,
    ) -> Option<(EventQueueSender, SnapshotProducerGuard)> {
        let sender = self.callback_event_sender()?;
        Some((sender, self.register_snapshot_producer()))
    }

    pub(crate) fn register_snapshot_producer(&self) -> SnapshotProducerGuard {
        self.snapshot_producers.fetch_add(1, Ordering::AcqRel);
        SnapshotProducerGuard {
            producers: Arc::clone(&self.snapshot_producers),
        }
    }

    /// Whether `other` is the SAME underlying node instance, not merely a node
    /// with the same name/id. `Node` is a cheap `Arc`-backed handle, so every
    /// clone of one node compares equal here.
    pub fn is_same_instance(&self, other: &Node) -> bool {
        Arc::ptr_eq(&self.event_sender, &other.event_sender)
    }

    pub(crate) fn set_timer_scheduler(&self, scheduler: ExecutorTimerScheduler) {
        if let Ok(mut guard) = self.timer_scheduler.write() {
            *guard = Some(scheduler);
        }
    }

    pub(crate) fn clear_timer_scheduler(&self) {
        if let Ok(mut guard) = self.timer_scheduler.write() {
            *guard = None;
        }
    }

    pub(crate) fn set_callback_group_registry(&self, registry: CallbackGroupRegistry) {
        if let Ok(mut guard) = self.callback_group_registry.write() {
            *guard = Some(registry);
        }
    }

    pub(crate) fn clear_callback_group_registry(&self) {
        if let Ok(mut guard) = self.callback_group_registry.write() {
            *guard = None;
        }
    }

    pub(crate) fn set_executor_admission_reader(&self, reader: AdmissionStatsReader) {
        self.runtime_metrics_telemetry
            .set_executor_admission_reader(reader);
    }

    pub(crate) fn clear_executor_admission_reader(&self) {
        self.runtime_metrics_telemetry
            .clear_executor_admission_reader();
    }

    pub(crate) fn set_executor_flow_readers(
        &self,
        readers: crate::telemetry::runtime::FlowExecutorReaders,
    ) {
        self.runtime_metrics_telemetry
            .set_executor_flow_readers(readers);
    }

    pub(crate) fn clear_executor_flow_readers(&self) {
        self.runtime_metrics_telemetry.clear_executor_flow_readers();
    }

    pub(crate) fn set_executor_spin_reader(&self, reader: Arc<AtomicU64>) {
        // Shared (cloned Arc) by both the bulk runtime metrics (raw spin_count)
        // and the vital health pulse (alive / last_progress derivation).
        self.vital_telemetry
            .set_executor_spin_reader(Arc::clone(&reader));
        self.runtime_metrics_telemetry
            .set_executor_spin_reader(reader);
    }

    pub(crate) fn clear_executor_spin_reader(&self) {
        self.vital_telemetry.clear_executor_spin_reader();
        self.runtime_metrics_telemetry.clear_executor_spin_reader();
    }

    pub(crate) fn set_executor_error_reader(&self, reader: Arc<AtomicU64>) {
        // Vital-only: the cumulative caught-panic count feeds the vital health
        // pulse (RuntimeHealth.error_count). The bulk runtime metrics has no
        // error field, so it is not fanned there.
        self.vital_telemetry.set_executor_error_reader(reader);
    }

    pub(crate) fn clear_executor_error_reader(&self) {
        self.vital_telemetry.clear_executor_error_reader();
    }

    pub(crate) fn set_executor_class_policy_defaults(&self, defaults: ExecutorClassPolicyOptions) {
        if let Ok(mut guard) = self.executor_class_policy_defaults.write() {
            *guard = defaults;
        }
    }

    pub fn executor_class_policy_defaults(&self) -> ExecutorClassPolicyOptions {
        self.executor_class_policy_defaults
            .read()
            .map(|guard| *guard)
            .unwrap_or_default()
    }

    pub fn has_event_sender(&self) -> bool {
        self.event_sender
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .map(|sender| !sender.is_closed())
            .unwrap_or(false)
    }

    /// `Condition::FdReadable`: drain a pollable fd into a
    /// depth-1 latest-value [`Watch`](crate::watch::Watch). The fd is made
    /// non-blocking by the call; the reader task exits when the watch is
    /// dropped or the fd reaches EOF. NOT a hard-RT path.
    #[cfg(unix)]
    pub async fn poll_fd(
        &self,
        fd: std::os::unix::io::OwnedFd,
    ) -> crate::error::AortaResult<std::sync::Arc<crate::watch::Watch>> {
        // The reader rides the node root scope — accounted by the
        // close() drain and cancelled at node teardown.
        crate::flow::readiness::poll_fd_into_watch(fd, &self.root_scope()).await
    }

    /// [`poll_fd`](Self::poll_fd) under an EXPLICIT scope (the reader is
    /// accounted by — and cancelled with — that scope instead of the node
    /// root).
    #[cfg(unix)]
    pub async fn poll_fd_in(
        &self,
        fd: std::os::unix::io::OwnedFd,
        scope: &crate::flow::Scope,
    ) -> crate::error::AortaResult<std::sync::Arc<crate::watch::Watch>> {
        crate::flow::readiness::poll_fd_into_watch(fd, scope).await
    }

    /// The node's Flow root scope: the natural parent for
    /// node-lifetime activities. Created lazily; `close()` cancels it with
    /// [`CancelReason::Shutdown`](crate::flow::CancelReason), cascading to
    /// every scope/task derived from it. `close()` then gives the tree a
    /// BOUNDED drain window (500ms) before session teardown — cooperative
    /// children usually unwind in time, but the node does not wait forever:
    /// pair with `scope.shutdown(..)` yourself when you need a full drain.
    pub fn root_scope(&self) -> crate::flow::Scope {
        self.root_scope
            .get_or_init(|| {
                crate::flow::Scope::new_root(crate::flow::ScopeOpts {
                    name: "node-root",
                    ..Default::default()
                })
            })
            .clone()
    }

    pub fn callback_event_sender(&self) -> Option<EventQueueSender> {
        self.event_sender
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .filter(|sender| !sender.is_closed())
    }

    pub(crate) fn event_sender(&self) -> Option<EventQueueSender> {
        self.callback_event_sender()
    }

    pub(crate) fn event_sender_cell(&self) -> Arc<RwLock<Option<EventQueueSender>>> {
        Arc::clone(&self.event_sender)
    }

    pub(crate) fn timer_scheduler(&self) -> Option<ExecutorTimerScheduler> {
        self.timer_scheduler
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
    }

    pub fn create_callback_group(
        &self,
        group_type: CallbackGroupType,
    ) -> AortaResult<CallbackGroupHandle> {
        self.create_callback_group_named(group_type, None)
    }

    /// Like [`Self::create_callback_group`] but attaches a human label used by
    /// the scheduler tracer (`sched.group_wait:<name>` / `sched.callback:<name>`
    /// and the per-callback `args.group`) so multi-group deployments are legible.
    pub fn create_callback_group_named(
        &self,
        group_type: CallbackGroupType,
        name: Option<&str>,
    ) -> AortaResult<CallbackGroupHandle> {
        let registry = self
            .callback_group_registry
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .ok_or_else(|| {
                AortaError::invalid_argument(
                    "callback group requires an attached executor; \
                     ensure the node is registered with an executor first",
                )
            })?;
        Ok(registry.create_group_named(group_type, name))
    }

    pub fn remove_callback_group(&self, handle: CallbackGroupHandle) -> AortaResult<bool> {
        let registry = self
            .callback_group_registry
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .ok_or_else(|| {
                AortaError::invalid_argument(
                    "callback group requires an attached executor; \
                     ensure the node is registered with an executor first",
                )
            })?;
        Ok(registry.remove_group(handle))
    }

    /// Best-effort registration of a telemetry family's FlatBuffers schema in
    /// the by-hash store, so recorders can resolve it from the schema-hash
    /// attached to each telemetry put. Failure only means recorders fall back to
    /// raw bytes for that family, so it is logged rather than propagated.
    async fn register_telemetry_schema(&self, hash: SchemaHash, bfbs: &[u8], what: &str) {
        if let Err(err) = self
            .schema_registry
            .ensure_schema(hash, bfbs.to_vec())
            .await
        {
            aorta_core_warn!(
                "Aorta WARN: {} telemetry schema registration failed: {}",
                what,
                err
            );
        }
    }

    pub async fn register_schema_for_topic_bfbs(
        &self,
        topic: &str,
        bfbs: Vec<u8>,
        root_type: Option<String>,
    ) -> AortaResult<SchemaHash> {
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        self.schema_registry
            .ensure_schema(hash, bfbs.clone())
            .await?;
        self.schema_registry
            .register_schema_by_key_topic(topic, hash, bfbs, root_type)
            .await?;
        Ok(hash)
    }

    pub async fn register_schema_for_topic<T: SchemaMeta>(
        &self,
        topic: &str,
    ) -> AortaResult<SchemaHash> {
        self.register_schema_for_topic_bfbs(
            topic,
            T::schema_bfbs().to_vec(),
            T::root_type().map(str::to_string),
        )
        .await
    }

    pub async fn create_publisher(&self, topic: &str) -> AortaResult<Publisher> {
        Publisher::new(self, topic).await
    }

    pub async fn create_publisher_with_bfbs(
        &self,
        topic: &str,
        bfbs: Vec<u8>,
    ) -> AortaResult<Publisher> {
        let options = PublisherOptions {
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            schema_bfbs: Some(bfbs),
            qos: Default::default(),
            ..Default::default()
        };
        Publisher::new_with_options(self, topic, options).await
    }

    pub async fn create_publisher_with_schema<T: SchemaMeta>(
        &self,
        topic: &str,
    ) -> AortaResult<Publisher> {
        let options = PublisherOptions {
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            schema_bfbs: Some(T::schema_bfbs().to_vec()),
            schema_root_type: T::root_type().map(str::to_string),
            qos: Default::default(),
            ..Default::default()
        };
        Publisher::new_with_options(self, topic, options).await
    }

    pub async fn create_publisher_with_options(
        &self,
        topic: &str,
        options: PublisherOptions,
    ) -> AortaResult<Publisher> {
        Publisher::new_with_options(self, topic, options).await
    }

    pub async fn create_subscriber<F>(&self, topic: &str, callback: F) -> AortaResult<Subscriber>
    where
        F: Fn(&crate::ZBytes, &crate::subscriber::MessageContext) + Send + Sync + 'static,
    {
        Subscriber::new(self, topic, callback).await
    }

    pub async fn create_subscriber_with_qos<F>(
        &self,
        topic: &str,
        callback: F,
        qos: crate::qos::QoS,
    ) -> AortaResult<Subscriber>
    where
        F: Fn(&crate::ZBytes, &crate::subscriber::MessageContext) + Send + Sync + 'static,
    {
        let options = crate::subscriber::SubscriberOptions::new().with_qos(qos);
        self.create_subscriber_with_options(topic, callback, options)
            .await
    }

    pub async fn create_subscriber_with_options<F>(
        &self,
        topic: &str,
        callback: F,
        mut options: crate::subscriber::SubscriberOptions,
    ) -> AortaResult<Subscriber>
    where
        F: Fn(&crate::ZBytes, &crate::subscriber::MessageContext) + Send + Sync + 'static,
    {
        let defaults = self.executor_class_policy_defaults();
        defaults.apply_subscriber_overflow_compat(&mut options);
        Subscriber::new_with_options(self, topic, callback, options).await
    }

    /// Creates a pull-mode (`DeliveryMode::InlinePull`) subscriber backed by a
    /// Zenoh-native keep-last-`depth` watch. No callback; drain via
    /// `Subscriber::recv` / `try_recv` / `recv_timeout` (Tier B-pull).
    pub async fn create_subscriber_pull(
        &self,
        topic: &str,
        depth: usize,
    ) -> AortaResult<Subscriber> {
        let options = crate::subscriber::SubscriberOptions::new().with_native_ring(depth);
        Subscriber::new_pull(self, topic, options).await
    }

    /// Pull-mode subscriber with full options control (Tier B-pull).
    pub async fn create_subscriber_pull_with_options(
        &self,
        topic: &str,
        options: crate::subscriber::SubscriberOptions,
    ) -> AortaResult<Subscriber> {
        Subscriber::new_pull(self, topic, options).await
    }

    /// Create a raw [capture](crate::capture) subscriber over `selectors` — a
    /// recorder/observer snoop that delivers concrete key + raw payload +
    /// attachment, with no header parse / dedup / reorder. The callback runs on
    /// the Zenoh callback thread; keep it short (push to your own queue).
    pub async fn create_capture_subscriber<F>(
        &self,
        selectors: &[crate::capture::CaptureSelector],
        callback: F,
    ) -> AortaResult<crate::capture::CaptureSubscriber>
    where
        F: Fn(&crate::capture::CaptureEvent<'_>) + Send + Sync + 'static,
    {
        crate::capture::CaptureSubscriber::new(
            self.session().inner(),
            self.key_builder(),
            selectors,
            callback,
        )
        .await
    }

    /// Capture the current context state under `ctx/<pattern>` as a one-shot
    /// wildcard GET: each value arrives as a `Snapshot` sample,
    /// terminated by a single `SnapshotDone`.
    pub async fn capture_context_snapshot<F>(
        &self,
        pattern: &str,
        deadline: std::time::Duration,
        callback: F,
    ) -> AortaResult<()>
    where
        F: Fn(&crate::capture::CaptureEvent<'_>) + Send + Sync + 'static,
    {
        crate::capture::capture_context_snapshot(
            self.session().inner(),
            self.key_builder(),
            pattern,
            deadline,
            callback,
        )
        .await
    }

    /// Creates a periodic timer that invokes a callback at regular intervals
    ///
    /// The timer sends events to the event queue, ensuring callbacks are executed
    /// on the executor thread. The timer starts automatically unless configured otherwise.
    ///
    /// # Arguments
    /// * `period` - Duration between timer ticks
    /// * `callback` - Function to invoke on each tick
    ///
    /// # Returns
    /// A Timer instance that runs until dropped or explicitly stopped
    ///
    /// # Errors
    /// Returns an error if:
    /// - The period is zero
    /// - The event queue is not initialized (timer requires an executor)
    ///
    /// # Example
    /// ```no_run
    /// use aorta_core::node::Node;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> aorta_core::error::AortaResult<()> {
    /// let node = Node::new("timer_node", "default").await?;
    /// let timer = node.create_timer(Duration::from_millis(100), || {
    ///     println!("Timer tick!");
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_timer<F>(&self, period: Duration, callback: F) -> AortaResult<Timer>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.create_timer_with_options(period, callback, TimerOptions::default())
            .await
    }

    /// Creates a periodic timer with custom options
    ///
    /// # Arguments
    /// * `period` - Duration between timer ticks
    /// * `callback` - Function to invoke on each tick
    /// * `options` - Timer configuration options
    ///
    /// # Example
    /// ```no_run
    /// use aorta_core::node::Node;
    /// use aorta_core::timer::TimerOptions;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> aorta_core::error::AortaResult<()> {
    /// let node = Node::new("timer_node", "default").await?;
    /// let options = TimerOptions::new()
    ///     .with_auto_start(false)
    ///     .with_invoke_on_missed(true);
    /// let timer = node.create_timer_with_options(
    ///     Duration::from_millis(100),
    ///     || println!("Tick"),
    ///     options
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_timer_with_options<F>(
        &self,
        period: Duration,
        callback: F,
        mut options: TimerOptions,
    ) -> AortaResult<Timer>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let defaults = self.executor_class_policy_defaults();
        let (resolved_policy, resolved_timeout) = defaults
            .resolve_timer_overflow_compat(options.overflow_policy, options.overflow_block_timeout);
        options.overflow_policy = resolved_policy;
        options.overflow_block_timeout = resolved_timeout;

        // The timer SNAPSHOTS this sender, so it is registered as a producer
        // that blocks an executor handoff.
        let (event_sender, producer_guard) = self
            .callback_event_sender_for_snapshot_producer()
            .ok_or_else(|| {
                AortaError::internal(
                    "timer requires an executor with event queue; \
                     ensure Node is registered with an executor before creating timers",
                )
            })?;

        Ok(Timer::new(period, callback, event_sender, options)?
            .with_snapshot_producer_guard(producer_guard))
    }
}

fn normalize_group(group: &str) -> String {
    if group.trim().is_empty() {
        "default".to_string()
    } else {
        group.trim().to_string()
    }
}

#[cfg(test)]
#[path = "../test/node_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/node_lifecycle_tests.rs"]
mod lifecycle_tests;
