use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex, RwLock as TokioRwLock};

use crate::error::AortaResult;
use crate::key_expr::{normalize_path, normalize_path_with_wildcards, KeyExprBuilder};
use crate::log_prelude::*;
use crate::schema::SchemaHash;
use crate::service::ServiceSchemaInfo;
use crate::session::Session;
use crate::snapshot_queryable::spawn_single_key_snapshot_queryable;
use crate::telemetry::schema::{
    NodeIdentityTelemetry, ResourceInventoryOverflow, ResourceInventoryTelemetry, RuntimeHealth,
    TelemetryClientEntry, TelemetryPublisherEntry, TelemetryResourceCommon, TelemetryServiceEntry,
    TelemetrySubscriberEntry, TELEMETRY_SCHEMA_VERSION,
};
use crate::telemetry::{CommandDispatch, CommandSink, PendingCommands};
use crate::ZBytes;

const DEFAULT_RESOURCE_INVENTORY_HEARTBEAT_MS: u64 = 30_000;
const INVENTORY_COMMAND_CHANNEL_CAPACITY: usize = 256;
const INVENTORY_FLUSH_WORKER_NAME: &str = "aorta-telemetry-inventory-flush";

/// Maximum number of distinct resources tracked per kind in the inventory.
/// Generous — a real node registers orders of magnitude fewer —
/// but bounds memory so a registration leak or hostile peer cannot grow the
/// inventory maps without limit. Registrations past the cap are dropped and
/// counted (never evicting an already-tracked live resource).
const MAX_INVENTORY_RESOURCES_PER_KIND: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelemetryResourceKind {
    Publisher,
    Subscriber,
    Service,
    Client,
}

#[derive(Debug, Clone)]
enum TelemetryCommand {
    RegisterPublisher(TelemetryPublisherEntry),
    RemovePublisher { resource_instance_id: String },
    RegisterSubscriber(TelemetrySubscriberEntry),
    RemoveSubscriber { resource_instance_id: String },
    RegisterService(TelemetryServiceEntry),
    RemoveService { resource_instance_id: String },
    RegisterClient(TelemetryClientEntry),
    RemoveClient { resource_instance_id: String },
}

#[derive(Debug, Default)]
struct ResourceInventoryState {
    inventory_revision: u64,
    publishers: BTreeMap<String, TelemetryPublisherEntry>,
    subscribers: BTreeMap<String, TelemetrySubscriberEntry>,
    services: BTreeMap<String, TelemetryServiceEntry>,
    clients: BTreeMap<String, TelemetryClientEntry>,
    overflow: ResourceInventoryOverflow,
}

#[derive(Default)]
struct ResourceInventoryTelemetryRuntime {
    queryable_stop: Option<oneshot::Sender<()>>,
    worker_stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

type PendingInventoryCommands = PendingCommands<TelemetryCommand>;
type InventoryCommandDispatch = CommandDispatch;
type InventoryCommandSink = CommandSink<TelemetryCommand>;

fn inventory_command_resource_id(command: &TelemetryCommand) -> String {
    match command {
        TelemetryCommand::RegisterPublisher(entry) => entry.common.resource_instance_id.clone(),
        TelemetryCommand::RemovePublisher {
            resource_instance_id,
        } => resource_instance_id.clone(),
        TelemetryCommand::RegisterSubscriber(entry) => entry.common.resource_instance_id.clone(),
        TelemetryCommand::RemoveSubscriber {
            resource_instance_id,
        } => resource_instance_id.clone(),
        TelemetryCommand::RegisterService(entry) => entry.common.resource_instance_id.clone(),
        TelemetryCommand::RemoveService {
            resource_instance_id,
        } => resource_instance_id.clone(),
        TelemetryCommand::RegisterClient(entry) => entry.common.resource_instance_id.clone(),
        TelemetryCommand::RemoveClient {
            resource_instance_id,
        } => resource_instance_id.clone(),
    }
}

pub struct TelemetryResourceGuard {
    kind: TelemetryResourceKind,
    resource_instance_id: String,
    sink: Option<Arc<InventoryCommandSink>>,
    released: AtomicBool,
}

impl std::fmt::Debug for TelemetryResourceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryResourceGuard")
            .field("kind", &self.kind)
            .field("resource_instance_id", &self.resource_instance_id)
            .finish()
    }
}

impl Default for TelemetryResourceGuard {
    fn default() -> Self {
        Self::disabled()
    }
}

impl TelemetryResourceGuard {
    pub fn disabled() -> Self {
        Self {
            kind: TelemetryResourceKind::Publisher,
            resource_instance_id: String::new(),
            sink: None,
            released: AtomicBool::new(true),
        }
    }

    fn new(
        kind: TelemetryResourceKind,
        resource_instance_id: String,
        sink: Option<Arc<InventoryCommandSink>>,
    ) -> Self {
        Self {
            kind,
            resource_instance_id,
            sink,
            released: AtomicBool::new(false),
        }
    }

    pub fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Some(sink) = self.sink.as_ref() else {
            return;
        };

        let command = match self.kind {
            TelemetryResourceKind::Publisher => TelemetryCommand::RemovePublisher {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            TelemetryResourceKind::Subscriber => TelemetryCommand::RemoveSubscriber {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            TelemetryResourceKind::Service => TelemetryCommand::RemoveService {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            TelemetryResourceKind::Client => TelemetryCommand::RemoveClient {
                resource_instance_id: self.resource_instance_id.clone(),
            },
        };

        match sink.enqueue(command) {
            InventoryCommandDispatch::Enqueued
            | InventoryCommandDispatch::Deferred
            | InventoryCommandDispatch::ShuttingDown => {}
            InventoryCommandDispatch::ClosedUnexpected => {
                warn_inventory_command_send_error(
                    &self.resource_instance_id,
                    "inventory removal enqueue failed",
                    "channel closed",
                );
            }
            InventoryCommandDispatch::WorkerSpawnFailed => {
                warn_inventory_command_send_error(
                    &self.resource_instance_id,
                    "inventory removal enqueue failed",
                    "flush worker spawn failed",
                );
            }
        }
    }
}

impl Drop for TelemetryResourceGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct VitalSnapshotTelemetryPublisher {
    session: Session,
    key: String,
    node_id: String,
    group: String,
    /// Static node-identity section of the vital snapshot (the `identity` field).
    identity_base: NodeIdentityTelemetry,
    /// X1 executor spin counter, shared with the bulk runtime publisher; drives
    /// the `health` section. Attached after construction (and after `start`)
    /// when the node is added to an executor, so it is shared with the worker
    /// task via `Arc`. Mirrors the runtime publisher's spin reader.
    executor_spin: Arc<Mutex<Option<Arc<AtomicU64>>>>,
    /// X1 follow-up: cumulative count of caught callback panics (isolated
    /// dispatch errors), attached like `executor_spin`; drives
    /// `health.error_count`. Vital-only (the bulk runtime metrics has no error
    /// field).
    executor_error: Arc<Mutex<Option<Arc<AtomicU64>>>>,
    sequence: Arc<AtomicU64>,
    latest_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    runtime: TokioMutex<ResourceInventoryTelemetryRuntime>,
    command_sink: Mutex<Option<Arc<InventoryCommandSink>>>,
    next_publisher_id: AtomicU64,
    next_subscriber_id: AtomicU64,
    next_service_id: AtomicU64,
    next_client_id: AtomicU64,
    active: AtomicBool,
}

impl std::fmt::Debug for VitalSnapshotTelemetryPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VitalSnapshotTelemetryPublisher")
            .field("key", &self.key)
            .field("node_id", &self.node_id)
            .field("group", &self.group)
            .finish()
    }
}

impl VitalSnapshotTelemetryPublisher {
    pub fn new(
        session: Session,
        key: String,
        node_id: String,
        group: String,
        identity_base: NodeIdentityTelemetry,
    ) -> Self {
        Self {
            session,
            key,
            node_id,
            group,
            identity_base,
            executor_spin: Arc::new(Mutex::new(None)),
            executor_error: Arc::new(Mutex::new(None)),
            sequence: Arc::new(AtomicU64::new(0)),
            latest_payload: Arc::new(TokioRwLock::new(None)),
            runtime: TokioMutex::new(ResourceInventoryTelemetryRuntime::default()),
            command_sink: Mutex::new(None),
            next_publisher_id: AtomicU64::new(0),
            next_subscriber_id: AtomicU64::new(0),
            next_service_id: AtomicU64::new(0),
            next_client_id: AtomicU64::new(0),
            active: AtomicBool::new(false),
        }
    }

    /// The vital snapshot is published whenever ANY of its three sections is
    /// enabled; a disabled section is simply left null in the record.
    pub fn enabled_from_env() -> bool {
        Self::identity_section_enabled()
            || Self::inventory_section_enabled()
            || Self::health_section_enabled()
    }

    pub fn identity_section_enabled() -> bool {
        crate::telemetry::enabled_from_env("AORTA_TELEMETRY_NODE_IDENTITY")
    }

    pub fn inventory_section_enabled() -> bool {
        crate::telemetry::enabled_from_env("AORTA_TELEMETRY_RESOURCE_INVENTORY")
    }

    pub fn health_section_enabled() -> bool {
        crate::telemetry::enabled_from_env("AORTA_TELEMETRY_RUNTIME_HEALTH")
    }

    /// Attach the executor spin counter that drives the `health` section. Shared
    /// (cloned `Arc`) with the bulk runtime publisher's reader. Mirrors
    /// `RuntimeMetricsTelemetryPublisher::set_executor_spin_reader`.
    pub fn set_executor_spin_reader(&self, reader: Arc<AtomicU64>) {
        *self.executor_spin.lock().expect("vital spin reader lock") = Some(reader);
    }

    pub fn clear_executor_spin_reader(&self) {
        *self.executor_spin.lock().expect("vital spin reader lock") = None;
    }

    /// Attach the cumulative caught-panic counter that drives
    /// `health.error_count`. Shared (cloned `Arc`) with the executor that owns
    /// the counter; vital-only (mirrors `set_executor_spin_reader`).
    pub fn set_executor_error_reader(&self, reader: Arc<AtomicU64>) {
        *self.executor_error.lock().expect("vital error reader lock") = Some(reader);
    }

    pub fn clear_executor_error_reader(&self) {
        *self.executor_error.lock().expect("vital error reader lock") = None;
    }

    pub fn heartbeat_interval_from_env() -> Duration {
        // The merged vital snapshot uses the primary telemetry heartbeat var
        // (what node_identity used pre-merge); 30 s default like the old families.
        crate::telemetry::heartbeat_interval_from_env(
            "AORTA_TELEMETRY_HEARTBEAT_MS",
            DEFAULT_RESOURCE_INVENTORY_HEARTBEAT_MS,
        )
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub async fn start(&self) -> AortaResult<()> {
        let mut runtime = self.runtime.lock().await;
        if self.is_active() {
            return Ok(());
        }

        let state = ResourceInventoryState::default();
        // Initial publish carries identity + inventory; `health` appears from the
        // first heartbeat (no spin delta to sample yet at start).
        publish_vital_snapshot(
            &self.session,
            &self.key,
            &self.node_id,
            &self.group,
            &state,
            &self.vital_sections(),
            None,
            &self.latest_payload,
            &self.sequence,
        )
        .await?;

        let (queryable_stop_tx, queryable_stop_rx) = oneshot::channel();
        let queryable_task = match spawn_single_key_snapshot_queryable(
            self.session.clone(),
            self.key.clone(),
            Arc::clone(&self.latest_payload),
            queryable_stop_rx,
        )
        .await
        {
            Ok(task) => task,
            Err(err) => {
                let _ = self.session.inner().delete(&self.key).await;
                *self.latest_payload.write().await = None;
                return Err(err);
            }
        };

        let (command_tx, command_rx) = mpsc::channel(INVENTORY_COMMAND_CHANNEL_CAPACITY);
        let command_sink = InventoryCommandSink::new(
            command_tx,
            PendingInventoryCommands::default(),
            inventory_command_resource_id,
            INVENTORY_FLUSH_WORKER_NAME,
        );
        let (worker_stop_tx, worker_stop_rx) = oneshot::channel();
        let session = self.session.clone();
        let key = self.key.clone();
        let node_id = self.node_id.clone();
        let group = self.group.clone();
        let latest_payload = Arc::clone(&self.latest_payload);
        let sequence = Arc::clone(&self.sequence);
        let heartbeat_interval = Self::heartbeat_interval_from_env();
        let cfg = self.vital_sections();
        // spawn_logged_task (not bare tokio::spawn): most fleet nodes never
        // call stop(), so a worker panic must be logged when it happens or
        // this node's telemetry dies silently forever.
        let worker_task = crate::runtime::spawn_logged_task("telemetry-vital-worker", async move {
            run_vital_worker(
                session,
                key,
                node_id,
                group,
                cfg,
                latest_payload,
                sequence,
                command_rx,
                worker_stop_rx,
                heartbeat_interval,
            )
            .await;
        });

        runtime.queryable_stop = Some(queryable_stop_tx);
        runtime.worker_stop = Some(worker_stop_tx);
        runtime.tasks = vec![queryable_task, worker_task];
        *self.command_sink.lock().expect("inventory command lock") = Some(command_sink);
        self.active.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn stop(&self) -> AortaResult<()> {
        let tasks = {
            let mut runtime = self.runtime.lock().await;
            if !self.is_active() {
                return Ok(());
            }
            self.active.store(false, Ordering::Release);
            if let Some(sink) = self
                .command_sink
                .lock()
                .expect("inventory command lock")
                .take()
            {
                sink.close();
            }
            if let Some(stop) = runtime.queryable_stop.take() {
                let _ = stop.send(());
            }
            if let Some(stop) = runtime.worker_stop.take() {
                let _ = stop.send(());
            }
            std::mem::take(&mut runtime.tasks)
        };

        for task in tasks {
            if let Err(err) = task.await {
                crate::telemetry::warn_task_join_failure("inventory", &err);
            }
        }

        *self.latest_payload.write().await = None;
        self.session.inner().delete(&self.key).await?;
        Ok(())
    }

    pub fn register_publisher(
        &self,
        key_builder: &KeyExprBuilder,
        topic: &str,
        schema_hash: Option<SchemaHash>,
    ) -> AortaResult<TelemetryResourceGuard> {
        let resource_instance_id = format!(
            "publisher:{}",
            self.next_publisher_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = TelemetryPublisherEntry {
            common: build_resource_common(
                &resource_instance_id,
                key_builder,
                topic,
                schema_hash.map(|hash| hash.to_hex()),
                ResourceKindForEntry::Publisher,
            )?,
        };
        self.enqueue(
            TelemetryCommand::RegisterPublisher(entry),
            &resource_instance_id,
        );
        Ok(TelemetryResourceGuard::new(
            TelemetryResourceKind::Publisher,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_service(
        &self,
        key_builder: &KeyExprBuilder,
        service_name: &str,
        full_key: &str,
        schema_info: Option<&ServiceSchemaInfo>,
    ) -> AortaResult<TelemetryResourceGuard> {
        let resource_instance_id = format!(
            "service:{}",
            self.next_service_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = TelemetryServiceEntry {
            common: build_resource_common(
                &resource_instance_id,
                key_builder,
                service_name,
                schema_info.map(|info| info.schema.to_hex()),
                ResourceKindForEntry::Service(full_key),
            )?,
            request_root_type: schema_info.and_then(|info| info.request_root_type.clone()),
            response_root_type: schema_info.and_then(|info| info.response_root_type.clone()),
        };
        self.enqueue(
            TelemetryCommand::RegisterService(entry),
            &resource_instance_id,
        );
        Ok(TelemetryResourceGuard::new(
            TelemetryResourceKind::Service,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_subscriber(
        &self,
        key_builder: &KeyExprBuilder,
        topic: &str,
        schema_hash: Option<SchemaHash>,
    ) -> AortaResult<TelemetryResourceGuard> {
        let resource_instance_id = format!(
            "subscriber:{}",
            self.next_subscriber_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = TelemetrySubscriberEntry {
            common: build_resource_common(
                &resource_instance_id,
                key_builder,
                topic,
                schema_hash.map(|hash| hash.to_hex()),
                ResourceKindForEntry::Subscriber,
            )?,
        };
        self.enqueue(
            TelemetryCommand::RegisterSubscriber(entry),
            &resource_instance_id,
        );
        Ok(TelemetryResourceGuard::new(
            TelemetryResourceKind::Subscriber,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_client(
        &self,
        key_builder: &KeyExprBuilder,
        service_name: &str,
        schema_hash: Option<SchemaHash>,
        request_root_type: Option<String>,
        response_root_type: Option<String>,
    ) -> AortaResult<TelemetryResourceGuard> {
        let resource_instance_id = format!(
            "client:{}",
            self.next_client_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = TelemetryClientEntry {
            common: build_resource_common(
                &resource_instance_id,
                key_builder,
                service_name,
                schema_hash.map(|hash| hash.to_hex()),
                ResourceKindForEntry::Client,
            )?,
            request_root_type,
            response_root_type,
        };
        self.enqueue(
            TelemetryCommand::RegisterClient(entry),
            &resource_instance_id,
        );
        Ok(TelemetryResourceGuard::new(
            TelemetryResourceKind::Client,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    fn command_sink(&self) -> Option<Arc<InventoryCommandSink>> {
        self.command_sink
            .lock()
            .expect("inventory command lock")
            .clone()
    }

    fn enqueue(&self, command: TelemetryCommand, resource_instance_id: &str) {
        let Some(sink) = self.command_sink() else {
            return;
        };
        match sink.enqueue(command) {
            InventoryCommandDispatch::Enqueued
            | InventoryCommandDispatch::Deferred
            | InventoryCommandDispatch::ShuttingDown => {}
            InventoryCommandDispatch::ClosedUnexpected => {
                warn_inventory_command_send_error(
                    resource_instance_id,
                    "inventory registration enqueue failed",
                    "channel closed",
                );
            }
            InventoryCommandDispatch::WorkerSpawnFailed => {
                warn_inventory_command_send_error(
                    resource_instance_id,
                    "inventory registration enqueue failed",
                    "flush worker spawn failed",
                );
            }
        }
    }

    fn vital_sections(&self) -> VitalSections {
        VitalSections {
            identity_base: self.identity_base.clone(),
            executor_spin: Arc::clone(&self.executor_spin),
            executor_error: Arc::clone(&self.executor_error),
            identity_enabled: Self::identity_section_enabled(),
            inventory_enabled: Self::inventory_section_enabled(),
            health_enabled: Self::health_section_enabled(),
        }
    }
}

fn warn_inventory_command_send_error(resource_instance_id: &str, message: &str, detail: &str) {
    aorta_core_warn!(
        "Aorta WARN: {} for '{}': {}",
        message,
        resource_instance_id,
        detail
    );
}

#[derive(Clone, Copy)]
enum ResourceKindForEntry<'a> {
    Publisher,
    Subscriber,
    Service(&'a str),
    Client,
}

fn build_resource_common(
    resource_instance_id: &str,
    key_builder: &KeyExprBuilder,
    name: &str,
    schema_hash: Option<String>,
    kind: ResourceKindForEntry<'_>,
) -> AortaResult<TelemetryResourceCommon> {
    // Subscriber and Client resources may legitimately be wildcard
    // patterns (e.g. `camera/**` or `sensors/*`); Publisher and Service
    // are concrete declarations. Pick the validator that matches each
    // direction so wildcard subscribers are reported in resource
    // inventory with their actual pattern instead of being silently
    // dropped.
    let (resource_name, full_key): (String, String) = match kind {
        ResourceKindForEntry::Publisher => (
            normalize_path(name)?,
            key_builder.topic_key_pub(name)?.into(),
        ),
        ResourceKindForEntry::Subscriber => (
            normalize_path_with_wildcards(name)?,
            key_builder.topic_key_sub(name)?.into(),
        ),
        ResourceKindForEntry::Service(full_key) => (normalize_path(name)?, full_key.to_string()),
        ResourceKindForEntry::Client => (
            normalize_path_with_wildcards(name)?,
            key_builder.service_key_client(name)?.into(),
        ),
    };

    Ok(TelemetryResourceCommon {
        resource_instance_id: resource_instance_id.to_string(),
        resource_name,
        full_key,
        schema_hash,
        declared_at_ns: Some(crate::time::unix_time_ns()),
        labels: BTreeMap::new(),
    })
}

// Telemetry plumbing: session + key + identity + the section config + channels.
#[allow(clippy::too_many_arguments)]
async fn run_vital_worker(
    session: Session,
    key: String,
    node_id: String,
    group: String,
    cfg: VitalSections,
    latest_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    sequence: Arc<AtomicU64>,
    mut command_rx: mpsc::Receiver<TelemetryCommand>,
    mut stop_rx: oneshot::Receiver<()>,
    heartbeat_interval: Duration,
) {
    let mut state = ResourceInventoryState::default();
    let mut prev_spin = 0u64;
    let mut last_progress_ns = 0u64;
    let start_at = tokio::time::Instant::now() + heartbeat_interval;
    let mut interval = tokio::time::interval_at(start_at, heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut stop_rx => break,
            _ = interval.tick() => {
                let health = cfg.health_enabled.then(||
                    sample_health(&cfg.executor_spin, &cfg.executor_error, &mut prev_spin, &mut last_progress_ns));
                if let Err(err) = publish_vital_snapshot(
                    &session, &key, &node_id, &group, &state, &cfg, health.as_ref(),
                    &latest_payload, &sequence,
                ).await {
                    aorta_core_warn!("Aorta WARN: vital telemetry heartbeat publish failed: {}", err);
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                let mut changed = apply_command(&mut state, command);
                while let Ok(command) = command_rx.try_recv() {
                    changed |= apply_command(&mut state, command);
                }
                if changed {
                    let health = cfg.health_enabled.then(||
                        sample_health(&cfg.executor_spin, &cfg.executor_error, &mut prev_spin, &mut last_progress_ns));
                    if let Err(err) = publish_vital_snapshot(
                        &session, &key, &node_id, &group, &state, &cfg, health.as_ref(),
                        &latest_payload, &sequence,
                    ).await {
                        aorta_core_warn!("Aorta WARN: vital telemetry publish failed: {}", err);
                    }
                }
            }
        }
    }
}

/// Inserts `value` under `key`, enforcing the per-kind cardinality cap.
///
/// Returns `true` only when a *new* resource was registered (so the caller bumps
/// the inventory revision), matching the prior `insert(..).is_none()` semantics.
/// Updating an already-tracked resource refreshes it in place and returns
/// `false`. When the map is at capacity and the key is new, the registration is
/// dropped (never evicting a live resource), `dropped` is incremented, and the
/// first overflow per kind is logged.
///
/// The returned bool is the worker's "publish + bump revision" trigger. It is
/// `true` for a new registration and for the **first** drop of each kind — the
/// 0→1 overflow transition, i.e. the moment the inventory becomes incomplete —
/// so that signal reaches consumers promptly instead of waiting for the next
/// heartbeat. Subsequent drops return `false`, so a sustained
/// over-cap flood cannot amplify into a publish storm; the heartbeat carries the
/// climbing counter from then on.
fn insert_capped<V>(
    map: &mut BTreeMap<String, V>,
    dropped: &mut u64,
    key: String,
    value: V,
    kind: &str,
) -> bool {
    use std::collections::btree_map::Entry;
    // Captured before `entry` borrows `map`; only consulted on the vacant path.
    let at_capacity = map.len() >= MAX_INVENTORY_RESOURCES_PER_KIND;
    match map.entry(key) {
        Entry::Occupied(mut slot) => {
            // Refreshing an already-tracked resource: no growth, no revision bump.
            slot.insert(value);
            false
        }
        Entry::Vacant(slot) if at_capacity => {
            *dropped = dropped.saturating_add(1);
            let first_drop = *dropped == 1;
            if first_drop || dropped.is_multiple_of(1024) {
                aorta_core_warn!(
                    "Aorta WARN: telemetry inventory {kind} cardinality cap ({}) reached; \
                     dropping registration {:?} (total {kind} drops: {})",
                    MAX_INVENTORY_RESOURCES_PER_KIND,
                    slot.key(),
                    *dropped
                );
            }
            // Publish once when inventory first becomes incomplete; the heartbeat
            // covers later increments so floods can't trigger a publish per drop.
            first_drop
        }
        Entry::Vacant(slot) => {
            slot.insert(value);
            true
        }
    }
}

fn apply_command(state: &mut ResourceInventoryState, command: TelemetryCommand) -> bool {
    let changed = match command {
        TelemetryCommand::RegisterPublisher(entry) => insert_capped(
            &mut state.publishers,
            &mut state.overflow.publishers_dropped,
            entry.common.resource_instance_id.clone(),
            entry,
            "publisher",
        ),
        TelemetryCommand::RemovePublisher {
            resource_instance_id,
        } => state.publishers.remove(&resource_instance_id).is_some(),
        TelemetryCommand::RegisterSubscriber(entry) => insert_capped(
            &mut state.subscribers,
            &mut state.overflow.subscribers_dropped,
            entry.common.resource_instance_id.clone(),
            entry,
            "subscriber",
        ),
        TelemetryCommand::RemoveSubscriber {
            resource_instance_id,
        } => state.subscribers.remove(&resource_instance_id).is_some(),
        TelemetryCommand::RegisterService(entry) => insert_capped(
            &mut state.services,
            &mut state.overflow.services_dropped,
            entry.common.resource_instance_id.clone(),
            entry,
            "service",
        ),
        TelemetryCommand::RemoveService {
            resource_instance_id,
        } => state.services.remove(&resource_instance_id).is_some(),
        TelemetryCommand::RegisterClient(entry) => insert_capped(
            &mut state.clients,
            &mut state.overflow.clients_dropped,
            entry.common.resource_instance_id.clone(),
            entry,
            "client",
        ),
        TelemetryCommand::RemoveClient {
            resource_instance_id,
        } => state.clients.remove(&resource_instance_id).is_some(),
    };

    if changed {
        state.inventory_revision += 1;
    }
    changed
}

/// Static / configuration inputs the vital worker needs to assemble a
/// `VitalSnapshot`, beyond the live inventory `state`.
struct VitalSections {
    identity_base: NodeIdentityTelemetry,
    executor_spin: Arc<Mutex<Option<Arc<AtomicU64>>>>,
    executor_error: Arc<Mutex<Option<Arc<AtomicU64>>>>,
    identity_enabled: bool,
    inventory_enabled: bool,
    health_enabled: bool,
}

/// Sample the executor spin counter at heartbeat cadence (NEVER per spin — the
/// hot path stays the X1 clock-free increment) and derive the liveness pulse by
/// comparing against the previous tick.
fn sample_health(
    executor_spin: &Mutex<Option<Arc<AtomicU64>>>,
    executor_error: &Mutex<Option<Arc<AtomicU64>>>,
    prev_spin: &mut u64,
    last_progress_ns: &mut u64,
) -> RuntimeHealth {
    let spin_count = executor_spin
        .lock()
        .expect("vital spin reader lock")
        .as_ref()
        .map(|counter| counter.load(Ordering::Relaxed))
        .unwrap_or(0);
    // Raw cumulative caught-panic count (cloud derives the error rate); read the
    // same heartbeat-cheap way as spin_count. 0 when no executor is attached.
    let error_count = executor_error
        .lock()
        .expect("vital error reader lock")
        .as_ref()
        .map(|counter| counter.load(Ordering::Relaxed))
        .unwrap_or(0);
    let alive = spin_count > *prev_spin;
    if alive {
        *last_progress_ns = crate::time::unix_time_ns();
    }
    *prev_spin = spin_count;
    RuntimeHealth {
        spin_count,
        last_progress_ns: *last_progress_ns,
        error_count,
        alive,
    }
}

// Telemetry plumbing: session + key + identity + state/config/health + sink.
#[allow(clippy::too_many_arguments)]
async fn publish_vital_snapshot(
    session: &Session,
    key: &str,
    node_id: &str,
    group: &str,
    state: &ResourceInventoryState,
    cfg: &VitalSections,
    health: Option<&RuntimeHealth>,
    latest_payload: &Arc<TokioRwLock<Option<ZBytes>>>,
    sequence: &Arc<AtomicU64>,
) -> AortaResult<()> {
    let next_sequence = sequence.fetch_add(1, Ordering::Relaxed) + 1;
    let timestamp_ns = crate::time::unix_time_ns();

    // A disabled section is left null in the record (not a dropped topic).
    let identity = if cfg.identity_enabled {
        let mut id = cfg.identity_base.clone();
        id.sequence = next_sequence;
        id.timestamp_ns = timestamp_ns;
        // Live per-heartbeat refresh: the base
        // snapshot is captured at startup, but router connectivity changes —
        // a router crash or link loss must show up as this list draining.
        id.connected_router_zids = crate::telemetry::identity::connected_router_zids(session).await;
        Some(id)
    } else {
        None
    };
    let inventory = cfg.inventory_enabled.then(|| ResourceInventoryTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence: next_sequence,
        timestamp_ns,
        node_id: node_id.to_string(),
        group: group.to_string(),
        inventory_revision: state.inventory_revision,
        publishers: state.publishers.values().cloned().collect(),
        subscribers: state.subscribers.values().cloned().collect(),
        services: state.services.values().cloned().collect(),
        clients: state.clients.values().cloned().collect(),
        overflow: state.overflow.clone(),
    });
    let bytes = ZBytes::from(crate::telemetry::fb::serialize_vital_snapshot(
        TELEMETRY_SCHEMA_VERSION,
        next_sequence,
        timestamp_ns,
        identity.as_ref(),
        inventory.as_ref(),
        health,
    ));
    *latest_payload.write().await = Some(bytes.clone());
    session
        .inner()
        .put(key, bytes)
        .attachment(crate::telemetry::fb::vital_snapshot_schema_attachment())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_health_reads_cumulative_error_count() {
        // The vital health pulse surfaces the executor's cumulative caught-panic
        // count (raw; the cloud derives the rate). With a reader attached,
        // sample_health reflects the live counter; a later panic bumps it.
        let spin = Mutex::new(Some(Arc::new(AtomicU64::new(7))));
        let errors_arc = Arc::new(AtomicU64::new(3));
        let errors = Mutex::new(Some(Arc::clone(&errors_arc)));
        let mut prev_spin = 0u64;
        let mut last_progress_ns = 0u64;

        let health = sample_health(&spin, &errors, &mut prev_spin, &mut last_progress_ns);
        assert_eq!(health.spin_count, 7);
        assert_eq!(
            health.error_count, 3,
            "carries the live cumulative error count"
        );
        assert!(health.alive, "spin advanced 0 -> 7 this tick");

        // A subsequent caught panic bumps the shared counter; the next sample
        // observes the new cumulative total (and no spin advance => not alive).
        errors_arc.fetch_add(1, Ordering::Relaxed);
        let health2 = sample_health(&spin, &errors, &mut prev_spin, &mut last_progress_ns);
        assert_eq!(health2.error_count, 4);
        assert!(!health2.alive, "spin_count did not advance this tick");
    }

    #[test]
    fn sample_health_error_count_zero_when_no_reader_attached() {
        // Before the node is added to an executor, no counters are attached;
        // the health section reports an honest zero (not a spurious value).
        let spin: Mutex<Option<Arc<AtomicU64>>> = Mutex::new(None);
        let errors: Mutex<Option<Arc<AtomicU64>>> = Mutex::new(None);
        let mut prev_spin = 0u64;
        let mut last_progress_ns = 0u64;
        let health = sample_health(&spin, &errors, &mut prev_spin, &mut last_progress_ns);
        assert_eq!(health.error_count, 0);
        assert_eq!(health.spin_count, 0);
    }

    fn publisher_entry(resource_instance_id: &str, resource_name: &str) -> TelemetryPublisherEntry {
        let full_key = crate::key_expr::KeyExprBuilder::new("default")
            .expect("default group is valid")
            .topic_key_pub(resource_name)
            .expect("test resource_name is valid")
            .to_string();
        TelemetryPublisherEntry {
            common: TelemetryResourceCommon {
                resource_instance_id: resource_instance_id.to_string(),
                resource_name: resource_name.to_string(),
                full_key,
                schema_hash: None,
                declared_at_ns: Some(1),
                labels: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn pending_inventory_commands_keep_latest_state_per_resource() {
        let mut pending = PendingInventoryCommands::default();
        pending.push(
            "publisher:1".to_string(),
            TelemetryCommand::RegisterPublisher(publisher_entry("publisher:1", "demo/topic")),
        );
        pending.push(
            "publisher:1".to_string(),
            TelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            },
        );

        let (_, pending_command) = pending.peek_next().expect("pending command");
        assert!(matches!(
            pending_command.command,
            TelemetryCommand::RemovePublisher { ref resource_instance_id }
                if resource_instance_id == "publisher:1"
        ));
    }

    #[test]
    fn pending_inventory_commands_preserve_newer_state_after_stale_send_ack() {
        let mut pending = PendingInventoryCommands::default();
        pending.push(
            "publisher:1".to_string(),
            TelemetryCommand::RegisterPublisher(publisher_entry("publisher:1", "demo/topic")),
        );

        let (resource_instance_id, first_pending) = pending.peek_next().expect("pending command");
        pending.push(
            "publisher:1".to_string(),
            TelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            },
        );
        pending.acknowledge_sent(&resource_instance_id, first_pending.sequence);

        let (_, latest_pending) = pending.peek_next().expect("latest pending command");
        assert!(matches!(
            latest_pending.command,
            TelemetryCommand::RemovePublisher { ref resource_instance_id }
                if resource_instance_id == "publisher:1"
        ));
    }

    #[test]
    fn inventory_command_sink_returns_shutting_down_after_close() {
        let (tx, _rx) = mpsc::channel(1);
        let sink = InventoryCommandSink::new(
            tx,
            PendingInventoryCommands::default(),
            inventory_command_resource_id,
            INVENTORY_FLUSH_WORKER_NAME,
        );
        sink.close();

        let outcome = sink.enqueue(TelemetryCommand::RemovePublisher {
            resource_instance_id: "publisher:1".to_string(),
        });

        assert_eq!(outcome, InventoryCommandDispatch::ShuttingDown);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inventory_command_sink_channel_full_flushes_only_latest_pending_state() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = InventoryCommandSink::new(
            tx,
            PendingInventoryCommands::default(),
            inventory_command_resource_id,
            INVENTORY_FLUSH_WORKER_NAME,
        );

        assert_eq!(
            sink.enqueue(TelemetryCommand::RegisterPublisher(publisher_entry(
                "publisher:1",
                "demo/topic",
            ))),
            InventoryCommandDispatch::Enqueued
        );
        assert_eq!(
            sink.enqueue(TelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            }),
            InventoryCommandDispatch::Deferred
        );
        assert_eq!(
            sink.enqueue(TelemetryCommand::RegisterPublisher(publisher_entry(
                "publisher:1",
                "demo/topic/latest",
            ))),
            InventoryCommandDispatch::Deferred
        );

        {
            let pending = sink.pending.lock().expect("pending lock");
            assert_eq!(pending.resources.len(), 1);
        }

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("first command timeout")
            .expect("first command");
        assert!(matches!(
            first,
            TelemetryCommand::RegisterPublisher(ref entry)
                if entry.common.resource_name == "demo/topic"
        ));

        tokio::task::yield_now().await;

        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("second command timeout")
            .expect("second command");
        assert!(matches!(
            second,
            TelemetryCommand::RegisterPublisher(ref entry)
                if entry.common.resource_name == "demo/topic/latest"
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!sink.flush_worker_scheduled.load(Ordering::Acquire));
    }

    #[test]
    fn apply_command_caps_publisher_cardinality_and_counts_overflow() {
        let mut state = ResourceInventoryState::default();

        // Fill the publisher map exactly to capacity; every insert is a new
        // registration that bumps the revision and is not dropped.
        for i in 0..MAX_INVENTORY_RESOURCES_PER_KIND {
            let id = format!("publisher:{i}");
            let changed = apply_command(
                &mut state,
                TelemetryCommand::RegisterPublisher(publisher_entry(&id, "demo/topic")),
            );
            assert!(changed, "registration {i} within cap should be accepted");
        }
        assert_eq!(state.publishers.len(), MAX_INVENTORY_RESOURCES_PER_KIND);
        assert_eq!(state.overflow.publishers_dropped, 0);
        let revision_at_cap = state.inventory_revision;

        // First overflow: dropped (no eviction), counted, and it MUST publish so
        // the 0→1 "inventory is now incomplete" signal reaches consumers promptly
        // rather than waiting for the next heartbeat. The publish is
        // gated by the revision bump, so the revision advances too.
        let changed = apply_command(
            &mut state,
            TelemetryCommand::RegisterPublisher(publisher_entry(
                "publisher:overflow",
                "demo/topic",
            )),
        );
        assert!(
            changed,
            "the first overflow drop must publish so overflow:1 is seen promptly"
        );
        assert_eq!(state.publishers.len(), MAX_INVENTORY_RESOURCES_PER_KIND);
        assert_eq!(state.overflow.publishers_dropped, 1);
        assert_eq!(state.inventory_revision, revision_at_cap + 1);
        assert!(!state.publishers.contains_key("publisher:overflow"));
        assert!(state.publishers.contains_key("publisher:0"));

        // Subsequent overflows still count but must NOT publish, so a sustained
        // over-cap flood cannot amplify into a publish storm; the heartbeat
        // carries the climbing counter from here on.
        let revision_after_first_drop = state.inventory_revision;
        let changed = apply_command(
            &mut state,
            TelemetryCommand::RegisterPublisher(publisher_entry(
                "publisher:overflow2",
                "demo/topic",
            )),
        );
        assert!(!changed, "later overflow drops must not trigger a publish");
        assert_eq!(state.overflow.publishers_dropped, 2);
        assert_eq!(state.inventory_revision, revision_after_first_drop);
    }

    #[test]
    fn apply_command_updates_existing_resource_at_cap_without_dropping() {
        let mut state = ResourceInventoryState::default();
        for i in 0..MAX_INVENTORY_RESOURCES_PER_KIND {
            apply_command(
                &mut state,
                TelemetryCommand::RegisterPublisher(publisher_entry(
                    &format!("publisher:{i}"),
                    "demo/topic",
                )),
            );
        }

        // Re-registering an already-tracked id updates it in place: not a new
        // registration (revision unchanged), and never an overflow drop.
        let changed = apply_command(
            &mut state,
            TelemetryCommand::RegisterPublisher(publisher_entry(
                "publisher:0",
                "demo/topic/updated",
            )),
        );
        assert!(!changed);
        assert_eq!(state.overflow.publishers_dropped, 0);
        assert_eq!(state.publishers.len(), MAX_INVENTORY_RESOURCES_PER_KIND);
        assert_eq!(
            state.publishers["publisher:0"].common.resource_name,
            "demo/topic/updated"
        );

        // Freeing a slot lets a brand-new registration succeed again.
        assert!(apply_command(
            &mut state,
            TelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            },
        ));
        assert!(apply_command(
            &mut state,
            TelemetryCommand::RegisterPublisher(publisher_entry("publisher:new", "demo/topic")),
        ));
        assert_eq!(state.overflow.publishers_dropped, 0);
    }
}
