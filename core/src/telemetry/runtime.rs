use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex, RwLock as TokioRwLock};

use crate::client::ClientStats;
use crate::error::AortaResult;
use crate::event_queue::AdmissionStatsReader;
use crate::key_expr::normalize_path;
use crate::log_prelude::*;
use crate::publisher::PublisherRuntimeStats;
use crate::schema::SchemaHash;
use crate::service::ServiceStats;
use crate::session::Session;
use crate::snapshot_queryable::spawn_single_key_snapshot_queryable;
use crate::subscriber::SubscriberRuntimeStats;
use crate::telemetry::schema::{
    RuntimeMetricsTelemetry, TelemetryClientRuntimeMetrics, TelemetryExecutorRuntimeMetrics,
    TelemetryPublisherRuntimeMetrics, TelemetryServiceRuntimeMetrics,
    TelemetrySubscriberRuntimeMetrics, TELEMETRY_SCHEMA_VERSION,
};
use crate::telemetry::{CommandDispatch, CommandSink, PendingCommands};
use crate::ZBytes;

const DEFAULT_RUNTIME_TELEMETRY_HEARTBEAT_MS: u64 = 5_000;
const RUNTIME_COMMAND_CHANNEL_CAPACITY: usize = 256;
const RUNTIME_FLUSH_WORKER_NAME: &str = "aorta-telemetry-runtime-flush";
const RUNTIME_EXECUTOR_RESOURCE_ID: &str = "executor";

type PublisherReader = Arc<dyn Fn() -> PublisherRuntimeStats + Send + Sync>;
type SubscriberReader = Arc<dyn Fn() -> SubscriberRuntimeStats + Send + Sync>;
type ServiceReader = Arc<dyn Fn() -> ServiceStats + Send + Sync>;
type ClientReader = Arc<dyn Fn() -> ClientStats + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeResourceKind {
    Publisher,
    Subscriber,
    Service,
    Client,
}

#[derive(Clone)]
enum RuntimeTelemetryCommand {
    RegisterPublisher(RuntimePublisherReaderEntry),
    RemovePublisher { resource_instance_id: String },
    RegisterSubscriber(RuntimeSubscriberReaderEntry),
    RemoveSubscriber { resource_instance_id: String },
    RegisterService(RuntimeServiceReaderEntry),
    RemoveService { resource_instance_id: String },
    RegisterClient(RuntimeClientReaderEntry),
    RemoveClient { resource_instance_id: String },
    SetExecutorReader(AdmissionStatsReader),
    ClearExecutorReader,
    SetSpinReader(Arc<AtomicU64>),
    ClearSpinReader,
    SetFlowReaders(FlowExecutorReaders),
    ClearFlowReaders,
}

#[derive(Clone)]
struct RuntimePublisherReaderEntry {
    resource_instance_id: String,
    resource_name: String,
    schema_hash: Option<String>,
    reader: PublisherReader,
}

#[derive(Clone)]
struct RuntimeSubscriberReaderEntry {
    resource_instance_id: String,
    resource_name: String,
    schema_hash: Option<String>,
    reader: SubscriberReader,
}

#[derive(Clone)]
struct RuntimeServiceReaderEntry {
    resource_instance_id: String,
    resource_name: String,
    schema_hash: Option<String>,
    reader: ServiceReader,
}

#[derive(Clone)]
struct RuntimeClientReaderEntry {
    resource_instance_id: String,
    resource_name: String,
    schema_hash: Option<String>,
    reader: ClientReader,
}

#[derive(Default)]
struct RuntimeMetricsState {
    publishers: BTreeMap<String, RuntimePublisherReaderEntry>,
    subscribers: BTreeMap<String, RuntimeSubscriberReaderEntry>,
    services: BTreeMap<String, RuntimeServiceReaderEntry>,
    clients: BTreeMap<String, RuntimeClientReaderEntry>,
    executor: Option<AdmissionStatsReader>,
    executor_spin: Option<Arc<AtomicU64>>,
    executor_flow: Option<FlowExecutorReaders>,
}

/// Flow observability readers : the reserved-lane stats source
/// and the deadline-expiry drop counter, attached alongside the admission
/// reader when a node joins an executor.
#[derive(Clone)]
pub struct FlowExecutorReaders {
    pub lane: Arc<crate::event_queue::ReservedLane>,
    pub dropped_expired: Arc<AtomicU64>,
    /// Telemetry v6: per-pool-lane counter source.
    pub lane_hooks: Arc<crate::executor::LaneHookSlot>,
}

#[derive(Default)]
struct RuntimeMetricsTelemetryRuntime {
    queryable_stop: Option<oneshot::Sender<()>>,
    worker_stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

type PendingRuntimeCommands = PendingCommands<RuntimeTelemetryCommand>;
type RuntimeCommandDispatch = CommandDispatch;
type RuntimeCommandSink = CommandSink<RuntimeTelemetryCommand>;

fn runtime_command_resource_id(command: &RuntimeTelemetryCommand) -> String {
    match command {
        RuntimeTelemetryCommand::RegisterPublisher(entry) => entry.resource_instance_id.clone(),
        RuntimeTelemetryCommand::RemovePublisher {
            resource_instance_id,
        } => resource_instance_id.clone(),
        RuntimeTelemetryCommand::RegisterSubscriber(entry) => entry.resource_instance_id.clone(),
        RuntimeTelemetryCommand::RemoveSubscriber {
            resource_instance_id,
        } => resource_instance_id.clone(),
        RuntimeTelemetryCommand::RegisterService(entry) => entry.resource_instance_id.clone(),
        RuntimeTelemetryCommand::RemoveService {
            resource_instance_id,
        } => resource_instance_id.clone(),
        RuntimeTelemetryCommand::RegisterClient(entry) => entry.resource_instance_id.clone(),
        RuntimeTelemetryCommand::RemoveClient {
            resource_instance_id,
        } => resource_instance_id.clone(),
        RuntimeTelemetryCommand::SetExecutorReader(_)
        | RuntimeTelemetryCommand::ClearExecutorReader => "executor".to_string(),
        RuntimeTelemetryCommand::SetFlowReaders(_) | RuntimeTelemetryCommand::ClearFlowReaders => {
            "executor_flow".to_string()
        }
        RuntimeTelemetryCommand::SetSpinReader(_) | RuntimeTelemetryCommand::ClearSpinReader => {
            "executor_spin".to_string()
        }
    }
}

pub struct RuntimeTelemetryGuard {
    kind: RuntimeResourceKind,
    resource_instance_id: String,
    sink: Option<Arc<RuntimeCommandSink>>,
    released: AtomicBool,
}

impl std::fmt::Debug for RuntimeTelemetryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeTelemetryGuard")
            .field("kind", &self.kind)
            .field("resource_instance_id", &self.resource_instance_id)
            .finish()
    }
}

impl Default for RuntimeTelemetryGuard {
    fn default() -> Self {
        Self::disabled()
    }
}

impl RuntimeTelemetryGuard {
    pub fn disabled() -> Self {
        Self {
            kind: RuntimeResourceKind::Publisher,
            resource_instance_id: String::new(),
            sink: None,
            released: AtomicBool::new(true),
        }
    }

    fn new(
        kind: RuntimeResourceKind,
        resource_instance_id: String,
        sink: Option<Arc<RuntimeCommandSink>>,
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
            RuntimeResourceKind::Publisher => RuntimeTelemetryCommand::RemovePublisher {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            RuntimeResourceKind::Subscriber => RuntimeTelemetryCommand::RemoveSubscriber {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            RuntimeResourceKind::Service => RuntimeTelemetryCommand::RemoveService {
                resource_instance_id: self.resource_instance_id.clone(),
            },
            RuntimeResourceKind::Client => RuntimeTelemetryCommand::RemoveClient {
                resource_instance_id: self.resource_instance_id.clone(),
            },
        };

        match sink.enqueue(command) {
            RuntimeCommandDispatch::Enqueued
            | RuntimeCommandDispatch::Deferred
            | RuntimeCommandDispatch::ShuttingDown => {}
            RuntimeCommandDispatch::ClosedUnexpected => {
                warn_runtime_command_send_error(
                    &self.resource_instance_id,
                    "runtime telemetry removal enqueue failed",
                    "channel closed",
                );
            }
            RuntimeCommandDispatch::WorkerSpawnFailed => {
                warn_runtime_command_send_error(
                    &self.resource_instance_id,
                    "runtime telemetry removal enqueue failed",
                    "flush worker spawn failed",
                );
            }
        }
    }
}

impl Drop for RuntimeTelemetryGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct RuntimeMetricsTelemetryPublisher {
    session: Session,
    key: String,
    node_id: String,
    group: String,
    sequence: Arc<AtomicU64>,
    latest_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    runtime: TokioMutex<RuntimeMetricsTelemetryRuntime>,
    command_sink: Mutex<Option<Arc<RuntimeCommandSink>>>,
    next_publisher_id: AtomicU64,
    next_subscriber_id: AtomicU64,
    next_service_id: AtomicU64,
    next_client_id: AtomicU64,
    active: AtomicBool,
}

impl std::fmt::Debug for RuntimeMetricsTelemetryPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeMetricsTelemetryPublisher")
            .field("key", &self.key)
            .field("node_id", &self.node_id)
            .field("group", &self.group)
            .finish()
    }
}

impl RuntimeMetricsTelemetryPublisher {
    pub fn new(session: Session, key: String, node_id: String, group: String) -> Self {
        Self {
            session,
            key,
            node_id,
            group,
            sequence: Arc::new(AtomicU64::new(0)),
            latest_payload: Arc::new(TokioRwLock::new(None)),
            runtime: TokioMutex::new(RuntimeMetricsTelemetryRuntime::default()),
            command_sink: Mutex::new(None),
            next_publisher_id: AtomicU64::new(0),
            next_subscriber_id: AtomicU64::new(0),
            next_service_id: AtomicU64::new(0),
            next_client_id: AtomicU64::new(0),
            active: AtomicBool::new(false),
        }
    }

    pub fn enabled_from_env() -> bool {
        crate::telemetry::enabled_from_env("AORTA_TELEMETRY_RUNTIME")
    }

    pub fn heartbeat_interval_from_env() -> Duration {
        crate::telemetry::heartbeat_interval_from_env(
            "AORTA_TELEMETRY_RUNTIME_HEARTBEAT_MS",
            DEFAULT_RUNTIME_TELEMETRY_HEARTBEAT_MS,
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
        let heartbeat_interval = Self::heartbeat_interval_from_env();
        let state = RuntimeMetricsState::default();
        self.publish_snapshot(&state, heartbeat_interval).await?;

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

        let (command_tx, command_rx) = mpsc::channel(RUNTIME_COMMAND_CHANNEL_CAPACITY);
        let command_sink = RuntimeCommandSink::new(
            command_tx,
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID),
            runtime_command_resource_id,
            RUNTIME_FLUSH_WORKER_NAME,
        );
        let (worker_stop_tx, worker_stop_rx) = oneshot::channel();
        // spawn_logged_task: a worker panic must be logged when it happens;
        // see inventory.rs for the rationale.
        let worker_task = crate::runtime::spawn_logged_task(
            "telemetry-runtime-worker",
            run_runtime_worker(
                self.session.clone(),
                self.key.clone(),
                self.node_id.clone(),
                self.group.clone(),
                Arc::clone(&self.latest_payload),
                Arc::clone(&self.sequence),
                command_rx,
                worker_stop_rx,
                heartbeat_interval,
            ),
        );

        runtime.queryable_stop = Some(queryable_stop_tx);
        runtime.worker_stop = Some(worker_stop_tx);
        runtime.tasks = vec![queryable_task, worker_task];
        *self.command_sink.lock().expect("runtime command lock") = Some(command_sink);
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
                .expect("runtime command lock")
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
                crate::telemetry::warn_task_join_failure("runtime", &err);
            }
        }

        *self.latest_payload.write().await = None;
        self.session.inner().delete(&self.key).await?;
        Ok(())
    }

    pub fn set_executor_admission_reader(&self, reader: AdmissionStatsReader) {
        self.enqueue(
            RuntimeTelemetryCommand::SetExecutorReader(reader),
            "executor",
        );
    }

    pub fn clear_executor_admission_reader(&self) {
        self.enqueue(RuntimeTelemetryCommand::ClearExecutorReader, "executor");
    }

    pub fn set_executor_spin_reader(&self, reader: Arc<AtomicU64>) {
        self.enqueue(
            RuntimeTelemetryCommand::SetSpinReader(reader),
            "executor_spin",
        );
    }

    pub fn clear_executor_spin_reader(&self) {
        self.enqueue(RuntimeTelemetryCommand::ClearSpinReader, "executor_spin");
    }

    pub fn set_executor_flow_readers(&self, readers: FlowExecutorReaders) {
        self.enqueue(
            RuntimeTelemetryCommand::SetFlowReaders(readers),
            "executor_flow",
        );
    }

    pub fn clear_executor_flow_readers(&self) {
        self.enqueue(RuntimeTelemetryCommand::ClearFlowReaders, "executor_flow");
    }

    pub fn register_publisher(
        &self,
        topic: &str,
        schema_hash: Option<SchemaHash>,
        reader: PublisherReader,
    ) -> AortaResult<RuntimeTelemetryGuard> {
        let resource_instance_id = format!(
            "publisher:{}",
            self.next_publisher_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = RuntimePublisherReaderEntry {
            resource_instance_id: resource_instance_id.clone(),
            resource_name: normalize_path(topic)?,
            schema_hash: schema_hash.map(|hash| hash.to_hex()),
            reader,
        };
        self.enqueue(
            RuntimeTelemetryCommand::RegisterPublisher(entry),
            &resource_instance_id,
        );
        Ok(RuntimeTelemetryGuard::new(
            RuntimeResourceKind::Publisher,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_service(
        &self,
        service_name: &str,
        schema_hash: Option<SchemaHash>,
        reader: ServiceReader,
    ) -> AortaResult<RuntimeTelemetryGuard> {
        let resource_instance_id = format!(
            "service:{}",
            self.next_service_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = RuntimeServiceReaderEntry {
            resource_instance_id: resource_instance_id.clone(),
            resource_name: normalize_path(service_name)?,
            schema_hash: schema_hash.map(|hash| hash.to_hex()),
            reader,
        };
        self.enqueue(
            RuntimeTelemetryCommand::RegisterService(entry),
            &resource_instance_id,
        );
        Ok(RuntimeTelemetryGuard::new(
            RuntimeResourceKind::Service,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_subscriber(
        &self,
        topic: &str,
        schema_hash: Option<SchemaHash>,
        reader: SubscriberReader,
    ) -> AortaResult<RuntimeTelemetryGuard> {
        let resource_instance_id = format!(
            "subscriber:{}",
            self.next_subscriber_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = RuntimeSubscriberReaderEntry {
            resource_instance_id: resource_instance_id.clone(),
            resource_name: normalize_path(topic)?,
            schema_hash: schema_hash.map(|hash| hash.to_hex()),
            reader,
        };
        self.enqueue(
            RuntimeTelemetryCommand::RegisterSubscriber(entry),
            &resource_instance_id,
        );
        Ok(RuntimeTelemetryGuard::new(
            RuntimeResourceKind::Subscriber,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    pub fn register_client(
        &self,
        service_name: &str,
        schema_hash: Option<SchemaHash>,
        reader: ClientReader,
    ) -> AortaResult<RuntimeTelemetryGuard> {
        let resource_instance_id = format!(
            "client:{}",
            self.next_client_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let entry = RuntimeClientReaderEntry {
            resource_instance_id: resource_instance_id.clone(),
            resource_name: normalize_path(service_name)?,
            schema_hash: schema_hash.map(|hash| hash.to_hex()),
            reader,
        };
        self.enqueue(
            RuntimeTelemetryCommand::RegisterClient(entry),
            &resource_instance_id,
        );
        Ok(RuntimeTelemetryGuard::new(
            RuntimeResourceKind::Client,
            resource_instance_id,
            self.command_sink(),
        ))
    }

    fn enqueue(&self, command: RuntimeTelemetryCommand, resource_instance_id: &str) {
        let Some(sink) = self.command_sink() else {
            return;
        };
        match sink.enqueue(command) {
            RuntimeCommandDispatch::Enqueued
            | RuntimeCommandDispatch::Deferred
            | RuntimeCommandDispatch::ShuttingDown => {}
            RuntimeCommandDispatch::ClosedUnexpected => {
                warn_runtime_command_send_error(
                    resource_instance_id,
                    "runtime telemetry enqueue failed",
                    "channel closed",
                );
            }
            RuntimeCommandDispatch::WorkerSpawnFailed => {
                warn_runtime_command_send_error(
                    resource_instance_id,
                    "runtime telemetry enqueue failed",
                    "flush worker spawn failed",
                );
            }
        }
    }

    fn command_sink(&self) -> Option<Arc<RuntimeCommandSink>> {
        self.command_sink
            .lock()
            .expect("runtime command lock")
            .clone()
    }

    async fn publish_snapshot(
        &self,
        state: &RuntimeMetricsState,
        heartbeat_interval: Duration,
    ) -> AortaResult<()> {
        let next = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let payload = build_runtime_payload(
            next,
            &self.node_id,
            &self.group,
            state,
            duration_to_collection_period_ms(heartbeat_interval),
        );
        let bytes = crate::ZBytes::from(crate::telemetry::fb::serialize_runtime_metrics(&payload));
        *self.latest_payload.write().await = Some(bytes.clone());
        self.session
            .inner()
            .put(&self.key, bytes)
            .attachment(crate::telemetry::fb::runtime_metrics_schema_attachment())
            .await?;
        Ok(())
    }
}

fn warn_runtime_command_send_error(resource_instance_id: &str, message: &str, detail: &str) {
    aorta_core_warn!(
        "Aorta WARN: {} for '{}': {}",
        message,
        resource_instance_id,
        detail
    );
}

async fn run_runtime_worker(
    session: Session,
    key: String,
    node_id: String,
    group: String,
    latest_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    sequence: Arc<AtomicU64>,
    mut command_rx: mpsc::Receiver<RuntimeTelemetryCommand>,
    mut stop_rx: oneshot::Receiver<()>,
    heartbeat_interval: Duration,
) {
    let mut state = RuntimeMetricsState::default();
    let start_at = tokio::time::Instant::now() + heartbeat_interval;
    let mut interval = tokio::time::interval_at(start_at, heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut stop_rx => break,
            Some(command) = command_rx.recv() => {
                apply_runtime_command(&mut state, command);
                while let Ok(command) = command_rx.try_recv() {
                    apply_runtime_command(&mut state, command);
                }
                if let Err(err) = publish_runtime_snapshot(
                    &session,
                    &key,
                    &node_id,
                    &group,
                    &latest_payload,
                    &sequence,
                    &state,
                    heartbeat_interval,
                ).await {
                    aorta_core_warn!("Aorta WARN: runtime telemetry publish failed: {}", err);
                }
            }
            _ = interval.tick() => {
                if let Err(err) = publish_runtime_snapshot(
                    &session,
                    &key,
                    &node_id,
                    &group,
                    &latest_payload,
                    &sequence,
                    &state,
                    heartbeat_interval,
                ).await {
                    aorta_core_warn!("Aorta WARN: runtime telemetry heartbeat publish failed: {}", err);
                }
            }
        }
    }
}

fn apply_runtime_command(state: &mut RuntimeMetricsState, command: RuntimeTelemetryCommand) {
    match command {
        RuntimeTelemetryCommand::RegisterPublisher(entry) => {
            state
                .publishers
                .insert(entry.resource_instance_id.clone(), entry);
        }
        RuntimeTelemetryCommand::RemovePublisher {
            resource_instance_id,
        } => {
            state.publishers.remove(&resource_instance_id);
        }
        RuntimeTelemetryCommand::RegisterSubscriber(entry) => {
            state
                .subscribers
                .insert(entry.resource_instance_id.clone(), entry);
        }
        RuntimeTelemetryCommand::RemoveSubscriber {
            resource_instance_id,
        } => {
            state.subscribers.remove(&resource_instance_id);
        }
        RuntimeTelemetryCommand::RegisterService(entry) => {
            state
                .services
                .insert(entry.resource_instance_id.clone(), entry);
        }
        RuntimeTelemetryCommand::RemoveService {
            resource_instance_id,
        } => {
            state.services.remove(&resource_instance_id);
        }
        RuntimeTelemetryCommand::RegisterClient(entry) => {
            state
                .clients
                .insert(entry.resource_instance_id.clone(), entry);
        }
        RuntimeTelemetryCommand::RemoveClient {
            resource_instance_id,
        } => {
            state.clients.remove(&resource_instance_id);
        }
        RuntimeTelemetryCommand::SetExecutorReader(reader) => {
            state.executor = Some(reader);
        }
        RuntimeTelemetryCommand::ClearExecutorReader => {
            state.executor = None;
        }
        RuntimeTelemetryCommand::SetSpinReader(reader) => {
            state.executor_spin = Some(reader);
        }
        RuntimeTelemetryCommand::ClearSpinReader => {
            state.executor_spin = None;
        }
        RuntimeTelemetryCommand::SetFlowReaders(readers) => {
            state.executor_flow = Some(readers);
        }
        RuntimeTelemetryCommand::ClearFlowReaders => {
            state.executor_flow = None;
        }
    }
}

async fn publish_runtime_snapshot(
    session: &Session,
    key: &str,
    node_id: &str,
    group: &str,
    latest_payload: &Arc<TokioRwLock<Option<ZBytes>>>,
    sequence: &Arc<AtomicU64>,
    state: &RuntimeMetricsState,
    heartbeat_interval: Duration,
) -> AortaResult<()> {
    let next = sequence.fetch_add(1, Ordering::Relaxed) + 1;
    let payload = build_runtime_payload(
        next,
        node_id,
        group,
        state,
        duration_to_collection_period_ms(heartbeat_interval),
    );
    let bytes = crate::ZBytes::from(crate::telemetry::fb::serialize_runtime_metrics(&payload));
    *latest_payload.write().await = Some(bytes.clone());
    session
        .inner()
        .put(key, bytes)
        .attachment(crate::telemetry::fb::runtime_metrics_schema_attachment())
        .await?;
    Ok(())
}

fn build_runtime_payload(
    sequence: u64,
    node_id: &str,
    group: &str,
    state: &RuntimeMetricsState,
    collection_period_ms: u32,
) -> RuntimeMetricsTelemetry {
    RuntimeMetricsTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence,
        timestamp_ns: crate::time::unix_time_ns(),
        node_id: node_id.to_string(),
        group: group.to_string(),
        collection_period_ms,
        publishers: state
            .publishers
            .values()
            .map(|entry| {
                let stats = (entry.reader)();
                TelemetryPublisherRuntimeMetrics {
                    resource_name: entry.resource_name.clone(),
                    resource_instance_id: entry.resource_instance_id.clone(),
                    schema_hash: entry.schema_hash.clone(),
                    publish_shm_hit_total: stats.publish_shm_hit_total,
                    publish_shm_fallback_total: stats.publish_shm_fallback_total,
                    publish_shm_required_fail_total: stats.publish_shm_required_fail_total,
                    loan_inflight_current: stats.loan_inflight_current,
                    loan_quota_exceeded_total: stats.loan_quota_exceeded_total,
                }
            })
            .collect(),
        subscribers: state
            .subscribers
            .values()
            .map(|entry| {
                let stats = (entry.reader)();
                TelemetrySubscriberRuntimeMetrics {
                    resource_name: entry.resource_name.clone(),
                    resource_instance_id: entry.resource_instance_id.clone(),
                    schema_hash: entry.schema_hash.clone(),
                    duplicate_drop_count: stats.duplicate_drop_count,
                    out_of_order_count: stats.out_of_order_count,
                    out_of_order_drop_count: stats.out_of_order_drop_count,
                    executor_drop_count: stats.executor_drop_count,
                    local_drop_count: stats.local_drop_count,
                    overflow_relay_enqueue_count: stats.overflow_relay_enqueue_count,
                    overflow_relay_drop_count: stats.overflow_relay_drop_count,
                    dropped_for_deadline: stats.dropped_for_deadline,
                }
            })
            .collect(),
        services: state
            .services
            .values()
            .map(|entry| {
                let stats = (entry.reader)();
                TelemetryServiceRuntimeMetrics {
                    resource_name: entry.resource_name.clone(),
                    resource_instance_id: entry.resource_instance_id.clone(),
                    schema_hash: entry.schema_hash.clone(),
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
                }
            })
            .collect(),
        clients: state
            .clients
            .values()
            .map(|entry| {
                let stats = (entry.reader)();
                TelemetryClientRuntimeMetrics {
                    resource_name: entry.resource_name.clone(),
                    resource_instance_id: entry.resource_instance_id.clone(),
                    schema_hash: entry.schema_hash.clone(),
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
            })
            .collect(),
        executor: state.executor.as_ref().map(|reader| {
            let stats = reader.snapshot();
            // v6: the SDK-pushed pool-lane snapshot (None when detached).
            let pool_lanes = state
                .executor_flow
                .as_ref()
                .and_then(|f| f.lane_hooks.lane_telemetry());
            TelemetryExecutorRuntimeMetrics {
                queued_events: stats.queued_events,
                queued_bytes: stats.queued_bytes,
                peak_queued_events: stats.peak_queued_events,
                peak_queued_bytes: stats.peak_queued_bytes,
                dropped_queue_full: stats.dropped_queue_full,
                dropped_queue_closed: stats.dropped_queue_closed,
                dropped_budget_events: stats.dropped_budget_events,
                dropped_budget_bytes: stats.dropped_budget_bytes,
                spin_count: state
                    .executor_spin
                    .as_ref()
                    .map(|c| c.load(Ordering::Relaxed))
                    .unwrap_or(0),
                lane_occupancy: state
                    .executor_flow
                    .as_ref()
                    .map(|f| f.lane.stats().occupancy)
                    .unwrap_or(0),
                lane_high_water: state
                    .executor_flow
                    .as_ref()
                    .map(|f| f.lane.stats().high_water)
                    .unwrap_or(0),
                lane_pushed_total: state
                    .executor_flow
                    .as_ref()
                    .map(|f| f.lane.stats().pushed_total)
                    .unwrap_or(0),
                dropped_expired: state
                    .executor_flow
                    .as_ref()
                    .map(|f| f.dropped_expired.load(Ordering::Relaxed))
                    .unwrap_or(0),
                // PROCESS-GLOBAL, not per-executor: the offload pool is a
                // process-wide singleton, so every executor row in one
                // process reports the same shared backlog. Kept in this
                // table for consumer convenience (the executor row is the
                // runtime-metrics anchor); do not diff it across executors.
                offload_queue_depth: crate::flow::offload::queue_depth() as u64,
                pool_lane_depth: pool_lanes.map(|s| s[0].to_vec()).unwrap_or_default(),
                pool_lane_high_water: pool_lanes.map(|s| s[1].to_vec()).unwrap_or_default(),
                pool_lane_enqueued_total: pool_lanes.map(|s| s[2].to_vec()).unwrap_or_default(),
                pool_lane_shed_total: pool_lanes.map(|s| s[3].to_vec()).unwrap_or_default(),
                pool_lane_dropped_expired_total: pool_lanes
                    .map(|s| s[4].to_vec())
                    .unwrap_or_default(),
            }
        }),
    }
}

fn duration_to_collection_period_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher_reader_entry(resource_instance_id: &str) -> RuntimePublisherReaderEntry {
        RuntimePublisherReaderEntry {
            resource_instance_id: resource_instance_id.to_string(),
            resource_name: "demo/topic".to_string(),
            schema_hash: None,
            reader: Arc::new(PublisherRuntimeStats::default),
        }
    }

    #[test]
    fn pending_runtime_commands_keep_latest_state_per_resource() {
        let mut pending =
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID);
        pending.push(
            "publisher:1".to_string(),
            RuntimeTelemetryCommand::RegisterPublisher(publisher_reader_entry("publisher:1")),
        );
        pending.push(
            "publisher:1".to_string(),
            RuntimeTelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            },
        );

        let (_, pending_command) = pending.peek_next().expect("pending command");
        assert!(matches!(
            pending_command.command,
            RuntimeTelemetryCommand::RemovePublisher { ref resource_instance_id }
                if resource_instance_id == "publisher:1"
        ));
    }

    #[test]
    fn pending_runtime_commands_keep_latest_executor_state() {
        let queue = crate::event_queue::EventQueue::new(1);
        let mut pending =
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID);
        pending.push(
            RUNTIME_EXECUTOR_RESOURCE_ID.to_string(),
            RuntimeTelemetryCommand::SetExecutorReader(queue.admission_reader()),
        );
        pending.push(
            RUNTIME_EXECUTOR_RESOURCE_ID.to_string(),
            RuntimeTelemetryCommand::ClearExecutorReader,
        );

        let (_, pending_command) = pending.peek_next().expect("pending executor command");
        assert!(matches!(
            pending_command.command,
            RuntimeTelemetryCommand::ClearExecutorReader
        ));
    }

    #[test]
    fn pending_runtime_commands_preserve_newer_state_after_stale_resource_send_ack() {
        let mut pending =
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID);
        pending.push(
            "publisher:1".to_string(),
            RuntimeTelemetryCommand::RegisterPublisher(publisher_reader_entry("publisher:1")),
        );

        let (resource_instance_id, first_pending) = pending.peek_next().expect("pending command");
        pending.push(
            "publisher:1".to_string(),
            RuntimeTelemetryCommand::RemovePublisher {
                resource_instance_id: "publisher:1".to_string(),
            },
        );
        pending.acknowledge_sent(&resource_instance_id, first_pending.sequence);

        let (_, latest_pending) = pending.peek_next().expect("latest pending command");
        assert!(matches!(
            latest_pending.command,
            RuntimeTelemetryCommand::RemovePublisher { ref resource_instance_id }
                if resource_instance_id == "publisher:1"
        ));
    }

    #[test]
    fn pending_runtime_commands_preserve_newer_executor_state_after_stale_send_ack() {
        let queue = crate::event_queue::EventQueue::new(1);
        let mut pending =
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID);
        pending.push(
            RUNTIME_EXECUTOR_RESOURCE_ID.to_string(),
            RuntimeTelemetryCommand::SetExecutorReader(queue.admission_reader()),
        );

        let (resource_instance_id, first_pending) = pending.peek_next().expect("pending command");
        pending.push(
            RUNTIME_EXECUTOR_RESOURCE_ID.to_string(),
            RuntimeTelemetryCommand::ClearExecutorReader,
        );
        pending.acknowledge_sent(&resource_instance_id, first_pending.sequence);

        let (_, latest_pending) = pending.peek_next().expect("latest pending command");
        assert!(matches!(
            latest_pending.command,
            RuntimeTelemetryCommand::ClearExecutorReader
        ));
    }

    #[test]
    fn runtime_command_sink_returns_shutting_down_after_close() {
        let (tx, _rx) = mpsc::channel(1);
        let sink = RuntimeCommandSink::new(
            tx,
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID),
            runtime_command_resource_id,
            RUNTIME_FLUSH_WORKER_NAME,
        );
        sink.close();

        let outcome = sink.enqueue(RuntimeTelemetryCommand::RemovePublisher {
            resource_instance_id: "publisher:1".to_string(),
        });

        assert_eq!(outcome, RuntimeCommandDispatch::ShuttingDown);
    }

    #[test]
    fn runtime_command_sink_clear_executor_returns_shutting_down_after_close() {
        let (tx, _rx) = mpsc::channel(1);
        let sink = RuntimeCommandSink::new(
            tx,
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID),
            runtime_command_resource_id,
            RUNTIME_FLUSH_WORKER_NAME,
        );
        sink.close();

        let outcome = sink.enqueue(RuntimeTelemetryCommand::ClearExecutorReader);

        assert_eq!(outcome, RuntimeCommandDispatch::ShuttingDown);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_command_sink_channel_full_flushes_only_latest_executor_state() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = RuntimeCommandSink::new(
            tx,
            PendingRuntimeCommands::with_priority_resource(RUNTIME_EXECUTOR_RESOURCE_ID),
            runtime_command_resource_id,
            RUNTIME_FLUSH_WORKER_NAME,
        );
        let queue = crate::event_queue::EventQueue::new(1);

        assert_eq!(
            sink.enqueue(RuntimeTelemetryCommand::RegisterPublisher(
                publisher_reader_entry("publisher:1"),
            )),
            RuntimeCommandDispatch::Enqueued
        );
        assert_eq!(
            sink.enqueue(RuntimeTelemetryCommand::SetExecutorReader(
                queue.admission_reader(),
            )),
            RuntimeCommandDispatch::Deferred
        );
        assert_eq!(
            sink.enqueue(RuntimeTelemetryCommand::ClearExecutorReader),
            RuntimeCommandDispatch::Deferred
        );

        {
            let pending = sink.pending.lock().expect("pending lock");
            assert!(pending.has_priority());
            assert!(pending.resources.is_empty());
        }

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("first command timeout")
            .expect("first command");
        assert!(matches!(
            first,
            RuntimeTelemetryCommand::RegisterPublisher(_)
        ));

        tokio::task::yield_now().await;

        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("second command timeout")
            .expect("second command");
        assert!(matches!(
            second,
            RuntimeTelemetryCommand::ClearExecutorReader
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!sink.flush_worker_scheduled.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod lane_telemetry_integration_tests {
    use super::*;

    // Drive the REAL LaneHookSlot →
    // build_runtime_payload integration — empty vectors when detached
    // (the "not in facade mode" schema contract), populated when the
    // SDK pushes a snapshot. Prior coverage only round-tripped
    // hand-built FlatBuffer structs.
    #[tokio::test]
    async fn lane_vectors_are_empty_detached_and_populated_attached() {
        let queue = crate::event_queue::EventQueue::new(16);
        let lane_hooks = Arc::new(crate::executor::LaneHookSlot::default());
        let state = RuntimeMetricsState {
            executor: Some(queue.admission_reader()),
            executor_flow: Some(FlowExecutorReaders {
                lane: queue.reserved_lane(),
                dropped_expired: Arc::new(AtomicU64::new(0)),
                lane_hooks: Arc::clone(&lane_hooks),
            }),
            ..Default::default()
        };

        let payload = build_runtime_payload(1, "node", "default", &state, 1000);
        let exec = payload.executor.as_ref().expect("executor block present");
        assert!(
            exec.pool_lane_depth.is_empty()
                && exec.pool_lane_high_water.is_empty()
                && exec.pool_lane_enqueued_total.is_empty()
                && exec.pool_lane_shed_total.is_empty()
                && exec.pool_lane_dropped_expired_total.is_empty(),
            "detached: every lane vector is EMPTY (never zero-filled)"
        );

        let snapshot = [
            [1u64, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
            [17, 18, 19, 20],
        ];
        lane_hooks.set_lane_telemetry(snapshot);
        let payload = build_runtime_payload(2, "node", "default", &state, 1000);
        let exec = payload.executor.as_ref().expect("executor block present");
        assert_eq!(exec.pool_lane_depth, vec![1, 2, 3, 4]);
        assert_eq!(exec.pool_lane_high_water, vec![5, 6, 7, 8]);
        assert_eq!(exec.pool_lane_enqueued_total, vec![9, 10, 11, 12]);
        assert_eq!(exec.pool_lane_shed_total, vec![13, 14, 15, 16]);
        assert_eq!(exec.pool_lane_dropped_expired_total, vec![17, 18, 19, 20]);

        // Round-trip the populated payload through the REAL FlatBuffers
        // encode/decode (not a hand-built struct).
        let bytes = crate::telemetry::fb::serialize_runtime_metrics(&payload);
        let decoded =
            crate::telemetry::fb::runtime_metrics_from_fb(&bytes).expect("wire round-trip");
        let dexec = decoded.executor.as_ref().expect("decoded executor");
        assert_eq!(dexec.pool_lane_depth, vec![1, 2, 3, 4]);
        assert_eq!(dexec.pool_lane_dropped_expired_total, vec![17, 18, 19, 20]);
    }
}
