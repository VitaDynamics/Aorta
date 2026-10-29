use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{oneshot, Mutex as TokioMutex, RwLock as TokioRwLock};

use crate::error::AortaResult;
use crate::log_prelude::*;
use crate::session::Session;
use crate::snapshot_queryable::spawn_single_key_snapshot_queryable;
use crate::telemetry::schema::NodeIdentityTelemetry;
use crate::ZBytes;

const DEFAULT_NODE_IDENTITY_HEARTBEAT_MS: u64 = 30_000;

#[derive(Default)]
struct NodeIdentityTelemetryRuntime {
    queryable_stop: Option<oneshot::Sender<()>>,
    heartbeat_stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

pub struct NodeIdentityTelemetryPublisher {
    session: Session,
    key: String,
    base_payload: NodeIdentityTelemetry,
    sequence: Arc<AtomicU64>,
    latest_payload: Arc<TokioRwLock<Option<ZBytes>>>,
    runtime: TokioMutex<NodeIdentityTelemetryRuntime>,
    active: AtomicBool,
}

impl std::fmt::Debug for NodeIdentityTelemetryPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentityTelemetryPublisher")
            .field("key", &self.key)
            .finish()
    }
}

impl NodeIdentityTelemetryPublisher {
    pub fn new(session: Session, key: String, base_payload: NodeIdentityTelemetry) -> Self {
        Self {
            session,
            key,
            base_payload,
            sequence: Arc::new(AtomicU64::new(0)),
            latest_payload: Arc::new(TokioRwLock::new(None)),
            runtime: TokioMutex::new(NodeIdentityTelemetryRuntime::default()),
            active: AtomicBool::new(false),
        }
    }

    pub fn enabled_from_env() -> bool {
        crate::telemetry::enabled_from_env("AORTA_TELEMETRY_NODE_IDENTITY")
    }

    pub fn heartbeat_interval_from_env() -> Duration {
        crate::telemetry::heartbeat_interval_from_env(
            "AORTA_TELEMETRY_HEARTBEAT_MS",
            DEFAULT_NODE_IDENTITY_HEARTBEAT_MS,
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

        self.publish_next().await?;

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

        let (heartbeat_stop_tx, mut heartbeat_stop_rx) = oneshot::channel();
        let session = self.session.clone();
        let key = self.key.clone();
        let payload = Arc::clone(&self.latest_payload);
        let heartbeat_interval = Self::heartbeat_interval_from_env();
        let sequence = Arc::clone(&self.sequence);
        let base_payload = self.base_payload.clone();
        // spawn_logged_task: a heartbeat panic must be logged when it happens;
        // see inventory.rs for the rationale.
        let heartbeat_task = crate::runtime::spawn_logged_task(
            "telemetry-identity-heartbeat",
            async move {
                let start_at = tokio::time::Instant::now() + heartbeat_interval;
                let mut interval = tokio::time::interval_at(start_at, heartbeat_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = &mut heartbeat_stop_rx => break,
                        _ = interval.tick() => {
                            let next = sequence.fetch_add(1, Ordering::Relaxed) + 1;
                            let mut snapshot = base_payload.clone();
                            snapshot.sequence = next;
                            snapshot.timestamp_ns = crate::time::unix_time_ns();
                            let bytes = ZBytes::from(
                                crate::telemetry::fb::serialize_node_identity(&snapshot),
                            );
                            *payload.write().await = Some(bytes.clone());
                            if let Err(err) = session.inner().put(&key, bytes).attachment(crate::telemetry::fb::node_identity_schema_attachment()).await {
                                aorta_core_warn!(
                                    "Aorta WARN: node identity telemetry heartbeat publish failed: {}",
                                    err
                                );
                            }
                        }
                    }
                }
            },
        );

        runtime.queryable_stop = Some(queryable_stop_tx);
        runtime.heartbeat_stop = Some(heartbeat_stop_tx);
        runtime.tasks = vec![queryable_task, heartbeat_task];
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
            if let Some(stop) = runtime.queryable_stop.take() {
                let _ = stop.send(());
            }
            if let Some(stop) = runtime.heartbeat_stop.take() {
                let _ = stop.send(());
            }
            std::mem::take(&mut runtime.tasks)
        };

        for task in tasks {
            if let Err(err) = task.await {
                crate::telemetry::warn_task_join_failure("node identity", &err);
            }
        }

        *self.latest_payload.write().await = None;
        self.session.inner().delete(&self.key).await?;
        Ok(())
    }

    async fn publish_next(&self) -> AortaResult<()> {
        let next = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let mut payload = self.base_payload.clone();
        payload.sequence = next;
        payload.timestamp_ns = crate::time::unix_time_ns();
        let bytes = ZBytes::from(crate::telemetry::fb::serialize_node_identity(&payload));
        *self.latest_payload.write().await = Some(bytes.clone());
        self.session
            .inner()
            .put(&self.key, bytes)
            .attachment(crate::telemetry::fb::node_identity_schema_attachment())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::telemetry::identity::build_node_identity_telemetry_base;
    use crate::telemetry::schema::{HostIdentity, TelemetryNodeMetadata};
    use crate::test_support::ENV_LOCK;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_start_only_publishes_once() -> crate::error::AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let session = Session::open_default().await?;
        let publisher = NodeIdentityTelemetryPublisher::new(
            session.clone(),
            format!(
                "aorta/default/sys/telemetry/node_identity/test-{}",
                crate::time::unix_time_ns()
            ),
            build_node_identity_telemetry_base(
                "test-node-id",
                "test-node",
                "default",
                &HostIdentity {
                    host_id: "test-host".to_string(),
                    host_name: "test-host".to_string(),
                },
                crate::time::unix_time_ns(),
                4242,
                &session.zid(),
                &TelemetryNodeMetadata::default(),
            ),
        );

        let (first, second) = tokio::join!(publisher.start(), publisher.start());
        first?;
        second?;

        assert!(publisher.is_active());
        assert_eq!(publisher.sequence.load(Ordering::Relaxed), 1);

        publisher.stop().await?;
        session.close().await?;
        Ok(())
    }
}
