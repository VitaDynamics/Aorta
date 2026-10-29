use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use std::time::Duration;

use tokio::sync::{oneshot, Mutex as TokioMutex, RwLock};
use tokio::time::timeout;
use zenoh::query::{ConsolidationMode, QueryTarget, Reply};

use crate::error::{AortaError, AortaResult};
use crate::key_expr::KeyExprBuilder;
use crate::log_prelude::*;
use crate::runtime_env;
use crate::schema::{infer_service_root_types_from_bfbs, SchemaHash};
use crate::schema_by_key_fb;
use crate::session::Session;

const SCHEMA_QUERY_TIMEOUT_MS: u64 = 200;
const SERVICE_SCHEMA_READY_TIMEOUT_MS: u64 = 500;
const SERVICE_SCHEMA_READY_POLL_MS: u64 = 50;

/// Action to take when a schema conflict is detected.
///
/// Configured via `AORTA_SCHEMA_CONFLICT_ACTION` environment variable.
/// - `warn` (default): Log a warning but allow the registration to proceed
/// - `reject`: Return an error and refuse registration
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaConflictAction {
    /// Return an error when schema conflict is detected (default)
    Reject,
    /// Log a warning and continue with existing schema
    Warn,
}

impl SchemaConflictAction {
    /// Parse from environment variable `AORTA_SCHEMA_CONFLICT_ACTION`.
    ///
    /// Valid values: `reject`, `warn` (case-insensitive)
    /// Default: `warn`
    pub fn from_env() -> Self {
        match runtime_env::var("AORTA_SCHEMA_CONFLICT_ACTION") {
            Some(raw) => {
                let raw = raw.trim().to_lowercase();
                match raw.as_str() {
                    "warn" => Self::Warn,
                    "reject" => Self::Reject,
                    _ => {
                        aorta_core_warn!(
                            "Aorta WARN: invalid AORTA_SCHEMA_CONFLICT_ACTION='{}', using 'warn'",
                            raw
                        );
                        Self::Warn
                    }
                }
            }
            None => Self::Warn,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaKind {
    Topic = 0,
    Service = 1,
    Param = 2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaByKeyValue {
    kind: SchemaKind,
    hash: SchemaHash,
    bfbs: Vec<u8>,
    root_type: Option<String>,
}

/// Service schema information using a single BFBS plus explicit request/response roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSchemaByKeyValue {
    pub service_hash: SchemaHash,
    pub service_bfbs: Vec<u8>,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
}

impl SchemaByKeyValue {
    pub fn topic(hash: SchemaHash, bfbs: Vec<u8>, root_type: Option<String>) -> Self {
        Self {
            kind: SchemaKind::Topic,
            hash,
            bfbs,
            root_type,
        }
    }

    pub fn param(hash: SchemaHash, bfbs: Vec<u8>) -> Self {
        Self {
            kind: SchemaKind::Param,
            hash,
            bfbs,
            root_type: None,
        }
    }

    pub fn kind(&self) -> SchemaKind {
        self.kind
    }

    pub fn hash(&self) -> SchemaHash {
        self.hash
    }

    pub fn bfbs(&self) -> &[u8] {
        &self.bfbs
    }

    pub fn root_type(&self) -> Option<&str> {
        self.root_type.as_deref()
    }

    /// True when `other` differs from `self` at most by ADDITIVE root-type
    /// metadata: identical kind/hash/bfbs, and at most one side carries a
    /// `topic_root_type`. A mixed-version rollout (legacy publisher without
    /// the field alongside an upgraded one) must not read as a schema
    /// conflict; two entries that both claim a root but disagree DO conflict
    /// (different tables of one multi-table schema on the same key).
    fn root_metadata_compatible(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.hash == other.hash
            && self.bfbs == other.bfbs
            && (self.root_type.is_none() || other.root_type.is_none())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let hash_hex = builder.create_string(&self.hash.to_hex());
        let bfbs = builder.create_vector(&self.bfbs);
        let root_type = self
            .root_type
            .as_ref()
            .map(|value| builder.create_string(value));
        let root = schema_by_key_fb::create_schema_by_key(
            &mut builder,
            self.kind as u8,
            Some(hash_hex),
            Some(bfbs),
            None,
            None,
            None,
            None,
            root_type,
        );
        builder.finish(root, None);
        builder.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let root = schema_by_key_fb::root_as_schema_by_key(bytes)?;
        let kind = match root.kind() {
            0 => SchemaKind::Topic,
            2 => SchemaKind::Param,
            1 => return None,
            _ => return None,
        };
        let bfbs = root.bfbs()?.to_vec();
        let hash_hex = root.hash_hex()?;
        let hash = SchemaHash::from_hex_str(hash_hex)?;
        let root_type = root.topic_root_type().map(str::to_string);
        Some(Self {
            kind,
            hash,
            bfbs,
            root_type,
        })
    }
}

impl ServiceSchemaByKeyValue {
    pub fn new(
        service_hash: SchemaHash,
        service_bfbs: Vec<u8>,
        request_root_type: Option<String>,
        response_root_type: Option<String>,
    ) -> Self {
        let (request_root_type, response_root_type) = infer_service_root_types_from_bfbs(
            &service_bfbs,
            request_root_type.as_deref(),
            response_root_type.as_deref(),
        );
        Self {
            service_hash,
            service_bfbs,
            request_root_type,
            response_root_type,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let service_hash_hex = builder.create_string(&self.service_hash.to_hex());
        let service_bfbs = builder.create_vector(&self.service_bfbs);
        let request_root_type = self
            .request_root_type
            .as_ref()
            .map(|value| builder.create_string(value));
        let response_root_type = self
            .response_root_type
            .as_ref()
            .map(|value| builder.create_string(value));
        let root = schema_by_key_fb::create_schema_by_key(
            &mut builder,
            SchemaKind::Service as u8,
            None,
            None,
            Some(service_hash_hex),
            Some(service_bfbs),
            request_root_type,
            response_root_type,
            None,
        );
        builder.finish(root, None);
        builder.finished_data().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let root = schema_by_key_fb::root_as_schema_by_key(bytes)?;
        if root.kind() != SchemaKind::Service as u8 {
            return None;
        }
        let service_bfbs = root.service_bfbs()?.to_vec();
        let service_hash_hex = root.service_hash_hex()?;
        let service_hash = SchemaHash::from_hex_str(service_hash_hex)?;
        let request_root_type = root.request_root_type().map(str::to_string);
        let response_root_type = root.response_root_type().map(str::to_string);
        Some(Self::new(
            service_hash,
            service_bfbs,
            request_root_type,
            response_root_type,
        ))
    }
}

#[derive(Clone)]
pub struct SchemaRegistry {
    session: Session,
    key_builder: KeyExprBuilder,
    cache: Arc<RwLock<HashMap<SchemaHash, Vec<u8>>>>,
    schema_by_key_cache: Arc<RwLock<HashMap<String, SchemaByKeyValue>>>,
    service_schema_by_key_cache: Arc<RwLock<HashMap<String, ServiceSchemaByKeyValue>>>,
    queryables: Arc<TokioMutex<SchemaQueryables>>,
}

#[derive(Default)]
struct SchemaQueryables {
    active: bool,
    schema_stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    schema_by_key_queryables: HashMap<String, SchemaByKeyQueryable>,
}

struct SchemaByKeyQueryable {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistry").finish()
    }
}

impl SchemaRegistry {
    pub fn new(session: Session, key_builder: KeyExprBuilder) -> Self {
        Self {
            session,
            key_builder,
            cache: Arc::new(RwLock::new(HashMap::new())),
            schema_by_key_cache: Arc::new(RwLock::new(HashMap::new())),
            service_schema_by_key_cache: Arc::new(RwLock::new(HashMap::new())),
            queryables: Arc::new(TokioMutex::new(SchemaQueryables::default())),
        }
    }

    pub fn queryable_enabled_from_env() -> bool {
        match runtime_env::var("AORTA_SCHEMA_QUERYABLE") {
            Some(raw) => {
                let raw = raw.trim().to_lowercase();
                !(raw == "0" || raw == "false" || raw == "no")
            }
            None => true,
        }
    }

    fn schema_query_timeout() -> Duration {
        match runtime_env::var("AORTA_SCHEMA_QUERY_TIMEOUT_MS") {
            Some(raw) => raw
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .map(Duration::from_millis)
                .unwrap_or_else(|| Duration::from_millis(SCHEMA_QUERY_TIMEOUT_MS)),
            None => Duration::from_millis(SCHEMA_QUERY_TIMEOUT_MS),
        }
    }

    fn service_schema_ready_timeout() -> Duration {
        match runtime_env::var("AORTA_SERVICE_SCHEMA_READY_TIMEOUT_MS") {
            Some(raw) => raw
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .map(Duration::from_millis)
                .unwrap_or_else(|| Duration::from_millis(SERVICE_SCHEMA_READY_TIMEOUT_MS)),
            None => Duration::from_millis(SERVICE_SCHEMA_READY_TIMEOUT_MS),
        }
    }

    fn service_schema_ready_poll() -> Duration {
        match runtime_env::var("AORTA_SERVICE_SCHEMA_READY_POLL_MS") {
            Some(raw) => raw
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .map(Duration::from_millis)
                .unwrap_or_else(|| Duration::from_millis(SERVICE_SCHEMA_READY_POLL_MS)),
            None => Duration::from_millis(SERVICE_SCHEMA_READY_POLL_MS),
        }
    }

    pub async fn start_schema_queryables(&self) -> AortaResult<()> {
        let guard = self.queryables.lock().await;
        if guard.active {
            return Ok(());
        }
        drop(guard);

        let (schema_stop_tx, mut schema_stop_rx) = oneshot::channel();

        let schema_query = self
            .session
            .inner()
            .declare_queryable("aorta/sys/schema/**")
            .await?;
        let schema_cache = Arc::clone(&self.cache);

        let schema_task = tokio::spawn(async move {
            let queryable = schema_query;
            loop {
                let query = tokio::select! {
                    _ = &mut schema_stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(err) => {
                            aorta_core_warn!("Aorta WARN: schema queryable receive failed: {}", err);
                            break;
                        }
                    },
                };

                let key = query.key_expr().as_str();
                if let Some(hash) = schema_hash_from_full_key(key) {
                    if let Some(bfbs) = schema_cache.read().await.get(&hash).cloned() {
                        if let Err(err) = query.reply(key, bfbs).await {
                            aorta_core_warn!("Aorta WARN: schema query reply failed: {}", err);
                        }
                    }
                }
            }

            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!("Aorta WARN: schema queryable undeclare failed: {}", err);
            }
        });

        let mut existing_suffixes: Vec<String> = self
            .schema_by_key_cache
            .read()
            .await
            .keys()
            .cloned()
            .collect();
        existing_suffixes.extend(
            self.service_schema_by_key_cache
                .read()
                .await
                .keys()
                .cloned(),
        );
        existing_suffixes.sort();
        existing_suffixes.dedup();

        let mut schema_by_key_queryables = HashMap::new();
        for key_suffix in existing_suffixes {
            match self.spawn_schema_by_key_queryable(&key_suffix).await {
                Ok(queryable) => {
                    schema_by_key_queryables.insert(key_suffix, queryable);
                }
                Err(err) => {
                    let _ = schema_stop_tx.send(());
                    if let Err(join_err) = schema_task.await {
                        aorta_core_warn!(
                            "Aorta WARN: schema queryable task join failed during rollback: {}",
                            join_err
                        );
                    }
                    for queryable in schema_by_key_queryables.into_values() {
                        let _ = queryable.stop.send(());
                        if let Err(join_err) = queryable.task.await {
                            aorta_core_warn!(
                                "Aorta WARN: schema_by_key queryable task join failed during rollback: {}",
                                join_err
                            );
                        }
                    }
                    return Err(err);
                }
            }
        }

        let mut guard = self.queryables.lock().await;
        guard.active = true;
        guard.schema_stop = Some(schema_stop_tx);
        guard.tasks = vec![schema_task];
        guard.schema_by_key_queryables = schema_by_key_queryables;

        Ok(())
    }

    pub async fn stop_schema_queryables(&self) {
        let (tasks, schema_by_key_queryables) = {
            let mut guard = self.queryables.lock().await;
            if !guard.active {
                return;
            }
            guard.active = false;
            if let Some(stop) = guard.schema_stop.take() {
                let _ = stop.send(());
            }
            (
                std::mem::take(&mut guard.tasks),
                std::mem::take(&mut guard.schema_by_key_queryables),
            )
        };

        for queryable in schema_by_key_queryables.into_values() {
            let _ = queryable.stop.send(());
            if let Err(err) = queryable.task.await {
                aorta_core_warn!(
                    "Aorta WARN: schema_by_key queryable task join failed: {}",
                    err
                );
            }
        }

        for task in tasks {
            if let Err(err) = task.await {
                aorta_core_warn!("Aorta WARN: schema queryable task join failed: {}", err);
            }
        }
    }

    pub async fn insert_local(&self, hash: SchemaHash, bfbs: Vec<u8>) {
        let mut guard = self.cache.write().await;
        guard.insert(hash, bfbs);
    }

    pub async fn insert_schema_by_key_local(&self, key_suffix: String, value: SchemaByKeyValue) {
        let mut guard = self.schema_by_key_cache.write().await;
        guard.insert(key_suffix, value);
    }

    pub async fn insert_service_schema_by_key_local(
        &self,
        key_suffix: String,
        value: ServiceSchemaByKeyValue,
    ) {
        let mut guard = self.service_schema_by_key_cache.write().await;
        guard.insert(key_suffix, value);
    }

    pub async fn get_schema_by_key_local(&self, key_suffix: &str) -> Option<SchemaByKeyValue> {
        let guard = self.schema_by_key_cache.read().await;
        guard.get(key_suffix).cloned()
    }

    pub async fn get_service_schema_by_key_local(
        &self,
        key_suffix: &str,
    ) -> Option<ServiceSchemaByKeyValue> {
        let guard = self.service_schema_by_key_cache.read().await;
        guard.get(key_suffix).cloned()
    }

    pub async fn get_local(&self, hash: SchemaHash) -> Option<Vec<u8>> {
        let guard = self.cache.read().await;
        guard.get(&hash).cloned()
    }

    async fn ensure_schema_by_key_queryable(&self, key_suffix: &str) -> AortaResult<()> {
        let mut guard = self.queryables.lock().await;
        if !guard.active || guard.schema_by_key_queryables.contains_key(key_suffix) {
            return Ok(());
        }

        let queryable = self.spawn_schema_by_key_queryable(key_suffix).await?;
        guard
            .schema_by_key_queryables
            .insert(key_suffix.to_string(), queryable);
        Ok(())
    }

    async fn wait_for_service_schema_queryable_ready(&self, key_suffix: &str) -> AortaResult<()> {
        let expected = self
            .get_service_schema_by_key_local(key_suffix)
            .await
            .ok_or_else(|| {
                AortaError::internal(format!(
                    "service schema_by_key '{}' missing from local cache during readiness check",
                    key_suffix
                ))
            })?;
        let key_expr = self.key_builder.schema_by_key(key_suffix)?;
        let key_expr: String = key_expr.into();
        let timeout_duration = Self::service_schema_ready_timeout();
        let poll_duration = Self::service_schema_ready_poll();
        let started = tokio::time::Instant::now();

        loop {
            let replies = self
                .session
                .inner()
                .get(key_expr.clone())
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .timeout(poll_duration)
                .await?;

            while let Ok(Ok(reply)) = timeout(poll_duration, replies.recv_async()).await {
                if let Some(bytes) = reply_to_bytes(&reply) {
                    if let Some(value) = ServiceSchemaByKeyValue::decode(&bytes) {
                        if value == expected {
                            aorta_core_debug!("service schema_by_key queryable became ready: key_suffix={}, key={}, elapsed_ms={}, service_hash={}", key_suffix, key_expr, started.elapsed().as_millis(), value.service_hash.to_hex());
                            return Ok(());
                        }
                        aorta_core_debug!("ignoring non-matching service schema_by_key readiness reply: key_suffix={}, key={}, service_hash={}, expected_service_hash={}", key_suffix, key_expr, value.service_hash.to_hex(), expected.service_hash.to_hex());
                    }
                }
            }

            if started.elapsed() >= timeout_duration {
                return Err(AortaError::timeout(format!(
                    "service schema_by_key '{}' did not become ready after {:?}",
                    key_suffix, timeout_duration
                )));
            }

            tokio::time::sleep(poll_duration).await;
        }
    }

    async fn spawn_schema_by_key_queryable(
        &self,
        key_suffix: &str,
    ) -> AortaResult<SchemaByKeyQueryable> {
        let key_expr = self.key_builder.schema_by_key(key_suffix)?;
        let key: String = key_expr.into();
        aorta_core_debug!(
            "declaring schema_by_key queryable: key_suffix={}, key={}",
            key_suffix,
            key
        );
        let queryable = self.session.inner().declare_queryable(key.clone()).await?;
        aorta_core_info!(
            "schema_by_key queryable declared: key_suffix={}, key={}",
            key_suffix,
            key
        );
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let by_key_cache = Arc::clone(&self.schema_by_key_cache);
        let service_by_key_cache = Arc::clone(&self.service_schema_by_key_cache);
        let task_key_suffix = key_suffix.to_string();
        let task = tokio::spawn(async move {
            let queryable = queryable;
            aorta_core_debug!(
                "schema_by_key queryable task started: key_suffix={}, key={}",
                task_key_suffix,
                key
            );
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => break,
                    query = queryable.recv_async() => match query {
                        Ok(query) => query,
                        Err(err) => {
                            aorta_core_warn!(
                                "Aorta WARN: schema_by_key queryable '{}' receive failed: {}",
                                task_key_suffix,
                                err
                            );
                            break;
                        }
                    },
                };

                if let Some(value) = by_key_cache.read().await.get(&task_key_suffix).cloned() {
                    let payload = value.encode();
                    aorta_core_debug!("replying with topic schema_by_key payload: key_suffix={}, key={}, payload_len={}", task_key_suffix, key, payload.len());
                    if let Err(err) = query.reply(&key, payload).await {
                        aorta_core_warn!("Aorta WARN: schema_by_key reply failed: {}", err);
                    }
                } else if let Some(service_value) = service_by_key_cache
                    .read()
                    .await
                    .get(&task_key_suffix)
                    .cloned()
                {
                    let payload = service_value.encode();
                    aorta_core_debug!("replying with service schema_by_key payload: key_suffix={}, key={}, payload_len={}, service_hash={}", task_key_suffix, key, payload.len(), service_value.service_hash.to_hex());
                    if let Err(err) = query.reply(&key, payload).await {
                        aorta_core_warn!("Aorta WARN: service schema_by_key reply failed: {}", err);
                    }
                }
            }

            aorta_core_info!(
                "schema_by_key queryable task exiting: key_suffix={}, key={}",
                task_key_suffix,
                key
            );
            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!(
                    "Aorta WARN: schema_by_key queryable '{}' undeclare failed: {}",
                    task_key_suffix,
                    err
                );
            }
        });

        Ok(SchemaByKeyQueryable {
            stop: stop_tx,
            task,
        })
    }

    pub async fn ensure_schema(&self, hash: SchemaHash, bfbs: Vec<u8>) -> AortaResult<()> {
        self.insert_local(hash, bfbs.clone()).await;
        self.put_schema(hash, bfbs).await
    }

    pub async fn register_schema_by_key_topic(
        &self,
        topic: &str,
        hash: SchemaHash,
        bfbs: Vec<u8>,
        root_type: Option<String>,
    ) -> AortaResult<()> {
        let key_suffix = self.key_builder.key_suffix_for_topic(topic)?;
        let value = SchemaByKeyValue::topic(hash, bfbs, root_type);
        self.register_schema_by_key(&key_suffix, value).await
    }

    pub async fn register_schema_by_key_service(
        &self,
        service_name: &str,
        service_hash: SchemaHash,
        service_bfbs: Vec<u8>,
        request_root_type: Option<String>,
        response_root_type: Option<String>,
    ) -> AortaResult<()> {
        let key_suffix = self.key_builder.key_suffix_for_service(service_name)?;
        let value = ServiceSchemaByKeyValue::new(
            service_hash,
            service_bfbs,
            request_root_type,
            response_root_type,
        );
        self.register_service_schema_by_key(&key_suffix, value)
            .await
    }

    pub async fn fetch_schema_by_key_service(
        &self,
        service_name: &str,
    ) -> AortaResult<Option<ServiceSchemaByKeyValue>> {
        let key_suffix = self.key_builder.key_suffix_for_service(service_name)?;

        if let Some(local) = self.get_service_schema_by_key_local(&key_suffix).await {
            return Ok(Some(local));
        }

        let key_expr = self.key_builder.schema_by_key(&key_suffix)?;
        let key_expr: String = key_expr.into();
        let replies = self.session.inner().get(key_expr).await?;

        let query_timeout = Self::schema_query_timeout();
        loop {
            let reply = match timeout(query_timeout, replies.recv_async()).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(_)) => break,
                Err(_) => return Ok(None),
            };

            if let Some(bytes) = reply_to_bytes(&reply) {
                if let Some(value) = ServiceSchemaByKeyValue::decode(&bytes) {
                    self.insert_service_schema_by_key_local(key_suffix.to_string(), value.clone())
                        .await;
                    if !value.service_bfbs.is_empty() {
                        self.insert_local(value.service_hash, value.service_bfbs.clone())
                            .await;
                    } else {
                        aorta_core_warn!(
                            "Aorta WARN: service '{}' schema bfbs is empty; skip schema cache insert for hash {}.",
                            service_name,
                            value.service_hash.to_hex(),
                        );
                    }
                    return Ok(Some(value));
                }
                return Err(AortaError::internal(
                    "service schema_by_key decode failed (unsupported format)",
                ));
            }
        }
        Ok(None)
    }

    pub async fn fetch_schema_by_key(
        &self,
        key_suffix: &str,
    ) -> AortaResult<Option<SchemaByKeyValue>> {
        if let Some(local) = self.get_schema_by_key_local(key_suffix).await {
            return Ok(Some(local));
        }

        let key_expr = self.key_builder.schema_by_key(key_suffix)?;
        let key_expr: String = key_expr.into();
        let replies = self.session.inner().get(key_expr).await?;

        let query_timeout = Self::schema_query_timeout();
        loop {
            let reply = match timeout(query_timeout, replies.recv_async()).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(_)) => break,
                Err(_) => return Ok(None),
            };

            if let Some(bytes) = reply_to_bytes(&reply) {
                if let Some(value) = SchemaByKeyValue::decode(&bytes) {
                    self.insert_schema_by_key_local(key_suffix.to_string(), value.clone())
                        .await;
                    self.insert_local(value.hash(), value.bfbs().to_vec()).await;
                    return Ok(Some(value));
                }
                return Err(AortaError::internal(
                    "schema_by_key decode failed (unsupported format)",
                ));
            }
        }
        Ok(None)
    }

    async fn fetch_schema_by_key_for_register(
        &self,
        key_suffix: &str,
    ) -> AortaResult<Option<SchemaByKeyValue>> {
        #[cfg(test)]
        {
            if let Some(hook) = register_fetch_hook().lock().expect("lock").as_ref() {
                if let Some(value) = (hook)(key_suffix) {
                    return Ok(Some(value));
                }
            }
        }
        self.fetch_schema_by_key(key_suffix).await
    }

    pub async fn fetch_schema(&self, hash: SchemaHash) -> AortaResult<Option<Vec<u8>>> {
        if let Some(cached) = self.get_local(hash).await {
            return Ok(Some(cached));
        }

        let fetched = self.get_schema_from_network(hash).await?;
        if let Some(bytes) = fetched.clone() {
            self.insert_local(hash, bytes).await;
        }
        Ok(fetched)
    }

    async fn put_schema(&self, hash: SchemaHash, bfbs: Vec<u8>) -> AortaResult<()> {
        let key_expr = KeyExprBuilder::schema_key(&hash)?;
        let key_expr: String = key_expr.into();
        self.session.inner().put(key_expr, bfbs).await?;
        Ok(())
    }

    async fn put_schema_by_key(
        &self,
        key_suffix: &str,
        value: &SchemaByKeyValue,
    ) -> AortaResult<()> {
        let key_expr = self.key_builder.schema_by_key(key_suffix)?;
        let key_expr: String = key_expr.into();
        self.session.inner().put(key_expr, value.encode()).await?;
        Ok(())
    }

    async fn put_service_schema_by_key(
        &self,
        key_suffix: &str,
        value: &ServiceSchemaByKeyValue,
    ) -> AortaResult<()> {
        let key_expr = self.key_builder.schema_by_key(key_suffix)?;
        let key_expr: String = key_expr.into();
        self.session.inner().put(key_expr, value.encode()).await?;
        Ok(())
    }

    async fn register_schema_by_key(
        &self,
        key_suffix: &str,
        value: SchemaByKeyValue,
    ) -> AortaResult<()> {
        if let Some(existing) = self.fetch_schema_by_key_for_register(key_suffix).await? {
            if existing == value {
                self.insert_schema_by_key_local(key_suffix.to_string(), existing)
                    .await;
                self.ensure_schema_by_key_queryable(key_suffix).await?;
                return Ok(());
            }
            if existing.root_metadata_compatible(&value) {
                // Additive metadata difference only (one side lacks
                // topic_root_type). Never a conflict: keep/publish the richer
                // entry so mixed-version fleets converge on the root-typed one.
                if value.root_type().is_some() {
                    self.put_schema_by_key(key_suffix, &value).await?;
                    self.insert_schema_by_key_local(key_suffix.to_string(), value)
                        .await;
                } else {
                    self.insert_schema_by_key_local(key_suffix.to_string(), existing)
                        .await;
                }
                self.ensure_schema_by_key_queryable(key_suffix).await?;
                return Ok(());
            }
            let conflict_action = SchemaConflictAction::from_env();
            match conflict_action {
                SchemaConflictAction::Reject => {
                    return Err(AortaError::schema_conflict(format!(
                        "schema_by_key mismatch for {} (existing hash: {}, new hash: {})",
                        key_suffix,
                        existing.hash().to_hex(),
                        value.hash().to_hex()
                    )));
                }
                SchemaConflictAction::Warn => {
                    aorta_core_warn!(
                        "Aorta WARN: schema_by_key conflict for {} (existing hash: {}, new hash: {}). Proceeding without verify.",
                        key_suffix,
                        existing.hash().to_hex(),
                        value.hash().to_hex()
                    );
                    self.put_schema_by_key(key_suffix, &value).await?;
                    self.insert_schema_by_key_local(key_suffix.to_string(), value)
                        .await;
                    self.ensure_schema_by_key_queryable(key_suffix).await?;
                    return Ok(());
                }
            }
        }

        self.put_schema_by_key(key_suffix, &value).await?;
        self.insert_schema_by_key_local(key_suffix.to_string(), value.clone())
            .await;
        self.insert_local(value.hash(), value.bfbs().to_vec()).await;
        self.ensure_schema_by_key_queryable(key_suffix).await?;
        Ok(())
    }

    async fn register_service_schema_by_key(
        &self,
        key_suffix: &str,
        value: ServiceSchemaByKeyValue,
    ) -> AortaResult<()> {
        aorta_core_debug!(
            "registering service schema_by_key: key_suffix={}, service_hash={}, bfbs_len={}",
            key_suffix,
            value.service_hash.to_hex(),
            value.service_bfbs.len()
        );
        if let Some(existing) = self
            .fetch_schema_by_key_service(key_suffix.trim_start_matches("srv/"))
            .await?
        {
            if existing == value {
                aorta_core_debug!("service schema_by_key already matches existing value: key_suffix={}, service_hash={}", key_suffix, existing.service_hash.to_hex());
                self.insert_service_schema_by_key_local(key_suffix.to_string(), existing)
                    .await;
                self.ensure_schema_by_key_queryable(key_suffix).await?;
                if self.queryables.lock().await.active {
                    self.wait_for_service_schema_queryable_ready(key_suffix)
                        .await?;
                }
                return Ok(());
            }
            let conflict_action = SchemaConflictAction::from_env();
            match conflict_action {
                SchemaConflictAction::Reject => {
                    return Err(AortaError::schema_conflict(format!(
                        "service schema_by_key mismatch for {} (existing hash: {} | new hash: {})",
                        key_suffix,
                        existing.service_hash.to_hex(),
                        value.service_hash.to_hex()
                    )));
                }
                SchemaConflictAction::Warn => {
                    aorta_core_warn!(
                        "Aorta WARN: service schema_by_key conflict for {} (existing hash: {} | new hash: {}). Proceeding without verify.",
                        key_suffix,
                        existing.service_hash.to_hex(),
                        value.service_hash.to_hex()
                    );
                    self.put_service_schema_by_key(key_suffix, &value).await?;
                    self.insert_service_schema_by_key_local(key_suffix.to_string(), value)
                        .await;
                    self.ensure_schema_by_key_queryable(key_suffix).await?;
                    if self.queryables.lock().await.active {
                        self.wait_for_service_schema_queryable_ready(key_suffix)
                            .await?;
                    }
                    return Ok(());
                }
            }
        }

        self.put_service_schema_by_key(key_suffix, &value).await?;
        aorta_core_debug!(
            "service schema_by_key written to network: key_suffix={}, service_hash={}",
            key_suffix,
            value.service_hash.to_hex()
        );
        self.insert_service_schema_by_key_local(key_suffix.to_string(), value.clone())
            .await;
        aorta_core_debug!(
            "service schema_by_key inserted into local cache: key_suffix={}, service_hash={}",
            key_suffix,
            value.service_hash.to_hex()
        );
        if !value.service_bfbs.is_empty() {
            self.insert_local(value.service_hash, value.service_bfbs.clone())
                .await;
        }
        self.ensure_schema_by_key_queryable(key_suffix).await?;
        if self.queryables.lock().await.active {
            self.wait_for_service_schema_queryable_ready(key_suffix)
                .await?;
        }
        Ok(())
    }

    async fn get_schema_from_network(&self, hash: SchemaHash) -> AortaResult<Option<Vec<u8>>> {
        let key_expr = KeyExprBuilder::schema_key(&hash)?;
        let key_expr: String = key_expr.into();
        let replies = self.session.inner().get(key_expr).await?;

        let query_timeout = Self::schema_query_timeout();
        loop {
            let reply = match timeout(query_timeout, replies.recv_async()).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(_)) => break,
                Err(_) => return Ok(None),
            };

            if let Some(bytes) = reply_to_bytes(&reply) {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    #[allow(dead_code)]
    pub fn key_builder(&self) -> &KeyExprBuilder {
        &self.key_builder
    }
}

#[cfg(test)]
type RegisterFetchHook = Box<dyn Fn(&str) -> Option<SchemaByKeyValue> + Send + Sync + 'static>;

#[cfg(test)]
static REGISTER_FETCH_HOOK: OnceLock<Mutex<Option<RegisterFetchHook>>> = OnceLock::new();

#[cfg(test)]
fn register_fetch_hook() -> &'static Mutex<Option<RegisterFetchHook>> {
    REGISTER_FETCH_HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn set_register_fetch_hook_for_test(hook: Option<RegisterFetchHook>) {
    let mut guard = register_fetch_hook().lock().expect("lock");
    *guard = hook;
}

fn reply_to_bytes(reply: &Reply) -> Option<Vec<u8>> {
    let sample = reply.result().ok()?;
    let payload = sample.payload().to_bytes();
    Some(payload.into_owned())
}

fn schema_hash_from_full_key(full_key: &str) -> Option<SchemaHash> {
    const PREFIX: &str = "aorta/sys/schema/";
    let hash_hex = full_key.strip_prefix(PREFIX)?;
    SchemaHash::from_hex_str(hash_hex)
}

#[cfg(test)]
#[path = "../test/schema_registry_tests.rs"]
mod tests;
