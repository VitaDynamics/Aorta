//! Schema resolution: `schema_hash` / topic key → bfbs (the FlatBuffers binary
//! schema) + root type name. This is the canonical, self-contained core resolver
//! behind the SDK `SchemaResolver`. Recorders/observers use it to
//! register a self-describing MCAP `flatbuffer` schema for captured payloads,
//! resolving lazily and off the capture delivery path.
//!
//! Layering: this lives in L1 `core` (not `tools/aorta-introspect`) so the C-ABI
//! / SDK `SchemaResolver` never pulls `core` into a dependency on the tool layer.
//!
//! Cache semantics:
//! - `hash → schema` is IMMUTABLE (content-addressed): safe to cache for the
//!   resolver's lifetime; eviction only bounds memory and is always safe (a
//!   re-fetch yields the identical bytes).
//! - `key → schema` is MUTABLE (a key can be re-registered / overwritten):
//!   run-scoped, cleared by [`SchemaResolver::clear_key_cache`]. Consumers should
//!   PREFER the per-message attachment hash (`resolve_by_hash`) when present and
//!   fall back to `resolve_by_key` when hash lookup is unavailable, accepting
//!   only a matching hash when the wire carries one. The default attachment
//!   policy is *never*, so by-key is the common path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{AortaError, AortaResult};
use crate::schema::{bfbs_root_type_name, SchemaHash};
use crate::schema_registry::SchemaByKeyValue;

/// Maximum individual reply size accepted by [`SchemaResolver::resolve_by_hash_bounded`].
/// Replies larger than this are rejected with [`AortaError::CapacityExceeded`] and
/// are not cached.
const MAX_SCHEMA_REPLY_BYTES: usize = 1 << 20; // 1 MiB

/// Cumulative resolution counters (observability).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SchemaResolverStats {
    /// `resolve_by_hash` calls that ended in error/not-found.
    pub by_hash_failures: u64,
    /// `resolve_by_key` calls that ended in error/not-found.
    pub by_key_failures: u64,
    /// `by_hash` resolves that actually issued a network query (cache misses,
    /// positive and negative). The gap between calls and this counter is the
    /// cache hit count.
    pub by_hash_network_queries: u64,
    /// `by_key` resolves that actually issued a network query.
    pub by_key_network_queries: u64,
}

/// A resolved schema: content identity + binary schema + FlatBuffers root type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSchema {
    /// Content-addressed identity of `bfbs`.
    pub schema_hash: SchemaHash,
    /// FlatBuffers root type name (for the MCAP schema name).
    pub root_type_name: String,
    /// FlatBuffers binary schema; embed as an MCAP schema, encoding `"flatbuffer"`.
    pub bfbs: Vec<u8>,
}

/// Tunables for [`SchemaResolver`].
#[derive(Clone, Debug)]
pub struct SchemaResolverConfig {
    /// Upper bound on entries kept in each cache (memory bound; eviction is safe).
    pub cache_max_entries: usize,
    /// Deadline for a `by_hash` network query.
    pub by_hash_timeout: Duration,
    /// Deadline for a `by_key` network query.
    pub by_key_timeout: Duration,
    /// TTL for negative (not-found) caching. When non-zero, a resolve that ends
    /// not-found is remembered and repeat lookups for the same hash/key
    /// short-circuit to an immediate not-found — no network query — until the
    /// entry expires. This protects high-rate consumers (e.g. a recorder
    /// resolving per-message) from re-paying the full query timeout on every
    /// message for a schema that is simply not registered.
    /// `Duration::ZERO` (the default) disables negative caching,
    /// preserving poll-until-registered retry-loop semantics for existing
    /// callers. Transport errors are never negative-cached — only a completed
    /// query with no valid reply.
    pub negative_ttl: Duration,
}

impl Default for SchemaResolverConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: 256,
            by_hash_timeout: Duration::from_secs(2),
            by_key_timeout: Duration::from_secs(2),
            negative_ttl: Duration::ZERO,
        }
    }
}

/// Resolves schemas over Zenoh, with content-addressed (`by_hash`) and
/// key-scoped (`by_key`) caches. `&self` methods + interior mutability so a
/// single resolver can be shared (e.g. behind a C-ABI handle).
pub struct SchemaResolver {
    session: zenoh::Session,
    group: String,
    config: SchemaResolverConfig,
    by_hash_cache: Mutex<HashMap<SchemaHash, ResolvedSchema>>,
    by_key_cache: Mutex<HashMap<String, ResolvedSchema>>,
    negative_by_hash: Mutex<HashMap<SchemaHash, Instant>>,
    negative_by_key: Mutex<HashMap<String, Instant>>,
    by_hash_failures: AtomicU64,
    by_key_failures: AtomicU64,
    by_hash_network_queries: AtomicU64,
    by_key_network_queries: AtomicU64,
}

impl SchemaResolver {
    /// Create a resolver bound to a Zenoh `session` and aorta `group`
    /// (`group` scopes `by_key` lookups; `by_hash` is group-independent).
    pub fn new(
        session: zenoh::Session,
        group: impl Into<String>,
        config: SchemaResolverConfig,
    ) -> Self {
        Self {
            session,
            group: group.into(),
            config,
            by_hash_cache: Mutex::new(HashMap::new()),
            by_key_cache: Mutex::new(HashMap::new()),
            negative_by_hash: Mutex::new(HashMap::new()),
            negative_by_key: Mutex::new(HashMap::new()),
            by_hash_failures: AtomicU64::new(0),
            by_key_failures: AtomicU64::new(0),
            by_hash_network_queries: AtomicU64::new(0),
            by_key_network_queries: AtomicU64::new(0),
        }
    }

    /// Snapshot of cumulative resolution counters.
    pub fn stats(&self) -> SchemaResolverStats {
        SchemaResolverStats {
            by_hash_failures: self.by_hash_failures.load(Ordering::Relaxed),
            by_key_failures: self.by_key_failures.load(Ordering::Relaxed),
            by_hash_network_queries: self.by_hash_network_queries.load(Ordering::Relaxed),
            by_key_network_queries: self.by_key_network_queries.load(Ordering::Relaxed),
        }
    }

    /// Resolve by content hash (immutable cache; `aorta/sys/schema/<hash>`).
    pub async fn resolve_by_hash(&self, hash: &SchemaHash) -> AortaResult<ResolvedSchema> {
        let result = self.resolve_by_hash_inner(hash).await;
        if result.is_err() {
            self.by_hash_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn resolve_by_hash_inner(&self, hash: &SchemaHash) -> AortaResult<ResolvedSchema> {
        if let Some(hit) = self.cached_by_hash(hash) {
            return Ok(hit);
        }
        if self.negative_hit(&self.negative_by_hash, hash) {
            return Err(AortaError::not_found(format!(
                "schema by hash not found (negative-cached): {}",
                hash.to_hex()
            )));
        }
        self.by_hash_network_queries.fetch_add(1, Ordering::Relaxed);
        match fetch_bfbs_by_hash(&self.session, hash, self.config.by_hash_timeout).await {
            Ok(payload) => {
                let resolved = resolved_from_bfbs(*hash, payload)?;
                self.insert_hash(resolved.clone());
                Ok(resolved)
            }
            Err(err) => {
                if matches!(err, AortaError::NotFound(_)) {
                    self.note_negative(&self.negative_by_hash, *hash);
                }
                Err(err)
            }
        }
    }

    /// Resolve by content hash with a per-reply size cap.
    ///
    /// Like [`resolve_by_hash`](Self::resolve_by_hash), but processes replies
    /// one at a time and skips (without caching) any single reply larger than
    /// [`MAX_SCHEMA_REPLY_BYTES`]. The first reply whose recomputed hash matches
    /// `hash` wins and is cached; this is safe because `by_hash` replies are
    /// content-addressed. If every reply is oversized or none match,
    /// `CapacityExceeded` or `NotFound` is returned, respectively.
    pub async fn resolve_by_hash_bounded(&self, hash: &SchemaHash) -> AortaResult<ResolvedSchema> {
        let result = self.resolve_by_hash_bounded_inner(hash).await;
        if result.is_err() {
            self.by_hash_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn resolve_by_hash_bounded_inner(
        &self,
        hash: &SchemaHash,
    ) -> AortaResult<ResolvedSchema> {
        let selector = schema_by_hash_selector(hash);

        if let Some(hit) = self.cached_by_hash(hash) {
            if hit.bfbs.len() > MAX_SCHEMA_REPLY_BYTES {
                return Err(AortaError::capacity_exceeded(format!(
                    "cached schema for {} exceeds size limit: {} > {} bytes",
                    selector,
                    hit.bfbs.len(),
                    MAX_SCHEMA_REPLY_BYTES
                )));
            }
            return Ok(hit);
        }
        if self.negative_hit(&self.negative_by_hash, hash) {
            return Err(AortaError::not_found(format!(
                "schema by hash not found (negative-cached): {}",
                hash.to_hex()
            )));
        }

        self.by_hash_network_queries.fetch_add(1, Ordering::Relaxed);

        // Single deadline budget across get + drain (stamp `started` BEFORE the get,
        // else worst-case wall-clock is ~2×timeout).
        let started = Instant::now();
        let replies = tokio::time::timeout(
            self.config.by_hash_timeout,
            self.session
                .get(&selector)
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .timeout(self.config.by_hash_timeout),
        )
        .await
        .map_err(|_| AortaError::timeout(format!("query '{}' timed out", selector)))?
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "failed to query '{}': {}",
                selector, err
            )))
        })?;

        let mut oversized_seen = false;
        loop {
            let elapsed = started.elapsed();
            if elapsed >= self.config.by_hash_timeout {
                break;
            }
            let remaining = self.config.by_hash_timeout.saturating_sub(elapsed);
            match tokio::time::timeout(remaining, replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        let payload = sample.payload();
                        let len = payload.len();
                        if len > MAX_SCHEMA_REPLY_BYTES {
                            oversized_seen = true;
                            continue;
                        }
                        let bytes = payload.to_bytes().to_vec();
                        if SchemaHash::from_bfbs_bytes(&bytes) == *hash {
                            let resolved = resolved_from_bfbs(*hash, bytes)?;
                            self.insert_hash(resolved.clone());
                            return Ok(resolved);
                        }
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        if oversized_seen {
            return Err(AortaError::capacity_exceeded(format!(
                "all schema replies for {} exceeded size limit (max {} bytes)",
                selector, MAX_SCHEMA_REPLY_BYTES
            )));
        }

        self.note_negative(&self.negative_by_hash, *hash);
        Err(AortaError::not_found(format!(
            "schema by hash not found: {}",
            hash.to_hex()
        )))
    }

    /// Resolve by topic key suffix (mutable cache; `aorta/<group>/sys/schema_by_key/<key>`).
    pub async fn resolve_by_key(&self, key_suffix: &str) -> AortaResult<ResolvedSchema> {
        let result = self.resolve_by_key_prefer(key_suffix, None).await;
        if result.is_err() {
            self.by_key_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Resolve by topic key suffix, preferring a reply whose content hash
    /// matches `want` when one is given. During a rolling schema
    /// re-registration a stale and a current `schema_by_key` queryable can both
    /// answer; selecting (and caching) the first decodable reply would pin the
    /// stale schema and make a hash-filtered [`resolve`](Self::resolve) fail
    /// even though a matching reply was received. All decodable replies are
    /// scanned: a `want` match wins, otherwise the first decodable reply is
    /// selected (and cached) so hash-less callers keep working.
    async fn resolve_by_key_prefer(
        &self,
        key_suffix: &str,
        want: Option<&SchemaHash>,
    ) -> AortaResult<ResolvedSchema> {
        if let Some(hit) = self.cached_by_key(key_suffix) {
            if want.is_none_or(|hash| hit.schema_hash == *hash) {
                return Ok(hit);
            }
            // Cached schema is from an older registration: drop it and ask the
            // network instead of returning a stale entry the caller rejected.
            if let Ok(mut guard) = self.by_key_cache.lock() {
                guard.remove(key_suffix);
            }
        }
        if self.negative_hit(&self.negative_by_key, &key_suffix.to_string()) {
            return Err(AortaError::not_found(format!(
                "schema by key not found (negative-cached): {}",
                key_suffix
            )));
        }
        let selector = schema_by_key_selector(&self.group, key_suffix)?;
        self.by_key_network_queries.fetch_add(1, Ordering::Relaxed);
        let replies =
            collect_get_replies(&self.session, &selector, self.config.by_key_timeout).await?;
        let mut first_decodable: Option<ResolvedSchema> = None;
        for payload in replies {
            if let Ok(resolved) = decode_by_key_payload(&payload) {
                if want.is_some_and(|hash| resolved.schema_hash == *hash) {
                    // A by-key reply also pins a content hash → seed the immutable cache.
                    self.insert_hash(resolved.clone());
                    self.insert_key(key_suffix.to_string(), resolved.clone());
                    return Ok(resolved);
                }
                if first_decodable.is_none() {
                    first_decodable = Some(resolved);
                }
            }
        }
        if let Some(resolved) = first_decodable {
            // A by-key reply also pins a content hash → seed the immutable cache.
            self.insert_hash(resolved.clone());
            self.insert_key(key_suffix.to_string(), resolved.clone());
            return Ok(resolved);
        }
        self.note_negative(&self.negative_by_key, key_suffix.to_string());
        Err(AortaError::not_found(format!(
            "schema by key not found: {}",
            selector
        )))
    }

    /// Recorder convenience: resolve a captured message's schema, preferring the
    /// per-message attachment `schema_hash` (immutable, no re-resolve) and
    /// falling back to `by_key`, where a reply matching the attachment hash is
    /// preferred over a stale one. Returns `None` when neither resolves — the
    /// caller then records schema-less (recording is never blocked by a miss).
    pub async fn resolve(
        &self,
        key_suffix: &str,
        attachment_hash: Option<&SchemaHash>,
    ) -> Option<ResolvedSchema> {
        if let Some(hash) = attachment_hash {
            if let Ok(resolved) = self.resolve_by_hash(hash).await {
                return Some(resolved);
            }
        }
        let result = self
            .resolve_by_key_prefer(key_suffix, attachment_hash)
            .await;
        if result.is_err() {
            self.by_key_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
            .ok()
            .filter(|resolved| attachment_hash.is_none_or(|hash| resolved.schema_hash == *hash))
    }

    /// Drop the mutable key→schema cache (e.g. after a known re-registration).
    /// The content-addressed hash cache is left intact (always valid). Negative
    /// by-key entries are dropped too: a re-registration is exactly the event
    /// that invalidates a remembered miss.
    pub fn clear_key_cache(&self) {
        if let Ok(mut guard) = self.by_key_cache.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.negative_by_key.lock() {
            guard.clear();
        }
    }

    fn cached_by_hash(&self, hash: &SchemaHash) -> Option<ResolvedSchema> {
        self.by_hash_cache.lock().ok()?.get(hash).cloned()
    }

    fn cached_by_key(&self, key_suffix: &str) -> Option<ResolvedSchema> {
        self.by_key_cache.lock().ok()?.get(key_suffix).cloned()
    }

    fn insert_hash(&self, resolved: ResolvedSchema) {
        if let Ok(mut guard) = self.negative_by_hash.lock() {
            guard.remove(&resolved.schema_hash);
        }
        if let Ok(mut guard) = self.by_hash_cache.lock() {
            guard.insert(resolved.schema_hash, resolved);
            trim(&mut *guard, self.config.cache_max_entries);
        }
    }

    fn insert_key(&self, key_suffix: String, resolved: ResolvedSchema) {
        if let Ok(mut guard) = self.negative_by_key.lock() {
            guard.remove(&key_suffix);
        }
        if let Ok(mut guard) = self.by_key_cache.lock() {
            guard.insert(key_suffix, resolved);
            trim(&mut *guard, self.config.cache_max_entries);
        }
    }

    /// True when `key` has a fresh negative entry (a remembered not-found
    /// younger than `negative_ttl`). Stale entries are removed on probe.
    /// Always false when negative caching is disabled (`negative_ttl == 0`).
    fn negative_hit<K>(&self, map: &Mutex<HashMap<K, Instant>>, key: &K) -> bool
    where
        K: std::hash::Hash + Eq,
    {
        if self.config.negative_ttl.is_zero() {
            return false;
        }
        let Ok(mut guard) = map.lock() else {
            return false;
        };
        match guard.get(key) {
            Some(noted) if noted.elapsed() < self.config.negative_ttl => true,
            Some(_) => {
                guard.remove(key);
                false
            }
            None => false,
        }
    }

    /// Remember a completed-but-not-found resolve. No-op when negative caching
    /// is disabled. Bounded by the same `cache_max_entries` as the positive
    /// caches (eviction is always safe — worst case one extra network query).
    fn note_negative<K>(&self, map: &Mutex<HashMap<K, Instant>>, key: K)
    where
        K: std::hash::Hash + Eq + Clone,
    {
        if self.config.negative_ttl.is_zero() {
            return;
        }
        if let Ok(mut guard) = map.lock() {
            guard.insert(key, Instant::now());
            trim(&mut *guard, self.config.cache_max_entries);
        }
    }
}

/// `aorta/sys/schema/<hash>` — global, group-independent.
fn schema_by_hash_selector(hash: &SchemaHash) -> String {
    format!("aorta/sys/schema/{}", hash.to_hex())
}

/// Fetch the raw BFBS bytes published under the global content-addressed key
/// `aorta/sys/schema/<hash>`, accepting only a reply whose recomputed hash
/// matches `hash`.
///
/// This is the one place that speaks the by-hash wire protocol. [`SchemaResolver`]
/// layers caching and root-type extraction on top; callers that need the raw
/// bytes without a root type (the action schema fallback in
/// [`crate::action_server::fetch_action_schema_bfbs`]) use it directly.
pub async fn fetch_bfbs_by_hash(
    session: &zenoh::Session,
    hash: &SchemaHash,
    timeout: Duration,
) -> AortaResult<Vec<u8>> {
    let selector = schema_by_hash_selector(hash);
    let replies = collect_get_replies(session, &selector, timeout).await?;
    for payload in replies {
        // The global schema queryable returns the raw bfbs; trust only a
        // reply whose recomputed hash matches what we asked for.
        if SchemaHash::from_bfbs_bytes(&payload) == *hash {
            return Ok(payload);
        }
    }
    Err(AortaError::not_found(format!(
        "schema by hash not found: {}",
        hash.to_hex()
    )))
}

/// `aorta/<group>/sys/schema_by_key/<key_suffix>`.
fn schema_by_key_selector(group: &str, key_suffix: &str) -> AortaResult<String> {
    let group = group.trim();
    let key_suffix = key_suffix.trim();
    if group.is_empty() {
        return Err(AortaError::invalid_argument("group cannot be empty"));
    }
    if key_suffix.is_empty() {
        return Err(AortaError::invalid_argument("key suffix cannot be empty"));
    }
    Ok(format!("aorta/{}/sys/schema_by_key/{}", group, key_suffix))
}

/// Build a [`ResolvedSchema`] from a trusted (hash, bfbs) pair, extracting the
/// FlatBuffers root type name. Errors if the bfbs has no readable root type.
fn resolved_from_bfbs(hash: SchemaHash, bfbs: Vec<u8>) -> AortaResult<ResolvedSchema> {
    let root_type_name = bfbs_root_type_name(&bfbs)
        .ok_or_else(|| AortaError::invalid_argument("bfbs has no readable root type name"))?;
    Ok(ResolvedSchema {
        schema_hash: hash,
        root_type_name,
        bfbs,
    })
}

/// Decode + validate a `schema_by_key` reply payload (`SchemaByKeyValue`).
fn decode_by_key_payload(payload: &[u8]) -> AortaResult<ResolvedSchema> {
    let value = SchemaByKeyValue::decode(payload)
        .ok_or_else(|| AortaError::invalid_argument("failed to decode schema_by_key payload"))?;
    let recomputed = SchemaHash::from_bfbs_bytes(value.bfbs());
    if recomputed != value.hash() {
        return Err(AortaError::invalid_argument(format!(
            "schema hash mismatch: payload claims {}, recomputed {}",
            value.hash().to_hex(),
            recomputed.to_hex()
        )));
    }
    // Wire-carried topic_root_type wins over the bfbs-declared root (multi-table
    // schemas publish a non-root table); legacy payloads without it keep the
    // bfbs fallback. Mirrors the introspect-side resolver semantics.
    match value.root_type() {
        Some(root_type_name) => Ok(ResolvedSchema {
            schema_hash: value.hash(),
            root_type_name: root_type_name.to_string(),
            bfbs: value.bfbs().to_vec(),
        }),
        None => resolved_from_bfbs(value.hash(), value.bfbs().to_vec()),
    }
}

fn trim<K, V>(map: &mut HashMap<K, V>, max: usize)
where
    K: std::hash::Hash + Eq + Clone,
{
    while map.len() > max {
        let Some(key) = map.keys().next().cloned() else {
            break;
        };
        map.remove(&key);
    }
}

async fn collect_get_replies(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> AortaResult<Vec<Vec<u8>>> {
    // Single deadline budget across get + drain (stamp `started` BEFORE the get,
    // else worst-case wall-clock is ~2×timeout).
    let started = std::time::Instant::now();
    // QueryTarget::All: the global `aorta/sys/schema/**` queryable is declared by
    // every node, so we must gather from all matching servers, not the single
    // "best matching" one (which may not hold this schema).
    let replies = tokio::time::timeout(
        timeout,
        session
            .get(selector)
            .target(zenoh::query::QueryTarget::All)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .timeout(timeout),
    )
    .await
    .map_err(|_| AortaError::timeout(format!("query '{}' timed out", selector)))?
    .map_err(|err| {
        AortaError::transport(crate::error::TransportError::backend(format!(
            "failed to query '{}': {}",
            selector, err
        )))
    })?;

    let mut out = Vec::new();
    loop {
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            break;
        }
        let remaining = timeout.saturating_sub(elapsed);
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => {
                if let Ok(sample) = reply.into_result() {
                    out.push(sample.payload().to_bytes().to_vec());
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
#[path = "../test/schema_resolve_tests.rs"]
mod tests;
