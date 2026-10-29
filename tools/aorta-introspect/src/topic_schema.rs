use std::collections::HashMap;
use std::time::Duration;

use aorta_core::schema::SchemaHash;
use aorta_core::schema_registry::SchemaByKeyValue;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::error::{IntrospectError, IntrospectErrorKind, IntrospectResult};
use crate::schema::{ResolvedTopicSchema, ResolvedTopicSchemaKind};

#[derive(Debug, Clone)]
pub struct TopicSchemaResolverConfig {
    pub cache_max_entries: usize,
    pub by_hash_timeout: Duration,
    pub by_key_timeout: Duration,
    pub attachment_fallback: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TopicSchemaResolverStats {
    pub by_hash_failures: u64,
    pub by_key_failures: u64,
}

impl Default for TopicSchemaResolverConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: 256,
            by_hash_timeout: Duration::from_secs(2),
            by_key_timeout: Duration::from_secs(2),
            attachment_fallback: true,
        }
    }
}

pub struct TopicSchemaResolver {
    config: TopicSchemaResolverConfig,
    by_hash_cache: HashMap<String, Vec<u8>>,
    by_key_cache: HashMap<String, ResolvedTopicSchema>,
    stats: TopicSchemaResolverStats,
}

impl TopicSchemaResolver {
    pub fn new(config: TopicSchemaResolverConfig) -> Self {
        Self {
            config,
            by_hash_cache: HashMap::new(),
            by_key_cache: HashMap::new(),
            stats: TopicSchemaResolverStats::default(),
        }
    }

    pub fn stats(&self) -> TopicSchemaResolverStats {
        self.stats
    }

    fn cached_by_key(&self, key_suffix: &str) -> Option<ResolvedTopicSchema> {
        self.by_key_cache.get(key_suffix).cloned()
    }

    pub async fn get_by_hash(
        &mut self,
        session: &zenoh::Session,
        schema_hash: &str,
    ) -> IntrospectResult<Vec<u8>> {
        let canonical_hash = match canonicalize_schema_hash(schema_hash) {
            Ok(hash) => hash,
            Err(err) => {
                self.stats.by_hash_failures = self.stats.by_hash_failures.saturating_add(1);
                return Err(err);
            }
        };
        if let Some(bfbs) = self.by_hash_cache.get(&canonical_hash) {
            return Ok(bfbs.clone());
        }

        let selector = schema_by_hash_selector(&canonical_hash)?;
        let expected_hash = parse_schema_hash(&canonical_hash)?;
        let replies =
            match collect_get_replies(session, &selector, self.config.by_hash_timeout).await {
                Ok(replies) => replies,
                Err(err) => {
                    self.stats.by_hash_failures = self.stats.by_hash_failures.saturating_add(1);
                    return Err(err);
                }
            };

        for payload in replies {
            let recomputed = SchemaHash::from_bfbs_bytes(&payload);
            if recomputed == expected_hash {
                self.insert_hash_cache(canonical_hash, payload.clone());
                return Ok(payload);
            }
        }

        self.stats.by_hash_failures = self.stats.by_hash_failures.saturating_add(1);
        Err(IntrospectError::new(
            IntrospectErrorKind::NotFound,
            format!("schema by hash not found: {}", schema_hash),
        ))
    }

    pub async fn get_by_key(
        &mut self,
        session: &zenoh::Session,
        group: &str,
        key_suffix: &str,
    ) -> IntrospectResult<ResolvedTopicSchema> {
        self.get_by_key_prefer(session, group, key_suffix, None)
            .await
    }

    /// `get_by_key` variant that prefers a reply whose schema hash matches
    /// `want` (ASCII case-insensitive) when one is given. During a rolling
    /// re-registration a stale and a current `schema_by_key` queryable can both
    /// answer; selecting (and caching) the first decodable reply would pin the
    /// stale schema and make a hash-filtered [`resolve`](Self::resolve) fail
    /// even though a matching reply was received. All decodable replies are
    /// scanned: a `want` match wins, otherwise the first decodable reply is
    /// selected (and cached).
    async fn get_by_key_prefer(
        &mut self,
        session: &zenoh::Session,
        group: &str,
        key_suffix: &str,
        want: Option<&str>,
    ) -> IntrospectResult<ResolvedTopicSchema> {
        if let Some(schema) = self.by_key_cache.get(key_suffix) {
            if want.is_none_or(|hash| schema.schema_hash.eq_ignore_ascii_case(hash)) {
                return Ok(schema.clone());
            }
            // Cached schema is from an older registration: drop it and ask the
            // network instead of returning a stale entry the caller rejected.
            self.by_key_cache.remove(key_suffix);
        }

        let selector = match topic_schema_by_key_selector(group, key_suffix) {
            Ok(selector) => selector,
            Err(err) => {
                self.stats.by_key_failures = self.stats.by_key_failures.saturating_add(1);
                return Err(err);
            }
        };
        let replies =
            match collect_get_replies(session, &selector, self.config.by_key_timeout).await {
                Ok(replies) => replies,
                Err(err) => {
                    self.stats.by_key_failures = self.stats.by_key_failures.saturating_add(1);
                    return Err(err);
                }
            };

        let mut first_decodable: Option<ResolvedTopicSchema> = None;
        for payload in replies {
            if let Ok(schema) = decode_topic_schema_payload(key_suffix, &payload) {
                if want.is_some_and(|hash| schema.schema_hash.eq_ignore_ascii_case(hash)) {
                    self.insert_hash_cache(schema.schema_hash.clone(), schema.bfbs.to_vec());
                    self.insert_key_cache(key_suffix.to_string(), schema.clone());
                    return Ok(schema);
                }
                if first_decodable.is_none() {
                    first_decodable = Some(schema);
                }
            }
        }
        if let Some(schema) = first_decodable {
            self.insert_hash_cache(schema.schema_hash.clone(), schema.bfbs.to_vec());
            self.insert_key_cache(key_suffix.to_string(), schema.clone());
            return Ok(schema);
        }

        self.stats.by_key_failures = self.stats.by_key_failures.saturating_add(1);
        Err(IntrospectError::new(
            IntrospectErrorKind::NotFound,
            format!("topic schema by key not found: {}", selector),
        ))
    }

    pub async fn resolve(
        &mut self,
        session: &zenoh::Session,
        group: &str,
        key_suffix: &str,
        attachment_hash: Option<&str>,
    ) -> IntrospectResult<Option<ResolvedTopicSchema>> {
        if let Some(hash_hex) = attachment_hash {
            match self.get_by_hash(session, hash_hex).await {
                Ok(bfbs) => {
                    let schema = ResolvedTopicSchema::new(key_suffix, bfbs);
                    self.insert_key_cache(key_suffix.to_string(), schema.clone());
                    return Ok(Some(schema));
                }
                Err(_) => {}
            }
        }

        if let Some(schema) = self.cached_by_key(key_suffix) {
            if attachment_hash.is_none_or(|hash| schema.schema_hash.eq_ignore_ascii_case(hash)) {
                return Ok(Some(schema));
            }
            self.by_key_cache.remove(key_suffix);
        }

        match self
            .get_by_key_prefer(session, group, key_suffix, attachment_hash)
            .await
        {
            Ok(schema) => Ok(Some(schema).filter(|schema| {
                attachment_hash.is_none_or(|hash| schema.schema_hash.eq_ignore_ascii_case(hash))
            })),
            Err(_) if !self.config.attachment_fallback => Ok(None),
            Err(_) => Ok(None),
        }
    }

    fn insert_hash_cache(&mut self, schema_hash: String, bfbs: Vec<u8>) {
        self.by_hash_cache.insert(schema_hash, bfbs);
        self.trim_if_needed();
    }

    fn insert_key_cache(&mut self, key_suffix: String, schema: ResolvedTopicSchema) {
        self.by_key_cache.insert(key_suffix, schema);
        self.trim_if_needed();
    }

    fn trim_if_needed(&mut self) {
        while self.by_hash_cache.len() > self.config.cache_max_entries {
            if let Some(key) = self.by_hash_cache.keys().next().cloned() {
                self.by_hash_cache.remove(&key);
            } else {
                break;
            }
        }
        while self.by_key_cache.len() > self.config.cache_max_entries {
            if let Some(key) = self.by_key_cache.keys().next().cloned() {
                self.by_key_cache.remove(&key);
            } else {
                break;
            }
        }
    }
}

fn schema_by_hash_selector(schema_hash: &str) -> IntrospectResult<String> {
    Ok(format!(
        "aorta/sys/schema/{}",
        canonicalize_schema_hash(schema_hash)?
    ))
}

fn topic_schema_by_key_selector(group: &str, key_suffix: &str) -> IntrospectResult<String> {
    let group = group.trim();
    let key_suffix = key_suffix.trim();
    if group.is_empty() {
        return Err(IntrospectError::invalid_input("group cannot be empty"));
    }
    if key_suffix.is_empty() {
        return Err(IntrospectError::invalid_input(
            "topic key suffix cannot be empty",
        ));
    }
    Ok(format!("aorta/{}/sys/schema_by_key/{}", group, key_suffix))
}

fn canonicalize_schema_hash(schema_hash: &str) -> IntrospectResult<String> {
    let normalized = schema_hash.trim().to_ascii_lowercase();
    let hash = parse_schema_hash(&normalized)?;
    Ok(hash.to_hex())
}

pub fn decode_topic_schema_payload(
    key_suffix: &str,
    payload: &[u8],
) -> IntrospectResult<ResolvedTopicSchema> {
    let schema = SchemaByKeyValue::decode(payload)
        .ok_or_else(|| IntrospectError::decode("failed to decode topic schema_by_key payload"))?;
    let recomputed = SchemaHash::from_bfbs_bytes(schema.bfbs());
    if recomputed != schema.hash() {
        return Err(IntrospectError::new(
            IntrospectErrorKind::SchemaMismatch,
            format!(
                "schema hash mismatch: payload has {}, recomputed {}",
                schema.hash().to_hex(),
                recomputed.to_hex()
            ),
        ));
    }
    Ok(ResolvedTopicSchema::with_kind_and_root_type(
        key_suffix,
        ResolvedTopicSchemaKind::from(schema.kind()),
        schema.bfbs().to_vec(),
        schema.root_type().map(str::to_string),
    ))
}

pub fn extract_attachment_schema_hash(sample: &zenoh::sample::Sample) -> Option<String> {
    extract_attachment_schema_hash_bytes(&sample.attachment()?.to_bytes())
}

fn extract_attachment_schema_hash_bytes(bytes: &[u8]) -> Option<String> {
    // Single source of truth: delegate to the core composite-attachment
    // decoder so this path accepts BOTH the legacy bare 64-hex form AND the
    // versioned TLV form. A bare-64-hex-only check (the old implementation)
    // silently dropped the schema hash whenever a publisher attached trace
    // context, promoting the attachment to the composite TLV form.
    aorta_core::attachment::decode(bytes)
        .schema_hash
        .map(|hash| hash.to_hex())
}

fn parse_schema_hash(schema_hash: &str) -> IntrospectResult<SchemaHash> {
    SchemaHash::from_hex_str(schema_hash).ok_or_else(|| {
        IntrospectError::invalid_input(format!("invalid schema hash: {}", schema_hash))
    })
}

async fn collect_get_replies(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> IntrospectResult<Vec<Vec<u8>>> {
    // QueryTarget::All + no consolidation, matching the two sibling "GET with
    // timeout, collect replies" helpers in client.rs: the default target can
    // return only the nearest queryable's reply, which spuriously fails
    // schema resolution on a multi-node fleet.
    let replies = tokio::time::timeout(
        timeout,
        session
            .get(selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(timeout),
    )
    .await
    .map_err(|_| {
        IntrospectError::new(
            IntrospectErrorKind::Timeout,
            format!("query '{}' timed out after {:?}", selector, timeout),
        )
    })?
    .map_err(|err| {
        IntrospectError::new(
            IntrospectErrorKind::Transport,
            format!("failed to query '{}': {}", selector, err),
        )
    })?;

    let started = std::time::Instant::now();
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
mod tests {
    use super::{
        canonicalize_schema_hash, decode_topic_schema_payload,
        extract_attachment_schema_hash_bytes, schema_by_hash_selector,
        topic_schema_by_key_selector,
    };
    use aorta_core::schema::SchemaHash;
    use aorta_core::schema_registry::SchemaByKeyValue;
    use flatbuffers::FlatBufferBuilder;
    use flatbuffers_reflection::reflection;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hashfree_cached_by_key_must_match_attachment() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        let session = zenoh::open(config).await.unwrap();
        let mut resolver =
            super::TopicSchemaResolver::new(super::TopicSchemaResolverConfig::default());
        let schema = crate::schema::ResolvedTopicSchema::new(
            "pub/hashfree",
            build_test_bfbs("demo.HashFree"),
        );
        let hash = schema.schema_hash.clone();
        resolver.insert_key_cache("pub/hashfree".into(), schema.clone());
        assert!(resolver
            .resolve(&session, "default", "pub/hashfree", Some(&"ab".repeat(32)))
            .await
            .unwrap()
            .is_none());
        resolver.insert_key_cache("pub/hashfree".into(), schema);
        assert!(resolver
            .resolve(&session, "default", "pub/hashfree", Some(&hash))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_by_key_cache_retries_updated_network_schema() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        let session = zenoh::open(config).await.unwrap();
        let mut resolver =
            super::TopicSchemaResolver::new(super::TopicSchemaResolverConfig::default());
        let key = "aorta/default/sys/schema_by_key/pub/updated";
        let first = build_test_bfbs("demo.First");
        let latest = build_test_bfbs("demo.Latest");
        let latest_hash = SchemaHash::from_bfbs_bytes(&latest);
        let payload = std::sync::Arc::new(std::sync::RwLock::new(
            SchemaByKeyValue::topic(SchemaHash::from_bfbs_bytes(&first), first.clone(), None)
                .encode(),
        ));
        let served = payload.clone();
        let _queryable = session
            .declare_queryable(key)
            .callback(move |query| {
                use zenoh::Wait;
                query
                    .reply(key, served.read().unwrap().clone())
                    .wait()
                    .unwrap();
            })
            .await
            .unwrap();
        assert_eq!(
            resolver
                .get_by_key(&session, "default", "pub/updated")
                .await
                .unwrap()
                .bfbs
                .as_ref(),
            first
        );
        *payload.write().unwrap() =
            SchemaByKeyValue::topic(latest_hash, latest.clone(), None).encode();
        let resolved = resolver
            .resolve(
                &session,
                "default",
                "pub/updated",
                Some(&latest_hash.to_hex()),
            )
            .await
            .unwrap();
        assert!(
            resolved.is_some(),
            "stale cache must retry the live by-key queryable"
        );
        assert_eq!(resolved.unwrap().bfbs.as_ref(), latest);
        session.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_by_key_replies_prefer_matching_hash() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        let session = zenoh::open(config).await.unwrap();
        let mut resolver = super::TopicSchemaResolver::new(super::TopicSchemaResolverConfig {
            by_hash_timeout: std::time::Duration::from_millis(50),
            by_key_timeout: std::time::Duration::from_millis(300),
            ..Default::default()
        });
        let key = "aorta/default/sys/schema_by_key/pub/rolling";
        let stale = build_test_bfbs("demo.Stale");
        let stale_payload =
            SchemaByKeyValue::topic(SchemaHash::from_bfbs_bytes(&stale), stale, None).encode();
        let current = build_test_bfbs("demo.Current");
        let current_hash = SchemaHash::from_bfbs_bytes(&current);
        let current_payload = SchemaByKeyValue::topic(current_hash, current.clone(), None).encode();

        // The stale queryable answers immediately; the current one answers after
        // a short delay so the stale reply is deterministically received first.
        let stale_queryable = session.declare_queryable(key).await.unwrap();
        let stale_server = tokio::spawn(async move {
            while let Ok(query) = stale_queryable.recv_async().await {
                let _ = query.reply(key, stale_payload.clone()).await;
            }
        });
        let current_queryable = session.declare_queryable(key).await.unwrap();
        let current_server = tokio::spawn(async move {
            while let Ok(query) = current_queryable.recv_async().await {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let _ = query.reply(key, current_payload.clone()).await;
            }
        });

        let resolved = resolver
            .resolve(
                &session,
                "default",
                "pub/rolling",
                Some(&current_hash.to_hex()),
            )
            .await
            .unwrap()
            .expect("a matching reply must be selected among stale ones");
        assert_eq!(resolved.schema_hash, current_hash.to_hex());
        assert_eq!(resolved.bfbs.as_ref(), current);

        // The cache must pin the matching schema, not the stale first reply.
        let cached = resolver
            .get_by_key(&session, "default", "pub/rolling")
            .await
            .unwrap();
        assert_eq!(cached.schema_hash, current_hash.to_hex());
        assert_eq!(cached.bfbs.as_ref(), current);

        stale_server.abort();
        current_server.abort();
        session.close().await.unwrap();
    }

    fn build_test_bfbs(root_table_name: &str) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let fields = builder.create_vector::<flatbuffers::WIPOffset<reflection::Field>>(&[]);
        let name = builder.create_string(root_table_name);
        let root_table = reflection::Object::create(
            &mut builder,
            &reflection::ObjectArgs {
                name: Some(name),
                fields: Some(fields),
                is_struct: false,
                ..Default::default()
            },
        );
        let objects = builder.create_vector(&[root_table]);
        let enums = builder.create_vector::<flatbuffers::WIPOffset<reflection::Enum>>(&[]);
        let schema = reflection::Schema::create(
            &mut builder,
            &reflection::SchemaArgs {
                objects: Some(objects),
                enums: Some(enums),
                root_table: Some(root_table),
                ..Default::default()
            },
        );
        builder.finish(schema, Some("BFBS"));
        builder.finished_data().to_vec()
    }

    #[test]
    fn schema_by_hash_selector_uses_global_schema_path() {
        let selector = schema_by_hash_selector(
            "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
        )
        .expect("selector");
        assert_eq!(
            selector,
            "aorta/sys/schema/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn topic_schema_by_key_selector_uses_group_scoped_path() {
        let selector =
            topic_schema_by_key_selector("default", "pub/demo/chatter").expect("selector");
        assert_eq!(selector, "aorta/default/sys/schema_by_key/pub/demo/chatter");
    }

    #[test]
    fn canonicalize_schema_hash_accepts_uppercase() {
        let canonical = canonicalize_schema_hash(
            "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
        )
        .expect("canonical hash");
        assert_eq!(
            canonical,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn decode_topic_schema_payload_rejects_mismatched_hash() {
        let bfbs = vec![1u8, 2, 3, 4];
        let payload =
            SchemaByKeyValue::topic(SchemaHash::from_bfbs_bytes(&[9u8, 9, 9, 9]), bfbs, None)
                .encode();
        let err = decode_topic_schema_payload("pub/demo/chatter", &payload)
            .expect_err("mismatched hash should fail");
        assert!(err.to_string().contains("mismatch"));
    }

    #[test]
    fn decode_topic_schema_payload_prefers_wire_root_type() {
        let bfbs = build_test_bfbs("demo.DeclaredRoot");
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        let payload = SchemaByKeyValue::topic(hash, bfbs, Some("demo.Foo".into())).encode();

        let schema = decode_topic_schema_payload("pub/demo/chatter", &payload).expect("decode");
        assert_eq!(schema.default_root_type.as_deref(), Some("demo.Foo"));
    }

    #[test]
    fn decode_topic_schema_payload_falls_back_to_bfbs_root_type() {
        let bfbs = build_test_bfbs("demo.DeclaredRoot");
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        let payload = SchemaByKeyValue::topic(hash, bfbs, None).encode();

        let schema = decode_topic_schema_payload("pub/demo/chatter", &payload).expect("decode");
        assert_eq!(
            schema.default_root_type.as_deref(),
            Some("demo.DeclaredRoot")
        );
    }

    #[test]
    fn extract_attachment_schema_hash_bytes_requires_64_hex() {
        assert!(extract_attachment_schema_hash_bytes(b"abc").is_none());
        assert!(extract_attachment_schema_hash_bytes(&[b'g'; 64]).is_none());
        // Canonical bare attachments are LOWERCASE 64-hex (`SchemaHash::to_hex_bytes`).
        // After consolidation this path shares the core decoder, which — like
        // every other attachment decode path (subscriber/client/service) — is
        // lowercase-only; uppercase is not a real wire form (and user-typed hashes
        // go through `canonicalize_schema_hash`, not this function).
        assert_eq!(
            extract_attachment_schema_hash_bytes(&[b'a'; 64]).as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        // Uppercase bare-hex is no longer accepted here (now consistent with core).
        assert!(extract_attachment_schema_hash_bytes(&[b'A'; 64]).is_none());
    }

    #[test]
    fn extract_attachment_schema_hash_bytes_decodes_composite_tlv() {
        // When a publisher attaches trace/reply context the attachment is
        // promoted to the composite TLV form; the schema hash must still be
        // recovered (the legacy bare-64-hex-only check dropped it).
        let hash = SchemaHash::from_bytes([0xABu8; 32]);
        let expected = hash.to_hex();
        let composite = aorta_core::attachment::encode(&aorta_core::attachment::AttachmentParts {
            schema_hash: Some(hash),
            reply_busy_ns: Some(42),
            ..Default::default()
        })
        .expect("composite attachment");
        // Sanity: this is the composite form, not the legacy 64-hex form.
        assert_ne!(composite.len(), 64);
        assert_eq!(
            extract_attachment_schema_hash_bytes(&composite).as_deref(),
            Some(expected.as_str())
        );
    }
}
