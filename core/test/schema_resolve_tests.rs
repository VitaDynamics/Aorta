//! Unit tests for the pure (non-network) pieces of `core::schema_resolve`:
//! selectors, by-key decode/validate, bfbs→ResolvedSchema, and cache trim.
//! The async `resolve_by_*` network paths are covered by integration tests.

use super::{
    decode_by_key_payload, resolved_from_bfbs, schema_by_hash_selector, schema_by_key_selector,
    trim, ResolvedSchema,
};
use crate::error::AortaError;
use crate::schema::SchemaHash;
use crate::schema_registry::SchemaByKeyValue;
use crate::test_support::EnvVarGuard;
use flatbuffers::FlatBufferBuilder;
use flatbuffers_reflection::reflection;
use std::collections::HashMap;

/// Minimal valid bfbs with a single root table named `root`.
fn bfbs_with_root(root: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let empty_fields = builder.create_vector::<flatbuffers::WIPOffset<reflection::Field>>(&[]);
    let name_offset = builder.create_string(root);
    let object = reflection::Object::create(
        &mut builder,
        &reflection::ObjectArgs {
            name: Some(name_offset),
            fields: Some(empty_fields),
            is_struct: false,
            ..Default::default()
        },
    );
    let objects = builder.create_vector(&[object]);
    let enums = builder.create_vector::<flatbuffers::WIPOffset<reflection::Enum>>(&[]);
    let schema = reflection::Schema::create(
        &mut builder,
        &reflection::SchemaArgs {
            objects: Some(objects),
            enums: Some(enums),
            root_table: Some(object),
            ..Default::default()
        },
    );
    builder.finish(schema, Some("BFBS"));
    builder.finished_data().to_vec()
}

#[test]
fn by_hash_selector_is_global_lowercase() {
    let hash = SchemaHash::from_bytes([0xABu8; 32]);
    assert_eq!(
        schema_by_hash_selector(&hash),
        format!("aorta/sys/schema/{}", hash.to_hex())
    );
    assert!(schema_by_hash_selector(&hash).starts_with("aorta/sys/schema/"));
}

#[test]
fn by_key_selector_is_group_scoped_and_validates() {
    assert_eq!(
        schema_by_key_selector("default", "pub/demo/chatter").unwrap(),
        "aorta/default/sys/schema_by_key/pub/demo/chatter"
    );
    assert!(schema_by_key_selector("  ", "pub/x").is_err());
    assert!(schema_by_key_selector("default", "  ").is_err());
}

#[test]
fn resolved_from_bfbs_extracts_root_type_name() {
    let bfbs = bfbs_with_root("demo.Chatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let resolved = resolved_from_bfbs(hash, bfbs.clone()).expect("resolve");
    assert_eq!(
        resolved,
        ResolvedSchema {
            schema_hash: hash,
            root_type_name: "demo.Chatter".to_string(),
            bfbs,
        }
    );
}

#[test]
fn resolved_from_bfbs_rejects_unreadable_bfbs() {
    let hash = SchemaHash::from_bfbs_bytes(&[1, 2, 3, 4]);
    assert!(resolved_from_bfbs(hash, vec![1, 2, 3, 4]).is_err());
}

#[test]
fn decode_by_key_payload_round_trips() {
    let bfbs = bfbs_with_root("demo.Chatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let payload = SchemaByKeyValue::topic(hash, bfbs.clone(), None).encode();
    let resolved = decode_by_key_payload(&payload).expect("decode");
    assert_eq!(resolved.schema_hash, hash);
    assert_eq!(resolved.root_type_name, "demo.Chatter");
    assert_eq!(resolved.bfbs, bfbs);
}

#[test]
fn decode_by_key_payload_prefers_wire_root_type() {
    // Multi-table schema case: the published table (carried on the wire as
    // topic_root_type) differs from the bfbs-declared root. The wire value
    // must win, matching the introspect-side resolver.
    let bfbs = bfbs_with_root("demo.Chatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let payload = SchemaByKeyValue::topic(hash, bfbs.clone(), Some("demo.Other".into())).encode();
    let resolved = decode_by_key_payload(&payload).expect("decode");
    assert_eq!(resolved.root_type_name, "demo.Other");
    assert_eq!(resolved.schema_hash, hash);
    assert_eq!(resolved.bfbs, bfbs);
}

#[test]
fn decode_by_key_payload_rejects_hash_mismatch() {
    let bfbs = bfbs_with_root("demo.Chatter");
    // Claim a hash that does NOT match the bfbs content.
    let wrong = SchemaHash::from_bytes([0x00u8; 32]);
    let payload = SchemaByKeyValue::topic(wrong, bfbs, None).encode();
    let err = decode_by_key_payload(&payload).expect_err("mismatch");
    assert!(err.to_string().contains("mismatch"));
}

#[test]
fn trim_bounds_cache_to_max() {
    let mut map: HashMap<String, i32> = HashMap::new();
    for i in 0..10 {
        map.insert(format!("k{i}"), i);
    }
    trim(&mut map, 4);
    assert_eq!(map.len(), 4);
    // A no-op when already under the cap.
    trim(&mut map, 4);
    assert_eq!(map.len(), 4);
}

/// Negative caching: once a resolve completes as
/// not-found, repeat lookups within `negative_ttl` must short-circuit without
/// re-paying the network-query timeout — the recorder resolves per message, so
/// an unregistered topic must not cost a full `by_key_timeout` every message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_cache_short_circuits_repeat_misses_within_ttl() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use crate::node::Node;
    use crate::test_support::ENV_LOCK;
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    let observer = Node::new("negcacheobserver", "default").await.unwrap();

    let timeout = Duration::from_millis(400);
    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: timeout,
            by_key_timeout: timeout,
            negative_ttl: Duration::from_secs(30),
            ..Default::default()
        },
    );

    // First miss issues exactly one network query; nothing serves this key.
    let key = format!("pub/negcache/none{}", &observer.id()[..8]);
    assert!(resolver.resolve_by_key(&key).await.is_err());
    assert_eq!(resolver.stats().by_key_network_queries, 1);

    // Repeat misses within the TTL must be served from the negative cache:
    // still not-found, but with NO additional network query (deterministic —
    // no timing assumptions).
    assert!(resolver.resolve_by_key(&key).await.is_err());
    assert!(resolver.resolve_by_key(&key).await.is_err());
    let stats = resolver.stats();
    assert_eq!(
        stats.by_key_network_queries, 1,
        "negative-cached misses must not issue network queries"
    );
    assert_eq!(
        stats.by_key_failures, 3,
        "every miss still counts as a failure"
    );

    // Same contract for the by-hash path.
    let hash = SchemaHash::from_bfbs_bytes(b"negcache-no-such-schema");
    assert!(resolver.resolve_by_hash(&hash).await.is_err());
    assert!(resolver.resolve_by_hash(&hash).await.is_err());
    let stats = resolver.stats();
    assert_eq!(stats.by_hash_network_queries, 1);
    assert_eq!(stats.by_hash_failures, 2);

    // `resolve()` (the recorder's per-message entry point) also short-circuits
    // on both legs: attachment hash first, then by-key — zero new queries.
    assert!(resolver.resolve(&key, Some(&hash)).await.is_none());
    let stats = resolver.stats();
    assert_eq!(stats.by_hash_network_queries, 1);
    assert_eq!(stats.by_key_network_queries, 1);

    observer.close().await.unwrap();
}

/// A negative entry must expire after `negative_ttl` (a late-starting
/// publisher registers the schema after the recorder's first miss), and
/// `clear_key_cache` must drop negative by-key entries immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_cache_expires_and_recovers_after_registration() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use crate::node::Node;
    use crate::test_support::{EnvVarGuard, ENV_LOCK};
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");

    let owner = Node::new("negcacheowner", "default").await.unwrap();
    let observer = Node::new("negcacherecover", "default").await.unwrap();

    let negative_ttl = Duration::from_millis(300);
    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(200),
            by_key_timeout: Duration::from_millis(200),
            negative_ttl,
            ..Default::default()
        },
    );

    // Miss once BEFORE the schema exists → a negative entry is noted.
    let topic = format!("/negcache/late{}", &owner.id()[..8]);
    let key_suffix = owner.key_builder().key_suffix_for_topic(&topic).unwrap();
    assert!(resolver.resolve_by_key(&key_suffix).await.is_err());

    // The publisher registers late.
    let bfbs = bfbs_with_root("demo.LateChatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    owner
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs.clone(), None)
        .await
        .unwrap();

    // Within the TTL the miss is still served from the negative cache: no new
    // network query even though the schema now exists.
    let queries_after_first_miss = resolver.stats().by_key_network_queries;
    assert!(
        resolver.resolve_by_key(&key_suffix).await.is_err(),
        "within negative_ttl the remembered miss must still short-circuit"
    );
    assert_eq!(
        resolver.stats().by_key_network_queries,
        queries_after_first_miss,
        "a negative-cached miss must not query the network"
    );

    // After expiry the resolver queries again and finds the late registration
    // (retry loop: cross-node queryable discovery is async).
    tokio::time::sleep(negative_ttl + Duration::from_millis(50)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let resolved = loop {
        match resolver.resolve_by_key(&key_suffix).await {
            Ok(r) => break r,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(negative_ttl.as_millis() as u64 + 50))
                    .await;
            }
            Err(e) => panic!("resolve_by_key never recovered after TTL expiry: {e}"),
        }
    };
    assert_eq!(resolved.schema_hash, hash);
    assert!(
        resolver.stats().by_key_network_queries > queries_after_first_miss,
        "recovery must have gone back to the network"
    );

    // A success purges the negative entry AND seeds the positive cache: an
    // immediate re-resolve stays Ok with zero additional queries.
    let queries_after_recovery = resolver.stats().by_key_network_queries;
    assert!(resolver.resolve_by_key(&key_suffix).await.is_ok());
    assert_eq!(
        resolver.stats().by_key_network_queries,
        queries_after_recovery
    );

    // clear_key_cache drops negative entries immediately (no TTL wait): a fresh
    // miss on another key, then clear, then the NEXT lookup goes back to the
    // network (query count increases — deterministic, no timing).
    let other = format!("pub/negcache/other{}", &observer.id()[..8]);
    assert!(resolver.resolve_by_key(&other).await.is_err());
    let queries_before_clear = resolver.stats().by_key_network_queries;
    assert!(resolver.resolve_by_key(&other).await.is_err());
    assert_eq!(
        resolver.stats().by_key_network_queries,
        queries_before_clear,
        "sanity: the repeat miss was negative-cached before the clear"
    );
    resolver.clear_key_cache();
    assert!(resolver.resolve_by_key(&other).await.is_err());
    assert_eq!(
        resolver.stats().by_key_network_queries,
        queries_before_clear + 1,
        "after clear_key_cache the miss must go back to the network"
    );

    observer.close().await.unwrap();
    owner.close().await.unwrap();
}

/// End-to-end: an owner node registers a schema; a separate node's resolver
/// fetches it both by key and by hash over the network, caches it, and reports
/// a clean not-found for an unknown key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolver_fetches_schema_by_key_and_hash_over_network() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use crate::node::Node;
    use crate::test_support::{EnvVarGuard, ENV_LOCK};
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    // Ensure the global `aorta/sys/schema/**` queryable is enabled.
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");

    let owner = Node::new("resolveowner", "default").await.unwrap();
    let observer = Node::new("resolveobserver", "default").await.unwrap();

    let bfbs = bfbs_with_root("demo.Chatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let topic = format!("/resolve/t{}", &owner.id()[..8]);
    let key_suffix = owner.key_builder().key_suffix_for_topic(&topic).unwrap();
    owner
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs.clone(), None)
        .await
        .unwrap();

    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(200),
            by_key_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );

    // Retry until the queryable is discovered (cross-node discovery is async).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let by_key = loop {
        match resolver.resolve_by_key(&key_suffix).await {
            Ok(r) => break r,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("resolve_by_key never succeeded: {e}"),
        }
    };
    assert_eq!(by_key.schema_hash, hash);
    assert_eq!(by_key.root_type_name, "demo.Chatter");
    assert_eq!(by_key.bfbs, bfbs);

    // by_hash resolves the same schema from the global schema queryable.
    let by_hash = loop {
        match resolver.resolve_by_hash(&hash).await {
            Ok(r) => break r,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("resolve_by_hash never succeeded: {e}"),
        }
    };
    assert_eq!(by_hash, by_key);

    // Cached: a second by-hash resolve returns the same value (now from cache).
    assert_eq!(resolver.resolve_by_hash(&hash).await.unwrap(), by_key);

    // Unknown key is a clean not-found — no panic, no hang.
    assert!(resolver.resolve_by_key("pub/does/not/exist").await.is_err());

    observer.close().await.unwrap();
    owner.close().await.unwrap();
}

/// `resolve_by_hash_bounded` fetches the same schema as `resolve_by_hash`,
/// caches it, and reports exactly one network query.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_by_hash_bounded_fetches_and_caches() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use crate::node::Node;
    use crate::test_support::ENV_LOCK;
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");

    let owner = Node::new("boundedowner", "default").await.unwrap();
    let observer = Node::new("boundedobserver", "default").await.unwrap();

    let bfbs = bfbs_with_root("demo.BoundedChatter");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let topic = format!("/bounded/t{}", &owner.id()[..8]);
    let _key_suffix = owner.key_builder().key_suffix_for_topic(&topic).unwrap();
    owner
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs.clone(), None)
        .await
        .unwrap();

    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(200),
            by_key_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let resolved = loop {
        match resolver.resolve_by_hash_bounded(&hash).await {
            Ok(r) => break r,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("resolve_by_hash_bounded never succeeded: {e}"),
        }
    };
    assert_eq!(resolved.schema_hash, hash);
    assert_eq!(resolved.root_type_name, "demo.BoundedChatter");
    assert_eq!(resolved.bfbs, bfbs);
    let queries_after_success = resolver.stats().by_hash_network_queries;
    assert!(
        queries_after_success >= 1,
        "discovery retries may issue more than one query"
    );

    // Second resolve must be served from cache with no extra network query.
    assert_eq!(
        resolver.resolve_by_hash_bounded(&hash).await.unwrap(),
        resolved
    );
    assert_eq!(
        resolver.stats().by_hash_network_queries,
        queries_after_success,
        "cached resolve must not issue another network query"
    );

    observer.close().await.unwrap();
    owner.close().await.unwrap();
}

/// `resolve_by_hash_bounded` rejects an oversized schema already seeded in the
/// shared `by_hash_cache` (e.g. by an unbounded `resolve_by_key`) instead of
/// cloning and returning it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_by_hash_bounded_rejects_oversized_cached_schema() {
    use super::{SchemaResolver, SchemaResolverConfig, MAX_SCHEMA_REPLY_BYTES};
    use crate::node::Node;
    use crate::test_support::ENV_LOCK;
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    // Ensure the global + per-key schema queryables are enabled so the
    // observer can resolve by-key and seed its shared hash cache.
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");

    let owner = Node::new("boundedcacheowner", "default").await.unwrap();
    let observer = Node::new("boundedcacheobserver", "default").await.unwrap();

    // Build a valid bfbs that is just over the bounded limit.
    let mut bfbs = bfbs_with_root("demo.OversizedCached");
    while bfbs.len() <= MAX_SCHEMA_REPLY_BYTES {
        bfbs.push(0);
    }
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let topic = format!("/bounded/cache/{}", &owner.id()[..8]);
    let key_suffix = owner.key_builder().key_suffix_for_topic(&topic).unwrap();
    owner
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs.clone(), None)
        .await
        .unwrap();

    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(200),
            by_key_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );

    // Seed the observer's shared hash cache via the unbounded by-key path.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match resolver.resolve_by_key(&key_suffix).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("resolve_by_key never succeeded: {e}"),
        }
    }

    // The bounded by-hash path must reject the cached oversized value.
    let err = resolver
        .resolve_by_hash_bounded(&hash)
        .await
        .expect_err("cached oversized schema should be rejected");
    assert!(
        matches!(err, AortaError::CapacityExceeded(_)),
        "expected capacity exceeded for cached oversized schema, got {err}"
    );

    observer.close().await.unwrap();
    owner.close().await.unwrap();
}

/// `resolve_by_hash_bounded` rejects a single oversized reply with a clear
/// error, does not cache it, and does not treat it as a negative hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_by_hash_bounded_rejects_oversized_reply() {
    use super::{SchemaResolver, SchemaResolverConfig, MAX_SCHEMA_REPLY_BYTES};
    use crate::node::Node;
    use crate::test_support::ENV_LOCK;
    use std::time::Duration;

    let _guard = ENV_LOCK.lock().expect("lock");
    // Disable the global schema queryable so only our malicious queryable replies.
    let _queryable_off = EnvVarGuard::set("AORTA_SCHEMA_QUERYABLE", "0");

    let observer = Node::new("boundedoversize", "default").await.unwrap();

    let hash = SchemaHash::from_bytes([0x42u8; 32]);
    let selector = format!("aorta/sys/schema/{}", hash.to_hex());
    let oversized = vec![0u8; MAX_SCHEMA_REPLY_BYTES + 1];

    let queryable = observer
        .session()
        .inner()
        .declare_queryable(&selector)
        .await
        .unwrap();
    let selector_for_task = selector.clone();
    let task = tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let _ = query
                .reply(selector_for_task.clone(), oversized.clone())
                .await;
        }
    });

    let resolver = SchemaResolver::new(
        observer.session().inner().clone(),
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(200),
            by_key_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );

    let err = resolver
        .resolve_by_hash_bounded(&hash)
        .await
        .expect_err("oversized reply should fail");
    assert!(
        matches!(err, AortaError::CapacityExceeded(_)),
        "expected capacity exceeded, got {err}"
    );
    assert_eq!(resolver.stats().by_hash_network_queries, 1);

    // The oversized reply must not be negative-cached: the next call goes back
    // to the network and fails with the same bounded-size error.
    let err2 = resolver
        .resolve_by_hash_bounded(&hash)
        .await
        .expect_err("repeat oversized reply should still fail");
    assert!(
        matches!(err2, AortaError::CapacityExceeded(_)),
        "expected capacity exceeded on repeat, got {err2}"
    );
    assert_eq!(resolver.stats().by_hash_network_queries, 2);

    task.abort();
    observer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hashfree_by_key_fallback_requires_attachment_hash_match() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use std::time::Duration;

    let mut config = zenoh::Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(config).await.unwrap();
    let key = "aorta/default/sys/schema_by_key/pub/hashfree";
    let queryable = session.declare_queryable(key).await.unwrap();
    let bfbs = bfbs_with_root("demo.HashFree");
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let payload = SchemaByKeyValue::topic(hash, bfbs.clone(), None).encode();
    let server = tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            query.reply(key, payload.clone()).await.unwrap();
        }
    });
    let resolver = SchemaResolver::new(
        session,
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(50),
            by_key_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );
    let other_hash = SchemaHash::from_bytes([0xab; 32]);
    assert!(
        resolver
            .resolve("pub/hashfree", Some(&other_hash))
            .await
            .is_none(),
        "a denied hash must not silently select a different by-key schema"
    );
    let resolved = resolver
        .resolve("pub/hashfree", Some(&hash))
        .await
        .expect("by-key fallback");
    assert_eq!(resolved.bfbs, bfbs);
    assert!(resolver.resolve("pub/hashfree", None).await.is_some());
    server.abort();
}

/// Rolling re-registration: a stale and a current `schema_by_key` queryable
/// both answer the same key, stale first. `resolve` must pick the reply whose
/// hash matches the attachment — and cache that one, not the stale first reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hashfree_by_key_fallback_selects_matching_reply_among_stale_ones() {
    use super::{SchemaResolver, SchemaResolverConfig};
    use std::time::Duration;

    let mut config = zenoh::Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(config).await.unwrap();
    let key = "aorta/default/sys/schema_by_key/pub/rolling";

    let stale_bfbs = bfbs_with_root("demo.Stale");
    let stale_payload =
        SchemaByKeyValue::topic(SchemaHash::from_bfbs_bytes(&stale_bfbs), stale_bfbs, None)
            .encode();
    let current_bfbs = bfbs_with_root("demo.Current");
    let current_hash = SchemaHash::from_bfbs_bytes(&current_bfbs);
    let current_payload =
        SchemaByKeyValue::topic(current_hash, current_bfbs.clone(), None).encode();

    // The stale queryable answers immediately; the current one answers after a
    // short delay so the stale reply is deterministically received first.
    let stale_queryable = session.declare_queryable(key).await.unwrap();
    let stale_key = key.to_string();
    let stale_server = tokio::spawn(async move {
        while let Ok(query) = stale_queryable.recv_async().await {
            let _ = query.reply(stale_key.clone(), stale_payload.clone()).await;
        }
    });
    let current_queryable = session.declare_queryable(key).await.unwrap();
    let current_key = key.to_string();
    let current_server = tokio::spawn(async move {
        while let Ok(query) = current_queryable.recv_async().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = query
                .reply(current_key.clone(), current_payload.clone())
                .await;
        }
    });

    let resolver = SchemaResolver::new(
        session,
        "default",
        SchemaResolverConfig {
            by_hash_timeout: Duration::from_millis(50),
            by_key_timeout: Duration::from_millis(300),
            ..Default::default()
        },
    );

    let resolved = resolver
        .resolve("pub/rolling", Some(&current_hash))
        .await
        .expect("a matching reply must be selected among stale ones");
    assert_eq!(resolved.schema_hash, current_hash);
    assert_eq!(resolved.bfbs, current_bfbs);

    // The cache must pin the matching schema, not the stale first reply.
    let cached = resolver
        .resolve_by_key("pub/rolling")
        .await
        .expect("cache hit");
    assert_eq!(cached.schema_hash, current_hash);
    assert_eq!(cached.bfbs, current_bfbs);

    stale_server.abort();
    current_server.abort();
}
