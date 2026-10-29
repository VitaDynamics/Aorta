use super::{SchemaByKeyValue, SchemaHash, SchemaKind, ServiceSchemaByKeyValue};
use crate::error::AortaResult;
use crate::node::Node;
use crate::schema_by_key_fb;
use crate::schema_registry::set_register_fetch_hook_for_test;
use crate::test_support::{EnvVarGuard, ENV_LOCK};
use flatbuffers::FlatBufferBuilder;
use flatbuffers_reflection::reflection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use zenoh::query::{ConsolidationMode, QueryTarget};

fn build_test_service_bfbs(root_table_name: &str, object_names: &[&str]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let empty_fields = builder.create_vector::<flatbuffers::WIPOffset<reflection::Field>>(&[]);
    let mut objects = Vec::with_capacity(object_names.len());
    let mut root_table = None;

    for name in object_names {
        let name_offset = builder.create_string(name);
        let object = reflection::Object::create(
            &mut builder,
            &reflection::ObjectArgs {
                name: Some(name_offset),
                fields: Some(empty_fields),
                is_struct: false,
                ..Default::default()
            },
        );
        if *name == root_table_name {
            root_table = Some(object);
        }
        objects.push(object);
    }

    let objects = builder.create_vector(&objects);
    let enums = builder.create_vector::<flatbuffers::WIPOffset<reflection::Enum>>(&[]);
    let schema = reflection::Schema::create(
        &mut builder,
        &reflection::SchemaArgs {
            objects: Some(objects),
            enums: Some(enums),
            root_table,
            ..Default::default()
        },
    );
    builder.finish(schema, Some("BFBS"));
    builder.finished_data().to_vec()
}

#[test]
fn schema_by_key_encode_decode_roundtrip() {
    let bfbs = vec![1, 2, 3, 4, 5];
    let hash = SchemaHash::from_hex_str(
        "000000000000000000000000000000000000000000000000000000000000002a",
    )
    .expect("hash");
    let value = SchemaByKeyValue::topic(hash, bfbs.clone(), Some("demo.Foo".into()));
    let encoded = value.encode();
    let decoded = SchemaByKeyValue::decode(&encoded).expect("decode");
    assert_eq!(decoded.kind(), SchemaKind::Topic);
    assert_eq!(
        decoded.hash().to_hex(),
        "000000000000000000000000000000000000000000000000000000000000002a"
    );
    assert_eq!(decoded.bfbs(), bfbs.as_slice());
    assert_eq!(decoded.root_type(), Some("demo.Foo"));
}

#[test]
fn schema_by_key_encode_decode_roundtrip_without_root_type() {
    let bfbs = vec![1, 2, 3, 4, 5];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = SchemaByKeyValue::topic(hash, bfbs, None);
    let decoded = SchemaByKeyValue::decode(&value.encode()).expect("decode");
    assert_eq!(decoded.root_type(), None);
}

#[test]
fn schema_by_key_decode_rejects_service_kind() {
    let mut builder = FlatBufferBuilder::new();
    let service_hash_hex =
        builder.create_string("000000000000000000000000000000000000000000000000000000000000000b");
    let service_bfbs = builder.create_vector(&[7u8, 8u8, 9u8]);
    let request_root = builder.create_string("demo.Request");
    let response_root = builder.create_string("demo.Response");
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        SchemaKind::Service as u8,
        None,
        None,
        Some(service_hash_hex),
        Some(service_bfbs),
        Some(request_root),
        Some(response_root),
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();
    assert!(SchemaByKeyValue::decode(&bytes).is_none());
}

#[test]
fn schema_by_key_decode_rejects_missing_bfbs() {
    let mut builder = FlatBufferBuilder::new();
    let hash_hex =
        builder.create_string("000000000000000000000000000000000000000000000000000000000000002a");
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        SchemaKind::Topic as u8,
        Some(hash_hex),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();
    assert!(SchemaByKeyValue::decode(&bytes).is_none());
}

#[test]
fn schema_by_key_decode_rejects_unknown_kind() {
    let mut builder = FlatBufferBuilder::new();
    let hash_hex =
        builder.create_string("0000000000000000000000000000000000000000000000000000000000000001");
    let bfbs = builder.create_vector(&[1u8]);
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        9,
        Some(hash_hex),
        Some(bfbs),
        None,
        None,
        None,
        None,
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();
    assert!(SchemaByKeyValue::decode(&bytes).is_none());
}

#[test]
fn schema_by_key_decode_rejects_invalid_hash_hex() {
    let mut builder = FlatBufferBuilder::new();
    let hash_hex = builder.create_string("not_hex");
    let bfbs = builder.create_vector(&[1u8]);
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        SchemaKind::Topic as u8,
        Some(hash_hex),
        Some(bfbs),
        None,
        None,
        None,
        None,
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();
    assert!(SchemaByKeyValue::decode(&bytes).is_none());
}

#[test]
fn schema_by_key_decode_rejects_missing_hash_hex() {
    let mut builder = FlatBufferBuilder::new();
    let bfbs = builder.create_vector(&[1u8]);
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        SchemaKind::Topic as u8,
        None,
        Some(bfbs),
        None,
        None,
        None,
        None,
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();
    assert!(SchemaByKeyValue::decode(&bytes).is_none());
}

#[test]
fn service_schema_by_key_new_infers_root_types_from_bfbs() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = ServiceSchemaByKeyValue::new(hash, bfbs, None, None);
    assert_eq!(value.request_root_type.as_deref(), Some("demo.EchoRequest"));
    assert_eq!(
        value.response_root_type.as_deref(),
        Some("demo.EchoResponse")
    );
}

#[test]
fn service_schema_by_key_decode_infers_root_types_when_payload_missing_roots() {
    let bfbs = build_test_service_bfbs(
        "demo.EchoRequest",
        &["demo.EchoRequest", "demo.EchoResponse"],
    );
    let hash_hex = SchemaHash::from_bfbs_bytes(&bfbs).to_hex();

    let mut builder = FlatBufferBuilder::new();
    let service_hash_hex = builder.create_string(&hash_hex);
    let service_bfbs = builder.create_vector(&bfbs);
    let root = schema_by_key_fb::create_schema_by_key(
        &mut builder,
        SchemaKind::Service as u8,
        None,
        None,
        Some(service_hash_hex),
        Some(service_bfbs),
        None,
        None,
        None,
    );
    builder.finish(root, None);
    let bytes = builder.finished_data().to_vec();

    let decoded = ServiceSchemaByKeyValue::decode(&bytes).expect("decode");
    assert_eq!(
        decoded.request_root_type.as_deref(),
        Some("demo.EchoRequest")
    );
    assert_eq!(
        decoded.response_root_type.as_deref(),
        Some("demo.EchoResponse")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_schema_by_key_uses_local_cache() -> AortaResult<()> {
    let node = Node::new("schema_cache_node", "default").await?;
    let topic = "/schema/cache/topic";
    let key_suffix = node.key_builder().key_suffix_for_topic(topic)?;
    let bfbs = vec![1u8, 2u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = SchemaByKeyValue::topic(hash, bfbs.clone(), None);

    node.schema_registry()
        .insert_schema_by_key_local(key_suffix.clone(), value.clone())
        .await;

    node.schema_registry()
        .register_schema_by_key_topic(topic, hash, bfbs.clone(), None)
        .await?;

    let cached = node
        .schema_registry()
        .get_schema_by_key_local(&key_suffix)
        .await;
    assert_eq!(cached, Some(value));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_schema_by_key_logs_conflict() -> AortaResult<()> {
    let node = Node::new("schema_cache_conflict", "default").await?;
    let topic = "/schema/cache/conflict";
    let key_suffix = node.key_builder().key_suffix_for_topic(topic)?;

    let bfbs = vec![1u8, 2u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = SchemaByKeyValue::topic(hash, bfbs, None);
    node.schema_registry()
        .insert_schema_by_key_local(key_suffix.clone(), value)
        .await;

    let new_bfbs = vec![9u8, 9u8, 9u8];
    let new_hash = SchemaHash::from_bfbs_bytes(&new_bfbs);
    let result = node
        .schema_registry()
        .register_schema_by_key_topic(topic, new_hash, new_bfbs, None)
        .await;
    assert!(result.is_ok());

    node.close().await?;
    Ok(())
}

#[test]
fn queryable_enabled_from_env_defaults_true() {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_SCHEMA_QUERYABLE");
    assert!(super::SchemaRegistry::queryable_enabled_from_env());
}

#[test]
fn queryable_enabled_from_env_false_values() {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_QUERYABLE", "0");
    assert!(!super::SchemaRegistry::queryable_enabled_from_env());
    std::env::set_var("AORTA_SCHEMA_QUERYABLE", "false");
    assert!(!super::SchemaRegistry::queryable_enabled_from_env());
    std::env::set_var("AORTA_SCHEMA_QUERYABLE", "no");
    assert!(!super::SchemaRegistry::queryable_enabled_from_env());
    std::env::remove_var("AORTA_SCHEMA_QUERYABLE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_schema_by_key_declares_exact_queryable() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");
    let owner = Node::new("schemaexactowner", "default").await?;
    let observer = Node::new("schemaexactobserver", "default").await?;
    let topic = format!("/schema/exact/t{}", &owner.id()[..8]);
    let key_suffix = owner.key_builder().key_suffix_for_topic(&topic)?;
    let key_expr: String = owner.key_builder().schema_by_key(&key_suffix)?.into();

    let bfbs = vec![1u8, 2u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = SchemaByKeyValue::topic(hash, bfbs, None);
    owner
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, value.bfbs().to_vec(), None)
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let reply = loop {
        let replies = observer
            .session()
            .inner()
            .get(key_expr.clone())
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(Duration::from_millis(100))
            .await?;
        match tokio::time::timeout(Duration::from_millis(100), replies.recv_async()).await {
            Ok(Ok(reply)) => break reply,
            Ok(Err(_)) | Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(Err(err)) => panic!("exact schema_by_key reply: {err:?}"),
            Err(_) => panic!("exact schema_by_key query timeout"),
        }
    };
    assert!(reply.result().is_ok());

    observer.close().await?;
    owner.close().await?;
    Ok(())
}

/// Verifies the mechanism behind `aorta schema list`: a single wildcard GET over
/// `aorta/{group}/sys/schema_by_key/**` fans out to every per-key queryable, so
/// schema enumeration needs NO new core aggregate queryable — it is purely
/// client-side. (If this regresses, `schema list` would need a dedicated index.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_schema_by_key_get_enumerates_all_registered_schemas() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _queryable_guard = EnvVarGuard::remove("AORTA_SCHEMA_QUERYABLE");
    let owner = Node::new("schemalistowner", "default").await?;
    let observer = Node::new("schemalistobserver", "default").await?;

    let topics = ["/schema/list/alpha", "/schema/list/beta"];
    let mut expected_keys = std::collections::BTreeSet::new();
    for topic in topics {
        let suffix = owner.key_builder().key_suffix_for_topic(topic)?;
        let key: String = owner.key_builder().schema_by_key(&suffix)?.into();
        let bfbs = format!("bfbs-for-{}", topic).into_bytes();
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        owner
            .schema_registry()
            .register_schema_by_key_topic(topic, hash, bfbs, None)
            .await?;
        expected_keys.insert(key);
    }

    let wildcard = "aorta/default/sys/schema_by_key/**";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut got_keys = std::collections::BTreeSet::new();
    while got_keys.len() < expected_keys.len() && tokio::time::Instant::now() < deadline {
        let replies = observer
            .session()
            .inner()
            .get(wildcard)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(Duration::from_millis(200))
            .await?;
        while let Ok(Ok(reply)) =
            tokio::time::timeout(Duration::from_millis(200), replies.recv_async()).await
        {
            if let Ok(sample) = reply.result() {
                got_keys.insert(sample.key_expr().as_str().to_string());
            }
        }
        if got_keys.len() < expected_keys.len() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    assert!(
        expected_keys.is_subset(&got_keys),
        "wildcard GET should enumerate all registered schema_by_key entries; expected {:?}, got {:?}",
        expected_keys,
        got_keys
    );

    observer.close().await?;
    owner.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_schema_by_key_returns_none_when_missing() -> AortaResult<()> {
    let node = Node::new("schema_fetch_none", "default").await?;
    let key_suffix = node.key_builder().key_suffix_for_topic("/schema/missing")?;

    let fetch = node
        .schema_registry()
        .fetch_schema_by_key(&key_suffix)
        .await;
    let value = fetch?;
    assert!(value.is_none());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_schema_returns_none_when_missing() -> AortaResult<()> {
    let node = Node::new("schema_fetch_none_hash", "default").await?;
    let hash = SchemaHash::from_hex_str(
        "00000000000000000000000000000000000000000000000000000000000000aa",
    )
    .expect("hash");

    let fetch = node.schema_registry().fetch_schema(hash).await;
    let value = fetch?;
    assert!(value.is_none());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_service_schema_by_key_normalizes_leading_slash() -> AortaResult<()> {
    let node = Node::new("schema_fetch_service_norm", "default").await?;
    let service = "/schema_fetch_service_norm";

    let service_bfbs = vec![1u8, 2u8, 3u8];
    let value = ServiceSchemaByKeyValue::new(
        SchemaHash::from_bfbs_bytes(&service_bfbs),
        service_bfbs,
        None,
        None,
    );

    let key_suffix = node.key_builder().key_suffix_for_service(service)?;
    node.schema_registry()
        .insert_service_schema_by_key_local(key_suffix, value.clone())
        .await;

    let fetched = node
        .schema_registry()
        .fetch_schema_by_key_service(service)
        .await?;
    assert_eq!(fetched, Some(value));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_service_schema_by_key_uses_normalized_suffix() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _timeout_guard = EnvVarGuard::set("AORTA_SERVICE_SCHEMA_READY_TIMEOUT_MS", "1000");
    let _poll_guard = EnvVarGuard::set("AORTA_SERVICE_SCHEMA_READY_POLL_MS", "50");
    let node = Node::new("schema_register_service_norm", "default").await?;
    let service = format!("/schema_register_service_norm_{}", &node.id()[..8]);

    let service_bfbs = vec![10u8, 11u8];
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    node.schema_registry()
        .register_schema_by_key_service(&service, service_hash, service_bfbs, None, None)
        .await?;

    let normalized = node.key_builder().key_suffix_for_service(&service)?;
    let normalized_value = node
        .schema_registry()
        .get_service_schema_by_key_local(&normalized)
        .await;
    assert!(normalized_value.is_some());

    let legacy_double_slash = format!("srv//{}", service.trim_start_matches('/'));
    let legacy_value = node
        .schema_registry()
        .get_service_schema_by_key_local(&legacy_double_slash)
        .await;
    assert!(legacy_value.is_none());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_service_schema_by_key_skips_ready_wait_when_queryables_disabled(
) -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _queryable_guard = EnvVarGuard::set("AORTA_SCHEMA_QUERYABLE", "0");
    let _timeout_guard = EnvVarGuard::set("AORTA_SERVICE_SCHEMA_READY_TIMEOUT_MS", "20");
    let _poll_guard = EnvVarGuard::set("AORTA_SERVICE_SCHEMA_READY_POLL_MS", "10");
    let node = Node::new("schema_register_service_no_queryable", "default").await?;
    let service = format!("/schema_register_service_no_queryable_{}", &node.id()[..8]);

    let service_bfbs = vec![10u8, 11u8];
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);

    let result = node
        .schema_registry()
        .register_schema_by_key_service(&service, service_hash, service_bfbs.clone(), None, None)
        .await;
    assert!(
        result.is_ok(),
        "service registration should not wait for a disabled queryable"
    );

    let normalized = node.key_builder().key_suffix_for_service(&service)?;
    let normalized_value = node
        .schema_registry()
        .get_service_schema_by_key_local(&normalized)
        .await;
    assert!(normalized_value.is_some());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_schema_by_key_register_path_performs_single_precheck_fetch() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node_name = format!("schema_concurrent_conflict_{}", std::process::id());
    let node = Node::new(&node_name, "default").await?;
    let topic = format!("/schema/concurrent/p{}", std::process::id());
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fetch_count_hook = Arc::clone(&fetch_count);

    set_register_fetch_hook_for_test(Some(Box::new(move |key| {
        if key == key_suffix {
            fetch_count_hook.fetch_add(1, Ordering::Relaxed);
        }
        None
    })));

    let bfbs = vec![1u8, 2u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let result = node
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs, None)
        .await;

    set_register_fetch_hook_for_test(None);

    assert!(result.is_ok(), "register should succeed");
    assert!(
        fetch_count.load(Ordering::Relaxed) == 1,
        "register path should run a single precheck fetch"
    );

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_accepts_hash_equal_entry_lacking_root_type_metadata() -> AortaResult<()> {
    // Mixed-version rollout: a legacy publisher registered the same key with
    // the same hash/bfbs but no topic_root_type. That is an additive metadata
    // difference, not a schema conflict — even under reject mode — and the
    // richer (root-typed) entry must win in the local cache.
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "reject");
    let node_name = format!("schema_meta_upgrade_{}", std::process::id());
    let node = Node::new(&node_name, "default").await?;
    let topic = format!("/schema/meta_upgrade/p{}", std::process::id());
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let bfbs = vec![1u8, 2u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let legacy = SchemaByKeyValue::topic(hash, bfbs.clone(), None);
    let hook_key = key_suffix.clone();
    set_register_fetch_hook_for_test(Some(Box::new(move |key| {
        (key == hook_key).then(|| legacy.clone())
    })));

    let result = node
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs, Some("demo.Foo".into()))
        .await;

    set_register_fetch_hook_for_test(None);
    std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");

    result?;
    let cached = node
        .schema_registry()
        .fetch_schema_by_key(&key_suffix)
        .await?
        .expect("local cache entry");
    assert_eq!(cached.root_type(), Some("demo.Foo"));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_keeps_richer_entry_when_new_value_lacks_root_type() -> AortaResult<()> {
    // Inverse rollout order: an upgraded publisher already registered the
    // root-typed entry; a legacy publisher joining later must neither conflict
    // nor downgrade the cached metadata.
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "reject");
    let node_name = format!("schema_meta_keep_{}", std::process::id());
    let node = Node::new(&node_name, "default").await?;
    let topic = format!("/schema/meta_keep/p{}", std::process::id());
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let bfbs = vec![4u8, 5u8, 6u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let upgraded = SchemaByKeyValue::topic(hash, bfbs.clone(), Some("demo.Foo".into()));
    let hook_key = key_suffix.clone();
    set_register_fetch_hook_for_test(Some(Box::new(move |key| {
        (key == hook_key).then(|| upgraded.clone())
    })));

    let result = node
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs, None)
        .await;

    set_register_fetch_hook_for_test(None);
    std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");

    result?;
    let cached = node
        .schema_registry()
        .fetch_schema_by_key(&key_suffix)
        .await?
        .expect("local cache entry");
    assert_eq!(cached.root_type(), Some("demo.Foo"));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_rejects_contradictory_root_types_on_same_bfbs() -> AortaResult<()> {
    // Same key, same bfbs, but BOTH sides claim a (different) root table:
    // two publishers publishing different tables of one multi-table schema on
    // one topic. That is a real conflict and reject mode must fail it.
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "reject");
    let node_name = format!("schema_meta_conflict_{}", std::process::id());
    let node = Node::new(&node_name, "default").await?;
    let topic = format!("/schema/meta_conflict/p{}", std::process::id());
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let bfbs = vec![7u8, 8u8, 9u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let other = SchemaByKeyValue::topic(hash, bfbs.clone(), Some("demo.Bar".into()));
    let hook_key = key_suffix.clone();
    set_register_fetch_hook_for_test(Some(Box::new(move |key| {
        (key == hook_key).then(|| other.clone())
    })));

    let result = node
        .schema_registry()
        .register_schema_by_key_topic(&topic, hash, bfbs, Some("demo.Foo".into()))
        .await;

    set_register_fetch_hook_for_test(None);
    std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");

    assert!(
        result.is_err(),
        "contradictory root types on the same bfbs must conflict in reject mode"
    );

    node.close().await?;
    Ok(())
}

#[test]
fn schema_query_timeout_from_env() {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_QUERY_TIMEOUT_MS", "15");
    let timeout = super::SchemaRegistry::schema_query_timeout();
    assert_eq!(timeout, Duration::from_millis(15));
    std::env::remove_var("AORTA_SCHEMA_QUERY_TIMEOUT_MS");
}

#[test]
fn schema_query_timeout_env_invalid_uses_default() {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_SCHEMA_QUERY_TIMEOUT_MS", "0");
    let timeout = super::SchemaRegistry::schema_query_timeout();
    assert_eq!(timeout, Duration::from_millis(200));
    std::env::set_var("AORTA_SCHEMA_QUERY_TIMEOUT_MS", "invalid");
    let timeout = super::SchemaRegistry::schema_query_timeout();
    assert_eq!(timeout, Duration::from_millis(200));
    std::env::remove_var("AORTA_SCHEMA_QUERY_TIMEOUT_MS");
}

mod schema_conflict_action_tests {
    use super::super::SchemaConflictAction;
    use crate::test_support::ENV_LOCK;

    #[test]
    fn test_default_is_warn() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
        assert_eq!(SchemaConflictAction::from_env(), SchemaConflictAction::Warn);
    }

    #[test]
    fn test_reject_explicit() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "reject");
        assert_eq!(
            SchemaConflictAction::from_env(),
            SchemaConflictAction::Reject
        );
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
    }

    #[test]
    fn test_warn_explicit() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "warn");
        assert_eq!(SchemaConflictAction::from_env(), SchemaConflictAction::Warn);
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
    }

    #[test]
    fn test_case_insensitive() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "WARN");
        assert_eq!(SchemaConflictAction::from_env(), SchemaConflictAction::Warn);
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "Reject");
        assert_eq!(
            SchemaConflictAction::from_env(),
            SchemaConflictAction::Reject
        );
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
    }

    #[test]
    fn test_whitespace_trimmed() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "  warn  ");
        assert_eq!(SchemaConflictAction::from_env(), SchemaConflictAction::Warn);
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
    }

    #[test]
    fn test_invalid_value_defaults_to_warn() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var("AORTA_SCHEMA_CONFLICT_ACTION", "invalid");
        assert_eq!(SchemaConflictAction::from_env(), SchemaConflictAction::Warn);
        std::env::remove_var("AORTA_SCHEMA_CONFLICT_ACTION");
    }
}
