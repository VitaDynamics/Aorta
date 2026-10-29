use crate::output::{emit_json, output_format, OutputFormat};
use anyhow::{anyhow, Result};
use aorta_core::key_expr::normalize_path;
use aorta_core::KeyExprBuilder;
use aorta_introspect::error::IntrospectErrorKind;
use aorta_introspect::schema::{
    ResolvedSchema, ResolvedServiceSchema, ResolvedTopicSchema, ResolvedTopicSchemaKind,
    SchemaRootSelector,
};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub async fn get(
    key: &str,
    dump_bfbs: Option<&str>,
    describe: bool,
    group: Option<&str>,
) -> Result<()> {
    let group = group.unwrap_or("default");
    let key_suffixes = resolve_schema_key_suffix_candidates(group, key)?;
    let mut attempted_queries: Vec<String> = Vec::new();
    let mut decode_failed_query: Option<String> = None;

    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    for key_suffix in key_suffixes {
        let query_key = format!("aorta/{}/sys/schema_by_key/{}", group, key_suffix);
        attempted_queries.push(query_key.clone());

        match client.resolve_schema(&key_suffix).await {
            Ok(schema) => {
                print_schema(&key_suffix, &schema);
                if describe {
                    print_schema_details(&client, &schema)?;
                }
                if let Some(path) = dump_bfbs {
                    dump_schema_bfbs(path, &schema)?;
                }
                return Ok(());
            }
            Err(err) if err.kind == IntrospectErrorKind::NotFound => continue,
            Err(err)
                if err.kind == IntrospectErrorKind::Decode
                    || err.kind == IntrospectErrorKind::SchemaMismatch =>
            {
                decode_failed_query = Some(query_key);
            }
            Err(err) => {
                return Err(anyhow!(
                    "Failed to resolve schema for '{}': {}",
                    query_key,
                    err
                ));
            }
        }
    }

    if let Some(query_key) = decode_failed_query {
        return Err(anyhow!(
            "Failed to decode schema_by_key payload for '{}'",
            query_key
        ));
    }

    Err(anyhow!(
        "No schema response for candidates: {}",
        attempted_queries.join(", ")
    ))
}

fn resolve_schema_key_suffix_candidates(group: &str, key: &str) -> Result<Vec<String>> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Schema key cannot be empty"));
    }

    let key_builder =
        KeyExprBuilder::new(group).map_err(|e| anyhow!("Invalid group '{}': {}", group, e))?;
    let group_prefix = format!("aorta/{}/", group);

    if let Some(rest) = trimmed.strip_prefix(&group_prefix) {
        return Ok(vec![normalize_suffix_input(rest)?]);
    }
    if trimmed.starts_with("aorta/") {
        return Err(anyhow!(
            "Schema key '{}' is not in group '{}'",
            trimmed,
            group
        ));
    }

    if trimmed.starts_with("pub/")
        || trimmed.starts_with("srv/")
        || trimmed.starts_with("prm/")
        || trimmed.starts_with("sys/schema_by_key/")
    {
        return Ok(vec![normalize_suffix_input(trimmed)?]);
    }

    let mut candidates = Vec::new();
    let topic_suffix = key_builder
        .key_suffix_for_topic(trimmed)
        .map_err(|e| anyhow!("Invalid topic key '{}': {}", trimmed, e))?;
    push_unique_suffix(&mut candidates, topic_suffix);

    if let Ok(service_suffix) = key_builder.key_suffix_for_service(trimmed) {
        push_unique_suffix(&mut candidates, service_suffix);
    }

    Ok(candidates)
}

fn push_unique_suffix(candidates: &mut Vec<String>, suffix: String) {
    if !candidates.iter().any(|existing| existing == &suffix) {
        candidates.push(suffix);
    }
}

fn normalize_suffix_input(input: &str) -> Result<String> {
    if let Some(rest) = input.strip_prefix("sys/schema_by_key/") {
        return normalize_path(rest)
            .map_err(|e| anyhow!("Invalid schema suffix '{}': {}", rest, e));
    }
    normalize_path(input).map_err(|e| anyhow!("Invalid schema suffix '{}': {}", input, e))
}

fn print_schema(key_suffix: &str, schema: &ResolvedSchema) {
    match schema {
        ResolvedSchema::Topic(topic) => print_topic_schema(key_suffix, topic),
        ResolvedSchema::Service(service) => print_service_schema(key_suffix, service),
    }
}

fn print_topic_schema(key_suffix: &str, schema: &ResolvedTopicSchema) {
    let kind = match schema.kind {
        ResolvedTopicSchemaKind::Topic => "topic",
        ResolvedTopicSchemaKind::Param => "param",
    };
    println!("Schema key suffix: {}", key_suffix);
    println!("Kind: {}", kind);
    println!("Hash: {}", schema.schema_hash);
    println!("BFBS size: {} bytes", schema.bfbs.len());
}

fn print_service_schema(key_suffix: &str, schema: &ResolvedServiceSchema) {
    println!("Schema key suffix: {}", key_suffix);
    println!("Kind: service");
    println!("Service hash: {}", schema.service_hash);
    println!("Service BFBS size: {} bytes", schema.bfbs.len());
    println!(
        "Request root type: {}",
        schema.request_root_type.as_deref().unwrap_or("(none)")
    );
    println!(
        "Response root type: {}",
        schema.response_root_type.as_deref().unwrap_or("(none)")
    );
}

fn print_schema_details(
    client: &aorta_introspect::client::IntrospectClient,
    schema: &ResolvedSchema,
) -> Result<()> {
    match schema {
        ResolvedSchema::Topic(_) => print_topic_schema_details(client, schema),
        ResolvedSchema::Service(service) => print_service_schema_details(client, schema, service),
    }
}

fn print_topic_schema_details(
    client: &aorta_introspect::client::IntrospectClient,
    schema: &ResolvedSchema,
) -> Result<()> {
    let details = client
        .describe_schema(schema, SchemaRootSelector::DefaultRoot)
        .map_err(|e| anyhow!("Failed to describe topic schema: {}", e))?;
    println!();
    println!("Schema detail:");
    println!("{}", details);
    Ok(())
}

fn print_service_schema_details(
    client: &aorta_introspect::client::IntrospectClient,
    schema: &ResolvedSchema,
    service: &ResolvedServiceSchema,
) -> Result<()> {
    println!();
    println!("Schema detail:");

    if service.request_root_type.is_some() {
        let request_text = client
            .describe_schema(schema, SchemaRootSelector::ServiceRequest)
            .map_err(|e| anyhow!("Failed to describe service request schema: {}", e))?;
        println!("Request:");
        println!("{}", request_text);
    } else {
        match client.describe_schema(schema, SchemaRootSelector::DefaultRoot) {
            Ok(text) => {
                println!("Request (BFBS default root; metadata missing):");
                println!("{}", text);
            }
            Err(_) => {
                println!("Request: root type not provided");
            }
        }
    }

    if let Some(response_root) = service.response_root_type.as_deref() {
        if service.request_root_type.as_deref() == Some(response_root) {
            println!();
            println!("Response: same root type as request ('{}')", response_root);
            return Ok(());
        }

        let response_text = client
            .describe_schema(schema, SchemaRootSelector::ServiceResponse)
            .map_err(|e| anyhow!("Failed to describe service response schema: {}", e))?;
        println!();
        println!("Response:");
        println!("{}", response_text);
    } else {
        println!();
        println!("Response: root type not provided");
    }

    Ok(())
}

fn dump_schema_bfbs(base_path: &str, schema: &ResolvedSchema) -> Result<()> {
    let bfbs = match schema {
        ResolvedSchema::Topic(topic) => topic.bfbs.as_ref(),
        ResolvedSchema::Service(service) => service.bfbs.as_ref(),
    };
    write_bfbs_file(base_path, bfbs)
}

fn write_bfbs_file(base_path: &str, bfbs: &[u8]) -> Result<()> {
    if let Some(parent) = Path::new(base_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| anyhow!("Failed to create directory '{}': {}", parent.display(), e))?;
        }
    }
    fs::write(base_path, bfbs).map_err(|e| anyhow!("Failed to write '{}': {}", base_path, e))?;
    println!("Dumped bfbs to {}", base_path);
    Ok(())
}

/// `aorta schema list` — enumerate every registered schema (topic + service)
/// across the fleet via the introspect client's wildcard schema_by_key query.
pub async fn list(group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    let schemas = client
        .list_schemas()
        .await
        .map_err(|e| anyhow!("Failed to list schemas: {}", e))?;

    if output_format() == OutputFormat::Json {
        let rows: Vec<_> = schemas.iter().map(schema_list_json_row).collect();
        emit_json(&rows)?;
        return Ok(());
    }

    if schemas.is_empty() {
        println!("No registered schemas found in group '{}'.", group);
        return Ok(());
    }

    eprintln!("Registered schemas in group '{}':", group);
    println!(
        "  {:<40} {:<8} {:<18} {}",
        "key_suffix", "kind", "hash", "size"
    );
    for schema in &schemas {
        println!("  {}", format_schema_list_row(schema));
    }
    Ok(())
}

/// JSON row for one `schema list` entry: full hash (not truncated) plus the
/// same `key_suffix`/`kind`/`size` fields shown in text mode.
fn schema_list_json_row(schema: &ResolvedSchema) -> serde_json::Value {
    let (suffix, kind, hash, size) = match schema {
        ResolvedSchema::Topic(topic) => (
            topic.key_suffix.as_str(),
            match topic.kind {
                ResolvedTopicSchemaKind::Topic => "topic",
                ResolvedTopicSchemaKind::Param => "param",
            },
            topic.schema_hash.as_str(),
            topic.bfbs.len(),
        ),
        ResolvedSchema::Service(service) => (
            service.key_suffix.as_str(),
            "service",
            service.service_hash.as_str(),
            service.bfbs.len(),
        ),
    };
    serde_json::json!({
        "key_suffix": suffix,
        "kind": kind,
        "hash": hash,
        "size": size,
    })
}

/// Format one `schema list` row: `key_suffix  kind  hash16…  N bytes`.
fn format_schema_list_row(schema: &ResolvedSchema) -> String {
    let (suffix, kind, hash, size) = match schema {
        ResolvedSchema::Topic(topic) => (
            topic.key_suffix.as_str(),
            match topic.kind {
                ResolvedTopicSchemaKind::Topic => "topic",
                ResolvedTopicSchemaKind::Param => "param",
            },
            topic.schema_hash.as_str(),
            topic.bfbs.len(),
        ),
        ResolvedSchema::Service(service) => (
            service.key_suffix.as_str(),
            "service",
            service.service_hash.as_str(),
            service.bfbs.len(),
        ),
    };
    let short_hash = &hash[..hash.len().min(16)];
    format!(
        "{:<40} {:<8} {:<18} {} bytes",
        suffix, kind, short_hash, size
    )
}

/// `aorta schema get --hash <sha256>` — direct hash lookup of the raw BFBS.
pub async fn get_by_hash(hash: &str, dump_bfbs: Option<&str>, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    let bfbs = client
        .fetch_schema_by_hash(hash)
        .await
        .map_err(|e| anyhow!("Failed to fetch schema by hash '{}': {}", hash, e))?;

    println!("Schema hash: {}", hash.trim());
    println!("BFBS size: {} bytes", bfbs.len());
    if let Some(path) = dump_bfbs {
        write_bfbs_file(path, &bfbs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{format_schema_list_row, resolve_schema_key_suffix_candidates};
    use aorta_introspect::schema::{ResolvedSchema, ResolvedServiceSchema, ResolvedTopicSchema};

    #[test]
    fn format_schema_list_row_renders_topic_and_service() {
        let topic = ResolvedSchema::Topic(ResolvedTopicSchema::new("pub/imu", vec![1, 2, 3, 4]));
        let topic_row = format_schema_list_row(&topic);
        assert!(topic_row.contains("pub/imu"), "row: {}", topic_row);
        assert!(topic_row.contains("topic"), "row: {}", topic_row);
        assert!(topic_row.contains("4 bytes"), "row: {}", topic_row);

        let service = ResolvedSchema::Service(ResolvedServiceSchema::new(
            "srv/echo",
            vec![5, 6, 7],
            None,
            None,
        ));
        let service_row = format_schema_list_row(&service);
        assert!(service_row.contains("srv/echo"), "row: {}", service_row);
        assert!(service_row.contains("service"), "row: {}", service_row);
        assert!(service_row.contains("3 bytes"), "row: {}", service_row);
    }

    #[test]
    fn resolve_schema_key_suffix_candidates_from_topic_shortcut_tries_topic_and_service() {
        let suffixes =
            resolve_schema_key_suffix_candidates("default", "/cpp/echo").expect("suffixes");
        assert_eq!(
            suffixes,
            vec!["pub/cpp/echo".to_string(), "srv/cpp/echo".to_string()]
        );
    }

    #[test]
    fn resolve_schema_key_suffix_candidates_from_full_key() {
        let suffixes = resolve_schema_key_suffix_candidates(
            "default",
            "aorta/default/sys/schema_by_key/pub/chatter",
        )
        .expect("suffixes");
        assert_eq!(suffixes, vec!["pub/chatter".to_string()]);
    }

    #[test]
    fn resolve_schema_key_suffix_candidates_from_service_prefix() {
        let suffixes =
            resolve_schema_key_suffix_candidates("default", "srv/echo_schema").expect("suffixes");
        assert_eq!(suffixes, vec!["srv/echo_schema".to_string()]);
    }

    #[test]
    fn resolve_schema_key_suffix_candidates_from_bare_key_tries_topic_and_service() {
        let suffixes =
            resolve_schema_key_suffix_candidates("default", "echo_schema").expect("suffixes");
        assert_eq!(
            suffixes,
            vec!["pub/echo_schema".to_string(), "srv/echo_schema".to_string()]
        );
    }
}
