use crate::output::{emit_json, output_format, OutputFormat};
use crate::util::to_user_key;
use aorta_introspect::client::{
    NodeDetailOptions, NodeDiscoverySource, NodeListOptions, Provenance, WithProvenance,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct NodeInfo {
    name: String,
    id: String,
    group: String,
}

pub async fn list(
    watch: bool,
    verbose: bool,
    with_resources: bool,
    count: Option<usize>,
    timeout_secs: Option<u64>,
    group: Option<&str>,
) -> anyhow::Result<()> {
    let group = group.unwrap_or("default");

    // Streaming watch has no JSON representation yet (NDJSON is a follow-up); reject
    // the combination up front (before opening a session) so we never corrupt stdout
    // with the text table header while the user asked for JSON.
    if watch && output_format() == OutputFormat::Json {
        return Err(anyhow::anyhow!(
            "`nodes list --watch` does not support --format json (streaming JSON is a follow-up)"
        ));
    }

    // Create Zenoh session
    let session = crate::cmd::open_session().await?;

    if watch {
        if verbose {
            eprintln!(
                "WARN: `nodes list --watch --verbose` does not include endpoint ownership details."
            );
        }
        if with_resources {
            eprintln!(
                "WARN: `nodes list --watch --with-resources` is not supported in streaming mode."
            );
        }
        eprintln!(
            "Watching for node changes in group '{}' (Ctrl+C to stop)...\n",
            group
        );
        watch_nodes(&session, group, count, timeout_secs).await?;
    } else {
        let client = crate::cmd::build_introspect_client(Arc::new(session), group)?;
        if output_format() != OutputFormat::Json {
            eprintln!("Discovering nodes in group '{}'...\n", group);
        }
        list_nodes(&client, group, verbose, with_resources).await?;
    }

    Ok(())
}

pub async fn info(node_query: &str, group: Option<&str>) -> anyhow::Result<()> {
    let group = group.unwrap_or("default");
    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;
    let selector = crate::cmd::parse_node_selector(node_query)?;
    let detail = client
        .get_node_detail(selector, NodeDetailOptions)
        .await
        .map_err(|err| anyhow::anyhow!("Failed to resolve node '{}': {}", node_query, err))?;

    println!("Node: {}", detail.node.name);
    println!("Node ID: {}", detail.node.id);
    println!("Group: {}", detail.node.group);
    let endpoints: Vec<_> = detail
        .node
        .owner_endpoints
        .value
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    if detail.node.owner_endpoints.provenance == Provenance::Unavailable {
        println!("Owner endpoints: (unavailable)");
    } else if endpoints.is_empty() {
        println!("Owner endpoints: (none)");
    } else {
        println!("Owner endpoints: {}", endpoints.join(", "));
    }
    if detail.candidate_topics.provenance == Provenance::Unavailable {
        println!("Topics: (unavailable)");
    } else {
        println!("Topics ({}):", detail.candidate_topics.value.len());
        if detail.candidate_topics.value.is_empty() {
            println!("  (none)");
        } else {
            for topic in &detail.candidate_topics.value {
                println!("  {}", to_user_key(topic.as_str()));
            }
        }
    }
    if detail.candidate_services.provenance == Provenance::Unavailable {
        println!("Services: (unavailable)");
    } else {
        println!("Services ({}):", detail.candidate_services.value.len());
        if detail.candidate_services.value.is_empty() {
            println!("  (none)");
        } else {
            for service in &detail.candidate_services.value {
                println!("  {}", to_user_key(service.as_str()));
            }
        }
    }
    if let Some(note) = detail.candidate_topics.note.as_deref() {
        println!("Mapping note: {}", note);
    }

    Ok(())
}

async fn list_nodes(
    client: &aorta_introspect::client::IntrospectClient,
    group: &str,
    verbose: bool,
    with_resources: bool,
) -> anyhow::Result<()> {
    let nodes = client
        .list_nodes(NodeListOptions {
            include_candidate_resources: with_resources,
        })
        .await
        .map_err(|err| anyhow::anyhow!("Failed to discover nodes in group '{}': {}", group, err))?;

    if output_format() == OutputFormat::Json {
        let rows: Vec<_> = nodes
            .nodes
            .iter()
            .map(|node| {
                let endpoints: Vec<_> = node
                    .owner_endpoints
                    .value
                    .iter()
                    .map(|id| id.as_str().to_string())
                    .collect();
                json!({
                    "name": node.name.as_str(),
                    "id": node.id.as_str(),
                    "group": node.group.as_str(),
                    "topics": resource_count_json(&node.candidate_topic_count, with_resources),
                    "services": resource_count_json(&node.candidate_service_count, with_resources),
                    "endpoints": if node.owner_endpoints.provenance == Provenance::Unavailable {
                        serde_json::Value::Null
                    } else {
                        json!(endpoints)
                    },
                })
            })
            .collect();
        emit_json(&json!({
            "group": group,
            "source": node_discovery_source_str(nodes.source),
            "nodes": rows,
        }))?;
        return Ok(());
    }

    if !verbose && !with_resources {
        println!("{:<30} {:<36} {:<15}", "NODE NAME", "NODE ID", "GROUP");
        println!("{}", "-".repeat(83));
        for node in &nodes.nodes {
            println!("{:<30} {:<36} {:<15}", node.name, node.id, node.group);
        }
        print_node_discovery_note(nodes.source, verbose, with_resources);
        return Ok(());
    }

    println!(
        "{:<30} {:<36} {:<15} {:<10} {:<10} {:<10}",
        "NODE NAME", "NODE ID", "GROUP", "TOPICS", "SERVICES", "ENDPOINTS"
    );
    println!("{}", "-".repeat(121));
    for node in &nodes.nodes {
        let endpoints: Vec<_> = node
            .owner_endpoints
            .value
            .iter()
            .map(|id| id.as_str().to_string())
            .collect();
        let endpoint_count = text_for_endpoint_count(&node.owner_endpoints);
        let topic_text = text_for_resource_count(&node.candidate_topic_count, with_resources);
        let service_text = text_for_resource_count(&node.candidate_service_count, with_resources);

        println!(
            "{:<30} {:<36} {:<15} {:<10} {:<10} {:<10}",
            node.name, node.id, node.group, topic_text, service_text, endpoint_count
        );
        if verbose {
            if node.owner_endpoints.provenance == Provenance::Unavailable {
                println!("  endpoints: (unavailable)");
            } else if endpoints.is_empty() {
                println!("  endpoints: (none)");
            } else {
                println!("  endpoints: {}", endpoints.join(", "));
            }
        }
    }
    print_node_discovery_note(nodes.source, verbose, with_resources);

    Ok(())
}

async fn watch_nodes(
    session: &zenoh::Session,
    group: &str,
    count: Option<usize>,
    timeout_secs: Option<u64>,
) -> anyhow::Result<()> {
    let pattern = format!("aorta/{}/sys/alive/**", group);

    // Subscribe to liveliness changes
    let subscriber = session
        .liveliness()
        .declare_subscriber(&pattern)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to liveliness: {}", e))?;

    println!(
        "{:<10} {:<30} {:<36} {:<15}",
        "EVENT", "NODE NAME", "NODE ID", "GROUP"
    );
    println!("{}", "-".repeat(93));

    let deadline = timeout_secs.map(|secs| Instant::now() + Duration::from_secs(secs));
    let mut printed = 0usize;

    // Watch for changes
    loop {
        let recv = subscriber.recv_async();
        let sample = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, recv).await {
                    Ok(Ok(sample)) => sample,
                    Ok(Err(_)) | Err(_) => break,
                }
            }
            None => match recv.await {
                Ok(sample) => sample,
                Err(_) => break,
            },
        };

        let key = sample.key_expr().as_str();
        if let Some(node_info) = parse_liveliness_key(key, group) {
            let event = match sample.kind() {
                zenoh::sample::SampleKind::Put => "ONLINE",
                zenoh::sample::SampleKind::Delete => "OFFLINE",
            };
            println!(
                "{:<10} {:<30} {:<36} {:<15}",
                event, node_info.name, node_info.id, node_info.group
            );
            printed += 1;
            if let Some(limit) = count {
                if printed >= limit {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn parse_liveliness_key(key: &str, expected_group: &str) -> Option<NodeInfo> {
    // Expected format: aorta/{group}/sys/alive/{node_name}/{node_id}
    let prefix = format!("aorta/{}/sys/alive/", expected_group);

    let suffix = key.strip_prefix(&prefix)?;
    let parts: Vec<&str> = suffix.split('/').collect();

    if parts.len() != 2 {
        return None;
    }

    let name = parts[0].to_string();
    let id = parts[1].to_string();

    // Validate node_id format (32 hex chars)
    if id.len() != 32 || !id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Some(NodeInfo {
        name,
        id,
        group: expected_group.to_string(),
    })
}

fn text_for_endpoint_count(
    endpoints: &WithProvenance<Vec<aorta_introspect::ids::EndpointId>>,
) -> String {
    match endpoints.provenance {
        Provenance::Unavailable => "n/a".to_string(),
        _ => endpoints.value.len().to_string(),
    }
}

fn text_for_resource_count(count: &WithProvenance<Option<usize>>, requested: bool) -> String {
    if !requested {
        return "-".to_string();
    }

    match count.value {
        Some(count) => count.to_string(),
        None => "n/a".to_string(),
    }
}

/// JSON representation of a candidate resource count: `null` when the count was
/// not requested (`--with-resources` absent) or is unavailable, else the integer.
fn resource_count_json(
    count: &WithProvenance<Option<usize>>,
    requested: bool,
) -> serde_json::Value {
    if !requested {
        return serde_json::Value::Null;
    }
    match count.value {
        Some(count) => json!(count),
        None => serde_json::Value::Null,
    }
}

fn node_discovery_source_str(source: NodeDiscoverySource) -> &'static str {
    match source {
        NodeDiscoverySource::TelemetrySnapshot => "telemetry_snapshot",
        NodeDiscoverySource::AdminSnapshot => "admin_snapshot",
        NodeDiscoverySource::LivelinessFallback => "liveliness_fallback",
    }
}

fn print_node_discovery_note(source: NodeDiscoverySource, verbose: bool, with_resources: bool) {
    if source == NodeDiscoverySource::LivelinessFallback {
        if verbose || with_resources {
            println!(
                "NOTE: resource/endpoint ownership details require router admin token snapshot and are unavailable in liveliness fallback mode."
            );
        } else {
            eprintln!("WARN: admin snapshot unavailable; showing liveliness fallback view.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_liveliness_key;

    #[test]
    fn parse_liveliness_key_extracts_name_and_id() {
        let text = "aorta/default/sys/alive/talker/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
        let node = parse_liveliness_key(text, "default").expect("node");
        assert_eq!(node.name, "talker");
        assert_eq!(node.id, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
    }
}
