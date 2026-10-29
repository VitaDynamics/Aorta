use crate::output::{emit_json, output_format, OutputFormat};
use crate::util::{format_latency_ns, format_stamp_age_ns, to_user_key};
use anyhow::{anyhow, Result};
use aorta_introspect::client::{
    IntrospectClient, IntrospectOptions, LatencyAccumulator, NodeDetailOptions, Provenance,
    ServiceCallRequest, ServiceDetailOptions, ServiceListOptions,
};
use aorta_introspect::ids::{GroupName, ServiceSuffix};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

/// Bound on how many distinct observed latency samples the `service latency`
/// window retains (caps the client-side ring; the window is time-bounded too).
const LATENCY_WINDOW_SAMPLE_CAP: usize = 4096;
/// Cadence at which `service latency` re-polls the service runtime telemetry.
const LATENCY_POLL_INTERVAL_MS: u64 = 200;

pub async fn list(
    group: Option<&str>,
    timeout_secs: u64,
    verbose: bool,
    node_filter: Option<&str>,
) -> Result<()> {
    let group = group.unwrap_or("default");
    let session = Arc::new(crate::cmd::open_session().await?);
    let discovery_timeout = Duration::from_secs(timeout_secs);
    let group_name =
        GroupName::new(group).map_err(|err| anyhow!("Invalid group '{}': {}", group, err))?;
    let mut options = IntrospectOptions::new(group_name);
    options.admin_query_timeout = discovery_timeout;
    options.schema_query_timeout = discovery_timeout;
    let client = IntrospectClient::from_session(session.clone(), options);
    let node_selector = node_filter
        .map(crate::cmd::parse_node_selector)
        .transpose()?;
    let filter_label = if let Some(selector) = node_selector.as_ref() {
        let node_detail = client
            .get_node_detail(selector.clone(), NodeDetailOptions)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to resolve node filter '{}': {}",
                    selector_label(selector),
                    err
                )
            })?;
        Some(format!(
            "{} ({})",
            node_detail.node.name, node_detail.node.id
        ))
    } else {
        None
    };
    let (services, directory) = tokio::join!(
        client.list_services(ServiceListOptions {
            include_candidate_nodes: verbose,
            node_filter: node_selector,
        }),
        crate::cmd::interface_directory::fetch(&session, group, "service", discovery_timeout),
    );
    let services = services.map_err(|err| anyhow!("failed to list services: {}", err))?;
    let admin_names = services
        .iter()
        .filter(|item| item.present_in_admin)
        .map(|item| to_user_key(item.service.as_str()))
        .collect();
    let telemetry_names = services
        .iter()
        .filter(|item| {
            [&item.provider_node_count, &item.client_node_count]
                .iter()
                .any(|count| {
                    count.provenance == Provenance::Exact
                        && count.value.is_some_and(|value| value > 0)
                })
        })
        .map(|item| to_user_key(item.service.as_str()))
        .collect();
    let mut discovered =
        crate::cmd::interface_directory::merge(admin_names, telemetry_names, directory?);
    if node_filter.is_some() {
        // Directory rows have no ownership information for a node filter.
        discovered.retain(|entry| {
            services
                .iter()
                .any(|item| to_user_key(item.service.as_str()) == entry.name)
        });
    }
    if discovered.is_empty() {
        crate::cmd::interface_directory::empty_hint("service");
    }

    if output_format() == OutputFormat::Json {
        let rows: Vec<_> = discovered
            .iter()
            .map(|entry| {
                let mut row = crate::cmd::interface_directory::discovery_row(entry, "service");
                if verbose {
                    let detail = services.iter().find(|item| to_user_key(item.service.as_str()) == entry.name);
                    row["endpoints"] = detail.map_or(serde_json::Value::Null, |item| {
                        let endpoints: Vec<_> = item.provider_endpoints.value.iter().map(|id| id.as_str()).collect();
                        serde_json::json!(endpoints)
                    });
                    row["nodes"] = detail
                        .filter(|item| item.candidate_nodes.provenance != Provenance::Unavailable)
                        .map_or(serde_json::Value::Null, |item| {
                            let nodes: Vec<_> = item.candidate_nodes.value.iter().map(|node| {
                                serde_json::json!({"name": node.name.as_str(), "id": node.id.as_str()})
                            }).collect();
                            serde_json::json!(nodes)
                        });
                }
                row
            })
            .collect();
        emit_json(&rows)?;
        return Ok(());
    }

    if !verbose {
        // Pre-#388 default output: one bare row per live service (see topic list).
        let live: Vec<_> = discovered.iter().filter(|entry| entry.live).collect();
        if live.is_empty() {
            if !discovered.is_empty() {
                crate::cmd::interface_directory::empty_hint("service");
            }
            eprintln!("No active services found.");
            return Ok(());
        }
        if let Some(label) = filter_label {
            eprintln!("Active services in group '{}' for node {}:", group, label);
        } else {
            eprintln!("Active services in group '{}':", group);
        }
        for entry in live {
            println!("  {}", entry.name);
        }
        return Ok(());
    }

    if discovered.is_empty() {
        return Ok(());
    }

    if let Some(label) = filter_label {
        eprintln!("Services in group '{}' for node {}:", group, label);
    } else {
        eprintln!("Services in group '{}':", group);
    }
    for entry in &discovered {
        println!(
            "  {}  {}  {}",
            entry.name,
            if entry.live { "live" } else { "absent" },
            entry.sources.join(",")
        );
    }

    if verbose {
        eprintln!("\nService provider details:");
        for service in services {
            let endpoints: Vec<_> = service
                .provider_endpoints
                .value
                .iter()
                .map(|id| id.as_str().to_string())
                .collect();
            let endpoint_text = if endpoints.is_empty() {
                "(none)".to_string()
            } else {
                endpoints.join(", ")
            };
            let node_labels: Vec<_> = service
                .candidate_nodes
                .value
                .iter()
                .map(|node| format!("{} ({})", node.name, node.id))
                .collect();
            let node_text = if service.candidate_nodes.provenance == Provenance::Unavailable {
                "(n/a)".to_string()
            } else if node_labels.is_empty() {
                "(unresolved)".to_string()
            } else {
                node_labels.join(", ")
            };

            println!("  {}", to_user_key(service.service.as_str()));
            println!("    endpoints: {}", endpoint_text);
            println!("    nodes: {}", node_text);
            if let Some(note) = service.candidate_nodes.note.as_deref() {
                println!("    note: {}", note);
            }
        }
    }
    Ok(())
}

pub async fn info(service: &str, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let service_suffix = ServiceSuffix::new(service)
        .map_err(|err| anyhow!("Invalid service '{}': {}", service, err))?;
    let service_display = to_user_key(service_suffix.as_str());

    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session.clone(), group)?;
    let (detail, directory) = tokio::join!(
        client.get_service_detail(&service_suffix, ServiceDetailOptions::default()),
        crate::cmd::interface_directory::fetch(&session, group, "service", Duration::from_secs(2))
    );
    let detail = detail.map_err(|err| {
        anyhow!(
            "Failed to resolve service detail for '{}': {}",
            service,
            err
        )
    })?;

    // Same telemetry source `service list` merges, derived from the detail
    // already fetched instead of a second introspect query.
    let telemetry_names: BTreeSet<String> = if ![&detail.provider_nodes, &detail.client_nodes]
        .iter()
        .any(|nodes| nodes.provenance == Provenance::Exact && !nodes.value.is_empty())
    {
        BTreeSet::new()
    } else {
        BTreeSet::from([service_display.clone()])
    };
    let admin_names = if detail.snapshot.present_in_admin {
        BTreeSet::from([service_display.clone()])
    } else {
        BTreeSet::new()
    };
    let discovery =
        crate::cmd::interface_directory::merge(admin_names, telemetry_names, directory?);

    let entry = discovery.iter().find(|entry| entry.name == service_display);
    if output_format() == OutputFormat::Json {
        emit_json(&serde_json::json!({
            "name": service_display,
            "key": detail.key_expr,
            "live": entry.is_some_and(|entry| entry.live),
            "sources": entry.map(|entry| entry.sources.as_slice()).unwrap_or(&[]),
            "schema_hash": detail.schema.value.as_ref().map(|schema| schema.service_hash.as_str()),
            "endpoints": detail.snapshot.provider_endpoints.value.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
        }))?;
        return Ok(());
    }
    if let Some(entry) = entry {
        println!(
            "Discovery: {} ({})",
            entry.sources.join(","),
            if entry.live { "live" } else { "absent" }
        );
    }
    println!("Service: {}", service_display);
    println!("Key: {}", detail.key_expr);
    let endpoints: Vec<_> = detail
        .snapshot
        .provider_endpoints
        .value
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    if endpoints.is_empty() {
        println!("Provider endpoints: (none)");
    } else {
        println!("Provider endpoints: {}", endpoints.join(", "));
    }

    let providers: Vec<_> = detail
        .snapshot
        .candidate_nodes
        .value
        .iter()
        .map(|node| format!("{} ({})", node.name, node.id))
        .collect();
    if providers.is_empty() {
        println!("Provider nodes: (unresolved)");
    } else {
        println!("Provider nodes: {}", providers.join(", "));
    }
    if let Some(note) = detail.snapshot.candidate_nodes.note.as_deref() {
        println!("Provider mapping note: {}", note);
    }

    // Latency readout from runtime telemetry (when available). last_call latency
    // is a true duration; last request/reply are wall-clock stamps shown as an
    // age relative to the telemetry snapshot timestamp.
    match detail.runtime.value.as_ref() {
        Some(runtime) => {
            println!(
                "Last call latency: {}",
                format_latency_ns(runtime.client_last_call_latency_ns)
            );
            println!(
                "Last request: {} ago",
                format_stamp_age_ns(runtime.latest_timestamp_ns, runtime.service_last_request_ns)
            );
            println!(
                "Last reply: {} ago",
                format_stamp_age_ns(runtime.latest_timestamp_ns, runtime.service_last_reply_ns)
            );
        }
        None => {
            println!("Latency: (runtime telemetry unavailable)");
        }
    }

    Ok(())
}

/// Poll a service's runtime telemetry over `window_secs`, accumulating the
/// DISTINCT observed `client_last_call_latency_ns` last-values (de-duplicated on
/// the paired `client_last_call_finished_ns` stamp), then report approximate
/// client-observed latency percentiles.
///
/// These are CLIENT-OBSERVED latencies fed through the bucket-bounded core
/// `FixedHistogram`; they are approximate (factor-of-2 buckets) and include
/// network + tool overhead — they are NOT the robot's internal per-stage
/// latency. The wire telemetry carries only a single last-value, so the
/// distribution is built up by polling over the window.
pub async fn latency(service: &str, window_secs: u64, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let window_secs = window_secs.max(1);
    let service_suffix = ServiceSuffix::new(service)
        .map_err(|err| anyhow!("Invalid service '{}': {}", service, err))?;
    let service_display = to_user_key(service_suffix.as_str());

    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    eprintln!(
        "Sampling client-observed latency for service '{}' over {}s window...",
        service_display, window_secs
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(window_secs);
    let mut poll_tick = tokio::time::interval(Duration::from_millis(LATENCY_POLL_INTERVAL_MS));
    poll_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut accumulator = LatencyAccumulator::with_capacity(LATENCY_WINDOW_SAMPLE_CAP);
    let resource_key = service_suffix.as_str().to_string();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Stopped latency sampling early.");
                break;
            }
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
            _ = poll_tick.tick() => {
                // Best-effort poll: transient detail errors do not abort the window.
                if let Ok(detail) = client
                    .get_service_detail(&service_suffix, ServiceDetailOptions::default())
                    .await
                {
                    if let Some(runtime) = detail.runtime.value.as_ref() {
                        accumulator.observe(
                            &resource_key,
                            runtime.client_last_call_latency_ns,
                            runtime.client_last_call_finished_ns,
                        );
                    }
                }
            }
        }
    }

    let snapshot = accumulator.snapshot_for(&resource_key);

    if output_format() == OutputFormat::Json {
        let body = match snapshot {
            Some(snapshot) => serde_json::json!({
                "service": service_display,
                "approx": true,
                "client_observed": true,
                "samples": snapshot.count,
                "p50_ns": snapshot.p50_ns,
                "p95_ns": snapshot.p95_ns,
                "p99_ns": snapshot.p99_ns,
                "max_ns": snapshot.max_ns,
                "mean_ns": snapshot.mean_ns,
            }),
            None => serde_json::json!({
                "service": service_display,
                "approx": true,
                "client_observed": true,
                "samples": 0,
            }),
        };
        emit_json(&body)?;
        return Ok(());
    }

    match snapshot {
        Some(snapshot) => {
            println!("Service: {}", service_display);
            println!(
                "p50 {} / p95 {} / p99 {} / max {} / mean {} (approx, client-observed, {} samples)",
                format_latency_ns(snapshot.p50_ns),
                format_latency_ns(snapshot.p95_ns),
                format_latency_ns(snapshot.p99_ns),
                format_latency_ns(snapshot.max_ns),
                format_latency_ns(snapshot.mean_ns),
                snapshot.count,
            );
        }
        None => {
            println!("Service: {}", service_display);
            println!(
                "No client-observed latency samples in {}s (no completed client calls or runtime telemetry unavailable).",
                window_secs
            );
        }
    }

    Ok(())
}

pub async fn call(
    service: &str,
    request: &str,
    timeout: Option<u64>,
    group: Option<&str>,
) -> Result<()> {
    let group = group.unwrap_or("default");
    let timeout = Duration::from_secs(timeout.unwrap_or(2));
    let service_suffix = ServiceSuffix::new(service)
        .map_err(|err| anyhow!("Invalid service '{}': {}", service, err))?;
    let service_display = to_user_key(service_suffix.as_str());

    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    eprintln!("Calling service '{}'", service_display);
    let response = client
        .call_service(ServiceCallRequest {
            service: service_suffix,
            timeout: Some(timeout),
            request_json: request.to_string(),
        })
        .await
        .map_err(|err| anyhow!("Service call failed: {}", err))?;

    eprintln!("Service reply:");
    if let Some(json) = response.response_json {
        println!("{}", json);
    } else if let Some(text) = response.response_utf8 {
        println!("{}", text);
    } else {
        println!("0x{}", response.response_hex);
    }
    Ok(())
}

fn selector_label(selector: &aorta_introspect::client::NodeSelector) -> &str {
    match selector {
        aorta_introspect::client::NodeSelector::ById(node_id) => node_id.as_str(),
        aorta_introspect::client::NodeSelector::ByName(node_name) => node_name.as_str(),
    }
}
