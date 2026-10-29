use crate::output::{emit_json, output_format, OutputFormat};
use crate::util::{human_bytes, to_hex, to_user_key};
use anyhow::{anyhow, Result};
use aorta_core::KeyExprBuilder;
use aorta_introspect::client::{
    IntrospectClient, IntrospectOptions, NodeDetailOptions, Provenance, TopicDetailOptions,
    TopicListOptions,
};
use aorta_introspect::ids::{GroupName, TopicSuffix};
use aorta_introspect::reflect;
use aorta_introspect::topic_schema::{
    extract_attachment_schema_hash, TopicSchemaResolver, TopicSchemaResolverConfig,
};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

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

    let json_mode = output_format() == OutputFormat::Json;

    if !json_mode {
        eprintln!(
            "Discovering topics in group '{}' via adminspace + telemetry + interface directory (timeout: {}s)...",
            group, timeout_secs
        );
    }

    let (topics, directory) = tokio::join!(
        client.list_topics(TopicListOptions {
            include_candidate_nodes: verbose,
            node_filter: node_selector.clone(),
        }),
        crate::cmd::interface_directory::fetch(&session, group, "topic", discovery_timeout),
    );
    let topics = topics.map_err(|err| anyhow!("failed to list topics: {}", err))?;
    let admin_names = topics
        .iter()
        .filter(|item| item.present_in_admin)
        .map(|item| to_user_key(item.topic.as_str()))
        .collect();
    let telemetry_names = topics
        .iter()
        .filter(|item| {
            [&item.publisher_node_count, &item.subscriber_node_count]
                .iter()
                .any(|count| {
                    count.provenance == Provenance::Exact
                        && count.value.is_some_and(|value| value > 0)
                })
        })
        .map(|item| to_user_key(item.topic.as_str()))
        .collect();
    let mut discovered =
        crate::cmd::interface_directory::merge(admin_names, telemetry_names, directory?);
    if node_filter.is_some() {
        // Directory rows have no ownership information for a node filter.
        discovered.retain(|entry| {
            topics
                .iter()
                .any(|item| to_user_key(item.topic.as_str()) == entry.name)
        });
    }
    if discovered.is_empty() {
        crate::cmd::interface_directory::empty_hint("topic");
    }

    if json_mode {
        let rows: Vec<_> = discovered
            .iter()
            .map(|entry| {
                let mut row = crate::cmd::interface_directory::discovery_row(entry, "topic");
                if verbose {
                    let detail = topics.iter().find(|item| to_user_key(item.topic.as_str()) == entry.name);
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
        // Pre-#388 default output: one bare row per live topic. Scripts match
        // these lines exactly, so the live/absent verdict and the sources stay
        // behind --verbose.
        let live: Vec<_> = discovered.iter().filter(|entry| entry.live).collect();
        if live.is_empty() {
            if !discovered.is_empty() {
                crate::cmd::interface_directory::empty_hint("topic");
            }
            eprintln!("No active topics found.");
            return Ok(());
        }
        if let Some(label) = filter_label {
            eprintln!("\nActive topics for node {}:", label);
        } else {
            eprintln!("\nActive topics:");
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
        eprintln!("\nTopics for node {}:", label);
    } else {
        eprintln!("\nTopics:");
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
        eprintln!("\nTopic provider details:");
        for topic in topics {
            let endpoints: Vec<_> = topic
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

            let node_labels: Vec<_> = topic
                .candidate_nodes
                .value
                .iter()
                .map(|node| format!("{} ({})", node.name, node.id))
                .collect();
            let node_text = if topic.candidate_nodes.provenance == Provenance::Unavailable {
                "(n/a)".to_string()
            } else if node_labels.is_empty() {
                "(unresolved)".to_string()
            } else {
                node_labels.join(", ")
            };

            println!("  {}", to_user_key(topic.topic.as_str()));
            println!("    endpoints: {}", endpoint_text);
            println!("    nodes: {}", node_text);
            if let Some(note) = topic.candidate_nodes.note.as_deref() {
                println!("    note: {}", note);
            }
        }
    }

    Ok(())
}

pub async fn echo(topic: &str, count: Option<usize>, raw: bool, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let json_mode = output_format() == OutputFormat::Json;
    let topic_name = topic.trim();
    let topic_display = to_user_key(topic_name);
    let key_builder =
        KeyExprBuilder::new(group).map_err(|e| anyhow!("Invalid group '{}': {}", group, e))?;
    // `key_suffix_for_topic` is the publish-side helper (no wildcards),
    // which is correct for `schema_by_key` lookup — schemas are
    // declared by publishers against a concrete topic name and there is
    // no schema-by-key for a glob. When the user passes a wildcard
    // pattern to `echo`, skip the schema-by-key fetch instead of
    // failing the whole command: the subscribe-side path (below) does
    // accept wildcards via `build_topic_echo_key` → `topic_key_sub`.
    let key_suffix = key_builder.key_suffix_for_topic(topic_name).ok();

    // Create Zenoh session
    let session = Arc::new(crate::cmd::open_session().await?);

    // Reuse core key expression builder to keep key semantics aligned.
    let topic_key = build_topic_echo_key(group, topic_name)?;
    let mut schema_resolver = TopicSchemaResolver::new(TopicSchemaResolverConfig::default());
    let mut fallback_topic_schema = if let Some(key_suffix) = key_suffix.as_deref() {
        match schema_resolver
            .get_by_key(&session, group, key_suffix)
            .await
        {
            Ok(schema) => Some((schema.bfbs.to_vec(), schema.default_root_type)),
            Err(err) => {
                eprintln!(
                    "WARN: failed to fetch topic schema_by_key for '{}': {}",
                    topic_name, err
                );
                None
            }
        }
    } else {
        // Wildcard topic — there's no single schema_by_key to fetch.
        // Each received sample falls back to the attachment-schema-hash
        // path below.
        None
    };

    eprintln!(
        "Subscribing to topic '{}' (key: {})",
        topic_display, topic_key
    );
    if let Some(n) = count {
        eprintln!("Will print {} messages", n);
    }

    // Subscribe to topic
    let subscriber = session
        .declare_subscriber(&topic_key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to {}: {}", topic_key, e))?;

    let mut msg_count = 0usize;

    loop {
        if let Ok(sample) = subscriber.recv_async().await {
            msg_count += 1;

            if !json_mode {
                eprintln!("--- Message {} ---", msg_count);
            }

            if raw {
                // Print raw hex
                let payload = sample.payload().to_bytes();
                if json_mode {
                    println!("{}", serde_json::json!({ "hex": to_hex(&payload) }));
                } else {
                    println!("Payload (hex): {}", to_hex(&payload));
                }
            } else {
                // Primary path: schema_by_key(topic). Attachment schema hash is fallback only.
                // `key_suffix` is `None` for wildcard topics — schema_by_key
                // is not meaningful for a glob, so skip the retry on those
                // and rely on the attachment-hash path below.
                if fallback_topic_schema.is_none() {
                    if let Some(key_suffix) = key_suffix.as_deref() {
                        fallback_topic_schema = schema_resolver
                            .get_by_key(&session, group, key_suffix)
                            .await
                            .ok()
                            .map(|schema| (schema.bfbs.to_vec(), schema.default_root_type));
                    }
                }

                let payload = sample.payload().to_bytes();
                let attachment_schema_hash = extract_attachment_schema_hash(&sample);

                if let Some((topic_bfbs, root_type)) = fallback_topic_schema.as_ref() {
                    match fb_to_json_safe(topic_bfbs, &payload, root_type.as_deref(), json_mode) {
                        Ok(json) => {
                            println!("{}", json);
                        }
                        Err(topic_err) => {
                            if let Some(schema_hash) = attachment_schema_hash {
                                match schema_resolver.get_by_hash(&session, &schema_hash).await {
                                    Ok(attachment_bfbs) => {
                                        match fb_to_json_safe(
                                            &attachment_bfbs,
                                            &payload,
                                            None,
                                            json_mode,
                                        ) {
                                            Ok(json) => println!("{}", json),
                                            Err(attachment_err) => {
                                                eprintln!(
                                                    "Topic schema decode error: {}",
                                                    topic_err
                                                );
                                                eprintln!(
                                                    "Attachment schema decode error ({}): {}",
                                                    schema_hash, attachment_err
                                                );
                                                eprintln!("Payload (hex): {}", to_hex(&payload));
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        eprintln!("Topic schema decode error: {}", topic_err);
                                        eprintln!(
                                            "Attachment schema fetch error ({}): {}",
                                            schema_hash, err
                                        );
                                        eprintln!("Payload (hex): {}", to_hex(&payload));
                                    }
                                }
                            } else {
                                eprintln!("Topic schema decode error: {}", topic_err);
                                eprintln!("No schema attachment found");
                                eprintln!("Payload (hex): {}", to_hex(&payload));
                            }
                        }
                    }
                } else if let Some(schema_hash) = attachment_schema_hash {
                    match schema_resolver.get_by_hash(&session, &schema_hash).await {
                        Ok(bfbs) => match fb_to_json_safe(&bfbs, &payload, None, json_mode) {
                            Ok(json) => println!("{}", json),
                            Err(err) => {
                                eprintln!(
                                    "Attachment schema decode error ({}): {}",
                                    schema_hash, err
                                );
                                eprintln!("Payload (hex): {}", to_hex(&payload));
                            }
                        },
                        Err(err) => {
                            eprintln!(
                                "No topic schema_by_key found; attachment schema fetch error ({}): {}",
                                schema_hash, err
                            );
                            eprintln!("Payload (hex): {}", to_hex(&payload));
                        }
                    }
                } else {
                    eprintln!("No topic schema_by_key or schema attachment found");
                    eprintln!("Payload (hex): {}", to_hex(&payload));
                }
            }

            if !json_mode {
                println!();
            }

            // Check if we've reached the desired count
            if let Some(n) = count {
                if msg_count >= n {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn build_topic_echo_key(group: &str, topic: &str) -> anyhow::Result<String> {
    let key_builder = KeyExprBuilder::new(group)
        .map_err(|e| anyhow::anyhow!("Invalid group '{}': {}", group, e))?;
    let key = key_builder
        .topic_key_sub(topic)
        .map_err(|e| anyhow::anyhow!("Invalid topic '{}': {}", topic, e))?;
    Ok(key.to_string())
}

fn selector_label(selector: &aorta_introspect::client::NodeSelector) -> &str {
    match selector {
        aorta_introspect::client::NodeSelector::ById(node_id) => node_id.as_str(),
        aorta_introspect::client::NodeSelector::ByName(node_name) => node_name.as_str(),
    }
}

fn fb_to_json_safe(
    bfbs: &[u8],
    payload: &[u8],
    root_type: Option<&str>,
    compact: bool,
) -> anyhow::Result<String> {
    let raw = if compact {
        reflect::fb_to_json(bfbs, payload, root_type)
    } else {
        reflect::fb_to_json_display(bfbs, payload, root_type)
    };
    match raw {
        Ok(json) => {
            if compact {
                // Re-serialize to guarantee one compact, valid JSON line.
                let value: serde_json::Value = serde_json::from_str(&json)?;
                Ok(serde_json::to_string(&value)?)
            } else {
                Ok(json)
            }
        }
        Err(e) => Err(anyhow::anyhow!("FlatBuffer reflection failed: {}", e)),
    }
}

#[derive(Debug, Clone, Copy)]
struct HzSnapshot {
    average_rate_hz: f64,
    min_interval_s: f64,
    max_interval_s: f64,
    stddev_interval_s: f64,
    window_samples: usize,
}

fn push_interval(intervals: &mut VecDeque<f64>, window: usize, interval_s: f64) {
    if interval_s <= 0.0 {
        return;
    }
    if intervals.len() >= window {
        intervals.pop_front();
    }
    intervals.push_back(interval_s);
}

fn compute_hz_snapshot(intervals: &VecDeque<f64>) -> Option<HzSnapshot> {
    if intervals.is_empty() {
        return None;
    }

    let window_samples = intervals.len();
    let sum: f64 = intervals.iter().copied().sum();
    let mean = sum / window_samples as f64;
    if mean <= 0.0 {
        return None;
    }

    let min_interval_s = intervals
        .iter()
        .copied()
        .reduce(f64::min)
        .unwrap_or_default();
    let max_interval_s = intervals
        .iter()
        .copied()
        .reduce(f64::max)
        .unwrap_or_default();
    let variance = intervals
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / window_samples as f64;

    Some(HzSnapshot {
        average_rate_hz: 1.0 / mean,
        min_interval_s,
        max_interval_s,
        stddev_interval_s: variance.sqrt(),
        window_samples,
    })
}

#[derive(Debug, Clone, Copy)]
struct BwSnapshot {
    msg_per_s: f64,
    bytes_per_s: f64,
    mean_bytes: f64,
    max_bytes: u64,
    /// Number of payload-size samples in the window (the `sizes` window; during
    /// ramp-up this is one more than the interval count, which is expected).
    window_samples: usize,
}

fn push_size(sizes: &mut VecDeque<u64>, window: usize, bytes: u64) {
    if sizes.len() >= window {
        sizes.pop_front();
    }
    sizes.push_back(bytes);
}

/// Bandwidth snapshot over the sliding windows: `msg_per_s` from the inter-message
/// `intervals` (same as `hz`), `bytes_per_s = msg_per_s * mean payload size`. Needs
/// at least one interval (≥2 messages) and one size sample.
fn compute_bw_snapshot(intervals: &VecDeque<f64>, sizes: &VecDeque<u64>) -> Option<BwSnapshot> {
    if intervals.is_empty() || sizes.is_empty() {
        return None;
    }
    let mean_interval = intervals.iter().copied().sum::<f64>() / intervals.len() as f64;
    if mean_interval <= 0.0 {
        return None;
    }
    let msg_per_s = 1.0 / mean_interval;
    let sum_bytes: u64 = sizes.iter().copied().sum();
    let mean_bytes = sum_bytes as f64 / sizes.len() as f64;
    let max_bytes = sizes.iter().copied().max().unwrap_or(0);
    Some(BwSnapshot {
        msg_per_s,
        bytes_per_s: msg_per_s * mean_bytes,
        mean_bytes,
        max_bytes,
        window_samples: sizes.len(),
    })
}

pub async fn hz(topic: &str, window: usize, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let topic_input = topic.trim();
    let topic_key = build_topic_echo_key(group, topic_input)?;
    let topic_display = to_user_key(topic_input);
    let window = window.max(1);

    let session = crate::cmd::open_session().await?;
    let subscriber = session
        .declare_subscriber(&topic_key)
        .await
        .map_err(|e| anyhow!("Failed to subscribe to {}: {}", topic_key, e))?;

    eprintln!(
        "Subscribing to topic '{}' (key: {})...",
        topic_display, topic_key
    );
    eprintln!("Using sliding window size: {}", window);
    eprintln!("Reporting every 1s. Press Ctrl+C to stop.");

    let mut report_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    report_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut total_messages: u64 = 0;
    let mut last_message_ts: Option<tokio::time::Instant> = None;
    let mut intervals = VecDeque::with_capacity(window);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Stopped topic rate monitor.");
                break;
            }
            sample = subscriber.recv_async() => {
                match sample {
                    Ok(_sample) => {
                        total_messages = total_messages.saturating_add(1);
                        let now = tokio::time::Instant::now();
                        if let Some(last_ts) = last_message_ts {
                            push_interval(&mut intervals, window, (now - last_ts).as_secs_f64());
                        }
                        last_message_ts = Some(now);
                    }
                    Err(err) => {
                        return Err(anyhow!("Failed to receive topic sample: {}", err));
                    }
                }
            }
            _ = report_tick.tick() => {
                if let Some(snapshot) = compute_hz_snapshot(&intervals) {
                    println!("average rate: {:.3}", snapshot.average_rate_hz);
                    println!(
                        "\tmin: {:.3}s max: {:.3}s std dev: {:.5}s window: {}",
                        snapshot.min_interval_s,
                        snapshot.max_interval_s,
                        snapshot.stddev_interval_s,
                        snapshot.window_samples
                    );
                } else if total_messages == 0 {
                    println!("waiting for messages...");
                } else {
                    println!(
                        "waiting for enough samples... (received {}, need at least 2)",
                        total_messages
                    );
                }
            }
        }
    }

    Ok(())
}

pub async fn bw(topic: &str, window: usize, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let topic_input = topic.trim();
    let topic_key = build_topic_echo_key(group, topic_input)?;
    let topic_display = to_user_key(topic_input);
    let window = window.max(1);

    let session = crate::cmd::open_session().await?;
    let subscriber = session
        .declare_subscriber(&topic_key)
        .await
        .map_err(|e| anyhow!("Failed to subscribe to {}: {}", topic_key, e))?;

    eprintln!(
        "Subscribing to topic '{}' (key: {})...",
        topic_display, topic_key
    );
    eprintln!("Using sliding window size: {}", window);
    eprintln!("Reporting every 1s. Press Ctrl+C to stop.");

    let mut report_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    report_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut total_messages: u64 = 0;
    let mut last_message_ts: Option<tokio::time::Instant> = None;
    let mut intervals = VecDeque::with_capacity(window);
    let mut sizes = VecDeque::with_capacity(window);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Stopped topic bandwidth monitor.");
                break;
            }
            sample = subscriber.recv_async() => {
                match sample {
                    Ok(sample) => {
                        total_messages = total_messages.saturating_add(1);
                        let payload_len = sample.payload().len() as u64;
                        push_size(&mut sizes, window, payload_len);
                        let now = tokio::time::Instant::now();
                        if let Some(last_ts) = last_message_ts {
                            push_interval(&mut intervals, window, (now - last_ts).as_secs_f64());
                        }
                        last_message_ts = Some(now);
                    }
                    Err(err) => {
                        return Err(anyhow!("Failed to receive topic sample: {}", err));
                    }
                }
            }
            _ = report_tick.tick() => {
                if let Some(snapshot) = compute_bw_snapshot(&intervals, &sizes) {
                    println!(
                        "msg/s: {:.2}\tbytes/s: {} ({}/s)",
                        snapshot.msg_per_s,
                        snapshot.bytes_per_s as u64,
                        human_bytes(snapshot.bytes_per_s as u64),
                    );
                    println!(
                        "\tmean payload: {} ({:.0} B)\tmax payload: {}\twindow: {}",
                        human_bytes(snapshot.mean_bytes as u64),
                        snapshot.mean_bytes,
                        human_bytes(snapshot.max_bytes),
                        snapshot.window_samples,
                    );
                } else if total_messages == 0 {
                    println!("waiting for messages...");
                } else {
                    println!(
                        "waiting for enough samples... (received {}, need at least 2)",
                        total_messages
                    );
                }
            }
        }
    }

    Ok(())
}

pub async fn info(topic: &str, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let topic_name = topic.trim();
    let topic_suffix = TopicSuffix::new(topic_name)
        .map_err(|err| anyhow!("Invalid topic '{}': {}", topic_name, err))?;
    let topic_display = to_user_key(topic_suffix.as_str());

    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session.clone(), group)?;
    let (detail, directory) = tokio::join!(
        client.get_topic_detail(
            &topic_suffix,
            TopicDetailOptions {
                include_candidate_nodes: true,
                include_rate_sample: true,
                rate_sample_duration: Duration::from_secs(1),
            },
        ),
        crate::cmd::interface_directory::fetch(&session, group, "topic", Duration::from_secs(2))
    );
    let detail = detail.map_err(|err| {
        anyhow!(
            "Failed to resolve topic detail for '{}': {}",
            topic_name,
            err
        )
    })?;

    // Telemetry knows this topic when it reported publisher or subscriber
    // nodes for it; that is the same source `topic list` merges, derived here
    // from the detail already fetched instead of a second introspect query.
    let telemetry_names: BTreeSet<String> = if ![&detail.publisher_nodes, &detail.subscriber_nodes]
        .iter()
        .any(|nodes| nodes.provenance == Provenance::Exact && !nodes.value.is_empty())
    {
        BTreeSet::new()
    } else {
        BTreeSet::from([topic_display.clone()])
    };
    let admin_names = if detail.snapshot.present_in_admin {
        BTreeSet::from([topic_display.clone()])
    } else {
        BTreeSet::new()
    };
    let discovery =
        crate::cmd::interface_directory::merge(admin_names, telemetry_names, directory?);

    let entry = discovery.iter().find(|entry| entry.name == topic_display);
    if output_format() == OutputFormat::Json {
        emit_json(&serde_json::json!({
            "name": topic_display,
            "key": detail.key_expr,
            "live": entry.is_some_and(|entry| entry.live),
            "sources": entry.map(|entry| entry.sources.as_slice()).unwrap_or(&[]),
            "schema_hash": detail.schema.value.as_ref().map(|schema| schema.schema_hash.as_str()),
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
    println!("Topic: {}", topic_display);
    println!("Key: {}", detail.key_expr);
    println!(
        "Present in admin snapshot: {}",
        detail.snapshot.present_in_admin
    );
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
    if let Some(schema) = detail.schema.value.as_ref() {
        println!("Schema hash: {}", schema.schema_hash);
        println!("Schema bfbs size: {} bytes", schema.bfbs.len());
    } else {
        println!("Schema hash: (unavailable)");
        println!("Schema bfbs size: (unavailable)");
    }
    match detail.approx_rate.value {
        Some(rate) => println!(
            "Approx runtime rate (1s sample): {:.2} Hz",
            rate.average_rate_hz
        ),
        None => println!("Approx runtime rate (1s sample): no messages"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_topic_echo_key;
    use super::compute_hz_snapshot;
    use super::push_interval;
    use std::collections::VecDeque;

    #[test]
    fn build_topic_echo_key_matches_core_pub_key() {
        let key = build_topic_echo_key("default", "/latency_test").expect("topic key");
        assert_eq!(key, "aorta/default/pub/latency_test");
    }

    #[test]
    fn build_topic_echo_key_accepts_non_prefixed_input() {
        let key = build_topic_echo_key("default", "latency_test").expect("topic key");
        assert_eq!(key, "aorta/default/pub/latency_test");
    }

    #[test]
    fn build_topic_echo_key_normalizes_topic_path() {
        // Outer-slash strip still works; middle `//` collapse is no
        // longer silent (it is now a validation error), so use a clean path.
        let key = build_topic_echo_key("default", "/sensor/imu/").expect("topic key");
        assert_eq!(key, "aorta/default/pub/sensor/imu");
    }

    #[test]
    fn build_topic_echo_key_rejects_empty_middle_segment() {
        assert!(build_topic_echo_key("default", "/sensor//imu").is_err());
    }

    #[test]
    fn compute_hz_snapshot_matches_expected_values() {
        let intervals = VecDeque::from(vec![0.1, 0.2, 0.3]);
        let snapshot = compute_hz_snapshot(&intervals).expect("snapshot");
        assert!((snapshot.average_rate_hz - 5.0).abs() < 1e-9);
        assert!((snapshot.min_interval_s - 0.1).abs() < 1e-9);
        assert!((snapshot.max_interval_s - 0.3).abs() < 1e-9);
        assert!((snapshot.stddev_interval_s - 0.081649658).abs() < 1e-6);
        assert_eq!(snapshot.window_samples, 3);
    }

    #[test]
    fn compute_bw_snapshot_matches_expected_values() {
        use super::compute_bw_snapshot;
        use super::push_size;
        // 3 intervals of 0.1s -> 10 msg/s; sizes mean = (100+200+300+400)/4 = 250.
        let intervals = VecDeque::from(vec![0.1, 0.1, 0.1]);
        let sizes = VecDeque::from(vec![100u64, 200, 300, 400]);
        let snapshot = compute_bw_snapshot(&intervals, &sizes).expect("snapshot");
        assert!((snapshot.msg_per_s - 10.0).abs() < 1e-9);
        assert!((snapshot.mean_bytes - 250.0).abs() < 1e-9);
        assert_eq!(snapshot.max_bytes, 400);
        // bytes/s = msg/s * mean_bytes = 10 * 250 = 2500.
        assert!((snapshot.bytes_per_s - 2500.0).abs() < 1e-6);
        assert_eq!(snapshot.window_samples, 4);

        // Needs both windows populated.
        assert!(compute_bw_snapshot(&VecDeque::new(), &sizes).is_none());
        assert!(compute_bw_snapshot(&intervals, &VecDeque::new()).is_none());

        // push_size keeps the latest `window` entries.
        let mut s = VecDeque::new();
        for n in [1u64, 2, 3, 4] {
            push_size(&mut s, 3, n);
        }
        assert_eq!(Vec::from(s), vec![2, 3, 4]);
    }

    #[test]
    fn push_interval_keeps_latest_window_samples() {
        let mut intervals = VecDeque::new();
        push_interval(&mut intervals, 3, 0.1);
        push_interval(&mut intervals, 3, 0.2);
        push_interval(&mut intervals, 3, 0.3);
        push_interval(&mut intervals, 3, 0.4);
        assert_eq!(intervals.len(), 3);
        assert!((intervals[0] - 0.2).abs() < 1e-9);
        assert!((intervals[1] - 0.3).abs() < 1e-9);
        assert!((intervals[2] - 0.4).abs() < 1e-9);
    }
}
