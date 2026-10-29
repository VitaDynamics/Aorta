use aorta_sdk::log_prelude::*;

use std::time::Duration;

use aorta_sdk::{HeaderInjector, HeaderValues, Node, SchemaMeta};
use cli_args::ExampleArgs;
use flatbuffers::FlatBufferBuilder;
use perf_args::{build_payload_body, parse_interval, parse_pattern, parse_size, Pattern};
use perf_payload_msg::{aorta, perf, PERF_PAYLOAD_BFBS, PERF_PAYLOAD_FIXED_SIZE};

mod cli_args;
mod perf_args;

struct PerfPayloadSchema;

impl SchemaMeta for PerfPayloadSchema {
    fn schema_bfbs() -> &'static [u8] {
        PERF_PAYLOAD_BFBS
    }

    fn fixed_size() -> Option<usize> {
        PERF_PAYLOAD_FIXED_SIZE
    }
}

impl HeaderInjector for PerfPayloadSchema {
    type Header<'a> = aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let node = if values.publisher_node.is_empty() {
            None
        } else {
            Some(builder.create_string(values.publisher_node))
        };
        aorta::sys::AortaHeader::create(
            builder,
            &aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: node,
                sequence: values.sequence,
            },
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum PublishApi {
    Closure,
    Builder,
    Bytes,
    All,
}

impl PublishApi {
    fn as_str(self) -> &'static str {
        match self {
            PublishApi::Closure => "closure",
            PublishApi::Builder => "builder",
            PublishApi::Bytes => "bytes",
            PublishApi::All => "all",
        }
    }
}

#[derive(Debug)]
struct Args {
    name: String,
    group: String,
    topic: String,
    payload_type: perf::PayloadKind,
    size: usize,
    interval: Duration,
    pattern: Pattern,
    publish_api: PublishApi,
    count: Option<u64>,
    tag: Option<String>,
}

fn now_ns(node: &Node) -> u64 {
    let now = node.now().as_nanos();
    if now < 0 {
        0
    } else {
        now as u64
    }
}

fn print_usage() {
    aorta_error!(
        "Usage: perf_talker [--name N] [--group G] [--topic T] \\
  [--payload-type control|lidar|image|custom|unknown] \\
  [--size BYTES] [--rate-hz HZ | --interval-ms MS] \\
  [--pattern zero|increment|random] \\
  [--publish-api closure|builder|bytes|all] \\
  [--count N] [--tag STR]\n\
\n\
Defaults:\n\
  name=perf_talker group=default topic=/perf/payload\n\
  payload-type=custom size=256B rate=10Hz pattern=zero publish-api=builder\n\
Size supports suffixes: K, M, G (e.g. 64K, 1M)\n"
    );
}

fn parse_payload_kind(value: &str) -> Option<perf::PayloadKind> {
    match value.to_ascii_lowercase().as_str() {
        "control" => Some(perf::PayloadKind::CONTROL),
        "lidar" => Some(perf::PayloadKind::LIDAR),
        "image" => Some(perf::PayloadKind::IMAGE),
        "custom" => Some(perf::PayloadKind::CUSTOM),
        "unknown" => Some(perf::PayloadKind::UNKNOWN),
        _ => None,
    }
}

fn parse_publish_api(value: &str) -> Option<PublishApi> {
    match value.to_ascii_lowercase().as_str() {
        "closure" => Some(PublishApi::Closure),
        "builder" => Some(PublishApi::Builder),
        "bytes" => Some(PublishApi::Bytes),
        "all" => Some(PublishApi::All),
        _ => None,
    }
}

fn default_size(kind: perf::PayloadKind) -> usize {
    match kind {
        perf::PayloadKind::CONTROL => 64,
        perf::PayloadKind::LIDAR => 64 * 1024,
        perf::PayloadKind::IMAGE => 1024 * 1024,
        _ => 256,
    }
}

fn parse_args() -> Result<Args, String> {
    let mut name = "perf_talker".to_string();
    let mut group = "default".to_string();
    let mut topic = "/perf/payload".to_string();
    let mut payload_type = perf::PayloadKind::CUSTOM;
    let mut size: Option<usize> = None;
    let mut interval_ms: Option<u64> = None;
    let mut rate_hz: Option<f64> = None;
    let mut pattern = Pattern::Zero;
    let mut publish_api = PublishApi::Builder;
    let mut count: Option<u64> = None;
    let mut tag: Option<String> = None;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--name" => {
                name = args.next_value().ok_or("missing --name value")?;
            }
            "--group" => {
                group = args.next_value().ok_or("missing --group value")?;
            }
            "--topic" => {
                topic = args.next_value().ok_or("missing --topic value")?;
            }
            "--payload-type" | "--type" => {
                let value = args.next_value().ok_or("missing --payload-type value")?;
                payload_type = parse_payload_kind(&value)
                    .ok_or_else(|| format!("invalid payload type: {}", value))?;
            }
            "--size" => {
                let value = args.next_value().ok_or("missing --size value")?;
                size = Some(parse_size(&value).ok_or_else(|| format!("invalid size: {}", value))?);
            }
            "--interval-ms" => {
                let value = args.next_value().ok_or("missing --interval-ms value")?;
                interval_ms = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "invalid --interval-ms value")?,
                );
            }
            "--rate-hz" => {
                let value = args.next_value().ok_or("missing --rate-hz value")?;
                rate_hz = Some(
                    value
                        .parse::<f64>()
                        .map_err(|_| "invalid --rate-hz value")?,
                );
            }
            "--pattern" => {
                let value = args.next_value().ok_or("missing --pattern value")?;
                pattern =
                    parse_pattern(&value).ok_or_else(|| format!("invalid pattern: {}", value))?;
            }
            "--publish-api" | "--api" => {
                let value = args.next_value().ok_or("missing --publish-api value")?;
                publish_api = parse_publish_api(&value)
                    .ok_or_else(|| format!("invalid publish api: {}", value))?;
            }
            "--count" => {
                let value = args.next_value().ok_or("missing --count value")?;
                count = Some(value.parse::<u64>().map_err(|_| "invalid --count value")?);
            }
            "--tag" => {
                tag = Some(args.next_value().ok_or("missing --tag value")?);
            }
            _ => {}
        }
    }

    let size = size.unwrap_or_else(|| default_size(payload_type));
    let interval = parse_interval(interval_ms, rate_hz, 100)?;

    Ok(Args {
        name,
        group,
        topic,
        payload_type,
        size,
        interval,
        pattern,
        publish_api,
        count,
        tag,
    })
}

fn build_perf_payload<'b>(
    builder: &mut FlatBufferBuilder<'b>,
    header: flatbuffers::WIPOffset<aorta::sys::AortaHeader<'b>>,
    payload: &[u8],
    payload_type: perf::PayloadKind,
    tag: Option<&str>,
    publish_call_ns: u64,
) {
    let payload_vec = builder.create_vector(payload);
    let tag_str = tag.map(|value| builder.create_string(value));
    let root = perf::PerfPayload::create(
        builder,
        &perf::PerfPayloadArgs {
            aorta_header: Some(header),
            payload_type,
            payload: Some(payload_vec),
            tag: tag_str,
            t_build_start_ns: 0,
            t_build_end_ns: 0,
            t_publish_call_ns: publish_call_ns,
        },
    );
    builder.finish(root, None);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|err| {
        print_usage();
        err
    })?;

    let payload_type = args.payload_type;
    let tag = args.tag.clone();
    let tag_ref = tag
        .as_deref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let mut bytes_builder = FlatBufferBuilder::with_capacity(args.size.saturating_add(512));

    let node = Node::new(args.name, args.group).await?;
    let payload = build_payload_body(args.size, args.pattern, 0, now_ns(&node));
    aorta_info!(
        "perf_talker node={} group={} topic={} size={} type={:?} interval_ms={} api={}",
        node.name(),
        node.group(),
        args.topic,
        args.size,
        payload_type,
        args.interval.as_millis(),
        args.publish_api.as_str()
    );

    let publisher = node
        .create_publisher::<PerfPayloadSchema>(&args.topic)
        .await?;
    if let Some(hash) = publisher.schema_hash() {
        aorta_info!("schema registered hash={}", hash.to_hex());
    }

    let mut ticker = tokio::time::interval(args.interval);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut sent: u64 = 0;
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            _ = ticker.tick() => {
                let api = if matches!(args.publish_api, PublishApi::All) {
                    match sent % 3 {
                        0 => PublishApi::Closure,
                        1 => PublishApi::Builder,
                        _ => PublishApi::Bytes,
                    }
                } else {
                    args.publish_api
                };
                let tag_value = if matches!(args.publish_api, PublishApi::All) && tag_ref.is_none() {
                    Some(api.as_str())
                } else {
                    tag_ref
                };

                match api {
                    PublishApi::Closure => {
                        publisher.publish(|builder, header| {
                            build_perf_payload(
                                builder,
                                header,
                                &payload,
                                payload_type,
                                tag_value,
                                0,
                            );
                        })?;
                    }
                    PublishApi::Builder => {
                        publisher.publish(|builder, header| {
                            build_perf_payload(
                                builder,
                                header,
                                &payload,
                                payload_type,
                                tag_value,
                                0,
                            );
                        })?;
                    }
                    PublishApi::Bytes => {
                        let publish_call_ns = now_ns(&node);
                        let header_values = HeaderValues {
                            publish_stamp_ns: publish_call_ns,
                            publisher_node: node.name(),
                            sequence: sent,
                        };
                        let header = PerfPayloadSchema::build_header(&mut bytes_builder, &header_values);
                        build_perf_payload(
                            &mut bytes_builder,
                            header,
                            &payload,
                            payload_type,
                            tag_value,
                            publish_call_ns,
                        );
                        let bytes = bytes_builder.finished_data().to_vec();
                        publisher.publish_bytes(bytes)?;
                        bytes_builder.reset();
                    }
                    PublishApi::All => {}
                }

                sent = sent.wrapping_add(1);
                if let Some(limit) = args.count {
                    if sent >= limit {
                        break;
                    }
                }
            }
        }
    }

    node.close().await?;
    Ok(())
}
