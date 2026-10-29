use aorta_sdk::log_prelude::*;
use aorta_sdk::unix_time_ns;

use std::time::Duration;

use cli_args::ExampleArgs;
use flatbuffers::FlatBufferBuilder;
use perf_args::{build_payload_body, parse_interval, parse_pattern, parse_size, Pattern};
use perf_payload_msg::perf;
use zenoh_perf_common::{
    config_from_env_or_default, normalize_zenoh_topic, parse_format, PayloadFormat,
};

mod cli_args;
mod perf_args;
mod zenoh_perf_common;

#[derive(Debug)]
struct Args {
    name: String,
    topic: String,
    size: usize,
    interval: Duration,
    pattern: Pattern,
    format: PayloadFormat,
    count: Option<u64>,
}

fn print_usage() {
    aorta_error!(
        "Usage: zenoh_perf_talker [--name N] [--topic T] [--size BYTES] \\
  [--rate-hz HZ | --interval-ms MS] [--pattern zero|increment|random] \\
  [--format raw|flatbuffer] [--count N]\n\
\n\
Defaults:\n\
  name=zenoh_perf_talker topic=/perf/zenoh size=1024 rate=10Hz\n\
  pattern=increment format=raw\n"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut name = "zenoh_perf_talker".to_string();
    let mut topic = "/perf/zenoh".to_string();
    let mut size = 1024usize;
    let mut interval_ms: Option<u64> = None;
    let mut rate_hz: Option<f64> = None;
    let mut pattern = Pattern::Increment;
    let mut format = PayloadFormat::Raw;
    let mut count = None;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--name" => name = args.next_value().ok_or("missing --name value")?,
            "--topic" => topic = args.next_value().ok_or("missing --topic value")?,
            "--size" => {
                let value = args.next_value().ok_or("missing --size value")?;
                size = parse_size(&value).ok_or_else(|| format!("invalid --size: {value}"))?;
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
                    parse_pattern(&value).ok_or_else(|| format!("invalid --pattern: {value}"))?;
            }
            "--format" => {
                let value = args.next_value().ok_or("missing --format value")?;
                format =
                    parse_format(&value).ok_or_else(|| format!("invalid --format: {value}"))?;
            }
            "--count" => {
                let value = args.next_value().ok_or("missing --count value")?;
                count = Some(value.parse::<u64>().map_err(|_| "invalid --count value")?);
            }
            _ => {}
        }
    }

    let interval = parse_interval(interval_ms, rate_hz, 100)?;

    Ok(Args {
        name,
        topic,
        size: size.max(16),
        interval,
        pattern,
        format,
        count,
    })
}

fn build_raw_payload(size: usize, pattern: Pattern, sequence: u64, publish_ns: u64) -> Vec<u8> {
    let mut payload =
        build_payload_body(size, pattern, sequence as usize, unix_time_ns() ^ sequence);
    payload[0..8].copy_from_slice(&publish_ns.to_le_bytes());
    payload[8..16].copy_from_slice(&sequence.to_le_bytes());
    payload
}

fn build_flatbuffer_payload(
    builder: &mut FlatBufferBuilder<'_>,
    size: usize,
    pattern: Pattern,
    sequence: u64,
    publish_ns: u64,
) -> Vec<u8> {
    let body = build_payload_body(size, pattern, sequence as usize, unix_time_ns() ^ sequence);
    builder.reset();
    let payload_vec = builder.create_vector(&body);
    let root = perf::PerfPayload::create(
        builder,
        &perf::PerfPayloadArgs {
            aorta_header: None,
            payload_type: perf::PayloadKind::CUSTOM,
            payload: Some(payload_vec),
            tag: None,
            t_build_start_ns: 0,
            t_build_end_ns: 0,
            t_publish_call_ns: publish_ns,
        },
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = parse_args().map_err(|err| {
        print_usage();
        err
    })?;

    let zenoh_topic = normalize_zenoh_topic(&args.topic)?;
    aorta_info!(
        "zenoh_perf_talker name={} topic={} size={} rate_hz={:.2} format={:?}",
        args.name,
        zenoh_topic,
        args.size,
        1.0 / args.interval.as_secs_f64(),
        args.format
    );

    let config = config_from_env_or_default()?;
    let session = zenoh::open(config).await?;
    let publisher = session.declare_publisher(zenoh_topic).await?;

    let mut sequence = 0u64;
    let mut next_tick = tokio::time::Instant::now();
    let mut builder = FlatBufferBuilder::with_capacity(args.size.saturating_mul(2).max(1024));

    loop {
        let publish_ns = unix_time_ns();
        let payload = match args.format {
            PayloadFormat::Raw => build_raw_payload(args.size, args.pattern, sequence, publish_ns),
            PayloadFormat::Flatbuffer => build_flatbuffer_payload(
                &mut builder,
                args.size,
                args.pattern,
                sequence,
                publish_ns,
            ),
        };
        publisher.put(payload).await?;

        sequence = sequence.wrapping_add(1);
        if let Some(limit) = args.count {
            if sequence >= limit {
                break;
            }
        }

        next_tick += args.interval;
        tokio::time::sleep_until(next_tick).await;
    }

    session.close().await?;
    Ok(())
}
