use aorta_sdk::log_prelude::*;
use aorta_sdk::with_contiguous_bytes;
use aorta_sdk::unix_time_ns;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cli_args::ExampleArgs;
use perf_payload_msg::perf;
use zenoh_perf_common::{
    config_from_env_or_default, normalize_zenoh_topic, parse_format, PayloadFormat,
};

mod cli_args;
mod zenoh_perf_common;

#[derive(Debug)]
struct Args {
    name: String,
    topic: String,
    report_interval: Duration,
    max_samples: usize,
    format: PayloadFormat,
}

#[derive(Debug, Clone)]
struct Snapshot {
    latencies: Vec<u64>,
    total_count: u64,
    total_bytes: u64,
    interval_count: u64,
    interval_bytes: u64,
    invalid_payload: u64,
    clock_skew: u64,
}

#[derive(Debug)]
struct Stats {
    samples: VecDeque<u64>,
    max_samples: usize,
    total_count: u64,
    total_bytes: u64,
    interval_count: u64,
    interval_bytes: u64,
    invalid_payload: u64,
    clock_skew: u64,
}

impl Stats {
    fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples.min(1024)),
            max_samples,
            total_count: 0,
            total_bytes: 0,
            interval_count: 0,
            interval_bytes: 0,
            invalid_payload: 0,
            clock_skew: 0,
        }
    }

    fn record(&mut self, payload_len: usize, publish_ns: Option<u64>, recv_ns: u64) {
        self.total_count = self.total_count.wrapping_add(1);
        self.total_bytes = self.total_bytes.wrapping_add(payload_len as u64);
        self.interval_count = self.interval_count.wrapping_add(1);
        self.interval_bytes = self.interval_bytes.wrapping_add(payload_len as u64);

        let Some(sent) = publish_ns else {
            self.invalid_payload = self.invalid_payload.wrapping_add(1);
            return;
        };
        if recv_ns < sent {
            self.clock_skew = self.clock_skew.wrapping_add(1);
            return;
        }
        let latency_ns = recv_ns - sent;
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(latency_ns);
    }

    fn snapshot(&mut self) -> Snapshot {
        let snapshot = Snapshot {
            latencies: self.samples.iter().copied().collect(),
            total_count: self.total_count,
            total_bytes: self.total_bytes,
            interval_count: self.interval_count,
            interval_bytes: self.interval_bytes,
            invalid_payload: self.invalid_payload,
            clock_skew: self.clock_skew,
        };
        self.interval_count = 0;
        self.interval_bytes = 0;
        snapshot
    }
}

fn print_usage() {
    aorta_error!(
        "Usage: zenoh_perf_listener [--name N] [--topic T] \\
  [--report-interval-ms MS] [--max-samples N] [--format raw|flatbuffer]\n\
\n\
Defaults:\n\
  name=zenoh_perf_listener topic=/perf/zenoh report-interval-ms=1000\n\
  max-samples=10000 format=raw\n"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut name = "zenoh_perf_listener".to_string();
    let mut topic = "/perf/zenoh".to_string();
    let mut report_interval = Duration::from_millis(1000);
    let mut max_samples = 10_000usize;
    let mut format = PayloadFormat::Raw;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--name" => name = args.next_value().ok_or("missing --name value")?,
            "--topic" => topic = args.next_value().ok_or("missing --topic value")?,
            "--report-interval-ms" => {
                let value = args
                    .next_value()
                    .ok_or("missing --report-interval-ms value")?;
                let ms = value
                    .parse::<u64>()
                    .map_err(|_| "invalid --report-interval-ms value")?;
                report_interval = Duration::from_millis(ms);
            }
            "--max-samples" => {
                let value = args.next_value().ok_or("missing --max-samples value")?;
                max_samples = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --max-samples value")?;
            }
            "--format" => {
                let value = args.next_value().ok_or("missing --format value")?;
                format =
                    parse_format(&value).ok_or_else(|| format!("invalid --format: {value}"))?;
            }
            _ => {}
        }
    }

    Ok(Args {
        name,
        topic,
        report_interval,
        max_samples,
        format,
    })
}

fn parse_publish_ns_raw(payload: &[u8]) -> Option<u64> {
    let bytes = payload.get(0..8)?;
    Some(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn parse_publish_ns_flatbuffer(payload: &[u8]) -> Option<u64> {
    perf::root_as_perf_payload(payload)
        .ok()
        .map(|msg| msg.t_publish_call_ns())
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn format_latency(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

fn summarize(samples: &[u64]) -> (u64, u64, f64, f64, u64, u64, u64, u64) {
    if samples.is_empty() {
        return (0u64, 0u64, 0.0f64, 0.0f64, 0u64, 0u64, 0u64, 0u64);
    }
    let min = samples[0];
    let max = samples[samples.len() - 1];
    let sum: u128 = samples.iter().map(|v| *v as u128).sum();
    let mean = sum as f64 / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|v| {
            let diff = *v as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / samples.len() as f64;
    let stddev = variance.sqrt();
    let p50 = percentile(samples, 0.50);
    let p90 = percentile(samples, 0.90);
    let p95 = percentile(samples, 0.95);
    let p99 = percentile(samples, 0.99);
    (min, max, mean, stddev, p50, p90, p95, p99)
}

fn report_snapshot(snapshot: &Snapshot, elapsed: Duration) {
    let mut latencies = snapshot.latencies.clone();
    latencies.sort_unstable();
    let window = latencies.len();

    let rate = if elapsed.as_secs_f64() > 0.0 {
        snapshot.interval_count as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        snapshot.interval_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
    } else {
        0.0
    };

    let (lat_min, lat_max, lat_mean, lat_std, lat_p50, lat_p90, lat_p95, lat_p99) =
        summarize(&latencies);

    aorta_info!(
        "perf_stats count={} window={} total_bytes={} rate={:.2} msg/s throughput={:.2} MB/s",
        snapshot.total_count,
        window,
        snapshot.total_bytes,
        rate,
        throughput
    );
    aorta_info!(
        "latency_us publish_to_cb min={:.2} p50={:.2} p90={:.2} p95={:.2} p99={:.2} max={:.2} mean={:.2} std={:.2}",
        format_latency(lat_min),
        format_latency(lat_p50),
        format_latency(lat_p90),
        format_latency(lat_p95),
        format_latency(lat_p99),
        format_latency(lat_max),
        lat_mean / 1_000.0,
        lat_std / 1_000.0
    );
    aorta_info!(
        "errors invalid_payload={} clock_skew={}",
        snapshot.invalid_payload,
        snapshot.clock_skew
    );
    aorta_info!(
        "payload bytes window_avg={:.1} last_window={} bytes",
        if snapshot.interval_count > 0 {
            snapshot.interval_bytes as f64 / snapshot.interval_count as f64
        } else {
            0.0
        },
        snapshot.interval_bytes
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = parse_args().map_err(|err| {
        print_usage();
        err
    })?;
    let zenoh_topic = normalize_zenoh_topic(&args.topic)?;
    aorta_info!(
        "zenoh_perf_listener name={} topic={} report_ms={} max_samples={} format={:?}",
        args.name,
        zenoh_topic,
        args.report_interval.as_millis(),
        args.max_samples,
        args.format
    );

    let stats = Arc::new(Mutex::new(Stats::new(args.max_samples)));
    let stats_cb = Arc::clone(&stats);
    let format = args.format;

    let config = config_from_env_or_default()?;
    let session = zenoh::open(config).await?;
    let _subscriber = session
        .declare_subscriber(zenoh_topic)
        .callback(move |sample| {
            let payload = sample.payload().clone();
            let recv_ns = unix_time_ns();
            with_contiguous_bytes(&payload, |bytes| {
                let publish_ns = match format {
                    PayloadFormat::Raw => parse_publish_ns_raw(bytes),
                    PayloadFormat::Flatbuffer => parse_publish_ns_flatbuffer(bytes),
                };
                if let Ok(mut guard) = stats_cb.lock() {
                    guard.record(bytes.len(), publish_ns, recv_ns);
                }
            });
        })
        .await?;

    let mut ticker = tokio::time::interval(args.report_interval);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut last_report = Instant::now();

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                let elapsed = last_report.elapsed();
                last_report = Instant::now();
                if let Ok(mut guard) = stats.lock() {
                    let snapshot = guard.snapshot();
                    report_snapshot(&snapshot, elapsed);
                }
            }
        }
    }

    session.close().await?;
    Ok(())
}
