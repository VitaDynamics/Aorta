use aorta_sdk::log_prelude::*;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aorta_sdk::{
    with_contiguous_bytes, ExecutorOptions, Node, SchedulerMode, ServiceRequest,
    SingleThreadedExecutor, SpinOutcome, StopHandle,
};
use cli_args::ExampleArgs;
use perf_payload_msg::perf;

mod cli_args;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveMode {
    Spin,
    SpinOnce,
}

impl DriveMode {
    fn as_str(self) -> &'static str {
        match self {
            DriveMode::Spin => "spin",
            DriveMode::SpinOnce => "spin_once",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpinOnceTimeoutProfile {
    Fixed,
    Adaptive,
}

impl SpinOnceTimeoutProfile {
    fn as_str(self) -> &'static str {
        match self {
            SpinOnceTimeoutProfile::Fixed => "fixed",
            SpinOnceTimeoutProfile::Adaptive => "adaptive",
        }
    }
}

#[derive(Debug)]
struct Args {
    name: String,
    group: String,
    topic: String,
    report_interval: Duration,
    max_samples: usize,
    executor_queue: usize,
    scheduler_mode: SchedulerMode,
    drive_mode: DriveMode,
    spin_once_timeout_profile: SpinOnceTimeoutProfile,
    spin_once_timeout_ms: u64,
    service_name: Option<String>,
    service_reply_delay: Duration,
    timer_period: Option<Duration>,
}

#[derive(Debug, Clone)]
struct Snapshot {
    publish_to_cb: Vec<u64>,
    decode: Vec<u64>,
    total_count: u64,
    total_bytes: u64,
    interval_count: u64,
    interval_bytes: u64,
    missing_header: u64,
    clock_skew: u64,
    invalid_payload: u64,
    missing_trace: u64,
}

#[derive(Debug)]
struct SeriesStats {
    samples: VecDeque<u64>,
    max_samples: usize,
}

impl SeriesStats {
    fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples.min(1024)),
            max_samples,
        }
    }

    fn record(&mut self, value: u64) {
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(value);
    }

    fn snapshot(&self) -> Vec<u64> {
        self.samples.iter().copied().collect()
    }
}

#[derive(Debug)]
struct Stats {
    publish_to_cb: SeriesStats,
    decode: SeriesStats,
    total_count: u64,
    total_bytes: u64,
    interval_count: u64,
    interval_bytes: u64,
    missing_header: u64,
    clock_skew: u64,
    invalid_payload: u64,
    missing_trace: u64,
}

impl Stats {
    fn new(max_samples: usize) -> Self {
        Self {
            publish_to_cb: SeriesStats::new(max_samples),
            decode: SeriesStats::new(max_samples),
            total_count: 0,
            total_bytes: 0,
            interval_count: 0,
            interval_bytes: 0,
            missing_header: 0,
            clock_skew: 0,
            invalid_payload: 0,
            missing_trace: 0,
        }
    }

    fn record(
        &mut self,
        payload_len: usize,
        has_header: bool,
        skewed: bool,
        invalid_payload: bool,
        trace: Trace,
        recv_cb_ns: u64,
        decode_done_ns: u64,
    ) {
        self.total_count = self.total_count.wrapping_add(1);
        self.total_bytes = self.total_bytes.wrapping_add(payload_len as u64);
        self.interval_count = self.interval_count.wrapping_add(1);
        self.interval_bytes = self.interval_bytes.wrapping_add(payload_len as u64);

        if invalid_payload {
            self.invalid_payload = self.invalid_payload.wrapping_add(1);
            return;
        }

        if !has_header {
            self.missing_header = self.missing_header.wrapping_add(1);
            return;
        }

        if skewed {
            self.clock_skew = self.clock_skew.wrapping_add(1);
            return;
        }

        if decode_done_ns >= recv_cb_ns {
            self.decode.record(decode_done_ns - recv_cb_ns);
        }

        let mut has_trace = false;
        if trace.publish_call_ns > 0 && recv_cb_ns >= trace.publish_call_ns {
            self.publish_to_cb
                .record(recv_cb_ns - trace.publish_call_ns);
            has_trace = true;
        }

        if !has_trace {
            self.missing_trace = self.missing_trace.wrapping_add(1);
        }
    }

    fn snapshot(&mut self) -> Snapshot {
        let snapshot = Snapshot {
            publish_to_cb: self.publish_to_cb.snapshot(),
            decode: self.decode.snapshot(),
            total_count: self.total_count,
            total_bytes: self.total_bytes,
            interval_count: self.interval_count,
            interval_bytes: self.interval_bytes,
            missing_header: self.missing_header,
            clock_skew: self.clock_skew,
            invalid_payload: self.invalid_payload,
            missing_trace: self.missing_trace,
        };
        self.interval_count = 0;
        self.interval_bytes = 0;
        snapshot
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Trace {
    build_start_ns: u64,
    build_end_ns: u64,
    publish_call_ns: u64,
}

fn print_usage() {
    aorta_error!(
        "Usage: perf_listener [--name N] [--group G] [--topic T] \\
  [--report-interval-ms MS] [--max-samples N] \\
  [--executor-queue N] [--scheduler fifo|priority] \\
  [--drive-mode spin|spin_once] \\
  [--spin-once-timeout-ms MS] [--spin-once-profile fixed|adaptive] \\
  [--service NAME] [--service-reply-delay-ms MS] [--timer-period-ms MS]\n\
\n\
Defaults:\n\
  name=perf_listener group=default topic=/perf/payload\n\
  report-interval-ms=1000 max-samples=10000\n\
  executor-queue=0(disabled) scheduler=fifo\n\
  drive-mode=spin_once spin-once-timeout-ms=10 spin-once-profile=adaptive\n"
    );
}

fn parse_scheduler_mode(value: &str) -> Option<SchedulerMode> {
    match value.to_ascii_lowercase().as_str() {
        "fifo" => Some(SchedulerMode::Fifo),
        "priority" => Some(SchedulerMode::Priority),
        _ => None,
    }
}

fn parse_drive_mode(value: &str) -> Option<DriveMode> {
    match value.to_ascii_lowercase().as_str() {
        "spin" => Some(DriveMode::Spin),
        "spin_once" | "spin-once" => Some(DriveMode::SpinOnce),
        _ => None,
    }
}

fn parse_spin_once_timeout_profile(value: &str) -> Option<SpinOnceTimeoutProfile> {
    match value.to_ascii_lowercase().as_str() {
        "fixed" => Some(SpinOnceTimeoutProfile::Fixed),
        "adaptive" => Some(SpinOnceTimeoutProfile::Adaptive),
        _ => None,
    }
}

fn spin_once_timeout(
    profile: SpinOnceTimeoutProfile,
    timeout_ms: u64,
    idle_streak: u32,
) -> Duration {
    let base = timeout_ms.max(1);
    let ms = match profile {
        SpinOnceTimeoutProfile::Fixed => base,
        SpinOnceTimeoutProfile::Adaptive => {
            if idle_streak < 4 {
                1
            } else if idle_streak < 16 {
                base.min(4)
            } else if idle_streak < 64 {
                base.min(8)
            } else {
                base
            }
        }
    };
    Duration::from_millis(ms)
}

fn parse_args() -> Result<Args, String> {
    let mut name = "perf_listener".to_string();
    let mut group = "default".to_string();
    let mut topic = "/perf/payload".to_string();
    let mut report_interval = Duration::from_millis(1000);
    let mut max_samples = 10_000usize;
    let mut executor_queue = 0usize;
    let mut scheduler_mode = SchedulerMode::Fifo;
    let mut drive_mode = DriveMode::SpinOnce;
    let mut spin_once_timeout_profile = SpinOnceTimeoutProfile::Adaptive;
    let mut spin_once_timeout_ms = 10u64;
    let mut service_name: Option<String> = None;
    let mut service_reply_delay = Duration::from_millis(0);
    let mut timer_period: Option<Duration> = None;

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
            "--report-interval-ms" => {
                let value = args
                    .next_value()
                    .ok_or("missing --report-interval-ms value")?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "invalid --report-interval-ms value")?;
                report_interval = Duration::from_millis(ms);
            }
            "--max-samples" => {
                let value = args.next_value().ok_or("missing --max-samples value")?;
                max_samples = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --max-samples value")?;
            }
            "--executor-queue" => {
                let value = args.next_value().ok_or("missing --executor-queue value")?;
                executor_queue = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --executor-queue value")?;
            }
            "--scheduler" => {
                let value = args.next_value().ok_or("missing --scheduler value")?;
                scheduler_mode = parse_scheduler_mode(&value)
                    .ok_or_else(|| format!("invalid --scheduler value: {}", value))?;
            }
            "--drive-mode" => {
                let value = args.next_value().ok_or("missing --drive-mode value")?;
                drive_mode = parse_drive_mode(&value)
                    .ok_or_else(|| format!("invalid --drive-mode value: {}", value))?;
            }
            "--spin-once-timeout-ms" => {
                let value = args
                    .next_value()
                    .ok_or("missing --spin-once-timeout-ms value")?;
                spin_once_timeout_ms = value
                    .parse::<u64>()
                    .map_err(|_| "invalid --spin-once-timeout-ms value")?
                    .max(1);
            }
            "--spin-once-profile" => {
                let value = args
                    .next_value()
                    .ok_or("missing --spin-once-profile value")?;
                spin_once_timeout_profile = parse_spin_once_timeout_profile(&value)
                    .ok_or_else(|| format!("invalid --spin-once-profile value: {}", value))?;
            }
            "--service" => {
                service_name = Some(args.next_value().ok_or("missing --service value")?);
            }
            "--service-reply-delay-ms" => {
                let value = args
                    .next_value()
                    .ok_or("missing --service-reply-delay-ms value")?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "invalid --service-reply-delay-ms value")?;
                service_reply_delay = Duration::from_millis(ms);
            }
            "--timer-period-ms" => {
                let value = args.next_value().ok_or("missing --timer-period-ms value")?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "invalid --timer-period-ms value")?;
                if ms == 0 {
                    return Err("timer-period-ms must be > 0".to_string());
                }
                timer_period = Some(Duration::from_millis(ms));
            }
            _ => {}
        }
    }

    if max_samples == 0 {
        return Err("max-samples must be > 0".to_string());
    }

    Ok(Args {
        name,
        group,
        topic,
        report_interval,
        max_samples,
        executor_queue,
        scheduler_mode,
        drive_mode,
        spin_once_timeout_profile,
        spin_once_timeout_ms,
        service_name,
        service_reply_delay,
        timer_period,
    })
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|err| {
        print_usage();
        err
    })?;

    let node = Node::new(args.name, args.group).await?;
    let mut spin_once_executor: Option<SingleThreadedExecutor> = None;
    let mut admission_reader = None;
    let mut spin_stop_handle: Option<StopHandle> = None;
    if args.executor_queue > 0 {
        let mut options = ExecutorOptions::default();
        options.mode = args.scheduler_mode;
        let mut exec = SingleThreadedExecutor::new_with_options(args.executor_queue, options);
        admission_reader = Some(exec.admission_reader());
        exec.add_node(node.inner().clone());
        match args.drive_mode {
            DriveMode::Spin => {
                let stop = exec.stop_handle();
                tokio::spawn(async move {
                    exec.spin().await;
                });
                spin_stop_handle = Some(stop);
            }
            DriveMode::SpinOnce => {
                spin_once_executor = Some(exec);
            }
        }
    }

    let scheduler_label = match args.scheduler_mode {
        SchedulerMode::Fifo => "fifo",
        SchedulerMode::Priority => "priority",
    };
    aorta_info!(
        "perf_listener node={} group={} topic={} report_ms={} window={} executor_queue={} scheduler={} drive_mode={} spin_once_profile={} spin_once_timeout_ms={}",
        node.name(),
        node.group(),
        args.topic,
        args.report_interval.as_millis(),
        args.max_samples,
        args.executor_queue,
        scheduler_label,
        args.drive_mode.as_str(),
        args.spin_once_timeout_profile.as_str(),
        args.spin_once_timeout_ms,
    );

    let stats = std::sync::Arc::new(std::sync::Mutex::new(Stats::new(args.max_samples)));
    let node_for_cb = node.clone();
    let stats_for_cb = stats.clone();
    let service_calls = Arc::new(AtomicU64::new(0));
    let timer_ticks = Arc::new(AtomicU64::new(0));

    let _subscriber = node
        .create_subscriber(&args.topic, move |payload, _context| {
            let now = node_for_cb.now().as_nanos();
            let recv_cb_ns_i64 = now;
            let recv_cb_ns = if recv_cb_ns_i64 < 0 {
                0
            } else {
                recv_cb_ns_i64 as u64
            };
            let decode_start = Instant::now();
            let (payload_len, has_header, skewed, trace, invalid_payload) =
                with_contiguous_bytes(payload, |payload_bytes| {
                    match perf::root_as_perf_payload(payload_bytes) {
                        Ok(msg) => {
                            let payload_len = msg.payload().map(|data| data.len()).unwrap_or(0);
                            let mut has_header = false;
                            let mut skewed = false;
                            let mut publish_stamp_ns = 0u64;
                            let mut trace = Trace::default();
                            if let Some(header) = msg.aorta_header() {
                                has_header = true;
                                publish_stamp_ns = header.publish_stamp_ns();
                                if recv_cb_ns_i64 < 0 {
                                    skewed = true;
                                } else if recv_cb_ns < publish_stamp_ns {
                                    skewed = true;
                                }
                            }
                            trace.build_start_ns = msg.t_build_start_ns();
                            trace.build_end_ns = msg.t_build_end_ns();
                            trace.publish_call_ns = msg.t_publish_call_ns();
                            if trace.publish_call_ns == 0 {
                                trace.publish_call_ns = publish_stamp_ns;
                            }
                            (payload_len, has_header, skewed, trace, false)
                        }
                        Err(_) => (payload_bytes.len(), false, false, Trace::default(), true),
                    }
                });
            let decode_done_ns =
                recv_cb_ns.saturating_add(decode_start.elapsed().as_nanos() as u64);

            if let Ok(mut guard) = stats_for_cb.lock() {
                guard.record(
                    payload_len,
                    has_header,
                    skewed,
                    invalid_payload,
                    trace,
                    recv_cb_ns,
                    decode_done_ns,
                );
            }
        })
        .await?;

    let _service = if let Some(service_name) = args.service_name.as_deref() {
        let calls = Arc::clone(&service_calls);
        let delay = args.service_reply_delay;
        Some(
            node.create_service(service_name, move |request: ServiceRequest| {
                calls.fetch_add(1, Ordering::Relaxed);
                let payload = request.payload().clone();
                tokio::spawn(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let _ = request.reply(payload).await;
                });
            })
            .await?,
        )
    } else {
        None
    };

    let _timer = if let Some(period) = args.timer_period {
        let ticks = Arc::clone(&timer_ticks);
        Some(
            node.create_timer(period, move || {
                ticks.fetch_add(1, Ordering::Relaxed);
            })
            .await?,
        )
    } else {
        None
    };

    let mut ticker = tokio::time::interval(args.report_interval);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut last_report = Instant::now();
    let mut last_service_calls = 0u64;
    let mut last_timer_ticks = 0u64;
    let mut spin_once_idle_streak = 0u32;

    loop {
        if let Some(exec) = spin_once_executor.as_mut() {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                _ = ticker.tick() => {
                    let elapsed = last_report.elapsed();
                    last_report = Instant::now();
                    let snapshot = stats.lock().ok().map(|mut guard| guard.snapshot());
                    if let Some(snapshot) = snapshot {
                        report_snapshot(&snapshot, elapsed);
                    }
                    let service_total = service_calls.load(Ordering::Relaxed);
                    let timer_total = timer_ticks.load(Ordering::Relaxed);
                    let service_window = service_total.saturating_sub(last_service_calls);
                    let timer_window = timer_total.saturating_sub(last_timer_ticks);
                    last_service_calls = service_total;
                    last_timer_ticks = timer_total;
                    aorta_info!(
                        "mixed_load service_calls_total={} service_calls_window={} timer_ticks_total={} timer_ticks_window={}",
                        service_total, service_window, timer_total, timer_window
                    );

                    if let Some(reader) = &admission_reader {
                        let admission = reader.snapshot();
                        aorta_info!(
                            "admission_stats queued_events={} queued_bytes={} peak_events={} peak_bytes={} drop_full={} drop_closed={} drop_budget_events={} drop_budget_bytes={}",
                            admission.queued_events,
                            admission.queued_bytes,
                            admission.peak_queued_events,
                            admission.peak_queued_bytes,
                            admission.dropped_queue_full,
                            admission.dropped_queue_closed,
                            admission.dropped_budget_events,
                            admission.dropped_budget_bytes
                        );
                    }
                }
                outcome = exec.spin_once(spin_once_timeout(
                    args.spin_once_timeout_profile,
                    args.spin_once_timeout_ms,
                    spin_once_idle_streak,
                )) => {
                    match outcome {
                        SpinOutcome::Event => {
                            spin_once_idle_streak = 0;
                        }
                        SpinOutcome::Timeout => {
                            spin_once_idle_streak = spin_once_idle_streak.saturating_add(1);
                        }
                        SpinOutcome::Stopped | SpinOutcome::QueueClosed => {
                            break;
                        }
                    }
                }
            }
        } else {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                _ = ticker.tick() => {
                    let elapsed = last_report.elapsed();
                    last_report = Instant::now();
                    let snapshot = stats.lock().ok().map(|mut guard| guard.snapshot());
                    if let Some(snapshot) = snapshot {
                        report_snapshot(&snapshot, elapsed);
                    }
                    let service_total = service_calls.load(Ordering::Relaxed);
                    let timer_total = timer_ticks.load(Ordering::Relaxed);
                    let service_window = service_total.saturating_sub(last_service_calls);
                    let timer_window = timer_total.saturating_sub(last_timer_ticks);
                    last_service_calls = service_total;
                    last_timer_ticks = timer_total;
                    aorta_info!(
                        "mixed_load service_calls_total={} service_calls_window={} timer_ticks_total={} timer_ticks_window={}",
                        service_total, service_window, timer_total, timer_window
                    );
                    if let Some(reader) = &admission_reader {
                        let admission = reader.snapshot();
                        aorta_info!(
                            "admission_stats queued_events={} queued_bytes={} peak_events={} peak_bytes={} drop_full={} drop_closed={} drop_budget_events={} drop_budget_bytes={}",
                            admission.queued_events,
                            admission.queued_bytes,
                            admission.peak_queued_events,
                            admission.peak_queued_bytes,
                            admission.dropped_queue_full,
                            admission.dropped_queue_closed,
                            admission.dropped_budget_events,
                            admission.dropped_budget_bytes
                        );
                    }
                }
            }
        }
    }

    if let Some(stop) = spin_stop_handle {
        stop.stop();
    }
    node.close().await?;
    Ok(())
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
    let mut publish_to_cb = snapshot.publish_to_cb.clone();
    let mut decode = snapshot.decode.clone();

    publish_to_cb.sort_unstable();
    decode.sort_unstable();

    let window = publish_to_cb.len();
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

    let (pub_min, pub_max, pub_mean, pub_std, pub_p50, pub_p90, pub_p95, pub_p99) =
        summarize(&publish_to_cb);
    let (dec_min, dec_max, dec_mean, dec_std, dec_p50, dec_p90, dec_p95, dec_p99) =
        summarize(&decode);

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
        format_latency(pub_min),
        format_latency(pub_p50),
        format_latency(pub_p90),
        format_latency(pub_p95),
        format_latency(pub_p99),
        format_latency(pub_max),
        pub_mean / 1_000.0,
        pub_std / 1_000.0
    );
    aorta_info!(
        "latency_us decode min={:.2} p50={:.2} p90={:.2} p95={:.2} p99={:.2} max={:.2} mean={:.2} std={:.2}",
        format_latency(dec_min),
        format_latency(dec_p50),
        format_latency(dec_p90),
        format_latency(dec_p95),
        format_latency(dec_p99),
        format_latency(dec_max),
        dec_mean / 1_000.0,
        dec_std / 1_000.0
    );
    aorta_info!(
        "errors missing_header={} missing_trace={} clock_skew={} invalid_payload={}",
        snapshot.missing_header,
        snapshot.missing_trace,
        snapshot.clock_skew,
        snapshot.invalid_payload
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
