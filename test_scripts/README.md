# Aorta Cross-Process Script Matrix

This directory contains interoperability scripts for:

- Rust <-> C++ pub/sub
- Rust <-> C++ RPC (echo)

All scripts run prebuilt binaries only. They never execute Bazel build commands.

CI: the public repository's generated build/test workflows run on push and
manual dispatch only (no schedule and no pull-request trigger) and do not run
the schema-attach, RPC, or RPC-strict matrices; of these scripts only the
Python interop scripts (`python_pubsub_interop.sh`, `python_rpc_interop.sh`,
`python_typed_examples.sh`) run there, in the Python SDK workflow. The scripts
stay directly runnable by developers with the prerequisites below.

## Scripts

### Pub/Sub

- `test_rust_talker_cpp_listener.sh`
  Rust talker -> C++ listener (single direction)
- `test_cpp_talker_rust_listener.sh`
  C++ talker -> Rust listener (single direction)
- `test_schema_attach_matrix.sh`
  Pub/sub matrix by schema attach policy (`never`, `always`)
- `test_subscriber_strategy_matrix.sh`
  Pub/sub matrix by subscriber strategy mode (`baseline`, `header_context`, `dedup`, `reorder`, `full`)
- `test_foxglove_compressed_video_e2e.sh`
  Minimal Foxglove topic E2E:
  - Rust publisher emits `foxglove.CompressedVideo`
  - CLI `schema get --describe` resolves topic schema
  - CLI `topic echo` decodes payload fields
- `test_foxglove_compressed_video_cpp_e2e.sh`
  Minimal Foxglove topic E2E:
  - C++ publisher emits `foxglove.CompressedVideo`
  - CLI `schema get --describe` resolves topic schema
  - CLI `topic echo` decodes payload fields
- `test_executor_admission_benchmark_matrix.sh`
  Executor admission/scheduler benchmark matrix:
  - `AORTA_EXECUTOR_MULTI_INGRESS` on/off
  - scheduler `fifo/priority`
  - drive mode `spin_once/spin`
  - load `sub_only/mixed` (`sub+rpc+timer`)
  - benchmark listener subscriber disables `MessageContext` topic/timestamp
    population to reduce callback hot-path overhead
  Outputs throughput, p50/p90/p95/p99 latency, CPU usage, and admission queue peaks
  for both `spin_once` and `spin`.
- `test_zenoh_pubsub_baseline_matrix.sh`
  Pure Zenoh pub/sub baseline matrix (no Aorta executor path):
  - format `raw/flatbuffer`
  - same rate/payload/report controls as Aorta benchmark scripts
  - Zenoh key expression uses canonical form without leading/trailing `/`
  Outputs throughput, p50/p90/p95/p99 latency, CPU usage, payload parse errors.
- `profile_samply_pubsub.sh`
  Samply profiling helper for Aorta perf pub/sub paths:
  - profile target `listener|talker|both`
  - configurable talker/listener runtime parameters
  - outputs `.json.gz` profiles and side logs for post-analysis
  - with pre-symbolication enabled, keep `.json.gz` and `.json.syms.json`
    together for symbolized view

### RPC

- `test_rust_rpc_client_cpp_rpc_server.sh`
  Rust RPC client -> C++ RPC server (single direction)
- `test_cpp_rpc_client_rust_rpc_server.sh`
  C++ RPC client -> Rust RPC server (single direction)
- `test_rpc_matrix.sh`
  RPC matrix by schema attach policy (`never`, `always`)
- `test_rpc_strict_matrix.sh`
  RPC strict matrix with two active providers (`off`, `on`) for both
  Rust-client/C++-servers and C++-client/Rust-servers

## Prerequisites (Developer-run)

Run from repository root:

```bash
bazel build //rust/examples:talker
bazel build //rust/examples:listener
bazel build //cpp/examples:talker
bazel build //cpp/examples:listener
bazel build //rust/examples:echo_server
bazel build //rust/examples:echo_client
bazel build //cpp/examples:echo_server
bazel build //cpp/examples:echo_client
bazel build //rust/examples:perf_listener
bazel build //rust/examples:perf_talker
bazel build //rust/examples:zenoh_perf_listener
bazel build //rust/examples:zenoh_perf_talker
bazel build //tools/aorta-cli:aorta
bazel build //rust/examples:foxglove_compressed_video_publisher
bazel build //cpp/examples:foxglove_compressed_video_publisher
```

## Quick Start

Pub/sub matrix:

```bash
bash test_scripts/test_schema_attach_matrix.sh
```

Foxglove minimal E2E:

```bash
bash test_scripts/test_foxglove_compressed_video_e2e.sh
```

Foxglove minimal E2E (C++ publisher):

```bash
bash test_scripts/test_foxglove_compressed_video_cpp_e2e.sh
```

Subscriber strategy matrix:

```bash
bash test_scripts/test_subscriber_strategy_matrix.sh
```

RPC matrix:

```bash
bash test_scripts/test_rpc_matrix.sh
```

RPC strict matrix:

```bash
bash test_scripts/test_rpc_strict_matrix.sh
```

Executor admission/scheduler benchmark matrix:

```bash
bash test_scripts/test_executor_admission_benchmark_matrix.sh
```

Pure Zenoh pub/sub baseline matrix:

```bash
bash test_scripts/test_zenoh_pubsub_baseline_matrix.sh
```

Samply profiling helper:

```bash
bash test_scripts/profile_samply_pubsub.sh --target both
```

## Validation Markers

- Pub/sub:
  - Rust -> C++ expects listener log contains `from rust_talker`
  - C++ -> Rust expects listener log contains `from cpp_talker`
  - Foxglove E2E expects `schema get --describe` output contains `CompressedVideo`
    and `topic echo` output contains `frame_id` + `h264`
  - Foxglove C++ E2E expects `schema get --describe` output contains `CompressedVideo`
    and `topic echo` output contains `frame_id` + `h264`
- RPC:
  - Client log contains `[echo ok]`
  - Server log contains `Replied echo request`

## Environment Variables

### Pub/Sub Scripts

- `AORTA_SCHEMA_ATTACH`
  `always` or `never` (default: `never`)
- `AORTA_SCHEMA_ATTACH_MATRIX`
  Space-separated list for pub/sub matrix (default: `never always`)
- `AORTA_TEST_TOPIC`
  Override topic for single-direction pub/sub scripts
- `AORTA_LISTENER_STARTUP_DELAY_SEC`
  Pub/sub listener startup delay in seconds (default: `2`)
- `AORTA_TALKER_TIMEOUT_SEC`
  Pub/sub talker timeout (default: `3s`)
- `AORTA_TEST_LOG_DIR`
  Log directory for single-direction pub/sub scripts
- `AORTA_TEST_LOG_ROOT`
  Root log directory for pub/sub matrix script
- `AORTA_TEST_GROUP`
  Group override for the Foxglove E2E script (default: `default`)
- `AORTA_TEST_RUN_ID`
  Optional deterministic run id for Foxglove topic suffix generation
- `AORTA_LISTENER_EXTRA_ARGS`
  Extra listener flags for single-direction pub/sub scripts, e.g. `--header-context --dedup`
- `AORTA_SUB_STRATEGY_MATRIX`
  Space-separated list for subscriber strategy matrix
  (default: `baseline header_context dedup reorder full`)
- `AORTA_STRATEGY_TEST_LOG_ROOT`
  Root log directory for subscriber strategy matrix script
- `AORTA_BENCH_INGRESS_MATRIX`
  Space-separated ingress list for benchmark matrix (`off on` by default)
- `AORTA_BENCH_SCHEDULER_MATRIX`
  Space-separated scheduler list (`fifo priority` by default)
- `AORTA_BENCH_LOAD_MATRIX`
  Space-separated load list (`sub_only mixed` by default)
- `AORTA_BENCH_DRIVE_MODE_MATRIX`
  Space-separated executor drive-mode list (`spin_once spin` by default)
- `AORTA_BENCH_SPIN_ONCE_TIMEOUT_MS`
  Base timeout for `spin_once` drive mode (default: `10`)
- `AORTA_BENCH_SPIN_ONCE_PROFILE`
  Timeout profile for `spin_once` drive mode (`fixed` or `adaptive`, default: `adaptive`)
- `AORTA_BENCH_DURATION_SEC`
  Benchmark duration per case in seconds (default: `20`)
- `AORTA_BENCH_WARMUP_SEC`
  Warmup time before measurement in seconds (default: `2`)
- `AORTA_BENCH_TALKER_RATE_HZ`
  Perf talker publish rate (default: `2000`)
- `AORTA_BENCH_PAYLOAD_SIZE`
  Perf payload size in bytes (default: `1024`)
- `AORTA_BENCH_EXECUTOR_QUEUE`
  Listener executor queue depth (default: `4096`)
- `AORTA_BENCH_RPC_INTERVAL_MS`
  RPC client request interval in mixed mode (default: `20`)
- `AORTA_BENCH_TIMER_PERIOD_MS`
  Listener timer period in mixed mode (default: `10`)
- `AORTA_BENCH_SERVICE_REPLY_DELAY_MS`
  Listener service reply delay in mixed mode (default: `0`)
- `AORTA_BENCH_LOG_ROOT`
  Root log directory for benchmark matrix script
- `AORTA_BENCH_ZENOH_SESSION_CONFIG_URI`
  Optional config URI passed as `ZENOH_SESSION_CONFIG_URI` for benchmark processes.
- `AORTA_BENCH_HEADER_INCLUDE_PUBLISHER_NODE`
  Controls typed publisher header `publisher_node` injection during benchmark
  (`1/true` default, matching normal SDK behavior). Set to `0/false` for
  optimized A/B experiments against pure Zenoh FlatBuffer baseline.
- `AORTA_EXECUTOR_ADMISSION_OBSERVABILITY`
  Optional executor admission observability switch (`0/false` default disabled).
  Set `1/true` to enable peak/drop counter tracking in executor hot path
  while keeping queue budget enforcement unchanged.
- `AORTA_ZENOH_BASELINE_FORMAT_MATRIX`
  Space-separated payload format list for pure Zenoh baseline script
  (`raw flatbuffer` by default)
- `AORTA_FOXGLOVE_STARTUP_TIMEOUT_SEC`
  Startup/schema wait timeout for Foxglove E2E (default: `12`)
- `AORTA_FOXGLOVE_ECHO_COUNT`
  Message count used by `topic echo` in Foxglove E2E (default: `2`)

### Samply Profiling Script

- `AORTA_SAMPLY_TARGET`
  Profile target: `listener`, `talker`, or `both` (default: `listener`)
- `AORTA_SAMPLY_DURATION_SEC`
  Samply recording duration in seconds (default: `20`)
- `AORTA_SAMPLY_RATE_HZ`
  Samply sampling rate in Hz (default: `1000`)
- `AORTA_SAMPLY_WARMUP_SEC`
  Warmup duration before recording starts (default: `10`)
- `AORTA_SAMPLY_AUTO_STOP_MARGIN_SEC`
  Extra seconds added to auto-derived sample budget for graceful target exit
  (default: `3`)
- `AORTA_SAMPLY_TOPIC`
  Topic used by perf talker/listener (default: `/bench/samply/profile`)
- `AORTA_SAMPLY_GROUP`
  Group passed to talker/listener (default: `default`)
- `AORTA_SAMPLY_LOG_ROOT`
  Output directory for profile files and logs
- `AORTA_SAMPLY_ZENOH_SESSION_CONFIG_URI`
  Optional config URI passed as `ZENOH_SESSION_CONFIG_URI`
- `AORTA_SAMPLY_MULTI_INGRESS`
  Multi-ingress switch (`off`/`on`, default: `off`)
- `AORTA_SAMPLY_HEADER_INCLUDE_PUBLISHER_NODE`
  Controls typed publisher header `publisher_node` injection during samply
  profiling (`1/true` default, matching normal SDK behavior). Set to `0/false`
  to profile the reduced publish-path string serialization mode.
- `AORTA_SAMPLY_TALKER_SIZE`
  Talker payload size (default: `1024`)
- `AORTA_SAMPLY_TALKER_RATE_HZ`
  Talker publish rate (default: `1000`)
- `AORTA_SAMPLY_TALKER_PATTERN`
  Talker pattern (`zero`/`increment`/`random`, default: `increment`)
- `AORTA_SAMPLY_TALKER_PUBLISH_API`
  Talker publish API (`closure`/`builder`/`bytes`/`all`, default: `builder`)
- `AORTA_SAMPLY_TALKER_COUNT`
  Talker message count. Default is auto-derived from
  `talker_rate_hz * (warmup_sec + duration_sec + auto_stop_margin_sec)`.
- `AORTA_SAMPLY_LISTENER_EXECUTOR_QUEUE`
  Listener executor queue depth (default: `4096`)
- `AORTA_SAMPLY_LISTENER_MAX_SAMPLES`
  Listener max samples. Default is auto-derived from
  `talker_rate_hz * (warmup_sec + duration_sec + auto_stop_margin_sec)`.
- `AORTA_SAMPLY_LISTENER_SCHEDULER`
  Listener scheduler (`fifo`/`priority`, default: `fifo`)
- `AORTA_SAMPLY_LISTENER_DRIVE_MODE`
  Listener drive mode (`spin`/`spin_once`, default: `spin_once`)
- `AORTA_SAMPLY_LISTENER_SPIN_ONCE_TIMEOUT_MS`
  Listener `spin_once` timeout (default: `10`)
- `AORTA_SAMPLY_LISTENER_SPIN_ONCE_PROFILE`
  Listener `spin_once` profile (`fixed`/`adaptive`, default: `adaptive`)
- `AORTA_SAMPLY_PRESYMBOLICATE`
  Enable samply pre-symbolication and sidecar symbols (`1` by default)
- `AORTA_SAMPLY_SYMBOL_DIR`
  Extra symbol search directory passed to `samply --symbol-dir`
  (default: `bazel-bin/rust/examples`)
- `AORTA_SAMPLY_SUMMARY_ENABLE`
  Emit profile summary after each run (`1` by default), including
  `cpu-only` and `no-libsystem_kernel.dylib` views.
- `AORTA_SAMPLY_TRIM_HEAD_MS`
  Trim head duration for steady-state summary in milliseconds (default: `3000`)
- `AORTA_SAMPLY_TRIM_TAIL_MS`
  Trim tail duration for steady-state summary in milliseconds (default: `3000`)
- `AORTA_SAMPLY_SUMMARY_TOPN`
  Top-N count for summary sections (default: `12`)

### RPC Scripts

- `AORTA_SCHEMA_ATTACH`
  `always`, `never`, or `inherit` (default: `never`)
- `AORTA_RPC_SCHEMA_ATTACH_MATRIX`
  Space-separated list for RPC matrix (default: `never always`)
- `AORTA_RPC_TEST_SERVICE`
  Override service name for single-direction RPC scripts
- `AORTA_RPC_SERVER_STARTUP_DELAY_SEC`
  RPC server startup delay in seconds (default: `2`)
- `AORTA_RPC_CLIENT_TIMEOUT_SEC`
  RPC client run timeout (default: `4s`)
- `AORTA_RPC_TEST_LOG_DIR`
  Log directory for single-direction RPC scripts
- `AORTA_RPC_TEST_LOG_ROOT`
  Root log directory for RPC matrix script
- `AORTA_RPC_REQUEST_SCHEMA_HASH`
  Optional request schema hash hex for RPC scripts. If `AORTA_SCHEMA_ATTACH=always`
  and not set, scripts auto-compute from `bazel-bin/.../echo.bfbs`.
- `AORTA_RPC_RESPONSE_SCHEMA_HASH`
  Optional response schema hash hex for RPC scripts. If `AORTA_SCHEMA_ATTACH=always`
  and not set, scripts auto-compute from `bazel-bin/.../echo.bfbs`.
- `AORTA_RPC_ENFORCE_SINGLE_PROVIDER`
  Optional strict single-provider check (`1/true` to enable). When enabled,
  client returns `ConcurrentConflict` if multiple replies are observed.
- `AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS`
  Optional strict-mode multi-reply detection window in milliseconds.
  Default is `5` in SDK; `test_rpc_strict_matrix.sh` uses `500` when unset to reduce flakiness.
- `AORTA_RPC_STRICT_MATRIX`
  Space-separated list for strict matrix modes (default: `off on`).
- `AORTA_RPC_STRICT_SERVICE_PREFIX`
  Service-name prefix used by strict matrix script
  (default: `/demo/echo/strict_matrix`).
- `AORTA_RPC_STRICT_LOG_ROOT`
  Root log directory for strict matrix script.
- `AORTA_RPC_ZENOH_SESSION_CONFIG_URI`
  Optional config URI passed to server/client processes as `ZENOH_SESSION_CONFIG_URI`
  (useful to disable shared_memory on environments without SHM permissions).

## Troubleshooting

- Missing binary
  Run the prerequisite Bazel build commands.
- `timeout` command not found
  Scripts fall back to internal seconds-based timeout (`3s`, `4s`, `10s`).
- Missing hash utility for RPC `always` policy
  Install one of: `sha256sum`, `shasum`, or `python3`.
- No messages or replies
  Check logs printed by script on failure, and verify both sides use the same topic/service and group.
