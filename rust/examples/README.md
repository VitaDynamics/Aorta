# Aorta Middleware Examples

This directory contains Rust SDK examples for pub/sub, RPC, action, timers, and performance scenarios.

## Status

All example binaries are aligned with current SDK contracts.

- Typed FlatBuffers schemas are used for scenario demos (`sensor_*`, `control_loop`, `late_joiner`, `performance_demo`).
- Shared helpers are used to reduce drift:
  - `cli_args.rs`
  - `schema_attach.rs`
  - `fb_schema.rs`
  - `perf_args.rs`
  - `zenoh_perf_common.rs`

## Import Pattern

For cleaner Rust example code, prefer:

- `use aorta_sdk::prelude::*;`
- `use <message_crate>::prelude::*;`

This is the Rust equivalent of C++ `#include "aorta/aorta.h"` + one message header.
It keeps most SDK/message symbols available through two imports.
Message namespace modules (for example `demo`, `perf`, `examples`) are also re-exported
from `<message_crate>::prelude::*`.

Typed publish direction:

- Preferred style is moving to SDK `publish_typed(...)` so example code fills
  payload message structs only.
- Legacy builder callback style remains supported for advanced/custom paths.
- Typical typed path:
  - `node.create_publisher::<ChatSchema>(...)`
  - `publisher.publish_typed(|msg: &mut ChatMsg| { ... })`

Typed/decoded subscribe direction:

- Prefer `with_contiguous_bytes(payload, |bytes| { ... })` for payload decoding.
- Fast path borrows single-slice payload directly (zero-copy).
- Multi-slice payload falls back to contiguous materialization only when needed.
- Avoid unconditional `payload.to_bytes()` in hot callbacks unless ownership is required.

## Key Style

- Example defaults use slash-prefixed topic/service keys (for example, `/demo/chatter`, `/echo`).
- Input remains backward-compatible: both slash-prefixed and non-prefixed forms are accepted.

## CLI Reference (Current Behavior)

The following flags reflect actual code behavior in this directory.

- `talker`
  - Flags: `--name`, `--group`, `--topic`
  - Defaults: `name=talker`, `group=default`, `topic=/demo/chatter`
- `all_types_talker`
  - Flags: `--name`, `--group`, `--topic`
  - Defaults: `name=all_types_talker`, `group=default`, `topic=/demo/all_types`
  - Behavior: publishes `demo.AllTypes` with scalar/string/enum/struct/table/vector fields via typed publisher
- `listener`
  - Flags: `--name`, `--group`, `--topic`, `--header-context`, `--dedup`, `--dedup-window`, `--reorder-check`, `--drop-out-of-order`
  - Defaults: `name=listener`, `group=default`, `topic=/demo/chatter`, `dedup_window=1024`
- `foxglove_compressed_video_publisher`
  - Flags: `--name`, `--group`, `--topic`
  - Defaults: `name=foxglove_cv_publisher`, `group=default`, `topic=/foxglove/compressed_video`
  - Behavior: publishes `foxglove.CompressedVideo` via typed publisher (`publish_typed`) with auto header injection
- `cli_talker`
  - Flags: `--name`, `--group`, `--topic`, `--schema-attach`
  - Defaults: `name=cli_talker`, `group=default`, `topic=/demo/chatter`, `schema_attach=always`
  - `--schema-attach` values: `never|always|inherit`
- `echo_server`
  - Flags: `--name`, `--group`, `--service`, `--schema-attach`
  - Defaults: `name=echo_server`, `group=default`, `service=/echo`, `schema_attach=never`
  - Behavior: typed RPC service example using `create_service_typed_view`
- `echo_client`
  - Flags: `--name`, `--group`, `--service`, `--schema-attach`, `--enforce-single-provider`, `--multi-reply-window-ms`, `--interval-ms`
  - Defaults: `name=echo_client`, `group=default`, `service=/echo`, `schema_attach=never`, `interval_ms=1000`
  - Behavior: typed RPC client example using `create_client_typed`
- `cli_echo_server`
  - Flags: `--name`, `--group`, `--service`, `--schema-attach`
  - Defaults: `name=cli_echo_server`, `group=default`, `service=/echo_schema`, `schema_attach=always`
  - `--schema-attach` values: `never|always|inherit`
- `cli_execute_task_server`
  - Flags: `--name`, `--group`
  - Defaults: `name=cli_execute_task_server`, `group=default`
  - Behavior: action server example for `example/flow_resume_probe`; emits feedback, supports cancel, and is used by `aorta-cli` e2e tests
- `cli_execute_task_client`
  - Flags: `--name`, `--group`, `--source`, `--preset`, `--goal-id`, `--wait-timeout-ms`, `--cancel-after-ms`
  - Defaults: `name=cli_execute_task_client`, `group=default`, `source=quick`, `preset=demo/<source>`, `wait-timeout-ms=10000`
  - Behavior: typed Action client example for `example/flow_resume_probe`; sends one goal, streams feedback, optionally requests cancel, then waits for final result. The goal is built as `source + ProbeInputMsg::ProbePreset(ProbePresetMsg { path: preset, args: [] })`.
- `navigation_service_server`
  - Flags: `--name`, `--group`, `--get-plan-service`, `--cancel-plan-service`, `--current-pose-service`
  - Defaults: `name=navigation_service_server`, `group=default`, `get_plan=/get_plan`, `cancel_plan=/cancel_plan`, `current_pose=/get_current_pose`
- `perf_talker`
  - Flags: `--name`, `--group`, `--topic`, `--payload-type`, `--size`, `--rate-hz`, `--interval-ms`, `--pattern`, `--publish-api`, `--count`, `--tag`
  - Defaults: `name=perf_talker`, `group=default`, `topic=/perf/payload`, `payload-type=custom`, `size=256`, `rate=10Hz`, `pattern=zero`, `publish-api=builder`
  - Notes: `--size` supports `K/M/G` suffixes; `--payload-type` supports `control|lidar|image|custom|unknown`
- `perf_listener`
  - Flags: `--name`, `--group`, `--topic`, `--report-interval-ms`, `--max-samples`, `--executor-queue`, `--scheduler`, `--drive-mode`, `--spin-once-timeout-ms`, `--spin-once-profile`, `--service`, `--service-reply-delay-ms`, `--timer-period-ms`
  - Defaults: `name=perf_listener`, `group=default`, `topic=/perf/payload`, `report-interval-ms=1000`, `max-samples=10000`, `executor-queue=0`, `scheduler=fifo`, `drive-mode=spin_once`, `spin-once-timeout-ms=10`, `spin-once-profile=adaptive`
- `zenoh_perf_talker`
  - Flags: `--name`, `--topic`, `--size`, `--rate-hz`, `--interval-ms`, `--pattern`, `--format`, `--count`
  - Defaults: `name=zenoh_perf_talker`, `topic=/perf/zenoh`, `size=1024`, `rate=10Hz`, `pattern=increment`, `format=raw`
  - `--format` values: `raw|flatbuffer`
- `zenoh_perf_listener`
  - Flags: `--name`, `--topic`, `--report-interval-ms`, `--max-samples`, `--format`
  - Defaults: `name=zenoh_perf_listener`, `topic=/perf/zenoh`, `report-interval-ms=1000`, `max-samples=10000`, `format=raw`
  - `--format` values: `raw|flatbuffer`
- `late_joiner`
  - Flags: `--subscriber`
  - Default mode: publisher
  - `--subscriber` switches to subscriber mode
- `sensor_publisher`, `sensor_subscriber`, `control_loop`, `performance_demo`, `timer_demo`, `simple_timer_publish`, `unified_publish_demo`
  - No CLI flags
  - Run with target defaults only

## Build/Run Commands

```bash
# Basic chat examples
bazel run //rust/examples:talker
bazel run //rust/examples:listener
bazel run //rust/examples:all_types_talker
bazel run //rust/examples:foxglove_compressed_video_publisher
bazel run //rust/examples:cli_talker -- \
  --group default --topic /demo/chatter --schema-attach always

# RPC examples
bazel run //rust/examples:echo_server
bazel run //rust/examples:echo_client
bazel run //rust/examples:cli_echo_server
bazel run //rust/examples:navigation_service_server
bazel run //rust/examples:navigation_service_client

# Action example
bazel run //rust/examples:cli_execute_task_server
bazel run //rust/examples:cli_execute_task_client -- \
  --group default \
  --source quick
bazel run //rust/examples:cli_execute_task_client -- \
  --group default \
  --source cancel \
  --cancel-after-ms 600

# RPC schema attach (always)
bazel run //rust/examples:echo_server -- \
  --schema-attach always \
  --service-schema-hash <hash_hex>

# Sensor scenario
bazel run //rust/examples:sensor_publisher
bazel run //rust/examples:sensor_subscriber

# Scenario demos
bazel run //rust/examples:control_loop
bazel run //rust/examples:late_joiner
bazel run //rust/examples:late_joiner -- --subscriber
bazel run //rust/examples:performance_demo

# Aorta perf pair
bazel run //rust/examples:perf_talker
bazel run //rust/examples:perf_listener

# Direct Zenoh perf pair
bazel run //rust/examples:zenoh_perf_talker
bazel run //rust/examples:zenoh_perf_listener
```

## Action Quickstart (`execute_task`)

Rust action examples currently ship as a matched pair:

- server: `//rust/examples:cli_execute_task_server`
- client: `//rust/examples:cli_execute_task_client`

The examples drive the `flow_resume_probe` action
(`message/schemas/action/example/flow_resume_probe.fbs`), whose goal carries:

- `source: string`
- `input: ProbeInput` (a union with the single member `ProbePreset { path, args }`)

The Rust example client populates `input` as
`ProbeInputMsg::ProbePreset(ProbePresetMsg { path: <preset>, args: [] })`. If
you are driving the same Action through `aorta-cli`, the equivalent FlatBuffers
JSON shape is:

```json
{
  "source": "quick",
  "input_type": "ProbePreset",
  "input": {
    "path": "demo/quick",
    "args": []
  }
}
```

Typical success path:

```bash
bazel run //rust/examples:cli_execute_task_server -- --group default

bazel run //rust/examples:cli_execute_task_client -- \
  --group default \
  --source quick \
  --preset demo/quick
```

Typical cooperative-cancel path:

```bash
bazel run //rust/examples:cli_execute_task_client -- \
  --group default \
  --source cancel \
  --preset demo/cancel \
  --cancel-after-ms 600
```

On acceptance, the client logs:

- `goal_id`
- `goal_secret`
- `server_epoch`

Persist `goal_id` and `goal_secret` if you want to restore a `GoalHandle`,
query the private result later, or issue cancel from another process.

The Action runtime supports multiple in-flight goals. Whether goal work is
executed in parallel is decided by the server-side application logic and the
executor/runtime it uses.

## Notes

- Group resolution is environment-authoritative:
  - Runtime uses `AORTA_GROUP` as the effective group.
  - If `AORTA_GROUP` is missing or blank, it falls back to `default`.
  - `Node::new(name, group)` keeps `group` for API compatibility.
- Typed RPC examples can use:
  - `Node::create_service_typed_view::<ReqSchema, ResSchema, _>()`
  - `Node::create_service_typed_retained::<ReqSchema, ResSchema, _>()`
  - `Node::create_client_typed::<ReqSchema, ResSchema>()`
- Action examples use (the action name is a runtime arg decoupled from the
  schema, like topic/service; pass `TAction::ACTION_NAME` for the codegen default):
  - `ActionServer::<TAction>::serve(&node, TAction::ACTION_NAME, options, handler)`
  - `ActionClient::<TAction>::new(&node, TAction::ACTION_NAME, ActionClientOptions::default())`
  - `GoalHandle::feedback_stream()` / `GoalHandle::wait_result(...)`
- IDE note (rust-analyzer):
  - This repo uses Bazel-generated message crates (for example crate `echo_msg`, from target `echo_msg_rs`), so
    Cargo-only IDE indexing is incomplete.
  - Generate Bazel Rust project metadata:
    - `bazel run //:refresh_rust_project`
    - Default scope is `//...`
  - See root guide:
    - `RUST_LSP_SETUP.md`
  - `rust/Cargo.toml` sets `autoexamples = false` to reduce
    helper-file false diagnostics when Bazel metadata is not loaded.

## Contributing

When adding or updating examples:

1. Prefer generated schema wrappers from `message`.
2. Keep publisher/subscriber pairs on the same payload contract.
3. Reuse shared helpers in this directory when argument or schema policy semantics overlap.
4. Update this README and related architecture/spec docs in the same change.

## References

- [Rust SDK guide](../README.md)
- [SDK common concepts](../../spec/Aorta_SDK_Common_Concepts.md)
