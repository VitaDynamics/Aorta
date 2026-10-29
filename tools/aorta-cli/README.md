# Aorta CLI Runtime Discovery Model

Last updated: 2026-09-21

This document defines the runtime discovery model for `aorta-cli` when deployment always includes at least one Zenoh router.

## Scope

- Commands covered: `topic`, `service`, `schema`, `nodes`, `action`, `record`, `play`, `clean`.
- Focus: where each command gets runtime visibility from (`admin space`, `liveliness`, or direct queryables), and how timeout should be applied.
- End-to-end and unit test execution details are in `TEST_PLAN.md`.

## Version Reporting

`aorta --version` (and `aorta -V`) prints the version identity the release
pipeline stamped:

- stamped build (`bazel build --stamp --embed_label=2026.9.99`):
  `aorta 2026.9.99 (<git-sha12>)`
- unstamped build (the local/Bazel default):
  `aorta 0.0.0+unstamped (<git-sha12|unknown>)`

The commit comes from `STABLE_GIT_COMMIT`, emitted by
`tools/bazel/workspace_status.sh` and injected through `rustc_env`. A build
without `--stamp` never reads the status file, so the commit degrades to
`unknown`, and the missing release label falls back to the explicit
`0.0.0+unstamped` form rather than a Cargo version that names no release.

## Router Assumptions

- A Zenoh router is always present and reachable from CLI.
- Router admin space read access is enabled.
- `aorta-cli` may still open a normal `zenoh::Session`; it does not need to create an Aorta `Node`.

## Discovery Backend Matrix

| Command | Primary backend | Key / selector | Timeout model | Notes |
|:---|:---|:---|:---|:---|
| `aorta topic list` | Router admin space + node telemetry + interface directory | `@/{router_zid}/router/publisher/aorta/{group}/pub/**` + telemetry inventories + `aorta/{group}/sys/edu_directory` | Query timeout only | Merges three sources. Each default text row prints `<name>` (live entries only); `--verbose` rows print `<name>  live|absent  <sources>`, where `sources` is any of `adminspace`, `telemetry`, `directory`. Telemetry contributes subscriber-only topics and keeps the listing alive when the admin space is unreachable; the directory contributes allowlisted names that may legitimately be `absent`. (`--verbose` adds provider endpoint/node candidates; `--node <name|id>` filters by provider node.) |
| `aorta topic echo <topic>` | Direct subscriber | `aorta/{group}/pub/{normalized_topic}` | Streaming (no discovery timeout) | Optional preflight via admin publisher view. |
| `aorta topic hz <topic>` | Direct subscriber | `aorta/{group}/pub/{normalized_topic}` | Streaming; periodic reporting | ROS2-style rate stats (`average/min/max/stddev`) over sliding window (`--window`, default `8192`). |
| `aorta topic info <topic>` | Admin + schema queryable + interface directory | Admin publisher snapshot + `aorta/{group}/sys/schema_by_key/{key_suffix}` + `aorta/{group}/sys/edu_directory` | Query timeout only | Includes provider endpoint IDs and candidate node mapping, plus a `Discovery: <sources> (live|absent)` line when any source knows the topic. |
| `aorta service list` | Router admin space + node telemetry + interface directory | `@/{router_zid}/router/queryable/aorta/{group}/srv/**` + telemetry inventories + `aorta/{group}/sys/edu_directory` | Query timeout only | Same three-source merge and row shape as `topic list` (`<name>` by default, `<name>  live|absent  <sources>` with `--verbose`). (`--verbose` adds provider endpoint/node candidates; `--node <name|id>` filters by provider node.) |
| `aorta service info <service>` | Router admin space + interface directory | `@/{router_zid}/router/queryable/aorta/{group}/srv/{normalized_service}` + `aorta/{group}/sys/edu_directory` | Query timeout only | Shows provider endpoint IDs and candidate node mapping, plus the same `Discovery:` line as `topic info`. |
| `aorta service call <service> <json>` | Direct service query + schema queryable | `aorta/{group}/srv/{normalized_service}` + `aorta/{group}/sys/schema_by_key/srv/{normalized_service}` | Request timeout (`--timeout`) | Request JSON is encoded to FB via C++ FFI; response FB is decoded back to JSON via C++ FFI. |
| `aorta action list` | Direct action info queryable | `aorta/{group}/act/info/**` | Query timeout only | Lists registered action servers with node name and schema hash. |
| `aorta action info <action>` | Direct action info queryable | `aorta/{group}/act/info/{normalized_action}` (`?schema=bfbs` for the schema, falling back to `aorta/sys/schema/{schema_hash}`) | Query timeout only | Shows server policy, server epoch, and derived root types. |
| `aorta action goals <action> [--all]` | Direct latest-value query + schema registry | `aorta/{group}/act/status/{normalized_action}/*` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Query timeout only | Decodes public goal status through shared BFBS reflection. Without `--all`, hides terminal goals. |
| `aorta action status <action> --goal-id <uuid>` | Direct latest-value query + schema registry | `aorta/{group}/act/status/{normalized_action}/{goal_key}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Query timeout only | Shows one goal's public lifecycle state. Requires active `ActionInfo` to resolve schema hash. |
| `aorta action feedback <action> [--goal-id <uuid>]` | Direct subscriber + schema registry | `aorta/{group}/act/feedback/{normalized_action}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Streaming (until interrupted) | Decodes feedback samples with the action feedback root type and optionally filters by `goal_id`. |
| `aorta action send <action> --json <goal-json-or-@file> [--non-blocking]` | Direct action queryable + schema registry; default blocking mode also subscribes to feedback/status | `aorta/{group}/act/send_goal/{normalized_action}` + `aorta/{group}/act/feedback/{normalized_action}` + `aorta/{group}/act/status/{normalized_action}/{goal_key}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Send request timeout (`10s`); blocking mode waits until terminal | CLI wraps caller JSON into `SendGoalRequest`, auto-generates `goal_id` unless provided, prints `goal_secret` on acceptance, then by default streams matching feedback and waits for the final result. Use `--non-blocking` to return immediately after send. |
| `aorta action cancel <action> --goal-id <uuid> --goal-secret <secret>` | Direct action queryable + schema registry | `aorta/{group}/act/cancel_goal/{normalized_action}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Request timeout (`10s`) | Sends authenticated cancel request and prints accepted/rejected result. |
| `aorta action result <action> --goal-id <uuid> --goal-secret <secret>` | Direct action queryable + schema registry | `aorta/{group}/act/get_result/{normalized_action}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Request timeout (`10s`) | Fetches private result snapshot and renders nested feedback/result JSON. |
| `aorta action wait <action> --goal-id <uuid> --goal-secret <secret>` | Public status latest-value + subscriber, then private result query | `aorta/{group}/act/status/{normalized_action}/{goal_key}` + `aorta/{group}/act/get_result/{normalized_action}` + `aorta/{group}/act/info/{normalized_action}?schema=bfbs` (legacy fallback: `aorta/sys/schema/{schema_hash}`) | Wait timeout (`--timeout`, default `300s`) | First waits on public status reaching terminal, then immediately fetches authenticated private result. |
| `aorta schema get <key>` | Direct schema_by_key queryable | `aorta/{group}/sys/schema_by_key/{normalized_key_suffix}` | Query timeout only | Accepts explicit suffix (`pub/...`, `srv/...`) or full key; for slash-prefixed and bare keys it tries topic first, then service. `--describe` prints field-level schema definitions from BFBS. Admin queryable view can be used for diagnostics only. |
| `aorta nodes list` | Router admin space + liveliness fallback | Snapshot: `@/{router_zid}/router/token/aorta/{group}/sys/alive/**`; Fallback: liveliness get; Watch: liveliness subscriber | Snapshot query timeout; watch is streaming | Prefer admin snapshot; fallback to liveliness when admin is unavailable (`--verbose` adds endpoint ownership details; `--with-resources` adds candidate topic/service counts in snapshot mode). |
| `aorta nodes info <node|node_id>` | Router admin space | token + publisher/queryable snapshots | Query timeout only | Lists candidate topics/services for a node via endpoint ownership mapping. |
| `aorta clean shm` | Local-only | N/A | Local operation timeout | No router/admin dependency. |
| `aorta interface-directory --allowlist <json>` | Router admin space, served back on a queryable | reads `@/{router_zid}/router/{publisher,queryable}/aorta/{group}/{pub,srv}/**`, serves `aorta/{group}/sys/edu_directory` | Refreshes every 1s; runs until interrupted | Trusted-side helper. Projects live publisher/queryable declarations onto an allowlist file and publishes only `{name, live}` pairs, so a client without admin-space access can still list the published surface. Allowlist names are validated with the publish-side key builder and canonicalised to `/`-prefixed form. |

## Listing Output Contract

`topic list` / `service list` default text rows are `<name>` for live entries
only; `--verbose` rows are `<name>  <live|absent>  <sources>`. With
`--format json` each row is:

```json
{"name": "/demo/chatter", "live": true, "sources": ["adminspace", "telemetry"], "topic": "/demo/chatter"}
```

`name`, `live` and `sources` are the current contract. Earlier versions of
the rows carried only `topic` (resp. `service`); that field is still emitted as a
deprecated alias of `name` so existing scripts keep working. New consumers
should read `name`. `--verbose` additionally sets `endpoints` and `nodes`.

An interface is `live` when router adminspace or node telemetry observed it,
or a trusted directory reports `live=true`. A directory-only row may therefore
be live or absent; `sources` records who observed the interface, independently
of its live state. Telemetry is credited only for an actual inventory entry,
not an admin-only row returned by the aggregate introspection listing.

Discovery reuses the command session, including its configured namespace and
`--no-client-override` policy. It does not open another listener. The directory
query runs concurrently with the existing introspection queries; single-interface
`info` commands do not print the empty-list directory hint.

## Timeout Policy

- Use Zenoh query timeout (`get(...).timeout(...)`) for query-like commands.
- Do not use passive "collect for N seconds" windows for discovery snapshots.
- Keep streaming commands open (`topic echo`, `topic hz`, `nodes list --watch`) until user count/interrupt ends them.

## Action Workflow Quickstart

The action subcommands are designed to be used as one workflow:

1. discover the action server;
2. send one goal;
3. keep the returned `goal_id` and `goal_secret`;
4. inspect feedback / public status;
5. wait for or fetch the private result;
6. optionally cancel the goal.

Example server:

```bash
bazel run //rust/examples:cli_execute_task_server -- \
  --group default
```

Inspect the action:

```bash
aorta --group default action list
aorta --group default action info /example/flow_resume_probe
```

Send a goal with inline JSON:

```bash
aorta --group default action send /example/flow_resume_probe \
  --json '{"source":"quick","input_type":"ProbePreset","input":{"path":"demo/quick","args":[]}}'
```

By default, `action send` stays attached until the goal reaches a terminal state. It prints the accepted metadata, streams matching feedback, and then renders the final result. Use `--non-blocking` to keep the previous fire-and-follow-up workflow.

Send a goal from a file:

```bash
cat > /tmp/execute_task_goal.json <<'EOF'
{"source":"slow","input_type":"ProbePreset","input":{"path":"demo/slow","args":[]}}
EOF

aorta --group default action send /example/flow_resume_probe \
  --json @/tmp/execute_task_goal.json
```

Return immediately after send:

```bash
aorta --group default action send /example/flow_resume_probe \
  --json @/tmp/execute_task_goal.json \
  --non-blocking
```

On success, `action send` still prints copy-pasteable follow-up commands for:

- `action status`
- `action feedback`
- `action wait`
- `action cancel`

Keep both `goal_id` and `goal_secret`. The `goal_secret` is required for:

- `action cancel`
- `action result`
- `action wait`

Typical follow-up commands:

```bash
aorta --group default action feedback /example/flow_resume_probe --goal-id <goal_id>
aorta --group default action wait /example/flow_resume_probe --goal-id <goal_id> --goal-secret <goal_secret> --timeout 30
aorta --group default action result /example/flow_resume_probe --goal-id <goal_id> --goal-secret <goal_secret>
```

Cancel flow:

```bash
aorta --group default action cancel /example/flow_resume_probe \
  --goal-id <goal_id> \
  --goal-secret <goal_secret>
```

### Using CLI against the C++ local example server

`cpp/examples:cli_execute_task_server` does **not** use the normal Zenoh default session. So that it does not depend on multicast scouting, which is unreliable on some development hosts, it starts with an isolated loopback-only peer config:

- listens on `tcp/127.0.0.1:17447`
- disables multicast / gossip scouting
- disables SHM

Because `bazel run` launches the CLI in a separate process, the CLI does not automatically inherit that server config. To talk to the C++ example server, create a client config such as:

```bash
cat > /tmp/aorta_cli_loopback_peer_17447.json5 <<'EOF'
{
  mode: "peer",
  connect: {
    endpoints: ["tcp/127.0.0.1:17447"]
  },
  listen: {
    endpoints: ["tcp/127.0.0.1:0"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
EOF
```

Then run CLI commands with:

```bash
aorta --group default \
  --zenoh-config /tmp/aorta_cli_loopback_peer_17447.json5 \
  action send /example/flow_resume_probe \
  --json '{"source":"quick","input_type":"ProbePreset","input":{"path":"demo/quick","args":[]}}'
```

## User Key Style

- User-facing topic/service names are rendered with a leading slash (for example, `/demo/chatter`, `/echo_schema`).
- CLI input accepts both forms for topic/service names:
  - slash-prefixed: `/demo/chatter`
  - non-prefixed: `demo/chatter`

## Reflection Path

- `aorta-cli` consumes the shared `aorta-introspect` crate for `FB -> JSON`, `JSON -> FB`, and `BFBS -> schema text`.
- `aorta-introspect` keeps a narrow C++ FlatBuffers bridge for text/JSON conversion while Rust owns higher-level schema introspection.
- Rust shared entrypoint: `../aorta-introspect/src/reflect.rs`
- C++ bridge: `../aorta-introspect/ffi/fb_reflect.{h,cc}` and `../aorta-introspect/ffi/json_to_fb.{h,cc}`
- Build mode:
  - Cargo: `aorta-introspect/build.rs` + `cc` crate
  - Bazel: `aorta-introspect` `cc_library` linked through the shared crate

This keeps one implementation direction with C++ FlatBuffers behavior and avoids parallel reflection logic in Rust.

### Cargo Build Note (C++ FlatBuffers Sources)

For Cargo mode, `aorta-introspect/build.rs` compiles FlatBuffers C++ parser/text-generator sources together with the shared FFI code.

- Preferred: set `AORTA_FLATBUFFERS_ROOT` to a FlatBuffers source tree root (contains `include/flatbuffers/idl.h` and `src/idl_parser.cpp`).
- Fallback: auto-discover Bazel external path under `/private/var/tmp/_bazel_$USER/.../external/flatbuffers` (or `/tmp/_bazel_$USER/.../external/flatbuffers`).

## Verification Targets

- Bazel CLI unit test target: `//tools/aorta-cli:aorta_cli_test`
- Bazel CLI version-output test: `//tools/aorta-cli:aorta_version_test`
- Bazel CLI binary target: `//tools/aorta-cli:aorta`
- Bazel CLI quick smoke e2e (manual): `//tools/aorta-cli:aorta_cli_smoke_test`
- Bazel CLI full regression e2e (manual): `//tools/aorta-cli:aorta_cli_regression_test`

## MCAP Playback

`aorta play <input>...` uses the same `aorta_play::PlayerConfig` engine as the
standalone `aorta-play` binary. It accepts the full playback flag surface from
the MCAP player proposal, including rate, timeline bounds, topic filters,
schema policy, size limits, buffer size, Zenoh config, and log level.

For playback group selection, an explicit global `--group` flag wins over the
recorded MCAP metadata. Ambient `AORTA_GROUP` is not a playback override. The
player currently replays recorded payloads only; it does not publish a
simulation clock.

## Automated E2E Profiles

`aorta-cli` now provides two shell-based e2e profiles via Bazel `sh_test`:

- `aorta_cli_smoke_test` (`quick`): verifies core command chain (`topic/service/schema/nodes`) against real `cli_talker` and `cli_echo_server`.
- `aorta_cli_regression_test` (`full`): includes additional checks (`topic hz/info`, schema dump, `clean shm`, `nodes list --watch` OFFLINE event).

Both targets are tagged `manual` and `no-sandbox`, so they are intended for developer-run environments with a local router/admin space enabled.

## Recommended Test Nodes

For stable local CLI validation, use the dedicated schema-aware Rust examples:

- Topic producer: `//rust/examples:cli_talker`
- Service provider: `//rust/examples:cli_echo_server`
- Action server: `//rust/examples:cli_execute_task_server`

Example startup commands:

```bash
bazel run //rust/examples:cli_talker -- \
  --group default --topic /demo/chatter --schema-attach always

bazel run //rust/examples:cli_echo_server -- \
  --group default --service /echo_schema --schema-attach always
```

## Why This Replaces Wildcard Sniffing

Previous discovery via wildcard subscriptions (for example, subscribe to `aorta/{group}/pub/**` and wait 2s) is traffic-dependent and incomplete:

- silent publishers/services are missed if no message arrives in the window;
- correctness depends on arbitrary collection timeout;
- output can fluctuate with traffic burst timing.

Router admin space provides an entity snapshot and is therefore the default discovery source when router presence is guaranteed.

## Router Admin Space Requirement

CLI discovery commands that rely on admin space require router read permission to `@/...` keys.

If admin space is disabled or denied:

- fail fast with a clear diagnostics message for discovery commands (`topic list`, `service list`, `nodes list` snapshot);
- keep direct operational commands usable (`topic echo`, `service call`, `schema get`, `clean shm`) where possible.
