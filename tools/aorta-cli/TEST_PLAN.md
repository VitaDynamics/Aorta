# Aorta CLI Test Plan

Last updated: 2026-04-05

## 1. Goals

This document defines a practical test strategy for `aorta-cli`, covering:

- unit tests (fast, deterministic);
- manual end-to-end tests (real router, real nodes/services);
- regression gates for discovery behavior (`admin space` + liveliness fallback);
- current FFI bridge behavior for request/response conversion.

## 2. Scope

Commands in scope:

- `topic list|echo|hz|info`
- `service list|info|call`
- `action list|info|goals|status|send|feedback|cancel|result|wait`
- `schema get`
- `nodes list [--watch]|info`
- `clean shm`

## 3. Test Levels

### 3.1 Unit Tests (Cargo + Bazel)

Purpose: validate pure logic and local wrappers without requiring external processes.

Run:

```bash
cargo test --manifest-path tools/aorta-cli/Cargo.toml
bazel test //tools/aorta-cli:aorta_cli_test
```

Current unit focus:

| Area | File | What is validated |
|:---|:---|:---|
| Topic key normalization | `src/cmd/topic.rs` | `topic` input -> core-aligned pub key |
| Admin key parsing + ownership decode | `src/cmd/admin.rs`, `src/cmd/topic.rs` | extract normalized suffix/node id and decode provider ownership ids from admin payload |
| Shared topic schema resolver | `../aorta-introspect/src/topic_schema.rs` | schema selector building, hash canonicalization, attachment hash decoding, and cache-backed lookups |
| Schema key normalization | `src/cmd/schema.rs` | topic/full-key forms -> canonical key suffix |
| Action CLI decode helpers | `src/cmd/action.rs` | action info selector building, goal-id generation, JSON file loading, action wrapper decode, feedback decode/filter helpers, enum parsing, and duration formatting |
| Shared FFI FB->JSON bridge wrapper | `../aorta-introspect/src/reflect.rs` | call/return/error ownership boundary |
| Shared FFI JSON->FB bridge wrapper | `../aorta-introspect/src/reflect.rs` | call/return/error ownership boundary |

### 3.2 Automated Smoke (Developer-Run)

Purpose: fast confidence before full manual matrix.

```bash
# Build CLI binary
bazel build //tools/aorta-cli:aorta

# Quick e2e smoke (manual target, requires local router/admin)
bazel test //tools/aorta-cli:aorta_cli_smoke_test

# Full e2e regression (manual target, requires local router/admin)
bazel test //tools/aorta-cli:aorta_cli_regression_test

# Optional whole middleware gate (longer)
bazel test //...
```

Notes:

- `aorta_cli_smoke_test` runs `tests/cli_e2e.sh --profile=quick`.
- `aorta_cli_regression_test` runs `tests/cli_e2e.sh --profile=full`.
- Both targets now start `cli_talker`, `cli_echo_server`, and `cli_execute_task_server` automatically and assert topic/service/action output.

### 3.3 Manual End-to-End Tests

Purpose: validate CLI behavior against a real router and running examples.

All Bazel commands below are developer-run.

### 3.4 Regression Gate

Minimum gate for every CLI change:

1. Cargo unit tests pass.
2. Bazel CLI unit target passes (`:aorta_cli_test`).
3. Bazel quick e2e smoke passes (`:aorta_cli_smoke_test`).
4. `nodes list` fallback path (`M-NOD-003`) is manually validated at least once.

## 4. Environment Preflight

### 4.1 Router Admin Requirement

For snapshot discovery commands (`topic list`, `service list`, `nodes list`), router admin read access must be enabled.

Your current LaunchAgent config is compatible:

- `--adminspace-permissions r`
- `--rest-http-port=8000`

### 4.2 Preflight Checks

```bash
plutil -p ~/Library/LaunchAgents/com.yifeima.zenohd.plist
curl -sf -g http://127.0.0.1:8000/@/local/router >/dev/null
```

If `curl` fails, check router logs:

```bash
tail -n 200 ~/Library/Logs/zenohd/zenohd.err.log
tail -n 200 ~/Library/Logs/zenohd/zenohd.out.log
```

### 4.3 Optional Config Toggle for Fallback Test

To validate `nodes list` fallback path, temporarily disable admin read and restart launchd service:

1. Edit `~/Library/LaunchAgents/com.yifeima.zenohd.plist` and remove or change `--adminspace-permissions r`.
2. Restart:

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.yifeima.zenohd.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.yifeima.zenohd.plist
```

3. Restore admin read after fallback test.

## 5. Manual Test Matrix

## 5.1 Setup (Terminals)

Terminal A: start topic publisher

```bash
bazel run //rust/examples:cli_talker -- \
  --group default --topic /demo/chatter --schema-attach always
```

Terminal B: start schema-aware echo service (positive case for `service call`)

```bash
bazel run //rust/examples:cli_echo_server -- \
  --group default --service /echo_schema --schema-attach always
```

Terminal C (optional negative case): start plain echo service without service schema

```bash
bazel run //rust/examples:echo_server -- \
  --group default --service /echo
```

Terminal D: start action server

```bash
bazel run //rust/examples:cli_execute_task_server -- \
  --group default
```

Terminal E: run CLI commands

```bash
bazel run //tools/aorta-cli:aorta -- <command...>
```

## 5.2 Cases

| ID | Case | Command | Expected |
|:---|:---|:---|:---|
| M-TOP-001 | Topic snapshot discovery | `... -- topic list --group default` | includes `/demo/chatter` |
| M-TOP-005 | Topic provider verbose list | `... -- topic list --group default --verbose` | includes provider endpoints and candidate node mapping |
| M-TOP-006 | Topic list by node filter | `... -- topic list --group default --node cli_talker` | only/primarily includes topics provided by the target node endpoint |
| M-TOP-002 | Topic stream | `... -- topic echo /demo/chatter --count 3 --group default` | prints 3 messages |
| M-TOP-003 | Topic rate | `... -- topic hz /demo/chatter --window 128 --group default` | prints ROS2-style rate stats every second until Ctrl+C |
| M-TOP-004 | Topic metadata | `... -- topic info /demo/chatter --group default` | shows key, schema hash, bfbs size |
| M-SRV-001 | Service snapshot discovery | `... -- service list --group default` | includes `/echo_schema` |
| M-SRV-005 | Service provider verbose list | `... -- service list --group default --verbose` | includes provider endpoints and candidate node mapping |
| M-SRV-006 | Service list by node filter | `... -- service list --group default --node cli_echo_server` | only/primarily includes services provided by the target node endpoint |
| M-SRV-004 | Service provider info | `... -- service info /echo_schema --group default` | shows provider endpoints and candidate provider nodes |
| M-SRV-002 | Service call success with req/res schema | `... -- service call /echo_schema '{"message":"hello cli"}' --group default --timeout 2` | prints decoded JSON response (contains `echoed_message`) |
| M-SRV-003 | Service call schema-missing failure | `... -- service call /echo '{"message":"hi"}' --group default --timeout 2` | fails with clear `No service schema found` message |
| M-ACT-001 | Action discovery | `... -- action list --group default` | includes `/example/flow_resume_probe` |
| M-ACT-002 | Action info | `... -- action info /example/flow_resume_probe --group default` | shows schema hash, node, TTLs, cancel support |
| M-ACT-003 | Action send default blocking | `... -- action send /example/flow_resume_probe --json '{"source":"quick","input_type":"ProbePreset","input":{"path":"demo/quick","args":[]}}' --group default` | send prints goal id/secret plus copy-pasteable follow-up commands, streams matching feedback, and ends in final `SUCCEEDED` result output |
| M-ACT-004 | Action feedback filter | `... -- action feedback /example/flow_resume_probe --goal-id <id> --group default` | prints only matching goal feedback until interrupted |
| M-ACT-005 | Action public status | `... -- action status /example/flow_resume_probe --goal-id <id> --group default` | shows lifecycle state and timestamps |
| M-ACT-006 | Action goals | `... -- action goals /example/flow_resume_probe --group default` | lists active goal ids with reply code/age |
| M-ACT-007 | Action result | `... -- action result /example/flow_resume_probe --goal-id <id> --goal-secret <secret> --group default` | prints private result JSON |
| M-ACT-008 | Action cancel | send `source=cancel` goal, then `... -- action cancel /example/flow_resume_probe --goal-id <id> --goal-secret <secret> --group default` and `wait` | cancel accepted and wait ends in `CANCELED` |
| M-ACT-009 | Action send non-blocking | `... -- action send /example/flow_resume_probe --json '{"source":"quick","input_type":"ProbePreset","input":{"path":"demo/quick","args":[]}}' --group default --non-blocking` | returns immediately after acceptance while still printing goal id/secret and follow-up commands |
| M-SCH-001 | Schema query topic | `... -- schema get /demo/chatter --group default` | prints topic hash + bfbs size |
| M-SCH-002 | Schema query service | `... -- schema get /echo_schema --group default` | prints req/res hashes + sizes |
| M-SCH-003 | Schema dump | `... -- schema get /echo_schema --group default --dump-bfbs /tmp/echo_schema` | writes `/tmp/echo_schema.req.bfbs` and `/tmp/echo_schema.res.bfbs` |
| M-NOD-001 | Nodes snapshot | `... -- nodes list --group default` | lists online node names and ids |
| M-NOD-005 | Nodes verbose snapshot | `... -- nodes list --group default --verbose` | includes endpoint ownership details when admin snapshot is available |
| M-NOD-006 | Nodes resource summary | `... -- nodes list --group default --with-resources` | includes candidate topic/service counts per node |
| M-NOD-002 | Nodes watch | `... -- nodes list --watch --group default` | ONLINE/OFFLINE events when examples start/stop |
| M-NOD-004 | Node provider inventory | `... -- nodes info cli_echo_server --group default` | lists candidate topics/services owned by the node endpoint |
| M-NOD-003 | Nodes fallback without admin | disable admin read, then `... -- nodes list --group default` | still lists nodes via liveliness fallback |
| M-CLN-001 | SHM cleanup safety | `... -- clean shm --stale-timeout 5` | scans and only removes stale `zenoh` files |

## 6. Evidence Capture

For each manual case, capture:

- command line used;
- exit code;
- first 20 relevant output lines;
- timestamp and router mode (`admin enabled` or `admin disabled`).

Store results in a run log (for example: `docs/test_runs/aorta_cli_YYYYMMDD.md`).

## 7. Known Limitations (Current Implementation)

- FFI conversion now uses FlatBuffers C++ parser/reflection path (`Deserialize + ParseJson + GenerateText`).
- Correct runtime behavior still depends on schema/payload compatibility of remote services.
- For Cargo builds, FlatBuffers C++ source discovery must succeed (via `AORTA_FLATBUFFERS_ROOT` or Bazel external cache path).

## 8. Execution Profiles

### 8.1 Quick Profile (PR Iteration)

Run when iterating quickly:

1. `cargo test --manifest-path tools/aorta-cli/Cargo.toml`
2. `bazel test //tools/aorta-cli:aorta_cli_test`
3. `bazel test //tools/aorta-cli:aorta_cli_smoke_test`
4. Manual spot-check: `M-NOD-003` (admin-disabled fallback)

### 8.2 Full Profile (Merge Gate)

Run before merge:

1. `cargo test --manifest-path tools/aorta-cli/Cargo.toml`
2. `bazel test //tools/aorta-cli:aorta_cli_test`
3. `bazel test //tools/aorta-cli:aorta_cli_regression_test`
4. `bazel test //...`
5. Manual fallback and router-misconfig cases: `M-NOD-003` + preflight checks in section 4

## 9. Exit Criteria

A CLI change is accepted when all are true:

1. `cargo test` for `aorta-cli` passes.
2. `bazel test //tools/aorta-cli:aorta_cli_test` passes.
3. All `M-*` cases pass in at least one full run.
4. Admin-disabled fallback case (`M-NOD-003`) passes.
5. No panic/crash in CLI during manual scenarios.
