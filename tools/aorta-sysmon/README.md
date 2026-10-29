# aorta-sysmon

Low-overhead Linux host load monitor.

## Features

### Aorta SDK publish (`daemon`)

`daemon` subcommand builds an `aorta_sdk::Node`, registers the
`SysmonSnapshot` schema via `schema_by_key`, and publishes one typed
snapshot per `interval_ms` tick (Tokio interval with
`MissedTickBehavior::Skip`). Schema lives at
`message/schemas/system/sysmon_snapshot.fbs`; the codegen wrapper crate
is `//message/bindings/rust:sysmon_snapshot_msg_rs`.

Build / test under Bazel (the codegen wrapper crate is only resolvable
via Bazel — cargo cannot build the daemon path today; see "Cargo cross-build"
below):

```bash
bazel build //tools/aorta-sysmon:aorta-sysmon
bazel test  //tools/aorta-sysmon:aorta_sysmon_test
```

### Snapshot CLI (`snapshot`)

Implements the `snapshot` subcommand: takes two `/proc` samples spaced by
`--interval-ms`, computes deltas, and prints JSON.

- Host families: CPU, memory, disk, network, PSI, process, and hardware
  perf counters.
- Process selection: sysmon self-PID + `--include-pid`.
- Perf counters are collected on Linux through `perf_event_open` (the
  `perf-event2` crate, a Linux-only dependency in `Cargo.toml`, `BUILD.bazel`
  and `MODULE.bazel`). Each counter reports `Active` when it was read and ran
  for part of the interval, `Degraded` when it never ran since the last read
  (`time_running == 0`) or the read failed, and `Unavailable` when it could not
  be opened. A kernel-multiplexed counter (running for only part of the time
  it was enabled) stays `Active` and notes the multiplexing in
  `degraded_reason`. Whether a counter opens depends on kernel support and
  access: `kernel.perf_event_paranoid`, the process's privileges, and whether a
  container or seccomp profile allows the syscall. The `degraded_reason` names
  the cause. Non-Linux builds always report perf as `Unavailable`.
- PSI reports `Unavailable` with a `degraded_reason` when the kernel does not
  provide `/proc/pressure`.
- Zenoh-topology collection is planned and not implemented; it is always
  reported as `Unavailable`. In the default `off` mode the `degraded_reason` is
  `"configured off"`; in `auto` or `on` mode it is `"not implemented"`.

The `/proc` families hand-parse `/proc/*` and `/proc/pressure/*` files
directly, using only crates already vendored by `aorta_core`'s `Cargo.toml`.

## Build & test

```bash
# Cargo (a member of the root Cargo workspace):
cargo build   --manifest-path tools/aorta-sysmon/Cargo.toml --release
cargo test    --manifest-path tools/aorta-sysmon/Cargo.toml
cargo clippy  --manifest-path tools/aorta-sysmon/Cargo.toml --all-targets -- -D warnings
cargo fmt     --manifest-path tools/aorta-sysmon/Cargo.toml --check

# Bazel:
bazel build //tools/aorta-sysmon:aorta-sysmon
bazel test  //tools/aorta-sysmon:aorta_sysmon_test
```

## Run

```bash
# Live host:
aorta-sysmon snapshot --pretty

# Fixture (debug only):
aorta-sysmon snapshot --procfs-root tools/aorta-sysmon/testdata/procfs/x86_linux --pretty
```

`--pretty` JSON is a debug view, **not a wire format**. The `daemon`
typed FlatBuffers publication is the stable contract.

## Cargo cross-build (current limitation)

The `daemon` path depends on `sysmon_snapshot_msg`, a codegen
wrapper crate emitted by Bazel at
`//message/bindings/rust:sysmon_snapshot_msg_rs`. Cargo has no path to
this crate today, so:

- `cargo build` / `cargo test` work with the `sdk-publish` feature OFF
  (default in `Cargo.toml`). The snapshot pipeline still gets
  exercised; `daemon` is compiled out.
- Producing an aarch64 binary for on-robot smoke testing currently
  requires Bazel under a Linux host (or Bazel's cross-toolchain). The
  Docker `rust:1-bookworm` arm64 path that works for `snapshot` does not
  work for `daemon` because cargo cannot resolve the codegen wrapper.

If/when we move sysmon publish onto cargo too, the path is to add a
`build.rs` that invokes flatc + `gen_message_wrapper.py`, vendoring the
codegen output into `OUT_DIR`. That work is intentionally deferred.
