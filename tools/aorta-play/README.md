# Aorta MCAP Player

`aorta-play` snapshots one or more MCAP files into private temporary storage,
validates the immutable snapshot, discovers the recorded schemas, and
replays the original payload bytes through Aorta. `aorta-cli play` is a thin
wrapper over the same engine.

## Usage

```bash
aorta-play [OPTIONS] <INPUT>...
aorta --group <GROUP> play [OPTIONS] <INPUT>...
```

The player supports:

- `-r, --rate <RATE>`, `-s, --start <SECS>`, `-d, --duration <SECS>`
- `--topics <T>...`, `--exclude <T>...`, and `-l, --loop`
- `--match-timeout <SECS>` and `--preflight-timeout <SECS>`
- `--schema-policy <strict|latest>` and `--allow-gaps`
- `--max-message-bytes`, `--max-chunk-bytes`, and `--buffer-bytes`
- `--group`, `--zenoh-config`, and `--log-level`

Every input is copied before replay. If a non-rotating recorder is still
writing an input, snapshot verification rejects it rather than replaying a
changing file. A missing or duplicate rotated segment is rejected unless
`--allow-gaps` is set.

Group precedence is explicit and narrow: `--group` wins; otherwise the player
uses the recorded MCAP metadata. Ambient `AORTA_GROUP` is not a source for
playback group selection. Simulation-clock publication and `Clock::Simulation`
are not part of this player release.

## Verification

```bash
cargo test -p aorta-play --lib -- --test-threads=1
bazel test //tools/aorta-play/...
bazel test //tools/aorta-play:aorta_play_test
bazel test //tools/aorta-play:aorta_play_e2e_test
```

The fault-injection unit variant is enabled explicitly with
`--features fault-injection`; production targets are built without those
hooks. The Bazel E2E records a live publisher, replays complete and
`--no-summary` files, verifies byte-identical message payloads through a
subscriber, checks the group precedence triple, rejects a live input during
snapshot, and checks the two-SIGINT forced-exit contract for both wrappers.

Live replay cancellation tests wait for published progress before cancelling;
startup delays are not cancellation latency. They retain outcome, publish-count
and teardown assertions and use the Bazel suite watchdog for hangs. To stress
the complete fault-injection suite, repeat it with `--runs_per_test=10` and
`--local_test_jobs=1`, then repeat with parallel test jobs. For a focused Rust
case, use `--test_arg=<test-name>` (the Rust test binary does not consume Bazel's
`--test_filter` here). The existing fault-only
`--test_env=AORTA_PLAY_TEST_SNAPSHOT_DELAY_MS=800` makes slow startup reproducible
when applied to a selected replay cancellation test.
