# aorta-record

Aorta MCAP recorder library and CLI.

This crate provides the programmatic entry point `aorta_record::run` and the
`aorta-record` binary for recording Aorta traffic to MCAP files.

The public API surface is intentionally small:

- `aorta_record::run(config) -> Result<RunSummary>`
- `aorta_record::run_with_segment_callback(config, callback) -> Result<RunSummary>`
- `aorta_record::config::RecorderConfig` and related configuration types
- `aorta_record::{RunSummary, SegmentFinalized}`

The segment callback runs synchronously on the writer thread after a completed
`.part` file is atomically promoted to its final path. Keep it short: persist or
enqueue the event, then upload asynchronously. A callback consumer owns file
retention, so `rotation.max_files` and `rotation.max_total_size_mb` must remain
unset. Filename templates support `{index}` as a zero-padded segment index
(`000`, `001`, ...).
