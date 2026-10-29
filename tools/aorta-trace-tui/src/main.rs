//! `aorta-trace-tui` — offline trace waterfall viewer.
//!
//! File-driven only. With no PATHS it scans the trace directory the writer uses
//! (`AORTA_TRACE_DIR`, default `<temp>/aorta-trace`); pass files or directories
//! to view a specific capture. No Zenoh, no live session, no trace control.

use std::path::PathBuf;

use clap::Parser;

/// Offline viewer for persisted Aorta trace (.atrc / .chunk) files.
#[derive(Parser, Debug)]
#[command(
    name = "aorta-trace-tui",
    about = "Offline waterfall viewer for persisted Aorta trace files (file-driven; no live session)."
)]
struct Args {
    /// Trace files or directories to view. Defaults to the trace directory the
    /// writer uses (AORTA_TRACE_DIR, else <temp>/aorta-trace).
    #[arg(value_name = "PATHS")]
    paths: Vec<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let inputs = if args.paths.is_empty() {
        aorta_trace_tui::load::default_inputs()
    } else {
        args.paths
    };
    aorta_trace_tui::run(&inputs)
}
