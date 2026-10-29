//! `aorta-trace2perfetto` — convert Aorta trace CHUNK files into a trace that
//! loads directly in `ui.perfetto.dev` (Chrome Trace Event JSON). Off-device by
//! design; merges every process/host under the given inputs.

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "aorta-trace2perfetto",
    about = "Convert Aorta trace CHUNK files into a ui.perfetto.dev trace (Chrome JSON)"
)]
struct Args {
    /// Chunk files (`*.chunk[.zst]`) and/or directories to scan (non-recursive).
    #[arg(required = true)]
    inputs: Vec<PathBuf>,

    /// Output path (default: stdout).
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Pretty-print the JSON.
    #[arg(long)]
    pretty: bool,

    /// Re-home the built-in `aorta.stage` lifecycle slices onto synthetic
    /// per-primitive stage lanes ("<name> [stages]") instead of their real
    /// OS threads — identical requests render identically regardless of
    /// async worker placement. The original thread is kept in `args.thread`.
    #[arg(long)]
    stage_lanes: bool,

    /// Emit ABSOLUTE wall-clock timestamps so Perfetto shows the real calendar
    /// time of each slice (e.g. `2026-07-04 08:36:…` UTC) instead of a
    /// `1970-01-01`-relative offset. Slice durations stay exact; absolute
    /// positions quantize to ~250 ns in f64 µs at 2026 magnitudes.
    #[arg(long)]
    wall_clock: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let files = aorta_trace2perfetto::collect_inputs(&args.inputs)?;
    if files.is_empty() {
        anyhow::bail!("no .atrc / .chunk.zst / .chunk files found under the given inputs");
    }
    let trace = aorta_trace2perfetto::convert_files_with(
        &files,
        aorta_trace2perfetto::ConvertOptions {
            stage_lanes: args.stage_lanes,
            wall_clock: args.wall_clock,
        },
    )?;
    let json = if args.pretty {
        trace.to_json_pretty()?
    } else {
        trace.to_json()?
    };
    match args.output {
        Some(path) => {
            std::fs::write(&path, &json)?;
            eprintln!(
                "wrote {} events from {} file(s) -> {}",
                trace.len(),
                files.len(),
                path.display()
            );
        }
        None => println!("{json}"),
    }
    Ok(())
}
