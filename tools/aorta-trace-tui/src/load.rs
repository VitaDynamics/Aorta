//! Discovery + decode of persisted trace files. Mirrors the writer's directory
//! resolution (so the viewer never drifts from where chunks are actually
//! written) and reuses the converter's reader + decode path verbatim.

use std::path::PathBuf;

use aorta_trace2perfetto::{collect_inputs, decode_trace_files, DecodedTrace};

/// The default input set: the SAME directory the trace writer persists to,
/// resolved from `AORTA_TRACE_DIR` exactly as `SinkConfig::from_env` does
/// (`aorta_core::trace::sink::default_trace_dir`). A viewer pointed here can
/// never look in the wrong place.
pub fn default_inputs() -> Vec<PathBuf> {
    vec![aorta_core::trace::sink::default_trace_dir()]
}

/// Result of a discovery+decode pass: the decoded trace plus the resolved file
/// list and the directories scanned (for the empty-state hint).
pub struct Loaded {
    pub trace: DecodedTrace,
    pub files: Vec<PathBuf>,
    pub inputs: Vec<PathBuf>,
}

/// Enumerate trace files under `inputs` (the SAME `collect_inputs` the
/// converter uses: non-recursive dir scan over `.atrc` / `.atrc.active` /
/// `.chunk.zst` / `.chunk`) and decode them through the shared
/// `decode_trace_files`. A missing directory yields an empty file list (not an
/// error) so the TUI can show its empty state rather than crash.
pub fn load(inputs: &[PathBuf]) -> anyhow::Result<Loaded> {
    let existing: Vec<PathBuf> = inputs.iter().filter(|p| p.exists()).cloned().collect();
    let files = if existing.is_empty() {
        Vec::new()
    } else {
        collect_inputs(&existing)?
    };
    let trace = if files.is_empty() {
        DecodedTrace::default()
    } else {
        decode_trace_files(&files)?
    };
    Ok(Loaded {
        trace,
        files,
        inputs: inputs.to_vec(),
    })
}

/// The message shown when no spans were found, naming the scanned input(s).
pub fn empty_hint(inputs: &[PathBuf]) -> String {
    let joined = inputs
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if joined.is_empty() {
        "no trace files found".to_string()
    } else {
        format!("no trace files found in: {joined}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aorta_core::trace::chunk::{build_chunk, ChunkEventType, ChunkProvenance, RingRecord};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn write_atrc(dir: &std::path::Path) {
        let recs = vec![
            RingRecord {
                trace_id: [1u8; 16],
                span_id: 1,
                mono_ns: 1_000,
                event_type: ChunkEventType::SliceBegin as u8,
                ..RingRecord::default()
            },
            RingRecord {
                trace_id: [1u8; 16],
                span_id: 1,
                mono_ns: 3_000,
                event_type: ChunkEventType::SliceEnd as u8,
                ..RingRecord::default()
            },
        ];
        let prov = ChunkProvenance {
            node_id: "n".into(),
            run_id: "r".into(),
            pid: 1,
            process_name: "proc".into(),
            host_clock_domain_id: 1,
            seq: 0,
            dropped: 0,
        };
        let bytes = build_chunk(
            &prov,
            1_000,
            0,
            &recs,
            |_| "s".into(),
            |_| "e".into(),
            |_| (100, "thread0".into()),
        );
        let z = zstd::stream::encode_all(&bytes[..], 1).unwrap();
        let mut file = Vec::new();
        file.extend_from_slice(b"ATCF");
        file.push(1);
        file.extend_from_slice(&[0u8; 3]);
        file.extend_from_slice(&(z.len() as u32).to_le_bytes());
        file.extend_from_slice(&z);
        std::fs::write(
            dir.join("n_proc_1_00000000000000000000_00000000.atrc"),
            file,
        )
        .unwrap();
    }

    #[test]
    fn load_missing_dir_is_empty_not_error() {
        let missing = std::env::temp_dir().join("aorta-trace-tui-does-not-exist-xyz");
        let loaded = load(&[missing]).unwrap();
        assert!(loaded.files.is_empty());
        assert!(loaded.trace.is_empty());
    }

    #[test]
    fn load_decodes_atrc_from_a_directory() {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "aorta-trace-tui-load-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        write_atrc(&dir);
        let loaded = load(std::slice::from_ref(&dir)).unwrap();
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.trace.spans.len(), 1);
        assert_eq!(loaded.trace.spans[0].duration_ns(), 2_000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_hint_names_the_directory() {
        let hint = empty_hint(&[PathBuf::from("/tmp/atrc")]);
        assert!(hint.contains("/tmp/atrc"));
    }
}
