use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let message_dir = manifest_dir.parent().expect("message dir").join("message");
    let base_schema = message_dir.join("schemas/system/aorta_base.fbs");
    let log_schema = message_dir.join("schemas/topic/foxglove_schema/Log.fbs");
    let foxglove_time_schema = message_dir.join("schemas/topic/foxglove_schema/Time.fbs");
    let trace_chunk_schema = message_dir.join("schemas/system/trace_chunk.fbs");
    let demo_schema = message_dir.join("schemas/topic/example/demo_chat.fbs");
    let perf_schema = message_dir.join("schemas/topic/example/perf_payload.fbs");
    let tni_schema = message_dir.join("schemas/system/telemetry_node_identity.fbs");
    let tri_schema = message_dir.join("schemas/system/telemetry_resource_inventory.fbs");
    let trm_schema = message_dir.join("schemas/system/telemetry_runtime_metrics.fbs");
    let tvs_schema = message_dir.join("schemas/system/telemetry_vital_snapshot.fbs");
    let tcommon_schema = message_dir.join("schemas/system/telemetry_common.fbs");
    let contextstore_schema = message_dir.join("schemas/system/contextstore.fbs");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let flatc = resolve_flatc(&manifest_dir);

    println!("cargo:rerun-if-env-changed=FLATC");
    println!("cargo:rerun-if-changed={}", log_schema.display());
    // Log.fbs `include`s Time.fbs (and aorta_base.fbs, listed below).
    println!("cargo:rerun-if-changed={}", foxglove_time_schema.display());
    println!("cargo:rerun-if-changed={}", trace_chunk_schema.display());
    println!("cargo:rerun-if-changed={}", demo_schema.display());
    println!("cargo:rerun-if-changed={}", perf_schema.display());
    println!("cargo:rerun-if-changed={}", base_schema.display());
    println!("cargo:rerun-if-changed={}", tni_schema.display());
    println!("cargo:rerun-if-changed={}", tri_schema.display());
    println!("cargo:rerun-if-changed={}", trm_schema.display());
    println!("cargo:rerun-if-changed={}", tvs_schema.display());
    // node_identity/resource_inventory/vital all `include` the shared KeyValue.
    println!("cargo:rerun-if-changed={}", tcommon_schema.display());
    println!("cargo:rerun-if-changed={}", contextstore_schema.display());

    // Signal that build.rs ran and wrote Log_bfbs_fresh.rs to OUT_DIR.
    // Bazel does not execute Cargo build scripts, so the drift test that
    // include!s that file must be gated on this cfg.
    println!("cargo:rustc-check-cfg=cfg(has_fresh_bfbs)");
    println!("cargo:rustc-cfg=has_fresh_bfbs");

    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg("-I")
        .arg(&message_dir)
        .arg(&base_schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for aorta_base: {}. Set FLATC or build //message:flatc",
                err
            )
        });

    if !status.success() {
        panic!("flatc aorta_base failed with status {}", status);
    }

    // `foxglove.Log` is the wire type of the `/log` forwarder. Only the BFBS
    // is regenerated here (for the drift test against the committed
    // `FOXGLOVE_LOG_BFBS`); the Rust binding in
    // `core/src/foxglove_log_generated.rs` is committed and authoritative,
    // since Log.fbs pulls in Time.fbs + aorta_base.fbs and therefore needs
    // `flatc --rust --gen-all` to produce a single self-contained module.
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &log_schema,
        &out_dir,
        "Log",
        "FOXGLOVE_LOG_BFBS_FRESH",
    );
    // NOTE: `trace_chunk.fbs` is intentionally NOT regenerated here. Its committed
    // binding `core/src/trace/chunk_generated.rs` is authoritative (and rustfmt'd,
    // so a byte/whitespace comparison against raw flatc output is meaningless).
    // Drift is guarded instead by version-insensitive tests (fixed 72-byte Record
    // layout + a build→verify→read-back roundtrip in trace_chunk_tests.rs) plus
    // the regenerate-with-//message:flatc instruction in the .fbs header. Bazel
    // never runs this script anyway; the committed binding is what compiles.

    // Telemetry V2 families: freshly compile each BFBS so the drift tests in
    // telemetry/fb.rs can verify the committed *_BFBS constants stay in sync
    // with their .fbs sources.
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &tni_schema,
        &out_dir,
        "telemetry_node_identity",
        "TELEMETRY_NODE_IDENTITY_BFBS_FRESH",
    );
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &tri_schema,
        &out_dir,
        "telemetry_resource_inventory",
        "TELEMETRY_RESOURCE_INVENTORY_BFBS_FRESH",
    );
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &trm_schema,
        &out_dir,
        "telemetry_runtime_metrics",
        "TELEMETRY_RUNTIME_METRICS_BFBS_FRESH",
    );
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &tvs_schema,
        &out_dir,
        "telemetry_vital_snapshot",
        "TELEMETRY_VITAL_SNAPSHOT_BFBS_FRESH",
    );
    // ContextValue: the single static schema shared by all ctx values; the
    // recorder embeds the committed `CONTEXT_VALUE_BFBS` so MCAP readers parse
    // typed ctx channels. Drift-tested in contextstore_codec_tests.rs.
    emit_fresh_bfbs(
        &flatc,
        &message_dir,
        &contextstore_schema,
        &out_dir,
        "contextstore",
        "CONTEXT_VALUE_BFBS_FRESH",
    );

    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg("-I")
        .arg(&message_dir)
        .arg(&demo_schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for demo: {}. Set FLATC or build //message:flatc",
                err
            )
        });

    if !status.success() {
        panic!("flatc demo failed with status {}", status);
    }

    let status = Command::new(&flatc)
        .arg("--binary")
        .arg("--schema")
        .arg("--bfbs-builtins")
        .arg("-o")
        .arg(&out_dir)
        .arg("-I")
        .arg(&message_dir)
        .arg(&demo_schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for demo bfbs: {}. Set FLATC or build //message:flatc",
                err
            )
        });

    if !status.success() {
        panic!("flatc demo bfbs failed with status {}", status);
    }

    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg("-I")
        .arg(&message_dir)
        .arg(&perf_schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for perf: {}. Set FLATC or build //message:flatc",
                err
            )
        });

    if !status.success() {
        panic!("flatc perf failed with status {}", status);
    }

    let status = Command::new(&flatc)
        .arg("--binary")
        .arg("--schema")
        .arg("--bfbs-builtins")
        .arg("-o")
        .arg(&out_dir)
        .arg("-I")
        .arg(&message_dir)
        .arg(&perf_schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for perf bfbs: {}. Set FLATC or build //message:flatc",
                err
            )
        });

    if !status.success() {
        panic!("flatc perf bfbs failed with status {}", status);
    }
}

/// Compile `schema` to a fresh BFBS and emit `<stem>_bfbs_fresh.rs` in `out_dir`
/// defining `const_name` as the freshly-compiled bytes. Used by the telemetry
/// and `foxglove.Log` drift tests to detect a committed `*_BFBS` constant that
/// has gone stale.
///
/// `--bfbs-builtins` preserves built-in attributes (bit_flags, key, required,
/// explicit id, ...) in the bfbs so reflection consumers can see them after
/// `Parser::Deserialize`. Kept in lockstep with the Bazel pipeline
/// (`message/tools/aorta_flatbuffers.bzl`, `schema_codegen.bzl`) — a mismatch
/// here would make the drift tests fail for a spurious reason.
fn emit_fresh_bfbs(
    flatc: &str,
    message_dir: &Path,
    schema: &Path,
    out_dir: &Path,
    stem: &str,
    const_name: &str,
) {
    let status = Command::new(flatc)
        .arg("--binary")
        .arg("--schema")
        .arg("--bfbs-builtins")
        .arg("-o")
        .arg(out_dir)
        .arg("-I")
        .arg(message_dir)
        .arg(schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for {} bfbs: {}. Set FLATC or build //message:flatc",
                stem, err
            )
        });
    if !status.success() {
        panic!("flatc {} bfbs failed with status {}", stem, status);
    }
    let bfbs = std::fs::read(out_dir.join(format!("{}.bfbs", stem)))
        .unwrap_or_else(|err| panic!("failed to read generated {}.bfbs: {}", stem, err));
    let literals: String = bfbs
        .iter()
        .map(|b| format!("0x{:02x}", b))
        .collect::<Vec<_>>()
        .join(", ");
    let generated = format!(
        "// @generated — do not edit; produced by build.rs from {}.fbs\n\
         pub const {}: &[u8] = &[{}];\n",
        stem, const_name, literals
    );
    std::fs::write(out_dir.join(format!("{}_bfbs_fresh.rs", stem)), generated)
        .unwrap_or_else(|err| panic!("failed to write {}_bfbs_fresh.rs: {}", stem, err));
}

fn resolve_flatc(manifest_dir: &Path) -> String {
    if let Ok(path) = env::var("FLATC") {
        return path;
    }

    if let Some(root) = find_workspace_root(manifest_dir) {
        let candidates = [
            root.join("bazel-bin/message/flatc"),
            root.join("bazel-bin/message/flatc.exe"),
            root.join("bazel-bin/external/flatbuffers/flatc"),
            root.join("bazel-bin/external/flatbuffers/flatc.exe"),
        ];

        for candidate in candidates {
            if candidate.is_file() {
                return candidate.to_string_lossy().into_owned();
            }
        }

        // Bzlmod canonicalizes the repo name (e.g. `flatbuffers~`, `flatbuffers+`),
        // so the fixed candidates above miss it. Scanning for `flatbuffers*` finds
        // the version-matched flatc — required because BFBS/Rust output is NOT
        // stable across flatc versions, so the committed bindings + drift tests
        // must use the same flatc the `flatbuffers` crate is pinned to.
        for base in [root.join("bazel-bin/external"), root.join("bazel-out")] {
            if let Some(candidate) = find_flatc_under(&base) {
                return candidate.to_string_lossy().into_owned();
            }
        }

        if let Some(candidate) = find_flatc_in_bazel_out(&root) {
            return candidate.to_string_lossy().into_owned();
        }
    }

    "flatc".to_string()
}

fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if current.join("WORKSPACE").exists() || current.join("MODULE.bazel").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

/// Find a `flatc` under any `flatbuffers*` external-repo dir directly inside
/// `base` (e.g. `bazel-bin/external/flatbuffers~/flatc`). One level deep for
/// `bazel-bin/external`; the deeper `bazel-out/*/bin/external/...` layout is
/// handled by [`find_flatc_in_bazel_out`].
fn find_flatc_under(base: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(base).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("flatbuffers") {
            continue;
        }
        for exe in ["flatc", "flatc.exe"] {
            let candidate = entry.path().join(exe);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn find_flatc_in_bazel_out(root: &Path) -> Option<PathBuf> {
    let bazel_out = root.join("bazel-out");
    let entries = std::fs::read_dir(&bazel_out).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let candidate = path.join("bin/external/flatbuffers/flatc");
        if candidate.is_file() {
            return Some(candidate);
        }
        let candidate = path.join("bin/external/flatbuffers/flatc.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}
