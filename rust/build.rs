use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn compile_schema(
    flatc: &str,
    out_dir: &PathBuf,
    message_dir: &PathBuf,
    schema: &PathBuf,
    name: &str,
) {
    println!("cargo:rerun-if-changed={}", schema.display());

    // Compile to Rust
    let status = Command::new(flatc)
        .arg("--rust")
        .arg("-o")
        .arg(out_dir)
        .arg("-I")
        .arg(message_dir)
        .arg(schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for {} rust: {}. Set FLATC or build //message:flatc",
                name, err
            )
        });

    if !status.success() {
        panic!("flatc {} rust failed with status {}", name, status);
    }

    // Compile to binary schema (.bfbs)
    let status = Command::new(flatc)
        .arg("--binary")
        .arg("--schema")
        .arg("-o")
        .arg(out_dir)
        .arg("-I")
        .arg(message_dir)
        .arg(schema)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke flatc for {} bfbs: {}. Set FLATC or build //message:flatc",
                name, err
            )
        });

    if !status.success() {
        panic!("flatc {} bfbs failed with status {}", name, status);
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let message_dir = manifest_dir.parent().expect("message dir").join("message");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    println!("cargo:rerun-if-env-changed=FLATC");
    let flatc = resolve_flatc(&manifest_dir);

    // Base schema (no bfbs needed, it's included by others)
    let base_schema = message_dir.join("schemas/system/aorta_base.fbs");
    println!("cargo:rerun-if-changed={}", base_schema.display());

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

    // Demo chat schema
    compile_schema(
        &flatc,
        &out_dir,
        &message_dir,
        &message_dir.join("schemas/topic/example/demo_chat.fbs"),
        "demo_chat",
    );

    // Performance payload schema
    compile_schema(
        &flatc,
        &out_dir,
        &message_dir,
        &message_dir.join("schemas/topic/example/perf_payload.fbs"),
        "perf_payload",
    );

    // Examples schemas
    compile_schema(
        &flatc,
        &out_dir,
        &message_dir,
        &message_dir.join("schemas/topic/example/examples_sensor.fbs"),
        "examples_sensor",
    );

    compile_schema(
        &flatc,
        &out_dir,
        &message_dir,
        &message_dir.join("schemas/topic/example/examples_control.fbs"),
        "examples_control",
    );

    compile_schema(
        &flatc,
        &out_dir,
        &message_dir,
        &message_dir.join("schemas/topic/example/examples_perf.fbs"),
        "examples_perf",
    );
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
