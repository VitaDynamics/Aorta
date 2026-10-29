use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let flatbuffers_root = find_flatbuffers_root().unwrap_or_else(|| {
        panic!(
            "Failed to locate FlatBuffers source tree for C++ FFI build. \
Set AORTA_FLATBUFFERS_ROOT to a flatbuffers checkout root containing include/flatbuffers/idl.h \
or run Bazel once so /private/var/tmp/_bazel_$USER/.../external/flatbuffers exists."
        )
    });

    let flatbuffers_include = flatbuffers_root.join("include");
    let flatbuffers_src = flatbuffers_root.join("src");

    cc::Build::new()
        .cpp(true)
        .file("ffi/fb_reflect.cc")
        .file("ffi/json_to_fb.cc")
        .file(flatbuffers_src.join("code_generators.cpp"))
        .file(flatbuffers_src.join("idl_parser.cpp"))
        .file(flatbuffers_src.join("idl_gen_text.cpp"))
        .file(flatbuffers_src.join("reflection.cpp"))
        .file(flatbuffers_src.join("util.cpp"))
        .include("ffi")
        .include(&flatbuffers_include)
        .flag_if_supported("-std=c++17")
        .define("FLATBUFFERS_LOCALE_INDEPENDENT", Some("1"))
        .compile("aorta_introspect_ffi");

    println!("cargo:rerun-if-changed=ffi/fb_reflect.cc");
    println!("cargo:rerun-if-changed=ffi/fb_reflect.h");
    println!("cargo:rerun-if-changed=ffi/json_to_fb.cc");
    println!("cargo:rerun-if-changed=ffi/json_to_fb.h");
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_src.join("code_generators.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_src.join("idl_parser.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_src.join("idl_gen_text.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_src.join("reflection.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_src.join("util.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        flatbuffers_include.join("flatbuffers/idl.h").display()
    );
}

fn find_flatbuffers_root() -> Option<PathBuf> {
    if let Ok(path) = env::var("AORTA_FLATBUFFERS_ROOT") {
        let root = PathBuf::from(path);
        if has_flatbuffers_layout(&root) {
            return Some(root);
        }
    }

    let user = env::var("USER").ok()?;
    let bases = [
        format!("/private/var/tmp/_bazel_{}", user),
        format!("/tmp/_bazel_{}", user),
    ];
    for base in bases {
        let base_path = Path::new(&base);
        let Ok(entries) = fs::read_dir(base_path) else {
            continue;
        };
        for entry in entries.flatten() {
            // Bzlmod names the repo dir with a canonical-name suffix
            // (`external/flatbuffers~`, and `~<version>` variants in older
            // Bazel releases), so probe prefix-matched entries too — the
            // bare `external/flatbuffers` only exists under WORKSPACE builds
            // (otherwise "run Bazel once" never actually satisfied this probe
            // under Bzlmod).
            let external = entry.path().join("external");
            let candidate = external.join("flatbuffers");
            if has_flatbuffers_layout(&candidate) {
                return Some(candidate);
            }
            let Ok(repos) = fs::read_dir(&external) else {
                continue;
            };
            for repo in repos.flatten() {
                let name = repo.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("flatbuffers~") && has_flatbuffers_layout(&repo.path()) {
                    return Some(repo.path());
                }
            }
        }
    }

    None
}

fn has_flatbuffers_layout(root: &Path) -> bool {
    root.join("include/flatbuffers/idl.h").is_file()
        && root.join("src/idl_parser.cpp").is_file()
        && root.join("src/idl_gen_text.cpp").is_file()
}
