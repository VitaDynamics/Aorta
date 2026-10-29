use std::env;
use std::path::PathBuf;

fn main() {
    let mut crate_dir: Option<PathBuf> = None;
    let mut out_path: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--crate-dir" => {
                if let Some(value) = args.next() {
                    crate_dir = Some(PathBuf::from(value));
                }
            }
            "--out" => {
                if let Some(value) = args.next() {
                    out_path = Some(PathBuf::from(value));
                }
            }
            "--config" => {
                if let Some(value) = args.next() {
                    config_path = Some(PathBuf::from(value));
                }
            }
            _ => {}
        }
    }

    let crate_dir = crate_dir
        .or_else(|| env::var("CARGO_MANIFEST_DIR").ok().map(PathBuf::from))
        .unwrap_or_else(|| env::current_dir().expect("current dir"));

    let config_path = config_path.unwrap_or_else(|| crate_dir.join("cbindgen.toml"));
    let out_path = out_path.unwrap_or_else(|| crate_dir.join("include").join("aorta.h"));

    let config = cbindgen::Config::from_file(&config_path)
        .unwrap_or_else(|err| panic!("failed to load config {:?}: {}", config_path, err));

    // Parse the crate's own source tree directly instead of `with_crate`.
    //
    // `with_crate` makes cbindgen shell out to `cargo metadata`, and cbindgen
    // 0.29 dropped the `--no-deps` flag it used to pass (see the note at the
    // top of its `cargo/cargo_metadata.rs`): every invocation therefore does a
    // FULL dependency resolve, downloading and extracting the whole dependency
    // graph. In the Bazel genrule that runs against a throwaway `CARGO_HOME`,
    // so it re-downloads ~1.1 GB on every cache miss — ~3 min on a mirrored
    // office builder and ~40 min on a proxied cloud runner, for a header that
    // is derived from `core/src/**` alone.
    //
    // Nothing here needs the dependency graph: `cbindgen.toml` sets
    // `parse.parse_deps = false`, so cbindgen only ever parses this crate.
    // `with_src` on the crate root gives it exactly that — the parser follows
    // `mod` declarations from `lib.rs` through the whole tree.
    let lib_rs = crate_dir.join("src").join("lib.rs");
    let bindings = cbindgen::Builder::new()
        .with_config(config)
        .with_src(&lib_rs)
        .generate()
        .expect("failed to generate bindings");

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent).expect("create output directory");
    }
    bindings.write_to_file(&out_path);
}
