// Declares the `loom` cfg so the `#[cfg(loom)]` code does not trip the
// `unexpected_cfgs` lint under `-D warnings` (the clippy hard gate). `--cfg
// loom` is set externally via RUSTFLAGS for the concurrency-proof build and is
// never set in normal builds. The Bazel target declares the same cfg via
// `rustc_flags` (sync/BUILD.bazel) because Bazel does not execute Cargo build
// scripts — mirroring core/build.rs's handling of `has_fresh_bfbs`.
fn main() {
    println!("cargo:rustc-check-cfg=cfg(loom)");
}
