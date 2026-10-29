# Rust LSP Support for Aorta (Bazel + rust-analyzer)

This repository builds Rust code with Bazel (`rules_rust`), not Cargo-only crate graphs.
For accurate IDE diagnostics (including generated message crates like `echo_msg` from Bazel target `echo_msg_rs`), use
`rust-analyzer` with Bazel project metadata.

## Why This Is Needed

Without Bazel metadata, rust-analyzer may show false diagnostics such as:

- `failed to resolve: use of unresolved module or unlinked crate 'echo_msg'`
- `unlinked-file` for files like `cli_echo_server.rs`

These are IDE indexing issues, not Bazel build issues.

## Option A (Recommended): Generate `rust-project.json`

Run:

```bash
bazel run //:refresh_rust_project
```

This generates `rust-project.json` at the repository root (already gitignored).
rust-analyzer will pick it up after reload/restart.

When Rust/Bazel dependencies change, regenerate it with the same command.

`//:refresh_rust_project` defaults to the Aorta subtree:

- `//...`

You can override targets when needed:

```bash
bazel run //:refresh_rust_project -- //rust/... //tools/aorta-cli/...
```

## Option B (VSCode Auto-discovery, No File Regeneration)

Use Bazel auto-discovery by adding this to your local VSCode settings:

```json
{
  "rust-analyzer.workspace.discoverConfig": {
    "command": [
      "tools/rust/discover_bazel_rust_project.sh",
      "{arg}"
    ],
    "progressLabel": "rust_analyzer",
    "filesToWatch": [
      "BUILD",
      "BUILD.bazel",
      "MODULE.bazel",
      "WORKSPACE"
    ]
  }
}
```

The helper script is:

- `tools/rust/discover_bazel_rust_project.sh`

## Notes

- If you still see stale diagnostics, reload the editor window.
- Disabling `unlinked-file` diagnostics should be treated as a last resort, not the default fix.
