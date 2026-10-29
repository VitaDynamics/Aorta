# Public mirror CI

This action installs Bazelisk and the pinned Rust toolchain on GitHub-hosted runners. It does not read credentials, configure a remote cache, or access private modules.

Local tests cover workflow rendering, action pin policy, script command routing, schema inventory checks, and fork-safe secret absence. Native runner availability, cold-build duration, arm64 wheel production, and GitHub event behavior require runs on GitHub Actions.

The initial mirror intentionally excludes sanitizer, tsan-xlang, miri, loom, fuzz, benchmark, release, runner maintenance, and long-running interop workflows. Their private-runner coverage is not implied by the six public workflows.
