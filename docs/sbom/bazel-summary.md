# Aorta — Bazel (Bzlmod) Module SBOM

Sources: `MODULE.bazel` (declared deps + overrides) and `MODULE.bazel.lock` (`registryFileHashes` `source.json` entries = BCR-resolved module closure). Machine-readable detail: [`bazel-modules.csv`](bazel-modules.csv).

- Direct `bazel_dep`: **9**; resolved closure incl. transitive modules: **64** (9 direct + 55 transitive)

## 1. Direct bazel_dep

| Module | Version | Override / patch | Source |
|---|---|---|---|
| aorta_schema_module_fixture | 0.0.0 | `local_path_override` | local_path_override: message/tools/tests/schema_module_fixture |
| flatbuffers | 25.12.19 | `archive_override` | archive_override: https://github.com/google/flatbuffers/archive/refs/tags/v25.12.19.tar.gz |
| hedron_compile_commands | 0.0.0-0e990032f3c5 | `git_override` | git_override: https://github.com/hedronvision/bazel-compile-commands-extractor @ 4f28899228fb3ad0126897876f147ca15026151e |
| platforms | 1.0.0 | — | Bazel Central Registry |
| rules_cc | 0.2.8 | — | Bazel Central Registry |
| rules_foreign_cc | 0.10.1 | — | Bazel Central Registry |
| rules_python | 1.0.0 | — | Bazel Central Registry |
| rules_rust | 0.68.1 | `single_version_override` **+ local patch** | single_version_override + patch //patches:rules_rust_linux_static_link.patch (patch_strip=1) |
| toolchains_llvm | 1.3.0 | — | Bazel Central Registry |

## 2. Closure methodology

The closure counts **64 modules** = **61 BCR-resolved** (lockfile `source.json` records) **+ 3 overridden direct modules** that the lockfile never records because their metadata and source are fetched from the override targets instead of the BCR:

| Module | Override | Source detail |
|---|---|---|
| aorta_schema_module_fixture | `local_path_override` | local_path_override: message/tools/tests/schema_module_fixture |
| flatbuffers | `archive_override` | archive_override: https://github.com/google/flatbuffers/archive/refs/tags/v25.12.19.tar.gz |
| hedron_compile_commands | `git_override` | git_override: https://github.com/hedronvision/bazel-compile-commands-extractor @ 4f28899228fb3ad0126897876f147ca15026151e |

Enumerating only the lockfile records would omit these 3. `MODULE.bazel`'s 9 public `bazel_dep` declarations are all present in the CSV with their override/patch metadata.

## 3. Notable resolved modules

- **flatbuffers 25.12.19** — `archive_override` (source replaced with the tagged GitHub archive `v25.12.19`); the Python runtime wheel is pinned to the same version in `pip.parse`.
- **rules_rust 0.68.1** — `single_version_override` **with local patch** `//patches:rules_rust_linux_static_link.patch` (patch_strip=1).
- **hedron_compile_commands 0.0.0-0e990032f3c5** — `git_override` at commit `4f28899228fb3ad0126897876f147ca15026151e`.
- **rules_python 1.0.0** — BCR; drives the `pip.parse` hub (flatbuffers wheel, py3-none-any).
- **Crate-universe** (`crate.from_cargo`) resolves `Cargo.bazel.lock`, regenerated from `Cargo.lock`. The `zenoh-config`/`zenoh-util` specs re-pin the Zenoh fork rev recorded in `[workspace.dependencies]` (see `rust-license-summary.md` §2.4).
- Toolchains via extension: Rust 1.92.0 (`rust.toolchain`), Python 3.10–3.13 (`python.toolchain`).

## 4. Lockfile provenance note

This inventory is derived from the COMMITTED `MODULE.bazel.lock`, not from a fresh resolution: it reports the closure that lockfile records. Historical observation (2026-08, not re-verified since): `bazel mod graph` failed to re-evaluate against the live BCR because of a `grpc-java 1.66.0` metadata bug — `grpc_repo_deps_ext` did not generate `com_envoyproxy_protoc_gen_validate` although `grpc-java/1.66.0` imports it. Re-check that before regenerating `MODULE.bazel.lock`; the lockfile itself was never affected.
