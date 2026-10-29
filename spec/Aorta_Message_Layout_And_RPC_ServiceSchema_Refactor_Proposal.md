# Aorta Message Layout And RPC Service-Schema Refactor Proposal

Last updated: 2026-02-19

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

## 1. Context

The current `message` layout is already mixing multiple concerns in one flat directory:

- FlatBuffers source schemas (`*.fbs`)
- Rust wrappers (previously hand-written as `*_msg.rs`, now auto-generated from `*_msg_rs` targets that produce `*_msg` crates)
- codegen scripts/macros (`gen_schema_meta.py`, `*.bzl`)
- a large monolithic `BUILD.bazel` with duplicated `genrule` patterns

In parallel, RPC service schema metadata is modeled as request/response dual BFBS (`req_*` + `res_*`) across Core/FFI/SDK/CLI. This creates repeated logic and scales poorly as service definitions grow.

## 2. Decisions Already Confirmed

These are fixed for this refactor:

1. No `v1/v2` style version directories or filenames.
2. Root `message/BUILD.bazel` should become aggregation-only; generation rules move to subdirectories.
3. Existing example schemas must be moved under `topic/example` and `service/example`.
4. Migration is one-shot (no compatibility path required for this stage).
5. Remove hand-maintained Rust wrapper files in `message/` (Rust wrappers are generated targets, for example `demo_chat_msg_rs`, `echo_msg_rs`, with crate names `demo_chat_msg`, `echo_msg`).
6. RPC protocol is allowed to change.

## 3. Target Outcomes

### 3.1 Directory architecture

```text
message/
├── BUILD.bazel                       # aggregation/exports only
├── README.md
├── tools/
│   ├── BUILD.bazel
│   ├── aorta_flatbuffers.bzl
│   ├── flatbuffers_rules.bzl
│   └── gen_schema_meta.py
├── schemas/
│   ├── BUILD.bazel                   # aggregate filegroups only
│   ├── system/
│   │   ├── BUILD.bazel
│   │   ├── aorta_base.fbs
│   │   └── schema_by_key.fbs
│   ├── shared/
│   │   └── service_common/
│   │       ├── BUILD.bazel
│   │       └── status.fbs
│   ├── topic/
│   │   └── example/
│   │       ├── BUILD.bazel
│   │       ├── demo_chat.fbs
│   │       ├── perf_payload.fbs
│   │       ├── examples_sensor.fbs
│   │       ├── examples_control.fbs
│   │       └── examples_perf.fbs
│   └── service/
│       └── example/
│           ├── BUILD.bazel
│           ├── echo.fbs
│           └── navigation.fbs
└── bindings/
    └── rust/
        ├── BUILD.bazel
        └── (optional generated wrapper artifacts only; no manual wrappers)
```

### 3.2 Include convention

All schema includes should be rooted from `schemas/` and avoid `../`.

Examples:

- `include "schemas/system/aorta_base.fbs";`
- `include "schemas/shared/service_common/status.fbs";`

## 4. RPC Protocol Redesign (Single Service Schema)

## 4.1 Current model (to be replaced)

Service metadata currently depends on:

- `req_hash_hex`, `req_bfbs`
- `res_hash_hex`, `res_bfbs`

This exists in:

- `message/schema_by_key.fbs`
- `core/src/schema_by_key_fb.rs`
- `core/src/schema_registry.rs`
- Core/FFI/SDK/C++ options and tests
- CLI schema/service commands

## 4.2 New model

For `SchemaKind::Service`, use **single service schema BFBS** plus root type names:

- `service_hash_hex`
- `service_bfbs`
- `request_root_type`
- `response_root_type`

Proposed `SchemaByKey` service payload semantics:

- `service_hash_hex`: SHA-256 of `service_bfbs`
- `service_bfbs`: one BFBS generated from one `.fbs` file containing both request and response tables
- `request_root_type`: fully qualified request table name
- `response_root_type`: fully qualified response table name

Topic/Param use `hash_hex + bfbs` and may carry an optional `topic_root_type`
when the published table differs from the BFBS-declared root table. Legacy
publishers omit the field.

## 4.3 Attachment policy with new model

- Service response attachment: `service_hash_hex`
- Client request attachment expectation: `service_hash_hex`
- `schema_attach=always` requires service schema hash availability

This removes req/res hash mismatch branches and keeps one service-level identity.

## 5. Required Code Changes By Layer

## 5.1 Message build and schema generation

- Split `message/BUILD.bazel` into per-subdir BUILDs.
- Remove duplicated manual `genrule` blocks.
- Rework `message/tools/aorta_flatbuffers.bzl` to support nested schema paths and explicit include roots.
- Delete deprecated `message/BUILD.bazel.new`.
- Remove `sed`-based `echo_response_bfbs` generation flow.

Primary files:

- `message/BUILD.bazel`
- `message/BUILD.bazel.new`
- `message/aorta_flatbuffers.bzl`
- `message/flatbuffers_rules.bzl`
- `message/gen_schema_meta.py`

## 5.2 Core (Rust)

### Service/client schema types

- Replace request/response pair in `ServiceSchemaInfo` with single service schema model.
- Simplify client schema expectations from request/response hashes to service hash.

Primary files:

- `core/src/service.rs`
- `core/src/client.rs`
- `core/src/schema_registry.rs`
- `core/src/schema_by_key_fb.rs`
- `core/src/lib.rs`

### Build-time schema path updates

- Update hardcoded message schema paths for Cargo builds.

Primary files:

- `core/build.rs`
- `rust/build.rs`

## 5.3 C ABI (FFI)

- Change `aorta_service_options_t` from req/res hashes+bfbs to single service schema fields.
- Change `aorta_client_options_t` from req/res hash fields to single service schema hash field.
- Update argument validation logic in FFI converters.

Primary file:

- `core/src/ffi/c_api.rs`

## 5.4 Rust SDK

- Update typed service creation path to inject a single service schema identity.
- Update `ClientOptions` and `ServiceOptions` schema fields.

Primary files:

- `rust/src/node.rs`
- `rust/src/client.rs`
- `rust/src/service.rs`
- `rust/src/lib.rs`

## 5.5 C++ SDK

- Replace request/response schema fields in `ServiceOptions` and `ClientOptions`.
- Update typed service helper behavior and tests.

Primary files:

- `cpp/include/aorta/options.h`
- `cpp/include/aorta/node.h`
- `cpp/src/node.cc`
- `cpp/test/options_test.cc`
- `cpp/test/node_test.cc`
- `cpp/test/mock/mock_aorta.h`
- `cpp/test/mock/mock_aorta.cc`

## 5.6 CLI and tools

- Update service schema decode/validation from req/res dual BFBS to single service BFBS.
- Adjust output and dump semantics accordingly.

Primary files:

- `tools/aorta-cli/src/cmd/schema.rs`
- `tools/aorta-cli/src/cmd/service.rs`

## 5.7 Examples and scripts

- Update Rust/C++ echo examples CLI options (`--service-schema-hash` semantics).
- Update navigation examples that embed old req/res BFBS filenames.
- Update cross-process RPC scripts that currently pass request/response hashes separately.

Primary files:

- `rust/examples/echo_server.rs`
- `rust/examples/echo_client.rs`
- `rust/examples/cli_echo_server.rs`
- `rust/examples/navigation_service_server.rs`
- `rust/examples/navigation_service_client.rs`
- `cpp/examples/echo_server.cc`
- `cpp/examples/echo_client.cc`
- `test_scripts/test_rust_rpc_client_cpp_rpc_server.sh`
- `test_scripts/test_cpp_rpc_client_rust_rpc_server.sh`
- `test_scripts/test_rpc_matrix.sh`
- `test_scripts/test_rpc_strict_matrix.sh`

## 5.8 Tests to update

Core/Rust/C++ tests with direct req/res semantics must be rewritten:

- `core/test/schema_registry_tests.rs`
- `core/test/service_client_integration_tests.rs`
- `core/test/service_tests.rs`
- `core/test/client_tests.rs`
- `core/test/ffi_service_tests.rs`
- `core/test/ffi_client_test.c`
- `rust/test/rpc_api_tests.rs`
- `cpp/test/options_test.cc`
- `cpp/test/node_test.cc`

## 5.9 Documentation to update

At minimum:

- `spec/Aorta_Service_Schema_Design_Guide.md`
- `cpp/README.md`
- the architecture, CLI-tool, and example/test documentation

## 6. Migration Plan (One-shot)

1. Create new `message/schemas`, `message/tools`, `message/bindings` structure and subdir BUILD files.
2. Move `.fbs` files to new paths (including `topic/example` and `service/example`).
3. Refactor build macros and root `message/BUILD.bazel` to aggregation-only.
4. Switch RPC schema model to single service schema in `schema_by_key.fbs` and Core registry.
5. Update Core client/service attachment and validation behavior.
6. Update FFI structs and conversion logic.
7. Update Rust SDK and C++ SDK options/API surfaces.
8. Replace wrapper usage in examples (remove manual Rust wrapper maintenance in `message/`).
9. Update CLI decode/inspect flow.
10. Update tests and docs in the same change.

## 7. Risks And Mitigations

1. C ABI break risk:
   - Mitigation: update C++ wrapper/tests immediately in same commit and regenerate header via existing pipeline.
2. Schema decode interoperability risk:
   - Mitigation: update `schema_by_key` encode/decode and integration tests before touching examples.
3. Build graph break risk from directory move:
   - Mitigation: migrate BUILD rules first, then schema file moves with immediate label fixes.
4. Hidden path assumptions in scripts/examples:
   - Mitigation: run a global grep for old message paths and BFBS names after migration.

## 8. Acceptance Criteria

1. `message/` no longer has flat mixed layout; schemas/build-tools/bindings are separated.
2. Root `message/BUILD.bazel` is aggregation-only.
3. No hand-written Rust wrapper `.rs` remains under `message/`.
4. RPC service schema registration and validation use single service schema model end-to-end.
5. Rust SDK, C++ SDK, and FFI options no longer expose req/res dual schema fields.
6. Example/service CLI and cross-process scripts run with single service schema hash semantics.
7. Specs and guides reflect new directory and protocol contracts.

## 9. Developer-run Verification (post-implementation)

Per repository policy, Bazel commands should be run by the developer and outputs shared back.

Suggested checks:

```bash
bazel test //...
```

Additional optional checks:

```bash
cargo test --manifest-path core/Cargo.toml
cargo test --manifest-path rust/Cargo.toml
```
