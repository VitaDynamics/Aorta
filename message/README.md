# Aorta Message Build Workflow

This document describes the current schema/codegen workflow under `message`.

> When authoring or modifying a `.fbs`, follow FlatBuffers best practices and
> keep the schema aligned with the registry-driven build pipeline. For migrations
> from ROS2 `.msg/.srv/.action` definitions, preserve field semantics and numeric
> enum values.

## Design Goals

- No hand-written Rust/C++ wrapper files for message bindings.
- Generated artifacts are produced by Bazel, not maintained manually.
- Wrapper declarations are derived from one composed metadata registry (`SCHEMA_DEFINITIONS`).
- Schema package BUILD boilerplate is reduced through shared Starlark macros.

## Endpoint Names

Business topic, service, and action names use `<domain>/<name>`, without a
`topic/`, `service/`, or `action/` prefix. Examples:

| Kind | Endpoint name |
|---|---|
| Topic | `demo/chatter`, `sensor/imu` |
| Service | `demo/echo`, `navigation/plan_path` |
| Action | `demo/execute_task` |

The SDK adds the transport primitive (`pub`, `srv`, or `act`) when building
the wire key, such as `aorta/default/pub/demo/chatter`. Those segments remain
part of full transport keys and schema-discovery key suffixes.

Schema paths remain under `message/schemas/{topic,service,action}/`; they
identify schema sources, not runtime endpoints. Endpoint renames must update
publishers, subscribers, service/action servers and clients, recorder
selectors, monitoring rules, and deployed configuration together. Old and new
endpoint names do not communicate automatically.

## Source of Truth

- Schema files: `message/schemas/**/*.fbs`
- Action schemas: `message/schemas/action/**/*.fbs` follow the same registry-driven build flow.
- Schema metadata sources: `message/schemas/registry_local.bzl` for Aorta-native
  public/fixture entries; `message/schemas/registry.bzl` derives the base-only
  `SCHEMA_DEFINITIONS` and generated faces. Application schemas and their wrappers live
  in separate schema modules (see [Application Schema Modules](#application-schema-modules)).
- Schema package aggregate exports: each package `BUILD.bazel` defines
  `all_rust/all_cpp/all_python/all_bfbs/all_schema_meta_cpp/examples_bfbs`
- Root-level schema output aliases: `SCHEMA_OUTPUT_ALIASES` in
  `message/schemas/registry.bzl`
- Schema target macro: `//message/tools:schema_codegen.bzl`
- Schema package macro: `aorta_define_schema_package(...)` in
  `//message/tools:schema_codegen.bzl`
- Wrapper target macro: `//message/tools:message_bindings.bzl`
- Wrapper metadata registry (single source): `SCHEMA_DEFINITIONS` in
  `message/schemas/registry.bzl`
- Wrapper binding re-export: `message/bindings/registry.bzl`
- Machine-readable schema manifest:
  `//message:schemas_registry_manifest`
  (`message/schemas_registry_manifest.json` in runfiles)

## Auto-Generated Artifacts

Generated at build time:

- Schema language outputs (`*_generated.rs`, `*_generated.h`, `*.bfbs`, schema meta outputs).
- For `action_schema = True` entries, Bazel first derives a generated action wrapper `.fbs`
  (domain types + protocol wrapper tables), then runs the normal FlatBuffers pipeline on that generated root.
- Rust wrapper source for each binding (`<binding_name>.rs`).
- C++ wrapper header for each binding (`<wrapper_header>.h`).
- Schema package aggregate filegroups (`schemas`, `schema_build_files`) now
  come from the single schema package registry instead of hand-written package lists.
- Root-level schema aliases/filegroups in `message/BUILD.bazel` are generated
  from per-package aggregate filegroups instead of per-schema hand wiring.

Rust wrapper behavior:

- Re-exports generated modules (`aorta`, schema namespace modules).
- Exposes `<STEM>_BFBS` and `<STEM>_FIXED_SIZE` constants.
- Auto-generates `schema::*Schema` marker types implementing `aorta_core::schema::SchemaMeta`
  (including `root_type()`), so examples/tools can reuse typed schema metadata without hand-written impls.
- For Action bindings under `schemas/action/**`, Rust wrapper generation also emits one
  `<Action>Action` marker type implementing `aorta_core::action::ActionSchema`
  and `aorta_core::action::ActionWire` for send-goal, cancel-goal, feedback,
  get-result, and public-status surfaces, including feedback goal-id decode used
  by client-side feedback filtering.
- Generates typed publish helpers used by SDK-level `PublishTyped`/`publish_typed`
  APIs, so caller code can fill message payload without manually injecting
  `aorta_header`.

Typed publish generation contract:

- Generated only for FlatBuffers table types that include `aorta_header`.
- Helpers expose payload fields only (header excluded from caller-facing message).
- Wrapper-generated traits bridge SDK typed API to generated table builder code.
- Current field coverage:
  scalar, enum, `string`, vector-of-scalar/enum, vector-of-string, local struct,
  local table, vector-of-local-table, and local `union` fields.
- C++ wrapper generation also supports `vector<union>` fields.
- Rust FlatBuffers codegen in this repository does not support `vector<union>`;
  use a C++-only schema target when `vector<union>` is required.

Typed subscribe generation contract:

- Generated for every local FlatBuffers table in the schema, not only `root_type`.
- This allows typed RPC response decode when the response table shares the same
  BFBS as the request but is not itself the schema `root_type`.

Reference schemas used for coverage:

- Cross-language typed publish coverage: `schemas/topic/example/all_types.fbs`
- C++ `vector<union>` coverage: `schemas/topic/example/all_types_cpp_vector_union.fbs`

Not checked into source tree:

- `message/bindings/rust/*.rs` wrapper sources
- `message/bindings/cpp/*.h` wrapper headers

## Wrapper Registry Model

Wrapper declarations are derived from `SCHEMA_DEFINITIONS`:

- `RUST_MESSAGE_BINDINGS`
- `CPP_MESSAGE_BINDINGS`
- `MESSAGE_BINDING_ALIASES`

All three are produced in `schemas/registry.bzl` and re-exported by
`bindings/registry.bzl` for BUILD load ergonomics.

Naming defaults:

- Rust wrapper target alias: `<schema_id>_msg_rs`
- Rust crate name: `<schema_id>_msg`
- C++ wrapper alias: `<schema_id>_msg_cc`
- C++ wrapper header: `<schema_id>_msg.h`

Schema output aliases are generated with a single naming convention:
`fbs_schema_<schema_id>_<artifact>` (for example
`fbs_schema_demo_chat_rust`, `fbs_schema_demo_chat_bfbs`,
`fbs_schema_demo_chat_schema_meta_cpp`).

## Add a New Schema

1. Add `<stem>.fbs` under an appropriate schema package in `schemas/...`.
2. Add an Aorta-owned public or test fixture entry to `schemas/registry_local.bzl`.
   Application schemas belong in their own schema module (see
   [Application Schema Modules](#application-schema-modules)). Every local entry requires
   `pkg`, `stem`, `root_fbs`, and `provenance` (`public` or `fixture`), plus configured
   flags/dependencies/alias metadata:
   - optional `dep_fbs`
   - optional `action_schema = True` for Action domain schemas (the source file keeps only `Goal` / `Result` / `FeedbackData`, build generates protocol wrappers)
   - output flags (`rust/cpp/python/bfbs/schema_meta_rust/schema_meta_cpp`)
   - optional C++ controls (`cpp_lib`, `cpp_lib_name`, `cc_alias`)
   - optional `alias_id` for root alias naming and wrapper naming
   - optional wrapper controls:
     - `rust_wrapper` (bool), optional `rust_wrapper_name`,
       optional `rust_wrapper_crate_name`,
       optional `rust_wrapper_include_schema_meta`, optional `rust_wrapper_deps`
     - `cpp_wrapper` (bool), optional `cpp_wrapper_name`,
       optional `cpp_wrapper_header`, optional `cpp_wrapper_schema_cc_target`
   Registry fragments are a deliberately restricted AST: only uppercase
   fragment-local string constants followed by the list assignment are allowed;
   entries must be sorted by `(pkg, stem)` and use `struct(...)` with scalar/list
   literals or previously declared local string constants. Do not add `load()`,
   functions, comprehensions, computed values, or cross-fragment constants.
   Keep public metadata aligned with each schema's generated targets.
3. Package `BUILD.bazel` files only call:
   `aorta_define_schema_package(schema_items_for_package(<pkg>))`.
   No per-schema duplication is required there.
4. If introducing a new schema package directory, add corresponding package
   `BUILD.bazel` and ensure `pkg` in the selected fragment matches exactly.
5. Consume wrapper aliases from `//message:<binding_name>` in SDK/examples/tools BUILD targets.
   For Rust wrapper targets, code imports the crate name (`<schema_id>_msg`) rather than the target alias (`<schema_id>_msg_rs`).
6. Root aliases are generated from the composed `SCHEMA_DEFINITIONS` using the
   `fbs_schema_<schema_id>_<artifact>` naming convention and are exported consistently.
7. If you want SDK typed publish support, ensure target table definitions contain
   `aorta_header: aorta.sys.AortaHeader`.

The registry gates reject invalid fragment syntax/provenance and
physical-file/entry drift (including new packages). Run
`bazel test //message/tools/...` together with the related message-tool checks.

### Python Bindings (`python = True`)

`python = True` emits the flatc Python module tree (`<stem>_py`), and — because
`schema_meta_py_enabled()` defaults to `python and bfbs` — also the
`<stem>_schema_meta` module. Both are bundled into the `aorta_msgs` wheel
(`//python:aorta_msgs_wheel`) automatically; there is no curated package list to
update. With `bfbs = True`, generation also exposes `<stem>_fastpath`:
root-local scalar struct writers and eligible table-vector readers in
`<namespace>/<stem>_fastpath.py`. An included struct belongs to its declaring
schema's companion. Struct-only owners enable BFBS and set
`schema_meta_py = False`; they do not need a root table.

The owner-aware distribution tree bundles each companion with its schema's
wheel. The authored `aorta_msgs_runtime` package (including its initializer)
is owned exclusively by `aorta_msgs`; product/custom wheels depend on that
base wheel. All message wheels require exactly `flatbuffers==25.12.19`, matching
the pinned generator and builder adapter. Generated namespace initializers
remain excluded. Codegen profile `p1.2` adds these configured outputs while
keeping the frozen API-2 registry fields unchanged.

Every production topic/service/action schema is Python-enabled. Three
constraints decide the exceptions:

1. **`[union]` (vector of unions) is unsupported**, exactly as in Rust: flatc
   fails with "Vectors of unions are not yet supported in at least one of the
   specified programming languages". Such a schema stays C++-only (see
   `topic/example/all_types_cpp_vector_union.fbs`).
2. **`schema_meta_py` requires a `root_type`.** A dep-only schema (no
   `root_type`) with `bfbs = True` must set `schema_meta_py = False`, or the
   generator hard-fails with "root_type missing in schema". This mirrors
   `schema_meta_rust`/`schema_meta_cpp` already being off on those entries.
3. **A namespace whose root collides with a Python SDK module needs an
   `__init__.py` drop.** The `aorta_msgs` and `aorta_sdk` wheels both install
   into `site-packages/aorta/`, so a schema in namespace `aorta.<x>.*` emits an
   `aorta/<x>/` *package*. If the SDK also ships `aorta/<x>.py`, the package
   shadows the module — and because `aorta/__init__.py` re-exports from its
   submodules, that breaks `import aorta` outright, not just the one symbol.

   This is live for `aorta.action.*` (`shared/action_common`, `action/rcp`,
   `action/data_upload`, `action/sys_insight`, `action/slam/map_asset`) and for
   `aorta.capture.*` (`topic/capture/capture_lifecycle`). It is resolved
   structurally, not by disabling Python: the SDK ships
   `aorta/<x>/__init__.py` (a package, so its own imports are parent-relative
   `..`, and path-extending via `pkgutil.extend_path` so the generated modules
   are still found when the SDK and the schema's `<stem>_py_lib` are separate
   Bazel targets in different import roots), and `python_msgs_tree` drops the
   generated empty `aorta/<x>/__init__.py` so the SDK's real one wins and the
   generated
   `aorta/<x>/<domain>/` modules attach as subpackages. Installed standalone,
   `aorta/<x>/` degrades to a PEP 420 namespace and `aorta.action.rcp` still
   imports. This is the same shape as the top-level `aorta/__init__.py` drop.

   `aorta.services.*` does **not** collide — the SDK module is `service.py`
   (singular). Before enabling a schema whose namespace root matches any entry in
   `python/aorta/`, do BOTH: make the SDK side a package (never a flat
   `aorta/<x>.py`), and add `<x>` to `_SDK_OWNED_SUBPACKAGES` in
   `message/tools/flatbuffers_rules.bzl`. `python/tests/test_architecture.py`
   enforces both halves, so forgetting either fails the Python SDK CI instead of
   the downstream consumer's venv.

### Include Dependency Rule (Important)

For schemas that use FlatBuffers `include` (for example `include "Time.fbs";`):

- Define schema targets for included files as well (for example `Time`).
- Keep `stem` aligned with the file basename to match flatc output naming.
- Rust binding compile-data include closure is auto-populated by
  `message_bindings.bzl` from `include` graph (with `_rust_all` fallback when
  Starlark file-read support is unavailable).
- C++ include export closure is validated by static check:
  `//message/tools:check_message_schema_deps_test`
  (cc_library `hdrs` completeness for each `schema_cc_target`).
  The checker consumes the generated schema manifest instead of parsing
  `schemas/registry.bzl` text directly.

## Foxglove Schemas

Official Foxglove FlatBuffers schemas can be vendored under
`schemas/topic/foxglove_schema` to keep Aorta-side schema build and reflection
tooling aligned with upstream definitions.

Current imported official files:

- `Point2.fbs`
- `Point3.fbs`
- `Vector2.fbs`
- `Vector3.fbs`
- `Quaternion.fbs`
- `Pose.fbs`
- `PackedElementField.fbs` (with embedded `NumericType` enum)
- `RawImage.fbs`
- `CompressedImage.fbs`
- `CameraCalibration.fbs`
- `PointCloud.fbs`
- `LaserScan.fbs`
- `FrameTransform.fbs`
- `FrameTransforms.fbs`
- `CompressedVideo.fbs`
- `RawAudio.fbs`
- `Time.fbs` (dependency)

Aorta integration adds an extension field:

- `aorta_header:aorta.sys.AortaHeader` on Aorta-published topic root tables
  (for example `CompressedVideo`, `RawAudio`, `RawImage`, `CompressedImage`,
  `CameraCalibration`, `PointCloud`, `LaserScan`, `FrameTransforms`),
  preserving original field IDs and semantics for Foxglove-defined fields.

The upstream source, license, and the full list of Foxglove-derived files are
recorded in [`docs/licensing.md` §4](../docs/licensing.md#4-derived-works).

## Application Schema Modules

Application-specific topic, service, and action contracts belong in separate
schema modules. Those modules can reuse Aorta's public schema rules
(`schema_package.bzl`, `registry_compose.bzl`, `schema_lint.bzl`) and expose their
own generated per-package targets. Adding an application schema does not require
an Aorta root alias or a per-schema listing in this repository.

Contribute changes here when they affect Aorta's base schemas, schema rules, or
code generation. The external-root macro below supports schema sources owned by
another module or repository.

## External Schema Roots (`aorta_external_schema_package`)

Everything above describes schemas that LIVE in this repository. A schema root
that lives somewhere else — an Aorta-owned schema source module, or a user's
own repository — goes through one public macro instead:

```starlark
load("@aorta//message/tools:external_schema_root.bzl", "aorta_external_schema_package")
load(":registry.bzl", "MY_SCHEMAS")

aorta_external_schema_package(
    name = "my_msgs",
    items = MY_SCHEMAS,
    fragment = ":registry.bzl",
    fragment_symbol = "MY_SCHEMAS",
    source_package = "//schemas",
    source_inventory = glob(["src/**/*.fbs"]),
    source_prefix = "src",
    expected_provenance = "external",
)
```

Each declared item is copied byte-for-byte into the CALLING package's canonical
`<stem>.fbs` slot, and that copy — never the source — drives the same
BFBS/schema-meta/typed-wrapper pipeline the in-repo registry uses. The source
repository name, the sandbox path and the checkout path therefore never enter
the schema identity.

**Generated targets** (stable public API from the first published version):

| Target | What it is |
|---|---|
| `:<stem>.fbs` | canonical copy output (the slot every downstream action reads) |
| `:<stem>_fbs_closure` | the schema + its full transitive `.fbs` closure |
| `:<stem>_cpp`, `:<stem>_py`, `:<stem>_rust` | raw flatc outputs |
| `:<stem>_rust_closure` | the schema's generated Rust module + its transitive include closure |
| `:<stem>_bfbs` | the ONE binary schema; every language's identity comes from it |
| `:<stem>_schema_meta` (Rust), `:<stem>_schema_meta_cpp`, `:<stem>_schema_meta_py(_lib)` | schema metadata |
| `:<stem>_cc` | C++ schema library (generated + meta headers) |
| `:<message base>_cc` | typed C++ binding (`TypedPublishTraits`, `SchemaMeta<T>`, action traits) |
| `:<message base>_rs` | typed Rust binding, crate name `<message base>` (`SchemaMeta`, `TypedPublishMessage`, `ActionSchema`/`ActionWire`) |
| `:<name>_cc` | root aggregate of every typed C++ binding |
| `:<name>_rs` | root Rust facade crate (crate name `<name>`), re-exporting each schema as `<name>::<message base>` |
| `:<name>_py_tree`, `:<name>_py` | merged Python tree + the `py_library` that carries `PyInfo` |
| `:<name>_canonical_fbs` | the root's canonical `.fbs` files |
| `:<name>_manifest` | `external_root_manifest_version = 1` evidence document |
| `:<name>_lint_test` | auto-registered content lint — **CI must run it** |
| `:<name>_workspace_lint` | `bazel run` physical/symlink census (same-package roots) |

The Rust leg runs through the SAME BFBS-driven `gen_message_wrapper.py` +
`aorta_rust_message_binding` path Aorta's own registry uses — not the older
stem-guessing wrapper in `aorta_flatbuffers.bzl`. A Rust consumer writes:

```rust
use aorta_sdk::prelude::*;
use my_msgs::my_msg::schema::MyMsgSchema;   // <root>::<message base>::schema
```

These are Bazel **source crates**, not published Cargo packages: their version
identity is the module revision your `MODULE.bazel` pins.

**Fragment rules.** The fragment uses a pure-data grammar: module-level
assignments, string constants before the list, `struct(...)` literals, sorted by
`(pkg, stem)`, no `load`, no `def`, no computation. A module docstring is
allowed. `provenance` is `external` for a user's own root (`internal` is
reserved for roots declared in Aorta's own
`message/schemas/external_roots.bzl`), and `edu_public` — the EDU schema
pack's projection flag, owned by the schema module — must not appear.

**Flag policy** (no `languages` argument exists; the item flags are the truth):

- an item with a root type sets `bfbs`, `cpp`, `python`, `rust`,
  `schema_meta_cpp`, `schema_meta_rust`, `cpp_wrapper` and `rust_wrapper` —
  first class in every supported language, or not at all;
- an include-only item (no root type) sets none of those, but DOES need raw
  `cpp`/`python`/`rust` when a typed schema in the root includes it (flatc's
  generated header/module includes the included schema's generated
  header/module, and the Rust wrapper names one `#[path] pub mod
  <stem>_generated;` per include unconditionally);
- every physical `.fbs` under the source root gets an item. There is no implicit
  include-only exception table.

**Gotchas.**

- A stem ending in `_msg` makes the schema alias `<stem>_cc` and the typed
  binding `<message base>_cc` the same label. The macro rejects it; rename the
  schema or set `cpp_wrapper_name`.
- Rust crate names must be unique across the whole root, INCLUDING the facade's
  own crate name (`<name>`). Both gates check the DEFAULTED names too, so a
  collision is an analysis-time error here rather than an ambiguous-name rustc
  error in somebody else's crate. `rust_wrapper_name` / `rust_wrapper_crate_name`
  override the defaults.
- `action_schema = True` requires an explicit `action_name` — an external root
  has no in-repo `schemas/action/<pkg>` path to derive a routing name from.
- Sources sit directly under `source_prefix` (canonical slots are flat per
  package), and `root_fbs` must be `<stem>.fbs`.
- `dep_fbs` is DIRECT edges only, spelled as labels (`:x.fbs`, `//pkg:x.fbs`,
  `@repo//pkg:x.fbs`), and must agree with the schema's `include` statements in
  both directions.
- The calling package may hold NO physical `.fbs` other than this root's own
  sources under `source_prefix/` — every canonical `.fbs` there is a generated
  copy output, so a stray file would be a second owner of a canonical slot. A
  `glob()` census enforces it at analysis time and again in the lint.
- A package NESTED inside a schema package must itself be a schema package
  (i.e. expose `all_source_fbs`). The census recurses through that target, so a
  nested package without one makes the census incomplete — and therefore fails
  the build rather than passing quietly.
- `bfbs` is checked against the FILE: a schema that declares a `root_type` must
  be typed, one that does not must not be (an action schema is the documented
  exception — its aggregate's root type comes from the Goal table).

**Worked example.** `message/tools/tests/external_root_edu/schemas` is a
complete EDU-class root — fragment, `src/`, macro call, and C++/Rust/Python
smokes across message, service and action (the full 3 × 3 matrix, every cell
built AND run, plus a four-way C++/Rust/Python/sha256(bfbs) hash-parity test) —
and is the seed of the EDU quickstart.
`message/tools/tests/external_root_internal` plus
`message/schemas/external_fixture/probe` show the internal-class shape: sources
outside `message/schemas/**`, a shadow package holding no tracked `.fbs`, and
the canonical outputs joining the release artifact.

## Update an Existing Schema

1. Edit the `.fbs` file.
2. Keep stem/target naming stable unless there is an intentional migration.
3. Do not manually edit wrapper outputs in `bindings/rust` or `bindings/cpp`.
4. Update registry/schema BUILD only when declaration or packaging behavior changes.

### FlatBuffers Version Bumps Can Change Schema Hashes

The schema hash is SHA-256 over the raw BFBS bytes, and flatc writes the BFBS.
A FlatBuffers bump can therefore change hashes without any `.fbs` edit. The
wire payload layout is unaffected. Mixed old/new deployments log
`schema_by_key` conflict warnings (default `AORTA_SCHEMA_CONFLICT_ACTION=warn`)
until every node is rebuilt.

The 25.9.23 → 25.12.19 bump is such a change: flatc no longer marks non-scalar
struct fields (fixed arrays, nested structs) `optional` in the BFBS. Every
schema whose include closure has a struct with such a field gets a new hash,
for example `system/trace_chunk`. Regenerate any recorded BFBS golden hashes in
the same change.

## What to Commit

- `.fbs` changes
- related `BUILD.bazel` changes
- the selected `schemas/registry_local.bzl` or
  `schemas/registry_local.bzl` change
- `schemas/registry_manifest.bzl` changes (manifest schema/encoding behavior changed)
- `bindings/registry.bzl` changes (only if export behavior changes)
- macro/tooling changes (if workflow behavior changed)
- docs updates

Do not commit Bazel-generated wrapper output files.

## Developer Verification (Run Locally)

```bash
bazel build //message/bindings/rust:all
bazel build //message/bindings/cpp:all
bazel test //message/tools:check_message_schema_deps_test
bazel test //message/tools:gen_action_wrapper_test
bazel test //message/tools:external_root_lint_test
bazel test //message/tools/tests/external_root_edu/...
bazel run //message/tools/tests/external_root_edu/schemas:edu_msgs_workspace_lint
bazel build //cpp/examples:talker //cpp/examples:listener
bazel build //rust/examples:sensor_subscriber
```
