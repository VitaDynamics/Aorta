# C++ SDK bundle contract — bridge dependency excerpt

This file retains only the SDK packaging contract from the bridge proposal.
The product owns the complete bridge route/schema design.
The product schema pack is produced outside aorta (product side).

## 8. SDK packaging and release boundary

The bridge consumes a staged C++ SDK archive named
`aorta-cpp-sdk-<release>-abi<N>-ubuntu22.04-<arch>.tar.zst`, with native x86_64 and
aarch64 builds on the glibc 2.35 baseline. `<N>` is the Core ABI generation
(`AORTA_ABI_VERSION` in `core/src/ffi/c_api/mod.rs`, the single declaration);
the Linux SONAME `libaorta_core.so.<N>`, the CMake package version `<N>.0.0`,
`Aorta_CORE_ABI_VERSION` and `PROVENANCE.json` all carry the same parsed value.
It includes Core ABI `<N>`, static and
shared C++ libraries, full public headers, middleware `aorta_base_generated.h` and `action_common_generated.h`,
the exact FlatBuffers runtime, CMake imported targets, licenses/notices,
per-member SHA-256 metadata, and embedded source revision provenance.
No product-generated headers or BFBS are part of this SDK-only archive.
Consumers obtain those from the product-side schema package.

`Aorta::cpp` selects the shared SDK; `Aorta::cpp_static` uses portable non-LTO
objects and the same shared Core. `Aorta::core_ffi`, `Aorta::schema_aorta_base`, `Aorta::schema_action_common`
and `Aorta::schemas` are also exported. The bundle supplies FlatBuffers CMake
configuration without requiring a system FlatBuffers package.

## 9. SDK verification boundary

The license gate fails closed using cargo-about 0.9.2 and the locked workspace.
Archive verification checks exact membership, hashes, loader names, portable
static objects and glibc requirements. A clean two-process CMake consumer uses
only the extracted SDK and exchanges a base-schema topic, raw RPC and ContextStore
value over a checksum-pinned upstream router. Both native architecture jobs must
pass before the official pipeline publishes their SDK release assets.
