# Aorta Licensing Policy

This document is the single source of truth for how Aorta is licensed, how
license headers are applied to source files, which parts of the tree are
derived from third-party work, and which obligations attach when Aorta is
shipped as a binary.

- Full license text: [`LICENSE`](../LICENSE) (Apache License, Version 2.0)
- Required attributions: [`NOTICE`](../NOTICE)
- Trademarks and brand assets: [`TRADEMARKS.md`](../TRADEMARKS.md)
- Full dependency inventory: [`docs/sbom/`](sbom/)

This document describes the license of this source tree and what you can
verify with it. Release tooling that maintainers run outside this tree is not
described here.

## 1. First-party license

All first-party Aorta code is licensed under the **Apache License, Version 2.0**
(`Apache-2.0`).

"First-party" means everything in this repository except:

| Path | Status |
|---|---|
| `src/third_party/**` | Vendored third-party source; keeps its own license. Currently GoogleTest (BSD-3-Clause, full text in [`src/third_party/gtest/googletest/LICENSE`](../src/third_party/gtest/googletest/LICENSE)). |
| `patches/**` | Patches against third-party projects; governed by the patched project's license. Currently `patches/rules_rust_linux_static_link.patch`, applied to `rules_rust` 0.68.1 through `single_version_override` in `MODULE.bazel`; rules_rust is Apache-2.0, the same license text as [`LICENSE`](../LICENSE). |
| `tools/zenohd/src/main.rs`, `tools/zenohd/build.rs` | Derived from Eclipse zenoh — see §4. |
| `message/schemas/topic/foxglove_schema/*.fbs` | Derived from foxglove-sdk — see §4. |
| `docs/assets/brand/**` | Brand assets (Aorta and vbot logos). **Not** licensed under Apache-2.0; all rights reserved. See [`TRADEMARKS.md`](../TRADEMARKS.md). |

The Apache License 2.0 grants no trademark rights (§6 of the license). The
Aorta and vbot names and logos are governed by [`TRADEMARKS.md`](../TRADEMARKS.md).

Every first-party Rust crate manifest declares `license = "Apache-2.0"`. The
crates keep `publish = false`; they are consumed through the Bazel module
boundary or built from this tree with Cargo, not through crates.io.

The copyright holder for first-party code is **VitaDynamics**. Existing C++
sources carry `// Copyright (c) 2025-2026 VitaDynamics`; the root `NOTICE`
states `Copyright 2026 VitaDynamics`.

## 2. SPDX header policy

Every first-party source file should carry a **single-line SPDX identifier** as
the first comment in the file:

```
SPDX-License-Identifier: Apache-2.0
```

Per file type, using that language's ordinary line-comment syntax:

| File type | Header line | Placement |
|---|---|---|
| `.rs` | `// SPDX-License-Identifier: Apache-2.0` | First line. |
| `.cc`, `.h`, `.hpp` | `// SPDX-License-Identifier: Apache-2.0` | First line, above the existing `// Copyright (c) ... VitaDynamics` line if present. |
| `.py` | `# SPDX-License-Identifier: Apache-2.0` | After the shebang (`#!...`) and any PEP 263 coding line; otherwise first line. |
| `.fbs` | `// SPDX-License-Identifier: Apache-2.0` | First line. **Plain `//` only — never `///`.** |
| `BUILD.bazel`, `*.bzl` | `# SPDX-License-Identifier: Apache-2.0` | First line. A comment above a module docstring does not stop it being the module docstring, in Python or Starlark. |

### `.fbs` files: never use `///`

FlatBuffers treats `///` as a *documentation comment* and serializes it into the
compiled `.bfbs`. Aorta derives a runtime `SCHEMA_HASH` identity from the
compiled schema, so adding or changing a `///` line **changes wire-visible
schema identity**. Plain `//` comments are stripped by `flatc` and are safe.
Any license or provenance comment added to a `.fbs` file MUST use plain `//`.

For this reason `message/schemas/**` is currently **excluded** from the SPDX
sweep. The provenance comments already added to the foxglove-derived schemas
(§4) were applied by hand and verified to be comment-only.

### Status

Applying the header across the whole tree is deliberately deferred to a quiet
merge window, because it touches nearly every source file and would conflict
with every open branch. Until that sweep runs, most first-party files do not
yet carry the SPDX line; a file without it is still licensed as described in
§1. The zenoh-derived files in `tools/zenohd/` keep their upstream
`EPL-2.0 OR Apache-2.0` header.

Shell scripts, Markdown, JSON, and TOML are not in this tranche. The header
checker that maintainers use is not part of the public source tree.

## 3. Dependency inventory

`docs/sbom/` is the authoritative dependency inventory and is regenerated from
the lockfiles, not maintained by hand:

| File | Contents |
|---|---|
| [`sbom/rust-crates.csv`](sbom/rust-crates.csv) | Every third-party Rust crate in `Cargo.lock` with its declared SPDX expression (verbatim, `AND` terms never flattened). |
| [`sbom/rust-license-summary.md`](sbom/rust-license-summary.md) | License-group statistics plus the license-review focus items (copyleft, conjunctive `AND` audit, MPL-2.0, EPL-2.0, git-pinned sources). |
| [`sbom/bazel-modules.csv`](sbom/bazel-modules.csv) | Bzlmod module closure including the overridden direct modules. |
| [`sbom/bazel-summary.md`](sbom/bazel-summary.md) | `bazel_dep` list with override/patch details. |

Third-party crates are **not** enumerated in `NOTICE`. Apache's NOTICE
convention is that the file carries only attributions that are legally required
of this distribution; a dump of several hundred crates would bury them. `docs/sbom/` carries the
inventory instead.

The license gate is enforced in CI by `cargo deny check sources licenses bans`,
configured in [`deny.toml`](../deny.toml).

## 4. Derived works

| Path | Upstream | Upstream license | Nature of the derivation |
|---|---|---|---|
| `message/schemas/topic/foxglove_schema/*.fbs` (18 files) | [foxglove-sdk](https://github.com/foxglove/foxglove-sdk), `schemas/flatbuffer` | MIT | Converted/adapted. Aorta adds an `aorta_header` field to the publishable root tables, adds Aorta-specific extension fields to `RawAudio` (id 6) and `Log` (ids 7-9), and keeps the foundational geometry tables header-free. |
| `tools/zenohd/src/main.rs`, `tools/zenohd/build.rs` | [Eclipse zenoh](https://github.com/eclipse-zenoh/zenoh), `zenohd` | EPL-2.0 OR Apache-2.0 | Router entry point. `main.rs` is modified (version identity and build stamping, `--connect` parsing) and carries a modification notice below the upstream header; `build.rs` is an unmodified copy. Both files retain the upstream `Copyright (c) 2023 ZettaScale Technology` notice and `SPDX-License-Identifier: EPL-2.0 OR Apache-2.0` header. Aorta exercises the Apache-2.0 branch of the disjunction; the crate manifest declares `license = "Apache-2.0"`. |

### 4.1 Foxglove-derived schemas — exact file list

All 18 live in `message/schemas/topic/foxglove_schema/`:

```
CameraCalibration.fbs      PackedElementField.fbs   Quaternion.fbs
CompressedImage.fbs        Point2.fbs               RawAudio.fbs
CompressedVideo.fbs        Point3.fbs               RawImage.fbs
FrameTransform.fbs         PointCloud.fbs           Time.fbs
FrameTransforms.fbs        Pose.fbs                 Vector2.fbs
LaserScan.fbs              Log.fbs                  Vector3.fbs
```

Each of these files now carries a plain `//` provenance block at the top of the
file naming the upstream project and license.

**Evidence in-repo:** 17 of the 18 files carry the upstream marker
`// Generated by https://github.com/foxglove/foxglove-sdk` near the top, on the
line immediately after the provenance block this change prepended (`Time.fbs`,
imported as a dependency, carries no such marker). The schemas were imported
from `https://github.com/foxglove/foxglove-sdk/tree/main/schemas/flatbuffer`.
The `foxglove` crate 0.26.0 published from that same repository declares `MIT` in
`docs/sbom/rust-crates.csv`, corroborating the determination.

### 4.2 Foxglove MIT notice preservation (source AND binary)

The MIT license requires that "the above copyright notice and this permission
notice shall be included in **all copies or substantial portions** of the
Software". This repository already redistributes MIT-derived source (the 18
`.fbs` files), so **the obligation is live today** — it is not deferred to the
first binary release.

The upstream text is therefore vendored verbatim at
[`THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE`](../THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE),
carrying the upstream copyright line:

```
Copyright (c) Foxglove Technologies Inc
```

(from `https://github.com/foxglove/foxglove-sdk` `LICENSE`; sha256
`da11235665c17d4c1634072dae92b8ba1b38d6fdde2ccf19a6bbede33253f58d`).

It ships on every surface, not just in this repo:

- **This repository** — `THIRD_PARTY_LICENSES/`, referenced from `NOTICE`.
- **The `aorta_msgs` wheel** — `//python:aorta_msgs_wheel` ships the
  byte-identical text as
  `aorta_msgs.dist-info/licenses/THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE`,
  next to a `THIRD_PARTY_NOTICES.md` that names every installed module derived
  from a foxglove-sdk schema (generated from the wheel's ownership manifest by
  `message/tools/python_distribution.py`, fails closed if that set is empty).

## 5. Binary distribution obligations

The obligations below attach when Aorta is distributed as a **compiled
artifact** (a `.deb`, a container image, a shipped router binary), because the
third-party code is then linked into what is shipped. Obligations that already
attach to **source** redistribution are covered above (§4.2 for the Foxglove
schemas) — this section is only about what is *additionally* triggered by
shipping a binary. Source of record:
[`sbom/rust-license-summary.md`](sbom/rust-license-summary.md).

### 5.1 Zenoh (EPL-2.0 OR Apache-2.0)

Zenoh is the transport and the single largest third-party surface: 29 git-pinned
crates plus 10 registry crates. Aorta consumes a **fork**,
`https://github.com/BOURBONCASK/zenoh.git`, pinned by commit rev in
`//:Cargo.toml`'s `[workspace.dependencies]` and mirrored in `MODULE.bazel`.
The fork carries upstream's own license terms (`EPL-2.0 OR Apache-2.0`) — being
a fork does not change them, and the fork's own `LICENSE`/`NOTICE` files govern
its contents.

**`OR` is a licensee choice, not a set of simultaneous obligations.** SPDX `OR`
means the licensee elects **one** branch and is bound by that branch alone.
**Aorta elects the Apache-2.0 branch** — the same election recorded for the
zenoh-derived `tools/zenohd` sources in §4. Consequently:

- The obligations that apply are **Apache-2.0's**: preserve copyright,
  license, and NOTICE material, and state significant modifications.
- **EPL-2.0's copyleft and source-availability obligations do NOT attach.**
  Electing Apache-2.0 is what makes them not attach; nothing else has to be
  done to avoid them.
- *If* the EPL branch were ever elected instead (it is not, today), EPL-2.0 is
  weak file-level copyleft: modifications to EPL-covered files would have to be
  published under EPL-2.0, and a Secondary License clause would govern linking.
  Electing EPL would be a deliberate legal decision, not a build-configuration
  accident.

Independently of the election, any binary that links zenoh must reproduce the
zenoh attribution material (an Apache-2.0 obligation in its own right).

### 5.2 AWS-LC / ring — cryptography attribution chains

`aws-lc-rs` 1.18.0 and `aws-lc-sys` 0.44.0 enter the lock graph through
Foxglove 0.26.0's Rustls closure. They are target/feature-gated and are not in
cargo-deny's current host-platform graph, but they are part of the full lock
snapshot and must not be omitted from a binary's attribution file if that
feature path is ever active.

`aws-lc-sys`'s declared expression is conjunctive throughout — every top-level
`AND` term applies simultaneously:

```
ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause
    AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)
```

The crate's bundled `LICENSE` identifies AWS-LC as a fork of **BoringSSL**,
BoringSSL as a fork of **OpenSSL**, and retains the original **SSLeay**
copyright on some files, alongside an inventory of MIT and BSD-3-Clause
components. A binary distribution that includes this code must **reproduce the
crate's bundled license and attribution material verbatim** — the OpenSSL and
SSLeay notices in particular carry explicit advertising/attribution wording.

`ring` 0.17.14 is `Apache-2.0 AND ISC` (conjunctive, both apply) and **is** in
the current deny graph.

### 5.3 Weak-copyleft components

| Declared expression | Crates | Obligation on a binary distribution |
|---|---|---|
| `EPL-2.0 OR Apache-2.0` | 39 (zenoh family) | **Choice, not conjunction** — Aorta elects Apache-2.0 (§5.1), so only Apache-2.0 obligations apply: reproduce copyright/license/NOTICE and state modifications. No EPL copyleft, no EPL source-availability duty. |
| `MPL-2.0` | 5 in the lockfile (`cbindgen`, `nucleo`, `nucleo-matcher`, `option-ext`, `serialport`); 4 reach a binary | **No alternative branch** — MPL-2.0 genuinely applies. File-level copyleft. Every binary that includes MPL-covered code, **modified or not**, must tell recipients how to obtain the Source Code Form of that code (MPL-2.0 §3.2(a)); modified covered files must also be made available under MPL-2.0. Combining them with differently-licensed code in a Larger Work is permitted (§3.3). |

Note the asymmetry: the zenoh family offers an `OR` branch we can elect out of;
MPL-2.0 does not, so it is the one weak-copyleft obligation Aorta actually
carries. The source-availability notice applies to all distributed MPL-covered
code, including unmodified upstream crates; it is not limited to files Aorta
changed. The generated `THIRD_PARTY_NOTICES.md` (§5.4) therefore lists every
component whose license expression contains MPL-2.0, including compound
expressions, under "MPL-2.0 source code availability" with the line "Source
code for this component is available at
https://crates.io/crates/{name}/{version}." for its exact name and version.
The generator refuses an MPL-2.0 component that does not come from crates.io,
because no source location would be stated for it. Which binaries carry
MPL-2.0 code:

- `option-ext` and `serialport` (via zenoh's `transport_serial` feature) link
  into `libaorta_core` — and therefore into the `aorta_sdk` wheel and the C++
  SDK bundle — and into `zenohd`.
- `nucleo` and `nucleo-matcher` link into `aorta-tui` only (source-built tool).
- `cbindgen` is the C-ABI header generator behind `//tools/aorta-cbindgen`, a
  standalone workspace member that only runs at build time. It is **not** a
  dependency of `aorta_core` (an unused declaration there was removed), so it
  is neither linked into any artifact nor part of the released `aorta_core`
  manifest. Its MPL-2.0 terms attach to the tool's own source, which is not
  redistributed in binary form.

### 5.4 Attribution file for releases

The Foxglove notice is vendored and wired into the release exporters (§4.2).
Aorta does not yet generate a **complete** per-release attribution bundle over
its full dependency graph, though: until it does, a binary release must attach
the bundled license files of the crates in its actual build graph (zenoh,
`ring`, and — if that feature path is active — the AWS-LC chain).

What the release artifacts carry today, all from the same cargo-about evidence
(`tools/release/gen_third_party_notices.py`, policy in
`tools/release/about.toml`; the wheel inputs are the
`//tools/release:collect_third_party_licenses` and
`//tools/release:aorta_sdk_wheel_third_party_notices` targets):

- **C++ SDK bundle** — `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` over the
  whole workspace graph (with the MPL-2.0 source-availability lines),
  `FLATBUFFERS-LICENSE`, `THIRD_PARTY_COMPONENTS.md`.
- **`aorta_sdk` wheel** — `LICENSE`, `NOTICE` and a `THIRD_PARTY_NOTICES.md`
  restricted to the crates `cargo tree -e normal --target <triple> -p
  aorta_core` reaches for the wheel's platform, i.e. the crates linked into
  the bundled `libaorta_core` (the `zstd` BSD-3-Clause notice included via the
  `zstd-sys` clarification in `about.toml`; §5.3) and the MPL-2.0
  source-availability lines, plus the glibc/libstdc++
  runtime notes on Linux. All of it lives under `.dist-info/licenses/` with
  PEP 639 `License-File` metadata.
- **`aorta_msgs` wheel** — `LICENSE`, `NOTICE`, the foxglove notice and its
  derived-module list (§4.2).

## 6. Attribution maintenance

Attribution and notice obligations are maintained in `NOTICE` and
`THIRD_PARTY_LICENSES/`; see §4 and §5 for the preserved upstream notices.
