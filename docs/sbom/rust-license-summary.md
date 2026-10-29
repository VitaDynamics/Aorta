# Aorta — Rust Crate SBOM: License Summary

Generated from `cargo metadata` (full lock graph, all targets) + `cargo-deny 0.19.6 list` (the deny graph for the selected workspace features, with no target filter). Machine-readable detail: [`rust-crates.csv`](rust-crates.csv).

- Total third-party crates in `Cargo.lock`: **630** (direct **45**, transitive **585**; git-pinned **29** from 1 git remote, see §2.4)
- Private workspace members excluded (`deny.toml [licenses] private.ignore`, 14 `aorta_*` crates)
- **License column methodology:** the `license` column preserves the crate's declared SPDX expression **verbatim**, including conjunctive `AND` terms (both licenses apply simultaneously — they are *not* interchangeable alternatives). 37 crates declare the deprecated slash form (`MIT/Apache-2.0`), which is not parseable SPDX. Inside cargo-deny 0.19.6's graph the form is normalized to an `OR` expression from the DECLARATION itself, with the discovered license set used only as corroboration — a disagreement becomes a review item rather than a guess (29 crates). Outside that graph nothing corroborates the split, so the declaration is kept VERBATIM in both the `license` and `declared` columns (8 crates). The `declared` column holds the raw `Cargo.toml` license field for cross-checking.
- The summary table groups by expression. Only a PLAIN `A OR B` list is reordered (`MIT OR Apache-2.0` ≡ `Apache-2.0 OR MIT`); an expression containing `AND`, parentheses or a legacy slash is grouped **exactly as declared**, because SPDX `AND` binds tighter than `OR` and rearranging around it changes what is owed. Rows are ordered by crate count, then by expression.

## 1. License group statistics

| License (canonicalized expression) | Crates | Notes |
|---|---|---|
| Apache-2.0 OR MIT | 350 | dual-license; either side may be exercised |
| MIT | 130 | permissive |
| Apache-2.0 OR EPL-2.0 | 39 | Eclipse zenoh family (weak copyleft alternative); see legal notes |
| Unicode-3.0 | 18 | Unicode data files license (icu_*) |
| Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT | 15 | LLVM-exception variant |
| Apache-2.0 OR MIT OR Zlib | 12 | tri-license |
| Apache-2.0 | 7 | permissive |
| MIT OR Unlicense | 7 | dual-license incl. public-domain dedication |
| MIT/Apache-2.0 | 7 | legacy slash form, kept verbatim (outside the deny graph) |
| Zlib | 7 | permissive |
| MPL-2.0 | 5 | weak copyleft (file-level); see legal notes |
| ISC | 4 | permissive |
| Apache-2.0 OR BSD-2-Clause OR MIT | 3 | tri-license |
| Apache-2.0 OR ISC OR MIT | 3 | tri-license |
| Apache-2.0 OR BSD-3-Clause | 2 | dual-license |
| Apache-2.0 OR LGPL-2.1-or-later OR MIT | 2 | LGPL copyleft ALTERNATIVE in expression; see legal notes |
| BSD-2-Clause | 2 | permissive |
| BSL-1.0 | 2 | Boost permissive |
| CDLA-Permissive-2.0 | 2 | data license (webpki roots) |
| (MIT OR Apache-2.0) AND Unicode-3.0 | 1 | **conjunctive (AND)** — see §2.6 |
| (MIT OR Apache-2.0) AND Unicode-DFS-2016 | 1 | **conjunctive (AND)**, verbatim — see §2.6 |
| 0BSD OR Apache-2.0 OR MIT | 1 | tri-license |
| Apache-2.0 AND ISC | 1 | **conjunctive (AND)** — both licenses apply simultaneously; see §2.6 |
| Apache-2.0 OR BSL-1.0 | 1 | dual-license |
| Apache-2.0 OR CC0-1.0 OR MIT-0 | 1 | permissive alternatives (`dunce` 1.0.5) |
| Apache-2.0/MIT | 1 | legacy slash form, kept verbatim (outside the deny graph) |
| BSD-3-Clause | 1 | permissive |
| GPL-3.0 OR MIT | 1 | legacy slash form for a dual offer; the MIT side is the one exercised — see §2.1 |
| ISC AND (Apache-2.0 OR ISC) | 1 | **conjunctive (AND)**; `aws-lc-rs` — see §2.5 and §2.6 |
| ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0) | 1 | **conjunctive (AND)**; `aws-lc-sys` — see §2.5 and §2.6 |
| MIT AND Unicode-DFS-2016 | 1 | **conjunctive (AND)**, verbatim — see §2.6 |
| WTFPL | 1 | not OSI-approved; outside the deny graph — see legal notes |
| **Total** | **630** | |

> Per-crate expressions are in `rust-crates.csv`; the only transformation applied anywhere is sorting the operands of a plain OR list.

## 2. Legal review focus items

### 2.1 Copyleft / GPL-family

| Crate | Version | Expression | Why it passes the gate | Notes |
|---|---|---|---|---|
| `r-efi` | 5.3.0, 6.0.0 | `MIT OR Apache-2.0 OR LGPL-2.1-or-later` | passes under the MIT/Apache-2.0 side (SPDX OR semantics) | LGPL-2.1-or-later is a **copyleft alternative** in the expression; choosing MIT/Apache-2.0 is legitimate, but legal should be aware. Two versions present (5.3.0 + 6.0.0) because `getrandom` 0.3.x and 0.4.x each pin their own. Both are UEFI-target dependencies of getrandom; cargo-deny's graph carries them because `deny.toml` sets no `targets`, which is not a statement about what any host actually compiles. |
| `terminfo` | 0.9.0 | `WTFPL` (declared) | outside the deny graph for the selected workspace features | WTFPL is not OSI-approved. Reachable only via ratatui's **optional** `termwiz` backend (`ratatui-termwiz`), which no workspace member enables (all use `features = ["crossterm"]`). It is **not** allowlisted in deny.toml — if the termwiz backend is ever enabled, the licenses gate will hard-fail. Keep it that way. |
| `unescaper` | 0.1.8 | `GPL-3.0/MIT` (legacy slash form) | the global `MIT` allow entry in deny.toml covers the MIT side | Declared `license = "GPL-3.0/MIT"` — Cargo's deprecated slash form for a dual offer. The crate ships both LICENSE-GPL3 and LICENSE-MIT, so upstream intent is GPL-3.0 **OR** MIT, and the MIT side is the one exercised. deny.toml also carries a crate-scoped `exceptions` entry for it; as of 2026-09-07 with cargo-deny 0.19.6 that entry is **redundant** — removing it and re-running `check` still reports `licenses ok`, because cargo-deny resolves the declaration to distinct `GPL-3.0` and `MIT` identifiers and the global `MIT` allow satisfies the OR. Retiring the exception is a deny.toml policy change, out of scope for this report. Pulled via serialport → tokio-serial → zenoh-link-serial. |

### 2.2 MPL-2.0 (weak copyleft, file-level)

5 crates are pure MPL-2.0 (no OR alternative):

| Crate | Version | Direct/Transitive |
|---|---|---|
| cbindgen | 0.29.2 | direct |
| nucleo | 0.5.0 | direct |
| nucleo-matcher | 0.3.1 | transitive |
| option-ext | 0.2.0 | transitive |
| serialport | 4.9.0 | transitive |

MPL-2.0 is file-level copyleft, and its obligations attach to DISTRIBUTION. Anyone who distributes MPL-covered code in executable form, modified or not, must make the Source Code Form of that code available and tell recipients how to obtain it (MPL-2.0 §3.2(a)); modified covered files must also be made available under MPL-2.0. Modifications kept in-house trigger nothing, and combining MPL files with differently-licensed files in a Larger Work is expressly permitted (MPL-2.0 §3.3). Allowlisted in deny.toml. See <https://www.mozilla.org/en-US/MPL/2.0/FAQ/>.

### 2.3 EPL-2.0 (39 crates)

Every EPL-2.0 crate reaches the tree through the Zenoh transport stack: 29 from the git-pinned fork `https://github.com/BOURBONCASK/zenoh.git` and 10 from crates.io (`keyed-set`, `ringbuffer-spsc`, `stabby`, `stabby-abi`, `stabby-macros`, `token-cell`, `uhlc`, `validated_struct`, `validated_struct_macros`, `z-serial`). All 39 carry the same declared offer — `Apache-2.0 OR EPL-2.0` — so Apache-2.0 is available as an alternative to EPL-2.0 for the whole set, and that is the choice this inventory records.

If the EPL-2.0 side is ever exercised instead, note that EPL-2.0 is a file-level weak copyleft whose obligations attach to distribution of the covered files and their modifications; its "Secondary License" mechanism (§1, Exhibit A) is specifically a GPL-compatibility option that a contributor must affirmatively grant, not a general permission to link under any compatible license. Zenoh is the core transport and the single largest third-party surface here — flag it for legal on that ground. See <https://www.eclipse.org/org/documents/epl-2.0/EPL-2.0.html>.

### 2.4 Git-pinned sources (supply chain)

| Source | Rev | Crates |
|---|---|---|
| `https://github.com/BOURBONCASK/zenoh.git` | `99ae7b413e0414ea02ba4bbd8aaf94df87a4a862` | 29 — Zenoh fork (`zenoh`, `zenoh-*`, `zenoh-config`, `zenoh-util`) |
| crates.io | — | 601 |

Each fork rev above is the one recorded in `[workspace.dependencies]` (`//:Cargo.toml`) and mirrored in `MODULE.bazel`'s `crate.spec(git=...)` pins for the Bazel-only crates; `deny.toml [sources] allow-git` is the list of git remotes permitted at all.

### 2.5 AWS-LC composite licensing

The Rustls dependency closure pulls 2 AWS-LC crates into `Cargo.lock` (2 of the 2 outside cargo-deny's graph for the selected workspace features). Every one is part of the full lock snapshot and must not be omitted from legal review:

| Crate | Version | Declared expression | In deny graph | Legal review note |
|---|---|---|---|---|
| `aws-lc-rs` | 1.18.0 | `ISC AND (Apache-2.0 OR ISC)` | no | Conjunctive: the ISC term and one license from the parenthesized alternative both apply. |
| `aws-lc-sys` | 0.44.0 | `ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)` | no | Multi-origin vendored cryptography code; every top-level `AND` term applies. The crate's bundled `LICENSE` identifies AWS-LC as a BoringSSL fork, BoringSSL as an OpenSSL fork, OpenSSL-derived code, and original SSLeay copyright retained on some files. It also inventories MIT, BSD-3-Clause, and other third-party components. Preserve the bundled license/attribution material in release notices and have legal review the complete expression and provenance chain. |

No deny allowlist expansion is needed while these stay outside the graph, and
each required term has an already-approved permissive path if that feature path
becomes active. In particular, the final `MIT-0` occurrence is an `OR`
alternative; `MIT-0` and `CC0-1.0` remain deliberately absent from `deny.toml`
and will still require a justified, crate-specific review if a future resolved
graph needs either license rather than another allowed branch.

### 2.6 Conjunctive (AND) expressions — full audit

`AND` means **both** licenses apply simultaneously; treating them as
alternatives would understate obligations. Every crate in the full lock graph
whose declared expression contains `AND` (6 total) is listed below; the
`license` column preserves the exact structure.

| Crate | Version | Declared expression | Previously reported as | In deny graph |
|---|---|---|---|---|
| aws-lc-rs | 1.18.0 | `ISC AND (Apache-2.0 OR ISC)` | new at the 2026-08 baseline | no |
| aws-lc-sys | 0.44.0 | `ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)` | new at the 2026-08 baseline | no |
| finl_unicode | 1.4.0 | `(MIT OR Apache-2.0) AND Unicode-DFS-2016` | already correct (declared fallback) | no |
| ring | 0.17.14 | `Apache-2.0 AND ISC` | Apache-2.0 OR ISC (WRONG — fixed) | yes |
| unicode-ident | 1.0.24 | `(MIT OR Apache-2.0) AND Unicode-3.0` | Apache-2.0 OR MIT OR Unicode-3.0 (WRONG — fixed) | yes |
| wezterm-bidi | 0.2.3 | `MIT AND Unicode-DFS-2016` | already correct (declared fallback) | no |

A scan for any remaining flattened `AND` (declared contains `AND`, license column does not) reports **0**.
