# Aorta SBOM (Software Bill of Materials)

Open-source readiness artifacts for legal review. Generated from the workspace
at commit `2678465b` (branch `chore/flatbuffers-25.12.19`). That commit records where the reports were
generated, not which release ships them: a release may carry them forward
unchanged only while its lockfiles resolve the same dependencies, which
`--check` (see Regeneration) verifies.

## Contents

| File | What it is |
|---|---|
| [`rust-crates.csv`](rust-crates.csv) | Full third-party Rust dependency list: name, version, **resolved license expression (declared SPDX preserved verbatim, `AND` terms never flattened)**, raw declared field, source (git revs annotated), direct/transitive, and whether cargo-deny's graph reached the crate. |
| [`rust-license-summary.md`](rust-license-summary.md) | License-group statistics table (plain OR lists sorted, every other expression grouped as declared) + **legal review focus items** (copyleft, conjunctive `AND` audit, MPL-2.0, EPL-2.0, git-pinned sources). |
| [`bazel-modules.csv`](bazel-modules.csv) | Bzlmod module closure — **all 9 direct modules (incl. the 3 overridden ones the lockfile never records)** + 55 resolved transitive modules, per-row `origin`, override kind, patch flags, source detail. |
| [`bazel-summary.md`](bazel-summary.md) | bazel_dep list with override/patch details (aorta_schema_module_fixture local_path_override, flatbuffers archive_override, hedron_compile_commands git_override, rules_rust single_version_override + patch) and lockfile provenance note. |

## Scope and method

- **Rust**: 630 third-party crates resolved from `Cargo.lock` (14 private
  `aorta_*` workspace members excluded, matching `deny.toml`'s `[licenses]
  private.ignore`). The `license` column preserves each crate's **declared
  SPDX expression verbatim — including conjunctive `AND` terms** (`Apache-2.0
  AND ISC` means *both* licenses apply; it is never rewritten to an `OR`
  choice). 37 crates declare the deprecated slash form (`MIT/Apache-2.0`),
  which is not parseable SPDX. Inside cargo-deny 0.19.6's graph the form is
  normalized to an `OR` expression from the DECLARATION itself, with the
  discovered license set used only as corroboration — a disagreement becomes a
  review item rather than a guess (29 crates). Outside that graph nothing
  corroborates the split, so the declaration is kept VERBATIM in both the
  `license` and `declared` columns (8 crates). A package that declares nothing
  and shows more than one discovered license is a review item too, not a
  guess. A crate cargo-deny's graph did not reach keeps its declared SPDX
  field and is flagged in the CSV. That graph is the one cargo-deny builds for
  the selected workspace features with NO target filter (`deny.toml` sets no
  `targets`), so membership is not evidence that a crate compiles on any
  particular host. Summary statistics reorder only a plain `A OR B` list; any
  expression with `AND`, parentheses or a slash is grouped exactly as
  declared.
- **Bazel**: the 9 public direct `bazel_dep` from `MODULE.bazel` (incl. the 3
  overridden modules — `aorta_schema_module_fixture` local_path_override,
  `flatbuffers` archive_override, `hedron_compile_commands` git_override —
  which `MODULE.bazel.lock` never records because their sources come from the
  override targets), plus the 55 transitive modules resolved from
  `MODULE.bazel.lock` `source.json` entries. **Closure total: 64.**

## Regeneration

These five files are **pre-generated reports**. Reading them needs nothing; regenerating them is a MAINTAINER operation that runs in the full source checkout, which carries the generator, `Cargo.lock` and the Bazel lockfile. The generator is not part of the published distribution, so the commands below are for maintainers working in that checkout — everyone else consumes the committed reports as they stand.

```bash
# Rewrite every file under docs/sbom/ (needs cargo + cargo-deny on PATH)
python3 tools/sbom/gen_sbom.py

# Verify the committed files still match a fresh generation
python3 tools/sbom/gen_sbom.py --check

# Expression/parser regressions (no cargo, no network)
python3 -m unittest discover -s tools/sbom
```

The generator shells out to `cargo metadata --format-version 1 --all-features
--locked` and `cargo deny --all-features list -f json --layout license`, and reads
`MODULE.bazel` + `MODULE.bazel.lock` directly for the Bazel side (see
`bazel-summary.md` §4 for why the committed lockfile is the stated source). It is
stdlib-only and deliberately NOT a `bazel test`: both inputs need `cargo` and a
populated registry cache, which no hermetic test may assume.

Legal narrative that cannot be derived from the two tools (why a copyleft crate
passes the gate, the AWS-LC provenance chain, per-expression notes) lives in the
`CURATED_*` tables at the top of the generator. A crate that reaches a legal-review
section without a curated entry is rendered as a `TODO(legal)` placeholder, so
`--check` fails loudly instead of quietly dropping the review item.

## Verification

This report is an inventory, not a gate result. The gate is a separate command,
and it must be RUN — this file cannot attest to it, because the generator only
invokes `cargo deny list`:

```bash
cargo deny --all-features check sources licenses bans
```

Expected: `bans ok, licenses ok, sources ok`. A failure there is authoritative
over anything written here.
