"""Shared staged-flatc command builder for byte-deterministic BFBS output.

SCHEMA_HASH stabilization — single source of truth for
BOTH codegen tiers (schema_codegen.bzl Tier-1 registry pipeline and
aorta_flatbuffers.bzl Tier-2 consumer macros):

A bfbs embeds each type's `declaration_file` and the `fbs_files` include
graph as paths relativized against flatc's project root, and
`SCHEMA_HASH_HEX` is sha256 over the RAW bfbs bytes — so those strings
are part of the wire-level schema hash. Left to flatc's own resolution
the encoding is build-layout-dependent, in two independently observed
ways:

- An ACTION schema's root `.fbs` is a GENERATED aggregate living under
  `bazel-out/<config>/bin/...` while its includes are source files — two
  trees with no common prefix, so flatc stores every include as a
  `../`-chain-to-/ plus the checkout's symlink-RESOLVED ABSOLUTE path.
  The schema hash of every action schema (both tiers — Tier-1
  execute_task/data_upload AND every Tier-2 aorta_action_schema
  aggregate) thereby depended on the sandbox depth and the checkout
  location: two machines, or two checkouts on one machine, computed
  different hashes for the identical schema.
- In an external Bzlmod consumer (aorta via `local_path_override`, the
  release validator's setup), the same degradation hit EVERY schema, so
  externally-computed hashes never matched in-repo ones.

Fix by construction, not by fighting flatc's path-resolution internals:
stage the root + the full transitive closure into ONE scratch tree at
their REPO-CANONICAL relative positions (any `bazel-out/<config>/bin/`
configuration prefix and any `external/<repo>/` prefix stripped — a
generated action aggregate lands at the same canonical slot its source
counterpart owns), then run flatc entirely inside that tree with
`--bfbs-filenames` pinned to the root's staged directory. Every stored
string becomes a pure function of the repository's schema layout
(`//<stem>.fbs`, `//../../system/aorta_base.fbs`) — independent of
sandbox, platform output-dir naming, checkout path, and consumer
context.

The executable staging logic lives in `bfbs_stage.sh` (this file only
assembles its argv), so the semantics are directly unit-testable —
`bfbs_stage_collision_test.sh` pins the COLLISION POLICY: two DIFFERENT
files whose canonical slots coincide (consumer-local
`message/schemas/common.fbs` vs an external dep's
`external/<repo>/message/schemas/common.fbs`) hard-error instead of
silently overwriting in staging order; byte-identical duplicates are
allowed; the ROOT is the single documented replace exception (generated
aggregate over its same-slot source, staged last, single writer).

Hash-migration contract (TESTING.md carries the same
statement): source-rooted schemas keep their historical in-repo byte
encoding exactly (gated by pre/post byte-compare — zero migration);
generated-root (action) schemas' include entries change ONCE from the
per-machine degraded form to the canonical form. That is a REAL
wire-visible hash migration for the two Tier-1 registry actions
(`data_upload`, `execute_task`) — released artifacts carry a fixed old
hash, so a mixed old/new deployment disagrees on those two action
schemas' hashes until both sides are rebuilt — documented in the
proposal's compatibility analysis rather than papered over.
"""

# Label() anchors to THIS repository, so Tier-2 macros loaded from an
# external consumer still resolve the script inside aorta.
BFBS_STAGE_SCRIPT = Label("//message/tools:bfbs_stage.sh")

def flatc_bfbs_staged_cmd(
        mode,
        root_fbs,
        dep_fbs,
        closure_srcs,
        flatc,
        root_stage_rel = None,
        dedupe = None):
    """Genrule `cmd` invoking bfbs_stage.sh for deterministic BFBS.

    The caller's genrule MUST list `BFBS_STAGE_SCRIPT` in `tools`
    (alongside the flatc label) so `$(location)` can resolve it.

    Args:
      mode: flatc mode flags (e.g. "--binary --schema --bfbs-builtins").
      root_fbs: the root schema label/path (source or generated).
      dep_fbs: DIRECT dependency edges only — drives `-I` derivation
        (never the expanded closure).
      closure_srcs: the full transitive closure the genrule declares in
        `srcs` — flatc opens all of it, so all of it must be staged.
        Entries may be filegroups (expanded via `$(locations)`).
      flatc: the flatc tool label to `$(location)`.
      root_stage_rel: explicit staging slot for the root, repo-relative
        (Tier-1 passes `<package>/<stem>.fbs` because its generated
        action aggregate lives in an extra `<stem>_action/` subdirectory
        that must not leak into the canonical layout). None derives the
        slot from the root's own canonical path (all Tier-2 macros).
      dedupe: optional list-dedupe helper; defaults to a simple
        order-preserving dedupe.
    """
    if dedupe == None:
        dedupe = _dedupe

    direct = dedupe(dep_fbs)

    # `$(locations ...)` (plural) for the staged closure: Tier-2 passes
    # `{dep}_fbs_closure` FILEGROUP targets (each expanding to many
    # `.fbs` files). Direct `-I`-driving entries are always single-file
    # labels, so their count equals len(direct) after shell splitting.
    stage_locations = " ".join([
        "$(locations {})".format(src)
        for src in dedupe(closure_srcs)
    ])
    direct_locations = " ".join([
        "$(location {})".format(src)
        for src in direct
    ])

    return '$(location {script}) $(location {flatc}) "{mode}" $(@D) "$(location {root_fbs})" "{root_slot}" {n_direct} {direct_locations} {stage_locations}'.format(
        script = BFBS_STAGE_SCRIPT,
        flatc = flatc,
        mode = mode,
        root_fbs = root_fbs,
        root_slot = root_stage_rel if root_stage_rel != None else "-",
        n_direct = len(direct),
        direct_locations = direct_locations,
        stage_locations = stage_locations,
    )

def _dedupe(items):
    seen = {}
    out = []
    for item in items:
        if item in seen:
            continue
        seen[item] = True
        out.append(item)
    return out
