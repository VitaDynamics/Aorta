#!/usr/bin/env bash
# Staged flatc BFBS generation — the executable half of bfbs_stage.bzl.
#
# Stages the root schema + its full transitive closure into one scratch
# tree at repo-canonical relative positions, then runs flatc entirely
# inside that tree with --bfbs-filenames pinned to the root's staged
# directory, making the bfbs's embedded path strings (hashed into
# SCHEMA_HASH_HEX, embedded into SDK binaries) byte-deterministic across
# sandbox layouts, platforms, checkout paths, and consumer contexts. See
# bfbs_stage.bzl's docstring for the full design rationale.
#
# Lives in its own script (not inline in the genrule cmd string) so the
# staging semantics — in particular the collision policy below — are
# directly unit-testable (bfbs_stage_collision_test.sh) instead of only
# observable through full builds.
#
# Collision policy: two DIFFERENT files whose canonical slots coincide
# (e.g. a consumer-local message/schemas/common.fbs and an external
# dep's external/<repo>/message/schemas/common.fbs — both strip to
# message/schemas/common.fbs) are a HARD ERROR: silently letting one
# overwrite the other would make the bfbs content depend on staging
# order. Byte-identical duplicates are allowed (diamond closures list
# the same file through several groups). The ROOT is the single
# documented exception: it is staged LAST and REPLACES whatever holds
# its slot, because a generated action aggregate legitimately supersedes
# its same-slot source counterpart (single writer, applied last —
# deterministic by construction).
#
# Usage:
#   bfbs_stage.sh <flatc> <mode...one arg> <out_dir> <root_src> <root_slot|-> \
#       <n_direct> <direct...> <closure...>
#   root_slot "-" derives the slot from the root's own canonical path.
set -euo pipefail

FLATC="$1"; MODE="$2"; OUT_DIR="$3"; ROOT_SRC="$4"; ROOT_SLOT="$5"; N_DIRECT="$6"
shift 6
DIRECT=()
for ((i = 0; i < N_DIRECT; i++)); do
  DIRECT+=("$1")
  shift
done
CLOSURE=("$@")

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/aorta_bfbs_stage.XXXXXX")"
trap 'rm -rf "$STAGE"' EXIT

stage_rel() {
  # Canonical staging slot: the file's repo-relative source path. Strip
  # the configuration prefix (generated files live under
  # bazel-out/<config>/bin/) and the external-repo prefix (in an
  # external Bzlmod consumer every resolved input sits under
  # external/<repo>/ while canonical slots never carry it).
  local rel="$1"
  case "$rel" in
    bazel-out/*/bin/*) rel="${rel#bazel-out/*/bin/}" ;;
  esac
  case "$rel" in
    external/*/*) rel="${rel#external/*/}" ;;
  esac
  printf '%s' "$rel"
}

for src in ${CLOSURE[@]+"${CLOSURE[@]}"}; do
  rel="$(stage_rel "$src")"
  dest="$STAGE/$rel"
  if [ -e "$dest" ]; then
    if ! cmp -s "$src" "$dest"; then
      echo "bfbs_stage.sh: staging collision at canonical slot '$rel':" >&2
      echo "  '$src' differs from the file already staged there —" >&2
      echo "  two different schema files map to the same repo-relative" >&2
      echo "  path (e.g. a consumer-local file vs an external dep's" >&2
      echo "  same-path file). Refusing to overwrite silently: the bfbs" >&2
      echo "  content would depend on staging order. Rename one of the" >&2
      echo "  conflicting files or restructure the dep graph." >&2
      exit 1
    fi
    continue
  fi
  mkdir -p "$(dirname "$dest")"
  cp "$src" "$dest"
done

if [ "$ROOT_SLOT" = "-" ]; then
  ROOT_SLOT="$(stage_rel "$ROOT_SRC")"
fi
ROOT_DIR_REL="$(dirname "$ROOT_SLOT")"
mkdir -p "$STAGE/$ROOT_DIR_REL"
# Documented single-writer replace (see collision policy above).
cp -f "$ROOT_SRC" "$STAGE/$ROOT_SLOT"

INCLUDE_FLAGS=(-I "$STAGE/$ROOT_DIR_REL")
for src in "$ROOT_SLOT" ${DIRECT[@]+"${DIRECT[@]}"}; do
  rel="$(stage_rel "$src")"
  INCLUDE_FLAGS+=(-I "$STAGE/$(dirname "$rel")")
  case "$rel" in
    schemas/*)
      INCLUDE_FLAGS+=(-I "$STAGE")
      ;;
    */schemas/*)
      INCLUDE_FLAGS+=(-I "$STAGE/${rel%%/schemas/*}")
      ;;
  esac
done

# shellcheck disable=SC2086 — MODE is intentionally a word-split flag list.
"$FLATC" $MODE --bfbs-filenames "$STAGE/$ROOT_DIR_REL" -o "$OUT_DIR" \
  "${INCLUDE_FLAGS[@]}" "$STAGE/$ROOT_SLOT"
