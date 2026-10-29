#!/usr/bin/env bash
# Unit test for bfbs_stage.sh's staging semantics — in particular the
# collision policy: two DIFFERENT files whose
# canonical slots coincide (consumer-local message/schemas/common.fbs vs
# an external dep's external/foo~/message/schemas/common.fbs) must be a
# HARD ERROR, never a silent staging-order-dependent overwrite.
# Red/green history: the initial script port (faithful to the old inline
# cp -f loop) PASSED the collision case silently; the content check turned
# it green.
set -euo pipefail

# Absolutize before any cd: under `bazel test` the arg is a
# runfiles-RELATIVE path, and every invocation below happens from
# inside $WORK.
STAGE_SH="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bfbs_stage_test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

# Stub flatc: records that it ran and with which args; emits a fake output.
cat > flatc_stub.sh <<'EOF'
#!/usr/bin/env bash
echo "$@" > flatc_invoked.txt
# Record the staged root's CONTENT (last arg) so callers can assert WHICH
# file won a same-slot replace, not merely that flatc ran (an argv-only
# check could pass even if the root copy were dropped).
for last; do :; done
cat "$last" > flatc_root_content.txt 2>/dev/null || true
exit 0
EOF
chmod +x flatc_stub.sh

fail() { echo "FAIL: $1" >&2; exit 1; }

# --- Case 1: local-vs-external same canonical slot, DIFFERENT content -> hard error.
mkdir -p message/schemas external/foo~/message/schemas
echo "table A {}" > message/schemas/common.fbs
echo "table B {}" > external/foo~/message/schemas/common.fbs
mkdir -p pkg out1
echo "table Root {}" > pkg/root.fbs
rm -f flatc_invoked.txt
if "$STAGE_SH" ./flatc_stub.sh "--binary --schema" out1 pkg/root.fbs - 0 \
    message/schemas/common.fbs "external/foo~/message/schemas/common.fbs" \
    2> stderr1.txt; then
  fail "collision (different content, same slot) must exit nonzero"
fi
grep -q "staging collision" stderr1.txt || fail "collision error message missing"
[ ! -f flatc_invoked.txt ] || fail "flatc must not run after a collision"

# --- Case 2: same slot, IDENTICAL content (diamond closure) -> allowed, flatc runs.
echo "table A {}" > external/foo~/message/schemas/common.fbs
rm -f flatc_invoked.txt
"$STAGE_SH" ./flatc_stub.sh "--binary --schema" out1 pkg/root.fbs - 0 \
  message/schemas/common.fbs "external/foo~/message/schemas/common.fbs" \
  || fail "identical duplicate staging must succeed"
[ -f flatc_invoked.txt ] || fail "flatc should have run for the identical-duplicate case"

# --- Case 3: ROOT replaces its same-slot source counterpart (generated
# aggregate over source — the single documented exception) -> allowed,
# and the staged root is the ROOT's content.
mkdir -p "bazel-out/cfg-fastbuild/bin/pkg2/agg_action" pkg2
echo "table Src {}" > pkg2/thing.fbs
echo "table Agg {}" > "bazel-out/cfg-fastbuild/bin/pkg2/agg_action/thing.fbs"
rm -f flatc_invoked.txt
"$STAGE_SH" ./flatc_stub.sh "--binary --schema" out1 \
  "bazel-out/cfg-fastbuild/bin/pkg2/agg_action/thing.fbs" "pkg2/thing.fbs" 0 \
  pkg2/thing.fbs \
  || fail "root replacing its same-slot source must succeed"
[ -f flatc_invoked.txt ] || fail "flatc should have run for the root-replace case"
grep -q "pkg2/thing.fbs" flatc_invoked.txt || fail "flatc argv should reference the staged root slot"
# The decisive assertion: the file AT the root slot must be
# the ROOT's (aggregate) content, not the closure-staged source's.
grep -q "table Agg" flatc_root_content.txt || fail "staged root slot must hold the ROOT (aggregate) content"
if grep -q "table Src" flatc_root_content.txt; then fail "staged root slot still holds the closure source content"; fi

echo "PASS"
