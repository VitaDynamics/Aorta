#!/usr/bin/env bash
set -euo pipefail
SOURCE="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve().parent)' "$1")"
WORK="$(mktemp -d "${TEST_TMPDIR}/release_owner.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
mkdir "$WORK/ws"
# Run nested Bazel on a disposable copy; never edit the developer checkout.
for entry in "$SOURCE"/.[!.]* "$SOURCE"/*; do
    [[ -e "$entry" ]] || continue
    case "$(basename "$entry")" in .git|bazel-*) continue ;; esac
    cp -R "$entry" "$WORK/ws/"
done
BUILD="$WORK/ws/message/tools/tests/release_owner_probe/schemas/root/BUILD.bazel"
cp "$BUILD" "$WORK/original.bazel"
python3 - "$BUILD" <<'PATCH'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text().replace(', "//message/tools/tests/release_owner_probe/foreign:wrong.fbs"', '')
s = s.replace("cat $(location root.fbs) >/dev/null; sed 's|^|// |' $(location //message/tools/tests/release_owner_probe/foreign:wrong.fbs)", "sed 's|^|// |' $(location root.fbs)")
p.write_text(s)
PATCH
run_release() {
    (cd "$WORK/ws" && bazel --output_user_root="$WORK/bazel" build --lockfile_mode=off \
        //message/tools/tests/release_owner_probe/schemas/root:release --jobs=4 \
        --repository_cache="${BAZEL_REPOSITORY_CACHE:-/tmp/.cache/bazel-repository-cache}" \
        --disk_cache="${BAZEL_DISK_CACHE:-/tmp/.cache/bazel-disk-cache}") >"$1" 2>&1
}
if ! run_release "$WORK/control.log"; then
    cat "$WORK/control.log"
    exit 1
fi
echo "PASS: configured root-only control stages"
cp "$WORK/original.bazel" "$BUILD"
if run_release "$WORK/foreign.log"; then
    echo "FAIL: configured output with an unregistered foreign schema staged"
    exit 1
fi
if ! grep -q 'OUTPUT_OWNER' "$WORK/foreign.log"; then
    cat "$WORK/foreign.log"
    exit 1
fi
echo "PASS: configured expected-root plus unregistered foreign input rejected with OUTPUT_OWNER"
