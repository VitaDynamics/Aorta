#!/usr/bin/env bash
#
# Bazel workspace status for stamped builds: the git commit the binaries were
# built from, whether the tree was dirty, and the rustc that compiled them.
# Consumed through `rustc_env` on //tools/aorta-cli:aorta and
# //tools/zenohd:zenohd (see `.bazelrc`).
#
# Never fails: an exported tree without `.git`, or a host without `rustc`,
# reports `unknown` so consumer and unstamped builds still link.
set -u

git_commit="unknown"
git_dirty="unknown"
if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
  git_commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  if status="$(git status --porcelain 2>/dev/null)"; then
    if [ -n "${status}" ]; then
      git_dirty="1"
    else
      git_dirty="0"
    fi
  fi
fi

rustc_version="unknown"
if command -v rustc >/dev/null 2>&1; then
  rustc_version="$(rustc --version 2>/dev/null || echo unknown)"
fi

echo "STABLE_GIT_COMMIT ${git_commit}"
echo "STABLE_GIT_DIRTY ${git_dirty}"
echo "STABLE_RUSTC_VERSION ${rustc_version}"
