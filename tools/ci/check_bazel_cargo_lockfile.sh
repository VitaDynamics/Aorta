#!/usr/bin/env bash
set -euo pipefail

readonly cargo_lock="${TEST_SRCDIR}/${TEST_WORKSPACE}/Cargo.lock"
readonly bazel_lock="${TEST_SRCDIR}/${TEST_WORKSPACE}/Cargo.bazel.lock"

package_count() {
  awk '
    BEGIN { RS = "" }
    /^\[\[package\]\]\nname = "direct-cargo-bazel-deps"(\n|$)/ { count++ }
    END { print count + 0 }
  ' "$1"
}

if [[ "$(package_count "${cargo_lock}")" -ne 0 ]]; then
  echo "Cargo.lock must not contain direct-cargo-bazel-deps" >&2
  exit 1
fi

if [[ "$(package_count "${bazel_lock}")" -ne 1 ]]; then
  echo "Cargo.bazel.lock must contain exactly one direct-cargo-bazel-deps package" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
readonly tmp_dir
trap 'rm -rf "${tmp_dir}"' EXIT

awk '
  function flush_package() {
    if (package !~ /^\[\[package\]\]\nname = "direct-cargo-bazel-deps"(\n|$)/) {
      printf "%s", package
    }
    package = ""
  }

  /^\[\[package\]\]$/ {
    flush_package()
    package = $0 "\n"
    in_package = 1
    next
  }

  in_package { package = package $0 "\n"; next }
  { print }

  END { flush_package() }
' "${bazel_lock}" > "${tmp_dir}/Cargo.bazel.lock"

if ! diff -u "${cargo_lock}" "${tmp_dir}/Cargo.bazel.lock"; then
  echo "Cargo.bazel.lock must match Cargo.lock except for direct-cargo-bazel-deps" >&2
  exit 1
fi
