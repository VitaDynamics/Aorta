#!/usr/bin/env bash
#
# `aorta --version` and `aorta -V` must print a traceable version string:
#
#   aorta <release-version> (<git-sha12>)
#   aorta 0.0.0+unstamped (<git-sha12|unknown>)
#
# The release label comes from `--stamp --embed_label=<v>`; without stamping
# the binary reports the explicit unstamped placeholder instead of a Cargo
# version that names no release.
set -euo pipefail

BIN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin=*) BIN="${1#*=}"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${BIN}" ]]; then
  echo "--bin=<path to aorta> is required" >&2
  exit 2
fi

# The version is `2026.9.99 (<sha12>)` when stamped and
# `0.0.0+unstamped (<sha12|unknown>)` otherwise.
VERSION_RE='^aorta ([0-9]{4}\.[0-9]+\.[0-9]+|0\.0\.0\+unstamped) \(([0-9a-f]{12}|unknown)\)$'

if ! LONG="$("${BIN}" --version 2>&1)"; then
  echo "ERROR: aorta --version exited non-zero: ${LONG}" >&2
  exit 1
fi
if ! SHORT="$("${BIN}" -V 2>&1)"; then
  echo "ERROR: aorta -V exited non-zero: ${SHORT}" >&2
  exit 1
fi
echo "aorta --version: ${LONG}"
echo "aorta -V:        ${SHORT}"

if ! printf '%s\n' "${LONG}" | grep -Eq "${VERSION_RE}"; then
  echo "ERROR: aorta --version does not match the version contract: ${LONG}" >&2
  exit 1
fi

if [[ "${SHORT}" != "${LONG}" ]]; then
  echo "ERROR: aorta -V (${SHORT}) differs from aorta --version (${LONG})" >&2
  exit 1
fi
