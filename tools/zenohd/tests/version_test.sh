#!/usr/bin/env bash
#
# `zenohd --version` must print the same traceable version identity as the
# aorta CLI: the release label stamped with `--stamp --embed_label=<v>`, or
# `0.0.0+unstamped`, plus the git commit. The long form may keep the upstream
# `built with <rustc>` suffix.
set -euo pipefail

BIN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin=*) BIN="${1#*=}"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${BIN}" ]]; then
  echo "--bin=<path to zenohd> is required" >&2
  exit 2
fi

VERSION='([0-9]{4}\.[0-9]+\.[0-9]+|0\.0\.0\+unstamped) \(([0-9a-f]{12}|unknown)\)'
LONG_RE="^zenohd ${VERSION}( built with .+)?$"
SHORT_RE="^zenohd ${VERSION}$"

if ! LONG="$("${BIN}" --version 2>&1)"; then
  echo "ERROR: zenohd --version exited non-zero: ${LONG}" >&2
  exit 1
fi
if ! SHORT="$("${BIN}" -V 2>&1)"; then
  echo "ERROR: zenohd -V exited non-zero: ${SHORT}" >&2
  exit 1
fi
echo "zenohd --version: ${LONG}"
echo "zenohd -V:        ${SHORT}"

if ! printf '%s\n' "${LONG}" | grep -Eq "${LONG_RE}"; then
  echo "ERROR: zenohd --version does not match the version contract: ${LONG}" >&2
  exit 1
fi

if ! printf '%s\n' "${SHORT}" | grep -Eq "${SHORT_RE}"; then
  echo "ERROR: zenohd -V does not match the version contract: ${SHORT}" >&2
  exit 1
fi
