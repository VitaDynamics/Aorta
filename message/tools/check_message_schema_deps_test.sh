#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${TEST_SRCDIR}/${TEST_WORKSPACE}"
"${REPO_ROOT}/message/tools/check_message_schema_deps" \
  --repo-root "${REPO_ROOT}"
