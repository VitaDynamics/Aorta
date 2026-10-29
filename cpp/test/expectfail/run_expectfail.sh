#!/usr/bin/env bash
# Expect-fail gate over the REAL flow headers (model:
# benchmark/flow/ws1_spike/run_gcc11_spike.sh, which proves the same
# shapes against a miniature model — THIS script proves them against
# cpp/include/aorta/flow*.hpp itself).
#
# A requires-probe cannot prove the rejection (the teaching fallback makes
# the await expression well-formed to a requires-expression), so each
# negative TU is actually compiled and must FAIL with its teaching text.
# The positive control compiles first so a negative failure can only be
# the static_assert, never a broken include path.
#
# Prereq: the cbindgen header —  bazel build //core:aorta_ffi_headers
# (or set AORTA_H_DIR to a directory containing aorta.h).
# Run manually (`bash cpp/test/expectfail/run_expectfail.sh`, needs a local
# `bazel build //core:aorta_ffi_headers` + //message/bindings/cpp:
# flow_resume_probe_msg_cc first — see the AORTA_H_DIR / MESSAGE_BIN_DIR /
# FLATBUFFERS_INC fallbacks below) or in the aorta-xbuild container.
#
# Bazel wiring: `bazel test
# //cpp:flow_expectfail_test` (cpp/BUILD.bazel) wraps this same script,
# supplying the compiler and every generated header path via `data` +
# the cc toolchain's $(CC) Make variable instead of assuming a local
# `bazel-bin` layout — see the AORTA_BAZEL_RUNFILES / FLATBUFFERS_H_FILE
# handling below. It is included in :cpp_flow_tests and runs in CI on both
# the default GCC toolchain and, as a separate step, Clang
# (`--repo_env=CC=clang --repo_env=CXX=clang++`).
#
# NOT wired into tools/release/validate_source_consumer.py: that validator
# spins up a STANDALONE external workspace (`local_path_override` + a
# `bazel_dep(name = "aorta")`) that only ever `bazel build`s the public,
# released target surface (source_release.PUBLIC_BAZEL_TARGETS plus one
# C++20 smoke TU) — it has no `bazel test` invocation and no notion of an
# internal, test-only sh_test at all, and this suite's negative TUs are
# compiled directly against internal generated schema headers (//message/
# bindings/cpp, //message/schemas/**) that are not part of that public
# surface. Folding it in would mean either exposing internal-only targets
# as public release surface (wrong layering) or duplicating this suite's
# compile-and-grep logic inside the release validator (two copies of one
# check) — so this suite stays a normal in-repo CI gate
# (:cpp_flow_tests / bazel-test.yml) instead.
#
# The negative/positive TU sets:
#
#   Set A — MUST reject TODAY (green characterization of the existing B1
#   endpoint-kind teaching gate): timeout()/race() wrapping a parked
#   endpoint verb (recv/aligned/wait_wave), explicit and ambient spellings.
#   Wired through check_negative() below, same as the foreign-
#   coroutine TUs — a compile success here fails the script.
#
#   Set B — GATING (D1 CHOKE GATE + BELT): every
#   Flow await surface is now a compile-time rejection under
#   -fno-exceptions, closing the -fno-exceptions gaps this set used to
#   characterize as RED (compiles-today) evidence. The CHOKE POINT is
#   Task<T,K>::promise_type's static_assert on instantiation (flow.hpp):
#   defining OR awaiting any flow::Task/EndpointTask coroutine trips it,
#   which is why noexc_task_await.cc / noexc_this_coro.cc /
#   noexc_nothrow_sleep.cc / noexc_recv.cc — each itself a
#   flow::Task<void> coroutine — reject on their OWN promise_type before
#   their probed await is ever reached. The BELT covers what the choke
#   point structurally cannot: a FOREIGN (non-flow) coroutine that never
#   instantiates Task's promise_type at all can still reach a
#   nothrow_awaiter or the explicit `sleep`'s await_resume directly —
#   noexc_foreign_nothrow_sleep_on.cc and noexc_explicit_sleep.cc are
#   that belt's evidence. Wired through check_negative_noexc() — a
#   compile SUCCESS here (the gate regressed) FAILS the script, and a
#   rejection for any OTHER reason than the choke/belt teaching text
#   also fails the script (do not paper over a differently-broken TU).
#
#   Set C (parameter-order API freeze) — the old pre-freeze
#   parameter-order spellings were DELETED (no deprecated aliases):
#   timeout()/race()/join_all() (plane, scope 2nd) and the raw call{}/
#   send_goal{}/wait_result{}/cancel_goal{} shapes ({obj, scope, exec,
#   ...}). join()/join_all() vs join() itself: join stays scope-2nd as
#   the RECORDED EXEMPTION (fixed param can't follow a deduced
#   pack); join_all has no such constraint and IS scope-last. Wired
#   through check_old_order() — GATING (a compile success = the old
#   order leaked back). The three action-verb TUs compile against the
#   REAL generated action headers (ACTION_FLAGS, prereq block above),
#   not a hand-rolled shim.
#
#   Set D — GRADUATED (Outcome<T>/Stopped contract landed): the final-API
#   Outcome<T>/Stopped contract TU (wp2_nondefault_outcome.cc) now
#   compiles against the real headers. It moved out of the RED
#   (check_red) section into the gating positive-compile check below —
#   a regression here (the TU failing to compile again) FAILS the script.
set -u

# Bazel wiring (cpp/BUILD.bazel :flow_expectfail_test):
# a single-src sh_test's runfiles executable is published at
# "<package>/<target_name>" (i.e. "cpp/flow_expectfail_test"), NOT at this
# file's real repo-nested path ("cpp/test/expectfail/run_expectfail.sh") —
# so the manual-run "$0"-relative navigation below (3 `cd ..` hops back to
# the repo root) would silently walk to the wrong directory under Bazel.
# Bazel's own test contract instead publishes TEST_SRCDIR/TEST_WORKSPACE as
# the runfiles root; the sh_test sets AORTA_BAZEL_RUNFILES=1 to select that
# path. Manual invocation never sets it, so that branch (and $0-relative
# navigation) is unchanged.
if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  REPO_ROOT="$TEST_SRCDIR/$TEST_WORKSPACE"
  cd "$REPO_ROOT/cpp/test/expectfail"
else
  cd "$(dirname "$0")"
  REPO_ROOT=$(cd ../../.. && pwd)
fi

# CXX is set by the sh_test to the cc toolchain's resolved compiler ($(CC),
# via `toolchains = ["@bazel_tools//tools/cpp:current_cc_toolchain"]`) so
# both the default GCC config and a `--repo_env=CC=clang` Clang run compile
# with the SAME toolchain Bazel itself resolved, not a hardcoded `g++`. $(CC)
# is a path relative to the runfiles root (REPO_ROOT here), same as
# FLATBUFFERS_H_FILE above — resolve it to absolute before use, since the
# script's cwd is cpp/test/expectfail, not REPO_ROOT, by the time $CXX runs.
CXX="${CXX:-g++}"
if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  case "$CXX" in
    /*) : ;; # already absolute
    *) CXX="$REPO_ROOT/$CXX" ;;
  esac
fi

# Generated headers land in the runfiles/sandbox tree at their logical
# repo-relative path (no `bazel-bin/` prefix — that prefix is `bazel
# build`'s LOCAL output-tree convention, not how runfiles mirror generated
# files), so the two REPO_ROOT-relative defaults below drop it under
# AORTA_BAZEL_RUNFILES=1.
if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  AORTA_H_DIR="${AORTA_H_DIR:-$REPO_ROOT/core/include}"
else
  AORTA_H_DIR="${AORTA_H_DIR:-$REPO_ROOT/bazel-bin/core/include}"
fi

if [ ! -f "$AORTA_H_DIR/aorta.h" ]; then
  echo "aorta.h not found under $AORTA_H_DIR — run:"
  echo "  bazel build //core:aorta_ffi_headers"
  echo "or set AORTA_H_DIR."
  exit 2
fi

FLAGS="-std=c++20 -fsyntax-only -I$REPO_ROOT/cpp/include -I$AORTA_H_DIR"
fail=0

# Action-verb old-order negatives (below) exercise the REAL generated
# action headers (same harness type as cpp/test/flow_action_test.cc:
# aorta::action::rcp::ExecuteTaskAction) instead of a hand-rolled stand-in
# — flow_action.hpp's `#include "action_common_generated.h"` is an
# unconditional, file-scope include (action.h also unconditionally pulls
# in node.h -> ... -> aorta_base_generated.h), so there is no way to reach
# flow_action.hpp without these Bazel-generated schema headers regardless
# of which Action type the TU itself uses. Prereq, beyond
# `bazel build //core:aorta_ffi_headers`:
#   bazel build //message/bindings/cpp:flow_resume_probe_msg_cc
# MESSAGE_BIN_DIR / FLATBUFFERS_INC may be overridden directly; by default
# FLATBUFFERS_INC is resolved via `bazel info execution_root` (robust to
# the Bzlmod-canonicalized `flatbuffers~` repo directory name, which is
# not a stable path to hardcode).
if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  MESSAGE_BIN_DIR="${MESSAGE_BIN_DIR:-$REPO_ROOT/message}"
else
  MESSAGE_BIN_DIR="${MESSAGE_BIN_DIR:-$REPO_ROOT/bazel-bin/message}"
fi
ACTION_HDR="$MESSAGE_BIN_DIR/bindings/cpp/flow_resume_probe_msg.h"
if [ ! -f "$ACTION_HDR" ]; then
  echo "$ACTION_HDR not found — run:"
  echo "  bazel build //message/bindings/cpp:flow_resume_probe_msg_cc"
  echo "or set MESSAGE_BIN_DIR."
  exit 2
fi
if [ -z "${FLATBUFFERS_INC:-}" ]; then
  if [ -n "${FLATBUFFERS_H_FILE:-}" ]; then
    # Bazel wiring: FLATBUFFERS_H_FILE is the sh_test's own $(location) of
    # a single real flatbuffers header (@flatbuffers//:include/flatbuffers/
    # flatbuffers.h), which Bazel resolves to a path relative to the
    # runfiles ROOT (== REPO_ROOT here, not the script's own cwd) regardless
    # of the Bzlmod-canonicalized external repo directory name — the same
    # robustness goal as the execution_root glob below, but without
    # invoking a nested `bazel` from inside a running bazel test action.
    FLATBUFFERS_INC="$(cd "$REPO_ROOT/$(dirname "$FLATBUFFERS_H_FILE")/.." && pwd)"
  else
    EXEC_ROOT=$(cd "$REPO_ROOT" && bazel info execution_root 2>/dev/null)
    # -d (not find -type d): the external/flatbuffers~ entry is a symlink,
    # which `find -type d` (without -L) does not match.
    FLATBUFFERS_INC=$(ls -d "$EXEC_ROOT"/external/flatbuffers*/include 2>/dev/null | head -1)
  fi
fi
if [ ! -f "$FLATBUFFERS_INC/flatbuffers/flatbuffers.h" ]; then
  echo "flatbuffers/flatbuffers.h not found under FLATBUFFERS_INC=$FLATBUFFERS_INC"
  echo "  — ensure 'bazel info execution_root' resolves and @flatbuffers is"
  echo "  fetched (any target build triggers the fetch), or set"
  echo "  FLATBUFFERS_INC explicitly."
  exit 2
fi
ACTION_FLAGS="$FLAGS -I$MESSAGE_BIN_DIR/bindings/cpp \
-I$MESSAGE_BIN_DIR/schemas/action/example \
-I$MESSAGE_BIN_DIR/schemas/shared/action_common \
-I$MESSAGE_BIN_DIR/schemas/system \
-I$MESSAGE_BIN_DIR/schemas/shared/service_common \
-I$FLATBUFFERS_INC"

echo "== toolchain: $($CXX --version | head -1)"
echo "== aorta.h:   $AORTA_H_DIR/aorta.h"
echo "== action hdrs: $ACTION_HDR"

# Positive control: the real headers + ambient surface must compile.
if $CXX $FLAGS positive_control.cc 2>/tmp/ws1_expectfail_pos.log; then
  echo "POSITIVE positive_control: PASS"
else
  echo "POSITIVE positive_control: FAIL (the real headers do not compile"
  echo "  in this environment — negative results below would be meaningless)"
  head -20 /tmp/ws1_expectfail_pos.log
  fail=1
fi

check_negative() {
  local tu="$1"
  local needle="$2"
  local out
  out=$($CXX $FLAGS "$tu" 2>&1)
  if [ $? -eq 0 ]; then
    echo "NEGATIVE $tu: FAIL (compiled — must be rejected)"
    fail=1
  elif echo "$out" | grep -q "$needle"; then
    echo "NEGATIVE $tu: PASS (rejected with teaching text)"
  else
    echo "NEGATIVE $tu: FAIL (rejected, but teaching text missing:"
    echo "  wanted: $needle)"
    echo "$out" | head -8
    fail=1
  fi
}

check_negative foreign_awaits_flow_task.cc \
  "awaiting a flow Task requires a flow coroutine"
check_negative foreign_awaits_this_coro.cc \
  "requires a flow coroutine"
check_negative foreign_awaits_ambient_verb.cc \
  "flow ambient verbs require a flow coroutine"
check_negative foreign_awaits_task_handle.cc \
  "co_await TaskHandle requires a flow coroutine"

# Set A: timeout()/race() over a parked endpoint
# verb — the B1 teaching gate, explicit and ambient spellings. Same needle
# mechanism as the foreign-coroutine TUs above.
check_negative timeout_wraps_recv.cc \
  "use recv_for / aligned_for / wait_wave_for"
check_negative race_wraps_wait_wave.cc \
  "use recv_for / aligned_for / wait_wave_for"
check_negative timeout_wraps_aligned.cc \
  "use recv_for / aligned_for / wait_wave_for"
check_negative timeout_wraps_ambient_recv.cc \
  "use recv_for / aligned_for / wait_wave_for"

# Set B (D1 CHOKE GATE + BELT, GATING): every
# Flow await surface must be REJECTED under -fno-exceptions, with the
# choke-gate teaching text ("C++ Flow requires exception support") as the
# needle. A compile SUCCESS here means the gate regressed — that fails
# the script, same shape as check_negative() above but compiled WITH
# -fno-exceptions (the base $FLAGS omit it, since every other check in
# this script wants exceptions ON).
check_negative_noexc() {
  local tu="$1"
  local needle="$2"
  local out
  out=$($CXX $FLAGS -fno-exceptions "$tu" 2>&1)
  if [ $? -eq 0 ]; then
    echo "NEGATIVE(noexc) $tu: FAIL (compiled under -fno-exceptions — must" \
         "be rejected; the D1 choke/belt gate regressed)"
    fail=1
  elif echo "$out" | grep -q "$needle"; then
    echo "NEGATIVE(noexc) $tu: PASS (rejected with the D1 teaching text)"
  else
    echo "NEGATIVE(noexc) $tu: FAIL (rejected, but teaching text missing:"
    echo "  wanted: $needle)"
    echo "$out" | head -8
    fail=1
  fi
}

# Choke-gate evidence: each TU is itself a flow::Task<void> coroutine, so
# Task's promise_type static_assert rejects it before the probed await
# inside is ever reached.
check_negative_noexc noexc_task_await.cc \
  "C++ Flow requires exception support"
check_negative_noexc noexc_this_coro.cc \
  "C++ Flow requires exception support"
check_negative_noexc noexc_nothrow_sleep.cc \
  "C++ Flow requires exception support"
check_negative_noexc noexc_recv.cc \
  "C++ Flow requires exception support"

# Belt evidence: a FOREIGN (non-flow) coroutine that never instantiates
# Task's promise_type at all — the choke point structurally cannot catch
# these; only the awaiter-level dependent static_asserts (flow.hpp's
# nothrow_awaiter types, flow_combinators.hpp's explicit `sleep` struct)
# do.
check_negative_noexc noexc_foreign_nothrow_sleep_on.cc \
  "C++ Flow requires exception support"
check_negative_noexc noexc_explicit_sleep.cc \
  "C++ Flow requires exception support"

# Set C (parameter-order API freeze): the old (rt, scope, ...)
# parameter orders were DELETED — these TUs must FAIL overload
# resolution. GATING (a compile success = the old order leaked back).
check_old_order() {
  local tu="$1"
  local extra_flags="${2:-}"
  local out
  if out=$($CXX $FLAGS $extra_flags "$tu" 2>&1); then
    echo "OLD-ORDER $tu: FAIL — the pre-freeze parameter order compiles" \
         "again (only the new parameter order may compile)"
    fail=1
    return
  fi
  # The "rejected for the expected reason" wording varies by compiler AND
  # by whether the callee is a plain function (timeout/race/join_all: GCC
  # and Clang both say "no matching function for call to"), a
  # non-template class constructor (flow::call: Clang instead says "no
  # matching constructor for initialization of"), or a class template
  # relying on deduction guides (send_goal/wait_result/cancel_goal: Clang
  # instead says "no viable constructor or deduction guide for deduction
  # of template arguments of", GCC says "class template argument
  # deduction failed"). Any one of these confirms overload/deduction
  # resolution rejected the call — not an unrelated break.
  if echo "$out" | grep -q "no matching function" || \
     echo "$out" | grep -q "no matching constructor" || \
     echo "$out" | grep -q "no viable constructor or deduction guide" || \
     echo "$out" | grep -q "deduction failed"; then
    echo "OLD-ORDER $tu: PASS (rejected — no matching overload)"
  else
    echo "OLD-ORDER $tu: FAIL — rejected for an unrelated reason:"
    echo "$out" | head -8
    fail=1
  fi
}

check_old_order old_order_timeout.cc
check_old_order old_order_race.cc
check_old_order old_order_call.cc
check_old_order old_order_join_all.cc

# Action verbs (send_goal/wait_result/cancel_goal): same old-order
# shape ({obj, scope, exec, ...}), against the REAL generated action
# headers (ACTION_FLAGS, see the prereq block above) — not a shim, so
# these prove the actual deduction-guide set the SDK ships.
check_old_order old_order_send_goal.cc "$ACTION_FLAGS"
check_old_order old_order_wait_result.cc "$ACTION_FLAGS"
check_old_order old_order_cancel_goal.cc "$ACTION_FLAGS"

# Set D, GRADUATED: the Outcome<T>/Stopped
# contract TU written against the FINAL Outcome/Stopped API. It used to be
# a RED (expected-fail) TU under the old aggregate Outcome; now that the
# contract has landed it is promoted into the GATING positive-compile suite — same
# shape as the positive_control check at the top of this script: a compile
# FAILURE here fails the whole script.
check_positive() {
  local tu="$1"
  local out
  if out=$($CXX $FLAGS "$tu" 2>&1); then
    echo "POSITIVE $tu: PASS (now compiles — graduated out of Set D/check_red)"
  else
    echo "POSITIVE $tu: FAIL (this translation unit must compile):"
    echo "$out" | head -20
    fail=1
  fi
}

check_positive wp2_nondefault_outcome.cc

# D1 abort-lint: the
# CANCELLATION/teardown-branch aborts scattered across the Flow headers
# are concentrated into ONE trap,
# `detail::flow_requires_exceptions_unreachable()` (flow.hpp) — every
# OTHER `std::abort` call site in these headers must be a sanctioned,
# individually-tagged loud-contract-failure, not a stray cancellation
# branch. This is a static grep, not a compile check: it runs regardless
# of the per-TU results above.
echo "== D1 abort-lint: grepping for stray std::abort in the flow headers"
# SIX headers (flow_runtime.hpp is now IN the sweep, not
# excluded — its self-teardown abort is matched by its own tag below,
# same as the other two, rather than being carved out of the count).
ABORT_LINT_HEADERS="$REPO_ROOT/cpp/include/aorta/flow.hpp \
$REPO_ROOT/cpp/include/aorta/flow_combinators.hpp \
$REPO_ROOT/cpp/include/aorta/flow_subscriber.hpp \
$REPO_ROOT/cpp/include/aorta/flow_readiness.hpp \
$REPO_ROOT/cpp/include/aorta/flow_action.hpp \
$REPO_ROOT/cpp/include/aorta/flow_runtime.hpp"
all_aborts=$(grep -n "std::abort" $ABORT_LINT_HEADERS)
# Exactly THREE sanctioned sites, each matched by ITS OWN tag needle on
# the SAME line as the `std::abort()` call (a drive-by rename of any tag,
# a moved tag, or a new untagged std::abort anywhere in the six headers
# changes these counts and fails the lint):
#   1. flow.hpp — the D1 choke point itself
#      (flow_requires_exceptions_unreachable).
#   2. flow.hpp — FatalOutcomeAccess: the Outcome wrong-side-access contract
#      failure (deliberate, death-tested, fires in ALL build modes).
#   3. flow_runtime.hpp — Runtime::Core::~Core self-teardown guard
#      (destroying the Runtime on its own dispatch thread would
#      self-join and std::terminate with no explanation otherwise).
tagged_choke=$(echo "$all_aborts" | grep "flow_requires_exceptions_unreachable")
tagged_fatal=$(echo "$all_aborts" | grep "FatalOutcomeAccess")
tagged_teardown=$(echo "$all_aborts" \
  | grep "self-teardown contract abort, D1 lint-exempt")
abort_offenders=$(echo "$all_aborts" \
  | grep -v "flow_requires_exceptions_unreachable" \
  | grep -v "FatalOutcomeAccess" \
  | grep -v "self-teardown contract abort, D1 lint-exempt")
choke_n=$(echo "$tagged_choke" | grep -c "std::abort")
fatal_n=$(echo "$tagged_fatal" | grep -c "std::abort")
teardown_n=$(echo "$tagged_teardown" | grep -c "std::abort")
total_n=$(echo "$all_aborts" | grep -c "std::abort")
abort_lint_ok=1
if [ "$choke_n" -ne 1 ]; then
  echo "ABORT-LINT: FAIL — expected exactly 1 flow_requires_exceptions_" \
       "unreachable abort in flow.hpp, found $choke_n"
  abort_lint_ok=0
fi
if [ "$fatal_n" -ne 1 ]; then
  echo "ABORT-LINT: FAIL — expected exactly 1 FatalOutcomeAccess abort" \
       "in flow.hpp, found $fatal_n (tag may be stale/renamed)"
  abort_lint_ok=0
fi
if [ "$teardown_n" -ne 1 ]; then
  echo "ABORT-LINT: FAIL — expected exactly 1 self-teardown abort in" \
       "flow_runtime.hpp tagged 'self-teardown contract abort, D1" \
       "lint-exempt', found $teardown_n"
  abort_lint_ok=0
fi
if [ -n "$abort_offenders" ]; then
  echo "ABORT-LINT: FAIL — std::abort outside the three sanctioned," \
       "individually-tagged sites (concentrate every CANCELLATION-branch" \
       "abort into flow_requires_exceptions_unreachable):"
  echo "$abort_offenders"
  abort_lint_ok=0
fi
if [ "$total_n" -ne 3 ]; then
  echo "ABORT-LINT: FAIL — expected exactly 3 std::abort sites across" \
       "the six flow headers, found $total_n"
  abort_lint_ok=0
fi
if [ "$abort_lint_ok" -eq 1 ]; then
  echo "ABORT-LINT: PASS (exactly 3 sanctioned std::abort sites: the D1" \
       "choke point, FatalOutcomeAccess, and flow_runtime.hpp's" \
       "self-teardown guard)"
else
  fail=1
fi

if [ $fail -eq 0 ]; then
  echo "== ws1 expectfail (real headers): ALL PASS (Set A + Set B choke/belt" \
       "gating + old-order negatives + wp2 positive + abort-lint)"
else
  echo "== ws1 expectfail (real headers): FAILURES"
fi
exit $fail
