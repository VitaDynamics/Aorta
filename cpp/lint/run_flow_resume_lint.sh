#!/usr/bin/env bash
# The flow resume AST lint gate: compile the
# probe TU with the real toolchain, ast-dump it, and verify every
# coroutine_handle::resume() call sits inside an inventoried delivery
# thunk (allowlist), plus the C-API post-symbol count pins. Then the RED
# self-test: a scratch TU adding a bare out-of-inventory resume MUST
# fail the lint — pinning the gate itself.
#
# Wiring mirrors cpp/test/expectfail/run_expectfail.sh (Bazel runfiles:
# AORTA_BAZEL_RUNFILES=1, $(CC) via CXX, FLATBUFFERS_H_FILE anchor).
# Manual run: bash cpp/lint/run_flow_resume_lint.sh after
#   bazel build //core:aorta_ffi_headers //message/bindings/cpp:flow_resume_probe_msg_cc
set -u

if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  REPO_ROOT="$(pwd)"
  AORTA_H_DIR="${AORTA_H_DIR:-$REPO_ROOT/core/include}"
  MESSAGE_BIN_DIR="${MESSAGE_BIN_DIR:-$REPO_ROOT/message}"
else
  REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
  AORTA_H_DIR="${AORTA_H_DIR:-$REPO_ROOT/bazel-bin/core/include}"
  MESSAGE_BIN_DIR="${MESSAGE_BIN_DIR:-$REPO_ROOT/bazel-bin/message}"
fi
CXX="${CXX:-clang++}"
LINT_DIR="$REPO_ROOT/cpp/lint"
HDR_DIR="$REPO_ROOT/cpp/include/aorta"

if [ ! -f "$AORTA_H_DIR/aorta.h" ]; then
  echo "aorta.h not found under $AORTA_H_DIR — bazel build //core:aorta_ffi_headers"
  exit 2
fi
if [ -z "${FLATBUFFERS_INC:-}" ]; then
  if [ -n "${FLATBUFFERS_H_FILE:-}" ]; then
    FLATBUFFERS_INC="$(cd "$REPO_ROOT/$(dirname "$FLATBUFFERS_H_FILE")/.." && pwd)"
  else
    EXEC_ROOT=$(cd "$REPO_ROOT" && bazel info execution_root 2>/dev/null)
    FLATBUFFERS_INC=$(ls -d "$EXEC_ROOT"/external/flatbuffers*/include 2>/dev/null | head -1)
  fi
fi
if [ ! -f "$FLATBUFFERS_INC/flatbuffers/flatbuffers.h" ]; then
  echo "flatbuffers headers not found (FLATBUFFERS_INC=$FLATBUFFERS_INC)"
  exit 2
fi

# The lint must run on a CLANG ast-dump (GCC has no -ast-dump); when the
# configured toolchain is GCC, fall back to system clang++ for the
# SYNTAX-ONLY dump — the probe is never linked, so toolchain parity is
# not at stake, only the AST shape.
if ! "$CXX" --version 2>/dev/null | grep -qi clang; then
  CXX=clang++
fi

TMP="${TEST_TMPDIR:-$(mktemp -d)}"
FLAGS="-std=c++20 -fsyntax-only -Wno-comment \
-I$REPO_ROOT/cpp/include -I$AORTA_H_DIR \
-I$MESSAGE_BIN_DIR/bindings/cpp \
-I$MESSAGE_BIN_DIR/schemas/action/example \
-I$MESSAGE_BIN_DIR/schemas/shared/action_common \
-I$MESSAGE_BIN_DIR/schemas/service/example \
-I$MESSAGE_BIN_DIR/schemas/system \
-I$MESSAGE_BIN_DIR/schemas/shared/service_common \
-I$FLATBUFFERS_INC"

echo "== flow resume lint: probe ast-dump =="
# The probe TU is GENERATED from the live header tree (a hardcoded
# include list silently drops new headers from
# the AST surface — enumeration makes every in-tree header, current or
# future, part of the probe; a header that cannot compile in the probe
# TU fails the lint loudly instead of vanishing from scope).
( cd "$REPO_ROOT/cpp/include" \
  && find aorta \( -name "*.h" -o -name "*.hpp" \) | sort \
  | sed 's|^|#include <|; s|$|>|' ) > "$TMP/probe.cc"
if [ ! -s "$TMP/probe.cc" ]; then
  echo "probe generation found no headers under cpp/include/aorta"
  exit 2
fi
# The dump's exit status GATES the run (clang emits
# a partial AST on fatal include errors — silently linting a
# truncated surface is a fail-open).
# shellcheck disable=SC2086
if ! "$CXX" $FLAGS -Xclang -ast-dump "$TMP/probe.cc" \
  > "$TMP/ast.txt" 2> "$TMP/ast_err.txt"; then
  echo "probe failed to parse (exit status nonzero) — the lint surface"
  echo "would be truncated; refusing to lint a partial AST:"
  cat "$TMP/ast_err.txt"
  exit 2
fi
if [ ! -s "$TMP/ast.txt" ]; then
  echo "ast-dump produced no output despite success status"
  exit 2
fi
python3 "$LINT_DIR/flow_resume_lint.py" "$TMP/ast.txt" \
  "$LINT_DIR/flow_resume_allowlist.txt" "$HDR_DIR" || exit 1

# RED/GREEN self-tests (every detection shape is pinned by
# its own rogue TU, and one benign shape must stay clean). Rogue code
# lives under a fake in-tree include dir ($TMP/red_root/include/aorta/)
# so the PATH-prefix scope sees it exactly as it would a shipped header.
red_compile() {
  local main_body="$1" helper_body="$2"
  mkdir -p "$TMP/red_root/include/aorta"
  printf '%s\n' "$main_body" \
    > "$TMP/red_root/include/aorta/flow_red_probe.hpp"
  if [ -n "$helper_body" ]; then
    printf '%s\n' "$helper_body" \
      > "$TMP/red_root/include/aorta/resume_helper.hpp"
  else
    rm -f "$TMP/red_root/include/aorta/resume_helper.hpp"
  fi
  cat > "$TMP/red_tu.cc" <<EOF
#include <aorta/flow.hpp>
#include <aorta/flow_red_probe.hpp>
EOF
  # shellcheck disable=SC2086
  "$CXX" $FLAGS -I"$TMP/red_root/include" -Xclang -ast-dump \
    "$TMP/red_tu.cc" > "$TMP/red_ast.txt" 2> "$TMP/red_err.txt"
}

run_red() {
  local label="$1" want="$2" body="$3" helper="${4:-}"
  if ! red_compile "$body" "$helper"; then
    echo "RED[$label]: probe failed to compile:"; cat "$TMP/red_err.txt"
    exit 1
  fi
  if python3 "$LINT_DIR/flow_resume_lint.py" "$TMP/red_ast.txt" \
    "$LINT_DIR/flow_resume_allowlist.txt" "$HDR_DIR" \
    > "$TMP/red_out.txt" 2>&1; then
    echo "RED[$label] FAILED: the rogue site was NOT flagged"
    exit 1
  fi
  if ! grep -q "$want" "$TMP/red_out.txt"; then
    echo "RED[$label] FAILED: violation did not name '$want'"
    cat "$TMP/red_out.txt"
    exit 1
  fi
  echo "RED[$label]: flagged as expected"
}

run_green() {
  local label="$1" body="$2"
  if ! red_compile "$body" ""; then
    echo "GREEN[$label]: probe failed to compile:"; cat "$TMP/red_err.txt"
    exit 1
  fi
  if ! python3 "$LINT_DIR/flow_resume_lint.py" "$TMP/red_ast.txt" \
    "$LINT_DIR/flow_resume_allowlist.txt" "$HDR_DIR" \
    > "$TMP/red_out.txt" 2>&1; then
    echo "GREEN[$label] FAILED: benign shape was wrongly flagged:"
    cat "$TMP/red_out.txt"
    exit 1
  fi
  echo "GREEN[$label]: correctly clean"
}

echo "== flow resume lint: RED/GREEN self-tests =="
run_red "member-call" "rogue_member" \
  'inline void rogue_member(std::coroutine_handle<> h) { h.resume(); }'
# Arrow access through a pointer-to-handle.
run_red "arrow-member-call" "rogue_arrow" \
  'inline void rogue_arrow(std::coroutine_handle<>* hp) { hp->resume(); }'
run_red "operator()-call" "rogue_opcall" \
  'inline void rogue_opcall(std::coroutine_handle<> h) { h(); }'
# The explicit operator() spelling is a plain
# CXXMemberCallExpr, not CXXOperatorCallExpr.
run_red "explicit-operator-call" "rogue_explicit_op" \
  'inline void rogue_explicit_op(std::coroutine_handle<> h) { h.operator()(); }'
# Dependent-ONLY: the template is never
# instantiated, so ONLY the CXXDependentScopeMemberExpr branch can
# catch it — deleting that branch turns this row red.
run_red "dependent-call-only" "rogue_dependent" \
  'template <typename P> void rogue_dependent(std::coroutine_handle<P> h) { h.resume(); }'
run_red "foreign-namespace-thunk" "rogue_ns::deliver_unbound_thunk" \
  'namespace rogue_ns { inline void deliver_unbound_thunk(std::coroutine_handle<> h) { h.resume(); } }'
# Indirect-laundering reproductions:
run_red "std-invoke" "rogue_invoke" \
  '#include <functional>
   inline void rogue_invoke(std::coroutine_handle<> h) { std::invoke(h); }'
run_red "member-pointer" "rogue_memptr" \
  'inline void rogue_memptr(std::coroutine_handle<> h) {
     auto p = &std::coroutine_handle<>::resume; (h.*p)(); }'
run_red "in-tree-helper" "rogue_helper" \
  '#include <aorta/resume_helper.hpp>
   inline void rogue_caller(std::coroutine_handle<> h) { rogue_helper(h); }' \
  'inline void rogue_helper(std::coroutine_handle<> h) { h.resume(); }'
# A NESTED in-tree helper is in scope too.
mkdir -p "$TMP/red_root/include/aorta/detail"
printf '%s\n' \
  'inline void rogue_nested(std::coroutine_handle<> h) { h.resume(); }' \
  > "$TMP/red_root/include/aorta/detail/resume_helper.hpp"
run_red "nested-in-tree-helper" "rogue_nested" \
  '#include <aorta/detail/resume_helper.hpp>
   inline void rogue_nested_caller(std::coroutine_handle<> h) { rogue_nested(h); }'
rm -rf "$TMP/red_root/include/aorta/detail"
# Adjacent same-name NAMESPACES must
# NOT fold into the allowlisted context.
run_red "nested-samename-namespace" "aorta::aorta" \
  'namespace aorta { namespace aorta { namespace flow { namespace detail {
   struct ArbiterFrame {
     static void deliver_plain(std::coroutine_handle<> h) { h.resume(); }
   }; }}}}'
# A functor merely TAKING a handle is benign.
run_green "functor-taking-handle" \
  'struct Sink { void operator()(std::coroutine_handle<>) const {} };
   inline void benign_sink(std::coroutine_handle<> h) { Sink{}(h); }'
# A foreign type carrying a handle as a TEMPLATE
# ARGUMENT is benign — .resume()/operator() on it must stay clean.
run_green "template-arg-resume" \
  'template <typename T> struct Foreign { void resume() {} };
   inline void benign_foreign(Foreign<std::coroutine_handle<>>& f) { f.resume(); }'
run_green "template-arg-functor" \
  'template <typename T> struct SinkT { void operator()() const {} };
   inline void benign_sinkt(const SinkT<std::coroutine_handle<>>& s) { s(); }'
# An UNRELATED std::invoke next to a handle argument
# of a DIFFERENT callee must not pair into a D4 hit.
run_green "unrelated-invoke" \
  '#include <functional>
   inline void consume(int, std::coroutine_handle<>) {}
   inline void benign_invoke(std::coroutine_handle<> h) {
     consume(std::invoke([] { return 1; }), h); }'
# A handle buried in a RECEIVER-CONSTRUCTING call is
# not the receiver — Foreign returned by make_foreign(h) stays benign.
run_green "receiver-constructing-call" \
  'struct Foreign2 { void resume() {} };
   inline Foreign2 make_foreign(std::coroutine_handle<>) { return {}; }
   inline void benign_receiver(std::coroutine_handle<> h) {
     make_foreign(h).resume(); }'
# An invoke NESTED INSIDE the callee subtree is not
# this call'"'"'s callee — choose(std::invoke(...))(h) stays benign.
run_green "nested-callee-invoke" \
  '#include <functional>
   inline void sink_fn(std::coroutine_handle<>) {}
   using SinkFn = void (*)(std::coroutine_handle<>);
   inline SinkFn choose(int) { return &sink_fn; }
   inline void benign_nested_callee(std::coroutine_handle<> h) {
     choose(std::invoke([] { return 1; }))(h); }'

# Count pins key by RELATIVE PATH — a nested
# same-basename header cannot shadow a root pin. Empty AST (contexts
# out of play), fake tree with a post token only in detail/flow.hpp,
# allowlist with no pins → must FAIL naming detail/flow.hpp.
echo "== flow resume lint: count-pin relative-path red =="
mkdir -p "$TMP/pin_tree/detail"
printf '// clean\n' > "$TMP/pin_tree/flow.hpp"
printf 'inline void f() { aorta_executor_post_resume(0, 0, 0, 0); }\n' \
  > "$TMP/pin_tree/detail/flow.hpp"
: > "$TMP/empty_ast.txt"
printf '# no pins\n' > "$TMP/pin_allow.txt"
if python3 "$LINT_DIR/flow_resume_lint.py" "$TMP/empty_ast.txt" \
  "$TMP/pin_allow.txt" "$TMP/pin_tree" > "$TMP/pin_out.txt" 2>&1; then
  echo "RED[nested-basename-pin] FAILED: shadowed post site not flagged"
  exit 1
fi
if ! grep -q "detail/flow.hpp" "$TMP/pin_out.txt"; then
  echo "RED[nested-basename-pin] FAILED: violation did not name the nested path"
  cat "$TMP/pin_out.txt"
  exit 1
fi
echo "RED[nested-basename-pin]: flagged as expected"
echo "flow resume lint PASSED"
