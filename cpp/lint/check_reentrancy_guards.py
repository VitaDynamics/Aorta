#!/usr/bin/env python3
"""Reentrancy-guard completeness lint (simplified version).

The originally designed generator (a `syn` Rust binary emitting
`cpp/lint/reentrancy_inventory.json`) is deliberately simplified here to a
dependency-free python lexical scanner plus in-code annotations — same
one-generated-side guarantee (the guarded-entry set is still derived from
core sources at run time, never hand-maintained), no JSON artifact.

Three steps:

1. SEEDS — the guarded-entry set is regenerated from `core/src/ffi/c_api/*.rs`
   at run time: every `pub extern "C" fn` whose (comment-stripped) body
   contains one of the four guard markers (`ffi_block_on!`,
   `in_async_context()`, `explicitly_reentrant()`, `aorta_reentrant(`),
   function-granularity via brace balance. Plus a small hardcoded
   DELEGATING set: externs whose own bodies carry no marker but reach a
   guarded entry only through private helpers, or (class W) block with no
   core guard at all:
     - aorta_node_new / aorta_subscriber_new / aorta_timer_new — thin
       trampolines to the *_with_options constructors (which carry
       ffi_block_on!).
     - aorta_action_client_send_goal / _send_goal_traced /
       _query_status_with_timeout / _get_result_with_timeout — thin
       trampolines to the private action_client_*_impl helpers (which
       carry ffi_block_on!). `_send_goal_traced` is called DIRECTLY from
       C++ (action.h SendGoalPayloadTraced) and was added after the
       lint was found to fail open without it.
     - aorta_action_server_free — delegates to shutdown_ffi_action_server
       (the sync-or-detach helper; class F).
     - aorta_timer_free — class W: unconditional synchronous in-flight
       drain with NO marker anywhere on its path (the spec's
       "explicit semantic-seed: sync-drain" case; annotated in core).
   The delegating names are the ones the C++ SDK actually calls; names
   reachable only via function pointers are out of lint scope by the
   spec's function-pointer exclusion and the live reentrancy tests.

2. CALL SITES — scan `cpp/src/**.cc` and `cpp/include/aorta/**.h(pp)` for
   direct calls of seed symbols (`\bSYM\s*\(`, word-boundary so
   `aorta_node_new` does not match `aorta_node_new_with_options`; `extern
   "C"` declaration lines in internal/ffi.h are skipped; function-pointer
   arguments are not followed by `(` and are out of scope, per the spec).

3. COVERAGE — each call site must be either lexically preceded, within the
   same enclosing function body, by an `AORTA_CHECK_NOT_REENTRANT(` (the
   guard macro), or carry a `// reentrancy: <class>` annotation on the same
   line or the immediately preceding line (class F/N/probe etc. — the
   legitimate no-check exemptions). The enclosing function is found by a
   backward brace-depth scan for the nearest line that opens a body
   (`... ) {`, not a control-flow/declaration keyword, not a lambda
   opener) — a stated approximation of dominance; scanning stops at file
   top, so an unannotated site with no enclosing function is a violation.

4. PURE_CPP_WAITS — the pure-C++ waits have no C call for step 2 to see, so
   they are pinned explicitly: each (file, function) entry must still
   contain an `AORTA_CHECK_NOT_REENTRANT(` inside its body — deleting a
   guard OR renaming the function both fail. Covers the explicit waits
   (GoalHandle::WaitResultInner, FeedbackStream::Recv,
   flow::TaskHandle::wait_for) and the guarded destructor drain-waits.
   (ActionServer::~ActionServer is deliberately absent: its guard is the
   own-callback TLS shape — `HandleReentrantHit`, not the macro.)

Sanity gates (fail-closed against a silently broken scan): the repo root
and the scan directories must exist, the regenerated seed count must be
>= 60, and the scanned C++ file count must be >= 40.

Exit status is nonzero (listing every violation) when any call site is
uncovered. Run: python3 cpp/lint/check_reentrancy_guards.py [REPO_ROOT]
"""

import os
import re
import sys
from pathlib import Path

# --- seeds: regenerate the guarded-entry set from core sources ----------

RUST_MARKERS = (
    "ffi_block_on!",
    "in_async_context()",
    "explicitly_reentrant()",
    "aorta_reentrant(",
)

# Externs whose own bodies contain no marker but are guarded entries
# nonetheless — see the module docstring for the per-name rationale.
DELEGATING_SEEDS = (
    "aorta_node_new",
    "aorta_subscriber_new",
    "aorta_timer_new",
    "aorta_action_client_send_goal",
    "aorta_action_client_send_goal_traced",
    "aorta_action_client_query_status_with_timeout",
    "aorta_action_client_get_result_with_timeout",
    "aorta_action_server_free",
    "aorta_timer_free",
)

_FN_RE = re.compile(r'pub\s+(?:unsafe\s+)?extern\s+"C"\s+fn\s+(\w+)')

# Pure-C++ waits: no C call for the call-site scan to see, so each
# (file, function) is pinned explicitly — the function must exist AND
# contain an AORTA_CHECK_NOT_REENTRANT in its body. Deleting a guard or
# renaming the function both fail. ActionServer::~ActionServer is
# deliberately absent: its guard is the own-callback TLS shape
# (direct HandleReentrantHit), not the macro.
# Third field: minimum number of AORTA_CHECK_NOT_REENTRANT occurrences the
# function body must contain. WaitResultInner carries TWO guards (the class-B
# check before the subscribe FFI call plus the pure-wait check before the future
# wait); a plain any-macro-present test would stay green if only the wait
# guard were deleted, so the pin counts them.
PURE_CPP_WAITS = (
    ("cpp/include/aorta/action.h", "GoalHandle::WaitResultInner", 2),
    ("cpp/include/aorta/action.h", "FeedbackStream::Recv", 1),
    ("cpp/include/aorta/action.h", "FeedbackStream::Close", 1),
    ("cpp/include/aorta/action.h",
     "ActionFeedbackSubscription::~ActionFeedbackSubscription", 1),
    ("cpp/include/aorta/flow.hpp", "flow::TaskHandle::wait_for", 1),
    ("cpp/include/aorta/flow_runtime.hpp", "flow::Runtime::Core::~Core", 1),
    ("cpp/include/aorta/lanes.hpp", "LanePool::join", 1),
    ("cpp/src/subscriber.cc", "Subscriber::~Subscriber", 1),
    ("cpp/src/service.cc", "Service::~Service", 1),
    ("cpp/src/capture.cc", "CaptureSubscriber::~CaptureSubscriber", 1),
    ("cpp/src/timer.cc", "Timer::~Timer", 1),
    ("cpp/src/executor.cc", "Executor::~Executor", 1),
    ("cpp/src/node.cc", "Node::~Node", 1),
    ("cpp/src/contextstore.cc", "WatchHandle::~WatchHandle", 1),
)


def _strip_comments(text):
    """Remove // line and /* */ block comments, keeping strings intact."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            out.append(c)
            i += 1
            while i < n and text[i] != '"':
                if text[i] == "\\":
                    out.append(text[i])
                    i += 1
                if i < n:
                    out.append(text[i])
                    i += 1
            if i < n:
                out.append(text[i])
                i += 1
        elif c == "'":
            out.append(c)
            i += 1
            while i < n and text[i] != "'":
                if text[i] == "\\":
                    out.append(text[i])
                    i += 1
                if i < n:
                    out.append(text[i])
                    i += 1
            if i < n:
                out.append(text[i])
                i += 1
        elif c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                i += 1
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                if text[i] == "\n":
                    out.append("\n")  # keep line alignment
                i += 1
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _fn_body(text, name_end):
    """Brace-balanced body (exclusive of the outer braces) after `fn NAME`."""
    open_idx = text.find("{", name_end)
    if open_idx < 0:
        return None
    depth = 0
    for j in range(open_idx, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[open_idx + 1 : j]
    return None


def rust_seeds(c_api_dir):
    seeds = set(DELEGATING_SEEDS)
    for path in sorted(Path(c_api_dir).glob("*.rs")):
        text = _strip_comments(path.read_text(encoding="utf-8"))
        for m in _FN_RE.finditer(text):
            body = _fn_body(text, m.end())
            if body and any(marker in body for marker in RUST_MARKERS):
                seeds.add(m.group(1))
    return sorted(seeds)


# --- C++ side: call sites and coverage -----------------------------------

_CONTROL_OR_DECL = frozenset(
    "if for while switch do else catch try struct class enum union "
    "namespace case default".split()
)

_ANNOTATION_RE = re.compile(r"//\s*reentrancy:\s*(\S+)")


def _strip_cpp_line(line):
    """Comment/string-stripped line (C++ // and /* */, "..." and '...')."""
    return _strip_comments(line)


def _function_header(line):
    """True when `line` opens a function body: `... ) [qualifiers] {`
    (attributes and control-flow/lambda/declaration lines excluded)."""
    s = line.strip()
    while s.startswith("[["):  # [[nodiscard]] etc.
        end = s.find("]]")
        if end < 0:
            break
        s = s[end + 2 :].lstrip()
    if not s.endswith("{"):
        return False
    before = s[:-1].rstrip()
    # Strip trailing qualifiers (const/noexcept/override/ref-qualifiers).
    while True:
        parts = before.rsplit(None, 1)
        if len(parts) == 2 and parts[1] in ("const", "noexcept", "override", "&", "&&"):
            before = parts[0].rstrip()
        else:
            break
    if not before.endswith(")"):
        return False
    if "[" in before:  # lambda opener (`[&](...) {`, `[=] {`)
        return False
    first = before.split(None, 1)[0] if before.split(None, 1) else ""
    if first in _CONTROL_OR_DECL or first.startswith(("}", ";")):
        return False
    return True


def _file_analysis(lines):
    """Stripped lines + per-line brace depth (start-of-line depth, delta)."""
    # Strip the WHOLE file at once: per-line stripping would leave the
    # interior of multi-line /* ... */ comments un-stripped.
    stripped_text = _strip_comments("\n".join(lines))
    stripped = stripped_text.split("\n")
    depth_before = []
    depth = 0
    deltas = []
    for s in stripped:
        deltas.append(s.count("{") - s.count("}"))
        depth_before.append(depth)
        depth += deltas[-1]
    return stripped, depth_before, deltas


def _call_sites_in_file(lines, seeds):
    """Yield (line_no, seed_symbol) for direct calls; 1-based line numbers."""
    stripped, depth_before, deltas = _file_analysis(lines)
    sites = []
    for idx, line in enumerate(lines):
        s = stripped[idx]
        for seed in seeds:
            for m in re.finditer(r"\b" + re.escape(seed) + r"\s*\(", s):
                prefix = s[: m.start()]
                if "extern" in prefix:
                    continue  # C-ABI declaration, not a call
                sites.append((idx + 1, seed))
    return sites, stripped, depth_before, deltas


def _covered(raw_lines, stripped, depth_before, deltas, line_no):
    """True when the call site is covered by a check or an annotation."""
    idx = line_no - 1
    # Annotation on the same or immediately preceding line wins.
    for check_line in (idx, idx - 1):
        if check_line < 0:
            continue
        if _ANNOTATION_RE.search(raw_lines[check_line]):
            return True
    # Enclosing function: nearest backward line opening a body at a lower
    # brace depth than the call site.
    call_depth = depth_before[idx] + (deltas[idx] if deltas[idx] > 0 else 0)
    boundary = None
    for f in range(idx - 1, -1, -1):
        if _function_header(stripped[f]) and depth_before[f] < call_depth:
            boundary = f
            break
    window = raw_lines[boundary + 1 : idx] if boundary is not None else raw_lines[:idx]
    return "AORTA_CHECK_NOT_REENTRANT(" in "\n".join(window)


def _lint_file(path, seeds, violations):
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    sites, stripped, depth_before, deltas = _call_sites_in_file(raw_lines, seeds)
    for line_no, seed in sites:
        if not _covered(raw_lines, stripped, depth_before, deltas, line_no):
            violations.append((path, line_no, seed))


def _definition_body(raw_lines, match_idx, stripped, depth_before, deltas):
    """Body text (inclusive of the brace line) of the definition whose
    signature contains `match_idx`, or None when the match is a call, not
    a definition. The body opener is the nearest line at the same depth
    that ends with `{` and passes the function-header test; a match whose
    `(` closes on its own line (a call) is not a definition."""
    depth = depth_before[match_idx]
    brace = None
    for i in range(match_idx, len(stripped)):
        if (depth_before[i] == depth and stripped[i].rstrip().endswith("{")
                and _function_header(stripped[i])):
            brace = i
            break
    if brace is None:
        return None
    if brace != match_idx:
        # The signature continues past the match line: the `(` opened at
        # the match must still be open at the end of that line.
        match_line = stripped[match_idx]
        pos = match_line.find("(")
        if match_line[pos:].count("(") <= match_line[pos:].count(")"):
            return None  # a call, not a definition
    end = len(stripped)
    for j in range(brace + 1, len(stripped)):
        if depth_before[j] + deltas[j] <= depth:
            end = j
            break
    return "\n".join(raw_lines[brace:end])


def _pure_cpp_waits(repo_root, violations):
    """Pin the pure-C++ waits: each (file, function) must exist and
    contain an AORTA_CHECK_NOT_REENTRANT inside its body."""
    for relpath, fname, min_count in PURE_CPP_WAITS:
        path = Path(repo_root) / relpath
        if not path.is_file():
            violations.append(
                (path, 0, f"PURE_CPP_WAITS: file missing for {fname}"))
            continue
        raw_lines = path.read_text(encoding="utf-8").splitlines()
        stripped, depth_before, deltas = _file_analysis(raw_lines)
        segment = fname.rsplit("::", 1)[-1]
        pattern = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(segment) + r"\s*\(")
        found = False
        for idx, s in enumerate(stripped):
            if not pattern.search(s):
                continue
            body = _definition_body(raw_lines, idx, stripped, depth_before, deltas)
            if body is None:
                continue  # a call, not the definition
            found = True
            got = body.count("AORTA_CHECK_NOT_REENTRANT(")
            if got < min_count:
                violations.append((
                    path, idx + 1,
                    f"PURE_CPP_WAITS: {fname} has {got} "
                    f"AORTA_CHECK_NOT_REENTRANT occurrence(s), expected >= "
                    f"{min_count} (guard deleted?)"))
        if not found:
            violations.append((
                path, 0,
                f"PURE_CPP_WAITS: function {fname} not found (renamed?)"))


def lint(repo_root):
    root = Path(repo_root)
    # Sanity gates: a silently broken scan (bad path, dead regex, empty
    # glob) must fail, never pass. Hard floors with today's counts: 70
    # seeds (61 marker + 9 delegating) and 55 C++ files.
    if not root.is_dir():
        print(f"reentrancy guard completeness lint: repo root {repo_root} "
              "does not exist")
        return 1
    c_api_dir = root / "core/src/ffi/c_api"
    cpp_root = root / "cpp"
    for required in (c_api_dir, cpp_root / "src", cpp_root / "include/aorta"):
        if not required.is_dir():
            print(f"reentrancy guard completeness lint: scan directory "
                  f"{required} does not exist")
            return 1
    seeds = rust_seeds(c_api_dir)
    if len(seeds) < 60:
        print(f"reentrancy guard completeness lint: seed count {len(seeds)} "
              "< 60 — the marker scan is broken")
        return 1
    violations = []
    scanned = 0
    for base, dirs, files in os.walk(cpp_root / "src"):
        for name in sorted(files):
            if name.endswith((".cc", ".h", ".hpp")):
                scanned += 1
                _lint_file(Path(base) / name, seeds, violations)
    for name in sorted((cpp_root / "include/aorta").rglob("*")):
        if name.is_file() and name.suffix in (".cc", ".h", ".hpp"):
            scanned += 1
            _lint_file(name, seeds, violations)
    if scanned < 40:
        print(f"reentrancy guard completeness lint: scanned C++ file count "
              f"{scanned} < 40 — the C++ scan is broken")
        return 1
    _pure_cpp_waits(repo_root, violations)
    if violations:
        print(f"reentrancy guard completeness lint: {len(violations)} violation(s):")
        for path, line, seed in violations:
            if seed.startswith("PURE_CPP_WAITS:"):
                print(f"  {path}:{line}: {seed}")
            else:
                print(f"  {path}:{line}: call to guarded entry {seed} lacks a "
                      f"preceding AORTA_CHECK_NOT_REENTRANT and has no "
                      f"`// reentrancy:` annotation")
        return 1
    print(f"reentrancy guard completeness lint: OK "
          f"({len(seeds)} seeds, {scanned} C++ files)")
    return 0


def _lint_file(path, seeds, violations):
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    sites, stripped, depth_before, deltas = _call_sites_in_file(raw_lines, seeds)
    for line_no, seed in sites:
        if not _covered(raw_lines, stripped, depth_before, deltas, line_no):
            violations.append((path, line_no, seed))


def main(argv):
    repo_root = argv[1] if len(argv) > 1 else str(Path(__file__).resolve().parent.parent.parent)
    return lint(repo_root)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
