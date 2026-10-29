#!/usr/bin/env python3
"""Sweep-all-schemas guard-symmetry regression test.

The compile probes under cpp/test/wrapper_guard_symmetry_*_test.cc
cover a handful of representative owner/includer pairs. This test is the
permanent, wholesale regression net: it runs
gen_message_wrapper.py --lang cpp over EVERY registry entry with
cpp_wrapper=True and scans all generated headers for the guard-asymmetry
defect family directly, so any FUTURE schema pair that reintroduces the
bug is caught without needing a dedicated compile probe.

Deliberately does NOT depend on any Bazel macro (aorta_define_schema_package,
aorta_define_cpp_bindings, ...) or `bazel query`/`bazel aquery` to discover
which schemas to sweep. It parses the registry data fragments directly with a
small test-local scanner (regex +
paren-depth block splitting), independent of the Starlark evaluator and the
BUILD graph, so this test keeps catching guard-asymmetry regressions even if
the registry-to-BUILD wiring itself breaks. This mirrors (in test code only)
the class of hand-rolled parsing that is a
production anti-pattern -- acceptable here because this parser has exactly
one job (find cpp_wrapper=True entries) and failure modes fail closed: see
`parse_cpp_wrapper_entries`'s cross-check against an independent flat count
of `cpp_wrapper = True` occurrences.

Being the PERMANENT regression gate for this
defect family, this scanner must fail CLOSED, not open. Every place it could
plausibly under-count or mis-parse is covered by an explicit check that
raises `ScanError` (surfaced as a labeled test failure, never a silent
pass): malformed `#ifndef`/`#define` guard pairs (including unclosed guards
and unclosed namespaces still open at end of file), `namespace a::b {`
nested-namespace shorthand (unsupported, rejected rather than silently
dropped), declarations that don't match the strict single-line regexes
(cross-checked against a whitespace-tolerant loose pattern -- see
`_assert_no_missed_declarations`), and a registry `struct(...)` block that
the paren-depth scanner silently skips (cross-checked in
`parse_cpp_wrapper_entries` against a flat text count). `ScannerSelfTest`
and `RegistryParserSelfTest` below prove each conflict/fail-closed class is
actually caught, using hand-written fixture text (not the real generator),
independent of whether any current registry entry happens to exercise it.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
TOOLS_DIR = THIS_DIR.parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import gen_action_wrapper  # noqa: E402
import gen_message_wrapper  # noqa: E402
import schema_reflection  # noqa: E402

_STRUCT_LINE_RE = re.compile(r"^struct\s*\($")
_SCHEMA_LIST_RE = re.compile(
    r"^(?:SCHEMA_DEFINITIONS|LOCAL_SCHEMA_DEFINITIONS|PRODUCT_[A-Z_]+_SCHEMAS) = \[\n(.*?)\n\]\n",
    re.S | re.M,
)
_PKG_RE = re.compile(r'\bpkg\s*=\s*"([^"]+)"')
_STEM_RE = re.compile(r'\bstem\s*=\s*"([^"]+)"')
_ROOT_FBS_RE = re.compile(r'\broot_fbs\s*=\s*"([^"]+)"')
_CPP_WRAPPER_TRUE_RE = re.compile(r"\bcpp_wrapper\s*=\s*True\b")
_ACTION_SCHEMA_TRUE_RE = re.compile(r"\baction_schema\s*=\s*True\b")


class ScanError(RuntimeError):
    """Raised when the generated-header scanner hits something it cannot
    confidently interpret. This is the fail-closed signal: every raise site
    is a place a silent mis-scan would otherwise let the guard-asymmetry
    check below pass vacuously."""


class RegistryEntry:
    """One SCHEMA_DEFINITIONS struct(...) entry with cpp_wrapper=True."""

    __slots__ = ("pkg", "stem", "root_fbs", "action_schema")

    def __init__(self, pkg: str, stem: str, root_fbs: str, action_schema: bool):
        self.pkg = pkg
        self.stem = stem
        self.root_fbs = root_fbs
        self.action_schema = action_schema

    @property
    def pkg_rel(self) -> str:
        assert self.pkg.startswith("//"), self.pkg
        return self.pkg[len("//"):]

    @property
    def schema_rel(self) -> str:
        prefix = "//message/"
        assert self.pkg.startswith(prefix), self.pkg
        return self.pkg[len(prefix):]

    @property
    def label(self) -> str:
        return "{}:{}".format(self.pkg, self.stem)


def _iter_struct_blocks(list_body: str) -> list[str]:
    """Yield the text of each top-level `struct(...)` block, tracking paren
    depth so nested `dep_fbs = [...]` lists (or anything else) can never be
    mistaken for a block boundary -- the boundary is ALWAYS `(` / `)`
    balance, dep_fbs lists only ever nest inside via `[` / `]`. Matches both
    `struct(` and the legal Starlark spelling `struct (` (optional
    whitespace before the paren) -- the
    caller (parse_cpp_wrapper_entries) independently cross-checks the
    resulting entry count against a flat text count, so if THIS function
    ever silently skips a block for some other reason, that mismatch still
    fails closed rather than the sweep quietly covering fewer schemas."""
    lines = list_body.splitlines()
    blocks: list[str] = []
    i, n = 0, len(lines)
    while i < n:
        if _STRUCT_LINE_RE.match(lines[i].strip()):
            start = i
            depth = lines[i].count("(") - lines[i].count(")")
            j = i + 1
            while j < n and depth > 0:
                depth += lines[j].count("(") - lines[j].count(")")
                j += 1
            blocks.append("\n".join(lines[start:j]))
            i = j
        else:
            i += 1
    return blocks


def _strip_starlark_comments(text: str) -> str:
    """Strip `#` line comments from Starlark source text, respecting string
    literals (single/double/triple-quoted) so a `#` inside a string is never
    treated as a comment start. Used ONLY for the flat cross-check count in
    parse_cpp_wrapper_entries -- Starlark has no block comments, just `#` to
    end-of-line, so this is deliberately simpler than
    schema_include_utils.strip_fbs_comments (which also handles C-style
    `/* */` for .fbs text)."""
    out: list[str] = []
    i = 0
    n = len(text)
    in_string: str | None = None  # None, or the opening quote/triple-quote
    while i < n:
        if in_string:
            if text.startswith(in_string, i):
                out.append(in_string)
                i += len(in_string)
                in_string = None
                continue
            if text[i] == "\\" and i + 1 < n:
                out.append(text[i:i + 2])
                i += 2
                continue
            out.append(text[i])
            i += 1
            continue
        if text[i:i + 3] in ('"""', "'''"):
            in_string = text[i:i + 3]
            out.append(in_string)
            i += 3
            continue
        ch = text[i]
        if ch in ('"', "'"):
            in_string = ch
            out.append(ch)
            i += 1
            continue
        if ch == "#":
            while i < n and text[i] != "\n":
                i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_cpp_wrapper_entries(registry_texts: str | list[str]) -> list[RegistryEntry]:
    if isinstance(registry_texts, str):
        registry_texts = [registry_texts]
    list_bodies = []
    for registry_text in registry_texts:
        list_bodies.extend(match.group(1) for match in _SCHEMA_LIST_RE.finditer(registry_text))
    if not list_bodies:
        raise AssertionError(
            "could not locate any schema definition list in the registry fragments; "
            "the test-local scanner in sweep_conflicts_test.py needs updating"
        )
    entries: list[RegistryEntry] = []
    for list_body in list_bodies:
        for block in _iter_struct_blocks(list_body):
            if not _CPP_WRAPPER_TRUE_RE.search(block):
                continue
            pkg_m = _PKG_RE.search(block)
            stem_m = _STEM_RE.search(block)
            root_fbs_m = _ROOT_FBS_RE.search(block)
            if not (pkg_m and stem_m and root_fbs_m):
                raise AssertionError(
                    "cpp_wrapper=True registry entry missing pkg/stem/root_fbs:\n{}".format(
                        block
                    )
                )
            entries.append(
                RegistryEntry(
                    pkg=pkg_m.group(1),
                    stem=stem_m.group(1),
                    root_fbs=root_fbs_m.group(1),
                    action_schema=bool(_ACTION_SCHEMA_TRUE_RE.search(block)),
                )
            )

    # Fail-closed cross-check: the previous
    # version of this test only asserted `len(entries) >= 10`, a floor loose
    # enough to tolerate silently losing many entries (e.g. every block
    # written as `struct (` instead of `struct(`, before that spacing was
    # supported). Independently count `cpp_wrapper = True` occurrences in
    # the SAME comment-stripped SCHEMA_DEFINITIONS body via a flat regex
    # search (no block-boundary logic at all, so it can't share
    # _iter_struct_blocks's failure mode) and assert it matches the number
    # of entries actually parsed -- any block silently skipped by the
    # paren-depth scanner now fails the sweep instead of quietly shrinking
    # its coverage.
    expected_count = sum(
        len(_CPP_WRAPPER_TRUE_RE.findall(_strip_starlark_comments(list_body)))
        for list_body in list_bodies
    )
    if len(entries) != expected_count:
        raise AssertionError(
            "parsed {} cpp_wrapper=True entries via struct(...) block-scanning, "
            "but an independent flat count of 'cpp_wrapper = True' in the same "
            "SCHEMA_DEFINITIONS body found {} -- a struct(...) block was likely "
            "skipped by _iter_struct_blocks (or double-counted); the "
            "SCHEMA_DEFINITIONS scanner in sweep_conflicts_test.py needs "
            "updating".format(len(entries), expected_count)
        )

    return entries


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message" / "schemas" / "registry.bzl").exists():
            return candidate
    # Fallback for running directly with `python3` outside `bazel test`.
    return TOOLS_DIR.parent.parent


def _manifest_cpp_wrapper_labels(repo_root: Path) -> set[str]:
    """`<pkg>:<stem>` of every `cpp_wrapper = True` entry the BUILT registry
    manifest carries (`//message:schemas_registry_manifest`).

    The sweep parses the fragment TEXT; this reads the evaluated registry the
    build actually used. Comparing the two is what keeps the sweep honest
    about coverage: a fragment that stops being loaded by `registry.bzl`, or a
    parser that silently drops entries, shrinks the swept set below the built
    set and fails here instead of reporting a hollow green.
    """
    manifest = json.loads(
        (repo_root / "message" / "schemas_registry_manifest.json").read_text(encoding="utf-8")
    )
    return {
        "{}:{}".format(row["pkg"], row["stem"])
        for row in manifest["schema_definitions"]
        if row.get("cpp_wrapper")
    }


def _bfbs_path(entry: RegistryEntry, repo_root: Path) -> Path:
    """The registry-built `.bfbs` for this entry, at the SAME output path
    `aorta_cpp_message_wrapper`'s genrule feeds `--root-bfbs` from
    (`{schema_pkg}:{schema_stem}_bfbs`, output `<stem>.bfbs` directly in the
    schema's own package -- see `schema_codegen.bzl`'s `aorta_define_schema_
    targets`; NOT nested under `<stem>_action/` even for action_schema=True
    entries, since only the genrule's SOURCE is the expanded aggregate, not
    its own output path). Pulled in as test `data` via `//message:bfbs_
    generated` (gen_message_wrapper.py's
    --root-bfbs is required, so this sweep -- which calls `write_cpp`
    directly -- must supply one for every entry it sweeps, exactly as real
    100% `cpp_wrapper=True` registry coverage already guarantees one
    exists)."""
    return repo_root / entry.pkg_rel / "{}.bfbs".format(entry.stem)


def _generate_wrapper_header(entry: RegistryEntry, repo_root: Path, sandbox_root: Path) -> str:
    """Reproduce what `aorta_cpp_message_wrapper` (message_bindings.bzl) feeds
    gen_message_wrapper.py for this registry entry, including the
    action-schema preprocessing step (registry.bzl `_wrapper_root_fbs_label`:
    action_schema=True entries are wrapped through gen_action_wrapper first,
    same as execute_task/data_upload's real build)."""
    raw_fbs_path = repo_root / entry.pkg_rel / entry.root_fbs

    if entry.action_schema:
        wrapped_text = gen_action_wrapper.generate_action_wrapper(raw_fbs_path)
        # Mirror the genrule output path convention
        # (`stem + "_action/" + root_fbs`, same package) inside a synthetic
        # Bazel-output-tree layout so gen_message_wrapper's include resolver
        # falls back to the real schema tree via the `sandbox_root/schemas`
        # symlink (see setUpClass) -- the same fallback path
        # `resolve_schema_include_path` uses for genuine sandboxed Bazel
        # actions (see message/tools/schema_include_utils.py).
        out_dir = (
            sandbox_root
            / "bazel-out"
            / "sweep-fastbuild"
            / "bin"
            / entry.pkg_rel
            / "{}_action".format(entry.stem)
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        wrapped_path = out_dir / entry.root_fbs
        wrapped_path.write_text(wrapped_text, encoding="utf-8")
        root_fbs_for_wrapper = wrapped_path
    else:
        root_fbs_for_wrapper = raw_fbs_path

    out_path = sandbox_root / "{}__sweep_msg.h".format(entry.label.replace("/", "_").replace(":", "_"))
    # `--root-bfbs` is required -- load this
    # entry's own registry-built bfbs (see `_bfbs_path`) and set the module
    # global `write_cpp` reads it from, exactly as `main()` does for a single
    # CLI invocation. Sequential per-entry (this loop is not threaded), so
    # setting/reading the global between iterations is safe.
    gen_message_wrapper._LOADED_SCHEMA = schema_reflection.load_bfbs_file(
        str(_bfbs_path(entry, repo_root))
    )
    gen_message_wrapper.write_cpp(
        out=str(out_path),
        stem=entry.stem,
        schema_rel=entry.schema_rel,
        root_fbs=str(root_fbs_for_wrapper),
        action_name=None,
    )
    return out_path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Generated-header symbol scanner (guard-asymmetry detector)
# ---------------------------------------------------------------------------

_IFNDEF_RE = re.compile(r"^#ifndef (\w+)$")
_DEFINE_RE = re.compile(r"^#define (\w+) 1$")
_ENDIF_RE = re.compile(r"^#endif\s*//\s*(\w+)$")
_NS_OPEN_RE = re.compile(r"^namespace ([\w:]+) \{$")
_NS_CLOSE_RE = re.compile(r"^\}\s*//\s*namespace ([\w:]+)$")
_TABLE_MSG_FWD_RE = re.compile(r"^struct \w+Msg;$")
_TABLE_MSG_DEF_RE = re.compile(r"^struct (\w+Msg) \{$")
_UNION_MSG_DEF_RE = re.compile(r"^using (\w+Msg) = std::variant<$")
_SUBSCRIBE_TRAITS_RE = re.compile(r"^struct TypedSubscribeTraits<(\S+)> \{$")
_PUBLISH_TRAITS_RE = re.compile(r"^struct TypedPublishTraits<(\S+)> \{$")

# Loose, whitespace-tolerant (`\s` matches newlines too) full-text patterns
# used ONLY as a fail-closed cross-check (see _assert_no_missed_declarations)
# -- never for extraction itself, so they don't need namespace/guard context.
_LOOSE_TABLE_MSG_RE = re.compile(r"\bstruct\s+\w+Msg\s*\{")
_LOOSE_UNION_MSG_RE = re.compile(r"\busing\s+\w+Msg\s*=\s*std::variant\s*<")
_LOOSE_SUBSCRIBE_TRAITS_RE = re.compile(r"\bstruct\s+TypedSubscribeTraits\s*<[^>]*>\s*\{")
_LOOSE_PUBLISH_TRAITS_RE = re.compile(r"\bstruct\s+TypedPublishTraits\s*<[^>]*>\s*\{")
_LOOSE_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("TABLE_MSG", _LOOSE_TABLE_MSG_RE),
    ("UNION_MSG", _LOOSE_UNION_MSG_RE),
    ("SUBSCRIBE_TRAITS", _LOOSE_SUBSCRIBE_TRAITS_RE),
    ("PUBLISH_TRAITS", _LOOSE_PUBLISH_TRAITS_RE),
)


class Symbol:
    __slots__ = ("category", "key", "guard", "body")

    def __init__(self, category: str, key: str, guard: str | None, body: tuple[str, ...]):
        self.category = category
        self.key = key
        self.guard = guard
        self.body = body


def _consume_braced_block(lines: list[str], start: int) -> tuple[int, tuple[str, ...]]:
    """Consume a `{`/`}`-delimited block starting at `lines[start]` (which
    must contain the opening `{`); returns the index of the closing line and
    the full line range (inclusive). Safe here because none of the emitted
    TABLE_MSG/SUBSCRIBE_TRAITS/PUBLISH_TRAITS bodies contain string literals
    with brace characters."""
    depth = lines[start].count("{") - lines[start].count("}")
    j = start
    while depth > 0:
        j += 1
        depth += lines[j].count("{") - lines[j].count("}")
    return j, tuple(lines[start:j + 1])


def _consume_until_line(lines: list[str], start: int, terminator: str) -> tuple[int, tuple[str, ...]]:
    """Consume lines until one ENDS WITH `terminator`. The union-Msg writer
    (gen_message_wrapper.py) appends the closing `>;` directly onto the same
    line as the last `std::shared_ptr<...>` variant (no newline in between),
    so the terminator is a line SUFFIX, not a standalone line -- e.g. the
    last line of a `using FooMsg = std::variant<...>;` block reads
    `    std::shared_ptr<LastVariantMsg>>;`, not a lone `>;`."""
    j = start
    while not lines[j].rstrip().endswith(terminator):
        j += 1
    return j, tuple(lines[start:j + 1])


def extract_symbols(header_text: str) -> list[Symbol]:
    """Strict, single-line-declaration scanner. Raises `ScanError` (fail
    closed) on structurally malformed guard pairs, on the `namespace a::b {`
    nested-namespace shorthand (which this scanner does not support --
    gen_message_wrapper.py currently only ever emits one namespace part per
    line; if that ever changes, this must be updated deliberately rather
    than silently mis-tracking the namespace stack), and on unclosed
    guards/namespaces still open at end of file (a header with a matched
    `#ifndef`/`#define` but no `#endif`, or a `namespace X {` with no
    closing `}  // namespace X`, would otherwise scan "successfully" with
    every symbol silently mis-tagged as guarded/namespaced when it
    shouldn't be, or vice versa)."""
    lines = header_text.splitlines()
    ns_stack: list[str] = []
    guard_stack: list[str] = []
    symbols: list[Symbol] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]

        m = _IFNDEF_RE.match(line)
        if m:
            guard_name = m.group(1)
            define_line = lines[i + 1] if i + 1 < n else None
            define_m = _DEFINE_RE.match(define_line) if define_line is not None else None
            if define_m is None or define_m.group(1) != guard_name:
                raise ScanError(
                    "malformed guard at line {}: '#ifndef {}' not immediately "
                    "followed by a matching '#define {} 1' (got: {!r})".format(
                        i, guard_name, guard_name, define_line
                    )
                )
            guard_stack.append(guard_name)
            i += 2
            continue
        m = _ENDIF_RE.match(line)
        if m:
            endif_name = m.group(1)
            if not guard_stack or guard_stack[-1] != endif_name:
                raise ScanError(
                    "malformed guard at line {}: '#endif  // {}' does not match "
                    "innermost open guard {!r}".format(
                        i, endif_name, guard_stack[-1] if guard_stack else None
                    )
                )
            guard_stack.pop()
            i += 1
            continue
        m = _NS_OPEN_RE.match(line)
        if m:
            ns_name = m.group(1)
            if "::" in ns_name:
                raise ScanError(
                    "unsupported nested-namespace shorthand 'namespace {} {{' at "
                    "line {}; this scanner assumes one namespace part per line "
                    "(matches gen_message_wrapper.py's current emission) -- update "
                    "the scanner (and re-verify the generator's namespace emission) "
                    "before trusting its output".format(ns_name, i)
                )
            ns_stack.append(ns_name)
            i += 1
            continue
        m = _NS_CLOSE_RE.match(line)
        if m:
            ns_name = m.group(1)
            if "::" in ns_name:
                raise ScanError(
                    "unsupported nested-namespace shorthand '}}  // namespace {}' "
                    "at line {}".format(ns_name, i)
                )
            if not ns_stack or ns_stack[-1] != ns_name:
                raise ScanError(
                    "malformed namespace close at line {}: '}}  // namespace {}' "
                    "does not match innermost open namespace {!r}".format(
                        i, ns_name, ns_stack[-1] if ns_stack else None
                    )
                )
            ns_stack.pop()
            i += 1
            continue
        if _TABLE_MSG_FWD_RE.match(line):
            i += 1
            continue

        m = _TABLE_MSG_DEF_RE.match(line)
        if m:
            key = "::".join(ns_stack + [m.group(1)]) if ns_stack else m.group(1)
            end, body = _consume_braced_block(lines, i)
            symbols.append(Symbol("TABLE_MSG", key, guard_stack[-1] if guard_stack else None, body))
            i = end + 1
            continue

        m = _UNION_MSG_DEF_RE.match(line)
        if m:
            key = "::".join(ns_stack + [m.group(1)]) if ns_stack else m.group(1)
            end, body = _consume_until_line(lines, i, ">;")
            symbols.append(Symbol("UNION_MSG", key, guard_stack[-1] if guard_stack else None, body))
            i = end + 1
            continue

        m = _SUBSCRIBE_TRAITS_RE.match(line)
        if m:
            end, body = _consume_braced_block(lines, i)
            symbols.append(
                Symbol("SUBSCRIBE_TRAITS", m.group(1), guard_stack[-1] if guard_stack else None, body)
            )
            i = end + 1
            continue

        m = _PUBLISH_TRAITS_RE.match(line)
        if m:
            end, body = _consume_braced_block(lines, i)
            symbols.append(
                Symbol("PUBLISH_TRAITS", m.group(1), guard_stack[-1] if guard_stack else None, body)
            )
            i = end + 1
            continue

        i += 1

    # EOF fail-closed check: a header with a
    # complete `#ifndef`/`#define` but a missing `#endif`, or an unclosed
    # `namespace X {`, must not count as a successful scan -- every symbol
    # emitted after the unclosed construct would have the WRONG
    # guard/namespace context (or the right one dropped), and the file
    # simply running out of lines is otherwise indistinguishable from a
    # correctly balanced one.
    if guard_stack:
        raise ScanError(
            "unclosed guard(s) at end of file: {!r} still open (missing a "
            "matching #endif)".format(guard_stack)
        )
    if ns_stack:
        raise ScanError(
            "unclosed namespace(s) at end of file: {!r} still open (missing "
            "a matching closing '}}  // namespace ...')".format(ns_stack)
        )
    return symbols


def _assert_no_missed_declarations(label: str, text: str, symbols: list[Symbol]) -> None:
    """Fail-closed cross-check: `extract_symbols`'s
    strict per-line regexes only match single-line declarations (e.g.
    `struct XMsg {`); a hypothetical multi-line reformatting
    (`struct XMsg\\n{`) would silently yield zero symbols for that
    declaration and let the guard-asymmetry check pass vacuously on an
    unscanned symbol. These LOOSE patterns tolerate whitespace -- including
    newlines, since `\\s` matches `\\n` -- between the declaration keyword
    and its opening brace/angle-bracket, so if the strict scanner's count
    for a category ever diverges from the loose full-text count, the strict
    scanner missed something. Raises ScanError rather than returning a bool
    so callers can't accidentally ignore the result."""
    counts: dict[str, int] = {}
    for sym in symbols:
        counts[sym.category] = counts.get(sym.category, 0) + 1
    for category, pattern in _LOOSE_PATTERNS:
        loose_count = len(pattern.findall(text))
        strict_count = counts.get(category, 0)
        if loose_count != strict_count:
            raise ScanError(
                "{}: strict per-line scanner found {} {} declaration(s) but the "
                "loose whitespace-tolerant pattern found {} -- the strict scanner "
                "likely missed a multi-line or otherwise reformatted declaration "
                "(fail-closed check)".format(label, strict_count, category, loose_count)
            )


def scan_header(label: str, text: str) -> list[Symbol]:
    """Strict-extract + fail-closed cross-check, combined. This is the ONE
    entry point production code (SweepConflictsTest) and self-tests should
    use; `extract_symbols` alone skips the missed-declaration cross-check."""
    symbols = extract_symbols(text)
    _assert_no_missed_declarations(label, text, symbols)
    return symbols


def check_conflicts(symbols_by_header: dict[str, list[Symbol]]) -> list[str]:
    """symbols_by_header: header label -> symbols already extracted via
    scan_header(). Returns a list of violation descriptions (empty == no
    guard-asymmetry conflicts), covering:

    1. no fully-qualified TypedSubscribe/PublishTraits specialization is
       emitted in more than one header unless ALL emissions are guarded
       AND share exactly ONE guard macro (two DIFFERENT guard macros
       protecting the "same" symbol do NOT prevent a redefinition -- each
       macro is independently undefined the first time ITS header is
       included, so both guarded blocks compile when both headers are
       co-included).
    2. no same-namespace `struct <X>Msg {`/`using <X>Msg =` definition
       appears unguarded in one header and again (guarded or not, or under
       a different guard) in another -- same rule as (1), just for the
       TABLE_MSG/UNION_MSG categories.
    3. for identical guard macros reused across headers, the guarded body
       content is identical (and keyed to the same symbol) -- catches
       silent semantic drift: reusing one guard macro name for "the same"
       symbol across two headers does NOT cause a redefinition error (the
       second header's block is skipped once the guard is already
       defined), but if the two bodies actually differ, whichever header
       happens to be included first silently wins and the other's content
       is silently discarded.
    """
    by_key: dict[tuple[str, str], list[tuple[str, str | None]]] = {}
    by_guard: dict[str, list[tuple[str, tuple[str, ...], str]]] = {}

    for header_label, symbols in symbols_by_header.items():
        for sym in symbols:
            by_key.setdefault((sym.category, sym.key), []).append((header_label, sym.guard))
            if sym.guard is not None:
                by_guard.setdefault(sym.guard, []).append((header_label, sym.body, sym.key))

    violations: list[str] = []

    for (category, key), occurrences in by_key.items():
        if len(occurrences) < 2:
            continue
        unguarded = [h for h, g in occurrences if g is None]
        guards = {g for _, g in occurrences if g is not None}
        if unguarded or len(guards) > 1:
            detail = ", ".join(
                "{}({})".format(h, g if g else "UNGUARDED") for h, g in occurrences
            )
            violations.append(
                "{} '{}' emitted in {} headers with asymmetric/inconsistent "
                "guarding (co-inclusion is a redefinition error): {}".format(
                    category, key, len(occurrences), detail
                )
            )

    for guard, occurrences in sorted(by_guard.items()):
        keys = {key for _, _, key in occurrences}
        if len(keys) > 1:
            violations.append(
                "guard macro '{}' reused for different symbols: {}".format(guard, sorted(keys))
            )
            continue
        bodies = {body for _, body, _ in occurrences}
        if len(bodies) > 1:
            detail = ", ".join(h for h, _, _ in occurrences)
            violations.append(
                "guard macro '{}' has divergent guarded-body content across headers "
                "(silent semantic drift -- whichever header is included first wins): "
                "{}".format(guard, detail)
            )

    return violations


class SweepConflictsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = _repo_root()
        # Aorta's registry is base-only: `registry.bzl` derives SCHEMA_DEFINITIONS
        # from `registry_local.bzl` alone (no product fragments, in-repo or
        # external). Sweep exactly that file, then prove the parsed set matches
        # the built manifest so a future fragment split cannot leave this sweep
        # covering a fraction of the registry while still reporting green.
        registry_paths = [cls.repo_root / "message" / "schemas" / "registry_local.bzl"]
        cls.entries = parse_cpp_wrapper_entries(
            [path.read_text(encoding="utf-8") for path in registry_paths]
        )
        swept = {entry.label for entry in cls.entries}
        built = _manifest_cpp_wrapper_labels(cls.repo_root)
        assert swept == built, (
            "sweep covers a different cpp_wrapper set than the built registry "
            "manifest: only-swept={} only-built={}".format(
                sorted(swept - built), sorted(built - swept)
            )
        )

        cls._tmpdir = tempfile.TemporaryDirectory(prefix="sweep_conflicts_")
        cls.sandbox_root = Path(cls._tmpdir.name)
        (cls.sandbox_root / "schemas").symlink_to(cls.repo_root / "message" / "schemas")

        cls.headers: dict[str, str] = {}
        cls.generation_errors: list[str] = []
        for entry in cls.entries:
            try:
                cls.headers[entry.label] = _generate_wrapper_header(
                    entry, cls.repo_root, cls.sandbox_root
                )
            except Exception as exc:  # noqa: BLE001 - want a labeled failure list, not a stack trace
                cls.generation_errors.append("{}: {}: {}".format(entry.label, type(exc).__name__, exc))

        cls.symbols_by_header: dict[str, list[Symbol]] = {}
        cls.scan_errors: list[str] = []
        for label, text in cls.headers.items():
            try:
                cls.symbols_by_header[label] = scan_header(label, text)
            except ScanError as exc:
                cls.scan_errors.append("{}: {}".format(label, exc))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmpdir.cleanup()

    def test_registry_scanner_found_entries(self) -> None:
        # The `>=10` floor this test used to
        # assert tolerated losing many entries silently. The real fail-closed
        # guarantee now lives in parse_cpp_wrapper_entries itself, which
        # cross-checks the parsed entry count against an independent flat
        # count of 'cpp_wrapper = True' occurrences and raises AssertionError
        # (failing every test in this class via setUpClass) on any mismatch.
        # This assertion is just a non-empty sanity check for a readable
        # failure message if registry.bzl moves/is renamed outright.
        self.assertGreater(
            len(self.entries),
            0,
            "found zero cpp_wrapper=True registry entries; the "
            "registry fragment scanner in sweep_conflicts_test.py may need "
            "updating",
        )

    def test_all_cpp_wrapper_entries_generate_without_error(self) -> None:
        self.assertEqual([], self.generation_errors, "\n".join(self.generation_errors))

    def test_no_scan_errors_across_all_generated_headers(self) -> None:
        # Fail-closed checks (malformed guards, unsupported namespace
        # shorthand, missed multi-line declarations) surface here rather
        # than silently degrading the conflict scan below.
        self.assertEqual([], self.scan_errors, "\n".join(self.scan_errors))

    def test_every_header_has_root_local_symbols(self) -> None:
        # Independent minimum-count sanity: the FBS authoring
        # convention requires exactly
        # one root_type per .fbs, and every registry entry swept here is a
        # real schema (not a struct/enum-only file), so its wrapper header
        # must emit at least one TABLE_MSG struct and one SUBSCRIBE_TRAITS
        # specialization (the root-local one). A header that scanned
        # "successfully" (no ScanError) but yielded zero symbols in either
        # category would mean BOTH the strict and loose patterns missed an
        # entire declaration class -- this is the last-resort floor.
        missing: list[str] = []
        for label, symbols in self.symbols_by_header.items():
            counts: dict[str, int] = {}
            for sym in symbols:
                counts[sym.category] = counts.get(sym.category, 0) + 1
            if counts.get("TABLE_MSG", 0) < 1 or counts.get("SUBSCRIBE_TRAITS", 0) < 1:
                missing.append(
                    "{}: TABLE_MSG={} SUBSCRIBE_TRAITS={}".format(
                        label, counts.get("TABLE_MSG", 0), counts.get("SUBSCRIBE_TRAITS", 0)
                    )
                )
        self.assertEqual([], missing, "\n".join(missing))

    def test_no_guard_asymmetry_across_all_generated_headers(self) -> None:
        violations = check_conflicts(self.symbols_by_header)
        self.assertEqual([], violations, "\n".join(violations))


class ScannerSelfTest(unittest.TestCase):
    """Proves the scanner itself (extract_symbols/scan_header/
    check_conflicts) actually catches each conflict/fail-closed class, using
    hand-written fixture header text -- independent of whether any CURRENT
    registry entry happens to exercise it."""

    def _symbols_by_header(self, headers: dict[str, str]) -> dict[str, list[Symbol]]:
        return {label: scan_header(label, text) for label, text in headers.items()}

    def test_detects_unguarded_duplicate_trait(self) -> None:
        owner = (
            "template <>\n"
            "struct TypedSubscribeTraits<::ns::Foo> {\n"
            "  static bool Verify(const uint8_t* data, std::size_t size) {\n"
            "    return true;\n"
            "  }\n"
            "};\n"
            "\n"
        )
        includer = (
            "#ifndef GUARD_FOO_DEFINED\n"
            "#define GUARD_FOO_DEFINED 1\n"
            "template <>\n"
            "struct TypedSubscribeTraits<::ns::Foo> {\n"
            "  static bool Verify(const uint8_t* data, std::size_t size) {\n"
            "    return true;\n"
            "  }\n"
            "};\n"
            "#endif  // GUARD_FOO_DEFINED\n"
            "\n"
        )
        violations = check_conflicts(
            self._symbols_by_header({"owner": owner, "includer": includer})
        )
        self.assertEqual(1, len(violations), violations)
        self.assertIn("SUBSCRIBE_TRAITS", violations[0])
        self.assertIn("::ns::Foo", violations[0])
        self.assertIn("UNGUARDED", violations[0])

    def test_detects_same_namespace_duplicate_msg_struct_one_side_unguarded(self) -> None:
        owner = (
            "namespace ns {\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "};\n"
            "}  // namespace ns\n"
            "\n"
        )
        includer = (
            "namespace ns {\n"
            "#ifndef GUARD_FOOMSG_DEFINED\n"
            "#define GUARD_FOOMSG_DEFINED 1\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "};\n"
            "#endif  // GUARD_FOOMSG_DEFINED\n"
            "}  // namespace ns\n"
            "\n"
        )
        violations = check_conflicts(
            self._symbols_by_header({"owner": owner, "includer": includer})
        )
        self.assertEqual(1, len(violations), violations)
        self.assertIn("TABLE_MSG", violations[0])
        self.assertIn("ns::FooMsg", violations[0])
        self.assertIn("UNGUARDED", violations[0])

    def test_detects_duplicate_under_two_different_guard_macros(self) -> None:
        # Both headers guard their emission -- but with DIFFERENT macro
        # names. Neither header's `#ifndef` is ever satisfied by the
        # other's `#define`, so both blocks compile when co-included: this
        # must be flagged even though neither individual emission is
        # "unguarded".
        header_a = (
            "#ifndef GUARD_A_DEFINED\n"
            "#define GUARD_A_DEFINED 1\n"
            "struct TypedSubscribeTraits<::ns::Bar> {\n"
            "  static bool Verify(const uint8_t* data, std::size_t size) {\n"
            "    return true;\n"
            "  }\n"
            "};\n"
            "#endif  // GUARD_A_DEFINED\n"
            "\n"
        )
        header_b = (
            "#ifndef GUARD_B_DEFINED\n"
            "#define GUARD_B_DEFINED 1\n"
            "struct TypedSubscribeTraits<::ns::Bar> {\n"
            "  static bool Verify(const uint8_t* data, std::size_t size) {\n"
            "    return true;\n"
            "  }\n"
            "};\n"
            "#endif  // GUARD_B_DEFINED\n"
            "\n"
        )
        violations = check_conflicts(
            self._symbols_by_header({"header_a": header_a, "header_b": header_b})
        )
        self.assertEqual(1, len(violations), violations)
        self.assertIn("::ns::Bar", violations[0])
        self.assertIn("GUARD_A_DEFINED", violations[0])
        self.assertIn("GUARD_B_DEFINED", violations[0])

    def test_detects_guard_body_drift(self) -> None:
        # SAME guard macro reused across two headers -- this does NOT cause
        # a redefinition (the second header's block is skipped once the
        # guard is already defined by the first), but if the bodies differ,
        # whichever header is included first silently wins.
        header_a = (
            "#ifndef GUARD_SAME_DEFINED\n"
            "#define GUARD_SAME_DEFINED 1\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "};\n"
            "#endif  // GUARD_SAME_DEFINED\n"
            "\n"
        )
        header_b = (
            "#ifndef GUARD_SAME_DEFINED\n"
            "#define GUARD_SAME_DEFINED 1\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "  std::optional<int> extra;\n"
            "};\n"
            "#endif  // GUARD_SAME_DEFINED\n"
            "\n"
        )
        violations = check_conflicts(
            self._symbols_by_header({"header_a": header_a, "header_b": header_b})
        )
        self.assertEqual(1, len(violations), violations)
        self.assertIn("GUARD_SAME_DEFINED", violations[0])
        self.assertIn("divergent guarded-body content", violations[0])

    def test_detects_multiline_declaration_via_fail_closed_check(self) -> None:
        # `struct FooMsg\n{` (brace on its own line) does not match the
        # strict single-line `_TABLE_MSG_DEF_RE`, so extract_symbols() alone
        # would silently yield zero TABLE_MSG symbols for this header. The
        # fail-closed loose-vs-strict cross-check must catch it instead of
        # letting the caller treat "zero symbols" as "no conflicts".
        multiline_header = (
            "struct FooMsg\n"
            "{\n"
            "  std::optional<int> value;\n"
            "};\n"
            "\n"
        )
        self.assertEqual([], extract_symbols(multiline_header))
        with self.assertRaises(ScanError) as ctx:
            scan_header("multiline_test", multiline_header)
        self.assertIn("TABLE_MSG", str(ctx.exception))

    def test_malformed_guard_pair_is_rejected(self) -> None:
        malformed = "#ifndef GUARD_X_DEFINED\nstruct FooMsg {\n};\n"
        with self.assertRaises(ScanError):
            extract_symbols(malformed)

    def test_nested_namespace_shorthand_is_rejected(self) -> None:
        nested_ns = "namespace a::b {\nstruct FooMsg {\n};\n}  // namespace a::b\n"
        with self.assertRaises(ScanError):
            extract_symbols(nested_ns)

    def test_missing_endif_at_eof_is_rejected(self) -> None:
        # `#ifndef`/`#define` are well-formed and the TABLE_MSG declaration
        # scans fine, but the file ends with the guard still open (no
        # `#endif` at all). Without an EOF check, this "successfully" scans
        # as a single guarded symbol -- masking the fact that gen_message_
        # wrapper.py would never actually emit an unterminated guard, so a
        # real header hitting this shape means something is badly wrong
        # upstream.
        missing_endif = (
            "#ifndef GUARD_X_DEFINED\n"
            "#define GUARD_X_DEFINED 1\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "};\n"
        )
        with self.assertRaises(ScanError) as ctx:
            extract_symbols(missing_endif)
        self.assertIn("unclosed guard", str(ctx.exception))

    def test_unclosed_namespace_at_eof_is_rejected(self) -> None:
        # `namespace ns {` opens but the file ends before the matching
        # `}  // namespace ns` -- every TABLE_MSG/UNION_MSG symbol scanned
        # inside is correctly namespaced, but a well-formed generated header
        # never leaves a namespace open, so this must fail closed rather
        # than scan "successfully".
        unclosed_ns = (
            "namespace ns {\n"
            "struct FooMsg {\n"
            "  std::optional<int> value;\n"
            "};\n"
        )
        with self.assertRaises(ScanError) as ctx:
            extract_symbols(unclosed_ns)
        self.assertIn("unclosed namespace", str(ctx.exception))


class RegistryParserSelfTest(unittest.TestCase):
    """Proves parse_cpp_wrapper_entries/_iter_struct_blocks handle the legal
    Starlark `struct (` spacing variant,
    using a synthetic registry snippet -- independent of whether the real
    registry.bzl happens to use that spelling anywhere today."""

    def test_struct_paren_spacing_variant_is_parsed(self) -> None:
        snippet = (
            "SCHEMA_DEFINITIONS = [\n"
            "    struct (\n"
            '        pkg = "//message/schemas/topic/example",\n'
            '        stem = "fixture_one",\n'
            '        root_fbs = "fixture_one.fbs",\n'
            "        cpp_wrapper = True,\n"
            "        cpp = True,\n"
            "    ),\n"
            "    struct(\n"
            '        pkg = "//message/schemas/topic/example",\n'
            '        stem = "fixture_two",\n'
            '        root_fbs = "fixture_two.fbs",\n'
            "        cpp_wrapper = True,\n"
            "        cpp = True,\n"
            "    ),\n"
            "]\n"
        )
        entries = parse_cpp_wrapper_entries(snippet)
        self.assertEqual(2, len(entries))
        self.assertEqual({"fixture_one", "fixture_two"}, {e.stem for e in entries})


if __name__ == "__main__":
    unittest.main()
