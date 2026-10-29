#!/usr/bin/env python3
"""Shared FlatBuffers schema include parsing helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence
import re

INCLUDE_RE = re.compile(r'\binclude\s+"([^"]+)"\s*;')

# flatc's Rust namer keyword set (vendored flatbuffers idl_gen_rust.cpp
# `RustKeywords()`). An identifier that collides is escaped by appending `_`
# (keyword_suffix "_", keyword_prefix "", Escape::BeforeConvertingCase). Codegen
# tools that build crate-qualified Rust paths from the loader's clean namespace/
# type names MUST reproduce flatc's naming exactly or those paths dangle. Shared
# here as the single source of truth for gen_message_wrapper (wrapper type paths)
# and patch_flatc_rust (the absolute foreign-reference rewrite).
FLATC_RUST_KEYWORDS = frozenset({
    "as", "break", "const", "continue", "crate", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
    "move", "mut", "pub", "ref", "return", "Self", "self", "static", "struct",
    "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    "abstract", "alignof", "become", "box", "do", "final", "macro", "offsetof",
    "override", "priv", "proc", "pure", "sizeof", "typeof", "unsized", "virtual",
    "yield", "std", "usize", "isize", "u8", "i8", "u16", "i16", "u32", "i32",
    "u64", "i64", "u128", "i128", "f32", "f64", "follow", "push",
    "to_little_endian", "from_little_endian", "ENUM_MAX", "ENUM_MIN",
    "ENUM_VALUES",
})


def _flatc_camel_to_snake(name: str) -> str:
    """Exact port of flatc's `CamelToSnake` (vendored flatbuffers util.cpp): the
    conversion `ConvertCase(x, kSnake, kLowerCamel)` reduces to. Inserts an `_`
    before an uppercase/digit that follows a lowercase char (or a digit->non-
    digit boundary) and lowercases; keeps existing underscores; a no-op on
    lowercase-snake input (`aorta` -> `aorta`, `follow_` -> `follow_`)."""
    out = []
    for i, ch in enumerate(name):
        if i == 0:
            out.append(ch.lower())
        elif ch == "_":
            out.append("_")
        elif not ch.islower():  # uppercase or digit (matches C `!islower`)
            prev = name[i - 1]
            if prev.islower() or (prev.isdigit() and not ch.isdigit()):
                out.append("_")
            out.append(ch.lower())
        else:  # lowercase
            out.append(ch)
    return "".join(out)


def flatc_rust_escape_ident(name: str) -> str:
    """flatc's Rust name for a TYPE (config `types = Case::kKeep`): keyword-
    escape only, no case conversion -- `Format(name, kKeep)` == `EscapeKeyword
    (name)`. PascalCase type names are kept verbatim (`Vector3`, `AortaHeader`);
    a keyword-named type gets a trailing `_`."""
    return name + "_" if name in FLATC_RUST_KEYWORDS else name


def flatc_rust_namespace_segment(seg: str) -> str:
    """flatc's Rust module name for ONE namespace COMPONENT (config
    `namespaces = Case::kSnake`, `Escape::BeforeConvertingCase`):
    `Format(seg, kSnake)` == `CamelToSnake(EscapeKeyword(seg))` -- keyword-escape
    first, then snake_case. So `follow` -> `follow_`, `OtherThing` -> `other_thing`,
    `MyRoot` -> `my_root`; identity for the lowercase-snake segments Aorta uses."""
    return _flatc_camel_to_snake(flatc_rust_escape_ident(seg))


def flatc_rust_namespace_path(namespace: str) -> str:
    """Translate a dotted namespace (loader-clean) into flatc's Rust module path
    (e.g. `aorta.follow.common` -> `aorta::follow_::common`,
    `MyRoot.OtherThing` -> `my_root::other_thing`) by applying
    `flatc_rust_namespace_segment` to each component."""
    return "::".join(flatc_rust_namespace_segment(seg) for seg in namespace.split("."))


def strip_fbs_comments(text: str) -> str:
    """Strip ``//`` line and ``/* */`` block comments from FlatBuffers schema
    text (C-style, non-nested), for the regex/line scanners in the codegen
    tools (comment-blind scanners used to parse commented-out types as
    live).

    - Newlines inside block comments are preserved so line-oriented consumers
      keep their line structure.
    - Comment markers inside double-quoted strings are left alone.
    - A ``/*`` inside a ``//`` comment (e.g. ``// see function_msgs/*.msg``)
      does NOT open a block comment, and a ``//`` inside a block comment does
      NOT extend it — which is why this is a small state machine rather than
      a regex substitution.
    """
    out: list[str] = []
    i = 0
    n = len(text)
    normal, string, line, block = 0, 1, 2, 3
    state = normal
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if state == normal:
            if ch == '"':
                state = string
                out.append(ch)
            elif ch == "/" and nxt == "/":
                state = line
                i += 1
            elif ch == "/" and nxt == "*":
                state = block
                i += 1
            else:
                out.append(ch)
        elif state == string:
            out.append(ch)
            if ch == "\\" and nxt:
                out.append(nxt)
                i += 1
            elif ch == '"':
                state = normal
        elif state == line:
            if ch == "\n":
                out.append(ch)
                state = normal
        else:  # block
            if ch == "\n":
                out.append(ch)  # keep line structure
            elif ch == "*" and nxt == "/":
                state = normal
                i += 1
        i += 1
    return "".join(out)


def parse_schema_includes(schema_path: str | Path) -> list[str]:
    text = strip_fbs_comments(Path(schema_path).read_text(encoding="utf-8"))
    return [match.group(1).strip() for match in INCLUDE_RE.finditer(text) if match.group(1).strip()]


def _strip_bazel_out_bin_prefix(path: Path) -> Path | None:
    """Map a `bazel-out/<config>/bin/<pkg-relative>` path to `<pkg-relative>`.

    Bazel's sandbox/exec root mounts the source tree AND `bazel-out/` side by
    side, so a plain repo-relative path (with no `bazel-out/...` prefix) is
    directly accessible even when `path` itself points at a GENERATED file
    under `bazel-out`. Returns None when `path` is not a `bazel-out` output
    path (or is one of the less common output roots we do not special-case).
    """
    parts = path.parts
    if len(parts) >= 3 and parts[0] == "bazel-out" and parts[2] in ("bin", "genfiles", "testlogs"):
        return Path(*parts[3:]) if len(parts) > 3 else Path()
    return None


class SchemaIncludeResolutionError(RuntimeError):
    """Raised when an fbs `include` statement cannot be resolved to a file
    that actually exists at any searched candidate path.

    `resolve_schema_include_path`
    used to fall back to a wrong-by-construction path
    (`root_path.parent / include_path`) when every real candidate search
    failed, and callers (`gen_message_wrapper.py`'s `collect_include_schema_docs`)
    silently skipped the result via an `.exists()` guard. That is a silent-
    degradation channel: an include that cannot actually be resolved should
    be a hard, loud failure naming the include and every path that was
    tried, not a quietly-dropped include. With this raise in place, the
    `.exists()` guard downstream can never see `False` — resolution either
    succeeds (returns an existing path) or raises — so it is dead code, left
    in place only until a later gen_message_wrapper.py cleanup removes it.
    """


def _path_parts(value: str | Path) -> tuple[str, ...]:
    return Path(value).as_posix().split("/")


def manifest_suffix_matches(include_path: str, manifest: Sequence[str | Path]) -> list[Path]:
    """Return every `manifest` entry whose trailing path components exactly
    equal `include_path`'s components (a path-boundary suffix match, not a
    substring match).

    E.g. `include_path = "schemas/system/aorta_base.fbs"` (components
    `("schemas", "system", "aorta_base.fbs")`) matches a manifest entry
    `"external/aorta~/message/schemas/system/aorta_base.fbs"` (its LAST 3
    components are exactly `("schemas", "system", "aorta_base.fbs")`) and
    also matches `"message/schemas/system/aorta_base.fbs"`, but does NOT
    match `"message/schemas/system/other_aorta_base.fbs"` (different
    basename) or `"not_schemas/system/aorta_base.fbs"` (the "schemas"
    component itself must align, not just the basename). A bare sibling
    include (e.g. `include_path = "xfer_types.fbs"`, one component) matches
    on basename alone within the manifest. NOTE: the manifest passed to a
    `gen_message_wrapper` invocation is the ROOT schema's FULL combined
    transitive closure, NOT rescoped per includer while recursively
    walking the graph (see `resolve_schema_include_path`'s docstring) — so
    a bare include's basename-only match here can legitimately return
    multiple, genuinely different paths when two unrelated subtrees share
    a basename. `resolve_schema_include_path` accounts for this by trying
    deterministic same-directory resolution BEFORE consulting the manifest
    for bare includes; this function itself makes no such judgment call —
    it is a pure suffix-match query over whatever manifest it is given.

    The returned list is deduplicated by IDENTICAL
    `Path` value (order-preserving) before the caller ever sees it. A Tier-2
    schema with diamond deps (e.g. depending on both `PointCloud` and
    `LaserScan`, whose closures both transitively carry `Pose.fbs`/
    `Time.fbs`/`aorta_base.fbs`) lists each shared closure member's SAME
    path multiple times across the combined `--include-fbs` manifest — one
    `$(locations ...)` expansion per dep's closure target. Without dedup,
    resolving e.g. `"Pose.fbs"` while recursively processing PointCloud's
    own includes would see 2 raw matches (identical path, listed twice) and
    the caller would wrongly reject it as ambiguous. Two DIFFERENT paths
    that merely happen to share the same suffix (e.g. two distinct modules'
    `.../aorta_base.fbs`) are never deduped — they remain genuinely
    ambiguous and the caller must still hard-error.
    """
    include_parts = tuple(_path_parts(include_path))
    n = len(include_parts)
    matches: list[Path] = []
    seen: set[Path] = set()
    for entry in manifest:
        entry_parts = _path_parts(entry)
        if len(entry_parts) >= n and tuple(entry_parts[len(entry_parts) - n :]) == include_parts:
            match_path = Path(entry)
            if match_path in seen:
                continue
            seen.add(match_path)
            matches.append(match_path)
    return matches


def resolve_schema_include_path(
    root_fbs: str | Path,
    include_path: str,
    manifest: Sequence[str | Path] | None = None,
) -> Path:
    """Resolve an fbs `include "..."` statement to the file it names.

    `manifest`, when given, is the BUILD SYSTEM's own declared transitive
    `.fbs` closure for the schema being processed (the same closure that
    already drives Bazel action hermeticity — see `schema_codegen.bzl` /
    `aorta_flatbuffers.bzl` / `message_bindings.bzl`'s `--include-fbs`
    wiring). This closure is the ROOT schema's FULL combined closure (one
    module-level manifest per `gen_message_wrapper` invocation, reused for
    every include resolved while recursively walking the whole graph — see
    `_INCLUDE_MANIFEST` in gen_message_wrapper.py) — it is NOT rescoped to
    the CURRENT includer's own closure.

    For "schemas/"-rooted (qualified) includes, the manifest is tried
    FIRST and is the only source of truth that can be correct for every
    consumer shape: directory-guessing (the fallback below) can never
    discover an include that resolves into a DIFFERENT Bazel module's
    sandboxed tree (e.g. a Tier-2 external Bzlmod consumer's own schema, in
    the consumer's own package, including `schemas/system/aorta_base.fbs`
    from the SEPARATE `@aorta` module — the file lives under
    `external/aorta~/message/...`, a path no consumer-relative directory
    walk can ever construct).

    For BARE sibling includes (`include "Pose.fbs";`, no "schemas/" prefix),
    flatc always resolves relative to the INCLUDING file's own directory —
    that is the entire semantic of a bare include. The deterministic
    same-directory candidate search is therefore tried FIRST for this case,
    NOT the manifest: two unrelated subtrees in the closure can each
    legitimately contain a same-basename file (e.g. two different
    `Pose.fbs`), and since those are genuinely different paths
    `manifest_suffix_matches` correctly returns both — consulting the
    manifest first would misreport a same-directory-resolvable bare include
    as ambiguous. The manifest remains a
    fallback for bare includes whose deterministic candidate does not exist
    on disk (e.g. a relocated generated sibling), preserving prior
    coverage.
    """
    root_path = Path(root_fbs)
    candidates: list[Path] = []
    manifest_entries: list[Path] = [Path(m) for m in manifest] if manifest else []
    is_qualified_include = include_path.startswith("schemas/")

    def _manifest_lookup() -> Path | None:
        if not manifest_entries:
            return None
        matches = manifest_suffix_matches(include_path, manifest_entries)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise SchemaIncludeResolutionError(
                "ambiguous fbs include '{}' from root '{}': matches {} manifest "
                "entries (expected exactly one):\n  {}".format(
                    include_path,
                    root_fbs,
                    len(matches),
                    "\n  ".join(str(m) for m in matches),
                )
            )
        return None  # 0 matches — caller falls through to path candidates.

    if is_qualified_include and manifest_entries:
        manifest_hit = _manifest_lookup()
        if manifest_hit is not None:
            return manifest_hit
        # 0 matches — fall through to the deterministic path-candidate
        # search below as a safety net.

    if include_path.startswith("schemas/"):
        marker = "/schemas/"
        as_posix = root_path.as_posix()
        index = as_posix.rfind(marker)
        if index >= 0:
            message_root = Path(as_posix[:index])
            candidate = message_root / include_path
            candidates.append(candidate)
            if candidate.exists():
                return candidate
        for ancestor in root_path.parents:
            candidate = ancestor / "message" / include_path
            candidates.append(candidate)
            if candidate.exists():
                return candidate
            # Bazel places generated schemas under
            # bazel-out/<cfg>/bin/external/<repo>/message/..., while the
            # source schemas remain under execroot/external/<repo>/message/...
            # inside the same sandbox.
            parts = ancestor.parts
            if "external" in parts:
                external_index = len(parts) - 1 - parts[::-1].index("external")
                if external_index + 1 < len(parts):
                    repo_name = parts[external_index + 1]
                    for parent in ancestor.parents:
                        external_root = parent / "external" / repo_name
                        candidate = external_root / "message" / include_path
                        candidates.append(candidate)
                        if candidate.exists():
                            return candidate
            # When the root FBS is in a Bazel output tree (e.g.
            # bazel-out/<cfg>/bin/...), the source tree schemas are
            # accessible at the execution root without the "message/"
            # prefix because the sandbox mounts them there.
            candidate = ancestor / include_path
            candidates.append(candidate)
            if candidate.exists():
                return candidate
    else:
        # Same-directory sibling include (e.g. `include "Pose.fbs";`) —
        # deterministic, FIXED candidate set, never an open ancestor walk
        # (an open walk can match an unrelated same-basename file arbitrarily
        # far up the tree — the exact hazard the original design review
        # flagged). Exactly two candidate classes, checked in order:
        #
        # (a) the workspace SOURCE path for the include — source always
        #     wins over any (possibly stale) bazel-out artifact. When
        #     `root_fbs` is itself a plain source file, this is simply its
        #     own directory. When `root_fbs` is GENERATED (bazel-out
        #     prefixed), the ONLY generated-root shape in this codebase is
        #     `gen_action_wrapper`'s expanded action schema — always nested
        #     exactly ONE directory below its authored package
        #     (`{name}_action_gen/{name}.fbs` in aorta_flatbuffers.bzl's
        #     `aorta_action_library`, `{stem}_action/{root_fbs}` in
        #     schema_codegen.bzl's `_action_wrapper_out`) — so the source
        #     sibling is checked at both the stripped path's own directory
        #     (covers a hypothetical non-nested generated root) and one
        #     level up (covers the real, nested case). Both are single,
        #     fixed directories — not a walk.
        # (b) the EXACT bin-dir sibling directory of the includer
        #     (`root_path.parent`, unstripped) — covers a genrule that
        #     happens to co-locate a generated sibling next to `root_fbs`
        #     in the same output directory. Exact directory only.
        stripped = _strip_bazel_out_bin_prefix(root_path)
        if stripped is not None:
            source_candidates = [
                stripped.parent / include_path,
                stripped.parent.parent / include_path,
            ]
        else:
            source_candidates = [root_path.parent / include_path]

        for candidate in source_candidates:
            if candidate in candidates:
                continue
            candidates.append(candidate)
            if candidate.exists():
                return candidate

        bin_candidate = root_path.parent / include_path
        if bin_candidate not in candidates:
            candidates.append(bin_candidate)
            if bin_candidate.exists():
                return bin_candidate

        # No on-disk sibling — fall back to the manifest as a safety net
        # (e.g. a relocated generated sibling not literally co-located with
        # the includer). Tried AFTER same-directory determinism, unlike the
        # qualified-include case above, precisely so an on-disk sibling
        # always wins over a same-basename file elsewhere in the closure.
        manifest_hit = _manifest_lookup()
        if manifest_hit is not None:
            return manifest_hit

    manifest_note = (
        "; considered {} declared manifest entry(ies), none matched by path "
        "suffix:\n  {}".format(
            len(manifest_entries),
            "\n  ".join(str(m) for m in manifest_entries),
        )
        if manifest_entries
        else "; no declared include manifest was given for this invocation"
    )
    raise SchemaIncludeResolutionError(
        "cannot resolve fbs include '{}' from root '{}'{}; also searched {} "
        "path candidate(s) and none exist:\n  {}".format(
            include_path,
            root_fbs,
            manifest_note,
            len(candidates),
            "\n  ".join(str(c) for c in candidates),
        )
    )
