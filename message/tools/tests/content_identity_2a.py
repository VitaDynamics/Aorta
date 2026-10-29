#!/usr/bin/env python3
"""Content-identity harness for the wrapper canonical-order change.

The canonical-order patch to
`gen_message_wrapper.py` is a one-time, reviewed *format* change — table/
union/enum iteration order, union variant order, include-module order, and
(for tables whose fields carry out-of-declaration-order explicit ids, e.g.
`DataUploadGoal`) field order all move to a fully-specified canonical key.
No wire-format or semantic content is supposed to change.

This script proves "reorder-only, no content drift" for a directory of
BEFORE (old-parser) and AFTER (canonicalized-parser) generated wrapper
files, sharing the same basenames, by comparing each pair as trees of
brace-delimited chunks — not as raw text, and not as a flat bag of lines
(a flat per-line bag would miss content that moved between two different
enclosing blocks while coincidentally preserving the whole-file line
multiset).

Design history (condensed)
--------------------------
Earlier versions of this harness tried to TOLERATE the known-safe reorders
with a text-pattern/schema-lookup mechanism. Adversarial testing found a
genuinely new false-pass shape in each refinement of it:

1. A pooled-multiset comparison across DIFFERENT mismatched parents let
   content silently "reconcile" between two unrelated functions. Fixed by
   identity-paired recursion: sibling chunk lists are compared as an
   unordered multiset only for elements that already match byte-for-byte;
   any remainder is paired by a stable identity key (a chunk's own header
   line) and recursed into PER PAIR, never pooling two different parents'
   children into one shared bag. This part of the design is still in
   place and remains the correctness argument for cross-parent
   isolation — see `compare_groups` below.
2. A single GROUP-WIDE "does ANY statement in this body look
   field-tagged" boolean let one legitimately-tagged statement drag EVERY
   other statement in the same function body (tagged or not) into
   unordered comparison, silently admitting unrelated content drift.
   Fixed by per-statement field-key partitioning.
3. Per-key partitioning allowed a field's statement to cross FIXED
   structural positions (a builder function's own `init`/`header`/
   `finish` points) with no protection at all, plus a casing-drift gap
   where two spellings of the same field (`foo_offset` vs `.AddFoo(`)
   were never unified into one identity. Fixed by structural-anchor
   segmentation plus an optional schema-ground-truth field lookup
   (`--bfbs-map`, backed by `schema_reflection.py`).
4. `CopyDataUploadGoalMsg` — a C++ struct-copy helper for the same
   `DataUploadGoal` table the already-tolerated builder functions
   handle — reorders its `type` field statement exactly like those
   builder functions (same root cause: `type` carries an explicit
   `(id: 8)` but is declared 2nd), but has NO `root_builder`/`finish()`
   anchors at all (it copies directly into a plain struct, never through
   a FlatBuffers builder) and its name does not match the
   `build_..._typed`/`Build...Typed` shape ground-truth resolution keys
   off — so a statement moved past this function's own structural
   equivalent of "finish" (its `return`) had ZERO protection: the anchor
   mechanism never even engaged for this shape. Additionally: (a)
   `--bfbs-map`'s `schema_reflection` import was never actually reachable
   via this tool's own Bazel target — `bazel run
   //message/tools:content_identity_2a -- ... --bfbs-map ...` failed with
   `ModuleNotFoundError`; earlier verification only ever exercised it via
   an ad-hoc `PYTHONPATH` workaround, never the documented path — and (b)
   an EMPTY `--bfbs-map` silently produced the exact same "121/121
   passed" verdict as a real one, with no way to tell from the tool's own
   output whether ground truth was used anywhere at all — making the
   acceptance claim unfalsifiable from the tool's own output.

Rather than a fifth refinement of pattern/anchor recognition (each such
fix had produced a new, real, adversarially-discovered gap), the whole
text-pattern/schema-lookup tolerance mechanism was REMOVED (`_field_key`,
`_anchor_kind`/`_segment_by_anchors`,
`_resolve_schema_fields_for_header`/`_statement_identity`, the
`--bfbs-map` CLI flag and its `schema_reflection` runtime dependency) and
replaced with the design below.

Final design: strict comparison, zero in-tool tolerance
-------------------------------------------------------
The generator's canonical-order patch reorders SOURCE STATEMENTS for
build reproducibility; it does not and must not change the WIRE FORMAT,
since FlatBuffers' binary layout is governed by each field's declared
`id` (recorded in the vtable), never by the generator's own loop/emission
order. This means: for any function whose statement order legitimately
changes, building a fully-populated message with distinguishable
(sentinel) values in every field and round-tripping it (encode with the
NEW generator's output, decode via FlatBuffers' own id-keyed accessors,
assert every field survives exactly) is a complete, direct,
text-shape-independent proof of safety — it never has to recognize or
reason about STATEMENT TEXT SHAPE at all, which is exactly the axis every
false-pass bug found across this harness's design history (condensed
above) lived on. This script's
own job is narrowed accordingly: report EVERY textual difference,
honestly and without exception, and leave the "is this specific known
reorder safe" question entirely to the compiled round-trip tests and
human review — never to this script.

1. **Strict, fully-ordered comparison, unconditionally, no exception.** A
   statement-body chunk (a function/method — anything `classify()` does
   NOT call `"unordered"`) compares its direct children as an EXACT,
   ORDERED list (`old_children == new_children`). No allowlist, no
   anchors, no per-field identity inference, no schema ground truth, no
   name matching of any kind: this is the ENTIRE acceptance argument for
   EVERY function, with no exception for any (file, function) pair, known
   or unknown.

2. **Why there is no reorder-tolerance mechanism left at all.** Later
   attempts at an AUTOMATED "is this one of the known-safe reorders"
   check — group-wide field tag, per-statement key + structural anchors,
   extracted-function-name matching, exact full-header-text matching —
   each closed the previous adversarial counter-example while introducing
   a NEW one on the exact same axis: inferring "is this a reorder I should
   tolerate" from the STATEMENT BODY'S OWN TEXT. The final hole:
   even byte-exact header-text equality is not a sound function identity,
   because a local signature carries no PARENT SCOPE information — two
   legitimately different, differently-namespaced C++/Rust functions can
   share an identical local header. Per the same "narrow over general
   heuristics" principle that ended a similar chain of fixes in
   `patch_flatc_rust.py`, a further attempt at
   smarter pattern-matching was rejected in favor of removing the
   mechanism's premise entirely: no in-tool exception, period.

   The three specific functions identified as legitimate
   canonical-order reorders (`DataUploadGoal.type` carries an explicit `(id: 8)`
   but is declared 2nd in
   `message/schemas/action/data_upload/data_upload.fbs`; canonical
   (id-sorted) emission moves its statement to last in
   `BuildDataUploadGoalTyped`/`CopyDataUploadGoalMsg`/
   `build_datauploadgoal_typed` — a structural,
   order-blind diff over all 121 real wrapper artifacts in both
   languages found NO other (artifact, function) pair with any
   difference beyond this one) now correctly show up as ordinary "content
   differs" entries in this harness's own output, exactly like any other
   real difference. The safety argument for those three is NOT this
   script — it is the compile+round-trip tests:
   `cpp/test/data_upload_goal_reorder_roundtrip_test.cc`
   (`//cpp:data_upload_goal_reorder_roundtrip_test`) and
   `rust/test/data_upload_goal_reorder_roundtrip_tests.rs`
   (`//rust:data_upload_goal_reorder_roundtrip_test`): both build the
   CURRENT generator's real output, populate every field of a
   DataUploadGoal with a distinguishable sentinel, and assert every field
   survives an actual encode/decode round-trip — direct runtime-behavior
   proof, independent of source statement order. These 3 functions'
   reorder in the 121-artifact comparison is a known, human-reviewed,
   compile+round-trip-verified difference — not a silent tool exemption.

The identity-pairing/never-pool-across-parents design (see
"Design history" point 1 above) is still in place and remains load-bearing: it
is what lets a genuinely moved statement between two DIFFERENT functions
still fail even when both functions are otherwise unchanged.

Algorithm
---------
1. Split each file into "depth-0" chunks: maximal runs of lines whose net
   `{`/`}` AND `(`/`)` balance both return to <=0 (top-level statements and
   top-level `{ ... }` blocks, taken whole). Tracking paren-depth alongside
   brace-depth keeps a multi-line function signature (parameters and/or a
   tuple return type spanning several lines before the opening `{`)
   together with its body as ONE chunk, so its first line is always the
   real, informative header (`fn build_x_typed<'a>(`) — never a
   meaningless continuation fragment like a bare `) {`, which would defeat
   both allowlist-name matching and identity-based pairing.
2. Compare two sibling chunk lists (starting with the whole file's own
   top-level list) as an unordered multiset. Elements common to both sides
   cancel out immediately — no per-item comparison needed for the (usual)
   case where most declarations are untouched.
3. For whatever remains mismatched, pair each OLD chunk with its NEW
   counterpart by identity key (first line, stripped) and recurse into
   EACH PAIR's own children ONLY — never pooling multiple pairs' children
   into one shared bag (the core fix for cross-parent content movement).
   - If the parent's header introduces a declaration list (`struct`/
     `enum`/`union`/`impl`/`trait`/`mod`/`namespace`/`class`), the pair's
     children are compared as one unordered multiset (any declaration may
     reorder relative to any other); whole,
     self-contained declarations reordering relative to each other has no
     wire-format or behavioral implication regardless of position.
   - Otherwise the parent is a statement body (most commonly a function/
     method): compared as an exact, ORDERED list, unconditionally — no
     exception for any `(file, function)` pair.
4. Recurse until every mismatch resolves to a match or bottoms out at a
   genuine, irreconcilable difference — reported as a FAIL.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

Chunk = tuple[str, ...]


def _clean_lines(text: str) -> list[str]:
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def depth0_chunks(lines: list[str]) -> list[Chunk]:
    """Split `lines` into maximal runs that return to BOTH brace-depth <= 0
    AND paren-depth <= 0.

    Tracking paren-depth keeps a multi-line function signature (Rust
    `fn build_x_typed<'a>(\\n    param,\\n) -> (\\n    Type,\\n) {`, or a
    C++ equivalent) accumulating as ONE chunk through to its closing `}`
    — otherwise each zero-brace parameter line would close as its own
    atomic chunk (since brace-depth alone returns to 0 after every such
    line), leaving the chunk that actually carries the function's BODY
    headed by an uninformative continuation fragment (a bare `") {"`)
    instead of the function's real name. This does not change any
    EXISTING chunk boundary that already worked correctly: guard/attribute
    lines (`#ifndef`/`#define`/`#endif`/`#[derive(...)]`) and single-line
    declarations/statements have no parens spanning multiple lines, so
    their depth-0-brace-AND-paren state is unaffected.
    """
    chunks: list[Chunk] = []
    current: list[str] = []
    brace_depth = 0
    paren_depth = 0
    for line in lines:
        current.append(line)
        brace_depth += line.count("{") - line.count("}")
        paren_depth += line.count("(") - line.count(")")
        if brace_depth <= 0 and paren_depth <= 0:
            chunks.append(tuple(current))
            current = []
            brace_depth = 0
            paren_depth = 0
    if current:
        chunks.append(tuple(current))
    return chunks


# A chunk's header (first line) classifies it as a declaration-list
# introducer (its children are further independent declarations/fields,
# which the canonical-order change legitimately reorders) versus, by
# default, a statement body (most commonly a `fn`/method signature).
# Anchored at the START of the line (`^`, plus `\b` after the keyword) so
# a builder function whose NAME happens to contain one of these words as a
# substring (e.g. `fn build_choice_union_typed(...)`) is never
# misclassified — the keyword must be the line's own leading token, not
# merely present somewhere in it.
_DECL_KEYWORD_RE = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:unsafe\s+)?"
    r"(?:mod|struct|enum|union|impl|trait|namespace|class)\b"
)


def classify(chunk: Chunk) -> str:
    """Return `"unordered"` for a declaration-list-introducing chunk,
    `"ordered"` (the default) for everything else -- see the module
    docstring."""
    return "unordered" if _DECL_KEYWORD_RE.match(chunk[0].strip()) else "ordered"


def _match_key(chunk: Chunk) -> str:
    """Stable identity for pairing a mismatched OLD chunk with its
    corresponding NEW chunk -- the canonical-order change reorders WHICH
    declarations/fields/builder-statements come first, it never renames or
    merges them, so a chunk's own header (first line) reliably identifies
    "the same" chunk even when its position, or its inner content,
    differs."""
    return chunk[0].strip()


def _split_header_body_footer(chunk: Chunk) -> tuple[Chunk, list[str], Chunk]:
    """Split `chunk` into (header, middle, footer) for decomposition.

    The header is EVERY line up to and including the one that first opens
    brace-depth from 0 to positive — NOT just `chunk[0]`. For a
    single-line declaration header (`struct Foo {`, `impl X for Y {`,
    `pub mod typed {`) this is the same one line as before; for a
    multi-line function signature (params and/or a tuple return type
    spanning several lines before the brace — see `depth0_chunks`'s own
    docstring) this correctly captures the WHOLE signature, not just its
    first fragment. Treating only `chunk[0]` as "the header" would leave
    the middle inheriting an already-open, unmatched paren/brace count
    from lines that should have stayed part of the header instead —
    silently producing one giant leftover chunk instead of the real
    per-statement decomposition.

    The footer is the chunk's own last line (this generator's actual
    output always closes a chunk on its own dedicated final line, e.g.
    `}` or `};`).
    """
    brace_depth = 0
    header_end = len(chunk) - 1
    for i, line in enumerate(chunk):
        brace_depth += line.count("{") - line.count("}")
        if brace_depth > 0:
            header_end = i
            break
    header = chunk[: header_end + 1]
    footer = chunk[-1:]
    if header_end >= len(chunk) - 1:
        return header, [], footer
    middle = list(chunk[header_end + 1 : -1])
    return header, middle, footer


# ---------------------------------------------------------------------------
# A former design: explicit reorder allowlist keyed
# by the EXACT, byte-verified, whitespace-normalized header text of each
# real, compile+round-trip-verified function — not a "declared function
# name" extracted via any regex. Two prior attempts at name EXTRACTION each
# had a real adversarial hole: (a) a bare `\bname\b` search over the whole
# header matched a PARAMETER name/type equal to an allowlisted name; (b) a
# "leftmost identifier immediately followed by `(`" regex matched a token
# inside a SAME-LINE doc-comment/attribute string (e.g.
# `#[doc = "build_datauploadgoal_typed()"] fn completely_different() {`)
# that merely LOOKED like a call before the real declared name later on the
# same line. Every one of these bugs lived on the same axis: trying to
# infer "is this the name" from partial text shape. Exact, full-header
# string equality has no such axis to fail on — an unrelated function's
# header, whatever it contains, is never byte-identical to one of these 3
# specific, real headers.
#
# Final disposition: even exact,
# byte-verified full-header equality was not a sound identity for "this is
# the SAME function" -- a local header carries no parent
# scope information, so two DIFFERENT, legitimately-namespaced C++/Rust
# functions can share an IDENTICAL local signature (same name, same
# parameters, same return type) while being genuinely different symbols.
# This was the fourth real hole found across four consecutive attempts at
# an AUTOMATED "is this one of the known-safe reorders" check (group-wide
# tag, per-key partition + anchors, name extraction, exact-header
# equality) -- each fix closed the PRIOR attempt's adversarial gap and
# introduced a new one on the same "some form of pattern-matching against
# the STATEMENT-BODY'S OWN TEXT decides whether to tolerate a reorder"
# axis. Per the same "narrow over general heuristics" principle that
# ended a similar chain of fixes in patch_flatc_rust.py, this axis is
# now abandoned entirely rather than attempted a fifth time:
#
# There is no more automated reorder-tolerance mechanism in this file.
# EVERY statement body compares strictly (`old_children == new_children`,
# exact order), full stop, with NO exception for any (file, function)
# pair, known or unknown. The three DataUploadGoal functions already
# identified, compile+round-trip verified, and formerly registered here
# (`BuildDataUploadGoalTyped`, `CopyDataUploadGoalMsg`,
# `build_datauploadgoal_typed` -- see git history for the exact reasoning)
# now correctly show up as ordinary "content differs" entries in this
# harness's own 121-artifact run, exactly like anything else that
# genuinely differs. That is the INTENDED, honest behavior: this script's
# job is now only to enumerate every textual difference, full stop, never
# to decide FOR a human which ones are safe.
#
# The actual safety argument for those three, specific, known reorders is
# NOT this script -- it is the compile+round-trip tests:
#   - cpp/test/data_upload_goal_reorder_roundtrip_test.cc
#     (//cpp:data_upload_goal_reorder_roundtrip_test)
#   - rust/test/data_upload_goal_reorder_roundtrip_tests.rs
#     (//rust:data_upload_goal_reorder_roundtrip_test)
# Both build the CURRENT (canonicalized) generator's real output, populate
# every field of a DataUploadGoal with a distinguishable sentinel, and
# assert every field survives an actual encode/decode round-trip -- direct
# runtime-behavior proof, independent of how the generator's own source
# statements happen to be ordered. These 3 functions' reorder is a known,
# human-reviewed, compile+round-trip-verified difference in the
# 121-artifact comparison -- not a silent tool exemption.


def compare_groups(
    old_chunks: list[Chunk],
    new_chunks: list[Chunk],
) -> tuple[bool, list[str]]:
    """Compare two SIBLING chunk lists (already known to share one parent
    scope -- the whole file's own top level, to start). Elements common to
    both sides cancel immediately; any remainder is paired by identity key
    (never pooled across different parents) and recursed into.

    No allowlist, no reorder tolerance for statement bodies at all --
    see the module comment above for why. The ONLY remaining
    order-insensitive comparison is for a declaration-list container
    (struct/enum/union/impl/trait/mod/namespace/class -- `classify() ==
    "unordered"`): those may freely reorder their own top-level members,
    which is the canonical-order change's actual, intended
    canonicalization target (table/union/enum iteration order etc.).
    """
    oc = Counter(old_chunks)
    nc = Counter(new_chunks)
    common = oc & nc
    diff_old = list((oc - common).elements())
    diff_new = list((nc - common).elements())
    if not diff_old and not diff_new:
        return True, []

    old_by_key: dict[str, list[Chunk]] = {}
    for c in diff_old:
        old_by_key.setdefault(_match_key(c), []).append(c)
    new_by_key: dict[str, list[Chunk]] = {}
    for c in diff_new:
        new_by_key.setdefault(_match_key(c), []).append(c)

    problems: list[str] = []
    for key in sorted(set(old_by_key) | set(new_by_key)):
        olds = old_by_key.get(key, [])
        news = new_by_key.get(key, [])
        if len(olds) != len(news):
            for o in olds:
                problems.append(f"  - only in OLD (x1): {o[0]!r}" + (" ..." if len(o) > 1 else ""))
            for n in news:
                problems.append(f"  - only in NEW (x1): {n[0]!r}" + (" ..." if len(n) > 1 else ""))
            continue
        for o, n in zip(olds, news):
            if o == n:
                continue
            if len(o) <= 1 or len(n) <= 1:
                # Atomic (single-line) chunks sharing a header but
                # differing overall can't happen (their whole text IS
                # their header) -- but guard defensively.
                problems.append(f"  - content differs: {o[0]!r} vs {n[0]!r}")
                continue
            o_header, o_middle, o_footer = _split_header_body_footer(o)
            n_header, n_middle, n_footer = _split_header_body_footer(n)
            o_children = depth0_chunks(o_middle)
            n_children = depth0_chunks(n_middle)
            header_footer_ok = o_header == n_header and o_footer == n_footer
            if classify(o) == "unordered":
                # Declaration list (struct/enum/union/impl/trait/mod/
                # namespace/class): every child may freely reorder
                # relative to every other.
                ok, sub = compare_groups(
                    [o_header, o_footer] + o_children,
                    [n_header, n_footer] + n_children,
                )
                if not ok:
                    problems.extend(sub)
            elif not header_footer_ok:
                problems.append(f"  - content differs: {o[0]!r} vs {n[0]!r}")
            else:
                # Statement body (most commonly a function/method): strict,
                # exact order, unconditionally -- no exception.
                if o_children != n_children:
                    problems.append(f"  - content differs (strict order required): {o[0]!r} vs {n[0]!r}")
    return (len(problems) == 0), problems


def compare_file(old_path: Path, new_path: Path) -> tuple[bool, str]:
    """Compare one (old, new) wrapper output file pair.

    Returns `(ok, detail)`. No allowlist accounting: every
    statement body is compared strictly, unconditionally, so there is
    nothing to exercise/track beyond the plain pass/fail result itself.
    """
    old_lines = _clean_lines(old_path.read_text(encoding="utf-8"))
    new_lines = _clean_lines(new_path.read_text(encoding="utf-8"))
    old_chunks = depth0_chunks(old_lines)
    new_chunks = depth0_chunks(new_lines)
    ok, problems = compare_groups(old_chunks, new_chunks)
    if ok:
        return True, ""
    # Neutral framing: this comparison cannot tell
    # whether the underlying cause was a pure reorder or a genuine
    # addition/removal/modification -- claiming "(not just reordering)"
    # would overstate what a strict ordered-list mismatch actually proves
    # for the (known) case where it really was only a reorder.
    lines = ["strict ordered-text difference detected:"]
    lines.extend(problems)
    return False, "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old_dir", type=Path, help="directory of pre-canonicalization outputs")
    parser.add_argument("new_dir", type=Path, help="directory of post-canonicalization outputs")
    args = parser.parse_args()

    old_files = {p.name: p for p in args.old_dir.iterdir() if p.is_file()}
    new_files = {p.name: p for p in args.new_dir.iterdir() if p.is_file()}

    missing_in_new = sorted(set(old_files) - set(new_files))
    missing_in_old = sorted(set(new_files) - set(old_files))
    if missing_in_new or missing_in_old:
        print("FAIL: mismatched file sets between old_dir and new_dir")
        for name in missing_in_new:
            print(f"  - missing in new_dir: {name}")
        for name in missing_in_old:
            print(f"  - missing in old_dir: {name}")
        return 1

    failures = []
    for name in sorted(old_files):
        ok, detail = compare_file(old_files[name], new_files[name])
        status = "PASS" if ok else "FAIL"
        print(f"{status}  {name}")
        if not ok:
            print(detail)
            failures.append(name)

    total = len(old_files)
    print()
    print(f"content-identity harness: {total - len(failures)}/{total} files reorder-only-identical")
    if failures:
        print(f"FAILED files ({len(failures)}): {', '.join(failures)}")
    print(
        "every statement body, in every file, was compared under the "
        "strict, fully-ordered rule -- no allowlist, no exceptions (round "
        "8, final; see module docstring). The 3 known DataUploadGoal "
        "functions (BuildDataUploadGoalTyped, CopyDataUploadGoalMsg -- both "
        "in data_upload_msg.h; build_datauploadgoal_typed, in "
        "data_upload_msg_rs.rs) are therefore EXPECTED to make those 2 "
        "FILES show up above as FAILED -- their reorder is a real, "
        "human-reviewed, compile+round-trip-verified difference (see "
        "//cpp:data_upload_goal_reorder_roundtrip_test and "
        "//rust:data_upload_goal_reorder_roundtrip_test), not something "
        "this script tolerates. A human reviewing this output cross-checks "
        "that (a) the FAILED FILES are exactly those 2, and (b) each "
        "FAILED file's own detail lines name exactly its known function(s) "
        "above and nothing else."
    )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
