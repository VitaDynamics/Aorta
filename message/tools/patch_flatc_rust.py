#!/usr/bin/env python3
"""Rewrite flatc's Rust wrapper output so cross-crate (foreign) type references
resolve — GENERAL, loader-driven ABSOLUTE-PATH rewriting.

flatc generates one `.rs` file PER schema. For a type from an INCLUDED schema
(a "foreign" type, declared in a different `.fbs` -> a different
`<stem>_generated` crate module) it emits a namespace-RELATIVE path
`super::(super::)*<rel path>::<Type>`, expecting the referenced module to sit at
the common-namespace-prefix ancestor. Across separately-generated files that
ancestor module does not exist locally, so the reference dangles (E0412 / E0433)
or resolves ambiguously through the per-scope glob imports (`use
crate::<dep>_generated::*;`, E0659) whenever the include closure reaches a
namespace tree provided by 2+ crates.

Historically this was patched by a growing pile of narrow, text-triggered
"needles" (sys / services::common / action::common re-export injections, a
CameraCalibration strip) plus a recursive module-tree SYNTHESIS pass. Each
new namespace SHAPE found in testing was one that the
trigger/placement heuristics mishandled. This module replaces all of that with a
single robust transform:

    For EVERY foreign type in the root schema's include closure (ground truth
    from the compiled `.bfbs`), rewrite its exact relative reference
    `super::(super::)*<escaped rel path>::<escaped Type>` to the ABSOLUTE
    `crate::<dep>_generated::<escaped full namespace>::<escaped Type>`.

Absolute paths resolve directly into the providing dep crate the wrapper already
declares (`pub mod <dep>_generated;`), so there are no synthesized modules, no
positional injection, no idempotency question, and no dependence on a namespace
SHAPE — a class of bugs is eliminated rather than guarded. The reference string
each foreign type produces is DETERMINISTIC (flatc's super count = local
namespace depth - common-prefix depth; the relative path = the foreign
namespace's segments below the common prefix), so it is computed exactly and
matched with a boundary-anchored substitution. The ONLY remaining failure mode
is two foreign types whose escaped reference string is identical but whose
target crate differs (e.g. the same fully-qualified type from two crates, or two
namespaces that escape to the same Rust path) — an ambiguity flatc's own output
cannot compile either — which HARD-FAILS with `ForeignRefConflict`.

Same-namespace foreign types (an included `.fbs` sharing the root's OWN
namespace, e.g. `foxglove.Time` in a `foxglove` schema) are emitted by BARE type
name — and a bare identifier appears in EVERY syntactic type position flatc's
templates produce (return type, field annotation, `Vector`/`Array` element, the
`&*(ptr as *const T)` struct cast, `T::VARIANT` value path, …). Text-anchoring
those positions proved to be whack-a-mole (each fix surfaced another
one), so they are NOT rewritten by text; instead a precise `use` alias
(`use crate::<dep>_generated::<escaped ns>::<Type>;`) is injected into the module
where the bare references live (the local namespace's leaf module — see
`_inject_module_use_aliases`), and Rust name resolution resolves every position.
This also leaves string literals and definition names untouched by construction.
A same-namespace foreign name that keyword-escapes to a ROOT-LOCAL type's name is
an unresolvable ambiguity and HARD-FAILS with `ForeignRefConflict`.

`--root-bfbs` is required to compute the rewrites/aliases; without it (a leaf
schema with no cross-tree references) only the local-namespace flattening
re-export is applied, exactly as before.
"""

import argparse
from pathlib import Path
import re
import sys

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

from schema_include_utils import (  # noqa: E402
    flatc_rust_escape_ident,
    flatc_rust_namespace_path,
    flatc_rust_namespace_segment,
)

# A top-level `pub mod <seg> {` opening and its `}  // pub mod <seg>` close (flatc
# does NOT indent nested modules, so nesting is tracked by these markers, not by
# column). Used to flatten EVERY top-level namespace nest to the generated file's
# crate root for bare-name access. A schema whose local types and foreign
# `root_type` live in DIFFERENT namespaces emits TWO top-level nests,
# so all of them are flattened, not just the first.
_PUB_MOD_OPEN_LINE = re.compile(r"^\s*pub mod ([A-Za-z_][A-Za-z0-9_]*) \{\s*$")
_PUB_MOD_CLOSE_LINE = re.compile(r"^\s*\}\s*//\s*pub mod ")


class ForeignRefConflict(Exception):
    """Two foreign types produce the SAME escaped reference string but resolve
    to DIFFERENT crates — e.g. the same fully-qualified type provided by 2+
    crates, or two source namespaces that escape to the same Rust path. flatc's
    own one-file-per-schema output cannot compile this either; the rewrite fails
    loudly instead of picking an arbitrary (possibly wrong) target."""


def _crate_stem_for(declaration_file):
    """`<…>/<stem>.fbs` -> `<stem>_generated` (the flatc-generated Rust module
    name the wrapper declares via `#[path] pub mod <stem>_generated;` and every
    generated file glob-imports as `use crate::<stem>_generated::*;`). Only the
    trailing basename is consulted — the prefix's shape is context-dependent
    (in-repo relative vs external sandbox), exactly as
    gen_message_wrapper._loaded_stem_from_decl documents."""
    return Path(declaration_file).stem + "_generated"


def _reference_and_absolute(local_segs, foreign_segs, type_name, crate):
    """Pure: the exact relative reference flatc emits for one foreign type, and
    its absolute crate-path replacement.

    For a foreign type in namespace F (`foreign_segs`) referenced from the local
    namespace R (`local_segs`):
      - c          = length of the longest common namespace prefix of R and F;
      - super_count= len(R) - c  (flatc climbs from R's depth to the common
                     ancestor);
      - rel_path   = F[c:] as flatc namespace SEGMENTS (keyword-escaped then
                     snake_cased -- the segments below the common ancestor).
    flatc emits `("super::" * super_count) + rel_path::<Type>`; the absolute is
    `crate::<crate>::<full flatc namespace path of F>::<Type>`. An EMPTY foreign
    namespace (no `namespace` in the dep) yields `super::…::<Type>` /
    `crate::<crate>::<Type>` (no namespace segment)."""
    c = 0
    for a, b in zip(local_segs, foreign_segs):
        if a != b:
            break
        c += 1
    super_count = len(local_segs) - c
    rel = "::".join(flatc_rust_namespace_segment(s) for s in foreign_segs[c:])
    esc_type = flatc_rust_escape_ident(type_name)
    reference = "super::" * super_count + (rel + "::" if rel else "") + esc_type
    ns_path = flatc_rust_namespace_path(".".join(foreign_segs)) if foreign_segs else ""
    absolute = "crate::{}::{}{}".format(
        crate, ns_path + "::" if ns_path else "", esc_type
    )
    return reference, absolute


def _build_rewrites(refs, local_names_by_module=None):
    """Pure core: split every foreign reference `(emission_namespace, (foreign
    namespace, foreign name, crate))` into the two resolution mechanisms, returning
    `(path_rewrites, bare_alias_modules)`. Each reference is computed relative to
    ITS OWN emission namespace -- the namespace of the ROOT-LOCAL type (or the root
    helpers) that emits it. A Tier-2 schema may declare root-local types in MULTIPLE
    namespaces (Tier-1 rejects that), and a foreign `root_type` lives in a namespace
    of its own, so there can be several emission namespaces in one file.
    `local_names_by_module` maps each emission-namespace segments tuple to the set of
    ROOT-LOCAL type names (keyword-escaped) declared in it -- the per-module
    alias-vs-local collision scope.

    - CROSS-namespace refs (emission != foreign ns) are emitted `super::…::Type` and
      TEXT-rewritten globally to absolute -> `path_rewrites{reference: absolute}`.
      Safe across multiple emission namespaces: a `::`-bearing token never occurs in
      a string or a definition, and the by-reference check HARD-FAILS if one
      `super::…` string ever maps to two absolutes (an unresolvable global rewrite).
    - SAME-namespace refs (emission == foreign ns) are emitted BARE in EVERY
      syntactic type position (return type, field, `Vector`/`Array` element,
      `&*(ptr as *const T)` cast, `T::VARIANT` value path, …). Bare names cannot be
      text-matched precisely, so each is resolved by a `use` alias
      injected into ITS emission namespace's leaf -> `bare_alias_modules{emission
      segments tuple: {escaped Type: absolute}}`.

    HARD-FAILS (`ForeignRefConflict`) when a `super::…` reference or a per-module
    bare alias resolves to two DIFFERENT absolutes, or a bare alias collides with a
    root-local type's escaped name in the same module (keyword-escaping can map
    distinct source names to one identifier, foreign `Self` / local `Self_`)."""
    local_names_by_module = local_names_by_module or {}
    path_rewrites = {}       # `super::…::Type` -> absolute (cross-namespace TEXT rewrite)
    bare_alias_modules = {}  # emission segments tuple -> {escaped Type: absolute}
    by_ref = {}              # path reference -> set of absolutes (conflict scope)
    by_alias = {}            # (module tuple, escaped name) -> set of absolutes
    local_overlap = set()
    for emission_ns, (foreign_ns, foreign_name, crate) in refs:
        emission_segs = [s for s in (emission_ns or "").split(".") if s]
        foreign_segs = [s for s in (foreign_ns or "").split(".") if s]
        reference, absolute = _reference_and_absolute(
            emission_segs, foreign_segs, foreign_name, crate
        )
        if "::" in reference:
            by_ref.setdefault(reference, set()).add(absolute)
            path_rewrites[reference] = absolute
        else:
            key = tuple(emission_segs)
            by_alias.setdefault((key, reference), set()).add(absolute)
            bare_alias_modules.setdefault(key, {})[reference] = absolute
            if reference in local_names_by_module.get(key, ()):
                local_overlap.add((key, reference))
    path_conflicts = {r: a for r, a in by_ref.items() if len(a) >= 2}
    if path_conflicts:
        detail = "; ".join(
            "{!r} <- {}".format(ref, sorted(a)) for ref, a in sorted(path_conflicts.items())
        )
        raise ForeignRefConflict(
            "cross-namespace foreign reference(s) resolve ambiguously (one "
            "`super::…` path -> multiple crate paths, un-rewritable): {}".format(detail)
        )
    alias_conflicts = {k: a for k, a in by_alias.items() if len(a) >= 2}
    if alias_conflicts:
        detail = "; ".join(
            "{} in {} <- {}".format(name, list(mod), sorted(a))
            for (mod, name), a in sorted(alias_conflicts.items())
        )
        raise ForeignRefConflict(
            "same-namespace foreign alias(es) resolve ambiguously (one Rust name in "
            "one module -> multiple crate paths, a `use` clash): {}".format(detail)
        )
    if local_overlap:
        raise ForeignRefConflict(
            "same-namespace foreign alias(es) {} collide with a root-local type of "
            "the same escaped Rust name -- a `use` alias cannot be injected without "
            "clashing (keyword-escaping can map distinct source names to one "
            "identifier, e.g. foreign `Self` and local `Self_`)".format(
                sorted((list(mod), name) for (mod, name) in local_overlap)
            )
        )
    return path_rewrites, bare_alias_modules


def _referenced_foreign_refs(loaded, is_root_local):
    """Every FOREIGN reference flatc emits from a root-local type's accessors, as
    `(emission_namespace, (foreign_namespace, foreign_name, crate))`. The emission
    namespace is the DECLARING root-local type's namespace: flatc emits each type's
    accessors in its OWN namespace nest, so a Tier-2 schema with root-local types in
    MULTIPLE namespaces (Tier-1 rejects that) emits a `beta`-type's same-namespace
    reference BARE in the `beta` nest, not the first local namespace.
    Sources match flatc's emit rules:

      - a field's `target` (table/struct/enum/union used as a field or vector
        element) -- BUT deprecated fields are skipped, since flatc's
        `ForAllTableFields` emits no accessor for them;
      - a union field's variant targets, and every root-local union's variant
        targets;
      - a `nested_flatbuffer` attribute's target -- a `[ubyte]` field whose plain
        `target` is None but for which flatc emits a `*_nested_flatbuffer()`
        accessor referencing the nested root type.

    ROOT-LOCAL targets are excluded (they resolve locally). A FOREIGN `root_type` is
    added separately in `_compute_foreign_ref_rewrites` (emitted in ITS OWN
    namespace)."""
    refs = []

    def _add(emission_ns, target):
        if target is not None and not is_root_local(target, loaded):
            refs.append(
                (
                    emission_ns,
                    (target.namespace, target.name, _crate_stem_for(target.declaration_file)),
                )
            )

    for mapping in (loaded.tables, loaded.structs, loaded.unions):
        for entry in mapping.values():
            if not is_root_local(entry.type_id, loaded):
                continue
            emission_ns = entry.type_id.namespace
            for field in getattr(entry, "fields", None) or []:
                if getattr(field, "deprecated", False):
                    continue  # flatc emits no accessor -> no reference
                _add(emission_ns, field.target)
                _add(emission_ns, getattr(field, "nested_flatbuffer", None))
                for variant in field.union_variants or []:
                    _add(emission_ns, variant.target)
            for variant in getattr(entry, "variants", None) or []:
                _add(emission_ns, variant.target)
    return refs


def _compute_foreign_ref_rewrites(loaded):
    """Loader adapter: pull every ACTUALLY-REFERENCED foreign type from the compiled
    root `.bfbs` (ground truth) -- WITH its emission namespace -- and return
    `(path_rewrites, bare_alias_modules)` via `_build_rewrites`.

    - `path_rewrites`: `{super::…::Type: absolute}` for CROSS-namespace references.
    - `bare_alias_modules`: `{emission-namespace segments tuple: {escaped Type:
      absolute}}` for BARE references, keyed by the module flatc emits each in.
      Emission namespaces can differ within one file: each root-local
      type's field references are emitted in that type's OWN namespace, and a
      FOREIGN `root_type`'s helpers are emitted in the root type's OWN namespace."""
    import schema_reflection

    is_root_local = schema_reflection.is_root_local
    refs = list(_referenced_foreign_refs(loaded, is_root_local))

    # The FOREIGN root_type's helpers are emitted BARE in the ROOT TYPE'S OWN
    # namespace -> a reference from that emission namespace.
    root_type = getattr(loaded, "root_type", None)
    if root_type is not None and not is_root_local(root_type, loaded):
        refs.append(
            (
                root_type.namespace,
                (root_type.namespace, root_type.name, _crate_stem_for(root_type.declaration_file)),
            )
        )

    # Root-local type escaped names, grouped by their (raw) namespace segments --
    # the PER-MODULE alias-vs-local collision scope. A Tier-2 schema
    # may declare root-local types in several namespaces, each its own module.
    local_names_by_module = {}
    for mapping in (loaded.tables, loaded.structs, loaded.enums, loaded.unions):
        for entry in mapping.values():
            if is_root_local(entry.type_id, loaded):
                key = tuple(s for s in (entry.type_id.namespace or "").split(".") if s)
                local_names_by_module.setdefault(key, set()).add(
                    flatc_rust_escape_ident(entry.type_id.name)
                )

    return _build_rewrites(refs, local_names_by_module)


# A Rust double-quoted string literal (with `\"` escapes). A cross-namespace
# `super::…::Type` never appears in a string, but PATH rewriting still masks
# strings so no `::`-bearing text substitution can ever touch literal contents
# (see `_rewrite_foreign_refs`).
_STRING_LITERAL = re.compile(r'"(?:\\.|[^"\\])*"')


def _rewrite_foreign_refs(text, rewrites):
    """Text-rewrite each CROSS-namespace foreign reference to its absolute crate
    path. Every `rewrites` key is a `super::…::Type` PATH (same-namespace bare
    references are handled structurally by `_inject_module_use_aliases`, NOT here).
    A `::`-bearing token is unambiguous — it never occurs as a string, a bare
    identifier, or a definition name — so a boundary anchor suffices:
      - leading `(?<![\\w:])` so a shorter super-chain (`super::sys::X`) is not
        matched inside a longer one (`super::super::sys::X`);
      - trailing `(?![\\w])` so `AortaHeader` does not match `AortaHeaderBuilder`.
    String literals are nonetheless MASKED (only the code spans between them are
    rewritten), so no text substitution can ever touch literal contents.
    An absolute replacement begins with `crate::` and contains no `super::`, so no
    substitution re-matches a previously-rewritten reference (order independent).
    Full-line `//` comments are left untouched."""
    if not rewrites:
        return text, False
    compiled = [
        (re.compile(r"(?<![\w:])" + re.escape(reference) + r"(?![\w])"), absolute)
        for reference, absolute in rewrites.items()
    ]

    def _apply_to_code(segment):
        """Apply every rewrite to a CODE segment (never a string-literal span)."""
        seg_changed = False
        for pattern, absolute in compiled:
            segment, n = pattern.subn(absolute, segment)
            if n:
                seg_changed = True
        return segment, seg_changed

    changed = False
    out = []
    for line in text.split("\n"):
        if line.lstrip().startswith("//"):
            out.append(line)
            continue
        # Rewrite only the CODE spans between string literals; pass literals
        # through verbatim (see `_STRING_LITERAL`).
        pieces = []
        last = 0
        for match in _STRING_LITERAL.finditer(line):
            code_seg, seg_changed = _apply_to_code(line[last:match.start()])
            pieces.append(code_seg)
            pieces.append(match.group(0))
            changed = changed or seg_changed
            last = match.end()
        code_seg, seg_changed = _apply_to_code(line[last:])
        pieces.append(code_seg)
        changed = changed or seg_changed
        out.append("".join(pieces))
    return "\n".join(out), changed


def _inject_module_use_aliases(text, bare_alias_modules):
    """Bring each bare-referenced foreign type into scope with a `use` alias in the
    module where flatc emits its BARE references, letting Rust name resolution
    resolve EVERY syntactic type position — return types, fields, `Vector`/`Array`
    elements, the `&*(ptr as *const T)` struct cast, `T::VARIANT` value paths — with
    no per-position text matching, and leaving string literals / definition names
    untouched by construction.

    `bare_alias_modules` maps each emission namespace (segments tuple) to the
    `{escaped Type: absolute}` aliases that flatc emits BARE in that module.
    Placement is per-module: flatc emits a SEPARATE linear `pub mod` nest per
    emission namespace (local field refs land in the local leaf, the
    foreign `root_type` helpers in the root type's own leaf, and the two nests may
    differ). Each chain is descended INDEPENDENTLY by matching its escaped
    segments' `pub mod <seg> {` openings in order, and the `use` block is inserted
    just inside that leaf; an empty namespace means crate root (before the first
    item). The imported name IS the bare reference, and a specific `use` outranks
    the file-top `use crate::<dep>_generated::*;` glob, so it never conflicts with
    it. A clash with a ROOT-LOCAL type of the same escaped name is rejected earlier
    (ForeignRefConflict)."""
    if not bare_alias_modules:
        return text, False
    changed = False
    for ns_segs, aliases in bare_alias_modules.items():
        if not aliases:
            continue
        text, did = _inject_one_module(text, list(ns_segs), aliases)
        changed = changed or did
    return text, changed


def _module_leaf_insert_pos(text, esc_segs):
    """Char position just AFTER the `pub mod <leaf> {` line whose FULL module path
    equals `esc_segs`, or None. A module-path STACK is tracked (open on
    `pub mod X {`, close on `}  // pub mod X`) so a FORK under a shared prefix --
    e.g. a foreign root at `a::x::y` and a local leaf at `a::y`, two modules ending
    in `y` -- resolves to the correct leaf by FULL path, not first-match.
    flatc does not indent nested modules, so the stack, not the column, is
    authoritative."""
    stack = []
    offset = 0
    for line in text.splitlines(keepends=True):
        opening = _PUB_MOD_OPEN_LINE.match(line)
        if opening:
            stack.append(opening.group(1))
            if stack == esc_segs:
                return offset + len(line)  # start of the line AFTER `pub mod <leaf> {`
        elif stack and _PUB_MOD_CLOSE_LINE.match(line):
            stack.pop()
        offset += len(line)
    return None


def _inject_one_module(text, ns_segs, aliases):
    """Insert `use <absolute>;` for each alias just inside the module whose FULL
    namespace path is `ns_segs` (crate root when empty)."""
    use_block = "".join(
        "  use {};\n".format(absolute) for absolute in sorted(aliases.values())
    )
    esc_segs = [flatc_rust_namespace_segment(s) for s in ns_segs]
    if not esc_segs:
        # No namespace: bare references sit at crate root. Insert before the first
        # top-level item (attribute / `pub` / `impl` / bare `fn`).
        first_item = re.search(r"^(?:#\[|pub |impl |unsafe |fn )", text, re.MULTILINE)
        if first_item is None:
            return text.rstrip() + "\n\n" + use_block, True
        at = first_item.start()
        return text[:at] + use_block + text[at:], True
    insert_pos = _module_leaf_insert_pos(text, esc_segs)
    if insert_pos is None:
        # Chain not found (should not happen for a well-formed generated file).
        return text, False
    return text[:insert_pos] + use_block + text[insert_pos:], True


def _top_level_pub_mods(text):
    """The names of the TOP-LEVEL `pub mod`s, in emission order. flatc does not
    indent nested modules, so nesting is tracked by the `pub mod X {` openings and
    `}  // pub mod X` closes rather than by column."""
    tops = []
    depth = 0
    for line in text.split("\n"):
        opening = _PUB_MOD_OPEN_LINE.match(line)
        if opening:
            if depth == 0:
                tops.append(opening.group(1))
            depth += 1
            continue
        if depth > 0 and _PUB_MOD_CLOSE_LINE.match(line):
            depth -= 1
    return tops


def _inject_namespace_reexport(text):
    """Flatten EVERY top-level namespace nest to the generated file's crate root
    (`pub use self::<top>::*;`). Single-namespace schemas have one top (unchanged
    behavior); a schema whose local types and foreign `root_type` live in
    different namespaces has TWO tops, and BOTH are flattened."""
    export_lines = []
    for name in _top_level_pub_mods(text):
        line = f"pub use self::{name}::*;"
        if line not in text and line not in export_lines:
            export_lines.append(line)
    if not export_lines:
        return text, False
    patched = text.rstrip() + "\n\n" + "\n".join(export_lines) + "\n"
    return patched, True


def patch_text(text, path_rewrites=None, bare_alias_modules=None):
    changed = False
    text, patched = _rewrite_foreign_refs(text, path_rewrites)
    changed = changed or patched
    text, patched = _inject_module_use_aliases(text, bare_alias_modules)
    changed = changed or patched
    text, patched = _inject_namespace_reexport(text)
    changed = changed or patched
    return text, changed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Path to <stem>_generated.rs")
    parser.add_argument(
        "--root-bfbs",
        default=None,
        help=(
            "Compiled `.bfbs` for this schema's ROOT (the same one the wrapper "
            "genrule feeds gen_message_wrapper). When given, foreign-type "
            "references are rewritten to absolute `crate::<dep>_generated::…` "
            "paths from its ground-truth namespace/type map. When omitted (a "
            "leaf schema with no cross-tree references) only the local-namespace "
            "flattening re-export is applied."
        ),
    )
    args = parser.parse_args()

    path_rewrites = None
    bare_alias_modules = None
    if args.root_bfbs:
        import schema_reflection

        try:
            loaded = schema_reflection.load_bfbs_file(args.root_bfbs)
        except Exception as err:  # pragma: no cover - surfaced as a build error
            print(
                "patch_flatc_rust error: could not load --root-bfbs '{}': {}".format(
                    args.root_bfbs, err
                ),
                file=sys.stderr,
            )
            return 2
        path_rewrites, bare_alias_modules = _compute_foreign_ref_rewrites(loaded)

    path = Path(args.path)
    text = path.read_text(encoding="utf-8")
    patched, changed = patch_text(text, path_rewrites, bare_alias_modules)
    if changed:
        path.write_text(patched, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
