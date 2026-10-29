"""Public entry point for an EXTERNAL schema root.

One macro, two consumers:

1. An Aorta-owned product-schema module is the source root; Aorta keeps a
   canonical shadow package and the historical label face.
2. EDU users — a schema directory in the USER's own repository is the source
   root; the generated targets belong to the user's repo and are never
   re-exported into `@aorta//message`.

Both go through exactly ONE materialize/codegen chain (invariant E1): every
declared item is copied byte-for-byte into the CALLING package's canonical
`.fbs` slot, and the copy output — never the source — drives the same
BFBS/schema-meta/typed-wrapper pipeline the in-repo registry uses
(`aorta_define_schema_package`). The only differences between the two
consumers are the values of `source_package` / `source_prefix` / `fragment` /
`source_inventory` / `expected_provenance`, plus which trust boundary the
evidence is checked against.

The language surface is complete: C++, Python AND Rust, all three
driven off the SAME `<stem>_bfbs` (invariant E3). The Rust leg deliberately
reuses the existing BFBS-driven `gen_message_wrapper.py` +
`aorta_rust_message_binding` path rather than the old stem-guessing heuristic
wrapper in `aorta_flatbuffers.bzl`: what was missing was never
an emitter, only external-root WIRING — an explicit `schema_rel`, anchored
Aorta labels, and a module closure that Bazel's graph (not Aorta's registry
graph) computes, since a registry walk is blind to another module's schemas.
"""

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
load("//message/tools:external_root_gate.bzl", "internal_external_root_entry")
load("//message/tools:flatbuffers_rules.bzl", "python_msgs_tree")
load(
    "//message/tools:message_bindings.bzl",
    "aorta_cpp_message_wrapper",
    "aorta_rust_message_binding",
)
load("//message/tools:schema_codegen.bzl", "aorta_define_schema_package", "dep_fbs_closure_target")
load(
    "//message/tools:schema_names.bzl",
    "sanitize_crate_name",
    "schema_meta_py_enabled",
    "schema_publication",
    "schema_codegen_policy",
    _schema_message_base = "schema_message_base",
)

_MANIFEST_TOOL = Label("//message/tools:external_root_manifest")
_LINT_LIB = Label("//message/tools:external_root_lint_lib")
_LINT_MAIN = Label("//message/tools:external_root_lint_main.py")
_WORKSPACE_LINT_MAIN = Label("//message/tools:external_root_workspace_lint.py")
_REGISTRY_MANIFEST = Label("//message:schemas_registry_manifest")
_EXTERNAL_ROOTS_ALLOWLIST = Label("//message:schemas/external_roots.bzl")
_FLATBUFFERS_PY_DEP = Label("//message/tools:flatbuffers_py")
_AORTA_CPP_HEADERS = Label("//cpp:aorta_cpp_headers")

EXTERNAL_ROOT_MANIFEST_VERSION = 1

# An item that has a root type is first class in
# EVERY supported language — there is no per-call `languages` knob. That set
# is C++, Python AND Rust, and the typed/include-only policies below carry a
# member for each of the three.
#
# SCOPE: the completeness requirement binds consumer (b) — an EXTERNAL
# (EDU) root, whose whole point is that a user's schema surface is uniform in
# every language Aorta supports. It does NOT bind consumer (a), an `internal`
# root, because an internal root is not a second policy domain: its items ARE
# Aorta registry entries that merely live in another repository, and the policy
# they answer to is Aorta's own registry policy (message/tools/registry_lint.py
# + the frozen v3 manifest golden), which 86 of today's 220 internal entries
# deliberately do not satisfy. Forcing them would silently change 86 schemas'
# generated surface as a side effect of moving a FILE, which is precisely the
# drift the migration exists to avoid. `internal` is unforgeable — it is
# granted only by Aorta's own `schemas/external_roots.bzl` allowlist — so this
# scoping cannot be used by a user's repository to escape the policy.
#
# The FLOOR is unaffected: `_TYPED_REQUIRED_FLAGS` still travels in the
# manifest and the Python gate still refuses a manifest that shrinks it, so an
# external root cannot narrow the rules it is graded against.
#
# `bfbs` is the marker that separates the two shapes: it exists exactly when
# the schema has a root type, and it is the ONE artifact every typed surface
# derives from (invariant E3).
_TYPED_MARKER_FLAG = "bfbs"
_TYPED_REQUIRED_FLAGS = [
    "bfbs",
    "cpp",
    "cpp_wrapper",
    "python",
    "rust",
    "rust_wrapper",
    "schema_meta_cpp",
    "schema_meta_rust",
]

# An include-only item (no root type) may still need RAW codegen: a typed
# schema that includes it gets a generated header/module that includes the
# INCLUDED schema's generated header/module, so `cpp`/`python`/`rust` must be
# on for it too — for Rust literally so, since the wrapper emits a
# `#[path = ...] pub mod <stem>_generated;` for every schema in the include
# closure. What it may never carry is anything that presupposes a root type.
_INCLUDE_ONLY_FORBIDDEN_FLAGS = [
    "action_schema",
    "bfbs",
    "cpp_wrapper",
    "rust_wrapper",
    "schema_meta_cpp",
    "schema_meta_py",
    "schema_meta_rust",
]
_RAW_LANGUAGE_FLAGS = ["cpp", "python", "rust"]

# Kept (empty) rather than deleted: it is the mechanism by which a flag that
# Aorta does not yet generate a surface for fails LOUDLY instead of being
# silently dropped, and both gates read it from the manifest. Stage 3 emptied
# it because there is no longer any refused flag.
_REFUSED_FLAGS = []

# Fields an external-root fragment entry may carry. Kept in ONE place and
# shared with the Python lint through the generated manifest so the Starlark
# gate and the AST gate cannot drift apart.
EXTERNAL_ITEM_FIELDS = [
    "action_name",
    "action_schema",
    "alias_id",
    "bfbs",
    "cc_alias",
    "cpp",
    "cpp_lib_name",
    "cpp_wrapper",
    "cpp_wrapper_name",
    "dep_fbs",
    "extra_cc_deps",
    "pkg",
    "provenance",
    "python",
    "root_fbs",
    "rust",
    "rust_wrapper",
    "rust_wrapper_crate_name",
    "rust_wrapper_facade_name",
    "rust_wrapper_name",
    "schema_meta_cpp",
    "schema_meta_py",
    "schema_meta_rust",
    "stem",
]

# `edu_public` (the EDU schema pack's projection flag, set only by the schema
# module's fragments) and `include_in_examples_bfbs` (`//message:examples_bfbs`)
# are projection flags, so neither is part of the field set a consumer-(b)
# (EDU) fragment may use — a user's repository must not be able to nominate
# its schema for the pack or for an Aorta aggregate. An `internal` root is the
# opposite case: its items ARE registry entries, the flags are the entry's, and
# dropping one on the way through would silently change the pack surface
# (resp. shrink `//message:examples_bfbs`) the moment a family migrates. The
# field set is therefore per-provenance, and it travels in the manifest so the
# AST gate grades each root against its own.
EXTERNAL_ITEM_FIELDS_INTERNAL = EXTERNAL_ITEM_FIELDS + [
    "edu_public",
    "include_in_examples_bfbs",
]

EXTERNAL_ITEM_REQUIRED_FIELDS = ["pkg", "provenance", "root_fbs", "stem"]

_ALLOWED_PROVENANCE = ["external", "internal"]

_MISSING = "__AORTA_EXTERNAL_ITEM_MISSING__"


def _get(item, key, default = _MISSING):
    if type(item) == "dict":
        if key in item:
            return item[key]
    elif hasattr(item, key):
        return getattr(item, key)
    if default != _MISSING:
        return default
    fail("external schema item is missing required field '{}'".format(key))


def _allowed_item_fields(expected_provenance):
    if expected_provenance == "internal":
        return EXTERNAL_ITEM_FIELDS_INTERNAL
    return EXTERNAL_ITEM_FIELDS


def _item_fields(item, allowed_fields):
    """The item's declared fields, keyed on PRESENCE rather than on non-None.

    `cc_alias = None` is a real registry spelling — it is how an entry says
    "this schema gets no `<stem>_cc` alias" — and a value-based filter erases
    it, so the manifest would omit a field the fragment TEXT declares and
    `external_root_lint._check_fragment` would report the two as differing on
    `cc_alias` for every include-only item that uses it.
    """
    fields = {}
    for key in allowed_fields:
        if type(item) == "dict":
            if key in item:
                fields[key] = item[key]
        elif hasattr(item, key):
            fields[key] = getattr(item, key)
    return fields


def _local_dep_stem(dep, package_label):
    """Stem of a `dep_fbs` edge that points INSIDE the calling package, else None."""
    if not dep.endswith(".fbs"):
        return None
    if dep.startswith(":"):
        return dep[1:-len(".fbs")]
    if dep.startswith("@"):
        # A REPOSITORY-QUALIFIED dep is not provably in-root. The old
        # `pkg.endswith(package_label)` test said otherwise, and it is wrong
        # across a module boundary: `@other//message/schemas/service/peripheral`
        # ends with `//message/schemas/service/peripheral`, so a FOREIGN
        # module's same-named package was classified as this root's own — which
        # is precisely the shape a self-hosted schema module produces (its
        # package path mirrors aorta's by design, because the canonical slot IS
        # the wire identity). Resolving the repo part correctly would mean
        # parsing canonical repository names, which is not a stable API.
        #
        # This only skips the "an in-root dependency must carry the same raw
        # codegen" convenience check; a genuinely missing target still fails, as
        # an ordinary Bazel missing-label error rather than a nicer message.
        return None
    if dep.startswith("//"):
        pkg, _, name = dep.partition(":")
        if not name:
            return None
        if pkg == package_label:
            return name[:-len(".fbs")]
        return None
    if "/" in dep:
        return None
    return dep[:-len(".fbs")]


# Single source of truth: //message/tools:schema_names.bzl. Kept as a local
# name so the ~20 call sites below read unchanged.
_message_base = _schema_message_base


def _rust_binding_name(item, alias_id):
    """Deterministic Rust target name: `<message base>_rs`."""
    return _get(item, "rust_wrapper_name", _message_base(alias_id) + "_rs")


def _rust_crate_name(item, alias_id):
    """Deterministic Rust crate name: `<message base>`."""
    return _get(item, "rust_wrapper_crate_name", _message_base(alias_id))


_sanitize_crate_name = sanitize_crate_name


_RUST_FACADE_KEYWORDS = depset([
    "as", "break", "const", "continue", "crate", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
    "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct",
    "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    "async", "await", "dyn", "abstract", "become", "box", "do", "final",
    "macro", "override", "priv", "typeof", "unsized", "virtual", "yield",
    "try", "union",
]).to_list()

def _is_rust_facade_module_name(module_name):
    """Legal Rust identifier AND not a Rust keyword (the facade emits
    `pub use ::<crate> as <module>;` — a keyword or hyphenated name would be
    uncompilable)."""
    if not module_name:
        return False
    if not (module_name[0].isalpha() or module_name[0] == "_"):
        return False
    for ch in module_name.elems():
        if not (ch.isalnum() or ch == "_"):
            return False
    return module_name not in _RUST_FACADE_KEYWORDS

def _check_crate_name(what, crate_name, errors):
    if not crate_name:
        errors.append("{}: crate name must not be empty".format(what))
        return
    if crate_name != _sanitize_crate_name(crate_name):
        errors.append(
            ("{}: crate name '{}' must contain only lower-case ASCII letters, digits " +
             "and underscores").format(what, crate_name),
        )
    first = crate_name[0]
    if first >= "0" and first <= "9":
        errors.append(
            "{}: crate name '{}' must not start with a digit".format(what, crate_name),
        )


def _normalize_source_prefix(source_prefix, errors):
    if source_prefix == None:
        errors.append("source_prefix must not be None (use \"\" for none)")
        return ""
    if source_prefix == "":
        return ""
    if source_prefix.startswith("/"):
        errors.append("source_prefix must be relative, got '{}'".format(source_prefix))
        return ""
    if source_prefix.endswith("/"):
        errors.append("source_prefix must not end with '/', got '{}'".format(source_prefix))
        return ""
    for segment in source_prefix.split("/"):
        if segment == "" or segment == "." or segment == "..":
            errors.append(
                "source_prefix must not contain empty, '.' or '..' segments, got '{}'".format(
                    source_prefix,
                ),
            )
            return ""
    return source_prefix


def _validate_package_census(census, source_package, source_prefix, package_label, errors):
    """Reject any tracked `.fbs` in the calling package that is not a source.

    Every canonical `.fbs` of an external root is a GENERATED copy
    output, so a physical `.fbs` sitting in the canonical package is either a
    file somebody forgot to declare or a second owner of a canonical slot —
    and neither is visible to a check built from the DECLARED item set (that
    is the same circularity the registry bijection lint had to break with an
    independent census). `census` is therefore a plain `glob()` over the
    calling package: it is discovered by the filesystem, never derived from
    `items`, the manifest or `<name>_canonical_fbs`.

    The only legitimate physical `.fbs` in the calling package is a SOURCE of
    this same root, which by contract lives under `source_prefix/` — a
    cross-module root has none at all.

    Nested packages: `glob()` stops at a package boundary, so a `.fbs` under a
    package nested INSIDE this one is not in `census`. That case is closed by
    the package macro's `all_source_fbs`, which recurses through each level's
    own `subpackages()` (schema_codegen.bzl) and which the generated lint test
    depends on — a nested package that is not a schema package has no
    `all_source_fbs`, so the lint test fails to build instead of censusing less
    than the tree holds. For a canonical (`message/schemas/**`) root the same
    recursion also carries such a file into `//message:schema_source_files`,
    where `registry_bijection_lint_test` reports it; that claim was VACUOUS
    before the recursion existed, which is what
    `//message/tools:nested_package_census_test` now pins.
    """
    local_sources = source_package == package_label
    for path in census:
        if local_sources and source_prefix and path.startswith(source_prefix + "/"):
            continue
        if local_sources:
            errors.append(
                ("tracked schema file '{}' sits in the canonical package: every " +
                 "canonical .fbs of an external root is a generated copy output, and " +
                 "the only physical .fbs allowed here are this root's sources under " +
                 "source_prefix '{}/'").format(path, source_prefix),
            )
        else:
            errors.append(
                ("tracked schema file '{}' sits in the canonical package, whose " +
                 "sources live in '{}': a shadow package must contain NO tracked " +
                 ".fbs at all").format(path, source_package),
            )


EXTERNAL_ROOT_AGGREGATE_SUFFIXES = [
    "_cc",
    "_py",
    "_py_tree",
    "_rs",
    "_rs_src",
    "_canonical_fbs",
    "_manifest",
    "_lint_test",
    "_workspace_lint",
]

def validate_schema_items(
        name,
        items,
        package_label,
        errors,
        expected_provenance = None,
        root_aggregate_suffixes = EXTERNAL_ROOT_AGGREGATE_SUFFIXES,
        emits_imports = True,
        require_all_languages = True):
    """Validate declarations and reserve exactly the caller's emitted surface.

    Publication and codegen policy are independent validated vocabularies.
    The collapsed package always enforces the language floor. Only the existing
    allowlisted copy-root adapter retains its historical selective-language
    surface; an entry's own provenance never selects validation policy.
    """

    seen_stems = {}
    seen_slots = {}
    seen_targets = {}
    seen_crates = {}
    seen_facades = {}

    def _claim(target, what):
        if target in seen_targets:
            errors.append(
                "target name collision on ':{}' ({} vs {})".format(
                    target,
                    seen_targets[target],
                    what,
                ),
            )
        else:
            seen_targets[target] = what

    def _claim_facade(module_name, what):
        if not _is_rust_facade_module_name(module_name):
            errors.append(
                "{}: facade module name '{}' is not a legal Rust identifier "
                + "(or is a Rust keyword); hyphenated alias_id values need an "
                + "explicit rust_wrapper_facade_name override".format(what, module_name)
            )
        if module_name in seen_facades:
            errors.append(
                "Rust facade module name collision on '{}': {} vs {}".format(
                    module_name,
                    seen_facades[module_name],
                    what,
                )
            )
        else:
            seen_facades[module_name] = what

    def _claim_crate(crate_name, what):
        # Two crates with one name inside a root would only surface
        # as an ambiguous-name rustc error deep in a downstream build, and
        # only for whoever happens to depend on both.
        if crate_name in seen_crates:
            errors.append(
                "Rust crate name collision on '{}' ({} vs {})".format(
                    crate_name,
                    seen_crates[crate_name],
                    what,
                ),
            )
        else:
            seen_crates[crate_name] = what

    for item in items:
        stem = _get(item, "stem")
        root_fbs = _get(item, "root_fbs")
        pkg = _get(item, "pkg")
        provenance = _get(item, "provenance")
        schema_publication(item)
        schema_codegen_policy(item)

        if pkg != package_label:
            errors.append(
                ("item '{}' declares pkg '{}' but the macro expands in '{}'; a " +
                 "cross-package root calls the macro once per package").format(
                    stem,
                    pkg,
                    package_label,
                ),
            )
        if expected_provenance != None and provenance != expected_provenance:
            errors.append(
                "item '{}' declares provenance '{}', expected '{}'".format(
                    stem,
                    provenance,
                    expected_provenance,
                ),
            )
        if expected_provenance == "external" and _get(item, "edu_public", None) != None:
            errors.append(
                ("item '{}' sets edu_public; that flag is the EDU schema pack " +
                 "projection and must not appear in an EXTERNAL-root fragment").format(stem),
            )
        if _get(item, "edu_public", True) != True:
            errors.append(
                ("item '{}' sets edu_public to a non-true value; the flag is " +
                 "omitted unless it is True").format(stem),
            )

        if not root_fbs.endswith(".fbs"):
            errors.append("item '{}' root_fbs '{}' must end with .fbs".format(stem, root_fbs))
        if "/" in root_fbs or root_fbs.startswith(".") or root_fbs in [".", ".."]:
            errors.append(
                "item '{}' root_fbs '{}' must be a bare basename".format(stem, root_fbs),
            )
        if root_fbs != stem + ".fbs":
            errors.append(
                ("item '{}' root_fbs '{}' must be '<stem>.fbs' — the canonical slot, the " +
                 "generated output names and the bfbs identity all derive from the stem").format(
                    stem,
                    root_fbs,
                ),
            )

        if stem in seen_stems:
            errors.append("duplicate stem '{}'".format(stem))
        seen_stems[stem] = True
        if root_fbs in seen_slots:
            errors.append("duplicate canonical slot '{}'".format(root_fbs))
        seen_slots[root_fbs] = True

        for dep in _get(item, "dep_fbs", []):
            if not dep.endswith(".fbs"):
                errors.append("item '{}' dep_fbs '{}' must be a .fbs label".format(stem, dep))
            absolute = dep.startswith("//") or (dep.startswith("@") and "//" in dep)
            package_local = dep.startswith(":") or "/" not in dep
            if not absolute and not package_local:
                errors.append(
                    ("item '{}' dep_fbs '{}' is a filesystem path; only ':x.fbs', " +
                     "'//pkg:x.fbs' and '@repo//pkg:x.fbs' labels are allowed").format(stem, dep),
                )
            elif ".." in dep.split("/"):
                errors.append(
                    "item '{}' dep_fbs '{}' must not contain a '..' segment".format(stem, dep),
                )

        for flag in _REFUSED_FLAGS:
            if _get(item, flag, False):
                errors.append(
                    ("item '{}' enables '{}', which this Aorta refuses for external " +
                     "roots (the surface is not generated yet) — refused rather than " +
                     "silently dropped").format(stem, flag),
                )

        typed = _get(item, _TYPED_MARKER_FLAG, False)
        if typed:
            if require_all_languages:
                missing = [
                    flag
                    for flag in (_TYPED_REQUIRED_FLAGS if emits_imports else _RAW_LANGUAGE_FLAGS)
                    if not _get(item, flag, False)
                ]
                if missing:
                    errors.append(
                        ("item '{}' has a root type ({}=True) but does not enable {}: " +
                         "such an item must be first class in every supported language " +
                         "— the full set is {}").format(
                            stem,
                            _TYPED_MARKER_FLAG,
                            missing,
                            _TYPED_REQUIRED_FLAGS,
                        ),
                    )

                if emits_imports and not schema_meta_py_enabled(item):
                    errors.append("item '{}' is typed but disables schema_meta_py".format(stem))
        else:
            # Include-only item: registered (no implicit exception table),
            # raw codegen optional, everything that presupposes a root
            # type forbidden.
            for flag in _INCLUDE_ONLY_FORBIDDEN_FLAGS:
                if flag != _TYPED_MARKER_FLAG and _get(item, flag, False):
                    errors.append(
                        ("item '{}' has no root type ({}=False) but enables '{}', which " +
                         "only exists for a root type").format(stem, _TYPED_MARKER_FLAG, flag),
                    )
            raw_on = [flag for flag in _RAW_LANGUAGE_FLAGS if _get(item, flag, False)]
            if raw_on and len(raw_on) != len(_RAW_LANGUAGE_FLAGS):
                errors.append(
                    ("item '{}' enables raw {} but not {}: raw codegen for an " +
                     "include-only schema is per-language symmetric").format(
                        stem,
                        raw_on,
                        [f for f in _RAW_LANGUAGE_FLAGS if f not in raw_on],
                    ),
                )

        if _get(item, "action_schema", False) and not _get(item, "action_name", ""):
            errors.append(
                ("item '{}' sets action_schema=True without action_name: an external " +
                 "root has no in-repo 'schemas/action/<pkg>' path to derive the routing " +
                 "name from, so it must be explicit").format(stem),
            )
        if _get(item, "action_name", "") and not _get(item, "action_schema", False):
            errors.append(
                "item '{}' sets action_name without action_schema=True".format(stem),
            )

        # Target-name uniqueness across EVERY label this root will define,
        # before Bazel would report it as a confusing duplicate-target error.
        alias_id = _get(item, "alias_id", stem)
        _claim(root_fbs, "canonical import output of '{}'".format(stem))
        if emits_imports:
            _claim(stem + "_import", "import rule of '{}'".format(stem))
        _claim(stem + "_fbs_closure", "closure of '{}'".format(stem))
        _claim(stem + "_rust_closure", "Rust module closure of '{}'".format(stem))
        if _get(item, "cpp", False):
            for suffix in ["_cpp", "_cpp_all"]:
                _claim(stem + suffix, "codegen output of '{}'".format(stem))
        if _get(item, "cpp_lib", _get(item, "cpp", False)):
            _claim(
                _get(item, "cpp_lib_name", stem + "_cpp_lib"),
                "C++ schema library of '{}'".format(stem),
            )
            # `cc_alias = None` DISABLES the alias (schema_codegen.bzl emits it
            # only when truthy), so there is no target name to claim — and
            # claiming `None` would make two such items in one root collide on
            # a target that neither of them defines.
            cc_alias = _get(item, "cc_alias", stem + "_cc")
            if cc_alias:
                _claim(cc_alias, "C++ schema alias of '{}'".format(stem))
        if _get(item, "python", False):
            for suffix in ["_py", "_py_lib"]:
                _claim(stem + suffix, "codegen output of '{}'".format(stem))
        if _get(item, "python", False) and _get(item, "bfbs", False):
            _claim(stem + "_fastpath", "codegen output of '{}'".format(stem))
        if _get(item, "rust", False):
            for suffix in ["_rust", "_rust_all"]:
                _claim(stem + suffix, "codegen output of '{}'".format(stem))
        if typed:
            for flag, suffix in [("bfbs", "_bfbs"), ("schema_meta_rust", "_schema_meta"), ("schema_meta_cpp", "_schema_meta_cpp")]:
                if _get(item, flag, False):
                    _claim(stem + suffix, "codegen output of '{}'".format(stem))
            # The Python schema-meta pair is emitted on `schema_meta_py_enabled`
            # (schema_codegen.bzl), NOT on `typed`. Claiming the names
            # unconditionally would report a collision against two targets this
            # root never defines — the name would be free for the other item.
            if schema_meta_py_enabled(item):
                for suffix in ["_schema_meta_py", "_schema_meta_py_lib"]:
                    _claim(stem + suffix, "codegen output of '{}'".format(stem))
        # The typed wrappers follow their OWN flags rather than `typed`. For an
        # external root that is a distinction without a difference (both flags
        # are required above); for an internal root it is what lets a registry
        # entry that never had a C++ or Rust binding keep not having one,
        # instead of the move inventing a target the registry does not declare.
        if typed and _get(item, "cpp_wrapper", False):
            _claim(
                _get(item, "cpp_wrapper_name", _message_base(alias_id) + "_cc"),
                "typed C++ binding of '{}'".format(stem),
            )
        if typed and _get(item, "rust_wrapper", False):
            rust_name = _rust_binding_name(item, alias_id)
            _claim(rust_name, "typed Rust binding of '{}'".format(stem))
            # The two helper targets `aorta_rust_message_binding` derives from
            # the binding name; claimed here so a collision is reported against
            # its real owner rather than as a duplicate-target error from
            # inside the shared engine.
            _claim(rust_name + "_wrapper_src", "Rust wrapper source of '{}'".format(stem))
            _claim(
                rust_name + "__crate_local_modules",
                "crate-local Rust modules of '{}'".format(stem),
            )
            crate_name = _rust_crate_name(item, alias_id)
            facade_name = _get(item, "rust_wrapper_facade_name", _message_base(alias_id))
            _check_crate_name("item '{}'".format(stem), crate_name, errors)
            _claim_crate(crate_name, "typed Rust binding of '{}'".format(stem))
            _claim_facade(facade_name, "Rust facade module of '{}'".format(stem))
        if _get(item, "action_schema", False):
            _claim(stem + "_action_schema", "action aggregate of '{}'".format(stem))

    # A dependency INSIDE this root must carry the raw codegen its dependents
    # need: flatc's generated header/module for a schema `#include`s the
    # generated header/module of every schema it includes, so an include-only
    # item that a C++/Python item depends on needs `cpp`/`python` too. Without
    # this check the failure surfaces as an unexplained "target
    # ':<dep>_cpp_lib' does not exist" from deep inside the shared engine.
    item_by_stem = {_get(item, "stem"): item for item in items}
    for item in items:
        stem = _get(item, "stem")
        for dep in _get(item, "dep_fbs", []):
            dep_stem = _local_dep_stem(dep, package_label)
            if dep_stem == None or dep_stem not in item_by_stem:
                continue
            dep_item = item_by_stem[dep_stem]
            for language in _RAW_LANGUAGE_FLAGS:
                if _get(item, language, False) and not _get(dep_item, language, False):
                    errors.append(
                        ("item '{}' enables '{}' but its in-root dependency '{}' does " +
                         "not: an included schema needs the same raw codegen as the " +
                         "schema that includes it").format(stem, language, dep_stem),
                    )

    for suffix in root_aggregate_suffixes:
        _claim(name + suffix, "root aggregate '{}{}'".format(name, suffix))
    for reserved in ["all_fbs", "all_source_fbs", "all_cpp", "all_python", "all_bfbs",
                     "all_rust", "all_schema_meta_rust", "all_schema_meta_cpp",
                     "all_schema_meta_py", "examples_bfbs"]:
        _claim(reserved, "package aggregate")
    _check_crate_name("root '{}'".format(name), _sanitize_crate_name(name), errors)
    _claim_crate(_sanitize_crate_name(name), "root Rust facade '{}_rs'".format(name))
    return errors


def _validate_root(
        name,
        items,
        fragment,
        fragment_symbol,
        source_package,
        source_inventory,
        source_prefix,
        expected_provenance,
        package_label,
        package_census):
    """Analysis-time gate. Returns (errors, normalized_source_prefix).

    Everything provable from Starlark values lands here; everything that needs
    FILE CONTENT (fragment AST, include text, physical inventory, hashes) is
    the generated `:<name>_lint_test` / `:<name>_workspace_lint`'s job.
    Both layers are mandatory — this one alone is NOT sufficient evidence.
    """
    errors = []

    # Evidence inputs: mandatory and non-empty. They
    # are keyword-only with NO defaults in the public signature, so omitting
    # one is already a load error; these checks close the "passed but empty"
    # hole (`fragment = None`, `source_inventory = []`, ...).
    if not name:
        errors.append("name must be a non-empty string")
    if not fragment:
        errors.append("fragment (the pure-data .bzl label) is required")
    if not fragment_symbol:
        errors.append("fragment_symbol (the list name inside `fragment`) is required")
    if not source_package:
        errors.append("source_package is required")
    if source_inventory == None or len(source_inventory) == 0:
        errors.append(
            "source_inventory is required and must cover EVERY physical .fbs under " +
            "source_package/source_prefix (a glob for a same-package root, the " +
            "source package's :all_fbs filegroup for a cross-module root)",
        )
    if not items:
        errors.append("items must be a non-empty list of fragment entries")

    prefix = _normalize_source_prefix(source_prefix, errors)
    _validate_package_census(package_census, source_package, prefix, package_label, errors)

    if expected_provenance not in _ALLOWED_PROVENANCE:
        errors.append(
            "expected_provenance must be one of {}, got '{}'".format(
                _ALLOWED_PROVENANCE,
                expected_provenance,
            ),
        )

    if not source_package.startswith("//") and not source_package.startswith("@"):
        errors.append(
            "source_package must be an absolute label ('//pkg' or '@repo//pkg'), got '{}'".format(
                source_package,
            ),
        )

    # Forgery gate: only Aorta itself can declare a root to be
    # consumer-(a) class ("internal", destined for the private artifact). The
    # allowlist lives in THIS module, so an external repository cannot add itself
    # to it, and the canonical-tree wiring reads the very same list.
    if expected_provenance == "internal":
        entry = internal_external_root_entry(package_label, name)
        if entry == None:
            errors.append(
                ("expected_provenance='internal' is reserved for roots declared in " +
                 "@aorta//message:schemas/external_roots.bzl; '{}:{}' is not declared " +
                 "there. An external root outside Aorta's own release ownership must " +
                 "use expected_provenance='external'.").format(package_label, name),
            )
        elif entry.source_package != source_package:
            errors.append(
                ("internal root '{}:{}' declares source_package '{}' but the Aorta " +
                 "allowlist pins '{}'").format(
                    package_label,
                    name,
                    source_package,
                    entry.source_package,
                ),
            )

    validate_schema_items(
        name = name,
        items = items,
        package_label = package_label,
        errors = errors,
        expected_provenance = expected_provenance,
        require_all_languages = expected_provenance == "external",
    )

    return errors, prefix


def _import_impl(ctx):
    out = ctx.outputs.canonical
    src = ctx.file.src
    ctx.actions.run_shell(
        inputs = [src],
        outputs = [out],
        command = 'set -eu; mkdir -p "$(dirname "$2")"; cp -f -- "$1" "$2"',
        arguments = [src.path, out.path],
        mnemonic = "AortaImportExternalFbs",
        progress_message = "Materializing canonical schema %s" % out.short_path,
    )
    return [DefaultInfo(files = depset([out]))]


aorta_import_external_fbs = rule(
    implementation = _import_impl,
    doc = """Byte-for-byte copy of one external source `.fbs` into its canonical slot.

PRIVATE implementation detail of `aorta_external_schema_package` — it is not a
second public entry point, and a caller must never split "import" and "codegen"
into two hand-written steps: the whole point is that the canonical copy
output, and nothing else, feeds flatc/bfbs/wrappers (invariant E2), so the
source repository name, the sandbox path and the checkout path can never reach
the wire identity.""",
    attrs = {
        "src": attr.label(
            mandatory = True,
            allow_single_file = [".fbs"],
            doc = "The single source `.fbs` under `source_package/source_prefix`.",
        ),
        "canonical": attr.output(
            mandatory = True,
            doc = "The canonical `<stem>.fbs` output in the CALLING package.",
        ),
    },
)


def _rust_facade_source_impl(ctx):
    ctx.actions.write(output = ctx.outputs.out, content = ctx.attr.content)
    return [DefaultInfo(files = depset([ctx.outputs.out]))]


aorta_external_root_rust_facade_source = rule(
    implementation = _rust_facade_source_impl,
    doc = """`lib.rs` of the root's Rust facade crate `:<name>_rs`.

PRIVATE implementation detail of `aorta_external_schema_package`. The facade
re-exports each per-schema crate under its stable MESSAGE-BASE module name; it
deliberately does NOT flatten every symbol into one namespace, so two schemas
of one root can never shadow each other's types.""",
    attrs = {
        "content": attr.string(mandatory = True),
        "out": attr.output(mandatory = True),
    },
)


def _manifest_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    declarations = ctx.actions.declare_file(ctx.label.name + ".declarations.json")
    ctx.actions.write(output = declarations, content = ctx.attr.declarations_json)
    args = ctx.actions.args()
    args.add("--out", out)
    args.add("--declarations", declarations)
    args.add("--fragment", ctx.file.fragment)
    args.add_all(ctx.files.source_inventory, before_each = "--source")
    args.add_all(ctx.files.canonical_outputs, before_each = "--canonical-output")
    ctx.actions.run(
        outputs = [out],
        inputs = ctx.files.source_inventory + ctx.files.canonical_outputs + [
            declarations,
            ctx.file.fragment,
        ],
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        arguments = [args],
        mnemonic = "AortaExternalRootManifest",
        progress_message = "Writing external-root manifest for %s" % str(ctx.label),
    )
    return [DefaultInfo(files = depset([out]))]


aorta_external_root_manifest = rule(
    implementation = _manifest_impl,
    doc = """Machine interface between an external root and its lints.

`external_root_manifest_version = 1` — a SEPARATE document
from Aorta's registry manifest v3, because a user's schema is not Aorta release
content and must never gain Aorta provenance by being written down next to it.
The lints re-derive every fact (fragment AST, inventory, hashes) rather than
trusting the values recorded here.""",
    attrs = {
        "declarations_json": attr.string(
            mandatory = True,
            doc = "Analysis-time facts (labels, flags, target names) as JSON.",
        ),
        "fragment": attr.label(
            mandatory = True,
            allow_single_file = [".bzl"],
            doc = "The pure-data fragment file, hashed and re-parsed by the lint.",
        ),
        "source_inventory": attr.label_list(
            mandatory = True,
            allow_files = [".fbs"],
            doc = "Every physical `.fbs` under `source_package/source_prefix`.",
        ),
        "canonical_outputs": attr.label_list(
            mandatory = True,
            allow_files = [".fbs"],
            doc = "The canonical copy outputs in the calling package.",
        ),
        "_tool": attr.label(
            default = _MANIFEST_TOOL,
            executable = True,
            cfg = "exec",
        ),
    },
)


def aorta_external_schema_package(
        name,
        items,
        fragment,
        fragment_symbol,
        source_package,
        source_inventory,
        source_prefix = "",
        expected_provenance = "external",
        visibility = None):
    """Declare an external schema root and generate its first-class bindings.

    Args:
      name: root name; also the prefix of every package-level aggregate
        (`:<name>_cc`, `:<name>_py`, `:<name>_py_tree`, `:<name>_rs`,
        `:<name>_manifest`, `:<name>_lint_test`, ...) and the crate name of
        the Rust facade. These labels — and the facade's module layout — are
        stable public API from the first published version.
      items: the fragment entries belonging to THIS package. Every entry's
        `pkg` must equal the calling package; a multi-package root calls the
        macro once per package and may share one fragment file.
      fragment: label of the pure-data `.bzl` file the entries come from.
        Mandatory evidence input — the lint re-parses the ORIGINAL text
        instead of trusting the already-evaluated structs.
      fragment_symbol: the list name inside `fragment`.
      source_package: package holding the source `.fbs` files, e.g.
        `@product_schemas//message/schemas/topic/example` or a user's own
        `//schemas`.
      source_inventory: label(s) covering EVERY physical `.fbs` under
        `source_package/source_prefix` — `glob(["src/**/*.fbs"])` for a
        same-package root, the source package's `:all_fbs` filegroup for a
        cross-module one.
      source_prefix: pure directory prefix inside `source_package` (EDU
        convention: `src`, so `src/my_msg.fbs` and the canonical output
        `my_msg.fbs` never collide).
      expected_provenance: `external` (default) or `internal`. `internal` is
        reserved for roots declared in Aorta's own allowlist.
      visibility: visibility for the public aggregates.

    There is deliberately no `languages` argument: the per-item flags
    are the single truth, and the lint forces an item with a root type to be
    first class in every supported language — C++, Python and Rust.
    """
    package_label = "//" + native.package_name()

    # An INDEPENDENT physical census of the calling package. `glob()`
    # asks the filesystem, so a `.fbs` nobody declared is still found — a check
    # derived from `items` / `source_inventory` / `<name>_canonical_fbs` can
    # only ever confirm what the author already wrote down.
    package_census = native.glob(["**/*.fbs"], allow_empty = True)

    errors, prefix = _validate_root(
        name = name,
        items = items,
        fragment = fragment,
        fragment_symbol = fragment_symbol,
        source_package = source_package,
        source_inventory = source_inventory,
        source_prefix = source_prefix,
        expected_provenance = expected_provenance,
        package_label = package_label,
        package_census = package_census,
    )
    if errors:
        fail(
            "aorta_external_schema_package('{}') in {}:\n  - {}".format(
                name,
                package_label,
                "\n  - ".join(errors),
            ),
        )

    source_dir = prefix + "/" if prefix else ""
    allowed_item_fields = _allowed_item_fields(expected_provenance)

    canonical_outputs = []
    closure_srcs_by_stem = {}
    include_dir_groups_by_stem = {}
    declarations = []
    py_trees = []
    py_metas = []
    cc_targets = []
    rs_facade_entries = []

    for item in items:
        stem = _get(item, "stem")
        root_fbs = _get(item, "root_fbs")
        dep_fbs = _get(item, "dep_fbs", [])
        alias_id = _get(item, "alias_id", stem)
        source_label = "{}:{}{}".format(source_package, source_dir, root_fbs)

        aorta_import_external_fbs(
            name = stem + "_import",
            src = source_label,
            canonical = root_fbs,
            visibility = visibility,
        )
        canonical_outputs.append(root_fbs)

        # Cross-module-correct hermetic closure: each DIRECT dep contributes
        # its own `_fbs_closure` filegroup (which already carries ITS
        # grandchildren). The registry graph walk cannot be used here — it
        # only knows Aorta's own `SCHEMA_DEFINITIONS`.
        # The DIRECT edges stay in the list because they also drive flatc's
        # `-I` computation through `$(location <dep>)`, which needs each direct
        # `.fbs` to be a declared input in its own right; the closure
        # filegroups add the grandchildren on top.
        closure_targets = [dep_fbs_closure_target(dep) for dep in dep_fbs]
        closure_srcs_by_stem[stem] = dep_fbs + closure_targets
        include_dir_groups_by_stem[stem] = closure_targets

        typed = _get(item, _TYPED_MARKER_FLAG, False)
        has_cpp = _get(item, "cpp", False)
        has_python = _get(item, "python", False)
        has_rust = _get(item, "rust", False)
        targets = {}
        if has_rust:
            targets["rust"] = "{}:{}_rust".format(package_label, stem)
            targets["rust_closure"] = "{}:{}_rust_closure".format(package_label, stem)
        if has_cpp:
            targets["cpp"] = "{}:{}_cpp".format(package_label, stem)
            targets["cpp_lib"] = "{}:{}".format(
                package_label,
                _get(item, "cpp_lib_name", stem + "_cpp_lib"),
            )
            cc_alias = _get(item, "cc_alias", stem + "_cc")
            if cc_alias:
                targets["cc"] = "{}:{}".format(package_label, cc_alias)
        if has_python:
            targets["py"] = "{}:{}_py".format(package_label, stem)
            targets["py_lib"] = "{}:{}_py_lib".format(package_label, stem)
            py_trees.append(":{}_py".format(stem))
        if typed:
            typed_cc = _get(item, "cpp_wrapper_name", _message_base(alias_id) + "_cc")
            typed_rs = _rust_binding_name(item, alias_id)
            crate_name = _rust_crate_name(item, alias_id)
            facade_name = _get(item, "rust_wrapper_facade_name", _message_base(alias_id))
            targets["bfbs"] = "{}:{}_bfbs".format(package_label, stem)

            # Every schema-meta target follows ITS OWN flag, exactly as
            # `aorta_define_schema_targets` emits it (schema_codegen.bzl):
            # `<stem>_schema_meta` on `schema_meta_rust`, `<stem>_schema_meta_cpp`
            # on `schema_meta_cpp`, and the Python pair on
            # `schema_meta_py_enabled` — never on `typed`. An INTERNAL root is
            # graded by Aorta's registry policy rather than the every-language
            # floor (see the SCOPE note above), so a C++-only typed entry is
            # legal there; declaring a meta target the codegen never emitted
            # would publish a phantom label in the external-root manifest, and
            # for the Python pair it would additionally fail ANALYSIS of the
            # root's `_py_tree`, which really does depend on it.
            #
            # `schema_meta_py_enabled` is the single source of truth the
            # codegen macro reads for the Python pair. It does NOT itself
            # refuse `schema_meta_py` without `python` — that is
            # `aorta_define_schema_targets`' own check
            # (schema_codegen.bzl:229-235).
            if _get(item, "schema_meta_rust", False):
                targets["schema_meta_rust"] = "{}:{}_schema_meta".format(package_label, stem)
            if _get(item, "schema_meta_cpp", False):
                targets["schema_meta_cpp"] = "{}:{}_schema_meta_cpp".format(
                    package_label,
                    stem,
                )
            if schema_meta_py_enabled(item):
                targets["schema_meta_py"] = "{}:{}_schema_meta_py".format(package_label, stem)
                targets["schema_meta_py_lib"] = "{}:{}_schema_meta_py_lib".format(
                    package_label,
                    stem,
                )
                py_metas.append(":{}_schema_meta_py".format(stem))
            if _get(item, "cpp_wrapper", False):
                targets["typed_cc"] = "{}:{}".format(package_label, typed_cc)
                cc_targets.append(":" + typed_cc)
            if _get(item, "rust_wrapper", False):
                targets["typed_rs"] = "{}:{}".format(package_label, typed_rs)
                targets["crate_name"] = crate_name
                rs_facade_entries.append((facade_name, crate_name, typed_rs))
        elif has_cpp:
            # Raw-only (include-only) item: still export its generated header
            # through the root aggregate, so `:<name>_cc` is the complete C++
            # surface of the root even when nothing typed includes it yet.
            cc_targets.append(":" + _get(item, "cpp_lib_name", stem + "_cpp_lib"))
        declaration = {
            "stem": stem,
            "fields": _item_fields(item, allowed_item_fields),
            "source_label": source_label,
            "canonical_output_label": "{}:{}".format(package_label, root_fbs),
            "closure_targets": closure_targets,
            "targets": targets,
        }
        declarations.append(declaration)

    aorta_define_schema_package(
        items,
        export_files = False,
        closure_srcs_by_stem = closure_srcs_by_stem,
        # The closure also supplies flatc's `-I` roots: an external root whose
        # only direct edge is a GENERATED sibling would otherwise derive its
        # include root from `bazel-out/...`, where a grandchild's
        # `schemas/...`-rooted include does not exist.
        include_dir_groups_by_stem = include_dir_groups_by_stem,
    )

    for item in items:
        if not _get(item, _TYPED_MARKER_FLAG, False):
            continue
        stem = _get(item, "stem")
        alias_id = _get(item, "alias_id", stem)
        root_fbs_label = ":" + _get(item, "root_fbs")
        if _get(item, "action_schema", False):
            root_fbs_label = ":{}_action_schema".format(stem)
        if _get(item, "cpp_wrapper", False):
            aorta_cpp_message_wrapper(
                name = _get(item, "cpp_wrapper_name", _message_base(alias_id) + "_cc"),
                schema_pkg = package_label,
                schema_stem = stem,
                schema_cc_target = ":" + _get(item, "cpp_lib_name", stem + "_cpp_lib"),
                wrapper_header = _message_base(alias_id) + ".h",
                root_fbs_label = root_fbs_label,
                # `<stem>_fbs_closure` is the root + the full transitive
                # closure; it is BOTH the declared action input set and the
                # `--include-fbs` resolution manifest, which is the only
                # spelling that works when an include lives in another Bzlmod
                # module's sandboxed tree.
                schema_dep_filegroups = [":{}_fbs_closure".format(stem)],
                schema_rel = "",
                action_name = _get(item, "action_name", None),
                visibility = visibility,
            )

        if not _get(item, "rust_wrapper", False):
            continue

        # The SAME BFBS-driven generator, same `--root-bfbs`, same
        # `--include-fbs` manifest — only `--lang` differs. The
        # heuristic Rust wrapper in aorta_flatbuffers.bzl is not on this path
        # at all.
        aorta_rust_message_binding(
            name = _rust_binding_name(item, alias_id),
            crate_name = _rust_crate_name(item, alias_id),
            schema_pkg = package_label,
            schema_stem = stem,
            # An EXTERNAL root is required to carry `schema_meta_rust`, so the
            # crate always embeds the Rust `SchemaMeta` module. An INTERNAL
            # root follows its registry entry — mirroring
            # `_rust_binding_include_schema_meta` in schemas/registry.bzl, so
            # the shadow package's crate and the historical
            # `//message/bindings/rust` crate stay the same crate.
            include_schema_meta = _get(item, "schema_meta_rust", False),
            root_fbs_label = root_fbs_label,
            # `<stem>_rust_closure` is this schema's own generated module plus
            # every module in its transitive include closure, computed through
            # Bazel's dependency graph — the only spelling that reaches into
            # another Bzlmod module. The wrapper emits
            # one `#[path]` per include, so a missing member is a hard rustc
            # error, never a silently-degraded crate.
            rust_module_filegroups = [":{}_rust_closure".format(stem)],
            schema_dep_filegroups = [":{}_fbs_closure".format(stem)],
            schema_rel = "",
            action_name = _get(item, "action_name", None),
            visibility = visibility,
        )

    # Root Rust facade: per-schema crates re-exported under their
    # stable message-base module names. NOT a flattened namespace — two
    # schemas of one root must never be able to shadow each other's types.
    facade_lines = [
        "//! Rust facade for the external schema root `{}` (generated).".format(name),
        "//!",
        "//! Each schema of the root is re-exported as a submodule named after its",
        "//! message base, so `<root>::<message base>::schema::<Type>Schema` is the",
        "//! stable path.",
        "",
    ]
    for module_name, crate_name, _target in rs_facade_entries:
        if module_name == crate_name:
            facade_lines.append("pub use ::{};".format(crate_name))
        else:
            facade_lines.append("pub use ::{} as {};".format(crate_name, module_name))
    aorta_external_root_rust_facade_source(
        name = name + "_rs_src",
        out = name + "_rs.rs",
        content = "\n".join(facade_lines) + "\n",
    )
    rust_library(
        name = name + "_rs",
        srcs = [":" + name + "_rs_src"],
        crate_root = ":" + name + "_rs_src",
        crate_name = _sanitize_crate_name(name),
        edition = "2021",
        deps = [":" + target for _module, _crate, target in rs_facade_entries],
        visibility = visibility,
    )

    native.filegroup(
        name = name + "_canonical_fbs",
        srcs = canonical_outputs,
        visibility = visibility,
    )

    cc_library(
        name = name + "_cc",
        deps = cc_targets + [_AORTA_CPP_HEADERS],
        visibility = visibility,
    )

    # Python is TWO layers on purpose: a merged importable tree
    # plus a `py_library` that actually carries `PyInfo`. A tree rule returns
    # only `DefaultInfo`, so it cannot be a `py_test` dep — "the tree built"
    # is not the same claim as "a consumer can import the message class".
    python_msgs_tree(
        name = name + "_py_tree",
        trees = py_trees,
        metas = py_metas,
        visibility = visibility,
    )

    py_library(
        name = name + "_py",
        data = [":" + name + "_py_tree"],
        imports = [name + "_py_tree"],
        deps = [_FLATBUFFERS_PY_DEP],
        visibility = visibility,
    )

    declarations_json = json.encode_indent({
        "external_root_manifest_version": EXTERNAL_ROOT_MANIFEST_VERSION,
        "name": name,
        "package": package_label,
        "source_package": source_package,
        "source_prefix": prefix,
        "fragment_label": str(fragment),
        "fragment_symbol": fragment_symbol,
        "expected_provenance": expected_provenance,
        "allowed_item_fields": allowed_item_fields,
        "required_item_fields": EXTERNAL_ITEM_REQUIRED_FIELDS,
        "typed_marker_flag": _TYPED_MARKER_FLAG,
        "typed_required_flags": _TYPED_REQUIRED_FLAGS,
        "include_only_forbidden_flags": _INCLUDE_ONLY_FORBIDDEN_FLAGS,
        "raw_language_flags": _RAW_LANGUAGE_FLAGS,
        "refused_flags": _REFUSED_FLAGS,
        "items": declarations,
        "package_targets": {
            "cc": "{}:{}_cc".format(package_label, name),
            "py": "{}:{}_py".format(package_label, name),
            "py_tree": "{}:{}_py_tree".format(package_label, name),
            "rs": "{}:{}_rs".format(package_label, name),
            "rs_crate_name": _sanitize_crate_name(name),
            "canonical_fbs": "{}:{}_canonical_fbs".format(package_label, name),
            "manifest": "{}:{}_manifest".format(package_label, name),
        },
    })

    aorta_external_root_manifest(
        name = name + "_manifest",
        declarations_json = declarations_json,
        fragment = fragment,
        source_inventory = source_inventory,
        canonical_outputs = [":" + name + "_canonical_fbs"],
        visibility = visibility,
    )

    lint_args = [
        "--manifest",
        "$(location :{}_manifest)".format(name),
        "--fragment",
        "$(location {})".format(fragment),
        "--registry-manifest",
        "$(location {})".format(_REGISTRY_MANIFEST),
        "--internal-root-allowlist",
        "$(location {})".format(_EXTERNAL_ROOTS_ALLOWLIST),
    ]
    for source in source_inventory:
        lint_args.append("--source")
        lint_args.append("$(locations {})".format(source))
    lint_args.append("--canonical-output")
    lint_args.append("$(locations :{}_canonical_fbs)".format(name))
    for census_file in package_census:
        lint_args.append("--package-census-fbs")
        lint_args.append("$(location {})".format(census_file))

    py_test(
        name = name + "_lint_test",
        srcs = [_LINT_MAIN],
        main = _LINT_MAIN,
        deps = [_LINT_LIB],
        args = lint_args,
        data = [
            ":" + name + "_manifest",
            ":" + name + "_canonical_fbs",
            fragment,
            _REGISTRY_MANIFEST,
            _EXTERNAL_ROOTS_ALLOWLIST,
            # The package macro's own census, which RECURSES into nested
            # packages (schema_codegen.bzl). The `glob()` above stops at this
            # package's boundary, so this dependency is what makes a package
            # nested INSIDE the canonical package fail closed: if it is not a
            # schema package it has no `all_source_fbs`, and this test then
            # fails to build rather than censusing less than the tree holds.
            # (Its files are runfiles, not lint arguments: `$(locations ...)`
            # rejects the empty filegroup an external root normally has.)
            ":all_source_fbs",
        ] + source_inventory + [
            # A same-package root globs its sources out of the very same tree,
            # so the two input classes overlap; Bazel rejects a duplicated
            # label in `data`.
            census_file
            for census_file in package_census
            if census_file not in source_inventory
        ],
    )

    # The workspace lint only exists when the sources live in the CALLER's own
    # workspace: it re-discovers them with `lstat` from
    # `BUILD_WORKSPACE_DIRECTORY`, which is exactly what a Bazel runfiles tree
    # (itself a forest of symlinks) can never tell you. A cross-module root's
    # physical/symlink census belongs to the source repository's own lint.
    if source_package == package_label:
        py_binary(
            name = name + "_workspace_lint",
            srcs = [_WORKSPACE_LINT_MAIN],
            main = _WORKSPACE_LINT_MAIN,
            deps = [_LINT_LIB],
            args = [
                "--manifest",
                "$(location :{}_manifest)".format(name),
            ],
            data = [":" + name + "_manifest"],
        )
