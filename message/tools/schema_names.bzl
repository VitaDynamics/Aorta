"""Pure name/row derivations shared by every schema-registry consumer.

This leaf loads only the pure-data API field contract.
`message/schemas/registry.bzl` can `load()` a separate product-schema
module, so anything that reaches the derivations THROUGH it inherits that
edge. The L2 fragment provider and the L3 composer
must be able to derive the same alias / wrapper / crate /
manifest-row values WITHOUT dragging a product-schema module into the load
graph, which is exactly the coupling the decoupling design exists to remove.

Every function here was previously duplicated in two or three places:

| derivation | previous homes |
|---|---|
| message base (`<alias_id>_msg`) | `registry.bzl::_wrapper_msg_base_name`, `external_schema_root.bzl::_message_base` |
| Rust binding name / crate name | `registry.bzl::_rust_binding_*`, `external_schema_root.bzl::_rust_*` |
| C++ binding name | `registry.bzl::_cpp_binding_name`, `external_schema_root.bzl` (inline) |
| v3 manifest row | `registry_manifest.bzl::_schema_item_manifest` |

They are now defined once, and the old sites load them from here, so the v3
golden test is a direct proof that consolidating changed nothing.

Item shape: every function accepts EITHER a `struct(...)` registry entry or a
plain `dict`, and — critically — distinguishes "field absent" from "field
present and None". `cc_alias = None` is a real registry spelling meaning "this
schema gets no `<stem>_cc` alias"; a value-based `getattr(item, "cc_alias",
default)` would silently substitute the default for it.
"""

load("//message/tools:schema_contract.bzl", "SCHEMA_FIELDS")

_MISSING = "__AORTA_SCHEMA_NAME_MISSING__"

def item_has(item, key):
    """Whether `item` DECLARES `key` (present-with-None counts as declared)."""
    if type(item) == "dict":
        return key in item
    return hasattr(item, key)

def item_get(item, key, default = _MISSING):
    """`item[key]` / `item.key`, or `default`; fails if absent and no default."""
    if type(item) == "dict":
        if key in item:
            return item[key]
    elif hasattr(item, key):
        return getattr(item, key)
    if default != _MISSING:
        return default
    fail("schema item is missing required field '{}'".format(key))

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def schema_alias_id(item):
    """The registry alias id — `alias_id` if declared, else the stem."""
    return item_get(item, "alias_id", item_get(item, "stem"))

def schema_message_base(alias_id):
    """`<alias_id>_msg`, idempotent when the alias already ends in `_msg`."""
    if alias_id.endswith("_msg"):
        return alias_id
    return alias_id + "_msg"

def schema_meta_py_enabled(item):
    """Whether an item emits the Python schema-meta module (`<stem>_schema_meta_py`).

    Single source of truth shared by the codegen macro (which emits the
    target), the `aorta_msgs` wheel package list (which references only
    packages that produce at least one) and the L2 fragment provider (which
    must derive the Python install slot without loading the registry).
    Defaults ON for any Python schema that also has a bfbs — both are cheap
    (bfbs is already built for schema_meta_rust/cpp) and typed
    publish/service/action needs the module. An explicit `schema_meta_py`
    wins.
    """
    # "Declared and not None wins." The two pre-consolidation copies differed
    # on `schema_meta_py = None` (the struct branch fell through to the
    # default, the dict branch returned None); no fragment in either repo
    # spells it, and this keeps the struct branch's exact behaviour.
    if item_get(item, "schema_meta_py", None) != None:
        return item_get(item, "schema_meta_py")
    return item_get(item, "python", False) and item_get(item, "bfbs", False)


# ---------------------------------------------------------------------------
# Derived target / crate names
# ---------------------------------------------------------------------------

def schema_cpp_lib_name(item):
    return item_get(item, "cpp_lib_name", item_get(item, "stem") + "_cpp_lib")

def schema_cc_alias(item):
    """The `<stem>_cc` schema alias, or None when the entry disables it."""
    if item_has(item, "cc_alias"):
        return item_get(item, "cc_alias")
    return item_get(item, "stem") + "_cc"

def schema_rust_binding_name(item):
    return item_get(
        item,
        "rust_wrapper_name",
        schema_message_base(schema_alias_id(item)) + "_rs",
    )

def schema_rust_crate_name(item):
    return item_get(
        item,
        "rust_wrapper_crate_name",
        schema_message_base(schema_alias_id(item)),
    )

def schema_rust_facade_name(item):
    return item_get(
        item,
        "rust_wrapper_facade_name",
        schema_message_base(schema_alias_id(item)),
    )

def schema_cpp_binding_name(item):
    return item_get(
        item,
        "cpp_wrapper_name",
        schema_message_base(schema_alias_id(item)) + "_cc",
    )

def schema_cpp_binding_header(item):
    return item_get(
        item,
        "cpp_wrapper_header",
        schema_message_base(schema_alias_id(item)) + ".h",
    )

_CRATE_NAME_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_"

def sanitize_crate_name(text):
    """Lower-case ASCII / digits / `_`; everything else becomes `_`."""
    out = []
    for i in range(len(text)):
        ch = text[i].lower()
        if ch in _CRATE_NAME_ALLOWED_CHARS:
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)

# ---------------------------------------------------------------------------
# Logical FlatBuffers include identity
# ---------------------------------------------------------------------------

_SCHEMAS_MARKER = "/schemas/"

def schema_logical_include_path(pkg, root_fbs):
    """Repository-neutral include key below the FIRST schemas path component.

    Paths outside a schemas tree retain their repository-relative spelling.
    BFBS staging preserves source slots; its -I roots, C++/Rust flatc, and
    Python flatc all expose this same first-component include spelling.
    """
    rel = pkg.split("//", 1)[-1] + "/" + root_fbs
    rooted = "/" + rel
    idx = rooted.find(_SCHEMAS_MARKER)
    if idx < 0:
        return rel
    return rooted[idx + 1:]

# These four existing owners shared two ambiguous wheel-root names.
# Qualify their installed basenames; all other existing imports stay stable.
_QUALIFIED_SCHEMA_META_PATHS = [
    "schemas/service/fsm/function_input.fbs",
    "schemas/service/rcp/function_input.fbs",
    "schemas/topic/rcp/trace_event.fbs",
    "schemas/topic/user_interaction/trace_event.fbs",
]

def schema_meta_py_module(item):
    """Installed metadata basename, shared by composition and tree assembly."""
    stem = item_get(item, "stem")
    logical = schema_logical_include_path(item_get(item, "pkg"), stem + ".fbs")
    if logical in _QUALIFIED_SCHEMA_META_PATHS:
        return logical[len("schemas/"):-len(".fbs")].replace("/", "_") + "_schema_meta.py"
    return stem + "_schema_meta.py"

def schema_publication(item):
    provenance = item_get(item, "provenance")
    if provenance not in ["public", "fixture", "internal", "external"]:
        fail("unknown provenance '{}'".format(provenance))
    publication = item_get(item, "publication", "internal" if provenance == "external" else provenance)
    if publication not in ["public", "fixture", "internal"]:
        fail("unknown publication '{}'".format(publication))
    return publication

def schema_codegen_policy(item):
    policy = item_get(item, "codegen_policy", "external")
    if policy not in ["external", "internal"]:
        fail("unknown codegen_policy '{}'".format(policy))
    return policy

def schema_output_labels(item, wrapper_packages = {}):
    pkg = item_get(item, "pkg")
    stem = item_get(item, "stem")
    labels = {}
    for flag, suffix in [
        ("rust", "rust"), ("cpp", "cpp"), ("python", "py"),
        ("bfbs", "bfbs"), ("schema_meta_rust", "schema_meta"),
        ("schema_meta_cpp", "schema_meta_cpp"), ("action_schema", "action_schema"),
    ]:
        if item_get(item, flag, False):
            labels[flag] = "{}:{}_{}".format(pkg, stem, suffix)
    if schema_meta_py_enabled(item):
        labels["schema_meta_py"] = "{}:{}_schema_meta_py".format(pkg, stem)
    if item_get(item, "cpp_lib", item_get(item, "cpp", False)):
        labels["cpp_lib"] = "{}:{}".format(pkg, schema_cpp_lib_name(item))
    if item_get(item, "rust_wrapper", False):
        labels["rust_wrapper"] = "{}:{}".format(wrapper_packages.get("rust", pkg), schema_rust_binding_name(item))
    if item_get(item, "cpp_wrapper", False):
        labels["cpp_wrapper"] = "{}:{}".format(wrapper_packages.get("cpp", pkg), schema_cpp_binding_name(item))
    return labels

# ---------------------------------------------------------------------------
# v3 manifest row
# ---------------------------------------------------------------------------

def schema_item_manifest_row(item):
    """One `schema_definitions` row of the v3 registry manifest.

    Byte-for-byte the serializer `registry_manifest.bzl` has always used; it
    now loads this function rather than keeping its own copy, so the frozen v3
    golden is a regression test on this definition.
    """
    stem = item_get(item, "stem")
    cpp = item_get(item, "cpp", False)
    cpp_lib = item_get(item, "cpp_lib", cpp)

    cc_alias = None
    if item_has(item, "cc_alias"):
        cc_alias = item_get(item, "cc_alias")
    elif cpp_lib:
        cc_alias = stem + "_cc"

    return {
        "pkg": item_get(item, "pkg"),
        "stem": stem,
        "provenance": item_get(item, "provenance"),
        "edu_public": item_get(item, "edu_public", False),
        "root_fbs": item_get(item, "root_fbs"),
        "dep_fbs": item_get(item, "dep_fbs", []),
        "rust": item_get(item, "rust", False),
        "cpp": cpp,
        "python": item_get(item, "python", False),
        "bfbs": item_get(item, "bfbs", False),
        "schema_meta_rust": item_get(item, "schema_meta_rust", False),
        "schema_meta_cpp": item_get(item, "schema_meta_cpp", False),
        "alias_id": item_get(item, "alias_id", stem),
        "cpp_lib": cpp_lib,
        "cpp_lib_name": item_get(item, "cpp_lib_name", stem + "_cpp_lib"),
        "cc_alias": cc_alias,
        "rust_wrapper": item_get(item, "rust_wrapper", False),
        "cpp_wrapper": item_get(item, "cpp_wrapper", False),
        "rust_wrapper_name": item_get(item, "rust_wrapper_name", None),
        "cpp_wrapper_name": item_get(item, "cpp_wrapper_name", None),
        "cpp_wrapper_schema_cc_target": item_get(item, "cpp_wrapper_schema_cc_target", None),
        "extra_cc_deps": item_get(item, "extra_cc_deps", []),
    }


_FLAG_FIELDS = SCHEMA_FIELDS["flags"]
SOURCE_CONTRACT_FIELDS = SCHEMA_FIELDS["source_contract"]
SCHEMA_ITEM_FIELDS = [field for fields in SCHEMA_FIELDS.values() for field in fields]


def normalize_dep_label(pkg, dep):
    """`dep_fbs` entry -> absolute label, module-qualification preserved.

    Accepts the three spellings the external-root gate already allows
    (`external_schema_root.bzl`: `:x.fbs`, `//pkg:x.fbs`, `@repo//pkg:x.fbs`)
    plus a bare `x.fbs`. The `@` case is the one the design turns on: a schema
    module's fragment says `@aorta//message/schemas/system:aorta_base.fbs`
    where today's in-aorta fragment says `//message/schemas/system:...`.
    """
    if dep.startswith("@"):
        return dep
    if dep.startswith("//"):
        return dep
    if dep.startswith(":"):
        return pkg + dep
    return "{}:{}".format(pkg, dep)

def normalize_schema_entry(item):
    """One fragment entry -> the dict the provider carries.

    Everything a downstream conflict check or exporter could need is derived
    HERE, at loading phase, with the shared helpers in `schema_names.bzl` — so
    the composer never re-implements a naming rule and can never disagree with
    the macro that actually defines the targets.
    """
    fields = item.keys() if type(item) == "dict" else [field for field in dir(item) if field not in ["to_json", "to_proto"]]
    unknown = [field for field in fields if field not in SCHEMA_ITEM_FIELDS]
    if unknown:
        fail("AORTA_SCHEMA_FIELDS {}: unsupported declaration fields {}".format(item_get(item, "stem"), sorted(unknown)))
    pkg = item_get(item, "pkg")
    stem = item_get(item, "stem")
    root_fbs = item_get(item, "root_fbs")
    alias_id = schema_alias_id(item)
    message_base = schema_message_base(alias_id)

    flags = {}
    for field in _FLAG_FIELDS:
        if field == "cpp_lib":
            flags[field] = item_get(item, "cpp_lib", item_get(item, "cpp", False))
        else:
            flags[field] = item_get(item, field, False)
    flags["schema_meta_py"] = schema_meta_py_enabled(item)

    typed = flags["bfbs"]
    python_include_dirs = item_get(item, "python_include_dirs", None)
    if python_include_dirs == None:
        python_include_dirs = ["message"]

    # Derived names, in the same order the macro claims them, so a collision
    # report names the same thing the build would have failed on.
    derived = {
        "alias_id": alias_id,
        "message_base": message_base,
        "cpp_lib_name": schema_cpp_lib_name(item),
        "cc_alias": schema_cc_alias(item),
        "rust_binding_name": schema_rust_binding_name(item) if flags["rust_wrapper"] else None,
        "rust_crate_name": schema_rust_crate_name(item) if flags["rust_wrapper"] else None,
        "rust_facade_name": schema_rust_facade_name(item) if flags["rust_wrapper"] else None,
        "cpp_binding_name": schema_cpp_binding_name(item) if flags["cpp_wrapper"] else None,
        "cpp_binding_header": schema_cpp_binding_header(item) if flags["cpp_wrapper"] else None,
        "schema_meta_py_module": schema_meta_py_module(item) if flags["schema_meta_py"] else None,
    }

    # The root-level `//message:fbs_schema_*` alias face. Same construction as
    # `registry.bzl::_build_schema_output_aliases`, but expressed as names so
    # the composer can detect two modules claiming one alias.
    output_aliases = []
    for field, suffix in [
        ("rust", "rust"),
        ("cpp", "cpp"),
        ("python", "py"),
        ("bfbs", "bfbs"),
        ("schema_meta_rust", "schema_meta"),
        ("schema_meta_cpp", "schema_meta_cpp"),
    ]:
        if flags[field]:
            output_aliases.append("fbs_schema_{}_{}".format(alias_id, suffix))
    if flags["cpp_lib"]:
        output_aliases.append("fbs_schema_{}_cpp_lib".format(alias_id))
    if derived["cc_alias"] != None and flags["cpp_lib"]:
        output_aliases.append("fbs_schema_{}_cc".format(alias_id))

    return {
        "pkg": pkg,
        "stem": stem,
        "root_fbs": root_fbs,
        "provenance": item_get(item, "provenance"),
        "publication": schema_publication(item),
        "codegen_policy": schema_codegen_policy(item),
        "typed": typed,
        "source_contract": {field: item_get(item, field) for field in SOURCE_CONTRACT_FIELDS if item_has(item, field)} if any([item_has(item, field) for field in SOURCE_CONTRACT_FIELDS]) else None,
        "action_name": item_get(item, "action_name", None),
        "python_include_dirs": python_include_dirs,
        "naming_overrides": {
            field: item_get(item, field)
            for field in ["alias_id", "cpp_lib_name", "cc_alias", "rust_wrapper_name",
                          "rust_wrapper_crate_name", "rust_wrapper_facade_name",
                          "cpp_wrapper_name", "cpp_wrapper_header"]
            if (field in item if type(item) == "dict" else hasattr(item, field))
        },
        "flags": flags,
        "derived": derived,
        "output_aliases": output_aliases,
        "dep_fbs": [normalize_dep_label(pkg, dep) for dep in item_get(item, "dep_fbs", [])],
        "logical_include_path": schema_logical_include_path(pkg, root_fbs),
        "fbs_label": "{}:{}".format(pkg, root_fbs),
        # The v3-shaped row, verbatim, so the composed manifest embeds each
        # module's rows in the format every current reader already parses
        # (v3 stays the per-module format; only the
        # COMPOSITION is newly versioned).
        "row": schema_item_manifest_row(item),
    }
