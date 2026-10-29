#!/usr/bin/env python3
"""bfbs-driven semantic loader for FlatBuffers schemas.

Loads a compiled `.bfbs` file into a NEW qualified data model
— this is a model migration, not a parser drop-in.
Originally wired only as a `--shadow-check` cross-check against
the old regex parser's facts, driving no real output; it then became the
SOLE semantic source for `gen_message_wrapper.py`'s Rust/C++
wrapper emitters (via `--root-bfbs`), and a later cleanup deleted the old parser +
`--shadow-check` machinery entirely (including this module's own
`facts_from_loaded_schema`/`diff_facts`, whose only caller that was) — this
loader is now the ONLY path.

Data model
----------
- `TypeId`: fully-qualified name (`namespace` + `name`), plus
  `declaration_file` (root-local vs included classification, see
  `is_root_local`) and `kind` (`"table" | "struct" | "enum" | "union"`).
- `UnionVariantDef(alias, target)`: a union member's flatc-assigned alias
  (its `EnumVal.name` — NOT assumed to equal the target's bare type name,
  unlike the old regex parser) and the `TypeId` it targets (`None` only for
  the synthesized `NONE` = 0 member, which this loader drops).
- `ObjectDef`/`EnumDef`/`UnionDef`: tables/structs/enums/unions keyed by
  qualified name in `LoadedSchema`. Bare-name lookups go through
  `LoadedSchema.resolve_bare`, which hard-errors on a same-bare-name
  collision across namespaces (policy: this stays a `GenerationError`-
  equivalent throughout the program; cross-namespace bare-name collision
  *support* is future work).
- `FieldDef.union_variants`: for a union-typed field, flatc emits TWO
  `reflection.Field` entries per declared union field (`<name>` and the
  synthesized `<name>_type` discriminator, both indexing the same
  union-pseudo-`Enum`). `_load_object_fields` folds the discriminator back
  into the logical field, matching what emitters (and the old parser, which
  never sees the synthesized field at all — it isn't textual) expect.
- `Schema.advanced_features` is checked against `SUPPORTED_ADVANCED_FEATURES`
  and hard-errors (`SchemaLoadError`) on any unsupported bit — notably
  `DefaultVectorsAndStrings`, whose vector/string defaults are not
  recoverable from `Field.default_integer`/`default_real` (reflection has
  no vector/string default payload at all). `optional`/`required`/`key`/
  `offset64` are modeled explicitly per field, straight from `reflection.Field`.

Supported way to run this: `bazel test`/`bazel run` (the reflection Python
bindings — `reflection.Schema` et al. — are generated at build time from the
PINNED flatc's `reflection.fbs`, via `//message/tools:reflection_py_lib`;
there is no vendored second copy of generated bindings, per the proposal's
Q1 resolution). A bare `python3 -m schema_reflection` outside Bazel will
fail to import `reflection` unless the caller has separately arranged for
it on `sys.path` (e.g. for fast local iteration against a scratch
`flatc --python` output — never committed).
"""

from __future__ import annotations

import ast
import inspect
import math
from collections import Counter
from dataclasses import dataclass
from typing import Optional

from reflection.BaseType import BaseType
from reflection.AdvancedFeatures import AdvancedFeatures
from reflection.Schema import Schema, SchemaT


class SchemaLoadError(RuntimeError):
    """Hard-error type for anything this loader does not (or should not)
    support — including any `Schema.advanced_features` bit outside
    `SUPPORTED_ADVANCED_FEATURES`, an unresolvable field `base_type`, or an
    ambiguous bare-name lookup. Fail-closed, matching the repo's
    silent-degradation-becomes-hard-error convention."""


# The loader declares a supported `Schema.advanced_features` bitmask and
# hard-errors on anything outside it (notably `DefaultVectorsAndStrings`).
# `AdvancedArrayFeatures` (fixed-length arrays inside a struct — e.g. a
# quaternion stored as `[float:4]`),
# `AdvancedUnionFeatures`
# (vector-of-union fields), and `OptionalScalars` (`= null` scalar defaults)
# are all fully modeled in this loader's data model (see `_field_shape`'s
# `array<...>`/`vector<...>` handling). `DefaultVectorsAndStrings` defaults
# are NOT recoverable from `Field.default_integer`/`default_real`
# (reflection carries no vector/string default payload at all), so it stays
# hard-unsupported.
SUPPORTED_ADVANCED_FEATURES = (
    AdvancedFeatures.AdvancedArrayFeatures
    | AdvancedFeatures.AdvancedUnionFeatures
    | AdvancedFeatures.OptionalScalars
)

# BaseType -> canonical .fbs scalar keyword, matching the short-form
# vocabulary gen_message_wrapper.py's own CPP_SCALAR_TYPES/RUST_SCALAR_TYPES
# tables key off (so shadow-check facts compare on the same vocabulary).
_SCALAR_BASE_TYPE_NAMES = {
    BaseType.Bool: "bool",
    BaseType.Byte: "byte",
    BaseType.UByte: "ubyte",
    BaseType.Short: "short",
    BaseType.UShort: "ushort",
    BaseType.Int: "int",
    BaseType.UInt: "uint",
    BaseType.Long: "long",
    BaseType.ULong: "ulong",
    BaseType.Float: "float",
    BaseType.Double: "double",
}

_INTEGRAL_BASE_TYPES = frozenset(_SCALAR_BASE_TYPE_NAMES) - {BaseType.Float, BaseType.Double}


@dataclass(frozen=True)
class TypeId:
    namespace: str
    name: str
    declaration_file: str
    kind: str  # "table" | "struct" | "enum" | "union"

    @property
    def qualified_name(self) -> str:
        return "{}.{}".format(self.namespace, self.name) if self.namespace else self.name


@dataclass(frozen=True)
class UnionVariantDef:
    alias: str
    # Every constructed UnionVariantDef carries a real target: the
    # synthesized NONE=0 member is filtered out before construction (see
    # `_union_variants_from_enum`), and any variant whose flatc-parsed
    # `union_type` is not an Object (table/struct) reference — e.g. a legal
    # but unmodeled string-typed union member — is a hard `SchemaLoadError`
    # rather than a silently-constructed `target=None`. There is therefore
    # no remaining case where `target` is `None` for a real instance.
    target: TypeId


@dataclass
class FieldDef:
    name: str
    id: int
    shape: str  # canonical shape string — see `_field_shape`'s docstring
    deprecated: bool
    required: bool
    key: bool
    optional: bool
    offset64: bool
    default: object  # int | float | None (scalar/enum-typed fields only)
    wire_offset: int  # byte offset in a struct; vtable slot in a table
    base_type: int  # reflection.BaseType, including the enum's wire scalar
    # The qualified TypeId this field references, when its shape names a
    # table/struct/enum/union (populated from the same `_field_shape` call
    # that computes `shape` — the model carries qualified TypeIds, not
    # bare names). `None` only for genuine scalar/string
    # fields (and vector/array thereof) with no referenced type. This is
    # what lets two fields referencing e.g. `ns1.X` and `ns2.X` — same bare
    # name, different qualified type — stay distinguishable even though
    # both have `shape == "table:X"`.
    target: Optional[TypeId] = None
    union_variants: Optional[list] = None  # list[UnionVariantDef]; union-kind fields only
    # The nested-buffer root type a `(nested_flatbuffer: "…")` attribute names
    # The field's own `shape` is `[ubyte]` (`target` None), but flatc
    # emits a `*_nested_flatbuffer()` accessor referencing THIS type by its full
    # namespace path — so it is a real cross-crate reference the codegen tools
    # must see. `None` for fields with no such attribute.
    nested_flatbuffer: Optional[TypeId] = None


@dataclass
class ObjectDef:
    type_id: TypeId
    fields: list  # list[FieldDef], sorted by id
    bytesize: Optional[int]  # struct only; None for tables
    minalign: Optional[int]  # struct only; None for tables


@dataclass(frozen=True)
class PythonScalarFact:
    accessor: str
    default: object
    matches_bfbs: bool


def python_scalar_facts(obj: ObjectDef, generated_table: type) -> dict[str, PythonScalarFact]:
    """Generation-time oracle from the matching pinned flatc table class.

    BFBS remains the semantic model. The generated source supplies accessor
    spelling and absent values that BFBS cannot always preserve. Callers must
    declare that source and BFBS together as generation inputs.
    """
    methods = {}
    tree = ast.parse(inspect.getsource(generated_table))
    definitions = [method for method in tree.body[0].body if isinstance(method, ast.FunctionDef)]
    name_counts = Counter(method.name for method in definitions)
    source_file = inspect.getsourcefile(generated_table)
    source_line = inspect.getsourcelines(generated_table)[1]
    for method in definitions:
        if len(method.args.args) != 1:
            continue
        for call in ast.walk(method):
            if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "Offset"
                    and isinstance(call.func.value, ast.Attribute)
                    and call.func.value.attr == "_tab" and len(call.args) == 1
                    and isinstance(call.args[0], ast.Constant)):
                methods.setdefault(call.args[0].value, []).append(method)
    empty = generated_table()
    # Four-byte vtable and empty table, with no present fields.
    empty.Init(b"\x04\x00\x04\x00\x04\x00\x00\x00", 4)
    facts = {}
    for field in obj.fields:
        if field.deprecated or field.base_type not in _SCALAR_BASE_TYPE_NAMES:
            continue
        candidates = methods[field.wire_offset]
        if len(candidates) != 1 or name_counts[candidates[0].name] != 1:
            raise ValueError("scalar accessor is not unique: {}".format(field.name))
        method = candidates[0]
        accessor = method.name
        getter = getattr(generated_table, accessor)
        # Bind the AST slot to the actual unshadowed class function.
        if (getter.__code__.co_filename != source_file
                or getter.__code__.co_firstlineno != source_line + method.lineno - 1):
            raise ValueError("scalar accessor does not match its slot: {}".format(field.name))
        value = getter(empty)
        expected = None if field.optional else field.default
        equal = expected == value
        if isinstance(expected, float) and isinstance(value, float):
            if math.isnan(expected) or math.isnan(value):
                equal = math.isnan(expected) and math.isnan(value)
            elif expected == value == 0.0:
                equal = math.copysign(1.0, expected) == math.copysign(1.0, value)
        facts[field.name] = PythonScalarFact(accessor, value, equal)
    return facts


@dataclass
class EnumDef:
    type_id: TypeId
    base_type: str  # canonical scalar keyword, e.g. "ubyte"
    values: list  # list[tuple[name, value]], sorted by (value, name)


@dataclass
class UnionDef:
    type_id: TypeId
    variants: list  # list[UnionVariantDef], sorted by (value, alias); NONE excluded


class LoadedSchema:
    def __init__(self) -> None:
        self.root_type: Optional[TypeId] = None
        self.file_ident: Optional[str] = None
        self.advanced_features: int = 0
        # The `declaration_file` string of the ROOT schema file, derived from
        # the bfbs's own `fbs_files` include graph (`_derive_root_declaration_file`)
        # — the ONLY sound key for root-local classification; see
        # `is_root_local` for why any assumption about this string's SHAPE
        # (basename, relative, absolute) is unsound.
        self.root_declaration_file: str = ""
        self.tables: dict[str, ObjectDef] = {}
        self.structs: dict[str, ObjectDef] = {}
        self.enums: dict[str, EnumDef] = {}
        self.unions: dict[str, UnionDef] = {}

    def resolve_bare(self, mapping: dict, bare_name: str) -> str:
        """Resolve `bare_name` to its unique qualified key in `mapping`
        (one of `self.tables`/`self.structs`/`self.enums`/`self.unions`).
        Hard-errors on zero or multiple matches (same-bare-name
        collision policy)."""
        matches = [q for q in mapping if q.rsplit(".", 1)[-1] == bare_name]
        if not matches:
            raise SchemaLoadError("no type named '{}' found in this schema".format(bare_name))
        if len(matches) > 1:
            raise SchemaLoadError(
                "ambiguous bare name '{}' matches {} qualified types: {}".format(
                    bare_name, len(matches), sorted(matches)
                )
            )
        return matches[0]


def _derive_root_declaration_file(fbs_files: list[tuple[str, list[str]]]) -> str:
    """Derive the ROOT schema file's `declaration_file` string from the
    bfbs's own `fbs_files` include graph (`[(filename, [included_filenames])]`).

    The root is the unique file no other file includes — flatc compiles
    exactly one root schema, and every other file in the bfbs got there via
    a (transitive) `include` from it. Hard-errors (`SchemaLoadError`,
    fail-closed) when the graph is absent or does not yield exactly one
    candidate, rather than guessing.

    This graph-derived key is compared against each type's own
    `declaration_file` by `is_root_local` — both strings are produced by
    the SAME flatc path-relativization pass over the SAME invocation, so
    they match each other REGARDLESS of what encoding that pass produced.
    That property is the entire point: no assumption about the string's
    shape survives contact with reality (see `is_root_local`'s docstring
    for the disproven previous attempt).
    """
    if not fbs_files:
        raise SchemaLoadError(
            "bfbs has no fbs_files entries — cannot derive the root schema "
            "file for root-local classification (fail-closed; the pinned "
            "flatc emits fbs_files for every schema this repo compiles, so "
            "an empty graph means an incompatible bfbs producer)"
        )
    included: set[str] = set()
    for _, includes in fbs_files:
        included.update(includes)
    candidates = [filename for filename, _ in fbs_files if filename not in included]
    if len(candidates) != 1:
        raise SchemaLoadError(
            "fbs_files include graph does not identify exactly one root "
            "schema file (candidates not included by any other file: {}) — "
            "fail-closed rather than guessing".format(sorted(candidates))
        )
    return candidates[0]


def is_root_local(type_id: TypeId, loaded: "LoadedSchema") -> bool:
    """True iff `type_id` was declared directly in the root schema file
    (not pulled in via an `include`) — by comparing its
    `declaration_file` against `loaded.root_declaration_file`, the root
    filename derived from the bfbs's own `fbs_files` include graph.

    Why graph-derived equality and NOT any assumption about the string's
    shape: a previous implementation compared against
    `"//" + <root basename>`, on the "empirically verified" claim that
    flatc always encodes `declaration_file` as a bare basename. That claim
    was wrong twice over. In-repo, an include from another directory is
    encoded relative to the root's directory WITH `../` segments
    (`//../../system/aorta_base.fbs`) — the original verification had
    only used same-directory fixtures. And in an EXTERNAL Bzlmod consumer
    (aorta via `local_path_override`, the release validator's exact
    setup), even the ROOT file's own entry comes out as a `../`-chain
    down to filesystem root plus the symlink-RESOLVED absolute path
    (`//../../../..(...)/Users/<user>/.../echo.fbs`), so the basename
    equality misclassified every root table as include-declared and the
    shadow-check hard-failed the external build. The encoding is
    context-dependent, but it is SELF-CONSISTENT within one bfbs — the
    include-graph filenames and every `declaration_file` come from the
    same relativization pass — so ground-truth-to-ground-truth equality
    is the only comparison that cannot be broken by a new path shape.
    (Deliberately NOT basename-vs-basename: a root including a file with
    the root's own basename from a subdirectory would then misclassify
    that include's types as root-local.)
    """
    return type_id.declaration_file == loaded.root_declaration_file


def _decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _split_qualified(qualified_name: str) -> tuple[str, str]:
    if "." in qualified_name:
        namespace, _, name = qualified_name.rpartition(".")
        return namespace, name
    return "", qualified_name


def _type_id_for_object(obj_t, kind: str) -> TypeId:
    namespace, name = _split_qualified(_decode(obj_t.name))
    return TypeId(
        namespace=namespace,
        name=name,
        declaration_file=_decode(obj_t.declarationFile) or "",
        kind=kind,
    )


def _resolve_object_by_name(schema_t: SchemaT, decl_obj_t, name: str) -> TypeId:
    """Resolve a (possibly bare or partially-qualified) table/struct name to its
    TypeId the way flatc's `LookupStruct` does: try the fully-qualified name
    exactly, then the declaring object's namespace and each ancestor namespace.
    flatc's parser guarantees the target exists, so a miss is a loader gap."""
    def _find(qualified: str):
        for obj_t in schema_t.objects:
            if _decode(obj_t.name) == qualified:
                return _type_id_for_object(
                    obj_t, "struct" if obj_t.isStruct else "table"
                )
        return None

    hit = _find(name)
    if hit is not None:
        return hit
    decl_ns, _ = _split_qualified(_decode(decl_obj_t.name))
    parts = decl_ns.split(".") if decl_ns else []
    for i in range(len(parts), -1, -1):
        prefix = ".".join(parts[:i])
        hit = _find((prefix + "." + name) if prefix else name)
        if hit is not None:
            return hit
    raise SchemaLoadError(
        "nested_flatbuffer target {!r} on object {!r} does not resolve to a "
        "known table/struct".format(name, _decode(decl_obj_t.name))
    )


def _nested_flatbuffer_target(schema_t: SchemaT, decl_obj_t, f) -> Optional[TypeId]:
    """The TypeId a field's `(nested_flatbuffer: "…")` attribute names, or None.
    The attribute value (reflection `Field.attributes` KeyValue where key ==
    `nested_flatbuffer`) is the nested root's name; resolve it like flatc."""
    for kv in (f.attributes or []):
        if _decode(kv.key) == "nested_flatbuffer":
            value = _decode(kv.value)
            if value:
                return _resolve_object_by_name(schema_t, decl_obj_t, value)
    return None


def _type_id_for_enum(enum_t) -> TypeId:
    namespace, name = _split_qualified(_decode(enum_t.name))
    kind = "union" if enum_t.isUnion else "enum"
    return TypeId(
        namespace=namespace,
        name=name,
        declaration_file=_decode(enum_t.declarationFile) or "",
        kind=kind,
    )


def _named_type_shape(schema_t: SchemaT, type_t) -> tuple[str, TypeId]:
    """For a field `Type` whose `index` names an object or enum (`base_type
    == Obj`, or any base_type paired with `index >= 0` — the union/
    union-discriminator/plain-enum-typed-integral cases, per reflection.fbs's
    `Type.index` docstring), resolve and return `(kind, TypeId)`."""
    if type_t.baseType == BaseType.Obj:
        obj_t = schema_t.objects[type_t.index]
        kind = "struct" if obj_t.isStruct else "table"
        return kind, _type_id_for_object(obj_t, kind)
    enum_t = schema_t.enums[type_t.index]
    type_id = _type_id_for_enum(enum_t)
    return type_id.kind, type_id


def _scalar_or_named_shape(schema_t: SchemaT, base_type: int, index: int) -> tuple[str, Optional[TypeId]]:
    """Shared by `_field_shape` (direct fields) and vector element
    resolution: classify one `(base_type, index)` pair — everything except
    `String`/`Vector`/`Vector64` themselves, which their callers handle."""
    if base_type == BaseType.Obj or (index is not None and index >= 0):
        if base_type == BaseType.Obj:
            obj_t = schema_t.objects[index]
            kind = "struct" if obj_t.isStruct else "table"
            type_id = _type_id_for_object(obj_t, kind)
        else:
            enum_t = schema_t.enums[index]
            type_id = _type_id_for_enum(enum_t)
            kind = type_id.kind
        return "{}:{}".format(kind, type_id.name), type_id
    if base_type in _SCALAR_BASE_TYPE_NAMES:
        return "scalar:{}".format(_SCALAR_BASE_TYPE_NAMES[base_type]), None
    raise SchemaLoadError(
        "unsupported base_type {} (index={}) — not a scalar, and not an "
        "object/enum reference".format(base_type, index)
    )


def _field_shape(schema_t: SchemaT, type_t) -> tuple[str, Optional[TypeId]]:
    """Canonical, language-agnostic field-type shape string, plus the
    referenced `TypeId` when the field names a table/struct/enum/union
    (`None` for scalars/strings). Vocabulary: `"scalar:<fbs-keyword>"`,
    `"string"`, `"table:<bare-name>"`, `"struct:<bare-name>"`,
    `"enum:<bare-name>"`, `"union:<bare-name>"`, `"vector<...>"` (element
    shape recurses one level — FlatBuffers disallows vector-of-vector, so
    one level is exhaustive), `"array<...,N>"` (fixed-length struct array,
    `AdvancedArrayFeatures` — a fixed-size array inside a struct, e.g. a
    quaternion stored as `[float:4]`; this repo has no
    array-of-struct or array-of-enum field today, but the element
    resolution shares `_scalar_or_named_shape` with `vector<...>` so both
    are handled uniformly regardless)."""
    base = type_t.baseType
    if base == BaseType.String:
        return "string", None
    if base in (BaseType.Vector, BaseType.Vector64):
        if type_t.element == BaseType.String:
            return "vector<string>", None
        elem_shape, elem_type_id = _scalar_or_named_shape(schema_t, type_t.element, type_t.index)
        return "vector<{}>".format(elem_shape), elem_type_id
    if base == BaseType.Array:
        elem_shape, elem_type_id = _scalar_or_named_shape(schema_t, type_t.element, type_t.index)
        return "array<{},{}>".format(elem_shape, type_t.fixedLength), elem_type_id
    return _scalar_or_named_shape(schema_t, base, type_t.index)


def _scalar_default(base_type: int, field_t) -> object:
    if base_type in (BaseType.Float, BaseType.Double):
        return field_t.defaultReal
    if base_type in _INTEGRAL_BASE_TYPES:
        return field_t.defaultInteger
    return None


def _union_variants_from_enum(schema_t: SchemaT, enum_t) -> list:
    """Build this union's variant list (NONE=0 excluded), hard-erroring on
    any variant kind this loader does not model.

    The pinned flatc allows a union member's target type to be a
    STRING, not just a table/struct (idl_parser.cpp's `ParseEnum`: `if
    (ev.union_type.base_type != BASE_TYPE_STRUCT && ev.union_type.base_type
    != BASE_TYPE_STRING) return Error(...)` — STRING is accepted). This
    loader only models table/struct (Object) targets today; previously a
    string-typed (or otherwise non-Object) variant silently fell through to
    `target=None`, indistinguishable from "this is the filtered NONE
    member" (it never is, by construction — NONE is filtered above, before
    any UnionVariantDef is built). Hard-erroring here is simpler than
    modeling a string-kind target for a shape no repo schema uses today
    (fail-closed convention), and closes the silent gap.
    """
    entries = []
    for v in enum_t.values or []:
        name = _decode(v.name)
        if name == "NONE" and v.value == 0:
            continue
        ut = v.unionType
        if ut is None or ut.baseType != BaseType.Obj or ut.index is None or ut.index < 0:
            raise SchemaLoadError(
                "union variant '{}' (value={}) has an unsupported variant "
                "kind (union_type.base_type={!r}) — this loader only "
                "models table/struct-typed union variants; string-typed "
                "union variants are valid FlatBuffers (idl_parser.cpp "
                "accepts BASE_TYPE_STRING for a union member) but are not "
                "yet modeled here".format(
                    name, v.value, ut.baseType if ut is not None else None
                )
            )
        obj_t = schema_t.objects[ut.index]
        kind = "struct" if obj_t.isStruct else "table"
        target = _type_id_for_object(obj_t, kind)
        entries.append((v.value, UnionVariantDef(alias=name, target=target)))
    entries.sort(key=lambda entry: (entry[0], entry[1].alias))
    return [variant for _, variant in entries]


def _is_discriminator_type(type_t) -> bool:
    """True for a union discriminator field's `Type` — either a scalar
    `UType` (plain union field) or a `Vector`/`Vector64` of `UType`
    (vector-of-union field: flatc emits a PARALLEL vector of per-element
    discriminators, e.g. `items_type: [ubyte]`, not a single scalar)."""
    return type_t.baseType == BaseType.UType or (
        type_t.baseType in (BaseType.Vector, BaseType.Vector64)
        and type_t.element == BaseType.UType
    )


def _load_object_fields(schema_t: SchemaT, obj_t) -> list:
    raw_fields = list(obj_t.fields or [])
    names = {_decode(f.name) for f in raw_fields}
    discriminator_names: set[str] = set()
    for f in raw_fields:
        name = _decode(f.name)
        if not _is_discriminator_type(f.type):
            continue
        if not name.endswith("_type"):
            raise SchemaLoadError(
                "unexpected UType field '{}' does not end in '_type' — the "
                "discriminator-folding assumption "
                "does not hold for this field".format(name)
            )
        sibling = name[: -len("_type")]
        if sibling not in names:
            raise SchemaLoadError(
                "UType discriminator field '{}' has no sibling union field "
                "'{}' in the same object".format(name, sibling)
            )
        discriminator_names.add(name)

    folded: list[FieldDef] = []
    for f in raw_fields:
        name = _decode(f.name)
        if name in discriminator_names:
            continue
        type_t = f.type
        # `_field_shape` already computes the precise, qualified
        # TypeId a table/struct/enum/union-typed field references (needed
        # to distinguish e.g. `ns1.X` from `ns2.X` sharing bare name `X`,
        # since the model carries qualified TypeIds, not bare
        # names) — previously that second return value was
        # discarded here (`shape, _ = ...`), keeping only the bare-name
        # `shape` string, so the loaded model itself could not distinguish
        # such fields even though the loader always computed the answer.
        shape, target = _field_shape(schema_t, type_t)
        union_variants = None
        is_union_field = type_t.baseType == BaseType.Union or (
            type_t.baseType in (BaseType.Vector, BaseType.Vector64)
            and type_t.element == BaseType.Union
        )
        if is_union_field:
            enum_t = schema_t.enums[type_t.index]
            union_variants = _union_variants_from_enum(schema_t, enum_t)
        folded.append(
            FieldDef(
                name=name,
                id=f.id,
                shape=shape,
                deprecated=bool(f.deprecated),
                required=bool(f.required),
                key=bool(f.key),
                optional=bool(f.optional),
                offset64=bool(f.offset64),
                default=_scalar_default(type_t.baseType, f),
                wire_offset=f.offset,
                base_type=type_t.baseType,
                target=target,
                union_variants=union_variants,
                nested_flatbuffer=_nested_flatbuffer_target(schema_t, obj_t, f),
            )
        )
    # Fields are ordered by field id — reflection's `Object.fields` is sorted by
    # FIELD NAME (verified empirically against a real compiled schema), not
    # by id, despite the "// Sorted." comment in reflection.fbs — this
    # loader re-sorts explicitly rather than trusting bfbs field order.
    folded.sort(key=lambda fd: fd.id)
    return folded


def load_bfbs(bfbs_bytes: bytes) -> LoadedSchema:
    """Load a compiled `.bfbs` buffer into a `LoadedSchema`. Raises
    `SchemaLoadError` on any unsupported `advanced_features` bit or
    unrecognized field shape (fail-closed — see module docstring)."""
    schema_raw = Schema.GetRootAs(bfbs_bytes, 0)
    schema_t = SchemaT.InitFromObj(schema_raw)

    advanced = schema_t.advancedFeatures or 0
    unsupported = advanced & ~SUPPORTED_ADVANCED_FEATURES
    if unsupported:
        raise SchemaLoadError(
            "schema uses unsupported advanced_features bit(s): {:#x} "
            "(supported mask: {:#x}) — notably DefaultVectorsAndStrings "
            "({:#x}), whose vector/string defaults are not recoverable from "
            "Field.default_integer/default_real".format(
                unsupported, SUPPORTED_ADVANCED_FEATURES, AdvancedFeatures.DefaultVectorsAndStrings
            )
        )

    loaded = LoadedSchema()
    loaded.file_ident = _decode(schema_t.fileIdent) or None
    loaded.advanced_features = advanced
    loaded.root_declaration_file = _derive_root_declaration_file(
        [
            (
                _decode(fbs_file.filename) or "",
                [_decode(inc) for inc in (fbs_file.includedFilenames or [])],
            )
            for fbs_file in (schema_t.fbsFiles or [])
        ]
    )

    for obj_t in schema_t.objects or []:
        kind = "struct" if obj_t.isStruct else "table"
        type_id = _type_id_for_object(obj_t, kind)
        object_def = ObjectDef(
            type_id=type_id,
            fields=_load_object_fields(schema_t, obj_t),
            bytesize=obj_t.bytesize if obj_t.isStruct else None,
            minalign=obj_t.minalign if obj_t.isStruct else None,
        )
        (loaded.structs if obj_t.isStruct else loaded.tables)[type_id.qualified_name] = object_def

    for enum_t in schema_t.enums or []:
        type_id = _type_id_for_enum(enum_t)
        if enum_t.isUnion:
            loaded.unions[type_id.qualified_name] = UnionDef(
                type_id=type_id,
                variants=_union_variants_from_enum(schema_t, enum_t),
            )
            continue
        underlying = enum_t.underlyingType
        base_type_name = _SCALAR_BASE_TYPE_NAMES.get(underlying.baseType if underlying else None)
        if base_type_name is None:
            raise SchemaLoadError(
                "enum '{}' has an unsupported underlying base_type".format(type_id.qualified_name)
            )
        values = sorted(
            ((_decode(v.name), v.value) for v in enum_t.values or []),
            key=lambda name_value: (name_value[1], name_value[0]),
        )
        loaded.enums[type_id.qualified_name] = EnumDef(
            type_id=type_id, base_type=base_type_name, values=values
        )

    if schema_t.rootTable is not None:
        root_kind = "struct" if schema_t.rootTable.isStruct else "table"
        loaded.root_type = _type_id_for_object(schema_t.rootTable, root_kind)

    return loaded


def load_bfbs_file(path: str) -> LoadedSchema:
    with open(path, "rb") as f:
        return load_bfbs(f.read())
