#!/usr/bin/env python3
"""Negative/positive case driver for the schema-decoupling rules.

WHY A DRIVER SCRIPT AND NOT `analysistest`
------------------------------------------
Both layers under test refuse by `fail()`ing, and the two `fail()`s happen in
different Bazel phases:

- `aorta_schema_package` (`//message/tools:schema_package.bzl`) validates during
  the LOADING phase, because it must refuse before it generates any target.
- `aorta_registry` (`//message/tools:registry_compose.bzl`) validates during the
  ANALYSIS phase, from providers.

`@bazel_skylib//lib:unittest.bzl`'s `analysistest` can capture an analysis-phase
failure, but skylib is not a direct `bazel_dep` of aorta, and a loading-phase
`fail()` cannot be captured by any in-build test at all: it aborts package
loading, so a fixture package spelling one would break `bazel build //...` for
the whole repository.

This driver therefore writes each fixture package into the workspace, runs the
OUTER bazel against it, asserts the expected refusal, and removes it again — so
no permanent package can ever contain a deliberate failure, and every case is
checked against the real rule rather than a re-implementation of it.

Usage (from the aorta checkout root):

    python3 message/tools/tests/schema_decoupling_cases_test.py
"""

import os
import json
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap
import uuid

FIXTURE_ROOT_REL = "message/tools/tests/_p05_cases"


def repo_root():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", "..", ".."))
    if not os.path.exists(os.path.join(root, "MODULE.bazel")):
        raise SystemExit("cannot locate the aorta checkout root from %s" % here)
    return root


def write(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write(textwrap.dedent(content).lstrip("\n"))


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

FRAGMENT_LOAD = 'load("//message/tools:tests/schema_fragment_fixture.bzl", aorta_schema_fragment = "fixture_fragment")'
PACKAGE_LOAD = 'load("//message/tools:schema_package.bzl", "aorta_schema_package")'
REGISTRY_LOAD = 'load("//message/tools:registry_compose.bzl", "aorta_registry")'


def entry(pkg, stem, **kwargs):
    fields = {
        "pkg": repr(pkg),
        "stem": repr(stem),
        "root_fbs": repr(stem + ".fbs"),
        "provenance": repr("internal"),
    }
    for key, value in kwargs.items():
        fields[key] = repr(value) if isinstance(value, str) else str(value)
    return "struct(%s)" % ", ".join("%s = %s" % kv for kv in sorted(fields.items()))


def write_items_bzl(case_dir, rel_pkg, entries, symbol="ITEMS"):
    """`struct()` is a .bzl-only builtin, so fixture entries live in a real
    pure-data fragment file — the same shape a schema module ships."""
    write(
        os.path.join(case_dir, rel_pkg, "fragment.bzl"),
        "%s = [\n    %s,\n]\n" % (symbol, ",\n    ".join(entries)),
    )
    return 'load(":fragment.bzl", "%s")' % symbol


def fragment_package(case_dir, rel_pkg, entries, module_name="aorta", api=2,
                     target="fragment"):
    """A package holding empty `.fbs` files plus one `aorta_schema_fragment`."""
    pkg_dir = os.path.join(case_dir, rel_pkg)
    pkg_label = "//%s/%s" % (FIXTURE_ROOT_REL, rel_pkg) if rel_pkg else "//" + FIXTURE_ROOT_REL
    items_load = write_items_bzl(case_dir, rel_pkg, entries)
    write(
        os.path.join(pkg_dir, "BUILD.bazel"),
        """
        %s
        %s

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(
            name = "%s",
            api = %d,
            items = ITEMS,
            module_name = "%s",
        )
        """ % (FRAGMENT_LOAD, items_load, target, api, module_name),
    )
    return pkg_label


def touch_fbs(case_dir, rel_pkg, stems):
    for stem in stems:
        path = os.path.join(case_dir, rel_pkg, stem + ".fbs")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as handle:
            handle.write("// Fixture, never compiled.\n")


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def case_api_mismatch(case_dir):
    rel = "message/schemas/topic/api"
    touch_fbs(case_dir, rel, ["a"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items_load = write_items_bzl(
        case_dir, rel, [entry(pkg, "a", cpp=True, python=True, rust=True)])
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        aorta_schema_package(
            module_name = "aorta",
            name = "api_case",
            api = 7,
            items = ITEMS,
            srcs = ["a.fbs"],
        )
        """ % (PACKAGE_LOAD, items_load),
    )
    return "%s:api_case_cc" % pkg, "api = 7 but this aorta provides SCHEMA_RULES_API = 2"


def case_pkg_prefix(case_dir):
    rel = "message/schemas/topic/pkgpfx"
    touch_fbs(case_dir, rel, ["a"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    # The review's §F1 finding, made executable: `package_name()` has no `//`,
    # so the design's own sample selector matched zero entries.
    items_load = write_items_bzl(
        case_dir, rel, [entry(pkg[2:], "a", cpp=True, python=True, rust=True)])
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        aorta_schema_package(
            module_name = "aorta",
            name = "pkg_case",
            api = 2,
            items = ITEMS,
            srcs = ["a.fbs"],
        )
        """ % (PACKAGE_LOAD, items_load),
    )
    return "%s:pkg_case_cc" % pkg, "entry pkg carries the '//' prefix"


def case_srcs_bijection(case_dir):
    rel = "message/schemas/topic/bijection"
    touch_fbs(case_dir, rel, ["a", "orphan"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items_load = write_items_bzl(
        case_dir, rel, [entry(pkg, "a", cpp=True, python=True, rust=True)])
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        aorta_schema_package(
            module_name = "aorta",
            name = "bij_case",
            api = 2,
            items = ITEMS,
            srcs = glob(["*.fbs"]),
        )
        """ % (PACKAGE_LOAD, items_load),
    )
    return "%s:bij_case_cc" % pkg, "source 'orphan.fbs' is in srcs but no item declares it"


def case_duplicate_stem(case_dir):
    rel = "message/schemas/topic/dupstem"
    touch_fbs(case_dir, rel, ["a"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items_load = write_items_bzl(case_dir, rel, [
        entry(pkg, "a", cpp=True, python=True, rust=True),
        entry(pkg, "a", cpp=True, python=True, rust=True, alias_id="a2"),
    ])
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        aorta_schema_package(
            module_name = "aorta",
            name = "dup_case",
            api = 2,
            items = ITEMS,
            srcs = ["a.fbs"],
        )
        """ % (PACKAGE_LOAD, items_load),
    )
    return "%s:dup_case_cc" % pkg, "duplicate stem 'a'"


def _registry(case_dir, name, fragment_labels):
    write(
        os.path.join(case_dir, "root", "BUILD.bazel"),
        """
        %s

        aorta_registry(
            name = "%s",
            fragments = [
                %s
            ],
        )
        """ % (
            REGISTRY_LOAD,
            name,
            "\n        ".join('"%s",' % label for label in fragment_labels),
        ),
    )
    return "//%s/root:%s" % (FIXTURE_ROOT_REL, name)


def case_logical_path_collision(case_dir):
    # Two packages whose labels differ but whose FlatBuffers include identity
    # is the same `schemas/topic/dup/x.fbs`. Include-only entries with distinct
    # alias ids, so the logical path is the ONLY thing that collides.
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/dup" % side
        touch_fbs(case_dir, rel, ["x"])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(pkg, "x", alias_id="x_" + side)],
            ) + ":fragment",
        )
    return _registry(case_dir, "logical", labels), "logical include path 'schemas/topic/dup/x.fbs'"


def case_alias_and_crate_collision(case_dir):
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/alias%s" % (side, side)
        touch_fbs(case_dir, rel, ["s" + side])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(
                    pkg, "s" + side,
                    alias_id="shared_alias",
                    cpp=True, python=True, rust=True, bfbs=True,
                    cpp_lib=True, rust_wrapper=True, cpp_wrapper=True,
                    schema_meta_rust=True, schema_meta_cpp=True,
                )],
            ) + ":fragment",
        )
    return _registry(case_dir, "alias", labels), "schema output alias 'fbs_schema_shared_alias_rust'"


def case_rust_crate_collision(case_dir):
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/crate%s" % (side, side)
        touch_fbs(case_dir, rel, ["c" + side])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(
                    pkg, "c" + side,
                    alias_id="crate_" + side,
                    cpp=True, python=True, rust=True, bfbs=True,
                    rust_wrapper=True,
                    rust_wrapper_crate_name="one_crate",
                )],
            ) + ":fragment",
        )
    return _registry(case_dir, "crate", labels), "Rust crate name 'one_crate'"


def case_python_module_collision(case_dir):
    # Same stem, DIFFERENT logical paths: legal Bazel labels, legal includes,
    # but `python_msgs_tree` copies both `<stem>_schema_meta.py` to the wheel
    # tree root with `cp -f`.
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/py%s" % (side, side)
        touch_fbs(case_dir, rel, ["samestem"])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(
                    pkg, "samestem",
                    alias_id="py_" + side,
                    cpp=True, python=True, rust=True, bfbs=True,
                )],
            ) + ":fragment",
        )
    return (
        _registry(case_dir, "pymod", labels),
        "Python schema-meta module path 'samestem_schema_meta.py'",
    )


def case_conflicting_redeclaration(case_dir):
    rel = "message/schemas/topic/redecl"
    touch_fbs(case_dir, rel, ["r"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    write(
        os.path.join(case_dir, rel, "fragment.bzl"),
        "ONE = [%s]\nTWO = [%s]\n" % (
            entry(pkg, "r", cpp=True),
            entry(pkg, "r", cpp=True, python=True),
        ),
    )
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        load(":fragment.bzl", "ONE", "TWO")

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(
            name = "one",
            api = 2,
            items = ONE,
            module_name = "aorta",
        )

        aorta_schema_fragment(
            name = "two",
            api = 2,
            items = TWO,
            module_name = "aorta",
        )
        """ % (FRAGMENT_LOAD,),
    )
    return (
        _registry(case_dir, "redecl", ["%s:one" % pkg, "%s:two" % pkg]),
        "is declared twice with DIFFERENT metadata",
    )


def case_binding_target_collision(case_dir):
    """Two entries in ONE package whose derived C++ binding target collides.

    Replaces the "module identity" case, which turned out not to be
    expressible in a single-repository fixture: `origin_repo` is assigned by
    Bazel, so every in-repo fragment shares one token and the surviving rule
    ("a module name is claimed by two repositories") can only fire across a
    real module boundary. That rule is exercised for the first time in
    stage 2.
    """
    rel = "message/schemas/topic/binding"
    touch_fbs(case_dir, rel, ["b1", "b2"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    labels = []
    for index, stem in enumerate(("b1", "b2")):
        write(
            os.path.join(case_dir, rel, "fragment%d.bzl" % index),
            "ITEMS%d = [%s]\n" % (
                index,
                entry(
                    pkg, stem,
                    alias_id="bind_" + stem,
                    cpp=True, python=True, rust=True, bfbs=True,
                    cpp_wrapper=True,
                    cpp_wrapper_name="one_binding_cc",
                ),
            ),
        )
        labels.append("%s:frag%d" % (pkg, index))
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        load(":fragment0.bzl", "ITEMS0")
        load(":fragment1.bzl", "ITEMS1")

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(name = "frag0", api = 2, items = ITEMS0, module_name = "aorta")

        aorta_schema_fragment(name = "frag1", api = 2, items = ITEMS1, module_name = "aorta")
        """ % (FRAGMENT_LOAD,),
    )
    return _registry(case_dir, "binding", labels), "message binding target"


def case_fragment_api_mismatch(case_dir):
    rel = "message/schemas/topic/fragapi"
    touch_fbs(case_dir, rel, ["f"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    fragment_package(case_dir, rel, [entry(pkg, "f")], api=7)
    return "%s:fragment" % pkg, "api = 7 but this aorta provides SCHEMA_RULES_API = 2"


def case_diamond_ok(case_dir):
    """POSITIVE: identical entries reached through two fragments are deduped."""
    rel = "message/schemas/topic/diamond"
    touch_fbs(case_dir, rel, ["d"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items_load = write_items_bzl(
        case_dir, rel,
        [entry(pkg, "d", cpp=True, python=True, rust=True, bfbs=True)])
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(
            name = "left",
            api = 2,
            items = ITEMS,
            module_name = "aorta",
        )

        aorta_schema_fragment(
            name = "right",
            api = 2,
            items = ITEMS,
            module_name = "aorta",
        )
        """ % (FRAGMENT_LOAD, items_load),
    )
    return _registry(case_dir, "diamond", ["%s:left" % pkg, "%s:right" % pkg]), None



# ---------------------------------------------------------------------------
# RED cases — the review's (r2) MAJOR counterexamples, as executable tests.
#
# Each one encodes the DESIRED behaviour, not today's. `RED_CASES` are known
# to disagree with the current rules; the driver reports them as `RED` and
# does NOT count them as suite failures, so a run always says precisely which
# of them are still open. When one starts behaving as desired the driver
# prints `FIXED` — that is the signal to promote it into NEGATIVE/POSITIVE.
#
# `PINNED_CASES` are the opposite: they lock in behaviour that is currently
# observed and NOT yet decided, so a silent change is caught. They must pass.
# ---------------------------------------------------------------------------

PROBE = 'filegroup(name = "probe", srcs = [])'


def package_case(case_dir, rel, stems, entries, name):
    """A package whose BUILD calls `aorta_schema_package` plus an inert probe.

    Targeting the probe isolates the LOADING-phase decision: if the macro
    refuses, loading the package fails and the probe cannot be built; if it
    accepts, an empty filegroup analyses trivially, so the case can never go
    green or red for an unrelated analysis reason.
    """
    touch_fbs(case_dir, rel, stems)
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items_load = write_items_bzl(case_dir, rel, entries)
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        %s

        aorta_schema_package(
            module_name = "aorta",
            name = "%s",
            api = 2,
            items = ITEMS,
            srcs = %r,
        )

        %s
        """ % (PACKAGE_LOAD, items_load, name,
               [stem + ".fbs" for stem in stems], PROBE),
    )
    return "%s:probe" % pkg


def red_dep_fbs_unresolved(case_dir):
    """r2 §A1: a dependency on a module that is not in the collection.

    `_compose` reads `dep_fbs` into the manifest but never resolves it against
    `(owner, pkg, stem)`, so a registry that cannot possibly be built composes
    with zero errors. A composition gate that certifies "the effective schema
    collection" has to close this.
    """
    rel = "message/schemas/topic/depmiss"
    touch_fbs(case_dir, rel, ["d"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    label = fragment_package(
        case_dir, rel,
        [entry(pkg, "d", cpp=True, bfbs=True,
               dep_fbs=["@missing_module//message/schemas/system:aorta_base.fbs"])],
    ) + ":fragment"
    return (
        _registry(case_dir, "depmiss", [label]),
        "dependency",
    )


def red_action_name_dedupe(case_dir):
    """r2 §A2: same source/owner/pkg/stem, DIFFERENT action routing metadata.

    `_entry_identity` serialises the NORMALIZED entry, and `action_name` is
    not normalized (`schema_fragment.bzl` `_FLAG_FIELDS` carries
    `action_schema` but not the name), so two declarations that generate
    different action targets dedupe to one entry with no error.
    """
    rel = "message/schemas/action/actname"
    touch_fbs(case_dir, rel, ["r"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    write(
        os.path.join(case_dir, rel, "fragment.bzl"),
        "ONE = [%s]\nTWO = [%s]\n" % (
            entry(pkg, "r", cpp=True, bfbs=True, action_schema=True, action_name="Alpha"),
            entry(pkg, "r", cpp=True, bfbs=True, action_schema=True, action_name="Beta"),
        ),
    )
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        load(":fragment.bzl", "ONE", "TWO")

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(name = "one", api = 2, items = ONE, module_name = "aorta")

        aorta_schema_fragment(name = "two", api = 2, items = TWO, module_name = "aorta")
        """ % (FRAGMENT_LOAD,),
    )
    return (
        _registry(case_dir, "actname", ["%s:one" % pkg, "%s:two" % pkg]),
        "is declared twice with DIFFERENT metadata",
    )


def red_python_include_dirs_dedupe(case_dir):
    """r2 §A2, second half: `python_include_dirs` changes generation
    (`schema_codegen.bzl:540`) and is likewise absent from the identity."""
    rel = "message/schemas/topic/pyinc"
    touch_fbs(case_dir, rel, ["r"])
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    write(
        os.path.join(case_dir, rel, "fragment.bzl"),
        "ONE = [%s]\nTWO = [%s]\n" % (
            entry(pkg, "r", cpp=True, python=True, bfbs=True, python_include_dirs=["a"]),
            entry(pkg, "r", cpp=True, python=True, bfbs=True, python_include_dirs=["b"]),
        ),
    )
    write(
        os.path.join(case_dir, rel, "BUILD.bazel"),
        """
        %s
        load(":fragment.bzl", "ONE", "TWO")

        package(default_visibility = ["//visibility:public"])

        exports_files(glob(["*.fbs"]))

        aorta_schema_fragment(name = "one", api = 2, items = ONE, module_name = "aorta")

        aorta_schema_fragment(name = "two", api = 2, items = TWO, module_name = "aorta")
        """ % (FRAGMENT_LOAD,),
    )
    return (
        _registry(case_dir, "pyinc", ["%s:one" % pkg, "%s:two" % pkg]),
        "is declared twice with DIFFERENT metadata",
    )


def red_facade_submodule_per_package(case_dir):
    """r2 §A3: the Rust facade submodule key is global, but the facade is not.

    `aorta_schema_package` builds ONE `<name>_rs` facade per package
    (`schema_package.bzl:363`) and re-exports each schema as a submodule. Two
    unrelated packages may therefore both expose a submodule `reading`; the
    composer rejects the legitimate layout. Desired: ACCEPTED.
    """
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/facade%s" % (side, side)
        touch_fbs(case_dir, rel, ["f" + side])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(
                    pkg, "f" + side,
                    alias_id="facade_" + side,
                    cpp=True, python=True, rust=True, bfbs=True,
                    rust_wrapper=True,
                    rust_wrapper_facade_name="reading",
                )],
            ) + ":fragment",
        )
    return _registry(case_dir, "facade", labels), None


def red_cpp_wrapper_header_collision(case_dir):
    """r2 §A3: `cpp_binding_header` is derived and never checked.

    Two packages both emitting `same.h` put an ambiguous header on the C++
    include surface even though their target names differ. Desired: REJECTED.
    """
    labels = []
    for side in ("a", "b"):
        rel = "%s/message/schemas/topic/hdr%s" % (side, side)
        touch_fbs(case_dir, rel, ["h" + side])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(
                    pkg, "h" + side,
                    alias_id="hdr_" + side,
                    cpp=True, python=True, rust=True, bfbs=True,
                    cpp_wrapper=True,
                    cpp_wrapper_header="same.h",
                )],
            ) + ":fragment",
        )
    return _registry(case_dir, "hdr", labels), "header"


def red_provenance_vocabulary(case_dir):
    """r2 §B3: with `expected_provenance = None` nothing checks the item's own
    publication value, so `provenance = "nonsense"` validates clean."""
    return (
        package_case(
            case_dir, "message/schemas/topic/prov", ["p"],
            [entry("//%s/message/schemas/topic/prov" % FIXTURE_ROOT_REL, "p",
                   provenance="nonsense", cpp=True, bfbs=True, cpp_wrapper=True)],
            "prov_case",
        ),
        "provenance",
    )


def red_internal_typed_language_floor(case_dir):
    """r2 §B3: the every-language floor for typed schemas binds only
    `provenance == "external"` (`external_schema_root.bzl:537`), so a caller
    selects its way out of it by writing `internal`.

    Desired for a self-hosted PRODUCT module: the floor decision must not be
    reachable from caller-supplied publication text.
    """
    return (
        package_case(
            case_dir, "message/schemas/topic/floor", ["t"],
            [entry("//%s/message/schemas/topic/floor" % FIXTURE_ROOT_REL, "t",
                   provenance="internal", bfbs=True, cpp=True)],
            "floor_case",
        ),
        "every supported language",
    )


def red_phantom_import_claim(case_dir):
    """r2 §B4: `validate_schema_items` claims `<stem>_import` unconditionally
    although the collapsed macro never defines an import rule, so a legal
    layout (include-only `x`, typed `y` whose C++ wrapper is named
    `x_import`) is refused. Desired: ACCEPTED."""
    pkg = "//%s/message/schemas/topic/phantom" % FIXTURE_ROOT_REL
    return (
        package_case(
            case_dir, "message/schemas/topic/phantom", ["x", "y"],
            [
                entry(pkg, "x"),
                entry(pkg, "y", alias_id="phantom_y", cpp=True, python=True,
                      rust=True, bfbs=True, cpp_wrapper=True,
                      cpp_wrapper_name="x_import"),
            ],
            "phantom_case",
        ),
        None,
    )


def case_nested_logical_path(case_dir):
    """r2 A3(iv): distinct first-segment include keys are legal nested paths."""
    labels = []
    for rel in ("message/schemas/topic/x",
                "outer/schemas/first/message/schemas/topic/x"):
        touch_fbs(case_dir, rel, ["a"])
        pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        labels.append(
            fragment_package(
                case_dir, rel,
                [entry(pkg, "a", alias_id="nested_%d" % len(labels))],
            ) + ":fragment",
        )
    return (
        _registry(case_dir, "nested", labels),
        None,
    )


def case_cross_repo_wrapper_target(case_dir):
    # Override the already declared schema module with a disposable, real module.
    # Both labels have the SAME package and target, but distinct repo identities.
    rel = "same"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["local"])
    local = fragment_package(case_dir, rel, [entry(
        pkg, "local", cpp=True, cpp_wrapper=True, bfbs=True,
        cpp_wrapper_name="binding_cc", cpp_wrapper_header="local.h")]) + ":fragment"
    other = os.path.join(case_dir, "other_module")
    write(os.path.join(other, "MODULE.bazel"),
          'module(name = "aorta_schema_module_fixture", version = "0.0.0")\n'
          'bazel_dep(name = "aorta", version = "0.0.0")\n')
    other_pkg = os.path.join(other, pkg[2:])
    write(os.path.join(other_pkg, "remote.fbs"), "// Metadata fixture.\n")
    write(os.path.join(other_pkg, "fragment.bzl"), "ITEMS = [%s, %s]\n" % (
        entry(pkg, "remote", cpp=True, cpp_wrapper=True, bfbs=True,
              dep_fbs=["//schemas/remote:dep.fbs"],
              cpp_wrapper_name="binding_cc", cpp_wrapper_header="remote.h"),
        entry("//schemas/remote", "dep")))
    write(os.path.join(other, "schemas/remote/dep.fbs"), "// Remote dependency.\n")
    write(os.path.join(other, "schemas/remote/BUILD.bazel"),
          'exports_files(["dep.fbs"], visibility = ["//visibility:public"])\n')
    write(os.path.join(other_pkg, "BUILD.bazel"),
          FRAGMENT_LOAD.replace('"//message', '"@aorta//message') + '\n'
          'load(":fragment.bzl", "ITEMS")\n'
          'package(default_visibility = ["//visibility:public"])\n'
          'exports_files(["remote.fbs"])\n'
          'aorta_schema_fragment(name = "fragment", api = 2, items = ITEMS, '
          'module_name = "other")\n')
    return (_registry(case_dir, "cross_repo", [
        local, "@aorta_schema_module_fixture" + pkg + ":fragment"]), None,
        ["--override_module=aorta_schema_module_fixture=" + other])


def case_local_dependency_without_owner(case_dir):
    rel = "message/schemas/topic/unowned"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["root", "dep"])
    label = fragment_package(case_dir, rel, [entry(
        pkg, "root", dep_fbs=[":dep.fbs"])]) + ":fragment"
    return _registry(case_dir, "unowned", [label]), "dependency has no contributing owner"


def case_dependency_owner_ok(case_dir):
    rel = "message/schemas/topic/owned"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["root", "dep"])
    label = fragment_package(case_dir, rel, [
        entry(pkg, "root", dep_fbs=[":dep.fbs"]), entry(pkg, "dep")]) + ":fragment"
    return _registry(case_dir, "owned", [label]), None


def case_explicit_fragment_api(case_dir):
    rel = "message/schemas/topic/explicitapi"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a"])
    fragment_package(case_dir, rel, [entry(pkg, "a")])
    path = os.path.join(case_dir, rel, "BUILD.bazel")
    with open(path) as handle:
        content = handle.read()
    write(path, content.replace("    api = 2,\n", ""))
    return pkg + ":fragment", "missing 1 required positional argument: api"


def case_policy_vocabulary(case_dir, field="publication"):
    rel = "message/schemas/topic/policy"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    return package_case(case_dir, rel, ["p"], [entry(
        pkg, "p", **{field: "nonsense"})], "policy"), field


def case_codegen_policy_vocabulary(case_dir):
    return case_policy_vocabulary(case_dir, "codegen_policy")


def case_publication_and_policy_ok(case_dir):
    rel = "message/schemas/topic/vocabulary"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    items = [entry(pkg, pub + "_" + policy, publication=pub, codegen_policy=policy)
             for pub in ("public", "fixture", "internal")
             for policy in ("external", "internal")]
    stems = [pub + "_" + policy for pub in ("public", "fixture", "internal")
             for policy in ("external", "internal")]
    return package_case(case_dir, rel, stems, items, "vocabulary"), None


def case_disabled_meta_names_are_free(case_dir):
    rel = "message/schemas/topic/metanames"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    return package_case(case_dir, rel, ["x", "y"], [
        entry(pkg, "x", cpp=True, python=True, rust=True, bfbs=True,
              cpp_lib=False, schema_meta_py=False),
        entry(pkg, "y", cpp=True, python=True, rust=True, bfbs=True,
              cpp_wrapper=True, cpp_wrapper_name="x_schema_meta", cc_alias="x_cc",
              cpp_lib_name="x_cpp_lib")], "metanames"), None


def case_nested_codegen_manifest(case_dir):
    root_type = "Sample" + uuid.uuid4().hex
    prefix = "outer/schemas/first/message/schemas"
    dep_rel, root_rel = prefix + "/common", prefix + "/topic/root"
    dep_pkg = "//%s/%s" % (FIXTURE_ROOT_REL, dep_rel)
    root_pkg = "//%s/%s" % (FIXTURE_ROOT_REL, root_rel)
    for rel, stem, item, source in [
        (dep_rel, "types", entry(dep_pkg, "types", cpp=True, python=True, rust=True),
         "namespace nested.types; table Reading { value:float; }\n"),
        (root_rel, "sample", entry(root_pkg, "sample", cpp=True, python=True, rust=True,
                                  bfbs=True, alias_id="nested_sample", publication="fixture",
                                  dep_fbs=["@missing_module//:ignored.fbs"]),
         'include "schemas/first/message/schemas/common/types.fbs";\n'
         'namespace nested.demo; table %s { reading:nested.types.Reading; } root_type %s;\n' % (root_type, root_type)),
    ]:
        items_load = write_items_bzl(case_dir, rel, [item])
        write(os.path.join(case_dir, rel, stem + ".fbs"), source)
        override = ('dep_fbs = {"sample": ["%s:types.fbs"]},' % dep_pkg) if stem == "sample" else ""
        write(os.path.join(case_dir, rel, "BUILD.bazel"),
              PACKAGE_LOAD + "\n" + items_load + "\n"
              'package(default_visibility = ["//visibility:public"])\n'
              'aorta_schema_package(name = "nested", api = 2, module_name = "aorta", '
              'items = ITEMS, srcs = ["%s.fbs"], %s)\n' % (stem, override))
    target = _registry(case_dir, "nested_codegen", [dep_pkg + ":nested_fragment", root_pkg + ":nested_fragment"])
    # Reading output paths from a provider configures targets but does not run
    # their actions. Request the real artifacts explicitly before inspecting them.
    build = Path(case_dir) / "root/BUILD.bazel"
    build.write_text(build.read_text() + '\nfilegroup(name = "nested_artifacts", srcs = %r)\n' % (
        [":nested_codegen"] + [root_pkg + ":sample_" + suffix for suffix in ("bfbs", "cpp", "rust", "py")]))
    target = "//%s/root:nested_artifacts" % FIXTURE_ROOT_REL

    def check(root):
        data = json.loads((Path(root) / "bazel-bin" / FIXTURE_ROOT_REL / "root/nested_codegen.json").read_text())
        rows = {row["stem"]: row for row in data["entries"]}
        sample = rows["sample"]
        assert data["composed_manifest_version"] == 2
        assert data["verified_tools"]["flatc_version"] == "flatc version 25.12.19"
        assert len(data["verified_tools"]["flatc_sha256"]) == 64
        source = "message/tools/schema_codegen.bzl"
        import hashlib
        assert data["verified_tools"]["codegen_sha256"][source] == hashlib.sha256((Path(root) / source).read_bytes()).hexdigest()
        assert sample["dep_fbs"] == [rows["types"]["fbs_label"]]
        assert sample["publication"] == "fixture"
        assert sample["derived"]["alias_id"] == "nested_sample"
        assert sample["flags"]["bfbs"] is True
        assert sample["action_name"] is None
        assert sample["python_include_dirs"] == ["message"]
        assert sample["naming_overrides"]["alias_id"] == "nested_sample"
        assert sample["logical_include_path"] == "schemas/first/message/schemas/topic/root/sample.fbs"
        assert sample["output_labels"]["bfbs"].endswith(":sample_bfbs")
        assert sample["output_files"]["bfbs"][0].endswith("sample.bfbs")
        for role in ("bfbs", "cpp", "rust", "python"):
            for output in sample["output_files"][role]:
                artifact = Path(root) / output
                assert artifact.exists(), "codegen output was not built: " + output
                if artifact.is_file():
                    assert root_type.encode() in artifact.read_bytes(), "stale codegen output: " + output
        python_tree = Path(root) / sample["output_files"]["python"][0]
        assert (python_tree / "nested/demo" / (root_type + ".py")).is_file()
        assert sample["facade_label"].endswith(":nested_rs")
        assert sample["source_identity"]["path"].endswith("sample.fbs")
        assert sample["dependency_sources"][sample["dep_fbs"][0]] == rows["types"]["source_identity"]
        assert data["modules"][0]["tool_identity"]["flatbuffers_version"] == "25.12.19"
    return target, None, [], check, "build"


def case_missing_output_target(case_dir):
    rel = "message/schemas/topic/nooutput"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a"])
    fragment_package(case_dir, rel, [entry(pkg, "a", bfbs=True)])
    path = Path(case_dir) / rel / "BUILD.bazel"
    path.write_text(path.read_text().replace(FRAGMENT_LOAD,
        'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")'))
    return pkg + ":fragment", "a_bfbs"


def case_flatc_version_mismatch(case_dir):
    rel = "message/schemas/topic/toolversion"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a"])
    label = fragment_package(case_dir, rel, [entry(pkg, "a")]) + ":fragment"
    other = Path(case_dir) / "flatbuffers_module"
    write(str(other / "MODULE.bazel"), 'module(name = "flatbuffers", version = "25.12.19")\n')
    write(str(other / "BUILD.bazel"), 'sh_binary(name = "flatc", srcs = ["flatc.sh"], visibility = ["//visibility:public"])\n')
    write(str(other / "flatc.sh"), '#!/bin/sh\necho "flatc version 0.0.0"\n')
    (other / "flatc.sh").chmod(0o755)
    return (_registry(case_dir, "toolversion", [label]), "FlatBuffers tool version mismatch",
            ["--override_module=flatbuffers=" + str(other)], None, "build")


def case_empty_python_include_dirs(case_dir):
    target, message = red_python_include_dirs_dedupe(case_dir)
    path = Path(case_dir) / "message/schemas/topic/pyinc/fragment.bzl"
    text = path.read_text().replace("python_include_dirs = ['a']", "python_include_dirs = []")
    text = text.replace("python_include_dirs = ['b']", "python_include_dirs = None")
    path.write_text(text)
    return target, message


def case_two_facades_in_one_package(case_dir):
    rel = "message/schemas/topic/twofacades"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a", "b"])
    write(str(Path(case_dir) / rel / "fragment.bzl"), "ONE = [%s]\nTWO = [%s]\n" % (
        entry(pkg, "a", rust=True, rust_wrapper=True, bfbs=True, rust_wrapper_facade_name="reading"),
        entry(pkg, "b", rust=True, rust_wrapper=True, bfbs=True, rust_wrapper_facade_name="reading")))
    write(str(Path(case_dir) / rel / "BUILD.bazel"), FRAGMENT_LOAD + '\n'
          'load(":fragment.bzl", "ONE", "TWO")\n'
          'package(default_visibility = ["//visibility:public"])\n'
          'exports_files(["a.fbs", "b.fbs"])\n'
          'filegroup(name = "first_rs")\nfilegroup(name = "second_rs")\n'
          'aorta_schema_fragment(name = "one", api = 2, module_name = "aorta", items = ONE, facade_targets = {"%s": ":first_rs"})\n'
          'aorta_schema_fragment(name = "two", api = 2, module_name = "aorta", items = TWO, facade_targets = {"%s": ":second_rs"})\n' % (pkg, pkg))
    return _registry(case_dir, "twofacades", [pkg + ":one", pkg + ":two"]), None


def case_dependency_source_mismatch(case_dir):
    rel = "message/schemas/topic/depsource"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["root", "dep", "other"])
    write(str(Path(case_dir) / rel / "fragment.bzl"), "ROOT = [%s]\nDEP = [%s]\n" % (
        entry(pkg, "root", dep_fbs=[":dep.fbs"]), entry(pkg, "dep")))
    write(str(Path(case_dir) / rel / "BUILD.bazel"), FRAGMENT_LOAD + '\n'
          'load(":fragment.bzl", "ROOT", "DEP")\n'
          'package(default_visibility = ["//visibility:public"])\n'
          'exports_files(["root.fbs", "dep.fbs", "other.fbs"])\n'
          'aorta_schema_fragment(name = "root", api = 2, module_name = "aorta", items = ROOT)\n'
          'aorta_schema_fragment(name = "dep", api = 2, module_name = "aorta", items = DEP, srcs = {":other.fbs": "%s|dep"})\n' % pkg)
    return _registry(case_dir, "depsource", [pkg + ":root", pkg + ":dep"]), "AORTA_SOURCE_FILE_MISMATCH"


def case_python_assembler_duplicate(case_dir):
    labels = []
    for side in ("left", "right"):
        rel = "metas/" + side
        write(str(Path(case_dir) / rel / "duplicate_schema_meta.py"), side.upper() + " = True\n")
        write(str(Path(case_dir) / rel / "BUILD.bazel"),
              'exports_files(["duplicate_schema_meta.py"], visibility = ["//visibility:public"])\n')
        labels.append("//%s/%s:duplicate_schema_meta.py" % (FIXTURE_ROOT_REL, rel))
    write(str(Path(case_dir) / "root/BUILD.bazel"),
          'load("//message/tools:flatbuffers_rules.bzl", "python_msgs_tree")\n'
          'python_msgs_tree(name = "tree", metas = %r)\n' % labels)
    return "//%s/root:tree" % FIXTURE_ROOT_REL, "duplicate schema-meta install basename"


def case_python_assembler_layout(case_dir):
    labels = []
    expected = {}
    for pkg, stem, value in [("service/fsm", "function_input", "FSM"),
                             ("service/rcp", "function_input", "RCP"),
                             ("topic/rcp", "trace_event", "RCP_TRACE"),
                             ("topic/user_interaction", "trace_event", "UI_TRACE")]:
        rel = "message/schemas/" + pkg
        module = stem + "_schema_meta.py"
        content = value + " = True\n"
        expected[pkg.replace("/", "_") + "_" + module] = content
        write(str(Path(case_dir) / rel / module), content)
        write(str(Path(case_dir) / rel / "BUILD.bazel"),
              'exports_files(["%s"], visibility = ["//visibility:public"])\n' % module)
        labels.append("//%s/%s:%s" % (FIXTURE_ROOT_REL, rel, module))
    write(str(Path(case_dir) / "root/BUILD.bazel"),
          'load("//message/tools:flatbuffers_rules.bzl", "python_msgs_tree")\n'
          'python_msgs_tree(name = "tree", metas = %r)\n' % labels)

    def check(root):
        out = Path(root) / "bazel-bin" / FIXTURE_ROOT_REL / "root/tree"
        assert {p.name: p.read_text() for p in out.glob("*.py")} == expected, "the four owners must retain distinct installed files"
    return "//%s/root:tree" % FIXTURE_ROOT_REL, None, [], check, "build"


def case_python_meta_layout_composes(case_dir):
    labels = []
    for pkg, stem in [("service/fsm", "function_input"), ("service/rcp", "function_input"),
                      ("topic/rcp", "trace_event"), ("topic/user_interaction", "trace_event")]:
        rel = "message/schemas/" + pkg
        pkg_label = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
        touch_fbs(case_dir, rel, [stem])
        labels.append(fragment_package(case_dir, rel, [entry(
            pkg_label, stem, python=True, bfbs=True,
            alias_id=pkg.replace("/", "_") + "_" + stem)]) + ":fragment")
    return _registry(case_dir, "metalayout", labels), None


def case_aliased_outputs(case_dir):
    rel = "message/schemas/topic/aliased"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a"])
    write(str(Path(case_dir) / rel / "facade.rs"), "// Aliased facade fixture.\n")
    write(str(Path(case_dir) / rel / "schema.bfbs"), "Aliased BFBS fixture.\n")
    items_load = write_items_bzl(case_dir, rel, [entry(pkg, "a", bfbs=True)])
    write(str(Path(case_dir) / rel / "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n' + items_load + '\n'
          'package(default_visibility = ["//visibility:public"])\n'
          'exports_files(["a.fbs"])\n'
          'filegroup(name = "real_facade", srcs = ["facade.rs"])\n'
          'alias(name = "public_facade", actual = ":real_facade")\n'
          'filegroup(name = "real_bfbs", srcs = ["schema.bfbs"])\n'
          'alias(name = "a_bfbs", actual = ":real_bfbs")\n'
          'aorta_schema_fragment(name = "fragment", api = 2, module_name = "aorta", items = ITEMS, '
          'facade_targets = {"%s": ":public_facade"})\n' % pkg)
    target = _registry(case_dir, "aliased", [pkg + ":fragment"])

    def check(root):
        data = json.loads((Path(root) / "bazel-bin" / FIXTURE_ROOT_REL / "root/aliased.json").read_text())
        row, = data["entries"]
        assert row["output_labels"]["rust_facade"].endswith(":public_facade")
        assert row["facade_label"].endswith(":real_facade"), row["facade_label"]
        assert row["output_labels"]["bfbs"].endswith(":a_bfbs")
        for role, filename in [("rust_facade", "facade.rs"), ("bfbs", "schema.bfbs")]:
            expected = FIXTURE_ROOT_REL + "/" + rel + "/" + filename
            assert row["output_files"][role] == [expected], row["output_files"]
            assert (Path(root) / expected).is_file()
    return target, None, [], check, "build"


def case_aliased_facade_collision(case_dir):
    target, _ = case_two_facades_in_one_package(case_dir)
    path = Path(case_dir) / "message/schemas/topic/twofacades/BUILD.bazel"
    path.write_text(path.read_text().replace(
        'filegroup(name = "first_rs")\nfilegroup(name = "second_rs")',
        'filegroup(name = "real_facade")\n'
        'alias(name = "first_rs", actual = ":real_facade")\n'
        'alias(name = "second_rs", actual = ":real_facade")'))
    return target, "Rust facade module name"


def case_aliased_binding_collision(case_dir):
    target, message = case_binding_target_collision(case_dir)
    pkg_dir = Path(case_dir) / "message/schemas/topic/binding"
    for index in range(2):
        path = pkg_dir / ("fragment%d.bzl" % index)
        path.write_text(path.read_text().replace("one_binding_cc", "binding%d_cc" % index))
    path = pkg_dir / "BUILD.bazel"
    path.write_text(path.read_text().replace(
        'aorta_schema_fragment(name = "frag0"',
        'filegroup(name = "real_binding")\n'
        'alias(name = "binding0_cc", actual = ":real_binding")\n'
        'alias(name = "binding1_cc", actual = ":real_binding")\n'
        'aorta_schema_fragment(name = "frag0"'))
    return target, message


NEGATIVE_CASES = [
    ("ownership: aliases share the actual facade", case_aliased_facade_collision),
    ("ownership: aliases share the actual binding", case_aliased_binding_collision),
    ("#374 duplicate basename in actual assembler", case_python_assembler_duplicate),
    ("ownership: dependency source matches owner", case_dependency_source_mismatch),
    ("identity: empty Python includes differ from default", case_empty_python_include_dirs),
    ("B3  'internal' skips typed-language floor", red_internal_typed_language_floor),
    ("B3  provenance vocabulary unchecked", red_provenance_vocabulary),
    ("A1  dep_fbs names an absent module", red_dep_fbs_unresolved),
    ("contract: codegen policy vocabulary", case_codegen_policy_vocabulary),
    ("contract: publication vocabulary", case_policy_vocabulary),
    ("contract: explicit fragment API", case_explicit_fragment_api),
    ("contract: local dependency without owner", case_local_dependency_without_owner),
    ("analysis: output reference must exist", case_missing_output_target),
    ("actions: actual flatc version must match", case_flatc_version_mismatch),
    ("A3  cpp_wrapper_header collision unchecked", red_cpp_wrapper_header_collision),
    ("A2  python_include_dirs difference deduped", red_python_include_dirs_dedupe),
    ("A2  action_name difference deduped", red_action_name_dedupe),
    ("loading: api mismatch (package macro)", case_api_mismatch),
    ("loading: entry pkg missing '//' prefix", case_pkg_prefix),
    ("loading: srcs<->items bijection", case_srcs_bijection),
    ("loading: duplicate stem in one package", case_duplicate_stem),
    ("loading: api mismatch (fragment macro)", case_fragment_api_mismatch),
    ("analysis: logical include path collision", case_logical_path_collision),
    ("analysis: derived schema alias collision", case_alias_and_crate_collision),
    ("analysis: Rust crate name collision", case_rust_crate_collision),
    ("analysis: Python schema-meta module collision", case_python_module_collision),
    ("analysis: conflicting redeclaration", case_conflicting_redeclaration),
    ("analysis: message binding target collision", case_binding_target_collision),
]

def case_collection_members(case_dir, reverse=False):
    rel = "message/schemas/topic/collections"
    pkg = "//%s/%s" % (FIXTURE_ROOT_REL, rel)
    touch_fbs(case_dir, rel, ["a", "b", "shared"])
    write(str(Path(case_dir) / rel / "fragment.bzl"), "ONE = [%s, %s]\nTWO = [%s, %s]\n" % (
        entry(pkg, "a"), entry(pkg, "shared"), entry(pkg, "b"), entry(pkg, "shared")))
    write(str(Path(case_dir) / rel / "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n'
          'load(":fragment.bzl", "ONE", "TWO")\n'
          'package(default_visibility = ["//visibility:public"])\n'
          'exports_files(["a.fbs", "b.fbs", "shared.fbs"])\n'
          'aorta_schema_fragment(name = "one", api = 2, module_name = "collections_owner", items = ONE)\n'
          'aorta_schema_fragment(name = "two", api = 2, module_name = "collections_owner", items = TWO)\n')
    fragments = [pkg + ":one", pkg + ":two"]
    if reverse:
        fragments.reverse()
    target = _registry(case_dir, "collections", fragments)

    def check(root):
        data = json.loads((Path(root) / "bazel-bin" / FIXTURE_ROOT_REL / "root/collections.json").read_text())
        assert len(data["entries"]) == 3
        assert len(data["modules"]) == 1
        collection = data["modules"][0]
        expected = sorted(row["fbs_label"] for row in data["entries"])
        assert collection["entries"] == expected, collection
        assert collection["entry_count"] == 3, collection
        assert collection["collection_name"] == "collections_owner"
    return target, None, [], check, "build"


def case_collection_members_reverse(case_dir):
    return case_collection_members(case_dir, reverse=True)


def case_package_module_name(case_dir):
    def check(root):
        manifest = Path(root) / "bazel-bin/message/tools/tests/schema_package_probe/composed_registry.json"
        data = json.loads(manifest.read_text())
        probe = [row for row in data["entries"]
                 if row["pkg"] == "//message/tools/tests/schema_package_probe"]
        assert len(probe) == 2
        assert all(row["origin_module"] == "aorta" for row in probe), probe
        collection = [row for row in data["modules"] if row["collection_name"] == "probe_msgs"]
        assert len(collection) == 1
        assert collection[0]["origin_module"] == "aorta", collection
        assert set(collection[0]["entries"]) == {row["fbs_label"] for row in probe}
        assert collection[0]["entry_count"] == 2
    return "//message/tools/tests/schema_package_probe:composed_registry", None, [], check, "build"


POSITIVE_CASES = [
    ("outputs: aliased facade and BFBS files", case_aliased_outputs),
    ("collections: merge and dedupe shared members", case_collection_members),
    ("collections: reversed fragment order", case_collection_members_reverse),
    ("identity: package preserves declared module name", case_package_module_name),
    ("#374 composer agrees with installed layout", case_python_meta_layout_composes),
    ("#374 four distinct installed metadata modules", case_python_assembler_layout),
    ("scope: two real facades in one package", case_two_facades_in_one_package),
    ("B4  phantom <stem>_import claim", red_phantom_import_claim),
    ("surface: disabled metadata and C++ names", case_disabled_meta_names_are_free),
    ("actions: nested codegen and effective manifest", case_nested_codegen_manifest),
    ("A3  first schemas segment distinguishes nested path", case_nested_logical_path),
    ("contract: dependency has a contributing owner", case_dependency_owner_ok),
    ("contract: publication and policy vocabulary", case_publication_and_policy_ok),
    ("analysis: real cross-repository wrapper labels", case_cross_repo_wrapper_target),
    ("A3  per-package facade submodule 'reading'", red_facade_submodule_per_package),
    ("analysis: diamond dedupe", case_diamond_ok),
]

def case_test_execution_refusal(case_dir):
    write(os.path.join(case_dir, "BUILD.bazel"),
          'sh_test(name = "execution_refusal", srcs = ["refusal.sh"])\n')
    write(os.path.join(case_dir, "refusal.sh"),
          '#!/bin/sh\necho "AORTA_TEST_MODE_REFUSAL probe.fbs:publication"\nexit 1\n')
    os.chmod(os.path.join(case_dir, "refusal.sh"), 0o755)
    return ("//%s:execution_refusal" % FIXTURE_ROOT_REL,
            "AORTA_TEST_MODE_REFUSAL probe.fbs:publication", [], None, "test")


NEGATIVE_CASES.append(("mode: observe execution-time refusal", case_test_execution_refusal))

# (label, builder, desired outcome). These do NOT hold today.
def case_l4_provider_evidence(case_dir, generated=False):
    pkg = "//" + FIXTURE_ROOT_REL
    write(os.path.join(case_dir, "sample.fbs"), "namespace probe; table Sample { value:int; }\n")
    items_load = write_items_bzl(case_dir, "", [entry(pkg, "sample", publication="public")])
    write(os.path.join(case_dir, "evidence.bzl"), '''
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo")
def _impl(ctx):
    info = ctx.attr.fragment[AortaSchemaFragmentInfo]
    audit = getattr(info, "lint_declaration", None)
    valid = audit != None and len(info.fragment_files) > 0
    if valid:
        original = audit["declarations"][0]["original"]
        valid = original.get("publication") == "public" and "cpp" not in original
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, "#!/bin/sh\\necho AORTA_L4_PROVIDER_EVIDENCE\\nexit " + ("0" if valid else "1") + "\\n", is_executable = True)
    return [DefaultInfo(executable = script)]
evidence_test = rule(implementation = _impl, test = True, attrs = {"fragment": attr.label()})
''')
    write(os.path.join(case_dir, "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n' + items_load + '\n'
          'load(":evidence.bzl", "evidence_test")\n'
          'aorta_schema_fragment(name="fragment", api=2, module_name="l4_owner", items=ITEMS' +
          ('' if generated else ', fragment=":fragment.bzl", fragment_symbols={":fragment.bzl": "ITEMS"}') + ')\n'
          'evidence_test(name="evidence_test", fragment=":fragment")\n')
    return pkg + ":evidence_test", None, [], None, "test"


def case_l4_generated_evidence(case_dir):
    return case_l4_provider_evidence(case_dir, generated=True)


def case_q12_source_override(case_dir, missing=False):
    pkg = "//" + FIXTURE_ROOT_REL
    items_load = write_items_bzl(case_dir, "", [entry(pkg, "sample")])
    write(os.path.join(case_dir, "real.fbs"), "namespace real; table Real {}\n")
    if not missing:
        write(os.path.join(case_dir, "sample.fbs"), "namespace other; table Other {}\n")
    write(os.path.join(case_dir, "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n' + items_load + '\n'
          'aorta_schema_fragment(name="fragment", api=2, module_name="q12_owner", items=ITEMS, '
          'srcs={":real.fbs": "' + pkg + '|sample"})\n')
    return pkg + ":fragment", "sample.fbs" if missing else "AORTA_SOURCE_FILE_MISMATCH"


def case_q12_virtual_source(case_dir):
    return case_q12_source_override(case_dir, missing=True)


POSITIVE_CASES.extend([
    ("L4: authored original provider evidence", case_l4_provider_evidence),
    ("L4: generated original provider evidence", case_l4_generated_evidence),
])
NEGATIVE_CASES.extend([
    ("Q12: override selects a different File", case_q12_source_override),
    ("Q12: virtual source label", case_q12_virtual_source),
])


def case_l4_text(case_dir, change=None):
    case_l4_provider_evidence(case_dir)
    path = Path(case_dir) / "fragment.bzl"
    text = path.read_text()
    if change == "publication":
        text = text.replace("publication = 'public'", "publication = 'internal'")
    elif change in ["cpp", "action_name", "cc_alias", "dep_fbs"]:
        value = {"cpp": "True", "action_name": "'Other'", "cc_alias": "None", "dep_fbs": "[':other.fbs']"}[change]
        text = text.replace("struct(", "struct(" + change + " = " + value + ", ")
    elif change == "missing":
        text = "ITEMS = []\n"
    elif change == "extra":
        row = text[text.index("struct("):text.index(")", text.index("struct(")) + 1]
        text = text.replace("\n]", "\n    " + row.replace("sample", "z") + ",\n]")
    elif change == "symbol":
        text = text.replace("ITEMS =", "WRONG =")
    elif change == "computed":
        text += "load(\":other.bzl\", \"X\")\n"
    # Load evaluated values from the untouched file, audit a separately supplied File.
    (Path(case_dir) / "audited.bzl").write_text(text)
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text(build.read_text().replace('fragment=":fragment.bzl", fragment_symbols={":fragment.bzl": "ITEMS"}',
                                              'fragment=":audited.bzl", fragment_symbols={":audited.bzl": "ITEMS"}'))
    message = None
    if change:
        message = "AORTA_L4_SYNTAX" if change in ["symbol", "computed"] else "AORTA_L4_COVERAGE" if change in ["missing", "extra"] else "AORTA_L4_MISMATCH sample.fbs:" + change
    return "//" + FIXTURE_ROOT_REL + ":fragment_fragment_lint_test", message, [], None, "test"


def case_l4_generated_mutation(case_dir):
    case_l4_provider_evidence(case_dir, generated=True)
    write(os.path.join(case_dir, "corrupt.bzl"), '''
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo")
def _impl(ctx):
    info = ctx.attr.fragment[AortaSchemaFragmentInfo]
    source = info.fragment_files[0]
    corrupt = ctx.actions.declare_file("corrupt.declaration.bzl")
    ctx.actions.run_shell(inputs = [source], outputs = [corrupt], arguments = [source.path, corrupt.path], command = "sed 's/public/internal/g; s/internalation/publication/g' \\\"$1\\\" > \\\"$2\\\"")
    audit = dict(info.lint_declaration)
    audit["fragments"] = [dict(audit["fragments"][0], file = corrupt.path)]
    audit["entries"] = info.entries
    audit["files"] = {corrupt.path: ctx.workspace_name + "/" + corrupt.short_path}
    out = ctx.actions.declare_file("corrupt.json")
    ctx.actions.write(out, json.encode(audit))
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out, corrupt]))]
corrupt_inputs = rule(implementation = _impl, attrs = {"fragment": attr.label()})
''')
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text('load(":corrupt.bzl", "corrupt_inputs")\n'
                     'load("//message/tools:schema_lint.bzl", "schema_fragment_lint_test")\n' + build.read_text() +
                     '\ncorrupt_inputs(name="corrupt", fragment=":fragment")\n'
                     'schema_fragment_lint_test(name="corruption_test", audit=":corrupt")\n')
    return "//" + FIXTURE_ROOT_REL + ":corruption_test", "AORTA_L4_MISMATCH sample.fbs:publication", [], None, "test"


def case_q12_same_file_alias(case_dir):
    case_q12_source_override(case_dir, missing=True)
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text(build.read_text() + '\nalias(name="sample.fbs", actual=":real.fbs")\n')
    return "//" + FIXTURE_ROOT_REL + ":fragment_fragment_lint_test", None, [], None, "test"


for _field in ["publication", "cpp", "action_name", "cc_alias", "dep_fbs", "missing", "extra", "symbol", "computed"]:
    NEGATIVE_CASES.append(("L4: authored " + _field, lambda case_dir, field=_field: case_l4_text(case_dir, field)))
NEGATIVE_CASES.append(("L4: corrupt actual generated File", case_l4_generated_mutation))
POSITIVE_CASES.extend([
    ("L4: authored agreement", case_l4_text),
    ("Q12: same File source alias", case_q12_same_file_alias),
])


def case_l4_external_mapping(case_dir):
    case_cross_repo_wrapper_target(case_dir)
    other = Path(case_dir) / "other_module"
    pkg = FIXTURE_ROOT_REL + "/same"
    build = other / pkg / "BUILD.bazel"
    build.write_text(build.read_text().replace('module_name = "other")',
        'module_name = "other", fragment=":fragment.bzl", fragment_symbols={":fragment.bzl": "ITEMS"})'))
    return ("@aorta_schema_module_fixture//" + pkg + ":fragment_fragment_lint_test", None,
            ["--override_module=aorta_schema_module_fixture=" + str(other)], None, "test")


def case_l4_override(case_dir):
    case_l4_provider_evidence(case_dir)
    build = Path(case_dir) / "BUILD.bazel"
    original = entry("//" + FIXTURE_ROOT_REL, "sample", publication="public", python_include_dirs=[])
    effective = entry("//" + FIXTURE_ROOT_REL, "sample", publication="public", python_include_dirs=[], dep_fbs=[":dep.fbs"])
    write(os.path.join(case_dir, "fragment.bzl"), "ITEMS = [" + original + "]\nEFFECTIVE = [" + effective + "]\n")
    # The audited pure-data File has one symbol; the evaluated values may be assembled separately.
    write(os.path.join(case_dir, "audited.bzl"), "ITEMS = [" + original + "]\n")
    write(os.path.join(case_dir, "dep.fbs"), "namespace dep; table Dep {}\n")
    build.write_text(build.read_text().replace('load(":fragment.bzl", "ITEMS")', 'load(":fragment.bzl", "ITEMS", "EFFECTIVE")')
        .replace('items=ITEMS, fragment=":fragment.bzl", fragment_symbols={":fragment.bzl": "ITEMS"}',
                 'items=EFFECTIVE, original_items=ITEMS, dep_fbs_overrides={"sample": [":dep.fbs"]}, fragment=":audited.bzl", fragment_symbols={":audited.bzl": "ITEMS"}'))
    return "//" + FIXTURE_ROOT_REL + ":fragment_fragment_lint_test", None, [], None, "test"


def case_l4_label_spellings(case_dir):
    case_l4_override(case_dir)
    fragment = Path(case_dir) / "fragment.bzl"
    original = entry("//" + FIXTURE_ROOT_REL, "sample", publication="public", dep_fbs=[":dep.fbs"], python_include_dirs=[])
    fragment.write_text("ITEMS = [" + original + "]\nEFFECTIVE = ITEMS\n")
    (Path(case_dir) / "audited.bzl").write_text("ITEMS = [" + original.replace(":dep.fbs", "//" + FIXTURE_ROOT_REL + ":dep.fbs") + "]\n")
    return "//" + FIXTURE_ROOT_REL + ":fragment_fragment_lint_test", None, [], None, "test"


POSITIVE_CASES.extend([
    ("L4: real external module mapping", case_l4_external_mapping),
    ("L4: recorded override and empty includes", case_l4_override),
    ("L4: equivalent label spellings", case_l4_label_spellings),
])


CENSUS_MODULE_LOAD = 'load("//message/tools:schema_lint.bzl", "aorta_schema_module_lint_test")'


def census_module_build(case_dir, fragments, census, dependencies=None):
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text(CENSUS_MODULE_LOAD + "\n" + build.read_text() +
        '\naorta_schema_module_lint_test(name="module_lint_test", fragments=%r, census=%r, dependency_fragments=%r)\n' %
        (fragments, census, dependencies or []))


def case_census_package(case_dir, spelling="string", orphan=True):
    pkg = "//" + FIXTURE_ROOT_REL
    load = write_items_bzl(case_dir, "", [entry(pkg, "sample")])
    touch_fbs(case_dir, "", ["sample"] + (["orphan"] if orphan else []))
    if spelling == "label":
        write(os.path.join(case_dir, "labels.bzl"), 'SELECTED = [Label(":sample.fbs")]\n')
        load += '\nload(":labels.bzl", "SELECTED")'
    srcs = {'string': '["sample.fbs"]', 'label': 'SELECTED',
            'filegroup': '[":selected"]'}[spelling]
    write(os.path.join(case_dir, "BUILD.bazel"), PACKAGE_LOAD + '\n' + load + '\n'
          'filegroup(name="selected", srcs=["sample.fbs"])\n'
          'aorta_schema_package(name="sample_pkg", api=2, module_name="aorta", items=ITEMS, srcs=' + srcs + ')\n')
    return pkg + ":sample_pkg_lint_test", "AORTA_CENSUS_UNREGISTERED" if orphan else None, [], None, "test"


def case_census_module(case_dir, mutation=None):
    case_l4_provider_evidence(case_dir, generated=True)
    build = Path(case_dir) / "BUILD.bazel"
    fragments = [":fragment"]
    census = [":sample.fbs"]
    text = build.read_text()
    if mutation in ["orphan", "omitted_package"]:
        rel = "child/orphan.fbs" if mutation == "omitted_package" else "orphan.fbs"
        write(os.path.join(case_dir, rel), "namespace census; table Orphan {}\n")
        if mutation == "omitted_package":
            write(os.path.join(case_dir, "child/BUILD.bazel"), 'exports_files(["orphan.fbs"])\n')
        census.append(":orphan.fbs" if mutation == "orphan" else "//" + FIXTURE_ROOT_REL + "/child:orphan.fbs")
    elif mutation == "missing":
        census = []
    elif mutation in ["duplicate", "diamond", "conflicting"]:
        if mutation == "duplicate":
            original = entry("//" + FIXTURE_ROOT_REL, "other", publication="public")
            text += '\nalias(name="other.fbs", actual=":sample.fbs")\n'
        else:
            original = entry("//" + FIXTURE_ROOT_REL, "sample", publication="internal" if mutation == "conflicting" else "public")
        write(os.path.join(case_dir, "other.bzl"), "OTHER = [" + original + "]\n")
        text = 'load(":other.bzl", "OTHER")\n' + text
        text += '\naorta_schema_fragment(name="other", api=2, module_name="l4_owner", items=OTHER)\n'
        fragments.append(":other")
    elif mutation == "generated":
        (Path(case_dir) / "sample.fbs").unlink()
        text += '\ngenrule(name="source", outs=["sample.fbs"], cmd="echo generated > $@")\n'
    build.write_text(text)
    census_module_build(case_dir, fragments, census)
    diagnostic = {"orphan": "UNREGISTERED", "omitted_package": "UNREGISTERED", "missing": "MISSING",
                  "duplicate": "DUPLICATE", "conflicting": "DUPLICATE", "generated": "GENERATED"}.get(mutation)
    return "//" + FIXTURE_ROOT_REL + ":module_lint_test", "AORTA_CENSUS_" + diagnostic if diagnostic else None, [], None, "test"


def case_census_nested(case_dir, mode="unregistered"):
    case_census_package(case_dir, orphan=False)
    child = Path(case_dir) / "child"
    child.mkdir()
    pkg = "//" + FIXTURE_ROOT_REL
    write_items_bzl(case_dir, "child", [entry(pkg + "/child", "nested")])
    touch_fbs(case_dir, "child", ["nested"])
    if mode == "missing_target":
        (child / "BUILD.bazel").write_text('exports_files(["nested.fbs"])\n')
    elif mode in ["deep_directory", "deep_orphan"]:
        (child / "BUILD.bazel").write_text(PACKAGE_LOAD + '\nload(":fragment.bzl", "ITEMS")\n'
            'aorta_schema_package(name="child", api=2, module_name="aorta", items=ITEMS, srcs=["nested.fbs"], visibility=["//visibility:public"])\n')
        write(os.path.join(child, "deep/orphan.fbs"), "namespace deep; table Orphan {}\n")
    else:
        (child / "BUILD.bazel").write_text(PACKAGE_LOAD + '\nload(":fragment.bzl", "ITEMS")\n'
            'aorta_schema_package(name="child", api=2, module_name="aorta", items=ITEMS, srcs=["nested.fbs"], visibility=["//visibility:public"])\n')
    if mode == "missing_target":
        return pkg + ":all_source_fbs", "all_source_fbs", [], None, "analysis"
    fragments = [":sample_pkg_fragment"]
    if mode in ["registered", "deep_directory", "deep_orphan"]:
        fragments.append(pkg + "/child:child_fragment")
    census_module_build(case_dir, fragments, [":all_source_fbs"])
    # Also prove the existing recursive census actually exports deep files.
    if mode == "deep_directory":
        def inspect(root):
            data = subprocess.check_output(["bazel", "cquery", "--lockfile_mode=off", pkg + ":all_source_fbs", "--output=files"], cwd=root, text=True)
            assert FIXTURE_ROOT_REL + "/child/deep/orphan.fbs" in data, "recursive census omitted deep/orphan.fbs"
        return pkg + ":all_source_fbs", None, [], inspect, "build"
    return pkg + ":module_lint_test", None if mode == "registered" else "AORTA_CENSUS_UNREGISTERED", [], None, "test"


B2_REFUSALS = [
    ("census: orphan omitted from string srcs/items", lambda d: case_census_package(d)),
    ("census: orphan omitted from Label srcs/items", lambda d: case_census_package(d, "label")),
    ("census: orphan omitted from filegroup srcs/items", lambda d: case_census_package(d, "filegroup")),
] + [("census: module " + m, lambda d, m=m: case_census_module(d, m))
     for m in ["orphan", "omitted_package", "missing", "duplicate", "conflicting", "generated"]] + [
    ("census: unregistered nested package", case_census_nested),
    ("census: deep orphan reaches module lint", lambda d: case_census_nested(d, "deep_orphan")),
]
B2_CONTROLS = [
    ("census: complete package", lambda d: case_census_package(d, orphan=False)),
    ("census: complete module", case_census_module),
    ("census: exact declaration diamond", lambda d: case_census_module(d, "diamond")),
    ("census: registered nested package", lambda d: case_census_nested(d, "registered")),
]


def case_census_external(case_dir, mode="dependency"):
    case_l4_provider_evidence(case_dir, generated=True)
    other = Path(case_dir) / "other_module"
    write(str(other / "MODULE.bazel"), 'module(name="aorta_schema_module_fixture", version="0.0.0")\nbazel_dep(name="aorta", version="0.0.0")\n')
    # Ownership refusal fixtures keep identical paths across repositories.
    rel = FIXTURE_ROOT_REL
    remote_rel = rel + "/remote" if mode == "dependency" else rel
    remote_file = "@aorta_schema_module_fixture//" + remote_rel + ":sample.fbs"
    remote_fragment = "@aorta_schema_module_fixture//" + remote_rel + ":remote"
    write(str(other / remote_rel / "sample.fbs"), "namespace foreign; table Sample {}\n")
    write(str(other / remote_rel / "fragment.bzl"), "ITEMS = [" + entry("//" + remote_rel, "sample") + "]\n")
    write(str(other / remote_rel / "BUILD.bazel"),
          'load("@aorta//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n'
          'load(":fragment.bzl", "ITEMS")\npackage(default_visibility=["//visibility:public"])\n'
          'exports_files(["sample.fbs"])\naorta_schema_fragment(name="remote", api=2, module_name="remote", items=ITEMS)\n')
    build = Path(case_dir) / "BUILD.bazel"
    fragments, census, dependencies = [":fragment"], [":sample.fbs"], [remote_fragment]
    if mode == "foreign_census":
        census.append(remote_file)
    elif mode == "foreign_fragment":
        fragments.append(remote_fragment)
    elif mode == "foreign_source":
        (Path(case_dir) / "sample.fbs").unlink()
        build.write_text(build.read_text() + '\nalias(name="sample.fbs", actual="' + remote_file + '")\n')
    else:
        f = Path(case_dir) / "fragment.bzl"
        f.write_text("ITEMS = [" + entry("//" + rel, "sample", publication="public", dep_fbs=[remote_file]) + "]\n")
    if mode == "dependency":
        (Path(case_dir) / "sample.fbs").write_text('include "' + remote_rel + '/sample.fbs";\nnamespace local; table Sample {}\n')
    if mode == "self_hosted":
        remote_build = other / remote_rel / "BUILD.bazel"
        remote_build.write_text('load("@aorta//message/tools:schema_lint.bzl", "aorta_schema_module_lint_test")\n' + remote_build.read_text() +
            '\naorta_schema_module_lint_test(name="module_lint_test", fragments=[":remote"], census=glob(["**/*.fbs"]))\n')
        return ("@aorta_schema_module_fixture//" + rel + ":module_lint_test", None,
                ["--override_module=aorta_schema_module_fixture=" + str(other)], None, "test")
    census_module_build(case_dir, fragments, census, dependencies)
    return ("//" + rel + ":module_lint_test", None if mode == "dependency" else "AORTA_CENSUS_FOREIGN",
            ["--override_module=aorta_schema_module_fixture=" + str(other)], None, "test")


B2_REFUSALS.extend(("census: " + mode, lambda d, m=mode: case_census_external(d, m))
                   for mode in ["foreign_census", "foreign_fragment", "foreign_source"])
B2_CONTROLS.extend([
    ("census: external dependency excluded", case_census_external),
    ("census: external module self audit", lambda d: case_census_external(d, "self_hosted")),
])

NEGATIVE_CASES.append(("census: nested missing recursive target", lambda d: case_census_nested(d, "missing_target")))
POSITIVE_CASES.extend(B2_CONTROLS)

NEGATIVE_CASES.extend(B2_REFUSALS)
POSITIVE_CASES.append(("census: deep files exported by recursive census", lambda d: case_census_nested(d, "deep_directory")))


def case_census_release_graph(case_dir):
    def inspect(root):
        graph = subprocess.check_output([
            "bazel", "query", "--lockfile_mode=off",
            'deps(filter("^//message:canonical_schema_source_", //message:*))',
        ], cwd=root, text=True).splitlines()
        assert any(label.startswith("//message:canonical_schema_source_") for label in graph)
        assert "//message:schema_source_files" not in graph, "release graph includes the physical census"
        assert not any(label.endswith(":all_source_fbs") or "_census_lint_test" in label for label in graph), "release graph includes census-only inputs"
    return "//message:schema_source_files", None, [], inspect, "build"


POSITIVE_CASES.append(("census: excluded from canonical release graph", case_census_release_graph))



def case_census_source_alias(case_dir):
    case_q12_same_file_alias(case_dir)
    census_module_build(case_dir, [":fragment"], [":real.fbs"])
    return "//" + FIXTURE_ROOT_REL + ":module_lint_test", None, [], None, "test"


POSITIVE_CASES.append(("census: same File source alias", case_census_source_alias))



def case_include_edges(case_dir, mode="bare"):
    root = "//" + FIXTURE_ROOT_REL
    rel = "schemas/demo" if mode in ["rooted", "undeclared", "unused", "unknown", "escaping"] else ""
    if mode in ["nested_key", "last_schemas"]:
        rel = "outer/schemas/first/message/schemas/demo"
    if mode == "no_schemas":
        rel = "custom/types"
    pkg = root + ("/" + rel if rel else "")
    key = ("schemas/first/message/schemas/demo/types.fbs" if "outer/" in rel else
           "schemas/demo/types.fbs" if rel.startswith("schemas/") else
           FIXTURE_ROOT_REL + "/custom/types/types.fbs" if rel else "types.fbs")
    include = key
    deps = [":types.fbs"]
    if mode in ["undeclared", "unknown", "comment"]:
        deps = []
    if mode == "unknown":
        include = "schemas/unknown/types.fbs"
    elif mode == "escaping":
        include = "../demo/types.fbs"
    elif mode == "last_schemas":
        include = "schemas/demo/types.fbs"
    text = 'include "' + include + '";\nnamespace include_demo; table Root { value:int; }\n'
    if mode == "unused":
        text = "namespace include_demo; table Root {}\n"
    elif mode == "comment":
        text = '// include "ghost.fbs";\n/* include "missing.fbs"; */\nnamespace include_demo; table Root {}\n'
    elif mode == "repeated":
        text = 'include "types.fbs";\n' + text
    elif mode in ["transitive", "direct_graph", "grandchild_only"]:
        text = 'include "branch.fbs";\ninclude "types.fbs";\nnamespace include_demo; table Root {}\n'
        deps = [":branch.fbs"] if mode == "transitive" else [":branch.fbs", ":types.fbs"]
        if mode == "grandchild_only":
            deps = [":types.fbs"]
    write(os.path.join(case_dir, rel, "root.fbs"), text)
    write(os.path.join(case_dir, rel, "types.fbs"), "namespace include_demo; table Types {}\n")
    items = [entry(pkg, "root", dep_fbs=deps), entry(pkg, "types")]
    if mode in ["transitive", "direct_graph", "grandchild_only"]:
        write(os.path.join(case_dir, "branch.fbs"), 'include "types.fbs";\nnamespace include_demo; table Branch {}\n')
        items.insert(0, entry(pkg, "branch", dep_fbs=[":types.fbs"]))
    if mode == "missing_owner":
        items = items[:1]
    load = write_items_bzl(case_dir, "", items)
    census = [":physical"]
    if rel:
        write(os.path.join(case_dir, rel, "BUILD.bazel"),
              'package(default_visibility=["//visibility:public"])\nexports_files(glob(["*.fbs"]))\nfilegroup(name="physical", srcs=glob(["*.fbs"]))\n')
        census = [pkg + ":physical"]
    write(os.path.join(case_dir, "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n' + load + '\n'
          'package(default_visibility=["//visibility:public"])\n'
          'filegroup(name="physical", srcs=glob(["*.fbs"]))\n'
          'aorta_schema_fragment(name="fragment", api=2, module_name="aorta", items=ITEMS)\n')
    if mode == "missing_owner":
        # The unowned dependency is not a local census omission: give it to a
        # separate, valid fragment whose provider is deliberately not an owner input.
        other = write_items_bzl(case_dir, "", [entry(root, "types")], symbol="TYPES")
        # Restore root declarations in their own file after writing the companion.
        (Path(case_dir) / "types.bzl").write_text((Path(case_dir) / "fragment.bzl").read_text())
        load = write_items_bzl(case_dir, "", items)
        build = Path(case_dir) / "BUILD.bazel"
        build.write_text('load(":types.bzl", "TYPES")\n' + build.read_text() +
                        '\naorta_schema_fragment(name="types", api=2, module_name="aorta", items=TYPES)\n')
        # The independent include target must receive the dependency provider;
        # a configured source File alone does not establish a declared owner.
        return root + ":fragment_include_lint_test", "AORTA_INCLUDE_OWNER", [], None, "test"
    census_module_build(case_dir, [":fragment"], census)
    diagnostic = {"undeclared": "EDGES", "unused": "EDGES", "unknown": "UNRESOLVED",
                  "escaping": "PATH", "last_schemas": "UNRESOLVED", "transitive": "EDGES",
                  "grandchild_only": "EDGES"}.get(mode)
    return root + ":module_lint_test", "AORTA_INCLUDE_" + diagnostic if diagnostic else None, [], None, "test"


def case_include_foreign(case_dir, mode="valid"):
    root = "//" + FIXTURE_ROOT_REL
    remote = "@aorta_schema_module_fixture//schemas/remote:types.fbs"
    other = Path(case_dir) / "other_module"
    write(str(other / "MODULE.bazel"), 'module(name="aorta_schema_module_fixture", version="0.0.0")\nbazel_dep(name="aorta", version="0.0.0")\n')
    remote_pkg = "//schemas/remote"
    write(str(other / "schemas/remote/types.fbs"), "namespace remote; table Types {}\n")
    write(str(other / "schemas/remote/fragment.bzl"), "ITEMS = [" + entry(remote_pkg, "types") + "]\n")
    write(str(other / "schemas/remote/BUILD.bazel"),
          'load("@aorta//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n'
          'load(":fragment.bzl", "ITEMS")\npackage(default_visibility=["//visibility:public"])\n'
          'exports_files(["types.fbs"])\naorta_schema_fragment(name="remote", api=2, module_name="remote", items=ITEMS)\n')
    text = 'include "schemas/remote/types.fbs";\nnamespace local; table Root {}\n'
    if mode == "bare_foreign":
        text = text.replace("schemas/remote/types.fbs", "types.fbs")
    local_items = [entry(root, "root", dep_fbs=[remote])]
    write(os.path.join(case_dir, "root.fbs"), text)
    if mode in ["same_basename", "duplicate_key"]:
        rel = "schemas/remote" if mode == "duplicate_key" else "schemas/local"
        write(os.path.join(case_dir, rel, "types.fbs"), "namespace same; table Types {}\n")
        write(os.path.join(case_dir, rel, "BUILD.bazel"), 'package(default_visibility=["//visibility:public"])\nexports_files(["types.fbs"])\nfilegroup(name="physical", srcs=glob(["*.fbs"]))\n')
        local_items.append(entry(root + "/" + rel, "types"))
    load = write_items_bzl(case_dir, "", local_items)
    write(os.path.join(case_dir, "BUILD.bazel"),
          'load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment")\n' + load + '\n'
          'filegroup(name="physical", srcs=glob(["*.fbs"]))\n'
          'aorta_schema_fragment(name="fragment", api=2, module_name="aorta", items=ITEMS, fragment=":fragment.bzl", fragment_symbols={":fragment.bzl":"ITEMS"})\n')
    census = [":physical"]
    if mode in ["same_basename", "duplicate_key"]:
        census.append(root + "/" + rel + ":physical")
    census_module_build(case_dir, [":fragment"], census, ["@aorta_schema_module_fixture//schemas/remote:remote"])
    diagnostic = {"bare_foreign": "UNRESOLVED", "duplicate_key": "KEY"}.get(mode)
    return root + ":module_lint_test", "AORTA_INCLUDE_" + diagnostic if diagnostic else None, ["--override_module=aorta_schema_module_fixture=" + str(other)], None, "test"


B3_REFUSALS = [("includes: " + mode, lambda d, m=mode: case_include_edges(d, m))
               for mode in ["undeclared", "unused", "unknown", "escaping", "last_schemas", "transitive", "grandchild_only", "missing_owner"]] + [
    ("includes: foreign bare basename", lambda d: case_include_foreign(d, "bare_foreign")),
    ("includes: duplicate logical key", lambda d: case_include_foreign(d, "duplicate_key")),
]
B3_CONTROLS = [("includes: " + mode, lambda d, m=mode: case_include_edges(d, m))
              for mode in ["bare", "rooted", "nested_key", "no_schemas", "comment", "direct_graph", "repeated"]] + [
    ("includes: foreign rooted", case_include_foreign),
    ("includes: same basename distinct keys", lambda d: case_include_foreign(d, "same_basename")),
]


def case_include_wrong_owner_file(case_dir):
    result = case_include_foreign(case_dir)
    other = Path(case_dir) / "other_module"
    remote_build = other / "schemas/remote/BUILD.bazel"
    remote_build.write_text(remote_build.read_text() + '\nexports_files(["wrong.fbs"])\n')
    (other / "schemas/remote/wrong.fbs").write_text("namespace wrong; table Wrong {}\n")
    write(os.path.join(case_dir, "wrong_owner.bzl"), '''
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo")
def _impl(ctx):
    info = ctx.attr.original[AortaSchemaFragmentInfo]
    fields = {field: getattr(info, field) for field in dir(info) if field not in ["to_json", "to_proto"]}
    fields["entries"] = [dict(entry, source_identity = {"owner": str(ctx.file.wrong.owner), "path": ctx.file.wrong.path}) for entry in info.entries]
    return [AortaSchemaFragmentInfo(**fields)]
wrong_owner = rule(implementation = _impl, attrs = {"original": attr.label(), "wrong": attr.label(allow_single_file = [".fbs"])})
''')
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text('load(":wrong_owner.bzl", "wrong_owner")\n' + build.read_text().replace(
        "dependency_fragments=['@aorta_schema_module_fixture//schemas/remote:remote']", "dependency_fragments=[':wrong_owner']") +
        '\nwrong_owner(name="wrong_owner", original="@aorta_schema_module_fixture//schemas/remote:remote", wrong="@aorta_schema_module_fixture//schemas/remote:wrong.fbs")\n')
    return result[0], "AORTA_INCLUDE_FILE", result[2], None, "test"


B3_REFUSALS.append(("includes: configured dependency differs from owner", case_include_wrong_owner_file))

POSITIVE_CASES.extend(B3_CONTROLS)



def case_include_generated_packages(case_dir):
    result = case_include_foreign(case_dir, "same_basename")
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text(build.read_text().replace(', fragment=":fragment.bzl", fragment_symbols={":fragment.bzl":"ITEMS"}', ''))
    return result


NEGATIVE_CASES.extend(B3_REFUSALS)
POSITIVE_CASES.append(("includes: generated parent/child declarations", case_include_generated_packages))


def case_include_action(case_dir):
    rel = "schemas/bare"
    pkg = "//" + FIXTURE_ROOT_REL + "/" + rel
    root_type = "IncludeRoot" + uuid.uuid4().hex
    include = "types.fbs"
    write(os.path.join(case_dir, rel, "types.fbs"), "namespace action_check; struct Types { value:int; }\n")
    write(os.path.join(case_dir, rel, "sample.fbs"),
          'include "' + include + '";\nnamespace action_check; table ' + root_type + ' { value:Types; } root_type ' + root_type + ';\n')
    load = write_items_bzl(case_dir, rel, [
        entry(pkg, "sample", cpp=True, python=True, rust=True, bfbs=True, dep_fbs=[":types.fbs"]),
        entry(pkg, "types", cpp=True, python=True, rust=True),
    ])
    write(os.path.join(case_dir, rel, "BUILD.bazel"), PACKAGE_LOAD + '\n' + load + '\n'
          'aorta_schema_package(name="sample_pkg", api=2, module_name="aorta", items=ITEMS, srcs=glob(["*.fbs"]))\n')
    def inspect(root):
        proc = subprocess.run(["bazel", "test", "--lockfile_mode=off", "--test_output=errors", pkg + ":sample_pkg_lint_test"],
                              cwd=root, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        data = (Path(root) / "bazel-bin" / FIXTURE_ROOT_REL / rel / "sample.bfbs").read_bytes()
        assert root_type.encode() in data, "BFBS does not contain this run's declared root type"
    return pkg + ":sample_bfbs", None, [], inspect, "build"


POSITIVE_CASES.extend([
    ("includes: bare spelling with actual flatc", case_include_action),
])



def case_include_dependency_hook(case_dir):
    target = case_include_edges(case_dir, "missing_owner")[0]
    build = Path(case_dir) / "BUILD.bazel"
    build.write_text(build.read_text().replace('name="fragment", api=2, module_name="aorta", items=ITEMS)',
        'name="fragment", api=2, module_name="aorta", items=ITEMS, lint_dependencies=[":types"])'))
    def inspect(root):
        fragment = "//" + FIXTURE_ROOT_REL + ":fragment"
        graph = subprocess.check_output(["bazel", "query", "--lockfile_mode=off", "deps(" + fragment + ")"], cwd=root, text=True).splitlines()
        assert "//" + FIXTURE_ROOT_REL + ":types" not in graph, "lint owner input leaked into L2 dependencies"
        assert not any(label.endswith("_lint_test") or label.endswith("_lint_inputs") for label in graph), "L2 depends on lint targets"
    return target, None, [], inspect, "test"


POSITIVE_CASES.append(("includes: independent lint owner hook has no L2 back-edge", case_include_dependency_hook))



def case_source_contract(case_dir, mutation=None):
    pkg = "//" + FIXTURE_ROOT_REL
    namespace = "sample"
    contract = dict(schema_kind="message", root_type="sample.Sample", namespace=namespace, file_identifier="")
    fields = dict(bfbs=True, cpp=True, python=True, rust=True)
    source = "namespace sample;\ntable Sample { value:int; }\nroot_type Sample;\n"
    if mutation == "raw_root":
        contract.update(schema_kind="include_only", root_type="")
        fields = {}
    elif mutation == "missing_root":
        source = source.replace("root_type Sample;", "")
    elif mutation == "wrong_root":
        contract["root_type"] = "sample.Other"
    elif mutation == "namespace":
        contract["namespace"] = "other"
    elif mutation in ("identifier", "unexpected_identifier", "malformed_identifier"):
        source += 'file_identifier "ABCD";\n'
        contract["file_identifier"] = {"identifier": "EFGH", "unexpected_identifier": "", "malformed_identifier": "BAD"}[mutation]
    elif mutation == "kind_path":
        contract["schema_kind"] = "action"
    elif mutation == "action_flag":
        fields["action_schema"] = True
    elif mutation in ("service_missing", "service_imported", "service_valid"):
        contract.update(schema_kind="service", request_type="sample.Sample", response_type="sample.Reply")
        if mutation == "service_valid":
            source += "table Reply { value:int; }\n"
        elif mutation == "service_imported":
            source = 'include "reply.fbs";\n' + source
            fields["dep_fbs"] = [":reply.fbs"]
            write(str(Path(case_dir) / "reply.fbs"), "namespace sample; table Reply { value:int; }\n")
    elif mutation in ("action_result", "action_absent", "action_qualified", "action_family"):
        contract.update(schema_kind="action", root_type="")
        fields["action_schema"] = True
        fields["action_name"] = "sample"
        fields["dep_fbs"] = ["//message/schemas/shared/action_common:action_common.fbs"]
        source = ('include "schemas/shared/action_common/action_common.fbs";\nnamespace sample;\n'
                  'table SampleGoal {}\ntable SampleResult {}\ntable SampleFeedbackData {}\n')
        if mutation in ("action_result", "action_qualified"):
            root = "SampleResult" if mutation == "action_result" else "SampleGoal"
            source += "root_type sample." + root + ";\n"
            contract["root_type"] = "sample." + root
        elif mutation == "action_family":
            source = source.replace("table SampleResult {}", "table Other {}")
    elif mutation == "include_only":
        fields = {}
        contract.update(schema_kind="include_only", root_type="")
        source = source.replace("root_type Sample;", "")
    elif mutation in ("vendor", "digit", "atrc"):
        if mutation == "atrc":
            source += 'file_identifier "ATRC";\n'
            contract["file_identifier"] = "ATRC"
        else:
            namespace = "foxglove" if mutation == "vendor" else "vendor.camera2D"
            source = source.replace("namespace sample;", "namespace " + namespace + ";")
            contract.update(namespace=namespace, root_type=namespace + ".Sample")
    fields.update(contract)
    items = [entry(pkg, "sample", **fields)]
    if mutation == "service_imported":
        items.insert(0, entry(pkg, "reply"))
    load = write_items_bzl(case_dir, "", items)
    write(str(Path(case_dir) / "sample.fbs"), source)
    build = FRAGMENT_LOAD + "\n" + load + "\n"
    if mutation in B4_CONTROLS:
        build = PACKAGE_LOAD + "\n" + load + "\n"
        build += 'aorta_schema_package(name="source_pkg", api=2, module_name="aorta", items=ITEMS, srcs=["sample.fbs"], lint_dependencies=["//message/schemas/registry/fragments:aorta_base_fragment"])\n'
    elif mutation == "output_provenance":
        write(str(Path(case_dir) / "other.fbs"), source)
        build += 'genrule(name="sample_bfbs", srcs=["other.fbs"], outs=["sample.bfbs"], cmd="cp $(location other.fbs) $@")\n'
    if mutation not in B4_CONTROLS:
        build += 'aorta_schema_fragment(name="fragment", api=2, module_name="aorta", items=ITEMS, lint_dependencies=["//message/schemas/registry/fragments:aorta_base_fragment"])\n'
    write(str(Path(case_dir) / "BUILD.bazel"), build)
    code = {"raw_root": "ROOT", "missing_root": "ROOT", "wrong_root": "ROOT", "namespace": "NAMESPACE",
            "identifier": "IDENTIFIER", "unexpected_identifier": "IDENTIFIER", "malformed_identifier": "IDENTIFIER",
            "kind_path": "KIND", "action_flag": "KIND", "service_missing": "SERVICE", "service_imported": "SERVICE",
            "action_result": "ACTION", "action_family": "ACTION", "output_provenance": "PROVENANCE"}.get(mutation)
    return pkg + (":source_pkg_source_lint_test" if mutation in B4_CONTROLS else ":fragment_source_lint_test"), "AORTA_SOURCE_" + code if code else None, [], None, "test"


B4_MUTATIONS = ["raw_root", "missing_root", "wrong_root", "namespace", "identifier", "unexpected_identifier",
                "malformed_identifier", "kind_path", "action_flag", "service_missing", "service_imported", "action_result", "action_family", "output_provenance"]
B4_CONTROLS = [None, "service_valid", "action_absent", "action_qualified", "include_only", "vendor", "digit", "atrc"]
NEGATIVE_CASES.extend(("source: " + mutation, lambda d, m=mutation: case_source_contract(d, m)) for mutation in B4_MUTATIONS)
POSITIVE_CASES.extend(("source: " + str(mutation), lambda d, m=mutation: case_source_contract(d, m)) for mutation in B4_CONTROLS)
def case_source_identity(case_dir):
    pkg = "//" + FIXTURE_ROOT_REL
    write(str(Path(case_dir) / "sample.fbs"), "namespace sample; table Sample {} root_type Sample;\n")
    write(str(Path(case_dir) / "fragment.bzl"), "ONE = [%s]\nTWO = [%s]\n" % (
        entry(pkg, "sample", schema_kind="message", root_type="sample.Sample", namespace="sample", file_identifier=""),
        entry(pkg, "sample", schema_kind="message", root_type="sample.Sample", namespace="sample", file_identifier="ABCD")))
    write(str(Path(case_dir) / "BUILD.bazel"), FRAGMENT_LOAD + '\nload(":fragment.bzl", "ONE", "TWO")\n'
          'package(default_visibility=["//visibility:public"])\nexports_files(["sample.fbs"])\n'
          'aorta_schema_fragment(name="one", api=2, module_name="aorta", items=ONE)\n'
          'aorta_schema_fragment(name="two", api=2, module_name="aorta", items=TWO)\n')
    return _registry(case_dir, "source_identity", [pkg + ":one", pkg + ":two"]), "is declared twice with DIFFERENT metadata"


def case_source_bfbs_bytes(case_dir, alias=False):
    pkg = "//" + FIXTURE_ROOT_REL
    load = write_items_bzl(case_dir, "", [entry(pkg, "sample", bfbs=True, cpp=True, python=True, rust=True, schema_meta_py=False,
        schema_kind="message", root_type="sample.Sample", namespace="sample", file_identifier="")])
    write(str(Path(case_dir) / "sample.fbs"), "namespace sample; table Sample { value:int; } root_type Sample;\n")
    name = "real_bfbs" if alias else "sample_bfbs"
    command = ('$(location //message/tools:bfbs_stage.sh) $(location //message/tools:flatc) "--binary --schema --bfbs-builtins" $(@D) $(location sample.fbs) - 0'
               if alias else '$(location //message/tools:flatc) --version > $@')
    write(str(Path(case_dir) / "BUILD.bazel"),
          'load("//message/tools:schema_codegen.bzl", "aorta_define_schema_targets")\n' + FRAGMENT_LOAD + "\n" + load + "\n" +
          'aorta_define_schema_targets(stem="sample", root_fbs="sample.fbs", cpp=True, rust=True, python=True)\n' +
          'genrule(name=%r, srcs=["sample.fbs"], outs=["sample.bfbs"], tools=["//message/tools:flatc", "//message/tools:bfbs_stage.sh"], cmd=%r)\n' % (name, command) +
          ('alias(name="sample_bfbs", actual=":real_bfbs")\n' if alias else '') +
          'aorta_schema_fragment(name="fragment", api=2, module_name="aorta", items=ITEMS)\n')
    return pkg + ":fragment_source_lint_test", None if alias else "AORTA_SOURCE_BFBS", [], None, "test"


NEGATIVE_CASES.extend([
    ("source: contract participates in composition identity", case_source_identity),
    ("source: claimed BFBS bytes differ despite source/tool inputs", case_source_bfbs_bytes),
])
POSITIVE_CASES.append(("source: same generated BFBS through output alias", lambda d: case_source_bfbs_bytes(d, True)))


def case_lint_external_surface(case_dir, gate=None, module=False):
    other = Path(case_dir) / "other_module"
    rel = "schemas/surface"
    pkg = "//" + rel
    write(str(other / "MODULE.bazel"), 'module(name="aorta_schema_module_fixture", version="0.0.0")\nbazel_dep(name="aorta", version="0.0.0", repo_name="middleware")\n')
    write(str(other / rel / "sample.fbs"), 'include "types.fbs";\nnamespace surface; table Sample { value:Types; } root_type Sample;\n')
    write(str(other / rel / "types.fbs"), 'namespace surface; struct Types { value:int; }\n')
    if gate == "include":
        write(str(other / rel / "sample.fbs"), 'namespace surface; table Sample { value:int; } root_type Sample;\n')
    rows = [
        entry(pkg, "sample", cpp=True, python=True, rust=True, bfbs=True, dep_fbs=[":types.fbs"],
              schema_kind="message", root_type="surface.Wrong" if gate == "source" else "surface.Sample", namespace="surface", file_identifier=""),
        entry(pkg, "types", cpp=True, python=True, rust=True,
              schema_kind="include_only", root_type="", namespace="surface", file_identifier=""),
    ]
    write_items_bzl(other, rel, rows)
    authored = (other / rel / "fragment.bzl").read_text()
    if gate == "fragment":
        authored = authored.replace("provenance = 'internal'", "provenance = 'external'")
    write(str(other / rel / "audited.bzl"), authored)
    if gate == "census":
        write(str(other / rel / "orphan.fbs"), 'namespace surface; table Orphan {}\n')
    write(str(other / rel / "BUILD.bazel"),
          'load("@middleware//message/tools:schema_package.bzl", "aorta_schema_package")\n'
          'load("@middleware//message/tools:schema_lint.bzl", "aorta_schema_module_lint_test")\n'
          'load(":fragment.bzl", "ITEMS")\n'
          'aorta_schema_package(name="messages", api=2, module_name="surface", items=ITEMS, srcs=["sample.fbs", "types.fbs"], '
          'fragment=":audited.bzl", fragment_symbols={":audited.bzl":"ITEMS"})\n'
          'aorta_schema_module_lint_test(name="module_lint_test", fragments=[":messages_fragment"], census=[":all_source_fbs"])\n')
    target = "@aorta_schema_module_fixture" + pkg + (":module_lint_test" if module else ":messages_lint_test")
    options = ["--override_module=aorta_schema_module_fixture=" + str(other), "--nocache_test_results"]
    diagnostic = {"fragment": "AORTA_L4_MISMATCH", "census": "AORTA_CENSUS_UNREGISTERED",
                  "include": "AORTA_INCLUDE_EDGES", "source": "AORTA_SOURCE_ROOT"}.get(gate)
    def inspect(root):
        graph = subprocess.check_output(["bazel", "query", "--lockfile_mode=off",
            "loadfiles(deps(" + target + "))"] + options[:1], cwd=root, text=True).splitlines()
        assert not any("/registry/" in label or label.endswith("/registry.bzl") for label in graph), graph
    return target, diagnostic, options, inspect if gate is None else None, "test"


def case_lint_surface_input(case_dir, method):
    return ("//message/tools:schema_lint_surface_test", None,
            ["--test_arg=SurfaceTest." + method, "--nocache_test_results"], None, "test")


for _module in [False, True]:
    for _gate in ["fragment", "census", "include", "source"]:
        NEGATIVE_CASES.append(("surface: external " + ("module " if _module else "package ") + _gate,
                               lambda d, g=_gate, m=_module: case_lint_external_surface(d, g, m)))
    POSITIVE_CASES.append(("surface: external " + ("module" if _module else "package") + " all gates and renamed tools",
                           lambda d, m=_module: case_lint_external_surface(d, module=m)))

B5_INPUT_CASES = [
    ("surface: missing output role cannot skip provenance", lambda d: case_lint_surface_input(d, "test_missing_output_role_is_not_success")),
    ("surface: missing contract cannot become legacy", lambda d: case_lint_surface_input(d, "test_missing_contract_is_not_legacy")),
    ("surface: missing published BFBS cannot skip comparison", lambda d: case_lint_surface_input(d, "test_missing_published_bfbs_is_not_success")),
]

POSITIVE_CASES.extend(B5_INPUT_CASES)


def case_frozen_caller(case_dir, surface="package", unknown=False):
    if surface == "package":
        target = case_census_package(case_dir, orphan=False)[0].replace(":sample_pkg_lint_test", ":sample_pkg_fragment")
    else:
        case_l4_provider_evidence(case_dir, generated=True)
        target = "//" + FIXTURE_ROOT_REL + ":fragment"
    path = Path(case_dir) / "BUILD.bazel"
    if not unknown:
        import re
        path.write_text(re.sub(r"api\s*=\s*\d+", "api=1", path.read_text()))
    else:
        path = Path(case_dir) / "fragment.bzl"
        path.write_text(path.read_text().replace("struct(", "struct(obsolete_field=True, "))
    return target, "AORTA_SCHEMA_FIELDS" if unknown else "api = 1 but this aorta provides SCHEMA_RULES_API = 2"


def case_frozen_provider(case_dir, mutation="api"):
    case_l4_provider_evidence(case_dir, generated=True)
    write(str(Path(case_dir) / "stale.bzl"), '''
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo")
def _impl(ctx):
    info = ctx.attr.original[AortaSchemaFragmentInfo]
    fields = {field: getattr(info, field) for field in dir(info) if field not in ["to_json", "to_proto"]}
    if ctx.attr.mutation == "api":
        fields["rules_api"] = 1
    elif ctx.attr.mutation == "fields":
        fields["lint_declaration"] = dict(info.lint_declaration)
        fields["lint_declaration"]["field_names"] = [field for field in info.lint_declaration["field_names"] if field != "request_type"]
    elif ctx.attr.mutation == "missing_entry":
        fields["entries"] = [{key: value for key, value in entry.items() if key != "source_contract"} for entry in info.entries]
    elif ctx.attr.mutation == "extra_entry":
        fields["entries"] = [dict(entry, obsolete_field = True) for entry in info.entries]
    return [AortaSchemaFragmentInfo(**fields)]
stale = rule(implementation = _impl, attrs = {"original": attr.label(), "mutation": attr.string()})
''')
    path = Path(case_dir) / "BUILD.bazel"
    path.write_text('load(":stale.bzl", "stale")\n' + path.read_text() +
        '\nstale(name="stale", original=":fragment", mutation=%r, visibility=["//visibility:public"])\n' % mutation)
    target = _registry(case_dir, "stale", ["//" + FIXTURE_ROOT_REL + ":stale"])
    return target, "was built against SCHEMA_RULES_API 1" if mutation == "api" else "AORTA_SCHEMA_FIELDS"


def case_frozen_audit(case_dir, method):
    return ("//message/tools:schema_api_freeze_test", None,
            ["--test_arg=ApiFreezeTest." + method, "--nocache_test_results"], None, "test")


B6_REFUSALS = [
    ("freeze: API-1 " + surface, lambda d, s=surface: case_frozen_caller(d, s))
    for surface in ["package", "fragment"]
] + [
    ("freeze: unknown raw field " + surface, lambda d, s=surface: case_frozen_caller(d, s, True))
    for surface in ["package", "fragment"]
] + [
    ("freeze: composer " + mutation, lambda d, m=mutation: case_frozen_provider(d, m))
    for mutation in ["api", "fields", "missing_entry", "extra_entry"]
]
B6_AUDIT_CASES = [
    ("freeze: " + method, lambda d, m=method: case_frozen_audit(d, m))
    for method in ["test_api_one_audit", "test_missing_api_is_not_current", "test_old_payload_version",
                   "test_stale_field_schema", "test_unknown_field_schema"]
]
NEGATIVE_CASES.extend(B6_REFUSALS)
POSITIVE_CASES.extend(B6_AUDIT_CASES)
RED_CASES = []



# A3(ii) now runs here with a disposable real module, so its acceptance is
# checked against two real repository-qualified labels on every driver run.

PINNED_CASES = [

]


def run_case(root, builder, expect):
    case_dir = os.path.join(root, FIXTURE_ROOT_REL)
    shutil.rmtree(case_dir, ignore_errors=True)
    try:
        case = builder(case_dir)
        target, message = case[:2]
        options = case[2] if len(case) > 2 else []
        mode = case[4] if len(case) > 4 else "analysis"
        command = {
            "analysis": ["build", "--nobuild", "--lockfile_mode=off"],
            "build": ["build", "--lockfile_mode=off"],
            "test": ["test", "--lockfile_mode=off", "--test_output=errors"],
        }[mode]
        if expect == "fail" and message is None:
            raise AssertionError("negative case declared no expected message")
        proc = subprocess.run(
            ["bazel"] + command + [target] + options,
            cwd=root, capture_output=True, text=True,
        )
        output = proc.stdout + proc.stderr
        if expect == "fail":
            if proc.returncode == 0:
                return False, mode + " SUCCEEDED but should have failed"
            if message not in output:
                tail = "\n".join(output.strip().split("\n")[-12:])
                return False, "expected %r in the refusal; got:\n%s" % (message, tail)
            return True, message
        if proc.returncode != 0:
            tail = "\n".join(output.strip().split("\n")[-12:])
            return False, mode + " FAILED but should have succeeded:\n%s" % tail
        if len(case) > 3 and case[3] is not None:
            try:
                case[3](root)
            except AssertionError as error:
                return False, "artifact verification failed: " + str(error)
        return True, "accepted"
    finally:
        shutil.rmtree(case_dir, ignore_errors=True)


def main():
    root = repo_root()
    failures = 0
    fixed = 0
    red = 0
    for label, builder in NEGATIVE_CASES:
        ok, detail = run_case(root, builder, "fail")
        print("%s   %-46s  %s" % ("PASS" if ok else "FAIL", label, detail))
        failures += 0 if ok else 1
    for label, builder in POSITIVE_CASES:
        ok, detail = run_case(root, builder, "pass")
        print("%s   %-46s  %s" % ("PASS" if ok else "FAIL", label, detail))
        failures += 0 if ok else 1
    for label, builder, desired in PINNED_CASES:
        ok, detail = run_case(root, builder, desired)
        print("%s    %-46s  %s" % ("PIN " if ok else "FAIL", label, detail))
        failures += 0 if ok else 1
    for label, builder, desired in RED_CASES:
        ok, detail = run_case(root, builder, desired)
        # `ok` here means the DESIRED behaviour was observed, i.e. the gap
        # closed. Until then the case is red by construction, not a failure.
        print("%s  %-46s  %s" % ("FIXED" if ok else "RED  ", label, detail))
        fixed += 1 if ok else 0
        red += 0 if ok else 1
    print("\n| Category | Cases |")
    print("| --- | ---: |")
    for category, cases in [("NEGATIVE", NEGATIVE_CASES), ("POSITIVE", POSITIVE_CASES),
                            ("PINNED", PINNED_CASES), ("RED", RED_CASES)]:
        print("| %s | %d |" % (category, len(cases)))
    print("\n%d case(s) failed, %d still red, %d newly fixed" % (failures, red, fixed))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
