"""Deterministic tar.zst release archive rule."""

_ENTRY_ATTRS = [
    "cpp_headers",
    "ffi_headers",
    "generated_headers",
    "cmake_files",
    "flatbuffers_headers",
    "flatbuffers_cmake_files",
    "core_shared",
    "cpp_static",
    "cpp_shared",
    "legal_files",
    "third_party_lock",
]


def _entry_kind(attr_name):
    return {
        "cpp_headers": "cpp_header",
        "ffi_headers": "ffi_header",
        "generated_headers": "generated_header",
        "cmake_files": "cmake",
        "flatbuffers_headers": "flatbuffers_header",
        "flatbuffers_cmake_files": "flatbuffers_cmake",
        "core_shared": "core_shared",
        "cpp_static": "cpp_static",
        "cpp_shared": "cpp_shared",
        "legal_files": "legal",
        "third_party_lock": "third_party_lock",
    }[attr_name]


AortaReleaseCommitInfo = provider(fields = ["value"])

def _aorta_release_commit_impl(ctx):
    return [AortaReleaseCommitInfo(value = ctx.build_setting_value)]

aorta_release_commit = rule(
    implementation = _aorta_release_commit_impl,
    build_setting = config.string(flag = True),
)

def _bundle_archive_impl(ctx):
    archive = ctx.actions.declare_file(ctx.label.name + ".tar.zst")
    members = ctx.actions.declare_file(ctx.label.name + ".members")
    commit = ctx.attr.aorta_commit[AortaReleaseCommitInfo].value
    if commit and (len(commit) != 40 or any([c not in "0123456789abcdef" for c in commit.elems()])):
        fail("aorta_commit must be a full lowercase SHA-1")
    # The Core ABI generation arrives as a file rendered from AORTA_ABI_VERSION
    # (core/tools/core_abi.bzl); the tool names lib/libaorta_core.so.<N>, its
    # symlink and PROVENANCE.json from it so no rule restates the number.
    core_abi = ctx.file.core_abi
    inputs = [core_abi]
    static_archives = []
    args = ctx.actions.args()
    args.add(archive.path)
    args.add(members.path)
    args.add("aorta_commit=" + commit)
    args.add("core_abi=" + core_abi.path)

    for attr_name in _ENTRY_ATTRS:
        kind = _entry_kind(attr_name)
        for target in getattr(ctx.attr, attr_name):
            for artifact in target.files.to_list():
                if attr_name == "cpp_static":
                    # cc_library also reports headers and PIC variants; the
                    # bundle wants the one plain static archive.
                    if artifact.extension != "a" or artifact.basename.endswith(".pic.a"):
                        continue
                    static_archives.append(artifact)
                inputs.append(artifact)
                args.add("{}={}".format(kind, artifact.path))

    if len(static_archives) > 1:
        fail("cpp_static must resolve to exactly one static archive, got {}".format(
            [artifact.basename for artifact in static_archives],
        ))

    ctx.actions.run(
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        arguments = [args],
        inputs = depset(inputs),
        outputs = [archive, members],
        mnemonic = "AortaReleaseBundle",
        progress_message = "Packaging %{label}",
    )
    return [
        DefaultInfo(files = depset([archive])),
        OutputGroupInfo(members = depset([members])),
    ]


def _bundle_attrs():
    attrs = {
        attr_name: attr.label_list(allow_files = True)
        for attr_name in _ENTRY_ATTRS
    }
    attrs["aorta_commit"] = attr.label(mandatory = True)
    attrs["core_abi"] = attr.label(allow_single_file = True, mandatory = True)
    attrs["_tool"] = attr.label(
        default = ":bundle_archive_tool",
        executable = True,
        cfg = "exec",
    )
    return attrs


bundle_archive = rule(
    implementation = _bundle_archive_impl,
    attrs = _bundle_attrs(),
)
