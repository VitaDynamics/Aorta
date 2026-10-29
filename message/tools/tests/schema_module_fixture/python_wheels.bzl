"""Dev-only composed wheel fixture."""
load("@middleware//message/tools:registry_compose.bzl", "aorta_registry")
load("@middleware//message/tools:python_distribution.bzl", "schema_python_wheel")
load("@rules_python//python:defs.bzl", "py_binary")

def fixture_wheels():
    py_binary(name = "wheel_requirements", srcs = ["wheel_requirements.py"], testonly = True)
    # Read the actual wheel versions: py_wheel stamps Version, not Requires-Dist.
    sdk = "@middleware//message/tools:aorta_sdk_wheel_fixture_notices"
    base = "@middleware//python:aorta_msgs_wheel"
    native.genrule(
        name = "aorta_requirements",
        srcs = [sdk, base],
        outs = ["aorta_requirements.txt"],
        tools = [":wheel_requirements"],
        cmd = "$(location :wheel_requirements) $(location %s) $(location %s) $@" % (sdk, base),
        testonly = True,
    )
    fragments = [
        "@middleware//message/schemas/registry/fragments:aorta_base_fragment",
        "//schemas/release:leaf_fragment",
        "//schemas/release/nested:readings_fragment",
        "//schemas/custom:custom_types_fragment",
    ]
    aorta_registry(name = "python_registry", fragments = fragments, visibility = ["//visibility:public"])
    for name, module, distribution in [
        ("product_wheel", "release_fixture", "vita_msgs"),
        ("custom_wheel", "custom_fixture", "custom_msgs"),
    ]:
        schema_python_wheel(
            name = name,
            registry = ":python_registry",
            fragments = fragments,
            module_name = module,
            distribution = distribution,
            version = "0.1.0",
            tested_aorta_version = "0.0.0",
            requires_file = ":aorta_requirements",
            testonly = True,
            visibility = ["//visibility:public"],
        )
        native.filegroup(name = name + "_ownership", srcs = [":" + name + "_tree"],
                         output_group = "ownership", visibility = ["//visibility:public"])
