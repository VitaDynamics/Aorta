"""Shared execution target without a fragment-provider load dependency."""

load("@rules_python//python:defs.bzl", "py_test")

def schema_lint_test(name, audit, mode = "fragment", visibility = None):
    py_test(
        name = name,
        srcs = [Label("//message/tools:schema_lint_main.py")],
        main = Label("//message/tools:schema_lint_main.py"),
        args = ["$(location " + audit + ")", mode],
        data = [audit],
        deps = [Label("//message/tools:schema_lint_lib")],
        visibility = visibility,
    )
