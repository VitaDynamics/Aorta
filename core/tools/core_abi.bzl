"""Render files from the Core C-ABI generation.

`AORTA_ABI_VERSION` in core/src/ffi/c_api/mod.rs is the one declaration of
the number; `core_abi_render` runs //core:core_abi (core/tools/core_abi.py)
over it at build time so BUILD files never restate the value.
"""

CORE_ABI_SOURCE = "//core:aorta_core_abi_source"
CORE_ABI_TOOL = "//core:core_abi"

def core_abi_render(name, out, template = None, text = None, **kwargs):
    """Writes `out` from the parsed `AORTA_ABI_VERSION`.

    With neither `template` nor `text` the file holds the bare number.
    Otherwise every `@AORTA_CORE_ABI_VERSION@` in the template file, or in
    the inline `text`, is replaced. `text` must not contain single quotes.
    """
    if template != None and text != None:
        fail("core_abi_render: pass either template or text")
    srcs = [CORE_ABI_SOURCE]
    cmd = "$(execpath {tool}) --mod-rs $(location {source})".format(
        tool = CORE_ABI_TOOL,
        source = CORE_ABI_SOURCE,
    )
    if template != None:
        srcs.append(template)
        cmd += " --template $(location {})".format(template)
    elif text != None:
        if "'" in text:
            fail("core_abi_render: text must not contain single quotes")
        # `--text=` form: the rendered flag itself starts with a dash.
        cmd += " --text='{}'".format(text)
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [out],
        cmd = cmd + " --output $@",
        tools = [CORE_ABI_TOOL],
        **kwargs
    )
