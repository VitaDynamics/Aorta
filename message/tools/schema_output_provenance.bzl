"""Configured generating-action input ancestry, kept outside release metadata."""

SchemaOutputProvenanceInfo = provider(fields = ["files"])

def _provenance_impl(target, ctx):
    ancestry = {}
    if ctx.rule != None:
        for name in dir(ctx.rule.attr):
            value = getattr(ctx.rule.attr, name)
            values = value if type(value) == "list" else (value.keys() if type(value) == "dict" else [value])
            for dep in values:
                if type(dep) == "Target" and SchemaOutputProvenanceInfo in dep:
                    ancestry.update(dep[SchemaOutputProvenanceInfo].files)
        pending = list(target.actions)
        own_outputs = {f: True for action in pending for f in action.outputs.to_list()}
        for _ in range(len(pending)):
            remaining = []
            for action in pending:
                inputs = action.inputs.to_list()
                if any([f in own_outputs and f not in ancestry for f in inputs]):
                    remaining.append(action)
                    continue
                sources = depset([f for f in inputs if (f.is_source and f.extension in ["fbs", "py"]) or f == ctx.executable._flatc], transitive = [ancestry[f] for f in inputs if not f.is_source and f in ancestry])
                for output in action.outputs.to_list():
                    ancestry[output] = sources
            if len(remaining) == len(pending):
                break
            pending = remaining
    # Preserve intermediate outputs too: downstream actions can consume them.
    if DefaultInfo in target:
        for file in target[DefaultInfo].files.to_list():
            if (file.is_source and file.extension in ["fbs", "py"]) or file == ctx.executable._flatc:
                ancestry[file] = depset([file])
    return [SchemaOutputProvenanceInfo(files = ancestry)]

schema_output_provenance = aspect(implementation = _provenance_impl, attr_aspects = ["*"], attrs = {"_flatc": attr.label(default = Label("//message/tools:flatc"), executable = True, cfg = "exec")})
