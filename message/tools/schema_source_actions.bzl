"""Private reflection actions over explicitly supplied provider source graphs."""

def source_reflection_attrs():
    return {
        "_source_reflect": attr.label(default = Label("//message/tools:schema_source_reflect"), executable = True, cfg = "exec"),
        "_source_flatc": attr.label(default = Label("//message/tools:flatc"), executable = True, cfg = "exec"),
        "_source_stage": attr.label(default = Label("//message/tools:bfbs_stage.sh"), allow_single_file = True),
    }

def source_reflections(ctx, fragments, dependencies):
    owners = {}
    files = {}
    provenance = {}
    for info in fragments + dependencies:
        for entry in info.entries:
            owners[entry["fbs_label"]] = entry
        files.update({str(label): file for label, file in info.source_files.items()})
        provenance.update(info.lint_declaration.get("output_provenance", {}))
    artifacts = []
    evidence = {}
    for info in fragments:
        for entry in info.entries:
            if entry.get("source_contract") == None or entry["fbs_label"] in evidence:
                continue
            label = entry["fbs_label"]
            source = files[label]
            selected = {label: True}
            frontier = entry["dep_fbs"]
            for _ in range(len(owners)):
                next_frontier = []
                for dep in frontier:
                    if dep not in selected:
                        if dep not in owners:
                            fail("AORTA_SOURCE_OWNER {}: missing provider for {}".format(label, dep))
                        selected[dep] = True
                        next_frontier.extend(owners[dep]["dep_fbs"])
                frontier = next_frontier
                if not frontier:
                    break
            closure = [files[key] for key in selected]
            stem = ctx.label.name + "/source_" + str(len(evidence))
            config = ctx.actions.declare_file(stem + ".input.json")
            reflected = ctx.actions.declare_file(stem + ".json")
            bfbs = ctx.actions.declare_file(stem + ".bfbs")
            prefix = source.owner.workspace_root + "/" if source.owner.workspace_root else ""
            slot = source.path[len(prefix):] if prefix else source.path
            ctx.actions.write(config, json.encode({
                "source": source.path,
                "slot": slot,
                "action": entry["flags"]["action_schema"],
                "direct": [files[dep].path for dep in entry["dep_fbs"]],
                "closure": [f.path for f in closure],
                "flatc": ctx.executable._source_flatc.path,
                "stage": ctx.file._source_stage.path,
            }))
            ctx.actions.run(
                executable = ctx.executable._source_reflect,
                env = {"PYTHONDONTWRITEBYTECODE": "1"},
                arguments = [config.path, reflected.path, bfbs.path],
                inputs = depset(closure + [config, ctx.file._source_stage]),
                tools = [ctx.executable._source_flatc],
                outputs = [reflected, bfbs],
                mnemonic = "AortaSourceReflection",
            )
            published = []
            if "bfbs" in entry["output_labels"]:
                published = info.output_targets[Label(entry["output_labels"]["bfbs"])][DefaultInfo].files.to_list()
            artifacts.extend([reflected, bfbs] + published)
            evidence[label] = {"reflection": reflected.path, "bfbs": bfbs.path, "published_bfbs": [f.path for f in published], "outputs": provenance[label]}
    return evidence, artifacts
