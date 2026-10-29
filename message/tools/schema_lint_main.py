"""Run the agreement checker using Bazel's explicit audit/runfiles inputs."""

import argparse
import json
import os
from pathlib import Path

from message.tools.schema_source_contract import check_source_contract, check_output_provenance
from message.tools.schema_lint import check_audit_schema, check_census, check_fragment_agreement, check_include_edges


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("audit")
    parser.add_argument("mode", choices=["fragment", "census", "module", "include", "source"])
    args = parser.parse_args()
    audit = json.loads(Path(args.audit).read_text())
    check_audit_schema(audit)
    runfiles = Path(os.environ["TEST_SRCDIR"])
    read_text = lambda name: (runfiles / audit["files"][name]).read_text()
    if args.mode in ("census", "module"):
        check_census(audit)
    if args.mode == "module":
        for fragment in audit["fragments"]:
            check_fragment_agreement(fragment["audit"], read_text)
        check_include_edges([entry for fragment in audit["fragments"] for entry in fragment["audit"]["entries"]],
                            audit["dependency_entries"], lambda name: runfiles / audit["files"][name])
    elif args.mode == "include":
        check_include_edges(audit["entries"], audit["dependency_entries"], lambda name: runfiles / audit["files"][name])
    elif args.mode == "fragment":
        check_fragment_agreement(audit, read_text)
    if args.mode in ("module", "source"):
        entries = [entry for fragment in audit["fragments"] for entry in fragment["audit"]["entries"]] if args.mode == "module" else audit["entries"]
        for entry in entries:
            if "source_contract" not in entry:
                raise ValueError(f"AORTA_SOURCE_INPUT {entry['fbs_label']}: missing source_contract; legacy requires explicit null")
            if entry["source_contract"] is None:
                continue
            evidence = audit["source_evidence"][entry["fbs_label"]]
            check_source_contract(entry, read_text(entry["source_identity"]["path"]), json.loads(read_text(evidence["reflection"])))
            if evidence["outputs"].keys() != entry["output_labels"].keys():
                raise ValueError(f"AORTA_SOURCE_INPUT {entry['fbs_label']}: output roles differ from declared outputs")
            if bool(evidence["published_bfbs"]) != ("bfbs" in entry["output_labels"]):
                raise ValueError(f"AORTA_SOURCE_INPUT {entry['fbs_label']}: published BFBS evidence differs from declared output")
            check_output_provenance(entry, evidence["outputs"])
            for published in evidence["published_bfbs"]:
                if (runfiles / audit["files"][published]).read_bytes() != (runfiles / audit["files"][evidence["bfbs"]]).read_bytes():
                    raise ValueError(f"AORTA_SOURCE_BFBS {entry['fbs_label']}: published BFBS differs from independent source compilation")


if __name__ == "__main__":
    main()
