"""Bind a composed manifest to the actual selected compiler and codegen files."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path


def main(args):
    template, output, flatc, *sources = args
    manifest = json.loads(Path(template).read_text())
    expected = manifest["flatbuffers_version"]
    actual = subprocess.check_output([flatc, "--version"], text=True).strip()
    if actual != "flatc version " + expected:
        raise ValueError(f"FlatBuffers tool version mismatch: expected {expected}, got {actual}")
    manifest["verified_tools"] = {
        "flatc_version": actual,
        "flatc_sha256": hashlib.sha256(Path(flatc).read_bytes()).hexdigest(),
        "codegen_sha256": {source: hashlib.sha256(Path(source).read_bytes()).hexdigest()
                           for source in sources},
    }
    Path(output).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
