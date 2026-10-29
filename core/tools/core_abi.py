#!/usr/bin/env python3
"""Read the Core C-ABI generation from its one declaration.

`core/src/ffi/c_api/mod.rs` declares `pub const AORTA_ABI_VERSION: u32 = N;`
and nothing else may restate N. Everything that names the ABI -- the Linux
`DT_SONAME` of `libaorta_core.so.N`, the SDK bundle member and its development
symlink, `PROVENANCE.json`, the CMake package version, and the release asset
name -- is rendered through this module, and the bundle gate compares what a
bundle advertises against the same parse. A hand-copied number once stamped
ABI 10 across all of those while Core had moved to 12.

Command line:

    core_abi.py                       # print N
    core_abi.py --template T -o OUT   # copy T with @AORTA_CORE_ABI_VERSION@ -> N
    core_abi.py --text 'x.@AORTA_CORE_ABI_VERSION@' -o OUT
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CORE_ABI_SOURCE = Path("core/src/ffi/c_api/mod.rs")
PLACEHOLDER = "@AORTA_CORE_ABI_VERSION@"

_DECLARATION = re.compile(r"^pub const AORTA_ABI_VERSION: u32 = (\d+);$", re.MULTILINE)


def parse_core_abi(mod_rs_text: str) -> int:
    """The `AORTA_ABI_VERSION` value declared in `mod_rs_text`, exactly once."""
    values = _DECLARATION.findall(mod_rs_text)
    if len(values) != 1:
        raise ValueError(
            "expected exactly one `pub const AORTA_ABI_VERSION: u32 = <n>;` "
            f"declaration, found {len(values)}"
        )
    return int(values[0])


def read_core_abi(repo_root: Path = REPO_ROOT) -> int:
    return parse_core_abi((repo_root / CORE_ABI_SOURCE).read_text(encoding="utf-8"))


def render(template: str, core_abi: int) -> str:
    """`template` with every placeholder replaced; a template without one is a bug."""
    if PLACEHOLDER not in template:
        raise ValueError(f"template does not mention {PLACEHOLDER}")
    return template.replace(PLACEHOLDER, str(core_abi))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mod-rs", type=Path, default=REPO_ROOT / CORE_ABI_SOURCE)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--template", type=Path, help="file containing the placeholder")
    source.add_argument("--text", help="inline text containing the placeholder")
    parser.add_argument("-o", "--output", type=Path, help="default: stdout")
    args = parser.parse_args()

    core_abi = parse_core_abi(args.mod_rs.read_text(encoding="utf-8"))
    if args.template is not None:
        rendered = render(args.template.read_text(encoding="utf-8"), core_abi)
    elif args.text is not None:
        rendered = render(args.text, core_abi) + "\n"
    else:
        rendered = f"{core_abi}\n"
    if args.output is None:
        sys.stdout.write(rendered)
    else:
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
