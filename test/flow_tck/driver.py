#!/usr/bin/env python3
"""Flow TCK driver (test/flow_tck/SCHEMA.md, Phase 0).

Runs the Rust and/or C++ scenario emitters, normalizes their NDJSON
event-record streams per SCHEMA.md's "Normalization (driver-side, before
comparison)" section, and diffs each plane's records against its
expectation file(s) under test/flow_tck/expected/ (either the
single-file form `<scenario>.json`, or the plane-qualified pair
`<scenario>.rust.json` + `<scenario>.cpp.json` — see SCHEMA.md
"Plane-qualified expectations").

Usage:
    driver.py --scenario cancel_reason_observation \\
        --rust-cmd "target/debug/examples/flow_tck_emitter cancel_reason_observation" \\
        --cpp-cmd "bazel-bin/cpp/flow_tck_emitter cancel_reason_observation"

Exit codes:
  0 — every tested plane matches its expectation.
  1 — at least one plane's observed records diverge from its
      expectation (a real semantic mismatch — the DRIVER's verdict, per
      SCHEMA.md "Transport": the emitter's own exit code is not this).
  2 — harness error: a plane could not be run/parsed, or the expectation
      files are missing/malformed. The driver could not render a verdict
      at all.
"""
import argparse
import difflib
import json
import pathlib
import shlex
import subprocess
import sys

# SCHEMA.md "Normalization": fields dropped before comparison (emitters
# MAY emit them for debugging; they carry no cross-plane semantics).
SCHEMA_DROP_FIELDS = ("thread", "ts_ns")

REQUIRED_FIELDS = ("scenario", "seq", "op", "event")

EXPECTED_DIR = pathlib.Path(__file__).resolve().parent / "expected"


class HarnessError(Exception):
    """Raised for anything that means the DRIVER could not render a verdict
    (as opposed to a real observed-vs-expected divergence)."""


def run_emitter(label, cmd_str):
    """Run one scenario emitter; return its RAW (pre-normalization) parsed
    NDJSON records, in observation order."""
    try:
        argv = shlex.split(cmd_str)
    except ValueError as exc:
        raise HarnessError(f"{label}: could not parse command {cmd_str!r}: {exc}")
    if not argv:
        raise HarnessError(f"{label}: empty command")
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    except FileNotFoundError as exc:
        raise HarnessError(f"{label}: emitter not found ({cmd_str!r}): {exc}")
    except subprocess.TimeoutExpired as exc:
        raise HarnessError(f"{label}: emitter timed out ({cmd_str!r}): {exc}")

    if proc.returncode != 0:
        raise HarnessError(
            f"{label}: emitter exited {proc.returncode} (SCHEMA.md: a "
            f"scenario binary's own exit code is 0 unless the harness itself "
            f"is broken — semantic divergence is the DRIVER's verdict, not "
            f"the emitter's) — stderr:\n{proc.stderr}"
        )

    records = []
    for lineno, line in enumerate(proc.stdout.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise HarnessError(
                f"{label}: stdout line {lineno} is not valid JSON ({exc}): "
                f"{line!r}"
            )
        if not isinstance(record, dict):
            raise HarnessError(
                f"{label}: stdout line {lineno} is not a JSON object: {line!r}"
            )
        records.append(record)
    return records


def normalize(records):
    """SCHEMA.md 'Normalization (driver-side, before comparison)':
    drop thread/ts_ns, drop debug.*-prefixed ops, re-densify seq."""
    kept = []
    for record in records:
        op = record.get("op")
        if isinstance(op, str) and op.startswith("debug."):
            continue
        cleaned = {k: v for k, v in record.items() if k not in SCHEMA_DROP_FIELDS}
        kept.append(cleaned)
    # "seq is compared as ORDER, not value" — reassign after filtering so a
    # dropped debug.* record never leaves a gap that fails the comparison.
    for i, record in enumerate(kept):
        if "seq" in record:
            record["seq"] = i
    return kept


def validate_required_fields(label, scenario, records):
    for i, record in enumerate(records):
        missing = [f for f in REQUIRED_FIELDS if f not in record]
        if missing:
            raise HarnessError(
                f"{label}: record {i} is missing required field(s) "
                f"{missing}: {record}"
            )
        if record["scenario"] != scenario:
            raise HarnessError(
                f"{label}: record {i} has scenario={record['scenario']!r}, "
                f"expected {scenario!r}"
            )


def load_json_array(path):
    try:
        data = json.loads(path.read_text())
    except OSError as exc:
        raise HarnessError(f"could not read expectation {path}: {exc}")
    except json.JSONDecodeError as exc:
        raise HarnessError(f"expectation {path} is not valid JSON: {exc}")
    if not isinstance(data, list):
        raise HarnessError(f"expectation {path} must be a JSON array of records")
    return data


def load_expectation(scenario):
    """Returns (mode, expectations, expected_paths):
      mode            'single' or 'paired'
      expectations    {'rust': [...], 'cpp': [...]}  (identical lists in
                       'single' mode, distinct in 'paired' mode)
      expected_paths  {'rust': Path, 'cpp': Path}     (for diff/report text)
    """
    single = EXPECTED_DIR / f"{scenario}.json"
    rust_file = EXPECTED_DIR / f"{scenario}.rust.json"
    cpp_file = EXPECTED_DIR / f"{scenario}.cpp.json"

    if single.exists():
        if rust_file.exists() or cpp_file.exists():
            raise HarnessError(
                f"both the single-file expectation {single} and a paired "
                f"expectation exist for {scenario!r} — SCHEMA.md's "
                f"'Plane-qualified expectations' allows only "
                f"one form per scenario"
            )
        records = load_json_array(single)
        return (
            "single",
            {"rust": records, "cpp": records},
            {"rust": single, "cpp": single},
        )

    if rust_file.exists() and cpp_file.exists():
        return (
            "paired",
            {"rust": load_json_array(rust_file), "cpp": load_json_array(cpp_file)},
            {"rust": rust_file, "cpp": cpp_file},
        )
    if rust_file.exists() or cpp_file.exists():
        have = rust_file if rust_file.exists() else cpp_file
        missing = cpp_file if rust_file.exists() else rust_file
        raise HarnessError(
            f"paired expectation for {scenario!r} is incomplete — found "
            f"{have} but not {missing} (SCHEMA.md requires BOTH halves of "
            f"the pair)"
        )
    raise HarnessError(
        f"no expectation found for scenario {scenario!r}: looked for "
        f"{single} or the pair {rust_file} + {cpp_file}"
    )


def diff_records(plane, observed, expected, expected_path):
    """Returns a unified-diff string, or None when the streams match."""
    obs_lines = [json.dumps(r, sort_keys=True) for r in observed]
    exp_lines = [json.dumps(r, sort_keys=True) for r in expected]
    if obs_lines == exp_lines:
        return None
    diff = difflib.unified_diff(
        exp_lines,
        obs_lines,
        fromfile=f"expected ({expected_path})",
        tofile=f"observed ({plane})",
        lineterm="",
    )
    return "\n".join(diff)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scenario", required=True, help="scenario id, e.g. cancel_reason_observation")
    parser.add_argument("--rust-cmd", default=None, help="shell-quoted command that runs the Rust emitter")
    parser.add_argument("--cpp-cmd", default=None, help="shell-quoted command that runs the C++ emitter")
    args = parser.parse_args(argv)

    if not args.rust_cmd and not args.cpp_cmd:
        print(
            "flow_tck driver: HARNESS ERROR: neither --rust-cmd nor "
            "--cpp-cmd given — nothing to run",
            file=sys.stderr,
        )
        return 2

    try:
        mode, expectations, expected_paths = load_expectation(args.scenario)
    except HarnessError as exc:
        print(f"flow_tck driver: HARNESS ERROR: {exc}", file=sys.stderr)
        return 2

    if mode == "paired":
        print(
            f"NOTE: scenario {args.scenario!r} uses PLANE-QUALIFIED "
            f"expectations (SCHEMA.md 'Plane-qualified expectations') — "
            f"the Rust and C++ planes are compared against "
            f"DIFFERENT expectation files "
            f"({expected_paths['rust'].name} vs {expected_paths['cpp'].name}). "
            f"This is the documented plane-qualified divergence, not a bug."
        )

    planes = []
    if args.rust_cmd:
        planes.append(("rust", args.rust_cmd))
    if args.cpp_cmd:
        planes.append(("cpp", args.cpp_cmd))

    divergence = False
    for plane, cmd in planes:
        try:
            raw = run_emitter(plane, cmd)
            observed = normalize(raw)
            validate_required_fields(plane, args.scenario, observed)
            expected = normalize(expectations[plane])
            validate_required_fields(f"{plane} expectation", args.scenario, expected)
        except HarnessError as exc:
            print(f"flow_tck driver: HARNESS ERROR ({plane}): {exc}", file=sys.stderr)
            return 2

        diff = diff_records(plane, observed, expected, expected_paths[plane])
        if diff is None:
            print(f"PASS {plane}: {len(observed)} records match {expected_paths[plane]}")
        else:
            divergence = True
            print(f"FAIL {plane}: observed diverges from {expected_paths[plane]}")
            print(diff)

    if divergence:
        print(f"flow_tck driver: DIVERGENCE — scenario {args.scenario!r}")
        return 1
    print(f"flow_tck driver: ALL PASS — scenario {args.scenario!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
