#!/usr/bin/env python3
"""Drive a real two-process consumer built from the staged SDK bundle alone.

Artifact-consumer check: a clean CMake consumer installs only the
archive and must link and run one topic, one service and one ContextStore route
through a real router.

The previous version of this test could not catch any of that. It fabricated a
FlatBuffers package config (hiding that the bundle shipped none), built on the
bundle's own build host (hiding that the archive could not link on the
documented target), and its `main.cc` only called `aorta_abi_version()` — it
never created a node, publisher, service or ContextStore, and never started a
router. This one:

* configures with the unpacked archive as the ONLY `CMAKE_PREFIX_PATH` entry
  and requires the package to match the Core ABI parsed from
  `core/src/ffi/c_api/mod.rs` (`find_package(Aorta <N>)`),
* builds `Aorta::cpp` (never the static fallback, never `-fno-lto`),
* starts a SHA-256-pinned upstream zenohd 1.9.0 on a free loopback port,
* runs the provider and client as separate processes against that router, and
* requires the exact `PROOF ... summary` lines both processes print.

Nothing here fabricates a result: the assertions are on the child processes'
own stdout and exit codes.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CORE_TOOLS = _HERE.parents[1] / "core" / "tools"
if str(_CORE_TOOLS) not in sys.path:
    sys.path.insert(0, str(_CORE_TOOLS))

from core_abi import read_core_abi  # noqa: E402

ROUTER_START_TIMEOUT_S = 30.0
SERVER_READY_TIMEOUT_S = 60.0
CLIENT_TIMEOUT_S = 180.0
SERVER_TIMEOUT_S = 120.0

UPSTREAM_VERSION = "1.9.0"
# The allowlist gate reads the revision from the URL literal, so it is written
# out once here; the assertion keeps it in step with UPSTREAM_VERSION.
UPSTREAM_URL_TEMPLATE = (
    "https://github.com/eclipse-zenoh/zenoh/releases/download/1.9.0/{asset}"
)
assert f"/{UPSTREAM_VERSION}/" in UPSTREAM_URL_TEMPLATE, UPSTREAM_URL_TEMPLATE
UPSTREAM_SHA256 = {
    "x86_64": "f18081184b089e79e605f2c0cb3f7790fbf101ae94942988f716e19e1810a46e",
    "aarch64": "34e24153dcf69e18f407eb1d2921226a273960cfaa03cefceb452d16434341e7",
}


def download_router(url: str, digest: str, root: Path) -> Path:
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    actual = hashlib.sha256(data).hexdigest()
    if actual != digest:
        raise ValueError(f"upstream router SHA-256 mismatch: expected {digest}, got {actual}")
    router = root / "zenohd"
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        router.write_bytes(archive.read("zenohd"))
    router.chmod(0o755)
    print(f"UPSTREAM_ROUTER url={url} sha256={actual}", flush=True)
    return router

REQUIRED_SERVER_LINES = (
    "PROOF server summary topic=PASS service=PASS context=PASS",
    "PROOF context local get key=sdk_proof/frame_id value=base_link",
)
REQUIRED_CLIENT_LINES = (
    "PROOF client summary topic=PASS service=PASS context=PASS",
    "PROOF context remote get key=sdk_proof/frame_id value=base_link",
)


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _extract(archive: Path, zstd: Path, root: Path) -> Path:
    tar_path = root / "sdk.tar"
    prefix = root / "sdk"
    subprocess.run([str(zstd), str(archive), str(tar_path)], check=True)
    with tarfile.open(tar_path, "r:") as bundle:
        for member in bundle.getmembers():
            if member.name.startswith("/") or ".." in Path(member.name).parts:
                raise ValueError(f"unsafe archive member: {member.name}")
        bundle.extractall(prefix)
    return prefix


def _zenoh_configs(root: Path, port: int) -> tuple[Path, Path]:
    """Loopback-only router and client configs, matching the deployment contract."""
    common = {
        "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
        "transport": {"shared_memory": {"enabled": False}},
    }
    endpoint = f"tcp/127.0.0.1:{port}"
    router = root / "zenohd.json5"
    client = root / "zenoh_client.json5"
    router.write_text(
        json.dumps({"mode": "router", "listen": {"endpoints": [endpoint]}, **common}),
        encoding="utf-8",
    )
    client.write_text(
        json.dumps({"mode": "client", "connect": {"endpoints": [endpoint]}, **common}),
        encoding="utf-8",
    )
    return router, client


def _wait_for_port(port: int, process: subprocess.Popen, log: Path) -> None:
    deadline = time.monotonic() + ROUTER_START_TIMEOUT_S
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"router exited early with {process.returncode}:\n"
                + log.read_text(encoding="utf-8", errors="replace")
            )
        with socket.socket() as probe:
            probe.settimeout(0.5)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise RuntimeError(f"router did not listen on 127.0.0.1:{port} in time")


def _wait_for_line(log: Path, needle: str, process: subprocess.Popen, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if log.exists() and needle in log.read_text(encoding="utf-8", errors="replace"):
            return
        if process.poll() is not None:
            raise RuntimeError(
                f"provider exited with {process.returncode} before '{needle}':\n"
                + log.read_text(encoding="utf-8", errors="replace")
            )
        time.sleep(0.2)
    raise RuntimeError(f"provider never printed '{needle}'")


def _runtime_env(prefix: Path, client_config: Path) -> dict[str, str]:
    env = os.environ.copy()
    library_path = str(prefix / "lib")
    env["LD_LIBRARY_PATH"] = library_path
    env["DYLD_LIBRARY_PATH"] = library_path
    env["ZENOH_SESSION_CONFIG_URI"] = str(client_config)
    env["AORTA_GROUP"] = "default"
    return env


def _report(name: str, log: Path) -> str:
    return f"===== {name} =====\n" + log.read_text(encoding="utf-8", errors="replace")


def _require(text: str, lines: tuple[str, ...], name: str) -> None:
    missing = [line for line in lines if line not in text]
    if missing:
        raise RuntimeError(f"{name} did not report: " + "; ".join(missing))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--zstd", type=Path, required=True)
    parser.add_argument("--router", type=Path, help="Use a local router, e.g. bazel-bin/tools/zenohd/zenohd")
    args = parser.parse_args()

    consumer_sources = Path(__file__).resolve().parent / "consumer"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        router_binary = args.router
        if router_binary is None:
            arch = platform.machine()
            asset = f"zenoh-{UPSTREAM_VERSION}-{arch}-unknown-linux-gnu-standalone.zip"
            url = UPSTREAM_URL_TEMPLATE.format(asset=asset)
            router_binary = download_router(url, UPSTREAM_SHA256[arch], root)
        subprocess.run([str(router_binary), "--version"], check=True)
        prefix = _extract(args.archive, args.zstd, root)
        source = root / "consumer"
        build = root / "build"
        shutil.copytree(consumer_sources, source)

        # The unpacked archive is the ONLY prefix: no fabricated FlatBuffers
        # package, no Aorta checkout, no distro Aorta. The consumer asks for
        # the Core's own ABI generation, so a bundle advertising another one
        # fails at configure time.
        core_abi = read_core_abi()
        print(f"CORE_ABI={core_abi}", flush=True)
        subprocess.run(
            [
                "cmake",
                "-S",
                str(source),
                "-B",
                str(build),
                f"-DCMAKE_PREFIX_PATH={prefix}",
                f"-DAORTA_EXPECTED_CORE_ABI={core_abi}",
            ],
            check=True,
        )
        subprocess.run(["cmake", "--build", str(build), "--parallel", "1"], check=True)

        port = _free_port()
        router_config, client_config = _zenoh_configs(root, port)
        router_log = root / "zenohd.log"
        env = _runtime_env(prefix, client_config)

        with router_log.open("wb") as router_out:
            router = subprocess.Popen(
                [str(router_binary), "-c", str(router_config)],
                stdout=router_out,
                stderr=subprocess.STDOUT,
            )
        server_log = root / "proof_server.log"
        client_log = root / "proof_client.log"
        try:
            _wait_for_port(port, router, router_log)
            with server_log.open("wb") as server_out:
                server = subprocess.Popen(
                    [str(build / "proof_server")],
                    stdout=server_out,
                    stderr=subprocess.STDOUT,
                    env=env,
                )
            try:
                _wait_for_line(server_log, "PROOF server ready", server, SERVER_READY_TIMEOUT_S)
                with client_log.open("wb") as client_out:
                    client_rc = subprocess.run(
                        [str(build / "proof_client")],
                        stdout=client_out,
                        stderr=subprocess.STDOUT,
                        env=env,
                        timeout=CLIENT_TIMEOUT_S,
                    ).returncode
                server_rc = server.wait(timeout=SERVER_TIMEOUT_S)
            finally:
                if server.poll() is None:
                    server.kill()
                    server.wait()
        finally:
            router.terminate()
            try:
                router.wait(timeout=10)
            except subprocess.TimeoutExpired:
                router.kill()

        server_text = _report("proof_server.log", server_log)
        client_text = _report("proof_client.log", client_log)
        print(server_text)
        print(client_text)
        print(f"SERVER_RC={server_rc} CLIENT_RC={client_rc}")
        sys.stdout.flush()

        if server_rc != 0 or client_rc != 0:
            print(_report("zenohd.log", router_log))
            raise RuntimeError(
                f"consumer proof failed: server={server_rc} client={client_rc}"
            )
        _require(server_text, REQUIRED_SERVER_LINES, "provider")
        _require(client_text, REQUIRED_CLIENT_LINES, "client")
        print(f"CONSUMER PASS router={'local' if args.router else 'upstream-' + UPSTREAM_VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
