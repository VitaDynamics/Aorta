#!/usr/bin/env python3
"""Aorta Python SDK — raw RPC echo client example.

Sends ``--count`` requests to ``<service>`` and prints each response. Pairs with
``echo_server.py`` for the cross-process RPC interop gate.
"""

from __future__ import annotations

import argparse
import time

import aorta


def main() -> None:
    ap = argparse.ArgumentParser(description="Aorta Python echo client")
    ap.add_argument("--service", default="/py/echo")
    ap.add_argument("--text", default="ping")
    ap.add_argument("--count", type=int, default=3)
    ap.add_argument("--timeout-ms", type=int, default=5000)
    ap.add_argument("--settle", type=float, default=1.0, help="discovery settle seconds")
    args = ap.parse_args()

    with aorta.Node("py_echo_client") as node:
        client = node.create_client(
            args.service, options=aorta.ClientOptions(timeout_ms=args.timeout_ms)
        )
        time.sleep(args.settle)  # let the service queryable be discovered
        for i in range(args.count):
            resp = client.call(f"{args.text} #{i}".encode())
            print(f"response #{i}: {resp.decode('utf-8', 'replace')}", flush=True)


if __name__ == "__main__":
    main()
