#!/usr/bin/env python3
"""Aorta Python SDK — raw RPC echo service example.

Serves ``<service>`` and replies ``echo from <name>: <request>`` to each request.
Pairs with ``echo_client.py`` for the cross-process RPC interop gate.
"""

from __future__ import annotations

import argparse
import time

import aorta


def main() -> None:
    ap = argparse.ArgumentParser(description="Aorta Python echo service")
    ap.add_argument("--service", default="/py/echo")
    ap.add_argument("--name", default="py_echo_server")
    ap.add_argument("--count", type=int, default=0, help="exit after N requests (0=forever)")
    args = ap.parse_args()

    served = [0]

    def handler(data: bytes, req: "aorta.ServiceRequest") -> None:
        req.reply(b"echo from " + args.name.encode() + b": " + data)
        served[0] += 1

    with aorta.Node(args.name) as node:
        node.create_service(args.service, handler)
        print(f"echo service up on {args.service}", flush=True)
        while args.count == 0 or served[0] < args.count:
            time.sleep(0.05)
        time.sleep(0.2)  # let the last reply flush before teardown


if __name__ == "__main__":
    main()
