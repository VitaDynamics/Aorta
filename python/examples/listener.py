#!/usr/bin/env python3
"""Aorta Python SDK — listener example (mirror of rust/examples/listener.rs).

Subscribes to ``demo::Chat`` messages, decodes the AortaHeader + text with
FlatBuffers primitives, and prints them. Interops with the Rust/C++ talkers.
"""

from __future__ import annotations

import argparse
import time

import flatbuffers
from flatbuffers import table as _table

import aorta
from aorta.header import read_aorta_header


def _read_chat(buf: bytes):
    root = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    tab = _table.Table(buf, root)
    header = None
    off = tab.Offset(4)  # field 0 = aorta_header
    if off:
        header = read_aorta_header(buf, tab.Indirect(tab.Pos + off))
    text = ""
    off = tab.Offset(6)  # field 1 = text
    if off:
        text = tab.String(tab.Pos + off).decode("utf-8")
    return header, text


def main() -> None:
    ap = argparse.ArgumentParser(description="Aorta Python listener")
    ap.add_argument("--topic", default="/demo/chatter")
    ap.add_argument("--count", type=int, default=0, help="exit after N msgs (0=forever)")
    args = ap.parse_args()

    seen = [0]

    def on_msg(payload, ctx):
        header, text = _read_chat(payload)
        seq = header.sequence if header else -1
        node_name = header.publisher_node if header else ""
        print(f"received: text={text} seq={seq} from={node_name}", flush=True)
        seen[0] += 1

    with aorta.Node("py_listener") as node:
        node.create_subscriber(args.topic, on_msg)
        print(f"listening on {args.topic}", flush=True)
        while args.count == 0 or seen[0] < args.count:
            time.sleep(0.1)


if __name__ == "__main__":
    main()
