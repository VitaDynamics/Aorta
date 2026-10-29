#!/usr/bin/env python3
"""Aorta Python SDK — talker example (mirror of rust/examples/talker.rs).

Publishes ``demo::Chat`` messages ({aorta_header, text}) with an injected,
byte-identical AortaHeader. Interops with the Rust/C++ listeners on the same
topic. The Chat table is built with FlatBuffers primitives so the example has no
dependency on generated accessor code (whose ``aorta.sys`` namespace would
collide with the ``aorta`` SDK package).
"""

from __future__ import annotations

import argparse
import time

import flatbuffers

import aorta


def _build_chat(builder: flatbuffers.Builder, header_off: int, text: str) -> int:
    # demo::Chat: field 0 = aorta_header (table), field 1 = text (string).
    text_off = builder.CreateString(text)
    builder.StartObject(2)
    builder.PrependUOffsetTRelativeSlot(0, header_off, 0)
    builder.PrependUOffsetTRelativeSlot(1, text_off, 0)
    return builder.EndObject()


def main() -> None:
    ap = argparse.ArgumentParser(description="Aorta Python talker")
    ap.add_argument("--topic", default="/demo/chatter")
    ap.add_argument("--text", default="hello from python")
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--interval", type=float, default=0.5)
    args = ap.parse_args()

    with aorta.Node("py_talker") as node:
        pub = node.create_publisher(args.topic)
        time.sleep(0.5)  # let discovery settle before the first publish
        for i in range(args.count):
            message = f"{args.text} #{i}"
            pub.publish_typed(
                lambda b, h, m=message: _build_chat(b, h, m)
            )
            print(f"published: {message}", flush=True)
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
