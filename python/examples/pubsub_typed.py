"""Typed pub/sub using GENERATED FlatBuffers message classes.

Unlike ``talker.py`` / ``listener.py`` (which hand-build the payload with
flatbuffers primitives to stay import-free), this example imports the **generated
message definition** exactly as a real app would. ONE import + ONE Bazel dep give
the message class, its builders, AND the schema metadata:

    import demo_chat_schema_meta as chat    # chat.Chat / chat.ChatStart / chat.SCHEMA_BFBS
    deps = ["//message/schemas/topic/example:demo_chat_schema_meta_py_lib"]

It also exercises the namespace coexistence: the message pulls in the generated
``aorta.sys.AortaHeader`` submodule, which must co-exist with the SDK ``aorta``
package (the SDK ``__init__`` calls ``pkgutil.extend_path``; keep the SDK first on
``sys.path``).

Run it (cdylib + generated dirs on the path) via
``test_scripts/python_typed_examples.sh``.
"""

from __future__ import annotations

import time

import aorta  # SDK package — MUST resolve before the generated aorta.sys.*
import demo_chat_schema_meta as chat  # one import: message class + builders + meta


def main() -> int:
    received = []
    node = aorta.Node("pubsub_typed_demo", group="example")
    topic = "/example/typed/chat"

    # Decoder: generated root accessor. Subscriber delivers the decoded Chat.
    def decode(payload: bytes):
        return chat.Chat.GetRootAs(payload, 0)

    def on_msg(msg, ctx) -> None:
        header = msg.AortaHeader()  # generated aorta.sys.AortaHeader (coexistence)
        seq = header.Sequence() if header else -1
        received.append((msg.Text().decode(), seq))

    node.create_subscriber_typed(decode, topic, on_msg)
    time.sleep(0.4)  # let the subscriber declaration propagate

    # Typed publisher: the schema-meta module (chat.SCHEMA_BFBS + hash) lets peers
    # discover/validate the schema; publish_typed injects the header.
    pub = node.create_publisher_typed(chat, topic)

    def fill(builder, header_off):
        # Build a demo.Chat with the SDK-injected header in slot 0 (aorta_header).
        text_off = builder.CreateString("hello from generated demo.Chat")
        chat.ChatStart(builder)
        chat.ChatAddAortaHeader(builder, header_off)
        chat.ChatAddText(builder, text_off)
        return chat.ChatEnd(builder)

    for _ in range(3):
        pub.publish_typed(fill)

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and len(received) < 3:
        time.sleep(0.02)

    node.close()
    assert len(received) >= 3, f"only got {received!r}"
    texts = [t for t, _ in received]
    seqs = [s for _, s in received]
    assert all(t == "hello from generated demo.Chat" for t in texts), texts
    assert seqs == [0, 1, 2], seqs  # per-publisher monotonic header sequence
    print(f"schema {chat.ROOT_TYPE_NAME} hash={chat.SCHEMA_HASH_HEX[:8]} "
          f"received={len(received)} seqs={seqs}")
    print("PUBSUB_TYPED OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
