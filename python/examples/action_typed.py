"""Action client + server using the GENERIC FlatbuffersActionCodec — no
hand-written or generated codec.

The action's goal/result/feedback DOMAIN payloads are the generated demo.Chat
(object API ``ChatT``, from ``--gen-object-api``); the SDK's
``aorta.FlatbuffersActionCodec`` splices them as the wrapper's nested tables
(wire-compatible with the Rust/C++ generated codecs). One import gives the
object-API class + the schema metadata:

    import demo_chat_schema_meta as chat                 # chat.ChatT / chat.Chat
    codec = aorta.FlatbuffersActionCodec(
        goal=chat.ChatT, result=chat.ChatT, feedback=chat.ChatT)
    deps = ["//message/schemas/topic/example:demo_chat_schema_meta_py_lib"]

Run via ``test_scripts/python_typed_examples.sh``.
"""

from __future__ import annotations

import time

import aorta
import demo_chat_schema_meta as chat  # generated demo.Chat (incl. object-API ChatT)

# Any consistent non-empty bytes; Core stores + hashes the action schema, never
# parses it (the codec is the only wrapper parser/builder).
SCHEMA_BFBS = b"aorta.example.chat.action.wrapper.v1"


def _chat_bytes(text: str) -> bytes:
    """Serialize a demo.Chat domain payload (an action goal/result) to bytes."""
    msg = chat.ChatT()
    msg.text = text
    return aorta.FlatbuffersActionCodec.serialize(msg)


def _chat_text(msg) -> str:
    return msg.text.decode() if isinstance(msg.text, bytes) else msg.text


def main() -> int:
    # The whole codec — no hand-written wrapper logic. Goal/result/feedback are
    # all demo.Chat here; a real action passes its <Goal>T / <Result>T / <Fb>T.
    codec = aorta.FlatbuffersActionCodec(goal=chat.ChatT, result=chat.ChatT, feedback=chat.ChatT)
    node = aorta.Node("action_typed_demo", group="example")
    action_name = "/example/typed/shout"

    # goal.decode() -> a ChatT (via goal_decoder=codec.decode_goal). Reply uppercased.
    def handler(goal, ctx) -> None:
        text = _chat_text(goal.decode())
        ctx.succeed(_chat_bytes(text.upper()))

    node.create_action_server(
        action_name, SCHEMA_BFBS, codec, handler, goal_decoder=codec.decode_goal
    )
    client = node.create_action_client(
        action_name, codec, options=aorta.ActionClientOptions(control_timeout_ms=4000)
    )
    time.sleep(0.6)

    goal_handle = client.send_goal(_chat_bytes("hello action"))
    result = goal_handle.wait_result(timeout=5.0)
    text = _chat_text(chat.ChatT.InitFromPackedBuf(result.result, 0))  # decode the Chat result

    node.close()
    assert result.status == aorta.ActionStatus.SUCCEEDED, result.status
    assert text == "HELLO ACTION", text
    print(f"goal='hello action' -> result='{text}' status={result.status.name} (generic codec)")
    print("ACTION_TYPED OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
