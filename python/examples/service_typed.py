"""Typed RPC (service + client) using a GENERATED FlatBuffers message class.

Shows where the request/response message definitions come from in the service
scenario: the generated ``demo.Chat`` class (reused as both request and
response here). ONE import + ONE Bazel dep give the class + builders + meta:

    import demo_chat_schema_meta as chat    # chat.Chat / chat.ChatStart
    deps = ["//message/schemas/topic/example:demo_chat_schema_meta_py_lib"]

The typed service decodes the request with the generated root accessor and
replies with a typed message (header injected by the SDK); the typed client
builds the request and decodes the reply the same way.

Run via ``test_scripts/python_typed_examples.sh``.
"""

from __future__ import annotations

import aorta
import demo_chat_schema_meta as chat  # one import: message class + builders + meta


def _decode(payload: bytes):
    return chat.Chat.GetRootAs(payload, 0)


def main() -> int:
    node = aorta.Node("service_typed_demo", group="example")
    service_name = "/example/typed/echo"

    # Server: decode the request Chat, reply with a typed Chat (header injected).
    def handler(request, responder) -> None:
        reply_text = "pong: " + request.Text().decode()

        def fill(builder, header_off):
            text_off = builder.CreateString(reply_text)
            chat.ChatStart(builder)
            chat.ChatAddAortaHeader(builder, header_off)
            chat.ChatAddText(builder, text_off)
            return chat.ChatEnd(builder)

        responder.reply(fill)

    node.create_service_typed_view(_decode, service_name, handler)

    # Client: build a typed request, decode the typed reply.
    client = node.create_client_typed(service_name, _decode)

    def fill_request(builder, header_off):
        text_off = builder.CreateString("ping")
        chat.ChatStart(builder)
        chat.ChatAddAortaHeader(builder, header_off)
        chat.ChatAddText(builder, text_off)
        return chat.ChatEnd(builder)

    response = client.call(fill_request)
    reply = response.decode()  # generated demo.Chat
    text = reply.Text().decode()

    node.close()
    assert text == "pong: ping", text
    print(f"request='ping' reply='{text}'")
    print("SERVICE_TYPED OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
