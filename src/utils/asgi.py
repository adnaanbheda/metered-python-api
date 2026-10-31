from starlette.types import Message, Receive


async def read_body(receive: Receive) -> bytes | None:
    """Buffer the full request body. None if the client disconnected."""
    chunks = []
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return None
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            return b"".join(chunks)


def replay_body(body: bytes, receive: Receive) -> Receive:
    """Hand the buffered body to the app once, then fall through to the real receive."""
    sent = False

    async def replay() -> Message:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replay
