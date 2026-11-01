from pydantic import ValidationError
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from middlewares.meter import FEATURE_COSTS
from utils.asgi import read_body, replay_body
from utils.responses import error_response

# Set by the gateway, which strips any a client sent (gateway/src/meter.rs).
RESERVATION_HEADER = "X-Meter-Reservation"
SKIP_HEADER = "X-Meter-Skip"


class GatewayGuard:
    """METER_MODE=gateway: the Rust gateway reserves before forwarding, so a metered request
    here without its reservation header skipped the meter. Refuse it rather than serve it free.
    No Redis call: the gateway already did the metering."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] not in FEATURE_COSTS:
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        if headers.get(RESERVATION_HEADER):
            await self.app(scope, receive, send)
            return

        if headers.get(SKIP_HEADER) == "invalid-body":
            # The gateway found no valid batch to charge. MeterMiddleware lets those through
            # for FastAPI's 422; do the same, unless this side can parse it after all.
            body = await read_body(receive)
            if body is None:
                return
            try:
                FEATURE_COSTS[scope["path"]](body)
            except ValidationError:
                await self.app(scope, replay_body(body, receive), send)
                return

        response = error_response(
            500, "meter_bypass", "metered route reached without the gateway", {}
        )
        await response(scope, receive, send)
