import asyncio
import logging
import uuid
from collections import Counter

from pydantic import ValidationError
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from exceptions import QuotaError
from schemas import BatchRequest
from store import QuotaStore
from utils.asgi import read_body, replay_body
from utils.fingerprint import fingerprint
from utils.responses import error_response

# Per-request outcomes log at DEBUG only: this sits in the hot path (<10ms budget).
log = logging.getLogger(__name__)


# TODO: config file to define features without re-deployments
def fixed(feature: str, cost: int = 1):
    return lambda body: {feature: cost}


FEATURE_COSTS = {
    "/container_tracking": fixed("container-tracking"),
    "/sailing_schedule": fixed("sailing-schedule"),
    "/shipment_data": fixed("shipment-data"),
    "/batch": lambda body: Counter(
        op.feature for op in BatchRequest.model_validate_json(body).operations
    ),
}


REQUEST_ID_HEADER = "X-Request-Id"
MAX_REQUEST_ID_LEN = 128


class MeterMiddleware:
    def __init__(self, app: ASGIApp, quota_store: QuotaStore):
        self.app = app
        self.store = quota_store

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # lifespan, websocket: pass through
            await self.app(scope, receive, send)
            return

        cost_fn = FEATURE_COSTS.get(scope["path"])
        if cost_fn is None:  # unmetered route
            await self.app(scope, receive, send)
            return

        org_id = scope["state"]["org_id"]
        request_id = (
            Headers(scope=scope).get(REQUEST_ID_HEADER, "").strip() or uuid.uuid4().hex
        )
        if len(request_id) > MAX_REQUEST_ID_LEN:
            response = error_response(
                400,
                "invalid_request_id",
                f"{REQUEST_ID_HEADER} longer than {MAX_REQUEST_ID_LEN}",
                {},
            )
            await response(scope, receive, send)
            return
        id_header = {REQUEST_ID_HEADER: request_id}

        body = await read_body(receive)
        if body is None:
            return
        receive = replay_body(body, receive)

        try:
            costs = cost_fn(body)
        except (
            ValidationError
        ):  # malformed batch: let FastAPI return its 422, no charge
            await self.app(scope, receive, send)
            return
        if len(costs) != 1:  # mixed-feature batch needs multi-key reserve
            response = error_response(
                501,
                "mixed_batch_unsupported",
                "batches spanning multiple features not metered yet",
                id_header,
            )
            await response(scope, receive, send)
            return
        ((feature, cost),) = costs.items()

        try:
            reservation = await self.store.reserve(
                org_id, feature, request_id, cost, fingerprint(scope, body)
            )
        except QuotaError as e:
            log.debug("rejected %s %s/%s: %s", request_id, org_id, feature, e.code)
            await error_response(e.status, e.code, e.detail, id_header)(
                scope, receive, send
            )
            return
        log.debug("reserved %s %s/%s cost=%d", request_id, org_id, feature, cost)
        response_status = 500  # app died before starting a response

        async def send_with_id(message: Message) -> None:
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = message["status"]
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        except BaseException:
            log.debug("app raised, refunding %s", request_id)
            try:
                await asyncio.shield(self.store.refund(reservation))
            except Exception:
                # don't let a Redis failure here mask the app's own exception
                log.exception(
                    "refund failed, quota leaked: %s", reservation.reservation_key
                )
            raise

        # meter only successful calls. The response is already sent, so a Redis
        # failure here can't change it: log with the key instead of re-raising.
        try:
            if 200 <= response_status < 300:
                await self.store.commit_later(reservation)
                log.debug("committing %s status=%d", request_id, response_status)
            else:
                await self.store.refund(reservation)
                log.debug("refunded %s status=%d", request_id, response_status)
        except Exception:
            log.exception(
                "settle failed after %d response: %s",
                response_status,
                reservation.reservation_key,
            )
