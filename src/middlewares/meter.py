import asyncio
import hashlib
import json
import uuid
from collections import Counter
from urllib.parse import parse_qsl, urlencode

from pydantic import ValidationError
from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from schemas import BatchRequest, ErrorResponse
from store import QuotaStore


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

REJECTIONS = {
    "reserved": (
        409,
        "request_in_progress",
        "request with this id is still processing",
    ),
    "committed": (
        409,
        "already_processed",
        "request with this id was already processed",
    ),
    "mismatch": (
        422,
        "request_id_reused",
        "request id reused with a different request",
    ),
    "exceeded": (429, "quota_exceeded", "quota exceeded"),
    "unprovisioned": (
        403,
        "quota_not_provisioned",
        "no quota provisioned for this period",
    ),
}


def error_response(
    status: int, code: str, detail: str, headers: dict[str, str]
) -> JSONResponse:
    body = ErrorResponse(detail=detail, code=code).model_dump()
    return JSONResponse(body, status_code=status, headers=headers)


def fingerprint(scope: Scope, body: bytes) -> str:
    """Hash of method, path, sorted query and canonical JSON body, so key order and
    whitespace don't change it."""
    query = urlencode(
        sorted(parse_qsl(scope["query_string"].decode(), keep_blank_values=True))
    )
    try:
        body = json.dumps(
            json.loads(body), sort_keys=True, separators=(",", ":")
        ).encode()
    except ValueError:
        pass  # empty or non-JSON body: hash raw bytes
    raw = b"|".join(
        [scope["method"].encode(), scope["path"].encode(), query.encode(), body]
    )
    return hashlib.sha256(raw).hexdigest()[:16]


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

        reservation = await self.store.reserve(
            org_id, feature, request_id, cost, fingerprint(scope, body)
        )

        if reservation.status != "ok":
            status, code, detail = REJECTIONS[reservation.status]
            await error_response(status, code, detail, id_header)(scope, receive, send)
            return

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
            await asyncio.shield(self.store.refund(reservation))
            raise

        # meter only successful calls
        if 200 <= response_status < 300:
            await self.store.commit(reservation)
        else:
            await self.store.refund(reservation)
