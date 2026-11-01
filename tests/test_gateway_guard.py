"""GatewayGuard (METER_MODE=gateway): metered requests that skipped the Rust gateway are refused."""

import json

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.responses import PlainTextResponse

from middlewares.gateway_guard import GatewayGuard

VALID_BATCH = {
    "mode": "all_or_nothing",
    "operations": [{"feature": "shipment-data", "shipment_id": "S1"}],
}


async def app(scope, receive, send):
    await PlainTextResponse("served")(scope, receive, send)


@pytest.fixture
async def guarded():
    transport = ASGITransport(app=GatewayGuard(app))
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_unmetered_path_passes(guarded):
    assert (await guarded.get("/usage")).text == "served"


async def test_reserved_request_passes(guarded):
    resp = await guarded.get(
        "/container_tracking", headers={"X-Meter-Reservation": "r1"}
    )
    assert resp.text == "served"


async def test_metered_request_without_gateway_is_refused(guarded):
    resp = await guarded.get("/container_tracking")
    assert resp.status_code == 500
    assert resp.json()["code"] == "meter_bypass"


async def test_invalid_batch_skip_reaches_the_app(guarded):
    resp = await guarded.post(
        "/batch", content=b"{broken", headers={"X-Meter-Skip": "invalid-body"}
    )
    assert resp.text == "served"


async def test_valid_batch_marked_invalid_is_refused(guarded):
    resp = await guarded.post(
        "/batch",
        content=json.dumps(VALID_BATCH),
        headers={"X-Meter-Skip": "invalid-body"},
    )
    assert resp.status_code == 500
    assert resp.json()["code"] == "meter_bypass"
