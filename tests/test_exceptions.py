"""Every rejection reaches the client with the right status, code and X-Request-Id.

store.reserve raises a QuotaError per reserve.lua status; MeterMiddleware turns it into the
response. Rejections must charge nothing.
"""

import pytest

import main
from exceptions import (
    BY_STATUS,
    AlreadyProcessed,
    QuotaError,
    QuotaExceeded,
    QuotaNotProvisioned,
    RequestIdReused,
    RequestInProgress,
)
from middlewares.meter import MAX_REQUEST_ID_LEN, REQUEST_ID_HEADER, fingerprint
from store import USAGE_STREAM, QuotaStore, current_period

CT, FEATURE = "/container_tracking", "container-tracking"
CONTAINER = {"container_id": "MSCU1719583"}


async def call(client, org="acme", request_id=None, params=CONTAINER):
    headers = {"X-Org-Id": org}
    if request_id:
        headers[REQUEST_ID_HEADER] = request_id
    return await client.get(CT, params=params, headers=headers)


async def used(redis_client, org) -> int:
    period, _ = current_period()
    return int(await redis_client.get(QuotaStore.usage_key(org, FEATURE, period)) or 0)


def assert_rejected(resp, exc: type[QuotaError], request_id: str):
    assert resp.status_code == exc.status, resp.text
    assert resp.json()["code"] == exc.code
    assert resp.headers[REQUEST_ID_HEADER] == request_id


# --- store: reserve.lua status -> exception ---


@pytest.mark.parametrize(
    "status, exc",
    [
        ("reserved", RequestInProgress),
        ("committed", AlreadyProcessed),
        ("mismatch", RequestIdReused),
        ("unprovisioned", QuotaNotProvisioned),
        ("exceeded", QuotaExceeded),
    ],
)
async def test_reserve_raises_per_status(redis_client, monkeypatch, status, exc):
    store = QuotaStore(redis_client)

    async def fake_reserve(**_):
        return status, 7, 5

    monkeypatch.setattr(store, "_reserve", fake_reserve)
    with pytest.raises(exc) as info:
        await store.reserve("acme", FEATURE, "r", 1, "fp")
    assert type(info.value) is exc


async def test_unknown_status_is_json_500(client, monkeypatch):
    async def fake_reserve(**_):
        return "bogus", 0, 0

    monkeypatch.setattr(main.quota_store, "_reserve", fake_reserve)
    resp = await call(client, request_id="r")

    assert_rejected(resp, QuotaError, "r")
    assert resp.status_code == 500
    assert resp.json()["detail"] == "unexpected reserve status 'bogus'"


def test_every_status_has_distinct_http_status_and_code():
    assert set(BY_STATUS) == {
        "reserved",
        "committed",
        "mismatch",
        "unprovisioned",
        "exceeded",
    }
    assert all(exc.status != 500 for exc in BY_STATUS.values())
    assert len({exc.code for exc in BY_STATUS.values()}) == len(BY_STATUS)


async def test_exceeded_carries_counts(redis_client):
    store = QuotaStore(redis_client)
    await store.reserve("tiny", FEATURE, "first", 1, "fp")

    with pytest.raises(QuotaExceeded) as info:
        await store.reserve("tiny", FEATURE, "second", 1, "fp")
    assert (info.value.feature, info.value.used, info.value.limit) == (FEATURE, 1, 1)


# --- HTTP: what the client sees ---


def make(exc: type[QuotaError]) -> QuotaError:
    return exc(FEATURE, 3, 3) if exc is QuotaExceeded else exc()


# Base QuotaError too: the middleware must catch the base, not a list of subclasses.
@pytest.mark.parametrize("exc", [*BY_STATUS.values(), QuotaError])
async def test_raised_error_becomes_response(client, monkeypatch, exc):
    raised = make(exc)

    async def reject(*_):
        raise raised

    monkeypatch.setattr(main.quota_store, "reserve", reject)
    resp = await call(client, request_id="r")

    assert_rejected(resp, exc, "r")
    assert resp.json() == {"detail": raised.detail, "code": exc.code}


async def test_in_progress_409(client, redis_client):
    scope = {"method": "GET", "path": CT, "query_string": b"container_id=MSCU1719583"}
    await QuotaStore(redis_client).reserve(
        "acme", FEATURE, "slow", 1, fingerprint(scope, b"")
    )

    assert_rejected(await call(client, request_id="slow"), RequestInProgress, "slow")
    assert await used(redis_client, "acme") == 1


async def test_already_processed_409(client, redis_client):
    assert (await call(client, request_id="a")).status_code == 200

    assert_rejected(await call(client, request_id="a"), AlreadyProcessed, "a")
    assert await used(redis_client, "acme") == 1


async def test_request_id_reused_422(client, redis_client):
    assert (await call(client, request_id="a")).status_code == 200

    resp = await call(client, request_id="a", params={"container_id": "MSCU3668136"})
    assert_rejected(resp, RequestIdReused, "a")
    assert await used(redis_client, "acme") == 1


async def test_unprovisioned_403(client, redis_client):
    assert_rejected(await call(client, "noquota", "n"), QuotaNotProvisioned, "n")
    assert await used(redis_client, "noquota") == 0
    assert not await redis_client.exists(QuotaStore.reservation_key("noquota", "n"))


async def test_exceeded_429(client, redis_client):
    assert (await call(client, "tiny")).status_code == 200

    resp = await call(client, "tiny", "over")
    assert_rejected(resp, QuotaExceeded, "over")
    assert resp.json()["detail"] == f"quota exceeded for {FEATURE}: 1/1"
    assert await used(redis_client, "tiny") == 1
    assert not await redis_client.exists(QuotaStore.reservation_key("tiny", "over"))


async def test_missing_org_401(client):
    resp = await client.get(CT, params=CONTAINER)
    assert resp.status_code == 401, resp.text
    assert "X-org-ID" in resp.json()["detail"]


async def test_request_id_too_long_400(client, redis_client):
    resp = await call(client, request_id="x" * (MAX_REQUEST_ID_LEN + 1))
    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == "invalid_request_id"
    assert await used(redis_client, "acme") == 0


async def test_mixed_batch_501(client, redis_client):
    body = {
        "mode": "all_or_nothing",
        "operations": [
            {"feature": "container-tracking", "container_id": "MSCU1719583"},
            {"feature": "shipment-data", "shipment_id": "SHP-00001"},
        ],
    }
    resp = await client.post(
        "/batch", json=body, headers={"X-Org-Id": "acme", REQUEST_ID_HEADER: "mix"}
    )
    assert resp.status_code == 501, resp.text
    assert resp.json()["code"] == "mixed_batch_unsupported"
    assert resp.headers[REQUEST_ID_HEADER] == "mix"
    assert await used(redis_client, "acme") == 0


async def test_malformed_batch_422_not_charged(client, redis_client):
    resp = await client.post(
        "/batch", json={"operations": []}, headers={"X-Org-Id": "acme"}
    )
    assert resp.status_code == 422, resp.text
    assert await used(redis_client, "acme") == 0


def tracking_batch(*container_ids):
    ops = [{"feature": FEATURE, "container_id": cid} for cid in container_ids]
    return {"mode": "all_or_nothing", "operations": ops}


async def test_batch_runs_every_op_in_order(client, redis_client):
    ids = ["MAEU0109031", "MSCU1719583", "MSCU3668136"]
    resp = await client.post(
        "/batch", json=tracking_batch(*ids), headers={"X-Org-Id": "acme"}
    )
    assert resp.status_code == 200, resp.text
    assert [r["container_id"] for r in resp.json()["results"]] == ids
    assert await used(redis_client, "acme") == 3


async def test_batch_one_unknown_op_fails_and_refunds_all(client, redis_client):
    body = tracking_batch("MSCU1719583", "XXXU0000000", "MSCU1719583")
    resp = await client.post(
        "/batch", json=body, headers={"X-Org-Id": "acme", REQUEST_ID_HEADER: "miss"}
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"].startswith("operation 1:")
    assert await used(redis_client, "acme") == 0
    assert not await redis_client.exists(QuotaStore.reservation_key("acme", "miss"))
    assert await redis_client.xlen(USAGE_STREAM) == 0
