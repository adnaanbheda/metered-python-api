"""Parallel requests against one org (manual checklist C1-C6).

Integration only: over nginx + replicas + real Redis, where the Lua scripts are what stops
separate processes racing each other. In-process they can't race: fakeredis never yields to
the event loop, so a gathered burst runs one request at a time (peak 1 live reservation).
"""

import asyncio
import re
from collections import Counter

import pytest

from store import USAGE_STREAM, QuotaStore, current_period

pytestmark = pytest.mark.integration

CT = "/container_tracking"
CONTAINER = {"container_id": "MSCU1719583"}
SD = "/shipment_data"
SHIPMENT = {"shipment_id": "SHP-00001"}


async def burst(client, org, n, path=CT, params=CONTAINER, request_id=None):
    headers = {"X-Org-Id": org}
    if request_id:
        headers["X-Request-Id"] = request_id
    return await asyncio.gather(
        *(client.get(path, params=params, headers=headers) for _ in range(n))
    )


async def used(redis_client, org, feature) -> int:
    period, _ = current_period()
    return int(await redis_client.get(QuotaStore.usage_key(org, feature, period)) or 0)


def statuses(responses) -> Counter:
    return Counter(r.status_code for r in responses)


async def provision(redis_client, org, feature, limit) -> None:
    period, expire_at = current_period()
    await redis_client.set(
        QuotaStore.limit_key(org, feature, period), limit, exat=expire_at
    )
    await redis_client.sadd(QuotaStore.features_key(org, period), feature)


def batch(client, org, cost):
    ops = [{"feature": "container-tracking", **CONTAINER}] * cost
    body = {"operations": ops, "mode": "all_or_nothing"}
    return client.post("/batch", json=body, headers={"X-Org-Id": org})


async def charged_events(redis_client, org) -> int:
    """Units the usage stream says were charged to org."""
    events = await redis_client.xrange(USAGE_STREAM)
    return sum(int(f["cost"]) for _, f in events if f["org_id"] == org)


def used_at_rejection(response) -> int:
    """The counter value reserve.lua saw when it returned 'exceeded' ("... used/limit")."""
    return int(re.search(r"(\d+)/\d+", response.json()["detail"])[1])


async def test_race_for_the_last_unit(client, redis_client):
    responses = await burst(client, "tiny", 20)  # tiny: container-tracking 1

    assert statuses(responses) == {200: 1, 429: 19}
    assert await used(redis_client, "tiny", "container-tracking") == 1
    assert await redis_client.xlen(USAGE_STREAM) == 1


async def test_race_near_the_limit(client, redis_client):
    responses = await burst(client, "acme", 30, SD, SHIPMENT)  # acme: shipment-data 20

    assert statuses(responses) == {200: 20, 429: 10}
    assert await used(redis_client, "acme", "shipment-data") == 20


@pytest.mark.parametrize("limit", [1, 2, 10, 100])
async def test_burst_never_overserves(client, redis_client, limit):
    await provision(redis_client, "race", "container-tracking", limit)

    responses = await burst(client, "race", 3 * limit + 5)

    assert statuses(responses) == {200: limit, 429: 2 * limit + 5}
    assert await used(redis_client, "race", "container-tracking") == limit
    assert await charged_events(redis_client, "race") == limit
    # rejected only once the quota was gone, never while a unit was left
    assert {used_at_rejection(r) for r in responses if r.status_code == 429} == {limit}


async def test_batches_race_near_the_limit(client, redis_client):
    # 11 units, batches of 3: three fit (9). The 2 left are one short of a fourth, so any slack
    # in the check lets it through.
    await provision(redis_client, "race", "container-tracking", 11)

    responses = await asyncio.gather(*(batch(client, "race", 3) for _ in range(12)))

    assert statuses(responses) == {200: 3, 429: 9}
    assert await used(redis_client, "race", "container-tracking") == 9
    assert await charged_events(redis_client, "race") == 9
    assert {used_at_rejection(r) for r in responses if r.status_code == 429} == {9}


async def test_mixed_costs_race_for_the_remainder(client, redis_client):
    # Who wins depends on timing; the invariants don't. Enough singles to fill any remainder,
    # so the quota always ends exactly full. Interleaved so batches also hit the edge, not only
    # an already-full counter.
    limit = 10
    await provision(redis_client, "race", "container-tracking", limit)
    single = lambda: client.get(CT, params=CONTAINER, headers={"X-Org-Id": "race"})  # noqa: E731
    calls = [
        c for _ in range(limit) for c in ((3, batch(client, "race", 3)), (1, single()))
    ]

    responses = await asyncio.gather(*(call for _, call in calls))
    outcomes = list(zip((cost for cost, _ in calls), responses, strict=True))

    assert sum(cost for cost, r in outcomes if r.status_code == 200) == limit
    assert await used(redis_client, "race", "container-tracking") == limit
    assert await charged_events(redis_client, "race") == limit
    assert {r.status_code for r in responses} <= {200, 429}
    for cost, r in outcomes:
        if r.status_code == 429:  # each rejection really didn't fit when it was checked
            assert used_at_rejection(r) + cost > limit


async def test_parallel_under_the_limit_all_charged(client, redis_client):
    responses = await burst(client, "acme", 50)  # acme: container-tracking 100

    assert statuses(responses) == {200: 50}
    assert await used(redis_client, "acme", "container-tracking") == 50
    assert await redis_client.xlen(USAGE_STREAM) == 50


async def test_parallel_features_dont_interfere(client, redis_client):
    tracking, shipments = await asyncio.gather(
        burst(client, "acme", 10), burst(client, "acme", 10, SD, SHIPMENT)
    )

    assert statuses([*tracking, *shipments]) == {200: 20}
    assert await used(redis_client, "acme", "container-tracking") == 10
    assert await used(redis_client, "acme", "shipment-data") == 10


async def test_parallel_failures_all_refunded(client, redis_client):
    responses = await burst(client, "acme", 20, params={"container_id": "XXXU0000000"})

    assert statuses(responses) == {404: 20}
    assert await used(redis_client, "acme", "container-tracking") == 0
    assert not [k async for k in redis_client.scan_iter(match="res:{acme}:*")]
    assert await redis_client.xlen(USAGE_STREAM) == 0


async def test_same_request_id_in_parallel_charges_once(client, redis_client):
    responses = await burst(client, "acme", 10, request_id="dup")

    assert statuses(responses) == {200: 1, 409: 9}
    # Which 409 depends on timing: the original still running or already done.
    assert {r.json()["code"] for r in responses if r.status_code == 409} <= {
        "request_in_progress",
        "already_processed",
    }
    assert await used(redis_client, "acme", "container-tracking") == 1
