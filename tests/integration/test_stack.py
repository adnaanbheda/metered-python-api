import asyncio
from collections import Counter

import pytest

from store import USAGE_STREAM, QuotaStore, current_period
from test_scenarios import test_scenario  # noqa: F401  re-run every scenario over HTTP

pytestmark = pytest.mark.integration


async def test_burst_across_replicas_charges_exactly_the_limit(client, redis_client):
    org, feature, limit, burst = "burst", "container-tracking", 10, 50
    period, expire_at = current_period()
    await redis_client.set(QuotaStore.limit_key(org, feature, period), limit, exat=expire_at)
    await redis_client.sadd(QuotaStore.features_key(org, period), feature)

    responses = await asyncio.gather(
        *(
            client.get("/container_tracking", params={"container_id": "MSCU1719583"}, headers={"X-Org-Id": org})
            for _ in range(burst)
        )
    )

    assert Counter(r.status_code for r in responses) == {200: limit, 429: burst - limit}
    assert int(await redis_client.get(QuotaStore.usage_key(org, feature, period))) == limit
    assert await redis_client.xlen(USAGE_STREAM) == limit
    # otherwise this only proved one process is atomic with itself
    assert len({r.headers["X-Upstream"] for r in responses}) > 1


async def test_retry_on_another_replica_is_deduped(client, redis_client):
    headers = {"X-Org-Id": "acme", "X-Request-Id": "retry-1"}
    params = {"container_id": "MSCU1719583"}

    first = await client.get("/container_tracking", params=params, headers=headers)
    retries = [await client.get("/container_tracking", params=params, headers=headers) for _ in range(3)]

    assert first.status_code == 200
    assert {r.status_code for r in retries} == {409}
    # round-robin puts at least one retry on a different replica than the first call
    assert {r.headers["X-Upstream"] for r in retries} - {first.headers["X-Upstream"]}
    period, _ = current_period()
    assert int(await redis_client.get(QuotaStore.usage_key("acme", "container-tracking", period))) == 1
