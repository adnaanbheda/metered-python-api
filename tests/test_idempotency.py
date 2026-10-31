"""Idempotency cases the markdown runner can't express: they reach into Redis between requests.

Plain request sequences live in scenarios/idempotency.md.
"""

from middlewares.meter import REQUEST_ID_HEADER, fingerprint
from store import RESERVATION_TTL, QuotaStore, current_period

ORG, FEATURE = "acme", "container-tracking"
PATH = "/container_tracking"
PARAMS = {"container_id": "MSCU1719583"}


async def call(client, request_id: str | None = None):
    headers = {"X-Org-Id": ORG}
    if request_id:
        headers[REQUEST_ID_HEADER] = request_id
    return await client.get(PATH, params=PARAMS, headers=headers)


async def used(redis_client) -> int:
    period, _ = current_period()
    return int(await redis_client.get(QuotaStore.usage_key(ORG, FEATURE, period)) or 0)


async def test_retry_while_in_flight_gets_in_progress(client, redis_client):
    # The original is still running on another replica: reserved, not yet committed.
    store = QuotaStore(redis_client)
    scope = {"method": "GET", "path": PATH, "query_string": b"container_id=MSCU1719583"}
    original = await store.reserve(ORG, FEATURE, "slow", 1, fingerprint(scope, b""))
    assert original.status == "ok"

    resp = await call(client, "slow")
    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "request_in_progress"
    assert await used(redis_client) == 1  # only the original's charge

    # Original fails and refunds, so the next retry runs for real.
    await store.refund(original)
    assert (await call(client, "slow")).status_code == 200
    assert await used(redis_client) == 1


async def test_dedupe_window_expires(client, redis_client):
    assert (await call(client, "w")).status_code == 200
    key = QuotaStore.reservation_key(ORG, "w")
    assert 0 < await redis_client.ttl(key) <= RESERVATION_TTL

    # Stand-in for waiting out the TTL.
    await redis_client.delete(key)
    assert (await call(client, "w")).status_code == 200
    assert await used(redis_client) == 2  # past the window a retry is a new charge


async def test_request_id_echoed(client):
    given = await call(client, "echo-me")
    assert given.headers[REQUEST_ID_HEADER] == "echo-me"

    # Rejections echo it too, so the client knows which id to keep retrying with.
    again = await call(client, "echo-me")
    assert again.status_code == 409
    assert again.headers[REQUEST_ID_HEADER] == "echo-me"

    # No id sent: the server makes one up and returns it for the client to retry with.
    generated = await call(client)
    assert generated.status_code == 200
    assert generated.headers[REQUEST_ID_HEADER]
