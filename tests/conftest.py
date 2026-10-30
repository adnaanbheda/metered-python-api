import os

# main builds a Redis client at import time; it never connects, the fixture swaps in fakeredis
os.environ["REDIS_URL"] = "redis://127.0.0.1:6380/15"

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient

import main
from main import app
from seed import load_orgs, write_limits
from store import current_period


@pytest.fixture
async def redis_client():
    # fresh in-memory server per test; sync handle for seed.write_limits, async for the app
    server = fakeredis.FakeServer()
    sync = fakeredis.FakeRedis(server=server, decode_responses=True)
    period, expire_at = current_period()
    for org_id, limits in load_orgs().items():
        write_limits(sync, org_id, limits, period, expire_at)
    r = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    # middlewares hold this exact instance, so rebind it in place
    main.quota_store.__init__(r)
    yield r
    await r.aclose()


@pytest.fixture
async def client(redis_client):  # requested only so the store is swapped first
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c
