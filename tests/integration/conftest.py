"""Integration fixtures: run compose.yaml as project "quota-it" and talk to it over HTTP.

Overrides the in-process `client` and `redis_client` fixtures from tests/conftest.py,
so the same scenario files run against nginx + 3 replicas + real Redis.
"""

import asyncio
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from httpx import AsyncClient

from seed import load_orgs
from store import QuotaStore, connect, current_period

ROOT = Path(__file__).resolve().parents[2]
STAGE_PORT = 8090
REDIS_PORT = 6390
SETTLE_TIMEOUT = 2.0  # seconds a response waits for its commit/refund to land
COMPOSE = ["docker", "compose", "-p", "quota-it", "-f", str(ROOT / "compose.yaml")]
COMPOSE_ENV = {**os.environ, "STAGE_PORT": str(STAGE_PORT), "REDIS_PORT": str(REDIS_PORT)}


@pytest.fixture(scope="session")
def stack():
    if shutil.which("docker") is None:
        pytest.skip("docker not installed")
    try:
        subprocess.run([*COMPOSE, "up", "-d", "--build", "--wait"], env=COMPOSE_ENV, check=True)
        yield
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], env=COMPOSE_ENV, check=True)


@pytest.fixture
async def redis_client(stack):
    # 127.0.0.1, not localhost: Windows tries ::1 first and the port is bound IPv4-only
    r = connect(f"redis://127.0.0.1:{REDIS_PORT}/0")
    await r.flushdb()
    period, expire_at = current_period()
    pipe = r.pipeline()
    for org_id, limits in load_orgs().items():
        for feature, limit in limits.items():
            pipe.set(QuotaStore.limit_key(org_id, feature, period), limit, exat=expire_at)
            pipe.sadd(QuotaStore.features_key(org_id, period), feature)
    await pipe.execute()
    yield r
    await r.aclose()


@pytest.fixture
async def client(stack, redis_client):
    # MeterMiddleware commits/refunds after the response is sent (tests/README.md), so a
    # test could read Redis before that lands. Hold each response until its reservation
    # settles, the same guarantee in-process ASGITransport gives.
    async def settled(response):
        request_id = response.headers.get("X-Request-Id")
        # 409 request_in_progress: the reservation belongs to another request still running
        if request_id is None or response.status_code == 409:
            return
        org_id = response.request.headers["X-Org-Id"]
        key = QuotaStore.reservation_key(org_id, request_id)
        deadline = asyncio.get_running_loop().time() + SETTLE_TIMEOUT
        while await redis_client.hget(key, "state") == "reserved":
            if asyncio.get_running_loop().time() > deadline:
                return  # leave it to the test's own assertions to fail
            await asyncio.sleep(0.005)

    async with AsyncClient(
        base_url=f"http://127.0.0.1:{STAGE_PORT}", event_hooks={"response": [settled]}
    ) as c:
        yield c
