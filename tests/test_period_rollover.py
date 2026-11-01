import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import main
import store
from store import QuotaStore
from utils import periods

ORG, FEATURE = "acme", "container-tracking"
PATH = "/container_tracking"
PARAMS = {"container_id": "MSCU1719583"}

# Far future: fakeredis expires keys on the real clock, so a past period's
# EXPIREAT would delete the keys the moment they're written.
JAN = datetime(2099, 1, 31, 23, 59, tzinfo=UTC)
FEB = datetime(2099, 2, 1, 0, 0, 1, tzinfo=UTC)


class Clock:
    def __init__(self, now: datetime):
        self.now = now


@pytest.fixture
def clock(monkeypatch):
    """Pin the period QuotaStore sees; move it with clock.now = ..."""
    c = Clock(JAN)
    real = store.current_period
    monkeypatch.setattr(store, "current_period", lambda now=None: real(now or c.now))
    return c


async def provision(r, now: datetime, limit: int) -> None:
    """Renewal job stand-in: write one period's limit, like seed.write_limits."""
    period, expire_at = store.current_period(now)
    await r.set(QuotaStore.limit_key(ORG, FEATURE, period), limit, exat=expire_at)
    await r.sadd(QuotaStore.features_key(ORG, period), FEATURE)
    await r.expireat(QuotaStore.features_key(ORG, period), expire_at)


async def used(r, now: datetime) -> int | None:
    period, _ = store.current_period(now)
    value = await r.get(QuotaStore.usage_key(ORG, FEATURE, period))
    return None if value is None else int(value)


async def call(client):
    return await client.get(PATH, params=PARAMS, headers={"X-Org-Id": ORG})


async def test_rollover_starts_a_fresh_counter(client, redis_client, clock):
    await provision(redis_client, JAN, limit=2)
    await provision(redis_client, FEB, limit=2)

    assert (await call(client)).status_code == 200
    assert (await call(client)).status_code == 200
    resp = await call(client)
    assert resp.status_code == 429, resp.text

    clock.now = FEB
    assert (await call(client)).status_code == 200
    assert await used(redis_client, JAN) == 2  # old period left as it was
    assert await used(redis_client, FEB) == 1

    usage = (await client.get("/usage", headers={"X-Org-Id": ORG})).json()
    assert usage["period"] == "2099-02"
    assert usage["features"][FEATURE] == {"used": 1, "limit": 2}


async def test_rollover_before_renewal_is_unprovisioned(client, redis_client, clock):
    await provision(redis_client, JAN, limit=2)
    assert (await call(client)).status_code == 200

    # Renewal hasn't run for February yet (DESIGN Limitation 4a).
    clock.now = FEB
    resp = await call(client)
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "quota_not_provisioned"
    assert await used(redis_client, FEB) is None  # rejection writes nothing

    await provision(redis_client, FEB, limit=5)
    assert (await call(client)).status_code == 200
    assert await used(redis_client, FEB) == 1


async def test_usage_counter_expires_after_period_end(client, redis_client, clock):
    await provision(redis_client, JAN, limit=2)
    assert (await call(client)).status_code == 200

    period, expire_at = store.current_period(JAN)
    assert expire_at == int(
        datetime(2099, 2, 2, tzinfo=UTC).timestamp()
    )  # +1 day grace
    ttl = await redis_client.ttl(QuotaStore.usage_key(ORG, FEATURE, period))
    assert abs(ttl - (expire_at - time.time())) < 5


async def test_refund_after_rollover_returns_units_to_original_period(
    redis_client, clock
):
    await provision(redis_client, JAN, limit=2)
    await provision(redis_client, FEB, limit=2)

    # Request reserved just before midnight, fails just after.
    reservation = await main.quota_store.reserve(ORG, FEATURE, "late", 1, "fp")
    assert reservation.status == "ok"
    clock.now = FEB
    await main.quota_store.refund(reservation)

    assert await used(redis_client, JAN) == 0
    assert await used(redis_client, FEB) is None  # refund didn't touch the new period


@pytest.fixture
def fake_time(monkeypatch):
    """Drive current_period()'s cached clock path; move it with fake_time.now = ..."""
    c = Clock(JAN)
    monkeypatch.setattr(
        periods, "time", SimpleNamespace(time=lambda: c.now.timestamp())
    )
    monkeypatch.setattr(periods, "_cache", (0.0, 0.0, ("", 0)))
    return c


@pytest.mark.parametrize(
    "before, after",
    [
        (
            datetime(2099, 1, 31, 23, 59, 59, tzinfo=UTC),
            datetime(2099, 2, 1, tzinfo=UTC),
        ),
        (
            datetime(2099, 12, 31, 23, 59, 59, tzinfo=UTC),
            datetime(2100, 1, 1, tzinfo=UTC),
        ),
    ],
)
def test_cached_period_rolls_over_at_month_boundary(fake_time, before, after):
    fake_time.now = before
    assert periods.current_period() == periods.current_period(before)

    fake_time.now = after  # first call past the boundary, no invalidation step
    assert periods.current_period() == periods.current_period(after)
    assert periods.current_period()[0] == after.strftime("%Y-%m")


def test_cached_period_recomputes_once_per_month(fake_time, monkeypatch):
    calls = []
    real = periods._period
    monkeypatch.setattr(periods, "_period", lambda now: calls.append(now) or real(now))

    for day in (1, 15, 31):
        fake_time.now = datetime(2099, 1, day, tzinfo=UTC)
        assert periods.current_period()[0] == "2099-01"
    assert len(calls) == 1

    fake_time.now = FEB
    assert periods.current_period()[0] == "2099-02"
    assert len(calls) == 2


def test_cached_period_follows_clock_going_backwards(fake_time):
    fake_time.now = FEB
    assert periods.current_period()[0] == "2099-02"

    fake_time.now = JAN  # e.g. an NTP correction across midnight
    assert periods.current_period()[0] == "2099-01"
