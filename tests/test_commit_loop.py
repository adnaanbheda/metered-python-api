"""QuotaStore's commit loop: commits queued by commit_later go out batched in one pipeline.

In-process app tests never start it (ASGITransport skips the lifespan), so commit_later commits
inline there; these tests start the loop on their own store.
"""

import asyncio

import pytest

import store
from store import USAGE_STREAM, QuotaStore

ORG, FEATURE = "globex", "container-tracking"


@pytest.fixture
async def qs(redis_client):
    s = QuotaStore(redis_client)
    s.start()
    yield s
    await s.stop()


@pytest.fixture
def batch_sizes(qs, monkeypatch):
    sizes = []
    real = qs._flush

    async def flush(batch):
        sizes.append(len(batch))
        await real(batch)

    monkeypatch.setattr(qs, "_flush", flush)
    return sizes


async def reserve(qs: QuotaStore, n: int) -> list[store.Reservation]:
    return [await qs.reserve(ORG, FEATURE, f"req-{i}", 1, "fp") for i in range(n)]


async def states(redis_client, reservations) -> set[str]:
    return {await redis_client.hget(r.reservation_key, "state") for r in reservations}


async def test_queued_commits_go_out_in_batches(qs, redis_client, batch_sizes):
    reservations = await reserve(qs, 250)
    # commit_later never yields on the queue path, so all 250 queue before the loop wakes
    for r in reservations:
        await qs.commit_later(r)
    await qs._commits.join()

    assert batch_sizes == [100, 100, 50]  # COMMIT_BATCH per send
    assert await states(redis_client, reservations) == {"committed"}
    assert await redis_client.xlen(USAGE_STREAM) == 250


async def test_lone_commit_goes_out_alone(qs, redis_client, batch_sizes):
    (r,) = await reserve(qs, 1)
    await qs.commit_later(r)
    await qs._commits.join()

    assert batch_sizes == [1]  # no timer: an idle loop doesn't wait for company
    assert await states(redis_client, [r]) == {"committed"}


async def test_stop_flushes_queued_commits(redis_client):
    qs = QuotaStore(redis_client)
    qs.start()
    reservations = await reserve(qs, 10)
    for r in reservations:
        await qs.commit_later(r)

    await qs.stop()
    assert await states(redis_client, reservations) == {"committed"}

    # stopped: commit_later commits inline again
    late = await qs.reserve(ORG, FEATURE, "late", 1, "fp")
    await qs.commit_later(late)
    assert await states(redis_client, [late]) == {"committed"}


async def test_full_queue_commits_inline(redis_client, monkeypatch):
    monkeypatch.setattr(store, "COMMIT_QUEUE", 2)
    qs = QuotaStore(redis_client)
    qs.start()
    reservations = await reserve(qs, 3)
    for r in reservations:
        await qs.commit_later(r)

    # the loop hasn't run yet: 2 queued, the 3rd didn't fit and committed inline
    assert qs._commits.qsize() == 2
    assert await states(redis_client, reservations[2:]) == {"committed"}
    await qs.stop()
    assert await states(redis_client, reservations) == {"committed"}


async def test_script_cache_loss_falls_back_to_loading_it(qs, redis_client):
    reservations = await reserve(qs, 3)
    await (
        redis_client.script_flush()
    )  # as after a Redis restart: AOF doesn't keep scripts
    for r in reservations:
        await qs.commit_later(r)
    await qs._commits.join()

    assert await states(redis_client, reservations) == {"committed"}
    assert await redis_client.xlen(USAGE_STREAM) == 3


async def test_failed_batch_doesnt_stop_the_loop(qs, redis_client, monkeypatch):
    real = qs._flush
    calls = 0

    async def flaky(batch):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("redis down")
        await real(batch)

    monkeypatch.setattr(qs, "_flush", flaky)
    lost, kept = await reserve(qs, 2)

    await qs.commit_later(lost)
    await qs._commits.join()
    await qs.commit_later(kept)
    await qs._commits.join()

    # the failed batch stays reserved (expires with the charge kept); the loop carried on
    assert await states(redis_client, [lost]) == {"reserved"}
    assert await states(redis_client, [kept]) == {"committed"}
    assert not qs._committer.done()


async def test_stop_gives_up_after_drain_timeout(redis_client, monkeypatch):
    monkeypatch.setattr(store, "COMMIT_DRAIN_TIMEOUT", 0.05)
    qs = QuotaStore(redis_client)
    qs.start()

    async def hang(batch):
        await asyncio.Event().wait()

    monkeypatch.setattr(qs, "_flush", hang)
    (r,) = await reserve(qs, 1)
    await qs.commit_later(r)

    await qs.stop()  # returns instead of hanging shutdown
    assert qs._committer is None
    assert await states(redis_client, [r]) == {"reserved"}
