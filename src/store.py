import asyncio
import contextlib
import logging
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from exceptions import BY_STATUS, QuotaError, QuotaExceeded
from utils.periods import current_period

log = logging.getLogger(__name__)

# Reservation TTL doubles as the retry dedupe window.
RESERVATION_TTL = 300
USAGE_STREAM = "usage_events"
USAGE_STREAM_MAXLEN = 1_000_000
# Commit loop: commits queued meanwhile go out in one pipeline, at most this many per send.
COMMIT_BATCH = 100
# Bound on queued commits; when full, commit_later commits inline instead of growing memory.
COMMIT_QUEUE = 1000
# Seconds stop() waits for queued commits to flush on shutdown.
COMMIT_DRAIN_TIMEOUT = 5
# Tags each usage event with the instance that committed it; in compose, the replica's container id.
INSTANCE = socket.gethostname()

# Redis command per key type, for dump_org.
READERS = {"string": "get", "hash": "hgetall", "set": "smembers"}

# Lua scripts live in src/lua/; each file documents its KEYS, ARGV and return values.
LUA_DIR = Path(__file__).parent / "lua"


def load_lua(name: str) -> str:
    return (LUA_DIR / f"{name}.lua").read_text(encoding="utf-8")


def _from_url(cls: Any, url: str | None) -> Any:
    """One client config for the app, benchmarks and scripts. Env is read per call: main.py loads
    .env after importing this module."""
    return cls.from_url(
        url or os.environ.get("REDIS_URL") or "redis://127.0.0.1:6380/0",
        decode_responses=True,
        # Per process. redis-py's default (100) raises MaxConnectionsError instead of waiting once
        # exhausted, which surfaced as 500s under ~256 concurrent requests on one replica.
        max_connections=int(os.environ.get("REDIS_MAX_CONNECTIONS") or 512),
        # A dropped reply fails the call instead of hanging it (seen via Docker Desktop's
        # host.docker.internal).
        socket_timeout=5,
    )


def connect(url: str | None = None) -> Redis:
    return _from_url(Redis, url)


def connect_sync(url: str | None = None) -> redis.Redis:
    return _from_url(redis.Redis, url)


@dataclass
class Reservation:
    status: str
    used: int
    limit: int
    org_id: str
    feature: str
    request_id: str
    period: str
    usage_key: str
    reservation_key: str


class QuotaStore:
    def __init__(self, redis: Redis):
        self.redis = redis
        self._reserve = redis.register_script(load_lua("reserve"))
        self._commit = redis.register_script(load_lua("commit"))
        self._refund = redis.register_script(load_lua("refund"))
        self._commits: asyncio.Queue[Reservation] = asyncio.Queue(COMMIT_QUEUE)
        self._committer: asyncio.Task | None = None

    # {org_id} hash tag: all of an org's keys land in one Cluster slot
    @staticmethod
    def limit_key(org_id: str, feature: str, period: str) -> str:
        return f"quota:{{{org_id}}}:{feature}:limit:{period}"

    @staticmethod
    def usage_key(org_id: str, feature: str, period: str) -> str:
        return f"quota:{{{org_id}}}:{feature}:usage:{period}"

    @staticmethod
    def reservation_key(org_id: str, request_id: str) -> str:
        return f"res:{{{org_id}}}:{request_id}"

    @staticmethod
    def features_key(org_id: str, period: str) -> str:
        return f"quota:{{{org_id}}}:features:{period}"

    async def reserve(
        self, org_id: str, feature: str, request_id: str, cost: int, fingerprint: str
    ) -> Reservation:
        period, expire_at = current_period()
        usage_key = self.usage_key(org_id, feature, period)
        reservation_key = self.reservation_key(org_id, request_id)
        status, used, limit = await self._reserve(
            keys=[self.limit_key(org_id, feature, period), usage_key, reservation_key],
            args=[cost, fingerprint, expire_at, RESERVATION_TTL],
        )
        if status == "exceeded":
            raise QuotaExceeded(feature, int(used), int(limit))
        if status != "ok":
            exc = BY_STATUS.get(status)
            # reserve.lua grew a status this code doesn't know: fail as a JSON 500, not a KeyError
            raise exc() if exc else QuotaError(f"unexpected reserve status {status!r}")

        return Reservation(
            status=status,
            used=int(used),
            limit=int(limit),
            org_id=org_id,
            feature=feature,
            request_id=request_id,
            period=period,
            usage_key=usage_key,
            reservation_key=reservation_key,
        )

    # Both return False when the reservation is no longer 'reserved': in the
    # middleware that means it expired mid-request (RESERVATION_TTL), so the
    # charge stays on the counter with no usage event / no refund.
    @staticmethod
    def _commit_argv(r: Reservation) -> list:
        """commit.lua KEYS then ARGV."""
        return [
            r.reservation_key,
            USAGE_STREAM,
            r.org_id,
            r.feature,
            r.request_id,
            r.period,
            USAGE_STREAM_MAXLEN,
            INSTANCE,
        ]

    async def commit(self, r: Reservation) -> bool:
        argv = self._commit_argv(r)
        ok = await self._commit(keys=argv[:2], args=argv[2:])
        if not ok:
            log.warning("commit skipped, reservation not live: %s", r.reservation_key)
        return bool(ok)

    async def commit_later(self, r: Reservation) -> None:
        """Queue a commit for the commit loop; commits inline if it isn't running or is full."""
        if self._committer is None:
            await self.commit(r)
            return
        try:
            self._commits.put_nowait(r)
        except asyncio.QueueFull:
            await self.commit(r)

    def start(self) -> None:
        self._committer = asyncio.create_task(self._commit_loop())

    async def stop(self) -> None:
        """Flush queued commits, then stop the loop. Whatever's left after the timeout is lost
        (those reservations expire with the charge kept, like any failed commit)."""
        if self._committer is None:
            return
        try:
            async with asyncio.timeout(COMMIT_DRAIN_TIMEOUT):
                await self._commits.join()
        except TimeoutError:
            log.error("commit loop stopped with %d unflushed", self._commits.qsize())
        self._committer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._committer
        self._committer = None

    async def _commit_loop(self) -> None:
        # No timer: wait for one commit, take whatever queued up meanwhile. Idle, a batch is
        # one commit sent at once; under load, batches grow while the previous send is out.
        while True:
            batch = [await self._commits.get()]
            while len(batch) < COMMIT_BATCH and not self._commits.empty():
                batch.append(self._commits.get_nowait())
            try:
                await self._flush(batch)
            except Exception:
                log.exception("commit batch of %d failed", len(batch))
            finally:
                for _ in batch:
                    self._commits.task_done()

    async def _flush(self, batch: list[Reservation]) -> None:
        # One send for the batch; Redis still runs each commit.lua atomically on its own.
        # Raw EVALSHA, not the Script: a Script in a pipeline adds a SCRIPT EXISTS round trip
        # per execute, which costs as much as it saves when batches are small.
        async with self.redis.pipeline(transaction=False) as pipe:
            for r in batch:
                pipe.evalsha(self._commit.sha, 2, *self._commit_argv(r))
            results = await pipe.execute(raise_on_error=False)
        for r, ok in zip(batch, results, strict=True):
            if isinstance(
                ok, NoScriptError
            ):  # script cache gone (Redis restart): load it
                await self.commit(r)
            elif isinstance(ok, Exception):
                log.error("commit failed: %s: %s", r.reservation_key, ok)
            elif not ok:
                log.warning(
                    "commit skipped, reservation not live: %s", r.reservation_key
                )

    async def refund(self, r: Reservation) -> bool:
        ok = await self._refund(keys=[r.reservation_key, r.usage_key])
        if not ok:
            log.warning("refund skipped, reservation not live: %s", r.reservation_key)
        return bool(ok)

    async def get_usage(self, org_id: str):
        period, _ = current_period()
        year, month = map(int, period.split("-"))
        features = [
            f async for f in self.redis.sscan_iter(self.features_key(org_id, period))
        ]
        keys = [
            k
            for f in features
            for k in (
                self.limit_key(org_id, f, period),
                self.usage_key(org_id, f, period),
            )
        ]
        values = await self.redis.mget(keys) if keys else []
        return {
            "org_id": org_id,
            "period": period,
            # Start of next calendar month, UTC.
            "resets_at": f"{year + month // 12}-{month % 12 + 1:02d}-01T00:00:00Z",
            "features": {
                f: {
                    "used": int(values[2 * i + 1] or 0),
                    "limit": int(values[2 * i] or 0),
                }
                for i, f in enumerate(features)
            },
        }

    async def dump_org(self, org_id: str) -> dict:
        """Every key an org owns with type, TTL and value. Dev/admin only: SCANs the whole keyspace."""
        org = "".join(
            "\\" + c if c in "*?[]\\" else c for c in org_id
        )  # escape glob chars so one org can't match others
        keys = sorted(
            [
                k
                for pattern in (f"quota:{{{org}}}:*", f"res:{{{org}}}:*")
                async for k in self.redis.scan_iter(match=pattern, count=1000)
            ]
        )
        pipe = self.redis.pipeline(transaction=False)
        for k in keys:
            pipe.type(k)
            pipe.ttl(k)
        meta = await pipe.execute()
        types, ttls = meta[::2], meta[1::2]

        pipe = self.redis.pipeline(transaction=False)
        for k, t in zip(keys, types, strict=True):
            getattr(pipe, READERS.get(t, "type"))(k)
        values = await pipe.execute()

        return {
            "org_id": org_id,
            "count": len(keys),
            "keys": {
                k: {"type": t, "ttl": ttl, "value": sorted(v) if t == "set" else v}
                for k, t, ttl, v in zip(keys, types, ttls, values, strict=True)
            },
        }
