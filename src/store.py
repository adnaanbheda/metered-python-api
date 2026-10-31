from dataclasses import dataclass
from pathlib import Path

from redis.asyncio import Redis

from exceptions import BY_STATUS, QuotaExceeded
from utils.periods import current_period

# Reservation TTL doubles as the retry dedupe window.
RESERVATION_TTL = 300
USAGE_STREAM = "usage_events"
USAGE_STREAM_MAXLEN = 1_000_000

# Redis command per key type, for dump_org.
READERS = {"string": "get", "hash": "hgetall", "set": "smembers"}

# Lua scripts live in src/lua/; each file documents its KEYS, ARGV and return values.
LUA_DIR = Path(__file__).parent / "lua"


def load_lua(name: str) -> str:
    return (LUA_DIR / f"{name}.lua").read_text(encoding="utf-8")


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
            raise BY_STATUS[status]()

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

    async def commit(self, r: Reservation) -> None:
        await self._commit(
            keys=[r.reservation_key, USAGE_STREAM],
            args=[r.org_id, r.feature, r.request_id, r.period, USAGE_STREAM_MAXLEN],
        )

    async def refund(self, r: Reservation) -> None:
        await self._refund(keys=[r.reservation_key, r.usage_key])

    @staticmethod
    def features_key(org_id: str, period: str) -> str:
        return f"quota:{{{org_id}}}:features:{period}"

    async def get_usage(self, org_id: str):
        period, _ = current_period()
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
