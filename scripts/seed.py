"""Dev tooling: load quota fixtures from mocks/orgs.json into Redis.

Run from repo root with src on the path:
    $env:PYTHONPATH="src"; uv run python scripts/seed.py seed|remove|reset [--period YYYY-MM]
"""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import redis

from store import USAGE_STREAM, QuotaStore, current_period

ORGS_FILE = Path(__file__).resolve().parents[1] / "mocks" / "orgs.json"
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6380/0")


def period_for(label: str | None) -> tuple[str, int]:
    if label is None:
        return current_period()
    year, month = map(int, label.split("-"))
    return current_period(datetime(year, month, 1, tzinfo=UTC))


def load_orgs() -> dict[str, dict[str, int]]:
    return json.loads(ORGS_FILE.read_text())


def write_limits(r: redis.Redis, org_id: str, limits: dict[str, int], period: str, expire_at: int) -> None:
    features_key = QuotaStore.features_key(org_id, period)
    pipe = r.pipeline()
    for feature, limit in limits.items():
        pipe.set(QuotaStore.limit_key(org_id, feature, period), limit, exat=expire_at)
        pipe.sadd(features_key, feature)
    pipe.expireat(features_key, expire_at)
    pipe.execute()


def seed(r: redis.Redis, label: str | None) -> None:
    period, expire_at = period_for(label)
    for org_id, limits in load_orgs().items():
        write_limits(r, org_id, limits, period, expire_at)
        print(f"{org_id:10} {period}  {limits or 'no limits (unprovisioned)'}")


def remove(r: redis.Redis, label: str | None) -> None:
    period, _ = period_for(label)
    keys = list(r.scan_iter(match=f"quota:*:{period}"))
    if keys:
        r.unlink(*keys)
    print(f"removed {period}: {len(keys)} keys")


def reset(r: redis.Redis, label: str | None) -> None:
    keys = [*r.scan_iter(match="quota:*"), *r.scan_iter(match="res:*")]
    if keys:
        r.unlink(*keys)
    r.unlink(USAGE_STREAM)
    print(f"reset {REDIS_URL}: removed {len(keys)} keys + {USAGE_STREAM}")
    seed(r, label)


COMMANDS = {"seed": seed, "remove": remove, "reset": reset}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "command",
        choices=COMMANDS,
        help="seed: load limits; remove: delete one period's keys; reset: wipe everything, then load",
    )
    p.add_argument("--period", help="YYYY-MM, default current month (required for remove)")
    args = p.parse_args()
    if args.command == "remove" and args.period is None:
        p.error("remove needs --period")
    COMMANDS[args.command](redis.Redis.from_url(REDIS_URL, decode_responses=True), args.period)


if __name__ == "__main__":
    main()
