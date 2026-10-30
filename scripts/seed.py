"""Dev tooling: seed quota fixtures into Redis and generate mock response data.

Run from repo root with src on the path:
    $env:PYTHONPATH="src"; uv run python scripts/seed.py <command>
"""

import argparse
import json
import os
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import redis

from store import USAGE_STREAM, QuotaStore, current_period

MOCKS = Path(__file__).resolve().parents[1] / "mocks"
ORGS_FILE = MOCKS / "orgs.json"
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6380/0")

FEATURES = ("container-tracking", "sailing-schedule", "shipment-data")
PORTS = {
    "CNSHA": "Shanghai",
    "SGSIN": "Singapore",
    "NLRTM": "Rotterdam",
    "DEHAM": "Hamburg",
    "USLAX": "Los Angeles",
    "AEJEA": "Jebel Ali",
    "INNSA": "Nhava Sheva",
}
CARRIERS = {"MSC": "MSCU", "Maersk": "MAEU", "CMA CGM": "CMAU", "Hapag-Lloyd": "HLCU"}
VESSELS = ["AURORA", "MERIDIAN", "HORIZON", "ATLAS", "SOLSTICE"]
COMPANIES = [
    "Blue Harbor Foods",
    "Kestrel Electronics",
    "Ironleaf Textiles",
    "Sunmere Furniture",
    "Polar Ridge Auto",
]
EVENTS = [
    "gate_out",
    "loaded",
    "departed",
    "transshipped",
    "arrived",
    "discharged",
    "gate_in",
]
ORIGIN_EVENTS = {"gate_out", "loaded", "departed"}

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")

# Each step: (org_id, request_id, path with query, expected status). Rename slots once they settle.
SCENARIOS: dict[str, list[tuple[str, str, str, int]]] = {
    "slot_1": [
        ("acme", "a", "/container_tracking?container_id=X", 200),
    ],
    "slot_2": [
        ("acme", "a", "/container_tracking?container_id=X", 200),
        ("acme", "a", "/container_tracking?container_id=X", 409),
    ],
    "slot_3": [
        ("acme", "a", "/container_tracking?container_id=X", 200),
        ("acme", "a", "/container_tracking?container_id=Y", 422),
    ],
    "slot_4": [
        ("tiny", "a", "/container_tracking?container_id=X", 200),
        ("tiny", "b", "/container_tracking?container_id=X", 429),
    ],
    "slot_5": [
        ("noquota", "a", "/container_tracking?container_id=X", 403),
    ],
}


def period_for(label: str | None) -> tuple[str, int]:
    if label is None:
        return current_period()
    year, month = map(int, label.split("-"))
    return current_period(datetime(year, month, 1, tzinfo=UTC))


def load_orgs() -> dict[str, dict[str, int]]:
    return json.loads(ORGS_FILE.read_text())


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n")


def write_limits(
    r: redis.Redis, org_id: str, limits: dict[str, int], period: str, expire_at: int
) -> None:
    features_key = QuotaStore.features_key(org_id, period)
    pipe = r.pipeline()
    for feature, limit in limits.items():
        pipe.set(QuotaStore.limit_key(org_id, feature, period), limit, exat=expire_at)
        pipe.sadd(features_key, feature)
    pipe.expireat(features_key, expire_at)
    pipe.execute()


def cmd_reset(r: redis.Redis, args) -> None:
    keys = [*r.scan_iter(match="quota:*"), *r.scan_iter(match="res:*")]
    if keys:
        r.unlink(*keys)
    r.unlink(USAGE_STREAM)
    print(f"reset {REDIS_URL}: removed {len(keys)} keys + {USAGE_STREAM}")


def cmd_seed(r: redis.Redis, args) -> None:
    if args.reset:
        cmd_reset(r, args)
    period, expire_at = period_for(args.period)
    orgs = load_orgs()
    if args.org:
        orgs = {args.org: orgs[args.org]}
    for org_id, limits in orgs.items():
        write_limits(r, org_id, limits, period, expire_at)
        print(f"{org_id:10} {period}  {limits or 'no limits (unprovisioned)'}")


def cmd_set_limit(r: redis.Redis, args) -> None:
    orgs = load_orgs()
    orgs.setdefault(args.org, {})[args.feature] = args.limit
    write_json(ORGS_FILE, orgs)
    period, expire_at = period_for(args.period)
    write_limits(r, args.org, {args.feature: args.limit}, period, expire_at)
    print(
        f"{args.org} {args.feature} = {args.limit} for {period} (saved to {ORGS_FILE.name})"
    )


def cmd_show(r: redis.Redis, args) -> None:
    period, _ = period_for(args.period)
    rows: dict[str, dict[str, int]] = {}
    # *:*: skips the 4-segment features index key
    for key in r.scan_iter(match=f"quota:{{{args.org}}}:*:*:{period}"):
        _, _, feature, kind, _ = key.split(":")
        rows.setdefault(feature, {})[kind] = int(r.get(key))
    if not rows:
        print(f"{args.org}: nothing in Redis for {period}")
    for feature, v in sorted(rows.items()):
        print(f"{feature:20} {v.get('usage', 0):>6} / {v.get('limit', '-')}")


def cmd_gen(r: redis.Redis, args) -> None:
    rng = random.Random(args.seed)
    start = datetime(2026, 10, 1, tzinfo=UTC)
    ports = list(PORTS)

    containers = {}
    for _ in range(args.containers):
        carrier, prefix = rng.choice(list(CARRIERS.items()))
        origin, destination = rng.sample(ports, 2)
        ts = start + timedelta(days=rng.randint(0, 20))
        events = []
        for event in EVENTS[: rng.randint(1, len(EVENTS))]:
            ts += timedelta(hours=rng.randint(6, 72))
            if event in ORIGIN_EVENTS:
                port = origin
            elif event == "transshipped":
                port = rng.choice([p for p in ports if p not in (origin, destination)])
            else:
                port = destination
            events.append({"ts": ts.isoformat(), "event": event, "port": port})
        cid = f"{prefix}{rng.randrange(10**7):07d}"
        containers[cid] = {
            "container_id": cid,
            "carrier": carrier,
            "origin": origin,
            "destination": destination,
            "status": events[-1]["event"],
            "events": events,
        }

    sailings = []
    for origin in ports:
        for destination in ports:
            if origin == destination:
                continue
            for _ in range(args.sailings_per_lane):
                carrier = rng.choice(list(CARRIERS))
                departure = start + timedelta(days=rng.randint(0, 60))
                transit = rng.randint(7, 40)
                sailings.append(
                    {
                        "carrier": carrier,
                        "vessel": f"{carrier.split()[0].upper()} {rng.choice(VESSELS)}",
                        "voyage": f"{rng.randint(100, 999)}{rng.choice('EWNS')}",
                        "origin": origin,
                        "destination": destination,
                        "departure": departure.date().isoformat(),
                        "arrival": (departure + timedelta(days=transit))
                        .date()
                        .isoformat(),
                        "transit_days": transit,
                    }
                )
    sailings.sort(key=lambda s: s["departure"])

    ids = list(containers)
    shipments = {}
    for i in range(1, args.shipments + 1):
        sid = f"SHP-{i:05d}"
        cids = rng.sample(ids, rng.randint(1, min(3, len(ids))))
        first = containers[cids[0]]
        shipments[sid] = {
            "shipment_id": sid,
            "shipper": rng.choice(COMPANIES),
            "consignee": rng.choice(COMPANIES),
            "origin": first["origin"],
            "destination": first["destination"],
            "containers": cids,
            "status": first["status"],
        }

    MOCKS.mkdir(exist_ok=True)
    write_json(MOCKS / "containers.json", containers)
    write_json(MOCKS / "sailings.json", sailings)
    write_json(MOCKS / "shipments.json", shipments)
    print(
        f"wrote {len(containers)} containers, {len(sailings)} sailings, {len(shipments)} shipments to {MOCKS}"
    )
    print("sample container ids:", ", ".join(ids[:3]))
    print("sample shipment ids: ", ", ".join(list(shipments)[:3]))


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("seed", help="load mocks/orgs.json limits into Redis")
    s.add_argument("--org", help="seed one org only")
    s.add_argument("--period", help="YYYY-MM, default current month")
    s.add_argument(
        "--reset", action="store_true", help="wipe quota/reservation keys first"
    )
    s.set_defaults(fn=cmd_seed)

    s = sub.add_parser("set-limit", help="set one limit in Redis and orgs.json")
    s.add_argument("org")
    s.add_argument("feature", choices=FEATURES)
    s.add_argument("limit", type=int)
    s.add_argument("--period", help="YYYY-MM, default current month")
    s.set_defaults(fn=cmd_set_limit)

    s = sub.add_parser("show", help="usage / limit per feature for an org")
    s.add_argument("org")
    s.add_argument("--period", help="YYYY-MM, default current month")
    s.set_defaults(fn=cmd_show)

    s = sub.add_parser("reset", help="delete all quota, reservation and stream keys")
    s.set_defaults(fn=cmd_reset)

    s = sub.add_parser("gen", help="generate mock response data into mocks/")
    s.add_argument("--containers", type=int, default=25)
    s.add_argument("--shipments", type=int, default=10)
    s.add_argument("--sailings-per-lane", type=int, default=2)
    s.add_argument("--seed", type=int, default=42)
    s.set_defaults(fn=cmd_gen)

    args = p.parse_args()
    args.fn(redis.Redis.from_url(REDIS_URL, decode_responses=True), args)


if __name__ == "__main__":
    main()
