"""Benchmark quota operations against a running Redis / stack.

    store  reserve+commit and reserve+refund straight against Redis: the cost the meter adds per request
    http   DEPRECATED, use oha (bench_suite.py B4-B6). Requests through nginx: unmetered baseline (/)
           vs a metered route. httpx saturates far below the stack, so it measures the client.

Provisions a throwaway "bench" org with an effectively unlimited quota, and deletes its keys afterwards.
Commits still append to the usage stream.

Run from repo root with src on the path:
    $env:PYTHONPATH="src"; $env:REDIS_URL="redis://127.0.0.1:6381/0"
    uv run python scripts/bench.py store -n 20000 -c 64
    uv run python scripts/bench.py store -n 5000 -c 16 --procs 8
    uv run python scripts/bench.py http -n 5000 -c 64 --base-url http://127.0.0.1:8080 --json bench.json

Closed loop: -c workers each send their next request as soon as the last one returns.
A single Python client tops out early (store: ~2.7k ops/s; http via httpx: a few hundred req/s on
Windows), so numbers near that measure the client. Use oha for HTTP throughput.
--procs N runs N such clients in parallel processes, each on its own org (bench-0, bench-1, ...) so
they never share a quota key. -n and -c are per process. All processes finish warming up before any
starts timing; ops/s is the combined total over the span from first start to last finish.

Open loop: --rate R offers R ops/s in total (R / procs per process), like oha -q, with at most -c in
flight per process. Latency is measured from each op's scheduled start, so time queued behind a
slow op counts (corrected for coordinated omission). If ops/s comes out below R, the client fell
behind and the latencies include its own queueing: add --procs.
    uv run python scripts/bench.py store --op refund --rate 5000 -n 50000 -c 50 --procs 4
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import statistics
import sys
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

import httpx
import redis
import redis.asyncio as aredis

from seed import BASE_URL, REDIS_URL, write_limits
from store import QuotaStore, current_period

ORG = "bench"
FEATURE = "container-tracking"
CONTAINER_ID = "MSCU1719583"
LIMIT = 10**12  # quota never runs out mid-run
# A dropped reply fails the run instead of hanging it (seen via Docker Desktop's host.docker.internal).
SOCKET_TIMEOUT_S = 5
BARRIER_TIMEOUT_S = 120

# (name, per-op latencies in ms, timed start and end as epoch seconds)
Sample = tuple[str, list[float], float, float]

# Set in each --procs worker by the pool initializer; barriers only cross processes by inheritance.
_barrier: threading.Barrier | None = None


async def run(
    op: Callable[[], Awaitable[None]], n: int, concurrency: int
) -> list[float]:
    """Run op n times across concurrency workers. Per-op latencies in ms."""
    latencies: list[float] = []
    remaining = n

    async def worker() -> None:
        nonlocal remaining
        while remaining > 0:
            remaining -= 1
            start = time.perf_counter()
            await op()
            latencies.append((time.perf_counter() - start) * 1000)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return latencies


async def run_paced(
    op: Callable[[], Awaitable[None]], n: int, concurrency: int, rate: float
) -> list[float]:
    """Start op i at t0 + i / rate, at most concurrency in flight. Latency from scheduled start, ms."""
    latencies: list[float] = []
    slots = asyncio.Semaphore(concurrency)

    async def one(scheduled: float) -> None:
        async with slots:
            await op()
        latencies.append((time.perf_counter() - scheduled) * 1000)

    t0 = time.perf_counter()
    tasks = []
    for i in range(n):
        scheduled = t0 + i / rate
        # A coarse sleep just releases the overdue ops in a burst; their latency still counts from
        # when they were due.
        if (delay := scheduled - time.perf_counter()) > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.create_task(one(scheduled)))
    await asyncio.gather(*tasks)
    return latencies


async def measure(name: str, op: Callable[[], Awaitable[None]], args) -> Sample:
    # Warm up connections and the Lua script cache (first EVALSHA is a NOSCRIPT miss).
    await run(op, min(1000, args.requests // 10 or 1), args.concurrency)
    if _barrier is not None:
        _barrier.wait()  # blocks the loop, but nothing else is in flight between runs
    start = time.time()
    if args.rate:
        latencies = await run_paced(
            op, args.requests, args.concurrency, args.rate / args.procs
        )
    else:
        latencies = await run(op, args.requests, args.concurrency)
    return name, latencies, start, time.time()


def summarize(samples: list[Sample]) -> dict[str, float | int | str]:
    """Merge one measurement across processes: total ops over the first-start to last-end span."""
    latencies = [ms for _, lat, _, _ in samples for ms in lat]
    wall = max(end for *_, end in samples) - min(start for _, _, start, _ in samples)
    q = statistics.quantiles(latencies, n=100)
    return {
        "name": samples[0][0],
        "ops": len(latencies),
        "ops_per_s": round(len(latencies) / wall),
        "p50_ms": round(q[49], 3),
        "p95_ms": round(q[94], 3),
        "p99_ms": round(q[98], 3),
        "max_ms": round(max(latencies), 3),
    }


async def bench_store(args, org: str) -> list[Sample]:
    r = aredis.Redis.from_url(
        REDIS_URL, decode_responses=True, socket_timeout=SOCKET_TIMEOUT_S
    )
    store = QuotaStore(r)

    async def reserve_commit() -> None:
        reservation = await store.reserve(org, FEATURE, uuid.uuid4().hex, 1, "bench")
        assert reservation.status == "ok", reservation.status
        await store.commit(reservation)

    async def reserve_refund() -> None:
        reservation = await store.reserve(org, FEATURE, uuid.uuid4().hex, 1, "bench")
        assert reservation.status == "ok", reservation.status
        await store.refund(reservation)

    ops = {
        "commit": ("reserve+commit", reserve_commit),
        "refund": ("reserve+refund", reserve_refund),
    }
    try:
        return [
            await measure(name, op, args)
            for key, (name, op) in ops.items()
            if args.op in (key, "both")
        ]
    finally:
        await r.aclose()


async def bench_http(args, org: str) -> list[Sample]:
    """Deprecated: httpx tops out at a few hundred req/s, use oha (bench_suite.py B4-B6)."""
    limits = httpx.Limits(max_connections=args.concurrency)
    async with httpx.AsyncClient(
        base_url=args.base_url, headers={"X-Org-Id": org}, limits=limits
    ) as client:

        async def get(path: str, params: dict | None = None) -> None:
            (await client.get(path, params=params)).raise_for_status()

        return [
            await measure("GET / (unmetered)", partial(get, "/"), args),
            await measure(
                "GET /container_tracking",
                partial(get, "/container_tracking", {"container_id": CONTAINER_ID}),
                args,
            ),
        ]


BENCHES = {"store": bench_store, "http": bench_http}


def bench_process(mode: str, args, org: str) -> list[Sample]:
    return asyncio.run(BENCHES[mode](args, org))


def init_worker(barrier: threading.Barrier) -> None:
    global _barrier
    _barrier = barrier


def provision(r: redis.Redis, org: str) -> None:
    period, expire_at = current_period()
    write_limits(r, org, {FEATURE: LIMIT}, period, expire_at)


def cleanup(r: redis.Redis, org: str) -> None:
    keys = [
        *r.scan_iter(match=f"quota:{{{org}}}:*", count=1000),
        *r.scan_iter(match=f"res:{{{org}}}:*", count=1000),
    ]
    if keys:
        r.unlink(*keys)


def print_table(rows: list[dict]) -> None:
    cols = list(rows[0])
    widths = {c: max(len(c), *(len(str(row[c])) for row in rows)) for c in cols}
    print("  ".join(c.ljust(widths[c]) for c in cols))
    for row in rows:
        print("  ".join(str(row[c]).ljust(widths[c]) for c in cols))


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("mode", choices=BENCHES)
    p.add_argument("-n", "--requests", type=int, default=10_000, help="per process")
    p.add_argument("-c", "--concurrency", type=int, default=50, help="per process")
    p.add_argument("--procs", type=int, default=1, help="parallel client processes")
    p.add_argument(
        "--rate", type=float, help="open loop: total ops/s offered across all processes"
    )
    p.add_argument(
        "--op", choices=["commit", "refund", "both"], default="both", help="store mode"
    )
    p.add_argument(
        "--base-url",
        default=BASE_URL,
        help="http mode (deprecated): nginx or a single api",
    )
    p.add_argument("--json", type=Path, help="also write raw results here")
    args = p.parse_args()
    if args.mode == "http":
        print(
            "warning: http mode is deprecated, httpx measures the client; "
            "use oha (bench_suite.py B4-B6)",
            file=sys.stderr,
        )

    orgs = [ORG] if args.procs == 1 else [f"{ORG}-{i}" for i in range(args.procs)]
    r = redis.Redis.from_url(
        REDIS_URL, decode_responses=True, socket_timeout=SOCKET_TIMEOUT_S
    )
    for org in orgs:
        provision(r, org)
    try:
        if args.procs == 1:
            per_proc = [bench_process(args.mode, args, ORG)]
        else:
            barrier = mp.Barrier(args.procs, timeout=BARRIER_TIMEOUT_S)
            with ProcessPoolExecutor(
                args.procs, initializer=init_worker, initargs=(barrier,)
            ) as pool:
                per_proc = list(pool.map(partial(bench_process, args.mode, args), orgs))
    finally:
        for org in orgs:
            cleanup(r, org)

    rows = [
        summarize([samples[i] for samples in per_proc]) for i in range(len(per_proc[0]))
    ]
    print(
        f"{args.mode}: {args.procs} process(es) x {args.requests} requests, "
        f"concurrency {args.concurrency} each"
        + (f", {args.rate:g} ops/s offered" if args.rate else ", closed loop")
    )
    print_table(rows)
    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "mode": args.mode,
                    "at": datetime.now(UTC).isoformat(),
                    "procs": args.procs,
                    "requests": args.requests,
                    "concurrency": args.concurrency,
                    "rate": args.rate,
                    "base_url": args.base_url if args.mode == "http" else None,
                    "results": rows,
                },
                indent=2,
            )
            + "\n"
        )


if __name__ == "__main__":
    main()
