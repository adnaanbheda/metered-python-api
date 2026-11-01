"""Benchmarks against the compose stack (8 replicas, or API_REPLICAS), for docs/benchmarks.md.

With no command, runs the defaults on a grid (full | quick):
  redis bench (store_bench.py, straight against Redis):
    B1  single requests                        -c 1, -n 5000 | 2000
    B2  closed-loop max                        --procs 1/2/4/8 | 1/4
    B3  rate sweep                             --op refund --rate <rates>, 20 s | 5 s each
  api bench (oha through nginx, once per server: uvicorn, granian):
    B4  rate sweep, unmetered                  -q <rates>, 20 s | 5 s each
    B5  rate sweep, metered                    -q <rates>, 20 s | 5 s each
    B6  sustained 2k/s metered                 60 s | 15 s
    B7  quota under the replica race           limit 10000 | 2000, 3x that many requests, unthrottled;
                                               passes if exactly `limit` got 200 (rest 429) and the
                                               Redis usage counter agrees
    B8  quota never over-served, contended     limits 1000/5000/10000 | 1000, 3x+5 requests each, -c 512,
                                               unthrottled; passes if exactly `limit` got 200 (rest 429)
                                               and the Redis usage counter and usage stream both agree
  rates: 2000/5000/10000 | 2000/5000

Run with the stack up (docker compose up -d --wait):
    uv run scripts/bench_suite.py                          # full grid (~15 min)
    uv run scripts/bench_suite.py --grid quick             # smaller grid (~5 min)
    uv run scripts/bench_suite.py --only redis             # B1-B3
    uv run scripts/bench_suite.py --only api               # B4-B8
    uv run scripts/bench_suite.py --servers granian        # api bench on one server
    uv run scripts/bench_suite.py --rates 2000 4000 6000   # custom B3-B5 sweep
    uv run scripts/bench_suite.py --no-rates               # B4-B5 unthrottled, B3 skipped

One custom run instead of the defaults:
    uv run scripts/bench_suite.py redis -n 20000 -c 64 --procs 4
    uv run scripts/bench_suite.py redis --op refund --rate 3000 -n 60000
    uv run scripts/bench_suite.py api --route unmetered -q 8000
    uv run scripts/bench_suite.py api --no-meter -q 1000 -c 50   # vs the same without: meter overhead
    uv run scripts/bench_suite.py api -c 1024 -z 30s --servers granian   # no -q: unthrottled

Meter overhead rate sweep (OH): the metered route with the meter off, then on, at each rate
(-q, default the grid's 1k-10k | 2k/5k), B4's duration, -c 512. The rate where p99 jumps or
successful req/s falls behind the target is the max sustainable throughput, per meter setting:
    uv run scripts/bench_suite.py overhead --servers uvicorn
    uv run scripts/bench_suite.py overhead -q 3000 4000 5000

Both load generators run inside the compose network (store_bench.py in a one-off api container, oha as
its own container), so Docker Desktop's port forwarding is never in the path. oha gets the last
OHA_CPUS of Docker's CPUs and the stack the rest, so the load generator never takes CPU from what it
measures; fixed-rate runs (-q) use --latency-correction, so a stall can't hide by delaying sends.

Two machines (isolated numbers, scripts/bench_remote.sh sets this up): with OHA_HOST set to a Docker
host for oha (ssh://user@loadgen), oha runs there against http://STAGE_ADDR:STAGE_PORT, and the
stack gets all of this machine's CPUs.

Output goes to
bench-results/<timestamp>/: redis/, api/, quota/ and race/ with one JSON per run, named after its flags
(B2-n5000-c16-procs4, custom-granian-metered-20s-c512-q8000), plus env.txt and summary.md.
"""

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]

from bench_report import results, summary  # noqa: E402
from store import USAGE_STREAM, QuotaStore, connect_sync, current_period  # noqa: E402
from store_bench import FEATURE, ORG, cleanup, print_table, provision  # noqa: E402
from store_bench import add_args as add_store_bench_args  # noqa: E402

METERED_PATH = "/container_tracking?container_id=MSCU1719583"
ROUTES = {"metered": METERED_PATH, "unmetered": "/"}
SERVERS = ["uvicorn", "granian"]
# One store_bench.py process tops out ~2.7k ops/s closed loop; stay well under so B3 measures Redis,
# not a saturated client.
METER_RATE_PER_PROC = 1700


@dataclass(frozen=True)
class Grid:
    rates: list[int]  # B3-B5 sweep
    procs: list[int]  # B2
    n: int  # B1, B2 ops per process
    redis_seconds: int  # B3, per rate
    api_duration: str  # B4, B5, per rate
    sustained_duration: str  # B6
    pause: int  # seconds after each api run
    quota_limit: int  # B7
    race_limits: list[int]  # B8
    overhead_rates: list[int]  # OH


GRIDS = {
    "full": Grid(
        [2000, 5000, 10000],
        [1, 2, 4, 8],
        5000,
        20,
        "20s",
        "60s",
        10,
        10000,
        [1000, 5000, 10000],
        [1000, 2000, 4000, 6000, 8000, 10000],
    ),
    "quick": Grid(
        [2000, 5000], [1, 4], 2000, 5, "5s", "15s", 3, 2000, [1000], [2000, 5000]
    ),
}
QUOTA_ORG = "bench-quota"  # B7/B8's own org, so its small limit never throttles B4-B6

OHA_IMAGE = "ghcr.io/hatoo/oha:latest"
OHA_NAME = f"bench-oha-{os.getpid()}"  # so Ctrl+C can stop the running oha
OHA_FLAGS = ["--ipv4", "--no-tui", "--output-format", "json"]
OHA_CPUS = 2  # pinned to the last 2 of Docker's CPUs; the stack gets the rest
# Remote oha: Docker host it runs on, and this machine's address as that host reaches it.
OHA_HOST = os.environ.get("OHA_HOST")
STAGE_ADDR = os.environ.get("STAGE_ADDR")
OHA_DOCKER = ["docker", "-H", OHA_HOST] if OHA_HOST else ["docker"]
# One-off api container on the compose network, with the current scripts/ mounted.
IN_API = [
    *"docker compose run --rm --no-deps -T".split(),
    *("-v", f"{(ROOT / 'scripts').as_posix()}:/app/scripts"),
    *("-e", "REDIS_URL=redis://redis:6379/0"),
    *("-e", "PYTHONPATH=/app/src:/app/scripts"),
    "api",
]
GRANIAN_OVERRIDE = """\
services:
  api:
    command: ["granian", "--interface", "asgi", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-ws", "main:app"]
"""


# set in main() and setup()
RES = ROOT / "bench-results"
NETWORK = ""
STACK_CPUS = OHA_CPUSET = ""  # e.g. "0-9" and "10-11" on 12 CPUs


def sh(*cmd: str, capture: bool = False) -> str:
    """Run cmd from the repo root, exit on failure; return stdout when captured."""
    proc = subprocess.run(
        cmd, cwd=ROOT, capture_output=capture, text=True, encoding="utf-8"
    )
    if proc.returncode:
        sys.exit(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr or ''}"
        )
    return (proc.stdout or "").strip()


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def published_port(service: str, port: str) -> str:
    out = sh("docker", "compose", "port", service, port, capture=True)
    return out.rsplit(":", 1)[1]


def setup(server: str) -> None:
    """(Re)start the api replicas on `server` and wait until the stack is healthy."""
    global NETWORK
    files = ["-f", "compose.yaml"]
    pin = RES / "cpuset.yaml"
    services = "".join(
        f"  {s}:\n    cpuset: {STACK_CPUS}\n" for s in ("api", "nginx", "redis")
    )
    pin.write_text(f"services:\n{services}", encoding="utf-8")
    files += ["-f", str(pin)]
    if server == "granian":
        override = RES / "granian.yaml"
        override.write_text(GRANIAN_OVERRIDE, encoding="utf-8")
        files += ["-f", str(override)]
    log(f"starting {os.environ.get('API_REPLICAS', '8')} x {server}")
    # captured: compose progress only shows (via sh's exit message) if a step fails
    sh("docker", "compose", *files, "up", "-d", "--wait", capture=True)
    # nginx resolves replica IPs once at startup; the second up waits for it to be healthy again
    sh("docker", "compose", "restart", "nginx", capture=True)
    sh("docker", "compose", *files, "up", "-d", "--wait", capture=True)
    nginx = sh("docker", "compose", "ps", "-q", "nginx", capture=True)
    fmt = "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}"
    NETWORK = sh("docker", "inspect", "-f", fmt, nginx, capture=True)


def flags(**kw: object) -> list[str]:
    """{"c": 512, "procs": 4, "q": None} -> ["-c", "512", "--procs", "4"]"""
    return [
        a
        for k, v in kw.items()
        if v is not None
        for a in (f"-{k}" if len(k) == 1 else f"--{k}", str(v))
    ]


def run_name(*parts: str, **kw: object) -> str:
    """("B3", op="refund", rate=5000, c=50) -> "B3-refund-rate5000-c50"; unset flags are left out."""
    tags = [
        v if isinstance(v, str) else f"{k}{v}" for k, v in kw.items() if v is not None
    ]
    return "-".join([*parts, *tags])


def save(bench: str, name: str, out: str) -> None:
    """Write one run's JSON to <RES>/<bench>/<name>.json."""
    f = RES / bench / f"{name}.json"
    f.parent.mkdir(exist_ok=True)
    f.write_text(out + "\n", encoding="utf-8")


def run_redis_bench(label: str, **kw: object) -> None:
    """store_bench.py in a one-off api container, straight against Redis."""
    name = run_name(label, **kw)
    log(f"{name}: store_bench.py {' '.join(flags(**kw))}")
    store_bench = ["python", "scripts/store_bench.py", *flags(**kw), "--json"]
    out = sh(*IN_API, *store_bench, capture=True)
    print_table(json.loads(out))
    save("redis", name, out)


def run_api_bench(
    label: str, server: str, route: str, pause: int = 0, org: str = ORG, **kw: object
) -> tuple[str, dict]:
    """oha through nginx to the api replicas as `org`, then pause seconds. No q: unthrottled.
    Returns the run name and oha's report."""
    name = run_name(label, server, route, **kw)
    log(f"{name}: oha {' '.join(flags(**kw))} {ROUTES[route]}")
    oha = [*OHA_DOCKER, "run", "--rm", "--name", OHA_NAME]
    if OHA_HOST:
        url = f"http://{STAGE_ADDR}:{os.environ['STAGE_PORT']}{ROUTES[route]}"
    else:
        oha += ["--network", NETWORK, "--cpuset-cpus", OHA_CPUSET]
        url = f"http://nginx{ROUTES[route]}"
    oha.append(OHA_IMAGE)
    # fixed rate: time each request from when it was due, not when a stalled worker sent it
    correction = ["--latency-correction"] if kw.get("q") else []
    out = sh(
        *oha,
        *flags(**kw),
        *OHA_FLAGS,
        *correction,
        *("-H", f"X-Org-Id: {org}", url),
        capture=True,
    )
    save("api", name, out)
    time.sleep(pause)
    return name, json.loads(out)


def host_redis():
    """The stack's Redis through its published port, for provisioning and checks."""
    return connect_sync(f"redis://127.0.0.1:{os.environ['REDIS_PORT']}/0")


def on_each_server(servers: list[str], bench: Callable[[str], None]) -> None:
    """Provision the bench org (the api bench's X-Org-Id), then restart the stack on each server
    and run bench(server). Deletes the org's keys afterwards."""
    provision(host_redis(), ORG)
    try:
        for server in servers:
            setup(server)
            bench(server)
    finally:
        subprocess.run([*OHA_DOCKER, "rm", "-f", OHA_NAME], capture_output=True)
        log("deleting bench keys")
        # new connection: setup() may have recreated redis (AOF keeps the keys, not the socket)
        cleanup(host_redis(), ORG)


# defaults


# `rates` is the grid's, or --rates; [None] for --no-rates.


def b1_redis_single(g: Grid) -> None:
    run_redis_bench("B1", n=g.n, c=1)


def b2_redis_max_load(g: Grid) -> None:
    for procs in g.procs:
        run_redis_bench("B2", n=g.n, c=16, procs=procs)


def b3_redis_fixed_rate(g: Grid, rates: list[int | None]) -> None:
    for rate in filter(None, rates):  # unthrottled is B2
        procs = math.ceil(rate / METER_RATE_PER_PROC)
        n = g.redis_seconds * rate // procs
        run_redis_bench("B3", op="refund", rate=rate, n=n, c=50, procs=procs)


def b4_api_unmetered(g: Grid, server: str, rates: list[int | None]) -> None:
    for rate in rates:
        run_api_bench(
            "B4", server, "unmetered", g.pause, z=g.api_duration, c=512, q=rate
        )


def b5_api_metered(g: Grid, server: str, rates: list[int | None]) -> None:
    for rate in rates:
        run_api_bench("B5", server, "metered", g.pause, z=g.api_duration, c=512, q=rate)


def b6_api_sustained(g: Grid, server: str) -> None:
    run_api_bench(
        "B6", server, "metered", g.pause, z=g.sustained_duration, c=256, q=2000
    )


def b7_api_quota(g: Grid, server: str) -> None:
    """Offer 3x the limit, unthrottled, so every replica races for the last units. -n, not -z:
    a run cut off by -z abandons in-flight requests the server may still commit."""
    r, limit = host_redis(), g.quota_limit
    cleanup(r, QUOTA_ORG)
    provision(r, QUOTA_ORG, limit)
    try:
        name, report = run_api_bench(
            "B7", server, "metered", org=QUOTA_ORG, n=3 * limit, c=512
        )
        # Refunds and commits land just after the response is sent; let them settle.
        time.sleep(1)
        used = int(
            r.get(QuotaStore.usage_key(QUOTA_ORG, FEATURE, current_period()[0])) or 0
        )
        live = sum(
            r.hget(k, "state") == "reserved"
            for k in r.scan_iter(match=f"res:{{{QUOTA_ORG}}}:*", count=1000)
        )
    finally:
        cleanup(r, QUOTA_ORG)
    codes = report.get("statusCodeDistribution", {})
    ok, rejected = codes.get("200", 0), codes.get("429", 0)
    check = {
        "limit": limit,
        "requests": 3 * limit,
        "ok": ok,
        "rejected": rejected,
        "other": 3 * limit - ok - rejected,
        "used": used,
        "live_reservations": live,
    }
    check["pass"] = ok == used == limit and check["other"] == 0 and live == 0
    log(f"{name}: {'PASS' if check['pass'] else 'FAIL'} {check}")
    save("quota", name, json.dumps(check))
    time.sleep(g.pause)


def b8_api_race(g: Grid, server: str) -> None:
    """tests/integration/test_burst_never_overserves at load: per limit, 3x+5 requests race for it,
    512 in flight across the replicas. -n, not -z, as in B7. Beyond B7:
      - usage stream: exactly `limit` units charged, committed by more than one replica (`instance`)
      - reservations: exactly `limit` keys, all `committed`. reserve.lua writes one per granted
        request and none for a 429, commit.lua marks it committed, refund.lua deletes it; so any
        other count or state is a leaked reserve, a missed commit or a refund that shouldn't be."""
    r = host_redis()
    for limit in g.race_limits:
        n = 3 * limit + 5
        cleanup(r, QUOTA_ORG)
        provision(r, QUOTA_ORG, limit)
        # Only this run's stream entries: everything after the current last one.
        last = r.xrevrange(USAGE_STREAM, count=1)
        since = f"({last[0][0]}" if last else "-"
        try:
            name, report = run_api_bench(
                "B8", server, "metered", org=QUOTA_ORG, n=n, c=512
            )
            # Refunds and commits land just after the response is sent; let them settle.
            time.sleep(1)
            used = int(
                r.get(QuotaStore.usage_key(QUOTA_ORG, FEATURE, current_period()[0]))
                or 0
            )
            events = [
                f
                for _, f in r.xrange(USAGE_STREAM, min=since)
                if f["org_id"] == QUOTA_ORG
            ]
            charged = sum(int(f["cost"]) for f in events)
            per_replica = Counter(f.get("instance") for f in events)
            states = Counter(
                r.hget(k, "state")
                for k in r.scan_iter(match=f"res:{{{QUOTA_ORG}}}:*", count=1000)
            )
        finally:
            cleanup(r, QUOTA_ORG)
        codes = report.get("statusCodeDistribution", {})
        ok, rejected = codes.get("200", 0), codes.get("429", 0)
        check = {
            "limit": limit,
            "requests": n,
            "ok": ok,
            "rejected": rejected,
            "other": n - ok - rejected,
            "used": used,
            "charged": charged,
            "reservations": dict(states),
            "replicas": len(per_replica),
            "commits_per_replica": sorted(per_replica.values(), reverse=True),
            "seconds": round(report["summary"]["total"], 2),
        }
        check["pass"] = (
            ok == used == charged == limit
            and check["other"] == 0
            and states == {"committed": limit}
            and len(per_replica) > 1
            and None not in per_replica
        )
        log(f"{name}: {'PASS' if check['pass'] else 'FAIL'} {check}")
        save("race", name, json.dumps(check))
        time.sleep(g.pause)


def record_env() -> None:
    fmt = (
        "{{.ServerVersion}}, {{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes"
    )
    docker = sh("docker", "info", "-f", fmt, capture=True)
    env = f"host: {platform.platform()}, {os.cpu_count()} CPUs\ndocker: {docker}\n"
    if OHA_HOST:
        oha = sh(*OHA_DOCKER, "info", "-f", fmt, capture=True)
        env += f"oha: {OHA_HOST} -> {STAGE_ADDR}, docker: {oha}\n"
    env += f"cpuset: stack {STACK_CPUS}, oha {OHA_CPUSET or OHA_HOST}\n"
    (RES / "env.txt").write_text(env, encoding="utf-8")
    log(env.replace("\n", "; "))


def add_servers(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--servers", nargs="+", choices=SERVERS, default=SERVERS, help="api bench"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_servers(p)
    p.add_argument("--only", choices=["redis", "api"], help="B1-B3 or B4-B6")
    p.add_argument("--grid", choices=GRIDS, default="full")
    rates = p.add_mutually_exclusive_group()
    rates.add_argument(
        "--rates", nargs="+", type=int, help="B3-B5 sweep; default: grid's"
    )
    rates.add_argument(
        "--no-rates", action="store_true", help="B4-B5 unthrottled, no B3"
    )

    commands = p.add_subparsers(dest="command", metavar="{redis,api}")
    add_store_bench_args(
        commands.add_parser("redis", help="one custom redis bench run (store_bench.py)")
    )
    ab = commands.add_parser("api", help="one custom api bench run (oha), per server")
    ab.add_argument("--route", choices=ROUTES, default="metered")
    ab.add_argument("-q", type=int, help="req/s; unset: unthrottled")
    ab.add_argument("-z", default="20s", help="duration")
    ab.add_argument("-c", type=int, default=512, help="connections")
    ab.add_argument(
        "--no-meter",
        action="store_true",
        help="replicas run with DISABLE_METER=1: same route, no Redis, for the meter's overhead",
    )
    add_servers(ab)
    oh = commands.add_parser(
        "overhead", help="meter off vs on on the metered route, swept over rates"
    )
    oh.add_argument("-q", type=int, nargs="+", help="req/s to sweep; default: grid's")
    add_servers(oh)

    args = p.parse_args()
    args.grid = GRIDS[args.grid]
    args.rates = [None] if args.no_rates else args.rates or args.grid.rates
    return args


def run_defaults(args: argparse.Namespace) -> None:
    g, rates = args.grid, args.rates

    def api_defaults(server: str) -> None:
        b4_api_unmetered(g, server, rates)
        b5_api_metered(g, server, rates)
        b6_api_sustained(g, server)
        b7_api_quota(g, server)
        b8_api_race(g, server)

    if args.only != "api":
        # store_bench.py provisions and deletes its own bench org(s), so run it before provisioning.
        b1_redis_single(g)
        b2_redis_max_load(g)
        b3_redis_fixed_rate(g, rates)
    if args.only != "redis":
        on_each_server(args.servers, api_defaults)


def run_custom_redis(args: argparse.Namespace) -> None:
    kw = dict(n=args.requests, c=args.concurrency, procs=args.procs, rate=args.rate)
    run_redis_bench("custom", op=args.op, **kw)


def run_custom_api(args: argparse.Namespace) -> None:
    label = "custom-nometer" if args.no_meter else "custom"
    # compose.yaml passes it to the replicas; setup()'s `up` recreates them when it changes
    os.environ["DISABLE_METER"] = "1" if args.no_meter else "0"

    def custom(server: str) -> None:
        run_api_bench(label, server, args.route, z=args.z, c=args.c, q=args.q)

    on_each_server(args.servers, custom)


def run_overhead(args: argparse.Namespace) -> None:
    """The metered route with the meter off (DISABLE_METER=1), then on, at each rate. Same route
    and flags both ways, so the delta is the meter alone. One restart per toggle."""
    g = args.grid
    loads = [dict(z=g.api_duration, c=512, q=q) for q in args.q or g.overhead_rates]
    # Off first, so the stack is left metered. on_each_server's setup() starts it unmetered.
    os.environ["DISABLE_METER"] = "1"

    def overhead(server: str) -> None:
        for meter in ("off", "on"):
            if meter == "on":
                os.environ["DISABLE_METER"] = "0"
                setup(server)
            for kw in loads:
                run_api_bench(f"OH-{meter}", server, "metered", g.pause, **kw)
        os.environ["DISABLE_METER"] = "1"  # the next server starts unmetered again

    on_each_server(args.servers, overhead)


RUNS = {
    "redis": run_custom_redis,
    "api": run_custom_api,
    "overhead": run_overhead,
    None: run_defaults,
}


def main() -> None:
    global RES, STACK_CPUS, OHA_CPUSET
    args = parse_args()

    ncpu = int(sh("docker", "info", "-f", "{{.NCPU}}", capture=True))
    if OHA_HOST:
        if not STAGE_ADDR:
            sys.exit(
                "OHA_HOST needs STAGE_ADDR: this machine's address as the oha host sees it"
            )
        STACK_CPUS = f"0-{ncpu - 1}"
    else:
        STACK_CPUS = f"0-{ncpu - OHA_CPUS - 1}"
        OHA_CPUSET = f"{ncpu - OHA_CPUS}-{ncpu - 1}"

    if not sh("docker", "compose", "ps", "-q", "nginx", capture=True):
        sys.exit("stack not running: docker compose up -d --wait")
    # setup() re-runs `compose up`, which recreates nginx/redis on the compose default ports
    # unless these match what the running stack published.
    os.environ.setdefault("STAGE_PORT", published_port("nginx", "80"))
    os.environ.setdefault("REDIS_PORT", published_port("redis", "6379"))

    # The api image bakes in src/: rebuild so the one-off store_bench.py container and the replicas
    # (recreated by setup()) run the current code.
    log("building api image")
    # captured: a failing step (ruff/pytest in the check stage) still prints its log on exit
    sh("docker", "compose", "build", "api", capture=True)

    RES = RES / f"{datetime.now():%Y%m%d-%H%M%S}"
    RES.mkdir(parents=True)
    log(f"results in {RES.relative_to(ROOT)}")
    record_env()

    RUNS[args.command](args)

    print(summary(RES))
    log("done")
    checks = [*results(RES, "quota"), *results(RES, "race")]
    if failed := [name for name, check in checks if not check["pass"]]:
        sys.exit(f"quota check failed: {', '.join(failed)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("interrupted")
