"""Full benchmark suite for docs/benchmarks.md, against the compose stack (8 replicas).

    B1  meter overhead, single requests        store_bench.py -c 1
    B2  meter max load                         store_bench.py --procs 1/2/4/8
    B3  meter overhead at fixed rate, 20 s     store_bench.py --op refund --rate 2k/5k/10k
    B4  max load, unmetered                    oha, fixed-rate sweep
    B5  max load, metered                      oha, fixed-rate sweep
    B6  sustained 2k/s metered, 60 s           oha
    B4-B6 run once per server (uvicorn, granian).

Run with the stack up (docker compose up -d --wait):
    uv run scripts/bench_suite.py                          # everything (~25 min)
    uv run scripts/bench_suite.py --servers granian        # one server
    uv run scripts/bench_suite.py --servers                # B1-B3 only
    uv run scripts/bench_suite.py --skip-meter             # skip B1-B3
    uv run scripts/bench_suite.py --rates 2000 4000 6000   # custom B3-B5 sweep

Both load generators run inside the compose network (store_bench.py in a one-off api container, oha as
its own container), so Docker Desktop's port forwarding is never in the path. Output goes to
bench-results/<timestamp>/: one file per run, env.txt, summary.md.
"""

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]

import redis  # noqa: E402

from store_bench import ORG, cleanup, provision  # noqa: E402

METERED_PATH = "/container_tracking?container_id=MSCU1719583"
RATES = [2000, 5000, 10000]
PROCS = [1, 2, 4, 8]
# One store_bench.py process tops out ~2.7k ops/s closed loop; stay well under so B3 measures Redis,
# not a saturated client.
METER_RATE_PER_PROC = 1700
OVERHEAD_SECONDS = 20
PAUSE_AFTER_RUN = 10  # seconds

OHA_IMAGE = "ghcr.io/hatoo/oha:latest"
OHA_NAME = f"bench-oha-{os.getpid()}"  # so Ctrl+C can stop the running oha
OHA_FLAGS = ["--ipv4", "--no-tui", "--output-format", "json", "-H", f"X-Org-Id: {ORG}"]
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
    return sh("docker", "compose", "port", service, port, capture=True).rsplit(":", 1)[
        1
    ]


def setup(server: str) -> None:
    """(Re)start the api replicas on `server` and wait until the stack is healthy."""
    global NETWORK
    files = ["-f", "compose.yaml"]
    if server == "granian":
        override = RES / "granian.yaml"
        override.write_text(GRANIAN_OVERRIDE, encoding="utf-8")
        files += ["-f", str(override)]
    log(f"starting 8 x {server}")
    sh("docker", "compose", *files, "up", "-d", "--wait")
    # nginx resolves replica IPs once at startup; the second up waits for it to be healthy again
    sh("docker", "compose", "restart", "nginx")
    sh("docker", "compose", *files, "up", "-d", "--wait")
    nginx = sh("docker", "compose", "ps", "-q", "nginx", capture=True)
    fmt = "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}"
    NETWORK = sh("docker", "inspect", "-f", fmt, nginx, capture=True)


def flags(**kw: object) -> list[str]:
    """{"c": 512, "procs": 4} -> ["-c", "512", "--procs", "4"]"""
    return [
        a for k, v in kw.items() for a in (f"-{k}" if len(k) == 1 else f"--{k}", str(v))
    ]


def run_redis_bench(name: str, **kw: object) -> None:
    """store_bench.py in a one-off api container, straight against Redis. Writes <name>.txt."""
    log(f"{name}: store_bench.py {' '.join(flags(**kw))}")
    out = sh(*IN_API, "python", "scripts/store_bench.py", *flags(**kw), capture=True)
    print(out)
    (RES / f"{name}.txt").write_text(out + "\n", encoding="utf-8")


def run_api_bench(name: str, path: str, **kw: object) -> None:
    """oha through nginx to the api replicas. Writes <name>.json, then pauses."""
    log(f"{name}: oha {' '.join(flags(**kw))} {path}")
    oha = ["docker", "run", "--rm", "--name", OHA_NAME, "--network", NETWORK, OHA_IMAGE]
    out = sh(*oha, *flags(**kw), *OHA_FLAGS, f"http://nginx{path}", capture=True)
    (RES / f"{name}.json").write_text(out, encoding="utf-8")
    time.sleep(PAUSE_AFTER_RUN)


# scenarios


def b1_redis_single() -> None:
    run_redis_bench("B1", n=5000, c=1)


def b2_redis_max_load() -> None:
    for n in PROCS:
        run_redis_bench(f"B2-procs{n}", n=5000, c=16, procs=n)


def b3_redis_fixed_rate(rates: list[int]) -> None:
    for rate in rates:
        procs = math.ceil(rate / METER_RATE_PER_PROC)
        n = OVERHEAD_SECONDS * rate // procs
        run_redis_bench(
            f"B3-rate{rate}", op="refund", rate=rate, n=n, c=50, procs=procs
        )


def b4_api_unmetered(server: str, rates: list[int]) -> None:
    for rate in rates:
        run_api_bench(f"B4-{server}-unmetered-{rate}", "/", z="20s", c=512, q=rate)


def b5_api_metered(server: str, rates: list[int]) -> None:
    for rate in rates:
        run_api_bench(
            f"B5-{server}-metered-{rate}", METERED_PATH, z="20s", c=512, q=rate
        )


def b6_api_sustained(server: str) -> None:
    run_api_bench(f"B6-{server}-sustained-2000", METERED_PATH, z="60s", c=256, q=2000)


# reporting


def ms(seconds: float | None) -> str:
    return "-" if seconds is None else f"{seconds * 1000:.1f}"


def record_env() -> None:
    fmt = (
        "{{.ServerVersion}}, {{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes"
    )
    docker = sh("docker", "info", "-f", fmt, capture=True)
    env = f"host: {platform.platform()}, {os.cpu_count()} CPUs\ndocker: {docker}\n"
    (RES / "env.txt").write_text(env, encoding="utf-8")
    log(env.replace("\n", "; "))


def summary() -> str:
    rows = [
        "| Run | 200s/s | Non-200 | Errors (excl. deadline) | p50 ms | p99 ms |",
        "|---|---|---|---|---|---|",
    ]
    for f in sorted(RES.glob("B*.json"), key=lambda f: f.stat().st_mtime):
        r = json.loads(f.read_text(encoding="utf-8"))
        codes = r.get("statusCodeDistribution", {})
        non200 = {k: v for k, v in codes.items() if k != "200"}
        errors = {
            k: v
            for k, v in r.get("errorDistribution", {}).items()
            if "deadline" not in k
        }
        p = r.get("latencyPercentiles") or {}
        rows.append(
            f"| {f.stem} | {codes.get('200', 0) / r['summary']['total']:,.0f} | {non200 or '-'} "
            f"| {errors or '-'} | {ms(p.get('p50'))} | {ms(p.get('p99'))} |"
        )
    return "\n".join(rows) + "\n"


def main() -> None:
    global RES
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--servers",
        nargs="*",
        choices=["uvicorn", "granian"],
        default=["uvicorn", "granian"],
        help="none given: skip HTTP runs",
    )
    p.add_argument("--skip-meter", action="store_true", help="skip B1-B3")
    p.add_argument("--rates", nargs="+", type=int, default=RATES, help="B3-B5 sweep")
    args = p.parse_args()

    if not sh("docker", "compose", "ps", "-q", "nginx", capture=True):
        sys.exit("stack not running: docker compose up -d --wait")
    # setup() re-runs `compose up`, which recreates nginx/redis on the compose default ports
    # unless these match what the running stack published.
    os.environ.setdefault("STAGE_PORT", published_port("nginx", "80"))
    os.environ.setdefault("REDIS_PORT", published_port("redis", "6379"))

    RES = RES / f"{datetime.now():%Y%m%d-%H%M%S}"
    RES.mkdir(parents=True)
    log(f"results in {RES.relative_to(ROOT)}")
    record_env()

    if not args.skip_meter:
        # store_bench.py provisions and deletes its own bench org(s), so run it before provisioning.
        b1_redis_single()
        b2_redis_max_load()
        b3_redis_fixed_rate(args.rates)

    if args.servers:
        r = redis.Redis(
            host="127.0.0.1", port=int(os.environ["REDIS_PORT"]), decode_responses=True
        )
        provision(r, ORG)
        try:
            for server in args.servers:
                setup(server)
                b4_api_unmetered(server, args.rates)
                b5_api_metered(server, args.rates)
                b6_api_sustained(server)
        finally:
            subprocess.run(["docker", "rm", "-f", OHA_NAME], capture_output=True)
            log("deleting bench keys")
            cleanup(r, ORG)

    table = summary()
    (RES / "summary.md").write_text(table, encoding="utf-8")
    print(table)
    log("done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("interrupted")
