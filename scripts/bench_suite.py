"""Full benchmark suite for docs/benchmarks.md, against the compose stack (8 replicas).

    B1  meter overhead, single requests        bench.py store -c 1
    B2  meter max load                         bench.py store --procs 1/2/4/8
    B3  meter overhead at fixed rate, 20 s     bench.py store --op refund --rate 2k/5k/10k
    B4 max load, unmetered                    oha in-network, fixed-rate sweep
    B5  max load, metered                      oha in-network, fixed-rate sweep
    B6  sustained 2k/s metered, 60 s           oha in-network
    B4-B6 run once per server (uvicorn, granian).

Run from anywhere with the stack up (docker compose up -d --wait), on Linux, macOS or Windows:
    uv run scripts/bench_suite.py                          # everything (~25 min)
    uv run scripts/bench_suite.py --servers granian        # one server
    uv run scripts/bench_suite.py --servers                # B1/B2 only
    uv run scripts/bench_suite.py --skip-meter             # skip B1-B3

Standard library only, and it drives docker directly, so no shell is involved on any OS.
Raw output goes to bench-results/<timestamp>/ (oha JSON per run, bench.py output, env.txt,
run.log); summary.md there has the tables. Load always comes from inside the compose network,
so Docker Desktop's port forwarding (Windows, macOS) is never in the path.
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
OHA_IMAGE = "ghcr.io/hatoo/oha:latest"
METERED_PATH = "/container_tracking?container_id=MSCU1719583"
GRANIAN_OVERRIDE = """\
services:
  api:
    command: ["granian", "--interface", "asgi", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-ws", "main:app"]
"""
PAUSE_AFTER_RUN = 10  # seconds
PAUSE_AFTER_START = 5
# One bench.py process tops out ~2.7k ops/s closed loop; stay well under so B3 measures Redis,
# not a saturated client.
METER_RATE_PER_PROC = 1700
OVERHEAD_SECONDS = 20

PROVISION = (
    "import redis; from seed import write_limits; from store import current_period; "
    "p,e=current_period(); "
    "write_limits(redis.Redis.from_url('redis://redis:6379/0', decode_responses=True), "
    "'bench', {'container-tracking': 10**12}, p, e)"
)
CLEANUP = (
    "import redis; r=redis.Redis.from_url('redis://redis:6379/0'); "
    "k=[*r.scan_iter(match='quota:{bench}:*'),*r.scan_iter(match='res:{bench}:*')]; "
    "k and r.unlink(*k)"
)
# Sockets in TIME_WAIT (state 06); runs inside the container, so this is the container's sh.
TIME_WAIT = "cat /proc/net/tcp /proc/net/tcp6 | awk '$4 == \"06\"' | wc -l"


class Suite:
    def __init__(self, res: Path):
        self.res = res
        self.log_file = (res / "run.log").open("a", encoding="utf-8")
        self.oha_name = f"bench-oha-{os.getpid()}"  # so Ctrl+C can stop the running oha
        self.runs: list[dict] = []
        self.nginx = self.api = self.network = ""

    def emit(self, text: str) -> None:
        sys.stdout.write(text)
        sys.stdout.flush()
        self.log_file.write(text)
        self.log_file.flush()

    def log(self, msg: str) -> None:
        self.emit(f"[{datetime.now():%H:%M:%S}] {msg}\n")

    def stream(self, *cmd: str) -> str:
        """Run cmd from the repo root, echoing its output to the console and run.log."""
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        lines = []
        for line in proc.stdout:
            self.emit(line)
            lines.append(line)
        if proc.wait():
            sys.exit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
        return "".join(lines)

    def in_api(self, *cmd: str) -> str:
        """One-off api container on the compose network, with the current scripts/ mounted."""
        return self.stream(
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "-v",
            f"{(ROOT / 'scripts').as_posix()}:/app/scripts",
            "-e",
            "REDIS_URL=redis://redis:6379/0",
            "-e",
            "PYTHONPATH=/app/src:/app/scripts",
            "api",
            *cmd,
        )

    def resolve_containers(self) -> None:
        # Names depend on the compose project (the repo directory), and replicas get new ids
        # when recreated, so look them up after every (re)start.
        def name(service: str) -> str:
            cid = output("docker", "compose", "ps", "-q", service).split()[0]
            return output("docker", "inspect", "-f", "{{.Name}}", cid).lstrip("/")

        self.nginx, self.api = name("nginx"), name("api")
        self.network = output(
            "docker",
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
            self.nginx,
        )

    def wait_healthy(self, container: str) -> None:
        for _ in range(60):
            if (
                output("docker", "inspect", "-f", "{{.State.Health.Status}}", container)
                == "healthy"
            ):
                return
            time.sleep(1)
        sys.exit(f"{container} not healthy after 60 s")

    def time_wait(self, container: str) -> str:
        return output("docker", "exec", container, "sh", "-c", TIME_WAIT)

    def start_server(self, server: str) -> None:
        self.log(f"starting 8 x {server}")
        if server == "granian":
            override = self.res / "granian.yaml"
            override.write_text(GRANIAN_OVERRIDE, encoding="utf-8")
            self.stream(
                "docker",
                "compose",
                "-f",
                "compose.yaml",
                "-f",
                str(override),
                "up",
                "-d",
                "--wait",
            )
        else:
            self.stream("docker", "compose", "up", "-d", "--wait")
        # nginx resolves replica IPs once at startup
        self.stream("docker", "compose", "restart", "nginx")
        self.resolve_containers()
        self.wait_healthy(self.nginx)
        cmdline = output("docker", "exec", self.api, "cat", "/proc/1/cmdline").replace(
            "\0", " "
        )
        self.log(f"{self.api} runs: {cmdline}")
        if server not in cmdline:
            sys.exit(f"expected {server} in {self.api}")
        time.sleep(PAUSE_AFTER_START)

    def oha(self, name: str, duration: int, conns: int, rate: int, path: str) -> None:
        no_other_load()
        self.log(f"{name}: -z {duration}s -c {conns} -q {rate} {path}")
        out = self.res / f"{name}.json"
        with out.open("w", encoding="utf-8") as f:
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    self.oha_name,
                    "--network",
                    self.network,
                    OHA_IMAGE,
                    "-z",
                    f"{duration}s",
                    "-c",
                    str(conns),
                    "-q",
                    str(rate),
                    "--ipv4",
                    "--no-tui",
                    "--output-format",
                    "json",
                    "-H",
                    "X-Org-Id: bench",
                    f"http://nginx{path}",
                ],
                stdout=f,
                check=True,
            )
        self.runs.append(
            {
                "name": name,
                "duration": duration,
                "result": json.loads(out.read_text(encoding="utf-8")),
                "tw_nginx": self.time_wait(self.nginx),
                "tw_api": self.time_wait(self.api),
            }
        )
        time.sleep(PAUSE_AFTER_RUN)

    def stop_oha(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.oha_name], capture_output=True)

    def summary(self) -> str:
        rows = [
            "| Run | 200s/s | Non-200 | Errors (excl. deadline) | p50 ms | p99 ms | TIME_WAIT nginx / api |",
            "|---|---|---|---|---|---|---|",
        ]
        for run in self.runs:
            r = run["result"]
            codes = r.get("statusCodeDistribution", {})
            non200 = {k: v for k, v in codes.items() if k != "200"}
            errors = {
                k: v
                for k, v in r.get("errorDistribution", {}).items()
                if "deadline" not in k
            }
            p = r.get("latencyPercentiles") or {}

            def ms(v):
                return f"{v * 1000:.1f}" if v is not None else "-"

            rows.append(
                f"| {run['name']} | {round(codes.get('200', 0) / run['duration']):,} | {non200 or '-'} "
                f"| {errors or '-'} | {ms(p.get('p50'))} | {ms(p.get('p99'))} "
                f"| {run['tw_nginx']} / {run['tw_api']} |"
            )
        return "\n".join(rows) + "\n"


def output(*cmd: str) -> str:
    """Run a quiet command and return its stripped stdout."""
    return subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()


def no_other_load() -> None:
    others = output("docker", "ps", "-q", "--filter", f"ancestor={OHA_IMAGE}")
    if others:
        sys.exit(f"another oha container is running ({others.split()}); stop it first")


def platform_name() -> str:
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    try:
        if "microsoft" in Path("/proc/version").read_text().lower():
            return "windows-wsl"
    except OSError:
        pass
    return "linux"


def cpu_name(plat: str) -> str:
    try:
        if plat == "windows":
            return output(
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_Processor).Name",
            )
        if plat == "macos":
            return output("sysctl", "-n", "machdep.cpu.brand_string")
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return platform.processor() or "unknown"


DOCKER_HINTS = {
    "windows": "start Docker Desktop",
    "macos": "start Docker Desktop",
    "windows-wsl": "turn on Docker Desktop's WSL integration for this distro, or run from Windows",
    "linux": "start the docker daemon, and add your user to the docker group (or run with sudo)",
}


def preflight(plat: str) -> None:
    def ok(*cmd: str) -> bool:
        try:
            return subprocess.run(cmd, cwd=ROOT, capture_output=True).returncode == 0
        except FileNotFoundError:
            return False

    if not ok("docker", "info"):
        sys.exit(f"docker not reachable ({plat}): {DOCKER_HINTS[plat]}")
    if not ok("docker", "compose", "version"):
        sys.exit("needs Docker Compose v2 (docker compose ...)")
    if not output("docker", "compose", "ps", "-q", "nginx"):
        sys.exit("stack not running: docker compose up -d --wait")
    no_other_load()


def write_env(res: Path, plat: str) -> str:
    try:
        oha = output(
            "docker", "image", "inspect", "-f", "{{index .RepoDigests 0}}", OHA_IMAGE
        )
    except subprocess.CalledProcessError:
        oha = "not pulled yet"
    env = (
        "\n".join(
            [
                f"platform: {plat}",
                f"os: {platform.platform()}",
                f"cpu: {cpu_name(plat)}",
                "docker: "
                + output("docker", "version", "-f", "{{.Server.Version}}")
                + ", "
                + output(
                    "docker",
                    "info",
                    "-f",
                    "{{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes",
                ),
                f"compose: {output('docker', 'compose', 'version', '--short')}",
                f"oha image: {oha}",
            ]
        )
        + "\n"
    )
    (res / "env.txt").write_text(env, encoding="utf-8")
    return env


def main() -> None:
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
    p.add_argument(
        "--unmetered-rates",
        nargs="+",
        type=int,
        default=[4000, 6000, 8000, 10000, 12000],
    )
    p.add_argument(
        "--metered-rates", nargs="+", type=int, default=[2000, 3000, 4000, 5000, 6000]
    )
    p.add_argument("--procs", nargs="+", type=int, default=[1, 2, 4, 8])
    p.add_argument("--overhead-rates", nargs="+", type=int, default=[2000, 5000, 10000])
    args = p.parse_args()

    # compose recreates nginx on 8080 without it
    os.environ.setdefault("STAGE_PORT", "8085")
    plat = platform_name()
    preflight(plat)

    res = ROOT / "bench-results" / f"{datetime.now():%Y%m%d-%H%M%S}"
    res.mkdir(parents=True)
    suite = Suite(res)
    suite.resolve_containers()
    suite.log(f"results in {res.relative_to(ROOT)}")
    suite.log(write_env(res, plat).replace("\n", "; "))
    nginx_conf = output("docker", "compose", "exec", "-T", "nginx", "nginx", "-T")
    upstream = [
        line.strip()
        for line in nginx_conf.splitlines()
        if "keepalive " in line or "proxy_http_version" in line
    ]
    (res / "nginx-upstream.txt").write_text(
        "\n".join(upstream) + "\n", encoding="utf-8"
    )
    suite.log(f"nginx upstream settings: {' '.join(upstream) or 'none'}")

    if not args.skip_meter:
        # bench.py provisions and deletes its own bench org(s), so run it before provisioning.
        suite.log("B1 meter overhead, single requests")
        out = suite.in_api(
            "python", "scripts/bench.py", "store", "-n", "5000", "-c", "1"
        )
        (res / "B1.txt").write_text(out, encoding="utf-8")
        for n in args.procs:
            suite.log(f"B2 meter max load, --procs {n}")
            out = suite.in_api(
                "python",
                "scripts/bench.py",
                "store",
                "-n",
                "5000",
                "-c",
                "16",
                "--procs",
                str(n),
            )
            (res / f"B2-procs{n}.txt").write_text(out, encoding="utf-8")
        for rate in args.overhead_rates:
            procs = math.ceil(rate / METER_RATE_PER_PROC)
            suite.log(f"B3 meter overhead at {rate} ops/s, --procs {procs}")
            out = suite.in_api(
                "python",
                "scripts/bench.py",
                "store",
                "--op",
                "refund",
                "--rate",
                str(rate),
                "-n",
                str(OVERHEAD_SECONDS * rate // procs),
                "-c",
                "50",
                "--procs",
                str(procs),
            )
            (res / f"B3-rate{rate}.txt").write_text(out, encoding="utf-8")

    if args.servers:
        suite.in_api("python", "-c", PROVISION)
        try:
            for server in args.servers:
                suite.start_server(server)
                for rate in args.unmetered_rates:
                    suite.oha(f"B4-{server}-unmetered-{rate}", 20, 512, rate, "/")
                for rate in args.metered_rates:
                    suite.oha(
                        f"B5-{server}-metered-{rate}", 20, 512, rate, METERED_PATH
                    )
                suite.oha(f"B6-{server}-sustained-2000", 60, 256, 2000, METERED_PATH)
        finally:
            suite.stop_oha()
            suite.log("deleting bench keys")
            suite.in_api("python", "-c", CLEANUP)

    summary = suite.summary()
    (res / "summary.md").write_text(summary, encoding="utf-8")
    suite.emit(summary)
    suite.log("done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("interrupted")
