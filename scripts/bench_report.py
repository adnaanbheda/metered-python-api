"""summary.md for one bench_suite.py run: a table per bench from its redis/ and api/ results.

bench_suite.py writes it at the end of every run. To rebuild it for an existing run:
    uv run scripts/bench_report.py bench-results/<timestamp>
"""

import argparse
import json
from pathlib import Path
from typing import Any


def results(res: Path, bench: str) -> list[tuple[str, Any]]:
    """(run name, parsed JSON) for each run of `bench`, oldest first."""
    files = sorted((res / bench).glob("*.json"), key=lambda f: f.stat().st_mtime)
    return [(f.stem, json.loads(f.read_text(encoding="utf-8"))) for f in files]


def md_table(header: list[str], rows: list[list[object]]) -> str:
    """Markdown table, or "" when there are no rows."""
    if not rows:
        return ""
    lines = [header, ["---"] * len(header), *rows]
    return "\n".join("| " + " | ".join(map(str, row)) + " |" for row in lines) + "\n"


def ms(seconds: float | None) -> str:
    return "-" if seconds is None else f"{seconds * 1000:.1f}"


def redis_summary(res: Path) -> str:
    """store_bench.py --json output: one row per op."""
    header = ["Run", "Op", "ops/s", "p50 ms", "p95 ms", "p99 ms", "max ms"]
    rows = []
    for name, ops in results(res, "redis"):
        for r in ops:
            latencies = [r[k] for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms")]
            rows.append([name, r["name"], f"{r['ops_per_s']:,}", *latencies])
    return md_table(header, rows)


def api_row(name: str, r: dict) -> list[object]:
    """One oha JSON report. Deadline errors are requests still in flight when -z ran out."""
    codes = r.get("statusCodeDistribution", {})
    non200 = {k: v for k, v in codes.items() if k != "200"}
    errors = {
        k: v for k, v in r.get("errorDistribution", {}).items() if "deadline" not in k
    }
    p = r.get("latencyPercentiles") or {}
    ok_per_s = f"{codes.get('200', 0) / r['summary']['total']:,.0f}"
    p50, p99 = ms(p.get("p50")), ms(p.get("p99"))
    return [name, ok_per_s, non200 or "-", errors or "-", p50, p99]


def api_summary(res: Path) -> str:
    header = ["Run", "200s/s", "Non-200", "Errors (excl. deadline)", "p50 ms", "p99 ms"]
    return md_table(header, [api_row(name, r) for name, r in results(res, "api")])


QUOTA_FIELDS = (
    "limit",
    "requests",
    "ok",
    "rejected",
    "other",
    "used",
    "live_reservations",
)


def quota_summary(res: Path) -> str:
    """B7: exactly `limit` requests may succeed when 3x that many race for it."""
    header = [
        "Run",
        "Limit",
        "Requests",
        "200",
        "429",
        "Other",
        "Redis used",
        "Live res.",
        "",
    ]
    rows = [
        [name, *(c[k] for k in QUOTA_FIELDS), "PASS" if c["pass"] else "FAIL"]
        for name, c in results(res, "quota")
    ]
    return md_table(header, rows)


def summary(res: Path) -> str:
    """Write <res>/summary.md and return it. A bench with no results gets no section."""
    sections = {
        "Redis bench": redis_summary(res),
        "Api bench": api_summary(res),
        "Quota check": quota_summary(res),
    }
    md = "\n".join(f"## {title}\n\n{body}" for title, body in sections.items() if body)
    (res / "summary.md").write_text(md, encoding="utf-8")
    return md


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("res", type=Path, help="a bench-results/<timestamp> directory")
    print(summary(p.parse_args().res))
