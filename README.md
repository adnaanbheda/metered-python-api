# api-metred-quota

[DESIGN.md](DESIGN.md)

## The problem

Every org gets a monthly allowance per feature: 1,000 container-tracking lookups, 1,000 sailing-schedule lookups, and so on, with heavier requests costing more than one unit. Sounds like a counter. It's a counter that:

- is hit by the same org from many tabs, scripts and that one cron job nobody remembers writing, all at once;
- lives behind a fleet of replicas, none of which trust each other's arithmetic;
- must never charge twice when a client retries because its Wi-Fi blinked;
- must give the unit back when the upstream vendor falls over (not the customer's fault, not their bill);
- resets on the 1st of the month, ideally without anyone being on call for it;
- does all this for 5k orgs (50k next year) × 30 features at ~2k ops/s sustained, 10k peak, adding under 10ms.

`count += 1` gets you exactly one of those, and only on a quiet day. This repo is the rest.

Per-org, per-feature quota metering for a FastAPI service. Redis + Lua reserve/commit/refund keeps charges atomic across replicas; `X-Request-Id` dedupes retries.

## Run

```sh
docker compose up -d --build --wait   # 8 API replicas behind nginx, Redis (AOF), seeded limits
```

Swagger: http://localhost:8080/docs

```sh
curl -H "X-Org-Id: acme" -H "X-Request-Id: r1" "localhost:8080/container_tracking?container_id=MSCU1719583"
curl -H "X-Org-Id: acme" localhost:8080/usage
```

Seeded orgs ([mocks/orgs.json](mocks/orgs.json)): `acme`, `globex`, `tiny` (limit 1), `noquota` (none).

| Status | Meaning |
|---|---|
| 401 | `X-Org-Id` missing |
| 400 | `X-Request-Id` over 128 chars |
| 403 | no quota provisioned for the feature this period |
| 429 | quota exhausted |
| 409 | request ID still in progress, or already processed |
| 422 | request ID reused with a different request |

No `X-Request-Id` means one is generated, so that call can't be safely retried.

## Test

```sh
uv run pytest                    # scenarios on fakeredis, no Docker
uv run pytest -m integration     # full compose stack: concurrency across replicas
```

Scenarios live in [tests/scenarios/](tests/scenarios/) as prose + YAML; manual checklist in [docs/manual-scenarios.md](docs/manual-scenarios.md).

## Bench

Needs the stack up (`docker compose up -d --wait`). Load runs from inside the compose network, so nothing else to install. Results and analysis: [docs/benchmarks.md](docs/benchmarks.md).

| Run | Measures |
|---|---|
| B1 | meter overhead, single requests (reserve + commit straight against Redis) |
| B2 | meter max load, 1/2/4/8 client processes |
| B3 | meter overhead at fixed rate (2k/5k/10k ops/s, 20 s) |
| B4 | HTTP max load, unmetered route |
| B5 | HTTP max load, metered route |
| B6 | sustained 2k req/s metered, 60 s |
| B7 | quota under the replica race, one limit |
| **B8** | **concurrency under load: quota never over-served, 512 connections across all replicas, limits 1k/5k/10k** |

B8 is the concurrency proof. It passes only if exactly `limit` requests got `200`, the Redis counter and usage stream agree, and more than one replica committed. See [DESIGN.md](DESIGN.md), Testing & validation, item 8.

```sh
uv run scripts/bench_suite.py                                 # defaults B1-B8, uvicorn + granian (~15 min)
uv run scripts/bench_suite.py --grid quick                    # smaller grid: fewer rates/procs, 5 s runs (~5 min)
uv run scripts/bench_suite.py --only redis                    # B1-B3, redis bench
uv run scripts/bench_suite.py --only api                      # B4-B8, api bench
uv run scripts/bench_suite.py --servers granian               # B1-B8, api bench on one server
uv run scripts/bench_suite.py --rates 2000 4000 6000          # custom B3-B5 sweep
uv run scripts/bench_suite.py --no-rates                      # B4-B5 unthrottled, B3 skipped
```

One custom run instead of the defaults, same containers and output:

```sh
uv run scripts/bench_suite.py redis -n 20000 -c 64 --procs 4         # store_bench.py flags
uv run scripts/bench_suite.py redis --op refund --rate 3000 -n 60000
uv run scripts/bench_suite.py api --route unmetered -q 8000          # per server
uv run scripts/bench_suite.py api -c 1024 -z 30s --servers granian   # no -q: unthrottled
```

Raw output (`redis/`, `api/`) and a `summary.md` land in `bench-results/<timestamp>/`. Rebuild the summary for an existing run with `uv run scripts/bench_report.py bench-results/<timestamp>`.

The meter alone, without the suite (Redis published on 6381):

```sh
PYTHONPATH=src REDIS_URL=redis://127.0.0.1:6381/0 uv run scripts/store_bench.py -n 20000 -c 64
```

