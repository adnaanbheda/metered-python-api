# api-metred-quota

Per-org, per-feature quota metering for a FastAPI service. Redis + Lua reserve/commit/refund keeps charges atomic across replicas; `X-Request-Id` dedupes retries. Design and trade-offs: [DESIGN.md](DESIGN.md).

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

```sh
uv run scripts/bench_suite.py                                 # everything, uvicorn + granian (~25 min)
uv run scripts/bench_suite.py --servers granian               # B1-B6, one server
uv run scripts/bench_suite.py --servers                       # B1-B3 only, no HTTP runs
uv run scripts/bench_suite.py --skip-meter --servers granian  # B4-B6 only
uv run scripts/bench_suite.py --metered-rates 2000 4000 6000  # custom B5 sweep
```

Raw output and a `summary.md` land in `bench-results/<timestamp>/`.

The meter alone, without the suite (Redis published on 6381):

```sh
PYTHONPATH=src REDIS_URL=redis://127.0.0.1:6381/0 uv run scripts/bench.py store -n 20000 -c 64
```

