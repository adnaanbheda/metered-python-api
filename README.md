# api-metered-quota

[DESIGN.md](DESIGN.md)

## The problem

This is an assignment that adds quotas to APIs, the quota is per org per operation/endpoint, every org gets a fixed amount of units per operation for a certain amount of time. If the quota is exhausted, the request is denied with a quota exceeded message. The quotas are reset monthly at UTC.

Implemented using FastAPI with Redis as the store with atomicity maintained per request, more information about the context of the key decisions, technical details, concurrency, benchmark numbers at [DESIGN.md](DESIGN.md).

## Assignment map

Each ask in the brief, and where it lives.

| Ask | Code | Tests | DESIGN.md |
|---|---|---|---|
| Integration shape | Middleware: [meter.py](src/middlewares/meter.py), [identity.py](src/middlewares/identity.py). Consumer routes: [operations.py](src/operations.py) | | [Architecture](DESIGN.md#architecture), [Key Decisions](DESIGN.md#key-decisions) 1 |
| Concurrent correctness | [reserve.lua](src/lua/reserve.lua): check and charge in one script run, [store.py](src/store.py) | [test_concurrency.py](tests/integration/test_concurrency.py) (nginx + 3 replicas + real Redis) | [Key Decisions](DESIGN.md#key-decisions) 3 |
| ...demonstrated under load | B8 in [bench_suite.py](scripts/bench_suite.py): 3L+5 requests, 512 in flight, 8 replicas | Exactly L served, Redis and stream agree, all replicas committed | [Testing & validation](DESIGN.md#testing--validation) 8 |
| State across instances | Redis only (AOF), [compose.yaml](compose.yaml) | [test_stack.py](tests/integration/test_stack.py) | [Architecture](DESIGN.md#architecture), [Correctness](DESIGN.md#correctness--failure-handling) 4 |
| Batch: all-or-nothing | `POST /batch` in [operations.py](src/operations.py) | `test_batches_race_near_the_limit`, `test_batch_one_unknown_op_fails_and_refunds_all`, `test_mixed_batch_501` | [Key Decisions](DESIGN.md#key-decisions) 6, [Limitations](DESIGN.md#limitations--risks) 2 |
| Failure & retries | [commit.lua](src/lua/commit.lua), [refund.lua](src/lua/refund.lua), [fingerprint.py](src/utils/fingerprint.py) | [test_idempotency.py](tests/test_idempotency.py), [scenarios/idempotency.md](tests/scenarios/idempotency.md), `test_parallel_failures_all_refunded`, `test_same_request_id_in_parallel_charges_once`, `test_retry_on_another_replica_is_deduped` | [Correctness](DESIGN.md#correctness--failure-handling) 1, 2, 7 |
| Reset & reporting | Calendar month, UTC: [periods.py](src/utils/periods.py). `GET /usage`: [usage.py](src/usage.py) | [test_period_rollover.py](tests/test_period_rollover.py) | [Architecture](DESIGN.md#architecture) |
| Single-command bring-up | [compose.yaml](compose.yaml), see [Run](#run) | | |
| Load-test numbers | [bench_suite.py](scripts/bench_suite.py), [bench_remote.sh](scripts/bench_remote.sh), see [Bench](#bench) | | [Isolated machines](docs/benchmarks.md#isolated-machines-2026-11-01): 2k–15k req/s held on 2 droplets, 0 errors in 144 runs. [Benchmarking](DESIGN.md#benchmarking) |
| Where it falls over | | | Not reached by 15k req/s on a 16 vCPU stack: uvicorn meter-on p99 6.2ms, inside budget; granian at its knee (p99 85ms). The ~5.1k cap in [Benchmarking](DESIGN.md#benchmarking) was the shared workstation's CPU, not the stack. [Limitations](DESIGN.md#limitations--risks) |
| 50k-org scale | | | [Future work](DESIGN.md#future-work--scaling) |
| AI-assisted vs own decisions | | | [Appendix](DESIGN.md#appendix) |

## Quick start

```sh
docker compose up -d --build --wait   # run: 8 API replicas behind nginx, Redis (AOF), seeded limits
uv run scripts/bench_suite.py         # benchmark: defaults B1-B8, uvicorn + granian (~15 min)
```

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

