# Quota metered API implemented in Python

## Context
Meter users based on organizational/feature limits, enforce these quotas.

Per feature operation lookups i.e. container-tracking 1000, sailing-schedule lookup 1000, a single request can also consume multiple units based on how complicated it is.

Concurrency has to be something to be taken care of, an org can call multiple APIs at the same time and their quotas can be exhausted because of this. On top of that we are assuming multiple instances of the service which can also simultaneously consume the units.

## Architecture

API with middlewares that reach redis to make quota checks

Redis houses quota limits, i.e. quota:{org}:feature:limit:period (mutation discouraged)
Redis houses another counter that stores the usage key with a period, i.e. orgID:feature:usage:period
Reset will change the period and therefore the keys, new month's renewal explicitly handled by another billing/renewal api/store (DEFERRED, additional Postgres required)

Atomicity with reserve/refund model with Redis's lua level scripting
reserve when the request is received, refund if the request fails.

Reservation through request ID, deduplication of charges + reserve / refund work through this request ID, we echo request IDs back in case of retries to nudge the client to reuse them. No reservations when vendor is failing. Committed / Charged request ID never calls the vendor again, TTL for every request ID 5 minutes. Bounded memory usage given 10K/s with 5mins TTL. Fingerprinting the request ID so that different body with the request ID is rejected.

## Requirements

1. Scale: >5000 organizations growing to 50,000 within a year. 30 metered features.
2. Traffic: ~2000 quota operations/second sustained, peaking at 10,000/second.
3. Horizontally scaled fleet
4. Reset of the quota.
5. Quota API that returns available quota and usage.

## Goals

1. Latency: <10ms per operation.
    a. Correction: Overhead should be <10ms per operation. i.e. Redis reserve/refund, atomicity check round trip.

## Key Decisions

1. Middleware/Decorator pattern
2. Redis/MemCached
    a. Redis clear choice since we can script at the Redis level for atomic checks
3. Atomicity maintained at the DB level. (CHOSEN: one Lua script, `reserve.lua`, does the dedupe, limit check, charge and reservation write in one round trip. Redis runs a script to completion, so no other instance's command can land in between, and a rejected request writes nothing.)
4. Balance stored separate or same as Redis? (DEFERRED, see Future work 1)
    a. Postgres stores the organization's limit
5. Redis cluster necessary if scale's required, bucket keys to nodes based on hashing
6. Batches are all-or-nothing and single feature (CHOSEN). Cost is the op count, reserved as one unit: one reservation key, one stream event with the total cost. A batch spanning features returns `501 mixed_batch_unsupported` (DEFERRED, see Future work 3).
    a. Single feature keeps `reserve.lua`/`commit.lua`/`refund.lua` on one usage key, the same scripts as a single request.
    b. Atomicity holds for single-feature batches: `reserve.lua` checks and charges the whole batch cost in one script run, so no other instance lands in between. Parallel batches near the limit never go over it and a rejected batch charges nothing (BA11, `test_batches_race_near_the_limit`, re-run across replicas in `tests/integration`).

## Correctness / failure handling

1. Failed requests shouldn't count towards the quota
2. Retries shouldn't count as double exhaustion of quota.
    a. What if the retry arrives while the first request is still in progress? due to client side timeouts, slow vendors can make this problem more prominent
3. Writing/Reading to/from postgres on every charge not applicable, performance problem since the target is <10ms (REJECTED: latency).
4. Redis durability important in case of failures since Redis going down means that the last state of usage is the source of truth, otherwise no quota! (CHOSEN)
    a. appendonly, appendfsync everysec, maxmemory-policy noeviction
5. Recovery path if we use postgres + Redis (deferred)
    a. if Redis wiped, snapshots of usage logs give us the exact rebuild, but it can also be stale.

6. Reserve then refund is redundant when 404 entries exist but behaviourally it makes sense since production environments will tend to have valid scenarios more often than not. Reserve/Refund is cheaper to always do due to Redis's fast memory storage.

7. Commit/refund runs after the response is sent (CHOSEN: latency). The client never waits on the settle round trip, but for a short window after it receives the response, Redis still shows the reservation as `reserved`.
    a. A retry with the same request id in that window gets `409 request_in_progress` (or `422 request_id_reused` if the body differs) instead of `already_processed`.
    b. Success: usage was already charged at reserve, only the usage stream event (written by commit) lags. Failure: usage stays charged until the refund lands, so it briefly over-reports. Quota is never over-granted, since reserve is what holds the units.
    c. Tests must account for it, see `tests/README.md`.

## Testing & validation

1. Integration tests, including Redis spinup
2. Quota exhaustion validation, negative quota should be absolutely prohibited.
3. Reset updates the quotas
4. ab to stress load the system, checking if there's a more apt tool for python development.
5. oha seems to be a good alternative.
6. Benchmark results cap at 4.5Kops/s for unmetered routes as of now, but benchmarks can be limited in queuing requests, adding multi process benchmarking as well as trying granian to reach peak 10K ops/s
7. httpx bottleneck @ 200ops/s, oha gives real benchmark results
8. Concurrency proof under load: B8 in `scripts/bench_suite.py` (CHOSEN). The assignment asks for "a concurrency test that proves quota is never over-served, not just one that passes with no contention", and for concurrent correctness to be demonstrated under load. In-process tests can't race (Limitations 9a), so this runs against the real stack.
    a. Setup: one org with limit L (1000, 5000, 10000). oha sends 3L+5 requests unthrottled, 512 in flight, nginx round-robins them across 8 replicas sharing one Redis. Every replica races for the last units, the assignment's "500 left, 500 units at once across instances", oversubscribed 3x.
    b. Passes only if: exactly L got 200 and the rest 429, the Redis usage counter is L, the usage stream charged exactly L units (only this run's events), and there are exactly L reservation keys, all `committed`. Over-serving shows up as 200s > L or usage > L, a lost or double refund as usage ≠ 200s.
    c. Reserve/commit/refund each leave a trace on the reservation key: `reserve.lua` writes one only for a granted request (a 429 writes nothing), `commit.lua` marks it `committed`, `refund.lua` deletes it. So {committed: L} means L reserves, L commits, no refunds, nothing left `reserved`.
    d. Contention is real, and shown from Redis, not assumed: every usage event carries the `instance` (container hostname) that committed it, and B8 requires more than one. In practice all 8 replicas commit, within ~10% of each other. 512 connections over 8 processes, 30K requests in ~6s. A read-then-write check would let several replicas read the same remaining count and all charge it.
    e. Result (uvicorn, 8 replicas, 2026-10-31):

        | Limit | Requests | 200 | 429 | Redis used | Stream charged | Reservations | Replicas | Commits per replica |
        | --- | --- | --- | --- | --- | --- | --- | --- | --- |
        | 1000 | 3005 | 1000 | 2005 | 1000 | 1000 | 1000 committed | 8 | 147, 141, 135, 132, 127, 127, 97, 94 |
        | 5000 | 15005 | 5000 | 10005 | 5000 | 5000 | 5000 committed | 8 | 666, 662, 658, 649, 644, 584, 576, 561 |
        | 10000 | 30005 | 10000 | 20005 | 10000 | 10000 | 10000 committed | 8 | 1283, 1281, 1280, 1271, 1268, 1240, 1199, 1178 |

    f. Same assertions as `test_burst_never_overserves` in `tests/integration` (3 replicas), at load. oha only reports status counts, so one check stays in the integration suite: every 429 saw used == limit, i.e. nothing was rejected while units were left.
    g. Pending: mutation check, swap `reserve.lua` for a non-atomic GET then INCRBY and confirm B8 fails. Until then B8 passing doesn't by itself show it would catch a race.

## Limitations & risks

1. Response validation, i.e. bad responses from vendors should they use up the quota?
2. Request requires N units, org has M < N: all-or-nothing, the batch gets 429 and nothing is charged. Partial request fulfilment dropped: ops are read-only so running the first M would be safe, but billing gets complicated (reserve has to return a grant, callers resend skipped ops under a new id, mixed features need prefix rules).
3. Do monthly quotas transfer ?
4. If opted for org:feature:limit:period and org:feature:usage:period
    a. if the renewal job hasn't run yet, at 00:00:01 the API can say that there's no quota
5. <10ms overhead realistically difficult on 10/15K requests per second.
6. If network failure causes a request to not be received by the client, they remain charged and don't get refunded.
7. nit: Ideally all code should be native to Python but since Redis scripting requires Lua, we have some source files that have lua scripting here. Can use something like this: https://github.com/IgnaceMaes/redis-lua-py, not a popular package, but their README shows our use case explicitly, wary: more dependencies.
8. Local Redis required to run pytest, bummer but because of the lua scripts this adds some friction to the dev loop. Fixtures can work but they would be limiting as well.
9. We can use fakeredis to do this, it has a lua interpreter! (CHOSEN)
    a. Note: in-process can't test concurrency. fakeredis never yields to the event loop and httpx `ASGITransport` runs each request to completion, so 50 gathered requests run one at a time (measured: peak 1 live reservation). Races pass for free. Concurrency tests (`tests/integration/test_concurrency.py`) run only in Docker: nginx + 3 replicas + real Redis.
10. Scenarios which can fail the reserve/refund model.
    a. Redis timeouts / connection issues / clients disconnecting mid-request
    b. Checks needed for enforcement under load
    c. A reservation that expires mid-request leaks or double-charges quota, and it gets logged, not fixed.

## Future work / scaling
(What changes at 10× scale.)

1. Postgres deployment that houses usage logs, billing and entitlements control for an organization.
    a. houses key limits per feature that get transposed to Redis for a period
2. If the client never received a successful response due to network failure, we charge them regardless. To solve this, we can store cached responses for 60 seconds in Redis (compressed with zstd/gzip). Overkill for v1.
3. Multi-feature batches: one multi-key reserve over every feature's limit/usage key.
    a. `reserve.lua` checks every feature before writing anything, so one feature over its limit (or unprovisioned) charges none of them, then `INCRBY`s each. Reservation hash keeps a `cost:<feature>` field per feature.
    b. `commit.lua` writes one stream event per feature, so billing stays per feature. `refund.lua` `DECRBY`s each usage key from its `cost:<feature>`.
    c. Usage keys go in KEYS, not built inside Lua, and all share the `{org}` hash tag, so the script stays in one Cluster slot.
    d. A single request becomes a batch of one feature, one code path.

## Testing environment

1. VSCode tasks: Spin up service / Redis instance, Seed mock data, reset mock data
2. Scenarios for important components: idempotency, batch
3. Manual checklist (basic metering, concurrency, idempotency, batch): [docs/manual-scenarios.md](docs/manual-scenarios.md)
4. One shot docker compose up: API, Redis, health checks, seed data

## Technical details
1. uv/ruff tooling with venv
2. FastAPI with uvicorn/granian (benchmarking to pick one)
3. docker compose to horizontally scale

## Appendix

Benchmark results and methodology: [docs/benchmarks.md](docs/benchmarks.md).

Design and key decisions are mine, the following implementations were assisted by AI.

The commit history is separated by chunks of code, v1 will mark usable code.

1. AI assisted parts
    a. Reserve / Refund lua scripting code.
    b. Seeding data, mock scenarios scaffolding code, i.e. orgs, features, limits, manual-scenarios.
    c. SHA256 fingerprinting to match valid retries.
    d. Router that shows all the Redis keys for each org
    e. Infra scaffolding i.e. docker compose yaml
    f. Swagger changes
    g. README document with a quickstart added
    h. Benchmarking scaffolding, design and tools decided by me i.e. oha, host/docker performance differences, latency correction, fixed qps (if any).
    i. Sanity checking, documentation checks
