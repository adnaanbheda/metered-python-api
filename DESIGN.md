# Quota metered API implemented in Python

## Context
Meter users based on organizational/feature limits, enforce these quotas.

Per feature operation lookups i.e. container-tracking 1000, sailing-schedule lookup 1000, a single request can also consume multiple units based on how complicated it is.

Concurrency has to be something to be taken care of, an org can call multiple APIs at the same time and their quotas can be exhausted because of this. On top of that we are assuming multiple instances of the service which can also simultaneously consume the units.

## Architecture

API with middlewares that reach to redis to make quota checks

Redis houses quota limits, i.e. orgID:feature:limit:period (mutation discouraged)
Redis houses another counter that stores the usage key with a period, i.e. orgID:feature:usage:period
Reset will change the period and therefore the keys, new month's renewal explicitly handled by another billing/renewal api/store

Atomicity with reserve/refund model with Redis's lua level scripting
reserve when the request received, refund if the request fails.

Reservation through request ID, deduplication of charges + reserve / refund work through this request ID, we echo request IDs back in case of retries to nudge the client to reuse them. No reservations when vendor is failing. Comitted / Charged request ID never calls the vendor again, TTL for every request ID 5 minutes. Bounded memory usage given 10K/s with 5mins TTL. Fingerprinting the request ID so that different body with the request ID is rejected.

## Requirements

1. Scale: >5000 organizations growing to 50,000 within a year. 30 metered features.
2. Traffic: ~2000 quota operations/second sustained, peaking at 10,000/second.
3. Horizontally scaled fleet
4. Reset of the quota.
5. Quota API that returns available quota and usage.

## Goals

1. Latency: <10ms per operation.

## Key Decisions

1. Middleware/Decorator pattern
2. Redis/MemCache
    a. Redis clear choice since we can script at the redis level for atommic checks
3. Atomicity maintained at the DB level.
4. Balance stored separate or same as Redis ?
    a. Postgres stores the organization's limit
5. Redis cluster necessary if scale's required, bucket keys to nodes based on hashing

## Correctness / failure handling
(Invariants, failure modes, retries, recovery.)

1. Failed requests shouldn't count towards the quota
2. Retries shouldn't count as double exhaustion of quota.
    a. What if the retry arrives while the first request is still in process? due to client side timeouts, slow vendors can make this problem more prominent
3. Writing/Reading to/from postgres on every charge, performance problem since the target is <10ms
4. Redis durability important in case of failures since redis going down means that the last state of usage is the source of truth, otherwise no quota! (CHOSEN)
    a. appendonly, appendfsync everysec, maxmemory-policy noeviction
5. Recovery path if we use postgres + redis
    a. if redis wiped, snapshots of usage logs give us the exact rebuild, but it can also be stale.

6. Reserve then refund is redundant when 404 entries exist but behaviourally it makes sense since production environments will tend to have valid scenarios more often than not. Reserve/Refund is cheaper to always do due to Redis's fast memory storage.

## Testing & validation

1. Integration tests, including redis spinup
2. Quota exhaustion validation, negative quota should be absolutely prohibited.
3. Reset updates the quotas

## Limitations & risks
(Where it breaks, known gaps.)

1. Response validation, i.e. bad responses from vendors should they use up the quota?
2. Request requires N units, org has M, What happens in the case of M < N, partial request fulfilment or all-or-nothing?
3. Do monthly quotas transfer ?
4. If opted for org:feature:limit:period and org:feature:usage:period
    a. if the renewal job hasn't run yet, at 00:00:01 the API can say that there's no quota
5. <10ms overhead realistically difficult on 10/15K requests per second.
6. If network failure causes a request to not be received by the client, they remain charged and don't
7. nit: Ideally all code should be native to Python but since Redis scripting requires Lua, we have some source files that have lua scripting here. Can use something like this: https://github.com/IgnaceMaes/redis-lua-py, not a popular package, but their README shows our usecase explicitly, wary: more dependencies.
8. Local redis required to run pytest, bummer but because of the lua scripts this adds some friction to the dev loop. Fixtures can work but they would be limiting as well.
9. We can use fakeredis to do this, it has a lua interpreter!

## Future work / scaling
(What changes at 10× scale.)

1. Postgres deployment that houses usage logs, billing and entitlements control for an organization.
    a. houses key limits per feature that get transposed to redis for a period
2. If the client never received a successful response due to network failure, we charge them regardless. To solve this, we can store cached responses for 60 seconds in redis (compressed with zstd/gzip). Overkill for v1.

## Testing environment

1. VSCode tasks: Spin up service / redis instance, Seed mock data, reset mock data
2. Scenarios for important components: idempotency, batch
3. Manual checklist (basic metering, concurrency, idempotency, batch): [docs/manual-scenarios.md](docs/manual-scenarios.md)
4. One shot docker compose up: api, redis, health checks, seed data

## Technical details
1. uv/ruff tooling with venv
2. FastAPI with uvicorn
3. docker compose to horizontally scale

## Appendix
AI-assisted parts, how to run, raw benchmark data.

The commit history is separated by chunks of code, v1 will mark usable code.

1. AI assisted parts
    a. Reserve / Refund lua scripting code.
    b. Seeding data, mock scenarios scaffolding code, i.e. orgs, features, limits.
    c. Router that shows all the redis keys for each org
    c. Infra scaffolding i.e. docker compose yaml
    d. Swagger changes
    e. README document with a quickstart added
