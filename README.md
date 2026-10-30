# api-metred-quota

Per-org, per-feature quota metering for a FastAPI service. Redis + Lua reserve/commit/refund keeps charges atomic across replicas; `X-Request-Id` dedupes retries. Design and trade-offs: [DESIGN.md](DESIGN.md).

## Run

```sh
docker compose up -d --build --wait   # 3 API replicas behind nginx, Redis (AOF), seeded limits
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
