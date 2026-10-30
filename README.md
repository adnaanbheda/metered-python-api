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
| 401 / 400 | `X-Org-Id` missing / invalid |
| 403 | org has no quota for the feature |
| 429 | quota exhausted |
| 409 | request ID already processed (retry) |
| 422 | request ID reused with different params |

## Test

```sh
uv run pytest                    # scenarios on fakeredis, no Docker
uv run pytest -m integration     # full compose stack: concurrency across replicas
```

Scenarios live in [tests/scenarios/](tests/scenarios/) as prose + YAML; manual checklist in [docs/manual-scenarios.md](docs/manual-scenarios.md).
