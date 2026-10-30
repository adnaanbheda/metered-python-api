# Mocks

| File | What | How to change |
|---|---|---|
| `orgs.json` | per-org quota limits | edit by hand or `scripts/seed.py set-limit` |
| `containers.json`, `sailings.json`, `shipments.json` | API response data | `scripts/mocks.py gen` (deterministic, `--seed`) |

```powershell
$env:PYTHONPATH="src"
uv run python scripts/seed.py gen                         # write response data
uv run python scripts/seed.py seed --reset                # load limits into Redis
uv run python scripts/seed.py show acme                   # usage / limit
uv run python scripts/seed.py set-limit tiny shipment-data 5
uv run python scripts/seed.py seed --period 2026-11       # next month's limits
```

Test orgs: `acme` (normal), `globex` (high), `tiny` (limit 1, test 429), `noquota` (test 403).
Restart the server after `gen`: data loads once at startup.
