# Mocks

| File | What | How to change |
|---|---|---|
| `orgs.json` | per-org quota limits | edit by hand, then `seed.py reset` |
| `containers.json`, `sailings.json`, `shipments.json` | API response data | edit by hand (fixed fixtures; tests reference their ids) |

```powershell
$env:PYTHONPATH="src"
uv run python scripts/seed.py seed                       # load limits for current month
uv run python scripts/seed.py seed --period 2026-11      # add another month
uv run python scripts/seed.py remove --period 2026-11    # delete that month's limits + usage
uv run python scripts/seed.py reset                      # wipe everything, reload current month
```

Test orgs: `acme` (normal), `globex` (high), `tiny` (limit 1, test 429), `noquota` (test 403).
Restart the server after editing: data loads once at startup.
