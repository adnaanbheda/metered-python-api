# Manual test scenarios

Checklist for verifying the metering middleware by hand against a running API and Redis.
Each scenario lists the requests to send, the expected HTTP response, and the Redis state to confirm afterwards.

## Setup

1. Start Redis and the API (VSCode task `dev`, or `redis` + `api`).
2. Before **each** scenario run `seed: reset` then `seed`. This gives these limits for the current period:

   | org       | container-tracking | sailing-schedule | shipment-data |
   |-----------|-------------------:|-----------------:|--------------:|
   | `acme`    | 100                | 50               | 20            |
   | `globex`  | 1000               | 1000             | 1000          |
   | `tiny`    | 1                  | 1                | 5             |
   | `noquota` | none               | none             | none          |

3. Open a Redis shell with the `redis-cli` task (`docker exec -it quota-redis redis-cli`).

Examples use Git Bash `curl`; in PowerShell call `curl.exe`. `$P` is the current period (`YYYY-MM`, e.g. `2026-10`).

```bash
API=http://127.0.0.1:8000
P=$(date -u +%Y-%m)
ct() { curl -s -i "$API/container_tracking?container_id=${3:-X}" -H "X-Org-Id: $1" ${2:+-H "X-Request-Id: $2"}; }
batch() { curl -s -i -X POST "$API/batch" -H "X-Org-Id: $1" ${2:+-H "X-Request-Id: $2"} -H "Content-Type: application/json" -d "$3"; }
```

### Redis keys

| Key | Type | Meaning |
|-----|------|---------|
| `quota:{<org>}:<feature>:limit:<P>` | string | Limit for the period. Never changed by requests. |
| `quota:{<org>}:<feature>:usage:<P>` | string | Units used. `EXPIREAT` start of next month + 1 day. |
| `res:{<org>}:<request_id>` | hash | `state` (`reserved`/`committed`), `fp`, `cost`. TTL 300s. Deleted on refund. |
| `usage_events` | stream | One entry per committed request: `org_id`, `feature`, `request_id`, `period`, `cost`. |

Useful commands:

```
GET   quota:{acme}:container-tracking:usage:2026-10
TTL   quota:{acme}:container-tracking:usage:2026-10
HGETALL res:{acme}:a
TTL   res:{acme}:a
XLEN  usage_events
XREVRANGE usage_events + - COUNT 5
```

---

## 1. Basic metering

| # | Scenario | Requests | Expected response | Expected Redis |
|---|----------|----------|-------------------|----------------|
| B1 | Successful call is charged | `ct acme a` | `200`, response header `X-Request-Id: a` | usage `1`; `res:{acme}:a` has `state=committed`, `cost=1`, TTL ≤ 300; `XLEN usage_events` = 1 with `org_id=acme feature=container-tracking request_id=a period=$P cost=1` |
| B2 | Usage key expiry | after B1: `TTL` on usage key | — | Positive TTL; `EXPIRETIME` = 00:00 UTC on the 2nd of next month |
| B3 | Limit key untouched | after B1: `GET` limit key | — | Still `100` |
| B4 | No request id given | `ct acme` (no `X-Request-Id`) | `200`, server-generated 32-char hex `X-Request-Id` in response | usage `1`; `res:{acme}:<generated id>` exists, committed |
| B5 | Features metered separately | `ct acme a`, then `curl "$API/shipment_data?shipment_id=S" -H "X-Org-Id: acme"` | both `200` | container-tracking usage `1`, shipment-data usage `1` |
| B6 | Orgs metered separately | `ct acme a`, `ct globex a` | both `200` (same id, different org is not a duplicate) | each org's usage `1`; two reservation keys |
| B7 | Quota exhausted | `ct tiny a`, `ct tiny b` | `200`, then `429 quota_exceeded` with `X-Request-Id: b` | usage stays `1`; no `res:{tiny}:b`; stream has 1 entry |
| B8 | Exact limit reached, not exceeded | 5× `curl "$API/shipment_data?shipment_id=S" -H "X-Org-Id: tiny"`, then a 6th | five `200`, sixth `429` | usage `5`, never `6` |
| B9 | No quota provisioned | `ct noquota a` | `403 quota_not_provisioned` | no usage key, no reservation key |
| B10 | Unknown org | `ct nobody a` | `403 quota_not_provisioned` | nothing written |
| B11 | Missing org header | `curl -i "$API/container_tracking?container_id=X"` | `401` `missing X-org-ID header` | nothing written |
| B12 | Failed request refunded | `curl -i "$API/container_tracking" -H "X-Org-Id: acme" -H "X-Request-Id: f"` (no `container_id`) | `422` from FastAPI | usage back to `0` (key may exist with `0`); `res:{acme}:f` deleted; no stream entry |
| B13 | Request id too long | `ct acme $(printf 'x%.0s' {1..129})` | `400 invalid_request_id` | nothing written |
| B14 | Unmetered route | `curl -i "$API/" -H "X-Org-Id: acme"` | `200` | nothing written |

## 2. Concurrency

Run parallel requests with `xargs -P`. The invariant for every scenario here: **usage never exceeds the limit, and usage equals the number of `200`s.**

| # | Scenario | Requests | Expected response | Expected Redis |
|---|----------|----------|-------------------|----------------|
| C1 | Race for the last unit | `seq 20 \| xargs -P 20 -I{} sh -c 'ct tiny r{} \| head -1'` (export `ct` first or inline the curl) | exactly one `200`, nineteen `429` | usage `1`; one committed reservation; one stream entry |
| C2 | Race near the limit | `seq 30 \| xargs -P 30 ...` against `acme` shipment-data (limit 20) | exactly twenty `200`, ten `429` | usage `20` |
| C3 | Parallel load under the limit | `seq 50 \| xargs -P 50 ...` against `acme` container-tracking with unique ids | fifty `200` | usage `50`; `XLEN usage_events` = 50 |
| C4 | Two instances share quota | start a second instance on port 8001; send C2 split across ports 8000 and 8001 | twenty `200` total across both | usage `20` |
| C5 | Concurrent features don't interfere | parallel mix of container-tracking and shipment-data for `acme` | all `200` | each feature's usage equals its own request count |
| C6 | Parallel failures all refunded | `seq 20 \| xargs -P 20 ...` against `acme` with no `container_id` (unique ids) | twenty `422` | usage `0`; no reservation keys; no stream entries |

## 3. Idempotency

| # | Scenario | Requests | Expected response | Expected Redis |
|---|----------|----------|-------------------|----------------|
| I1 | Retry after success | `ct acme a`, `ct acme a` | `200`, then `409 already_processed` | usage `1`; one stream entry |
| I2 | Same id, different params | `ct acme a X`, `ct acme a Y` | `200`, then `422 request_id_reused` | usage `1`; `fp` unchanged |
| I3 | Same id, different route | `ct acme a`, then `curl "$API/shipment_data?shipment_id=S" -H "X-Org-Id: acme" -H "X-Request-Id: a"` | `200`, then `422 request_id_reused` | shipment-data usage not created |
| I4 | Query order doesn't matter | `curl "$API/sailing_schedule?origin=CNSHA&destination=NLRTM" ... -H "X-Request-Id: s"`, then same with params swapped | `200`, then `409 already_processed` (same fingerprint) | sailing-schedule usage `1` |
| I5 | Retry after failure is a fresh request | B12 (`422`, id `f`), then `ct acme f` | `422`, then `200` | usage `1`; `res:{acme}:f` committed |
| I6 | Retry while in flight | send the same id twice in parallel: `seq 2 \| xargs -P 2 -I{} sh -c 'ct acme dup \| head -1'` | one `200`, other `409 request_in_progress` **or** `409 already_processed` depending on timing | usage `1` |
| I7 | Dedupe window expires | `ct acme a`; wait 300s (or `EXPIRE res:{acme}:a 1`); `ct acme a` | `200`, then `200` | usage `2`; two stream entries |
| I8 | Rejected request doesn't hold the id | `ct tiny a`, `ct tiny b` (`429`), then `SET quota:{tiny}:container-tracking:limit:$P 2`, `ct tiny b` | `200`, `429`, `200` | usage `2`; `b` was never reserved by the `429` |
| I9 | Same id across orgs | `ct acme a`, `ct globex a` | both `200` | keys are scoped per org |

> I6: handlers return instantly, so `request_in_progress` is hard to hit. Add a temporary `await asyncio.sleep(2)` to the handler to reproduce it reliably, then remove it.

## 4. Batch

Batch body shape:

```json
{"mode": "all_or_nothing", "operations": [{"feature": "container-tracking", "container_id": "X"}]}
```

Cost is the number of operations. A batch is reserved as one unit: one reservation key and one stream entry with the total cost.

| # | Scenario | Body | Expected response | Expected Redis |
|---|----------|------|-------------------|----------------|
| BA1 | Single-feature batch | `acme`, 3× container-tracking | `200` | usage `3`; `res:{acme}:<id>` `cost=3`; one stream entry `cost=3` |
| BA2 | Batch larger than remaining quota | `tiny`, 2× container-tracking | `429 quota_exceeded` | usage `0` (all-or-nothing, nothing partially charged) |
| BA3 | Batch exactly at limit | `tiny`, 5× shipment-data, then single `shipment_data` | `200`, then `429` | usage `5` |
| BA4 | Mixed features | `acme`, 1× container-tracking + 1× shipment-data | `501 mixed_batch_unsupported` | nothing written for either feature |
| BA5 | Empty operations | `acme`, `"operations": []` | `422` | nothing written, no reservation key |
| BA6 | Unknown feature | `acme`, `{"feature": "nope"}` | `422` | nothing written |
| BA7 | Missing `mode` | `acme`, no `mode` field | `422` | nothing written |
| BA8 | Batch retry | BA1 twice with the same `X-Request-Id` | `200`, then `409 already_processed` | usage `3`, not `6` |
| BA9 | Same batch, keys reordered / whitespace changed | BA1, then the same body with `mode` first and extra spaces, same id | `200`, then `409 already_processed` | usage `3` (canonical JSON fingerprint) |
| BA10 | Same id, different batch | BA1, then 2× container-tracking with the same id | `200`, then `422 request_id_reused` | usage `3` |
| BA11 | Concurrent batches near the limit | `acme` shipment-data (limit 20): 10 parallel batches of 3 ops | six `200`, four `429` | usage `18`, never above `20` |
| BA12 | `partial_fulfillment` mode | `tiny`, 2× container-tracking, `"mode": "partial_fulfillment"` | `422` (only `all_or_nothing` is accepted) | nothing written |
| BA13 | One unknown id in a batch | `acme`, container-tracking `MSCU1719583`, `XXXU0000000`, `MSCU1719583` | `404`, detail starts `operation 1:` | usage `0`, no reservation key, no stream entry (whole batch refunded) |

## Known gaps found while writing these

- Mixed-feature batches are rejected with `501` until multi-key reserve lands (BA4).
- `X-Org-Id` is only checked for presence; there's no format validation, and unknown orgs surface as `403 quota_not_provisioned` (B10).
