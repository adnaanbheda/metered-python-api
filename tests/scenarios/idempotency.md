# Idempotency

Retries carry the same `X-Request-Id`. The reservation key dedupes them for 300s.

## duplicate_request_rejected

A client times out and retries with the same id. The first call already
**committed**, so the retry gets `409 already_processed` and usage stays at 1:
no double charge.

```yaml
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200}
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 409, code: already_processed, usage: {container-tracking: 1}}
```

## request_id_reused_with_different_params
...
