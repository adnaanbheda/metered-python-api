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

Same id, different container. The fingerprint doesn't match, so the second call
is rejected with `422 request_id_reused` instead of being served from the first.

```yaml
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200}
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: XXXU0000000}
  expect: {status: 422, code: request_id_reused, usage: {container-tracking: 1}}
```

## request_id_reused_on_another_route

The fingerprint covers the path too, and the reservation key is per org, not per
feature: reusing an id on another route is rejected and charges nothing there.

```yaml
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200}
- org: acme
  request_id: a
  path: /shipment_data
  params: {shipment_id: SHP-00001}
  expect: {status: 422, code: request_id_reused, usage: {shipment-data: 0}}
```

## query_order_is_the_same_request

Query params are sorted before fingerprinting, so a retry that sends them in a
different order is still recognised as a retry.

```yaml
- org: acme
  request_id: s
  path: /sailing_schedule
  params: {origin: CNSHA, destination: SGSIN}
  expect: {status: 200}
- org: acme
  request_id: s
  path: /sailing_schedule
  params: {destination: SGSIN, origin: CNSHA}
  expect: {status: 409, code: already_processed, usage: {sailing-schedule: 1}}
```

## retry_after_failure_is_a_fresh_request

A failed call is refunded and its reservation dropped, so the id is free again:
the retry is served and charged once.

```yaml
- org: acme
  request_id: f
  path: /container_tracking
  params: {container_id: XXXU0000000}
  expect: {status: 404, usage: {container-tracking: 0}}
- org: acme
  request_id: f
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200, usage: {container-tracking: 1}}
```

## rejected_request_does_not_hold_the_id

`tiny` has 1 container-tracking unit. A `429` writes nothing, so retrying the
rejected id is checked against quota again (`429`), not deduped (`409`).

```yaml
- org: tiny
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200}
- org: tiny
  request_id: b
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 429, code: quota_exceeded}
- org: tiny
  request_id: b
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 429, code: quota_exceeded, usage: {container-tracking: 1}}
```

## same_id_in_different_orgs

Reservation keys are scoped per org, so two orgs can use the same id.

```yaml
- org: acme
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200}
- org: globex
  request_id: a
  path: /container_tracking
  params: {container_id: MSCU1719583}
  expect: {status: 200, usage: {container-tracking: 1}}
```
