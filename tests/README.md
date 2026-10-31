# Tests required

redis script load
tests for rollover
tests for idempotency
tests for concurrency
tests for batch
tests for retrying
tests for correct exceptions being raised
tests for correct responses being sent on failures
tests to check quota enforcement under sustained/peak load
test quota enforcement at what point it starts dishing out 429, is it atomic or overserves ?
test that checks for atomicity/concurrency and makes sure that there are no overserves near race limits / batching


## Markdown scenarios vs pytest

- Markdown (`scenarios/*.md`, run by `test_scenarios.py`): plain request sequences that double as docs. Each step is one request plus expected status, error code and usage. Use for idempotency, batch, quota exceeded.
- Pytest (`test_*.py`): anything that needs setup or control beyond requests. Clock control, mid-test provisioning, store-level `reserve`/`refund` calls, concurrency. E.g. `test_period_rollover.py`, `integration/test_stack.py`.

Rule: if the YAML would need a new step type, write pytest instead.


## In-process vs real HTTP: when Redis is settled

`MeterMiddleware` commits or refunds *after* the route has sent the response (`src/middlewares/meter.py`, after `await self.app(...)`). Whether a test can observe that gap depends on the client.

- Unit tests (`conftest.py`, httpx `ASGITransport`): the transport's `send` only buffers the response messages. It builds the `Response` after `await app(scope, receive, send)` returns, and `app` includes `MeterMiddleware`. So the test gets the response only after commit/refund ran. Redis is always settled, and no gap can be observed.
- Integration tests (`integration/conftest.py`, httpx over HTTP to nginx): uvicorn/granian's `send` writes to the socket immediately. The client can get its response, and the test can read Redis, before commit/refund has run.

Consequence: a test that passes in-process can flake over HTTP when it asserts on state written by the settle step right after a response:
- usage stream (`xlen`), written by commit
- usage counter (`used`) after failed requests, decremented by refund (reserve already charged it)
- leftover `res:{org}:*` reservation keys, deleted by refund
- the outcome of a follow-up request with the same request id (`409 request_in_progress` / `422 request_id_reused` instead of `already_processed` / a fresh 200)

Fix: the integration `client` has a response hook that holds each response until its reservation leaves `reserved` (committed or deleted by refund), up to 2s. Skipped for 409, where the reservation belongs to another request still running. Tests can assert once, same as in-process.