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


## Markdown scenarios vs pytest

- Markdown (`scenarios/*.md`, run by `test_scenarios.py`): plain request sequences that double as docs. Each step is one request plus expected status, error code and usage. Use for idempotency, batch, quota exceeded.
- Pytest (`test_*.py`): anything that needs setup or control beyond requests. Clock control, mid-test provisioning, store-level `reserve`/`refund` calls, concurrency. E.g. `test_period_rollover.py`, `integration/test_stack.py`.

Rule: if the YAML would need a new step type, write pytest instead.