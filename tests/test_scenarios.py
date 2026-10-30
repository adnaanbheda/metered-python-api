import re
from pathlib import Path

import pytest
import yaml

from mock_data import MOCKS
from store import QuotaStore, current_period

SCENARIOS_DIR = Path(__file__).parent / "scenarios"
SCENARIO_RE = re.compile(
    r"^## (?P<name>\S+)\n(?P<prose>.*?)```yaml\n(?P<steps>.*?)```", re.S | re.M
)


def load_scenarios() -> dict[str, dict]:
    scenarios = {}
    for path in sorted(SCENARIOS_DIR.glob("*.md")):
        for m in SCENARIO_RE.finditer(path.read_text()):
            scenarios[f"{path.stem}::{m['name']}"] = {
                "description": m["prose"].strip(),
                "steps": yaml.safe_load(m["steps"]),
            }
    return scenarios


SCENARIOS = load_scenarios()


@pytest.mark.parametrize("name", SCENARIOS)
async def test_scenario(name, client, redis_client):
    for step in SCENARIOS[name]["steps"]:
        resp = await client.request(
            step.get("method", "GET"),
            step["path"],
            params=step.get("params"),
            json=step.get("body"),
            headers={"X-Org-Id": step["org"], "X-Request-Id": step["request_id"]},
        )
        expect = step["expect"]
        assert resp.status_code == expect["status"], resp.text
        if "code" in expect:
            assert resp.json()["code"] == expect["code"]
        period, _ = current_period()
        for feature, used in expect.get("usage", {}).items():
            assert (
                int(
                    await redis_client.get(
                        QuotaStore.usage_key(step["org"], feature, period)
                    )
                    or 0
                )
                == used
            )
