"""Mock response data from mocks/*.json (fixed fixtures, edit by hand)."""

import json
from pathlib import Path

MOCKS = Path(__file__).resolve().parents[1] / "mocks"


def _load(name: str, default):
    path = MOCKS / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else default


CONTAINERS: dict[str, dict] = _load("containers", {})
SAILINGS: list[dict] = _load("sailings", [])
SHIPMENTS: dict[str, dict] = _load("shipments", {})
ORGS: dict[str, dict[str, int]] = _load("orgs", {})
