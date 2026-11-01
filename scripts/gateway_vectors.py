"""Write gateway/tests/vectors.json: fingerprints from the Python implementation, which the Rust
gateway's fingerprint.rs must reproduce byte for byte (cargo test).

    uv run scripts/gateway_vectors.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from utils.fingerprint import fingerprint  # noqa: E402

QUERIES = [
    "",
    "container_id=MSCU1719583",
    "b=2&a=1",
    "a=1&a=0",
    "a=&b",
    "q=a+b",
    "q=%41%7e%2A",
    "q=*~",
    "q=%C3%A9",
    "q=%zz&r=%4",
    "&&a=1&",
    "origin=CNSHA&destination=NLRTM",
]

BODIES = [
    "",
    '{"operations":[{"feature":"container-tracking","container_id":"MSCU1719583"}],"mode":"all_or_nothing"}',
    '{ "mode" : "all_or_nothing",\n  "operations": [ {"container_id": "MSCU1719583", "feature": "container-tracking"} ] }',
    '{"z": {"b": [1, 2, {"d": null, "c": true}], "a": false}}',
    '{"s": "caf\\u00e9 é \U0001f600"}',
    '{"s": "tab\\t nl\\n quote\\" back\\\\ ctl\\u0001 del\u007f"}',
    '{"f": [1.5, 1.0, 1e16, 1.5e16, 1e-7, 0.0001, -0.0, 123456789012345.6, 2.5e-300]}',
    '{"i": [0, -1, 9007199254740993, 12345678901234567890]}',
    '{"dup": 1, "dup": 2}',
    "[]",
    '"just a string"',
    "not json",
    "{broken",
]


def scope(method: str, path: str, query: str) -> dict:
    return {"method": method, "path": path, "query_string": query.encode()}


def main() -> None:
    cases = [("GET", "/container_tracking", q, "") for q in QUERIES]
    cases += [("POST", "/batch", "", b) for b in BODIES]
    vectors = [
        {
            "method": m,
            "path": p,
            "query": q,
            "body": b,
            "fingerprint": fingerprint(scope(m, p, q), b.encode()),
        }
        for m, p, q, b in cases
    ]
    out = ROOT / "gateway" / "tests" / "vectors.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(
        json.dumps({"fingerprints": vectors}, indent=2) + "\n", encoding="utf-8"
    )
    print(f"{len(vectors)} vectors -> {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
