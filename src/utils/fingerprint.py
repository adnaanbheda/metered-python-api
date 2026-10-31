import hashlib
import json
from urllib.parse import parse_qsl, urlencode

from starlette.types import Scope


def fingerprint(scope: Scope, body: bytes) -> str:
    """Hash of method, path, sorted query and canonical JSON body, so key order and
    whitespace don't change it."""
    query = urlencode(
        sorted(parse_qsl(scope["query_string"].decode(), keep_blank_values=True))
    )
    try:
        body = json.dumps(
            json.loads(body), sort_keys=True, separators=(",", ":")
        ).encode()
    except ValueError:
        pass  # empty or non-JSON body: hash raw bytes
    raw = b"|".join(
        [scope["method"].encode(), scope["path"].encode(), query.encode(), body]
    )
    return hashlib.sha256(raw).hexdigest()[:16]
