import time
from datetime import UTC, datetime

# (month start, next month start, result) for the clock path: the meter calls it per
# request, and rebuilding datetimes + strftime each time was ~2% of replica CPU (py-spy).
_cache: tuple[float, float, tuple[str, int]] = (0.0, 0.0, ("", 0))


def current_period(now: datetime | None = None) -> tuple[str, int]:
    """Calendar-month period label and expiry timestamp (start of next month + 1 day grace)."""
    global _cache
    if now is not None:
        return _period(now)[2]
    t = time.time()
    if not _cache[0] <= t < _cache[1]:
        _cache = _period(datetime.fromtimestamp(t, UTC))
    return _cache[2]


def _period(now: datetime) -> tuple[float, float, tuple[str, int]]:
    start = datetime(now.year, now.month, 1, tzinfo=UTC)
    if now.month == 12:
        next_month = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    result = now.strftime("%Y-%m"), int(next_month.timestamp()) + 86400
    return start.timestamp(), next_month.timestamp(), result
