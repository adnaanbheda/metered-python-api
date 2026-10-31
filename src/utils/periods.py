from datetime import UTC, datetime


def current_period(now: datetime | None = None) -> tuple[str, int]:
    """Calendar-month period label and expiry timestamp (start of next month + 1 day grace)."""
    now = now or datetime.now(UTC)
    if now.month == 12:
        next_month = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    return now.strftime("%Y-%m"), int(next_month.timestamp()) + 86400
