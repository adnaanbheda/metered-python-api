class QuotaError(Exception):
    """Base for reserve rejections. Each subclass carries its HTTP status and error code."""

    status: int = 500
    code: str = "quota_error"
    detail: str = "quota error"

    def __init__(self, detail: str | None = None):
        self.detail = detail or self.detail
        super().__init__(self.detail)


class RequestInProgress(QuotaError):
    status = 409
    code = "request_in_progress"
    detail = "request with this id is still processing"


class AlreadyProcessed(QuotaError):
    status = 409
    code = "already_processed"
    detail = "request with this id was already processed"


class RequestIdReused(QuotaError):
    status = 422
    code = "request_id_reused"
    detail = "request id reused with a different request"


class QuotaNotProvisioned(QuotaError):
    status = 403
    code = "quota_not_provisioned"
    detail = "no quota provisioned for this period"


class QuotaExceeded(QuotaError):
    status = 429
    code = "quota_exceeded"

    def __init__(self, feature: str, used: int, limit: int):
        super().__init__(f"quota exceeded for {feature}: {used}/{limit}")
        self.feature = feature
        self.used = used
        self.limit = limit


# reserve.lua status -> exception, for QuotaStore.reserve
BY_STATUS: dict[str, type[QuotaError]] = {
    "reserved": RequestInProgress,
    "committed": AlreadyProcessed,
    "mismatch": RequestIdReused,
    "unprovisioned": QuotaNotProvisioned,
    "exceeded": QuotaExceeded,
}
