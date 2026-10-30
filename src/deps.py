from fastapi import Request

from store import QuotaStore


def get_store(request: Request) -> QuotaStore:
    return request.app.state.quota_store
