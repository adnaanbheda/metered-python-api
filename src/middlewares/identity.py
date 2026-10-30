from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from schemas import ErrorResponse

PUBLIC_PATHS = {"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"}


class IdentityMiddleware:
    def __init__(self, app: ASGIApp, quota_store):
        self.app = app
        self.store = quota_store
        self.header_name = "X-org-ID"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http" or scope["path"] in PUBLIC_PATHS
        ):  # lifespan, websocket: pass through
            await self.app(scope, receive, send)
            return

        org_id = Headers(scope=scope).get(self.header_name, "").strip()

        if not org_id:
            body = ErrorResponse(detail=f"missing {self.header_name} header")
            response = JSONResponse(body.model_dump(), status_code=401)
            await response(scope, receive, send)
            return

        scope.setdefault("state", {})["org_id"] = org_id
        await self.app(scope, receive, send)
