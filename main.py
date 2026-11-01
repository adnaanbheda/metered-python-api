import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from admin import router as admin_router
from middlewares.identity import IdentityMiddleware
from middlewares.meter import MeterMiddleware
from operations import router as operations_router
from store import QuotaStore, connect
from usage import router as usage_router

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "WARNING"))

load_dotenv()
redis_client = connect()
quota_store = QuotaStore(redis_client)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await redis_client.aclose()


app = FastAPI(
    lifespan=lifespan,
    openapi_tags=[{"name": "operations"}, {"name": "usage"}, {"name": "admin"}],
    swagger_ui_parameters={
        "tryItOutEnabled": True,
        "docExpansion": "list",
        "defaultModelsExpandDepth": -1,
    },
)
app.state.quota_store = quota_store


@app.get("/", include_in_schema=False)
def root():
    return {"message": "Hello World"}


app.include_router(operations_router)
app.include_router(usage_router)
if os.environ.get("ENABLE_ADMIN") == "1":  # dev only: exposes raw Redis keys
    app.include_router(admin_router)

if os.environ.get("DISABLE_METER") != "1":  # bench only: A/B the meter's overhead
    app.add_middleware(MeterMiddleware, quota_store=quota_store)
app.add_middleware(IdentityMiddleware, quota_store=quota_store)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", reload=True)
