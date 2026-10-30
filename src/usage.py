from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from deps import get_store
from store import QuotaStore
from swagger import org_id_header

router = APIRouter(tags=["usage"])


@router.get("/usage")
async def usage(
    org_id: Annotated[str, Depends(org_id_header)],
    store: Annotated[QuotaStore, Depends(get_store)],
):
    usage = await store.get_usage(org_id)

    return JSONResponse(usage, 200)
