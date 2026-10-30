from typing import Annotated

from fastapi import APIRouter, Depends

from deps import get_store
from store import QuotaStore
from swagger import org_id_header

# Dev only: main.py mounts this when ENABLE_ADMIN=1. Unmetered, unauthenticated beyond X-Org-Id.
router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/keys")
async def org_keys(
    org_id: Annotated[str, Depends(org_id_header)],
    store: Annotated[QuotaStore, Depends(get_store)],
):
    return await store.dump_org(org_id)
