from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from mock_data import CONTAINERS, SAILINGS, SHIPMENTS
from schemas import (
    BatchRequest,
    ContainerTrackingOp,
    Operation,
    SailingScheduleOp,
    ShipmentDataOp,
)
from swagger import examples, org_id_header, request_id_header

LANES = list(dict.fromkeys((s["origin"], s["destination"]) for s in SAILINGS))[:5]

CONTAINER_EXAMPLES = examples(
    {cid: f"{cid}: {c['status']}" for cid, c in list(CONTAINERS.items())[:5]}
    | {"XXXU0000000": "unknown (404)"}
)
SHIPMENT_EXAMPLES = examples(
    {sid: f"{sid}: {s['status']}" for sid, s in list(SHIPMENTS.items())[:5]}
    | {"SHP-99999": "unknown (404)"}
)
ORIGIN_EXAMPLES = examples({origin: origin for origin, _ in LANES})
DESTINATION_EXAMPLES = examples({destination: destination for _, destination in LANES})


"""
TODO: Wire up find_containers to use real db calls / API calls / add random/fixed delay
"""


def find_container(container_id: str) -> dict:
    if container_id not in CONTAINERS:
        raise HTTPException(404, f"container {container_id} not found")
    return CONTAINERS[container_id]


def find_sailings(origin: str, destination: str) -> dict:
    origin, destination = origin.upper(), destination.upper()
    return {
        "sailings": [
            s
            for s in SAILINGS
            if s["origin"] == origin and s["destination"] == destination
        ]
    }


def find_shipment(shipment_id: str) -> dict:
    if shipment_id not in SHIPMENTS:
        raise HTTPException(404, f"shipment {shipment_id} not found")
    return SHIPMENTS[shipment_id]


router = APIRouter(
    tags=["operations"],
    dependencies=[Depends(org_id_header), Depends(request_id_header)],
)


@router.get("/container_tracking")
async def container_tracking(
    container_id: Annotated[str, Query(openapi_examples=CONTAINER_EXAMPLES)],
):
    return find_container(container_id=container_id)


@router.get("/sailing_schedule")
async def sailing_schedule(
    origin: Annotated[str, Query(openapi_examples=ORIGIN_EXAMPLES)],
    destination: Annotated[str, Query(openapi_examples=DESTINATION_EXAMPLES)],
):
    # TODO: send an API to our eagle vendor that finds the boat and gets back in a long while!
    # TODO: can add a config that takes a random/fixed amount of time to process this request
    return find_sailings(origin, destination)


@router.get("/shipment_data")
async def shipment_data(
    shipment_id: Annotated[str, Query(openapi_examples=SHIPMENT_EXAMPLES)],
):
    return find_shipment(shipment_id)


def run_operation(op: Operation) -> dict:
    match op:
        case ContainerTrackingOp():
            return find_container(op.container_id)
        case SailingScheduleOp():
            return find_sailings(op.origin, op.destination)
        case ShipmentDataOp():
            return find_shipment(op.shipment_id)


@router.post("/batch")
async def batch(req: BatchRequest):
    # all_or_nothing: one failed op fails the batch, so the meter refunds the whole cost
    results = []
    for i, op in enumerate(req.operations):
        try:
            results.append(run_operation(op))
        except HTTPException as e:
            raise HTTPException(e.status_code, f"operation {i}: {e.detail}") from e
    return {"results": results}
