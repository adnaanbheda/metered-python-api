from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    detail: str
    code: str | None = None  # e.g. "missing_org_id", "quota_exceeded"


@dataclass
class Charge:
    key: str
    cost: int


@dataclass
class Ledger:
    org_id: str
    charges: list[Charge] = field(default_factory=list)


current_ledger: ContextVar[Ledger | None] = ContextVar("current_ledger", default=None)


class ContainerTrackingOp(BaseModel):
    feature: Literal["container-tracking"]
    container_id: str


class SailingScheduleOp(BaseModel):
    feature: Literal["sailing-schedule"]
    origin: str
    destination: str


class ShipmentDataOp(BaseModel):
    feature: Literal["shipment-data"]
    shipment_id: str


Operation = Annotated[
    ContainerTrackingOp | SailingScheduleOp | ShipmentDataOp,
    Field(discriminator="feature"),
]


class BatchRequest(BaseModel):
    operations: Annotated[list[Operation], Field(min_length=1)]
    mode: Literal["all_or_nothing", "partial_fulfillment"]
