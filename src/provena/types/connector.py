"""Connector result and health types."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from provena.types.context import ContextSlotType
from provena.types.provenance import ProvenanceEnvelope


class ConnectorResultMeta(BaseModel):
    execution_ms: float
    record_count: int
    truncated: bool
    native_query: str | None = None


class ConnectorResult(BaseModel):
    """Result returned by a typed connector."""

    records: list[Any]
    provenance: ProvenanceEnvelope
    slot_type: ContextSlotType
    entity_keys: list[str] | None = None
    meta: ConnectorResultMeta


class ConnectorHealth(BaseModel):
    connector_id: str
    status: str
    latency_ms: float
    last_checked: str
    message: str | None = None
