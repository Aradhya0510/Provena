"""Context frame types — the structured replacement for flat context windows."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel

from sdol.types.provenance import ProvenanceEnvelope, TrustScore


class ContextSlotType(StrEnum):
    STRUCTURED = "STRUCTURED"
    RELATIONAL = "RELATIONAL"
    TEMPORAL = "TEMPORAL"
    UNSTRUCTURED = "UNSTRUCTURED"
    INFERRED = "INFERRED"


class ContextElement(BaseModel):
    """Atomic unit of the context frame — data + provenance."""

    id: str
    data: Any
    provenance: ProvenanceEnvelope
    trust: TrustScore
    source_intent_id: str
    entity_key: str | None = None


class ContextSlot(BaseModel):
    """Typed slot — elements share interpretation semantics."""

    type: ContextSlotType
    elements: list[ContextElement]
    interpretation_notes: str


class ConflictResolution(BaseModel):
    strategy: Literal[
        "prefer_freshest",
        "prefer_authoritative",
        "prefer_strongest_consistency",
        "defer_to_agent",
    ]
    winner: str | None = None
    reason: str


class ContextConflict(BaseModel):
    element_a: ContextElement
    element_b: ContextElement
    field: str
    value_a: Any
    value_b: Any
    resolution: ConflictResolution


class ContextFrameStats(BaseModel):
    total_elements: int
    avg_trust_score: float
    slot_counts: dict[str, int]


class ContextFrame(BaseModel):
    """The complete context frame passed to the agent."""

    slots: list[ContextSlot]
    conflicts: list[ContextConflict]
    stats: ContextFrameStats
    assembled_at: str
