"""Context frame types — the structured replacement for flat context windows."""

from __future__ import annotations

from provena.types._compat import StrEnum
from typing import Any, Literal

from pydantic import BaseModel

from provena.types.provenance import ProvenanceEnvelope, TrustScore


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
        "prefer_present_source",
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


class PresenceConflict(BaseModel):
    """Flagged when a composite query expects data from N sources but fewer respond."""

    present_element: ContextElement
    missing_source_system: str
    missing_connector_id: str
    resolution: ConflictResolution


class ContextFrameStats(BaseModel):
    total_elements: int
    avg_trust_score: float
    slot_counts: dict[str, int]


class TrustSummary(BaseModel):
    """Pre-digested trust signal for LLM consumption."""

    overall_confidence: str
    lowest_trust_source: str | None = None
    advisory: str | None = None


class ContextFrame(BaseModel):
    """The complete context frame passed to the agent."""

    slots: list[ContextSlot]
    conflicts: list[ContextConflict]
    presence_conflicts: list[PresenceConflict] = []
    stats: ContextFrameStats
    assembled_at: str
    trust_summary: TrustSummary | None = None
