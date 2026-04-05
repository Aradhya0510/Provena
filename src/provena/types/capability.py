"""Capability declarations for typed connectors."""

from __future__ import annotations

from pydantic import BaseModel


class ConnectorCapabilities(BaseModel):
    supports_aggregation: bool = False
    supports_windowing: bool = False
    supports_traversal: bool = False
    supports_similarity: bool = False
    supports_inference: bool = False
    supports_temporal_bucketing: bool = False
    supports_full_text_search: bool = False


class ConnectorPerformance(BaseModel):
    estimated_latency_ms: float
    max_result_cardinality: int
    supports_batch_lookup: bool = False


class EntitySchema(BaseModel):
    """Schema metadata for a single entity (table/collection)."""
    columns: list[str]
    description: str = ""


class ConnectorCapability(BaseModel):
    connector_id: str
    connector_type: str
    supported_intent_types: list[str]
    capabilities: ConnectorCapabilities
    performance: ConnectorPerformance
    available_entities: list[str]
    entity_schemas: dict[str, EntitySchema] = {}
    consistency_guarantee: str = ""
    staleness_window_sec: float | None = None
