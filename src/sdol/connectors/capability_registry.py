"""
Registry of typed connectors and their capabilities.
The Semantic Router uses this to route intents to connectors.
"""

from __future__ import annotations

from dataclasses import dataclass

from sdol.connectors.base_connector import BaseConnector
from sdol.types.capability import ConnectorCapability
from sdol.types.intent import BaseIntent


@dataclass
class ConnectorCandidate:
    connector: BaseConnector
    capability: ConnectorCapability
    suitability_score: float


class CapabilityRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {}
        self._capabilities: dict[str, ConnectorCapability] = {}

    def register(self, connector: BaseConnector) -> None:
        """Register a connector. Reads its capabilities automatically."""
        caps = connector.get_capabilities()
        self._connectors[caps.connector_id] = connector
        self._capabilities[caps.connector_id] = caps

    def unregister(self, connector_id: str) -> None:
        self._connectors.pop(connector_id, None)
        self._capabilities.pop(connector_id, None)

    def find_candidates(self, intent: BaseIntent) -> list[ConnectorCandidate]:
        """
        Find connectors that can handle this intent, ranked by suitability.

        Ranking criteria:
        1. Direct intent type match (required — filter, not score)
        2. Entity availability (connector has access to the entity)
        3. Performance profile (lower latency preferred)
        4. Capability richness (more relevant capabilities preferred)
        """
        candidates: list[ConnectorCandidate] = []

        for conn_id, caps in self._capabilities.items():
            if intent.type not in caps.supported_intent_types:
                continue

            connector = self._connectors[conn_id]
            score = self._compute_suitability(intent, caps)
            candidates.append(
                ConnectorCandidate(
                    connector=connector,
                    capability=caps,
                    suitability_score=score,
                )
            )

        candidates.sort(key=lambda c: c.suitability_score, reverse=True)
        return candidates

    def _compute_suitability(
        self, intent: BaseIntent, caps: ConnectorCapability
    ) -> float:
        score = 0.0

        entity_name = getattr(intent, "entity", None) or getattr(intent, "collection", None)
        if entity_name and entity_name in caps.available_entities:
            score += 0.4
        elif not caps.available_entities:
            score += 0.2

        max_latency = 5000.0
        latency_score = max(0.0, 1.0 - caps.performance.estimated_latency_ms / max_latency)
        score += 0.3 * latency_score

        cap_flags = caps.capabilities
        relevant_caps = 0
        total_caps = 0
        cap_map = {
            "aggregate_analysis": ["supports_aggregation"],
            "temporal_trend": ["supports_windowing", "supports_temporal_bucketing"],
            "graph_traversal": ["supports_traversal"],
            "semantic_search": ["supports_similarity", "supports_full_text_search"],
            "ontology_query": ["supports_inference"],
        }
        needed = cap_map.get(intent.type, [])
        for cap_name in needed:
            total_caps += 1
            if getattr(cap_flags, cap_name, False):
                relevant_caps += 1
        if total_caps > 0:
            score += 0.3 * (relevant_caps / total_caps)
        else:
            score += 0.15

        return score

    def get_connector(self, connector_id: str) -> BaseConnector | None:
        return self._connectors.get(connector_id)

    def list_capabilities(self) -> list[ConnectorCapability]:
        return list(self._capabilities.values())
