"""Base class for all document / vector-search connectors.

Encodes what it *means* to be a document connector: supported intent types,
capability shape, interpret_intent validation, and normalize_result with
document-appropriate provenance defaults.  Provider extensions (Pinecone,
Weaviate, Elasticsearch, …) subclass this and only implement synthesize_query
plus performance/provenance overrides.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from sdol.connectors.base_connector import BaseConnector
from sdol.connectors.executor import QueryExecutor
from sdol.types.capability import (
    ConnectorCapabilities,
    ConnectorCapability,
    ConnectorPerformance,
)
from sdol.types.connector import ConnectorHealth, ConnectorResult, ConnectorResultMeta
from sdol.types.context import ContextSlotType
from sdol.types.errors import InvalidIntentError
from sdol.types.intent import BaseIntent, SemanticSearchIntent
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


class BaseDocumentConnector(BaseConnector, ABC):
    """Paradigm base for document / vector-search connectors.

    Providers must implement:
        - ``id`` property
        - ``synthesize_query(params)``
        - ``get_performance() -> ConnectorPerformance``

    Providers *may* override:
        - ``source_system``, ``default_staleness_sec``, ``default_consistency``
        - ``get_capabilities()``
        - ``normalize_result()``
    """

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str,
        source_system: str = "document.generic",
        available_entities: list[str] | None = None,
    ) -> None:
        self._executor = executor
        self._id = connector_id
        self._source_system = source_system
        self._available_entities = available_entities or []

    # -- Properties --------------------------------------------------------

    @property
    def id(self) -> str:
        return self._id

    @property
    def connector_type(self) -> str:
        return "document"

    @property
    def source_system(self) -> str:
        return self._source_system

    @property
    def available_entities(self) -> list[str]:
        return self._available_entities

    @property
    def default_staleness_sec(self) -> float:
        return 300.0

    @property
    def default_consistency(self) -> ConsistencyGuarantee:
        return ConsistencyGuarantee.EVENTUAL

    # -- Paradigm contract -------------------------------------------------

    @abstractmethod
    def get_performance(self) -> ConnectorPerformance:
        """Provider-specific performance profile."""
        ...

    def get_capabilities(self) -> ConnectorCapability:
        return ConnectorCapability(
            connector_id=self._id,
            connector_type="document",
            supported_intent_types=["semantic_search"],
            capabilities=ConnectorCapabilities(
                supports_similarity=True,
                supports_full_text_search=True,
            ),
            performance=self.get_performance(),
            available_entities=self._available_entities,
        )

    def interpret_intent(self, intent: BaseIntent) -> SemanticSearchIntent:
        if isinstance(intent, SemanticSearchIntent):
            return intent
        raise InvalidIntentError(
            f"{type(self).__name__} cannot handle intent type: {intent.type}",
            [{"type": intent.type, "expected": ["semantic_search"]}],
        )

    async def execute_query(self, query: Any) -> dict[str, Any]:
        return await self._executor.execute(query)

    def normalize_result(
        self,
        raw: Any,
        intent: BaseIntent,
        execution_ms: float,
    ) -> ConnectorResult:
        records = raw.get("records", [])
        max_results = intent.max_results or 100

        return ConnectorResult(
            records=records,
            provenance=ProvenanceEnvelope(
                source_system=self._source_system,
                retrieval_method=RetrievalMethod.VECTOR_SIMILARITY,
                consistency=self.default_consistency,
                precision=PrecisionClass.SIMILARITY_RANKED,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                staleness_window_sec=self.default_staleness_sec,
                execution_ms=execution_ms,
                result_truncated=len(records) >= max_results,
                total_available=raw.get("meta", {}).get("total_available"),
            ),
            slot_type=ContextSlotType.UNSTRUCTURED,
            entity_keys=None,
            meta=ConnectorResultMeta(
                execution_ms=execution_ms,
                record_count=len(records),
                truncated=len(records) >= max_results,
                native_query=raw.get("meta", {}).get("native_query"),
            ),
        )

    async def check_health(self) -> ConnectorHealth:
        return ConnectorHealth(
            connector_id=self._id,
            status="healthy",
            latency_ms=0.0,
            last_checked=datetime.now(timezone.utc).isoformat(),
        )
