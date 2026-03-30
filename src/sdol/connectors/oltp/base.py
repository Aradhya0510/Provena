"""Base class for all OLTP-paradigm connectors.

Encodes what it *means* to be an OLTP connector: supported intent types,
capability shape, interpret_intent validation, and normalize_result with
OLTP-appropriate provenance defaults.  Provider extensions (Databricks
Lakebase, Postgres, DynamoDB, …) subclass this and only implement
synthesize_query plus performance/provenance overrides.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from sdol.connectors.base_connector import BaseConnector
from sdol.connectors.executor import QueryExecutor
from sdol.connectors.sql_utils import extract_entity_keys
from sdol.types.capability import (
    ConnectorCapabilities,
    ConnectorCapability,
    ConnectorPerformance,
)
from sdol.types.connector import ConnectorHealth, ConnectorResult, ConnectorResultMeta
from sdol.types.context import ContextSlotType
from sdol.types.errors import InvalidIntentError
from sdol.types.intent import (
    AggregateAnalysisIntent,
    BaseIntent,
    PointLookupIntent,
)
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


class BaseOLTPConnector(BaseConnector, ABC):
    """Paradigm base for OLTP (transactional / point-lookup) connectors.

    Providers must implement:
        - ``id`` property
        - ``synthesize_query(params)``
        - ``get_performance() -> ConnectorPerformance``

    Providers *may* override:
        - ``source_system``, ``default_staleness_sec``, ``default_consistency``
        - ``get_capabilities()``
        - ``normalize_result()``
    """

    _DEFAULT_KEY_FIELDS: tuple[str, ...] = ("customer_id", "id", "entity_id")

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str,
        source_system: str = "oltp.generic",
        available_entities: list[str] | None = None,
        entity_key_fields: tuple[str, ...] | None = None,
    ) -> None:
        self._executor = executor
        self._id = connector_id
        self._source_system = source_system
        self._available_entities = available_entities or []
        self._entity_key_fields = entity_key_fields or self._DEFAULT_KEY_FIELDS

    # -- Properties --------------------------------------------------------

    @property
    def id(self) -> str:
        return self._id

    @property
    def connector_type(self) -> str:
        return "oltp"

    @property
    def source_system(self) -> str:
        return self._source_system

    @property
    def available_entities(self) -> list[str]:
        return self._available_entities

    @property
    def default_staleness_sec(self) -> float:
        return 60.0

    @property
    def default_consistency(self) -> ConsistencyGuarantee:
        return ConsistencyGuarantee.READ_COMMITTED

    # -- Paradigm contract -------------------------------------------------

    @abstractmethod
    def get_performance(self) -> ConnectorPerformance:
        """Provider-specific performance profile."""
        ...

    def get_capabilities(self) -> ConnectorCapability:
        return ConnectorCapability(
            connector_id=self._id,
            connector_type="oltp",
            supported_intent_types=["point_lookup", "aggregate_analysis"],
            capabilities=ConnectorCapabilities(
                supports_aggregation=True,
            ),
            performance=self.get_performance(),
            available_entities=self._available_entities,
        )

    def interpret_intent(
        self, intent: BaseIntent
    ) -> PointLookupIntent | AggregateAnalysisIntent:
        if isinstance(intent, PointLookupIntent):
            return intent
        if isinstance(intent, AggregateAnalysisIntent):
            return intent
        raise InvalidIntentError(
            f"{type(self).__name__} cannot handle intent type: {intent.type}",
            [{"type": intent.type, "expected": ["point_lookup", "aggregate_analysis"]}],
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
        precision = (
            PrecisionClass.EXACT
            if isinstance(intent, PointLookupIntent)
            else PrecisionClass.EXACT_AGGREGATE
        )
        retrieval = (
            RetrievalMethod.DIRECT_QUERY
            if isinstance(intent, PointLookupIntent)
            else RetrievalMethod.COMPUTED_AGGREGATE
        )
        max_results = intent.max_results or 10_000

        return ConnectorResult(
            records=records,
            provenance=ProvenanceEnvelope(
                source_system=self._source_system,
                retrieval_method=retrieval,
                consistency=self.default_consistency,
                precision=precision,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                staleness_window_sec=self.default_staleness_sec,
                execution_ms=execution_ms,
                result_truncated=len(records) >= max_results,
                total_available=raw.get("meta", {}).get("total_available"),
            ),
            slot_type=ContextSlotType.STRUCTURED,
            entity_keys=extract_entity_keys(records, self._entity_key_fields),
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
