"""Base class for all OLAP-paradigm connectors.

Encodes what it *means* to be an OLAP connector: supported intent types,
capability shape, interpret_intent validation, and normalize_result with
OLAP-appropriate provenance defaults.  Provider extensions (Databricks DBSQL,
Snowflake, BigQuery, …) subclass this and only implement synthesize_query
plus performance/provenance overrides.
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
    TemporalTrendIntent,
)
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


class BaseOLAPConnector(BaseConnector, ABC):
    """Paradigm base for OLAP (analytical / aggregation) connectors.

    Providers must implement:
        - ``id`` property
        - ``synthesize_query(params)``
        - ``get_performance() -> ConnectorPerformance``

    Providers *may* override:
        - ``source_system``, ``default_staleness_sec``, ``default_consistency``
        - ``get_capabilities()`` (to advertise extra capability flags)
        - ``normalize_result()`` (rarely needed)
    """

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str,
        source_system: str = "olap.generic",
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
        return "olap"

    @property
    def source_system(self) -> str:
        return self._source_system

    @property
    def available_entities(self) -> list[str]:
        return self._available_entities

    @property
    def default_staleness_sec(self) -> float:
        return 3600.0

    @property
    def default_consistency(self) -> ConsistencyGuarantee:
        return ConsistencyGuarantee.STRONG

    # -- Paradigm contract -------------------------------------------------

    @abstractmethod
    def get_performance(self) -> ConnectorPerformance:
        """Provider-specific performance profile."""
        ...

    def get_capabilities(self) -> ConnectorCapability:
        return ConnectorCapability(
            connector_id=self._id,
            connector_type="olap",
            supported_intent_types=["aggregate_analysis", "temporal_trend"],
            capabilities=ConnectorCapabilities(
                supports_aggregation=True,
                supports_windowing=True,
                supports_temporal_bucketing=True,
            ),
            performance=self.get_performance(),
            available_entities=self._available_entities,
        )

    def interpret_intent(
        self, intent: BaseIntent
    ) -> AggregateAnalysisIntent | TemporalTrendIntent:
        if isinstance(intent, AggregateAnalysisIntent):
            return intent
        if isinstance(intent, TemporalTrendIntent):
            return intent
        raise InvalidIntentError(
            f"{type(self).__name__} cannot handle intent type: {intent.type}",
            [{"type": intent.type, "expected": ["aggregate_analysis", "temporal_trend"]}],
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
        slot_type = (
            ContextSlotType.TEMPORAL
            if isinstance(intent, TemporalTrendIntent)
            else ContextSlotType.STRUCTURED
        )
        max_results = intent.max_results or 10_000_000

        return ConnectorResult(
            records=records,
            provenance=ProvenanceEnvelope(
                source_system=self._source_system,
                retrieval_method=RetrievalMethod.COMPUTED_AGGREGATE,
                consistency=self.default_consistency,
                precision=PrecisionClass.EXACT_AGGREGATE,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                staleness_window_sec=self.default_staleness_sec,
                execution_ms=execution_ms,
                result_truncated=len(records) >= max_results,
                total_available=raw.get("meta", {}).get("total_available"),
            ),
            slot_type=slot_type,
            entity_keys=extract_entity_keys(records),
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
