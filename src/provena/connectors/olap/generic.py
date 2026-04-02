"""Generic OLAP connector — reference implementation using standard SQL syntax."""

from __future__ import annotations

from typing import Any

from provena.connectors.executor import QueryExecutor
from provena.connectors.olap.base import BaseOLAPConnector
from provena.connectors.olap.query import (
    OLAPQuery,
    build_aggregate_query,
    build_temporal_query,
)
from provena.types.capability import ConnectorPerformance
from provena.types.errors import InvalidIntentError
from provena.types.intent import AggregateAnalysisIntent, TemporalTrendIntent


class GenericOLAPConnector(BaseOLAPConnector):
    """OLAP connector using generic SQL syntax (positional $-params, time_bucket, PERCENTILE_CONT).

    Suitable for Snowflake, generic analytical databases, or as a testing stub.
    """

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str = "olap.default",
        source_system: str = "snowflake.analytics",
        available_entities: list[str] | None = None,
    ) -> None:
        super().__init__(
            executor=executor,
            connector_id=connector_id,
            source_system=source_system,
            available_entities=available_entities,
        )

    def get_performance(self) -> ConnectorPerformance:
        return ConnectorPerformance(
            estimated_latency_ms=500,
            max_result_cardinality=1_000_000,
        )

    def synthesize_query(self, params: Any) -> OLAPQuery:
        if isinstance(params, AggregateAnalysisIntent):
            return build_aggregate_query(params)
        if isinstance(params, TemporalTrendIntent):
            return build_temporal_query(params)
        raise InvalidIntentError(
            "Unexpected intent type in synthesize_query",
            [{"type": type(params).__name__}],
        )
