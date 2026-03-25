"""Generic OLTP connector — reference implementation using standard SQL syntax."""

from __future__ import annotations

from typing import Any

from sdol.connectors.executor import QueryExecutor
from sdol.connectors.oltp.base import BaseOLTPConnector
from sdol.connectors.oltp.query import (
    OLTPQuery,
    build_point_lookup_query,
    build_simple_aggregate_query,
)
from sdol.types.capability import ConnectorPerformance
from sdol.types.errors import InvalidIntentError
from sdol.types.intent import AggregateAnalysisIntent, PointLookupIntent


class GenericOLTPConnector(BaseOLTPConnector):
    """OLTP connector using generic SQL syntax (positional $-params).

    Suitable for Postgres, MySQL, or as a testing stub.
    """

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str = "oltp.default",
        source_system: str = "postgres.production",
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
            estimated_latency_ms=50,
            max_result_cardinality=10_000,
            supports_batch_lookup=True,
        )

    def synthesize_query(self, params: Any) -> OLTPQuery:
        if isinstance(params, PointLookupIntent):
            return build_point_lookup_query(params)
        if isinstance(params, AggregateAnalysisIntent):
            return build_simple_aggregate_query(params)
        raise InvalidIntentError(
            "Unexpected intent type in synthesize_query",
            [{"type": type(params).__name__}],
        )
