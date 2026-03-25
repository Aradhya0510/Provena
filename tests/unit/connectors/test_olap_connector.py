"""Tests for OLAP connector."""

import pytest

from sdol.connectors.executor import MockQueryExecutor
from sdol.connectors.olap.generic import GenericOLAPConnector
from sdol.types.context import ContextSlotType
from sdol.types.errors import InvalidIntentError
from sdol.types.intent import (
    AggregateAnalysisIntent,
    MeasureSpec,
    PointLookupIntent,
    TemporalTrendIntent,
    TimeWindow,
)


class TestGenericOLAPConnector:
    def test_handles_aggregate_analysis(self) -> None:
        executor = MockQueryExecutor(records=[{"region": "west", "sum_revenue": 1000}])
        connector = GenericOLAPConnector(executor=executor)
        assert connector.can_handle(
            AggregateAnalysisIntent(
                id="i-1",
                entity="orders",
                measures=[MeasureSpec(field="revenue", aggregation="sum")],
                dimensions=["region"],
            )
        )

    def test_handles_temporal_trend(self) -> None:
        executor = MockQueryExecutor(records=[])
        connector = GenericOLAPConnector(executor=executor)
        assert connector.can_handle(
            TemporalTrendIntent(
                id="i-1",
                entity="usage",
                metric="api_calls",
                window=TimeWindow(relative="last_90d"),
            )
        )

    def test_rejects_point_lookup(self) -> None:
        executor = MockQueryExecutor()
        connector = GenericOLAPConnector(executor=executor)
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        assert not connector.can_handle(intent)

    @pytest.mark.asyncio
    async def test_execute_aggregate(self) -> None:
        executor = MockQueryExecutor(
            records=[{"region": "west", "sum_revenue": 1000}]
        )
        connector = GenericOLAPConnector(executor=executor)
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["region"],
        )
        result = await connector.execute(intent)
        assert result.slot_type == ContextSlotType.STRUCTURED
        assert len(result.records) == 1
        assert result.provenance.precision.value == "exact_aggregate"

    @pytest.mark.asyncio
    async def test_execute_temporal(self) -> None:
        executor = MockQueryExecutor(
            records=[{"bucket": "2024-01-01", "api_calls": 42}]
        )
        connector = GenericOLAPConnector(executor=executor)
        intent = TemporalTrendIntent(
            id="i-1",
            entity="usage",
            metric="api_calls",
            window=TimeWindow(relative="last_90d"),
            granularity="1d",
        )
        result = await connector.execute(intent)
        assert result.slot_type == ContextSlotType.TEMPORAL
        assert len(result.records) == 1

    @pytest.mark.asyncio
    async def test_rejects_invalid_intent_type(self) -> None:
        executor = MockQueryExecutor()
        connector = GenericOLAPConnector(executor=executor)
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        with pytest.raises(InvalidIntentError):
            await connector.execute(intent)

    @pytest.mark.asyncio
    async def test_empty_results(self) -> None:
        executor = MockQueryExecutor(records=[])
        connector = GenericOLAPConnector(executor=executor)
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["region"],
        )
        result = await connector.execute(intent)
        assert result.records == []
        assert result.meta.record_count == 0

    @pytest.mark.asyncio
    async def test_entity_keys_detected(self) -> None:
        executor = MockQueryExecutor(
            records=[{"customer_id": "C-1", "revenue": 100}]
        )
        connector = GenericOLAPConnector(executor=executor)
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["customer_id"],
        )
        result = await connector.execute(intent)
        assert result.entity_keys == ["C-1"]

    def test_query_builder_applies_optimizations(self) -> None:
        from sdol.connectors.olap.query import build_aggregate_query
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["region"],
        )
        query = build_aggregate_query(intent)
        assert "pushdown_aggregation" in query.optimizations
        assert "GROUP BY" in query.sql
