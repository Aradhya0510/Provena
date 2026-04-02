"""Tests for BaseOLAPConnector paradigm base class."""

import pytest

from provena.connectors.executor import MockQueryExecutor
from provena.connectors.olap.base import BaseOLAPConnector
from provena.types.capability import ConnectorPerformance
from provena.types.context import ContextSlotType
from provena.types.errors import InvalidIntentError
from provena.types.intent import (
    AggregateAnalysisIntent,
    MeasureSpec,
    PointLookupIntent,
    SemanticSearchIntent,
    TemporalTrendIntent,
    TimeWindow,
)
from provena.types.provenance import ConsistencyGuarantee


class StubOLAPConnector(BaseOLAPConnector):
    """Minimal concrete subclass for testing the paradigm base."""

    def get_performance(self) -> ConnectorPerformance:
        return ConnectorPerformance(
            estimated_latency_ms=100,
            max_result_cardinality=500_000,
        )

    def synthesize_query(self, params):
        return {"intent": params}


class TestBaseOLAPConnector:
    def test_connector_type_is_olap(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        assert conn.connector_type == "olap"

    def test_capabilities_include_olap_intents(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        caps = conn.get_capabilities()
        assert "aggregate_analysis" in caps.supported_intent_types
        assert "temporal_trend" in caps.supported_intent_types
        assert caps.capabilities.supports_aggregation is True
        assert caps.capabilities.supports_windowing is True

    def test_performance_from_subclass(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        caps = conn.get_capabilities()
        assert caps.performance.estimated_latency_ms == 100
        assert caps.performance.max_result_cardinality == 500_000

    def test_can_handle_aggregate(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["region"],
        )
        assert conn.can_handle(intent)

    def test_can_handle_temporal(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        intent = TemporalTrendIntent(
            id="i-1", entity="usage", metric="calls",
            window=TimeWindow(relative="last_30d"),
        )
        assert conn.can_handle(intent)

    def test_rejects_point_lookup(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "1"})
        assert not conn.can_handle(intent)

    def test_interpret_intent_accepts_aggregate(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        intent = AggregateAnalysisIntent(
            id="i-1", entity="o",
            measures=[MeasureSpec(field="x", aggregation="sum")],
            dimensions=["d"],
        )
        result = conn.interpret_intent(intent)
        assert result is intent

    def test_interpret_intent_rejects_semantic_search(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        intent = SemanticSearchIntent(id="i-1", query="q", collection="c")
        with pytest.raises(InvalidIntentError):
            conn.interpret_intent(intent)

    @pytest.mark.asyncio
    async def test_execute_aggregate_produces_structured_slot(self) -> None:
        executor = MockQueryExecutor(records=[{"region": "west", "sum_x": 100}])
        conn = StubOLAPConnector(executor=executor, connector_id="test.olap")
        intent = AggregateAnalysisIntent(
            id="i-1", entity="o",
            measures=[MeasureSpec(field="x", aggregation="sum")],
            dimensions=["region"],
        )
        result = await conn.execute(intent)
        assert result.slot_type == ContextSlotType.STRUCTURED
        assert result.provenance.precision.value == "exact_aggregate"

    @pytest.mark.asyncio
    async def test_execute_temporal_produces_temporal_slot(self) -> None:
        executor = MockQueryExecutor(records=[{"bucket": "2024-01-01", "calls": 42}])
        conn = StubOLAPConnector(executor=executor, connector_id="test.olap")
        intent = TemporalTrendIntent(
            id="i-1", entity="usage", metric="calls",
            window=TimeWindow(relative="last_30d"),
        )
        result = await conn.execute(intent)
        assert result.slot_type == ContextSlotType.TEMPORAL

    def test_default_staleness_and_consistency(self) -> None:
        conn = StubOLAPConnector(executor=MockQueryExecutor(), connector_id="test.olap")
        assert conn.default_staleness_sec == 3600.0
        assert conn.default_consistency == ConsistencyGuarantee.STRONG

    def test_synthesize_query_is_abstract(self) -> None:
        assert BaseOLAPConnector.synthesize_query is not StubOLAPConnector.synthesize_query

    @pytest.mark.asyncio
    async def test_entity_keys_detected(self) -> None:
        executor = MockQueryExecutor(records=[{"customer_id": "C-1", "v": 1}])
        conn = StubOLAPConnector(executor=executor, connector_id="test.olap")
        intent = AggregateAnalysisIntent(
            id="i-1", entity="o",
            measures=[MeasureSpec(field="v", aggregation="sum")],
            dimensions=["customer_id"],
        )
        result = await conn.execute(intent)
        assert result.entity_keys == ["C-1"]
