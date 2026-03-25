"""Tests for BaseOLTPConnector paradigm base class."""

import pytest

from sdol.connectors.executor import MockQueryExecutor
from sdol.connectors.oltp.base import BaseOLTPConnector
from sdol.types.capability import ConnectorPerformance
from sdol.types.context import ContextSlotType
from sdol.types.errors import InvalidIntentError
from sdol.types.intent import (
    AggregateAnalysisIntent,
    MeasureSpec,
    PointLookupIntent,
    SemanticSearchIntent,
    TemporalTrendIntent,
    TimeWindow,
)
from sdol.types.provenance import ConsistencyGuarantee


class StubOLTPConnector(BaseOLTPConnector):
    """Minimal concrete subclass for testing the paradigm base."""

    def get_performance(self) -> ConnectorPerformance:
        return ConnectorPerformance(
            estimated_latency_ms=5,
            max_result_cardinality=1_000,
            supports_batch_lookup=True,
        )

    def synthesize_query(self, params):
        return {"intent": params}


class TestBaseOLTPConnector:
    def test_connector_type_is_oltp(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        assert conn.connector_type == "oltp"

    def test_capabilities_include_oltp_intents(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        caps = conn.get_capabilities()
        assert "point_lookup" in caps.supported_intent_types
        assert "aggregate_analysis" in caps.supported_intent_types

    def test_performance_from_subclass(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        caps = conn.get_capabilities()
        assert caps.performance.estimated_latency_ms == 5

    def test_can_handle_point_lookup(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "1"})
        assert conn.can_handle(intent)

    def test_rejects_temporal_trend(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        intent = TemporalTrendIntent(
            id="i-1", entity="u", metric="x",
            window=TimeWindow(relative="last_7d"),
        )
        assert not conn.can_handle(intent)

    def test_interpret_intent_accepts_point_lookup(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "1"})
        assert conn.interpret_intent(intent) is intent

    def test_interpret_intent_rejects_semantic_search(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        intent = SemanticSearchIntent(id="i-1", query="q", collection="c")
        with pytest.raises(InvalidIntentError):
            conn.interpret_intent(intent)

    @pytest.mark.asyncio
    async def test_execute_point_lookup(self) -> None:
        executor = MockQueryExecutor(records=[{"id": "C-1", "name": "Alice"}])
        conn = StubOLTPConnector(executor=executor, connector_id="test.oltp")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "C-1"})
        result = await conn.execute(intent)
        assert result.slot_type == ContextSlotType.STRUCTURED
        assert result.provenance.precision.value == "exact"

    @pytest.mark.asyncio
    async def test_execute_aggregate(self) -> None:
        executor = MockQueryExecutor(records=[{"s": "active", "cnt": 5}])
        conn = StubOLTPConnector(executor=executor, connector_id="test.oltp")
        intent = AggregateAnalysisIntent(
            id="i-1", entity="o",
            measures=[MeasureSpec(field="cnt", aggregation="count")],
            dimensions=["s"],
        )
        result = await conn.execute(intent)
        assert result.provenance.precision.value == "exact_aggregate"

    def test_default_staleness_and_consistency(self) -> None:
        conn = StubOLTPConnector(executor=MockQueryExecutor(), connector_id="test.oltp")
        assert conn.default_staleness_sec == 60.0
        assert conn.default_consistency == ConsistencyGuarantee.READ_COMMITTED

    def test_synthesize_query_is_abstract(self) -> None:
        assert BaseOLTPConnector.synthesize_query is not StubOLTPConnector.synthesize_query

    @pytest.mark.asyncio
    async def test_entity_keys_detected(self) -> None:
        executor = MockQueryExecutor(records=[{"customer_id": "C-1", "v": 1}])
        conn = StubOLTPConnector(executor=executor, connector_id="test.oltp")
        intent = PointLookupIntent(
            id="i-1", entity="c", identifier={"customer_id": "C-1"},
        )
        result = await conn.execute(intent)
        assert result.entity_keys == ["C-1"]
