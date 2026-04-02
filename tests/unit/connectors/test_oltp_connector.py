"""Tests for OLTP connector."""

import pytest

from provena.connectors.executor import MockQueryExecutor
from provena.connectors.oltp.generic import GenericOLTPConnector
from provena.types.context import ContextSlotType
from provena.types.errors import InvalidIntentError
from provena.types.intent import (
    AggregateAnalysisIntent,
    MeasureSpec,
    PointLookupIntent,
    SemanticSearchIntent,
)


class TestGenericOLTPConnector:
    def test_handles_point_lookup(self) -> None:
        connector = GenericOLTPConnector(executor=MockQueryExecutor())
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        assert connector.can_handle(intent)

    def test_handles_aggregate_analysis(self) -> None:
        connector = GenericOLTPConnector(executor=MockQueryExecutor())
        intent = AggregateAnalysisIntent(
            id="i-1",
            entity="orders",
            measures=[MeasureSpec(field="total", aggregation="count")],
            dimensions=["status"],
        )
        assert connector.can_handle(intent)

    def test_rejects_semantic_search(self) -> None:
        connector = GenericOLTPConnector(executor=MockQueryExecutor())
        intent = SemanticSearchIntent(
            id="i-1", query="find docs", collection="kb"
        )
        assert not connector.can_handle(intent)

    @pytest.mark.asyncio
    async def test_execute_point_lookup(self) -> None:
        executor = MockQueryExecutor(
            records=[{"id": "C-1", "name": "Alice"}]
        )
        connector = GenericOLTPConnector(executor=executor)
        intent = PointLookupIntent(
            id="i-1",
            entity="customer",
            identifier={"id": "C-1"},
            fields=["name"],
        )
        result = await connector.execute(intent)
        assert result.slot_type == ContextSlotType.STRUCTURED
        assert result.provenance.precision.value == "exact"
        assert len(result.records) == 1

    @pytest.mark.asyncio
    async def test_empty_results(self) -> None:
        executor = MockQueryExecutor(records=[])
        connector = GenericOLTPConnector(executor=executor)
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "MISSING"}
        )
        result = await connector.execute(intent)
        assert result.records == []

    @pytest.mark.asyncio
    async def test_rejects_invalid_intent_type(self) -> None:
        connector = GenericOLTPConnector(executor=MockQueryExecutor())
        intent = SemanticSearchIntent(id="i-1", query="test", collection="kb")
        with pytest.raises(InvalidIntentError):
            await connector.execute(intent)

    def test_query_builder_applies_optimizations(self) -> None:
        from provena.connectors.oltp.query import build_point_lookup_query
        intent = PointLookupIntent(
            id="i-1",
            entity="customer",
            identifier={"id": "C-1"},
            fields=["name", "email"],
        )
        query = build_point_lookup_query(intent)
        assert "parameterized_query" in query.optimizations
        assert "index_aware_field_selection" in query.optimizations
        assert "name, email" in query.sql
