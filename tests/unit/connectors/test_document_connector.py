"""Tests for Document connector."""

import pytest

from provena.connectors.document.generic import GenericDocumentConnector
from provena.connectors.executor import MockQueryExecutor
from provena.types.context import ContextSlotType
from provena.types.errors import InvalidIntentError
from provena.types.intent import PointLookupIntent, SemanticSearchIntent


class TestGenericDocumentConnector:
    def test_handles_semantic_search(self) -> None:
        connector = GenericDocumentConnector(executor=MockQueryExecutor())
        intent = SemanticSearchIntent(
            id="i-1", query="machine learning", collection="kb"
        )
        assert connector.can_handle(intent)

    def test_rejects_point_lookup(self) -> None:
        connector = GenericDocumentConnector(executor=MockQueryExecutor())
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        assert not connector.can_handle(intent)

    @pytest.mark.asyncio
    async def test_execute_semantic_search(self) -> None:
        executor = MockQueryExecutor(
            records=[{"text": "ML paper", "score": 0.95}]
        )
        connector = GenericDocumentConnector(executor=executor)
        intent = SemanticSearchIntent(
            id="i-1", query="machine learning", collection="kb"
        )
        result = await connector.execute(intent)
        assert result.slot_type == ContextSlotType.UNSTRUCTURED
        assert result.provenance.precision.value == "similarity_ranked"
        assert len(result.records) == 1

    @pytest.mark.asyncio
    async def test_empty_results(self) -> None:
        executor = MockQueryExecutor(records=[])
        connector = GenericDocumentConnector(executor=executor)
        intent = SemanticSearchIntent(
            id="i-1", query="nonexistent topic", collection="kb"
        )
        result = await connector.execute(intent)
        assert result.records == []

    @pytest.mark.asyncio
    async def test_rejects_invalid_intent_type(self) -> None:
        connector = GenericDocumentConnector(executor=MockQueryExecutor())
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        with pytest.raises(InvalidIntentError):
            await connector.execute(intent)

    def test_query_builder_hybrid_retrieval(self) -> None:
        from provena.connectors.document.query import build_search_query
        intent = SemanticSearchIntent(
            id="i-1",
            query="machine learning",
            collection="kb",
            hybrid_weight=0.7,
        )
        query = build_search_query(intent)
        assert "hybrid_retrieval" in query.optimizations
        assert query.vector_weight == 0.7
        assert query.keyword_weight == pytest.approx(0.3)

    def test_query_builder_reranking(self) -> None:
        from provena.connectors.document.query import build_search_query
        intent = SemanticSearchIntent(
            id="i-1",
            query="ml",
            collection="kb",
            rerank=True,
        )
        query = build_search_query(intent)
        assert "reranking" in query.optimizations
        assert query.include_reranking is True
