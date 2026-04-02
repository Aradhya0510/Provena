"""Tests for BaseDocumentConnector paradigm base class."""

import pytest

from provena.connectors.document.base import BaseDocumentConnector
from provena.connectors.executor import MockQueryExecutor
from provena.types.capability import ConnectorPerformance
from provena.types.context import ContextSlotType
from provena.types.errors import InvalidIntentError
from provena.types.intent import PointLookupIntent, SemanticSearchIntent
from provena.types.provenance import ConsistencyGuarantee


class StubDocumentConnector(BaseDocumentConnector):
    """Minimal concrete subclass for testing the paradigm base."""

    def get_performance(self) -> ConnectorPerformance:
        return ConnectorPerformance(
            estimated_latency_ms=150,
            max_result_cardinality=50,
        )

    def synthesize_query(self, params):
        return {"intent": params}


class TestBaseDocumentConnector:
    def test_connector_type_is_document(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        assert conn.connector_type == "document"

    def test_capabilities_include_semantic_search(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        caps = conn.get_capabilities()
        assert "semantic_search" in caps.supported_intent_types
        assert caps.capabilities.supports_similarity is True

    def test_performance_from_subclass(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        caps = conn.get_capabilities()
        assert caps.performance.estimated_latency_ms == 150

    def test_can_handle_semantic_search(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        intent = SemanticSearchIntent(id="i-1", query="ml", collection="kb")
        assert conn.can_handle(intent)

    def test_rejects_point_lookup(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "1"})
        assert not conn.can_handle(intent)

    def test_interpret_intent_accepts_semantic_search(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        intent = SemanticSearchIntent(id="i-1", query="q", collection="c")
        assert conn.interpret_intent(intent) is intent

    def test_interpret_intent_rejects_point_lookup(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        intent = PointLookupIntent(id="i-1", entity="c", identifier={"id": "1"})
        with pytest.raises(InvalidIntentError):
            conn.interpret_intent(intent)

    @pytest.mark.asyncio
    async def test_execute_semantic_search(self) -> None:
        executor = MockQueryExecutor(records=[{"text": "ML paper", "score": 0.95}])
        conn = StubDocumentConnector(executor=executor, connector_id="test.doc")
        intent = SemanticSearchIntent(id="i-1", query="ml", collection="kb")
        result = await conn.execute(intent)
        assert result.slot_type == ContextSlotType.UNSTRUCTURED
        assert result.provenance.precision.value == "similarity_ranked"

    def test_default_staleness_and_consistency(self) -> None:
        conn = StubDocumentConnector(executor=MockQueryExecutor(), connector_id="test.doc")
        assert conn.default_staleness_sec == 300.0
        assert conn.default_consistency == ConsistencyGuarantee.EVENTUAL

    def test_synthesize_query_is_abstract(self) -> None:
        assert BaseDocumentConnector.synthesize_query is not StubDocumentConnector.synthesize_query

    @pytest.mark.asyncio
    async def test_entity_keys_always_none(self) -> None:
        executor = MockQueryExecutor(records=[{"text": "doc"}])
        conn = StubDocumentConnector(executor=executor, connector_id="test.doc")
        intent = SemanticSearchIntent(id="i-1", query="q", collection="c")
        result = await conn.execute(intent)
        assert result.entity_keys is None
