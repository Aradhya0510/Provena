"""Generic document connector — reference implementation for vector/semantic search."""

from __future__ import annotations

from typing import Any

from provena.connectors.document.base import BaseDocumentConnector
from provena.connectors.document.query import DocumentQuery, build_search_query
from provena.connectors.executor import QueryExecutor
from provena.types.capability import ConnectorPerformance
from provena.types.errors import InvalidIntentError
from provena.types.intent import SemanticSearchIntent


class GenericDocumentConnector(BaseDocumentConnector):
    """Document connector using a generic vector-search interface.

    Suitable for Pinecone, Weaviate, or as a testing stub.
    """

    def __init__(
        self,
        executor: QueryExecutor,
        connector_id: str = "document.default",
        source_system: str = "pinecone.vectors",
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
            estimated_latency_ms=200,
            max_result_cardinality=100,
        )

    def synthesize_query(self, params: Any) -> DocumentQuery:
        if isinstance(params, SemanticSearchIntent):
            return build_search_query(params)
        raise InvalidIntentError(
            "Unexpected intent type in synthesize_query",
            [{"type": type(params).__name__}],
        )
