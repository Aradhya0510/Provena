"""Search query builder for document/vector systems."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provena.types.intent import FilterClause, SemanticSearchIntent


@dataclass
class DocumentQuery:
    """Native query representation for document/vector search systems."""

    query_text: str
    collection: str
    vector_weight: float
    keyword_weight: float
    filters: dict[str, Any]
    max_results: int
    include_reranking: bool
    optimizations: list[str]
    relevance_threshold: float


def build_search_query(intent: SemanticSearchIntent) -> DocumentQuery:
    """Build a document search query with hybrid retrieval optimizations."""
    optimizations: list[str] = []

    hybrid_weight = intent.hybrid_weight if intent.hybrid_weight is not None else 0.7
    vector_weight = hybrid_weight
    keyword_weight = 1.0 - hybrid_weight

    if vector_weight > 0 and keyword_weight > 0:
        optimizations.append("hybrid_retrieval")
    elif vector_weight > 0:
        optimizations.append("vector_only")
    else:
        optimizations.append("keyword_only")

    filters: dict[str, Any] = {}
    if intent.filters:
        for f in intent.filters:
            filters[f.field] = _convert_filter(f)
        optimizations.append("filter_pushdown")

    if intent.rerank:
        optimizations.append("reranking")

    optimizations.append("score_based_truncation")

    max_results = intent.max_results or 20

    return DocumentQuery(
        query_text=intent.query,
        collection=intent.collection,
        vector_weight=vector_weight,
        keyword_weight=keyword_weight,
        filters=filters,
        max_results=max_results,
        include_reranking=intent.rerank,
        optimizations=optimizations,
        relevance_threshold=0.3,
    )


def _convert_filter(f: FilterClause) -> dict[str, Any]:
    return {"operator": f.operator, "value": f.value}
