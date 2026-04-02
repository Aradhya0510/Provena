"""
IntentFormulator provides builder methods for constructing well-formed intents.
Validates everything via Pydantic before returning.
"""

from __future__ import annotations

import time
from typing import Any

from provena.types.intent import (
    AggregateAnalysisIntent,
    BaseIntent,
    CompositeIntent,
    EscapeHatchIntent,
    FilterClause,
    FusionOperator,
    GraphTraversalIntent,
    MeasureSpec,
    NodeSpec,
    OntologyQueryIntent,
    OrderSpec,
    PointLookupIntent,
    SemanticSearchIntent,
    TemporalTrendIntent,
    TimeWindow,
)


class IntentFormulator:
    def __init__(self) -> None:
        self._counter = 0

    def _next_id(self) -> str:
        self._counter += 1
        return f"intent-{self._counter}-{int(time.time() * 1000)}"

    def point_lookup(
        self,
        entity: str,
        identifier: dict[str, str | int],
        fields: list[str] | None = None,
    ) -> PointLookupIntent:
        return PointLookupIntent(
            id=self._next_id(),
            entity=entity,
            identifier=identifier,
            fields=fields,
        )

    def temporal_trend(
        self,
        entity: str,
        metric: str,
        window: dict[str, str],
        granularity: str | None = None,
        direction: str | None = None,
        join_key: str | None = None,
    ) -> TemporalTrendIntent:
        return TemporalTrendIntent(
            id=self._next_id(),
            entity=entity,
            metric=metric,
            window=TimeWindow(**window),
            granularity=granularity,
            direction=direction,
            join_key=join_key,
        )

    def aggregate_analysis(
        self,
        entity: str,
        measures: list[dict[str, Any]],
        dimensions: list[str],
        filters: list[dict[str, Any]] | None = None,
        order_by: list[dict[str, str]] | None = None,
        having: list[dict[str, Any]] | None = None,
    ) -> AggregateAnalysisIntent:
        return AggregateAnalysisIntent(
            id=self._next_id(),
            entity=entity,
            measures=[MeasureSpec(**m) for m in measures],
            dimensions=dimensions,
            filters=[FilterClause(**f) for f in filters] if filters else None,
            order_by=[OrderSpec(**o) for o in order_by] if order_by else None,
            having=[FilterClause(**h) for h in having] if having else None,
        )

    def graph_traversal(
        self,
        start_node: dict[str, Any],
        max_depth: int,
        edge_types: list[str] | None = None,
        direction: str | None = None,
        return_paths: bool = False,
    ) -> GraphTraversalIntent:
        node_filters = start_node.get("filters")
        node = NodeSpec(
            type=start_node.get("type"),
            identifier=start_node.get("identifier"),
            filters=[FilterClause(**f) for f in node_filters] if node_filters else None,
        )
        return GraphTraversalIntent(
            id=self._next_id(),
            start_node=node,
            max_depth=max_depth,
            edge_types=edge_types,
            direction=direction,
            return_paths=return_paths,
        )

    def semantic_search(
        self,
        query: str,
        collection: str,
        filters: list[dict[str, Any]] | None = None,
        hybrid_weight: float | None = None,
        rerank: bool = False,
    ) -> SemanticSearchIntent:
        return SemanticSearchIntent(
            id=self._next_id(),
            query=query,
            collection=collection,
            filters=[FilterClause(**f) for f in filters] if filters else None,
            hybrid_weight=hybrid_weight,
            rerank=rerank,
        )

    def ontology_query(
        self,
        subject: str | None = None,
        predicate: str | None = None,
        object: str | None = None,
        inference_depth: int | None = None,
        include_entailments: bool = True,
    ) -> OntologyQueryIntent:
        return OntologyQueryIntent(
            id=self._next_id(),
            subject=subject,
            predicate=predicate,
            object=object,
            inference_depth=inference_depth,
            include_entailments=include_entailments,
        )

    def escape_hatch(
        self,
        target_connector: str,
        raw_parameters: dict[str, Any],
        description: str,
    ) -> EscapeHatchIntent:
        return EscapeHatchIntent(
            id=self._next_id(),
            target_connector=target_connector,
            raw_parameters=raw_parameters,
            description=description,
        )

    def composite(
        self,
        sub_intents: list[BaseIntent],
        fusion_operator: str | FusionOperator,
        fusion_key: str | None = None,
    ) -> CompositeIntent:
        op = (
            FusionOperator(fusion_operator)
            if isinstance(fusion_operator, str)
            else fusion_operator
        )
        return CompositeIntent(
            id=self._next_id(),
            sub_intents=sub_intents,
            fusion_operator=op,
            fusion_key=fusion_key,
        )
