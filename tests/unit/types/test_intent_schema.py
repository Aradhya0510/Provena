"""Tests for intent model validation."""

import pytest
from pydantic import ValidationError

from provena.types.intent import (
    AggregateAnalysisIntent,
    CompositeIntent,
    EscapeHatchIntent,
    FusionOperator,
    GraphTraversalIntent,
    NodeSpec,
    OntologyQueryIntent,
    PointLookupIntent,
    SemanticSearchIntent,
    TemporalTrendIntent,
    TimeWindow,
    validate_intent,
)


class TestPointLookupIntent:
    def test_valid_point_lookup(self) -> None:
        intent = PointLookupIntent(
            id="intent-001",
            entity="customer",
            identifier={"customer_id": "C-1042"},
        )
        assert intent.type == "point_lookup"
        assert intent.entity == "customer"

    def test_rejects_empty_entity(self) -> None:
        with pytest.raises(ValidationError):
            PointLookupIntent(
                id="intent-001",
                entity="",
                identifier={"customer_id": "C-1042"},
            )

    def test_rejects_missing_identifier(self) -> None:
        with pytest.raises(ValidationError):
            PointLookupIntent(id="intent-001", entity="customer")  # type: ignore[call-arg]

    def test_optional_fields(self) -> None:
        intent = PointLookupIntent(
            id="intent-001",
            entity="customer",
            identifier={"customer_id": "C-1042"},
            fields=["name", "email"],
            max_results=10,
            budget_ms=500,
        )
        assert intent.fields == ["name", "email"]
        assert intent.max_results == 10


class TestTemporalTrendIntent:
    def test_valid_with_relative_window(self) -> None:
        intent = TemporalTrendIntent(
            id="intent-002",
            entity="usage",
            metric="api_calls",
            window=TimeWindow(relative="last_90d"),
            granularity="1d",
        )
        assert intent.type == "temporal_trend"

    def test_rejects_empty_time_window(self) -> None:
        with pytest.raises(ValidationError):
            TemporalTrendIntent(
                id="intent-002",
                entity="usage",
                metric="api_calls",
                window=TimeWindow(),
            )

    def test_valid_with_absolute_window(self) -> None:
        intent = TemporalTrendIntent(
            id="intent-002",
            entity="usage",
            metric="api_calls",
            window=TimeWindow(start="2025-01-01T00:00:00Z", end="2025-03-01T00:00:00Z"),
        )
        assert intent.window.start is not None


class TestAggregateAnalysisIntent:
    def test_valid_aggregate(self) -> None:
        intent = AggregateAnalysisIntent(
            id="intent-003",
            entity="orders",
            measures=[{"field": "revenue", "aggregation": "sum"}],
            dimensions=["region"],
        )
        assert intent.type == "aggregate_analysis"
        assert len(intent.measures) == 1

    def test_rejects_empty_measures(self) -> None:
        with pytest.raises(ValidationError):
            AggregateAnalysisIntent(
                id="intent-003",
                entity="orders",
                measures=[],
                dimensions=["region"],
            )


class TestGraphTraversalIntent:
    def test_valid_graph_traversal(self) -> None:
        intent = GraphTraversalIntent(
            id="intent-004",
            start_node=NodeSpec(type="person", identifier={"id": "P-1"}),
            max_depth=3,
            edge_types=["knows"],
        )
        assert intent.type == "graph_traversal"
        assert intent.max_depth == 3

    def test_rejects_depth_over_limit(self) -> None:
        with pytest.raises(ValidationError):
            GraphTraversalIntent(
                id="intent-004",
                start_node=NodeSpec(type="person"),
                max_depth=11,
            )


class TestSemanticSearchIntent:
    def test_valid_semantic_search(self) -> None:
        intent = SemanticSearchIntent(
            id="intent-005",
            query="find documents about machine learning",
            collection="knowledge_base",
        )
        assert intent.type == "semantic_search"


class TestOntologyQueryIntent:
    def test_valid_ontology_query(self) -> None:
        intent = OntologyQueryIntent(
            id="intent-006",
            subject="Dog",
            predicate="subClassOf",
            object="Animal",
        )
        assert intent.type == "ontology_query"
        assert intent.include_entailments is True


class TestCompositeIntent:
    def test_valid_composite(self) -> None:
        sub1 = PointLookupIntent(
            id="sub-1", entity="customer", identifier={"id": "C-1"}
        )
        sub2 = PointLookupIntent(
            id="sub-2", entity="order", identifier={"id": "O-1"}
        )
        intent = CompositeIntent(
            id="composite-1",
            sub_intents=[sub1, sub2],
            fusion_operator=FusionOperator.INTERSECT,
            fusion_key="customer_id",
        )
        assert intent.type == "composite"
        assert len(intent.sub_intents) == 2


class TestEscapeHatchIntent:
    def test_valid_escape_hatch(self) -> None:
        intent = EscapeHatchIntent(
            id="intent-007",
            target_connector="custom.db",
            raw_parameters={"query": "SELECT 1"},
            description="Test query",
        )
        assert intent.type == "escape_hatch"


class TestValidateIntent:
    def test_discriminates_by_type_field(self) -> None:
        result = validate_intent({
            "id": "intent-001",
            "type": "point_lookup",
            "entity": "customer",
            "identifier": {"customer_id": "C-1042"},
        })
        assert isinstance(result, PointLookupIntent)

    def test_discriminates_aggregate(self) -> None:
        result = validate_intent({
            "id": "intent-003",
            "type": "aggregate_analysis",
            "entity": "orders",
            "measures": [{"field": "revenue", "aggregation": "sum"}],
            "dimensions": ["region"],
        })
        assert isinstance(result, AggregateAnalysisIntent)

    def test_discriminates_temporal(self) -> None:
        result = validate_intent({
            "id": "intent-002",
            "type": "temporal_trend",
            "entity": "usage",
            "metric": "api_calls",
            "window": {"relative": "last_90d"},
        })
        assert isinstance(result, TemporalTrendIntent)

    def test_discriminates_semantic_search(self) -> None:
        result = validate_intent({
            "id": "intent-005",
            "type": "semantic_search",
            "query": "find documents",
            "collection": "kb",
        })
        assert isinstance(result, SemanticSearchIntent)

    def test_discriminates_graph_traversal(self) -> None:
        result = validate_intent({
            "id": "intent-004",
            "type": "graph_traversal",
            "start_node": {"type": "person"},
            "max_depth": 3,
        })
        assert isinstance(result, GraphTraversalIntent)

    def test_discriminates_ontology(self) -> None:
        result = validate_intent({
            "id": "intent-006",
            "type": "ontology_query",
            "subject": "Dog",
        })
        assert isinstance(result, OntologyQueryIntent)

    def test_discriminates_escape_hatch(self) -> None:
        result = validate_intent({
            "id": "intent-007",
            "type": "escape_hatch",
            "target_connector": "x",
            "raw_parameters": {},
            "description": "test",
        })
        assert isinstance(result, EscapeHatchIntent)

    def test_rejects_unknown_type(self) -> None:
        with pytest.raises(ValidationError):
            validate_intent({
                "id": "intent-bad",
                "type": "nonexistent_type",
                "entity": "x",
            })

    def test_rejects_malformed_data(self) -> None:
        with pytest.raises(ValidationError):
            validate_intent({"garbage": True})
