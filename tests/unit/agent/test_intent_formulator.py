"""Tests for IntentFormulator."""

import pytest
from pydantic import ValidationError

from sdol.agent.intent_formulator import IntentFormulator
from sdol.types.intent import (
    AggregateAnalysisIntent,
    CompositeIntent,
    EscapeHatchIntent,
    GraphTraversalIntent,
    OntologyQueryIntent,
    PointLookupIntent,
    SemanticSearchIntent,
    TemporalTrendIntent,
)


class TestIntentFormulator:
    def test_point_lookup(self) -> None:
        f = IntentFormulator()
        intent = f.point_lookup("customer", {"id": "C-1"}, fields=["name"])
        assert isinstance(intent, PointLookupIntent)
        assert intent.entity == "customer"
        assert intent.fields == ["name"]

    def test_temporal_trend(self) -> None:
        f = IntentFormulator()
        intent = f.temporal_trend(
            "usage", "api_calls", {"relative": "last_90d"}, granularity="1d"
        )
        assert isinstance(intent, TemporalTrendIntent)

    def test_aggregate_analysis(self) -> None:
        f = IntentFormulator()
        intent = f.aggregate_analysis(
            "orders",
            measures=[{"field": "revenue", "aggregation": "sum"}],
            dimensions=["region"],
        )
        assert isinstance(intent, AggregateAnalysisIntent)
        assert len(intent.measures) == 1

    def test_graph_traversal(self) -> None:
        f = IntentFormulator()
        intent = f.graph_traversal(
            start_node={"type": "person", "identifier": {"id": "P-1"}},
            max_depth=3,
            edge_types=["knows"],
        )
        assert isinstance(intent, GraphTraversalIntent)

    def test_semantic_search(self) -> None:
        f = IntentFormulator()
        intent = f.semantic_search("machine learning", "kb")
        assert isinstance(intent, SemanticSearchIntent)

    def test_ontology_query(self) -> None:
        f = IntentFormulator()
        intent = f.ontology_query(subject="Dog", predicate="subClassOf")
        assert isinstance(intent, OntologyQueryIntent)

    def test_escape_hatch(self) -> None:
        f = IntentFormulator()
        intent = f.escape_hatch("custom.db", {"sql": "SELECT 1"}, "test")
        assert isinstance(intent, EscapeHatchIntent)

    def test_composite(self) -> None:
        f = IntentFormulator()
        sub1 = f.point_lookup("customer", {"id": "C-1"})
        sub2 = f.point_lookup("order", {"id": "O-1"})
        intent = f.composite([sub1, sub2], "intersect", fusion_key="customer_id")
        assert isinstance(intent, CompositeIntent)
        assert len(intent.sub_intents) == 2

    def test_unique_ids(self) -> None:
        f = IntentFormulator()
        i1 = f.point_lookup("a", {"id": "1"})
        i2 = f.point_lookup("b", {"id": "2"})
        assert i1.id != i2.id

    def test_rejects_invalid_params(self) -> None:
        f = IntentFormulator()
        with pytest.raises(ValidationError):
            f.point_lookup("", {"id": "C-1"})
