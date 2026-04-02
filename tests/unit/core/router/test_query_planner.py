"""Tests for QueryPlanner."""

import pytest

from provena.connectors.capability_registry import CapabilityRegistry
from provena.connectors.executor import MockQueryExecutor
from provena.connectors.olap.generic import GenericOLAPConnector
from provena.connectors.oltp.generic import GenericOLTPConnector
from provena.core.router.cost_estimator import CostEstimator
from provena.core.router.intent_decomposer import IntentDecomposer
from provena.core.router.query_planner import QueryPlanner
from provena.types.errors import NoCapableConnectorError
from provena.types.intent import (
    AggregateAnalysisIntent,
    CompositeIntent,
    FusionOperator,
    MeasureSpec,
    PointLookupIntent,
    SemanticSearchIntent,
)


def _setup_planner() -> QueryPlanner:
    registry = CapabilityRegistry()
    registry.register(GenericOLAPConnector(executor=MockQueryExecutor()))
    registry.register(GenericOLTPConnector(executor=MockQueryExecutor()))
    return QueryPlanner(registry, IntentDecomposer(), CostEstimator())


class TestQueryPlanner:
    def test_plans_single_intent(self) -> None:
        planner = _setup_planner()
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        plan = planner.plan(intent)
        assert len(plan.steps) == 1
        assert plan.steps[0].connector_id == "oltp.default"

    def test_plans_composite_intent(self) -> None:
        planner = _setup_planner()
        sub1 = AggregateAnalysisIntent(
            id="s-1",
            entity="orders",
            measures=[MeasureSpec(field="revenue", aggregation="sum")],
            dimensions=["region"],
        )
        sub2 = PointLookupIntent(
            id="s-2", entity="customer", identifier={"id": "C-1"}
        )
        composite = CompositeIntent(
            id="c-1",
            sub_intents=[sub1, sub2],
            fusion_operator=FusionOperator.INTERSECT,
        )
        plan = planner.plan(composite)
        assert len(plan.steps) == 2
        assert plan.fusion_strategy == "intersect"

    def test_raises_for_unhandled_intent(self) -> None:
        planner = _setup_planner()
        intent = SemanticSearchIntent(
            id="i-1", query="find docs", collection="kb"
        )
        with pytest.raises(NoCapableConnectorError):
            planner.plan(intent)

    def test_parallel_steps_detected(self) -> None:
        planner = _setup_planner()
        sub1 = PointLookupIntent(id="s-1", entity="a", identifier={"id": "1"})
        sub2 = PointLookupIntent(id="s-2", entity="b", identifier={"id": "2"})
        composite = CompositeIntent(
            id="c-1",
            sub_intents=[sub1, sub2],
            fusion_operator=FusionOperator.INTERSECT,
        )
        plan = planner.plan(composite)
        assert plan.has_parallel_steps is True

    def test_estimates_total_ms(self) -> None:
        planner = _setup_planner()
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        plan = planner.plan(intent)
        assert plan.estimated_total_ms > 0
        assert plan.estimated_total_tokens > 0
