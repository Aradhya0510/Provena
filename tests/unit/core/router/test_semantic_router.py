"""Tests for SemanticRouter."""

import pytest

from sdol.connectors.capability_registry import CapabilityRegistry
from sdol.connectors.executor import MockQueryExecutor
from sdol.connectors.olap.generic import GenericOLAPConnector
from sdol.connectors.oltp.generic import GenericOLTPConnector
from sdol.core.context.context_compiler import ContextCompiler
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner
from sdol.core.router.semantic_router import SemanticRouter
from sdol.types.intent import (
    AggregateAnalysisIntent,
    CompositeIntent,
    FusionOperator,
    MeasureSpec,
    PointLookupIntent,
)


def _setup_router(
    olap_records=None,
    oltp_records=None,
) -> SemanticRouter:
    registry = CapabilityRegistry()
    registry.register(GenericOLAPConnector(
        executor=MockQueryExecutor(records=olap_records or [])
    ))
    registry.register(GenericOLTPConnector(
        executor=MockQueryExecutor(records=oltp_records or [])
    ))
    compiler = ContextCompiler()
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    return SemanticRouter(planner, compiler, registry)


class TestSemanticRouter:
    @pytest.mark.asyncio
    async def test_routes_single_intent(self) -> None:
        router = _setup_router(oltp_records=[{"id": "C-1", "name": "Alice"}])
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        frame = await router.route(intent)
        assert frame.stats.total_elements == 1
        assert len(frame.slots) == 1

    @pytest.mark.asyncio
    async def test_routes_composite_intent(self) -> None:
        router = _setup_router(
            olap_records=[{"region": "west", "sum_revenue": 1000}],
            oltp_records=[{"id": "C-1", "name": "Alice"}],
        )
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
        frame = await router.route(composite)
        assert frame.stats.total_elements == 2

    @pytest.mark.asyncio
    async def test_empty_results(self) -> None:
        router = _setup_router()
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "MISSING"}
        )
        frame = await router.route(intent)
        assert frame.stats.total_elements == 0

    @pytest.mark.asyncio
    async def test_provenance_attached(self) -> None:
        router = _setup_router(oltp_records=[{"id": "C-1"}])
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        frame = await router.route(intent)
        for slot in frame.slots:
            for elem in slot.elements:
                assert elem.provenance is not None
                assert elem.trust is not None
