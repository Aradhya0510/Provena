"""Tests for Agent SDK."""

import pytest

from sdol.agent.agent_sdk import SDOL
from sdol.connectors.capability_registry import CapabilityRegistry
from sdol.connectors.executor import MockQueryExecutor
from sdol.connectors.oltp.generic import GenericOLTPConnector
from sdol.core.context.context_compiler import ContextCompiler
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner
from sdol.core.router.semantic_router import SemanticRouter


def _setup_sdol(records=None) -> SDOL:
    registry = CapabilityRegistry()
    registry.register(GenericOLTPConnector(
        executor=MockQueryExecutor(records=records or [{"id": "C-1", "name": "Alice"}])
    ))
    compiler = ContextCompiler()
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    router = SemanticRouter(planner, compiler, registry)
    return SDOL(router)


class TestSDOL:
    @pytest.mark.asyncio
    async def test_query_returns_context_frame(self) -> None:
        sdol = _setup_sdol()
        intent = sdol.formulator.point_lookup("customer", {"id": "C-1"})
        frame = await sdol.query(intent)
        assert frame.stats.total_elements == 1

    @pytest.mark.asyncio
    async def test_epistemic_context_after_query(self) -> None:
        sdol = _setup_sdol()
        intent = sdol.formulator.point_lookup("customer", {"id": "C-1"})
        await sdol.query(intent)
        context = sdol.get_epistemic_context()
        assert "1 data elements" in context
        assert "1 sources" in context

    @pytest.mark.asyncio
    async def test_reset_clears_tracker(self) -> None:
        sdol = _setup_sdol()
        intent = sdol.formulator.point_lookup("customer", {"id": "C-1"})
        await sdol.query(intent)
        sdol.reset()
        context = sdol.get_epistemic_context()
        assert "No data ingested" in context

    def test_formulator_available(self) -> None:
        sdol = _setup_sdol()
        assert sdol.formulator is not None
