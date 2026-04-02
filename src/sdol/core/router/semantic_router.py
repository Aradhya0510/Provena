"""
SemanticRouter — the main orchestrator.
Takes an intent, plans execution, runs the plan, compiles into ContextFrame.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from sdol.connectors.base_connector import BaseConnector
from sdol.connectors.capability_registry import CapabilityRegistry
from sdol.core.context.context_compiler import CompilerInput, ContextCompiler
from sdol.core.router.query_planner import QueryPlanner
from sdol.types.context import ContextFrame
from sdol.types.intent import BaseIntent
from sdol.types.router import ExecutionError, ExecutionPlan, ExecutionResult, ExecutionStep


class SemanticRouter:
    def __init__(
        self,
        planner: QueryPlanner,
        compiler: ContextCompiler,
        registry: CapabilityRegistry,
    ) -> None:
        self.planner = planner
        self.compiler = compiler
        self.registry = registry

    async def route(self, intent: BaseIntent) -> ContextFrame:
        """
        Main entry point.
        Intent -> Plan -> Execute -> Compile -> ContextFrame
        """
        plan = self.planner.plan(intent)
        execution_result = await self._execute_plan(plan)

        self.compiler.reset()
        for step_id, result in execution_result.results.items():
            step = next(s for s in plan.steps if s.step_id == step_id)
            for i, record in enumerate(result.records):
                self.compiler.add_element(
                    CompilerInput(
                        slot_type=result.slot_type,
                        data=record,
                        provenance=result.provenance,
                        source_intent_id=step.intent.id,
                        entity_key=(
                            result.entity_keys[i] if result.entity_keys else None
                        ),
                    )
                )

        # Build expected sources list for presence conflict detection
        expected_sources = []
        for step in plan.steps:
            connector = self.registry.get_connector(step.connector_id)
            if connector is not None:
                expected_sources.append({
                    "source_system": connector.source_system,
                    "connector_id": step.connector_id,
                })

        return self.compiler.compile(expected_sources=expected_sources)

    async def _execute_plan(self, plan: ExecutionPlan) -> ExecutionResult:
        """
        Execute plan with dependency-aware parallelism.
        Uses topological sort to group steps into levels.
        Each level runs in parallel via asyncio.gather.
        """
        start = time.monotonic()
        results: dict[str, Any] = {}
        errors: list[ExecutionError] = []
        completed_step_ids: set[str] = set()

        levels = self._topological_levels(plan.steps)

        for level in levels:
            tasks: list[Any] = []
            level_steps: list[ExecutionStep] = []
            for step in level:
                connector = self.registry.get_connector(step.connector_id)
                if connector is None:
                    errors.append(
                        ExecutionError(
                            step_id=step.step_id,
                            error_message=f"Connector not found: {step.connector_id}",
                            error_code="NO_CAPABLE_CONNECTOR",
                        )
                    )
                    continue
                tasks.append(self._execute_step(step, connector))
                level_steps.append(step)

            if tasks:
                step_results = await asyncio.gather(*tasks, return_exceptions=True)

                for step, result in zip(level_steps, step_results):
                    if isinstance(result, Exception):
                        errors.append(
                            ExecutionError(
                                step_id=step.step_id,
                                error_message=str(result),
                                error_code="CONNECTOR_ERROR",
                            )
                        )
                    else:
                        results[step.step_id] = result
                        completed_step_ids.add(step.step_id)

        elapsed = (time.monotonic() - start) * 1000
        return ExecutionResult(
            results=results,
            plan=plan,
            actual_total_ms=elapsed,
            errors=errors,
        )

    async def _execute_step(
        self, step: ExecutionStep, connector: BaseConnector
    ) -> Any:
        return await connector.execute(step.intent)

    def _topological_levels(
        self, steps: list[ExecutionStep]
    ) -> list[list[ExecutionStep]]:
        """
        Group steps into execution levels.
        Level 0: steps with no dependencies.
        Level 1: steps depending only on level 0 steps. Etc.
        """
        step_map = {s.step_id: s for s in steps}
        levels: dict[str, int] = {}

        def get_level(step_id: str) -> int:
            if step_id in levels:
                return levels[step_id]
            step = step_map[step_id]
            if not step.depends_on:
                levels[step_id] = 0
                return 0
            dep_levels = []
            for dep in step.depends_on:
                if dep in step_map:
                    dep_levels.append(get_level(dep))
            max_dep = max(dep_levels) if dep_levels else -1
            levels[step_id] = max_dep + 1
            return max_dep + 1

        for s in steps:
            get_level(s.step_id)

        if not levels:
            return []

        max_level = max(levels.values())
        grouped: list[list[ExecutionStep]] = [[] for _ in range(max_level + 1)]
        for s in steps:
            grouped[levels[s.step_id]].append(s)
        return grouped
