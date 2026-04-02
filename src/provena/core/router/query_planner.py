"""
QueryPlanner generates execution plans for intents.
1. Decompose into sub-intents
2. Route each to best connector
3. Analyze dependencies
4. Build execution plan with parallel/sequential steps
5. Estimate total cost
"""

from __future__ import annotations

from provena.connectors.capability_registry import CapabilityRegistry
from provena.core.router.cost_estimator import CostEstimator
from provena.core.router.intent_decomposer import IntentDecomposer
from provena.types.errors import NoCapableConnectorError
from provena.types.intent import BaseIntent, CompositeIntent
from provena.types.router import ExecutionPlan, ExecutionStep


class QueryPlanner:
    def __init__(
        self,
        registry: CapabilityRegistry,
        decomposer: IntentDecomposer,
        cost_estimator: CostEstimator,
    ) -> None:
        self.registry = registry
        self.decomposer = decomposer
        self.cost_estimator = cost_estimator

    def plan(self, intent: BaseIntent) -> ExecutionPlan:
        sub_intents = self.decomposer.decompose(intent)

        steps: list[ExecutionStep] = []
        for i, sub in enumerate(sub_intents):
            candidates = self.registry.find_candidates(sub)
            if not candidates:
                raise NoCapableConnectorError(sub.type)

            best = candidates[0]
            step = ExecutionStep(
                step_id=f"step-{i}",
                intent=sub,
                connector_id=best.capability.connector_id,
                depends_on=[],
                estimated_ms=self.cost_estimator.estimate_latency(best.capability),
                estimated_tokens=self.cost_estimator.estimate_tokens(best.capability),
            )
            steps.append(step)

        if isinstance(intent, CompositeIntent):
            deps = self.decomposer.analyze_dependencies(
                sub_intents, intent.fusion_operator.value, intent.fusion_key
            )
            for step in steps:
                step.depends_on = deps.get(step.intent.id, [])

        has_parallel = (
            any(len(s.depends_on) == 0 for s in steps[1:]) if len(steps) > 1 else False
        )

        return ExecutionPlan(
            steps=steps,
            estimated_total_ms=self.cost_estimator.estimate_total_ms(steps),
            estimated_total_tokens=sum(s.estimated_tokens for s in steps),
            has_parallel_steps=has_parallel,
            fusion_strategy=(
                intent.fusion_operator.value if isinstance(intent, CompositeIntent) else None
            ),
        )
