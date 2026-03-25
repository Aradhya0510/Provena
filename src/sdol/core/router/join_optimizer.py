"""
Cross-source join optimizer.
Determines how to efficiently combine results from different storage paradigms.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Awaitable, Callable

from sdol.types.connector import ConnectorResult
from sdol.types.router import ExecutionStep


class JoinStrategy(StrEnum):
    HASH_MATERIALIZE = "hash_materialize"
    NESTED_LOOKUP = "nested_lookup"
    CONTEXT_WINDOW_JOIN = "context_window_join"
    PUSH_DOWN = "push_down"


@dataclass
class JoinPlan:
    strategy: JoinStrategy
    left_step_id: str
    right_step_id: str
    join_key: str
    build_side: str | None = None
    estimated_result_size: int = 0


class JoinOptimizer:
    def plan_join(
        self,
        left_step: ExecutionStep,
        right_step: ExecutionStep,
        join_key: str,
        left_cardinality: int,
        right_cardinality: int,
    ) -> JoinPlan:
        """
        Determine optimal join strategy.

        Decision logic:
        1. If both target same connector -> push_down
        2. If one side < 100 records -> hash_materialize (build on small side)
        3. If both sides < 50 records -> context_window_join
        4. Otherwise -> hash_materialize on smaller side
        """
        if left_step.connector_id == right_step.connector_id:
            return JoinPlan(
                strategy=JoinStrategy.PUSH_DOWN,
                left_step_id=left_step.step_id,
                right_step_id=right_step.step_id,
                join_key=join_key,
                estimated_result_size=min(left_cardinality, right_cardinality),
            )

        if left_cardinality < 50 and right_cardinality < 50:
            return JoinPlan(
                strategy=JoinStrategy.CONTEXT_WINDOW_JOIN,
                left_step_id=left_step.step_id,
                right_step_id=right_step.step_id,
                join_key=join_key,
                estimated_result_size=min(left_cardinality, right_cardinality),
            )

        if left_cardinality <= right_cardinality:
            build_side = "left"
        else:
            build_side = "right"

        return JoinPlan(
            strategy=JoinStrategy.HASH_MATERIALIZE,
            left_step_id=left_step.step_id,
            right_step_id=right_step.step_id,
            join_key=join_key,
            build_side=build_side,
            estimated_result_size=min(left_cardinality, right_cardinality),
        )

    async def execute_hash_materialize(
        self,
        build_result: ConnectorResult,
        probe_step: ExecutionStep,
        join_key: str,
        execute_step: Callable[[ExecutionStep], Awaitable[ConnectorResult]],
    ) -> list[ConnectorResult]:
        """
        1. Extract join key values from build_result
        2. Add IN filter to probe_step's intent to narrow scope
        3. Execute narrowed probe_step
        4. Return both results
        """
        join_values: list[Any] = []
        for record in build_result.records:
            if isinstance(record, dict) and join_key in record:
                join_values.append(record[join_key])

        if join_values and hasattr(probe_step.intent, "filters"):
            from sdol.types.intent import FilterClause

            narrowing_filter = FilterClause(
                field=join_key,
                operator="in",
                value=join_values,
            )
            existing_filters = getattr(probe_step.intent, "filters", None) or []
            object.__setattr__(
                probe_step.intent, "filters", [*existing_filters, narrowing_filter]
            )

        probe_result = await execute_step(probe_step)
        return [build_result, probe_result]
