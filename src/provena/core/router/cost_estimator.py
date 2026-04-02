"""Cost estimator for execution planning."""

from __future__ import annotations

from provena.types.capability import ConnectorCapability
from provena.types.router import ExecutionStep


class CostEstimator:
    def __init__(
        self,
        overhead_ms: float = 10.0,
        tokens_per_record: int = 50,
    ) -> None:
        self._overhead_ms = overhead_ms
        self._tokens_per_record = tokens_per_record
        self._historical_latencies: list[float] = []

    def estimate_latency(self, capability: ConnectorCapability) -> float:
        base = capability.performance.estimated_latency_ms + self._overhead_ms
        if self._historical_latencies:
            avg_actual = sum(self._historical_latencies) / len(self._historical_latencies)
            return (base + avg_actual) / 2.0
        return base

    def estimate_tokens(self, capability: ConnectorCapability) -> int:
        max_results = min(capability.performance.max_result_cardinality, 1000)
        return max_results * self._tokens_per_record

    def estimate_total_ms(self, steps: list[ExecutionStep]) -> float:
        """
        Account for parallelism: parallel steps contribute max(latencies), not sum.
        Group steps by dependency level.
        """
        if not steps:
            return 0.0

        levels = self._group_by_level(steps)
        total = 0.0
        for level_steps in levels:
            total += max(s.estimated_ms for s in level_steps)
        return total

    def record_actual(self, actual_ms: float) -> None:
        self._historical_latencies.append(actual_ms)
        if len(self._historical_latencies) > 100:
            self._historical_latencies = self._historical_latencies[-100:]

    def _group_by_level(self, steps: list[ExecutionStep]) -> list[list[ExecutionStep]]:
        step_map = {s.step_id: s for s in steps}
        levels: dict[str, int] = {}

        def get_level(step_id: str) -> int:
            if step_id in levels:
                return levels[step_id]
            step = step_map[step_id]
            if not step.depends_on:
                levels[step_id] = 0
                return 0
            max_dep = max(get_level(d) for d in step.depends_on if d in step_map)
            levels[step_id] = max_dep + 1
            return max_dep + 1

        for s in steps:
            get_level(s.step_id)

        max_level = max(levels.values()) if levels else 0
        grouped: list[list[ExecutionStep]] = [[] for _ in range(max_level + 1)]
        for s in steps:
            grouped[levels[s.step_id]].append(s)
        return grouped
