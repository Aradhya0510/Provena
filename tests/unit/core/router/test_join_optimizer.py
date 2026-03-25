"""Tests for JoinOptimizer."""

from sdol.core.router.join_optimizer import JoinOptimizer, JoinStrategy
from sdol.types.intent import PointLookupIntent
from sdol.types.router import ExecutionStep


def _make_step(step_id: str, connector_id: str) -> ExecutionStep:
    intent = PointLookupIntent(
        id=f"intent-{step_id}", entity="x", identifier={"id": "1"}
    )
    return ExecutionStep(
        step_id=step_id,
        intent=intent,
        connector_id=connector_id,
        depends_on=[],
        estimated_ms=100.0,
        estimated_tokens=500,
    )


class TestJoinOptimizer:
    def test_push_down_when_same_connector(self) -> None:
        optimizer = JoinOptimizer()
        left = _make_step("s1", "same_connector")
        right = _make_step("s2", "same_connector")
        plan = optimizer.plan_join(left, right, "id", 1000, 1000)
        assert plan.strategy == JoinStrategy.PUSH_DOWN

    def test_context_window_join_for_small_cardinalities(self) -> None:
        optimizer = JoinOptimizer()
        left = _make_step("s1", "conn_a")
        right = _make_step("s2", "conn_b")
        plan = optimizer.plan_join(left, right, "id", 30, 20)
        assert plan.strategy == JoinStrategy.CONTEXT_WINDOW_JOIN

    def test_hash_materialize_with_correct_build_side(self) -> None:
        optimizer = JoinOptimizer()
        left = _make_step("s1", "conn_a")
        right = _make_step("s2", "conn_b")
        plan = optimizer.plan_join(left, right, "id", 50, 10000)
        assert plan.strategy == JoinStrategy.HASH_MATERIALIZE
        assert plan.build_side == "left"

    def test_hash_materialize_build_right(self) -> None:
        optimizer = JoinOptimizer()
        left = _make_step("s1", "conn_a")
        right = _make_step("s2", "conn_b")
        plan = optimizer.plan_join(left, right, "id", 10000, 50)
        assert plan.strategy == JoinStrategy.HASH_MATERIALIZE
        assert plan.build_side == "right"
