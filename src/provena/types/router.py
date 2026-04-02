"""Router and execution plan types."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from provena.types.connector import ConnectorResult
from provena.types.intent import BaseIntent


class ExecutionStep(BaseModel):
    step_id: str
    intent: BaseIntent
    connector_id: str
    depends_on: list[str]
    estimated_ms: float
    estimated_tokens: int


class ExecutionPlan(BaseModel):
    steps: list[ExecutionStep]
    estimated_total_ms: float
    estimated_total_tokens: int
    has_parallel_steps: bool
    fusion_strategy: str | None = None


class ExecutionError(BaseModel):
    step_id: str
    error_message: str
    error_code: str


class ExecutionResult(BaseModel):
    """
    Note: results is a dict of step_id -> ConnectorResult.
    Pydantic v2 handles dict serialization.
    """

    results: dict[str, ConnectorResult]
    plan: ExecutionPlan
    actual_total_ms: float
    errors: list[ExecutionError]
