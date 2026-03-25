"""Pluggable query executor protocol for decoupling connectors from real databases."""

from __future__ import annotations

from typing import Any, Protocol


class QueryExecutor(Protocol):
    """Protocol for executing native queries against a data source."""

    async def execute(self, query: Any) -> dict[str, Any]:
        """Returns {"records": [...], "meta": {...}}"""
        ...


class MockQueryExecutor:
    """Mock executor for testing — returns pre-configured results."""

    def __init__(
        self,
        records: list[Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.records = records or []
        self.meta = meta or {}
        self.last_query: Any = None

    async def execute(self, query: Any) -> dict[str, Any]:
        self.last_query = query
        return {"records": self.records, "meta": self.meta}
