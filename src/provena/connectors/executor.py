"""Pluggable query executor protocol for decoupling connectors from real databases."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


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


class RetryExecutor:
    """Wraps a QueryExecutor with exponential backoff retry logic.

    Useful for production deployments where transient failures
    (429s, warehouse cold-start timeouts) are expected.
    """

    def __init__(
        self,
        inner: Any,
        max_retries: int = 3,
        base_delay_sec: float = 1.0,
        retryable_errors: tuple[type[Exception], ...] | None = None,
    ) -> None:
        self._inner = inner
        self._max_retries = max_retries
        self._base_delay_sec = base_delay_sec
        self._retryable_errors = retryable_errors or (Exception,)

    async def execute(self, query: Any) -> dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return await self._inner.execute(query)
            except self._retryable_errors as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    delay = self._base_delay_sec * (2 ** attempt)
                    logger.warning(
                        "RetryExecutor: attempt %d/%d failed (%s), retrying in %.1fs",
                        attempt + 1,
                        self._max_retries + 1,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
        raise last_exc  # type: ignore[misc]
