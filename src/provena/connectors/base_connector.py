"""
Abstract base class for typed connectors.
Subclasses implement the four internal stages:
  1. interpret_intent  — validate and extract parameters
  2. synthesize_query  — build native query
  3. execute_query     — run the query (via MCP or direct)
  4. normalize_result  — transform to ConnectorResult
"""

from __future__ import annotations

import hashlib
import json
import time
from abc import ABC, abstractmethod
from typing import Any

from provena.types.capability import ConnectorCapability
from provena.types.connector import ConnectorHealth, ConnectorResult
from provena.types.intent import BaseIntent


class _CacheEntry:
    __slots__ = ("result", "cached_at")

    def __init__(self, result: ConnectorResult) -> None:
        self.result = result
        self.cached_at = time.monotonic()


class BaseConnector(ABC):

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def connector_type(self) -> str: ...

    @property
    @abstractmethod
    def source_system(self) -> str: ...

    @abstractmethod
    def get_capabilities(self) -> ConnectorCapability: ...

    def can_handle(self, intent: BaseIntent) -> bool:
        """Check if this connector can handle the given intent."""
        caps = self.get_capabilities()
        return intent.type in caps.supported_intent_types

    # -- Staleness-aware cache -----------------------------------------------

    _cache_enabled: bool = False
    _cache: dict[str, _CacheEntry] = {}
    _cache_ttl_sec: float | None = None

    def enable_cache(self, ttl_sec: float | None = None) -> None:
        """Enable staleness-aware caching.

        Args:
            ttl_sec: Cache TTL in seconds. If None, uses the connector's
                default staleness window from provenance config.
        """
        self._cache_enabled = True
        self._cache = {}
        self._cache_ttl_sec = ttl_sec

    def disable_cache(self) -> None:
        self._cache_enabled = False
        self._cache.clear()

    def _cache_key(self, intent: BaseIntent) -> str:
        payload = intent.model_dump_json(exclude={"id"})
        return hashlib.sha256(payload.encode()).hexdigest()

    def _get_effective_ttl(self, result: ConnectorResult | None = None) -> float:
        if self._cache_ttl_sec is not None:
            return self._cache_ttl_sec
        if result is not None and result.provenance.staleness_window_sec:
            return result.provenance.staleness_window_sec
        return 60.0  # conservative default

    async def execute(self, intent: BaseIntent) -> ConnectorResult:
        """
        Execute an intent end-to-end.
        Main entry point called by the Semantic Router.
        """
        if self._cache_enabled:
            key = self._cache_key(intent)
            entry = self._cache.get(key)
            if entry is not None:
                ttl = self._get_effective_ttl(entry.result)
                age = time.monotonic() - entry.cached_at
                if age < ttl:
                    return entry.result
                del self._cache[key]

        start = time.monotonic()

        params = self.interpret_intent(intent)
        query = self.synthesize_query(params)
        raw_result = await self.execute_query(query)

        elapsed_ms = (time.monotonic() - start) * 1000
        result = self.normalize_result(raw_result, intent, elapsed_ms)

        if self._cache_enabled:
            self._cache[self._cache_key(intent)] = _CacheEntry(result)

        return result

    @abstractmethod
    async def check_health(self) -> ConnectorHealth: ...

    @abstractmethod
    def interpret_intent(self, intent: BaseIntent) -> Any:
        """Stage 1: Validate intent and extract typed parameters."""
        ...

    @abstractmethod
    def synthesize_query(self, params: Any) -> Any:
        """Stage 2: Build native query from parameters."""
        ...

    @abstractmethod
    async def execute_query(self, query: Any) -> Any:
        """Stage 3: Execute the native query."""
        ...

    @abstractmethod
    def normalize_result(
        self,
        raw: Any,
        intent: BaseIntent,
        execution_ms: float,
    ) -> ConnectorResult:
        """Stage 4: Transform raw results to ConnectorResult."""
        ...
