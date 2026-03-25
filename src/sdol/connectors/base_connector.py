"""
Abstract base class for typed connectors.
Subclasses implement the four internal stages:
  1. interpret_intent  — validate and extract parameters
  2. synthesize_query  — build native query
  3. execute_query     — run the query (via MCP or direct)
  4. normalize_result  — transform to ConnectorResult
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any

from sdol.types.capability import ConnectorCapability
from sdol.types.connector import ConnectorHealth, ConnectorResult
from sdol.types.intent import BaseIntent


class BaseConnector(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def connector_type(self) -> str: ...

    @abstractmethod
    def get_capabilities(self) -> ConnectorCapability: ...

    def can_handle(self, intent: BaseIntent) -> bool:
        """Check if this connector can handle the given intent."""
        caps = self.get_capabilities()
        return intent.type in caps.supported_intent_types

    async def execute(self, intent: BaseIntent) -> ConnectorResult:
        """
        Execute an intent end-to-end.
        Main entry point called by the Semantic Router.
        """
        start = time.monotonic()

        params = self.interpret_intent(intent)
        query = self.synthesize_query(params)
        raw_result = await self.execute_query(query)

        elapsed_ms = (time.monotonic() - start) * 1000
        return self.normalize_result(raw_result, intent, elapsed_ms)

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
