"""
High-level SDK — the main public API of SDOL.
Agent frameworks instantiate this and call query().
"""

from __future__ import annotations

from sdol.agent.intent_formulator import IntentFormulator
from sdol.core.epistemic.epistemic_tracker import EpistemicTracker
from sdol.core.router.semantic_router import SemanticRouter
from sdol.types.context import ContextFrame
from sdol.types.intent import BaseIntent


class SDOL:
    """
    Main entry point for agent frameworks.

    Usage:
        sdol = SDOL(router)
        intent = sdol.formulator.point_lookup("customer", {"id": "C-1042"})
        frame = await sdol.query(intent)
        print(sdol.get_epistemic_context())
    """

    def __init__(self, router: SemanticRouter) -> None:
        self.formulator = IntentFormulator()
        self.tracker = EpistemicTracker()
        self._router = router

    async def query(self, intent: BaseIntent) -> ContextFrame:
        """Send an intent and get back an enriched context frame."""
        frame = await self._router.route(intent)
        self.tracker.ingest(frame)
        return frame

    def get_epistemic_context(self) -> str:
        """Get epistemic summary. Inject into agent's system prompt."""
        return self.tracker.generate_epistemic_prompt()

    def reset(self) -> None:
        """Reset for new conversation/session."""
        self.tracker.reset()
