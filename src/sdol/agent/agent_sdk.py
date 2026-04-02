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
        self._cost_records: list[dict] = []

    async def query(
        self, intent: BaseIntent, *, min_trust: float | None = None
    ) -> ContextFrame:
        """Send an intent and get back an enriched context frame.

        Args:
            intent: The typed intent to execute.
            min_trust: Optional minimum trust threshold (0.0–1.0). Elements
                below this threshold are excluded from the returned frame,
                and a warning is added to the epistemic context.
        """
        frame = await self._router.route(intent)

        if min_trust is not None:
            frame = self._apply_trust_threshold(frame, min_trust)

        # Track cost from provenance
        for slot in frame.slots:
            for elem in slot.elements:
                if elem.provenance.execution_ms is not None:
                    self._cost_records.append({
                        "source": elem.provenance.source_system,
                        "execution_ms": elem.provenance.execution_ms,
                        "intent_id": elem.source_intent_id,
                    })

        self.tracker.ingest(frame)
        return frame

    def _apply_trust_threshold(
        self, frame: ContextFrame, min_trust: float
    ) -> ContextFrame:
        excluded_count = 0
        filtered_slots = []
        for slot in frame.slots:
            kept = []
            for elem in slot.elements:
                if elem.trust.composite >= min_trust:
                    kept.append(elem)
                else:
                    excluded_count += 1
            if kept:
                filtered_slots.append(
                    slot.model_copy(update={"elements": kept})
                )

        if excluded_count > 0:
            self.tracker.add_warning(
                f"{excluded_count} element(s) excluded by min_trust={min_trust:.2f} threshold"
            )

        stats = frame.stats.model_copy(update={
            "total_elements": frame.stats.total_elements - excluded_count,
        })
        return frame.model_copy(update={"slots": filtered_slots, "stats": stats})

    def get_epistemic_context(self) -> str:
        """Get epistemic summary. Inject into agent's system prompt."""
        return self.tracker.generate_epistemic_prompt()

    def get_cost_summary(self) -> dict:
        """Get summary of execution costs across all queries in this session."""
        if not self._cost_records:
            return {"total_queries": 0, "total_execution_ms": 0.0, "by_source": {}}

        by_source: dict[str, dict] = {}
        for rec in self._cost_records:
            src = rec["source"]
            if src not in by_source:
                by_source[src] = {"query_count": 0, "total_ms": 0.0}
            by_source[src]["query_count"] += 1
            by_source[src]["total_ms"] += rec["execution_ms"]

        return {
            "total_queries": len(self._cost_records),
            "total_execution_ms": sum(r["execution_ms"] for r in self._cost_records),
            "by_source": by_source,
        }

    def reset(self) -> None:
        """Reset for new conversation/session."""
        self.tracker.reset()
        self._cost_records.clear()
