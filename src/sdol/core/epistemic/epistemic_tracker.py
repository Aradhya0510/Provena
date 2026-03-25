"""
EpistemicTracker maintains running confidence model over context.
Provides trust-weighted reasoning support and prompt injection.
"""

from __future__ import annotations

from sdol.types.context import ContextElement, ContextFrame
from sdol.types.provenance import TrustScore


class EpistemicTracker:
    def __init__(self) -> None:
        self._frames: list[ContextFrame] = []

    def ingest(self, frame: ContextFrame) -> None:
        """Ingest a new context frame."""
        self._frames.append(frame)

    def get_trust(self, element_id: str) -> TrustScore | None:
        """Get trust score for a specific element."""
        for frame in self._frames:
            for slot in frame.slots:
                for elem in slot.elements:
                    if elem.id == element_id:
                        return elem.trust
        return None

    def get_low_trust_elements(self, threshold: float = 0.4) -> list[ContextElement]:
        """Get all elements below a trust threshold."""
        results: list[ContextElement] = []
        for frame in self._frames:
            for slot in frame.slots:
                for elem in slot.elements:
                    if elem.trust.composite < threshold:
                        results.append(elem)
        return results

    def get_unresolved_conflicts(self) -> list[object]:
        """Get all conflicts deferred to agent."""
        return [
            conflict
            for frame in self._frames
            for conflict in frame.conflicts
            if conflict.resolution.strategy == "defer_to_agent"
        ]

    def generate_epistemic_prompt(self) -> str:
        """
        Generate structured text that communicates epistemic context to the LLM.
        This is injected into the agent's prompt so it can reason about confidence.
        """
        if not self._frames:
            return "## Data Confidence Summary\nNo data ingested yet."

        all_elements: list[ContextElement] = []
        sources: set[str] = set()
        for frame in self._frames:
            for slot in frame.slots:
                for elem in slot.elements:
                    all_elements.append(elem)
                    sources.add(elem.provenance.source_system)

        avg_trust = (
            sum(e.trust.composite for e in all_elements) / len(all_elements)
            if all_elements
            else 0.0
        )

        low_trust = self.get_low_trust_elements()
        conflicts = self.get_unresolved_conflicts()

        lines = [
            "## Data Confidence Summary",
            f"- {len(all_elements)} data elements from {len(sources)} sources",
            f"- Average trust: {avg_trust:.2f}",
        ]

        if low_trust:
            lines.append(f"- {len(low_trust)} low-trust elements:")
            for elem in low_trust[:5]:
                lines.append(
                    f"  - {elem.id}: source={elem.provenance.source_system}, "
                    f"precision={elem.provenance.precision.value}, "
                    f"trust={elem.trust.composite:.2f}"
                )

        if conflicts:
            lines.append(f"- {len(conflicts)} unresolved conflicts:")
            for c in conflicts[:3]:
                lines.append(
                    f"  - {c.field}: {c.value_a} ({c.element_a.provenance.source_system}) "
                    f"vs {c.value_b} ({c.element_b.provenance.source_system})"
                )

        return "\n".join(lines)

    def reset(self) -> None:
        self._frames.clear()
