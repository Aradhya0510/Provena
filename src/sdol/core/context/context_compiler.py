"""
ContextCompiler assembles retrieval results into typed context frames.
This is the key innovation — replacing flat token soup with structured,
provenance-enriched context.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sdol.core.context.conflict_detector import ConflictDetector
from sdol.core.context.conflict_resolver import ConflictResolver
from sdol.core.context.typed_slot import INTERPRETATION_NOTES
from sdol.core.provenance.trust_scorer import TrustScorer
from sdol.types.context import (
    ContextElement,
    ContextFrame,
    ContextFrameStats,
    ContextSlot,
    ContextSlotType,
)
from sdol.types.provenance import ProvenanceEnvelope


class CompilerInput:
    """Input to add a single element to the compiler."""

    def __init__(
        self,
        slot_type: ContextSlotType,
        data: Any,
        provenance: ProvenanceEnvelope,
        source_intent_id: str,
        entity_key: str | None = None,
    ) -> None:
        self.slot_type = slot_type
        self.data = data
        self.provenance = provenance
        self.source_intent_id = source_intent_id
        self.entity_key = entity_key


class ContextCompiler:
    def __init__(self, trust_scorer: TrustScorer | None = None) -> None:
        self.trust_scorer = trust_scorer or TrustScorer()
        self.conflict_detector = ConflictDetector()
        self.conflict_resolver = ConflictResolver()
        self._elements: list[tuple[ContextSlotType, ContextElement]] = []
        self._counter = 0

    def add_element(self, input: CompilerInput) -> ContextElement:
        """
        Add a data element. Call for each result from typed connectors.
        Returns the created ContextElement.
        """
        trust = self.trust_scorer.score(input.provenance)
        self._counter += 1
        element = ContextElement(
            id=f"elem-{self._counter}-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
            data=input.data,
            provenance=input.provenance,
            trust=trust,
            source_intent_id=input.source_intent_id,
            entity_key=input.entity_key,
        )
        self._elements.append((input.slot_type, element))
        return element

    def compile(self) -> ContextFrame:
        """
        Compile all added elements into a ContextFrame.
        Groups into typed slots, detects conflicts, computes stats.
        """
        slot_groups: dict[ContextSlotType, list[ContextElement]] = {}
        for slot_type, element in self._elements:
            slot_groups.setdefault(slot_type, []).append(element)

        slots = [
            ContextSlot(
                type=slot_type,
                elements=elements,
                interpretation_notes=INTERPRETATION_NOTES.get(slot_type, ""),
            )
            for slot_type, elements in slot_groups.items()
        ]

        all_elements = [elem for _, elem in self._elements]
        conflicts = self.conflict_detector.detect(all_elements)

        resolved_conflicts = [self.conflict_resolver.resolve(c) for c in conflicts]

        trust_scores = [elem.trust.composite for _, elem in self._elements]
        avg_trust = sum(trust_scores) / len(trust_scores) if trust_scores else 0.0

        stats = ContextFrameStats(
            total_elements=len(self._elements),
            avg_trust_score=round(avg_trust, 4),
            slot_counts={st.value: len(elems) for st, elems in slot_groups.items()},
        )

        return ContextFrame(
            slots=slots,
            conflicts=resolved_conflicts,
            stats=stats,
            assembled_at=datetime.now(timezone.utc).isoformat(),
        )

    def reset(self) -> None:
        self._elements.clear()
        self._counter = 0
