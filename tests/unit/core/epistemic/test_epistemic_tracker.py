"""Tests for EpistemicTracker."""

from datetime import datetime, timezone

from provena.core.epistemic.epistemic_tracker import EpistemicTracker
from provena.types.context import (
    ConflictResolution,
    ContextConflict,
    ContextElement,
    ContextFrame,
    ContextFrameStats,
    ContextSlot,
    ContextSlotType,
)
from provena.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
    TrustDimensions,
    TrustScore,
)


def _make_element(
    id: str,
    trust_composite: float = 0.8,
    source: str = "test.db",
) -> ContextElement:
    label = "high" if trust_composite >= 0.8 else "medium" if trust_composite >= 0.55 else "low" if trust_composite >= 0.3 else "uncertain"
    return ContextElement(
        id=id,
        data={"value": 42},
        provenance=ProvenanceEnvelope(
            source_system=source,
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=ConsistencyGuarantee.STRONG,
            precision=PrecisionClass.EXACT,
            retrieved_at=datetime.now(timezone.utc).isoformat(),
        ),
        trust=TrustScore(
            composite=trust_composite,
            dimensions=TrustDimensions(
                source_authority=0.5,
                consistency_score=1.0,
                freshness_score=1.0,
                precision_score=1.0,
            ),
            label=label,
        ),
        source_intent_id="i-1",
    )


def _make_frame(
    elements: list[ContextElement] | None = None,
    conflicts: list[ContextConflict] | None = None,
) -> ContextFrame:
    elems = elements or [_make_element("e-1")]
    return ContextFrame(
        slots=[
            ContextSlot(
                type=ContextSlotType.STRUCTURED,
                elements=elems,
                interpretation_notes="Test notes",
            )
        ],
        conflicts=conflicts or [],
        stats=ContextFrameStats(
            total_elements=len(elems),
            avg_trust_score=sum(e.trust.composite for e in elems) / len(elems) if elems else 0,
            slot_counts={"STRUCTURED": len(elems)},
        ),
        assembled_at=datetime.now(timezone.utc).isoformat(),
    )


class TestEpistemicTracker:
    def test_retrieves_trust_for_element(self) -> None:
        tracker = EpistemicTracker()
        tracker.ingest(_make_frame([_make_element("e-1", 0.9)]))
        trust = tracker.get_trust("e-1")
        assert trust is not None
        assert trust.composite == 0.9

    def test_returns_none_for_missing_element(self) -> None:
        tracker = EpistemicTracker()
        assert tracker.get_trust("nonexistent") is None

    def test_gets_low_trust_elements(self) -> None:
        tracker = EpistemicTracker()
        tracker.ingest(_make_frame([
            _make_element("e-1", 0.9),
            _make_element("e-2", 0.2),
            _make_element("e-3", 0.3),
        ]))
        low = tracker.get_low_trust_elements(threshold=0.4)
        assert len(low) == 2
        ids = {e.id for e in low}
        assert "e-2" in ids
        assert "e-3" in ids

    def test_gets_unresolved_conflicts(self) -> None:
        tracker = EpistemicTracker()
        elem_a = _make_element("a")
        elem_b = _make_element("b")
        conflict = ContextConflict(
            element_a=elem_a,
            element_b=elem_b,
            field="name",
            value_a="Alice",
            value_b="Bob",
            resolution=ConflictResolution(
                strategy="defer_to_agent", winner=None, reason="Ambiguous"
            ),
        )
        tracker.ingest(_make_frame([elem_a, elem_b], [conflict]))
        unresolved = tracker.get_unresolved_conflicts()
        assert len(unresolved) == 1

    def test_ignores_resolved_conflicts(self) -> None:
        tracker = EpistemicTracker()
        elem_a = _make_element("a")
        elem_b = _make_element("b")
        conflict = ContextConflict(
            element_a=elem_a,
            element_b=elem_b,
            field="name",
            value_a="Alice",
            value_b="Bob",
            resolution=ConflictResolution(
                strategy="prefer_freshest", winner="a", reason="Fresher"
            ),
        )
        tracker.ingest(_make_frame([elem_a, elem_b], [conflict]))
        assert len(tracker.get_unresolved_conflicts()) == 0

    def test_epistemic_prompt_empty(self) -> None:
        tracker = EpistemicTracker()
        prompt = tracker.generate_epistemic_prompt()
        assert "No data ingested" in prompt

    def test_epistemic_prompt_with_data(self) -> None:
        tracker = EpistemicTracker()
        tracker.ingest(_make_frame([
            _make_element("e-1", 0.9, source="db_a"),
            _make_element("e-2", 0.3, source="db_b"),
        ]))
        prompt = tracker.generate_epistemic_prompt()
        assert "2 data elements" in prompt
        assert "2 sources" in prompt
        assert "low-trust" in prompt

    def test_works_across_multiple_frames(self) -> None:
        tracker = EpistemicTracker()
        tracker.ingest(_make_frame([_make_element("e-1", 0.9)]))
        tracker.ingest(_make_frame([_make_element("e-2", 0.2)]))
        assert tracker.get_trust("e-1") is not None
        assert tracker.get_trust("e-2") is not None
        low = tracker.get_low_trust_elements(0.5)
        assert len(low) == 1

    def test_reset_clears_all(self) -> None:
        tracker = EpistemicTracker()
        tracker.ingest(_make_frame())
        tracker.reset()
        assert tracker.get_trust("e-1") is None
        assert tracker.generate_epistemic_prompt() == "## Data Confidence Summary\nNo data ingested yet."
