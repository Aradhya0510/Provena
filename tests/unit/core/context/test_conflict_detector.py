"""Tests for ConflictDetector."""

from datetime import datetime, timezone

from provena.core.context.conflict_detector import ConflictDetector
from provena.types.context import ContextElement
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
    source: str,
    data: dict,
    entity_key: str | None = None,
) -> ContextElement:
    return ContextElement(
        id=id,
        data=data,
        provenance=ProvenanceEnvelope(
            source_system=source,
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=ConsistencyGuarantee.STRONG,
            precision=PrecisionClass.EXACT,
            retrieved_at=datetime.now(timezone.utc).isoformat(),
        ),
        trust=TrustScore(
            composite=0.8,
            dimensions=TrustDimensions(
                source_authority=0.5,
                consistency_score=1.0,
                freshness_score=1.0,
                precision_score=1.0,
            ),
            label="high",
        ),
        source_intent_id="i-1",
        entity_key=entity_key,
    )


class TestConflictDetector:
    def test_finds_conflicts_for_same_entity_different_values(self) -> None:
        detector = ConflictDetector()
        elements = [
            _make_element("e1", "source_a", {"name": "Alice"}, entity_key="C-1042"),
            _make_element("e2", "source_b", {"name": "Bob"}, entity_key="C-1042"),
        ]
        conflicts = detector.detect(elements)
        assert len(conflicts) == 1
        assert conflicts[0].field == "name"

    def test_ignores_elements_without_entity_keys(self) -> None:
        detector = ConflictDetector()
        elements = [
            _make_element("e1", "source_a", {"name": "Alice"}, entity_key=None),
            _make_element("e2", "source_b", {"name": "Bob"}, entity_key=None),
        ]
        conflicts = detector.detect(elements)
        assert len(conflicts) == 0

    def test_no_conflict_for_same_source(self) -> None:
        detector = ConflictDetector()
        elements = [
            _make_element("e1", "same_source", {"name": "Alice"}, entity_key="C-1042"),
            _make_element("e2", "same_source", {"name": "Bob"}, entity_key="C-1042"),
        ]
        conflicts = detector.detect(elements)
        assert len(conflicts) == 0

    def test_no_conflict_for_matching_values(self) -> None:
        detector = ConflictDetector()
        elements = [
            _make_element("e1", "source_a", {"name": "Alice"}, entity_key="C-1042"),
            _make_element("e2", "source_b", {"name": "Alice"}, entity_key="C-1042"),
        ]
        conflicts = detector.detect(elements)
        assert len(conflicts) == 0

    def test_handles_no_conflicts_gracefully(self) -> None:
        detector = ConflictDetector()
        conflicts = detector.detect([])
        assert len(conflicts) == 0

    def test_detects_multiple_field_conflicts(self) -> None:
        detector = ConflictDetector()
        elements = [
            _make_element("e1", "source_a", {"name": "Alice", "age": 30}, entity_key="C-1"),
            _make_element("e2", "source_b", {"name": "Bob", "age": 25}, entity_key="C-1"),
        ]
        conflicts = detector.detect(elements)
        assert len(conflicts) == 2
