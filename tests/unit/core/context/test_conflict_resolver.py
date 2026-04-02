"""Tests for ConflictResolver."""

from datetime import datetime, timezone, timedelta

from provena.core.context.conflict_resolver import ConflictResolver
from provena.types.context import ConflictResolution, ContextConflict, ContextElement
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
    consistency: ConsistencyGuarantee = ConsistencyGuarantee.STRONG,
    authority: float = 0.5,
    retrieved_at: str | None = None,
    staleness_window_sec: float | None = 60.0,
) -> ContextElement:
    return ContextElement(
        id=id,
        data=data,
        provenance=ProvenanceEnvelope(
            source_system=source,
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=consistency,
            precision=PrecisionClass.EXACT,
            retrieved_at=retrieved_at or datetime.now(timezone.utc).isoformat(),
            staleness_window_sec=staleness_window_sec,
        ),
        trust=TrustScore(
            composite=0.7,
            dimensions=TrustDimensions(
                source_authority=authority,
                consistency_score=0.8,
                freshness_score=0.9,
                precision_score=1.0,
            ),
            label="medium",
        ),
        source_intent_id="i-1",
        entity_key="C-1",
    )


def _make_conflict(
    elem_a: ContextElement,
    elem_b: ContextElement,
    field: str = "name",
    value_a: str = "Alice",
    value_b: str = "Bob",
) -> ContextConflict:
    return ContextConflict(
        element_a=elem_a,
        element_b=elem_b,
        field=field,
        value_a=value_a,
        value_b=value_b,
        resolution=ConflictResolution(
            strategy="defer_to_agent",
            winner=None,
            reason="Unresolved",
        ),
    )


class TestConflictResolver:
    def test_picks_freshest_when_age_gap_large(self) -> None:
        resolver = ConflictResolver()
        now = datetime.now(timezone.utc)
        old_time = (now - timedelta(days=30)).isoformat()
        new_time = now.isoformat()

        a = _make_element("a", "old_src", {"name": "Alice"}, retrieved_at=old_time, staleness_window_sec=60.0)
        b = _make_element("b", "new_src", {"name": "Bob"}, retrieved_at=new_time, staleness_window_sec=60.0)
        conflict = _make_conflict(a, b)

        resolved = resolver.resolve(conflict)
        assert resolved.resolution.strategy == "prefer_freshest"
        assert resolved.resolution.winner == "b"

    def test_defers_when_signals_ambiguous(self) -> None:
        resolver = ConflictResolver()
        now = datetime.now(timezone.utc).isoformat()
        a = _make_element("a", "src_a", {"name": "Alice"}, retrieved_at=now, authority=0.5)
        b = _make_element("b", "src_b", {"name": "Bob"}, retrieved_at=now, authority=0.5)
        conflict = _make_conflict(a, b)

        resolved = resolver.resolve(conflict)
        assert resolved.resolution.strategy == "defer_to_agent"

    def test_picks_authoritative_when_authority_gap(self) -> None:
        resolver = ConflictResolver()
        now = datetime.now(timezone.utc).isoformat()
        a = _make_element("a", "auth_src", {"name": "Alice"}, authority=0.95, retrieved_at=now, staleness_window_sec=None)
        b = _make_element("b", "weak_src", {"name": "Bob"}, authority=0.5, retrieved_at=now, staleness_window_sec=None)
        conflict = _make_conflict(a, b)

        resolved = resolver.resolve(conflict)
        assert resolved.resolution.strategy == "prefer_authoritative"
        assert resolved.resolution.winner == "a"

    def test_picks_strongest_consistency_when_gap(self) -> None:
        resolver = ConflictResolver()
        now = datetime.now(timezone.utc).isoformat()
        a = _make_element(
            "a", "strong_src", {"name": "Alice"},
            consistency=ConsistencyGuarantee.STRONG,
            retrieved_at=now, staleness_window_sec=None,
        )
        b = _make_element(
            "b", "weak_src", {"name": "Bob"},
            consistency=ConsistencyGuarantee.BEST_EFFORT,
            retrieved_at=now, staleness_window_sec=None,
        )
        conflict = _make_conflict(a, b)

        resolved = resolver.resolve(conflict)
        assert resolved.resolution.strategy == "prefer_strongest_consistency"
        assert resolved.resolution.winner == "a"
