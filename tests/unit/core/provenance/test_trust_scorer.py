"""Tests for TrustScorer."""

from datetime import datetime, timezone

import pytest

from sdol.core.provenance.trust_scorer import TrustScorer, TrustScorerConfig
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


def _make_envelope(**overrides) -> ProvenanceEnvelope:
    defaults = {
        "source_system": "test.db",
        "retrieval_method": RetrievalMethod.DIRECT_QUERY,
        "consistency": ConsistencyGuarantee.STRONG,
        "precision": PrecisionClass.EXACT,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "staleness_window_sec": 3600.0,
    }
    return ProvenanceEnvelope(**(defaults | overrides))


class TestTrustScorer:
    def test_high_trust_for_strong_exact_fresh(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope())
        assert score.label == "high"
        assert score.composite >= 0.8

    def test_low_trust_for_weak_heuristic_stale(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope(
            consistency=ConsistencyGuarantee.BEST_EFFORT,
            precision=PrecisionClass.HEURISTIC,
            retrieved_at="2020-01-01T00:00:00+00:00",
            staleness_window_sec=60.0,
        ))
        assert score.label in ("low", "uncertain")
        assert score.composite < 0.4

    def test_unknown_source_gets_neutral_authority(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope())
        assert score.dimensions.source_authority == 0.5

    def test_known_source_gets_configured_authority(self) -> None:
        config = TrustScorerConfig(source_authority_map={"test.db": 0.95})
        scorer = TrustScorer(config)
        score = scorer.score(_make_envelope())
        assert score.dimensions.source_authority == 0.95

    def test_null_staleness_gets_neutral_freshness(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope(staleness_window_sec=None))
        assert score.dimensions.freshness_score == 0.5

    def test_freshly_retrieved_gets_high_freshness(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope(staleness_window_sec=3600.0))
        assert score.dimensions.freshness_score > 0.9

    def test_composite_is_bounded(self) -> None:
        scorer = TrustScorer()
        score = scorer.score(_make_envelope())
        assert 0.0 <= score.composite <= 1.0

    def test_all_precision_classes_have_scores(self) -> None:
        scorer = TrustScorer()
        for pc in PrecisionClass:
            score = scorer.score(_make_envelope(precision=pc))
            assert 0.0 <= score.composite <= 1.0

    def test_all_consistency_levels_have_scores(self) -> None:
        scorer = TrustScorer()
        for cg in ConsistencyGuarantee:
            score = scorer.score(_make_envelope(consistency=cg))
            assert 0.0 <= score.composite <= 1.0
