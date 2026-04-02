"""
TrustScorer computes composite trust signals from provenance metadata.
Trust scores are ADVISORY — the agent retains full autonomy over weighting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from provena.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    TrustDimensions,
    TrustScore,
)

CONSISTENCY_SCORES: dict[ConsistencyGuarantee, float] = {
    ConsistencyGuarantee.STRONG: 1.0,
    ConsistencyGuarantee.READ_COMMITTED: 0.8,
    ConsistencyGuarantee.EVENTUAL: 0.5,
    ConsistencyGuarantee.BEST_EFFORT: 0.2,
}

PRECISION_SCORES: dict[PrecisionClass, float] = {
    PrecisionClass.EXACT: 1.0,
    PrecisionClass.EXACT_AGGREGATE: 0.95,
    PrecisionClass.LOGICALLY_ENTAILED: 0.9,
    PrecisionClass.ESTIMATED: 0.6,
    PrecisionClass.SIMILARITY_RANKED: 0.55,
    PrecisionClass.PREDICTED: 0.5,
    PrecisionClass.HEURISTIC: 0.3,
}


@dataclass
class TrustScorerConfig:
    weight_source_authority: float = 0.2
    weight_consistency: float = 0.3
    weight_freshness: float = 0.2
    weight_precision: float = 0.3
    source_authority_map: dict[str, float] = field(default_factory=dict)


class TrustScorer:
    def __init__(self, config: TrustScorerConfig | None = None) -> None:
        self.config = config or TrustScorerConfig()

    def score(self, envelope: ProvenanceEnvelope) -> TrustScore:
        source_authority = self.config.source_authority_map.get(envelope.source_system, 0.5)
        consistency_score = CONSISTENCY_SCORES[envelope.consistency]
        precision_score = PRECISION_SCORES[envelope.precision]
        freshness_score = self._compute_freshness(envelope)

        composite = (
            self.config.weight_source_authority * source_authority
            + self.config.weight_consistency * consistency_score
            + self.config.weight_freshness * freshness_score
            + self.config.weight_precision * precision_score
        )

        composite = max(0.0, min(1.0, composite))

        if composite >= 0.8:
            label = "high"
        elif composite >= 0.55:
            label = "medium"
        elif composite >= 0.3:
            label = "low"
        else:
            label = "uncertain"

        return TrustScore(
            composite=composite,
            dimensions=TrustDimensions(
                source_authority=source_authority,
                consistency_score=consistency_score,
                freshness_score=freshness_score,
                precision_score=precision_score,
            ),
            label=label,
        )

    def _compute_freshness(self, envelope: ProvenanceEnvelope) -> float:
        if envelope.staleness_window_sec is None:
            return 0.5

        retrieved_at = datetime.fromisoformat(envelope.retrieved_at)
        now = datetime.now(timezone.utc)
        age_sec = (now - retrieved_at).total_seconds()
        window_sec = envelope.staleness_window_sec

        if window_sec <= 0:
            return 0.5

        ratio = age_sec / window_sec
        return max(0.0, 1.0 - ratio / 2.0)
