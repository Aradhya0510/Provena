"""Resolves detected conflicts using provenance-based heuristics."""

from __future__ import annotations

from datetime import datetime

from provena.types.context import ConflictResolution, ContextConflict
from provena.types.provenance import ConsistencyGuarantee

CONSISTENCY_ORDERING: dict[ConsistencyGuarantee, int] = {
    ConsistencyGuarantee.STRONG: 3,
    ConsistencyGuarantee.READ_COMMITTED: 2,
    ConsistencyGuarantee.EVENTUAL: 1,
    ConsistencyGuarantee.BEST_EFFORT: 0,
}


class ConflictResolver:
    def resolve(self, conflict: ContextConflict) -> ContextConflict:
        """Apply resolution heuristics and return conflict with resolution filled in."""
        resolution = self._determine_resolution(conflict)
        return conflict.model_copy(update={"resolution": resolution})

    def _determine_resolution(self, conflict: ContextConflict) -> ConflictResolution:
        a = conflict.element_a
        b = conflict.element_b

        freshness_resolution = self._check_freshness(conflict)
        if freshness_resolution is not None:
            return freshness_resolution

        authority_resolution = self._check_authority(conflict)
        if authority_resolution is not None:
            return authority_resolution

        consistency_resolution = self._check_consistency(conflict)
        if consistency_resolution is not None:
            return consistency_resolution

        return ConflictResolution(
            strategy="defer_to_agent",
            winner=None,
            reason=(
                f"No clear winner: both sources have similar trust signals for field '{conflict.field}'"
            ),
        )

    def _check_freshness(self, conflict: ContextConflict) -> ConflictResolution | None:
        a = conflict.element_a
        b = conflict.element_b

        a_staleness = a.provenance.staleness_window_sec
        b_staleness = b.provenance.staleness_window_sec

        if a_staleness is None or b_staleness is None:
            return None
        if a_staleness <= 0 or b_staleness <= 0:
            return None

        a_time = datetime.fromisoformat(a.provenance.retrieved_at)
        b_time = datetime.fromisoformat(b.provenance.retrieved_at)

        age_diff_sec = abs((a_time - b_time).total_seconds())
        min_staleness = min(a_staleness, b_staleness)

        if age_diff_sec > 10 * min_staleness:
            winner = a if a_time > b_time else b
            return ConflictResolution(
                strategy="prefer_freshest",
                winner=winner.id,
                reason=(
                    f"Freshness gap ({age_diff_sec:.0f}s) exceeds "
                    f"10x staleness window ({min_staleness:.0f}s)"
                ),
            )
        return None

    def _check_authority(self, conflict: ContextConflict) -> ConflictResolution | None:
        a = conflict.element_a
        b = conflict.element_b

        a_auth = a.trust.dimensions.source_authority
        b_auth = b.trust.dimensions.source_authority

        if a_auth > 0.9 and b_auth < 0.7:
            return ConflictResolution(
                strategy="prefer_authoritative",
                winner=a.id,
                reason=f"Source authority gap: {a_auth:.2f} vs {b_auth:.2f}",
            )
        if b_auth > 0.9 and a_auth < 0.7:
            return ConflictResolution(
                strategy="prefer_authoritative",
                winner=b.id,
                reason=f"Source authority gap: {b_auth:.2f} vs {a_auth:.2f}",
            )
        return None

    def _check_consistency(self, conflict: ContextConflict) -> ConflictResolution | None:
        a = conflict.element_a
        b = conflict.element_b

        a_level = CONSISTENCY_ORDERING[a.provenance.consistency]
        b_level = CONSISTENCY_ORDERING[b.provenance.consistency]

        gap = abs(a_level - b_level)
        if gap >= 2:
            winner = a if a_level > b_level else b
            return ConflictResolution(
                strategy="prefer_strongest_consistency",
                winner=winner.id,
                reason=(
                    f"Consistency gap: {a.provenance.consistency.value} "
                    f"vs {b.provenance.consistency.value}"
                ),
            )
        return None
