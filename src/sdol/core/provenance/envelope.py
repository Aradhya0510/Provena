"""Factory functions for creating ProvenanceEnvelopes."""

from datetime import datetime, timezone

from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


def create_envelope(
    source_system: str,
    retrieval_method: RetrievalMethod,
    consistency: ConsistencyGuarantee,
    precision: PrecisionClass,
    staleness_window_sec: float | None = None,
    execution_ms: float | None = None,
    result_truncated: bool | None = None,
    total_available: int | None = None,
) -> ProvenanceEnvelope:
    """Create a validated ProvenanceEnvelope with current timestamp."""
    return ProvenanceEnvelope(
        source_system=source_system,
        retrieval_method=retrieval_method,
        consistency=consistency,
        precision=precision,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        staleness_window_sec=staleness_window_sec,
        execution_ms=execution_ms,
        result_truncated=result_truncated,
        total_available=total_available,
    )


def create_default_envelope(source_system: str) -> ProvenanceEnvelope:
    """
    Conservative defaults for legacy MCP responses
    that don't provide provenance metadata.
    """
    return ProvenanceEnvelope(
        source_system=source_system,
        retrieval_method=RetrievalMethod.MCP_PASSTHROUGH,
        consistency=ConsistencyGuarantee.BEST_EFFORT,
        precision=PrecisionClass.ESTIMATED,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        staleness_window_sec=None,
    )
