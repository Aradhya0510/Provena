"""Tests for envelope factory functions."""

from pydantic import ValidationError
import pytest

from sdol.core.provenance.envelope import create_default_envelope, create_envelope
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    RetrievalMethod,
)


class TestCreateEnvelope:
    def test_creates_valid_envelope(self) -> None:
        env = create_envelope(
            source_system="test.db",
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=ConsistencyGuarantee.STRONG,
            precision=PrecisionClass.EXACT,
        )
        assert env.source_system == "test.db"
        assert env.retrieval_method == RetrievalMethod.DIRECT_QUERY
        assert env.retrieved_at is not None

    def test_includes_optional_fields(self) -> None:
        env = create_envelope(
            source_system="test.db",
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=ConsistencyGuarantee.STRONG,
            precision=PrecisionClass.EXACT,
            staleness_window_sec=3600.0,
            execution_ms=42.0,
            result_truncated=True,
            total_available=1000,
        )
        assert env.staleness_window_sec == 3600.0
        assert env.execution_ms == 42.0
        assert env.result_truncated is True
        assert env.total_available == 1000

    def test_sets_current_timestamp(self) -> None:
        env = create_envelope(
            source_system="test.db",
            retrieval_method=RetrievalMethod.DIRECT_QUERY,
            consistency=ConsistencyGuarantee.STRONG,
            precision=PrecisionClass.EXACT,
        )
        assert "T" in env.retrieved_at


class TestCreateDefaultEnvelope:
    def test_uses_conservative_defaults(self) -> None:
        env = create_default_envelope("legacy.server")
        assert env.source_system == "legacy.server"
        assert env.retrieval_method == RetrievalMethod.MCP_PASSTHROUGH
        assert env.consistency == ConsistencyGuarantee.BEST_EFFORT
        assert env.precision == PrecisionClass.ESTIMATED
        assert env.staleness_window_sec is None
