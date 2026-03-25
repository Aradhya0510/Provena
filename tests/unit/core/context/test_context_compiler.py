"""Tests for ContextCompiler."""

from datetime import datetime, timezone

from sdol.core.context.context_compiler import CompilerInput, ContextCompiler
from sdol.core.provenance.trust_scorer import TrustScorer
from sdol.types.context import ContextSlotType
from sdol.types.provenance import (
    ConsistencyGuarantee,
    PrecisionClass,
    ProvenanceEnvelope,
    RetrievalMethod,
)


def _make_provenance(**overrides) -> ProvenanceEnvelope:
    defaults = {
        "source_system": "test.db",
        "retrieval_method": RetrievalMethod.DIRECT_QUERY,
        "consistency": ConsistencyGuarantee.STRONG,
        "precision": PrecisionClass.EXACT,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "staleness_window_sec": 3600.0,
    }
    return ProvenanceEnvelope(**(defaults | overrides))


class TestContextCompiler:
    def test_groups_elements_into_correct_slot_types(self) -> None:
        compiler = ContextCompiler()
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.STRUCTURED,
            data={"id": 1},
            provenance=_make_provenance(),
            source_intent_id="i-1",
        ))
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.TEMPORAL,
            data={"ts": "2024-01-01"},
            provenance=_make_provenance(),
            source_intent_id="i-2",
        ))
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.STRUCTURED,
            data={"id": 2},
            provenance=_make_provenance(),
            source_intent_id="i-1",
        ))

        frame = compiler.compile()
        assert len(frame.slots) == 2
        slot_types = {s.type for s in frame.slots}
        assert ContextSlotType.STRUCTURED in slot_types
        assert ContextSlotType.TEMPORAL in slot_types

    def test_computes_stats_correctly(self) -> None:
        compiler = ContextCompiler()
        for _ in range(3):
            compiler.add_element(CompilerInput(
                slot_type=ContextSlotType.STRUCTURED,
                data={"x": 1},
                provenance=_make_provenance(),
                source_intent_id="i-1",
            ))

        frame = compiler.compile()
        assert frame.stats.total_elements == 3
        assert frame.stats.avg_trust_score > 0
        assert "STRUCTURED" in frame.stats.slot_counts

    def test_handles_empty_input(self) -> None:
        compiler = ContextCompiler()
        frame = compiler.compile()
        assert frame.stats.total_elements == 0
        assert frame.stats.avg_trust_score == 0.0
        assert len(frame.slots) == 0
        assert len(frame.conflicts) == 0

    def test_produces_valid_context_frame(self) -> None:
        compiler = ContextCompiler()
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.STRUCTURED,
            data={"name": "Alice"},
            provenance=_make_provenance(),
            source_intent_id="i-1",
            entity_key="customer:C-1042",
        ))
        frame = compiler.compile()
        assert frame.assembled_at is not None
        assert len(frame.slots) == 1
        assert frame.slots[0].elements[0].entity_key == "customer:C-1042"

    def test_reset_clears_state(self) -> None:
        compiler = ContextCompiler()
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.STRUCTURED,
            data={"x": 1},
            provenance=_make_provenance(),
            source_intent_id="i-1",
        ))
        compiler.reset()
        frame = compiler.compile()
        assert frame.stats.total_elements == 0

    def test_interpretation_notes_set_per_slot_type(self) -> None:
        compiler = ContextCompiler()
        compiler.add_element(CompilerInput(
            slot_type=ContextSlotType.TEMPORAL,
            data={"ts": "now"},
            provenance=_make_provenance(),
            source_intent_id="i-1",
        ))
        frame = compiler.compile()
        assert frame.slots[0].interpretation_notes != ""
