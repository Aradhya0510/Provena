"""Tests for IntentDecomposer."""

from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.types.intent import (
    CompositeIntent,
    FusionOperator,
    PointLookupIntent,
    TemporalTrendIntent,
    TimeWindow,
)


class TestIntentDecomposer:
    def test_atomic_intent_returns_single_list(self) -> None:
        decomposer = IntentDecomposer()
        intent = PointLookupIntent(
            id="i-1", entity="customer", identifier={"id": "C-1"}
        )
        result = decomposer.decompose(intent)
        assert len(result) == 1
        assert result[0].id == "i-1"

    def test_flattens_composite(self) -> None:
        decomposer = IntentDecomposer()
        sub1 = PointLookupIntent(id="s-1", entity="customer", identifier={"id": "C-1"})
        sub2 = PointLookupIntent(id="s-2", entity="order", identifier={"id": "O-1"})
        composite = CompositeIntent(
            id="c-1",
            sub_intents=[sub1, sub2],
            fusion_operator=FusionOperator.INTERSECT,
        )
        result = decomposer.decompose(composite)
        assert len(result) == 2
        assert result[0].id == "s-1"
        assert result[1].id == "s-2"

    def test_flattens_nested_composites(self) -> None:
        decomposer = IntentDecomposer()
        sub1 = PointLookupIntent(id="s-1", entity="a", identifier={"id": "1"})
        sub2 = PointLookupIntent(id="s-2", entity="b", identifier={"id": "2"})
        sub3 = PointLookupIntent(id="s-3", entity="c", identifier={"id": "3"})
        inner = CompositeIntent(
            id="inner",
            sub_intents=[sub1, sub2],
            fusion_operator=FusionOperator.UNION,
        )
        outer = CompositeIntent(
            id="outer",
            sub_intents=[inner, sub3],
            fusion_operator=FusionOperator.INTERSECT,
        )
        result = decomposer.decompose(outer)
        assert len(result) == 3

    def test_sequence_creates_dependencies(self) -> None:
        decomposer = IntentDecomposer()
        sub1 = PointLookupIntent(id="s-1", entity="a", identifier={"id": "1"})
        sub2 = PointLookupIntent(id="s-2", entity="b", identifier={"id": "2"})
        sub3 = PointLookupIntent(id="s-3", entity="c", identifier={"id": "3"})
        deps = decomposer.analyze_dependencies(
            [sub1, sub2, sub3], "sequence"
        )
        assert deps["s-1"] == []
        assert deps["s-2"] == ["s-1"]
        assert deps["s-3"] == ["s-2"]

    def test_non_sequence_no_dependencies(self) -> None:
        decomposer = IntentDecomposer()
        sub1 = PointLookupIntent(id="s-1", entity="a", identifier={"id": "1"})
        sub2 = PointLookupIntent(id="s-2", entity="b", identifier={"id": "2"})
        deps = decomposer.analyze_dependencies([sub1, sub2], "intersect")
        assert deps["s-1"] == []
        assert deps["s-2"] == []
