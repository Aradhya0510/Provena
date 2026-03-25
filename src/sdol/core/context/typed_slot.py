"""Interpretation notes per slot type."""

from sdol.types.context import ContextSlotType

INTERPRETATION_NOTES: dict[ContextSlotType, str] = {
    ContextSlotType.STRUCTURED: (
        "Tabular data. Numbers are precise within query scope. "
        "NULLs mean absent, not unknown. Trust provenance precision class for confidence."
    ),
    ContextSlotType.RELATIONAL: (
        "Relationship data. Absence of an edge means not-found, not does-not-exist. "
        "Check traversal depth limits before assuming completeness."
    ),
    ContextSlotType.TEMPORAL: (
        "Time-series data. Always check window and granularity. "
        "Trends are computed within the stated window — do not extrapolate beyond it."
    ),
    ContextSlotType.UNSTRUCTURED: (
        "Natural language text. Apply standard NLP-level caution. "
        "Claims are assertions, not verified facts. Cross-reference with structured data."
    ),
    ContextSlotType.INFERRED: (
        "Derived by formal reasoning. Validity depends on ontology correctness. "
        "Uses open-world assumption: absence of entailment is not negation."
    ),
}
