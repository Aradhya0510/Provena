"""
Intent types represent WHAT the agent wants to know, never HOW to retrieve it.
Each intent type maps to a different ontological category of question.

All models use Pydantic v2 for combined type safety + runtime validation.
LLM-generated intents WILL be malformed sometimes — Pydantic catches that.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, model_validator


class IntentType(StrEnum):
    POINT_LOOKUP = "point_lookup"
    TEMPORAL_TREND = "temporal_trend"
    AGGREGATE_ANALYSIS = "aggregate_analysis"
    GRAPH_TRAVERSAL = "graph_traversal"
    SEMANTIC_SEARCH = "semantic_search"
    ONTOLOGY_QUERY = "ontology_query"
    COMPOSITE = "composite"
    ESCAPE_HATCH = "escape_hatch"


# ── Supporting models ──


class TimeWindow(BaseModel):
    """At least one of start, end, or relative must be provided."""

    start: str | None = None
    end: str | None = None
    relative: str | None = None

    @model_validator(mode="after")
    def at_least_one_field(self) -> TimeWindow:
        if not any([self.start, self.end, self.relative]):
            raise ValueError("TimeWindow must have at least one of: start, end, relative")
        return self


class FilterClause(BaseModel):
    field: str = Field(min_length=1)
    operator: Literal["eq", "neq", "gt", "gte", "lt", "lte", "in", "contains", "exists"]
    value: Any


class MeasureSpec(BaseModel):
    field: str = Field(min_length=1)
    aggregation: Literal[
        "sum", "avg", "min", "max", "count", "count_distinct", "p50", "p95", "p99"
    ]
    alias: str | None = None


class OrderSpec(BaseModel):
    field: str
    direction: Literal["asc", "desc"]


class NodeSpec(BaseModel):
    type: str | None = None
    identifier: dict[str, str | int] | None = None
    filters: list[FilterClause] | None = None


class FusionOperator(StrEnum):
    INTERSECT = "intersect"
    UNION = "union"
    SEQUENCE = "sequence"
    LEFT_JOIN = "left_join"
    CROSS_ENRICH = "cross_enrich"


# ── Intent models ──


class BaseIntent(BaseModel):
    """Base fields shared by all intents."""

    id: str = Field(min_length=1)
    max_results: int | None = Field(default=None, gt=0)
    budget_ms: int | None = Field(default=None, gt=0)
    priority: float | None = None


class PointLookupIntent(BaseIntent):
    """Retrieve current state of a specific entity by identifier."""

    type: Literal["point_lookup"] = "point_lookup"
    entity: str = Field(min_length=1)
    identifier: dict[str, str | int]
    fields: list[str] | None = None


class TemporalTrendIntent(BaseIntent):
    """Retrieve change patterns over a time window."""

    type: Literal["temporal_trend"] = "temporal_trend"
    entity: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    window: TimeWindow
    granularity: str | None = None
    filters: list[FilterClause] | None = None
    direction: Literal["rising", "falling", "any"] | None = None
    join_key: str | None = None


class AggregateAnalysisIntent(BaseIntent):
    """Retrieve statistical summaries across dimensions."""

    type: Literal["aggregate_analysis"] = "aggregate_analysis"
    entity: str = Field(min_length=1)
    measures: list[MeasureSpec] = Field(min_length=1)
    dimensions: list[str] = Field(min_length=1)
    filters: list[FilterClause] | None = None
    order_by: list[OrderSpec] | None = None
    having: list[FilterClause] | None = None


class GraphTraversalIntent(BaseIntent):
    """Retrieve entity relationships within depth and filter constraints."""

    type: Literal["graph_traversal"] = "graph_traversal"
    start_node: NodeSpec
    edge_types: list[str] | None = None
    max_depth: int = Field(ge=1, le=10)
    direction: Literal["outbound", "inbound", "both"] | None = None
    node_filters: list[FilterClause] | None = None
    return_paths: bool = False


class SemanticSearchIntent(BaseIntent):
    """Retrieve information by meaning similarity."""

    type: Literal["semantic_search"] = "semantic_search"
    query: str = Field(min_length=1)
    collection: str = Field(min_length=1)
    filters: list[FilterClause] | None = None
    hybrid_weight: float | None = Field(default=None, ge=0.0, le=1.0)
    rerank: bool = False


class OntologyQueryIntent(BaseIntent):
    """Retrieve entailments and class-based inferences."""

    type: Literal["ontology_query"] = "ontology_query"
    subject: str | None = None
    predicate: str | None = None
    object: str | None = None
    inference_depth: int | None = Field(default=None, ge=0)
    include_entailments: bool = True


class CompositeIntent(BaseIntent):
    """Combines sub-intents with fusion operators."""

    type: Literal["composite"] = "composite"
    sub_intents: list[Intent]
    fusion_operator: FusionOperator
    fusion_key: str | None = None


class EscapeHatchIntent(BaseIntent):
    """Bypass for queries that don't fit the type system."""

    type: Literal["escape_hatch"] = "escape_hatch"
    target_connector: str
    raw_parameters: dict[str, Any]
    description: str


# ── Discriminated union ──

Intent = Annotated[
    Union[
        PointLookupIntent,
        TemporalTrendIntent,
        AggregateAnalysisIntent,
        GraphTraversalIntent,
        SemanticSearchIntent,
        OntologyQueryIntent,
        CompositeIntent,
        EscapeHatchIntent,
    ],
    Field(discriminator="type"),
]

CompositeIntent.model_rebuild()


def validate_intent(data: dict[str, Any]) -> BaseIntent:
    """
    Validate raw dict (e.g. from LLM output) into a typed Intent.
    Raises pydantic.ValidationError on invalid data.
    """
    from pydantic import TypeAdapter

    adapter = TypeAdapter(Intent)
    return adapter.validate_python(data)
