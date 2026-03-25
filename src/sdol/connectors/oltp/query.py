"""Transactional query builder for generic OLTP systems."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sdol.connectors.sql_utils import OPERATOR_MAP
from sdol.types.intent import AggregateAnalysisIntent, FilterClause, PointLookupIntent


@dataclass
class OLTPQuery:
    """Native query representation for OLTP systems."""

    sql: str
    params: list[Any]
    optimizations: list[str]
    is_batch: bool


def build_point_lookup_query(intent: PointLookupIntent) -> OLTPQuery:
    """Build a point lookup query with field selection optimization."""
    params: list[Any] = []
    optimizations: list[str] = ["parameterized_query"]

    if intent.fields:
        select_clause = ", ".join(intent.fields)
        optimizations.append("index_aware_field_selection")
    else:
        select_clause = "*"

    where_parts: list[str] = []
    for key, value in intent.identifier.items():
        params.append(value)
        where_parts.append(f"{key} = ${len(params)}")

    where_clause = " AND ".join(where_parts)
    sql = f"SELECT {select_clause} FROM {intent.entity} WHERE {where_clause}"

    if intent.max_results:
        sql += f" LIMIT {intent.max_results}"

    return OLTPQuery(
        sql=sql,
        params=params,
        optimizations=optimizations,
        is_batch=False,
    )


def build_batch_lookup_query(
    entity: str,
    id_field: str,
    ids: list[str | int],
    fields: list[str] | None = None,
) -> OLTPQuery:
    """Convert multiple point lookups into single IN query."""
    params: list[Any] = list(ids)
    optimizations = ["parameterized_query", "batch_lookup"]

    select_clause = ", ".join(fields) if fields else "*"
    placeholders = ", ".join([f"${i + 1}" for i in range(len(ids))])
    sql = f"SELECT {select_clause} FROM {entity} WHERE {id_field} IN ({placeholders})"

    return OLTPQuery(
        sql=sql,
        params=params,
        optimizations=optimizations,
        is_batch=True,
    )


def build_simple_aggregate_query(intent: AggregateAnalysisIntent) -> OLTPQuery:
    """Build a simple aggregate query suitable for OLTP systems."""
    params: list[Any] = []
    optimizations = ["parameterized_query"]

    agg_parts: list[str] = []
    for m in intent.measures:
        alias = m.alias or f"{m.aggregation}_{m.field}"
        agg_parts.append(f"{m.aggregation.upper()}({m.field}) AS {alias}")

    select_clause = ", ".join(intent.dimensions + agg_parts)

    where_clause = ""
    if intent.filters:
        where_parts: list[str] = []
        for f in intent.filters:
            op = OPERATOR_MAP.get(f.operator, "=")
            params.append(f.value)
            where_parts.append(f"{f.field} {op} ${len(params)}")
        where_clause = " WHERE " + " AND ".join(where_parts)
        optimizations.append("filter_pushdown")

    group_by = ", ".join(intent.dimensions)

    sql = f"SELECT {select_clause} FROM {intent.entity}{where_clause} GROUP BY {group_by}"

    if intent.max_results:
        sql += f" LIMIT {intent.max_results}"

    return OLTPQuery(
        sql=sql,
        params=params,
        optimizations=optimizations,
        is_batch=False,
    )
