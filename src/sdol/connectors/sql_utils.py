"""Shared SQL utilities for typed connectors and query builders."""

from __future__ import annotations

from typing import Any


OPERATOR_MAP: dict[str, str] = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
    "contains": "LIKE",
    "exists": "IS NOT NULL",
}


def qualify_table(
    entity: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> str:
    """Build a qualified table name (e.g. catalog.schema.table) when parts are available."""
    if catalog and schema:
        return f"{catalog}.{schema}.{entity}"
    if schema:
        return f"{schema}.{entity}"
    return entity


def extract_entity_keys(
    records: list[dict[str, Any]],
    key_candidates: tuple[str, ...] = ("customer_id", "id", "entity_id"),
) -> list[str] | None:
    """Extract entity key values from the first record's matching key column."""
    if not records or not isinstance(records[0], dict):
        return None
    for key in key_candidates:
        if key in records[0]:
            return [str(r.get(key, "")) for r in records]
    return None
