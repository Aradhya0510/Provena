"""Detects conflicting data across context elements from different sources."""

from __future__ import annotations

from sdol.types.context import (
    ConflictResolution,
    ContextConflict,
    ContextElement,
    ContextSlotType,
)

COMPARABLE_SLOT_TYPES = {ContextSlotType.STRUCTURED, ContextSlotType.TEMPORAL}


class ConflictDetector:
    def detect(
        self,
        elements: list[ContextElement],
        slot_types: dict[str, ContextSlotType] | None = None,
    ) -> list[ContextConflict]:
        """
        Detect conflicts among elements sharing the same entity_key
        but from different source systems.
        """
        by_entity: dict[str, list[ContextElement]] = {}
        for elem in elements:
            if elem.entity_key is None:
                continue
            by_entity.setdefault(elem.entity_key, []).append(elem)

        conflicts: list[ContextConflict] = []
        for _entity_key, group in by_entity.items():
            if len(group) < 2:
                continue

            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    a, b = group[i], group[j]
                    if a.provenance.source_system == b.provenance.source_system:
                        continue

                    field_conflicts = self._compare_data(a, b)
                    conflicts.extend(field_conflicts)

        return conflicts

    def _compare_data(
        self,
        a: ContextElement,
        b: ContextElement,
    ) -> list[ContextConflict]:
        conflicts: list[ContextConflict] = []
        if not isinstance(a.data, dict) or not isinstance(b.data, dict):
            return conflicts

        shared_keys = set(a.data.keys()) & set(b.data.keys())
        for key in shared_keys:
            if a.data[key] != b.data[key]:
                conflicts.append(
                    ContextConflict(
                        element_a=a,
                        element_b=b,
                        field=key,
                        value_a=a.data[key],
                        value_b=b.data[key],
                        resolution=ConflictResolution(
                            strategy="defer_to_agent",
                            winner=None,
                            reason="Unresolved — pending conflict resolution",
                        ),
                    )
                )
        return conflicts
