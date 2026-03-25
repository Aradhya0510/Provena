"""Decomposes composite intents into atomic sub-intents."""

from __future__ import annotations

from sdol.types.intent import BaseIntent, CompositeIntent, FusionOperator


class IntentDecomposer:
    def decompose(self, intent: BaseIntent) -> list[BaseIntent]:
        """
        If composite, recursively flatten into atomic intents.
        If atomic, return single-element list.
        """
        if not isinstance(intent, CompositeIntent):
            return [intent]

        flattened: list[BaseIntent] = []
        for sub in intent.sub_intents:
            flattened.extend(self.decompose(sub))
        return flattened

    def analyze_dependencies(
        self,
        sub_intents: list[BaseIntent],
        fusion_operator: str,
        fusion_key: str | None = None,
    ) -> dict[str, list[str]]:
        """
        Determine execution dependencies between sub-intents.

        Returns: {intent_id: [list of intent_ids it depends on]}

        Dependencies exist when:
        - fusion_operator is "sequence" (strict ordering)
        - A sub-intent's filters reference another's join_key (scope narrowing)
        """
        deps: dict[str, list[str]] = {sub.id: [] for sub in sub_intents}

        if fusion_operator == FusionOperator.SEQUENCE.value:
            for i in range(1, len(sub_intents)):
                deps[sub_intents[i].id].append(sub_intents[i - 1].id)
            return deps

        if fusion_key:
            providers: list[str] = []
            consumers: list[str] = []
            for sub in sub_intents:
                join_key = getattr(sub, "join_key", None)
                if join_key == fusion_key:
                    providers.append(sub.id)
                else:
                    consumers.append(sub.id)

            for consumer_id in consumers:
                deps[consumer_id] = list(providers)

        return deps
