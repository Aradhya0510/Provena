"""
Example: "Show me customers likely to churn who also have
unresolved support tickets and declining usage trends"

Spans OLAP (churn scores), OLTP (tickets), time-series (usage).
"""

import asyncio

from sdol import (
    SDOL,
    CapabilityRegistry,
    ContextCompiler,
    GenericOLAPConnector,
    GenericOLTPConnector,
    SemanticRouter,
    TrustScorer,
)
from sdol.connectors.executor import MockQueryExecutor
from sdol.core.provenance.trust_scorer import TrustScorerConfig
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner


async def main() -> None:
    olap_executor = MockQueryExecutor(records=[
        {"customer_id": "C-1042", "churn_probability": 0.89, "region": "west"},
        {"customer_id": "C-2091", "churn_probability": 0.76, "region": "east"},
    ])
    oltp_executor = MockQueryExecutor(records=[
        {"customer_id": "C-1042", "ticket_id": "T-501", "status": "unresolved"},
    ])

    registry = CapabilityRegistry()
    registry.register(GenericOLAPConnector(executor=olap_executor))
    registry.register(GenericOLTPConnector(executor=oltp_executor))

    trust_config = TrustScorerConfig(source_authority_map={
        "snowflake.analytics": 0.95,
        "postgres.production": 0.9,
    })
    compiler = ContextCompiler(TrustScorer(trust_config))
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    router = SemanticRouter(planner, compiler, registry)

    sdol = SDOL(router)

    intent = sdol.formulator.composite(
        sub_intents=[
            sdol.formulator.aggregate_analysis(
                entity="customer_churn_scores",
                measures=[{"field": "churn_probability", "aggregation": "max"}],
                dimensions=["customer_id", "region"],
                having=[{"field": "churn_probability", "operator": "gt", "value": 0.7}],
            ),
            sdol.formulator.point_lookup("support_tickets", {"status": "unresolved"}),
        ],
        fusion_operator="intersect",
        fusion_key="customer_id",
    )

    frame = await sdol.query(intent)

    print("=== Context Frame Stats ===")
    print(f"  Elements: {frame.stats.total_elements}")
    print(f"  Avg trust: {frame.stats.avg_trust_score:.2f}")
    print(f"  Slots: {frame.stats.slot_counts}")
    print(f"  Conflicts: {len(frame.conflicts)}")
    print()
    print("=== Epistemic Context ===")
    print(sdol.get_epistemic_context())


if __name__ == "__main__":
    asyncio.run(main())
