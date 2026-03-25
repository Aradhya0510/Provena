# Databricks Integration Guide

SDOL ships with two Databricks-native typed connectors that let your agent query the Databricks Lakehouse without writing SQL or managing connections — while automatically tracking provenance, consistency, and trust.

| Connector | Paradigm | Backend | Intent Types | Latency Profile |
|-----------|----------|---------|-------------|-----------------|
| `DatabricksDBSQLConnector` | OLAP | SQL Warehouse (Photon) | `aggregate_analysis`, `temporal_trend` | ~300ms |
| `DatabricksLakebaseConnector` | OLTP | Lakebase | `point_lookup`, `aggregate_analysis` | ~10ms |

Both support **Unity Catalog** three-level namespacing and generate Databricks-native SQL with platform-specific optimizations.

These are **provider extensions** of the paradigm base classes — `DatabricksDBSQLConnector` extends `BaseOLAPConnector` and `DatabricksLakebaseConnector` extends `BaseOLTPConnector`. They live under their paradigm directories (`connectors/olap/` and `connectors/oltp/`) rather than a vendor-specific directory.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [DBSQL Connector (OLAP)](#dbsql-connector-olap)
   - [Aggregate Analysis](#aggregate-analysis)
   - [Temporal Trend Analysis](#temporal-trend-analysis)
   - [Query Builder Features](#dbsql-query-builder-features)
3. [Lakebase Connector (OLTP)](#lakebase-connector-oltp)
   - [Point Lookup](#point-lookup)
   - [Batch Lookups](#batch-lookups)
   - [Simple Aggregates](#simple-aggregates)
4. [Unity Catalog Integration](#unity-catalog-integration)
5. [Cross-Paradigm Queries: DBSQL + Lakebase](#cross-paradigm-queries-dbsql--lakebase)
6. [Writing a Custom QueryExecutor](#writing-a-custom-queryexecutor)
   - [DBSQL Executor](#dbsql-executor-via-databricks-sql-connector)
   - [Lakebase Executor](#lakebase-executor-via-rest-api)
   - [Wiring Executors to Connectors](#wiring-executors-to-connectors)
7. [Provenance and Trust](#provenance-and-trust)
8. [Optimization Details](#optimization-details)

---

## Architecture Overview

```
                       ┌─────────────────────────┐
                       │      Agent / LLM         │
                       └────────────┬─────────────┘
                                    │ Intent
                       ┌────────────▼─────────────┐
                       │         SDOL SDK          │
                       │   (IntentFormulator +     │
                       │    SemanticRouter)         │
                       └────────────┬─────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │  DatabricksDBSQL  │           │ DatabricksLakebase│
        │  Connector (OLAP) │           │ Connector (OLTP)  │
        ├───────────────────┤           ├───────────────────┤
        │ • Photon accel.   │           │ • Row-level index │
        │ • Delta skipping  │           │ • Sub-10ms lookup │
        │ • DATE_TRUNC      │           │ • Batch support   │
        │ • Named params    │           │ • Named params    │
        └────────┬──────────┘           └────────┬──────────┘
                 │                                │
                 ▼                                ▼
        ┌────────────────┐              ┌────────────────┐
        │ SQL Warehouse  │              │   Lakebase     │
        │ (Databricks)   │              │  (Databricks)  │
        └────────────────┘              └────────────────┘
                 │                                │
                 └────────────┬───────────────────┘
                              ▼
                     ┌────────────────┐
                     │   Delta Lake   │
                     │ (Unity Catalog)│
                     └────────────────┘
```

The Semantic Router automatically picks the right connector for each intent. `aggregate_analysis` and `temporal_trend` route to DBSQL; `point_lookup` routes to Lakebase. When you issue a composite query spanning both paradigms, each sub-intent goes to the best-suited backend.

---

## DBSQL Connector (OLAP)

`DatabricksDBSQLConnector` targets Databricks SQL warehouses for high-throughput analytical queries with Photon acceleration.

### Aggregate Analysis

```python
import asyncio
from sdol import (
    SDOL, CapabilityRegistry, ContextCompiler,
    DatabricksDBSQLConnector, SemanticRouter, TrustScorer,
)
from sdol.connectors.executor import MockQueryExecutor
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner

async def main():
    executor = MockQueryExecutor(records=[
        {"region": "west", "sum_revenue": 2_500_000},
        {"region": "east", "sum_revenue": 1_800_000},
        {"region": "central", "sum_revenue": 900_000},
    ])

    dbsql = DatabricksDBSQLConnector(
        executor=executor,
        connector_id="databricks.analytics",
        source_system="databricks.sql_warehouse.prod",
        available_entities=["orders", "customers", "revenue_daily"],
        catalog="prod_catalog",
        schema="analytics",
    )

    registry = CapabilityRegistry()
    registry.register(dbsql)

    compiler = ContextCompiler(TrustScorer())
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    router = SemanticRouter(planner, compiler, registry)
    sdol = SDOL(router)

    intent = sdol.formulator.aggregate_analysis(
        entity="orders",
        measures=[{"field": "revenue", "aggregation": "sum"}],
        dimensions=["region"],
        filters=[{"field": "status", "operator": "eq", "value": "active"}],
        order_by=[{"field": "sum_revenue", "direction": "desc"}],
    )

    frame = await sdol.query(intent)

    for slot in frame.slots:
        for elem in slot.elements:
            print(f"  {elem.data}")
            print(f"    Trust: {elem.trust.composite:.2f}")
            print(f"    Source: {elem.provenance.source_system}")

asyncio.run(main())
```

Generated SQL:

```sql
SELECT region, SUM(revenue) AS sum_revenue
FROM prod_catalog.analytics.orders
WHERE status = :p0
GROUP BY region
ORDER BY sum_revenue DESC
```

### Temporal Trend Analysis

```python
intent = sdol.formulator.temporal_trend(
    entity="api_metrics",
    metric="request_count",
    window={"relative": "last_90d"},
    granularity="1d",
)
frame = await sdol.query(intent)
```

Generated SQL uses Databricks-native `DATE_TRUNC`:

```sql
SELECT DATE_TRUNC('DAY', timestamp) AS bucket, AVG(request_count) AS request_count
FROM prod_catalog.analytics.api_metrics
WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL last_90d
GROUP BY bucket
ORDER BY bucket
```

Supported granularities: `1h` (HOUR), `1d` (DAY), `1w` (WEEK), `1M` (MONTH), `1Q` (QUARTER), `1y` (YEAR).

### DBSQL Query Builder Features

| Feature | Description |
|---------|------------|
| **Named parameters** | `:p0`, `:p1`, ... — native to Databricks SQL, avoids positional ambiguity |
| **`PERCENTILE_APPROX`** | Used for p50/p95/p99 instead of `PERCENTILE_CONT` (Photon-optimized) |
| **`COUNT(DISTINCT ...)`** | Properly expanded for `count_distinct` aggregation |
| **Unity Catalog** | Three-level `catalog.schema.table` when configured |
| **Optimization tracking** | `photon_acceleration`, `predicate_pushdown`, `delta_data_skipping`, `pushdown_aggregation`, `delta_caching` |

---

## Lakebase Connector (OLTP)

`DatabricksLakebaseConnector` targets Lakebase for sub-10ms row-level serving on Delta Lake tables with automatic indexing.

### Point Lookup

```python
import asyncio
from sdol import (
    SDOL, CapabilityRegistry, ContextCompiler,
    DatabricksLakebaseConnector, SemanticRouter, TrustScorer,
)
from sdol.connectors.executor import MockQueryExecutor
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner

async def main():
    executor = MockQueryExecutor(records=[
        {"customer_id": "C-1042", "name": "Alice", "tier": "enterprise",
         "last_login": "2025-03-24T08:15:00Z"},
    ])

    lakebase = DatabricksLakebaseConnector(
        executor=executor,
        connector_id="databricks.serving",
        source_system="databricks.lakebase.prod",
        available_entities=["customers", "products", "sessions"],
        catalog="prod_catalog",
        schema="serving",
    )

    registry = CapabilityRegistry()
    registry.register(lakebase)

    compiler = ContextCompiler(TrustScorer())
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    router = SemanticRouter(planner, compiler, registry)
    sdol = SDOL(router)

    intent = sdol.formulator.point_lookup(
        "customers",
        {"customer_id": "C-1042"},
        fields=["name", "tier", "last_login"],
    )
    frame = await sdol.query(intent)

    for slot in frame.slots:
        for elem in slot.elements:
            print(f"Customer: {elem.data}")
            print(f"  Precision: {elem.provenance.precision}")
            print(f"  Consistency: {elem.provenance.consistency}")
            print(f"  Staleness: {elem.provenance.staleness_window_sec}s")

asyncio.run(main())
```

Generated SQL:

```sql
SELECT name, tier, last_login
FROM prod_catalog.serving.customers
WHERE customer_id = :p0
```

Query builder optimizations: `lakebase_row_index`, `column_pruning`, `parameterized_query`.

### Batch Lookups

The query builder supports batch lookups for multi-key retrieval:

```python
from sdol.connectors.oltp.databricks_lakebase_query import build_lakebase_batch_lookup

query = build_lakebase_batch_lookup(
    entity="customers",
    id_field="customer_id",
    ids=["C-1042", "C-2091", "C-3150"],
    fields=["name", "tier"],
    catalog="prod_catalog",
    schema="serving",
)
```

Generated SQL:

```sql
SELECT name, tier
FROM prod_catalog.serving.customers
WHERE customer_id IN (:p0, :p1, :p2)
```

### Simple Aggregates

Lakebase also handles simple `aggregate_analysis` intents for lightweight counts and sums that don't need a full SQL warehouse:

```python
intent = sdol.formulator.aggregate_analysis(
    entity="orders",
    measures=[{"field": "total", "aggregation": "count"}],
    dimensions=["status"],
)
```

---

## Unity Catalog Integration

Both connectors accept `catalog` and `schema` at construction time for Unity Catalog three-level namespacing:

```python
dbsql = DatabricksDBSQLConnector(
    executor=executor,
    catalog="prod_catalog",
    schema="analytics",
    available_entities=["orders", "revenue_daily"],
)

lakebase = DatabricksLakebaseConnector(
    executor=executor,
    catalog="prod_catalog",
    schema="serving",
    available_entities=["customers", "products"],
)
```

All generated queries use fully-qualified table names:

```sql
-- DBSQL
SELECT region, SUM(revenue) AS sum_revenue FROM prod_catalog.analytics.orders ...

-- Lakebase
SELECT name, tier FROM prod_catalog.serving.customers ...
```

If `catalog` and `schema` are omitted, queries use bare table names — useful when the default catalog/schema is set at the connection level.

---

## Cross-Paradigm Queries: DBSQL + Lakebase

Register both connectors and let SDOL's Semantic Router handle intent routing automatically.

```python
import asyncio
from sdol import (
    SDOL, CapabilityRegistry, ContextCompiler,
    DatabricksDBSQLConnector, DatabricksLakebaseConnector,
    SemanticRouter, TrustScorer,
)
from sdol.connectors.executor import MockQueryExecutor
from sdol.core.provenance.trust_scorer import TrustScorerConfig
from sdol.core.router.cost_estimator import CostEstimator
from sdol.core.router.intent_decomposer import IntentDecomposer
from sdol.core.router.query_planner import QueryPlanner

async def main():
    dbsql_exec = MockQueryExecutor(records=[
        {"customer_id": "C-1042", "churn_score": 0.89, "region": "west"},
        {"customer_id": "C-2091", "churn_score": 0.76, "region": "east"},
    ])
    lakebase_exec = MockQueryExecutor(records=[
        {"customer_id": "C-1042", "name": "Alice", "tier": "enterprise"},
    ])

    dbsql = DatabricksDBSQLConnector(
        executor=dbsql_exec,
        connector_id="databricks.analytics",
        source_system="databricks.sql_warehouse.prod",
        available_entities=["churn_scores", "revenue_metrics"],
        catalog="prod_catalog",
        schema="ml_features",
    )
    lakebase = DatabricksLakebaseConnector(
        executor=lakebase_exec,
        connector_id="databricks.serving",
        source_system="databricks.lakebase.prod",
        available_entities=["customers", "products"],
        catalog="prod_catalog",
        schema="serving",
    )

    registry = CapabilityRegistry()
    registry.register(dbsql)
    registry.register(lakebase)

    trust_config = TrustScorerConfig(source_authority_map={
        "databricks.sql_warehouse.prod": 0.95,
        "databricks.lakebase.prod": 0.90,
    })

    compiler = ContextCompiler(TrustScorer(trust_config))
    planner = QueryPlanner(registry, IntentDecomposer(), CostEstimator())
    router = SemanticRouter(planner, compiler, registry)
    sdol = SDOL(router)

    intent = sdol.formulator.composite(
        sub_intents=[
            sdol.formulator.aggregate_analysis(
                entity="churn_scores",
                measures=[{"field": "churn_score", "aggregation": "max"}],
                dimensions=["customer_id", "region"],
                having=[{"field": "churn_score", "operator": "gt", "value": 0.7}],
            ),
            sdol.formulator.point_lookup(
                "customers", {"customer_id": "C-1042"}
            ),
        ],
        fusion_operator="left_join",
        fusion_key="customer_id",
    )

    frame = await sdol.query(intent)

    print(f"Elements: {frame.stats.total_elements}")
    print(f"Avg trust: {frame.stats.avg_trust_score:.2f}")
    for slot in frame.slots:
        print(f"\n  [{slot.type}] — {slot.interpretation_notes}")
        for elem in slot.elements:
            print(f"    {elem.data}")
            print(f"    Source: {elem.provenance.source_system}")

    print(f"\n{sdol.get_epistemic_context()}")

asyncio.run(main())
```

The router automatically sends:
- `aggregate_analysis` intent to **DBSQL** (supports aggregation, windowing, temporal bucketing)
- `point_lookup` intent to **Lakebase** (lowest latency, row-index optimization)

---

## Writing a Custom QueryExecutor

`MockQueryExecutor` is great for development. For production, implement the `QueryExecutor` protocol — a single `async execute(query) -> dict` method returning `{"records": [...], "meta": {...}}`.

### DBSQL Executor (via `databricks-sql-connector`)

```python
from databricks import sql as dbsql

class DatabricksDBSQLExecutor:
    def __init__(self, host: str, http_path: str, access_token: str) -> None:
        self._host = host
        self._http_path = http_path
        self._token = access_token

    async def execute(self, query) -> dict:
        connection = dbsql.connect(
            server_hostname=self._host,
            http_path=self._http_path,
            access_token=self._token,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(query.sql, parameters=query.parameters)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            records = [dict(zip(columns, row)) for row in rows]
            return {
                "records": records,
                "meta": {
                    "native_query": query.sql,
                    "total_available": cursor.rowcount,
                },
            }
        finally:
            connection.close()
```

### Lakebase Executor (via REST API)

```python
import httpx

class DatabricksLakebaseExecutor:
    def __init__(self, workspace_url: str, access_token: str) -> None:
        self._base_url = workspace_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {access_token}"}

    async def execute(self, query) -> dict:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base_url}/api/2.0/sql/statements",
                headers=self._headers,
                json={
                    "statement": query.sql,
                    "parameters": [
                        {"name": k, "value": str(v)}
                        for k, v in query.parameters.items()
                    ],
                    "wait_timeout": "30s",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            columns = [col["name"] for col in data["manifest"]["schema"]["columns"]]
            rows = data.get("result", {}).get("data_array", [])
            records = [dict(zip(columns, row)) for row in rows]
            return {
                "records": records,
                "meta": {
                    "native_query": query.sql,
                    "total_available": len(records),
                },
            }
```

### Wiring Executors to Connectors

```python
dbsql = DatabricksDBSQLConnector(
    executor=DatabricksDBSQLExecutor(
        host="your-workspace.databricks.com",
        http_path="/sql/1.0/warehouses/your-warehouse-id",
        access_token="dapi...",
    ),
    catalog="prod_catalog",
    schema="analytics",
    available_entities=["orders", "revenue_daily"],
)

lakebase = DatabricksLakebaseConnector(
    executor=DatabricksLakebaseExecutor(
        workspace_url="https://your-workspace.databricks.com",
        access_token="dapi...",
    ),
    catalog="prod_catalog",
    schema="serving",
    available_entities=["customers", "products"],
)
```

---

## Provenance and Trust

Every result from a Databricks connector carries full provenance metadata that feeds into trust scoring and epistemic tracking.

| Dimension | DBSQL (OLAP) | Lakebase (OLTP) |
|-----------|-------------|----------------|
| **Source System** | `databricks.sql_warehouse` | `databricks.lakebase` |
| **Retrieval Method** | `computed_aggregate` | `direct_query` / `computed_aggregate` |
| **Consistency** | `strong` | `read_committed` |
| **Precision** | `exact_aggregate` | `exact` (lookup) / `exact_aggregate` |
| **Staleness Window** | 600s | 30s |
| **Max Cardinality** | 10,000,000 | 10,000 |

Configure trust scoring authority for your Databricks environments:

```python
from sdol.core.provenance.trust_scorer import TrustScorer, TrustScorerConfig

trust_config = TrustScorerConfig(source_authority_map={
    "databricks.sql_warehouse.prod": 0.95,
    "databricks.lakebase.prod": 0.90,
    "databricks.sql_warehouse.staging": 0.70,
})
scorer = TrustScorer(trust_config)
```

---

## Optimization Details

### DBSQL Query Builder (`DBSQLQuery`)

The `DBSQLQuery` dataclass tracks which optimizations were applied:

| Field | Meaning |
|-------|---------|
| `uses_photon` | Query eligible for Photon acceleration |
| `uses_delta_data_skipping` | WHERE clauses enable Delta file-level skipping |
| `uses_liquid_clustering` | Table uses liquid clustering (reserved for future use) |
| `catalog` / `schema` | Unity Catalog namespace context |
| `optimizations` | List of applied strategies: `photon_acceleration`, `predicate_pushdown`, `delta_data_skipping`, `pushdown_aggregation`, `order_pushdown`, `delta_caching` |

### Lakebase Query Builder (`LakebaseQuery`)

| Field | Meaning |
|-------|---------|
| `uses_row_index` | Point lookup uses Lakebase's automatic row-level index |
| `is_batch` | Multi-key IN query for batch lookups |
| `optimizations` | `lakebase_row_index`, `parameterized_query`, `column_pruning`, `batch_lookup`, `filter_pushdown` |
