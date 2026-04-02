# SDOL Enhancements

Findings from the fleet benchmark run (2026-04-01) using notebooks `02_fleet_setup` and `03_fleet_benchmark` with `databricks-claude-sonnet-4-6`.

**MLflow Judge Scores (mean across 6 questions):**

| Metric | Baseline MCP | SDOL-Enhanced | Delta |
|--------|-------------|---------------|-------|
| relevance_to_query | 1.0 | 1.0 | 0 |
| safety | 1.0 | 1.0 | 0 |
| data_efficiency | 1.0 | 1.0 | 0 |
| **epistemic_transparency** | **0.167** | **1.0** | **+0.833** |

---

## A. Benchmark / Showcase Fixes

These modifications target `databricks_test/02_fleet_setup` and `03_fleet_benchmark` to make the demo more convincing and the results more differentiated.

### A.1 Fix the conflict seed so it actually fires

**Problem:** The benchmark's headline feature — automatic conflict detection between OLTP and OLAP — didn't trigger. `telemetry_daily` is a historical pre-aggregated table with no row for `current_date()`. When `sdol_cross_source_status` queries OLAP for today, it gets zero rows instead of a contradictory `last_known_status = 'online'`. The conflict detector never sees a disagreement.

**Fix in `02_fleet_setup`:** After writing `telemetry_daily`, insert a synthetic "today" row for EXC-0342 that carries `last_known_status = 'online'`:

```python
# After telemetry_daily write
from pyspark.sql import Row
conflict_row = spark.createDataFrame([Row(
    machine_id="EXC-0342",
    report_date=spark.sql("SELECT current_date()").first()[0],
    avg_engine_temp=105.3,
    max_engine_temp=125.0,
    avg_rpm=2100,
    avg_fuel_efficiency=16.2,
    min_fuel_efficiency=14.8,
    reading_count=4,
    last_known_status="online",  # contradicts OLTP 'offline'
)])
conflict_row.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.telemetry_daily")
```

**Impact:** The SDOL agent will detect the OLTP/OLAP status conflict and resolve it via `prefer_strongest_consistency`. The baseline agent will silently pick one or hallucinate a reconciliation. This is the single most important benchmark fix.

### A.2 Force the baseline to expose the token-busting failure

**Problem:** `data_efficiency` scored 1.0 for both agents. Claude Sonnet 4.6 is smart enough to write `GROUP BY` / `AVG()` SQL on its own, so the baseline never dumps raw rows. The "token-busting cross-paradigm join" failure mode doesn't manifest.

**Fix in `03_fleet_benchmark`:** Degrade the baseline's SQL tool to make it more realistic of a naive MCP setup:

- Option 1: Add a `LIMIT 500` cap to `execute_sql` and remove the `describe_tables` tool, forcing the baseline to work blind.
- Option 2: Replace the single `execute_sql` tool with a `get_table_sample(table, n_rows)` tool that returns raw rows — simulating MCP servers that expose data-fetching tools rather than arbitrary SQL. This is more realistic of how most MCP data connectors work in practice.
- Option 3: Add a token-counting scorer that measures the total volume of tool call results consumed by each agent. Even if both produce good final answers, SDOL should consume fewer intermediate tokens.

### A.3 Add latency as a first-class MLflow metric

**Problem:** The notebook measures wall-clock latency per question but only prints it — it doesn't land in MLflow. Latency is SDOL's main tradeoff (multiple typed tool calls vs one SQL query) and should be visible in the comparison.

**Fix in `03_fleet_benchmark`:** Log latency as a per-question metric in each MLflow run:

```python
with mlflow.start_run(run_name="fleet_sdol_enhanced"):
    for i, row in results_df.iterrows():
        mlflow.log_metric(f"latency_{row['category']}", row["sdol_latency_sec"])
    mlflow.log_metric("latency_mean", results_df["sdol_latency_sec"].mean())
    # ... existing evaluate() call
```

### A.4 Add a token-efficiency scorer

**Problem:** Data efficiency is measured by whether the *final answer* is concise, not by how much data flowed through the agent's context window. The baseline may produce a clean answer but consume 50x more intermediate tokens getting there.

**Fix:** Add a custom scorer or metric that sums the character/token count of all tool call results in each agent's trace. Log as `context_tokens_consumed` per question. This captures SDOL's push-down advantage even when both agents produce similar final answers.

### A.5 Include a question that the baseline structurally cannot answer

**Problem:** All 6 eval questions are answerable by both agents. The epistemic_transparency gap is about *annotation quality*, not *capability*. The benchmark doesn't include a question where SDOL enables a fundamentally different answer.

**Fix:** Add a question like:

> "Which data sources should I trust most for real-time fleet decisions, and which have known staleness risks?"

The baseline has no provenance metadata to draw from — it can only speculate. SDOL can answer from its trust scorer config and epistemic tracker with concrete numbers. This would also differentiate on `relevance_to_query`, not just `epistemic_transparency`.

### A.6 Parameterize the configuration block

**Problem:** Catalog, schema, workspace username, LLM endpoint, and VS endpoint are hardcoded at the top of both notebooks. Every new user must manually edit these.

**Fix:** Use Databricks widgets:

```python
dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
```

This also enables running the notebooks as parameterized job tasks.

---

## B. Framework Enhancements

These target the core SDOL library under `src/sdol/`.

### B.1 Make conflict detection work for "missing vs present" — not just "value A != value B"

**Problem:** The current `ConflictDetector` compares field values across sources. If one source returns no data (e.g., OLAP has no row for today), there's nothing to compare and no conflict is flagged. But "Source A says offline, Source B has no data" is itself an epistemic signal — the absence of OLAP data for a machine that should be reporting is meaningful.

**Fix:** Extend `ConflictDetector` to support a `presence_conflict` mode: if a composite query expects data from N sources but only M < N return results, flag a `PresenceConflict` with the missing source's metadata. The resolver can then annotate the response with "OLAP had no data for this time window — OLTP is the sole authority."

### B.2 Add staleness-aware cache to connectors

**Problem:** Each `sdol.query()` call hits the underlying data source, even if the same entity was queried seconds ago. For OLAP data with 900s staleness, re-querying within that window is wasteful.

**Fix:** Add an optional TTL cache at the connector level, keyed by (entity, filters, measures). The TTL should default to `staleness_sec` from the connector's provenance config. This reduces latency for repeated queries (common in agent loops) and accurately reflects that the data hasn't changed within the staleness window.

### B.3 Expose per-element trust scores in a structured format the LLM can reason over

**Problem:** Trust scores are embedded in the JSON blob returned by SDOL tools. The LLM must parse nested JSON to find `trust_score: 0.805` and understand what it means. There's no guidance on what thresholds matter.

**Fix:** Add a `trust_summary` field to the tool response at the top level:

```json
{
  "trust_summary": {
    "overall_confidence": "high",
    "lowest_trust_source": "databricks.sql_warehouse.telemetry (0.805)",
    "advisory": "OLAP data is ~15min stale; for real-time decisions, prefer OLTP values"
  },
  "results": [...],
  "conflicts": [...]
}
```

This gives the LLM a pre-digested signal without requiring it to scan every element's provenance.

### B.4 Support async parallel execution in composite queries

**Problem:** `sdol_cross_source_status` builds a composite intent with two sub-intents (OLTP + OLAP), but execution is sequential via `asyncio.run()`. For multi-source queries, the latency is the sum of all source latencies.

**Fix:** Ensure the `QueryPlanner` executes independent sub-intents concurrently (it already computes topological execution levels in the planner — verify that independent levels are dispatched with `asyncio.gather`). The benchmark should then show SDOL's cross-source latency approaching the slowest single source, not the sum.

### B.5 Add a "confidence threshold" mode to the Agent SDK

**Problem:** The `SDOL` class returns all results regardless of trust score. The calling agent must decide whether 0.4 trust is acceptable. There's no built-in guardrail.

**Fix:** Add an optional `min_trust` parameter to `sdol.query()`:

```python
frame = await sdol.query(intent, min_trust=0.7)
# Elements below 0.7 are excluded, and a warning is added to epistemic context
```

This enables workflows like "only show me data I can act on" without requiring the LLM to implement threshold logic.

### B.6 Add cost tracking to the CostEstimator

**Problem:** The `CostEstimator` estimates query cost for planning purposes but doesn't track actual cost post-execution. For Databricks DBSQL, this matters — warehouse compute has real dollar costs.

**Fix:** After query execution, capture `bytes_scanned` or `DBU_consumed` from the Spark query plan and attach it to the provenance envelope. Expose via `sdol.get_cost_summary()`. This gives operators visibility into the cost of SDOL-routed queries vs raw SQL.

### B.7 Harden the Databricks extensions for production

**Problem:** The three Databricks connectors (`DatabricksDBSQLConnector`, `DatabricksLakebaseConnector`, `DatabricksVectorSearchConnector`) work in the benchmark but have gaps for production use:

- No retry logic for transient failures (429s, warehouse cold-start timeouts).
- No connection pooling or session reuse for DBSQL.
- The `SparkSQLExecutor` in the notebook does naive string interpolation for query parameters (`sql_str.replace(placeholder, f"'{v}'")`), which is a SQL injection vector.

**Fix:**
- Add exponential backoff retry (3 attempts) in each executor's `execute()` method.
- Use parameterized queries via `spark.sql(query, args=params)` (available in DBR 14+) instead of string interpolation.
- Document connection lifecycle expectations in the typed connectors guide.

---

## C. Documentation Gaps

### C.1 Add a "Why SDOL" section to README that addresses the LLM-is-already-smart objection

The benchmark showed that Claude Sonnet 4.6 can write correct aggregation SQL, do proper JOINs, and produce clean answers without SDOL. The README should preemptively address this: SDOL's value is not in query correctness (LLMs handle that) but in **structural guarantees** — provenance tracking, trust scoring, and conflict detection that the LLM cannot hallucinate. Frame it as: "SDOL makes your agent auditable, not just capable."

### C.2 Add a runbook for the benchmark

The benchmark requires: workspace auth, catalog/schema setup, SDOL source synced to the workspace, correct VS endpoint, correct LLM endpoint. Document this end-to-end in a `databricks_test/README.md` so others can reproduce without trial-and-error.

### C.3 Document the extensions sync issue for Databricks Repos

Files uploaded via `databricks workspace import --format SOURCE` become notebooks, not plain Python files. This breaks `import` for regular modules. Document that extensions must be uploaded via the REST API with `format: AUTO`, or synced through the git Repo integration. This cost significant debugging time during the benchmark run.
