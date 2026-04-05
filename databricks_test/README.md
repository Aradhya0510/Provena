# Provena Fleet Benchmark

End-to-end benchmark comparing a baseline MCP agent vs a Provena-enhanced agent on a fleet management scenario using Databricks.

## Notebooks

Run in order:

| # | Notebook | Purpose | Duration |
|---|----------|---------|----------|
| 00 | `00_setup_benchmark_resources` | Educational walkthrough of Provena concepts (no side effects) | ~90s |
| 01 | `01_fleet_setup` | Creates tables (fleet_machines, telemetry_readings, telemetry_daily, maintenance_logs) + Vector Search index | ~4 min |
| 02 | `02_baseline_vs_provena_demo` | Interactive demo of each Provena concept against real data | ~90s |
| 03 | `03_fleet_benchmark` | Full benchmark: 7 questions x 2 agents, MLflow scoring, cost/latency metrics | ~12 min |

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog
- Databricks CLI configured with a profile (e.g., `e2-demo-west`)
- Provena source synced to workspace (see below)

### 1. Sync Provena source to workspace

```bash
databricks workspace import-dir --profile <PROFILE> --overwrite \
  src/provena "/Workspace/Users/<email>/SDOL/src/provena"
```

### 2. Run setup

```bash
databricks jobs submit --profile <PROFILE> --no-wait --json '{
  "run_name": "provena_01_fleet_setup",
  "tasks": [{
    "task_key": "fleet_setup",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<email>/SDOL/databricks_test/01_fleet_setup",
      "base_parameters": {
        "catalog": "users",
        "schema": "<your_schema>",
        "vs_endpoint": "<vs_endpoint_name>",
        "use_existing_catalog": "true"
      }
    },
    "environment_key": "Default"
  }],
  "environments": [{"environment_key": "Default", "spec": {"client": "1", "dependencies": []}}]
}'
```

### 3. Run benchmark

```bash
databricks jobs submit --profile <PROFILE> --no-wait --json '{
  "run_name": "provena_03_fleet_benchmark",
  "tasks": [{
    "task_key": "fleet_benchmark",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<email>/SDOL/databricks_test/03_fleet_benchmark",
      "base_parameters": {
        "catalog": "users",
        "schema": "<your_schema>",
        "llm_endpoint": "databricks-claude-sonnet-4-6",
        "vs_endpoint": "<vs_endpoint_name>",
        "provena_project_root": "/Workspace/Users/<email>/SDOL",
        "num_runs": "1",
        "input_price_per_1k_tokens": "0.003",
        "output_price_per_1k_tokens": "0.015"
      }
    },
    "environment_key": "Default"
  }],
  "environments": [{"environment_key": "Default", "spec": {"client": "1", "dependencies": []}}]
}'
```

## What the Benchmark Measures

7 evaluation questions across 7 MLflow judge scorers:

| Question Category | Tests |
|------------------|-------|
| cross_paradigm | OLTP filter + OLAP aggregate + semantic search in one query |
| epistemic_conflict | Detecting OLTP/OLAP data contradiction (EXC-0342 conflict seed) |
| point_lookup | Single-machine OLTP lookup |
| aggregate | Push-down aggregation on telemetry |
| semantic_search | Vector Search over maintenance logs |
| confidence | Meta-question: "How reliable is the data?" |
| trust_meta | Meta-question: "Which sources should I trust?" |

| Scorer | What It Measures |
|--------|-----------------|
| relevance_to_query | Does the answer address the question? |
| safety | No harmful content |
| data_efficiency | Lean, structured data (not raw row dumps) |
| epistemic_transparency | Cites sources, freshness, confidence |
| provenance_completeness | Machine-verifiable provenance chain |
| conflict_detection_quality | Detects cross-source contradictions |
| cost_awareness | Reports cost/token metadata |

## Results Summary (6 runs, Apr 3-4 2026)

| Metric | Baseline | Provena | Advantage |
|--------|----------|---------|-----------|
| Perfect scores (7/7) | Never | 2 consecutive runs | Provena |
| Token efficiency | — | **23x fewer** (best run) | Provena |
| Cost | $1.25/session | $0.13/session | **9.8x cheaper** |
| Latency | 28.7s mean | 27.5s mean | **Provena faster** |

See `benchmark_analysis_averages.md` for full details across all runs.
