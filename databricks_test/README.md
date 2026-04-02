# Fleet Benchmark Runbook

Step-by-step guide to running the SDOL fleet management benchmark on a Databricks workspace.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Catalog and schema** — default: `users.<your_username>`. The notebooks create the schema if it doesn't exist.
- **LLM endpoint** — a Foundation Model API endpoint serving Claude Sonnet 4.6 (default: `databricks-claude-sonnet-4-6`)
- **Vector Search endpoint** — created automatically by `02_fleet_setup` if it doesn't exist (default name: `sdol_fleet_vs`)
- **Cluster** — DBR 14+ recommended (for parameterized `spark.sql()` support)

## Syncing SDOL Source to Workspace

SDOL source code must be accessible on the workspace for `import sdol` to work.

**Option A: Git Repo integration (recommended)**
1. Add this repo as a Databricks Git folder: Workspace → Repos → Add Repo
2. Set `sdol_project_root` widget to `/Workspace/Repos/<your_email>/SDOL-python`

**Option B: REST API upload**
```bash
# Upload using format=AUTO (NOT format=SOURCE — that creates notebooks, not modules)
databricks workspace import_dir ./src /Workspace/Users/<you>/SDOL/src --overwrite
```

> **Warning:** `databricks workspace import --format SOURCE` converts `.py` files to notebooks, which breaks `import`. Always use `format: AUTO` or the Git Repo integration.

## Running the Benchmark

### Step 1: Data Setup (`02_fleet_setup`)

1. Open `02_fleet_setup` on the workspace
2. Configure widgets at the top (or accept defaults):
   - `catalog` — Unity Catalog catalog name (default: `users`)
   - `schema` — schema name (default: `default`)
   - `vs_endpoint` — Vector Search endpoint name (default: `sdol_fleet_vs`)
   - `embedding_model` — embedding model endpoint (default: `databricks-bge-large-en`)
   - `use_existing_catalog` — set to `false` to create the catalog
3. Run All
4. Wait for the Vector Search index to become `ONLINE_NO_PENDING_UPDATE` (~5–15 min on first run)
5. Verify: the final cell shows row counts for all 4 tables

**What it creates:**

| Table | Paradigm | Rows | Notes |
|-------|----------|------|-------|
| `fleet_machines` | OLTP | 500 | EXC-0342 set to `offline` |
| `telemetry_readings` | OLAP | ~360K | Hourly sensor data, 180 days |
| `telemetry_daily` | OLAP | ~90K + 1 | Pre-aggregated + conflict seed row |
| `maintenance_logs` | Document | ~5,000 | Free-text, VS-indexed |

### Step 2: Benchmark (`03_fleet_benchmark`)

1. Open `03_fleet_benchmark`
2. Configure widgets:
   - `catalog`, `schema` — must match Step 1
   - `llm_endpoint` — LLM serving endpoint
   - `sdol_project_root` — path to SDOL source (use `{user}` placeholder)
   - `vs_endpoint` — must match Step 1
3. Run All
4. Review results:
   - **Per-question comparison table** — latency, context chars consumed
   - **MLflow judge scores** — relevance, safety, epistemic_transparency, data_efficiency
   - **MLflow metrics** — latency and context_chars per question category
   - **Sample responses** — side-by-side for all 7 questions

## Evaluation Questions

| # | Category | What It Tests |
|---|----------|---------------|
| 1 | `cross_paradigm` | OLAP push-down + Vector Search vs raw row scanning |
| 2 | `epistemic_conflict` | OLTP/OLAP status conflict detection and resolution |
| 3 | `point_lookup` | Single-machine OLTP lookup with provenance |
| 4 | `aggregate` | Regional temperature aggregation |
| 5 | `semantic_search` | Hydraulic failure pattern search |
| 6 | `confidence` | Data reliability assessment |
| 7 | `trust_meta` | Source trust ranking — baseline structurally cannot answer |

## MLflow Metrics Logged

- `latency_<category>` — wall-clock seconds per question
- `latency_mean` — mean across all questions
- `context_chars_<category>` — total characters in tool call results per question
- `context_chars_mean` / `context_chars_total` — aggregate token efficiency
- Judge scores: `relevance_to_query`, `safety`, `epistemic_transparency`, `data_efficiency`

## Troubleshooting

- **`ImportError: No module named 'sdol'`** — check `sdol_project_root` widget. The SDOL `src/` directory must be on the Python path. See "Syncing SDOL Source" above.
- **Vector Search index stuck in `PROVISIONING`** — VS endpoints can take 10–20 min to provision on first use. Check the VS UI.
- **`No conflict detected for EXC-0342`** — make sure you re-ran `02_fleet_setup` after the enhancement that inserts the conflict seed row for `current_date()`.
- **MLflow `Guidelines` scorer not available** — requires `mlflow>=2.15`. The notebook falls back gracefully to `RelevanceToQuery` + `Safety` only.
