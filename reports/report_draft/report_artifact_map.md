# Report Section-to-Artifact Map

**Output root:** Paths below are relative to `OPENF1_DATA_ROOT` (Google Drive on Colab).  
**Smoke bundle (repo):** `evidence/smoke_2024_maxsessions2_spark/` mirrors the same relative paths for audit review — not official full-run evidence.

**Status legend**

| Status | Meaning |
|--------|---------|
| **smoke evidence available** | Present in smoke Colab bundle or repo copy; suitable for pipeline/join/DQ narrative only |
| **full-run evidence pending** | Requires `SMOKE_TEST=False`, seasons 2023–2025, notebooks `01`–`03` on Drive |
| **pending until retry completes** | Requires `RUN_TARGETED_RETRY=True` in notebook `01` and a subsequent reconciliation refresh; placeholders used until then |
| **final modeling pending** | Requires notebook `04` with `MODELING_MODE="full"` after full Gold |
| **authoring pending** | Reflection or synthesis not yet written |

---

## Chapter 1 — Executive Summary

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 1 Executive Summary | Synthesize pipeline + results | All chapters; `reports/tables/*.csv`; manifests | `05` | `reports/tables/`; `artifacts/manifests/run_manifest.json` | authoring pending | Do not invent metrics; use `[PENDING]` until full run |

---

## Chapter 2 — Data Landscape

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 2.1 OpenF1 source | API and data provenance | Docs + API reference | — | `README.md`; `project_plan.md` | smoke evidence available | No CSV required |
| 2.2 Scope & endpoints | Seasons, endpoints, sessions | Ingestion + retry manifests, file inventory, reconciliation | `01` | `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv`; `reports/data_quality/bronze_file_inventory.csv`; `reports/data_quality/bronze_manifest_file_reconciliation.csv` | smoke evidence available; retry CSV **pending until retry completes** | Smoke: 2 sessions, 2024; full: 2023–2025 pending; retry CSV only written when `RUN_TARGETED_RETRY=True` |
| 2.3 Volume & characteristics | Row/file counts by layer | Bronze/Silver/Gold counts, DuckDB, reconciliation | `01`–`03`, `05` | `bronze_row_counts.csv`; `silver_table_inventory.csv`; `duckdb_bronze_*.csv`; `duckdb_silver_silver_row_counts.csv`; `duckdb_gold_gold_row_count.csv`; `reports/tables/data_volume_by_layer.csv`; `bronze_manifest_file_reconciliation_summary.csv` | smoke evidence available (partial); reconciliation totals **pending until retry completes** | Gold mart parquet on Drive; smoke mart ~40 rows; reconciliation summary surfaces manifest-vs-disk deltas |
| 2.4 Quality risks | Structural gaps, API limits, stale-file risk | DQ notes, audits, schema drift, reconciliation issues | `01`–`02` | `silver_data_quality_notes.csv`; `bronze_schema_drift.csv`; `duckdb_bronze_bronze_schema_drift_flags.csv`; `duckdb_bronze_manifest_file_reconciliation_issues.csv`; `artifacts/pipeline_logs/phase1_*.md`, `phase2_*.md`, `full_bronze_output_review.md`, `full_bronze_retry_plan.md`, `bronze_manifest_file_reconciliation_added.md` | smoke evidence available | `starting_grid` 404 (optional); HTTP 429 rate-limit cluster documented as a real, surfaced risk |

---

## Chapter 3 — Pipeline Architecture and Technology Rationale

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 3.1 Medallion overview | Layer diagram and flow | Docs, figure placeholder | `05` | `README.md`; `reports/figures/architecture_diagram_placeholder.md` | smoke evidence available | Replace placeholder with diagram before submission |
| 3.2 Bronze | Ingestion + JSONL + targeted retry + reconciliation | Manifest, retry manifest, schema, row counts, reconciliation reports | `01` | `data/bronze/`; `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv`; `reports/data_quality/bronze_schema_report.csv`; `reports/data_quality/bronze_row_counts.csv`; `reports/data_quality/bronze_manifest_file_reconciliation.csv`; `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv`; `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_summary.csv`; `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv`; `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_issues.csv` | smoke evidence available; retry CSV **pending until retry completes** | Bulk JSONL on Drive, not in git; retry CSV only when `RUN_TARGETED_RETRY=True`; reconciliation reports gate progression to Silver |
| 3.3 Silver | PySpark cleaning | Cleaning rules, impact, inventory | `02` | `data/silver/*_clean.parquet`; `silver_cleaning_rules.csv`; `silver_cleaning_impact_summary.csv`; `silver_table_inventory.csv` | smoke evidence available | Spark engine default |
| 3.4 Gold | Feature mart | Mart parquet, join report | `03` | `data/gold/driver_race_feature_mart.parquet`; `gold_join_quality_report.csv` | smoke evidence available | Full-run row counts pending |
| 3.5 PySpark & DuckDB | Stack rationale | DuckDB validation CSVs | `01`–`03` | `reports/data_quality/duckdb_bronze_*.csv`; `duckdb_silver_*.csv`; `duckdb_gold_*.csv` | smoke evidence available | Databricks out of scope per `project_context.md` |
| 3.6 Reproducibility | Colab workflow | Run manifests, README | `00`–`05` | `artifacts/manifests/run_manifest.json`; `model_run_manifest.json`; `README.md` §11–13 | smoke evidence available (partial) | `run_manifest.json` updated in `05` after full pipeline |

---

## Chapter 4 — Data Quality and Cleaning

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 4.1 Error taxonomy | Classify DQ issue types (Bronze + Silver) | Cleaning rules, taxonomy table, reconciliation status categories | `02`, `05` | `silver_cleaning_rules.csv`; `reports/tables/silver_error_taxonomy_table.csv`; `bronze_manifest_file_reconciliation.csv` (`reconciliation_status`, `issue_type`) | smoke evidence available | Taxonomy now spans Bronze (manifest-vs-file) and Silver (cleaning rules) |
| 4.2 Detection strategy | How issues are found | Duplicate, outlier, temporal, RI reports; Bronze manifest-vs-file reconciliation | `01`, `02` | `bronze_manifest_file_reconciliation.csv`; `bronze_manifest_file_reconciliation_summary.csv`; `duckdb_bronze_manifest_file_reconciliation_summary.csv`; `duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv`; `duckdb_bronze_manifest_file_reconciliation_issues.csv`; `silver_duplicate_report.csv`; `silver_outlier_report.csv`; `silver_temporal_anomaly_report.csv`; `silver_referential_integrity_report.csv` | smoke evidence available; reconciliation totals **pending until retry completes** | Bronze reconciliation is the gate before Silver runs; IQR flags ≠ automatic removal |
| 4.3 Remediation rules | Fix steps applied (Bronze retry + Silver cleaning) | Rules, rejected summary, impact; retry manifest; stale-file deletion | `01`, `02` | `silver_cleaning_rules.csv`; `silver_rejected_records_summary.csv`; `silver_cleaning_impact_summary.csv`; `artifacts/manifests/ingestion_retry_manifest.csv` | smoke evidence available; retry CSV **pending until retry completes** | Retry default sleep 3 s; `starting_grid` excluded; stale-file deletion opt-in |
| 4.4 Before/after validation | Missingness delta + Bronze coverage delta | Before/after missingness, figure; original vs retry manifest; refreshed Bronze reports | `01`, `02`, `05` | `silver_missingness_before.csv`; `silver_missingness_after.csv`; `reports/figures/missingness_before_after.png`; `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv`; refreshed `bronze_row_counts.csv` and `bronze_manifest_file_reconciliation_summary.csv` | smoke evidence available; Bronze before/after **pending until retry completes** | Figure requires `05` run; Bronze before/after compares manifest success rows pre- and post-retry against on-disk row counts |
| 4.5 Impact on modeling | Bronze coverage → Silver/Gold retention | `session_result` coverage; join quality; row counts | `01`, `03` | `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv`; `bronze_file_inventory.csv`; `bronze_manifest_file_reconciliation.csv`; `gold_join_quality_report.csv`; `duckdb_gold_gold_row_count.csv` | smoke evidence available; final coverage **pending until retry completes** | Target coverage = `session_result` successes ∪ retry successes; goal 89/89 race sessions across 2023–2025 |

**Chapter 4 — DuckDB supplements**

| Artifact | Notebook | Path | Status |
|----------|----------|------|--------|
| Silver row counts | `02` | `duckdb_silver_silver_row_counts.csv` | smoke evidence available |
| Session result keys | `02` | `duckdb_silver_session_result_duplicate_keys.csv` | smoke evidence available |
| Target support | `02` | `duckdb_silver_session_result_target_support.csv` | smoke evidence available |
| Laps/pit/weather by session | `02` | `duckdb_silver_laps_rows_by_session_key.csv`; `duckdb_silver_pit_rows_by_session_key.csv`; `duckdb_silver_weather_rows_by_session_key.csv` | smoke evidence available |

**Bronze retry + reconciliation artifacts (Chapters 2, 3, 4, 6)**

| Artifact | Purpose | Generated by notebook | Source file path | Full-run status | Interpretation notes |
|----------|---------|-----------------------|------------------|------------------|----------------------|
| `artifacts/manifests/ingestion_manifest.csv` | Canonical Bronze provenance: one row per `(endpoint, year, session_key)` ingestion attempt with `status`, `record_count`, `output_path`, `ingestion_timestamp_utc`. | `01` | `artifacts/manifests/ingestion_manifest.csv` | full-run evidence available; **never modified by retry** | Cited in Chapters 2, 3, 4, 6. Counts must be compared to disk via reconciliation; do not infer Drive state from this file alone. |
| `artifacts/manifests/ingestion_retry_manifest.csv` | Result of `retry_failed_session_endpoints(...)`. Extends manifest columns with `previous_status`, `previous_error_message`, `retry_attempt`. Writes only retry attempts; preserves the original manifest. | `01` (retry section) | `artifacts/manifests/ingestion_retry_manifest.csv` | **pending until retry completes** | Use alongside the original manifest. Effective `session_result` coverage = original successes ∪ retry successes. |
| `artifacts/manifests/ingestion_manifest_effective.csv` | Effective post-retry manifest. Built by `write_effective_manifest_after_retry(...)` by overlaying retry rows on the original manifest. Schema = `MANIFEST_COLUMNS + manifest_source` (`"original"` or `"retry"`). Original CSV is **never** modified. | `01` (retry-refresh section; also auto-built by reconciliation cell when a retry manifest is detected) | `artifacts/manifests/ingestion_manifest_effective.csv` | **pending until retry completes** | This is the manifest **reconciliation is run against** after retry. Without it, retry-recovered files are incorrectly classified as `failed_but_file_present`. |
| `reports/data_quality/bronze_manifest_file_reconciliation.csv` | One row per `(endpoint, year, session_key)` reconciling the manifest against the JSONL inventory on disk. Includes `manifest_status`, `manifest_record_count`, `file_exists`, `file_row_count`, `row_count_delta`, `reconciliation_status`, `issue_type`, `notes`. Built against the effective manifest when a retry manifest is present, otherwise against the original manifest. | `01` (Bronze reports + reconciliation section) | `reports/data_quality/bronze_manifest_file_reconciliation.csv` | full-run evidence available; refreshed after retry; **post-retry version pending until retry completes** | The canonical Bronze DQ gate before Silver. Non-`matched` / non-`optional_missing` rows must be triaged. |
| `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv` | Long-form summary of the reconciliation: `scope ∈ {by_status, by_endpoint, by_issue_type, totals}`. | `01` | `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv` | full-run evidence available; refreshed after retry; **post-retry version pending until retry completes** | Source for Table: "Bronze manifest-file reconciliation summary". |
| `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_summary.csv` | DuckDB-validated per-status counts (independent of pandas). | `01` (DuckDB validation) | `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_summary.csv` | full-run evidence available; refreshed after retry | Cross-checks pandas-computed summary using SQL. |
| `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv` | DuckDB per-endpoint × per-status counts. | `01` (DuckDB validation) | `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv` | full-run evidence available; refreshed after retry | Surfaces endpoint-localized issues (e.g. session_result 429 cluster). |
| `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_issues.csv` | Full rows where `reconciliation_status NOT IN ('matched', 'optional_missing')`. Fast triage view. | `01` (DuckDB validation) | `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_issues.csv` | full-run evidence available; refreshed after retry; ideally empty post-retry | Drives the "stop the line before Silver" warning. |
| `artifacts/pipeline_logs/full_bronze_retry_plan.md` | Plan document: design constraints, retry policy, throttling, output contract, run instructions, verification. | manual / Cursor | `artifacts/pipeline_logs/full_bronze_retry_plan.md` | document-only; not run-dependent | Cited in Chapter 3.2, 4.3, 6.6. |
| `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md` | Audit log for the reconciliation utility: rationale, expected counts on current Drive state, interpretation tables, ops checklist. | manual / Cursor | `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md` | document-only; not run-dependent | Cited in Chapter 3.2, 4.2, 4.4, 6.6. |
| `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md` | Audit log for the effective post-retry manifest: motivation (134 false `failed_but_file_present` rows after retry), contract, merge rules, defensive notebook integration. | manual / Cursor | `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md` | document-only; not run-dependent | Cited in Chapter 3.2, 4.3, 4.4, 6.6 whenever retry is discussed. |

---

## Chapter 5 — Feature Engineering and Integration

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 5.1 Feature mart | Grain, domains, target | Mart, summary stats | `03` | `driver_race_feature_mart.parquet`; `gold_feature_summary_stats.csv`; `gold_target_distribution.csv` | smoke evidence available | Target rate smoke ~50% — not population prior |
| 5.2 Join strategy | Join coverage | Join quality report | `03` | `gold_join_quality_report.csv` | smoke evidence available | Smoke: 40→40 all groups |
| 5.3 Tiers & temporal alignment | Tier 1 vs Tier 2 | Model feature plan | `03`, `04` | `artifacts/feature_definitions/model_feature_plan.csv`; `feature_dictionary.csv` | smoke evidence available | 8 Tier 1 + 32 Tier 2 default numeric |
| 5.4 Dictionary & leakage | Blocked vs allowed cols | Dictionary, leakage guard | `03` | `feature_dictionary.csv`; `gold_leakage_guard_report.csv` | smoke evidence available | Tier 2 allowed — not leakage |
| 5.5 Feature validation | Missingness, DuckDB | Gold missingness, DuckDB gold | `03` | `gold_feature_missingness.csv`; `duckdb_gold_gold_missingness_summary.csv`; `duckdb_gold_gold_duplicate_keys.csv`; `duckdb_gold_points_finish_by_team.csv`; `duckdb_gold_points_finish_by_circuit.csv` | smoke evidence available | |

---

## Chapter 6 — Experimental Results and Analysis

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 6.1 Classification task | Task + features | Model plan, narrative guardrails | `04` | `model_feature_plan.csv`; `reports/report_draft/narrative_guardrails.md` | smoke evidence available | Integrated session features framing |
| 6.2 Split strategy | Season splits | Model manifest, splits metadata | `04` | `artifacts/manifests/model_run_manifest.json` | final modeling pending | Smoke uses fallback/wiring tier |
| 6.3 Baselines | Random, majority, heuristic | Baseline metrics | `04` | `reports/model_results/baseline_metrics.csv` | final modeling pending | Smoke file may exist — not final findings |
| 6.4 Model performance | LR, RF, LightGBM | Val/test metrics | `04`, `05` | `validation_metrics.csv`; `test_metrics.csv`; `reports/tables/model_validation_test_metrics_table.csv` | final modeling pending | **Do not cite smoke metrics as official** |
| 6.5 Confusion & errors | Per-model diagnostics | Confusion matrix, error analysis | `04`, `05` | `confusion_matrix.csv`; `error_analysis.csv`; `feature_importance.csv` | final modeling pending | |
| 6.6 Reproducibility | Seeds, paths, Bronze provenance | Manifests, retry manifest, reconciliation reports, repro table | `04`, `05` | `model_run_manifest.json`; `reports/tables/reproducibility_artifacts_table.csv`; `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv`; `reports/data_quality/bronze_manifest_file_reconciliation.csv`; `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv`; `duckdb_bronze_manifest_file_reconciliation_*.csv`; `artifacts/pipeline_logs/full_bronze_retry_plan.md`; `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md` | final modeling pending; retry-related artifacts **pending until retry completes** | `RANDOM_SEED=42` in config; Bronze provenance chain = manifest → retry manifest → reconciliation → refreshed Bronze reports |

---

## Chapter 7 — Conclusion and Reflection

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 7.1 Lessons learned | What worked / failed | Phase audits, pipeline logs | — | `artifacts/pipeline_logs/phase1_*.md` … `phase4_fixes_applied.md` | smoke evidence available | Author synthesis pending |
| 7.2 Deployment | Ops on Colab/Drive | README cleanup policy | — | `README.md`; `project_plan.md` | smoke evidence available | |
| 7.3 Future improvements | Next steps | Audit recommendations | — | Phase 2–4 audits; `report_structure.md` §7.3 | authoring pending | Tier-1-only ablation, full seasons, etc. |

---

## Notebook run order (artifact dependency)

| Order | Notebook | Primary artifact directories |
|-------|----------|------------------------------|
| 00 | `00_colab_setup.ipynb` | Environment validation |
| 01 | `01_ingestion_bronze.ipynb` | `data/bronze/`; `artifacts/manifests/` (incl. `ingestion_retry_manifest.csv` when retry runs); `reports/data_quality/bronze_*` (incl. `bronze_manifest_file_reconciliation*.csv`); `duckdb_bronze_*` (incl. `duckdb_bronze_manifest_file_reconciliation_*.csv`) |
| 02 | `02_silver_cleaning_quality.ipynb` | `data/silver/`; `reports/data_quality/silver_*`; `duckdb_silver_*` |
| 03 | `03_gold_feature_engineering.ipynb` | `data/gold/`; `reports/data_quality/gold_*`; `artifacts/feature_definitions/`; `duckdb_gold_*` |
| 04 | `04_modeling_evaluation.ipynb` | `reports/model_results/`; `model_run_manifest.json` |
| 05 | `05_report_artifacts.ipynb` | `reports/tables/`; `reports/figures/`; `run_manifest.json` |

---

## Quick reference — all pipeline CSV artifacts

| Path pattern | Chapter |
|--------------|---------|
| `artifacts/manifests/ingestion_manifest.csv` | 2, 3, 4, 6 |
| `artifacts/manifests/ingestion_retry_manifest.csv` | 2, 3, 4, 6 *(when retry runs)* |
| `artifacts/manifests/ingestion_manifest_effective.csv` | 2, 3, 4, 6 *(when retry runs)* |
| `reports/data_quality/bronze_*.csv` (incl. `bronze_manifest_file_reconciliation*.csv`) | 2, 3, 4, 6 |
| `reports/data_quality/duckdb_bronze_*.csv` (incl. `duckdb_bronze_manifest_file_reconciliation_*.csv`) | 2, 3, 4, 6 |
| `reports/data_quality/silver_*.csv` | 4 |
| `reports/data_quality/duckdb_silver_*.csv` | 4 |
| `reports/data_quality/gold_*.csv` | 5 |
| `reports/data_quality/duckdb_gold_*.csv` | 5 |
| `artifacts/feature_definitions/feature_dictionary.csv` | 5 |
| `artifacts/feature_definitions/model_feature_plan.csv` | 5, 6 |
| `reports/model_results/*.csv` | 6 |
| `reports/tables/*.csv` | 1, 6 (via `05`) |
| `reports/figures/*` | 3–6 (via `05`) |
| `artifacts/manifests/run_manifest.json` | 3, 6, 7 |
| `artifacts/manifests/model_run_manifest.json` | 6 |
