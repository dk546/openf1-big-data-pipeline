# Report Section-to-Artifact Map

**Output root:** Paths below are relative to `OPENF1_DATA_ROOT` (Google Drive on Colab).  
**Smoke bundle (repo):** `evidence/smoke_2024_maxsessions2_spark/` mirrors the same relative paths for audit review — not official full-run evidence.

**Status legend**

| Status | Meaning |
|--------|---------|
| **smoke evidence available** | Present in smoke Colab bundle or repo copy; suitable for pipeline/join/DQ narrative only |
| **full-run evidence pending** | Requires `SMOKE_TEST=False`, seasons 2023–2025, notebooks `01`–`03` on Drive |
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
| 2.2 Scope & endpoints | Seasons, endpoints, sessions | Ingestion manifest, file inventory | `01` | `artifacts/manifests/ingestion_manifest.csv`; `reports/data_quality/bronze_file_inventory.csv` | smoke evidence available | Smoke: 2 sessions, 2024; full: 2023–2025 pending |
| 2.3 Volume & characteristics | Row/file counts by layer | Bronze/Silver/Gold counts, DuckDB | `01`–`03`, `05` | `bronze_row_counts.csv`; `silver_table_inventory.csv`; `duckdb_bronze_*.csv`; `duckdb_silver_silver_row_counts.csv`; `duckdb_gold_gold_row_count.csv`; `reports/tables/data_volume_by_layer.csv` | smoke evidence available (partial) | Gold mart parquet on Drive; smoke mart ~40 rows |
| 2.4 Quality risks | Structural gaps, API limits | DQ notes, audits, schema drift | `01`–`02` | `silver_data_quality_notes.csv`; `bronze_schema_drift.csv`; `duckdb_bronze_bronze_schema_drift_flags.csv`; `artifacts/pipeline_logs/phase1_*.md`, `phase2_*.md` | smoke evidence available | `starting_grid` 404; race_control driver_number often null |

---

## Chapter 3 — Pipeline Architecture and Technology Rationale

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 3.1 Medallion overview | Layer diagram and flow | Docs, figure placeholder | `05` | `README.md`; `reports/figures/architecture_diagram_placeholder.md` | smoke evidence available | Replace placeholder with diagram before submission |
| 3.2 Bronze | Ingestion + JSONL | Manifest, schema, row counts | `01` | `data/bronze/`; `ingestion_manifest.csv`; `bronze_schema_report.csv`; `bronze_row_counts.csv` | smoke evidence available | Bulk JSONL on Drive, not in git |
| 3.3 Silver | PySpark cleaning | Cleaning rules, impact, inventory | `02` | `data/silver/*_clean.parquet`; `silver_cleaning_rules.csv`; `silver_cleaning_impact_summary.csv`; `silver_table_inventory.csv` | smoke evidence available | Spark engine default |
| 3.4 Gold | Feature mart | Mart parquet, join report | `03` | `data/gold/driver_race_feature_mart.parquet`; `gold_join_quality_report.csv` | smoke evidence available | Full-run row counts pending |
| 3.5 PySpark & DuckDB | Stack rationale | DuckDB validation CSVs | `01`–`03` | `reports/data_quality/duckdb_bronze_*.csv`; `duckdb_silver_*.csv`; `duckdb_gold_*.csv` | smoke evidence available | Databricks out of scope per `project_context.md` |
| 3.6 Reproducibility | Colab workflow | Run manifests, README | `00`–`05` | `artifacts/manifests/run_manifest.json`; `model_run_manifest.json`; `README.md` §11–13 | smoke evidence available (partial) | `run_manifest.json` updated in `05` after full pipeline |

---

## Chapter 4 — Data Quality and Cleaning

| Subsection | Purpose | Required artifact(s) | Source notebook | Source path(s) | Status | Notes |
|------------|---------|----------------------|-----------------|------------------|--------|-------|
| 4.1 Error taxonomy | Classify DQ issue types | Cleaning rules, taxonomy table | `02`, `05` | `silver_cleaning_rules.csv`; `reports/tables/silver_error_taxonomy_table.csv` | smoke evidence available | Taxonomy table built in `05` from rules |
| 4.2 Detection strategy | How issues are found | Duplicate, outlier, temporal, RI reports | `02` | `silver_duplicate_report.csv`; `silver_outlier_report.csv`; `silver_temporal_anomaly_report.csv`; `silver_referential_integrity_report.csv` | smoke evidence available | IQR flags ≠ automatic removal |
| 4.3 Remediation rules | Fix steps applied | Rules, rejected summary, impact | `02` | `silver_cleaning_rules.csv`; `silver_rejected_records_summary.csv`; `silver_cleaning_impact_summary.csv` | smoke evidence available | Smoke: 0 rejected rows (template populated) |
| 4.4 Before/after validation | Missingness delta | Before/after missingness, figure | `02`, `05` | `silver_missingness_before.csv`; `silver_missingness_after.csv`; `reports/figures/missingness_before_after.png` | smoke evidence available | Figure requires `05` run |
| 4.5 Impact on modeling | Silver → Gold retention | Join quality, row counts | `03` | `gold_join_quality_report.csv`; `duckdb_gold_gold_row_count.csv` | smoke evidence available | Full-run join stats pending |

**Chapter 4 — DuckDB supplements**

| Artifact | Notebook | Path | Status |
|----------|----------|------|--------|
| Silver row counts | `02` | `duckdb_silver_silver_row_counts.csv` | smoke evidence available |
| Session result keys | `02` | `duckdb_silver_session_result_duplicate_keys.csv` | smoke evidence available |
| Target support | `02` | `duckdb_silver_session_result_target_support.csv` | smoke evidence available |
| Laps/pit/weather by session | `02` | `duckdb_silver_laps_rows_by_session_key.csv`; `duckdb_silver_pit_rows_by_session_key.csv`; `duckdb_silver_weather_rows_by_session_key.csv` | smoke evidence available |

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
| 6.6 Reproducibility | Seeds, paths | Manifests, repro table | `04`, `05` | `model_run_manifest.json`; `reports/tables/reproducibility_artifacts_table.csv` | final modeling pending | `RANDOM_SEED=42` in config |

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
| 01 | `01_ingestion_bronze.ipynb` | `data/bronze/`; `artifacts/manifests/`; `reports/data_quality/bronze_*`; `duckdb_bronze_*` |
| 02 | `02_silver_cleaning_quality.ipynb` | `data/silver/`; `reports/data_quality/silver_*`; `duckdb_silver_*` |
| 03 | `03_gold_feature_engineering.ipynb` | `data/gold/`; `reports/data_quality/gold_*`; `artifacts/feature_definitions/`; `duckdb_gold_*` |
| 04 | `04_modeling_evaluation.ipynb` | `reports/model_results/`; `model_run_manifest.json` |
| 05 | `05_report_artifacts.ipynb` | `reports/tables/`; `reports/figures/`; `run_manifest.json` |

---

## Quick reference — all pipeline CSV artifacts

| Path pattern | Chapter |
|--------------|---------|
| `artifacts/manifests/ingestion_manifest.csv` | 2, 3 |
| `reports/data_quality/bronze_*.csv` | 2, 3 |
| `reports/data_quality/duckdb_bronze_*.csv` | 2, 3 |
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
