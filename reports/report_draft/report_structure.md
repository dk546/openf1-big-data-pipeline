# Final Report Structure (Locked)

**Project:** A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

**Task framing (locked):** Points-finish classification using integrated race-session features — not strict pre-race prediction.

**Status:** Structure locked for final written deliverable. Numeric results and official performance claims require full 2023–2025 Colab execution (`MODELING_MODE="full"`).

**Companion docs:** `report_artifact_map.md` · `table_figure_register.md` · `narrative_guardrails.md`

---

## 1. Executive Summary

**Purpose:** One-page synthesis of the pipeline, data scope, quality posture, feature mart, and modeling headline (after full run).

**Evidence sources:** Cross-chapter artifacts; `reports/tables/` summaries from notebook `05`; `artifacts/manifests/run_manifest.json`.

**Placeholder until full run:** State pipeline is implemented and smoke-validated; report final metrics as `[PENDING: full 2023–2025 run]`.

---

## 2. Data Landscape

### 2.1 OpenF1 source description

**Purpose:** Describe OpenF1 API, licensing context, and why it fits a big-data infrastructure narrative.

**Evidence:** `README.md` §5; `project_plan.md` §2; [OpenF1 API](https://api.openf1.org/v1/).

### 2.2 Dataset scope and endpoints

**Purpose:** List seasons, session types, and Bronze endpoints ingested.

**Evidence:** `artifacts/manifests/ingestion_manifest.csv`; `reports/data_quality/bronze_file_inventory.csv`.

### 2.3 Data volume and characteristics

**Purpose:** Row counts, file counts, and layer-level volume comparison.

**Evidence:** `reports/data_quality/bronze_row_counts.csv`; `reports/data_quality/silver_table_inventory.csv`; `reports/data_quality/duckdb_*`; `reports/tables/data_volume_by_layer.csv` (notebook `05`).

### 2.4 Data quality risks

**Purpose:** Structural missingness, optional endpoints, API failures, and racing-domain outliers.

**Evidence:** Phase 1–2 audit logs; `reports/data_quality/silver_data_quality_notes.csv`; `narrative_guardrails.md` §D.

---

## 3. Pipeline Architecture and Technology Rationale

### 3.1 Medallion architecture overview

**Purpose:** Bronze → Silver → Gold → ML consumption layer diagram and narrative.

**Evidence:** `README.md` §4–8; Figure 1 (planned); `reports/figures/architecture_diagram_placeholder.md`.

### 3.2 Bronze layer: raw API ingestion and immutable JSONL storage

**Purpose:** `requests` ingestion, JSONL layout, manifests, schema profiling, targeted retry for transient API failures, manifest-vs-file reconciliation, and independent Bronze Spark / DuckDB validation.

**Evidence:** Notebook `01`; `data/bronze/`; `ingestion_manifest.csv`; `ingestion_retry_manifest.csv` *(generated only when targeted retry is run)*; `ingestion_manifest_effective.csv` *(generated after retry; reconciliation runs against this)*; Bronze DQ CSVs including `bronze_manifest_file_reconciliation.csv`, `bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_*` (including `duckdb_bronze_manifest_file_reconciliation_summary.csv`, `..._by_endpoint.csv`, `..._issues.csv`).

**Narrative anchors:**

- Manifest is the canonical Bronze provenance record per `(endpoint, year, session_key)`.
- Targeted retry uses `openf1_pipeline.ingestion.ingest.retry_failed_session_endpoints(...)` to re-fetch only failed required session endpoints with slower pacing (default 3 s base sleep); it preserves the original manifest and writes a separate retry manifest.
- **Effective post-retry manifest** built by `write_effective_manifest_after_retry(...)` overlays retry rows on the original manifest with a `manifest_source` provenance tag (`"original"` / `"retry"`). Reconciliation runs against this effective manifest; without it, retry-recovered files appear as `failed_but_file_present`.
- Manifest-vs-file reconciliation joins the (effective) manifest to the on-disk JSONL inventory and classifies every triple into `matched`, `row_count_mismatch`, `manifest_success_missing_file`, `failed_manifest_file_exists`, `stale_file_not_in_success_manifest`, `optional_missing`, or `manifest_failed_no_file`.
- See `artifacts/pipeline_logs/full_bronze_retry_plan.md`, `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md`, and `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md`.

### 3.3 Silver layer: PySpark cleaning, schema enforcement, and quality checks

**Purpose:** Spark-first cleaning, rule logging, before/after DQ.

**Evidence:** Notebook `02`; `src/openf1_pipeline/silver/build_silver_spark.py`; Silver DQ CSVs.

### 3.4 Gold layer: driver-race feature mart

**Purpose:** Grain, target construction, join groups, mart output.

**Evidence:** Notebook `03`; `data/gold/driver_race_feature_mart.parquet`; Gold DQ CSVs.

### 3.5 PySpark and DuckDB technology rationale

**Purpose:** Why Spark transforms and DuckDB validates; why Databricks is out of scope.

**Evidence:** `README.md` §7; `project_context.md` §G; `duckdb_*` reports.

### 3.6 Reproducibility workflow

**Purpose:** Colab + Drive, seeds, manifests, notebook order, cleanup flags.

**Evidence:** `README.md` §11–13; `artifacts/manifests/run_manifest.json`; `model_run_manifest.json`.

---

## 4. Data Quality and Cleaning

### 4.1 Error taxonomy

**Purpose:** Classify error types (schema, referential, temporal, outlier, structural missingness).

**Evidence:** `reports/tables/silver_error_taxonomy_table.csv`; `silver_cleaning_rules.csv`; Phase 2 audit.

### 4.2 Detection strategy

**Purpose:** How each error class is detected (Spark steps, profiling, DuckDB SQL). Includes a **Bronze-layer detection control** for manifest-vs-file inconsistencies before any Silver work runs.

**Evidence:**

- **Bronze-layer detection** — `bronze_manifest_file_reconciliation.csv`, `bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv`, `duckdb_bronze_manifest_file_reconciliation_issues.csv` (from `generate_bronze_reconciliation_reports(...)`).
- **Silver-layer detection** — `silver_cleaning_rules.csv`; `silver_duplicate_report.csv`; `silver_outlier_report.csv`; `silver_temporal_anomaly_report.csv`; `silver_referential_integrity_report.csv`.

### 4.3 Remediation rules

**Purpose:** Casting, dedupe, domain filters, imputation flags — with step IDs. Also documents the **Bronze remediation loop** for transient API failures.

**Evidence:**

- **Bronze remediation** — `retry_failed_session_endpoints(...)` re-fetches failed required session endpoints with slower pacing (default 3 s base sleep), writing `artifacts/manifests/ingestion_retry_manifest.csv` without modifying the original manifest. `write_effective_manifest_after_retry(...)` then produces `artifacts/manifests/ingestion_manifest_effective.csv` — the merged provenance-tagged manifest reconciliation runs against. Optional `delete_stale_bronze_files(...)` is opt-in for the three known stale smoke files. `starting_grid` is excluded from retry by default because it is optional.
- **Silver remediation** — `silver_cleaning_rules.csv`; `silver_rejected_records_summary.csv`; `silver_cleaning_impact_summary.csv`.

### 4.4 Before/after validation

**Purpose:** Quantify missingness, cleaning impact, and Bronze retry effect.

**Evidence:**

- **Bronze before/after** — `ingestion_manifest.csv` (original full run) vs `ingestion_retry_manifest.csv` (retry results) vs `ingestion_manifest_effective.csv` (merged) vs refreshed `bronze_row_counts.csv`, `bronze_file_inventory.csv`, and `bronze_manifest_file_reconciliation_summary.csv` after retry. Status: retry results `[PENDING: targeted retry not yet executed]` *(or already executed — confirm whether the effective-manifest reconciliation was used to produce the post-retry totals cited in the report).*
- **Silver before/after** — `silver_missingness_before.csv`; `silver_missingness_after.csv`; Figure 4; `duckdb_silver_*.csv`.

### 4.5 Cleaning impact on modeling dataset

**Purpose:** Link Bronze coverage and Silver quality to Gold row retention and join completeness, with explicit attention to `session_result` coverage as the upstream constraint on the modeling dataset.

**Evidence:**

- **Bronze target coverage** — `session_result` success counts from `ingestion_manifest.csv` + `ingestion_retry_manifest.csv`, cross-checked against `bronze_file_inventory.csv` via `bronze_manifest_file_reconciliation.csv`. Full coverage target: 89/89 race sessions across 2023–2025. Status: `[PENDING: target coverage after retry]`.
- **Silver/Gold downstream** — `gold_join_quality_report.csv`; Phase 3 audit; `[PENDING: full-run row counts]`.

---

## 5. Feature Engineering and Integration

### 5.1 Driver-race feature mart

**Purpose:** Grain, target `points_finish`, feature domains.

**Evidence:** `driver_race_feature_mart.parquet`; `gold_feature_summary_stats.csv`; `project_context.md` §C.

### 5.2 Join strategy

**Purpose:** Spine table, left joins, unmatched counts.

**Evidence:** `gold_join_quality_report.csv`; Phase 3 audit.

### 5.3 Temporal alignment and feature tiers

**Purpose:** Tier 1 early-session vs Tier 2 full-session analytical features.

**Evidence:** `model_feature_plan.csv`; `narrative_guardrails.md` §B; `feature_dictionary.csv` (`feature_tier`).

### 5.4 Feature dictionary and leakage guard

**Purpose:** Roles, allowed columns, blocked outcomes.

**Evidence:** `feature_dictionary.csv`; `gold_leakage_guard_report.csv`; `openf1_pipeline.modeling.feature_selection`.

### 5.5 Feature validation

**Purpose:** Missingness, target distribution, DuckDB cross-checks.

**Evidence:** `gold_feature_missingness.csv`; `gold_target_distribution.csv`; `duckdb_gold_*.csv`.

---

## 6. Experimental Results and Analysis

### 6.1 Classification task

**Purpose:** Define target, task framing, default feature bundle (40 numeric + optional categoricals).

**Evidence:** `model_feature_plan.csv`; `narrative_guardrails.md` §A–B.

### 6.2 Data split strategy

**Purpose:** Season splits 2023 / 2024 / 2025; smoke vs full modes.

**Evidence:** `model_run_manifest.json` (`split_method`, `modeling_mode`); `src/openf1_pipeline/modeling/splits.py`.

### 6.3 Baselines

**Purpose:** Random, majority, heuristic (`first_observed_position` ≤ 10).

**Evidence:** `reports/model_results/baseline_metrics.csv`; notebook `04`.

### 6.4 Model performance

**Purpose:** LR, RF, LightGBM on validation and test.

**Evidence:** `validation_metrics.csv`; `test_metrics.csv`; `reports/tables/model_*_table.csv`.

**Placeholder:** `[PENDING: full-run metrics — smoke not reportable as final findings]`.

### 6.5 Confusion matrix and error analysis

**Purpose:** Per-model confusion; errors by team/circuit/season.

**Evidence:** `confusion_matrix.csv`; `error_analysis.csv`; Figure 8.

### 6.6 Reproducibility statement

**Purpose:** Seeds, artifact paths, manifest lineage, and the full Bronze ingestion → retry → reconciliation provenance chain.

**Evidence:** `reproducibility_artifacts_table.csv`; `run_manifest.json`; `README.md` §13; `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv` *(when retry has run)*; `artifacts/manifests/ingestion_manifest_effective.csv` *(when retry has run)*; `reports/data_quality/bronze_manifest_file_reconciliation.csv` and `..._summary.csv`; `duckdb_bronze_manifest_file_reconciliation_*.csv`; `artifacts/pipeline_logs/full_bronze_output_review.md`, `full_bronze_retry_plan.md`, `bronze_manifest_file_reconciliation_added.md`, `bronze_effective_manifest_post_retry.md`.

---

## 7. Conclusion and Reflection

### 7.1 Lessons learned

**Purpose:** Infrastructure-first insights, smoke vs full, data quirks (starting_grid, structural nulls).

**Evidence:** `artifacts/pipeline_logs/phase*_*.md`; author reflection `[TO BE WRITTEN]`.

### 7.2 Deployment considerations

**Purpose:** Colab/Drive ops, re-run policy, no Databricks requirement.

**Evidence:** `README.md` cleanup table; `project_plan.md`.

### 7.3 Future improvements

**Purpose:** Tier-1-only sensitivity, hyperparameter study (out of scope), streaming, etc.

**Evidence:** Phase audit recommendations; `[TO BE WRITTEN after full run]`.

---

## Writing order (recommended)

1. Chapters 2–3 (landscape + architecture) — mostly stable from docs and smoke DQ.
2. Chapter 4–5 — cite smoke tables; flag full-run volume placeholders.
3. Chapter 6 — only after `04` full mode + `05` on Drive.
4. Chapter 1 and 7 — last, after all artifacts confirmed.
