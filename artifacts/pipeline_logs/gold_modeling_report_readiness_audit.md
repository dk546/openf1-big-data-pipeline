# Gold, Modeling, and Report Artifacts Readiness Audit

**Audit date/time:** 2026-05-19 (static review + code draft; no notebooks executed)

**Scope:** Gold layer audit, modeling layer draft, report artifacts draft, technology compliance.

---

## Files reviewed

| Category | Files |
|----------|-------|
| Project docs | `README.md`, `project_context.md`, `project_plan.md`, `implementation_checklist.md` |
| Config / deps | `requirements.txt`, `pyproject.toml` |
| Gold | `build_feature_mart.py`, `build_feature_mart_spark.py`, `feature_dictionary.py` |
| Validation | `analytics/duckdb_validation.py` |
| Modeling | `modeling/splits.py`, `baselines.py`, `train.py`, `evaluate.py` |
| Reporting | `reporting/report_tables.py` |
| Notebooks | `03_gold_feature_engineering.ipynb`, `04_modeling_evaluation.ipynb`, `05_report_artifacts.ipynb` |
| Prior audits | `pyspark_duckdb_full_refactor_plan.md`, `pyspark_duckdb_pre_colab_audit.md`, `ml_feature_technology_plan_audit.md`, `gold_feature_mart_implementation.md` |

**Execution status:** No notebooks were executed during this audit. No fake outputs were created.

---

## Gold readiness assessment

### Verdict: **Ready for Colab smoke** (after GitHub push)

| Check | Status |
|-------|--------|
| Base = `session_result_clean.parquet` | PASS |
| Grain = `session_key`, `meeting_key`, `driver_number` | PASS |
| Target = `points_finish = 1 if points > 0` | PASS |
| No rows from sessions alone | PASS |
| Feature groups (lap, pit, position, weather, race control, metadata) | PASS (conditional on Silver columns) |
| Leakage guard + feature dictionary | PASS |
| Gold CSV reports (7 files) | PASS |
| DuckDB Gold validation in notebook 03 | PASS |

### Gold fix applied

**Spark/pandas base alignment:** [`build_target_base_spark`](src/openf1_pipeline/gold/build_feature_mart_spark.py) now drops raw `position` and `points` after deriving `final_position` and `result_points`, matching pandas `build_target_base`.

---

## Modeling plan (locked)

| Item | Definition |
|------|------------|
| Target | `points_finish` |
| Models | Random, heuristic (`first_observed_position <= 10`), Logistic Regression, Random Forest, LightGBM |
| Split | Train 2023 · Validation 2024 · Test 2025 on `session_year` |
| Smoke mode | `MODELING_MODE = "smoke"` — wiring only, not official MBA metrics |
| Full mode | `MODELING_MODE = "full"` — requires non-empty train/val/test |

---

## Modeling files implemented

| File | Functions |
|------|-----------|
| [`modeling/splits.py`](src/openf1_pipeline/modeling/splits.py) | `create_season_split`, `create_fallback_time_split`, `split_by_season` (alias) |
| [`modeling/baselines.py`](src/openf1_pipeline/modeling/baselines.py) | `random_baseline_predictions`, `heuristic_position_baseline`, `majority_class_baseline` |
| [`modeling/train.py`](src/openf1_pipeline/modeling/train.py) | `get_model_feature_columns`, `prepare_model_matrix`, pipeline builders, `train_models`, `extract_feature_importance` |
| [`modeling/evaluate.py`](src/openf1_pipeline/modeling/evaluate.py) | `compute_classification_metrics`, `compute_confusion_matrix_table`, `build_error_analysis`, `save_modeling_outputs` |
| [`04_modeling_evaluation.ipynb`](notebooks/04_modeling_evaluation.ipynb) | Full Colab workflow with `MODELING_MODE` |

### Model outputs (written on Colab run)

- `reports/model_results/baseline_metrics.csv`
- `reports/model_results/validation_metrics.csv`
- `reports/model_results/test_metrics.csv`
- `reports/model_results/confusion_matrix.csv`
- `reports/model_results/error_analysis.csv`
- `reports/model_results/feature_importance.csv`
- `artifacts/manifests/model_run_manifest.json`

---

## Report artifact plan

| Component | Status |
|-----------|--------|
| [`reporting/report_tables.py`](src/openf1_pipeline/reporting/report_tables.py) | Implemented — 11 table builders, skip missing inputs with warnings |
| [`05_report_artifacts.ipynb`](notebooks/05_report_artifacts.ipynb) | Implemented — tables, matplotlib figures, `run_manifest.json` |

### Report tables (`reports/tables/`)

1. `data_volume_by_layer.csv`
2. `bronze_endpoint_row_counts.csv`
3. `silver_cleaning_impact_table.csv`
4. `silver_error_taxonomy_table.csv`
5. `gold_feature_group_summary.csv`
6. `gold_target_distribution_table.csv`
7. `model_baseline_comparison_table.csv`
8. `model_validation_test_metrics_table.csv`
9. `confusion_matrix_table.csv`
10. `error_analysis_summary_table.csv`
11. `reproducibility_artifacts_table.csv`

### Figures (`reports/figures/`)

- `architecture_diagram_placeholder.md`
- `target_distribution.png`
- `model_metric_comparison.png`
- `confusion_matrix.png`
- `feature_importance_top20.png`
- `missingness_before_after.png`

All figures skip gracefully when source CSVs are missing.

---

## Technology compliance assessment

| Technology | Role | Documented |
|------------|------|------------|
| Python + requests | OpenF1 ingestion | PASS |
| PySpark | Bronze reports, Silver, Gold (primary) | PASS |
| DuckDB | Validation/reporting SQL | PASS |
| pandas | CSV export, ML handoff, notebook display | PASS |
| scikit-learn / LightGBM | Modeling after Gold | PASS |
| Databricks | Out of scope | PASS (not required) |

Updated: `project_plan.md` (Spark-default), `implementation_checklist.md`, `README.md`.

---

## Remaining risks

| Risk | Mitigation |
|------|------------|
| Refactor not pushed to GitHub | Commit/push before Colab clone |
| Spark Gold untested on Colab | Run 01→02→03 smoke; verify `engine=="spark"` |
| Smoke has only 2024 data | Use `MODELING_MODE="smoke"` for wiring; `full` after 2023–2025 ingestion |
| Silent Spark→pandas fallback | Check `outputs["summary"]["engine"]` in 01–03 |
| Stale Drive outputs | Clear output root before smoke |
| Report notebook 05 needs model_results | Run notebook 04 first (or expect skipped figures/tables) |

---

## Required Colab run order

| Step | Notebook | Notes |
|------|----------|-------|
| 00 | `00_colab_setup.ipynb` | Optional sanity check |
| 01 | `01_ingestion_bronze.ipynb` | `SMOKE_TEST=True`, `BRONZE_REPORT_ENGINE="spark"` |
| 02 | `02_silver_cleaning_quality.ipynb` | `SILVER_ENGINE="spark"` |
| 03 | `03_gold_feature_engineering.ipynb` | `GOLD_ENGINE="spark"` |
| 04 | `04_modeling_evaluation.ipynb` | `MODELING_MODE="smoke"` then `"full"` |
| 05 | `05_report_artifacts.ipynb` | After 04 produces model_results |

---

## Go / no-go verdicts

| Notebook | Verdict |
|----------|---------|
| **03 Gold** | **Go** — after GitHub push + Spark smoke on Drive |
| **04 Modeling** | **Go (code)** — run after Gold mart exists; smoke mode for wiring |
| **05 Report artifacts** | **Go (code)** — run after 04; full MBA tables/figures need full pipeline + modeling |

---

## Files modified/created in this implementation

| Action | Path |
|--------|------|
| Fixed | `src/openf1_pipeline/gold/build_feature_mart_spark.py` |
| Implemented | `src/openf1_pipeline/modeling/splits.py` |
| Implemented | `src/openf1_pipeline/modeling/baselines.py` |
| Implemented | `src/openf1_pipeline/modeling/train.py` |
| Implemented | `src/openf1_pipeline/modeling/evaluate.py` |
| Created | `src/openf1_pipeline/reporting/__init__.py` |
| Created | `src/openf1_pipeline/reporting/report_tables.py` |
| Rebuilt | `notebooks/04_modeling_evaluation.ipynb` |
| Rebuilt | `notebooks/05_report_artifacts.ipynb` |
| Extended | `scripts/rebuild_colab_notebooks.py` (`build_04`, `build_05`) |
| Updated | `README.md`, `project_plan.md`, `implementation_checklist.md` |
| Created | `artifacts/pipeline_logs/gold_modeling_report_readiness_audit.md` (this file) |
