# Downstream Notebook Safety Audit

**Date:** 2026-05-19  
**Scope:** Notebooks 03–05, shared utilities, DuckDB validation, fallback behavior  
**Related:** [spark_silver_starting_grid_fix.md](./spark_silver_starting_grid_fix.md), [spark_silver_duplicate_report_fix.md](./spark_silver_duplicate_report_fix.md)

---

## Files reviewed

| Category | Paths |
|----------|-------|
| Config / I/O | `README.md`, `config.py`, `utils/io.py`, `utils/spark.py`, `utils/cleanup.py` (new) |
| Silver (prior fix) | `silver/build_silver.py`, `silver/build_silver_spark.py` |
| Gold | `gold/build_feature_mart.py`, `gold/build_feature_mart_spark.py`, `features/feature_dictionary.py` |
| Modeling | `modeling/splits.py`, `baselines.py`, `train.py`, `evaluate.py` |
| Reporting | `reporting/report_tables.py` |
| Validation | `analytics/duckdb_validation.py` |
| Bronze | `bronze/build_bronze.py` |
| Notebooks | `01`–`05` (via `scripts/rebuild_colab_notebooks.py`) |

---

## Drive output cleanup policy

Centralized in `openf1_pipeline.utils.cleanup`:

| Function | Cleans | Preserves |
|----------|--------|-----------|
| `clean_bronze_layer_outputs` | Bronze JSONL, bronze DQ CSVs, ingestion manifest, schemas | Silver+ |
| `clean_silver_layer_outputs` | Silver Parquet dirs, silver DQ CSVs, duckdb_silver CSVs | Bronze, Gold |
| `clean_gold_layer_outputs` | Gold Parquet, gold DQ CSVs, duckdb_gold CSVs, feature dictionary | Silver, models |
| `clean_model_outputs` | model_results CSVs, model_run_manifest.json | Gold, DQ |
| `clean_report_artifacts` | reports/tables, reports/figures | DQ, model_results |

Low-level helpers in `utils/io.py`: `clean_directory_contents`, `ensure_clean_output_dir`, `clean_files_matching`.

Notebook defaults documented in README.

---

## Notebook 03 — Gold safety assessment

### Before

- Silent Spark→pandas fallback after partial Gold Parquet write (same class as Silver failure)
- No Gold output cleanup before rerun
- `session_result` missing check only (not empty)

### After (fixes applied)

- `build_gold_feature_mart(..., allow_fallback=False)` — fail fast on Spark errors
- `clean_gold_layer_outputs()` in notebook 03 when `CLEAR_GOLD_OUTPUTS=True`
- Spark Gold: explicit error if `session_result` missing **or** empty
- Optional empty Silver inputs (starting_grid, pit, weather, position, race_control) already handled via left joins + `EVENT_ABSENCE_ZERO_COLS`
- Leakage guard unchanged — raw result columns blocked; `points_finish` retained as target
- DuckDB reads Spark Parquet directories via `_glob_parquet()`

### Remaining risks

- Spark Gold converts full mart to pandas for CSV reports via `safe_to_pandas(max_rows=500_000)` — fine for smoke; full-season runs may truncate report sampling at 500k rows (mart itself is written in Spark)

---

## Notebook 04 — Modeling safety assessment

### Before

- Ad-hoc smoke split via `gold_df.sample()` when season splits empty
- No model output cleanup
- No explicit empty-feature guard
- Heuristic baseline silent when `first_observed_position` missing

### After (fixes applied)

- `resolve_modeling_splits(gold_df, mode)` — full mode raises on empty splits; smoke uses season split when possible else `create_fallback_time_split()`
- Manifest fields: `split_method`, `evidence_tier` (`smoke_wiring_only` in smoke)
- `CLEAR_MODEL_OUTPUTS=True` + `clean_model_outputs()` before run
- Gold mart loaded via `read_parquet_if_exists` (Spark directory-safe)
- Clear failures if mart or feature dictionary missing/empty
- `get_model_feature_columns()` raises if dictionary empty or no allowed features
- Heuristic baseline logs warning when position column missing/all-null
- ROC-AUC already null-safe for single-class splits (`evaluate.py`)

---

## Notebook 05 — Report artifacts safety assessment

### Before

- No cleanup of stale tables/figures
- Otherwise skip-if-missing pattern already in place

### After (fixes applied)

- `CLEAR_REPORT_ARTIFACTS=True` + `clean_report_artifacts()` before building
- Figures: matplotlib only, skip when inputs missing — unchanged
- `write_report_tables` skips empty tables — unchanged
- No fake data generation

---

## DuckDB SQL safety assessment

| Check | Status |
|-------|--------|
| `normalize_key_columns` / `quote_identifier` / `_sql_column_list` | Fixed (prior audit) |
| All GROUP BY via `_group_by_count_report` | OK |
| Missing columns → `skipped_missing_columns` | OK |
| Parquet directories via `_glob_parquet` | OK |
| Empty optional starting_grid (0 rows, valid schema) | COUNT=0, status=ok |
| Unreadable Parquet dirs | `_parquet_readable()` helper added; status=error |

---

## Fallback behavior assessment

| Layer | Dispatcher | Default | Cleanup before fallback |
|-------|------------|---------|-------------------------|
| Silver | `run_silver_cleaning` | `allow_fallback=False` | `clean_silver_layer_outputs` |
| Gold | `build_gold_feature_mart` | `allow_fallback=False` | `clean_gold_layer_outputs` |
| Bronze reports | `generate_bronze_reports` | `allow_fallback=False` | N/A (CSV only; no partial Parquet collision) |

Notebooks 01–03 set `ALLOW_FALLBACK = False`.

---

## Fixes applied (summary)

1. `utils/io.py` — generalized directory/file cleanup helpers
2. `utils/cleanup.py` — layer-specific cleanup bundles
3. `silver/build_silver.py` — delegate cleanup; fallback uses `clean_silver_layer_outputs`
4. `gold/build_feature_mart.py` — `allow_fallback=False`; cleanup-before-fallback
5. `gold/build_feature_mart_spark.py` — session_result empty/missing hard fail
6. `bronze/build_bronze.py` — `allow_fallback=False`
7. `modeling/splits.py` — `resolve_modeling_splits()`
8. `modeling/baselines.py` — heuristic warnings
9. `modeling/train.py` — empty feature dictionary/columns guard
10. `analytics/duckdb_validation.py` — `_parquet_readable()` defensive read
11. `scripts/rebuild_colab_notebooks.py` + notebooks 01–05 — cleanup flags and cells
12. `README.md` — Drive cleanup policy section

---

## Existing Drive outputs before rerun

Path: `/content/drive/MyDrive/openf1_big_data_pipeline`

1. **Partial Silver from failed run:** Notebook 02 with `CLEAR_SILVER_OUTPUTS=True` (default) cleans automatically after push
2. **Never completed 03+:** Safe to run full chain 02→05
3. **Optional manual clean:** Delete `data/silver/`, `data/gold/`, `reports/model_results/`, `reports/tables/`, `reports/figures/` — **do not delete `data/bronze/`** unless re-ingesting

---

## Colab rerun order (after GitHub push)

| Step | Notebook | Key flags |
|------|----------|-----------|
| 00 | Setup | `USE_GOOGLE_DRIVE=True` |
| 01 | Bronze | `CLEAR_BRONZE_OUTPUTS=False`, `ALLOW_FALLBACK=False` |
| 02 | Silver | `CLEAR_SILVER_OUTPUTS=True`, `ALLOW_FALLBACK=False` |
| 03 | Gold | `CLEAR_GOLD_OUTPUTS=True`, `ALLOW_FALLBACK=False` |
| 04 | Modeling | `MODELING_MODE="smoke"`, `CLEAR_MODEL_OUTPUTS=True` |
| 05 | Reports | `CLEAR_REPORT_ARTIFACTS=True` |

Same `OPENF1_DATA_ROOT` in every notebook.

---

## Remaining risks

- Gold Spark report CSVs use pandas conversion of mart (truncated at 500k rows for very large full runs)
- Smoke modeling metrics are wiring-only, not MBA evidence (`evidence_tier=smoke_wiring_only`)
- Mixed Spark-dir/pandas-file artifacts from **pre-fix** runs outside notebook cleanup paths may need one-time manual deletion
- Bronze pandas fallback still possible if explicitly enabled — lower risk (CSV outputs only)
- **Follow-up (2026-05-19):** pandas duplicate reports on lap segment ndarray columns — fixed in `quality/profiling.py` / `duplicates.py` ([spark_silver_duplicate_report_fix.md](./spark_silver_duplicate_report_fix.md))
