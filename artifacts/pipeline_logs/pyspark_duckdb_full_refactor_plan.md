# PySpark / DuckDB full refactor plan

**Date/time:** 2026-05-19  
**Trigger:** MBA technology requirement — pipeline must be **PySpark/DuckDB-first**, not pandas-first.

---

## Why the refactor was needed

The smoke pipeline worked with pandas, but the official architecture must demonstrate:

1. **PySpark** for scalable Medallion transforms (Bronze profiling, Silver cleaning, Gold features).
2. **DuckDB** for SQL-based validation and report-ready tables over Parquet/CSV.
3. **pandas** only at boundaries (CSV export, notebook display, future ML).

Databricks is **explicitly out of scope** (lecturer confirmed).

---

## New official architecture

```
OpenF1 API (Python requests)
    → Bronze JSONL (immutable)
    → PySpark Bronze profiling → CSV reports
    → DuckDB Bronze validation
    → PySpark Silver cleaning → Silver Parquet
    → DuckDB Silver validation
    → PySpark Gold feature mart → Gold Parquet
    → DuckDB Gold validation
    → pandas/sklearn/LightGBM modeling (notebook 04)
```

---

## What changed in Bronze

| Component | Change |
|-----------|--------|
| Ingestion | **Unchanged** — `ingest.py` still writes JSONL |
| Reports | **Spark-first** — `compute_bronze_row_counts_spark`, `build_bronze_schema_report_spark` |
| Entry | `generate_bronze_reports(engine="spark")` with pandas fallback |
| Notebook 01 | `get_spark()`, `BRONZE_REPORT_ENGINE="spark"`, DuckDB validation cell |

---

## What changed in Silver

| Component | Change |
|-----------|--------|
| Primary | `silver/build_silver_spark.py` — `run_silver_cleaning_spark()` |
| Cleaning | Spark DataFrame rules per endpoint (mirror pandas domain rules) |
| Reports | Spark missingness/inventory; pandas RI/outlier/temporal at **report boundary** |
| Entry | `run_silver_cleaning(engine="spark")` with pandas fallback |
| Notebook 02 | `SILVER_ENGINE="spark"`, DuckDB validation |

---

## What changed in Gold

| Component | Change |
|-----------|--------|
| Primary | `gold/build_feature_mart_spark.py` — Spark joins and aggregations |
| Target | `points_finish` from `points > 0` (unchanged) |
| Reports | Spark mart → `safe_to_pandas()` → existing CSV/leakage/dictionary writers |
| Entry | `build_gold_feature_mart(engine="spark")` with pandas fallback |
| Notebook 03 | `GOLD_ENGINE="spark"`, DuckDB validation |

---

## What DuckDB validates

| Layer | Reports (`reports/data_quality/duckdb_*`) |
|-------|-------------------------------------------|
| Bronze | Rows by endpoint, totals, drift flags, endpoint coverage |
| Silver | Table row counts, `session_result` target support, duplicate keys, laps/pit/weather by session |
| Gold | Row count, duplicate keys, target distribution, by team/circuit, missingness sample |

---

## What remains pandas-based and why

| Use | Reason |
|-----|--------|
| `generate_bronze_reports_pandas` | Fallback if Spark unavailable |
| `run_silver_cleaning_pandas` | Fallback; reference implementation |
| `build_gold_feature_mart_pandas` | Fallback; reference implementation |
| RI / outlier / temporal Silver reports | Complex logic reused at report boundary (post-Spark clean) |
| CSV writers (`save_dataframe_csv`) | Small files for MBA evidence |
| Notebook `display()` | Human inspection |
| Future modeling | Gold mart is small enough for sklearn |

---

## Risks introduced

1. **Colab Spark startup** — Java/memory; first cell may be slow.
2. **Empty Bronze endpoints** — Spark empty DataFrames need careful handling (`starting_grid`).
3. **Schema inference** — Spark JSON schema may differ slightly from pandas for rare columns.
4. **Join quality metrics** — Spark path uses distinct-key sets via small pandas extracts.
5. **Parquet write layout** — Spark writes directories; DuckDB uses `read_parquet('path/**/*.parquet')`.

---

## Recommended smoke run order

1. `00_colab_setup.ipynb` — verify imports including `pyspark`, `duckdb`
2. `01` — `SMOKE_TEST=True`, Spark Bronze reports, DuckDB bronze CSV validation
3. `02` — Spark Silver + DuckDB silver validation
4. `03` — Spark Gold + DuckDB gold validation
5. Copy outputs to `evidence/smoke_2024_maxsessions2/`

---

## Rollback / fallback strategy

Set in notebooks:

```python
BRONZE_REPORT_ENGINE = "pandas"
SILVER_ENGINE = "pandas"
GOLD_ENGINE = "pandas"
```

Or rely on automatic fallback: Spark entry points log a warning and call pandas implementations on exception.

---

## Files added/updated (implementation)

- `src/openf1_pipeline/utils/spark.py`
- `src/openf1_pipeline/analytics/duckdb_validation.py`
- `src/openf1_pipeline/silver/build_silver_spark.py`
- `src/openf1_pipeline/gold/build_feature_mart_spark.py`
- `src/openf1_pipeline/bronze/build_bronze.py` (Spark report functions)
- `src/openf1_pipeline/silver/build_silver.py` (dispatcher)
- `src/openf1_pipeline/gold/build_feature_mart.py` (dispatcher)
- `scripts/rebuild_colab_notebooks.py` (01–03 Spark + DuckDB cells)
- `README.md`, `project_context.md`, `implementation_checklist.md`

**Notebooks were not executed in Cursor** — verify on Colab Pro Plus.
