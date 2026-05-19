# PySpark / DuckDB Pre-Colab Audit

**Audit date/time:** 2026-05-19 (static code review; no notebooks executed)

**Auditor:** Cursor agent (pre-Colab smoke readiness review)

**Verdict:** **Ready after minor fixes**

Local codebase and notebook defaults are smoke-ready. Colab clone path is blocked until the refactor is committed and pushed to GitHub.

---

## Files reviewed

| File | Role |
|------|------|
| `README.md` | Architecture documentation |
| `requirements.txt` | Runtime deps (`pyspark>=3.5.0`, `duckdb>=0.9.0`) |
| `pyproject.toml` | Editable package scaffold |
| `src/openf1_pipeline/config.py` | Paths, seasons, endpoints |
| `src/openf1_pipeline/utils/spark.py` | Local Spark session, Parquet/JSONL I/O |
| `src/openf1_pipeline/utils/io.py` | JSONL/CSV/Parquet helpers |
| `src/openf1_pipeline/analytics/duckdb_validation.py` | DuckDB Bronze/Silver/Gold validation |
| `src/openf1_pipeline/ingestion/openf1_client.py` | HTTP client |
| `src/openf1_pipeline/ingestion/ingest.py` | Bronze JSONL ingestion |
| `src/openf1_pipeline/bronze/build_bronze.py` | Spark/pandas Bronze reports |
| `src/openf1_pipeline/silver/build_silver.py` | Silver dispatcher |
| `src/openf1_pipeline/silver/build_silver_spark.py` | Spark Silver cleaning + DQ reports |
| `src/openf1_pipeline/gold/build_feature_mart.py` | Gold dispatcher + pandas reference |
| `src/openf1_pipeline/gold/build_feature_mart_spark.py` | Spark Gold feature mart |
| `src/openf1_pipeline/features/feature_dictionary.py` | Feature dictionary generation |
| `notebooks/00_colab_setup.ipynb` | Environment validation |
| `notebooks/01_ingestion_bronze.ipynb` | Bronze ingestion + Spark reports + DuckDB |
| `notebooks/02_silver_cleaning_quality.ipynb` | Spark Silver + DuckDB |
| `notebooks/03_gold_feature_engineering.ipynb` | Spark Gold + DuckDB |
| `artifacts/pipeline_logs/pyspark_duckdb_full_refactor_plan.md` | Refactor plan |

**Notebook execution status:** All notebooks `00`–`03` have `execution_count: null` and empty outputs — **no notebooks were executed after the Spark refactor**.

---

## Readiness checklist

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 1 | Python `requests` handles OpenF1 API ingestion | PASS | `ingest.py` + `openf1_client.py` unchanged |
| 2 | Bronze raw JSONL preserved | PASS | `save_jsonl` → `data/bronze/{endpoint}/year=…/*.jsonl` |
| 3 | Bronze reports use Spark by default | PASS | `generate_bronze_reports(engine="spark")` |
| 4 | Silver cleaning uses Spark by default | PASS | `run_silver_cleaning(engine="spark")` → `build_silver_spark.py` |
| 5 | Gold feature mart uses Spark by default | PASS | `build_gold_feature_mart(engine="spark")` → `build_feature_mart_spark.py` |
| 6 | DuckDB validations for Bronze, Silver, Gold | PASS | `duckdb_validation.py` + cells in notebooks 01–03 |
| 7 | pandas not primary data engineering engine | PASS | pandas at CSV export, report boundary, ML handoff only |
| 8 | Notebook 01: `BRONZE_REPORT_ENGINE = "spark"` | PASS | Cell in `01_ingestion_bronze.ipynb` |
| 9 | Notebook 02: `SILVER_ENGINE = "spark"` | PASS | Cell in `02_silver_cleaning_quality.ipynb` |
| 10 | Notebook 03: `GOLD_ENGINE = "spark"` | PASS | Cell in `03_gold_feature_engineering.ipynb` |
| 11 | All notebooks: `USE_GOOGLE_DRIVE = True` | PASS | Notebooks 00–03 setup cell section B |
| 12 | All notebooks: `OPENF1_DATA_ROOT` before config import | PASS | Set in section B before `import openf1_pipeline` |
| 13 | All notebooks: `pip install -r requirements.txt` + `pip install -e .` | PASS | Setup cell section F |
| 14 | Standardized Colab setup pattern (A–I) | PASS | Identical across 00–03 |
| 15 | `get_spark()` works in local Colab mode | PASS | `local[*]`, 4g driver, UTC, adaptive shuffle |
| 16 | Spark paths compatible with Google Drive | PASS | `spark_path()` resolves + forward slashes |
| 17 | Spark reads JSONL and Parquet from Drive | PASS | `read_spark_jsonl`, `read_spark_parquet_if_exists` |
| 18 | Spark writes Parquet to Drive | PASS | `write_spark_dataframe` |
| 19 | Spark reports → pandas only after aggregation | PASS | `safe_to_pandas`, Bronze CSV via pandas |
| 20 | DuckDB reads CSV/Parquet via local/Drive paths | PASS | `as_posix()` + `_glob_parquet()` |
| 21 | DuckDB reports under `reports/data_quality/` | PASS | `save_duckdb_validation_reports` → `duckdb_{prefix}_*.csv` |
| 22 | DuckDB validation avoids loading huge data into pandas | PASS | SQL `COUNT`/`GROUP BY` over Parquet globs |
| 23 | API ingestion saves raw JSONL | PASS | `ingest.py` |
| 24 | Spark Bronze reports after ingestion | PASS | Notebook 01 cell order |
| 25 | DuckDB Bronze validation uses Bronze CSV reports | PASS | `validate_bronze_with_duckdb(data_quality_reports_dir)` |
| 26 | `starting_grid` optional | PASS | `OPTIONAL_SESSION_ENDPOINTS`; empty Spark cleaner |
| 27 | `session_result` required for target | PASS | Gold raises if empty; ingest always attempts |
| 28 | Spark Silver reads Bronze JSONL endpoint folders | PASS | `discover_bronze_jsonl_by_endpoint` + `read_spark_jsonl` |
| 29 | Spark Silver writes `*_clean.parquet` | PASS | `write_spark_dataframe` per endpoint |
| 30 | All 10 Silver DQ reports produced | PASS | See Silver report list below |
| 31 | pandas Silver fallback available, not default | PASS | `engine="pandas"` or Spark exception fallback |
| 32 | Spark Gold reads Silver Parquet | PASS | `read_spark_parquet_if_exists` |
| 33 | Target: `points_finish = 1 if points > 0` | PASS | `build_target_base_spark` |
| 34 | Gold grain: session_key, meeting_key, driver_number | PASS | `DRIVER_KEYS` + duplicate check |
| 35 | Leakage guard excludes result columns | PASS | `LEAKAGE_FORBIDDEN_COLUMNS` + `build_leakage_guard_report` |
| 36 | Feature dictionary produced | PASS | `write_feature_dictionary` in Spark Gold path |
| 37 | DuckDB Gold validation after Gold output | PASS | Notebook 03 DuckDB cell |
| 38 | GitHub sync for Colab clone | **FAIL** | Refactor not pushed (see Issues) |

### Silver DQ reports (all required)

- `silver_table_inventory.csv`
- `silver_missingness_before.csv`
- `silver_missingness_after.csv`
- `silver_duplicate_report.csv`
- `silver_outlier_report.csv`
- `silver_temporal_anomaly_report.csv`
- `silver_referential_integrity_report.csv`
- `silver_cleaning_rules.csv`
- `silver_cleaning_impact_summary.csv`
- `silver_rejected_records_summary.csv`

---

## Architecture (confirmed)

```
OpenF1 API (Python requests)
    → Bronze JSONL (immutable)
    → PySpark Bronze profiling → CSV reports
    → DuckDB Bronze validation
    → PySpark Silver cleaning → Silver Parquet (directories)
    → DuckDB Silver validation
    → PySpark Gold feature mart → Gold Parquet (directory)
    → DuckDB Gold validation
    → pandas/sklearn/LightGBM modeling (notebook 04, planned)
```

Databricks is out of scope.

---

## Issues found

### Blocker — refactor not on GitHub (Colab clone path)

At audit time, `git status` showed uncommitted/unpushed refactor:

- **Untracked:** `src/openf1_pipeline/utils/spark.py`, `src/openf1_pipeline/analytics/`, `build_silver_spark.py`, `build_feature_mart_spark.py`
- **Modified:** Bronze/Silver/Gold dispatchers, notebooks 01–03, README, etc.
- **Remote HEAD:** `115ad2d Saving smoke test evidence` (pre-Spark era)

Every notebook clones `https://github.com/dk546/openf1-big-data-pipeline.git` and runs `git pull`. **Colab will not receive Spark modules or updated notebooks until commit + push.**

### Minor — silent Spark → pandas fallback

Dispatchers catch Spark exceptions and fall back to pandas with a log warning. Smoke runs meant to validate Spark could succeed on pandas without an obvious notebook error.

**Mitigation:** Confirm `outputs["summary"]["engine"] == "spark"` in notebooks 01–03; watch logs for `"falling back to pandas"`.

### Minor — `00_colab_setup.ipynb` does not verify PySpark/DuckDB

Setup validates repo files and package import but does not call `get_spark()` or `import duckdb`. First Spark failure surfaces in notebook 01.

### Operational risks (no code change)

| Risk | Impact | Mitigation |
|------|--------|------------|
| Stale Drive data from pre-refactor pandas run | Mixed layouts / wrong row counts | Clear `data/`, `reports/`, `artifacts/` on Drive before smoke |
| Spark startup latency / Java | Slow first cell, occasional init failure | Wait 1–3 min; restart runtime if needed |
| Silver report boundary memory (full run) | OOM on 2023–2025 laps/position | Smoke (2 sessions) is fine; monitor on full run |
| `session_result` zero rows on smoke | Gold fails | Check manifest after notebook 01 |
| Evidence folder stale | Misleading pre-Spark CSVs | `evidence/smoke_2024_maxsessions2/` is pandas-era only |

---

## Fixes applied

| Fix | File | Status |
|-----|------|--------|
| `read_parquet_if_exists` accepts Spark Parquet directories | `src/openf1_pipeline/utils/io.py` | **Applied** |
| DuckDB Silver GROUP BY treats `"session_key"` as one column | `src/openf1_pipeline/analytics/duckdb_validation.py` | **Applied** — see `duckdb_validation_fix.md` |
| Full audit report | `artifacts/pipeline_logs/pyspark_duckdb_pre_colab_audit.md` | **Applied** |

### `read_parquet_if_exists` change

Before: only `path.is_file()` — pandas fallback could not read Spark-written `{table}_clean.parquet` directories.

After:

```python
def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    path = Path(path)
    if not (path.is_file() or path.is_dir()):
        return None
    return pd.read_parquet(path)
```

---

## Remaining actions (user)

### Required before Colab smoke

1. **Commit and push** the full refactor to `origin/main`, including:
   - `src/openf1_pipeline/utils/spark.py`
   - `src/openf1_pipeline/analytics/`
   - `src/openf1_pipeline/silver/build_silver_spark.py`
   - `src/openf1_pipeline/gold/build_feature_mart_spark.py`
   - Modified dispatchers, notebooks, README, `requirements.txt`
   - This audit report and `io.py` fix

2. **Clear or use fresh Drive output root** at `/content/drive/MyDrive/openf1_big_data_pipeline` before smoke.

3. **After each notebook**, verify `outputs["summary"]["engine"] == "spark"` (where applicable).

---

## Exact Colab smoke run order

| Step | Notebook | Key settings | Success signals |
|------|----------|--------------|-----------------|
| 0 | (optional) Clear Drive output root | `USE_GOOGLE_DRIVE=True` | Empty or fresh folder |
| 1 | `00_colab_setup.ipynb` | Standard setup | Paths under Drive; imports OK |
| 2 | `01_ingestion_bronze.ipynb` | `SMOKE_TEST=True`, `MAX_SESSIONS=2`, `INGEST_SEASONS=[2024]`, `BRONZE_REPORT_ENGINE="spark"` | JSONL in `data/bronze/`; manifest; Bronze CSV reports; `duckdb_bronze_*`; `session_result` rows > 0 |
| 3 | `02_silver_cleaning_quality.ipynb` | `SILVER_ENGINE="spark"` | 10 `silver_*.csv` reports; `*_clean.parquet` dirs; `engine=="spark"`; `duckdb_silver_*` |
| 4 | `03_gold_feature_engineering.ipynb` | `GOLD_ENGINE="spark"` | `driver_race_feature_mart.parquet` dir; leakage guard; feature dictionary; `duckdb_gold_*`; `engine=="spark"` |
| 5 | Copy to evidence | Per `evidence/smoke_2024_maxsessions2/EVIDENCE_FREEZE_CHECKLIST.md` | Include new `duckdb_*` reports |

**Per-notebook cell order:** Setup → `get_spark()` → prerequisites → pipeline run → DuckDB validation → inspect.

---

## Final verdict

**Ready after minor fixes**

| Layer | Assessment |
|-------|------------|
| Local codebase | PySpark/DuckDB-first architecture implemented; notebook defaults correct |
| Code fixes | `read_parquet_if_exists` applied for Parquet-directory compatibility |
| Colab smoke today | **Blocked until GitHub push**; then run with clean Drive state |

**No notebooks were executed during this audit.**
