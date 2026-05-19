# Spark Silver — starting_grid empty schema & fallback fix

**Date:** 2026-05-19  
**Notebook:** `02_silver_cleaning_quality.ipynb`  
**Scope:** Silver layer only (no Gold/modeling changes)

---

## Error observed (Colab)

Spark Silver wrote 9 of 10 tables, then failed on `starting_grid`:

```
EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE
The Parquet datasource does not support writing empty or nested empty schemas.
```

`run_silver_cleaning()` then **automatically fell back to pandas**. Spark had already written Parquet as **directories** (e.g. `meetings_clean.parquet/`). Pandas tried to write **files** at the same paths and failed:

```
IsADirectoryError: Failed to open local file .../meetings_clean.parquet. Detail: Is a directory
```

---

## Root cause

1. **`starting_grid` is optional** — smoke data has 0 rows / no Bronze JSONL files.
2. Spark Silver used `_empty_df()` with `StructType()` (zero columns). Parquet cannot be written without at least one column.
3. **`run_silver_cleaning()` always fell back to pandas** on any Spark exception, even after partial Spark outputs were written.
4. Spark writes Parquet as directories; pandas writes single files — mixed outputs collide on the same path.

---

## Fix applied

### 1. Optional `starting_grid` with empty schema Parquet (option A)

- `build_silver_spark.py`: `SILVER_EMPTY_SCHEMAS["starting_grid"]` with columns:
  `session_key`, `meeting_key`, `driver_number`, `position`, `source_endpoint`, `source_year`, `source_session_key`
- Empty optional grid writes `starting_grid_clean.parquet/` (0 rows, valid schema).
- Cleaning rule: `SIL_OPTIONAL_MISSING` — "starting_grid optional — no Bronze files or zero rows".

### 2. Safe empty DataFrame writing

- `utils/spark.py`: `write_empty_parquet_with_schema(spark, path, schema)`
- `write_spark_dataframe(..., empty_schema=...)` refuses zero-column frames without a schema.
- `build_silver_spark.py`: `_write_silver_table()` uses endpoint-specific fallback schemas for all Silver endpoints.

### 3. Disabled unsafe automatic fallback

- `build_silver.py`: `run_silver_cleaning(..., allow_fallback=False)` **default**.
- Spark failure with `allow_fallback=False` → raises `RuntimeError` with clear message (no silent pandas switch).
- If `allow_fallback=True` → `clean_silver_output_dir(silver_dir)` runs **before** pandas fallback.

### 4. Silver output cleanup utility

- `build_silver.py`: `clean_silver_output_dir(silver_dir)` removes files and directories under Silver only.
- Notebook 02 calls this explicitly before each Spark run.

---

## Why `starting_grid` is optional

OpenF1 may return **404** or empty payloads for `starting_grid` depending on session type and API availability. Gold can use sessions/drivers heuristics when grid data is absent. The pipeline must complete Silver without treating missing grid as a hard failure.

---

## Why automatic fallback was disabled

Partial Spark writes + pandas retry is worse than failing fast:

- Leaves inconsistent mixed formats (Spark dirs vs pandas files).
- Masks the real Spark bug (empty schema).
- Official architecture is **Spark-first**; pandas is a manual boundary/fallback only.

Notebook 02 now sets:

```python
SILVER_ENGINE = "spark"
ALLOW_FALLBACK = False
```

---

## How to rerun notebook 02

1. Push latest code to GitHub and re-clone / pull in Colab.
2. Run `00` and `01` with the same `USE_GOOGLE_DRIVE` setting (Bronze must exist).
3. Run `02_silver_cleaning_quality.ipynb`:
   - Spark session starts
   - `clean_silver_output_dir(SILVER_DIR)` runs
   - `run_silver_cleaning(..., allow_fallback=False)` completes all 10 `*_clean.parquet` outputs
   - DuckDB validation cells run on Silver Parquet
4. Expected smoke outputs include empty `starting_grid_clean.parquet` with `SIL_OPTIONAL_MISSING` in `silver_cleaning_rules.csv`.

---

## Files modified

| File | Change |
|------|--------|
| `src/openf1_pipeline/utils/spark.py` | `write_empty_parquet_with_schema`, safe `write_spark_dataframe` |
| `src/openf1_pipeline/silver/build_silver_spark.py` | Endpoint empty schemas, `_write_silver_table`, starting_grid optional rule |
| `src/openf1_pipeline/silver/build_silver.py` | `clean_silver_output_dir`, `allow_fallback=False` default |
| `scripts/rebuild_colab_notebooks.py` | Notebook 02 cleanup cell + `ALLOW_FALLBACK` |
| `notebooks/02_silver_cleaning_quality.ipynb` | Regenerated from rebuild script |
