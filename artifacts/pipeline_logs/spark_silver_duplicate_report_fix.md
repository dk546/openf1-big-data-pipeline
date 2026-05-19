# Spark Silver — duplicate report unhashable column fix

**Date:** 2026-05-19  
**Notebook:** `02_silver_cleaning_quality.ipynb`  
**Scope:** Duplicate reporting only (Silver cleaning logic unchanged)

---

## Error observed (Colab)

Spark Silver **successfully wrote all 10 Silver Parquet tables**, including empty `starting_grid` (0 rows). Report generation then failed:

```
TypeError: unhashable type: 'numpy.ndarray'
```

Trace:

- `build_silver_spark.py` → `build_duplicate_report()`
- `duplicates.py` → `detect_full_duplicates()`
- `profiling.py` → `_count_full_row_duplicates()` → `df.duplicated()`

Likely columns: OpenF1 lap segment fields such as `segments_sector_1`, `segments_sector_2`, `segments_sector_3` (list/ndarray payloads from JSONL).

---

## Root cause

Pandas `DataFrame.duplicated()` requires hashable cell values. OpenF1 Bronze/Silver lap rows retain nested **lists**, **dicts**, and **numpy.ndarray** values in object columns. The prior fallback only converted columns where `isinstance(v, (list, dict))` — it did **not** handle `numpy.ndarray`, so the second `duplicated()` call still raised `TypeError`.

Silver **cleaning and Parquet writes had already completed**; only the pandas audit CSV step failed.

---

## Fix applied

### `quality/profiling.py`

- `make_hashable_for_duplicate_checks()` — JSON-stable normalization for list/tuple/set/ndarray/dict; `str()` fallback on error
- `make_dataframe_hashable_for_duplicates()` — converts only object columns containing unhashable values
- `_count_full_row_duplicates()` — uses hashable copy; returns `(count, note)`; logs warning and returns 0 on persistent failure
- `_self_check_duplicate_counting()` — lightweight internal sanity check

### `quality/duplicates.py`

- `detect_full_duplicates()` — adds `duplicate_check_note` when conversion or skip occurs
- `detect_key_duplicates()` — uses key columns only; separate `skipped_empty` vs `skipped_missing_columns`; hashable fallback for key subset

### `silver/build_silver_spark.py`

- TODO note: full-season runs may prefer Spark-native key-duplicate counts; pandas boundary OK for smoke

---

## How to rerun notebook 02

1. Push/pull latest code in Colab.
2. Run with `CLEAR_SILVER_OUTPUTS=True`, `ALLOW_FALLBACK=False` (defaults).
3. Spark Silver should complete **and** write `silver_duplicate_report.csv` without error.

Existing Silver Parquet on Drive from the partial run can be kept or cleaned via `clean_silver_layer_outputs()` before rerun.

---

## Files modified

| File | Change |
|------|--------|
| `src/openf1_pipeline/quality/profiling.py` | Hashable helpers, robust `_count_full_row_duplicates`, self-check |
| `src/openf1_pipeline/quality/duplicates.py` | Notes on full dupes; safer key dupes |
| `src/openf1_pipeline/silver/build_silver_spark.py` | TODO for full-run Spark-native dup reporting |
