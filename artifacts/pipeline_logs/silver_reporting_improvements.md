# Silver reporting improvements (pre–Phase 3)

**Date:** 2026-05-19

## Changes

1. **Granular `silver_cleaning_rules.csv`** — Spark cleaners log per-step rules (`SIL_RENAME`, `SIL_CAST_*`, `SIL_DROP_NULL_KEYS`, domain filters, dedupe). Pandas cleaners log schema prep via `log_schema_prep`. Columns include `values_imputed` and `columns_affected` (always 0 imputed in Silver).

2. **Rejected-records template** — `build_rejected_records_summary()` writes one row per Silver table with `rule_id=SIL_NONE`, `rejected_count=0` when no rows were removed.

3. **`silver_data_quality_notes.csv`** — Static documentation for structural nulls (race_control, pit, session_result, laps) and non-blocking `weather_outside_session_bounds`.

4. **Temporal report note** — `weather_outside_session_bounds` rows in `silver_temporal_anomaly_report.csv` reference the notes file.

## Re-freeze evidence

After re-running `02_silver_cleaning_quality.ipynb` (Spark), copy updated CSVs into `evidence/smoke_2024_maxsessions2_spark/reports/data_quality/`.
