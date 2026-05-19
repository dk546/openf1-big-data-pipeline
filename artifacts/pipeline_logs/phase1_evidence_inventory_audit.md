# Phase 1 Evidence Inventory & Run Consistency Audit

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T15:51:37Z |
| **Evidence folder** | `evidence/smoke_2024_maxsessions2_spark/` |
| **Run profile** | `SMOKE_TEST=True`, `MAX_SESSIONS=2`, `INGEST_SEASONS=[2024]`, `BRONZE_REPORT_ENGINE=spark`, `SILVER_ENGINE=spark`, `GOLD_ENGINE=spark` |
| **Sessions ingested** | `9472`, `9480` (2024 season smoke) |
| **Files in evidence bundle** | 37 CSV artifacts (no parquet or feature dictionary copied) |

## Files reviewed

All CSV files under `evidence/smoke_2024_maxsessions2_spark/`:

- `artifacts/manifests/ingestion_manifest.csv`
- `artifacts/schemas/bronze_schema_report.csv`
- `reports/data_quality/bronze_file_inventory.csv`
- `reports/data_quality/bronze_row_counts.csv`
- `reports/data_quality/bronze_schema_report.csv`
- `reports/data_quality/bronze_schema_drift.csv`
- `reports/data_quality/duckdb_bronze_bronze_total_rows.csv`
- `reports/data_quality/duckdb_bronze_bronze_rows_by_endpoint.csv`
- `reports/data_quality/duckdb_bronze_bronze_endpoint_coverage.csv`
- `reports/data_quality/duckdb_bronze_bronze_schema_drift_flags.csv`
- Silver: `silver_table_inventory.csv`, `silver_missingness_before.csv`, `silver_missingness_after.csv`, `silver_duplicate_report.csv`, `silver_outlier_report.csv`, `silver_temporal_anomaly_report.csv`, `silver_referential_integrity_report.csv`, `silver_cleaning_rules.csv`, `silver_cleaning_impact_summary.csv`, `silver_rejected_records_summary.csv`
- Silver DuckDB: `duckdb_silver_silver_row_counts.csv`, `duckdb_silver_session_result_duplicate_keys.csv`, `duckdb_silver_session_result_target_support.csv`, `duckdb_silver_laps_rows_by_session_key.csv`, `duckdb_silver_pit_rows_by_session_key.csv`, `duckdb_silver_weather_rows_by_session_key.csv`
- Gold: `gold_feature_summary_stats.csv`, `gold_feature_missingness.csv`, `gold_target_distribution.csv`, `gold_join_quality_report.csv`, `gold_leakage_guard_report.csv`
- Gold DuckDB: `duckdb_gold_gold_row_count.csv`, `duckdb_gold_gold_target_distribution.csv`, `duckdb_gold_gold_duplicate_keys.csv`, `duckdb_gold_gold_missingness_summary.csv`, `duckdb_gold_points_finish_by_team.csv`, `duckdb_gold_points_finish_by_circuit.csv`

**Not present in evidence folder (checked):**

- `artifacts/feature_definitions/feature_dictionary.csv`
- `data/gold/driver_race_feature_mart.parquet/` (or any parquet under evidence)

---

## 1. Evidence folder completeness

| Layer | Expected file | Found | Notes |
|-------|---------------|-------|-------|
| Bronze | `artifacts/manifests/ingestion_manifest.csv` | Yes | 18 manifest rows; 16 success, 2 failed (`starting_grid`) |
| Bronze | `artifacts/schemas/bronze_schema_report.csv` | Yes | Matches `reports/data_quality/bronze_schema_report.csv` (sampled) |
| Bronze | `reports/data_quality/bronze_file_inventory.csv` | Yes | 16 files (successful endpoints only) |
| Bronze | `reports/data_quality/bronze_row_counts.csv` | Yes | 16 file-level rows |
| Bronze | `reports/data_quality/bronze_schema_report.csv` | Yes | Duplicate of artifacts copy |
| Bronze | `reports/data_quality/bronze_schema_drift.csv` | Yes | 106 column rows; all `possible_schema_drift_flag=False` |
| Bronze | `reports/data_quality/duckdb_bronze_*.csv` | Yes | 4 files present |
| Silver | `silver_table_inventory.csv` | Yes | |
| Silver | `silver_missingness_before.csv` | Yes | |
| Silver | `silver_missingness_after.csv` | Yes | |
| Silver | `silver_duplicate_report.csv` | Yes | |
| Silver | `silver_outlier_report.csv` | Yes | |
| Silver | `silver_temporal_anomaly_report.csv` | Yes | |
| Silver | `silver_referential_integrity_report.csv` | Yes | |
| Silver | `silver_cleaning_rules.csv` | Yes | |
| Silver | `silver_cleaning_impact_summary.csv` | Yes | |
| Silver | `silver_rejected_records_summary.csv` | Yes | Header only (no rejections) |
| Silver | `reports/data_quality/duckdb_silver_*.csv` | Yes | 7 files present |
| Gold | `gold_feature_summary_stats.csv` | Yes | Mart `count=40` |
| Gold | `gold_feature_missingness.csv` | Yes | `row_count=40` on all columns |
| Gold | `gold_target_distribution.csv` | Yes | |
| Gold | `gold_join_quality_report.csv` | Yes | |
| Gold | `gold_leakage_guard_report.csv` | Yes | |
| Gold | `reports/data_quality/duckdb_gold_*.csv` | Yes | 5 files present |
| Gold | `artifacts/feature_definitions/feature_dictionary.csv` | **No** | Not copied into evidence bundle |
| Gold | `data/gold/driver_race_feature_mart.parquet/` | **No** | Parquet not included in frozen evidence |

**Completeness summary:** All requested CSV quality reports are present (37/37 CSV paths under evidence). Two optional Gold artifacts (feature dictionary, parquet mart) were not frozen in this folder.

---

## 2. Bronze consistency

| Check | Result | Detail |
|-------|--------|--------|
| Manifest successful row sum | **3,690** | Sum of `record_count` where `status=success` |
| `bronze_row_counts.csv` total | **3,690** | Sum of `row_count` across 16 files |
| DuckDB `bronze_total_rows` | **3,690** | `file_count=16` |
| Per-endpoint totals | **Match** | Manifest success aggregates = `bronze_row_counts` = `duckdb_bronze_rows_by_endpoint` |
| Failed endpoints | **2** | `starting_grid` sessions 9472, 9480 — HTTP 404 (no API data) |
| `session_result` rows | **40** | 20 per session (9472, 9480) |
| `starting_grid` | **0 rows** | Failed ingestion; no bronze files in inventory |
| Schema drift (Spark report) | **No drift** | All flags `False` in `bronze_schema_drift.csv` |
| DuckDB schema drift flags | **Empty file** | `duckdb_bronze_bronze_schema_drift_flags.csv` has header only |
| File inventory vs manifest | **Consistent** | 16 files; timestamps align with manifest (`2026-05-19T14:01:45`–`14:01:52` UTC) |

**Bronze verdict:** Yes — Bronze evidence consistently describes the same smoke run. Row totals and endpoint breakdowns agree across manifest, Spark row counts, file inventory, and DuckDB validation.

---

## 3. Silver consistency

| Check | Result | Detail |
|-------|--------|--------|
| `silver_table_inventory` vs Bronze | **Match** | All endpoint row counts equal Bronze (e.g. `session_result=40`, `laps=2031`) |
| `silver_cleaning_impact_summary` | **No row loss** | `rows_before` = `rows_after` for every table; `session_result` 40→40 |
| `session_result` row count | **40** | Reports use table name `session_result` (not `session_result_clean`) |
| `starting_grid` | **0 rows** | Aligns with Bronze API failure; duplicate/integrity checks `skipped_empty` |
| DuckDB `silver_row_counts` | **Match** | Identical counts and `status=ok` for all 10 tables |
| Rejected records | **0** | `silver_rejected_records_summary.csv` empty |
| Referential integrity | **Pass** | 0 unmatched rows on checked joins; `starting_grid` skipped as empty |

**Silver verdict:** Yes — Silver evidence consistently describes the same smoke run and preserves Bronze row volumes (no cleaning removals in this smoke).

**Naming note:** Evidence uses `session_result` in Silver reports, not `session_result_clean`. Row count 40 is the cleaned Silver output for the driver-race grain source.

---

## 4. Gold consistency

| Check | Result | Detail |
|-------|--------|--------|
| Gold mart row count | **40** | `gold_feature_summary_stats.csv` (`count` on `session_key`) |
| vs Silver `session_result` | **40 = 40** | Expected 1:1 driver-session grain |
| DuckDB `gold_row_count` | **40** | Matches Spark/pandas Gold reports |
| Target distribution | **50% / 50%** | `points_finish`: 20 zeros, 20 ones — agrees with DuckDB `gold_target_distribution` and `session_result_target_support` (20 positive points) |
| Duplicate grain keys | **0** | `duckdb_gold_gold_duplicate_keys.csv` empty; Silver duplicate check 0 on `(session_key, driver_number)` |
| Join quality | **100% matched** | All feature groups: 40 rows in, 40 out, 0 unmatched |
| Leakage guard | **Present** | Target and post-race fields flagged; modeling features marked appropriately |

**Gold verdict:** Yes — Gold evidence consistently describes the same smoke run with a 40-row driver-race feature mart.

---

## 5. Cross-layer row-flow audit

| Stage | Metric | Rows | Expected relationship | Pass/Fail |
|-------|--------|------|------------------------|-----------|
| Bronze | `session_result` (all files) | **40** | Base driver-session outcomes | — |
| Silver | `session_result` (cleaned) | **40** | Should equal Bronze `session_result` | **PASS** |
| Gold | `driver_race_feature_mart` | **40** | Should equal Silver `session_result` grain | **PASS** |

**Other endpoints (informational):** Laps 2031, position 927, etc. flow Bronze→Silver with zero row removal; Gold aggregates to driver-session grain (40), not lap-level row parity.

---

## 6. Staleness audit

| Finding | Severity | Description |
|---------|----------|-------------|
| Ingestion timestamps | OK | Single run window `2026-05-19T14:01:45`–`14:01:53` UTC across manifest and file inventory |
| Cross-report row totals | OK | No mismatches between manifest, Bronze, Silver, Gold, or DuckDB CSVs |
| Duplicate schema reports | Info | `artifacts/schemas/bronze_schema_report.csv` and `reports/.../bronze_schema_report.csv` appear identical |
| DuckDB bronze drift export | Minor | `duckdb_bronze_bronze_schema_drift_flags.csv` empty while Spark `bronze_schema_drift.csv` is populated — validation export gap, not data inconsistency |
| Missing evidence copies | Minor | `feature_dictionary.csv` and Gold parquet not in frozen folder — cannot independently verify mart bytes from evidence alone |
| `starting_grid` | Documented | API 404 for both sessions; consistently 0 rows Bronze/Silver — not stale, expected for this smoke |

No evidence of mixed runs or stale row counts from an earlier execution.

---

## 7. Phase 1 verdict

### **PASS WITH MINOR ISSUES**

Evidence is **internally consistent** for the `smoke_2024_maxsessions2_spark` run. Minor issues are limited to:

1. Empty DuckDB bronze schema-drift flags export (Spark drift report is fine).
2. Gold feature dictionary and parquet mart not included in the frozen evidence folder.
3. Silver table naming uses `session_result` rather than `session_result_clean` in CSV reports (row counts still align).

These do **not** block using the evidence for Phase 2 Silver data-quality deep dive.

---

## 8. Recommended next step

Proceed to **Phase 2: Silver data quality deep dive**, focusing on:

- Missingness patterns (`silver_missingness_before` / `after`) — especially `race_control`, `laps`, `pit`
- Outlier and temporal anomaly reports for lap/pit features
- Documented `starting_grid` API absence and impact on any grid-derived features
- Optionally copy `feature_dictionary.csv` and a Gold parquet sample into evidence for Phase 3 modeling readiness

---

*Audit performed by automated inventory script and manual CSV cross-checks. No code or notebooks were modified.*
