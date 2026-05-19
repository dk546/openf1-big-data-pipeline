# Silver Output Audit Report

**Audit date (UTC):** 2026-05-19 (re-audit after evidence folder update)  
**Scope:** Bronze + Silver smoke (`SMOKE_TEST=True`, `MAX_SESSIONS=2`, `INGEST_SEASONS=[2024]`)  
**Authoritative evidence folder:** `evidence/smoke_2024_maxsessions2/`

---

## Executive summary

| Question | Verdict |
|----------|---------|
| Evidence bundle complete? | **Yes** — Bronze + all 9 Silver CSVs in evidence (temporal copied from reports post-audit) |
| Silver smoke passed? | **Yes** |
| `session_result_clean` usable for Gold? | **Yes** (40 rows; `position` in Bronze schema) |
| Cleaning evidence report-ready? | **Yes** |
| Proceed to full Bronze/Silver? | **Yes** |
| **Final verdict** | **Silver smoke passed** |

---

## 1. Evidence freshness

### Folder status

`evidence/smoke_2024_maxsessions2/` exists with **16 files** (updated Colab/Drive copy).

### Bronze evidence — now complete

| File | Status | Detail |
|------|--------|--------|
| `artifacts/manifests/ingestion_manifest.csv` | **Present** | 18 rows; Drive paths; 16 success, 2 `starting_grid` failed |
| `reports/data_quality/bronze_row_counts.csv` | **Present** | **16 files**, **3,690 rows** |
| `reports/data_quality/bronze_file_inventory.csv` | **Present** | 16 files |
| `reports/data_quality/bronze_schema_report.csv` | **Present** | **106** columns, 9 endpoints incl. `session_result` |
| `reports/data_quality/bronze_schema_drift.csv` | **Present** | **0** drift flags |
| `artifacts/schemas/bronze_schema_report.csv` | **Present** | Copy of schema report |

**Manifest reconciliation:**

| Metric | Value |
|--------|-------|
| Success rows (manifest sum) | **3,690** |
| Bronze row_counts sum | **3,690** |
| Silver inventory row sum | **3,690** |
| `session_result` rows | **40** |
| Race sessions | 9472, 9480 |

Bronze paths use Google Drive: `/content/drive/MyDrive/openf1_big_data_pipeline/...`

### Silver evidence — all 9 report CSVs

| File | In evidence | Matches `reports/data_quality/` |
|------|-------------|----------------------------------|
| `silver_table_inventory.csv` | Yes | **Identical** |
| `silver_missingness_before.csv` | Yes | **Identical** |
| `silver_missingness_after.csv` | Yes | **Identical** |
| `silver_duplicate_report.csv` | Yes | **Identical** |
| `silver_outlier_report.csv` | Yes | **Identical** |
| `silver_referential_integrity_report.csv` | Yes | **Identical** |
| `silver_cleaning_rules.csv` | Yes | **Identical** |
| `silver_cleaning_impact_summary.csv` | Yes | **Identical** |
| `silver_rejected_records_summary.csv` | Yes (empty) | **Identical** |
| `silver_temporal_anomaly_report.csv` | Yes | **Identical** (copied into evidence folder during re-audit) |

### `reports/data_quality/` vs evidence — staleness

| Category | Stale at repo root? |
|----------|---------------------|
| All **Silver** CSVs (9 files) | **No** — identical to evidence |
| **Bronze** row counts, inventory, schema, drift | **Yes** — root still has old 8-file / 95-column partial snapshot |

**Rule:** Cite Bronze and Silver numbers from `evidence/smoke_2024_maxsessions2/` only (plus temporal from reports until copied).

### Silver Parquet

No `data/silver/*.parquet` in repo or evidence folder (expected on Drive only). Row counts inferred from `silver_table_inventory.csv`.

---

## 2. Silver outputs exist

| Artifact | Status |
|----------|--------|
| All 9 Silver report types | 8 in evidence + 1 temporal in `reports/` |
| `session_result_clean.parquet` | Not in repo; inventory shows **40 rows**, 15 columns |
| `starting_grid_clean.parquet` | Empty — **allowed** (Bronze 404) |

---

## 3. Silver table inventory

| table_name | row_count | column_count | duplicate_full_rows | missing_cell_pct | Assessment |
|------------|-----------|--------------|---------------------|------------------|------------|
| meetings | 25 | 21 | 0 | 0.0% | OK |
| sessions | 123 | 18 | 0 | 0.0% | OK |
| drivers | 40 | 16 | 0 | 0.16% | OK |
| laps | 2,031 | 20 | 0 | 1.78% | OK |
| pit | 62 | 12 | 0 | 8.33% | OK — 62 pit events |
| weather | 303 | 14 | 0 | 0.0% | OK |
| position | 927 | 9 | 0 | 0.0% | OK |
| race_control | 139 | 15 | 0 | 25.61% | OK — optional fields |
| session_result | 40 | 15 | 0 | 3.67% | **Gold-critical** |
| starting_grid | 0 | 0 | 0 | 0.0% | Expected |

**Suspicious tables:** None beyond expected empty `starting_grid`.

---

## 4. Missingness before vs after

| table_name | avg missing_pct before | avg missing_pct after | missing cells before → after | Changed? |
|------------|------------------------|----------------------|------------------------------|----------|
| meetings | 0.00% | 0.00% | 0 → 0 | No |
| sessions | 0.00% | 0.00% | 0 → 0 | No |
| drivers | 0.16% | 0.16% | 1 → 1 | No |
| laps | 1.78% | 1.79% | 725 → 726 | Minimal (+1 cell) |
| pit | 8.33% | 8.33% | 62 → 62 | No |
| weather | 0.00% | 0.00% | 0 → 0 | No |
| position | 0.00% | 0.02% | 0 → 2 | Minimal |
| race_control | 25.61% | 25.80% | 534 → 538 | Slight |
| session_result | 3.67% | 3.67% | 22 → 22 | No |

### Event absence vs missing

- **Pit:** 62 rows at session level — absence of pit rows for a driver with no stops is **not** a Silver error; Gold derives `pit_stop_count`.
- **Race control:** High missingness on `driver_number` (73%), `flag`, `qualifying_phase` — optional API fields for circuit-wide messages.
- **Session result:** `duration` ~45% null, `position` 5% null — normal for DNFs / API semantics.

**Rows removed:** 0 for all tables (`row_removal_pct` = 0%).

---

## 5. Duplicate findings

- **Full-row duplicates:** 0% before and after (all tables).
- **Key duplicates:** 0% where checked.

| table | Key columns |
|-------|-------------|
| laps | session_key, driver_number, lap_number |
| session_result | session_key, driver_number |
| pit | session_key, driver_number, lap_number |
| starting_grid | skipped (empty) |

No duplicate removal required on smoke data.

---

## 6. Outlier findings

| table | column | method | flagged | removed |
|-------|--------|--------|---------|---------|
| laps | lap_duration | IQR | 131 (6.45%) | No |
| pit | pit_duration | IQR | 10 (16.13%) | No |
| weather | air/track temp | IQR | 0% | No |
| laps/pit/session_result/position | domain rules | invalid ≤0 | 0% | No |

**Report narrative:** Outliers are **detected**, not deleted — appropriate for Safety Car / slow laps.

---

## 7. Temporal anomaly findings

Source: `evidence/smoke_2024_maxsessions2/reports/data_quality/silver_temporal_anomaly_report.csv`.

| table | anomaly_type | count | pct | Notes |
|-------|--------------|-------|-----|-------|
| weather | weather_outside_session_bounds | 113 | 37.29% | Flag only — API sampling vs session window |
| laps | unparseable_datetime | 1 → 0 after | — | Improved by cleaning |
| laps | lap_number_decrease_within_stint | 1 | 0.05% | Monitor on full data |
| position | unparseable_datetime | 2 → 0 after | — | Improved |
| race_control | unparseable_datetime | 4 → 0 after | — | Improved |

Checks are meaningful; no automatic row deletion from temporal module.

---

## 8. Referential integrity findings

Source: evidence `silver_referential_integrity_report.csv` (column `child_rows` = **distinct key count**, not total fact rows).

| Check | distinct keys | unmatched % |
|-------|---------------|-------------|
| All children → sessions | 2 session_keys | 0% |
| laps/pit/position/session_result → drivers | 38–40 keys | 0% |
| starting_grid | skipped_empty_child | — |

**Note:** Repo code now emits `distinct_child_keys`; re-run Silver on Colab to refresh RI CSV column name. Semantics unchanged.

---

## 9. Cleaning rules assessment

All 10 tables logged with: `rule_id`, `rule_description`, `rows_before/after`, `rows_removed`, `severity`, `rationale`, `columns_affected`.

| table | Smoke rules | rows_removed |
|-------|-------------|--------------|
| Non-empty tables | `SIL_NULL_*` key guards | 0 |
| starting_grid | `SIL_EMPTY` | 0 |

Rules are specific and defensible. Smoke had no bad keys to remove.

**Report note:** Zero removals does not mean cleaning is unused — pair with outlier/temporal/RI detection counts.

---

## 10. Cleaning impact — key report numbers

| Column | MBA use |
|--------|---------|
| `rows_before` / `rows_after` / `rows_removed` / `row_removal_pct` | Remediation headline |
| `missing_cell_pct_before` / `_after` | DQ before/after |
| `duplicate_rows_before` / `_after` | Duplicate policy |

Smoke: all `rows_removed = 0`; missingness stable except minor laps/position/race_control shifts.

---

## 11. Rejected records

`silver_rejected_records_summary.csv` is **empty** — no aggressive deletion on smoke. Will populate when `rows_removed > 0` on future runs.

---

## 12. Report readiness

| Rubric area | Supported? |
|-------------|------------|
| Data Quality & Cleaning | **Yes** — numeric %, per-table, before/after |
| Error taxonomy | **Partial** — document domain / IQR / temporal / RI in prose |
| Detection strategy | **Yes** |
| Remediation approach | **Yes** — rules + impact; conservative on smoke |
| Experimental validation | **Yes** — enables Gold/modeling |
| Reproducibility | **Yes** — full Bronze bundle + Silver CSVs on Drive |

---

## 13. Recommended fixes before full run

| Priority | Action |
|----------|--------|
| Done | `silver_temporal_anomaly_report.csv` added to evidence folder |
| Low | Sync `reports/data_quality/bronze_*.csv` from evidence (root Bronze still stale) |
| Optional | Re-run Silver on Colab to get `distinct_child_keys` in RI report |
| Monitor | Colab RAM for full 2023–2025 Silver (`build_silver.py` loads all Bronze) |
| Narrative | Explain 0% row removal on smoke vs detection-heavy evidence |

**No blocking code changes for full run.**

---

## 14. Gold readiness

| Silver source | Gold use |
|---------------|----------|
| **session_result** | `position`, `driver_number`, `session_key`, `dnf`, `dns`, `dsq` → **`points_finish`** |
| laps | `lap_duration`, `lap_number`, sectors → pace features |
| pit | `pit_duration`, `lap_number` → strategy aggregates |
| weather | temps, rainfall, humidity, wind → environment |
| position | `position`, `date` → position evolution |
| race_control | `category`, `flag`, `message`, `lap_number` → incidents/SC |
| drivers / sessions / meetings | metadata, calendar, circuit |

**Gaps:** `starting_grid` empty — use `session_result.position` or Qualifying for grid features. `gap_to_leader` mixed types in Bronze — handle in Gold only.

---

## 15. Final verdict

### **Silver smoke passed**

Updated evidence confirms a consistent end-to-end smoke pipeline:

- Bronze: 3,690 rows, 16 files, manifest on Drive, `session_result` = 40
- Silver: full DQ artifact set, 0% row removal, strong detection reports
- Evidence bundle now includes all 9 Silver DQ CSVs and complete Bronze artifacts

Ready for **Gold feature engineering** (smoke or full Silver) and **full 2023–2025** ingestion on Drive.

---

## Appendix: Files to cite in MBA report

From `evidence/smoke_2024_maxsessions2/reports/data_quality/`:

1. `silver_cleaning_impact_summary.csv`
2. `silver_missingness_before.csv` + `silver_missingness_after.csv`
3. `silver_outlier_report.csv`
4. `silver_referential_integrity_report.csv`
5. `silver_cleaning_rules.csv`
6. `silver_temporal_anomaly_report.csv` (from `reports/data_quality/` until copied)
7. Bronze: `artifacts/manifests/ingestion_manifest.csv` + `bronze_row_counts.csv`
