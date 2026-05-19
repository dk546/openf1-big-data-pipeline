# Phase 2 Silver Data Quality Deep Dive

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T15:58:13Z |
| **Evidence folder** | `evidence/smoke_2024_maxsessions2_spark/` |
| **Run profile** | Spark Silver (`SILVER_ENGINE=spark`), 2 sessions (9472, 9480), season 2024 |
| **Phase 1 reference** | Row-flow PASS (Bronze/Silver/Gold `session_result` grain = 40) |

## Files reviewed

**Silver (required):**

- `reports/data_quality/silver_table_inventory.csv`
- `reports/data_quality/silver_missingness_before.csv`
- `reports/data_quality/silver_missingness_after.csv`
- `reports/data_quality/silver_duplicate_report.csv`
- `reports/data_quality/silver_outlier_report.csv`
- `reports/data_quality/silver_temporal_anomaly_report.csv`
- `reports/data_quality/silver_referential_integrity_report.csv`
- `reports/data_quality/silver_cleaning_rules.csv`
- `reports/data_quality/silver_cleaning_impact_summary.csv`
- `reports/data_quality/silver_rejected_records_summary.csv`
- `reports/data_quality/duckdb_silver_silver_row_counts.csv`
- `reports/data_quality/duckdb_silver_session_result_duplicate_keys.csv`
- `reports/data_quality/duckdb_silver_session_result_target_support.csv`
- `reports/data_quality/duckdb_silver_laps_rows_by_session_key.csv`
- `reports/data_quality/duckdb_silver_pit_rows_by_session_key.csv`
- `reports/data_quality/duckdb_silver_weather_rows_by_session_key.csv`

**Related (context):**

- `artifacts/manifests/ingestion_manifest.csv` (`starting_grid` HTTP 404)
- `reports/data_quality/bronze_row_counts.csv` (Silver row parity confirmed in Phase 1)

---

## 1. Silver table inventory audit

` silver_table_inventory.csv` provides `table_name`, `row_count`, `column_count` only. Aggregate **missing_cell_pct** and **duplicate_rows** come from `silver_cleaning_impact_summary.csv`.

### Inventory + risk table

| table_name | row_count | missing_cell_pct | essential_for_gold | risk_level | notes |
|------------|-----------|------------------|--------------------|------------|-------|
| session_result | 40 | 4.58% | **Yes** (base + target) | Low | Required grain; 40 driver-session rows |
| drivers | 40 | 0.19% | **Yes** (metadata) | Low | Join keys complete |
| sessions | 123 | 0% | **Yes** (meeting_key, bounds) | Low | 2 smoke sessions embedded in season catalog |
| meetings | 25 | 0% | Recommended | Low | Circuit/meeting metadata |
| laps | 2031 | 2.10% | **Yes** (lap aggregates) | Medium | `i1_speed` 26.4% missing; IQR outliers flagged, not removed |
| pit | 62 | 11.11% | Yes (pit features) | Medium | `stop_duration` 100% missing (API); use `pit_duration` |
| position | 927 | 0% | Yes (early position features) | Low | Full row count; temporal parse fixed in cleaning |
| weather | 303 | 0% | Yes (session-level weather) | Medium | 37% timestamps flagged outside session bounds (see temporal) |
| race_control | 139 | **32.01%** | Yes (flag/message counts) | Medium | Sparse driver/sector/flag fields expected for session-level messages |
| starting_grid | **0** | 0% | **No** (not used in current Gold mart) | Low | Optional; Bronze 404 documented; empty schema OK |

### Answers

| Question | Answer |
|----------|--------|
| Highest missingness (table level) | **race_control** (32.0%), then **pit** (11.1%), **session_result** (4.6%), **laps** (2.1%) |
| Zero-row tables | **starting_grid** only |
| Essential for Gold | `session_result`, `drivers`, `sessions`, `laps` (+ `meetings`, `pit`, `position`, `weather`, `race_control` for engineered features) |
| Essential table unexpectedly empty? | **No** — all Gold-used tables have data |
| starting_grid optional? | **Yes** — correctly empty with `SIL_OPTIONAL_MISSING`; Gold builder does not join starting_grid |

---

## 2. Cleaning impact audit

| table_name | rows_before | rows_after | rows_removed | missing_cell_pct Δ | duplicate_rows Δ |
|------------|-------------|------------|--------------|-------------------|------------------|
| All 10 tables | = before | 0 | **unchanged** | 0 → 0 |

### Answers

| Question | Answer |
|----------|--------|
| Did Silver remove any rows? | **No** (0 rows removed on every table) |
| Is 0% removal plausible for smoke? | **Yes** — Bronze data is already clean at domain-rule thresholds; Spark cleaners only drop null keys, invalid domain values, and duplicates (none present). |
| Rules too weak / aggressive / appropriate? | **Appropriate for smoke** — detection reports (outliers, temporal) do not drive row drops. Not aggressive. Rule *descriptions* in logs are vague (see §8). |
| Useful numbers for final report | `rows_before`/`rows_after`, `row_removal_pct`, `missing_cell_pct_before`/`after`, `duplicate_rows_before`/`after` per table |

**Cleaning did change dtypes** (e.g. `sessions.date_start` string→timestamp, `weather.date`, `position.date`, `race_control.date`) without changing null counts — improves downstream parsing; temporal checks improved for `position` and `race_control` after cleaning.

---

## 3. Missingness before/after audit

### Missingness by table (after cleaning; same as before for all null counts)

| table | missing_cell_pct | top column issues |
|-------|------------------|-------------------|
| race_control | 32.01% | `qualifying_phase` 100%, `sector` 90.6%, `driver_number` 72.7%, `flag`/`scope` ~60% |
| pit | 11.11% | `stop_duration` **100%** (all null) |
| session_result | 4.58% | `duration` 45%, `gap_to_leader`/`position` 5% |
| laps | 2.10% | `i1_speed` 26.4%, `st_speed` 8.5%, sector durations &lt;0.1% |
| drivers | 0.19% | `headshot_url` 2.5% |
| Others | 0% | — |

### Highest-missingness columns (≥5%)

| table.column | missing_pct | Classification |
|--------------|-------------|----------------|
| pit.stop_duration | 100% | **Structural** — field unused in OpenF1 pit payload; `pit_duration`/`lane_duration` populated |
| race_control.qualifying_phase | 100% | **Structural** — smoke sessions are race-type; field applies to qualifying |
| race_control.sector | 90.6% | **Structural** — many messages are session-wide, not sector-specific |
| race_control.driver_number | 72.7% | **Structural** — broadcast/session messages lack driver |
| race_control.flag / scope | ~60% | **Structural** — not all message types carry flag/scope |
| session_result.duration | 45% | **Expected** — DNF/DNS/NC rows lack race time |
| laps.i1_speed | 26.4% | **Structural** — intermediate speed not reported every lap |
| laps.st_speed | 8.5% | **Structural** — speed trap gaps |
| session_result.gap_to_leader, position | 5% | **Expected** — non-finishers |
| drivers.headshot_url | 2.5% | **Low priority** — metadata only |

### Before vs after

- **0 columns** changed `missing_pct` between before and after reports.
- Dtype/coercion changes only; **no imputation** in Silver (correct for modeling boundary).

### Recommendations on new Silver rules

| Issue | Silver action | Rationale |
|-------|---------------|-----------|
| `stop_duration` 100% null | **No new rule** — document; do not impute | Use `pit_duration` in Gold (already does) |
| race_control sparse columns | **No row removal** | Gold aggregates message counts; nulls expected |
| session_result duration/gap | **No imputation** | Outcome-related; keep for diagnostics / target logic in Gold |
| laps speed gaps | **No IQR removal** | Missing speeds ≠ bad laps |
| qualifying_phase always null (race) | **Optional report filter** by `session_type` | Nice-to-have for final report clarity |

**Defer to Gold/modeling:** target construction (`points_finish`), feature aggregation with null-safe means, event-absence zero-fill (already in Gold).

---

## 4. Duplicate audit

| Check | Result |
|-------|--------|
| Full-row duplicates | **0** on all tables (before and after) |
| Key duplicates | **0** on all defined keys (before and after) |
| starting_grid | **skipped_empty** (acceptable) |
| laps complex columns | Note: *"Converted complex object columns to stable strings for duplicate check"* — ndarray/list fix is working; no false duplicate inflation |

### Answers

| Question | Answer |
|----------|--------|
| Meaningful after ndarray/list fix? | **Yes** — laps check runs with stable string serialization; 0 key dupes on `(session_key, driver_number, lap_number)` |
| Key vs full-row checks | **Key checks are more important** for fact tables; full-row is supplementary |
| Strengthen before full run? | **No change required** — current keys match Gold grain and fact granularity |
| DuckDB confirmation | `duckdb_silver_session_result_duplicate_keys.csv` empty — aligns with Spark report |

---

## 5. Outlier audit

### IQR (detection only — identical before/after)

| table.column | outlier_pct | min | max | Interpretation |
|--------------|-------------|-----|-----|----------------|
| laps.lap_duration | 6.45% | 91.6s | 193.7s | Likely SC/slow laps/out-laps — **real events** |
| pit.pit_duration | 16.13% | 20.1s | 74.7s | Long stops / repairs — **real events** |
| weather air/track temp | 0% | — | — | No IQR outliers in smoke |

### Domain rules (invalid values)

| rule | invalid_count |
|------|---------------|
| lap_number ≤ 0 | 0 |
| lap_duration ≤ 0 | 0 |
| sector durations ≤ 0 | 0 |
| pit lap_number ≤ 0, pit_duration ≤ 0 | 0 |
| position ≤ 0 | 0 |
| session_result position ≤ 0, points &lt; 0 | 0 |

### Answers

| Question | Answer |
|----------|--------|
| Real race events vs impossible | All domain invalid counts are **0**; IQR flags are **plausible race events**, not corrupt data |
| Domain rules sufficient? | **Yes** for smoke — catches impossible numerics; no violations |
| Avoid removing IQR outliers? | **Yes — do not remove** — IQR is session-heterogeneous; removing would bias lap/pit aggregates; report-only is correct |

---

## 6. Temporal anomaly audit

| table | anomaly_type | count | pct | Blocking? |
|-------|--------------|-------|-----|-----------|
| laps | unparseable `date_start` | 1 | 0.05% | No |
| laps | lap_number decrease within stint | 0 | 0% | — |
| weather | **outside session bounds** | 113 | **37.29%** | No (Gold joined 100%) |
| position | unparseable `date` (before) | 2 | 0.22% | Fixed after cleaning (0 after) |
| race_control | unparseable `date` (before) | 4 | 2.88% | Fixed after cleaning (0 after) |
| sessions | start after end | 0 | 0% | — |

### Answers

| Question | Answer |
|----------|--------|
| Temporal checks useful? | **Yes** — caught dtype/parsing improvements; weather bound check highlights API vs session window mismatch |
| Block full run? | **No** — no critical parse failures remain after Silver; weather bound flag needs documentation, not pipeline stop |
| Missing checks to add? | **Nice-to-have:** monotonic `lap_number` per driver when `date_start` unparseable; pit `date` vs session bounds; optional `session_type`-aware temporal rules |

---

## 7. Referential integrity audit

| child → parent | distinct_child_keys | unmatched | status |
|----------------|----------------------|-----------|--------|
| * → sessions (session_key) | 2 | 0 | checked |
| laps/pit/position/session_result → drivers | 38–40 | 0 | checked |
| starting_grid → * | 0 | 0 | skipped_empty_child |

**Note:** `pit` has 38 distinct `(session_key, driver_number)` vs 40 for session_result — **expected** (2 drivers had no pit stops in smoke sessions).

### Answers

| Question | Answer |
|----------|--------|
| Required joins valid? | **Yes** |
| Unmatched rows expected? | **None** — pit subset is expected, not orphan keys |
| Skipped checks acceptable? | **Yes** for empty `starting_grid` |
| Safe for Gold joins? | **Yes** — Phase 1 Gold join quality 40/40, 0 unmatched |

---

## 8. Cleaning rules audit

Evidence `silver_cleaning_rules.csv` columns: `table_name`, `rule_id`, `rule_description`, `rows_before`, `rows_after`, `rows_removed`, `severity`, `rationale`.

**Missing vs code schema:** `values_imputed`, `columns_affected` exist in `CLEANING_LOG_COLUMNS` but are **not exported** in Spark smoke evidence (Spark `_rule_log` omits them).

| rule_id | Assessment |
|---------|------------|
| SIL_MEETINGS … SIL_SESSION_RESULT | **Vague** — single line per table (e.g. "Laps cleaning") |
| SIL_OPTIONAL_MISSING | **Good** — documents starting_grid 404 / empty |

### Answers

| Question | Answer |
|----------|--------|
| Specific enough for final report? | **Partially** — need finer-grained rule IDs (cast, dedupe, domain filter) in export |
| Vague rules? | **Yes** — all per-table bundles hide individual steps |
| Split detection vs remediation? | **Recommended** — outliers/temporal/RI are detection-only; cleaning_rules should list remediation only |
| Target / modeling leakage in Silver? | **No** — Silver does not build `points_finish` or impute modeling features |

---

## 9. Rejected records audit

| Finding | Detail |
|---------|--------|
| Rejected rows | **0** (`silver_rejected_records_summary.csv` header only) |
| Plausible for smoke? | **Yes** — no domain violations, no dupes, no failed key drops |
| Full-run logging | **Should** log rejected samples when `rows_removed` &gt; 0 |
| Zero-row summary | **Should** emit template row per table: `rejected_count=0` for audit trail |

---

## 10. DuckDB Silver validation audit

| DuckDB report | Confirms |
|---------------|----------|
| `silver_row_counts` | All 10 tables match Spark inventory; `starting_grid` status `ok` at 0 rows |
| `session_result_duplicate_keys` | No duplicate `(session_key, driver_number)` |
| `session_result_target_support` | 40 rows, 20 points-positive (50%) — target viable |
| `laps_rows_by_session_key` | 1129 + 902 = 2031 |
| `pit_rows_by_session_key` | 43 + 19 = 62 |
| `weather_rows_by_session_key` | 157 + 146 = 303 |

### Answers

| Question | Answer |
|----------|--------|
| Row counts confirmed? | **Yes** |
| Target support confirmed? | **Yes** |
| Beyond Spark CSVs? | Session-level breakdowns useful for report; no contradictions |
| Report-ready? | **Yes** — concise, joins well to Phase 1 Gold DuckDB checks |

---

## 11. Silver-to-Gold readiness

| source_table | gold_use | quality_status | risks | action_before_full_run |
|--------------|----------|----------------|-------|------------------------|
| session_result | Base grain + target source | **Ready** | 45% missing `duration` (DNF) | None — expected |
| drivers | Metadata join | **Ready** | headshot_url sparse | None |
| sessions | meeting_key, session bounds | **Ready** | 123 rows vs 2 active sessions | None — catalog table |
| meetings | Circuit/meeting metadata | **Ready** | — | None |
| laps | Lap aggregates | **Ready** | i1_speed/st_speed gaps; IQR outliers | Do not IQR-drop; document |
| pit | Pit stop features | **Ready** | `stop_duration` dead column | Document; use pit_duration |
| position | Early position features | **Ready** | — | None |
| weather | Session weather aggregates | **Ready** | 37% outside session window flag | Document bound rule; monitor full season |
| race_control | Flag/message counts | **Ready** | High column sparsity | None — aggregate in Gold |
| starting_grid | Not used in mart | **N/A (optional)** | Empty on smoke | None — keep optional handling |

---

## 12. Recommended fixes before full run

### A. Must fix before full run

*None identified from evidence.* Structural integrity, RI, duplicates, and Gold join success support proceeding with current Silver logic.

### B. Should fix before final report

1. **Enrich `silver_cleaning_rules.csv`** — export `values_imputed`, `columns_affected`, and per-step rule IDs (cast, dedupe, domain filter) from Spark `_rule_log`.
2. **Rejected records template** — write zero-count rows per table/rule for audit completeness.
3. **Document `weather_outside_session_bounds`** — clarify whether comparison uses UTC session bounds vs weather sample cadence (37% is likely definitional, not bad data).
4. **Report narrative** — distinguish structural nulls (`qualifying_phase`, `stop_duration`, race_control driver_number) from data defects.

### C. Nice to have

1. Session-type-aware missingness/outlier sections (race vs qualifying).
2. Temporal checks for pit `date` vs session window.
3. Add `missing_cell_pct` to `silver_table_inventory.csv` for single-table view.
4. Sample rejected-row parquet when `rows_removed` &gt; 0 on full 2023–2025 run.

### D. No action needed

- 0% row removal on smoke (appropriate).
- IQR outlier reporting without removal.
- Key duplicate checks and laps array normalization.
- Empty `starting_grid` optional path.
- No Silver target construction or feature imputation.

---

## 13. Phase 2 verdict

### **PASS WITH FIXES**

Silver quality is **good enough to proceed with full 2023–2025 ingestion and Spark Silver/Gold execution**. Core fact tables are complete, referentially sound, duplicate-free, and Gold-ready at the driver-session grain. Fixes are **reporting and documentation** improvements, not blockers discovered in smoke evidence.

**Not a FAIL because:** no contradictory counts, no RI failures, no duplicate grain violations, and Gold already consumed Silver successfully (40 rows, 0 unmatched joins).

---

## 14. Recommended next step

Proceed to **Phase 3: Join and Integration Audit** — validate Gold join logic, leakage guard, feature dictionary, and cross-layer feature semantics using the same evidence folder plus Gold reports.

Optionally refresh evidence bundle with `feature_dictionary.csv` and Gold parquet for byte-level mart verification.

---

*Audit performed from frozen CSV evidence only. No code, notebooks, or synthetic outputs were modified or created.*
