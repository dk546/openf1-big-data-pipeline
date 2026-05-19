# Phase 3 Gold Join and Integration Audit

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T16:09:06Z |
| **Evidence folder** | `evidence/smoke_2024_maxsessions2_spark/` |
| **Run profile** | Spark Gold, 2 sessions (9472, 9480), season 2024 smoke |
| **Prior phases** | Phase 1 PASS WITH MINOR ISSUES; Phase 2 PASS WITH FIXES |

## Files reviewed

**Gold reports:**

- `reports/data_quality/gold_join_quality_report.csv`
- `reports/data_quality/gold_target_distribution.csv`
- `reports/data_quality/gold_feature_missingness.csv`
- `reports/data_quality/gold_feature_summary_stats.csv`
- `reports/data_quality/gold_leakage_guard_report.csv`
- `reports/data_quality/duckdb_gold_gold_row_count.csv`
- `reports/data_quality/duckdb_gold_gold_target_distribution.csv`
- `reports/data_quality/duckdb_gold_gold_duplicate_keys.csv`
- `reports/data_quality/duckdb_gold_gold_missingness_summary.csv`
- `reports/data_quality/duckdb_gold_points_finish_by_team.csv`
- `reports/data_quality/duckdb_gold_points_finish_by_circuit.csv`

**Feature definitions:**

- `artifacts/feature_definitions/feature_dictionary.csv` (**present**)

**Gold data:**

- `data/gold/driver_race_feature_mart.parquet/` — **not in evidence folder**

**Silver context:**

- `reports/data_quality/silver_table_inventory.csv`
- `reports/data_quality/silver_referential_integrity_report.csv`
- `reports/data_quality/duckdb_silver_silver_row_counts.csv`
- `reports/data_quality/duckdb_silver_session_result_target_support.csv`

---

## 1. Gold evidence completeness

| expected_artifact | found | importance | notes |
|-------------------|-------|------------|-------|
| `gold_join_quality_report.csv` | Yes | High | All feature groups 40→40, 0 unmatched |
| `gold_target_distribution.csv` | Yes | High | 50/50 binary target |
| `gold_feature_missingness.csv` | Yes | High | 63 columns profiled |
| `gold_feature_summary_stats.csv` | Yes | High | Numeric ranges for QA |
| `gold_leakage_guard_report.csv` | Yes | High | 63 columns classified |
| `duckdb_gold_gold_row_count.csv` | Yes | Medium | Confirms 40 rows |
| `duckdb_gold_gold_target_distribution.csv` | Yes | Medium | Matches Spark report |
| `duckdb_gold_gold_duplicate_keys.csv` | Yes | Medium | Empty (no dup keys) |
| `duckdb_gold_gold_missingness_summary.csv` | Yes | Medium | Aligns with Spark missingness |
| `duckdb_gold_points_finish_by_team.csv` | Yes | Low | Exploratory slice |
| `duckdb_gold_points_finish_by_circuit.csv` | Yes | Low | Exploratory slice |
| `feature_dictionary.csv` | **Yes** | High | 63 features documented |
| `driver_race_feature_mart.parquet` | **No** | Medium | Audit from CSV reports only; copy for byte-level QA optional |

---

## 2. Gold grain audit

| Check | Result |
|-------|--------|
| Gold row count | **40** (`gold_feature_summary_stats` count on `session_key`) |
| Silver `session_result` rows | **40** |
| Bronze `session_result` rows | **40** (Phase 1) |
| DuckDB Gold row count | **40** |
| Duplicate keys `(session_key, meeting_key, driver_number)` | **0** (DuckDB duplicate report empty; leakage/base from `session_result`) |
| Row gain/loss vs Silver | **None** (40 = 40 = 40) |

### Answers

| Question | Answer |
|----------|--------|
| Is Gold grain correct? | **Yes** — one row per driver-session (`session_key`, `meeting_key`, `driver_number`). |
| Duplicate driver-race keys? | **No** |
| Unexplained row gain/loss? | **No** |
| Built from `session_result` not `sessions`? | **Yes** — base table is `session_result` (40 rows); `sessions` has 123 catalog rows and is join-only for metadata/bounds. |

---

## 3. Join quality audit

| feature_group | rows_before | rows_after | unmatched_base_rows | unmatched_pct | Assessment |
|---------------|-------------|------------|---------------------|---------------|------------|
| metadata | 40 | 40 | 0 | 0% | Perfect |
| laps | 40 | 40 | 0 | 0% | Perfect |
| pit | 40 | 40 | 0 | 0% | Perfect |
| position | 40 | 40 | 0 | 0% | Perfect |
| weather | 40 | 40 | 0 | 0% | Perfect |
| race_control | 40 | 40 | 0 | 0% | Perfect |

### Answers

| Question | Answer |
|----------|--------|
| Perfect joins? | **All six groups** — 100% of base rows retained |
| Unmatched rows? | **None** |
| Expected? | Yes — Silver RI passed; smoke has complete driver-session coverage |
| Join keys | `DRIVER_KEYS` for driver-level features; `SESSION_KEYS` for session-level weather/race_control (broadcast to drivers) |
| Join cardinality | **Many-to-one** after aggregation (laps/pit/position → driver-session; weather/race_control → session → driver) |
| Row multiplication risk? | **None observed** — `rows_before` = `rows_after` = 40 for every group |

---

## 4. Source contribution audit

| source_table | feature_group | expected_features (from dictionary) | present_in_gold | missing_features | notes |
|--------------|---------------|-----------------------------------|-----------------|------------------|-------|
| session_result | target + diagnostic | `points_finish`, `final_position`, `result_*`, keys | All | — | Base grain |
| drivers + sessions + meetings | metadata | 12 metadata columns | All | — | Not for default modeling |
| laps | laps | 15 lap aggregates | All | — | Full-session lap aggregates |
| pit | pit | 6 pit features | All | — | 5% null pit duration fields when no stops |
| position | position | 5 predictive + `diagnostic_final_observed_position` | All | — | Early-race position features |
| weather | weather | 7 weather features | All | — | Session-level broadcast |
| race_control | race_control | 7 count features | All | — | Session-level broadcast |
| starting_grid | — | None in current mart | N/A | N/A | **Correctly absent** (optional; not used in builder) |

### Answers

| Question | Answer |
|----------|--------|
| Each source contributed expected features? | **Yes** — dictionary lists 40 `allowed_for_modeling=True` features; all present in Gold CSVs |
| starting_grid optional? | **Yes** — 0 Silver rows; no Gold columns (by design) |
| Important features missing? | **No** for current modeling plan |

---

## 5. Target audit

| Metric | Value |
|--------|-------|
| `points_finish = 0` | 20 (50%) |
| `points_finish = 1` | 20 (50%) |
| DuckDB confirmation | Identical |
| Definition (code) | `points_finish = 1` if `points > 0` else `0` |

### Answers

| Question | Answer |
|----------|--------|
| Target construction valid? | **Yes** — aligns with README and dictionary |
| Smoke distribution for performance claims? | **No** — 50/50 with 2 sessions / 21 drivers is **code-validation only**; not representative of full-season class imbalance |
| After full 2023–2025 run | Check global and per-season `points_finish` rates; stratify metrics by season; confirm both classes exist in train/val/test splits |

---

## 6. Feature missingness audit

### By severity (predictive features only)

| Category | Columns | missing_pct | Expected? |
|----------|---------|-------------|-----------|
| Zero missing | Most lap, position, weather, race_control, `pit_stop_count`, `early_pit_stop_flag` | 0% | Yes |
| Low (2.5%) | `std_lap_duration`, `std_first5_lap_duration` | 2.5% | Yes — std undefined for single lap |
| Moderate (5%) | `avg/min/max_pit_duration`, `first_pit_lap` | 5% | Yes — **2 drivers with no pit stops** (38/40 with pit data) |
| Diagnostic only | `final_position` | 5% | Yes — DNF-style null classified positions |

### Answers

| Question | Answer |
|----------|--------|
| Expected missingness? | Pit duration fields for no-stop drivers; std lap features with insufficient laps |
| Drop if high on full run? | Only if a feature stays &gt;50% null after full ingest — not indicated in smoke |
| Impute in modeling? | Optional median/impute for pit duration **or** use `pit_stop_count=0` as indicator; tree models handle NaN |
| Zero-fill event counts? | **Already handled** in Gold for `pit_stop_count`, race_control counts when absent (`EVENT_ABSENCE_ZERO_COLS` in builder) |

---

## 7. Feature summary / statistics audit

| Domain | Plausible? | Smoke caveat |
|--------|------------|--------------|
| Lap duration | Yes — ~91–122 s best/avg, sector splits reasonable | Full-session aggregates |
| Pit duration | Yes — ~20–75 s | Long stops flagged as outliers in Silver |
| Position | Yes — positions 1–20 | `first_observed_position` mean ~10.5 |
| Weather | Yes — air ~18–26°C, track ~24–32°C | **Low variance** (2 sessions → 2 unique session-level values per feature) |
| Race control counts | Yes — ~68–71 messages/session | Some counts **constant** in smoke (`yellow_flag_count=3`, `red_flag_count=1`, etc.) |
| Rainfall | All zero | Dry sessions only in smoke |

### Constant predictive features in smoke (std = 0)

`rainfall_mean`, `rainfall_flag`, `yellow_flag_count`, `red_flag_count`, `green_flag_count`, `pit_exit_message_count`

These are **not bugs** — smoke has no rain and limited session diversity. Expect variation on full 2023–2025.

### Scaling

Numeric lap/pit/position features benefit from scaling for **Logistic Regression**; tree models (RF, LightGBM) need less preprocessing.

---

## 8. Leakage guard audit

| Category | Examples | `allowed_for_modeling` |
|----------|----------|------------------------|
| Identifiers | `session_key`, `meeting_key`, `driver_number` | False |
| Target | `points_finish` | False |
| Post-race diagnostics | `final_position`, `result_points`, `result_dnf/dns/dsq`, `diagnostic_final_observed_position` | False |
| Metadata | `team_name`, `circuit_short_name`, etc. | False (encode explicitly if used) |
| Predictive | Lap, pit, position, weather, race_control aggregates | True (40 features) |

### Answers

| Question | Answer |
|----------|--------|
| Leakage control sufficient for notebook plan? | **Yes** for explicit forbidden outcome columns |
| Suspicious allowed features? | **Lap pace aggregates** (`avg_lap_duration`, `best_lap_duration`, etc.) use **all laps in the session**, not only early laps — for a strict *pre-race* prediction framing this is **outcome-adjacent leakage**; for *in-session / post-qualifying* feature use (project README) it is acceptable. Document in final report. |
| `first_observed_position` | Allowed — early race proxy; used by heuristic baseline; correlated with target by design |
| Reclassify before modeling? | Keep diagnostics forbidden; consider a **modeling feature list** CSV that excludes full-session lap means if claiming pre-race prediction |

---

## 9. Feature dictionary audit

**Status:** `feature_dictionary.csv` **present** (63 rows).

| Check | Result |
|-------|--------|
| `modeling_role` values | `identifier`, `target`, `diagnostic_outcome`, `metadata`, `predictive_feature` |
| Target separation | `points_finish` → `target`, not allowed |
| Predictive source linkage | Each feature maps to Silver table (`laps`, `pit`, etc.) |
| `missing_pct` | Populated; matches Gold missingness report |
| Descriptions | Useful one-line text for report |

### Answers

| Question | Answer |
|----------|--------|
| Modeling roles correct? | **Yes** |
| Metadata classified correctly? | **Yes** — not allowed by default |
| Descriptions report-ready? | **Yes** |

---

## 10. DuckDB Gold validation audit

| DuckDB report | Spark Gold report | Match? |
|---------------|-------------------|--------|
| Row count 40 | 40 rows | Yes |
| Target 20/20 | 20/20 | Yes |
| Duplicate keys empty | No dupes implied | Yes |
| Missingness summary | Same 5% pit / 2.5% std pattern | Yes |
| By team / circuit | Extra slices | Report-ready exploratory |

No issues found in DuckDB that contradict Spark Gold CSVs.

---

## 11. ML readiness from Gold

| modeling_component | required_gold_columns | available | risk | action_before_modeling |
|--------------------|----------------------|-----------|------|------------------------|
| Random baseline | `points_finish` (both classes) | Yes | Low | Use class prior on **full-season** distribution |
| Heuristic baseline | `first_observed_position`, `points_finish` | Yes | Medium | Document correlation; smoke 50/50 not representative |
| Logistic Regression | Allowed numeric features, no leakage cols | Yes (40 features) | Medium | Scale features; handle pit NaN; encode metadata if used |
| Random Forest | Same + categorical optional | Yes | Low | Monitor full-session lap feature leakage narrative |
| LightGBM | Same | Yes | Low | Same as RF |
| Season splits 2023/2024/2025 | `session_year` in mart | Yes (2024 only in smoke) | High for smoke | Run full ingest before official metrics |
| Leakage exclusion | Guard report + dictionary | Yes | Medium | Publish explicit `X` column list per model |
| Event zero-fill | pit/RC counts | Yes | Low | Verify zeros after full run |

**Overall ML readiness (smoke):** Wiring and schema are **ready**. **Official performance claims** require full 2023–2025 Gold and season-based evaluation only.

---

## 12. Recommended fixes

### A. Must fix before full 2023–2025 run

*None blocking* — joins, grain, target, and leakage tagging are sound.

### B. Should fix before modeling

1. Define and freeze **modeling feature column list** (40 predictive vs metadata encoded separately).
2. Document **temporal scope** of lap features (full-session vs first-five) in final report to avoid overstating pre-race predictability.
3. Modeling pipeline: strategy for **pit duration NaN** (5% in smoke; may grow slightly on full data).

### C. Should fix before final report

1. Copy **`driver_race_feature_mart.parquet`** into evidence for reproducibility.
2. After full run: refresh target distribution, constant-feature check, and DuckDB slices by season.
3. Clarify `position_observation_count` semantics vs raw Silver position row counts in report appendix.

### D. Nice to have

1. Separate **early-lap-only** lap feature group for stricter leakage experiments.
2. starting_grid features if API data becomes available on full ingest.

### E. No action needed

- Join quality (all 0% unmatched).
- Grain 40 = session_result.
- Target definition `points > 0`.
- starting_grid absent from mart.
- DuckDB validation agreement.

---

## 13. Phase 3 verdict

### **PASS WITH FIXES**

Gold integration is **correct and complete for smoke**: grain is valid, all joins succeed without row explosion, target and leakage guard are well defined, and the feature dictionary is present. Fixes are **documentation and modeling-scope** items (full-session lap features, smoke-only constants, parquet in evidence), not join bugs.

**Not FAIL** — no contradictory counts, duplicate keys, or failed joins.

---

## 14. Recommended next step

**Proceed to modeling notebook wiring on smoke**, then full 2023–2025 Gold rebuild.

**Phase 4 (Feature quality and leakage)** can be **combined** with Phase 3 findings above rather than re-auditing joins:

- Deep-dive: full-session lap aggregates vs early-lap features.
- Confirm modeling `X` excludes diagnostics.
- Re-run target/imbalance and constant-feature checks on full-season mart.

---

*Audit from frozen CSV evidence only. No code, notebooks, or synthetic outputs were modified.*
