# Phase 4 Feature Quality, Leakage, and ML Readiness Audit

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T16:09:06Z |
| **Evidence folder** | `evidence/smoke_2024_maxsessions2_spark/` |
| **Mart verified** | `data/gold/driver_race_feature_mart.parquet/` — 40 rows × 62 columns (readable Parquet) |
| **Prior phases** | P1 PASS WITH MINOR ISSUES; P2/P3 PASS WITH FIXES |

## Files reviewed

**Gold evidence:**

- `reports/data_quality/gold_feature_missingness.csv`
- `reports/data_quality/gold_feature_summary_stats.csv`
- `reports/data_quality/gold_target_distribution.csv`
- `reports/data_quality/gold_leakage_guard_report.csv`
- `reports/data_quality/gold_join_quality_report.csv`
- `reports/data_quality/duckdb_gold_*.csv` (5 files)
- `artifacts/feature_definitions/feature_dictionary.csv`
- `data/gold/driver_race_feature_mart.parquet/`

**Modeling code:**

- `src/openf1_pipeline/modeling/splits.py`
- `src/openf1_pipeline/modeling/baselines.py`
- `src/openf1_pipeline/modeling/train.py`
- `src/openf1_pipeline/modeling/evaluate.py`

**Project docs:**

- `README.md`
- `project_plan.md`
- `notebooks/04_modeling_evaluation.ipynb` (structure and constants)

---

## 1. ML task framing assessment

### Stated task (README / notebook 04 / project_plan)

> Predict whether a driver finishes in the points (`points_finish` = 1 if `points` > 0).

### Defensibility

| Framing | Assessment |
|---------|------------|
| **Integrated race-session classification** | **Defensible** — multi-endpoint Gold mart, season splits, explicit target from `session_result` |
| **Strict pre-race prediction** | **Not defensible** with current default features — many inputs use **full-session** information |

### Features that change interpretation (not pre-race)

| Feature type | Examples | Window in code |
|--------------|----------|----------------|
| Full-session lap pace | `lap_count`, `avg_lap_duration`, `median_lap_duration`, `best_lap_duration`, sector/speed means | All laps in `build_lap_features` |
| Full-session pit | `pit_stop_count`, `avg_pit_duration`, `first_pit_lap` | All pit rows |
| Session weather means | `avg_air_temperature`, `avg_track_temperature`, … | Full session weather series |
| Race-control counts | `race_control_message_count`, flag counts | Full session messages |

### Early-race / lower-leakage features (document separately)

| Feature type | Examples | Window |
|--------------|----------|--------|
| First-five laps | `avg_first5_lap_duration`, `best_first5_lap_duration`, `std_first5_lap_duration` | Laps 1–5 only |
| Early position | `first_observed_position`, `early_avg_position`, … | First 5 position observations |
| Heuristic baseline | `first_observed_position` ≤ 10 | Same early window |

### Wording fixes recommended

| Location | Issue | Fix |
|----------|-------|-----|
| `README.md` §10 / Gold §269 | Says “early-race windows **only where specified**” | Clarify: **some** features are early-window; **lap pace aggregates (except first-five)** and **pit/RC counts** are **full-session analytical** features |
| `project_plan.md` §1.1 | Good intent (“decision point”) | Align text with implementation: default model uses **in-race / full-session analytical** features unless a reduced feature set is chosen |
| Report title | Avoid “pre-race points prediction” | Use **“points finish classification from integrated session features”** |

### Answers

| Question | Answer |
|----------|--------|
| Task framing defensible? | **Yes**, as **in-race / end-of-session analytical classification**, not strict pre-race |
| Wording changes needed? | **Yes** — README Gold leakage bullet; optional `modeling_feature_scope.md` one-pager |
| Distinguish early vs full-session? | **Yes** — two feature tiers in final report (see §9) |

---

## 2. Feature dictionary assessment

### Summary by `feature_group`

| feature_group | total_features | allowed_for_modeling | diagnostic | metadata | target | identifier |
|---------------|----------------|----------------------|------------|----------|--------|------------|
| laps | 15 | 15 | 0 | 0 | 0 | 0 |
| pit | 6 | 6 | 0 | 0 | 0 | 0 |
| position | 5 | 5 | 0 | 0 | 0 | 0 |
| weather | 7 | 7 | 0 | 0 | 0 | 0 |
| race_control | 7 | 7 | 0 | 0 | 0 | 0 |
| metadata | 12 | 0 | 0 | 12 | 0 | 0 |
| diagnostic_outcome | 6 | 0 | 6 | 0 | 0 | 0 |
| identifiers | 3 | 0 | 0 | 0 | 0 | 3 |
| target | 1 | 0 | 0 | 0 | 1 | 0 |
| **Total** | **62** | **40** | **6** | **12** | **1** | **3** |

### Classification checks

| Check | Result |
|-------|--------|
| `points_finish` → target, not allowed | **Pass** |
| Identifiers not allowed | **Pass** |
| `final_position`, `result_points`, `result_dnf/dns/dsq` → diagnostic, not allowed | **Pass** |
| `diagnostic_final_observed_position` → diagnostic, not allowed | **Pass** |
| 40 predictive features allowed | **Pass** |
| Metadata consistent (`allowed_for_modeling=False`, role=metadata) | **Pass** |

### Answers

| Question | Answer |
|----------|--------|
| Dictionary ready to control modeling? | **Yes** — `get_model_feature_columns()` in `train.py` reads `allowed_for_modeling` and applies extra exclusions |
| Misclassified fields? | **No material misclassification** |
| Metadata in default model? | **Excluded by design** — use explicit opt-in + one-hot if needed; `session_year` stays for **splits only** |

---

## 3. Leakage guard deep audit

### Explicitly excluded from modeling (present in mart, blocked in X)

| Column | In mart | In `LEAKAGE_FORBIDDEN` | Guard `allowed=False` |
|--------|---------|------------------------|------------------------|
| `position` (raw) | No (renamed to `final_position`) | Yes | N/A |
| `points` (raw) | No (→ `result_points`) | Yes | N/A |
| `final_position` | Yes | Yes | Yes |
| `result_points` | Yes | Yes | Yes |
| `result_dnf/dns/dsq` | Yes | Yes | Yes |
| `duration` | No in mart | Yes | N/A |
| `gap_to_leader` | No in mart | Yes | N/A |
| `number_of_laps` | No in mart | Yes | N/A |
| `diagnostic_final_observed_position` | Yes | via `diagnostic_*` | Yes |
| `source_*` | No in mart | N/A | N/A |

`validate_no_leakage()` in notebook 04 will **raise** if forbidden columns appear in `allowed_for_modeling=True` set.

### Feature-group leakage risk (interpretation / task scope)

| Feature group | Risk level | Rationale |
|---------------|------------|-----------|
| Identifiers | **No** (excluded from X) | Keys only |
| Target / diagnostics | **High** (excluded) | Direct outcome |
| Metadata | **Low** (excluded by default) | Encoding choice; no raw outcome |
| Position (early) | **Low–medium** | First 5 observations; heuristic-aligned |
| Laps — first-five | **Low** | Explicit early window |
| Laps — full-session aggregates | **Medium–high** | Encode post-start performance; OK for analytical task |
| Pit | **Medium** | Post-start strategy signal |
| Weather | **Low–medium** | Session conditions; full-session mean |
| Race control | **Medium** | Session events; outcome-correlated counts |

### Subtle features (allowed but interpret carefully)

| Feature | Risk | Note |
|---------|------|------|
| `lap_count` | Medium | Completes only after racing |
| `pit_stop_count` | Medium | Zero-filled when no stops (correct) |
| `avg_lap_duration` | Medium–high | Full-session mean |
| `race_control_message_count` | Medium | Full session |

### Answers

| Question | Answer |
|----------|--------|
| Outcome leakage controlled? | **Yes** for explicit outcome columns |
| Subtle leakage? | **Interpretation leakage** on full-session features — document, do not claim pre-race |
| Guard reason text | Says “pre-race or in-race” for all allowed features — **overstates** full-session lap/pit/RC features |

---

## 4. Feature missingness assessment

### Buckets (`driver_race_feature_mart`, 62 columns)

| Bucket | Count |
|--------|-------|
| `missing_pct = 0` | 55 |
| `0 < missing_pct ≤ 20` | 7 |
| `missing_pct > 20` | 0 |

### Top missing (all ≤ 5%)

| column_name | missing_pct | Type |
|-------------|-------------|------|
| `avg_pit_duration`, `min_pit_duration`, `max_pit_duration`, `first_pit_lap` | 5.0% | No pit stop (2/40 drivers) |
| `final_position` | 5.0% | Diagnostic only |
| `std_lap_duration`, `std_first5_lap_duration` | 2.5% | Single-lap std undefined |

### Constant / near-constant in smoke (predictive)

| Feature | Smoke value |
|---------|-------------|
| `rainfall_mean`, `rainfall_flag` | 0 (dry sessions) |
| `yellow_flag_count` | 3 |
| `red_flag_count` | 1 |
| `green_flag_count` | 2 |
| `pit_exit_message_count` | 3 |

### Handling plan

| Action | Features |
|--------|----------|
| **Median impute (modeling pipeline)** | `std_*`, pit duration fields when null |
| **Zero-fill (already in Gold)** | `pit_stop_count`, RC counts when absent |
| **Drop if high on full run** | Any feature &gt;50% missing after 2023–2025 (none in smoke) |
| **Structural — do not impute in Silver** | Pit duration null = no stop |

---

## 5. Feature summary assessment

| Domain | Plausible? | Smoke caveat |
|--------|------------|--------------|
| Lap duration ~91–122 s | Yes | `best_lap_duration` max 121.9 s (outlier lap retained) |
| Pit duration ~20–75 s | Yes | Long stop max 74.7 s |
| Position 1–20 | Yes | Aligns with F1 field |
| Weather | Yes | **2 unique session-level values** (2 sessions) |
| RC counts | Yes | Low variance across sessions |

### Scaling

| Model | Need |
|-------|------|
| Logistic Regression | **Yes** — `StandardScaler` on numeric (implemented) |
| Random Forest | No |
| LightGBM | No |

### Answers

| Question | Answer |
|----------|--------|
| Ranges plausible? | **Yes** |
| Obviously wrong? | **No** |
| Constant in smoke? | Rainfall + some RC counts — **expected** |
| Useful on full data? | Lap/pit/position features yes; rainfall/RC counts should vary |

---

## 6. Baseline readiness

| Baseline | Supported? | Evidence |
|----------|------------|----------|
| Random | **Yes** | `random_baseline_predictions` in `baselines.py`, notebook 04 |
| Heuristic `first_observed_position ≤ 10` | **Yes** | Column present, 0 null in smoke; used outside sklearn pipeline |
| Majority class | **Code only** | `majority_class_baseline()` exists; **not wired** in notebook 04 |

### Heuristic guidance

| Question | Recommendation |
|----------|----------------|
| `first_observed_position` vs `early_avg_position`? | Keep **`first_observed_position`** — matches project_plan and interpretability |
| Missing position? | Predict 0 (implemented) — rare in smoke |
| Defensible for report? | **Yes** as simple **top-10 early-position** rule; state correlation with target |

---

## 7. Model readiness

| Model | Ready? | Notes |
|-------|--------|-------|
| Logistic Regression | **Yes** | `class_weight="balanced"`, median impute, OHE if categoricals added |
| Random Forest | **Yes** | 200 trees, balanced |
| LightGBM | **Yes** | Balanced; all default X columns numeric → no native categorical needed |

| Check | Status |
|-------|--------|
| Numeric features | 40 in default `X` |
| Categorical in default `X` | **None** — metadata excluded; OHE ready if opted in |
| Missing handling | `SimpleImputer` in pipelines |
| Class imbalance | `class_weight="balanced"` (full run may still need metric focus on recall/F1) |
| Season split | `session_year` in mart; `resolve_modeling_splits(mode="full")` requires 2023–2025 |
| Smoke limitations | 2024-only → `smoke` mode + fallback split; **no official metrics** |

### Answers

| Question | Answer |
|----------|--------|
| Modules aligned with data? | **Yes** |
| Missing transforms? | Optional: explicit **feature tier** config; majority baseline in notebook |
| `class_weight` balanced? | **Already yes** |
| LightGBM categoricals? | Default X is all numeric; **one-hot metadata only if added to feature list** |

---

## 8. Split readiness

| Strategy | Assessment |
|----------|------------|
| Train 2023 / Val 2024 / Test 2025 | **Best primary strategy** — implemented in `splits.py` |
| Incomplete 2025 | `full` mode **raises** on empty split — correct; ingest missing seasons or document gap |
| Fallback time split | **Smoke/development only** — `evidence_tier: smoke_wiring_only` in manifest |
| Notebook 04 smoke vs official | `MODELING_MODE="smoke"` → wiring only; official evidence requires `full` + multi-season Gold |

---

## 9. Proposed final modeling feature list

### A. Required identifiers (never in `X`)

- `session_key`
- `meeting_key`
- `driver_number`

### B. Target

- `points_finish`

### C. Diagnostic outcome columns (evaluation / error analysis only)

- `final_position`
- `result_points`
- `result_dnf`
- `result_dns`
- `result_dsq`
- `diagnostic_final_observed_position`

### D. Default numeric modeling features (40) — `train.py` / dictionary `allowed_for_modeling=True`

**Tier 1 — Early-race / lower interpretation risk**

- `avg_first5_lap_duration`, `best_first5_lap_duration`, `std_first5_lap_duration`
- `first_observed_position`, `early_avg_position`, `early_min_position`, `early_max_position`, `position_observation_count`

**Tier 2 — Full-session analytical (default bundle includes these)**

- **Laps:** `lap_count`, `avg_lap_duration`, `median_lap_duration`, `best_lap_duration`, `std_lap_duration`, `avg_sector_1`, `avg_sector_2`, `avg_sector_3`, `avg_i1_speed`, `avg_i2_speed`, `avg_st_speed`, `pit_out_lap_count`
- **Pit:** `pit_stop_count`, `avg_pit_duration`, `min_pit_duration`, `max_pit_duration`, `first_pit_lap`, `early_pit_stop_flag`
- **Weather:** `avg_air_temperature`, `avg_track_temperature`, `avg_humidity`, `avg_pressure`, `avg_wind_speed`, `rainfall_mean`, `rainfall_flag`
- **Race control:** `race_control_message_count`, `flag_message_count`, `yellow_flag_count`, `red_flag_count`, `green_flag_count`, `safety_car_message_count`, `pit_exit_message_count`

### E. Optional categorical modeling features (opt-in; not in default 40)

- `team_name`
- `circuit_short_name`
- `session_country_name`
- `location`
- `driver_country_code`
- `session_type`
- `session_name`

Do **not** put `session_year` in `X` when using season-based splits (leakage across split boundary if misused).

### F. Exclude from `X`

- All identifiers, target, diagnostics
- Raw outcomes: `final_position`, `result_points`, `result_*`
- Metadata (unless explicitly encoded in a sensitivity experiment)
- `starting_grid` features (not in mart)
- Smoke-constant features for importance claims only: document, optional drop for ablation (`rainfall_*`, fixed RC counts)

---

## 10. Recommended fixes

### A. Must fix before full 2023–2025 run

*None blocking pipeline execution.*

### B. Should fix before modeling / official metrics

1. Set `MODELING_MODE="full"` only after multi-season Gold exists.
2. Freeze **`modeling_features_v1.txt`** (or CSV) listing the 40 default columns + tier labels.
3. Run notebook 04 with `validate_no_leakage` passing on full mart.

### C. Should fix before final report

1. **README / project_plan:** Clarify task = **integrated session features**, not strict pre-race; split Tier 1 vs Tier 2 features.
2. **Leakage guard reason text:** Change “pre-race or in-race” to **“engineered session feature (see feature tier)”** per group.
3. Report **target distribution by season** after full run.
4. Document **heuristic baseline** and its correlation with target.

### D. Nice to have

1. Wire **majority_class_baseline** in notebook 04.
2. Optional **Tier-1-only** model run for stricter leakage narrative.
3. Export `modeling_feature_manifest.json` alongside `model_run_manifest.json`.

### E. No action needed

- Target definition (`points > 0`)
- Forbidden outcome columns in guard
- Default 40-feature selection logic in `train.py`
- Imputation + balanced class weights in pipelines
- Heuristic + random baselines in notebook 04

---

## 11. Phase 4 verdict

### **PASS WITH FIXES**

The Gold mart and modeling stack are **ready for full 2023–2025 pipeline execution and notebook 04 wiring**. Leakage is **controlled for explicit post-race fields**. The required fix is **documentation and task framing**: default features include **full-session analytical** signals; the written report must not claim strict pre-race prediction without a reduced feature set.

---

## 12. Proceed to full run?

**Yes.** Run ingestion → Silver → Gold for 2023–2025, then `MODELING_MODE="full"` for official metrics. Use smoke evidence only for **pipeline verification**, not performance conclusions.

---

*Audit from frozen CSV + Parquet evidence and source code review. No code or notebooks were modified.*
