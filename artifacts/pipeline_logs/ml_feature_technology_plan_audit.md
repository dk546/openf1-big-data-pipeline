# ML, feature, and technology plan audit

**Date/time:** 2026-05-19  
**Scope:** Planning alignment and implementation readiness before Gold Colab smoke and modeling  
**Databricks:** Ignored — explicitly out of scope per lecturer confirmation

---

## Files reviewed

| File | Purpose |
|------|---------|
| `README.md` | Public run order, stack, ML/leakage strategy |
| `project_context.md` | Locked decisions, artifacts, non-negotiables |
| `project_plan.md` | Phases, technology table, modeling plan |
| `implementation_checklist.md` | Progress tracking |
| `artifacts/pipeline_logs/silver_output_audit.md` | Silver smoke verdict |
| `artifacts/pipeline_logs/gold_feature_mart_implementation.md` | Gold code audit |
| `src/openf1_pipeline/gold/build_feature_mart.py` | Mart builder + leakage guard |
| `src/openf1_pipeline/features/feature_dictionary.py` | Feature dictionary |
| `notebooks/03_gold_feature_engineering.ipynb` | Gold Colab notebook |
| `requirements.txt` | `pyspark`, `duckdb` declared |

---

## Locked classification target

| Item | Definition |
|------|------------|
| Target | `points_finish` = 1 if `points` > 0, else 0 |
| Source | `session_result_clean.parquet` via `build_target_base()` |
| Grain | `session_key`, `meeting_key`, `driver_number` |
| Output | `data/gold/driver_race_feature_mart.parquet` |

Diagnostic columns: `final_position`, `result_points`, `result_dnf`, `result_dns`, `result_dsq` — not model features.

---

## Locked model list (notebook `04` — not implemented)

| # | Model | Purpose |
|---|--------|---------|
| 1 | Random baseline | Minimum performance floor |
| 2 | Heuristic: `first_observed_position` ≤ 10 | Domain early-race proxy |
| 3 | Logistic Regression | Interpretability |
| 4 | Random Forest | Nonlinear interactions |
| 5 | LightGBM | Strong tabular classifier |

No deep learning.

---

## Locked split strategy

**Primary:** Train 2023 · Validation 2024 · Test 2025 (season column from session metadata).

**Fallback:** Train 2023–early 2024 · Validation late 2024 · Test 2025 — document cutoff in `run_manifest.json`.

**Prohibited as primary:** Random row-level split (leaks session/race structure).

---

## Locked feature groups

| Group | Gold implementation | Modeling notes |
|-------|----------------------|----------------|
| Driver/team metadata | `build_metadata_features` | Encode if used; `allowed_for_modeling=False` by default in dictionary |
| Session/circuit metadata | sessions + meetings join | Same |
| Early lap pace | `avg_first5_*`, first-five filter | Allowed |
| Lap pace aggregates | `build_lap_features` | Allowed |
| Pit strategy | `build_pit_features` | Allowed; zeros filled for no-stop drivers |
| Early position proxy | `first_observed_position`, `early_avg_*` | Allowed; heuristic baseline uses `first_observed_position` |
| Weather | `build_weather_features` | Session-level join |
| Race control | `build_race_control_features` | Session-level counts |
| Starting grid | Not in mart | Optional Silver table; empty on smoke — OK |

---

## Leakage rules

**Forbidden as predictors** (enforced in `LEAKAGE_FORBIDDEN_COLUMNS` + `diagnostic_*`):

`position`, `points`, `final_position`, `result_points`, `result_dnf`, `result_dns`, `result_dsq`, `duration`, `gap_to_leader`, `number_of_laps`, `diagnostic_*`

**Mechanisms:**

- `build_leakage_guard_report()` → `gold_leakage_guard_report.csv`
- `get_model_feature_columns()` — excludes identifiers, target, leakage, metadata (metadata can be re-enabled in modeling with explicit encoding)
- `build_feature_dictionary()` — `modeling_role`, `allowed_for_modeling`

---

## Gold implementation audit vs locked strategy

| Check | Status | Notes |
|-------|--------|-------|
| Base = `session_result_clean` | **Pass** | `build_target_base(tables["session_result"])` |
| `points_finish` from `points > 0` | **Pass** | `(points > 0).astype(int)` |
| Grain = keys + driver | **Pass** | `DRIVER_KEYS`; duplicate check raises |
| Feature groups match plan | **Pass** | Lap, pit, position, weather, RC, metadata |
| Leakage guard complete | **Pass** | Forbidden set + diagnostic prefix |
| `get_model_feature_columns` | **Pass** | Uses leakage report |
| `starting_grid` not required | **Pass** | Not loaded into mart; notebook warns if empty |
| Early position proxy | **Pass** | `first_observed_position`, `early_avg/min/max_position` |
| Feature dictionary columns | **Pass** | `modeling_role`, `allowed_for_modeling`, `missing_pct` |
| Notebook 03 preflight | **Pass** | Silver paths + `build_gold_feature_mart` |

**Gaps (minimal fixes recommended — no notebook run in this task):**

1. **Heuristic baseline doc drift:** Older text referenced grid position; now locked to `first_observed_position` ≤ 10 — docs updated in this audit pass.
2. **Metadata in modeling:** `get_model_feature_columns()` excludes metadata by design; notebook `04` should explicitly one-hot or drop metadata columns — document in modeling notebook.
3. **Race-only filter:** Mart includes all `session_result_clean` rows; modeling may filter `session_type == 'Race'` — document in splits module.
4. **DuckDB / PySpark:** Not yet in `src/` — see compliance below.

---

## PySpark / DuckDB compliance assessment

### Current usage

| Technology | In `requirements.txt` | In `src/openf1_pipeline` | In notebooks |
|------------|----------------------|--------------------------|--------------|
| **PySpark** | Yes (`pyspark>=3.5.0`) | **No** | Mentioned in `scripts/generate_notebooks.py` only |
| **DuckDB** | Yes (`duckdb>=0.9.0`) | **No** | **No** |
| **pandas** | Yes | **Yes** (Silver, Gold, profiling) | `01`–`03` |

**Conclusion:** PySpark and DuckDB are **declared but not yet demonstrated** in pipeline code. The course technology requirement is met in **planning and README**; **implementation evidence** still needed before final submission.

### Recommended minimal additions

| Priority | Addition | Essential before submission? | Report mention |
|----------|----------|------------------------------|----------------|
| **A** | `src/openf1_pipeline/quality/duckdb_validation.py` + call from notebook `03` or small `notebooks/03b_duckdb_validation.ipynb` | **Yes** — low effort, high value for “analytical SQL” pillar | “DuckDB validated Gold row counts, target distribution, and missingness against pandas outputs” |
| **B** | Optional `src/openf1_pipeline/gold/spark_lap_features.py` or cells in full-run appendix notebook — PySpark groupBy on `laps_clean` vs pandas sample | **Recommended** for full 2023–2025 narrative | “PySpark used for lap aggregations at scale; smoke used pandas” |
| **C** | Re-run DuckDB queries on full Parquet after full pipeline | After full run | Include SQL snippets in appendix |

**Smoke runs:** pandas-only Gold is acceptable; do **not** block notebook `03` on PySpark.

**Optional:** PySpark can remain optional for smoke if DuckDB validation runs and full-run PySpark is documented once.

---

## Go / no-go

| Gate | Verdict | Rationale |
|------|---------|-----------|
| **Run notebook `03` (Gold smoke)** | **GO** | Silver smoke passed; Gold code aligns with locked target, grain, leakage rules; synthetic local test passed |
| **Build notebook `04` (modeling)** | **NO-GO until** | (1) Gold smoke artifacts on Drive, (2) leakage report reviewed, (3) target distribution sensible for smoke, (4) modeling code not requested yet |

**Pre–notebook 03 checklist:**

- [x] Silver smoke passed (`evidence/smoke_2024_maxsessions2/`)
- [x] ML / feature / leakage strategy locked in project docs
- [ ] Run `03` on Colab with same `OPENF1_DATA_ROOT` as `01`/`02`
- [ ] Copy Gold parquet + DQ CSVs to evidence folder
- [ ] (Recommended before final report) Add DuckDB validation module + one Colab cell

**Pre–notebook 04 checklist:**

- [ ] Gold smoke (and ideally full) mart on Drive
- [ ] `gold_leakage_guard_report.csv` reviewed — no forbidden column with `allowed_for_modeling=True`
- [ ] `feature_dictionary.csv` reviewed
- [ ] Implement `modeling/splits.py`, `baselines.py`, `train.py`, `evaluate.py` per locked model list
- [ ] DuckDB validation run (recommended)

---

## Files updated in this planning pass

- `project_context.md` — locked decisions section; target/models/splits/leakage/tech
- `project_plan.md` — v1.1 alignment; heuristic; PySpark/DuckDB as planned
- `implementation_checklist.md` — §0 planning locked; modeling items updated
- `README.md` — technology stack, ML strategy, leakage, run order `00`–`05`
- `artifacts/pipeline_logs/ml_feature_technology_plan_audit.md` — this file

**Not run:** notebooks, ingestion, modeling, fake outputs.
