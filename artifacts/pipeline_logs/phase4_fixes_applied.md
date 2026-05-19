# Phase 4 Fixes Applied

| Field | Value |
|-------|-------|
| **Date/time (UTC)** | 2026-05-19T18:39:00Z (approx.; applied during Cursor implementation) |
| **Plan reference** | Phase 4 ML Fixes — wording, frozen feature plan, modeling wiring |
| **Notebooks executed** | **No** — code and notebook source regenerated only |

---

## Files changed

| File | Change |
|------|--------|
| `README.md` | Integrated race-session task framing; Tier 1/Tier 2; infrastructure-first; `model_feature_plan.csv` |
| `project_plan.md` | §1.1 and §7.2 wording aligned with Tier 1/Tier 2 |
| `project_context.md` | Task framing, §D smoke/full, §F tiers, ML as consumption layer |
| `src/openf1_pipeline/modeling/feature_selection.py` | **Created** — frozen constants, plan builder, resolver |
| `artifacts/feature_definitions/model_feature_plan.csv` | **Created** — 62 rows (40 default numeric + metadata/diagnostic/keys) |
| `src/openf1_pipeline/modeling/train.py` | `get_model_feature_columns()` delegates to `resolve_model_feature_columns()` |
| `src/openf1_pipeline/features/feature_dictionary.py` | `feature_tier` column; Tier-aware descriptions |
| `src/openf1_pipeline/gold/build_feature_mart.py` | Leakage guard reason text for allowed engineered features |
| `scripts/rebuild_colab_notebooks.py` | `build_04()` — plan load/display, majority baseline, full-mode season checks |
| `notebooks/04_modeling_evaluation.ipynb` | Regenerated from rebuild script (not executed) |
| `implementation_checklist.md` | Phase 4 fixes subsection added |

---

## Task framing fix

Replaced implicit **strict pre-race prediction** wording with:

> **Points-finish classification using integrated race-session features**

Raw post-race outcomes (`position`, `points`, `result_*`, diagnostics) remain blocked. Tier 2 full-session analytical features are **allowed** in the default model and documented—not treated as label leakage.

---

## Tier 1 vs Tier 2 feature strategy

| Tier | Count (default X) | Examples |
|------|---------------------|----------|
| **tier1_early** | 8 | `avg_first5_lap_duration`, `first_observed_position`, `early_avg_position`, … |
| **tier2_full_session** | 32 | `lap_count`, `avg_lap_duration`, `pit_stop_count`, `avg_air_temperature`, `race_control_message_count`, … |
| **Total default numeric** | **40** | Frozen in `model_feature_plan.csv` |

Optional categoricals (`team_name`, `circuit_short_name`, …) are `allowed_for_modeling=True`, `default_include=False`. `session_year` is optional metadata only—excluded from default X to avoid season-split leakage.

---

## Model feature plan behavior

`resolve_model_feature_columns()` / `get_model_feature_columns()` resolution order:

1. `artifacts/feature_definitions/model_feature_plan.csv` — rows with `allowed_for_modeling=True` and `default_include=True`, minus runtime forbidden columns
2. Else `feature_dictionary.csv` — `allowed_for_modeling=True` with same exclusions
3. Else clear `ValueError` with remediation steps

Notebook 04 creates `model_feature_plan.csv` if missing via `save_model_feature_plan()`.

---

## Baseline updates (notebook 04)

- **majority_class_baseline** added (validation + test splits; train prevalence from `y_train`)
- Smoke outputs labeled explicitly (`SMOKE MODE` / `SMOKE OUTPUT`)
- `MODELING_MODE="full"` requires non-empty train/val/test and seasons `{2023}`, `{2024}`, `{2025}` respectively

---

## Remaining risks before full 2023–2025 run

| Risk | Mitigation |
|------|------------|
| Smoke mart has 50/50 `points_finish`, constant weather/RC on 2 sessions | Do not use smoke metrics in MBA report |
| `feature_dictionary.csv` in Drive may lack `feature_tier` until Gold notebook 03 re-run | `model_feature_plan.csv` is modeling source of truth until refresh |
| Full season splits empty until multi-season ingest | Run `01` with `SMOKE_TEST=False`, `INGEST_SEASONS=[2023,2024,2025]`, then `02`–`03`, then `04` with `MODELING_MODE="full"` |
| Silver reporting CSVs from Phase 2 code fixes need Colab 02 rerun | Re-run notebook 02 to refresh evidence |

**Full 2023–2025 modeling:** **Not ready** until Colab pipeline + Gold complete; code/docs are ready to support that run.

---

## Verification (local, no notebooks)

```text
model_feature_plan.csv: tier1_early=8, tier2_full_session=32, total_default_numeric=40
get_model_feature_columns(): resolves 40 columns from plan CSV
```
