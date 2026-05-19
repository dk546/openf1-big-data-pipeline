# Narrative Guardrails (Locked)

Use these statements consistently in the final written deliverable, slides, and viva. They align with Phase 4 audits and `project_context.md`.

---

## A. Task framing — not strict pre-race prediction

**Do not claim** strict pre-race prediction, grid-only models, or a single fixed “lap 10 decision point” unless you explicitly run a reduced Tier-1-only experiment and label it as sensitivity analysis.

**Use:**

> **Points-finish classification using integrated race-session features.**

The default model includes early-session signals **and** full-session analytical aggregates produced from the complete race session in the Gold mart.

---

## B. Feature tiers

Explain two tiers in Chapter 5 and Chapter 6:

| Tier | Label | Examples | Interpretation |
|------|-------|----------|----------------|
| **Tier 1** | `tier1_early` | `first_observed_position`, `early_avg_position`, `avg_first5_lap_duration`, … | Early-session features (first five laps / first five position observations) |
| **Tier 2** | `tier2_full_session` | `lap_count`, `avg_lap_duration`, `pit_stop_count`, `avg_air_temperature`, `race_control_message_count`, … | Full-session analytical features — allowed in default model; document as outcome-adjacent, not label leakage |

**Source of truth:** `artifacts/feature_definitions/model_feature_plan.csv` (40 default numeric features = 8 + 32).

**Optional categoricals** (`team_name`, `circuit_short_name`, …) are opt-in; `session_year` must not appear in **X** when using season-based train/val/test splits.

---

## C. pandas, PySpark, and DuckDB boundaries

| Technology | Role | Report wording |
|------------|------|----------------|
| **Python + requests** | OpenF1 API ingestion to Bronze JSONL | Ingestion layer |
| **PySpark** | Primary engine: Bronze profiling, Silver cleaning, Gold feature mart | Main transformation engine |
| **DuckDB** | Independent SQL validation over Parquet/CSV artifacts | Independent validation layer — not a replacement for Spark transforms |
| **pandas** | Small CSV exports, notebook previews, ML matrix handoff after Gold | Supporting layer at reporting/ML boundaries only |
| **scikit-learn / LightGBM** | Notebook `04` classifiers | Final consumption layer on Gold |
| **Databricks** | — | **Out of scope** — do not imply workspace dependency |

---

## D. Missingness and outliers

**Not all missingness is a data error.**

| Pattern | Typical cause | Report stance |
|---------|---------------|-----------------|
| `race_control` without `driver_number` | Session-level messages | Structural — document, do not impute as driver error |
| `pit.stop_duration` null | API / event structure | Often structural — see `silver_data_quality_notes.csv` |
| `starting_grid` empty | Endpoint 404 or no qual data in scope | Optional endpoint — heuristic baseline uses early position, not grid |
| High missingness on optional joins | No pit/weather in session | Distinguish “no event” vs “bad join” |

**Outliers in racing telemetry:** IQR-based flags in `silver_outlier_report.csv` are **detection**, not automatic removal. Racing lap times and speeds can be legitimately extreme.

---

## E. Smoke vs full evidence

| Run profile | Purpose | Allowed in final performance claims? |
|-------------|---------|--------------------------------------|
| **Smoke** (`SMOKE_TEST=True`, `MAX_SESSIONS=2`, often 2024 only) | Validate code paths, joins, leakage guard, notebook wiring | **No** — label as wiring / integration evidence only |
| **Full** (2023–2025, `MODELING_MODE="full"`) | Official season splits and reportable metrics | **Yes** — after successful Colab `01`–`05` on Drive |

**Smoke-specific caveats (observed in audits):**

- Small row counts (~40 Gold rows) → unstable rates and metrics
- `points_finish` rate may be ~50% — not representative of full seasons
- Some weather/RC features constant within smoke sessions

**Manifest fields:** Check `modeling_mode`, `evidence_tier`, and `split_method` in `model_run_manifest.json`.

---

## F. What to block from model features (always)

Regardless of tier, **never** use as predictors:

- Identifiers: `session_key`, `meeting_key`, `driver_number`
- Target: `points_finish`
- Raw outcomes: `position`, `points`, `final_position`, `result_*`
- Diagnostics: `diagnostic_*`
- Columns blocked in `gold_leakage_guard_report.csv`

Tier 2 features are **engineered aggregates**, not raw finishing outcomes.

---

## G. Placeholder convention for pending full run

When full-run artifacts are missing, use explicit placeholders in tables and prose:

- `[PENDING: full 2023–2025 ingest]`
- `[PENDING: full-run Gold row count]`
- `[PENDING: official test metrics — MODELING_MODE=full]`

Do not interpolate or estimate final metrics from smoke runs.
