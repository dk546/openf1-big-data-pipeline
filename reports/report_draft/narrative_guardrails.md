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
| `race_control.qualifying_phase` | Known 100 % null in current OpenF1 pull | Structural — ignored by Gold; race-control features use `message` and `flag` only |
| `race_control` duplicate diagnostic groups | Event-stream rebroadcasts, multi-driver messages | Retained on purpose (`SIL_RC_DUP_CHECK_RETAINED` in `silver_cleaning_rules.csv`); also noted in `silver_data_quality_notes.csv` |
| `pit.stop_duration` null | API / event structure | Often structural — see `silver_data_quality_notes.csv` |
| Gold `avg_pit_duration`, `min_pit_duration`, `max_pit_duration`, `first_pit_lap` ~25 % missing | Driver-races with zero pit stops | Structural — pit *count* is zero-filled; pit *duration* aggregates correctly stay NaN because the set of stops is empty |
| `starting_grid` empty | Endpoint 404 or no qual data in scope | Optional endpoint — heuristic baseline uses `first_observed_position` (Tier 1) as the early-race position proxy, not grid |
| Gold `driver_country_code` ~34 % missing | OpenF1 drivers endpoint frequently omits country code for older sessions | Metadata only; excluded from model `X` by `model_feature_plan.csv` (`default_include=False`, `allowed_for_modeling=False`); not a feature-quality problem |
| Gold `final_position` ~10 % missing | DNF drivers — no classified position | Diagnostic outcome blocked from features by leakage guard; never enters `X` |
| Gold `lap_count`, `pit_out_lap_count` 0.17 % missing (pre-fix) | 3 DNS drivers with no laps recorded | Safe Gold cleanup added these to `EVENT_ABSENCE_ZERO_COLS` so the rebuild zero-fills them; **post-fix Gold rerun confirms 0 % missing** for both (alongside all other count-style event-absence features) |
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

When full-run artifacts are missing, use explicit placeholders in tables and prose. The list below has been pruned to reflect the current state — Bronze, Silver, and Gold full-run artifacts are available; only Notebook 04 modeling outputs and a few derived `reports/tables/*.csv` from Notebook 05 remain.

**Resolved (do NOT use as placeholders any more):**

- ~~`[PENDING: full 2023–2025 ingest]`~~ — Bronze full run completed; 148,184 rows.
- ~~`[PENDING: full-run Gold row count]`~~ — Gold full run completed; 1,756 rows at driver-race grain.
- ~~`[PENDING: targeted retry results — RUN_TARGETED_RETRY=True]`~~ — Retry executed; reconciliation runs against `ingestion_manifest_effective.csv`.
- ~~`[PENDING: post-retry target coverage]`~~ — 88 race sessions populated in Silver `session_result` (vs goal 89).

**Still active:**

- `[PENDING: official test metrics — MODELING_MODE=full]` (Notebook 04)
- `[PENDING: Notebook 04 baseline metrics]`
- `[PENDING: confusion matrix and per-segment error analysis]` (Notebook 04 + 05)
- `[PENDING: reproducibility artifacts table]` (Notebook 05)
- `[PENDING: feature importance top-20]` (Notebook 04 + 05)
- `[PENDING: Notebook 05 derived report tables under reports/tables/]`

Do not interpolate or estimate final metrics from smoke runs.

---

## H. Bronze retry and reconciliation language

Use the wording below when describing the Bronze targeted retry and manifest-file reconciliation workflow. These statements are the source of truth for Chapter 3.2, 4.2–4.5, and 6.6.

### H.1 Do not hide API failures

> The initial full Bronze run encountered HTTP 429 rate-limit failures on some session endpoints. These failures were recorded in the ingestion manifest and handled through a targeted retry workflow.

Do not omit these failures or describe them as "minor"; treat them as a first-class data-quality finding that the pipeline surfaces and remediates.

### H.2 Do not call stale files corruption

> The stale files were real OpenF1 records from an earlier smoke run, but they were not part of the current full-run manifest. The reconciliation report exposed this manifest-file mismatch before Silver processing.

Use "stale" or "out of manifest" — not "corrupt", "invalid", or "broken". The data inside is valid; the inconsistency is between the manifest and Drive state.

### H.3 Explain why retry is better than a full rerun

> A targeted retry preserves successful full-run outputs and only reattempts failed required session endpoints with slower pacing, reducing API load and avoiding unnecessary re-ingestion.

Always pair the word "retry" with the words "targeted" and "required session endpoints" so the reader understands global endpoints (`meetings`, `sessions`) and `starting_grid` are not retried by default.

### H.4 Explain `starting_grid` correctly

> `starting_grid` is treated as optional because OpenF1 does not consistently return it for race sessions; the downstream target and feature mart do not depend on it.

`starting_grid` failures are recorded in the manifest with `reconciliation_status = optional_missing` and do not block Silver. The heuristic baseline uses `first_observed_position ≤ 10` from Gold features, not grid position.

### H.5 Explain target coverage

> `session_result` coverage is the key Bronze completeness metric because it determines whether a race session can contribute labeled examples to the Gold modeling dataset.

When citing coverage numbers, always state both the original-run successes and any retry successes; report effective coverage as `original successes ∪ retry successes` out of 89 race sessions for the 2023–2025 scope.

### H.6 Avoid overclaiming

> Retry improves coverage where API failures are transient. Remaining true 404s or persistent endpoint failures are documented as source availability limitations.

Do not promise that retry recovers everything. Persistent failures after retry — particularly any `manifest_failed_no_file` rows for required endpoints — must be discussed as a source-data limitation, not a pipeline defect.

### H.7 Cite reconciliation as a data-quality control, not a side note

> Manifest-vs-file reconciliation is the Bronze data-quality control that prevents Silver from running on an inconsistent Bronze state. Each `(endpoint, year, session_key)` is classified as `matched`, `row_count_mismatch`, `manifest_success_missing_file`, `failed_manifest_file_exists`, `stale_file_not_in_success_manifest`, `optional_missing`, or `manifest_failed_no_file`.

Treat the reconciliation summary as evidence on the same footing as Silver missingness reports. Quote the seven status categories verbatim when defining terms in Chapter 4.

### H.8 Always reconcile against the effective post-retry manifest

> After targeted retry, reconciliation must run against `artifacts/manifests/ingestion_manifest_effective.csv` — the original manifest overlaid with retry rows — not the original `ingestion_manifest.csv`. Reconciling against the original after retry incorrectly classifies recovered files as `failed_but_file_present`.

Use the wording: "the effective post-retry manifest combines the original ingestion manifest with successful retry rows; its `manifest_source` column tags each row as `original` or `retry` for auditability. The original manifest is preserved unchanged for provenance."

When citing reconciliation totals in the report:

- If reconciliation was run **before** retry, cite the manifest as `ingestion_manifest.csv` and label totals as "pre-retry".
- If reconciliation was run **after** retry, cite the manifest as `ingestion_manifest_effective.csv` and label totals as "post-retry (effective)".

Never compare pre-retry reconciliation totals directly to post-retry totals without naming which manifest each was computed against.
