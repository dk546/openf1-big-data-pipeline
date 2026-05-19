# Gold feature mart implementation audit

**Date/time:** 2026-05-19 (implementation in Cursor; Colab execution pending)

## Files changed

| File | Change |
|------|--------|
| `src/openf1_pipeline/gold/build_feature_mart.py` | Full Gold builder, joins, reports, leakage guard |
| `src/openf1_pipeline/features/feature_dictionary.py` | Feature dictionary from Gold mart |
| `notebooks/03_gold_feature_engineering.ipynb` | Rebuilt via `scripts/rebuild_colab_notebooks.py` |
| `scripts/rebuild_colab_notebooks.py` | Added `build_03()` |
| `README.md` | Gold run order, grain, target, leakage, reports |
| `implementation_checklist.md` | Gold code items marked (Colab runs still pending) |
| `artifacts/pipeline_logs/gold_feature_mart_implementation.md` | This audit |

## Silver inputs expected

Under `{OPENF1_DATA_ROOT}/data/silver/` (or repo-local `data/silver/`):

| Table | File | Required |
|-------|------|----------|
| Session results | `session_result_clean.parquet` | **Yes** (base + target) |
| Laps | `laps_clean.parquet` | **Yes** (notebook preflight) |
| Drivers | `drivers_clean.parquet` | **Yes** (notebook preflight) |
| Sessions | `sessions_clean.parquet` | Yes (meeting_key / metadata) |
| Meetings | `meetings_clean.parquet` | Recommended |
| Pit | `pit_clean.parquet` | Optional (zeros filled if absent) |
| Position | `position_clean.parquet` | Optional |
| Weather | `weather_clean.parquet` | Optional |
| Race control | `race_control_clean.parquet` | Optional |
| Starting grid | `starting_grid_clean.parquet` | Optional (empty OK on smoke) |

Smoke reference: `evidence/smoke_2024_maxsessions2/` Silver inventories (40 session_result rows, 2031 laps, etc.).

## Gold grain

One row per **`session_key` + `meeting_key` + `driver_number`**, keyed from `session_result_clean`.

## Target definition

- **`points_finish`** = `1` if `points` > 0, else `0`
- Diagnostic columns (not for modeling): `final_position`, `result_points`, `result_dnf`, `result_dns`, `result_dsq`

## Feature groups

1. **Target base** — from `session_result`
2. **Metadata** — drivers, sessions, meetings
3. **Laps** — counts, duration stats, sectors, speeds, first-five lap stats, pit-out lap count
4. **Pit** — stop count, durations, first pit lap, early pit flag (≤ lap 10)
5. **Position** — first five observations only (early avg/min/max); `diagnostic_final_observed_position` excluded from modeling
6. **Weather** — session-level averages + rainfall flag
7. **Race control** — session-level message counts (case-insensitive text match)

## Leakage prevention rules

- Forbidden predictive columns: `position`, `points`, `final_position`, `result_*`, `duration`, `gap_to_leader`, `number_of_laps`
- `diagnostic_*` prefix excluded from `allowed_for_modeling`
- Identifiers and metadata flagged `allowed_for_modeling=False` (metadata may be encoded later explicitly)
- `gold_leakage_guard_report.csv` documents each column
- `get_model_feature_columns()` returns safe feature list for downstream modeling

## Reports generated

| Report | Path |
|--------|------|
| Gold mart | `data/gold/driver_race_feature_mart.parquet` |
| Summary stats | `reports/data_quality/gold_feature_summary_stats.csv` |
| Missingness | `reports/data_quality/gold_feature_missingness.csv` |
| Target distribution | `reports/data_quality/gold_target_distribution.csv` |
| Join quality | `reports/data_quality/gold_join_quality_report.csv` |
| Leakage guard | `reports/data_quality/gold_leakage_guard_report.csv` |
| Feature dictionary | `artifacts/feature_definitions/feature_dictionary.csv` |

## How to run notebook 03 in Colab

1. Complete `01_ingestion_bronze.ipynb` and `02_silver_cleaning_quality.ipynb` with **`USE_GOOGLE_DRIVE=True`** (same `OPENF1_DATA_ROOT`).
2. Open `notebooks/03_gold_feature_engineering.ipynb` in a **new** tab (fresh kernel).
3. Run the setup cell (clone, `pip install -e .`, Drive mount).
4. Run Silver preflight (checks `session_result`, `laps`, `drivers`).
5. Run `build_gold_feature_mart(...)` and review target distribution, missingness, join quality, leakage guard, and mart head.
6. Copy outputs to `evidence/<run_id>/` for MBA submission.

## Remaining risks

- **Colab not run yet** — all Gold CSV/parquet artifacts must be produced on Drive before checklist “Colab smoke” items are checked.
- **Empty `starting_grid_clean`** — no grid features in current builder (by design; notebook warns only).
- **Drivers without lap/pit/position rows** — left joins leave nulls for continuous features (not zero-filled except pit/race-control counts).
- **Session types** — mart includes all rows in `session_result_clean`; filter to Race sessions in modeling if required.
- **Target vs course wording** — target is **points > 0**, not top-10 position proxy; align MBA report text with `gold_target_distribution.csv`.

## Verification

- Local synthetic test: `build_gold_feature_mart` completes and writes all report paths.
- **No modeling** implemented in this change.
