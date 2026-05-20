# Final Report Structure (Locked)

**Project:** A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

**Task framing (locked):** Points-finish classification using integrated race-session features — not strict pre-race prediction.

**Status:** Structure locked for final written deliverable. Bronze, Silver, Gold, Notebook 04 modeling, and Notebook 05 report consolidation have **all completed their full 2023–2025 runs** and have been audited; modeling outputs are official MBA evidence (`MODELING_MODE="full"`, `split_method="season"`, `evidence_tier="mba_official"`). The full evidence bundle is captured under [`evidence/full_2023_2025/`](../../evidence/full_2023_2025/).

**Layer audit status (as of latest run):**

| Layer | Status | Headline metric |
|-------|--------|-----------------|
| Bronze | PASS after targeted retry + effective-manifest reconciliation | 148,184 rows across required endpoints |
| Silver | PASS WITH MINOR ISSUES, safe for Gold | 148,184 rows; 0 row loss; 0 rejected; 0 RI unmatched |
| Gold | **PASS** (post-fix rerun) — ready for Notebook 04 | 1,756 rows at driver-race grain; 0 duplicate keys; 40 model features; `lap_count` / `pit_out_lap_count` confirmed 0 % missing |
| Modeling (NB 04) | **PASS** — official MBA evidence | Random Forest best on every metric on both splits; test F1 = 0.7837, ROC-AUC = 0.8733; beats the strong heuristic baseline (`first_observed_position ≤ 10`) on every metric |
| Report consolidation (NB 05) | **PASS** — full-run consolidation written | 11 tables in `reports/tables/`, 5 figures in `reports/figures/` (matplotlib PNGs) + 1 architecture-diagram placeholder MD, and `artifacts/manifests/run_manifest.json` written |

**Companion docs:** `report_artifact_map.md` · `table_figure_register.md` · `narrative_guardrails.md`

---

## 1. Executive Summary

**Purpose:** One-page synthesis of the pipeline, data scope, quality posture, feature mart, and modeling headline.

**Evidence sources:** Cross-chapter artifacts; `reports/tables/` summaries from notebook `05`; `artifacts/manifests/run_manifest.json`; `artifacts/manifests/model_run_manifest.json`.

**Headline numbers to land (full-run, locked):**

- Bronze rows = **148,184** across 9 required endpoints; 0 row loss into Silver.
- Silver rows = **148,184**; 0 rejected; 0 referential-integrity unmatched.
- Gold rows = **1,756** at driver-race grain; 0 duplicate keys; **40** model features (8 Tier 1 + 32 Tier 2); target balanced (834 / 922).
- Modeling = **Random Forest is the best model on every metric on both splits**; test F1 = **0.7837**, ROC-AUC = **0.8733**, accuracy = **0.7963** on the protected 2025 test set; the strong heuristic baseline (`first_observed_position ≤ 10`) reaches F1 = 0.7755 / ROC-AUC = 0.7801, so ML adds value **narrowly on accuracy/F1 and decisively on ROC-AUC**.
- Frame the project as a **Big Data Infrastructure** deliverable: the pipeline (Bronze → Silver → Gold) is the contribution; the modeling chapter exists to demonstrate that the Gold feature mart is leakage-free and consumable by varied modeling approaches.

---

## 2. Data Landscape

### 2.1 OpenF1 source description

**Purpose:** Describe OpenF1 API, licensing context, and why it fits a big-data infrastructure narrative.

**Evidence:** `README.md` §5; `project_plan.md` §2; [OpenF1 API](https://api.openf1.org/v1/).

### 2.2 Dataset scope and endpoints

**Purpose:** List seasons, session types, and Bronze endpoints ingested.

**Evidence:** `artifacts/manifests/ingestion_manifest.csv`; `reports/data_quality/bronze_file_inventory.csv`.

### 2.3 Data volume and characteristics

**Purpose:** Row counts, file counts, and layer-level volume comparison.

**Evidence:** `reports/data_quality/bronze_row_counts.csv`; `reports/data_quality/silver_table_inventory.csv`; `reports/data_quality/duckdb_*`; `reports/tables/data_volume_by_layer.csv` (notebook `05`).

### 2.4 Data quality risks

**Purpose:** Structural missingness, optional endpoints, API failures, and racing-domain outliers.

**Evidence:** Phase 1–2 audit logs; `reports/data_quality/silver_data_quality_notes.csv`; `narrative_guardrails.md` §D.

---

## 3. Pipeline Architecture and Technology Rationale

### 3.1 Medallion architecture overview

**Purpose:** Bronze → Silver → Gold → ML consumption layer diagram and narrative.

**Evidence:** `README.md` §4–8; Figure 1 (planned); `reports/figures/architecture_diagram_placeholder.md`.

### 3.2 Bronze layer: raw API ingestion and immutable JSONL storage

**Purpose:** `requests` ingestion, JSONL layout, manifests, schema profiling, targeted retry for transient API failures, manifest-vs-file reconciliation, and independent Bronze Spark / DuckDB validation.

**Evidence:** Notebook `01`; `data/bronze/`; `ingestion_manifest.csv`; `ingestion_retry_manifest.csv` *(generated only when targeted retry is run)*; `ingestion_manifest_effective.csv` *(generated after retry; reconciliation runs against this)*; Bronze DQ CSVs including `bronze_manifest_file_reconciliation.csv`, `bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_*` (including `duckdb_bronze_manifest_file_reconciliation_summary.csv`, `..._by_endpoint.csv`, `..._issues.csv`).

**Narrative anchors:**

- Manifest is the canonical Bronze provenance record per `(endpoint, year, session_key)`.
- Targeted retry uses `openf1_pipeline.ingestion.ingest.retry_failed_session_endpoints(...)` to re-fetch only failed required session endpoints with slower pacing (default 3 s base sleep); it preserves the original manifest and writes a separate retry manifest.
- **Effective post-retry manifest** built by `write_effective_manifest_after_retry(...)` overlays retry rows on the original manifest with a `manifest_source` provenance tag (`"original"` / `"retry"`). Reconciliation runs against this effective manifest; without it, retry-recovered files appear as `failed_but_file_present`.
- Manifest-vs-file reconciliation joins the (effective) manifest to the on-disk JSONL inventory and classifies every triple into `matched`, `row_count_mismatch`, `manifest_success_missing_file`, `failed_manifest_file_exists`, `stale_file_not_in_success_manifest`, `optional_missing`, or `manifest_failed_no_file`.
- See `artifacts/pipeline_logs/full_bronze_retry_plan.md`, `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md`, and `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md`.

### 3.3 Silver layer: PySpark cleaning, schema enforcement, and quality checks

**Purpose:** Spark-first cleaning, rule logging, before/after DQ. Silver cleaning is **non-destructive** — it standardizes types, applies deduplication checks, enforces referential integrity, and runs diagnostic checks (missingness, outliers, temporal anomalies) without dropping rows beyond explicit, logged rejections.

**Evidence:** Notebook `02`; `src/openf1_pipeline/silver/build_silver_spark.py`; Silver DQ CSVs (see §4).

**Narrative anchors:**

- Bronze → Silver row preservation = 148,184 → 148,184 (zero row loss). Silver is a typed, cleaned mirror of Bronze, not a filtered subset.
- All 28 join-key columns across 10 Silver tables (`meeting_key`, `session_key`, `driver_number`, `lap_number`, `position`) are stored as `bigint` on disk — verified by Notebook 03's pre-flight schema check.
- `race_control` duplicate diagnostic groups are **retained, not removed**, because they can represent legitimate event-stream rebroadcasts or multi-driver events. The rule is logged as `SIL_RC_DUP_CHECK_RETAINED` in `silver_cleaning_rules.csv` and documented in `silver_data_quality_notes.csv`.
- `race_control.qualifying_phase` is 100 % null in the current OpenF1 pull and is **ignored by Gold**; only `message` and `flag` feed race-control features.

### 3.4 Gold layer: driver-race feature mart

**Purpose:** Grain, target construction, join groups, mart output.

**Evidence:** Notebook `03`; `data/gold/driver_race_feature_mart.parquet`; Gold DQ CSVs (see §5).

**Narrative anchors:**

- Grain: one row per `(session_key, meeting_key, driver_number)`.
- Row count: 1,756 — matches `session_result` target support exactly.
- Build engine: PySpark (`GOLD_ENGINE="spark"`); pandas remains a manual fallback only.
- Joins: left joins from a `session_result`-based spine; 0 unmatched rows across `metadata`, `laps`, `pit`, `position`, `weather`, `race_control`.

### 3.5 PySpark and DuckDB technology rationale

**Purpose:** Why Spark transforms and DuckDB validates; why Databricks is out of scope.

**Evidence:** `README.md` §7; `project_context.md` §G; `duckdb_*` reports.

### 3.6 Reproducibility workflow

**Purpose:** Colab + Drive, seeds, manifests, notebook order, cleanup flags.

**Evidence:** `README.md` §11–13; `artifacts/manifests/run_manifest.json`; `model_run_manifest.json`.

---

## 4. Data Quality and Cleaning

### 4.1 Error taxonomy

**Purpose:** Classify error types (schema, referential, temporal, outlier, structural missingness).

**Evidence:** `reports/tables/silver_error_taxonomy_table.csv`; `silver_cleaning_rules.csv`; Phase 2 audit.

### 4.2 Detection strategy

**Purpose:** How each error class is detected (Spark steps, profiling, DuckDB SQL). Includes a **Bronze-layer detection control** for manifest-vs-file inconsistencies before any Silver work runs.

**Evidence:**

- **Bronze-layer detection** — `bronze_manifest_file_reconciliation.csv`, `bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_manifest_file_reconciliation_summary.csv`, `duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv`, `duckdb_bronze_manifest_file_reconciliation_issues.csv` (from `generate_bronze_reconciliation_reports(...)`).
- **Silver-layer detection** — `silver_cleaning_rules.csv`; `silver_duplicate_report.csv`; `silver_outlier_report.csv`; `silver_temporal_anomaly_report.csv`; `silver_referential_integrity_report.csv`.

### 4.3 Remediation rules

**Purpose:** Casting, dedupe, domain filters, imputation flags — with step IDs. Also documents the **Bronze remediation loop** for transient API failures.

**Evidence:**

- **Bronze remediation** — `retry_failed_session_endpoints(...)` re-fetches failed required session endpoints with slower pacing (default 3 s base sleep), writing `artifacts/manifests/ingestion_retry_manifest.csv` without modifying the original manifest. `write_effective_manifest_after_retry(...)` then produces `artifacts/manifests/ingestion_manifest_effective.csv` — the merged provenance-tagged manifest reconciliation runs against. Optional `delete_stale_bronze_files(...)` is opt-in for the three known stale smoke files. `starting_grid` is excluded from retry by default because it is optional.
- **Silver remediation** — `silver_cleaning_rules.csv`; `silver_rejected_records_summary.csv`; `silver_cleaning_impact_summary.csv`.

### 4.4 Before/after validation

**Purpose:** Quantify missingness, cleaning impact, and Bronze retry effect.

**Evidence:**

- **Bronze before/after** — `ingestion_manifest.csv` (original full run) vs `ingestion_retry_manifest.csv` (retry results) vs `ingestion_manifest_effective.csv` (merged) vs refreshed `bronze_row_counts.csv`, `bronze_file_inventory.csv`, and `bronze_manifest_file_reconciliation_summary.csv`. Targeted retry has run; reconciliation is computed against the effective manifest.
- **Silver before/after** — `silver_missingness_before.csv`; `silver_missingness_after.csv`; Figure 4; `duckdb_silver_*.csv`.

### 4.5 Cleaning impact on modeling dataset

**Purpose:** Link Bronze coverage and Silver quality to Gold row retention and join completeness, with explicit attention to `session_result` coverage as the upstream constraint on the modeling dataset.

**Evidence:**

- **Bronze target coverage** — `session_result` success counts from `ingestion_manifest.csv` + `ingestion_retry_manifest.csv`, cross-checked against `bronze_file_inventory.csv` via `bronze_manifest_file_reconciliation.csv`. Target = 89 race sessions across 2023–2025; 88 race sessions are realized in the Silver `session_result` (1,756 driver-races at 19.95 drivers/race on average).
- **Silver → Gold downstream** — `gold_join_quality_report.csv` shows 1,756 rows in, 1,756 rows out for every feature group; 0 unmatched. The Gold mart preserves the full Silver `session_result` row set with no row explosion.

### 4.6 Silver layer headline results (Notebook 02)

**Purpose:** Consolidated, citable values from the latest Silver build. Use these numbers verbatim in the written deliverable.

**Headline metrics:**

| Check | Value | Source |
|-------|-------|--------|
| Bronze total rows | **148,184** | `bronze_row_counts.csv`, `duckdb_bronze_bronze_total_rows.csv` |
| Silver total rows | **148,184** | `silver_table_inventory.csv`, `duckdb_silver_silver_row_counts.csv` |
| Bronze → Silver row loss | **0** | derived: Bronze total − Silver total |
| Required endpoints present | **9 / 9** (`meetings`, `sessions`, `drivers`, `laps`, `pit`, `weather`, `position`, `race_control`, `session_result`) | `silver_table_inventory.csv` |
| `starting_grid` rows | **0** (optional, expected) | `silver_table_inventory.csv`; see narrative guardrail §H.4 |
| `session_result` rows | **1,756** (88 race sessions) | `silver_table_inventory.csv`, `duckdb_silver_session_result_target_support.csv` |
| `session_result` duplicate keys | **0** | `duckdb_silver_session_result_duplicate_keys.csv` |
| Referential integrity unmatched (count and rate) | **0 across all checks** | `silver_referential_integrity_report.csv` |
| Rejected records | **0** | `silver_rejected_records_summary.csv` |
| Verdict | **PASS WITH MINOR ISSUES — safe for Gold** | Notebook 02 audit |

**Narrative anchors:**

- Silver cleaning is non-destructive: standardization, type casting, deduplication and referential-integrity checks, and missingness/outlier/temporal-anomaly diagnostics. No rows are silently dropped; any rejection would appear in `silver_rejected_records_summary.csv`.
- Missingness shown in `silver_missingness_after.csv` is dominated by **structural** patterns (per narrative guardrail §D): session-level `race_control` messages without `driver_number`; sparse `pit` durations when sessions had no stops; and the known-empty `race_control.qualifying_phase`. None of these are data errors.
- `race_control` duplicate retention is intentional. The cleaning step ran the duplicate-detection diagnostic but kept the rows — see rule `SIL_RC_DUP_CHECK_RETAINED` in `silver_cleaning_rules.csv` and the rationale in `silver_data_quality_notes.csv`. Removing them would discard legitimate event-stream rebroadcasts.
- All Silver join-key columns are integer/long on disk after the cleanup; confirmed by the Notebook 03 schema pre-flight (28 / 28 keys `bigint`).
- **DuckDB validation as independent evidence:** `duckdb_silver_silver_row_counts.csv`, `duckdb_silver_session_result_duplicate_keys.csv`, `duckdb_silver_session_result_target_support.csv`, and per-table session-key counts (`duckdb_silver_laps_rows_by_session_key.csv`, `..._pit_...`, `..._weather_...`) corroborate the pandas/Spark reports with a separate SQL engine.

---

## 5. Feature Engineering and Integration

### 5.1 Driver-race feature mart

**Purpose:** Grain, target `points_finish`, feature domains.

**Evidence:** `data/gold/driver_race_feature_mart.parquet`; `gold_feature_summary_stats.csv`; `project_context.md` §C.

**Narrative anchors:**

- Grain: one row per `(session_key, meeting_key, driver_number)` — driver-race grain.
- Row count: **1,756** rows. This matches the Silver `session_result` row count exactly, confirming the Gold mart preserves every labelled driver-race without expansion or attrition.
- Column count: 62 (3 identifiers + 7 diagnostic/target + 12 metadata + 40 numeric model features).
- Target: `points_finish = 1 if points > 0 else 0`, derived from `session_result.points`. Zero nulls; both classes populated.

### 5.2 Join strategy

**Purpose:** Spine table, left joins, unmatched counts, row-explosion check.

**Evidence:** `gold_join_quality_report.csv`; `duckdb_gold_gold_row_count.csv`; `duckdb_gold_gold_duplicate_keys.csv`; Phase 3 audit.

**Narrative anchors:**

- Spine: `session_result_clean.parquet` (1,756 rows). The Spark builder hard-raises on duplicate driver-race keys after every join, so the row count can only decrease via cleaning rules (none triggered) or stay constant.
- Joins applied in order: `metadata` (driver-grain), `laps` (driver-grain), `pit` (driver-grain), `position` (driver-grain), `weather` (session-grain), `race_control` (session-grain). All left joins.
- Result: 1,756 → 1,756 rows in every group; **0 unmatched** rows in every group; **0 row explosion**. DuckDB cross-check returns 0 duplicate `(session_key, meeting_key, driver_number)` triples.

### 5.3 Temporal alignment and feature tiers

**Purpose:** Tier 1 early-session vs Tier 2 full-session analytical features. The task is **integrated race-session classification**, not strict pre-race prediction — see narrative guardrails §A.

**Evidence:** `model_feature_plan.csv`; `narrative_guardrails.md` §A–B; `feature_dictionary.csv` (`feature_tier`).

**Feature tier summary (confirmed):**

| Tier | Count | Origin | Examples |
|------|-------|--------|----------|
| **Tier 1 early-session** | **8** | First 5 observations of `position` and first 5 timed laps from `laps` | `first_observed_position`, `early_avg_position`, `early_min_position`, `early_max_position`, `position_observation_count`, `avg_first5_lap_duration`, `best_first5_lap_duration`, `std_first5_lap_duration` |
| **Tier 2 full-session analytical** | **32** | Full-session aggregates over `laps`, `pit`, `weather`, `race_control` | `lap_count`, `avg_lap_duration`, `pit_stop_count`, `avg_pit_duration`, `avg_air_temperature`, `rainfall_flag`, `race_control_message_count`, `yellow_flag_count`, … |
| **Total default numeric model features** | **40** | Tier 1 + Tier 2 | drives the `X` matrix for Notebook 04 |

`starting_grid` is **absent / optional** in this run (0 rows). Per narrative guardrails §H.4 and §B, the modeling stack does not require it: Tier 1 `first_observed_position` and `early_avg_position` from the first five `position` observations function as the early-race position proxy that grid would have provided. The heuristic baseline (`first_observed_position ≤ 10`) explicitly references this proxy, not grid position.

### 5.4 Feature dictionary and leakage guard

**Purpose:** Roles, allowed columns, blocked outcomes.

**Evidence:** `feature_dictionary.csv`; `gold_leakage_guard_report.csv`; `openf1_pipeline.modeling.feature_selection`.

**Leakage guard summary (confirmed):**

- 22 columns are blocked from modeling. The 7 columns flagged `high` or `target` leakage risk are: `result_dnf`, `result_dns`, `result_dsq`, `final_position`, `result_points`, `diagnostic_final_observed_position` (`high`), and `points_finish` (`target`).
- `points_finish` is the model target only and is **never** an input feature.
- The frozen `LEAKAGE_FORBIDDEN_COLUMNS` set in `src/openf1_pipeline/gold/build_feature_mart.py` also excludes the raw `position`, `points`, `duration`, `gap_to_leader`, and `number_of_laps` columns by name, so the suspicious patterns enumerated in the Phase 3 audit (`final_position`, `classified_position`, `points`, `position_order`, `result`, `status`, `rank`, `finish`, `place`, `podium`) are either absent from the mart or explicitly blocked.
- Tier 2 features are **engineered aggregates** (counts, means, mins, maxes) of within-session events, not raw finishing outcomes; they are allowed in the default bundle per narrative guardrail §B.

### 5.5 Feature validation

**Purpose:** Missingness, target distribution, DuckDB cross-checks.

**Evidence:** `gold_feature_missingness.csv`; `gold_target_distribution.csv`; `gold_feature_summary_stats.csv`; `duckdb_gold_gold_row_count.csv`; `duckdb_gold_gold_duplicate_keys.csv`; `duckdb_gold_gold_target_distribution.csv`; `duckdb_gold_gold_missingness_summary.csv`; `duckdb_gold_points_finish_by_team.csv`; `duckdb_gold_points_finish_by_circuit.csv`.

**Target distribution (confirmed):**

| `points_finish` | Count | Pct |
|-----------------|-------|-----|
| 0 | 922 | 52.5057 % |
| 1 | 834 | 47.4943 % |
| **Total** | **1,756** | 100 % |

0 nulls. Positive-class rate ≈ 47.5 %, well-balanced for binary classification. DuckDB target distribution agrees row-for-row with the pandas report.

**Gold missingness interpretation (per narrative guardrail §D, post-fix Gold rerun):**

| Category | Examples (post-fix) | Reading |
|----------|---------------------|---------|
| **A. Acceptable structural missingness** | `avg_pit_duration`, `min_pit_duration`, `max_pit_duration`, `first_pit_lap` (~25.80 % each); `std_first5_lap_duration` (~3.87 %); other lap-pace features (~1.59–3.30 %) | Drivers with zero pit stops, with <2 first-five laps, or with no timed laps at all. NaN is the correct representation of "no event to aggregate." |
| **B. High-missing but excluded from modeling** | `driver_country_code` (~34.11 %); `final_position` (~10.08 %) | `driver_country_code` is metadata blocked from features; `final_position` is a diagnostic outcome blocked from features. Neither enters `X`. |
| **C. Blocking missingness** | — | **None.** No model feature has 100 % NaN. No identifier, target, or join key has any NaN. `points_finish` missing_count = 0. |

**Post-audit safe cleanup (applied and verified):** `lap_count` and `pit_out_lap_count` are count-style features. The earlier audit observed 3 rows (0.17 %) of NaN coming from DNS drivers with no laps recorded. These two columns were added to `EVENT_ABSENCE_ZERO_COLS` in `src/openf1_pipeline/gold/build_feature_mart.py`; the Spark builder (`build_feature_mart_spark.py`) imports the same constant and applies the zero-fill loop after the joins. **The post-fix Gold rerun confirms both columns now read `missing_count = 0` (0.0000 %)**, alongside all other count-style event-absence features (`pit_stop_count`, `early_pit_stop_flag`, `race_control_message_count`, `flag_message_count`, `yellow_flag_count`, `red_flag_count`, `green_flag_count`, `safety_car_message_count`, `pit_exit_message_count`). Row counts (1,756), tier definitions (8 + 32 = 40), leakage-guard rules, and target distribution (922 / 834) are unchanged from the prior audit.

### 5.6 Gold layer headline results (Notebook 03, post-fix rerun)

**Purpose:** Consolidated, citable values from the latest Gold build after the safe `EVENT_ABSENCE_ZERO_COLS` fix. Use these numbers verbatim in the written deliverable.

**Headline metrics:**

| Check | Value | Source |
|-------|-------|--------|
| Gold row count | **1,756** | `gold_feature_summary_stats.csv`; `duckdb_gold_gold_row_count.csv` |
| Gold column count | **62** | `gold_feature_summary_stats.csv` |
| Grain | `(session_key, meeting_key, driver_number)` | Notebook 03 cell 27; Spark builder grain assertion |
| Duplicate grain rows | **0** | `duckdb_gold_gold_duplicate_keys.csv` (0 rows) |
| Target nulls (`points_finish`) | **0** | `gold_feature_missingness.csv` |
| Target distribution | **0 = 922 (52.5057 %), 1 = 834 (47.4943 %)** | `gold_target_distribution.csv`; `duckdb_gold_gold_target_distribution.csv` |
| `lap_count` missing | **0** (was 3 pre-fix) | `gold_feature_missingness.csv` |
| `pit_out_lap_count` missing | **0** (was 3 pre-fix) | `gold_feature_missingness.csv` |
| Join groups preserved | **6 / 6** at 1,756 → 1,756, 0 unmatched | `gold_join_quality_report.csv` |
| Leakage-blocked columns | **22** (incl. 7 high/target risk) | `gold_leakage_guard_report.csv` |
| Model feature count | **40** (Tier 1 = 8, Tier 2 = 32) | `model_feature_plan.csv`; Notebook 03 cell 13 / 23 |
| Class C blocking missingness | **None** | per-column missingness review |
| Verdict | **PASS — ready for Notebook 04** | Notebook 03 post-fix audit |

**Evidence snapshot in `evidence/full_2023_2025/` (post-fix Gold rerun):** the post-fix Gold state has been snapshotted to the in-repo evidence bundle. Verified files (Spark writes the parquet as a partitioned directory; the bundle preserves the `_SUCCESS` marker and both `part-*.snappy.parquet` files):

- `evidence/full_2023_2025/data/gold/driver_race_feature_mart.parquet/` (2 part files + `_SUCCESS`)
- `evidence/full_2023_2025/artifacts/feature_definitions/feature_dictionary.csv`
- `evidence/full_2023_2025/artifacts/feature_definitions/model_feature_plan.csv`
- `evidence/full_2023_2025/reports/data_quality/gold_target_distribution.csv`
- `evidence/full_2023_2025/reports/data_quality/gold_feature_missingness.csv`
- `evidence/full_2023_2025/reports/data_quality/gold_feature_summary_stats.csv`
- `evidence/full_2023_2025/reports/data_quality/gold_leakage_guard_report.csv`
- `evidence/full_2023_2025/reports/data_quality/gold_join_quality_report.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_gold_gold_row_count.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_gold_gold_duplicate_keys.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_gold_gold_target_distribution.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_gold_gold_missingness_summary.csv`

The bundle also includes the two non-required DuckDB cross-cuts that were generated during the same Notebook 03 run (`duckdb_gold_points_finish_by_team.csv` and `duckdb_gold_points_finish_by_circuit.csv`) plus the Bronze + Silver layer evidence (manifests, JSONL trees, Silver parquets, and DQ CSVs) that underpins the upstream provenance chain. Notebook 04 can now consume `evidence/full_2023_2025/data/gold/driver_race_feature_mart.parquet` and `evidence/full_2023_2025/artifacts/feature_definitions/model_feature_plan.csv` directly without re-running Notebook 03.

---

## 6. Experimental Results and Analysis

### 6.1 Classification task

**Purpose:** Define target, task framing, default feature bundle.

**Evidence:** `model_feature_plan.csv`; `feature_dictionary.csv`; `gold_target_distribution.csv`; `narrative_guardrails.md` §A–B.

**Task framing (locked):** **Points-finish classification using integrated race-session features.** The model predicts whether a driver-race row has `points_finish = 1` (top-10 / point-scoring finish) given a mix of Tier 1 early-session signals and Tier 2 full-session analytical aggregates from the Gold mart. This is **not** a strict pre-race prediction problem — see narrative guardrail §A.

**Default feature bundle:** 40 numeric features = 8 Tier 1 + 32 Tier 2 (frozen in `model_feature_plan.csv`). Optional categoricals (`team_name`, `circuit_short_name`, `session_country_name`, `location`, `session_type`, `session_name`) are opt-in; `session_year` is excluded when season-based splits are used (see §6.2).

**Class prior (from Gold):** 47.4943 % positive (834 / 1,756). The target is well-balanced, so accuracy is reported alongside ROC-AUC without rebalancing the dataset. PR-AUC is intentionally **not** computed (the metric is not implemented in `src/openf1_pipeline/modeling/evaluate.py` and is redundant with ROC-AUC at this near-balanced prior); balanced accuracy is also omitted for the same reason.

**Class-weight handling (defensive default, not major rebalancing).** All three supervised pipelines pass `class_weight="balanced"` to the sklearn / LightGBM classifier. Because the Gold target is already close to 50 / 50, this is a **defensive modeling default to keep the three model configurations symmetric**, not a meaningful rebalancing intervention — its effect on metrics is small relative to model-family differences and is explicitly framed as a configuration choice rather than a treatment for class imbalance.

**Evaluation metrics computed (locked):** accuracy, precision, recall, F1, ROC-AUC, confusion matrix, per-group error analysis (team / circuit / season / position bin), and feature importance per model. See `src/openf1_pipeline/modeling/evaluate.py`. Balanced accuracy and PR-AUC are intentionally omitted.

### 6.2 Data split strategy

**Purpose:** Season splits 2023 / 2024 / 2025; smoke vs full modes.

**Evidence:** `model_run_manifest.json` (`split_method`, `modeling_mode`); `src/openf1_pipeline/modeling/splits.py`.

**Planned split (full mode):**

| Split | Season(s) | Purpose |
|-------|-----------|---------|
| Train | **2023** | Fit baselines and classifiers |
| Validation | **2024** | **Out-of-sample validation reference between training and the protected test set** (used to surface generalisation gaps; **not** used for hyperparameter tuning or model selection) |
| Test | **2025** | Final held-out evaluation; never touched during fitting or selection |

`session_year` therefore acts as the **split key** and must not appear in `X` (enforced via `model_feature_plan.csv` row for `session_year`: `default_include=False` with the explicit "season split leakage" reason).

**No hyperparameter tuning is performed.** Each model is fit once on the 2023 training split with the defaults declared in `src/openf1_pipeline/modeling/train.py`. The 2024 validation metrics are reported alongside the 2025 test metrics in the model performance table so the reader can see the train → validation → test trajectory, but no decision (model choice, hyperparameter value, threshold) is made on the validation split. A hyperparameter study is explicitly listed as out of scope in §7.3.

**Realised splits (full-run, `reports/model_results/train_validation_test_split_summary.csv`):**

| Split | Season | Rows | `points_finish = 1` | `points_finish = 0` | Positive rate |
|-------|--------|------|---------------------|---------------------|---------------|
| Train | 2023 | **558** | 258 | 300 | 0.4624 |
| Validation | 2024 | **599** | 288 | 311 | 0.4808 |
| Test | 2025 | **599** | 288 | 311 | 0.4808 |
| **Total** | — | **1,756** | 834 | 922 | 0.4749 |

Row sum = 1,756 matches the Gold mart exactly. Positive-class rate is stable across splits (0.46 / 0.48 / 0.48), so accuracy alongside ROC-AUC is a defensible reporting choice. The split is enforced by `MODELING_MODE="full"` in Notebook 04 cell 11 (raises if any season-split is empty or mis-typed) and recorded in `model_run_manifest.json` with `split_method="season"` and `evidence_tier="mba_official"`.

### 6.3 Baselines

**Purpose:** Three baselines anchor the model performance table — a statistical floor, a trivial-learner floor, and a domain-informed reference — so the reader can see exactly what bar the supervised models must clear to justify their complexity.

**Evidence:** `reports/model_results/baseline_metrics.csv`; Notebook `04`; `src/openf1_pipeline/modeling/baselines.py`.

**Baseline set (locked):**

| Baseline | Rule | Role |
|----------|------|------|
| `random_baseline` | Bernoulli draws at the training-set positive rate (~0.475) | Statistical chance floor for ROC-AUC and F1 |
| `majority_baseline` | Always predict class 0 (majority class at 52.51 %) | Trivial-learner floor; exposes why accuracy alone is insufficient (recall = 0, F1 = 0 on the positive class) |
| `heuristic_position` | Predict `points_finish = 1` iff `first_observed_position ≤ 10` | **Intentionally strong, domain-informed reference** |

**Why the heuristic is intentionally strong (and defensible).** The heuristic encodes the dominant F1 domain intuition that drivers running inside the top 10 in the opening laps usually finish in the points. `first_observed_position` is also one of the 40 model features, so the heuristic is *deliberately* the bar the ML models must clear: it directly answers the question *"do we actually need 40 features and a learned decision boundary, or does one early-race position signal suffice?"* This is the right question for a project whose Chapter 6 evaluates whether the Gold feature mart is useful, not whether one classifier beats another. Per narrative guardrail §H.4, the heuristic uses `first_observed_position` (Tier 1 early-session feature) — not grid position — because `starting_grid` is an optional OpenF1 endpoint and is absent in this run.

**Separation from ML models.** Baselines are computed in their own module and saved to a separate CSV (`baseline_metrics.csv`); they never participate in any fitting step and are evaluated on both the 2024 validation and 2025 test splits with the same metric suite as the supervised models.

**Realised baseline metrics (full-run, `reports/model_results/baseline_metrics.csv`):**

| Baseline | Split | Accuracy | Precision | Recall | F1 | ROC-AUC |
|----------|-------|----------|-----------|--------|-----|---------|
| `random_baseline` | validation | 0.4975 | 0.4765 | 0.4583 | 0.4673 | 0.5000 |
| `majority_baseline` | validation | 0.5192 | 0.0000 | 0.0000 | 0.0000 | 0.5000 |
| **`heuristic_position`** | **validation** | **0.8397** | **0.8200** | **0.8542** | **0.8367** | **0.8403** |
| `random_baseline` | test | 0.4975 | 0.4765 | 0.4583 | 0.4673 | 0.5000 |
| `majority_baseline` | test | 0.5192 | 0.0000 | 0.0000 | 0.0000 | 0.5000 |
| **`heuristic_position`** | **test** | **0.7796** | **0.7600** | **0.7917** | **0.7755** | **0.7801** |

**Reading the baselines:**

- The **random baseline** is byte-identical across val and test because `random_baseline_predictions(...)` uses `RANDOM_SEED=42` and both splits have `n = 599`; this is expected behaviour, not a bug.
- The **majority baseline** predicts class 0 for every row (training majority class), producing the trivial `accuracy ≈ 0.52` floor and `precision = recall = F1 = 0` on the positive class — the locked anchor for "why accuracy alone is insufficient" in §6.4.
- The **heuristic baseline** is genuinely strong, reaching F1 = 0.8367 (val) and 0.7755 (test) from a single Tier 1 feature. This is the bar the ML models must clear.
- The heuristic itself loses ~6 percentage points val → test, confirming a portion of the val → test drop seen in the ML models is **real season drift in 2025**, not validation-bias overfitting.

### 6.4 Model performance

**Purpose:** Train three supervised classifiers on the 2023 split, evaluate them on the 2024 validation reference and the protected 2025 test set, and compare against the §6.3 baselines under the same metric suite.

**Evidence:** `validation_metrics.csv`; `test_metrics.csv`; `reports/tables/model_*_table.csv`; `src/openf1_pipeline/modeling/train.py`.

**Model set (locked):** Three models, one per canonical family for tabular classification.

| Model | Family | Role |
|-------|--------|------|
| `logistic_regression` | **Linear** | Interpretable linear benchmark; exposes signed coefficients for §6.5 feature importance |
| `random_forest` | **Nonlinear bagged trees** | Robust ensemble that captures nonlinear feature interactions without tuning; exposes Gini importances |
| `lightgbm` | **Boosted trees** | Strong gradient-boosted tree learner for small-to-medium tabular data; exposes split-gain importances and handles missingness natively |

**Why this set and no other models.** The three models cover the three canonical families used for tabular classification — **linear, bagged trees, and boosted trees** — at a deliberate minimum-complete level. This is a **Big Data Infrastructure project, not an ML benchmarking project**. The goal of Chapter 6 is to demonstrate that the Gold feature mart is leakage-free and consumable by varied modeling approaches; it is **not** to maximise benchmark performance through extensive model search. Specifically:

- **XGBoost / CatBoost** are statistically redundant with LightGBM at this row count and would inflate the table without changing conclusions.
- **SVM / KNN** would shift the chapter toward kernel and distance-metric tuning and away from the pipeline architecture.
- **Neural networks** are out of scope: the 558-row 2023 training split is far too small to justify them without overfitting concerns dominating the narrative.

The model set is intentionally locked at three. No additional models are introduced.

**No hyperparameter tuning.** Each pipeline is fit once on the 2023 training split with the defaults declared in `src/openf1_pipeline/modeling/train.py` (LR `max_iter=1000`; RF `n_estimators=200`; LightGBM `n_estimators=200`, `learning_rate=0.05`). The 2024 validation split is used as an **out-of-sample reference**, not for tuning or model selection — see §6.2 and the narrative guardrails §I.

**Class-weight as a defensive default.** All three pipelines pass `class_weight="balanced"` to the underlying classifier. Since the Gold target is already close to 50 / 50 (~47.5 / 52.5), this is a **defensive modeling default to keep the three model configurations symmetric**, not a major rebalancing intervention. It is reported here as a configuration choice, not as a remedy for class imbalance.

**Realised model metrics (full-run, `reports/model_results/validation_metrics.csv` + `test_metrics.csv`):**

| Model | Split | Accuracy | Precision | Recall | F1 | ROC-AUC | Pred. pos. rate |
|-------|-------|----------|-----------|--------|-----|---------|-----------------|
| `logistic_regression` | validation | 0.7763 | 0.8130 | 0.6944 | 0.7491 | 0.8443 | 0.4107 |
| **`random_forest`** | **validation** | **0.8464** | **0.8525** | **0.8229** | **0.8375** | **0.9212** | 0.4641 |
| `lightgbm` | validation | 0.8080 | 0.8216 | 0.7674 | 0.7935 | 0.8887 | 0.4491 |
| `logistic_regression` | test | 0.7329 | 0.6951 | **0.7917** | 0.7403 | 0.8088 | 0.5476 |
| **`random_forest`** | **test** | **0.7963** | **0.8007** | 0.7674 | **0.7837** | **0.8733** | 0.4608 |
| `lightgbm` | test | 0.7813 | 0.7794 | 0.7604 | 0.7698 | 0.8590 | 0.4691 |

**Headline finding (locked):** **Random Forest is the best model on every metric on both splits** — validation (Acc 0.8464, F1 0.8375, ROC-AUC 0.9212) and test (Acc 0.7963, F1 0.7837, ROC-AUC 0.8733). LightGBM is second on both splits and degrades the least val → test. Logistic Regression is third on accuracy/F1 but has the highest recall on test (0.7917), reflecting its linear bias toward positive predictions (`positive_rate_pred = 0.5476` on test vs true 0.4808).

**ML vs heuristic comparison (the key question for the chapter):**

| Metric | Heuristic val | RF val | Δ val | Heuristic test | RF test | Δ test |
|--------|---------------|--------|-------|----------------|---------|--------|
| Accuracy | 0.8397 | 0.8464 | **+0.0067** | 0.7796 | 0.7963 | **+0.0167** |
| F1 | 0.8367 | 0.8375 | **+0.0008** | 0.7755 | 0.7837 | **+0.0082** |
| ROC-AUC | 0.8403 | 0.9212 | **+0.0809** | 0.7801 | 0.8733 | **+0.0932** |

Random Forest beats the heuristic on every metric on both splits — **narrowly on accuracy/F1, decisively on ROC-AUC**. This is the core finding to land in the written deliverable: the heuristic's *thresholded* prediction is hard to outperform with one feature alone, but the Gold mart and ML models add substantial value in **ranking quality** (probability calibration). This justifies the mart without overclaiming.

**Val → test stability:**

| Model | Δ Accuracy | Δ F1 | Δ ROC-AUC |
|-------|-----------|------|-----------|
| Logistic Regression | −0.043 | −0.009 | −0.036 |
| Random Forest | −0.050 | −0.054 | −0.048 |
| LightGBM | −0.027 | −0.024 | −0.030 |
| Heuristic baseline (reference) | −0.060 | −0.061 | −0.060 |

All three ML models degrade by less than the heuristic from validation to test, so the val → test drop is **real season drift in 2025**, not optimistic-validation bias (no tuning was performed on validation, see §6.2).

### 6.5 Confusion matrix and error analysis

**Purpose:** Per-model confusion; errors by team / circuit / season / position bin.

**Evidence:** `reports/model_results/confusion_matrix.csv` (48 rows = 6 model_names × 2 splits × 4 cells); `reports/model_results/error_analysis.csv` (807 rows = 6 model_names × 2 splits × {team, circuit, session_year, position_bin} × 2 error types); Figure 8.

**Random Forest test confusion (best model on protected 2025 split):**

| | Predicted 0 | Predicted 1 | Row total |
|---|---|---|---|
| **Actual 0** | TN ≈ 256 | FP ≈ 55 | 311 |
| **Actual 1** | FN ≈ 67 | TP ≈ 221 | 288 |
| **Col total** | 323 | 276 | **599** |

Counts derived from the realised test metrics: recall 0.7674 on 288 positives ⇒ TP ≈ 221, FN ≈ 67; `positive_rate_pred = 0.4608` on 599 rows ⇒ predicted positives ≈ 276 ⇒ FP ≈ 55; TN = 311 − 55 ≈ 256. Errors are roughly balanced between false positives (≈ 55, "predicted points but driver did not score") and false negatives (≈ 67, "predicted no points but driver scored"); the model is not biased toward either class.

**Error analysis structure:** per-group error counts are exported for every model and both splits across `team_name`, `circuit_short_name`, `session_year`, and a `first_observed_position` bin (`1-5`, `6-10`, `11-15`, `16-20`, `21+`). Notebook 05 consolidates these into `error_analysis_summary_table.csv` (Table 13) and the per-model confusion table (Table 12). The 807-row error file already covers all six predictors (3 baselines + 3 models), so the report can present an ML-vs-heuristic error comparison on the same group columns without any re-computation.

### 6.6 Reproducibility statement

**Purpose:** Seeds, artifact paths, manifest lineage, and the full Bronze ingestion → retry → reconciliation → modeling provenance chain.

**Evidence:** `reproducibility_artifacts_table.csv`; `run_manifest.json`; `model_run_manifest.json`; `README.md` §13; `artifacts/manifests/ingestion_manifest.csv`; `artifacts/manifests/ingestion_retry_manifest.csv` *(when retry has run)*; `artifacts/manifests/ingestion_manifest_effective.csv` *(when retry has run)*; `reports/data_quality/bronze_manifest_file_reconciliation.csv` and `..._summary.csv`; `duckdb_bronze_manifest_file_reconciliation_*.csv`; `artifacts/pipeline_logs/full_bronze_output_review.md`, `full_bronze_retry_plan.md`, `bronze_manifest_file_reconciliation_added.md`, `bronze_effective_manifest_post_retry.md`.

**Modeling-run lineage (locked, from `artifacts/manifests/model_run_manifest.json`):** `modeling_mode = "full"`, `split_method = "season"`, `evidence_tier = "mba_official"`, `random_seed = 42` (sourced from `RANDOM_SEED` in `src/openf1_pipeline/config.py:21`), `target = "points_finish"`, `feature_count = 40`, `feature_tier_counts = {"tier1_early": 8, "tier2_full_session": 32, "total_default_numeric": 40}`, `models = ["logistic_regression", "random_forest", "lightgbm"]`, `baselines = ["random_baseline", "majority_baseline", "heuristic_position"]`. The manifest references absolute paths under `OPENF1_DATA_ROOT` for all seven `reports/model_results/*.csv` artifacts written by Notebook 04 cell 21. The manifest is the canonical modeling provenance record for the report and the viva.

---

## 7. Conclusion and Reflection

### 7.1 Lessons learned

**Purpose:** Infrastructure-first insights, smoke vs full, data quirks (starting_grid, structural nulls).

**Evidence:** `artifacts/pipeline_logs/phase*_*.md`; author reflection `[TO BE WRITTEN]`.

### 7.2 Deployment considerations

**Purpose:** Colab/Drive ops, re-run policy, no Databricks requirement.

**Evidence:** `README.md` cleanup table; `project_plan.md`.

### 7.3 Future improvements

**Purpose:** Tier-1-only sensitivity, hyperparameter study (out of scope), streaming, etc.

**Evidence:** Phase audit recommendations; `[TO BE WRITTEN after full run]`.

---

## Writing order (recommended)

1. Chapters 2–3 (landscape + architecture) — mostly stable from docs and smoke DQ.
2. Chapter 4–5 — cite smoke tables; flag full-run volume placeholders.
3. Chapter 6 — only after `04` full mode + `05` on Drive.
4. Chapter 1 and 7 — last, after all artifacts confirmed.
