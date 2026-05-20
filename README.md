# A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

**Repository:** `openf1-big-data-pipeline`

---

## 1. Project title

A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

---

## 2. Project purpose

This project builds an **evidence-driven, reproducible Big Data Infrastructure pipeline** for Formula 1 data from the [OpenF1 API](https://openf1.org/). The pipeline lands raw data (Bronze), cleans and audits it (Silver), integrates a driver-race feature mart (Gold), and uses simple classification models as the **final consumption layer** to validate that the Gold mart supports analysis.

The written deliverable is supported by **generated tables, manifests, and quality reports**—not narrative claims alone.

---

## 3. Course context

This project was developed in an **MBA Big Data Infrastructures** course. The work emphasizes:

1. **Data Quality & Cleaning**
2. **Pipeline Architecture & Technology Rationale**
3. **Feature Engineering & Integration**
4. **Experimental Results & Analysis**

See local project planning docs (if available in your clone) for full requirements and artifact lists.

---

## 4. Why this is a big data infrastructure project first

The core deliverable is the **Medallion pipeline** (ingestion, cleaning, integration, provenance)—not model tuning. Supervised models exist to:

- Enforce a clear **Gold grain** (one row per driver per race session)
- Expose integration and data-quality issues through measurable outputs
- Satisfy the experimental analysis pillar with baselines and season-based evaluation

Do **not** treat this as a Kaggle-style classification competition.

---

## 5. Dataset: OpenF1 Formula 1 Racing Data

| Item | Value |
|------|--------|
| Source | OpenF1 API (`https://api.openf1.org/v1/`) |
| Seasons | 2023, 2024, 2025 |
| Primary sessions | Race |
| Supporting sessions | Qualifying (grid / starting context) |
| Endpoints | `meetings`, `sessions`, `drivers`, `laps`, `pit`, `weather`, `position`, `race_control`, `session_result`, `starting_grid` |

---

## Bronze layer evidence

Bronze ingestion (`01_ingestion_bronze.ipynb`) should be executed in **Google Colab Pro Plus** for reproducible, documented pipeline evidence. A local smoke test is for development only.

**Artifacts produced:**

| Artifact | Path |
|----------|------|
| Ingestion manifest (original) | `artifacts/manifests/ingestion_manifest.csv` |
| Ingestion retry manifest (optional) | `artifacts/manifests/ingestion_retry_manifest.csv` |
| Effective post-retry manifest (optional) | `artifacts/manifests/ingestion_manifest_effective.csv` |
| File inventory | `reports/data_quality/bronze_file_inventory.csv` |
| Row counts | `reports/data_quality/bronze_row_counts.csv` |
| Schema report | `reports/data_quality/bronze_schema_report.csv` |
| Schema drift | `reports/data_quality/bronze_schema_drift.csv` |
| Manifest ↔ files reconciliation | `reports/data_quality/bronze_manifest_file_reconciliation.csv` |
| Reconciliation summary | `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv` |

`bronze_manifest_file_reconciliation.csv` joins `ingestion_manifest.csv` to the JSONL inventory on disk and classifies every `(endpoint, year, session_key)` triple into `matched`, `row_count_mismatch`, `manifest_success_missing_file`, `failed_manifest_file_exists`, `stale_file_not_in_success_manifest`, `optional_missing`, or `manifest_failed_no_file`. It is used to verify that the manifest and the physical Bronze files agree **before** Silver processing — notably to detect stale files left behind when `CLEAR_BRONZE_OUTPUTS=False` was used over a non-empty Drive root.

Raw payloads: `data/bronze/{endpoint}/…/*.jsonl` (typically on Google Drive via `OPENF1_DATA_ROOT`).

**`session_result`** is included in Bronze because it provides race classification and points used to construct the Gold target **`points_finish`** (`1` if `points` > 0, else `0`).

**`starting_grid`** is optional (may be empty on smoke). The planned **heuristic baseline** uses **`first_observed_position` ≤ 10** from Gold early-position features, not grid position.

### Targeted retry and manifest-file reconciliation

Two Bronze data-quality controls work together to keep the manifest, the on-disk JSONL inventory, and the modeling target consistent.

**Why reconciliation exists.** `bronze_manifest_file_reconciliation.csv` (and its summary) join `ingestion_manifest.csv` against the JSONL inventory on disk and classify every `(endpoint, year, session_key)` triple. They detect stale files (e.g. JSONL left behind by an earlier smoke run when `CLEAR_BRONZE_OUTPUTS=False`), row-count drift between manifest and disk, manifest-success rows with no file on Drive, failures that nonetheless left data on Drive, and required-endpoint failures with no file. The notebook prints a clear `OK` / `WARNING` line based on this report and **does not let Silver run on an inconsistent Bronze state**.

**Why targeted retry exists.** Full Bronze runs occasionally leave behind failed session-level rows in the manifest (most commonly HTTP 429 from the OpenF1 API). To recover those sessions without re-running the entire 2023–2025 ingestion, notebook `01_ingestion_bronze.ipynb` exposes an **optional targeted retry** at the bottom:

```python
RUN_TARGETED_RETRY    = False   # set True to actually run the retry
RETRY_SLEEP_SECONDS   = 3.0     # slower throttling to avoid another 429 storm
RETRY_INCLUDE_OPTIONAL = False  # set True to also retry starting_grid
RETRY_ENDPOINTS       = None    # e.g. ["session_result", "pit"]; None = all required
DELETE_STALE_SMOKE_FILES = False
```

Behaviour:

- Reads `artifacts/manifests/ingestion_manifest.csv`, picks rows where `status != success` for required **session-level** endpoints (`drivers`, `laps`, `pit`, `weather`, `position`, `race_control`, `session_result`), and retries only those (endpoint, session_key) pairs with a throttled `OpenF1Client` (default 3 s base sleep).
- `starting_grid` is excluded by default. Global endpoints (`meetings`, `sessions`) are never retried by this utility.
- Overwrites/creates only the corresponding `data/bronze/{endpoint}/year={year}/session_key={session_key}.jsonl` files on Drive.
- The original `ingestion_manifest.csv` is preserved unchanged. Retry results are written to `artifacts/manifests/ingestion_retry_manifest.csv`.
- After retry, the notebook **builds an effective post-retry manifest** at `artifacts/manifests/ingestion_manifest_effective.csv` (via `openf1_pipeline.ingestion.ingest.write_effective_manifest_after_retry(...)`), then regenerates `bronze_file_inventory.csv`, `bronze_row_counts.csv`, `bronze_schema_report.csv`, `bronze_schema_drift.csv`, the `artifacts/schemas/bronze_schema_report.csv` schema snapshot, the `bronze_manifest_file_reconciliation*.csv` reports **against the effective manifest**, and the `duckdb_bronze_*` validation CSVs so the on-disk Bronze evidence reflects the new files.
- Programmatic API: `openf1_pipeline.ingestion.ingest.retry_failed_session_endpoints(...)` and `openf1_pipeline.ingestion.ingest.write_effective_manifest_after_retry(...)`. See `artifacts/pipeline_logs/full_bronze_retry_plan.md` for the retry workflow, `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md` for the reconciliation contract, and `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md` for the effective-manifest contract.

**Effective post-retry manifest.** `ingestion_manifest_effective.csv` is built by overlaying the retry manifest on the original manifest:

- Successful original rows pass through unchanged (`manifest_source = "original"`).
- For every `(endpoint, session_key)` attempted by retry, the retry row supersedes the original — successful retries become `success`, still-failed retries remain `failed` (`manifest_source = "retry"`).
- Optional `starting_grid` failures (skipped by retry) pass through as `failed` and are accepted by reconciliation as `optional_missing`.
- The original `ingestion_manifest.csv` is **never modified**.

Reconciliation auto-detects this: if `ingestion_retry_manifest.csv` is present with at least one recovered row, the notebook reconciles against the effective manifest. Otherwise it reconciles against the original. The notebook prints which manifest path was used.

**How these reports gate Silver.** Proceed to `02_silver_cleaning_quality.ipynb` only when the reconciliation summary shows zero `row_count_mismatch`, zero `manifest_success_missing_file`, zero `stale_file_not_in_success_manifest`, and zero `failed_manifest_file_exists` for required endpoints (`optional_missing` for `starting_grid` is accepted). If the reconciliation flags issues, run the targeted retry first, then re-check against the effective manifest.

---

## 6. Tooling and execution

| Role | Tool |
|------|------|
| Development | **Cursor** |
| Version control | **GitHub** |
| Official execution | **Google Colab Pro Plus** |
| Persistent outputs | **Google Drive** (`OPENF1_DATA_ROOT`) |

**No local execution is required** for full pipeline evidence. Paths assume a cloned repo in Colab; bulk `data/` lives on Drive (see `notebooks/00_colab_setup.ipynb` and the setup cell in each pipeline notebook).

**Databricks is not required** and is out of scope for this project.

---

## 7. Technology stack and rationale

This is a **big data infrastructure** project first; ML consumes validated Gold outputs.

| Technology | Role |
|------------|------|
| **Python + requests** | OpenF1 API ingestion (immutable Bronze JSONL) |
| **PySpark** | **Primary** pipeline engine: Bronze reports, Silver cleaning, Gold feature mart |
| **DuckDB** | **Primary** SQL validation over Parquet/CSV (independent checks in notebooks 01–03) |
| **pandas** | Small audit CSV exports, notebook display, ML handoff (not the primary ETL engine) |
| **scikit-learn / LightGBM** | Modeling (notebook 04) |

**Databricks is not required** and is not used. PySpark runs locally in Colab (`get_spark()`).

### Execution flow

| Step | Notebook | Engines |
|------|----------|---------|
| 00 | Setup | — |
| 01 | Bronze ingestion + **Spark** Bronze reports + **DuckDB** validation | `BRONZE_REPORT_ENGINE=spark` |
| 02 | **Spark** Silver cleaning + **DuckDB** validation | `SILVER_ENGINE=spark` |
| 03 | **Spark** Gold mart + **DuckDB** validation | `GOLD_ENGINE=spark` |
| 04 | Modeling (pandas/sklearn) | — |
| 05 | Report artifacts | DuckDB + pandas display |

---

## 8. Pipeline architecture

```
OpenF1 API → Bronze (raw) → Silver (cleaned) → Gold (feature mart) → Modeling & reports
```

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw API payloads, ingestion metadata, row counts, schema reports |
| **Silver** | Typed, standardized tables; data quality audits; documented cleaning rules |
| **Gold** | Driver-race feature mart (`driver_race_feature_mart.parquet`); `points_finish` target; leakage guard; feature dictionary |

---

## 9. ML strategy (locked before modeling)

**Question:** Can we predict whether a driver finishes in the points?

**Task framing:** **Points-finish classification using integrated race-session features** — not strict pre-race prediction. The default model uses **Tier 1** early-session features (e.g. first-five-lap pace, early position) and **Tier 2** full-session analytical features (e.g. lap aggregates, pit stops, race-control counts, weather means).

| Field | Definition |
|-------|------------|
| `points_finish` | `1` if `points` > 0 in `session_result`; `0` otherwise |
| Base table | `session_result_clean.parquet` |
| Gold grain | `session_key`, `meeting_key`, `driver_number` |

### Models (notebook `04`)

1. **Random baseline** — class prior / random with seed 42  
2. **Heuristic baseline** — `points_finish = 1` if `first_observed_position` ≤ 10  
3. **Logistic Regression** — interpretability  
4. **Random Forest** — nonlinear interactions  
5. **LightGBM** — strong tabular model  

Set `MODELING_MODE = "smoke"` for wiring verification; use `"full"` for official season splits after a full 2023–2025 Gold run.

No deep learning. Feature engineering and leakage control are completed in Gold **before** modeling.

### Season splits (primary)

| Split | Season |
|-------|--------|
| Train | 2023 |
| Validation | 2024 |
| Test | 2025 |

Use **season-based** splits only — not a random row-level split (avoids leaking race/session structure). Fallback if needed: train 2023–early 2024, validation late 2024, test 2025 (document in manifest).

---

## 10. Feature and leakage strategy

**Tier 1 — early-session features:** first-five-lap pace (`avg_first5_lap_duration`, …), early position proxies (`first_observed_position`, `early_avg_position`, …).

**Tier 2 — full-session analytical features:** full-session lap pace (`lap_count`, `avg_lap_duration`, …), pit-stop counts and durations, session-level weather aggregates, race-control message counts.

**Optional categoricals (opt-in):** `team_name`, `circuit_short_name`, `session_country_name`, `location`, `session_type`, `session_name` (`session_year` excluded from default X when using season splits).

**Forbidden model inputs** (may exist for labels/diagnostics): `position`, `points`, `final_position`, `result_*`, `duration`, `gap_to_leader`, `number_of_laps`, `diagnostic_*`.

Enforced by:

- `reports/data_quality/gold_leakage_guard_report.csv`
- `artifacts/feature_definitions/feature_dictionary.csv` (`modeling_role`, `allowed_for_modeling`, `feature_tier`)
- `artifacts/feature_definitions/model_feature_plan.csv` (frozen default 40 numeric features)
- `openf1_pipeline.modeling.feature_selection` and `get_model_feature_columns()` in `modeling/train.py`

Design Gold and review leakage reports **before** running notebook `04`.

---

## 11. Google Colab execution (official)

**Repository:** https://github.com/dk546/openf1-big-data-pipeline

### Why each notebook has its own setup cell

Colab **notebook tabs do not share runtime state**. Opening `01` or `02` in a new tab starts a fresh kernel — variables, mounts, and `sys.path` from `00` are **not** available. Every execution notebook (`00`, `01`, `02`, …) therefore includes the **same standard setup cell** at the top.

### Recommended persistence setup

| Location | Purpose |
|----------|---------|
| `/content/openf1-big-data-pipeline` | GitHub clone — **source code and notebooks** |
| `/content/drive/MyDrive/openf1_big_data_pipeline` | **Generated outputs** when `USE_GOOGLE_DRIVE=True` |

Set `USE_GOOGLE_DRIVE=True` in each notebook. The setup cell will:

1. Mount Google Drive (if enabled)
2. Set `OPENF1_DATA_ROOT=/content/drive/MyDrive/openf1_big_data_pipeline`
3. Clone or `git pull` the repo into `/content/openf1-big-data-pipeline`
4. Run `pip install -r requirements.txt`
5. Run `pip install -e .` (requires `pyproject.toml`)
6. Add `src/` to `sys.path` as fallback
7. Import `openf1_pipeline` and print resolved paths

**Important:** `OPENF1_DATA_ROOT` is set **before** importing `openf1_pipeline.config`.

### Run order

| Step | Notebook | Purpose |
|------|----------|---------|
| 00 | `00_colab_setup.ipynb` | Validate environment (optional first time) |
| 01 | `01_ingestion_bronze.ipynb` | Bronze ingestion (`SMOKE_TEST=True` first) |
| 02 | `02_silver_cleaning_quality.ipynb` | Silver cleaning + DQ reports |
| 03 | `03_gold_feature_engineering.ipynb` | Gold feature mart + leakage guard |
| 04 | `04_modeling_evaluation.ipynb` | Baselines + models (`MODELING_MODE=smoke` or `full`) |
| 05 | `05_report_artifacts.ipynb` | Report tables and figures |

**Smoke path:** `01` with `SMOKE_TEST=True`, `MAX_SESSIONS=2` → `02` → `03`.  
**Full path:** re-run `01` with `SMOKE_TEST=False` (2023–2025) → `02` → `03` → `04` → `05`.

### Drive output cleanup policy (idempotent reruns)

Notebooks are safe to rerun on Google Drive when cleanup flags are set. Each notebook cleans **only its layer** — upstream data is preserved by default.

| Notebook | Flag | Default | Cleans | Does not delete |
|----------|------|---------|--------|-----------------|
| 01 | `CLEAR_BRONZE_OUTPUTS` | `False` | `data/bronze/`, Bronze DQ CSVs, ingestion manifest, schemas | Silver, Gold, models |
| 02 | `CLEAR_SILVER_OUTPUTS` | `True` | `data/silver/`, `silver_*.csv`, `duckdb_silver_*.csv` | Bronze, Gold |
| 03 | `CLEAR_GOLD_OUTPUTS` | `True` | `data/gold/`, Gold DQ CSVs, `duckdb_gold_*.csv`, feature dictionary | Silver, model results |
| 04 | `CLEAR_MODEL_OUTPUTS` | `True` | `reports/model_results/`, `model_run_manifest.json` | Gold, DQ reports |
| 05 | `CLEAR_REPORT_ARTIFACTS` | `True` | `reports/tables/`, `reports/figures/` | DQ reports, model results |

**Engine fallback:** `ALLOW_FALLBACK = False` in notebooks 01–03 (default). Spark is the official engine; pandas fallback is manual only and cleans the target layer first when enabled.

**After a failed partial run:** Rerun notebook 02+ with cleanup flags at their defaults. If notebook 02 failed mid-Spark, `CLEAR_SILVER_OUTPUTS=True` removes partial Spark Parquet directories before retry.

**Warning:** `CLEAR_BRONZE_OUTPUTS=True` deletes all Bronze JSONL and requires re-ingestion from the OpenF1 API (slow).

Utilities: `openf1_pipeline.utils.cleanup` (`clean_silver_layer_outputs`, `clean_gold_layer_outputs`, etc.) and `openf1_pipeline.utils.io` (`clean_directory_contents`).

### Packaging

```bash
pip install -r requirements.txt
pip install -e .
```

Runtime dependencies live in `requirements.txt`. `pyproject.toml` defines the installable `src/openf1_pipeline` package only.

### Gold layer (`03_gold_feature_engineering.ipynb`)

**Prerequisites:** Silver `session_result_clean.parquet`, `laps_clean.parquet`, and `drivers_clean.parquet` must exist. Empty `starting_grid_clean.parquet` is expected on some smoke runs.

| Item | Detail |
|------|--------|
| **Grain** | One row per `session_key`, `meeting_key`, `driver_number` |
| **Base table** | `session_result_clean.parquet` |
| **Target** | `points_finish` = 1 if `points` > 0, else 0 |
| **Output** | `data/gold/driver_race_feature_mart.parquet` |

**Feature groups:** Tier 1 early-session (first-five laps, early position); Tier 2 full-session analytical (lap aggregates, pit, weather, race control); driver/session/meeting metadata (optional categoricals).

**Leakage guard:** `reports/data_quality/gold_leakage_guard_report.csv` marks raw outcome fields (`position`, `points`, `final_position`, `result_*`, etc.) as **not** allowed for modeling. Tier 2 analytical features are **allowed** for the default integrated-session task; they are documented in `model_feature_plan.csv`, not treated as label leakage.

**Reports:**

| Report | Path |
|--------|------|
| Feature summary stats | `reports/data_quality/gold_feature_summary_stats.csv` |
| Feature missingness | `reports/data_quality/gold_feature_missingness.csv` |
| Target distribution | `reports/data_quality/gold_target_distribution.csv` |
| Join quality | `reports/data_quality/gold_join_quality_report.csv` |
| Leakage guard | `reports/data_quality/gold_leakage_guard_report.csv` |
| Feature dictionary | `artifacts/feature_definitions/feature_dictionary.csv` |

### Before modeling

Confirm Gold mart row count matches Silver `session_result_clean` driver-session grain. Review leakage guard and feature dictionary before notebook `04`.

### After Colab runs

- Bulk `data/` stays on **Drive** (gitignored).
- Commit **code** and lightweight **CSV summaries** to GitHub when appropriate.
- For written analysis or reporting, cite **Drive-generated outputs** from the full Colab run.

---

## 12. Repository structure

```
openf1-big-data-pipeline/
├── README.md
├── pyproject.toml                # Editable package (pip install -e .)
├── requirements.txt
├── notebooks/                    # Colab notebooks (each has setup cell)
├── scripts/                      # colab_bootstrap.py, rebuild_colab_notebooks.py
├── src/openf1_pipeline/          # Pipeline Python package
├── data/                         # bronze | silver | gold (gitignored bulk)
├── reports/                      # data_quality | model_results | figures | tables
└── artifacts/                    # manifests | schemas | feature_definitions | logs
```

On Colab with Drive: outputs live under `/content/drive/MyDrive/openf1_big_data_pipeline/`, not inside the repo clone.

---

## 13. Reproducibility statement

- Dependencies are listed in `requirements.txt`.
- Random seed: `42` (see `src/openf1_pipeline/config.py`).
- Each run should update `artifacts/manifests/run_manifest.json` with git commit, seasons, row counts, and artifact paths.
- Check items in `implementation_checklist.md` only after successful Colab execution.

---

## 14. Current implementation status

| Phase | Status |
|-------|--------|
| Project docs (`project_context`, `project_plan`, checklist) | Done |
| Repository scaffold (folders, placeholders, README) | Done |
| Bronze ingestion | **Done** — full 2023–2025 run + targeted retry; effective-manifest reconciliation passed (`evidence/full_2023_2025/`); 148,184 rows across required endpoints |
| Silver cleaning & DQ reports | **Done** — full run; 148,184 rows preserved with 0 row loss; 0 rejected records; 0 referential-integrity unmatched |
| Gold feature mart | **Done** — full run; 1,756 driver-race rows × 62 cols; 40 default numeric model features (8 Tier 1 + 32 Tier 2); leakage guard and feature dictionary committed |
| Modeling & evaluation | **Done** — `MODELING_MODE="full"`, season splits 2023/2024/2025 (558/599/599); best model = Random Forest (test F1 = 0.7837, ROC-AUC = 0.8733, accuracy = 0.7963); `model_run_manifest.json` written |
| Report artifacts & final manifest | **Done** — Notebook 05 wrote 11 report tables (`reports/tables/`) and 5 figures (`reports/figures/`); `artifacts/manifests/run_manifest.json` written; full evidence consolidated under `evidence/full_2023_2025/` |

Final-run evidence: see [`evidence/full_2023_2025/`](evidence/full_2023_2025/). Track progress in `implementation_checklist.md`.

---

## 15. Report structure and artifact traceability

The final written deliverable structure is **locked** in [`reports/report_draft/report_structure.md`](reports/report_draft/report_structure.md).

| Document | Purpose |
|----------|---------|
| [`report_artifact_map.md`](reports/report_draft/report_artifact_map.md) | Maps every report subsection to pipeline CSVs, notebooks, and paths |
| [`table_figure_register.md`](reports/report_draft/table_figure_register.md) | Planned Tables 1–14 and Figures 1–8 with source artifacts |
| [`narrative_guardrails.md`](reports/report_draft/narrative_guardrails.md) | Locked wording (task framing, tiers, smoke vs full) |

**Evidence policy:** Smoke runs under `evidence/smoke_2024_maxsessions2*/` validated pipeline wiring and joins. **Official performance claims** are based on the full 2023–2025 Colab execution and are consolidated under [`evidence/full_2023_2025/`](evidence/full_2023_2025/) — `reports/data_quality/`, `reports/model_results/`, `reports/tables/`, `reports/figures/`, and `artifacts/manifests/{ingestion_manifest,ingestion_manifest_effective,ingestion_retry_manifest,model_run_manifest,run_manifest}` collectively constitute the submission evidence bundle. Do not cite smoke model metrics as final findings.
