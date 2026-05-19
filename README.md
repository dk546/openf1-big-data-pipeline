# A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

**Repository:** `openf1-big-data-pipeline`

---

## 1. Project title

A Medallion Architecture Data Pipeline for Formula 1 Race Performance Classification Using OpenF1 Data

---

## 2. Project purpose

This resit project builds an **evidence-driven, reproducible data pipeline** for Formula 1 data from the [OpenF1 API](https://openf1.org/). The pipeline lands raw data (Bronze), cleans and audits it (Silver), integrates a driver-race feature mart (Gold), and runs simple classification models to validate that the Gold layer supports analysis.

The MBA report is supported by **generated tables, manifests, and quality reports**—not narrative claims alone.

---

## 3. Course / resit framing

This work is submitted for an **MBA Big Data Infrastructures** resit. Grading emphasizes:

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

Bronze ingestion (`01_ingestion_bronze.ipynb`) must be executed in **Google Colab Pro Plus** for official resit evidence. A local smoke test is for development only.

**Artifacts produced:**

| Artifact | Path |
|----------|------|
| Ingestion manifest | `artifacts/manifests/ingestion_manifest.csv` |
| File inventory | `reports/data_quality/bronze_file_inventory.csv` |
| Row counts | `reports/data_quality/bronze_row_counts.csv` |
| Schema report | `reports/data_quality/bronze_schema_report.csv` |
| Schema drift | `reports/data_quality/bronze_schema_drift.csv` |

Raw payloads: `data/bronze/{endpoint}/…/*.jsonl` (typically on Google Drive via `OPENF1_DATA_ROOT`).

**`session_result`** is included in Bronze because it provides race classification and points used to construct the Gold target **`points_finish`** (`1` if `points` > 0, else `0`).

**`starting_grid`** is optional (may be empty on smoke). The planned **heuristic baseline** uses **`first_observed_position` ≤ 10** from Gold early-position features, not grid position.

---

## 6. Tooling and execution

| Role | Tool |
|------|------|
| Development | **Cursor** |
| Version control | **GitHub** |
| Official execution | **Google Colab Pro Plus** |
| Persistent outputs | **Google Drive** (`OPENF1_DATA_ROOT`) |

**No local execution is required** for MBA evidence. Paths assume a cloned repo in Colab; bulk `data/` lives on Drive (see `notebooks/00_colab_setup.ipynb` and the setup cell in each pipeline notebook).

**Databricks is not required** and is out of scope for this resit.

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

Set `MODELING_MODE = "smoke"` for wiring verification; use `"full"` for official MBA season splits after 2023–2025 Gold run.

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

**Allowed predictive groups:** metadata, early lap pace, lap aggregates, pit strategy, early position proxy, weather, race control counts.

**Forbidden model inputs** (may exist for labels/diagnostics): `position`, `points`, `final_position`, `result_*`, `duration`, `gap_to_leader`, `number_of_laps`, `diagnostic_*`.

Enforced by:

- `reports/data_quality/gold_leakage_guard_report.csv`
- `artifacts/feature_definitions/feature_dictionary.csv` (`modeling_role`, `allowed_for_modeling`)
- `get_model_feature_columns()` in `gold/build_feature_mart.py`

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

**Feature groups:** lap pace (incl. first-five laps), pit stops, early position, session weather, race-control counts, driver/session/meeting metadata.

**Leakage guard:** `reports/data_quality/gold_leakage_guard_report.csv` marks outcome fields (`position`, `points`, `final_position`, `result_*`, etc.) as **not** allowed for modeling. Engineered features use early-race windows only where specified (e.g. first five laps / position observations).

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
- For the MBA report, cite **Drive-generated outputs** from the full Colab run.

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
| Bronze ingestion | Code complete — Colab evidence in `evidence/smoke_2024_maxsessions2/` |
| Silver cleaning & DQ reports | Code complete — Colab smoke passed |
| Gold feature mart | Code complete — run `03` in Colab for artifacts |
| Modeling & evaluation | Code complete — run `04` in Colab (`MODELING_MODE=smoke` then `full`) |
| Report artifacts & final manifest | Code complete — run `05` in Colab after modeling |

Track progress in `implementation_checklist.md`.
