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

**`session_result`** is included in Bronze because it provides final race classification positions used to construct the Gold target **`points_finish`** (top 10 classified = points finish).

**`starting_grid`** is included when the API returns data; it supports the grid-position heuristic baseline. Some sessions return HTTP 404 or zero rows—the pipeline records this in the manifest and continues.

---

## 6. Tooling

| Role | Tool |
|------|------|
| Development | **Cursor** |
| Version control | **GitHub** |
| Execution | **Google Colab Pro Plus** |

**No local execution is required.** Paths assume a cloned repo in Colab; large datasets should live on **Google Drive** (see `notebooks/00_colab_setup.ipynb` and the standard setup cell in each pipeline notebook).

---

## 7. Pipeline architecture

```
OpenF1 API → Bronze (raw) → Silver (cleaned) → Gold (feature mart) → Modeling & reports
```

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw API payloads, ingestion metadata, row counts, schema reports |
| **Silver** | Typed, standardized tables; data quality audits; documented cleaning rules |
| **Gold** | Driver-race feature mart; `points_finish` target; feature dictionary |

---

## 8. Classification task

**Question:** Can we predict whether a driver finishes in the points?

| Field | Definition |
|-------|------------|
| `points_finish` | `1` if driver finishes in **top 10 classified** race result; `0` otherwise |

**Modeling splits (by season):**

| Split | Season |
|-------|--------|
| Train | 2023 |
| Validation | 2024 |
| Test | 2025 |

Features must respect a documented **decision-time cutoff** (no label leakage). See `artifacts/feature_definitions/feature_dictionary.csv` when generated.

---

## 9. Google Colab execution (official)

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

1. `notebooks/00_colab_setup.ipynb` — validate environment (optional but recommended first time)
2. `notebooks/01_ingestion_bronze.ipynb` with `SMOKE_TEST=True`, `MAX_SESSIONS=2`
3. `notebooks/02_silver_cleaning_quality.ipynb` on smoke Bronze outputs
4. Re-run `01` with `SMOKE_TEST=False` for full seasons 2023–2025 (Drive **required** — long run)
5. Re-run `02` on full Bronze
6. `03` → `05` when implemented

### Packaging

```bash
pip install -r requirements.txt
pip install -e .
```

Runtime dependencies live in `requirements.txt`. `pyproject.toml` defines the installable `src/openf1_pipeline` package only.

### Before Gold / modeling

Confirm `session_result` has non-zero rows in the ingestion manifest and Bronze inventory. Silver must produce `session_result_clean.parquet` with rows before building the Gold target `points_finish`.

### After Colab runs

- Bulk `data/` stays on **Drive** (gitignored).
- Commit **code** and lightweight **CSV summaries** to GitHub when appropriate.
- For the MBA report, cite **Drive-generated outputs** from the full Colab run.

---

## 10. Repository structure

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

## 11. Reproducibility statement

- Dependencies are listed in `requirements.txt`.
- Random seed: `42` (see `src/openf1_pipeline/config.py`).
- Each run should update `artifacts/manifests/run_manifest.json` with git commit, seasons, row counts, and artifact paths.
- Check items in `implementation_checklist.md` only after successful Colab execution.

---

## 12. Current implementation status

| Phase | Status |
|-------|--------|
| Project docs (`project_context`, `project_plan`, checklist) | Done |
| Repository scaffold (folders, placeholders, README) | Done |
| Bronze ingestion | Not started |
| Silver cleaning & DQ reports | Not started |
| Gold feature mart | Not started |
| Modeling & evaluation | Not started |
| Report artifacts & final manifest | Not started |

Track progress in `implementation_checklist.md`.
