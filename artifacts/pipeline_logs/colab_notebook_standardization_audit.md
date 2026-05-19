# Colab Notebook Standardization Audit

**Audit date (UTC):** 2026-05-19  
**Repo:** https://github.com/dk546/openf1-big-data-pipeline

---

## Problem summary

Repeated Colab failures during manual troubleshooting:

| # | Issue |
|---|--------|
| 1 | `ModuleNotFoundError: No module named 'openf1_pipeline'` |
| 2 | `pip install -e .` failed — no `setup.py` / `pyproject.toml` |
| 3 | Setup in `00` did not carry to `01`/`02` in separate Colab tabs |
| 4 | Imports before clone / `src` path / package install |
| 5 | Ephemeral `/content` outputs lost on disconnect |

`01_ingestion_bronze.ipynb` had accumulated debug cells (`importlib`, `ls`, file content dumps, duplicate import cells) from manual fixes.

---

## Files reviewed

- `notebooks/00_colab_setup.ipynb` (Colab outputs with successful Drive mount)
- `notebooks/01_ingestion_bronze.ipynb` (messy troubleshooting cells)
- `notebooks/02_silver_cleaning_quality.ipynb`
- `requirements.txt`, `pyproject.toml` (missing)
- `src/openf1_pipeline/config.py`
- `README.md`, `.gitignore`
- `artifacts/pipeline_logs/colab_setup_import_audit.md`
- Git tracked files under `data/`, `reports/`, `artifacts/`

---

## Files changed

| File | Change |
|------|--------|
| `pyproject.toml` | **Created** — setuptools src-layout, empty `[project].dependencies` |
| `scripts/colab_bootstrap.py` | **Created** — reference bootstrap after install |
| `scripts/rebuild_colab_notebooks.py` | **Created** — regenerates 00/01/02 without saved outputs |
| `notebooks/00_colab_setup.ipynb` | Rebuilt — standard setup + import check |
| `notebooks/01_ingestion_bronze.ipynb` | Rebuilt — removed debug cells; single setup pattern |
| `notebooks/02_silver_cleaning_quality.ipynb` | Rebuilt — setup + Bronze preflight |
| `src/openf1_pipeline/config.py` | `MARKER_FILES = ("README.md",)` only |
| `src/openf1_pipeline/silver/build_silver.py` | Error message uses `USE_GOOGLE_DRIVE` wording |
| `README.md` | Colab tabs, packaging, run order |
| `.gitignore` | `*.egg-info/` etc. |
| `implementation_checklist.md` | Setup items marked (local, gitignored) |

---

## Standard setup pattern (A–I)

Embedded identically in `00`, `01`, `02`:

- **A** `REPO_URL`, `REPO_NAME`, `PROJECT_ROOT=/content/openf1-big-data-pipeline`
- **B** `USE_GOOGLE_DRIVE`, `DRIVE_OUTPUT_ROOT`, mount Drive, set `OPENF1_DATA_ROOT`
- **D** Clone or `git pull`
- **E** Verify `README.md`, `pyproject.toml`, `src/openf1_pipeline/`
- **F** `pip install -r requirements.txt` + `pip install -e .`
- **G** `sys.path` fallback for `src/`
- **H** `import openf1_pipeline` + `ensure_project_directories()`
- **I** Print `PROJECT_ROOT`, `OUTPUT_ROOT`, `DATA_DIR`, `BRONZE_DIR`, `SILVER_DIR`, `GOLD_DIR`, `REPORTS_DIR`, `ARTIFACTS_DIR`

Regenerate notebooks: `python scripts/rebuild_colab_notebooks.py`

---

## Packaging fix status

| Item | Status |
|------|--------|
| `pyproject.toml` | **Added** |
| `pip install -e .` in notebooks | **Yes** |
| `requirements.txt` without `-e .` | **Verified** |
| `src/openf1_pipeline` discoverable | **Yes** (editable install + sys.path fallback) |

---

## Google Drive persistence status

| Item | Status |
|------|--------|
| `OPENF1_DATA_ROOT` | `/content/drive/MyDrive/openf1_big_data_pipeline` |
| `config.get_output_root()` | Uses env when set |
| Same flag in 00/01/02 | `USE_GOOGLE_DRIVE = True` |
| Outputs on Drive | `data/`, `reports/`, `artifacts/` |

---

## Notebook-specific checks

| Notebook | Check |
|----------|-------|
| `00` | Setup only; no ingestion |
| `01` | `SMOKE_TEST`, `MAX_SESSIONS=2`; endpoint lists; Drive warning for full run |
| `02` | Bronze JSONL + manifest/row-count preflight; no Gold/modeling |

---

## Git / tracked files

- **Pull:** `origin/main` already up to date; working tree was clean before edits.
- **Tracked:** Lightweight CSVs under `reports/data_quality/` and `artifacts/` (smoke evidence) — OK per project policy.
- **Not tracked:** `*.jsonl`, `*.parquet` under `data/` (gitignored).
- **No** large binary data files found in git index.

---

## Remaining risks

| Risk | Mitigation |
|------|------------|
| User opens notebook without running setup cell | Run top setup cell first every session |
| `USE_GOOGLE_DRIVE` mismatch between 01 and 02 | Use same value in both; error message documents this |
| Colab disconnect during full Bronze | Drive persistence + rerun idempotent paths |
| `pyspark` / `lightgbm` slow or heavy on Colab | Kept in requirements; monitor install time |
| First run after push: Colab needs `git pull` for `pyproject.toml` | Setup cell runs `git pull` |

---

## Recommended Colab run order

1. `00_colab_setup.ipynb` (validate once per session)
2. `01` smoke (`SMOKE_TEST=True`, `MAX_SESSIONS=2`)
3. `02` on smoke Bronze
4. `01` full (`SMOKE_TEST=False`, `USE_GOOGLE_DRIVE=True`)
5. `02` full
6. Gold / modeling when implemented

---

## Final verdict

**Ready for Colab smoke run**

After pushing these changes to GitHub, open `01` in Colab, run the setup cell, confirm `import openf1_pipeline` succeeds and paths point to Drive, then run smoke ingestion.

---

## Execution note

No full Bronze ingestion or Silver cleaning was executed during this audit in Cursor.
