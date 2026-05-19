# Colab Setup & Import Path Audit

**Last updated (UTC):** 2026-05-19

---

## Purpose

Document Colab runtime setup, import paths, and Google Drive persistence so Bronze/Silver runs survive disconnects and path resolution is reproducible for MBA evidence.

---

## Files reviewed / updated

| File | Change |
|------|--------|
| `notebooks/00_colab_setup.ipynb` | Full setup: clone, pip, Drive mount, `OPENF1_DATA_ROOT`, `ensure_project_directories()` |
| `notebooks/01_ingestion_bronze.ipynb` | `USE_GOOGLE_DRIVE`, env before config import, path verification |
| `notebooks/02_silver_cleaning_quality.ipynb` | Same Drive pattern; Bronze preflight with resolved `BRONZE_DIR` |
| `src/openf1_pipeline/config.py` | `OUTPUT_ROOT` via `OPENF1_DATA_ROOT`; unified output paths |
| `src/openf1_pipeline/silver/build_silver.py` | `FileNotFoundError` with `BRONZE_DIR` guidance |
| `README.md` | Colab persistence section |

---

## Google Drive persistence

### Why it was added

Colab `/content` is **ephemeral**. Long Bronze ingestion (2023–2025, all race sessions) or Silver cleaning can exceed session lifetime or disconnect. Without Drive, raw JSONL, manifests, and DQ reports may be lost.

### Environment variable

| Variable | When set | Effect |
|----------|----------|--------|
| `OPENF1_DATA_ROOT` | Colab with `USE_GOOGLE_DRIVE=True` | All generated outputs under this folder |
| *(unset)* | Local dev or Colab without Drive | Outputs under repo `PROJECT_ROOT` |

**Example (Colab):**

```
OPENF1_DATA_ROOT=/content/drive/MyDrive/openf1_big_data_pipeline
```

### Output root layout

When `OPENF1_DATA_ROOT` is set:

```
/content/drive/MyDrive/openf1_big_data_pipeline/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── reports/
│   └── data_quality/
└── artifacts/
    ├── manifests/
    ├── schemas/
    └── pipeline_logs/
```

**Code** remains in:

```
/content/openf1-big-data-pipeline/   # GitHub clone
```

### Notebooks updated

| Notebook | Drive behavior |
|----------|----------------|
| `00_colab_setup.ipynb` | `USE_GOOGLE_DRIVE=True` → mount + set env → import config |
| `01_ingestion_bronze.ipynb` | Same; path verification before ingestion |
| `02_silver_cleaning_quality.ipynb` | Same; Bronze preflight uses Drive `BRONZE_DIR` |

### Critical import order

1. Add `src/` to `sys.path`
2. Set `os.environ["OPENF1_DATA_ROOT"]` (if using Drive)
3. **Then** `from openf1_pipeline.config import ...`

If `config` is imported before `OPENF1_DATA_ROOT` is set, path getters still read the env at **call time** (not import time), so `get_bronze_dir()` after setting env is correct. Re-importing a cached module is not required; only call getters after env setup.

### Remaining risk

**User must use the same `USE_GOOGLE_DRIVE` (and thus `OPENF1_DATA_ROOT`) in Bronze and Silver.**

- Bronze to Drive + Silver without Drive → Silver looks at repo-local empty Bronze → preflight error.
- Fix: run `02` with matching `USE_GOOGLE_DRIVE=True`.

### Legacy `DATA_ROOT`

Older docs referenced `DATA_ROOT` as the `data/` folder path. `config.get_output_root()` still accepts legacy `DATA_ROOT` if it points at a directory named `data` (parent used as output root). New Colab notebooks use **`OPENF1_DATA_ROOT`** only.

---

## Project root detection

| Marker | Used |
|--------|------|
| `README.md` | Yes (public GitHub) |
| `project_context.md` | Yes (local Cursor only) |
| `OPENF1_PROJECT_ROOT` env | Optional override |

Colab fallback: `/content/openf1-big-data-pipeline`

---

## Verdict

| Check | Status |
|-------|--------|
| Drive mount in setup notebook | Implemented |
| `OPENF1_DATA_ROOT` before config use | Documented + notebook order |
| Outputs on Drive when enabled | Implemented |
| Bronze/Silver path consistency | User responsibility (documented) |
| Full ingestion run in Cursor | **Not run** (by design) |
