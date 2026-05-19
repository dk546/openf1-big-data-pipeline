"""Rebuild 00–03 Colab notebooks with standardized setup (no saved outputs)."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / "notebooks"

COLAB_SETUP = r'''import os
import subprocess
import sys
from pathlib import Path

# A. Repository (code)
REPO_URL = "https://github.com/dk546/openf1-big-data-pipeline.git"
REPO_NAME = "openf1-big-data-pipeline"
PROJECT_ROOT = Path(f"/content/{REPO_NAME}")

# B. Google Drive persistence (outputs) — set OPENF1_DATA_ROOT before importing config
USE_GOOGLE_DRIVE = True
DRIVE_OUTPUT_ROOT = Path("/content/drive/MyDrive/openf1_big_data_pipeline")

if USE_GOOGLE_DRIVE:
    from google.colab import drive
    drive.mount("/content/drive")
    DRIVE_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    os.environ["OPENF1_DATA_ROOT"] = str(DRIVE_OUTPUT_ROOT)
    print("OPENF1_DATA_ROOT:", os.environ["OPENF1_DATA_ROOT"])
else:
    os.environ.pop("OPENF1_DATA_ROOT", None)
    print("OPENF1_DATA_ROOT not set — repo-local outputs.")

# D. Clone or update repository
if not PROJECT_ROOT.exists():
    print("Cloning repository...")
    subprocess.run(["git", "clone", REPO_URL, str(PROJECT_ROOT)], check=True)
else:
    print("Updating repository...")
    subprocess.run(["git", "-C", str(PROJECT_ROOT), "pull"], check=False)

os.chdir(PROJECT_ROOT)
print("Working directory:", Path.cwd())

# E. Verify project files
_checks = {
    "README.md": PROJECT_ROOT / "README.md",
    "pyproject.toml": PROJECT_ROOT / "pyproject.toml",
    "src/openf1_pipeline": PROJECT_ROOT / "src" / "openf1_pipeline",
    "src/openf1_pipeline/__init__.py": PROJECT_ROOT / "src" / "openf1_pipeline" / "__init__.py",
    "src/openf1_pipeline/config.py": PROJECT_ROOT / "src" / "openf1_pipeline" / "config.py",
}
for name, path in _checks.items():
    if not path.exists():
        raise FileNotFoundError(f"Missing {name}: {path}")

# F. Install dependencies and editable package
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"], check=True)
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-e", "."], check=True)

# G. Fallback: src on sys.path
_src = PROJECT_ROOT / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

# H. Import test
import openf1_pipeline  # noqa: E402

from openf1_pipeline.config import (  # noqa: E402
    ensure_project_directories,
    get_artifacts_dir,
    get_bronze_dir,
    get_data_dir,
    get_gold_dir,
    get_output_root,
    get_project_root,
    get_reports_dir,
    get_silver_dir,
)

paths = ensure_project_directories()

# I. Path summary
print("PROJECT_ROOT:", get_project_root())
print("OUTPUT_ROOT:", get_output_root())
print("DATA_DIR:", get_data_dir())
print("BRONZE_DIR:", get_bronze_dir())
print("SILVER_DIR:", get_silver_dir())
print("GOLD_DIR:", get_gold_dir())
print("REPORTS_DIR:", get_reports_dir())
print("ARTIFACTS_DIR:", get_artifacts_dir())
'''


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [line + "\n" for line in text.split("\n")]}


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "source": [line + "\n" for line in text.split("\n")],
        "outputs": [],
        "execution_count": None,
    }


def notebook(cells: list) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.10.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def write_nb(name: str, cells: list) -> None:
    path = NB / name
    path.write_text(json.dumps(notebook(cells), indent=1), encoding="utf-8")
    print("Wrote", path)


def build_00() -> None:
    cells = [
        md("""# 00 — Colab Setup

Run this notebook first to validate the environment. **Each pipeline notebook (01, 02, …) includes its own identical setup cell** because Colab tabs and runtimes do not share state."""),
        md("""## Why separate setup in every notebook?

- Opening `01` or `02` in a **new Colab tab** starts a **fresh runtime** — `00` setup is not inherited.
- `/content` is **temporary**; disconnects can wipe local outputs.
- **Google Drive** (`USE_GOOGLE_DRIVE=True`) stores `data/`, `reports/`, and `artifacts/` at:
  `/content/drive/MyDrive/openf1_big_data_pipeline`
- **Code** stays in `/content/openf1-big-data-pipeline` (GitHub clone).
- Set `OPENF1_DATA_ROOT` **before** importing `openf1_pipeline.config`."""),
        md("## Standard Colab setup"),
        code(COLAB_SETUP),
        md("## Verify OpenF1 client"),
        code("""import pandas as pd
import requests

from openf1_pipeline.config import ENDPOINTS, OPENF1_BASE_URL, SEASONS
from openf1_pipeline.ingestion.openf1_client import OpenF1Client

print("SEASONS:", SEASONS)
print("ENDPOINTS:", ENDPOINTS)
print("OpenF1 base URL:", OPENF1_BASE_URL)
print("OpenF1Client:", OpenF1Client().base_url)
print("Setup complete — proceed to 01_ingestion_bronze.ipynb")"""),
    ]
    write_nb("00_colab_setup.ipynb", cells)


def build_01() -> None:
    cells = [
        md("""# 01 — Ingestion & Bronze Layer

Download OpenF1 data, save **raw Bronze JSONL**, and generate ingestion evidence (manifest, row counts, schema reports).

| Endpoint | Role |
|----------|------|
| `session_result` | **Required** for Gold target `points_finish` |
| `starting_grid` | **Optional** (may be 404 / empty) |
| Other session endpoints | Per-race telemetry and results |
| `meetings`, `sessions` | Global calendar discovery |

**Run order:** `SMOKE_TEST=True` first → then full run with `SMOKE_TEST=False` and `USE_GOOGLE_DRIVE=True`.

> Full ingestion can take **hours**. Use Drive persistence. Official MBA evidence must come from **Colab Pro Plus**."""),
        md("""## Colab setup (required every session)

This cell clones/updates the repo, installs `requirements.txt` and `pip install -e .`, mounts Drive, and sets `OPENF1_DATA_ROOT`. Do not import `openf1_pipeline` before this cell completes."""),
        code(COLAB_SETUP),
        md("## Start Spark (PySpark — local session, no Databricks)"),
        code("""from openf1_pipeline.utils.spark import get_spark

spark = get_spark()
print("Spark version:", spark.version)"""),
        md("## Bronze configuration & path check"),
        code("""from openf1_pipeline.config import (
    ENDPOINTS,
    GLOBAL_ENDPOINTS,
    SESSION_ENDPOINTS,
    SEASONS,
    get_bronze_dir,
    get_data_quality_reports_dir,
    get_manifests_dir,
    get_output_root,
    get_project_root,
    get_schemas_dir,
)
from openf1_pipeline.ingestion.ingest import run_bronze_ingestion, summarize_manifest
from openf1_pipeline.bronze.build_bronze import generate_bronze_reports

print("PROJECT_ROOT:", get_project_root())
print("OUTPUT_ROOT:", get_output_root())
print("ENDPOINTS:", ENDPOINTS)
print("GLOBAL_ENDPOINTS:", GLOBAL_ENDPOINTS)
print("SESSION_ENDPOINTS:", SESSION_ENDPOINTS)
print("BRONZE_DIR:", get_bronze_dir())
print("MANIFESTS_DIR:", get_manifests_dir())
print("DATA_QUALITY_REPORTS_DIR:", get_data_quality_reports_dir())
print("SCHEMAS_DIR:", get_schemas_dir())
print("session_result: REQUIRED for Gold points_finish")
print("starting_grid: OPTIONAL — failures are logged and ingestion continues")"""),
        code("""# Step 1: smoke test. Step 2: set SMOKE_TEST = False for full 2023–2025 evidence.
SMOKE_TEST = True
MAX_SESSIONS = 2 if SMOKE_TEST else None
INGEST_SEASONS = [2024] if SMOKE_TEST else SEASONS

# WARNING: CLEAR_BRONZE_OUTPUTS=True deletes all Bronze JSONL and re-ingests from API (slow).
CLEAR_BRONZE_OUTPUTS = False
BRONZE_REPORT_ENGINE = "spark"
ALLOW_FALLBACK = False

print(f"SMOKE_TEST={SMOKE_TEST}, seasons={INGEST_SEASONS}, max_sessions={MAX_SESSIONS}")
print(f"CLEAR_BRONZE_OUTPUTS={CLEAR_BRONZE_OUTPUTS}, ALLOW_FALLBACK={ALLOW_FALLBACK}")
if not SMOKE_TEST:
    print("WARNING: Full ingestion may take a long time. Ensure USE_GOOGLE_DRIVE=True above.")"""),
        md("## Optional: clean Bronze outputs"),
        code("""from openf1_pipeline.utils.cleanup import clean_bronze_layer_outputs

if CLEAR_BRONZE_OUTPUTS:
    print("WARNING: Deleting Bronze data and Bronze reports — re-ingestion required.")
    clean_bronze_layer_outputs()
else:
    print("Skipping Bronze cleanup (CLEAR_BRONZE_OUTPUTS=False).")"""),
        md("## Run Bronze ingestion"),
        code("""manifest_df = run_bronze_ingestion(
    seasons=INGEST_SEASONS,
    endpoints=ENDPOINTS,
    bronze_dir=get_bronze_dir(),
    manifests_dir=get_manifests_dir(),
    max_sessions=MAX_SESSIONS,
)

manifest_summary = summarize_manifest(manifest_df)
manifest_summary"""),
        md("## Endpoint status summary"),
        code("""print("=== Manifest status counts ===")
print(manifest_summary["status_counts"])
print("\\n=== Success endpoints ===")
print(manifest_summary["success_endpoints"])
print("\\n=== Failed endpoints (continued) ===")
print(manifest_summary["failed_endpoints"])
print("\\n=== Row counts by endpoint ===")
for ep, n in sorted(manifest_summary["row_counts_by_endpoint"].items()):
    print(f"  {ep}: {n}")
print(f"\\nsession_result rows: {manifest_summary['session_result_total_rows']}")
print(f"starting_grid rows: {manifest_summary['starting_grid_total_rows']}")
if manifest_summary["session_result_total_rows"] == 0 and not SMOKE_TEST:
    print("WARNING: session_result has zero rows — check manifest before Silver/Gold.")
manifest_df.groupby(["endpoint", "status"]).size().unstack(fill_value=0)"""),
        md("## Generate Bronze evidence reports (Spark-first)"),
        code("""report_result = generate_bronze_reports(
    bronze_dir=get_bronze_dir(),
    data_quality_reports_dir=get_data_quality_reports_dir(),
    schemas_dir=get_schemas_dir(),
    engine=BRONZE_REPORT_ENGINE,
    spark=spark,
    allow_fallback=ALLOW_FALLBACK,
)
report_result"""),
        md("## DuckDB validation (Bronze CSV evidence)"),
        code("""from openf1_pipeline.analytics.duckdb_validation import (
    save_duckdb_validation_reports,
    validate_bronze_with_duckdb,
)

bronze_duckdb = validate_bronze_with_duckdb(get_data_quality_reports_dir())
duckdb_bronze_paths = save_duckdb_validation_reports(
    bronze_duckdb, get_data_quality_reports_dir(), prefix="bronze"
)
duckdb_bronze_paths"""),
        md("## Inspect outputs"),
        code("""import pandas as pd

row_counts = pd.read_csv(report_result["paths"]["bronze_row_counts"])
schema_report = pd.read_csv(report_result["paths"]["bronze_schema_report"])
schema_drift = pd.read_csv(report_result["paths"]["bronze_schema_drift"])

print("Row counts by endpoint:")
display(row_counts.groupby("endpoint")["row_count"].sum().reset_index())
display(schema_report.head(10))
drift_flags = schema_drift[schema_drift["possible_schema_drift_flag"] == True]
print(f"Schema drift flags: {len(drift_flags)}")
if len(drift_flags):
    display(drift_flags.head(15))"""),
    ]
    write_nb("01_ingestion_bronze.ipynb", cells)


def build_02() -> None:
    cells = [
        md("""# 02 — Silver Cleaning & Data Quality

Bronze JSONL → **Spark** Silver Parquet + DQ reports + **DuckDB** validation. **No Gold. No modeling.**

- Primary engine: **PySpark** (`SILVER_ENGINE = "spark"`). Spark is the official engine; pandas fallback is available only manually (`allow_fallback=True`) and must not be triggered silently.
- Event absence ≠ missing data (e.g. no pit rows → handled in Gold).
- Outliers flagged; only domain-invalid values removed.
- Run after Bronze with the **same** `USE_GOOGLE_DRIVE` setting as notebook 01."""),
        md("""## Colab setup (required every session)

Identical to `00` and `01`: clone, `pip install -e .`, Drive mount, then import `openf1_pipeline`."""),
        code(COLAB_SETUP),
        md("## Start Spark"),
        code("""from openf1_pipeline.utils.spark import get_spark

spark = get_spark()
print("Spark version:", spark.version)"""),
        md("## Bronze prerequisites"),
        code("""from pathlib import Path

from openf1_pipeline.config import (
    get_bronze_dir,
    get_data_quality_reports_dir,
    get_manifests_dir,
    get_output_root,
    get_silver_dir,
)
from openf1_pipeline.silver.build_silver import run_silver_cleaning
from openf1_pipeline.utils.cleanup import clean_silver_layer_outputs

SILVER_ENGINE = "spark"  # manual fallback only: "pandas" with allow_fallback=True
ALLOW_FALLBACK = False
CLEAR_SILVER_OUTPUTS = True

BRONZE_DIR = get_bronze_dir()
SILVER_DIR = get_silver_dir()
DATA_QUALITY_REPORTS_DIR = get_data_quality_reports_dir()
MANIFESTS_DIR = get_manifests_dir()

print("OUTPUT_ROOT:", get_output_root())
print("BRONZE_DIR:", BRONZE_DIR)
print("SILVER_DIR:", SILVER_DIR)
print("DATA_QUALITY_REPORTS_DIR:", DATA_QUALITY_REPORTS_DIR)

jsonl_files = list(BRONZE_DIR.rglob("*.jsonl")) if BRONZE_DIR.is_dir() else []
manifest_path = MANIFESTS_DIR / "ingestion_manifest.csv"
row_counts_path = DATA_QUALITY_REPORTS_DIR / "bronze_row_counts.csv"

print(f"Bronze JSONL files found: {len(jsonl_files)}")
print("ingestion_manifest.csv:", manifest_path.exists(), manifest_path)
print("bronze_row_counts.csv:", row_counts_path.exists(), row_counts_path)

if not jsonl_files:
    raise FileNotFoundError(
        f"Bronze data not found at {BRONZE_DIR}. "
        "Run 01_ingestion_bronze.ipynb first with the same USE_GOOGLE_DRIVE setting."
    )"""),
        md("""## Clean Silver outputs (required before fresh Spark run)

Removes prior Spark Parquet **directories**, Silver DQ CSVs, and DuckDB Silver reports.
Does not delete Bronze data."""),
        code("""if CLEAR_SILVER_OUTPUTS:
    print("Cleaning Silver layer outputs...")
    clean_silver_layer_outputs(silver_dir=SILVER_DIR, data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR)
else:
    print("Skipping Silver cleanup (CLEAR_SILVER_OUTPUTS=False).")"""),
        md("## Run Silver cleaning (Spark-first)"),
        code("""outputs = run_silver_cleaning(
    bronze_dir=BRONZE_DIR,
    silver_dir=SILVER_DIR,
    data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR,
    engine=SILVER_ENGINE,
    spark=spark,
    allow_fallback=ALLOW_FALLBACK,
)
outputs["summary"]"""),
        md("## DuckDB validation (Silver Parquet)"),
        code("""from openf1_pipeline.analytics.duckdb_validation import (
    save_duckdb_validation_reports,
    validate_silver_with_duckdb,
)

silver_duckdb = validate_silver_with_duckdb(SILVER_DIR)
duckdb_silver_paths = save_duckdb_validation_reports(
    silver_duckdb, DATA_QUALITY_REPORTS_DIR, prefix="silver"
)
duckdb_silver_paths"""),
        md("## Inspect cleaning impact"),
        code("""import pandas as pd

impact = pd.read_csv(outputs["paths"]["silver_cleaning_impact_summary"])
inventory = pd.read_csv(outputs["paths"]["silver_table_inventory"])
rules = pd.read_csv(outputs["paths"]["silver_cleaning_rules"])

display(inventory)
display(impact[["table_name", "rows_before", "rows_after", "rows_removed", "row_removal_pct"]])
display(rules.head(20))"""),
        md("## Missingness before vs after"),
        code("""miss_before = pd.read_csv(outputs["paths"]["silver_missingness_before"])
miss_after = pd.read_csv(outputs["paths"]["silver_missingness_after"])

display(miss_before.sort_values("missing_pct", ascending=False).groupby("table_name").head(5))
display(miss_after.sort_values("missing_pct", ascending=False).groupby("table_name").head(5))"""),
        md("## Duplicates & referential integrity"),
        code("""dups = pd.read_csv(outputs["paths"]["silver_duplicate_report"])
ref = pd.read_csv(outputs["paths"]["silver_referential_integrity_report"])
rejected = pd.read_csv(outputs["paths"]["silver_rejected_records_summary"])

display(dups)
display(ref)
display(rejected)"""),
    ]
    write_nb("02_silver_cleaning_quality.ipynb", cells)


def build_03() -> None:
    cells = [
        md("""# 03 — Gold Feature Engineering

Build the **driver-race feature mart** from Silver Parquet using **PySpark** + **DuckDB** validation.

- **Engine:** `GOLD_ENGINE = "spark"`. Spark is the official engine; pandas fallback is manual only (`allow_fallback=True`).
- **Grain:** one row per `session_key`, `meeting_key`, `driver_number`
- **Base:** `session_result_clean.parquet`
- **Target:** `points_finish` = 1 if `points` > 0, else 0
- **No modeling** in this notebook — features and leakage guard only

Run after `02_silver_cleaning_quality.ipynb` with the same `USE_GOOGLE_DRIVE` setting."""),
        md("""## Connection to grading rubric

| Rubric area | This notebook |
|-------------|----------------|
| Gold layer | Feature mart at `data/gold/driver_race_feature_mart.parquet` |
| Target variable | `points_finish` from session points |
| Feature engineering | Lap, pit, position, weather, race control, metadata |
| Data quality | Gold DQ reports under `reports/data_quality/` |
| Leakage prevention | `gold_leakage_guard_report.csv` + feature dictionary |"""),
        md("""## Colab setup (required every session)

Identical to `00`–`02`: clone, `pip install -e .`, Drive mount, set `OPENF1_DATA_ROOT` **before** importing config."""),
        code(COLAB_SETUP),
        md("## Start Spark"),
        code("""from openf1_pipeline.utils.spark import get_spark

spark = get_spark()
print("Spark version:", spark.version)"""),
        md("## Silver prerequisites"),
        code("""from pathlib import Path

import pandas as pd

from openf1_pipeline.config import (
    get_data_quality_reports_dir,
    get_feature_definitions_dir,
    get_gold_dir,
    get_output_root,
    get_silver_dir,
)
from openf1_pipeline.gold.build_feature_mart import build_gold_feature_mart
from openf1_pipeline.utils.io import read_parquet_if_exists
from openf1_pipeline.utils.cleanup import clean_gold_layer_outputs

GOLD_ENGINE = "spark"
ALLOW_FALLBACK = False
CLEAR_GOLD_OUTPUTS = True

SILVER_DIR = get_silver_dir()
GOLD_DIR = get_gold_dir()
DATA_QUALITY_REPORTS_DIR = get_data_quality_reports_dir()
FEATURE_DEFINITIONS_DIR = get_feature_definitions_dir()

print("OUTPUT_ROOT:", get_output_root())
print("SILVER_DIR:", SILVER_DIR)
print("GOLD_DIR:", GOLD_DIR)

required = [
    SILVER_DIR / "session_result_clean.parquet",
    SILVER_DIR / "laps_clean.parquet",
    SILVER_DIR / "drivers_clean.parquet",
]
for path in required:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing Silver table: {path}. Run 02_silver_cleaning_quality.ipynb first."
        )

starting_grid = SILVER_DIR / "starting_grid_clean.parquet"
if starting_grid.exists():
    sg = read_parquet_if_exists(starting_grid)
    if sg is not None and sg.empty:
        print("WARNING: starting_grid_clean.parquet is empty — grid features skipped.")
else:
    print("NOTE: starting_grid_clean.parquet not found (optional).")"""),
        md("""## Clean Gold outputs (required before fresh Spark run)

Removes Gold Parquet, Gold DQ reports, DuckDB Gold reports, and feature dictionary.
Does not delete Silver."""),
        code("""if CLEAR_GOLD_OUTPUTS:
    print("Cleaning Gold layer outputs...")
    clean_gold_layer_outputs(
        gold_dir=GOLD_DIR,
        data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR,
        feature_definitions_dir=FEATURE_DEFINITIONS_DIR,
    )
else:
    print("Skipping Gold cleanup (CLEAR_GOLD_OUTPUTS=False).")"""),
        md("## Build Gold feature mart (Spark-first)"),
        code("""outputs = build_gold_feature_mart(
    silver_dir=SILVER_DIR,
    gold_dir=GOLD_DIR,
    data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR,
    feature_definitions_dir=FEATURE_DEFINITIONS_DIR,
    engine=GOLD_ENGINE,
    spark=spark,
    allow_fallback=ALLOW_FALLBACK,
)

outputs["summary"]"""),
        md("## DuckDB validation (Gold Parquet)"),
        code("""from openf1_pipeline.analytics.duckdb_validation import (
    save_duckdb_validation_reports,
    validate_gold_with_duckdb,
)

gold_duckdb = validate_gold_with_duckdb(outputs["paths"]["driver_race_feature_mart"])
duckdb_gold_paths = save_duckdb_validation_reports(
    gold_duckdb, DATA_QUALITY_REPORTS_DIR, prefix="gold"
)
duckdb_gold_paths"""),
        md("## Validate target distribution"),
        code("""target_dist = pd.read_csv(outputs["paths"]["gold_target_distribution"])
display(target_dist)"""),
        md("## Validate feature missingness (top 20)"),
        code("""missingness = pd.read_csv(outputs["paths"]["gold_feature_missingness"])
display(
    missingness.sort_values("missing_pct", ascending=False).head(20)
)"""),
        md("## Validate join quality"),
        code("""join_quality = pd.read_csv(outputs["paths"]["gold_join_quality_report"])
display(join_quality)"""),
        md("## Leakage guard"),
        code("""leakage = pd.read_csv(outputs["paths"]["gold_leakage_guard_report"])
forbidden = leakage[leakage["allowed_for_modeling"] == False]
print(f"Columns blocked from modeling: {len(forbidden)}")
display(leakage[leakage["leakage_risk"].isin(["high", "target"])])
print(f"Model-safe feature count: {len(outputs['model_feature_columns'])}")"""),
        md("## Feature dictionary"),
        code("""feature_dict = pd.read_csv(outputs["paths"]["feature_dictionary"])
display(feature_dict.head(20))
print("Predictive features:")
display(
    feature_dict[feature_dict["modeling_role"] == "predictive_feature"][
        ["feature_name", "feature_group", "missing_pct"]
    ].head(25)
)"""),
        md("## Inspect Gold output"),
        code("""gold_df = pd.read_parquet(outputs["paths"]["driver_race_feature_mart"])
print("Shape:", gold_df.shape)
print("Grain keys:", ["session_key", "meeting_key", "driver_number"])
display(gold_df.head(10))
print("Target rate (points_finish=1):", gold_df["points_finish"].mean())"""),
        md("""## Checklist / next steps

- [ ] Copy Gold parquet + DQ CSVs to `evidence/<run_id>/` after smoke/full Colab run
- [ ] Confirm `gold_leakage_guard_report.csv` has no forbidden columns with `allowed_for_modeling=True`
- [ ] Proceed to **04 — modeling**
- [ ] Do **not** use `final_position`, `result_*`, or raw `position`/`points` as model inputs"""),
    ]
    write_nb("03_gold_feature_engineering.ipynb", cells)


def build_04() -> None:
    cells = [
        md("""# 04 — Modeling & Evaluation

Season-based splits, baselines, supervised models, and model result artifacts.

- **Target:** `points_finish`
- **Models:** Random baseline, heuristic (`first_observed_position` ≤ 10), Logistic Regression, Random Forest, LightGBM
- **Split:** Train 2023 · Validation 2024 · Test 2025 (season-based, no random row split)
- **Engine:** pandas + scikit-learn + LightGBM (Gold mart handoff)

Run after `03_gold_feature_engineering.ipynb` with the same `USE_GOOGLE_DRIVE` setting."""),
        md("""## Connection to grading rubric

| Rubric area | This notebook |
|-------------|----------------|
| Experimental Results & Analysis | Baselines, model metrics, confusion matrix, error analysis |
| Feature Engineering | Uses Gold mart + feature dictionary with leakage guard |
| Data Quality | Error analysis by team/circuit/season |"""),
        md("""## Colab setup (required every session)

Identical to `00`–`03`: clone, `pip install -e .`, Drive mount, set `OPENF1_DATA_ROOT` **before** importing config."""),
        code(COLAB_SETUP),
        md("## Configuration"),
        code("""MODELING_MODE = "smoke"  # "full" for official MBA season splits
CLEAR_MODEL_OUTPUTS = True

from pathlib import Path

import pandas as pd

from openf1_pipeline.config import (
    RANDOM_SEED,
    get_data_quality_reports_dir,
    get_feature_definitions_dir,
    get_gold_dir,
    get_manifests_dir,
    get_model_results_dir,
    get_output_root,
)
from openf1_pipeline.features.feature_dictionary import validate_no_leakage
from openf1_pipeline.gold.build_feature_mart import GOLD_MART_FILENAME, TARGET_COLUMN
from openf1_pipeline.modeling.baselines import (
    heuristic_position_baseline,
    random_baseline_predictions,
)
from openf1_pipeline.modeling.evaluate import (
    build_error_analysis,
    compute_classification_metrics,
    compute_confusion_matrix_table,
    save_modeling_outputs,
)
from openf1_pipeline.modeling.splits import resolve_modeling_splits
from openf1_pipeline.modeling.train import (
    build_lightgbm_pipeline,
    build_logistic_regression_pipeline,
    build_random_forest_pipeline,
    extract_feature_importance,
    get_model_feature_columns,
    prepare_model_matrix,
    train_models,
)
from openf1_pipeline.utils.cleanup import clean_model_outputs
from openf1_pipeline.utils.io import read_parquet_if_exists

GOLD_DIR = get_gold_dir()
FEATURE_DEFINITIONS_DIR = get_feature_definitions_dir()
MODEL_RESULTS_DIR = get_model_results_dir()
MANIFESTS_DIR = get_manifests_dir()
DATA_QUALITY_REPORTS_DIR = get_data_quality_reports_dir()

MART_PATH = GOLD_DIR / GOLD_MART_FILENAME
DICT_PATH = FEATURE_DEFINITIONS_DIR / "feature_dictionary.csv"
LEAKAGE_PATH = DATA_QUALITY_REPORTS_DIR / "gold_leakage_guard_report.csv"

print("MODELING_MODE:", MODELING_MODE)
print("CLEAR_MODEL_OUTPUTS:", CLEAR_MODEL_OUTPUTS)
print("MART_PATH:", MART_PATH)
print("DICT_PATH:", DICT_PATH)"""),
        md("## Clean model outputs"),
        code("""if CLEAR_MODEL_OUTPUTS:
    print("Cleaning model results...")
    clean_model_outputs(model_results_dir=MODEL_RESULTS_DIR, manifests_dir=MANIFESTS_DIR)
else:
    print("Skipping model cleanup (CLEAR_MODEL_OUTPUTS=False).")"""),
        md("## Load Gold mart and feature dictionary"),
        code("""if not MART_PATH.exists():
    raise FileNotFoundError(
        f"Gold mart missing: {MART_PATH}. Run 03_gold_feature_engineering.ipynb first."
    )
if not DICT_PATH.is_file():
    raise FileNotFoundError(
        f"Feature dictionary missing: {DICT_PATH}. Run notebook 03 first."
    )

gold_df = read_parquet_if_exists(MART_PATH)
if gold_df is None or gold_df.empty:
    raise ValueError(f"Gold mart at {MART_PATH} is missing or empty.")

feature_dict = pd.read_csv(DICT_PATH)
leakage_report = pd.read_csv(LEAKAGE_PATH) if LEAKAGE_PATH.is_file() else None

print("Gold shape:", gold_df.shape)
print("Target rate:", gold_df[TARGET_COLUMN].mean())
validate_no_leakage(gold_df, feature_dict)
print("Leakage guard: OK")
if leakage_report is not None:
    blocked = leakage_report.loc[leakage_report["allowed_for_modeling"] == False]
    print(f"Leakage report: {len(blocked)} columns blocked from modeling")

feature_columns = get_model_feature_columns(feature_dict)
print(f"Selected model features ({len(feature_columns)}):")
print(feature_columns)"""),
        md("## Resolve train/validation/test splits"),
        code("""splits, split_meta = resolve_modeling_splits(gold_df, mode=MODELING_MODE)
train_df = splits["train"]
val_df = splits["validation"]
test_df = splits["test"]

print("Split metadata:", split_meta)
for name, part in splits.items():
    if part.empty:
        print(f"WARNING: split '{name}' is empty")
    else:
        rate = part[TARGET_COLUMN].mean()
        seasons = sorted(part["session_year"].dropna().unique().tolist()) if "session_year" in part.columns else []
        print(f"{name}: n={len(part)}, points_finish rate={rate:.4f}, seasons={seasons}")

if MODELING_MODE == "smoke":
    print("SMOKE MODE: wiring verification only — metrics are NOT official MBA evidence.")"""),
        md("## Baselines"),
        code("""X_train, y_train = prepare_model_matrix(train_df, feature_columns)
X_val, y_val = prepare_model_matrix(val_df, feature_columns)
X_test, y_test = prepare_model_matrix(test_df, feature_columns)

baseline_rows = []
cm_parts = []
error_parts = []

for split_name, eval_df, y_true in [
    ("validation", val_df, y_val),
    ("test", test_df, y_test),
]:
    if len(eval_df) == 0:
        continue
    rand_pred, rand_proba = random_baseline_predictions(
        y_true, positive_rate=float(y_train.mean()) if len(y_train) else None
    )
    baseline_rows.append(
        compute_classification_metrics(y_true, rand_pred, rand_proba, "random_baseline", split_name)
    )
    cm_parts.append(compute_confusion_matrix_table(y_true, rand_pred, "random_baseline", split_name))
    error_parts.append(
        build_error_analysis(eval_df, y_true, rand_pred, rand_proba, "random_baseline", split_name)
    )

    heur_pred, heur_proba = heuristic_position_baseline(eval_df)
    baseline_rows.append(
        compute_classification_metrics(y_true, heur_pred, heur_proba, "heuristic_position", split_name)
    )
    cm_parts.append(compute_confusion_matrix_table(y_true, heur_pred, "heuristic_position", split_name))
    error_parts.append(
        build_error_analysis(eval_df, y_true, heur_pred, heur_proba, "heuristic_position", split_name)
    )

baseline_metrics = pd.DataFrame(baseline_rows)
display(baseline_metrics)"""),
        md("## Train supervised models"),
        code("""model_specs = {
    "logistic_regression": build_logistic_regression_pipeline(feature_columns, X_train),
    "random_forest": build_random_forest_pipeline(feature_columns, X_train),
    "lightgbm": build_lightgbm_pipeline(feature_columns, X_train),
}
fitted_models = train_models(X_train, y_train, model_specs)
list(fitted_models.keys())"""),
        md("## Validation and test evaluation"),
        code("""val_rows = []
test_rows = []
importance_parts = []

for model_name, pipeline in fitted_models.items():
    if len(X_val) > 0:
        val_pred = pipeline.predict(X_val)
        val_proba = pipeline.predict_proba(X_val)[:, 1] if hasattr(pipeline, "predict_proba") else None
        val_rows.append(compute_classification_metrics(y_val, val_pred, val_proba, model_name, "validation"))
        cm_parts.append(compute_confusion_matrix_table(y_val, val_pred, model_name, "validation"))
        error_parts.append(build_error_analysis(val_df, y_val, val_pred, val_proba, model_name, "validation"))
    if len(X_test) > 0:
        test_pred = pipeline.predict(X_test)
        test_proba = pipeline.predict_proba(X_test)[:, 1] if hasattr(pipeline, "predict_proba") else None
        test_rows.append(compute_classification_metrics(y_test, test_pred, test_proba, model_name, "test"))
        cm_parts.append(compute_confusion_matrix_table(y_test, test_pred, model_name, "test"))
        error_parts.append(build_error_analysis(test_df, y_test, test_pred, test_proba, model_name, "test"))
    importance_parts.append(extract_feature_importance(model_name, pipeline, feature_columns))

validation_metrics = pd.DataFrame(val_rows)
test_metrics = pd.DataFrame(test_rows)
confusion_matrix_df = pd.concat(cm_parts, ignore_index=True) if cm_parts else pd.DataFrame()
error_analysis_df = pd.concat(error_parts, ignore_index=True) if error_parts else pd.DataFrame()
feature_importance_df = pd.concat(importance_parts, ignore_index=True) if importance_parts else pd.DataFrame()

display(validation_metrics)
display(test_metrics)"""),
        md("## Save modeling outputs"),
        code("""output_paths = save_modeling_outputs(
    baseline_metrics=baseline_metrics,
    validation_metrics=validation_metrics,
    test_metrics=test_metrics,
    confusion_matrix_df=confusion_matrix_df,
    error_analysis_df=error_analysis_df,
    feature_importance_df=feature_importance_df,
    model_results_dir=MODEL_RESULTS_DIR,
    manifests_dir=MANIFESTS_DIR,
    manifest_extra={
        "modeling_mode": MODELING_MODE,
        "split_method": split_meta.get("split_method"),
        "evidence_tier": split_meta.get("evidence_tier"),
        "random_seed": RANDOM_SEED,
        "target": TARGET_COLUMN,
        "feature_count": len(feature_columns),
        "models": list(fitted_models.keys()),
    },
)
output_paths"""),
        md("""## Next steps

- [ ] Review `reports/model_results/` CSVs on Drive
- [ ] For official MBA evidence: set `MODELING_MODE = "full"` after 2023–2025 Gold run
- [ ] Proceed to **05 — report artifacts** to build tables and figures"""),
    ]
    write_nb("04_modeling_evaluation.ipynb", cells)


def build_05() -> None:
    cells = [
        md("""# 05 — Report Artifacts

Synthesize pipeline, data quality, and modeling outputs into MBA report-ready tables and figures.

- Loads existing CSV/JSON artifacts only — **no fake data**
- Uses DuckDB where helpful; matplotlib for figures (no seaborn)
- Writes `reports/tables/`, `reports/figures/`, and `artifacts/manifests/run_manifest.json`

Run after notebooks `01`–`04` on the same `OPENF1_DATA_ROOT`."""),
        md("""## Colab setup (required every session)"""),
        code(COLAB_SETUP),
        md("## Load paths"),
        code("""from pathlib import Path

import pandas as pd

from openf1_pipeline.config import (
    get_artifacts_dir,
    get_data_quality_reports_dir,
    get_gold_dir,
    get_manifests_dir,
    get_model_results_dir,
    get_output_root,
    get_reports_dir,
)
from openf1_pipeline.reporting.report_tables import (
    build_bronze_endpoint_row_counts,
    build_confusion_matrix_table,
    build_data_volume_by_layer,
    build_error_analysis_summary_table,
    build_gold_feature_group_summary,
    build_gold_target_distribution_table,
    build_model_baseline_comparison_table,
    build_model_validation_test_metrics_table,
    build_reproducibility_artifacts_table,
    build_silver_cleaning_impact_table,
    build_silver_error_taxonomy_table,
    write_report_tables,
)
from openf1_pipeline.utils.cleanup import clean_report_artifacts

CLEAR_REPORT_ARTIFACTS = True

OUTPUT_ROOT = get_output_root()
DQ_DIR = get_data_quality_reports_dir()
MODEL_DIR = get_model_results_dir()
TABLES_DIR = get_reports_dir() / "tables"
FIGURES_DIR = get_reports_dir() / "figures"
MANIFESTS_DIR = get_manifests_dir()
ARTIFACTS_DIR = get_artifacts_dir()
GOLD_DIR = get_gold_dir()

print("OUTPUT_ROOT:", OUTPUT_ROOT)
print("CLEAR_REPORT_ARTIFACTS:", CLEAR_REPORT_ARTIFACTS)
print("TABLES_DIR:", TABLES_DIR)
print("FIGURES_DIR:", FIGURES_DIR)"""),
        md("## Clean report artifacts"),
        code("""if CLEAR_REPORT_ARTIFACTS:
    print("Cleaning report tables and figures...")
    clean_report_artifacts(reports_dir=get_reports_dir())
else:
    print("Skipping report cleanup (CLEAR_REPORT_ARTIFACTS=False).")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)"""),
        md("## Build report tables"),
        code("""gold_row_count = None
gold_mart = GOLD_DIR / "driver_race_feature_mart.parquet"
if gold_mart.exists():
    try:
        gold_row_count = len(pd.read_parquet(gold_mart, columns=["session_key"]))
    except Exception as exc:
        print("WARNING: could not read Gold mart row count:", exc)

tables = {
    "data_volume_by_layer": build_data_volume_by_layer(
        DQ_DIR / "bronze_row_counts.csv",
        DQ_DIR / "silver_table_inventory.csv",
        gold_row_count=gold_row_count,
    ),
    "bronze_endpoint_row_counts": build_bronze_endpoint_row_counts(DQ_DIR / "bronze_row_counts.csv"),
    "silver_cleaning_impact_table": build_silver_cleaning_impact_table(
        DQ_DIR / "silver_cleaning_impact_summary.csv"
    ),
    "silver_error_taxonomy_table": build_silver_error_taxonomy_table(
        DQ_DIR / "silver_duplicate_report.csv",
        DQ_DIR / "silver_outlier_report.csv",
        DQ_DIR / "silver_temporal_anomaly_report.csv",
        DQ_DIR / "silver_referential_integrity_report.csv",
    ),
    "gold_feature_group_summary": build_gold_feature_group_summary(
        ARTIFACTS_DIR / "feature_definitions" / "feature_dictionary.csv"
    ),
    "gold_target_distribution_table": build_gold_target_distribution_table(
        DQ_DIR / "gold_target_distribution.csv"
    ),
    "model_baseline_comparison_table": build_model_baseline_comparison_table(
        MODEL_DIR / "baseline_metrics.csv"
    ),
    "model_validation_test_metrics_table": build_model_validation_test_metrics_table(
        MODEL_DIR / "validation_metrics.csv",
        MODEL_DIR / "test_metrics.csv",
    ),
    "confusion_matrix_table": build_confusion_matrix_table(MODEL_DIR / "confusion_matrix.csv"),
    "error_analysis_summary_table": build_error_analysis_summary_table(
        MODEL_DIR / "error_analysis.csv"
    ),
    "reproducibility_artifacts_table": build_reproducibility_artifacts_table(
        MANIFESTS_DIR, ARTIFACTS_DIR, get_reports_dir()
    ),
}

table_paths = write_report_tables(tables, TABLES_DIR)
table_paths"""),
        md("## Architecture diagram placeholder"),
        code("""arch_md = FIGURES_DIR / "architecture_diagram_placeholder.md"
arch_md.write_text(
    '''# Architecture diagram placeholder\\n\\n'
    'Insert Medallion pipeline diagram for MBA report:\\n\\n'
    'OpenF1 API → Bronze JSONL → PySpark Bronze reports → DuckDB validation\\n'
    '→ PySpark Silver → DuckDB → PySpark Gold → DuckDB → sklearn/LightGBM modeling\\n',
    encoding="utf-8",
)
print("Wrote", arch_md)"""),
        md("## Figures (matplotlib only)"),
        code("""import matplotlib.pyplot as plt

# Target distribution
target_path = DQ_DIR / "gold_target_distribution.csv"
if target_path.is_file():
    td = pd.read_csv(target_path)
    if not td.empty and "points_finish" in td.columns:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(td["points_finish"].astype(str), td["count"])
        ax.set_xlabel("points_finish")
        ax.set_ylabel("count")
        ax.set_title("Gold target distribution")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "target_distribution.png", dpi=120)
        plt.close(fig)
        print("Wrote target_distribution.png")
else:
    print("WARNING: gold_target_distribution.csv missing — skipped figure")

# Model metric comparison
metrics_path = MODEL_DIR / "validation_metrics.csv"
if metrics_path.is_file():
    metrics = pd.read_csv(metrics_path)
    if not metrics.empty and "f1" in metrics.columns:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(metrics["model_name"], metrics["f1"])
        ax.set_ylabel("F1 (validation)")
        ax.set_title("Model metric comparison")
        plt.xticks(rotation=30, ha="right")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "model_metric_comparison.png", dpi=120)
        plt.close(fig)
        print("Wrote model_metric_comparison.png")
else:
    print("WARNING: validation_metrics.csv missing — skipped figure")

# Confusion matrix (first model in file)
cm_path = MODEL_DIR / "confusion_matrix.csv"
if cm_path.is_file():
    cm = pd.read_csv(cm_path)
    if not cm.empty:
        first_model = cm["model_name"].iloc[0]
        sub = cm[(cm["model_name"] == first_model) & (cm["split"] == cm["split"].iloc[0])]
        matrix = sub.pivot(index="actual", columns="predicted", values="count").fillna(0)
        fig, ax = plt.subplots(figsize=(5, 4))
        im = ax.imshow(matrix.values)
        ax.set_xticks(range(len(matrix.columns)))
        ax.set_yticks(range(len(matrix.index)))
        ax.set_xticklabels(matrix.columns)
        ax.set_yticklabels(matrix.index)
        ax.set_xlabel("predicted")
        ax.set_ylabel("actual")
        ax.set_title(f"Confusion matrix: {first_model}")
        fig.colorbar(im, ax=ax)
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "confusion_matrix.png", dpi=120)
        plt.close(fig)
        print("Wrote confusion_matrix.png")
else:
    print("WARNING: confusion_matrix.csv missing — skipped figure")

# Feature importance top 20
fi_path = MODEL_DIR / "feature_importance.csv"
if fi_path.is_file():
    fi = pd.read_csv(fi_path)
    if not fi.empty and "importance" in fi.columns:
        top = fi.sort_values("importance", ascending=False).head(20)
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.barh(top["feature_name"], top["importance"])
        ax.invert_yaxis()
        ax.set_title("Top 20 feature importances")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "feature_importance_top20.png", dpi=120)
        plt.close(fig)
        print("Wrote feature_importance_top20.png")
else:
    print("WARNING: feature_importance.csv missing — skipped figure")

# Missingness before/after (top columns)
miss_before = DQ_DIR / "silver_missingness_before.csv"
miss_after = DQ_DIR / "silver_missingness_after.csv"
if miss_before.is_file() and miss_after.is_file():
    mb = pd.read_csv(miss_before)
    ma = pd.read_csv(miss_after)
    if not mb.empty and not ma.empty:
        top_cols = mb.sort_values("missing_pct", ascending=False).head(10)
        merged = top_cols.merge(
            ma[["table_name", "column_name", "missing_pct"]],
            on=["table_name", "column_name"],
            suffixes=("_before", "_after"),
        )
        labels = merged["table_name"] + "." + merged["column_name"]
        x = range(len(labels))
        width = 0.35
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar([i - width/2 for i in x], merged["missing_pct_before"], width, label="before")
        ax.bar([i + width/2 for i in x], merged["missing_pct_after"], width, label="after")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylabel("missing_pct")
        ax.set_title("Silver missingness before vs after (top 10)")
        ax.legend()
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "missingness_before_after.png", dpi=120)
        plt.close(fig)
        print("Wrote missingness_before_after.png")
else:
    print("WARNING: silver missingness reports missing — skipped figure")"""),
        md("## Write run manifest"),
        code("""import json
from datetime import datetime, timezone

run_manifest = {
    "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "output_root": str(OUTPUT_ROOT),
    "report_tables": {k: str(v) for k, v in table_paths.items()},
    "figures_dir": str(FIGURES_DIR),
    "note": "Paths only — populate row counts from Colab execution artifacts.",
}

model_manifest_path = MANIFESTS_DIR / "model_run_manifest.json"
if model_manifest_path.is_file():
    run_manifest["model_run_manifest"] = str(model_manifest_path)

manifest_out = MANIFESTS_DIR / "run_manifest.json"
manifest_out.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
print("Wrote", manifest_out)"""),
    ]
    write_nb("05_report_artifacts.ipynb", cells)


if __name__ == "__main__":
    build_00()
    build_01()
    build_02()
    build_03()
    build_04()
    build_05()
