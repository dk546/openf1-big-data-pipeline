"""Rebuild 00/01/02 Colab notebooks with standardized setup (no saved outputs)."""

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

print(f"SMOKE_TEST={SMOKE_TEST}, seasons={INGEST_SEASONS}, max_sessions={MAX_SESSIONS}")
if not SMOKE_TEST:
    print("WARNING: Full ingestion may take a long time. Ensure USE_GOOGLE_DRIVE=True above.")"""),
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
        md("## Generate Bronze evidence reports"),
        code("""report_result = generate_bronze_reports(
    bronze_dir=get_bronze_dir(),
    data_quality_reports_dir=get_data_quality_reports_dir(),
    schemas_dir=get_schemas_dir(),
)
report_result"""),
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

Bronze JSONL → audited Silver Parquet + DQ reports. **No Gold target. No modeling.**

- Event absence ≠ missing data (e.g. no pit rows → handled in Gold).
- Outliers flagged; only domain-invalid values removed.
- Run after Bronze with the **same** `USE_GOOGLE_DRIVE` setting as notebook 01."""),
        md("""## Colab setup (required every session)

Identical to `00` and `01`: clone, `pip install -e .`, Drive mount, then import `openf1_pipeline`."""),
        code(COLAB_SETUP),
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
        md("## Run Silver cleaning"),
        code("""outputs = run_silver_cleaning(
    bronze_dir=BRONZE_DIR,
    silver_dir=SILVER_DIR,
    data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR,
)
outputs["summary"]"""),
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


if __name__ == "__main__":
    build_00()
    build_01()
    build_02()
