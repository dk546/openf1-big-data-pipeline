"""Generate notebooks/01_ingestion_bronze.ipynb.

DEPRECATED: use scripts/rebuild_colab_notebooks.py for the standardized Colab setup pattern.
"""

import json
from pathlib import Path

cells = []


def md(text: str):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": [l + "\n" for l in text.split("\n")]})


def code(text: str):
    cells.append({
        "cell_type": "code",
        "metadata": {},
        "source": [l + "\n" for l in text.split("\n")],
        "outputs": [],
        "execution_count": None,
    })


md("""# 01 — Ingestion & Bronze Layer

## Purpose

Download OpenF1 data for configured seasons, save **raw Bronze JSONL** with minimal transformation, and generate **ingestion evidence** (manifest, row counts, schema reports).

## Connection to grading rubric

| Pillar | This notebook |
|--------|----------------|
| Pipeline Architecture | Medallion Bronze landing, manifests, Colab-first paths |
| Data Quality & Cleaning | Bronze row counts, schema report, schema drift flags |

## Bronze endpoints (expanded)

| Endpoint | Role |
|----------|------|
| `session_result` | **Required** for Gold target `points_finish` (final classification position) |
| `starting_grid` | **Optional** — supports grid-position heuristic baseline (grid ≤ 10); may be empty or 404 for some sessions |
| Other session endpoints | Laps, pit, weather, position, race_control, drivers |
| `meetings`, `sessions` | Global calendar / session discovery |

Run **SMOKE_TEST** first, then set `SMOKE_TEST = False` for the full 2023–2025 Colab evidence run.

> **Official evidence:** The local developer smoke test is **not** the official project evidence. The MBA report must use artifacts generated from **Google Colab Pro Plus** execution of this notebook.""")

md("""## Setup

Run `00_colab_setup.ipynb` first, or ensure the repo is cloned and `src/` is on `PYTHONPATH`.

Set `DATA_ROOT` on Google Drive for large Bronze files.""")

code("""import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd()
if not (PROJECT_ROOT / "project_context.md").exists():
    PROJECT_ROOT = Path("/content/openf1-big-data-pipeline")

SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from openf1_pipeline.config import (
    SEASONS,
    ENDPOINTS,
    ensure_project_directories,
    get_bronze_dir,
    get_manifests_dir,
    get_data_quality_reports_dir,
    get_schemas_dir,
)
from openf1_pipeline.ingestion.ingest import run_bronze_ingestion, summarize_manifest
from openf1_pipeline.bronze.build_bronze import generate_bronze_reports
from openf1_pipeline.quality.schema_checks import detect_schema_drift

paths = ensure_project_directories()
print("Project root:", PROJECT_ROOT)
print("Bronze dir:", get_bronze_dir())""")

code("""# Step 1: SMOKE_TEST = True (few sessions). Step 2: SMOKE_TEST = False (full evidence).
SMOKE_TEST = True
MAX_SESSIONS = 2 if SMOKE_TEST else None
INGEST_SEASONS = [2024] if SMOKE_TEST else SEASONS

print(f"SMOKE_TEST={SMOKE_TEST}, seasons={INGEST_SEASONS}, max_sessions={MAX_SESSIONS}")""")

md("## Run Bronze ingestion")

code("""manifest_df = run_bronze_ingestion(
    seasons=INGEST_SEASONS,
    endpoints=ENDPOINTS,
    bronze_dir=get_bronze_dir(),
    manifests_dir=get_manifests_dir(),
    max_sessions=MAX_SESSIONS,
)

manifest_summary = summarize_manifest(manifest_df)
manifest_summary""")

md("## Endpoint status summary")

code("""print("=== Manifest status counts ===")
print(manifest_summary["status_counts"])

print("\\n=== Endpoints with at least one success ===")
print(manifest_summary["success_endpoints"])

print("\\n=== Endpoints with failures (pipeline continued) ===")
print(manifest_summary["failed_endpoints"])

print("\\n=== Row counts by endpoint (manifest) ===")
for ep, n in sorted(manifest_summary["row_counts_by_endpoint"].items()):
    print(f"  {ep}: {n}")

print("\\n=== Target-support endpoints ===")
print(f"session_result total rows: {manifest_summary['session_result_total_rows']}")
print(f"starting_grid total rows: {manifest_summary['starting_grid_total_rows']}")

if manifest_summary["session_result_total_rows"] == 0 and not SMOKE_TEST:
    print("WARNING: session_result has zero rows — check manifest failures before Silver/Gold.")

manifest_df.groupby(["endpoint", "status"]).size().unstack(fill_value=0)""")

md("## Generate Bronze evidence reports")

code("""report_result = generate_bronze_reports(
    bronze_dir=get_bronze_dir(),
    data_quality_reports_dir=get_data_quality_reports_dir(),
    schemas_dir=get_schemas_dir(),
)

report_result""")

md("## Inspect outputs")

code("""row_counts = pd.read_csv(report_result["paths"]["bronze_row_counts"])
schema_report = pd.read_csv(report_result["paths"]["bronze_schema_report"])
schema_drift = pd.read_csv(report_result["paths"]["bronze_schema_drift"])

print("Row counts by endpoint (Bronze files):")
display(row_counts.groupby("endpoint")["row_count"].sum().reset_index())

print("\\nSchema report (head):")
display(schema_report.head(10))

drift_flags = schema_drift[schema_drift["possible_schema_drift_flag"] == True]
print(f"Schema drift flags: {len(drift_flags)}")
if len(drift_flags):
    display(drift_flags.head(15))""")

md("""## Checklist update notes

After a **successful Colab run**:

1. Confirm `artifacts/manifests/ingestion_manifest.csv` exists.
2. Confirm `session_result` has non-zero rows for full ingestion (or document gaps).
3. Note `starting_grid` row counts (may be zero for some sessions — not a failure if API returned success).
4. Commit Bronze/Silver evidence (manifests + DQ CSVs) after a successful Colab run.
5. Next: `02_silver_cleaning_quality.ipynb`.""")

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10.0"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).resolve().parents[1] / "notebooks" / "01_ingestion_bronze.ipynb"
out.write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("Wrote", out)
