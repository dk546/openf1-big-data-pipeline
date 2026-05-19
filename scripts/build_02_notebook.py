"""Generate notebooks/02_silver_cleaning_quality.ipynb."""

import json
from pathlib import Path

cells = []


def md(t):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": [l + "\n" for l in t.split("\n")]})


def code(t):
    cells.append({
        "cell_type": "code",
        "metadata": {},
        "source": [l + "\n" for l in t.split("\n")],
        "outputs": [],
        "execution_count": None,
    })


md("""# 02 — Silver Cleaning & Data Quality

## Purpose

Transform Bronze JSONL into **audited Silver Parquet** tables with documented cleaning rules and **before/after evidence** (counts and percentages, not vague labels).

## Connection to grading rubric

| Pillar | This notebook |
|--------|----------------|
| **Data Quality & Cleaning** | Missingness, duplicates, outliers, temporal checks, referential integrity, cleaning impact |
| **Pipeline Architecture** | Medallion Silver layer, reproducible reports |

## Design principles

- **Event absence ≠ missing data:** No pit rows means no pit stops occurred — `pit_stop_count = 0` is derived in **Gold**, not imputed in Silver.
- **Outliers are flagged, not deleted:** Slow laps under Safety Car are real; only **domain-invalid** values (e.g. `lap_number <= 0`) are removed.
- **No target in Silver:** `points_finish` is built in **Gold** from `session_result`.
- **Imputation for modeling** belongs in Gold/modeling with explicit rules — not blind Silver fills.

> **Official evidence:** Run this notebook in **Google Colab Pro Plus**. Local runs are for development only.""")

md("## Setup")

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
    ensure_project_directories,
    get_bronze_dir,
    get_silver_dir,
    get_data_quality_reports_dir,
)
from openf1_pipeline.silver.build_silver import run_silver_cleaning

paths = ensure_project_directories()
BRONZE_DIR = get_bronze_dir()
SILVER_DIR = get_silver_dir()
DATA_QUALITY_REPORTS_DIR = get_data_quality_reports_dir()

print("Bronze:", BRONZE_DIR)
print("Silver:", SILVER_DIR)""")

md("## Run Silver cleaning (audit → clean → reports)")

code("""outputs = run_silver_cleaning(
    bronze_dir=BRONZE_DIR,
    silver_dir=SILVER_DIR,
    data_quality_reports_dir=DATA_QUALITY_REPORTS_DIR,
)

outputs["summary"]""")

md("## Inspect cleaning impact")

code("""impact = pd.read_csv(outputs["paths"]["silver_cleaning_impact_summary"])
inventory = pd.read_csv(outputs["paths"]["silver_table_inventory"])
rules = pd.read_csv(outputs["paths"]["silver_cleaning_rules"])

print("=== Table inventory (Bronze load) ===")
display(inventory)

print("\\n=== Cleaning impact summary ===")
display(impact)

print("\\n=== Rows removed by table ===")
display(impact[["table_name", "rows_before", "rows_after", "rows_removed", "row_removal_pct"]])""")

md("## Inspect missingness (before vs after)")

code("""miss_before = pd.read_csv(outputs["paths"]["silver_missingness_before"])
miss_after = pd.read_csv(outputs["paths"]["silver_missingness_after"])

print("Top missingness BEFORE (by missing_pct):")
display(
    miss_before.sort_values("missing_pct", ascending=False).groupby("table_name").head(5)
)

print("\\nTop missingness AFTER:")
display(
    miss_after.sort_values("missing_pct", ascending=False).groupby("table_name").head(5)
)""")

md("## Duplicates, referential integrity, rules")

code("""dups = pd.read_csv(outputs["paths"]["silver_duplicate_report"])
ref = pd.read_csv(outputs["paths"]["silver_referential_integrity_report"])
rejected = pd.read_csv(outputs["paths"]["silver_rejected_records_summary"])

print("=== Duplicate report ===")
display(dups)

print("\\n=== Referential integrity ===")
display(ref)

print("\\n=== Cleaning rules (sample) ===")
display(rules.head(20))

print("\\n=== Rejected records summary ===")
display(rejected)""")

md("""## Checklist update notes

After a successful **Colab** run:

1. Verify all `reports/data_quality/silver_*.csv` files exist with numeric values.
2. Verify `data/silver/*_clean.parquet` files exist.
3. Confirm `session_result_clean.parquet` has rows (required for Gold target).
4. Update `implementation_checklist.md` Section 6 — mark Colab run only after Colab execution.
5. Next: `03_gold_feature_engineering.ipynb`.""")

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10.0"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).resolve().parents[1] / "notebooks" / "02_silver_cleaning_quality.ipynb"
out.write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("Wrote", out)
