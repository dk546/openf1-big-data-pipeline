"""One-off script to generate notebook placeholders. Not part of the pipeline."""

import json
from pathlib import Path

NOTEBOOKS = [
    {
        "file": "00_colab_setup.ipynb",
        "title": "00 — Colab Setup",
        "purpose": "Prepare the Colab runtime: clone or locate the repo, install dependencies, optionally mount Google Drive, set project paths, and verify imports.",
        "inputs": "GitHub repository URL (if cloning); requirements.txt; optional Google Drive mount.",
        "outputs": "Configured PROJECT_ROOT, DATA_ROOT, src on sys.path; verified package imports.",
        "notes": "- Run this notebook first on every new Colab session.\n- Set OPENF1_PROJECT_ROOT and DATA_ROOT for Drive-backed data.\n- Do not assume local execution.",
        "todos": [
            "Clone or pull the repository from GitHub",
            "Install packages: pip install -r requirements.txt",
            "Optionally mount Google Drive and set DATA_ROOT",
            "Add src/ to sys.path and import openf1_pipeline.config",
            "Print resolved paths (get_project_root, get_data_root)",
            "Verify imports: pandas, requests, duckdb, sklearn, pyspark (optional)",
        ],
        "code_todos": [
            "# TODO: Clone repository (if not already present)\n# !git clone https://github.com/<user>/openf1-big-data-pipeline.git",
            "# TODO: %cd openf1-big-data-pipeline",
            "# TODO: !pip install -r requirements.txt",
            "# TODO: Mount Google Drive (optional)\n# from google.colab import drive\n# drive.mount('/content/drive')",
            "# TODO: os.environ['DATA_ROOT'] = '/content/drive/MyDrive/openf1-pipeline/data'",
            "# TODO: Configure paths and verify imports",
        ],
    },
    {
        "file": "01_ingestion_bronze.ipynb",
        "title": "01 — Ingestion & Bronze Layer",
        "purpose": "Download OpenF1 data, save Bronze payloads, and generate ingestion manifest plus Bronze quality reports.",
        "inputs": "OpenF1 API; config.SEASONS and config.ENDPOINTS; Bronze directory under DATA_ROOT.",
        "outputs": "data/bronze/; artifacts/manifests/ingestion_manifest.json; bronze_row_counts.csv; bronze_schema_report.csv.",
        "notes": "- Respect API rate limits; log failures in the manifest.\n- Preserve raw JSON without silent column drops.",
        "todos": [
            "Discover race and qualifying session keys per season",
            "Ingest each endpoint for configured scope",
            "Write Bronze files by endpoint/season/session",
            "Generate ingestion manifest",
            "Generate bronze_row_counts.csv",
            "Generate bronze_schema_report.csv",
        ],
        "code_todos": [
            "# TODO: Import ingestion and bronze modules",
            "# TODO: Run ingest_season for 2023, 2024, 2025",
            "# TODO: write_ingestion_manifest(...)",
            "# TODO: generate_bronze_row_counts(...) and generate_bronze_schema_report(...)",
        ],
    },
    {
        "file": "02_silver_cleaning_quality.ipynb",
        "title": "02 — Silver Cleaning & Data Quality",
        "purpose": "Load Bronze, clean endpoint tables, run DQ audits, save Silver data, and emit before/after quality reports.",
        "inputs": "Bronze data; Silver output path; reports/data_quality/.",
        "outputs": "data/silver/; missingness, duplicate, outlier, FK, and cleaning_impact CSVs.",
        "notes": "- Document every cleaning rule with a rule_id.\n- Do not proceed to Gold until Silver reports exist.",
        "todos": [
            "Load Bronze per endpoint",
            "Run missingness report (before)",
            "Apply clean_* transforms",
            "Run duplicate, outlier, temporal, and FK checks",
            "Run missingness report (after)",
            "Write cleaning_impact_summary.csv",
            "Save Silver parquet files",
        ],
        "code_todos": [
            "# TODO: Load Bronze and run Silver cleaners",
            "# TODO: missingness, duplicates, outliers, referential integrity reports",
        ],
    },
    {
        "file": "03_gold_feature_engineering.ipynb",
        "title": "03 — Gold Feature Engineering",
        "purpose": "Build driver-race feature mart, create points_finish, export feature dictionary and validation outputs.",
        "inputs": "Silver tables; feature cutoff configuration.",
        "outputs": "driver_race_mart.parquet; feature_dictionary.csv; gold profiling and target CSVs.",
        "notes": "- Grain: one row per (session_key, driver_number) for race sessions.\n- Document leakage window in feature dictionary.",
        "todos": [
            "Build driver-race spine",
            "Join feature domains (grid, pace, pit, weather, etc.)",
            "Create points_finish target",
            "Validate uniqueness and missingness",
            "Write feature_dictionary and gold QA CSVs",
        ],
        "code_todos": [
            "# TODO: build_driver_race_mart and create_points_finish",
            "# TODO: write feature dictionary and gold reports",
        ],
    },
    {
        "file": "04_modeling_evaluation.ipynb",
        "title": "04 — Modeling & Evaluation",
        "purpose": "Season-based splits, baselines, supervised models, and export all model result artifacts.",
        "inputs": "Gold mart; feature dictionary; RANDOM_SEED and season splits.",
        "outputs": "All CSVs under reports/model_results/.",
        "notes": "- Train 2023 / Val 2024 / Test 2025.\n- Exclude leakage columns.",
        "todos": [
            "Load Gold and select features",
            "split_by_season",
            "Random and grid heuristic baselines",
            "Logistic Regression and Random Forest or LightGBM",
            "Export metrics, confusion matrix, error analysis, feature importance",
        ],
        "code_todos": [
            "# TODO: modeling pipeline — splits, baselines, train, evaluate",
        ],
    },
    {
        "file": "05_report_artifacts.ipynb",
        "title": "05 — Report Artifacts",
        "purpose": "Build report-ready tables and figures; write final run_manifest.json.",
        "inputs": "All prior reports and artifacts.",
        "outputs": "reports/figures/; reports/tables/; artifacts/manifests/run_manifest.json.",
        "notes": "- Every report claim must trace to a generated file.",
        "todos": [
            "Load DQ and model CSVs",
            "Build summary tables",
            "Generate figures",
            "Write run_manifest.json",
        ],
        "code_todos": [
            "# TODO: aggregate reports and write run manifest",
        ],
    },
]


def md_cell(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [line + "\n" for line in text.split("\n")]}


def code_cell(text: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "source": [line + "\n" for line in text.split("\n")],
        "outputs": [],
        "execution_count": None,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    nb_dir = root / "notebooks"
    nb_dir.mkdir(exist_ok=True)
    for spec in NOTEBOOKS:
        cells = [
            md_cell(f"# {spec['title']}"),
            md_cell(f"## Purpose\n\n{spec['purpose']}"),
            md_cell(f"## Inputs\n\n{spec['inputs']}"),
            md_cell(f"## Outputs\n\n{spec['outputs']}"),
            md_cell("## Execution notes (Colab)\n\n" + spec["notes"]),
            md_cell("## TODO checklist\n\n" + "\n".join(f"- [ ] {t}" for t in spec["todos"])),
        ]
        cells.extend(code_cell(c) for c in spec["code_todos"])
        nb = {
            "cells": cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python", "version": "3.10.0"},
            },
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        path = nb_dir / spec["file"]
        path.write_text(json.dumps(nb, indent=1), encoding="utf-8")
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
