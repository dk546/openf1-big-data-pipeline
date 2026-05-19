"""
Layer-specific Drive output cleanup for idempotent Colab reruns.
"""

from __future__ import annotations

from pathlib import Path

from openf1_pipeline.config import (
    get_artifacts_dir,
    get_bronze_dir,
    get_data_quality_reports_dir,
    get_feature_definitions_dir,
    get_gold_dir,
    get_manifests_dir,
    get_model_results_dir,
    get_reports_dir,
    get_schemas_dir,
    get_silver_dir,
)
from openf1_pipeline.utils.io import clean_directory_contents, clean_files_matching
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def clean_bronze_layer_outputs(
    bronze_dir: Path | None = None,
    data_quality_reports_dir: Path | None = None,
    manifests_dir: Path | None = None,
    schemas_dir: Path | None = None,
) -> dict[str, int]:
    """
    Clean Bronze data and Bronze-specific reports/manifests.

    Does not touch Silver, Gold, or model outputs.
    """
    bronze_dir = Path(bronze_dir or get_bronze_dir())
    dq_dir = Path(data_quality_reports_dir or get_data_quality_reports_dir())
    manifests_dir = Path(manifests_dir or get_manifests_dir())
    schemas_dir = Path(schemas_dir or get_schemas_dir())

    counts = {
        "bronze_data": clean_directory_contents(bronze_dir),
        "bronze_reports": clean_files_matching(
            dq_dir, ["bronze_*.csv", "duckdb_bronze_*.csv"]
        ),
        "ingestion_manifest": 0,
        "schemas": clean_directory_contents(schemas_dir),
    }
    manifest_path = manifests_dir / "ingestion_manifest.csv"
    if manifest_path.is_file():
        manifest_path.unlink()
        counts["ingestion_manifest"] = 1
    logger.info("Bronze layer cleanup complete: %s", counts)
    return counts


def clean_silver_layer_outputs(
    silver_dir: Path | None = None,
    data_quality_reports_dir: Path | None = None,
) -> dict[str, int]:
    """
    Clean Silver Parquet outputs and Silver-specific DQ reports.

    Does not touch Bronze or Gold.
    """
    silver_dir = Path(silver_dir or get_silver_dir())
    dq_dir = Path(data_quality_reports_dir or get_data_quality_reports_dir())

    counts = {
        "silver_data": clean_directory_contents(silver_dir),
        "silver_reports": clean_files_matching(
            dq_dir, ["silver_*.csv", "duckdb_silver_*.csv"]
        ),
    }
    logger.info("Silver layer cleanup complete: %s", counts)
    return counts


def clean_gold_layer_outputs(
    gold_dir: Path | None = None,
    data_quality_reports_dir: Path | None = None,
    feature_definitions_dir: Path | None = None,
) -> dict[str, int]:
    """
    Clean Gold Parquet, Gold DQ reports, DuckDB Gold reports, and feature dictionary.

    Does not touch Silver or model results.
    """
    gold_dir = Path(gold_dir or get_gold_dir())
    dq_dir = Path(data_quality_reports_dir or get_data_quality_reports_dir())
    feature_definitions_dir = Path(
        feature_definitions_dir or get_feature_definitions_dir()
    )

    counts = {
        "gold_data": clean_directory_contents(gold_dir),
        "gold_reports": clean_files_matching(
            dq_dir, ["gold_*.csv", "duckdb_gold_*.csv"]
        ),
        "feature_dictionary": 0,
    }
    dict_path = feature_definitions_dir / "feature_dictionary.csv"
    if dict_path.is_file():
        dict_path.unlink()
        counts["feature_dictionary"] = 1
    logger.info("Gold layer cleanup complete: %s", counts)
    return counts


def clean_model_outputs(
    model_results_dir: Path | None = None,
    manifests_dir: Path | None = None,
) -> dict[str, int]:
    """
    Clean modeling CSV outputs and model_run_manifest.json.

    Does not touch Gold or data_quality reports.
    """
    model_results_dir = Path(model_results_dir or get_model_results_dir())
    manifests_dir = Path(manifests_dir or get_manifests_dir())

    counts = {
        "model_results": clean_directory_contents(model_results_dir),
        "model_run_manifest": 0,
    }
    manifest_path = manifests_dir / "model_run_manifest.json"
    if manifest_path.is_file():
        manifest_path.unlink()
        counts["model_run_manifest"] = 1
    logger.info("Model outputs cleanup complete: %s", counts)
    return counts


def clean_report_artifacts(
    reports_dir: Path | None = None,
) -> dict[str, int]:
    """
    Clean MBA report tables and figures only.

    Does not touch data_quality or model_results.
    """
    reports_dir = Path(reports_dir or get_reports_dir())
    tables_dir = reports_dir / "tables"
    figures_dir = reports_dir / "figures"

    counts = {
        "report_tables": clean_directory_contents(tables_dir),
        "report_figures": clean_directory_contents(figures_dir),
    }
    logger.info("Report artifacts cleanup complete: %s", counts)
    return counts


def clean_silver_output_dir(silver_dir: Path) -> None:
    """Backward-compatible alias: clean Silver data dir only."""
    clean_directory_contents(Path(silver_dir))


def clean_gold_output_dir(gold_dir: Path) -> None:
    """Backward-compatible alias: clean Gold data dir only."""
    clean_directory_contents(Path(gold_dir))
