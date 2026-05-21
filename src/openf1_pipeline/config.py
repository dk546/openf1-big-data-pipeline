"""
Project-wide configuration: paths, seasons, API settings, and directory layout.

Designed for Colab execution.
- PROJECT_ROOT: GitHub repo root (code, notebooks).
- OUTPUT_ROOT: generated data/reports/artifacts (repo or Google Drive via OPENF1_DATA_ROOT).

Set OPENF1_DATA_ROOT before importing this module in Colab when using Drive persistence.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project identity
# ---------------------------------------------------------------------------

PROJECT_NAME = "openf1-big-data-pipeline"
RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# Data scope
# ---------------------------------------------------------------------------

SEASONS = [2023, 2024, 2025]
TRAIN_SEASONS = [2023]
VALIDATION_SEASONS = [2024]
TEST_SEASONS = [2025]

OPENF1_BASE_URL = "https://api.openf1.org/v1"

ENDPOINTS = [
    "meetings",
    "sessions",
    "drivers",
    "laps",
    "pit",
    "weather",
    "position",
    "race_control",
    "session_result",
    "starting_grid",
]

GLOBAL_ENDPOINTS = ["meetings", "sessions"]

SESSION_ENDPOINTS = [
    "drivers",
    "laps",
    "pit",
    "weather",
    "position",
    "race_control",
    "session_result",
    "starting_grid",
]

SESSION_LEVEL_ENDPOINTS = SESSION_ENDPOINTS

RACE_SESSION_NAMES = ("Race",)
OPTIONAL_SESSION_NAMES = ("Sprint",)

MARKER_FILES = ("README.md",)


def get_project_root() -> Path:
    """
    Resolve repository root for Colab or local development (source code, notebooks).

    Order: OPENF1_PROJECT_ROOT env → walk up from cwd for marker file → cwd.
    """
    env_root = os.environ.get("OPENF1_PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if any((candidate / name).is_file() for name in MARKER_FILES):
            return candidate

    return cwd


def get_output_root() -> Path:
    """
    Root for generated outputs: data/, reports/, artifacts/.

    Uses OPENF1_DATA_ROOT when set (e.g. Google Drive). Otherwise PROJECT_ROOT.

    Legacy: if only DATA_ROOT is set and points at a ``data`` directory, its parent
    is used as OUTPUT_ROOT for backward compatibility.
    """
    env_output = os.environ.get("OPENF1_DATA_ROOT")
    if env_output:
        return Path(env_output).resolve()

    legacy_data = os.environ.get("DATA_ROOT")
    if legacy_data:
        legacy_path = Path(legacy_data).resolve()
        if legacy_path.name == "data":
            return legacy_path.parent
        return legacy_path

    return get_project_root()


def get_data_root() -> Path:
    """Alias for data directory (OUTPUT_ROOT / data)."""
    return get_data_dir()


def get_data_dir() -> Path:
    return get_output_root() / "data"


def get_bronze_dir() -> Path:
    return get_data_dir() / "bronze"


def get_silver_dir() -> Path:
    return get_data_dir() / "silver"


def get_gold_dir() -> Path:
    return get_data_dir() / "gold"


def get_reports_dir() -> Path:
    return get_output_root() / "reports"


def get_data_quality_reports_dir() -> Path:
    return get_reports_dir() / "data_quality"


def get_model_results_dir() -> Path:
    return get_reports_dir() / "model_results"


def get_artifacts_dir() -> Path:
    return get_output_root() / "artifacts"


def get_manifests_dir() -> Path:
    return get_artifacts_dir() / "manifests"


def get_schemas_dir() -> Path:
    return get_artifacts_dir() / "schemas"


def get_feature_definitions_dir() -> Path:
    return get_artifacts_dir() / "feature_definitions"


def get_pipeline_logs_dir() -> Path:
    return get_artifacts_dir() / "pipeline_logs"


# Notebook-friendly aliases (call after OPENF1_DATA_ROOT is set in Colab)
OUTPUT_ROOT = get_output_root
DATA_DIR = get_data_dir
BRONZE_DIR = get_bronze_dir
SILVER_DIR = get_silver_dir
GOLD_DIR = get_gold_dir
REPORTS_DIR = get_reports_dir
DATA_QUALITY_REPORTS_DIR = get_data_quality_reports_dir
MODEL_RESULTS_DIR = get_model_results_dir
ARTIFACTS_DIR = get_artifacts_dir
MANIFESTS_DIR = get_manifests_dir
SCHEMAS_DIR = get_schemas_dir
PIPELINE_LOGS_DIR = get_pipeline_logs_dir


def ensure_project_directories() -> dict[str, Path]:
    """Create standard output directories under OUTPUT_ROOT; return path map."""
    paths = {
        "OUTPUT_ROOT": get_output_root(),
        "PROJECT_ROOT": get_project_root(),
        "DATA_DIR": get_data_dir(),
        "BRONZE_DIR": get_bronze_dir(),
        "SILVER_DIR": get_silver_dir(),
        "GOLD_DIR": get_gold_dir(),
        "REPORTS_DIR": get_reports_dir(),
        "DATA_QUALITY_REPORTS_DIR": get_data_quality_reports_dir(),
        "MODEL_RESULTS_DIR": get_model_results_dir(),
        "reports_figures_dir": get_reports_dir() / "figures",
        "reports_tables_dir": get_reports_dir() / "tables",
        "ARTIFACTS_DIR": get_artifacts_dir(),
        "MANIFESTS_DIR": get_manifests_dir(),
        "SCHEMAS_DIR": get_schemas_dir(),
        "feature_definitions_dir": get_feature_definitions_dir(),
        "PIPELINE_LOGS_DIR": get_pipeline_logs_dir(),
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths
