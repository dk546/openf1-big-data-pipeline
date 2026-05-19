"""
Project-wide configuration: paths, seasons, API settings, and directory layout.

Designed for Colab execution; paths resolve relative to the repository root.
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

# All endpoints referenced by the pipeline (global + session-level).
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

# Session-level Bronze tables (queried with session_key).
SESSION_ENDPOINTS = [
    "drivers",
    "laps",
    "pit",
    "weather",
    "position",
    "race_control",
    # Final classification — required to build Gold target points_finish (top 10).
    "session_result",
    # Grid positions for heuristic baseline (grid <= 10); may be empty/404 for some sessions.
    "starting_grid",
]

# Backward-compatible alias
SESSION_LEVEL_ENDPOINTS = SESSION_ENDPOINTS

# Race session filter (OpenF1 session_name / session_type values)
RACE_SESSION_NAMES = ("Race",)
OPTIONAL_SESSION_NAMES = ("Sprint",)

MARKER_FILES = ("project_context.md", "README.md")


def get_project_root() -> Path:
    """
    Resolve repository root for Colab or Cursor.

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


def get_data_root() -> Path:
    """Data directory; override with DATA_ROOT for Google Drive."""
    override = os.environ.get("DATA_ROOT")
    if override:
        return Path(override).resolve()
    return get_project_root() / "data"


def get_reports_root() -> Path:
    return get_project_root() / "reports"


def get_artifacts_root() -> Path:
    return get_project_root() / "artifacts"


# ---------------------------------------------------------------------------
# Directory paths (call functions — safe after cwd / env setup in Colab)
# ---------------------------------------------------------------------------


def get_data_dir() -> Path:
    return get_data_root()


def get_bronze_dir() -> Path:
    return get_data_dir() / "bronze"


def get_silver_dir() -> Path:
    return get_data_dir() / "silver"


def get_gold_dir() -> Path:
    return get_data_dir() / "gold"


def get_reports_dir() -> Path:
    return get_reports_root()


def get_data_quality_reports_dir() -> Path:
    return get_reports_dir() / "data_quality"


def get_model_results_dir() -> Path:
    return get_reports_dir() / "model_results"


def get_artifacts_dir() -> Path:
    return get_artifacts_root()


def get_manifests_dir() -> Path:
    return get_artifacts_dir() / "manifests"


def get_schemas_dir() -> Path:
    return get_artifacts_dir() / "schemas"


def get_feature_definitions_dir() -> Path:
    return get_artifacts_dir() / "feature_definitions"


def get_pipeline_logs_dir() -> Path:
    return get_artifacts_dir() / "pipeline_logs"


# Notebook-friendly aliases (same as get_* — use after ensure_project_directories())
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
    """Create standard project directories if missing; return path map."""
    paths = {
        "data_dir": get_data_dir(),
        "bronze_dir": get_bronze_dir(),
        "silver_dir": get_silver_dir(),
        "gold_dir": get_gold_dir(),
        "reports_dir": get_reports_dir(),
        "data_quality_reports_dir": get_data_quality_reports_dir(),
        "model_results_dir": get_model_results_dir(),
        "reports_figures_dir": get_reports_dir() / "figures",
        "reports_tables_dir": get_reports_dir() / "tables",
        "artifacts_dir": get_artifacts_dir(),
        "manifests_dir": get_manifests_dir(),
        "schemas_dir": get_schemas_dir(),
        "feature_definitions_dir": get_feature_definitions_dir(),
        "pipeline_logs_dir": get_pipeline_logs_dir(),
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths
