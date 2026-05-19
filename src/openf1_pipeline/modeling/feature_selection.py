"""
Frozen modeling feature plan: Tier 1 early-session and Tier 2 full-session analytical features.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from openf1_pipeline.config import get_feature_definitions_dir
from openf1_pipeline.gold.build_feature_mart import (
    IDENTIFIER_COLUMNS,
    LEAKAGE_FORBIDDEN_COLUMNS,
    METADATA_COLUMNS,
    TARGET_COLUMN,
)
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv

FORBIDDEN_FEATURE_PATTERNS: tuple[str, ...] = (
    "source_",
    "diagnostic_",
    "result_",
)

IDENTIFIER_COLUMNS_FROZEN: frozenset[str] = IDENTIFIER_COLUMNS

TARGET_COLUMN_FROZEN: str = TARGET_COLUMN

DIAGNOSTIC_OUTCOME_COLUMNS: frozenset[str] = frozenset(
    {
        "final_position",
        "result_points",
        "result_dnf",
        "result_dns",
        "result_dsq",
        "diagnostic_final_observed_position",
    }
)

TIER1_EARLY_FEATURES: tuple[str, ...] = (
    "avg_first5_lap_duration",
    "best_first5_lap_duration",
    "std_first5_lap_duration",
    "first_observed_position",
    "early_avg_position",
    "early_min_position",
    "early_max_position",
    "position_observation_count",
)

TIER2_FULL_SESSION_FEATURES: tuple[str, ...] = (
    "lap_count",
    "avg_lap_duration",
    "median_lap_duration",
    "best_lap_duration",
    "std_lap_duration",
    "avg_sector_1",
    "avg_sector_2",
    "avg_sector_3",
    "avg_i1_speed",
    "avg_i2_speed",
    "avg_st_speed",
    "pit_out_lap_count",
    "pit_stop_count",
    "avg_pit_duration",
    "min_pit_duration",
    "max_pit_duration",
    "first_pit_lap",
    "early_pit_stop_flag",
    "avg_air_temperature",
    "avg_track_temperature",
    "avg_humidity",
    "avg_pressure",
    "avg_wind_speed",
    "rainfall_mean",
    "rainfall_flag",
    "race_control_message_count",
    "flag_message_count",
    "yellow_flag_count",
    "red_flag_count",
    "green_flag_count",
    "safety_car_message_count",
    "pit_exit_message_count",
)

DEFAULT_NUMERIC_MODEL_FEATURES: tuple[str, ...] = (
    TIER1_EARLY_FEATURES + TIER2_FULL_SESSION_FEATURES
)

OPTIONAL_CATEGORICAL_FEATURES: tuple[str, ...] = (
    "team_name",
    "circuit_short_name",
    "session_country_name",
    "location",
    "session_type",
    "session_name",
    "session_year",
)

MODEL_FEATURE_PLAN_COLUMNS = [
    "feature_name",
    "feature_tier",
    "feature_group",
    "default_include",
    "allowed_for_modeling",
    "reason",
]

_FEATURE_GROUP_BY_PREFIX = {
    "lap_": "laps",
    "avg_lap": "laps",
    "median_lap": "laps",
    "best_lap": "laps",
    "std_lap": "laps",
    "avg_first5": "laps",
    "best_first5": "laps",
    "std_first5": "laps",
    "avg_sector": "laps",
    "avg_i1": "laps",
    "avg_i2": "laps",
    "avg_st": "laps",
    "pit_out": "laps",
    "pit_stop": "pit",
    "first_pit": "pit",
    "avg_pit": "pit",
    "min_pit": "pit",
    "max_pit": "pit",
    "early_pit": "pit",
    "first_observed_position": "position",
    "early_avg_position": "position",
    "early_min_position": "position",
    "early_max_position": "position",
    "position_observation": "position",
    "avg_air": "weather",
    "avg_track": "weather",
    "avg_humidity": "weather",
    "avg_pressure": "weather",
    "avg_wind": "weather",
    "rainfall": "weather",
    "race_control": "race_control",
    "flag_message": "race_control",
    "yellow_flag": "race_control",
    "red_flag": "race_control",
    "green_flag": "race_control",
    "safety_car": "race_control",
    "pit_exit_message": "race_control",
}


def infer_feature_tier(feature_name: str) -> str:
    """Return tier1_early, tier2_full_session, or none."""
    if feature_name in TIER1_EARLY_FEATURES:
        return "tier1_early"
    if feature_name in TIER2_FULL_SESSION_FEATURES:
        return "tier2_full_session"
    return "none"


def _infer_feature_group(name: str) -> str:
    if name in IDENTIFIER_COLUMNS:
        return "identifiers"
    if name == TARGET_COLUMN:
        return "target"
    if name.startswith("diagnostic_") or name in LEAKAGE_FORBIDDEN_COLUMNS:
        return "diagnostic_outcome"
    if name in METADATA_COLUMNS:
        return "metadata"
    for prefix, group in _FEATURE_GROUP_BY_PREFIX.items():
        if name.startswith(prefix) or name == prefix.rstrip("_"):
            return group
    return "engineered"


def _plan_row(
    feature_name: str,
    *,
    feature_tier: str,
    feature_group: str,
    default_include: bool,
    allowed_for_modeling: bool,
    reason: str,
) -> dict:
    return {
        "feature_name": feature_name,
        "feature_tier": feature_tier,
        "feature_group": feature_group,
        "default_include": default_include,
        "allowed_for_modeling": allowed_for_modeling,
        "reason": reason,
    }


def get_default_model_feature_plan() -> pd.DataFrame:
    """Build the frozen modeling feature plan DataFrame."""
    rows: list[dict] = []

    for col in sorted(IDENTIFIER_COLUMNS):
        rows.append(
            _plan_row(
                col,
                feature_tier="none",
                feature_group="identifiers",
                default_include=False,
                allowed_for_modeling=False,
                reason="Grain key; never in model X.",
            )
        )

    rows.append(
        _plan_row(
            TARGET_COLUMN,
            feature_tier="none",
            feature_group="target",
            default_include=False,
            allowed_for_modeling=False,
            reason="Binary target: points_finish.",
        )
    )

    for col in sorted(DIAGNOSTIC_OUTCOME_COLUMNS):
        rows.append(
            _plan_row(
                col,
                feature_tier="none",
                feature_group="diagnostic_outcome",
                default_include=False,
                allowed_for_modeling=False,
                reason="Post-race outcome for evaluation only; not for modeling.",
            )
        )

    for col in DEFAULT_NUMERIC_MODEL_FEATURES:
        tier = infer_feature_tier(col)
        tier_label = "Tier 1 early-session" if tier == "tier1_early" else "Tier 2 full-session analytical"
        rows.append(
            _plan_row(
                col,
                feature_tier=tier,
                feature_group=_infer_feature_group(col),
                default_include=True,
                allowed_for_modeling=True,
                reason=f"Default numeric model feature ({tier_label}).",
            )
        )

    for col in OPTIONAL_CATEGORICAL_FEATURES:
        reason = "Optional categorical metadata; encode explicitly if used."
        if col == "session_year":
            reason = (
                "Optional metadata; default_exclude — season split leakage if used as feature."
            )
        rows.append(
            _plan_row(
                col,
                feature_tier="none",
                feature_group="metadata",
                default_include=False,
                allowed_for_modeling=True,
                reason=reason,
            )
        )

    other_metadata = sorted(METADATA_COLUMNS - set(OPTIONAL_CATEGORICAL_FEATURES))
    for col in other_metadata:
        rows.append(
            _plan_row(
                col,
                feature_tier="none",
                feature_group="metadata",
                default_include=False,
                allowed_for_modeling=False,
                reason="Metadata; not in default model bundle.",
            )
        )

    return pd.DataFrame(rows, columns=MODEL_FEATURE_PLAN_COLUMNS)


def summarize_feature_tiers(plan_df: pd.DataFrame) -> dict[str, int]:
    """Count default-included features by tier."""
    included = plan_df.loc[plan_df["default_include"] == True]  # noqa: E712
    return {
        "tier1_early": int((included["feature_tier"] == "tier1_early").sum()),
        "tier2_full_session": int((included["feature_tier"] == "tier2_full_session").sum()),
        "total_default_numeric": len(included),
    }


def save_model_feature_plan(output_path: Path | None = None) -> Path:
    """Write artifacts/feature_definitions/model_feature_plan.csv."""
    output_path = Path(output_path or get_feature_definitions_dir() / "model_feature_plan.csv")
    ensure_dir(output_path.parent)
    plan = get_default_model_feature_plan()
    save_dataframe_csv(plan, output_path)
    return output_path


def _is_forbidden_column(col: str) -> bool:
    if col in IDENTIFIER_COLUMNS | {TARGET_COLUMN} | DIAGNOSTIC_OUTCOME_COLUMNS:
        return True
    if col in LEAKAGE_FORBIDDEN_COLUMNS:
        return True
    if any(col.startswith(p) for p in FORBIDDEN_FEATURE_PATTERNS):
        return True
    return False


def _columns_from_feature_plan(plan_path: Path) -> list[str]:
    plan = pd.read_csv(plan_path)
    required = {"feature_name", "allowed_for_modeling", "default_include"}
    if not required.issubset(plan.columns):
        raise ValueError(f"model_feature_plan.csv missing columns: {required - set(plan.columns)}")
    selected = plan.loc[
        (plan["allowed_for_modeling"] == True) & (plan["default_include"] == True),  # noqa: E712
        "feature_name",
    ].tolist()
    cols: list[str] = []
    for col in selected:
        if _is_forbidden_column(col):
            continue
        cols.append(col)
    if not cols:
        raise ValueError(
            f"No model features after applying model_feature_plan.csv at {plan_path}. "
            "Regenerate with save_model_feature_plan()."
        )
    return cols


def _columns_from_dictionary(feature_dictionary: pd.DataFrame) -> list[str]:
    if feature_dictionary.empty:
        raise ValueError(
            "Feature dictionary is empty. Run notebook 03 (Gold) to generate feature_dictionary.csv."
        )
    allowed = feature_dictionary.loc[
        feature_dictionary["allowed_for_modeling"] == True, "feature_name"  # noqa: E712
    ].tolist()
    cols: list[str] = []
    for col in allowed:
        if _is_forbidden_column(col):
            continue
        cols.append(col)
    if not cols:
        raise ValueError(
            "No model feature columns available after applying feature_dictionary "
            "and leakage exclusions. Re-run Gold (notebook 03) and check "
            "gold_leakage_guard_report.csv."
        )
    return cols


def resolve_model_feature_columns(
    feature_dictionary: pd.DataFrame | None = None,
    feature_plan_path: Path | None = None,
) -> list[str]:
    """
    Resolve model feature columns.

    Priority: model_feature_plan.csv → feature_dictionary → clear error.
    """
    plan_path = Path(
        feature_plan_path or get_feature_definitions_dir() / "model_feature_plan.csv"
    )
    if plan_path.is_file():
        return _columns_from_feature_plan(plan_path)

    if feature_dictionary is not None and not feature_dictionary.empty:
        return _columns_from_dictionary(feature_dictionary)

    dict_path = get_feature_definitions_dir() / "feature_dictionary.csv"
    if dict_path.is_file():
        return _columns_from_dictionary(pd.read_csv(dict_path))

    raise ValueError(
        "No modeling feature source found. Expected model_feature_plan.csv or "
        "feature_dictionary.csv under artifacts/feature_definitions/. "
        "Run notebook 03 (Gold) or call save_model_feature_plan()."
    )
