"""
Generate and validate artifacts/feature_definitions/feature_dictionary.csv.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from openf1_pipeline.gold.build_feature_mart import (
    IDENTIFIER_COLUMNS,
    LEAKAGE_FORBIDDEN_COLUMNS,
    METADATA_COLUMNS,
    TARGET_COLUMN,
)
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv

FEATURE_DICTIONARY_COLUMNS = [
    "feature_name",
    "feature_group",
    "feature_tier",
    "dtype",
    "description",
    "source_table",
    "modeling_role",
    "allowed_for_modeling",
    "missing_pct",
]

FEATURE_GROUP_BY_PREFIX = {
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
    "diagnostic_": "position",
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
    "points_finish": "session_result",
    "final_position": "session_result",
    "result_": "session_result",
}

SOURCE_TABLE_BY_GROUP = {
    "session_result": "session_result",
    "laps": "laps",
    "pit": "pit",
    "position": "position",
    "weather": "weather",
    "race_control": "race_control",
    "metadata": "drivers,sessions,meetings",
    "identifiers": "session_result",
    "target": "session_result",
    "diagnostic_outcome": "session_result",
}


def _infer_feature_group(name: str) -> str:
    if name in IDENTIFIER_COLUMNS:
        return "identifiers"
    if name == TARGET_COLUMN:
        return "target"
    if name.startswith("diagnostic_") or name in LEAKAGE_FORBIDDEN_COLUMNS:
        return "diagnostic_outcome"
    if name in METADATA_COLUMNS:
        return "metadata"
    for prefix, group in FEATURE_GROUP_BY_PREFIX.items():
        if name.startswith(prefix) or name == prefix.rstrip("_"):
            return group
    if name in {"full_name", "name_acronym", "team_name", "driver_country_code"}:
        return "metadata"
    if name in {
        "session_type",
        "session_name",
        "session_year",
        "circuit_short_name",
        "session_country_name",
        "location",
        "meeting_name",
        "circuit_key",
    }:
        return "metadata"
    return "engineered"


def _modeling_role(name: str, allowed: bool) -> str:
    if name in IDENTIFIER_COLUMNS:
        return "identifier"
    if name == TARGET_COLUMN:
        return "target"
    if name.startswith("diagnostic_") or name in {
        "final_position",
        "result_points",
        "result_dnf",
        "result_dns",
        "result_dsq",
    }:
        return "diagnostic_outcome"
    if name in METADATA_COLUMNS:
        return "metadata"
    if allowed:
        return "predictive_feature"
    return "metadata"


def _description(name: str, role: str, group: str, feature_tier: str) -> str:
    if role == "target":
        return "Binary target: 1 if driver scored points (>0), else 0."
    if role == "identifier":
        return f"Grain key ({group})."
    if role == "diagnostic_outcome":
        return "Post-race outcome for evaluation only; not for modeling."
    if name.startswith("diagnostic_"):
        return "Diagnostic field excluded from model features."
    if group == "laps":
        if feature_tier == "tier1_early":
            return "Tier 1 early-session lap aggregate (first five laps) from Silver laps_clean."
        if feature_tier == "tier2_full_session":
            return "Tier 2 full-session analytical lap aggregate from Silver laps_clean."
        return "Lap-level aggregate from Silver laps_clean."
    if group == "pit":
        if feature_tier == "tier2_full_session":
            return "Tier 2 full-session analytical pit aggregate from Silver pit_clean."
        return "Pit stop aggregate from Silver pit_clean."
    if group == "position":
        if feature_tier == "tier1_early":
            return "Tier 1 early-session position aggregate (first five observations)."
        return "Position aggregate from Silver position_clean."
    if group == "weather":
        if feature_tier == "tier2_full_session":
            return "Tier 2 full-session analytical weather aggregate joined to each driver."
        return "Session-level weather joined to each driver."
    if group == "race_control":
        if feature_tier == "tier2_full_session":
            return "Tier 2 full-session analytical race control message counts."
        return "Session-level race control message counts."
    if group == "metadata":
        return "Driver, session, or meeting metadata."
    return "Engineered feature for modeling."


def build_feature_dictionary(gold_df: pd.DataFrame) -> pd.DataFrame:
    """Build feature dictionary from Gold mart columns."""
    from openf1_pipeline.gold.build_feature_mart import build_leakage_guard_report
    from openf1_pipeline.modeling.feature_selection import infer_feature_tier

    guard = build_leakage_guard_report(gold_df)
    allowed_map = guard.set_index("column_name")["allowed_for_modeling"].to_dict()
    n = len(gold_df)
    rows: list[dict] = []
    for col in gold_df.columns:
        group = _infer_feature_group(col)
        allowed = bool(allowed_map.get(col, False))
        role = _modeling_role(col, allowed)
        feature_tier = infer_feature_tier(col)
        missing = gold_df[col].isna().sum()
        rows.append(
            {
                "feature_name": col,
                "feature_group": group,
                "feature_tier": feature_tier,
                "dtype": str(gold_df[col].dtype),
                "description": _description(col, role, group, feature_tier),
                "source_table": SOURCE_TABLE_BY_GROUP.get(group, "gold_mart"),
                "modeling_role": role,
                "allowed_for_modeling": allowed,
                "missing_pct": round(missing / n * 100, 4) if n else 0.0,
            }
        )
    return pd.DataFrame(rows, columns=FEATURE_DICTIONARY_COLUMNS)


def write_feature_dictionary(df: pd.DataFrame, output_path: Path) -> None:
    """Write artifacts/feature_definitions/feature_dictionary.csv."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    save_dataframe_csv(df, output_path)


def validate_no_leakage(df: pd.DataFrame, dictionary: pd.DataFrame) -> None:
    """Raise if model feature set includes post-decision-time columns."""
    forbidden = set(LEAKAGE_FORBIDDEN_COLUMNS) | {
        c for c in df.columns if c.startswith("diagnostic_")
    }
    model_cols = dictionary.loc[
        dictionary["allowed_for_modeling"] == True, "feature_name"  # noqa: E712
    ].tolist()
    leaked = [c for c in model_cols if c in forbidden or c == TARGET_COLUMN]
    if leaked:
        raise ValueError(f"Leakage guard failed; forbidden columns in model set: {leaked}")
