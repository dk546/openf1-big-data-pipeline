"""
Referential integrity checks across Silver tables.
"""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def check_referential_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_table: str,
    parent_table: str,
    key_columns: list[str],
) -> pd.DataFrame:
    """Compare child keys against parent keys."""
    base = {
        "child_table": child_table,
        "parent_table": parent_table,
        "key_columns": ",".join(key_columns),
        "child_rows": len(child_df),
        "unmatched_rows": 0,
        "unmatched_pct": 0.0,
        "status": "checked",
    }

    missing_child = [c for c in key_columns if c not in child_df.columns]
    missing_parent = [c for c in key_columns if c not in parent_df.columns]
    if child_df.empty:
        base["status"] = "skipped_empty_child"
        return pd.DataFrame([base])
    if parent_df.empty:
        base["status"] = "skipped_empty_parent"
        return pd.DataFrame([base])
    if missing_child or missing_parent:
        base["status"] = "skipped_missing_columns"
        return pd.DataFrame([base])

    child_keys = child_df[key_columns].drop_duplicates().dropna(how="any")
    parent_keys = parent_df[key_columns].drop_duplicates().dropna(how="any")
    merged = child_keys.merge(
        parent_keys,
        on=key_columns,
        how="left",
        indicator=True,
    )
    unmatched = int((merged["_merge"] == "left_only").sum())
    child_rows = len(child_keys)
    base["child_rows"] = child_rows
    base["unmatched_rows"] = unmatched
    base["unmatched_pct"] = round(unmatched / child_rows * 100, 4) if child_rows else 0.0
    return pd.DataFrame([base])


def build_referential_integrity_report(
    tables: dict[str, pd.DataFrame],
    stage: str,
) -> pd.DataFrame:
    """Standard FK checks between session and driver-level tables."""
    parts: list[pd.DataFrame] = []
    sessions = tables.get("sessions", pd.DataFrame())
    drivers = tables.get("drivers", pd.DataFrame())

    session_children = [
        "drivers",
        "laps",
        "pit",
        "weather",
        "position",
        "race_control",
        "session_result",
        "starting_grid",
    ]
    for child_name in session_children:
        child = tables.get(child_name)
        if child is None:
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "child_table": child_name,
                            "parent_table": "sessions",
                            "key_columns": "session_key",
                            "child_rows": 0,
                            "unmatched_rows": 0,
                            "unmatched_pct": 0.0,
                            "status": "skipped_missing_table",
                            "stage": stage,
                        }
                    ]
                )
            )
            continue
        row = check_referential_integrity(
            child, sessions, child_name, "sessions", ["session_key"]
        )
        row["stage"] = stage
        parts.append(row)

    driver_children = ["laps", "pit", "position", "session_result", "starting_grid"]
    for child_name in driver_children:
        child = tables.get(child_name)
        if child is None:
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "child_table": child_name,
                            "parent_table": "drivers",
                            "key_columns": "session_key,driver_number",
                            "child_rows": 0,
                            "unmatched_rows": 0,
                            "unmatched_pct": 0.0,
                            "status": "skipped_missing_table",
                            "stage": stage,
                        }
                    ]
                )
            )
            continue
        row = check_referential_integrity(
            child,
            drivers,
            child_name,
            "drivers",
            ["session_key", "driver_number"],
        )
        row["stage"] = stage
        parts.append(row)

    if not parts:
        return pd.DataFrame(columns=["child_table", "parent_table", "stage", "status"])
    return pd.concat(parts, ignore_index=True)
