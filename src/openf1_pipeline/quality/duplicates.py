"""
Duplicate detection and key-based deduplication for Silver audits.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

ENDPOINT_KEY_COLUMNS: dict[str, list[str]] = {
    "meetings": ["meeting_key"],
    "sessions": ["session_key"],
    "drivers": ["session_key", "driver_number"],
    "laps": ["session_key", "driver_number", "lap_number"],
    "pit": ["session_key", "driver_number", "lap_number"],
    "weather": ["session_key", "date"],
    "position": ["session_key", "driver_number", "date"],
    "race_control": ["session_key", "date", "message"],
    "session_result": ["session_key", "driver_number"],
    "starting_grid": ["session_key", "driver_number"],
}


def detect_full_duplicates(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Count exact duplicate rows (robust to list/ndarray/dict columns)."""
    from openf1_pipeline.quality.profiling import _count_full_row_duplicates

    row_count = len(df)
    duplicate_count, duplicate_check_note = _count_full_row_duplicates(df)
    duplicate_pct = round(duplicate_count / row_count * 100, 4) if row_count else 0.0
    row: dict[str, Any] = {
        "table_name": table_name,
        "duplicate_type": "full_row_duplicate",
        "duplicate_count": duplicate_count,
        "duplicate_pct": duplicate_pct,
    }
    if duplicate_check_note:
        row["duplicate_check_note"] = duplicate_check_note
    return pd.DataFrame([row])


def detect_key_duplicates(
    df: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
) -> pd.DataFrame:
    """Detect duplicate primary keys (key columns only; skips array/list fields elsewhere)."""
    from openf1_pipeline.quality.profiling import make_dataframe_hashable_for_duplicates

    missing_keys = [c for c in key_columns if c not in df.columns]
    if missing_keys:
        logger.info(
            "Skipped key duplicate check for %s: missing columns %s",
            table_name,
            missing_keys,
        )
        return pd.DataFrame(
            [
                {
                    "table_name": table_name,
                    "duplicate_type": "key_duplicate",
                    "key_columns": ",".join(key_columns),
                    "duplicate_key_count": 0,
                    "affected_rows": 0,
                    "affected_rows_pct": 0.0,
                    "status": "skipped_missing_columns",
                }
            ]
        )

    if df.empty:
        return pd.DataFrame(
            [
                {
                    "table_name": table_name,
                    "duplicate_type": "key_duplicate",
                    "key_columns": ",".join(key_columns),
                    "duplicate_key_count": 0,
                    "affected_rows": 0,
                    "affected_rows_pct": 0.0,
                    "status": "skipped_empty",
                }
            ]
        )

    subset = df[key_columns].copy()
    try:
        dup_mask = subset.duplicated(keep=False)
    except TypeError:
        subset, _ = make_dataframe_hashable_for_duplicates(subset)
        dup_mask = subset.duplicated(keep=False)

    affected_rows = int(dup_mask.sum())
    try:
        key_dup_counts = subset.groupby(key_columns, dropna=False).size()
        duplicate_key_count = int((key_dup_counts > 1).sum())
    except TypeError:
        subset, _ = make_dataframe_hashable_for_duplicates(subset)
        key_dup_counts = subset.groupby(key_columns, dropna=False).size()
        duplicate_key_count = int((key_dup_counts > 1).sum())

    row_count = len(df)
    affected_rows_pct = round(affected_rows / row_count * 100, 4) if row_count else 0.0

    return pd.DataFrame(
        [
            {
                "table_name": table_name,
                "duplicate_type": "key_duplicate",
                "key_columns": ",".join(key_columns),
                "duplicate_key_count": duplicate_key_count,
                "affected_rows": affected_rows,
                "affected_rows_pct": affected_rows_pct,
                "status": "checked",
            }
        ]
    )


def remove_key_duplicates(
    df: pd.DataFrame,
    key_columns: list[str],
    keep: str = "first",
) -> tuple[pd.DataFrame, int]:
    """Remove duplicate keys; return cleaned frame and rows removed."""
    if df.empty:
        return df, 0
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        return df, 0
    before = len(df)
    cleaned = df.drop_duplicates(subset=key_columns, keep=keep)
    return cleaned, before - len(cleaned)


def build_duplicate_report(
    tables: dict[str, pd.DataFrame],
    stage: str,
) -> pd.DataFrame:
    """Full + key duplicate report for all tables."""
    parts: list[pd.DataFrame] = []
    for name, df in tables.items():
        full = detect_full_duplicates(df, name)
        full["stage"] = stage
        parts.append(full)
        keys = ENDPOINT_KEY_COLUMNS.get(name, [])
        if keys:
            key_df = detect_key_duplicates(df, name, keys)
            key_df["stage"] = stage
            parts.append(key_df)
    if not parts:
        return pd.DataFrame(
            columns=[
                "table_name",
                "duplicate_type",
                "stage",
            ]
        )
    return pd.concat(parts, ignore_index=True)
