"""Clean and standardize OpenF1 starting_grid endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_numeric,
    drop_exact_duplicates,
    drop_key_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    log_schema_prep,
    standardize_column_names,
)


def clean_starting_grid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "starting_grid"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze starting_grid data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Optional endpoint; heuristic may use sessions/drivers in Gold",
        )
        return df, log

    df = standardize_column_names(df)
    # OpenF1 may use position or grid_position
    if "grid_position" not in df.columns and "position" in df.columns:
        df["grid_position"] = df["position"]
    df = coerce_numeric(df, ["session_key", "driver_number", "position", "grid_position"])
    log = log_schema_prep(
        log,
        table,
        len(df),
        numeric_cols=["session_key", "driver_number", "position", "grid_position"],
    )

    df, log = drop_rows_missing_keys(
        df,
        ["session_key", "driver_number"],
        log,
        table,
        "SIL_NULL_GRID_KEYS",
        "Grid rows require session and driver",
        rejected,
    )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    df, log = drop_key_duplicates(df, ["session_key", "driver_number"], log, table, rejected)

    return df.reset_index(drop=True), log
