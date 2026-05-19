"""Clean and standardize OpenF1 position endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_datetime,
    coerce_numeric,
    drop_exact_duplicates,
    drop_key_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    log_schema_prep,
    remove_domain_invalid,
    standardize_column_names,
)


def clean_position(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "position"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze position data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Endpoint not ingested or empty",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_numeric(df, ["session_key", "driver_number", "position", "meeting_key"])
    df = coerce_datetime(df, ["date"])
    log = log_schema_prep(
        log,
        table,
        len(df),
        numeric_cols=["session_key", "driver_number", "position", "meeting_key"],
        datetime_cols=["date"],
    )

    df, log = drop_rows_missing_keys(
        df,
        ["session_key", "driver_number"],
        log,
        table,
        "SIL_NULL_POSITION_KEYS",
        "Position updates require session and driver",
        rejected,
    )

    if "position" in df.columns:
        pos = pd.to_numeric(df["position"], errors="coerce")
        df, log = remove_domain_invalid(
            df,
            (pos <= 0) & pos.notna(),
            log,
            table,
            "SIL_POSITION_POS",
            "Remove position <= 0",
            "Race position must be a positive integer when present",
            rejected,
        )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    if "date" in df.columns:
        df, log = drop_key_duplicates(
            df, ["session_key", "driver_number", "date"], log, table, rejected
        )

    return df.reset_index(drop=True), log
