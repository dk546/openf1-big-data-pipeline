"""Clean and standardize OpenF1 session_result endpoint."""

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
    remove_domain_invalid,
    standardize_column_names,
)


def clean_session_result(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "session_result"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze session_result data",
            rows_before=0,
            rows_after=0,
            severity="high",
            rationale="Required for Gold points_finish target — check Bronze ingestion",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_numeric(
        df,
        ["session_key", "driver_number", "position", "points", "number_of_laps", "meeting_key"],
    )
    log = log_schema_prep(
        log,
        table,
        len(df),
        numeric_cols=[
            "session_key",
            "driver_number",
            "position",
            "points",
            "number_of_laps",
            "meeting_key",
        ],
    )

    df, log = drop_rows_missing_keys(
        df,
        ["session_key", "driver_number"],
        log,
        table,
        "SIL_NULL_RESULT_KEYS",
        "Final classification requires session and driver",
        rejected,
    )

    if "position" in df.columns:
        pos = pd.to_numeric(df["position"], errors="coerce")
        df, log = remove_domain_invalid(
            df,
            (pos <= 0) & pos.notna(),
            log,
            table,
            "SIL_RESULT_POSITION",
            "Remove classification position <= 0 when non-null",
            "Invalid classified position; DNF may use null position per API",
            rejected,
        )

    if "points" in df.columns:
        pts = pd.to_numeric(df["points"], errors="coerce")
        df, log = remove_domain_invalid(
            df,
            (pts < 0) & pts.notna(),
            log,
            table,
            "SIL_RESULT_POINTS",
            "Remove negative points",
            "Points cannot be negative",
            rejected,
        )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    df, log = drop_key_duplicates(df, ["session_key", "driver_number"], log, table, rejected)

    return df.reset_index(drop=True), log
