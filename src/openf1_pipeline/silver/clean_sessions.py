"""Clean and standardize OpenF1 sessions endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_datetime,
    drop_exact_duplicates,
    drop_key_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    log_schema_prep,
    standardize_column_names,
)


def clean_sessions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "sessions"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze sessions data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Endpoint not ingested or empty",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_datetime(df, ["date_start", "date_end"])
    log = log_schema_prep(log, table, len(df), datetime_cols=["date_start", "date_end"])

    df, log = drop_rows_missing_keys(
        df,
        ["session_key"],
        log,
        table,
        "SIL_NULL_SESSION_KEY",
        "session_key is the primary join key for all session tables",
        rejected,
    )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    df, log = drop_key_duplicates(df, ["session_key"], log, table, rejected)

    return df.reset_index(drop=True), log
