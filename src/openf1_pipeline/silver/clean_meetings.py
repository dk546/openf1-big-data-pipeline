"""Clean and standardize OpenF1 meetings endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_numeric,
    drop_exact_duplicates,
    drop_key_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    standardize_column_names,
)


def clean_meetings(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "meetings"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze meetings data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Endpoint not ingested or empty",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_numeric(df, ["year", "meeting_key", "circuit_key", "country_key"])

    if "meeting_key" in df.columns:
        df, log = drop_rows_missing_keys(
            df,
            ["meeting_key"],
            log,
            table,
            "SIL_NULL_MEETING_KEY",
            "meeting_key is required for calendar joins",
            rejected,
        )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    if "meeting_key" in df.columns:
        df, log = drop_key_duplicates(df, ["meeting_key"], log, table, rejected)

    return df.reset_index(drop=True), log
