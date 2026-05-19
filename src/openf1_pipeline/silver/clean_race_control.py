"""Clean and standardize OpenF1 race_control endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_datetime,
    drop_exact_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    log_schema_prep,
    standardize_column_names,
)


def clean_race_control(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "race_control"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze race_control data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Endpoint not ingested or empty",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_datetime(df, ["date", "time"])
    log = log_schema_prep(log, table, len(df), datetime_cols=["date", "time"])

    df, log = drop_rows_missing_keys(
        df,
        ["session_key"],
        log,
        table,
        "SIL_NULL_RC_SESSION",
        "Race control messages must link to a session",
        rejected,
    )

    df, log = drop_exact_duplicates(df, log, table, rejected)

    return df.reset_index(drop=True), log
