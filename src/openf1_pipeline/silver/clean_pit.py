"""Clean and standardize OpenF1 pit endpoint."""

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
    remove_domain_invalid,
    standardize_column_names,
)


def clean_pit(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "pit"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze pit data (event absence handled in Gold)",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="No pit stops is valid; pit_stop_count derived in Gold",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_numeric(df, ["session_key", "driver_number", "lap_number", "pit_duration", "duration"])
    df = coerce_datetime(df, ["date", "time"])

    df, log = drop_rows_missing_keys(
        df,
        ["session_key", "driver_number"],
        log,
        table,
        "SIL_NULL_PIT_KEYS",
        "Pit rows require session and driver",
        rejected,
    )

    if "lap_number" in df.columns:
        lap = pd.to_numeric(df["lap_number"], errors="coerce")
        df, log = remove_domain_invalid(
            df,
            lap <= 0,
            log,
            table,
            "SIL_PIT_LAP_POS",
            "Remove pit lap_number <= 0",
            "Pit stop must occur on a positive lap",
            rejected,
        )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    if "lap_number" in df.columns:
        df, log = drop_key_duplicates(
            df, ["session_key", "driver_number", "lap_number"], log, table, rejected
        )
    else:
        df, log = drop_key_duplicates(df, ["session_key", "driver_number"], log, table, rejected)

    return df.reset_index(drop=True), log
