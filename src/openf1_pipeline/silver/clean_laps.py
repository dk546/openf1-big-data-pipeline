"""Clean and standardize OpenF1 laps endpoint."""

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


def clean_laps(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "laps"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze laps data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Endpoint not ingested or empty",
        )
        return df, log

    df = standardize_column_names(df)
    numeric_cols = [
        "session_key",
        "driver_number",
        "lap_number",
        "lap_duration",
        "duration",
        "duration_sector_1",
        "duration_sector_2",
        "duration_sector_3",
        "sector_1",
        "sector_2",
        "sector_3",
    ]
    df = coerce_numeric(df, [c for c in numeric_cols if c in df.columns])
    df = coerce_datetime(df, ["date_start", "date"])

    df, log = drop_rows_missing_keys(
        df,
        ["session_key", "driver_number", "lap_number"],
        log,
        table,
        "SIL_NULL_LAP_KEYS",
        "Lap grain requires session, driver, and lap number",
        rejected,
    )

    if "lap_number" in df.columns:
        lap_num = pd.to_numeric(df["lap_number"], errors="coerce")
        df, log = remove_domain_invalid(
            df,
            lap_num <= 0,
            log,
            table,
            "SIL_LAP_NUM_POS",
            "Remove lap_number <= 0",
            "Lap index must be positive",
            rejected,
        )

    for col in ("lap_duration", "duration"):
        if col in df.columns:
            dur = pd.to_numeric(df[col], errors="coerce")
            df, log = remove_domain_invalid(
                df,
                (dur <= 0) & dur.notna(),
                log,
                table,
                "SIL_LAP_DUR_POS",
                f"Remove {col} <= 0 when non-null",
                "Recorded lap time must be positive; slow SC laps retained if > 0",
                rejected,
            )

    df, log = drop_exact_duplicates(df, log, table, rejected)
    df, log = drop_key_duplicates(
        df, ["session_key", "driver_number", "lap_number"], log, table, rejected
    )

    return df.reset_index(drop=True), log
