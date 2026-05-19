"""Clean and standardize OpenF1 weather endpoint."""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.silver.cleaning_common import (
    coerce_datetime,
    coerce_numeric,
    drop_exact_duplicates,
    drop_rows_missing_keys,
    empty_cleaning_log,
    log_rule,
    log_schema_prep,
    standardize_column_names,
)


def clean_weather(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = "weather"
    rejected: list = []
    log = empty_cleaning_log()

    if df.empty:
        log = log_rule(
            log,
            table_name=table,
            rule_id="SIL_EMPTY",
            rule_description="No Bronze weather data",
            rows_before=0,
            rows_after=0,
            severity="low",
            rationale="Gaps reported in missingness; no imputation in Silver",
        )
        return df, log

    df = standardize_column_names(df)
    df = coerce_datetime(df, ["date", "time"])
    df = coerce_numeric(
        df,
        [
            "session_key",
            "air_temperature",
            "track_temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "wind_direction",
            "rainfall",
        ],
    )
    log = log_schema_prep(
        log,
        table,
        len(df),
        numeric_cols=[
            "session_key",
            "air_temperature",
            "track_temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "wind_direction",
            "rainfall",
        ],
        datetime_cols=["date", "time"],
    )

    df, log = drop_rows_missing_keys(
        df,
        ["session_key"],
        log,
        table,
        "SIL_NULL_WEATHER_SESSION",
        "Weather rows must link to a session",
        rejected,
    )

    df, log = drop_exact_duplicates(df, log, table, rejected)

    return df.reset_index(drop=True), log
