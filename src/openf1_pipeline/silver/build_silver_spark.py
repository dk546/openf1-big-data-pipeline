"""
Spark-first Silver cleaning: Bronze JSONL → cleaned Parquet + DQ CSV reports.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from openf1_pipeline.quality.duplicates import build_duplicate_report
from openf1_pipeline.quality.outliers import build_outlier_report
from openf1_pipeline.quality.referential_integrity import build_referential_integrity_report
from openf1_pipeline.quality.temporal import build_temporal_anomaly_report
from openf1_pipeline.silver.build_silver import (
    SILVER_ENDPOINT_ORDER,
    build_cleaning_impact_summary,
    discover_bronze_jsonl_by_endpoint,
)
from openf1_pipeline.quality.silver_dq_notes import (
    build_rejected_records_summary,
    build_silver_data_quality_notes,
)
from openf1_pipeline.silver.cleaning_common import CLEANING_LOG_COLUMNS
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv
from openf1_pipeline.utils.logging import get_logger
from openf1_pipeline.utils.spark import (
    read_spark_jsonl,
    safe_to_pandas,
    spark_path,
    write_empty_parquet_with_schema,
    write_spark_dataframe,
)

logger = get_logger(__name__)


def _silver_empty_schemas():
    """Minimal schemas for empty Silver Parquet writes (Spark rejects zero-column frames)."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    long = LongType()
    s = StringType()

    return {
        "meetings": StructType([StructField("meeting_key", long, True)]),
        "sessions": StructType(
            [
                StructField("session_key", long, True),
                StructField("meeting_key", long, True),
            ]
        ),
        "drivers": StructType(
            [
                StructField("session_key", long, True),
                StructField("driver_number", long, True),
            ]
        ),
        "laps": StructType(
            [
                StructField("session_key", long, True),
                StructField("driver_number", long, True),
                StructField("lap_number", long, True),
            ]
        ),
        "pit": StructType(
            [
                StructField("session_key", long, True),
                StructField("driver_number", long, True),
            ]
        ),
        "weather": StructType([StructField("session_key", long, True)]),
        "position": StructType(
            [
                StructField("session_key", long, True),
                StructField("driver_number", long, True),
            ]
        ),
        "race_control": StructType([StructField("session_key", long, True)]),
        "session_result": StructType(
            [
                StructField("session_key", long, True),
                StructField("driver_number", long, True),
            ]
        ),
        "starting_grid": StructType(
            [
                StructField("session_key", long, True),
                StructField("meeting_key", long, True),
                StructField("driver_number", long, True),
                StructField("position", long, True),
                StructField("source_endpoint", s, True),
                StructField("source_year", long, True),
                StructField("source_session_key", long, True),
            ]
        ),
    }


SILVER_EMPTY_SCHEMAS = _silver_empty_schemas()


def _empty_df(spark: SparkSession, endpoint: str) -> DataFrame:
    schema = SILVER_EMPTY_SCHEMAS[endpoint]
    return spark.createDataFrame([], schema)


def _write_silver_table(spark: SparkSession, sdf: DataFrame, path: Path, endpoint: str) -> None:
    schema = SILVER_EMPTY_SCHEMAS[endpoint]
    if len(sdf.columns) == 0:
        write_empty_parquet_with_schema(spark, path, schema)
    else:
        write_spark_dataframe(sdf, path, empty_schema=schema, spark=spark)


def _rule_log(
    table: str,
    rule_id: str,
    description: str,
    rows_before: int,
    rows_after: int,
    severity: str = "medium",
    rationale: str = "",
    *,
    values_imputed: int = 0,
    columns_affected: str = "",
) -> dict[str, Any]:
    return {
        "table_name": table,
        "rule_id": rule_id,
        "rule_description": description,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_removed": rows_before - rows_after,
        "values_imputed": values_imputed,
        "columns_affected": columns_affected,
        "severity": severity,
        "rationale": rationale,
    }


def _spark_step(
    logs: list[dict[str, Any]],
    sdf: DataFrame,
    table: str,
    rule_id: str,
    description: str,
    transform,
    *,
    columns_affected: str = "",
    severity: str = "medium",
    rationale: str = "",
) -> DataFrame:
    """Apply one cleaning transform and append a granular rule log row."""
    before = sdf.count()
    sdf = transform(sdf)
    after = sdf.count()
    logs.append(
        _rule_log(
            table,
            rule_id,
            description,
            before,
            after,
            severity,
            rationale,
            columns_affected=columns_affected,
        )
    )
    return sdf


def _snake_columns(sdf: DataFrame) -> DataFrame:
    for col in sdf.columns:
        new_name = col.strip().lower().replace(" ", "_")
        if new_name != col:
            sdf = sdf.withColumnRenamed(col, new_name)
    return sdf


def _cast_cols(sdf: DataFrame, cols: list[str], dtype: str = "double") -> DataFrame:
    for c in cols:
        if c in sdf.columns:
            sdf = sdf.withColumn(c, F.col(c).cast(dtype))
    return sdf


def _drop_null_keys(sdf: DataFrame, keys: list[str]) -> DataFrame:
    present = [k for k in keys if k in sdf.columns]
    if not present:
        return sdf
    cond = None
    for k in present:
        c = F.col(k).isNotNull()
        cond = c if cond is None else (cond & c)
    return sdf.filter(cond)


def _dedupe_exact(sdf: DataFrame) -> DataFrame:
    return sdf.dropDuplicates()


def _dedupe_keys(sdf: DataFrame, keys: list[str]) -> DataFrame:
    present = [k for k in keys if k in sdf.columns]
    if not present:
        return sdf
    w = Window.partitionBy(*[F.col(k) for k in present]).orderBy(F.lit(1))
    return sdf.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def load_bronze_endpoint_spark(
    spark: SparkSession, bronze_dir: Path, endpoint: str
) -> DataFrame | None:
    files = discover_bronze_jsonl_by_endpoint(bronze_dir).get(endpoint, [])
    if not files:
        return None
    sdf = read_spark_jsonl(spark, files)
    if sdf is None:
        return None
    return sdf.withColumn("source_file", F.input_file_name())


def clean_meetings_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "meetings"
    logs: list[dict[str, Any]] = []
    spark = sdf.sparkSession
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(spark, table), [_rule_log(table, "SIL_EMPTY", "No Bronze meetings", 0, 0)]
    num_cols = ["meeting_key", "year"]
    sdf = _spark_step(
        logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast meeting_key and year to long",
        lambda d: _cast_cols(d, num_cols, "long"),
        columns_affected=",".join(num_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null meeting_key",
        lambda d: _drop_null_keys(d, ["meeting_key"]),
        columns_affected="meeting_key",
        severity="high",
        rationale="Meeting grain requires meeting_key",
    )
    sdf = _spark_step(
        logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact, severity="medium"
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on meeting_key",
        lambda d: _dedupe_keys(d, ["meeting_key"]),
        columns_affected="meeting_key",
        severity="high",
    )
    return sdf, logs


def clean_sessions_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "sessions"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze sessions", 0, 0)]
    num_cols = ["session_key", "meeting_key", "year"]
    ts_cols = ["date_start", "date_end"]

    def _cast_sessions(d: DataFrame) -> DataFrame:
        out = _cast_cols(d, num_cols, "long")
        for dc in ts_cols:
            if dc in out.columns:
                out = out.withColumn(dc, F.to_timestamp(F.col(dc)))
        return out

    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_SCHEMA",
        "Cast keys to long and session dates to timestamp",
        _cast_sessions,
        columns_affected=",".join(num_cols + ts_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key",
        lambda d: _drop_null_keys(d, ["session_key"]),
        columns_affected="session_key",
        severity="high",
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on session_key",
        lambda d: _dedupe_keys(d, ["session_key"]),
        columns_affected="session_key",
        severity="high",
    )
    return sdf, logs


def clean_drivers_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "drivers"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze drivers", 0, 0)]
    key_cols = ["session_key", "driver_number", "meeting_key"]
    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast driver and session keys to long",
        lambda d: _cast_cols(d, key_cols, "long"),
        columns_affected=",".join(key_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key or driver_number",
        lambda d: _drop_null_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on session_key and driver_number",
        lambda d: _dedupe_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
    )
    return sdf, logs


def clean_laps_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "laps"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze laps", 0, 0)]
    num_cols = [
        "session_key",
        "driver_number",
        "lap_number",
        "lap_duration",
        "duration",
        "duration_sector_1",
        "duration_sector_2",
        "duration_sector_3",
    ]
    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast lap timing and key fields to numeric types",
        lambda d: _cast_cols(d, [c for c in num_cols if c in d.columns]),
        columns_affected=",".join(num_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows missing session_key, driver_number, or lap_number",
        lambda d: _drop_null_keys(d, ["session_key", "driver_number", "lap_number"]),
        columns_affected="session_key,driver_number,lap_number",
        severity="high",
        rationale="Lap grain requires session, driver, and lap number",
    )
    if "lap_number" in sdf.columns:
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_LAP_NUM_POS",
            "Remove lap_number <= 0",
            lambda d: d.filter(F.col("lap_number") > 0),
            columns_affected="lap_number",
            severity="high",
            rationale="Lap index must be positive",
        )

    def _filter_positive_duration(d: DataFrame) -> DataFrame:
        out = d
        for col in ("lap_duration", "duration"):
            if col in out.columns:
                out = out.filter((F.col(col).isNull()) | (F.col(col) > 0))
        return out

    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_LAP_DUR_POS",
        "Remove non-null lap_duration or duration <= 0",
        _filter_positive_duration,
        columns_affected="lap_duration,duration",
        severity="high",
        rationale="Recorded lap time must be positive when present",
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on session_key, driver_number, lap_number",
        lambda d: _dedupe_keys(d, ["session_key", "driver_number", "lap_number"]),
        columns_affected="session_key,driver_number,lap_number",
        severity="high",
    )
    return sdf, logs


def clean_pit_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "pit"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze pit", 0, 0)]
    cast_cols = ["session_key", "driver_number", "lap_number", "pit_duration", "lane_duration"]
    dedupe_keys = ["session_key", "driver_number", "lap_number"]
    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast pit keys and duration fields to numeric types",
        lambda d: _cast_cols(d, cast_cols),
        columns_affected=",".join(cast_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key or driver_number",
        lambda d: _drop_null_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
    )
    if "lap_number" in sdf.columns:
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_PIT_LAP_POS",
            "Remove non-null lap_number <= 0",
            lambda d: d.filter((F.col("lap_number").isNull()) | (F.col("lap_number") > 0)),
            columns_affected="lap_number",
            severity="high",
        )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on session_key, driver_number, lap_number when present",
        lambda d: _dedupe_keys(d, [k for k in dedupe_keys if k in d.columns]),
        columns_affected=",".join(dedupe_keys),
        severity="high",
    )
    return sdf, logs


def clean_weather_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "weather"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze weather", 0, 0)]
    num_cols = [
        "session_key",
        "air_temperature",
        "track_temperature",
        "humidity",
        "pressure",
        "rainfall",
    ]

    def _cast_weather(d: DataFrame) -> DataFrame:
        out = _cast_cols(d, num_cols)
        if "date" in out.columns:
            out = out.withColumn("date", F.to_timestamp(F.col("date")))
        return out

    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_SCHEMA",
        "Cast weather measures and parse date to timestamp",
        _cast_weather,
        columns_affected=",".join(num_cols + ["date"]),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key",
        lambda d: _drop_null_keys(d, ["session_key"]),
        columns_affected="session_key",
        severity="high",
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    return sdf, logs


def clean_position_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "position"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze position", 0, 0)]
    num_cols = ["session_key", "driver_number", "position", "meeting_key"]

    def _cast_position(d: DataFrame) -> DataFrame:
        out = _cast_cols(d, num_cols, "long")
        if "date" in out.columns:
            out = out.withColumn("date", F.to_timestamp(F.col("date")))
        return out

    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_SCHEMA",
        "Cast position keys and parse observation date to timestamp",
        _cast_position,
        columns_affected=",".join(num_cols + ["date"]),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key or driver_number",
        lambda d: _drop_null_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
    )
    if "position" in sdf.columns:
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_POS_POSITIVE",
            "Remove non-null position <= 0",
            lambda d: d.filter((F.col("position").isNull()) | (F.col("position") > 0)),
            columns_affected="position",
            severity="high",
        )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    if all(k in sdf.columns for k in ("session_key", "driver_number", "date")):
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_DUP_KEY",
            "Deduplicate on session_key, driver_number, date",
            lambda d: _dedupe_keys(d, ["session_key", "driver_number", "date"]),
            columns_affected="session_key,driver_number,date",
            severity="high",
        )
    return sdf, logs


def clean_race_control_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "race_control"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [_rule_log(table, "SIL_EMPTY", "No Bronze race_control", 0, 0)]
    num_cols = ["session_key", "meeting_key"]

    def _cast_rc(d: DataFrame) -> DataFrame:
        out = _cast_cols(d, num_cols, "long")
        if "date" in out.columns:
            out = out.withColumn("date", F.to_timestamp(F.col("date")))
        return out

    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_SCHEMA",
        "Cast keys and parse message date to timestamp",
        _cast_rc,
        columns_affected=",".join(num_cols + ["date"]),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key",
        lambda d: _drop_null_keys(d, ["session_key"]),
        columns_affected="session_key",
        severity="high",
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    return sdf, logs


def clean_session_result_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "session_result"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [
            _rule_log(
                table,
                "SIL_EMPTY",
                "No Bronze session_result",
                0,
                0,
                "high",
                "Required for Gold target",
            )
        ]
    cast_cols = [
        "session_key",
        "driver_number",
        "position",
        "points",
        "number_of_laps",
        "meeting_key",
    ]
    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast result keys and outcome fields to long",
        lambda d: _cast_cols(d, cast_cols, "long"),
        columns_affected=",".join(cast_cols),
    )
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DROP_NULL_KEYS",
        "Drop rows with null session_key or driver_number",
        lambda d: _drop_null_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
        rationale="Driver-session grain required for Gold mart",
    )
    if "position" in sdf.columns:
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_RES_POS_POSITIVE",
            "Remove non-null position <= 0",
            lambda d: d.filter((F.col("position").isNull()) | (F.col("position") > 0)),
            columns_affected="position",
            severity="high",
        )
    if "points" in sdf.columns:
        sdf = _spark_step(
            logs,
            sdf,
            table,
            "SIL_RES_POINTS_NONNEG",
            "Remove rows with points < 0",
            lambda d: d.filter((F.col("points").isNull()) | (F.col("points") >= 0)),
            columns_affected="points",
            severity="high",
        )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_DUP_KEY",
        "Deduplicate on session_key and driver_number",
        lambda d: _dedupe_keys(d, ["session_key", "driver_number"]),
        columns_affected="session_key,driver_number",
        severity="high",
    )
    return sdf, logs


def clean_starting_grid_spark(sdf: DataFrame) -> tuple[DataFrame, list[dict[str, Any]]]:
    table = "starting_grid"
    logs: list[dict[str, Any]] = []
    if sdf is None or sdf.rdd.isEmpty():
        return _empty_df(sdf.sparkSession, table), [
            _rule_log(
                table,
                "SIL_OPTIONAL_EMPTY",
                "Optional starting_grid — no Bronze files or zero rows",
                0,
                0,
                "low",
                "OpenF1 may return 404; empty schema Parquet written for downstream joins",
            )
        ]
    cast_cols = ["session_key", "driver_number", "position"]
    sdf = _spark_step(logs, sdf, table, "SIL_RENAME", "Standardize column names to snake_case", _snake_columns)
    sdf = _spark_step(
        logs,
        sdf,
        table,
        "SIL_CAST_NUM",
        "Cast grid keys and position to long",
        lambda d: _cast_cols(d, cast_cols, "long"),
        columns_affected=",".join(cast_cols),
    )
    sdf = _spark_step(logs, sdf, table, "SIL_DUP_FULL", "Remove exact duplicate rows", _dedupe_exact)
    return sdf, logs


SPARK_CLEANERS = {
    "meetings": clean_meetings_spark,
    "sessions": clean_sessions_spark,
    "drivers": clean_drivers_spark,
    "laps": clean_laps_spark,
    "pit": clean_pit_spark,
    "weather": clean_weather_spark,
    "position": clean_position_spark,
    "race_control": clean_race_control_spark,
    "session_result": clean_session_result_spark,
    "starting_grid": clean_starting_grid_spark,
}


def spark_missingness(sdf: DataFrame, table_name: str) -> pd.DataFrame:
    total = sdf.count()
    if total == 0:
        return pd.DataFrame(
            columns=[
                "table_name",
                "column_name",
                "dtype",
                "missing_count",
                "missing_pct",
                "non_missing_count",
                "unique_count",
            ]
        )
    rows = []
    for col in sdf.columns:
        nulls = sdf.filter(F.col(col).isNull()).count()
        non_null = total - nulls
        uniq = sdf.select(col).distinct().count()
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "dtype": dict(sdf.dtypes).get(col, ""),
                "missing_count": nulls,
                "missing_pct": round(nulls / total * 100, 4),
                "non_missing_count": non_null,
                "unique_count": uniq,
            }
        )
    return pd.DataFrame(rows)


def spark_inventory(tables: dict[str, DataFrame]) -> pd.DataFrame:
    rows = []
    for name, sdf in tables.items():
        rows.append(
            {
                "table_name": name,
                "row_count": sdf.count(),
                "column_count": len(sdf.columns),
            }
        )
    return pd.DataFrame(rows)


def spark_tables_to_pandas(
    spark: SparkSession,
    silver_dir: Path,
    max_rows: int = 500_000,
) -> dict[str, pd.DataFrame]:
    """Load cleaned Silver Parquet for pandas report helpers (report boundary only)."""
    tables: dict[str, pd.DataFrame] = {}
    for ep in SILVER_ENDPOINT_ORDER:
        path = silver_dir / f"{ep}_clean.parquet"
        if not path.exists():
            tables[ep] = pd.DataFrame()
            continue
        sdf = spark.read.parquet(spark_path(path))
        n = sdf.count()
        if n <= max_rows:
            tables[ep] = sdf.toPandas()
        else:
            logger.warning("Sampling %s for pandas reports: %s rows", ep, n)
            tables[ep] = safe_to_pandas(sdf, max_rows=max_rows)
    return tables


def run_silver_cleaning_spark(
    spark: SparkSession,
    bronze_dir: Path,
    silver_dir: Path,
    data_quality_reports_dir: Path,
) -> dict[str, Any]:
    """Spark-first Silver pipeline."""
    bronze_dir = Path(bronze_dir)
    silver_dir = Path(silver_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    ensure_dir(silver_dir)
    ensure_dir(data_quality_reports_dir)

    if not bronze_dir.is_dir() or not any(bronze_dir.iterdir()):
        raise FileNotFoundError(
            f"Bronze data not found at {bronze_dir}. Run 01_ingestion_bronze.ipynb first."
        )

    bronze_spark: dict[str, DataFrame] = {}
    for endpoint in SILVER_ENDPOINT_ORDER:
        sdf = load_bronze_endpoint_spark(spark, bronze_dir, endpoint)
        if sdf is not None:
            bronze_spark[endpoint] = sdf
        else:
            bronze_spark[endpoint] = _empty_df(spark, endpoint)

    missingness_before = pd.concat(
        [spark_missingness(sdf, ep) for ep, sdf in bronze_spark.items()],
        ignore_index=True,
    )
    inventory_before = spark_inventory(bronze_spark)

    silver_spark: dict[str, DataFrame] = {}
    all_logs: list[dict[str, Any]] = []

    for endpoint in SILVER_ENDPOINT_ORDER:
        raw = bronze_spark.get(endpoint)
        if raw is None:
            silver_spark[endpoint] = _empty_df(spark, endpoint)
            _write_silver_table(spark, silver_spark[endpoint], silver_dir / f"{endpoint}_clean.parquet", endpoint)
            continue
        cleaned, logs = SPARK_CLEANERS[endpoint](raw)
        silver_spark[endpoint] = cleaned
        all_logs.extend(logs)
        out = silver_dir / f"{endpoint}_clean.parquet"
        _write_silver_table(spark, cleaned, out, endpoint)
        logger.info("Silver Spark saved %s (%s rows)", endpoint, cleaned.count())

    cleaning_rules = pd.DataFrame(all_logs) if all_logs else pd.DataFrame(columns=CLEANING_LOG_COLUMNS)

    missingness_after = pd.concat(
        [spark_missingness(sdf, ep) for ep, sdf in silver_spark.items()],
        ignore_index=True,
    )

    # Pandas report helpers at report boundary (RI, outliers, temporal) — cleaning was Spark
    # TODO: For full-season runs, prefer Spark-native key-duplicate counts on large tables
    # (laps/position); pandas full-row checks here are acceptable for smoke-sized samples.
    bronze_pandas = {
        ep: (
            bronze_spark[ep].toPandas()
            if bronze_spark[ep].count() <= 500_000
            else safe_to_pandas(bronze_spark[ep], 500_000)
        )
        for ep in SILVER_ENDPOINT_ORDER
    }
    silver_pandas = spark_tables_to_pandas(spark, silver_dir)

    duplicate_before = build_duplicate_report(bronze_pandas, stage="before")
    duplicate_after = build_duplicate_report(silver_pandas, stage="after")
    outlier_before = build_outlier_report(bronze_pandas, stage="before")
    outlier_after = build_outlier_report(silver_pandas, stage="after")
    temporal_before = build_temporal_anomaly_report(
        bronze_pandas, stage="before", sessions_df=bronze_pandas.get("sessions")
    )
    temporal_after = build_temporal_anomaly_report(
        silver_pandas, stage="after", sessions_df=silver_pandas.get("sessions")
    )
    referential_before = build_referential_integrity_report(bronze_pandas, stage="before")
    referential_after = build_referential_integrity_report(silver_pandas, stage="after")

    duplicate_report = pd.concat([duplicate_before, duplicate_after], ignore_index=True)
    outlier_report = pd.concat([outlier_before, outlier_after], ignore_index=True)
    temporal_report = pd.concat([temporal_before, temporal_after], ignore_index=True)
    referential_report = pd.concat([referential_before, referential_after], ignore_index=True)
    impact_summary = build_cleaning_impact_summary(bronze_pandas, silver_pandas)

    rejected_summary = build_rejected_records_summary(cleaning_rules, SILVER_ENDPOINT_ORDER)
    dq_notes = build_silver_data_quality_notes()

    paths = {
        "silver_table_inventory": data_quality_reports_dir / "silver_table_inventory.csv",
        "silver_missingness_before": data_quality_reports_dir / "silver_missingness_before.csv",
        "silver_missingness_after": data_quality_reports_dir / "silver_missingness_after.csv",
        "silver_duplicate_report": data_quality_reports_dir / "silver_duplicate_report.csv",
        "silver_outlier_report": data_quality_reports_dir / "silver_outlier_report.csv",
        "silver_temporal_anomaly_report": data_quality_reports_dir
        / "silver_temporal_anomaly_report.csv",
        "silver_referential_integrity_report": data_quality_reports_dir
        / "silver_referential_integrity_report.csv",
        "silver_cleaning_rules": data_quality_reports_dir / "silver_cleaning_rules.csv",
        "silver_cleaning_impact_summary": data_quality_reports_dir
        / "silver_cleaning_impact_summary.csv",
        "silver_rejected_records_summary": data_quality_reports_dir
        / "silver_rejected_records_summary.csv",
        "silver_data_quality_notes": data_quality_reports_dir
        / "silver_data_quality_notes.csv",
    }

    save_dataframe_csv(inventory_before, paths["silver_table_inventory"])
    save_dataframe_csv(missingness_before, paths["silver_missingness_before"])
    save_dataframe_csv(missingness_after, paths["silver_missingness_after"])
    save_dataframe_csv(duplicate_report, paths["silver_duplicate_report"])
    save_dataframe_csv(outlier_report, paths["silver_outlier_report"])
    save_dataframe_csv(temporal_report, paths["silver_temporal_anomaly_report"])
    save_dataframe_csv(referential_report, paths["silver_referential_integrity_report"])
    save_dataframe_csv(cleaning_rules, paths["silver_cleaning_rules"])
    save_dataframe_csv(impact_summary, paths["silver_cleaning_impact_summary"])
    save_dataframe_csv(rejected_summary, paths["silver_rejected_records_summary"])
    save_dataframe_csv(dq_notes, paths["silver_data_quality_notes"])

    summary = {
        "engine": "spark",
        "tables_loaded": len(bronze_spark),
        "total_rows_before": int(inventory_before["row_count"].sum())
        if not inventory_before.empty
        else 0,
        "total_rows_after": int(impact_summary["rows_after"].sum())
        if not impact_summary.empty
        else 0,
        "session_result_rows_after": int(silver_pandas.get("session_result", pd.DataFrame()).shape[0]),
    }
    logger.info("Silver Spark cleaning complete: %s", summary)
    return {
        "paths": {k: str(v) for k, v in paths.items()},
        "summary": summary,
        "silver_tables": {k: silver_spark[k].count() for k in silver_spark},
    }
