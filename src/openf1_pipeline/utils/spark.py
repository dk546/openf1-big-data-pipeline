"""
Local PySpark session helpers (Colab-compatible, no Databricks).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from openf1_pipeline.utils.io import ensure_dir
from openf1_pipeline.utils.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

_SPARK: SparkSession | None = None


def get_spark(
    app_name: str = "openf1-big-data-pipeline",
    shuffle_partitions: int = 8,
) -> SparkSession:
    """Start or return a singleton local ``SparkSession``."""
    global _SPARK
    if _SPARK is not None:
        return _SPARK

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    _SPARK = builder.getOrCreate()
    _SPARK.sparkContext.setLogLevel("WARN")
    logger.info("Spark session started: %s", app_name)
    return _SPARK


def stop_spark(spark: SparkSession | None = None) -> None:
    """Stop the active Spark session."""
    global _SPARK
    target = spark or _SPARK
    if target is not None:
        target.stop()
        logger.info("Spark session stopped")
    _SPARK = None


def spark_path(path: Path | str) -> str:
    """Convert a path to a Spark-friendly URI string."""
    return str(Path(path).resolve()).replace("\\", "/")


def write_empty_parquet_with_schema(
    spark: SparkSession,
    path: Path | str,
    schema,
    mode: str = "overwrite",
) -> None:
    """Write an empty Parquet dataset with an explicit schema (Spark cannot write zero-column frames)."""
    out = Path(path)
    ensure_dir(out.parent)
    empty = spark.createDataFrame([], schema)
    empty.write.mode(mode).parquet(spark_path(out))
    logger.info("Wrote empty Parquet with schema -> %s", out)


def write_spark_dataframe(
    df: DataFrame,
    path: Path | str,
    mode: str = "overwrite",
    *,
    empty_schema=None,
    spark: SparkSession | None = None,
) -> None:
    """Write a Spark DataFrame as Parquet; use empty_schema when df has no columns."""
    out = Path(path)
    ensure_dir(out.parent)
    if len(df.columns) == 0:
        if empty_schema is None:
            raise ValueError(
                f"Cannot write empty DataFrame with no schema to {out}. "
                "Provide empty_schema or use write_empty_parquet_with_schema()."
            )
        session = spark or df.sparkSession
        write_empty_parquet_with_schema(session, out, empty_schema, mode=mode)
        return
    df.write.mode(mode).parquet(spark_path(out))


def read_spark_parquet_if_exists(spark: SparkSession, path: Path | str) -> DataFrame | None:
    """Read Parquet if the path exists; otherwise return None."""
    p = Path(path)
    if not p.exists():
        return None
    return spark.read.parquet(spark_path(p))


def read_spark_jsonl(spark: SparkSession, paths: list[Path | str]) -> DataFrame | None:
    """Read one or more JSONL files as a Spark DataFrame."""
    from pyspark.sql.types import StructType

    if not paths:
        return None
    uri = [spark_path(p) for p in paths]
    try:
        return spark.read.json(uri)
    except Exception as exc:
        logger.warning("Spark JSON read failed (%s paths): %s", len(uri), exc)
        return spark.createDataFrame([], StructType())


def safe_to_pandas(df: DataFrame, max_rows: int | None = None):
    """
    Convert a **small** Spark DataFrame to pandas for CSV export or display.

    Use only for aggregated audit outputs — not for full lap/position tables.
    """
    if max_rows is not None:
        n = df.count()
        if n > max_rows:
            logger.warning(
                "Truncating Spark→pandas conversion: %s rows > max_rows=%s",
                n,
                max_rows,
            )
            df = df.limit(max_rows)
    return df.toPandas()
