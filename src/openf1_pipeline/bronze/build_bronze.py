"""
Bronze layer evidence artifacts: inventory, row counts, schema reports.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from openf1_pipeline.quality.schema_checks import detect_schema_drift
from openf1_pipeline.utils.io import (
    count_jsonl_rows,
    ensure_dir,
    read_jsonl,
    save_dataframe_csv,
)
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_BRONZE_PATH_RE = re.compile(
    r"bronze[\\/](?P<endpoint>[^/\\]+)[\\/]year=(?P<year>\d+)"
    r"(?:[\\/]session_key=(?P<session_key>\d+))?"
)


def _parse_bronze_path(file_path: Path, bronze_dir: Path) -> dict[str, Any]:
    rel = str(file_path.relative_to(bronze_dir)).replace("\\", "/")
    endpoint = file_path.parts[-3] if "year=" in rel else file_path.parent.parent.name
    year = None
    session_key = None

    match = _BRONZE_PATH_RE.search(str(file_path).replace("\\", "/"))
    if match:
        endpoint = match.group("endpoint")
        year = int(match.group("year"))
        sk = match.group("session_key")
        session_key = int(sk) if sk else None
    else:
        parts = rel.split("/")
        if parts:
            endpoint = parts[0]
        for part in parts:
            if part.startswith("year="):
                year = int(part.split("=", 1)[1])
            if part.startswith("session_key="):
                session_key = int(part.split("=", 1)[1])

    return {
        "endpoint": endpoint,
        "year": year,
        "session_key": session_key,
        "file_path": str(file_path),
    }


def discover_bronze_files(bronze_dir: Path) -> pd.DataFrame:
    """List all JSONL files under Bronze with size and modified time."""
    bronze_dir = Path(bronze_dir)
    rows: list[dict[str, Any]] = []

    if not bronze_dir.is_dir():
        return pd.DataFrame(
            columns=[
                "endpoint",
                "year",
                "session_key",
                "file_path",
                "file_size_bytes",
                "modified_time_utc",
            ]
        )

    for file_path in sorted(bronze_dir.rglob("*.jsonl")):
        meta = _parse_bronze_path(file_path, bronze_dir)
        stat = file_path.stat()
        rows.append(
            {
                **meta,
                "file_size_bytes": stat.st_size,
                "modified_time_utc": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
            }
        )

    return pd.DataFrame(rows)


def compute_bronze_row_counts(bronze_dir: Path) -> pd.DataFrame:
    """Count rows per Bronze JSONL file."""
    inventory = discover_bronze_files(bronze_dir)
    if inventory.empty:
        return pd.DataFrame(
            columns=[
                "endpoint",
                "year",
                "session_key",
                "file_path",
                "row_count",
                "file_size_bytes",
            ]
        )

    rows = []
    for _, item in inventory.iterrows():
        path = Path(item["file_path"])
        rows.append(
            {
                "endpoint": item["endpoint"],
                "year": item["year"],
                "session_key": item["session_key"],
                "file_path": item["file_path"],
                "row_count": count_jsonl_rows(path),
                "file_size_bytes": item["file_size_bytes"],
            }
        )
    return pd.DataFrame(rows)


def _python_type_name(value: Any) -> str:
    if value is None:
        return "null"
    return type(value).__name__


def infer_jsonl_schema(file_path: Path, sample_size: int = 1000) -> dict[str, Any]:
    """
    Infer column names and observed types from a JSONL sample.

    Returns dict with keys: columns (column -> type stats), sample_size_used.
    """
    file_path = Path(file_path)
    column_types: dict[str, set[str]] = defaultdict(set)
    non_null_counts: dict[str, int] = defaultdict(int)
    sample_used = 0

    for record in read_jsonl(file_path):
        if sample_used >= sample_size:
            break
        sample_used += 1
        for key, value in record.items():
            column_types[key].add(_python_type_name(value))
            if value is not None:
                non_null_counts[key] += 1

    columns = {}
    for col, types in column_types.items():
        columns[col] = {
            "observed_types": "|".join(sorted(types)),
            "non_null_count": non_null_counts.get(col, 0),
        }

    return {
        "file_path": str(file_path),
        "sample_size_used": sample_used,
        "columns": columns,
    }


def build_bronze_schema_report(bronze_dir: Path) -> pd.DataFrame:
    """
    Aggregate per-file schema inference into endpoint-column rows.
    """
    inventory = discover_bronze_files(bronze_dir)
    if inventory.empty:
        return pd.DataFrame(
            columns=[
                "endpoint",
                "column_name",
                "observed_types",
                "files_seen",
                "non_null_sample_count",
                "sample_rows_checked",
            ]
        )

    agg: dict[tuple[str, str], dict[str, Any]] = {}

    for _, item in inventory.iterrows():
        endpoint = item["endpoint"]
        path = Path(item["file_path"])
        inferred = infer_jsonl_schema(path)
        for col_name, stats in inferred["columns"].items():
            key = (endpoint, col_name)
            if key not in agg:
                agg[key] = {
                    "endpoint": endpoint,
                    "column_name": col_name,
                    "observed_types": set(),
                    "files_seen": 0,
                    "non_null_sample_count": 0,
                    "sample_rows_checked": 0,
                }
            entry = agg[key]
            entry["files_seen"] += 1
            entry["observed_types"].update(stats["observed_types"].split("|"))
            entry["non_null_sample_count"] += stats["non_null_count"]
            entry["sample_rows_checked"] += inferred["sample_size_used"]

    rows = []
    for entry in agg.values():
        rows.append(
            {
                "endpoint": entry["endpoint"],
                "column_name": entry["column_name"],
                "observed_types": "|".join(sorted(entry["observed_types"])),
                "files_seen": entry["files_seen"],
                "non_null_sample_count": entry["non_null_sample_count"],
                "sample_rows_checked": entry["sample_rows_checked"],
            }
        )

    return pd.DataFrame(rows).sort_values(["endpoint", "column_name"]).reset_index(drop=True)


def discover_bronze_files_spark_or_python(bronze_dir: Path) -> pd.DataFrame:
    """Filesystem discovery (Python/pathlib) — metadata only."""
    return discover_bronze_files(bronze_dir)


def compute_bronze_row_counts_spark(spark, bronze_dir: Path) -> pd.DataFrame:
    """Count Bronze JSONL rows per file using Spark."""
    from openf1_pipeline.utils.spark import read_spark_jsonl, safe_to_pandas

    inventory = discover_bronze_files(bronze_dir)
    if inventory.empty:
        return compute_bronze_row_counts(bronze_dir)

    rows: list[dict[str, Any]] = []
    for endpoint in sorted(inventory["endpoint"].dropna().unique()):
        ep_files = inventory.loc[inventory["endpoint"] == endpoint, "file_path"].tolist()
        for fp in ep_files:
            meta = inventory.loc[inventory["file_path"] == fp].iloc[0]
            sdf = read_spark_jsonl(spark, [Path(fp)])
            count = sdf.count() if sdf is not None else 0
            rows.append(
                {
                    "endpoint": meta["endpoint"],
                    "year": meta["year"],
                    "session_key": meta["session_key"],
                    "file_path": meta["file_path"],
                    "row_count": count,
                    "file_size_bytes": meta["file_size_bytes"],
                }
            )
    return pd.DataFrame(rows)


def build_bronze_schema_report_spark(spark, bronze_dir: Path) -> pd.DataFrame:
    """Infer Bronze schemas endpoint-by-endpoint using Spark."""
    from pyspark.sql import functions as F

    from openf1_pipeline.utils.spark import read_spark_jsonl, safe_to_pandas

    inventory = discover_bronze_files(bronze_dir)
    if inventory.empty:
        return build_bronze_schema_report(bronze_dir)

    agg_rows: list[dict[str, Any]] = []
    for endpoint in sorted(inventory["endpoint"].dropna().unique()):
        ep_files = [
            Path(p)
            for p in inventory.loc[inventory["endpoint"] == endpoint, "file_path"].tolist()
        ]
        sdf = read_spark_jsonl(spark, ep_files)
        if sdf is None or sdf.rdd.isEmpty():
            continue
        sample_n = min(1000, sdf.count())
        sample = sdf.limit(sample_n)
        dtypes = dict(sample.dtypes)
        for col_name, dtype in dtypes.items():
            non_null = sample.filter(F.col(col_name).isNotNull()).count()
            agg_rows.append(
                {
                    "endpoint": endpoint,
                    "column_name": col_name,
                    "observed_types": dtype,
                    "files_seen": len(ep_files),
                    "non_null_sample_count": non_null,
                    "sample_rows_checked": sample_n,
                }
            )
    if not agg_rows:
        return build_bronze_schema_report(bronze_dir)
    return pd.DataFrame(agg_rows).sort_values(["endpoint", "column_name"]).reset_index(drop=True)


def generate_bronze_reports_spark(
    spark,
    bronze_dir: Path,
    data_quality_reports_dir: Path,
    schemas_dir: Path,
) -> dict[str, Any]:
    """Spark-first Bronze evidence reports (CSV export via pandas)."""
    bronze_dir = Path(bronze_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    schemas_dir = Path(schemas_dir)
    ensure_dir(data_quality_reports_dir)
    ensure_dir(schemas_dir)

    inventory = discover_bronze_files_spark_or_python(bronze_dir)
    row_counts = compute_bronze_row_counts_spark(spark, bronze_dir)
    schema_report = build_bronze_schema_report_spark(spark, bronze_dir)
    schema_drift = detect_schema_drift(schema_report)

    inventory_path = data_quality_reports_dir / "bronze_file_inventory.csv"
    row_counts_path = data_quality_reports_dir / "bronze_row_counts.csv"
    schema_report_path = data_quality_reports_dir / "bronze_schema_report.csv"
    schema_artifact_path = schemas_dir / "bronze_schema_report.csv"
    drift_path = data_quality_reports_dir / "bronze_schema_drift.csv"

    save_dataframe_csv(inventory, inventory_path)
    save_dataframe_csv(row_counts, row_counts_path)
    save_dataframe_csv(schema_report, schema_report_path)
    save_dataframe_csv(schema_report, schema_artifact_path)
    save_dataframe_csv(schema_drift, drift_path)

    summary = {
        "engine": "spark",
        "bronze_files": len(inventory),
        "total_rows": int(row_counts["row_count"].sum()) if not row_counts.empty else 0,
        "endpoints": sorted(inventory["endpoint"].dropna().unique().tolist())
        if not inventory.empty
        else [],
        "schema_columns": len(schema_report),
        "schema_drift_flags": int(schema_drift["possible_schema_drift_flag"].sum())
        if not schema_drift.empty and "possible_schema_drift_flag" in schema_drift.columns
        else 0,
    }

    paths = {
        "bronze_file_inventory": str(inventory_path),
        "bronze_row_counts": str(row_counts_path),
        "bronze_schema_report": str(schema_report_path),
        "bronze_schema_artifact": str(schema_artifact_path),
        "bronze_schema_drift": str(drift_path),
    }
    logger.info("Bronze Spark reports generated: %s", summary)
    return {"paths": paths, "summary": summary}


def generate_bronze_reports_pandas(
    bronze_dir: Path,
    data_quality_reports_dir: Path,
    schemas_dir: Path,
) -> dict[str, Any]:
    """Pandas fallback for Bronze evidence reports."""
    bronze_dir = Path(bronze_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    schemas_dir = Path(schemas_dir)
    ensure_dir(data_quality_reports_dir)
    ensure_dir(schemas_dir)

    inventory = discover_bronze_files(bronze_dir)
    row_counts = compute_bronze_row_counts(bronze_dir)
    schema_report = build_bronze_schema_report(bronze_dir)
    schema_drift = detect_schema_drift(schema_report)

    inventory_path = data_quality_reports_dir / "bronze_file_inventory.csv"
    row_counts_path = data_quality_reports_dir / "bronze_row_counts.csv"
    schema_report_path = data_quality_reports_dir / "bronze_schema_report.csv"
    schema_artifact_path = schemas_dir / "bronze_schema_report.csv"
    drift_path = data_quality_reports_dir / "bronze_schema_drift.csv"

    save_dataframe_csv(inventory, inventory_path)
    save_dataframe_csv(row_counts, row_counts_path)
    save_dataframe_csv(schema_report, schema_report_path)
    save_dataframe_csv(schema_report, schema_artifact_path)
    save_dataframe_csv(schema_drift, drift_path)

    summary = {
        "engine": "pandas",
        "bronze_files": len(inventory),
        "total_rows": int(row_counts["row_count"].sum()) if not row_counts.empty else 0,
        "endpoints": sorted(inventory["endpoint"].dropna().unique().tolist())
        if not inventory.empty
        else [],
        "schema_columns": len(schema_report),
        "schema_drift_flags": int(schema_drift["possible_schema_drift_flag"].sum())
        if not schema_drift.empty and "possible_schema_drift_flag" in schema_drift.columns
        else 0,
    }

    paths = {
        "bronze_file_inventory": str(inventory_path),
        "bronze_row_counts": str(row_counts_path),
        "bronze_schema_report": str(schema_report_path),
        "bronze_schema_artifact": str(schema_artifact_path),
        "bronze_schema_drift": str(drift_path),
    }
    logger.info("Bronze pandas reports generated: %s", summary)
    return {"paths": paths, "summary": summary}


def generate_bronze_reports(
    bronze_dir: Path,
    data_quality_reports_dir: Path,
    schemas_dir: Path,
    engine: str = "spark",
    spark=None,
) -> dict[str, Any]:
    """
    Write Bronze evidence CSVs. Default engine is Spark; pandas fallback on failure.
    """
    if engine == "spark":
        try:
            from openf1_pipeline.utils.spark import get_spark

            spark = spark or get_spark()
            return generate_bronze_reports_spark(
                spark, bronze_dir, data_quality_reports_dir, schemas_dir
            )
        except Exception as exc:
            logger.warning("Bronze Spark reporting failed; falling back to pandas: %s", exc)
    return generate_bronze_reports_pandas(bronze_dir, data_quality_reports_dir, schemas_dir)
