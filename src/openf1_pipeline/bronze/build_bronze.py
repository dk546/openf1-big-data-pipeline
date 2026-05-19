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
    allow_fallback: bool = False,
) -> dict[str, Any]:
    """
    Write Bronze evidence CSVs (Spark by default).

    When ``allow_fallback=False``, Spark failures raise immediately.
    """
    if engine == "spark":
        try:
            from openf1_pipeline.utils.spark import get_spark

            spark = spark or get_spark()
            return generate_bronze_reports_spark(
                spark, bronze_dir, data_quality_reports_dir, schemas_dir
            )
        except Exception as exc:
            if not allow_fallback:
                raise RuntimeError(
                    "Bronze Spark reporting failed with allow_fallback=False. "
                    f"Fix the Spark error before retrying. Original error: {exc}"
                ) from exc
            logger.warning(
                "Bronze Spark reporting failed; allow_fallback=True — pandas fallback: %s",
                exc,
            )
    return generate_bronze_reports_pandas(bronze_dir, data_quality_reports_dir, schemas_dir)


# ---------------------------------------------------------------------------
# Manifest <-> Bronze file reconciliation
# ---------------------------------------------------------------------------

RECONCILIATION_COLUMNS = [
    "endpoint",
    "year",
    "session_key",
    "manifest_status",
    "manifest_record_count",
    "manifest_output_path",
    "file_exists",
    "file_path",
    "file_row_count",
    "row_count_delta",
    "reconciliation_status",
    "issue_type",
    "notes",
]

# Endpoints that may legitimately be missing for some sessions. Kept in sync
# with ``openf1_pipeline.ingestion.ingest.OPTIONAL_SESSION_ENDPOINTS``.
RECONCILIATION_OPTIONAL_ENDPOINTS = frozenset({"starting_grid"})

# Endpoints that are session-keyed. Used to distinguish global vs session rows
# when reconciling. Kept in sync with ``openf1_pipeline.config.SESSION_ENDPOINTS``.
_RECONCILIATION_SESSION_ENDPOINTS_FALLBACK = frozenset(
    {
        "drivers",
        "laps",
        "pit",
        "weather",
        "position",
        "race_control",
        "session_result",
        "starting_grid",
    }
)


def _coerce_year(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_session_key(value: Any) -> int | None:
    return _coerce_year(value)


def _session_endpoint_set() -> frozenset[str]:
    try:
        from openf1_pipeline.config import SESSION_ENDPOINTS

        return frozenset(SESSION_ENDPOINTS)
    except Exception:  # pragma: no cover - defensive
        return _RECONCILIATION_SESSION_ENDPOINTS_FALLBACK


def _collapse_manifest_to_latest(manifest_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce manifest to one row per ``(endpoint, year, session_key)``.

    Prefers a ``success`` row; otherwise keeps the latest by
    ``ingestion_timestamp_utc``.
    """
    if manifest_df.empty:
        return manifest_df.copy()

    df = manifest_df.copy()
    df["__endpoint"] = df["endpoint"].astype(str)
    df["__year"] = df["year"].apply(_coerce_year)
    df["__session_key"] = df["session_key"].apply(_coerce_session_key)
    df["__status_rank"] = (df["status"] == "success").astype(int)
    df["__ts"] = pd.to_datetime(
        df.get("ingestion_timestamp_utc"), errors="coerce", utc=True
    )

    df = df.sort_values(["__status_rank", "__ts"], ascending=[False, False])
    collapsed = df.drop_duplicates(
        subset=["__endpoint", "__year", "__session_key"], keep="first"
    ).copy()

    collapsed = collapsed.rename(
        columns={
            "status": "manifest_status",
            "record_count": "manifest_record_count",
            "output_path": "manifest_output_path",
        }
    )
    keep_cols = [
        "__endpoint",
        "__year",
        "__session_key",
        "manifest_status",
        "manifest_record_count",
        "manifest_output_path",
        "error_message",
        "ingestion_timestamp_utc",
    ]
    keep_cols = [c for c in keep_cols if c in collapsed.columns]
    return collapsed[keep_cols].reset_index(drop=True)


def _collapse_files_to_unique(files_df: pd.DataFrame) -> pd.DataFrame:
    """One row per ``(endpoint, year, session_key)`` from the on-disk inventory."""
    if files_df.empty:
        return files_df.copy()
    df = files_df.copy()
    df["__endpoint"] = df["endpoint"].astype(str)
    df["__year"] = df["year"].apply(_coerce_year)
    df["__session_key"] = df["session_key"].apply(_coerce_session_key)
    # Multiple files for the same (endpoint, year, session_key) shouldn't happen
    # in normal Bronze layout. If they do, keep the largest row count.
    return (
        df.sort_values("row_count", ascending=False)
        .drop_duplicates(subset=["__endpoint", "__year", "__session_key"], keep="first")
        .reset_index(drop=True)
    )


def _decide_reconciliation(
    endpoint: str,
    is_session_endpoint: bool,
    manifest_status: str | float | None,
    manifest_record_count: float | None,
    file_exists: bool,
    file_row_count: float | None,
) -> tuple[str, str, str]:
    """
    Return ``(reconciliation_status, issue_type, notes)`` for one reconciled row.

    Mirrors the rules in the user spec:

    - ``matched``: manifest success + file + matching row count.
    - ``row_count_mismatch``: manifest success + file + count mismatch.
    - ``manifest_success_missing_file``: manifest success + no file.
    - ``failed_manifest_file_exists``: manifest non-success + file exists.
    - ``stale_file_not_in_success_manifest``: file exists + no manifest row.
    - ``optional_missing``: manifest failed + no file + optional endpoint.
    - ``manifest_failed_no_file``: manifest failed + no file + required endpoint.
    - ``unknown``: anything that does not match the above.
    """
    optional = endpoint in RECONCILIATION_OPTIONAL_ENDPOINTS
    has_manifest = manifest_status is not None and not (
        isinstance(manifest_status, float) and pd.isna(manifest_status)
    )
    is_success = has_manifest and str(manifest_status) == "success"

    if file_exists and is_success:
        mr = int(manifest_record_count) if manifest_record_count is not None else None
        fr = int(file_row_count) if file_row_count is not None else None
        if mr is not None and fr is not None and mr == fr:
            return "matched", "none", ""
        return (
            "row_count_mismatch",
            "row_mismatch",
            f"manifest={mr} file={fr} delta={None if (mr is None or fr is None) else fr - mr}",
        )

    if file_exists and has_manifest and not is_success:
        return (
            "failed_manifest_file_exists",
            "failed_but_file_present",
            "manifest failed but JSONL exists — likely stale / pre-existing file",
        )

    if file_exists and not has_manifest:
        return (
            "stale_file_not_in_success_manifest",
            "stale_file",
            "file present but no manifest row — orphaned / pre-existing",
        )

    if not file_exists and is_success:
        return (
            "manifest_success_missing_file",
            "missing_file",
            "manifest claims success but no JSONL on disk",
        )

    if not file_exists and has_manifest and not is_success:
        if optional:
            return (
                "optional_missing",
                "optional_endpoint",
                f"optional endpoint {endpoint} failed and no file — expected/acceptable",
            )
        if is_session_endpoint:
            return (
                "manifest_failed_no_file",
                "manifest_only",
                "required session endpoint failed; consider targeted retry",
            )
        return (
            "manifest_failed_no_file",
            "manifest_only",
            "global endpoint failed; rerun the relevant ingestion call",
        )

    return ("unknown", "none", "unhandled combination")


def reconcile_manifest_to_bronze_files(
    manifest_path: Path,
    bronze_dir: Path,
    row_counts_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Compare ``ingestion_manifest.csv`` against the actual JSONL inventory.

    Joins manifest rows and on-disk file rows by ``(endpoint, year, session_key)``
    and classifies every pair into a ``reconciliation_status`` / ``issue_type``
    bucket. Designed to surface the kinds of inconsistencies seen in the full
    2023–2025 Bronze run — stale smoke files, manifest-vs-file row mismatches,
    failures that nonetheless left data on Drive, missing required files, and
    accepted optional gaps (``starting_grid``).

    Parameters
    ----------
    manifest_path : Path
        Path to ``ingestion_manifest.csv`` (the original Bronze manifest).
    bronze_dir : Path
        Root Bronze data directory (e.g. ``data/bronze``).
    row_counts_df : pd.DataFrame | None
        Optional pre-computed Bronze row counts (the same shape produced by
        ``compute_bronze_row_counts`` / ``compute_bronze_row_counts_spark``).
        When ``None``, this function falls back to Python row counting. Passing
        a Spark-counted DataFrame keeps reconciliation aligned with
        ``bronze_row_counts.csv`` without re-reading every JSONL file.

    Returns
    -------
    pd.DataFrame
        One row per ``(endpoint, year, session_key)`` triple seen in either the
        manifest or on disk, with the columns listed in
        ``RECONCILIATION_COLUMNS``.
    """
    manifest_path = Path(manifest_path)
    bronze_dir = Path(bronze_dir)

    if manifest_path.is_file():
        manifest_df = pd.read_csv(manifest_path)
    else:
        logger.warning("Manifest not found at %s; reconciling with files only.", manifest_path)
        manifest_df = pd.DataFrame(
            columns=[
                "endpoint",
                "year",
                "session_key",
                "output_path",
                "record_count",
                "status",
                "error_message",
                "ingestion_timestamp_utc",
            ]
        )

    if row_counts_df is None:
        files_df = compute_bronze_row_counts(bronze_dir)
    else:
        files_df = row_counts_df.copy()

    manifest_slim = _collapse_manifest_to_latest(manifest_df)
    files_slim = _collapse_files_to_unique(files_df)

    if manifest_slim.empty and files_slim.empty:
        return pd.DataFrame(columns=RECONCILIATION_COLUMNS)

    merged = pd.merge(
        manifest_slim,
        files_slim,
        on=["__endpoint", "__year", "__session_key"],
        how="outer",
        suffixes=("_m", "_f"),
    )

    session_endpoint_set = _session_endpoint_set()
    rows: list[dict[str, Any]] = []

    for _, item in merged.iterrows():
        endpoint = str(item["__endpoint"])
        year = item["__year"]
        session_key = item["__session_key"]
        is_session_endpoint = endpoint in session_endpoint_set

        manifest_status = item.get("manifest_status")
        manifest_record_count = item.get("manifest_record_count")
        if pd.isna(manifest_record_count):
            manifest_record_count = None
        manifest_output_path = item.get("manifest_output_path")
        if pd.isna(manifest_output_path):
            manifest_output_path = None

        file_path = item.get("file_path")
        file_exists = isinstance(file_path, str) and file_path != ""
        if file_exists:
            file_row_count_val = item.get("row_count")
            file_row_count = (
                int(file_row_count_val) if pd.notna(file_row_count_val) else None
            )
        else:
            file_path = None
            file_row_count = None

        status, issue_type, notes = _decide_reconciliation(
            endpoint=endpoint,
            is_session_endpoint=is_session_endpoint,
            manifest_status=manifest_status,
            manifest_record_count=manifest_record_count,
            file_exists=file_exists,
            file_row_count=file_row_count,
        )

        row_count_delta: int | None = None
        if manifest_record_count is not None and file_row_count is not None:
            row_count_delta = int(file_row_count) - int(manifest_record_count)

        rows.append(
            {
                "endpoint": endpoint,
                "year": _coerce_year(year),
                "session_key": _coerce_session_key(session_key),
                "manifest_status": (
                    None if manifest_status is None or pd.isna(manifest_status)
                    else str(manifest_status)
                ),
                "manifest_record_count": (
                    int(manifest_record_count)
                    if manifest_record_count is not None
                    else None
                ),
                "manifest_output_path": manifest_output_path,
                "file_exists": bool(file_exists),
                "file_path": file_path,
                "file_row_count": file_row_count,
                "row_count_delta": row_count_delta,
                "reconciliation_status": status,
                "issue_type": issue_type,
                "notes": notes,
            }
        )

    out = pd.DataFrame(rows, columns=RECONCILIATION_COLUMNS)
    return (
        out.sort_values(
            ["reconciliation_status", "endpoint", "year", "session_key"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def summarize_bronze_reconciliation(
    reconciliation_df: pd.DataFrame,
) -> dict[str, pd.DataFrame | dict[str, int]]:
    """
    Roll up a reconciliation DataFrame into compact summaries.

    Returns a dict with:

    - ``by_status``: counts by ``reconciliation_status`` (DataFrame).
    - ``by_endpoint``: counts by ``endpoint`` × ``reconciliation_status``
      (DataFrame).
    - ``by_issue_type``: counts by ``issue_type`` (DataFrame).
    - ``totals``: dict with named totals
      (``matched``, ``stale_files``, ``missing_files``, ``row_mismatches``,
       ``failed_but_file_present``, ``optional_missing``,
       ``total_rows_reconciled``).
    """
    if reconciliation_df.empty:
        return {
            "by_status": pd.DataFrame(columns=["reconciliation_status", "count"]),
            "by_endpoint": pd.DataFrame(
                columns=["endpoint", "reconciliation_status", "count"]
            ),
            "by_issue_type": pd.DataFrame(columns=["issue_type", "count"]),
            "totals": {
                "matched": 0,
                "stale_files": 0,
                "missing_files": 0,
                "row_mismatches": 0,
                "failed_but_file_present": 0,
                "optional_missing": 0,
                "total_rows_reconciled": 0,
            },
        }

    by_status = (
        reconciliation_df.groupby("reconciliation_status")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )
    by_endpoint = (
        reconciliation_df.groupby(["endpoint", "reconciliation_status"])
        .size()
        .reset_index(name="count")
        .sort_values(["endpoint", "reconciliation_status"])
        .reset_index(drop=True)
    )
    by_issue_type = (
        reconciliation_df.groupby("issue_type")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    def _count_status(name: str) -> int:
        return int((reconciliation_df["reconciliation_status"] == name).sum())

    totals = {
        "matched": _count_status("matched"),
        "stale_files": _count_status("stale_file_not_in_success_manifest"),
        "missing_files": _count_status("manifest_success_missing_file"),
        "row_mismatches": _count_status("row_count_mismatch"),
        "failed_but_file_present": _count_status("failed_manifest_file_exists"),
        "optional_missing": _count_status("optional_missing"),
        "manifest_failed_no_file": _count_status("manifest_failed_no_file"),
        "unknown": _count_status("unknown"),
        "total_rows_reconciled": int(len(reconciliation_df)),
    }

    return {
        "by_status": by_status,
        "by_endpoint": by_endpoint,
        "by_issue_type": by_issue_type,
        "totals": totals,
    }


def generate_bronze_reconciliation_reports(
    manifest_path: Path,
    bronze_dir: Path,
    data_quality_reports_dir: Path,
    row_counts_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Compute reconciliation and write the two evidence CSVs.

    Writes:

    - ``reports/data_quality/bronze_manifest_file_reconciliation.csv``
    - ``reports/data_quality/bronze_manifest_file_reconciliation_summary.csv``
      (long-form summary with ``scope`` ∈ ``{by_status, by_endpoint,
      by_issue_type, totals}``).

    Returns a dict with ``paths``, ``summary`` (the totals), and ``df``
    (the reconciliation DataFrame, useful in notebooks).
    """
    manifest_path = Path(manifest_path)
    bronze_dir = Path(bronze_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    ensure_dir(data_quality_reports_dir)

    recon_df = reconcile_manifest_to_bronze_files(
        manifest_path=manifest_path,
        bronze_dir=bronze_dir,
        row_counts_df=row_counts_df,
    )
    summary = summarize_bronze_reconciliation(recon_df)

    recon_path = data_quality_reports_dir / "bronze_manifest_file_reconciliation.csv"
    summary_path = (
        data_quality_reports_dir / "bronze_manifest_file_reconciliation_summary.csv"
    )

    save_dataframe_csv(recon_df, recon_path)

    summary_rows: list[dict[str, Any]] = []
    for _, srow in summary["by_status"].iterrows():
        summary_rows.append(
            {
                "scope": "by_status",
                "key": srow["reconciliation_status"],
                "endpoint": "",
                "count": int(srow["count"]),
            }
        )
    for _, srow in summary["by_endpoint"].iterrows():
        summary_rows.append(
            {
                "scope": "by_endpoint",
                "key": srow["reconciliation_status"],
                "endpoint": srow["endpoint"],
                "count": int(srow["count"]),
            }
        )
    for _, srow in summary["by_issue_type"].iterrows():
        summary_rows.append(
            {
                "scope": "by_issue_type",
                "key": srow["issue_type"],
                "endpoint": "",
                "count": int(srow["count"]),
            }
        )
    for k, v in summary["totals"].items():
        summary_rows.append(
            {"scope": "totals", "key": k, "endpoint": "", "count": int(v)}
        )

    summary_df = pd.DataFrame(
        summary_rows, columns=["scope", "key", "endpoint", "count"]
    )
    save_dataframe_csv(summary_df, summary_path)

    paths = {
        "bronze_manifest_file_reconciliation": str(recon_path),
        "bronze_manifest_file_reconciliation_summary": str(summary_path),
    }
    logger.info(
        "Bronze reconciliation: %s rows -> %s; totals=%s",
        len(recon_df),
        recon_path,
        summary["totals"],
    )
    return {"paths": paths, "summary": summary["totals"], "df": recon_df}
