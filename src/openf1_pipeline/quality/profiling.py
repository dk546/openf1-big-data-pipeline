"""
General pandas-based profiling helpers (Bronze load, Silver/Gold audits).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from openf1_pipeline.utils.io import read_jsonl
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_BRONZE_META_RE = re.compile(
    r"(?:^|[\\/])(?P<endpoint>[^/\\]+)[\\/]year=(?P<year>\d+)"
    r"(?:[\\/]session_key=(?P<session_key>\d+))?"
)


def _parse_path_metadata(file_path: Path) -> dict[str, Any]:
    path_str = str(file_path).replace("\\", "/")
    endpoint = None
    year = None
    session_key = None
    match = _BRONZE_META_RE.search(path_str)
    if match:
        endpoint = match.group("endpoint")
        year = int(match.group("year"))
        sk = match.group("session_key")
        session_key = int(sk) if sk else None
    else:
        parts = path_str.split("/")
        if "bronze" in parts:
            idx = parts.index("bronze")
            if idx + 1 < len(parts):
                endpoint = parts[idx + 1]
        for part in parts:
            if part.startswith("year="):
                year = int(part.split("=", 1)[1])
            if part.startswith("session_key="):
                session_key = int(part.split("=", 1)[1])
    return {"endpoint": endpoint, "year": year, "session_key": session_key}


def load_jsonl_files_to_dataframe(file_paths: list[Path]) -> pd.DataFrame:
    """
    Load multiple JSONL files into one DataFrame with source metadata.

    Adds source_file, and endpoint/year/session_key when parseable from path.
    """
    if not file_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for fp in sorted(file_paths):
        fp = Path(fp)
        if not fp.is_file():
            continue
        records = list(read_jsonl(fp))
        if not records:
            continue
        chunk = pd.DataFrame(records)
        meta = _parse_path_metadata(fp)
        chunk["source_file"] = str(fp)
        if meta["endpoint"] is not None:
            chunk["source_endpoint"] = meta["endpoint"]
        if meta["year"] is not None:
            chunk["source_year"] = meta["year"]
        if meta["session_key"] is not None:
            chunk["source_session_key"] = meta["session_key"]
        frames.append(chunk)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def make_hashable_for_duplicate_checks(value: Any) -> Any:
    """
    Normalize a cell value so pandas duplicate checks can hash/compare it.

    OpenF1 JSON fields (e.g. lap segment arrays) may be list/ndarray/dict objects.
    """
    if value is None:
        return value
    try:
        if pd.isna(value):
            return value
    except (TypeError, ValueError):
        pass

    try:
        if isinstance(value, np.ndarray):
            return json.dumps(value.tolist(), sort_keys=True, default=str)
        if isinstance(value, (list, tuple, set)):
            return json.dumps(list(value), sort_keys=True, default=str)
        if isinstance(value, dict):
            return json.dumps(value, sort_keys=True, default=str)
        hash(value)
        return value
    except TypeError:
        return str(value)
    except Exception:
        return str(value)


def _is_unhashable_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, (list, dict, tuple, set, np.ndarray)):
        return True
    try:
        hash(value)
        return False
    except TypeError:
        return True


def _column_needs_hashable_conversion(series: pd.Series) -> bool:
    if series.dtype != object:
        return False
    sample = series.head(500)
    return any(_is_unhashable_value(v) for v in sample)


def make_dataframe_hashable_for_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """
    Copy DataFrame and convert object columns with unhashable values to stable strings.

    Returns (comparable_df, conversion_applied).
    """
    if df.empty:
        return df.copy(), False

    comparable = df.copy()
    converted = False
    for col in comparable.columns:
        if not _column_needs_hashable_conversion(comparable[col]):
            continue
        converted = True
        comparable[col] = comparable[col].map(make_hashable_for_duplicate_checks)
    return comparable, converted


def _count_full_row_duplicates(df: pd.DataFrame) -> tuple[int, str | None]:
    """
    Count duplicate rows; normalize unhashable nested values for comparison.

    Returns (duplicate_count, duplicate_check_note).
    """
    if df.empty:
        return 0, None

    try:
        return int(df.duplicated().sum()), None
    except TypeError:
        pass

    try:
        comparable, converted = make_dataframe_hashable_for_duplicates(df)
        count = int(comparable.duplicated().sum())
        note = (
            "Converted complex object columns to stable strings for duplicate check"
            if converted
            else None
        )
        return count, note
    except Exception as exc:
        logger.warning(
            "Full-row duplicate count failed (%s rows, %s cols): %s",
            len(df),
            len(df.columns),
            exc,
        )
        return 0, f"duplicate check skipped: {exc}"


def _self_check_duplicate_counting() -> None:
    """Lightweight sanity check: ndarray/list columns must not break duplicate counts."""
    df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "arr": [np.array([1, 2]), np.array([1, 2]), np.array([3])],
            "segments": [[{"segment": 1}], [{"segment": 1}], [{"segment": 2}]],
        }
    )
    count, note = _count_full_row_duplicates(df)
    if count != 1:
        raise AssertionError(f"expected 1 duplicate row, got {count} (note={note})")
    if note is None:
        raise AssertionError("expected conversion note for complex columns")


def profile_table(df: pd.DataFrame, table_name: str) -> dict[str, Any]:
    """High-level table profile with missing and duplicate cell stats."""
    row_count = len(df)
    column_count = len(df.columns) if not df.empty else 0
    duplicate_full_rows, _ = _count_full_row_duplicates(df)
    total_cells = row_count * column_count
    total_missing_cells = int(df.isna().sum().sum()) if not df.empty else 0
    missing_cell_pct = round(total_missing_cells / total_cells * 100, 4) if total_cells else 0.0

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "duplicate_full_rows": duplicate_full_rows,
        "total_missing_cells": total_missing_cells,
        "total_cells": total_cells,
        "missing_cell_pct": missing_cell_pct,
    }


def compute_missingness(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Per-column missingness with counts and percentages."""
    columns = [
        "table_name",
        "column_name",
        "dtype",
        "row_count",
        "missing_count",
        "missing_pct",
        "non_missing_count",
        "unique_count",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    n = len(df)
    rows = []
    for col in df.columns:
        missing_count = int(df[col].isna().sum())
        non_missing = n - missing_count
        try:
            unique_count = int(df[col].nunique(dropna=True))
        except TypeError:
            unique_count = int(df[col].astype(str).nunique(dropna=True))
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "dtype": str(df[col].dtype),
                "row_count": n,
                "missing_count": missing_count,
                "missing_pct": round(missing_count / n * 100, 4) if n else 0.0,
                "non_missing_count": non_missing,
                "unique_count": unique_count,
            }
        )
    return pd.DataFrame(rows)


def summarize_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Numeric column distribution summary (percentiles)."""
    columns = [
        "table_name",
        "column_name",
        "count",
        "mean",
        "std",
        "min",
        "p01",
        "p05",
        "p25",
        "median",
        "p75",
        "p95",
        "p99",
        "max",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for col in df.select_dtypes(include="number").columns:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "count": int(series.count()),
                "mean": round(float(series.mean()), 6),
                "std": round(float(series.std()), 6) if len(series) > 1 else 0.0,
                "min": round(float(series.min()), 6),
                "p01": round(float(series.quantile(0.01)), 6),
                "p05": round(float(series.quantile(0.05)), 6),
                "p25": round(float(series.quantile(0.25)), 6),
                "median": round(float(series.median()), 6),
                "p75": round(float(series.quantile(0.75)), 6),
                "p95": round(float(series.quantile(0.95)), 6),
                "p99": round(float(series.quantile(0.99)), 6),
                "max": round(float(series.max()), 6),
            }
        )
    return pd.DataFrame(rows)


def build_table_inventory(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Inventory of tables with row/column/missing/duplicate stats."""
    rows = [profile_table(df, name) for name, df in tables.items()]
    if not rows:
        return pd.DataFrame(
            columns=[
                "table_name",
                "row_count",
                "column_count",
                "duplicate_full_rows",
                "total_missing_cells",
                "total_cells",
                "missing_cell_pct",
            ]
        )
    return pd.DataFrame(rows)
