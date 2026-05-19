"""
Path helpers and file read/write for pipeline layers (Colab-friendly).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def ensure_dir(path: Path) -> Path:
    """Create directory if missing; return path."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _matches_keep_pattern(name: str, keep_patterns: list[str] | None) -> bool:
    if not keep_patterns:
        return False
    import fnmatch

    return any(fnmatch.fnmatch(name, pattern) for pattern in keep_patterns)


def clean_directory_contents(path: Path, keep_patterns: list[str] | None = None) -> int:
    """
    Remove all files and subdirectories inside ``path`` (not the root folder itself).

    Handles Spark Parquet directories. Missing folder is created empty.
    """
    import shutil

    path = Path(path)
    if not path.exists():
        ensure_dir(path)
        return 0

    removed = 0
    for item in path.iterdir():
        if _matches_keep_pattern(item.name, keep_patterns):
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
        removed += 1
    logger.info("Cleaned directory contents %s (%s items removed)", path, removed)
    return removed


def ensure_clean_output_dir(path: Path, keep_patterns: list[str] | None = None) -> Path:
    """Create ``path`` if missing, then remove its contents."""
    path = Path(path)
    ensure_dir(path)
    clean_directory_contents(path, keep_patterns=keep_patterns)
    return path


def clean_files_matching(directory: Path, patterns: list[str]) -> int:
    """Remove files in ``directory`` matching any glob pattern (non-recursive by default)."""
    import fnmatch

    directory = Path(directory)
    if not directory.is_dir():
        return 0

    removed = 0
    for item in directory.iterdir():
        if not item.is_file():
            continue
        if any(fnmatch.fnmatch(item.name, pattern) for pattern in patterns):
            item.unlink()
            removed += 1
    if removed:
        logger.info("Removed %s files matching %s from %s", removed, patterns, directory)
    return removed


def save_jsonl(records: list[dict[str, Any]], output_path: Path) -> int:
    """Write one JSON object per line (UTF-8). Returns rows written."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    count = 0
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False, default=str))
            fh.write("\n")
            count += 1
    logger.info("Wrote %s rows to %s", count, output_path)
    return count


def read_jsonl(input_path: Path) -> Iterator[dict[str, Any]]:
    """Yield dicts from a JSONL file."""
    input_path = Path(input_path)
    with input_path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_no} in {input_path}") from exc


def read_jsonl_as_list(input_path: Path) -> list[dict[str, Any]]:
    """Load entire JSONL file into memory."""
    return list(read_jsonl(input_path))


def count_jsonl_rows(input_path: Path) -> int:
    """Count non-empty lines in a JSONL file without parsing JSON."""
    input_path = Path(input_path)
    if not input_path.is_file():
        return 0
    count = 0
    with input_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def save_dataframe_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Write DataFrame to CSV with parent dirs created."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Wrote CSV %s (%s rows)", output_path, len(df))


def prepare_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce mixed-type object columns so Parquet export succeeds."""
    import json

    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            continue
        if out[col].dtype != object:
            continue
        if out[col].apply(lambda v: isinstance(v, (list, dict))).any():
            out[col] = out[col].apply(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )
        numeric = pd.to_numeric(out[col], errors="coerce")
        non_null = out[col].notna().sum()
        if non_null and numeric.notna().sum() < non_null:
            out[col] = out[col].astype(str)
        elif non_null:
            out[col] = numeric
    return out


def save_dataframe_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Write DataFrame to Parquet with parent dirs created."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    prepared = prepare_dataframe_for_parquet(df)
    prepared.to_parquet(output_path, index=False)
    logger.info("Wrote Parquet %s (%s rows)", output_path, len(df))


def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    """Read parquet if file or Spark-style parquet directory exists; otherwise return None."""
    path = Path(path)
    if not (path.is_file() or path.is_dir()):
        return None
    return pd.read_parquet(path)


def write_run_manifest(manifest: dict[str, Any], output_path: Path) -> None:
    """Write run manifest JSON (UTF-8, indented)."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False, default=str)
    logger.info("Wrote manifest %s", output_path)
