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
    """Read parquet if file exists; otherwise return None."""
    path = Path(path)
    if not path.is_file():
        return None
    return pd.read_parquet(path)


def write_run_manifest(manifest: dict[str, Any], output_path: Path) -> None:
    """Write run manifest JSON (UTF-8, indented)."""
    output_path = Path(output_path)
    ensure_dir(output_path.parent)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False, default=str)
    logger.info("Wrote manifest %s", output_path)
