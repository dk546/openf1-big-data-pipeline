"""
Missing value detection and before/after Silver reports.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def compute_missingness(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Per-column null count and percentage."""
    # TODO: implement
    raise NotImplementedError("TODO: compute_missingness")


def write_missingness_report(
    df: pd.DataFrame,
    table_name: str,
    output_path: Path,
    stage: str,
) -> None:
    """Append or write silver_missingness_{before|after}.csv rows."""
    # TODO: implement
    raise NotImplementedError("TODO: write_missingness_report")
