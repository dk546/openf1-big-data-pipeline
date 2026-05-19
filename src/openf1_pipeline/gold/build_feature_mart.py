"""
Build Gold driver-race feature mart and points_finish target.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_driver_race_mart(silver_root: Path) -> pd.DataFrame:
    """
    Integrate Silver tables at grain: one row per (session_key, driver_number) for race sessions.

    TODO: joins for grid, pace, pit, weather, race_control, position, circuit, results.
    """
    raise NotImplementedError("TODO: build_driver_race_mart")


def create_points_finish(df: pd.DataFrame, position_col: str = "finishing_position") -> pd.DataFrame:
    """Set points_finish = 1 if top 10 classified; document DNF/DSQ handling."""
    # TODO: implement
    raise NotImplementedError("TODO: create_points_finish")


def save_gold_mart(df: pd.DataFrame, output_path: Path) -> None:
    """Write data/gold/driver_race_mart.parquet."""
    # TODO: implement
    raise NotImplementedError("TODO: save_gold_mart")


def write_target_distribution(df: pd.DataFrame, output_path: Path) -> None:
    """Write reports/data_quality/gold_target_distribution.csv."""
    # TODO: implement
    raise NotImplementedError("TODO: write_target_distribution")
