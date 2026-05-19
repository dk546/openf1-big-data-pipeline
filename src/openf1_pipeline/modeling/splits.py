"""
Season-based train / validation / test splits.
"""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.config import (
    TEST_SEASONS,
    TRAIN_SEASONS,
    VALIDATION_SEASONS,
)


def split_by_season(
    df: pd.DataFrame,
    season_col: str = "season",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return train, validation, test DataFrames using config season lists."""
    # TODO: implement
    raise NotImplementedError("TODO: split_by_season")
