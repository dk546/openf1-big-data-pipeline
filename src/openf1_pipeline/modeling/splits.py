"""
Season-based train / validation / test splits.
"""

from __future__ import annotations

import pandas as pd

from openf1_pipeline.config import TEST_SEASONS, TRAIN_SEASONS, VALIDATION_SEASONS
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def _coerce_season_series(df: pd.DataFrame, season_col: str) -> pd.Series:
    if season_col not in df.columns:
        raise ValueError(f"Season column '{season_col}' not found in DataFrame.")
    return pd.to_numeric(df[season_col], errors="coerce").astype("Int64")


def create_season_split(
    df: pd.DataFrame,
    train_seasons: list[int] | None = None,
    validation_seasons: list[int] | None = None,
    test_seasons: list[int] | None = None,
    season_col: str = "session_year",
) -> dict[str, pd.DataFrame]:
    """
    Split Gold mart by season lists. No silent fallback if a split is empty.
    """
    train_seasons = train_seasons if train_seasons is not None else TRAIN_SEASONS
    validation_seasons = validation_seasons if validation_seasons is not None else VALIDATION_SEASONS
    test_seasons = test_seasons if test_seasons is not None else TEST_SEASONS

    seasons = _coerce_season_series(df, season_col)
    work = df.copy()
    work["_split_season"] = seasons

    splits = {
        "train": work.loc[work["_split_season"].isin(train_seasons)].drop(columns=["_split_season"]),
        "validation": work.loc[work["_split_season"].isin(validation_seasons)].drop(
            columns=["_split_season"]
        ),
        "test": work.loc[work["_split_season"].isin(test_seasons)].drop(columns=["_split_season"]),
    }

    available = sorted(work["_split_season"].dropna().unique().tolist())
    for name, part in splits.items():
        if part.empty:
            logger.warning(
                "Season split '%s' is empty (seasons=%s; available in data=%s)",
                name,
                {"train": train_seasons, "validation": validation_seasons, "test": test_seasons}[name],
                available,
            )
        else:
            logger.info("Split '%s': %s rows", name, len(part))

    return splits


def create_fallback_time_split(
    df: pd.DataFrame,
    date_col: str | None = None,
    train_frac: float = 0.6,
    val_frac: float = 0.2,
) -> dict[str, pd.DataFrame]:
    """
    Optional fallback when season split is unusable.

    Sorts by date (or session_key) and assigns contiguous time blocks.
    Document any use in model_run_manifest.json — not the primary MBA split.
    """
    if df.empty:
        logger.warning("Fallback time split: empty DataFrame.")
        return {"train": df.copy(), "validation": df.copy(), "test": df.copy()}

    work = df.copy()
    sort_col = date_col if date_col and date_col in work.columns else "session_key"
    if sort_col not in work.columns:
        raise ValueError(f"Neither date_col={date_col!r} nor session_key found for fallback split.")
    work = work.sort_values(sort_col).reset_index(drop=True)

    n = len(work)
    train_end = int(n * train_frac)
    val_end = train_end + int(n * val_frac)

    splits = {
        "train": work.iloc[:train_end].copy(),
        "validation": work.iloc[train_end:val_end].copy(),
        "test": work.iloc[val_end:].copy(),
    }
    for name, part in splits.items():
        logger.warning("Fallback time split '%s': %s rows (sort_col=%s)", name, len(part), sort_col)
    return splits


def split_by_season(
    df: pd.DataFrame,
    season_col: str = "session_year",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Backward-compatible alias returning (train, validation, test) tuple."""
    parts = create_season_split(df, season_col=season_col)
    return parts["train"], parts["validation"], parts["test"]
