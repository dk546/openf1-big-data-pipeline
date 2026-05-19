"""
Train Logistic Regression and Random Forest / LightGBM on Gold features.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def prepare_feature_matrix(
    df: pd.DataFrame,
    feature_columns: list[str],
    target_col: str = "points_finish",
) -> tuple[pd.DataFrame, pd.Series]:
    """Select features and target; handle missing values per documented strategy."""
    # TODO: implement
    raise NotImplementedError("TODO: prepare_feature_matrix")


def train_logistic_regression(X_train, y_train) -> Any:
    """Fit logistic regression with sensible defaults."""
    # TODO: implement
    raise NotImplementedError("TODO: train_logistic_regression")


def train_tree_model(X_train, y_train, model_type: str = "random_forest") -> Any:
    """Fit Random Forest or LightGBM with defaults (no heavy tuning)."""
    # TODO: implement
    raise NotImplementedError("TODO: train_tree_model")
