"""
Random and heuristic baselines for points_finish classification.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from openf1_pipeline.config import RANDOM_SEED


def random_baseline_predictions(
    y_true: pd.Series | np.ndarray,
    positive_rate: float | None = None,
    random_seed: int = RANDOM_SEED,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Bernoulli random labels. Default positive_rate = observed prevalence in y_true.
    """
    y = np.asarray(y_true)
    n = len(y)
    if positive_rate is None:
        positive_rate = float(np.nanmean(y)) if n else 0.0
    rng = np.random.default_rng(random_seed)
    y_pred = rng.binomial(1, positive_rate, size=n).astype(int)
    y_proba = np.full(n, positive_rate, dtype=float)
    return y_pred, y_proba


def heuristic_position_baseline(
    df: pd.DataFrame,
    threshold: int = 10,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Predict points_finish = 1 if first_observed_position <= threshold; else 0.
    Missing first_observed_position → predict 0.
    """
    if "first_observed_position" not in df.columns:
        n = len(df)
        return np.zeros(n, dtype=int), np.zeros(n, dtype=float)

    pos = pd.to_numeric(df["first_observed_position"], errors="coerce")
    y_pred = np.where(pos.notna() & (pos <= threshold), 1, 0).astype(int)
    y_proba = y_pred.astype(float)
    return y_pred, y_proba


def majority_class_baseline(
    y_train: pd.Series | np.ndarray,
    y_eval: pd.Series | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Predict the majority class from training labels for all eval rows."""
    y_train_arr = np.asarray(y_train)
    y_eval_arr = np.asarray(y_eval)
    if len(y_train_arr) == 0:
        return np.zeros(len(y_eval_arr), dtype=int), np.zeros(len(y_eval_arr), dtype=float)
    values, counts = np.unique(y_train_arr[~np.isnan(y_train_arr)], return_counts=True)
    majority = int(values[np.argmax(counts)])
    rate = float(counts.max() / counts.sum())
    y_pred = np.full(len(y_eval_arr), majority, dtype=int)
    y_proba = np.full(len(y_eval_arr), rate, dtype=float)
    return y_pred, y_proba
