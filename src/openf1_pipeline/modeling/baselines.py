"""
Random and heuristic baselines for points_finish classification.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from openf1_pipeline.config import RANDOM_SEED


def random_baseline_predict(y_train: pd.Series, n: int, seed: int = RANDOM_SEED) -> np.ndarray:
    """Predict using training class distribution (or uniform)."""
    # TODO: implement
    raise NotImplementedError("TODO: random_baseline_predict")


def grid_heuristic_predict(grid_position: pd.Series, threshold: int = 10) -> np.ndarray:
    """Predict points_finish = 1 if grid_position <= threshold."""
    # TODO: implement
    raise NotImplementedError("TODO: grid_heuristic_predict")
