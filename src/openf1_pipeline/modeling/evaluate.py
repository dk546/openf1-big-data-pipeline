"""
Model evaluation metrics and report exports.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray | None = None) -> dict[str, float]:
    """Accuracy, precision, recall, F1, ROC-AUC."""
    # TODO: implement with sklearn.metrics
    raise NotImplementedError("TODO: compute_metrics")


def confusion_matrix_table(y_true: np.ndarray, y_pred: np.ndarray) -> pd.DataFrame:
    """TN, FP, FN, TP for reports/model_results/confusion_matrix.csv."""
    # TODO: implement
    raise NotImplementedError("TODO: confusion_matrix_table")


def write_metrics_csv(metrics_rows: list[dict], output_path: Path) -> None:
    """Write baseline, validation, or test metrics CSV."""
    # TODO: implement
    raise NotImplementedError("TODO: write_metrics_csv")


def error_analysis(
    df: pd.DataFrame,
    y_true_col: str,
    y_pred_col: str,
) -> pd.DataFrame:
    """Misclassified driver-race rows for error_analysis.csv."""
    # TODO: implement
    raise NotImplementedError("TODO: error_analysis")


def export_feature_importance(model: Any, feature_names: list[str]) -> pd.DataFrame:
    """Coefficients or importances for feature_importance.csv."""
    # TODO: implement
    raise NotImplementedError("TODO: export_feature_importance")
