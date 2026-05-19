"""
Model evaluation metrics and report exports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv, write_run_manifest
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def compute_classification_metrics(
    y_true: np.ndarray | pd.Series,
    y_pred: np.ndarray | pd.Series,
    y_proba: np.ndarray | None = None,
    model_name: str = "",
    split: str = "",
) -> dict[str, Any]:
    """Return one metrics row for CSV export."""
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    row: dict[str, Any] = {
        "model_name": model_name,
        "split": split,
        "accuracy": round(float(accuracy_score(yt, yp)), 4),
        "precision": round(float(precision_score(yt, yp, zero_division=0)), 4),
        "recall": round(float(recall_score(yt, yp, zero_division=0)), 4),
        "f1": round(float(f1_score(yt, yp, zero_division=0)), 4),
        "roc_auc": None,
        "positive_rate_true": round(float(np.mean(yt)), 4) if len(yt) else None,
        "positive_rate_pred": round(float(np.mean(yp)), 4) if len(yp) else None,
        "n_rows": int(len(yt)),
    }
    if y_proba is not None and len(np.unique(yt)) > 1:
        try:
            row["roc_auc"] = round(float(roc_auc_score(yt, y_proba)), 4)
        except ValueError:
            row["roc_auc"] = None
    return row


def compute_confusion_matrix_table(
    y_true: np.ndarray | pd.Series,
    y_pred: np.ndarray | pd.Series,
    model_name: str = "",
    split: str = "",
) -> pd.DataFrame:
    """Long-form confusion matrix for reports."""
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    cm = confusion_matrix(yt, yp, labels=[0, 1])
    rows = []
    for i, actual in enumerate([0, 1]):
        for j, predicted in enumerate([0, 1]):
            rows.append(
                {
                    "model_name": model_name,
                    "split": split,
                    "actual": actual,
                    "predicted": predicted,
                    "count": int(cm[i, j]),
                }
            )
    return pd.DataFrame(rows)


def build_error_analysis(
    df: pd.DataFrame,
    y_true: np.ndarray | pd.Series,
    y_pred: np.ndarray | pd.Series,
    y_proba: np.ndarray | None,
    model_name: str,
    split: str,
) -> pd.DataFrame:
    """Summarize false positives and false negatives by context columns."""
    work = df.copy()
    work["_y_true"] = np.asarray(y_true)
    work["_y_pred"] = np.asarray(y_pred)
    if y_proba is not None:
        work["_y_proba"] = np.asarray(y_proba)

    work["_error_type"] = np.where(
        (work["_y_true"] == 0) & (work["_y_pred"] == 1),
        "false_positive",
        np.where(
            (work["_y_true"] == 1) & (work["_y_pred"] == 0),
            "false_negative",
            "correct",
        ),
    )
    errors = work.loc[work["_error_type"].isin(["false_positive", "false_negative"])].copy()
    if errors.empty:
        return pd.DataFrame(
            columns=[
                "model_name",
                "split",
                "error_type",
                "group_column",
                "group_value",
                "count",
            ]
        )

    if "first_observed_position" in errors.columns:
        pos = pd.to_numeric(errors["first_observed_position"], errors="coerce")
        errors["_position_bin"] = pd.cut(
            pos,
            bins=[0, 5, 10, 15, 20, 100],
            labels=["1-5", "6-10", "11-15", "16-20", "21+"],
            include_lowest=True,
        )

    group_cols = [
        c
        for c in ("team_name", "circuit_short_name", "session_year", "_position_bin")
        if c in errors.columns
    ]

    rows: list[dict[str, Any]] = []
    for error_type in ("false_positive", "false_negative"):
        subset = errors.loc[errors["_error_type"] == error_type]
        for col in group_cols:
            counts = subset.groupby(col, dropna=False).size().reset_index(name="count")
            for _, r in counts.iterrows():
                rows.append(
                    {
                        "model_name": model_name,
                        "split": split,
                        "error_type": error_type,
                        "group_column": col,
                        "group_value": str(r[col]),
                        "count": int(r["count"]),
                    }
                )
    return pd.DataFrame(rows)


def save_modeling_outputs(
    *,
    baseline_metrics: pd.DataFrame,
    validation_metrics: pd.DataFrame,
    test_metrics: pd.DataFrame,
    confusion_matrix_df: pd.DataFrame,
    error_analysis_df: pd.DataFrame,
    feature_importance_df: pd.DataFrame,
    model_results_dir: Path,
    manifests_dir: Path,
    manifest_extra: dict[str, Any] | None = None,
) -> dict[str, Path]:
    """Write all modeling CSVs and model_run_manifest.json."""
    model_results_dir = Path(model_results_dir)
    manifests_dir = Path(manifests_dir)
    ensure_dir(model_results_dir)
    ensure_dir(manifests_dir)

    paths = {
        "baseline_metrics": model_results_dir / "baseline_metrics.csv",
        "validation_metrics": model_results_dir / "validation_metrics.csv",
        "test_metrics": model_results_dir / "test_metrics.csv",
        "confusion_matrix": model_results_dir / "confusion_matrix.csv",
        "error_analysis": model_results_dir / "error_analysis.csv",
        "feature_importance": model_results_dir / "feature_importance.csv",
        "model_run_manifest": manifests_dir / "model_run_manifest.json",
    }

    save_dataframe_csv(baseline_metrics, paths["baseline_metrics"])
    save_dataframe_csv(validation_metrics, paths["validation_metrics"])
    save_dataframe_csv(test_metrics, paths["test_metrics"])
    save_dataframe_csv(confusion_matrix_df, paths["confusion_matrix"])
    save_dataframe_csv(error_analysis_df, paths["error_analysis"])
    save_dataframe_csv(feature_importance_df, paths["feature_importance"])

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "artifact_paths": {k: str(v) for k, v in paths.items()},
        **(manifest_extra or {}),
    }
    write_run_manifest(manifest, paths["model_run_manifest"])
    logger.info("Modeling outputs saved under %s", model_results_dir)
    return paths
