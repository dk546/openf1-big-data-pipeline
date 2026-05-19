"""
Train Logistic Regression, Random Forest, and LightGBM on Gold features.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from openf1_pipeline.config import RANDOM_SEED
from openf1_pipeline.gold.build_feature_mart import (
    IDENTIFIER_COLUMNS,
    LEAKAGE_FORBIDDEN_COLUMNS,
    TARGET_COLUMN,
)
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

SOURCE_PREFIXES = ("source_",)


def get_model_feature_columns(feature_dictionary: pd.DataFrame) -> list[str]:
    """Select features allowed for modeling from feature dictionary CSV."""
    if feature_dictionary.empty:
        return []
    allowed = feature_dictionary.loc[
        feature_dictionary["allowed_for_modeling"] == True, "feature_name"  # noqa: E712
    ].tolist()
    excluded = IDENTIFIER_COLUMNS | LEAKAGE_FORBIDDEN_COLUMNS | {TARGET_COLUMN}
    cols = []
    for col in allowed:
        if col in excluded:
            continue
        if col.startswith("diagnostic_"):
            continue
        if any(col.startswith(p) for p in SOURCE_PREFIXES):
            continue
        cols.append(col)
    return cols


def _infer_column_types(df: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    numeric: list[str] = []
    categorical: list[str] = []
    for col in feature_columns:
        if col not in df.columns:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric.append(col)
        else:
            categorical.append(col)
    return numeric, categorical


def prepare_model_matrix(
    df: pd.DataFrame,
    feature_columns: list[str],
    target_col: str = TARGET_COLUMN,
) -> tuple[pd.DataFrame, pd.Series]:
    """Separate X and y for modeling."""
    present = [c for c in feature_columns if c in df.columns]
    missing = [c for c in feature_columns if c not in df.columns]
    if missing:
        logger.warning("Feature columns missing from DataFrame: %s", missing)
    X = df[present].copy()
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found.")
    y = df[target_col].astype(int)
    return X, y


def _build_preprocessor(
    df_sample: pd.DataFrame,
    feature_columns: list[str],
    scale_numeric: bool = True,
) -> ColumnTransformer:
    numeric_cols, categorical_cols = _infer_column_types(df_sample, feature_columns)
    transformers: list[tuple[str, Any, list[str]]] = []

    if numeric_cols:
        num_steps: list[tuple[str, Any]] = [("imputer", SimpleImputer(strategy="median"))]
        if scale_numeric:
            num_steps.append(("scaler", StandardScaler()))
        transformers.append(("num", Pipeline(num_steps), numeric_cols))

    if categorical_cols:
        cat_pipe = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
            ]
        )
        transformers.append(("cat", cat_pipe, categorical_cols))

    if not transformers:
        raise ValueError("No numeric or categorical feature columns available for preprocessing.")

    return ColumnTransformer(transformers=transformers, remainder="drop")


def build_logistic_regression_pipeline(
    feature_columns: list[str],
    df_sample: pd.DataFrame,
) -> Pipeline:
    preprocessor = _build_preprocessor(df_sample, feature_columns, scale_numeric=True)
    clf = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        random_state=RANDOM_SEED,
    )
    return Pipeline([("preprocessor", preprocessor), ("classifier", clf)])


def build_random_forest_pipeline(
    feature_columns: list[str],
    df_sample: pd.DataFrame,
) -> Pipeline:
    from sklearn.ensemble import RandomForestClassifier

    preprocessor = _build_preprocessor(df_sample, feature_columns, scale_numeric=False)
    clf = RandomForestClassifier(
        n_estimators=200,
        class_weight="balanced",
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )
    return Pipeline([("preprocessor", preprocessor), ("classifier", clf)])


def build_lightgbm_pipeline(
    feature_columns: list[str],
    df_sample: pd.DataFrame,
) -> Pipeline:
    preprocessor = _build_preprocessor(df_sample, feature_columns, scale_numeric=False)
    clf = LGBMClassifier(
        n_estimators=200,
        learning_rate=0.05,
        random_state=RANDOM_SEED,
        class_weight="balanced",
        verbose=-1,
    )
    return Pipeline([("preprocessor", preprocessor), ("classifier", clf)])


def train_models(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_specs: dict[str, Pipeline] | None = None,
) -> dict[str, Pipeline]:
    """
    Fit sklearn pipelines. model_specs keys become model names in outputs.
    """
    if model_specs is None:
        feature_cols = list(X_train.columns)
        model_specs = {
            "logistic_regression": build_logistic_regression_pipeline(feature_cols, X_train),
            "random_forest": build_random_forest_pipeline(feature_cols, X_train),
            "lightgbm": build_lightgbm_pipeline(feature_cols, X_train),
        }

    fitted: dict[str, Pipeline] = {}
    for name, pipeline in model_specs.items():
        logger.info("Training model: %s (%s rows)", name, len(X_train))
        pipeline.fit(X_train, y_train)
        fitted[name] = pipeline
    return fitted


def extract_feature_importance(
    model_name: str,
    fitted_pipeline: Pipeline,
    feature_columns: list[str],
) -> pd.DataFrame:
    """Extract coefficients or importances after preprocessing."""
    clf = fitted_pipeline.named_steps["classifier"]
    preprocessor = fitted_pipeline.named_steps["preprocessor"]

    try:
        feature_names = preprocessor.get_feature_names_out()
    except Exception:
        feature_names = np.array(feature_columns, dtype=object)

    rows: list[dict[str, Any]] = []
    if hasattr(clf, "feature_importances_"):
        for fname, imp in zip(feature_names, clf.feature_importances_):
            rows.append({"model_name": model_name, "feature_name": fname, "importance": float(imp)})
    elif hasattr(clf, "coef_"):
        coefs = clf.coef_.ravel()
        for fname, coef in zip(feature_names, coefs):
            rows.append({"model_name": model_name, "feature_name": fname, "importance": float(abs(coef))})
    else:
        rows.append(
            {
                "model_name": model_name,
                "feature_name": "_none_",
                "importance": np.nan,
                "note": "Model does not expose feature importances",
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty and "importance" in out.columns:
        out = out.sort_values("importance", ascending=False).reset_index(drop=True)
    return out
