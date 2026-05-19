"""
Shared helpers for Silver endpoint cleaning and audit logs.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

CLEANING_LOG_COLUMNS = [
    "table_name",
    "rule_id",
    "rule_description",
    "rows_before",
    "rows_after",
    "rows_removed",
    "values_imputed",
    "columns_affected",
    "severity",
    "rationale",
]

REJECTED_SUMMARY_COLUMNS = [
    "table_name",
    "reason",
    "rule_id",
    "rejected_count",
]


def empty_cleaning_log() -> pd.DataFrame:
    return pd.DataFrame(columns=CLEANING_LOG_COLUMNS)


def log_rule(
    log: pd.DataFrame,
    *,
    table_name: str,
    rule_id: str,
    rule_description: str,
    rows_before: int,
    rows_after: int,
    values_imputed: int = 0,
    columns_affected: str = "",
    severity: str = "medium",
    rationale: str,
) -> pd.DataFrame:
    """Append one cleaning rule row to the log."""
    row = {
        "table_name": table_name,
        "rule_id": rule_id,
        "rule_description": rule_description,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_removed": rows_before - rows_after,
        "values_imputed": values_imputed,
        "columns_affected": columns_affected,
        "severity": severity,
        "rationale": rationale,
    }
    return pd.concat([log, pd.DataFrame([row])], ignore_index=True)


def log_schema_prep(
    log: pd.DataFrame,
    table_name: str,
    row_count: int,
    *,
    numeric_cols: list[str] | None = None,
    datetime_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Log non-row-changing schema steps (rename, cast) for audit granularity."""
    log = log_rule(
        log,
        table_name=table_name,
        rule_id="SIL_RENAME",
        rule_description="Standardize column names to snake_case",
        rows_before=row_count,
        rows_after=row_count,
        columns_affected="*",
        severity="low",
        rationale="Schema consistency for joins and downstream reports",
    )
    if numeric_cols:
        log = log_rule(
            log,
            table_name=table_name,
            rule_id="SIL_CAST_NUM",
            rule_description="Cast numeric fields",
            rows_before=row_count,
            rows_after=row_count,
            columns_affected=",".join(numeric_cols),
            severity="low",
            rationale="Typed columns for domain checks and aggregates",
        )
    if datetime_cols:
        present = [c for c in datetime_cols if c]
        if present:
            log = log_rule(
                log,
                table_name=table_name,
                rule_id="SIL_CAST_TS",
                rule_description="Parse datetime fields to UTC timestamps",
                rows_before=row_count,
                rows_after=row_count,
                columns_affected=",".join(present),
                severity="low",
                rationale="Enable temporal anomaly checks and session-boundary joins",
            )
    return log


def log_rejection(
    rejected: list[dict[str, Any]],
    table_name: str,
    rule_id: str,
    reason: str,
    count: int,
) -> None:
    if count > 0:
        rejected.append(
            {
                "table_name": table_name,
                "reason": reason,
                "rule_id": rule_id,
                "rejected_count": count,
            }
        )


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to snake_case."""
    def to_snake(name: str) -> str:
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
        return name.lower().replace(" ", "_").replace("-", "_")

    df = df.copy()
    df.columns = [to_snake(c) for c in df.columns]
    return df


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def coerce_datetime(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def drop_rows_missing_keys(
    df: pd.DataFrame,
    keys: list[str],
    log: pd.DataFrame,
    table_name: str,
    rule_id: str,
    rationale: str,
    rejected: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove rows with null critical keys."""
    present = [k for k in keys if k in df.columns]
    if not present or df.empty:
        return df, log
    before = len(df)
    mask = df[present].notna().all(axis=1)
    cleaned = df.loc[mask].copy()
    removed = before - len(cleaned)
    log = log_rule(
        log,
        table_name=table_name,
        rule_id=rule_id,
        rule_description=f"Drop rows with null keys: {present}",
        rows_before=before,
        rows_after=len(cleaned),
        columns_affected=",".join(present),
        severity="high",
        rationale=rationale,
    )
    log_rejection(rejected, table_name, rule_id, f"null keys {present}", removed)
    return cleaned, log


def drop_exact_duplicates(
    df: pd.DataFrame,
    log: pd.DataFrame,
    table_name: str,
    rejected: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    before = len(df)
    try:
        cleaned = df.drop_duplicates()
    except TypeError:
        comparable = df.copy()
        for col in comparable.columns:
            if comparable[col].apply(lambda v: isinstance(v, (list, dict))).any():
                comparable[col] = comparable[col].astype(str)
        idx = ~comparable.duplicated(keep="first")
        cleaned = df.loc[idx].copy()
    removed = before - len(cleaned)
    if removed:
        log = log_rule(
            log,
            table_name=table_name,
            rule_id="SIL_DUP_FULL",
            rule_description="Remove exact duplicate rows",
            rows_before=before,
            rows_after=len(cleaned),
            severity="medium",
            rationale="Identical rows add no information and break counts",
        )
        log_rejection(rejected, table_name, "SIL_DUP_FULL", "exact duplicate rows", removed)
    return cleaned, log


def drop_key_duplicates(
    df: pd.DataFrame,
    keys: list[str],
    log: pd.DataFrame,
    table_name: str,
    rejected: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    from openf1_pipeline.quality.duplicates import remove_key_duplicates

    if not all(k in df.columns for k in keys):
        return df, log
    before = len(df)
    cleaned, removed = remove_key_duplicates(df, keys, keep="first")
    if removed:
        log = log_rule(
            log,
            table_name=table_name,
            rule_id="SIL_DUP_KEY",
            rule_description=f"Remove duplicate keys: {keys}",
            rows_before=before,
            rows_after=len(cleaned),
            columns_affected=",".join(keys),
            severity="high",
            rationale="Primary key must be unique per entity",
        )
        log_rejection(rejected, table_name, "SIL_DUP_KEY", f"duplicate keys {keys}", removed)
    return cleaned, log


def remove_domain_invalid(
    df: pd.DataFrame,
    mask: pd.Series,
    log: pd.DataFrame,
    table_name: str,
    rule_id: str,
    description: str,
    rationale: str,
    rejected: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove rows matching domain-invalid mask."""
    before = len(df)
    cleaned = df.loc[~mask.fillna(False)].copy()
    removed = before - len(cleaned)
    if removed:
        log = log_rule(
            log,
            table_name=table_name,
            rule_id=rule_id,
            rule_description=description,
            rows_before=before,
            rows_after=len(cleaned),
            severity="high",
            rationale=rationale,
        )
        log_rejection(rejected, table_name, rule_id, description, removed)
    return cleaned, log
