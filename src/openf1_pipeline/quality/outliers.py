"""
Outlier detection: IQR flags and domain-rule invalid values.
"""

from __future__ import annotations

import pandas as pd


def detect_numeric_outliers_iqr(
    df: pd.DataFrame,
    table_name: str,
    columns: list[str],
    multiplier: float = 1.5,
) -> pd.DataFrame:
    """Flag numeric outliers using the IQR method (report only)."""
    columns_out = [
        "table_name",
        "column_name",
        "method",
        "lower_bound",
        "upper_bound",
        "outlier_count",
        "outlier_pct",
        "min_value",
        "max_value",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns_out)

    row_count = len(df)
    rows: list[dict] = []
    for col in columns:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        valid = series.dropna()
        if valid.empty:
            continue
        q1 = valid.quantile(0.25)
        q3 = valid.quantile(0.75)
        iqr = q3 - q1
        lower = float(q1 - multiplier * iqr)
        upper = float(q3 + multiplier * iqr)
        mask = (series < lower) | (series > upper)
        outlier_count = int(mask.sum())
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "method": "iqr",
                "lower_bound": round(lower, 6),
                "upper_bound": round(upper, 6),
                "outlier_count": outlier_count,
                "outlier_pct": round(outlier_count / row_count * 100, 4) if row_count else 0.0,
                "min_value": round(float(valid.min()), 6),
                "max_value": round(float(valid.max()), 6),
            }
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns_out)


def detect_domain_outliers(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Domain-specific impossible or invalid values."""
    columns_out = [
        "table_name",
        "column_name",
        "rule",
        "invalid_count",
        "invalid_pct",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns_out)

    row_count = len(df)
    rules: list[tuple[str, str, pd.Series]] = []

    def _rule(col: str, desc: str, mask: pd.Series) -> None:
        rules.append((col, desc, mask))

    if table_name == "laps":
        if "lap_number" in df.columns:
            s = pd.to_numeric(df["lap_number"], errors="coerce")
            _rule("lap_number", "lap_number <= 0", s <= 0)
        for col in ("lap_duration", "duration"):
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce")
                _rule(col, f"{col} <= 0", (s <= 0) & s.notna())
        for col in ("duration_sector_1", "duration_sector_2", "duration_sector_3", "sector_1", "sector_2", "sector_3"):
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce")
                _rule(col, f"{col} <= 0", (s <= 0) & s.notna())

    elif table_name == "pit":
        if "lap_number" in df.columns:
            s = pd.to_numeric(df["lap_number"], errors="coerce")
            _rule("lap_number", "lap_number <= 0", s <= 0)
        for col in ("pit_duration", "duration"):
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce")
                _rule(col, f"{col} <= 0", (s <= 0) & s.notna())

    elif table_name == "position":
        if "position" in df.columns:
            s = pd.to_numeric(df["position"], errors="coerce")
            _rule("position", "position <= 0", (s <= 0) & s.notna())

    elif table_name == "session_result":
        if "position" in df.columns:
            s = pd.to_numeric(df["position"], errors="coerce")
            _rule("position", "position <= 0", (s <= 0) & s.notna())
        if "points" in df.columns:
            s = pd.to_numeric(df["points"], errors="coerce")
            _rule("points", "points < 0", (s < 0) & s.notna())

    rows = []
    for col, rule, mask in rules:
        invalid_count = int(mask.fillna(False).sum())
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "rule": rule,
                "invalid_count": invalid_count,
                "invalid_pct": round(invalid_count / row_count * 100, 4) if row_count else 0.0,
            }
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns_out)


def build_outlier_report(
    tables: dict[str, pd.DataFrame],
    stage: str,
    iqr_columns: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    """Combine IQR and domain outlier reports."""
    iqr_columns = iqr_columns or {
        "laps": ["lap_duration", "duration"],
        "pit": ["pit_duration"],
        "weather": ["air_temperature", "track_temperature"],
    }
    parts: list[pd.DataFrame] = []
    for name, df in tables.items():
        cols = iqr_columns.get(name, [])
        if cols:
            iqr_df = detect_numeric_outliers_iqr(df, name, cols)
            if not iqr_df.empty:
                iqr_df["report_type"] = "iqr"
                iqr_df["stage"] = stage
                parts.append(iqr_df)
        domain_df = detect_domain_outliers(df, name)
        if not domain_df.empty:
            domain_df["report_type"] = "domain"
            domain_df["stage"] = stage
            parts.append(domain_df)
    if not parts:
        return pd.DataFrame(columns=["table_name", "stage", "report_type"])
    return pd.concat(parts, ignore_index=True)
