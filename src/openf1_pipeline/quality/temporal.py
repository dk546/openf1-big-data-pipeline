"""
Temporal anomaly detection for Silver tables.
"""

from __future__ import annotations

import pandas as pd


def detect_temporal_anomalies(
    df: pd.DataFrame,
    table_name: str,
    sessions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Practical temporal checks per table.

    sessions_df optional for session boundary checks on weather.
    """
    columns_out = [
        "table_name",
        "anomaly_type",
        "column_name",
        "anomaly_count",
        "anomaly_pct",
        "notes",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns_out)

    row_count = len(df)
    rows: list[dict] = []

    def add(anomaly_type: str, column: str, mask: pd.Series, notes: str) -> None:
        count = int(mask.fillna(False).sum())
        if count == 0 and anomaly_type.startswith("unparseable") is False:
            pass
        rows.append(
            {
                "table_name": table_name,
                "anomaly_type": anomaly_type,
                "column_name": column,
                "anomaly_count": count,
                "anomaly_pct": round(count / row_count * 100, 4) if row_count else 0.0,
                "notes": notes,
            }
        )

    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        unparseable = parsed.isna() & df[col].notna()
        add(
            "unparseable_datetime",
            col,
            unparseable,
            "Non-null values that failed datetime parsing",
        )

    if table_name == "sessions":
        if "date_start" in df.columns and "date_end" in df.columns:
            start = pd.to_datetime(df["date_start"], errors="coerce", utc=True)
            end = pd.to_datetime(df["date_end"], errors="coerce", utc=True)
            add(
                "date_start_after_date_end",
                "date_start,date_end",
                start > end,
                "Session start after session end",
            )

    if table_name == "laps" and {"session_key", "driver_number", "lap_number"}.issubset(df.columns):
        sort_cols = ["session_key", "driver_number"]
        date_col = next((c for c in ("date_start", "date") if c in df.columns), None)
        tmp = df.copy()
        tmp["_lap"] = pd.to_numeric(tmp["lap_number"], errors="coerce")
        if date_col:
            tmp = tmp.sort_values(sort_cols + [date_col])
        else:
            tmp = tmp.sort_values(sort_cols + ["_lap"])
        decreases = tmp.groupby(sort_cols)["_lap"].diff() < 0
        add(
            "lap_number_decrease_within_stint",
            "lap_number",
            decreases,
            "Lap number decreased when ordered within session/driver",
        )

    if table_name == "pit" and "lap_number" in df.columns:
        s = pd.to_numeric(df["lap_number"], errors="coerce")
        add("pit_lap_non_positive", "lap_number", s <= 0, "Pit stop lap should be positive")

    if (
        table_name == "weather"
        and sessions_df is not None
        and not sessions_df.empty
        and "session_key" in df.columns
        and "date" in df.columns
    ):
        sess = sessions_df[["session_key", "date_start", "date_end"]].copy()
        sess["date_start"] = pd.to_datetime(sess["date_start"], errors="coerce", utc=True)
        sess["date_end"] = pd.to_datetime(sess["date_end"], errors="coerce", utc=True)
        merged = df.merge(sess, on="session_key", how="left")
        wdate = pd.to_datetime(merged["date"], errors="coerce", utc=True)
        outside = (wdate < merged["date_start"]) | (wdate > merged["date_end"])
        outside = outside & wdate.notna() & merged["date_start"].notna()
        add(
            "weather_outside_session_bounds",
            "date",
            outside,
            "Non-blocking: weather timestamp outside session start/end — often API sampling "
            "or session-boundary timing (see silver_data_quality_notes.csv)",
        )

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns_out)


def build_temporal_anomaly_report(
    tables: dict[str, pd.DataFrame],
    stage: str,
    sessions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Temporal anomalies for all tables."""
    parts: list[pd.DataFrame] = []
    sessions = (
        sessions_df
        if sessions_df is not None and not sessions_df.empty
        else tables.get("sessions")
    )
    for name, df in tables.items():
        report = detect_temporal_anomalies(
            df, name, sessions_df=sessions if name == "weather" else None
        )
        if not report.empty:
            report["stage"] = stage
            parts.append(report)
    if not parts:
        return pd.DataFrame(columns=["table_name", "stage", "anomaly_type"])
    return pd.concat(parts, ignore_index=True)
