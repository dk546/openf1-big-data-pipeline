"""
Schema profiling and drift detection (Bronze and future Silver).
"""

from __future__ import annotations

import pandas as pd


def collect_columns_by_endpoint(bronze_schema_report: pd.DataFrame) -> pd.DataFrame:
    """Summarize distinct columns per endpoint from Bronze schema report."""
    if bronze_schema_report.empty:
        return pd.DataFrame(columns=["endpoint", "column_name", "files_seen"])

    return (
        bronze_schema_report.groupby(["endpoint", "column_name"], as_index=False)["files_seen"]
        .max()
        .sort_values(["endpoint", "column_name"])
        .reset_index(drop=True)
    )


def detect_schema_drift(bronze_schema_report: pd.DataFrame) -> pd.DataFrame:
    """
    Flag columns that do not appear in all files for an endpoint.

    coverage_pct = files_seen / max(files_seen) per endpoint.
    """
    if bronze_schema_report.empty:
        return pd.DataFrame(
            columns=[
                "endpoint",
                "column_name",
                "files_seen",
                "max_files_for_endpoint",
                "coverage_pct",
                "possible_schema_drift_flag",
            ]
        )

    max_files = (
        bronze_schema_report.groupby("endpoint")["files_seen"].max().rename(
            "max_files_for_endpoint"
        )
    )
    drift = bronze_schema_report.merge(max_files, on="endpoint", how="left")
    drift["coverage_pct"] = (
        drift["files_seen"] / drift["max_files_for_endpoint"].replace(0, 1) * 100
    ).round(2)
    drift["possible_schema_drift_flag"] = drift["coverage_pct"] < 100.0
    return drift[
        [
            "endpoint",
            "column_name",
            "files_seen",
            "max_files_for_endpoint",
            "coverage_pct",
            "possible_schema_drift_flag",
        ]
    ].sort_values(["endpoint", "coverage_pct", "column_name"])
