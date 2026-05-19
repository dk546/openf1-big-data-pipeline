"""
Silver layer orchestration: Bronze JSONL → cleaned Parquet + DQ reports.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pandas as pd

from openf1_pipeline.quality.duplicates import build_duplicate_report
from openf1_pipeline.quality.outliers import build_outlier_report
from openf1_pipeline.quality.profiling import (
    build_table_inventory,
    compute_missingness,
    load_jsonl_files_to_dataframe,
    profile_table,
)
from openf1_pipeline.quality.referential_integrity import build_referential_integrity_report
from openf1_pipeline.quality.temporal import build_temporal_anomaly_report
from openf1_pipeline.silver.clean_drivers import clean_drivers
from openf1_pipeline.silver.clean_laps import clean_laps
from openf1_pipeline.silver.clean_meetings import clean_meetings
from openf1_pipeline.silver.clean_pit import clean_pit
from openf1_pipeline.silver.clean_position import clean_position
from openf1_pipeline.silver.clean_race_control import clean_race_control
from openf1_pipeline.silver.clean_session_result import clean_session_result
from openf1_pipeline.silver.clean_sessions import clean_sessions
from openf1_pipeline.silver.clean_starting_grid import clean_starting_grid
from openf1_pipeline.silver.clean_weather import clean_weather
from openf1_pipeline.silver.cleaning_common import REJECTED_SUMMARY_COLUMNS
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv, save_dataframe_parquet
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

SILVER_ENDPOINT_ORDER = [
    "meetings",
    "sessions",
    "drivers",
    "laps",
    "pit",
    "weather",
    "position",
    "race_control",
    "session_result",
    "starting_grid",
]

CLEANERS: dict[str, Callable[[pd.DataFrame], tuple[pd.DataFrame, pd.DataFrame]]] = {
    "meetings": clean_meetings,
    "sessions": clean_sessions,
    "drivers": clean_drivers,
    "laps": clean_laps,
    "pit": clean_pit,
    "weather": clean_weather,
    "position": clean_position,
    "race_control": clean_race_control,
    "session_result": clean_session_result,
    "starting_grid": clean_starting_grid,
}


def discover_bronze_jsonl_by_endpoint(bronze_dir: Path) -> dict[str, list[Path]]:
    """Map endpoint name to list of JSONL paths under Bronze."""
    bronze_dir = Path(bronze_dir)
    result: dict[str, list[Path]] = {ep: [] for ep in SILVER_ENDPOINT_ORDER}
    if not bronze_dir.is_dir():
        return result
    for ep_dir in bronze_dir.iterdir():
        if ep_dir.is_dir():
            endpoint = ep_dir.name
            files = sorted(ep_dir.rglob("*.jsonl"))
            if endpoint in result:
                result[endpoint] = files
            else:
                result[endpoint] = files
    return result


def load_bronze_tables(bronze_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all Bronze endpoints into pandas DataFrames."""
    tables: dict[str, pd.DataFrame] = {}
    by_endpoint = discover_bronze_jsonl_by_endpoint(bronze_dir)
    for endpoint in SILVER_ENDPOINT_ORDER:
        files = by_endpoint.get(endpoint, [])
        df = load_jsonl_files_to_dataframe(files)
        tables[endpoint] = df
        logger.info("Loaded Bronze %s: %s rows from %s files", endpoint, len(df), len(files))
    return tables


def _concat_missingness(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts = []
    for name, df in tables.items():
        part = compute_missingness(df, name)
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame(columns=compute_missingness(pd.DataFrame(), "x").columns)
    return pd.concat(parts, ignore_index=True)


def build_cleaning_impact_summary(
    before: dict[str, pd.DataFrame],
    after: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Per-table before/after row, column, missing, and duplicate metrics."""
    rows = []
    for name in SILVER_ENDPOINT_ORDER:
        b = before.get(name, pd.DataFrame())
        a = after.get(name, pd.DataFrame())
        b_prof = profile_table(b, name)
        a_prof = profile_table(a, name)
        rows.append(
            {
                "table_name": name,
                "rows_before": b_prof["row_count"],
                "rows_after": a_prof["row_count"],
                "rows_removed": b_prof["row_count"] - a_prof["row_count"],
                "row_removal_pct": round(
                    (b_prof["row_count"] - a_prof["row_count"]) / b_prof["row_count"] * 100, 4
                )
                if b_prof["row_count"]
                else 0.0,
                "columns_before": b_prof["column_count"],
                "columns_after": a_prof["column_count"],
                "missing_cells_before": b_prof["total_missing_cells"],
                "missing_cells_after": a_prof["total_missing_cells"],
                "missing_cell_pct_before": b_prof["missing_cell_pct"],
                "missing_cell_pct_after": a_prof["missing_cell_pct"],
                "duplicate_rows_before": b_prof["duplicate_full_rows"],
                "duplicate_rows_after": a_prof["duplicate_full_rows"],
            }
        )
    return pd.DataFrame(rows)


def run_silver_cleaning(
    bronze_dir: Path,
    silver_dir: Path,
    data_quality_reports_dir: Path,
) -> dict[str, Any]:
    """
    Full Silver pipeline: audit → clean → save parquet → write DQ CSV reports.
    """
    bronze_dir = Path(bronze_dir)
    silver_dir = Path(silver_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    ensure_dir(silver_dir)
    ensure_dir(data_quality_reports_dir)

    bronze_tables = load_bronze_tables(bronze_dir)

    # Pre-cleaning audits
    inventory_before = build_table_inventory(bronze_tables)
    missingness_before = _concat_missingness(bronze_tables)
    duplicate_before = build_duplicate_report(bronze_tables, stage="before")
    outlier_before = build_outlier_report(bronze_tables, stage="before")
    temporal_before = build_temporal_anomaly_report(
        bronze_tables, stage="before", sessions_df=bronze_tables.get("sessions")
    )
    referential_before = build_referential_integrity_report(bronze_tables, stage="before")

    # Clean each endpoint
    silver_tables: dict[str, pd.DataFrame] = {}
    cleaning_logs: list[pd.DataFrame] = []
    all_rejected: list[dict[str, Any]] = []

    for endpoint in SILVER_ENDPOINT_ORDER:
        raw = bronze_tables.get(endpoint, pd.DataFrame())
        cleaner = CLEANERS[endpoint]
        cleaned, log = cleaner(raw)
        silver_tables[endpoint] = cleaned
        if not log.empty:
            cleaning_logs.append(log)

        out_path = silver_dir / f"{endpoint}_clean.parquet"
        save_dataframe_parquet(cleaned, out_path)
        logger.info("Saved Silver %s -> %s (%s rows)", endpoint, out_path, len(cleaned))

    cleaning_rules = (
        pd.concat(cleaning_logs, ignore_index=True)
        if cleaning_logs
        else pd.DataFrame(
            columns=[
                "table_name",
                "rule_id",
                "rule_description",
                "rows_before",
                "rows_after",
                "rows_removed",
            ]
        )
    )

    # Post-cleaning audits
    missingness_after = _concat_missingness(silver_tables)
    duplicate_after = build_duplicate_report(silver_tables, stage="after")
    outlier_after = build_outlier_report(silver_tables, stage="after")
    temporal_after = build_temporal_anomaly_report(
        silver_tables, stage="after", sessions_df=silver_tables.get("sessions")
    )
    referential_after = build_referential_integrity_report(silver_tables, stage="after")

    duplicate_report = pd.concat([duplicate_before, duplicate_after], ignore_index=True)
    outlier_report = pd.concat([outlier_before, outlier_after], ignore_index=True)
    temporal_report = pd.concat([temporal_before, temporal_after], ignore_index=True)
    referential_report = pd.concat(
        [referential_before, referential_after], ignore_index=True
    )

    impact_summary = build_cleaning_impact_summary(bronze_tables, silver_tables)

    rejected_summary = pd.DataFrame(columns=REJECTED_SUMMARY_COLUMNS)
    if not cleaning_rules.empty and "rows_removed" in cleaning_rules.columns:
        rejected_from_rules = cleaning_rules.loc[
            cleaning_rules["rows_removed"] > 0,
            ["table_name", "rule_id", "rule_description", "rows_removed"],
        ].rename(
            columns={
                "rule_description": "reason",
                "rows_removed": "rejected_count",
            }
        )
        rejected_summary = pd.concat(
            [rejected_summary, rejected_from_rules], ignore_index=True
        )

    paths = {
        "silver_table_inventory": data_quality_reports_dir / "silver_table_inventory.csv",
        "silver_missingness_before": data_quality_reports_dir / "silver_missingness_before.csv",
        "silver_missingness_after": data_quality_reports_dir / "silver_missingness_after.csv",
        "silver_duplicate_report": data_quality_reports_dir / "silver_duplicate_report.csv",
        "silver_outlier_report": data_quality_reports_dir / "silver_outlier_report.csv",
        "silver_temporal_anomaly_report": data_quality_reports_dir
        / "silver_temporal_anomaly_report.csv",
        "silver_referential_integrity_report": data_quality_reports_dir
        / "silver_referential_integrity_report.csv",
        "silver_cleaning_rules": data_quality_reports_dir / "silver_cleaning_rules.csv",
        "silver_cleaning_impact_summary": data_quality_reports_dir
        / "silver_cleaning_impact_summary.csv",
        "silver_rejected_records_summary": data_quality_reports_dir
        / "silver_rejected_records_summary.csv",
    }

    save_dataframe_csv(inventory_before, paths["silver_table_inventory"])
    save_dataframe_csv(missingness_before, paths["silver_missingness_before"])
    save_dataframe_csv(missingness_after, paths["silver_missingness_after"])
    save_dataframe_csv(duplicate_report, paths["silver_duplicate_report"])
    save_dataframe_csv(outlier_report, paths["silver_outlier_report"])
    save_dataframe_csv(temporal_report, paths["silver_temporal_anomaly_report"])
    save_dataframe_csv(referential_report, paths["silver_referential_integrity_report"])
    save_dataframe_csv(cleaning_rules, paths["silver_cleaning_rules"])
    save_dataframe_csv(impact_summary, paths["silver_cleaning_impact_summary"])
    save_dataframe_csv(rejected_summary, paths["silver_rejected_records_summary"])

    summary = {
        "tables_loaded": len(bronze_tables),
        "total_rows_before": int(inventory_before["row_count"].sum())
        if not inventory_before.empty
        else 0,
        "total_rows_after": int(impact_summary["rows_after"].sum())
        if not impact_summary.empty
        else 0,
        "total_rows_removed": int(impact_summary["rows_removed"].sum())
        if not impact_summary.empty
        else 0,
        "cleaning_rules_applied": len(cleaning_rules),
        "session_result_rows_after": int(
            silver_tables.get("session_result", pd.DataFrame()).shape[0]
        ),
    }

    logger.info("Silver cleaning complete: %s", summary)
    return {
        "paths": {k: str(v) for k, v in paths.items()},
        "summary": summary,
        "silver_tables": {k: len(v) for k, v in silver_tables.items()},
    }
