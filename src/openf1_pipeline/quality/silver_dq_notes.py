"""
Static Silver data-quality documentation for reports (structural nulls, temporal checks).
"""

from __future__ import annotations

import pandas as pd

SILVER_DQ_NOTES_COLUMNS = [
    "note_category",
    "table_name",
    "column_or_check",
    "description",
    "blocks_full_run",
    "recommended_action",
]

# Curated notes for final report / audit; not derived from a single run's row counts.
SILVER_DATA_QUALITY_NOTES: list[dict[str, str]] = [
    {
        "note_category": "structural_missingness",
        "table_name": "race_control",
        "column_or_check": "driver_number",
        "description": (
            "Often null because many race-control messages are session-wide (safety car, "
            "track limits, session status) and are not tied to a single driver."
        ),
        "blocks_full_run": "no",
        "recommended_action": (
            "Do not impute in Silver. Gold aggregates message counts at session level; "
            "driver-specific messages remain optional."
        ),
    },
    {
        "note_category": "structural_missingness",
        "table_name": "race_control",
        "column_or_check": "sector",
        "description": (
            "Frequently null when the message is not sector-specific (e.g. session-level "
            "flags or global announcements)."
        ),
        "blocks_full_run": "no",
        "recommended_action": "Report missingness; no Silver row removal.",
    },
    {
        "note_category": "structural_missingness",
        "table_name": "race_control",
        "column_or_check": "flag",
        "description": (
            "Null for message categories that do not include a flag field (timing, "
            "session status, or non-flag events)."
        ),
        "blocks_full_run": "no",
        "recommended_action": (
            "Use flag-derived counts in Gold with null-safe aggregation; do not impute."
        ),
    },
    {
        "note_category": "structural_missingness",
        "table_name": "race_control",
        "column_or_check": "qualifying_phase",
        "description": (
            "Expected to be entirely null for race/session types where OpenF1 does not "
            "populate qualifying metadata."
        ),
        "blocks_full_run": "no",
        "recommended_action": "Filter or segment reports by session_type when interpreting.",
    },
    {
        "note_category": "structural_missingness",
        "table_name": "pit",
        "column_or_check": "stop_duration",
        "description": (
            "OpenF1 pit payloads in practice expose pit_duration and lane_duration; "
            "stop_duration is commonly absent (100% null in smoke)."
        ),
        "blocks_full_run": "no",
        "recommended_action": "Use pit_duration / lane_duration for features; do not impute stop_duration.",
    },
    {
        "note_category": "structural_missingness",
        "table_name": "session_result",
        "column_or_check": "duration",
        "description": (
            "Null for DNF, DNS, DSQ, or non-classified finishers who have no valid race time."
        ),
        "blocks_full_run": "no",
        "recommended_action": (
            "Keep nulls in Silver; use for diagnostics only. Target points_finish is built in Gold."
        ),
    },
    {
        "note_category": "structural_missingness",
        "table_name": "session_result",
        "column_or_check": "gap_to_leader",
        "description": (
            "May be null for retired or non-finishing drivers when OpenF1 does not report a gap."
        ),
        "blocks_full_run": "no",
        "recommended_action": "Exclude from modeling features; optional diagnostic only.",
    },
    {
        "note_category": "structural_missingness",
        "table_name": "laps",
        "column_or_check": "i1_speed,st_speed",
        "description": (
            "Intermediate and speed-trap telemetry are not reported on every lap (circuit "
            "layout, session phase, or sensor availability)."
        ),
        "blocks_full_run": "no",
        "recommended_action": (
            "Aggregate with null-safe means in Gold; do not drop laps or impute speeds in Silver."
        ),
    },
    {
        "note_category": "temporal_check",
        "table_name": "weather",
        "column_or_check": "weather_outside_session_bounds",
        "description": (
            "Temporal report may flag weather timestamps outside sessions.date_start/date_end. "
            "This is usually non-blocking: OpenF1 weather samples can sit just outside parsed "
            "session boundaries, use UTC vs local offsets, or reflect pre/post-session sampling "
            "while the session catalog window is narrower."
        ),
        "blocks_full_run": "no",
        "recommended_action": (
            "Treat as informational in silver_temporal_anomaly_report.csv; verify Gold weather "
            "aggregates still join. Do not reject weather rows solely for this flag."
        ),
    },
]


def build_silver_data_quality_notes() -> pd.DataFrame:
    """Return static Silver DQ documentation rows for CSV export."""
    if not SILVER_DATA_QUALITY_NOTES:
        return pd.DataFrame(columns=SILVER_DQ_NOTES_COLUMNS)
    return pd.DataFrame(SILVER_DATA_QUALITY_NOTES, columns=SILVER_DQ_NOTES_COLUMNS)


def build_rejected_records_summary(
    cleaning_rules: pd.DataFrame,
    table_names: list[str],
) -> pd.DataFrame:
    """
    Build rejected-records summary.

    When no rows were removed, emit one template row per table (rejected_count=0).
    Otherwise list only rules that removed rows.
    """
    from openf1_pipeline.silver.cleaning_common import REJECTED_SUMMARY_COLUMNS

    if (
        not cleaning_rules.empty
        and "rows_removed" in cleaning_rules.columns
        and (cleaning_rules["rows_removed"] > 0).any()
    ):
        removed = cleaning_rules.loc[cleaning_rules["rows_removed"] > 0].copy()
        return removed.rename(
            columns={
                "rule_description": "reason",
                "rows_removed": "rejected_count",
            }
        )[
            ["table_name", "rule_id", "reason", "rejected_count"]
        ].reset_index(drop=True)

    template = [
        {
            "table_name": table,
            "rule_id": "SIL_NONE",
            "reason": "No records rejected in this run",
            "rejected_count": 0,
        }
        for table in table_names
    ]
    return pd.DataFrame(template, columns=REJECTED_SUMMARY_COLUMNS)
