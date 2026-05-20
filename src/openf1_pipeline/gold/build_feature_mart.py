"""
Build Gold driver-race feature mart from Silver cleaned Parquet tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from openf1_pipeline.quality.profiling import compute_missingness, summarize_dataframe
from openf1_pipeline.utils.io import ensure_dir, read_parquet_if_exists, save_dataframe_csv, save_dataframe_parquet
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

GOLD_MART_FILENAME = "driver_race_feature_mart.parquet"

DRIVER_KEYS = ["session_key", "meeting_key", "driver_number"]
SESSION_KEYS = ["session_key", "meeting_key"]

SILVER_TABLE_NAMES = [
    "drivers",
    "laps",
    "meetings",
    "pit",
    "position",
    "race_control",
    "session_result",
    "sessions",
    "weather",
    "starting_grid",
]

# Outcome / label fields — not allowed as predictive features.
LEAKAGE_FORBIDDEN_COLUMNS = frozenset(
    {
        "position",
        "points",
        "final_position",
        "result_points",
        "result_dnf",
        "result_dns",
        "result_dsq",
        "duration",
        "gap_to_leader",
        "number_of_laps",
    }
)

IDENTIFIER_COLUMNS = frozenset({"session_key", "meeting_key", "driver_number"})
TARGET_COLUMN = "points_finish"
METADATA_COLUMNS = frozenset(
    {
        "full_name",
        "name_acronym",
        "team_name",
        "driver_country_code",
        "session_type",
        "session_name",
        "session_year",
        "circuit_short_name",
        "session_country_name",
        "location",
        "meeting_name",
        "circuit_key",
    }
)

EVENT_ABSENCE_ZERO_COLS = [
    "pit_stop_count",
    "early_pit_stop_flag",
    "lap_count",
    "pit_out_lap_count",
    "race_control_message_count",
    "flag_message_count",
    "yellow_flag_count",
    "red_flag_count",
    "green_flag_count",
    "safety_car_message_count",
    "pit_exit_message_count",
]


def load_silver_tables(silver_dir: Path) -> dict[str, pd.DataFrame]:
    """Load Silver parquet tables keyed by endpoint name (without ``_clean`` suffix)."""
    silver_dir = Path(silver_dir)
    tables: dict[str, pd.DataFrame] = {}
    for name in SILVER_TABLE_NAMES:
        path = silver_dir / f"{name}_clean.parquet"
        df = read_parquet_if_exists(path)
        if df is None:
            if name != "starting_grid":
                logger.warning("Silver table missing: %s", path)
            tables[name] = pd.DataFrame()
        elif df.empty and name != "starting_grid":
            logger.warning("Silver table empty: %s", name)
            tables[name] = df
        else:
            tables[name] = df
            logger.info("Loaded Silver %s: %s rows", name, len(df))
    return tables


def _session_meeting_map(sessions: pd.DataFrame) -> pd.DataFrame:
    if sessions.empty or "session_key" not in sessions.columns:
        return pd.DataFrame(columns=["session_key", "meeting_key"])
    cols = ["session_key", "meeting_key"]
    extra = [c for c in ("year", "session_name", "session_type") if c in sessions.columns]
    return sessions[cols + extra].drop_duplicates(subset=["session_key"])


def attach_meeting_key(df: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Ensure meeting_key on driver- or session-grain frames."""
    if df.empty:
        return df
    out = df.copy()
    if "meeting_key" in out.columns and out["meeting_key"].notna().any():
        if out["meeting_key"].isna().any() and not sessions.empty:
            sk_map = _session_meeting_map(sessions)[["session_key", "meeting_key"]]
            out = out.drop(columns=["meeting_key"]).merge(sk_map, on="session_key", how="left")
        return out
    if sessions.empty:
        return out
    sk_map = _session_meeting_map(sessions)[["session_key", "meeting_key"]]
    return out.merge(sk_map, on="session_key", how="left")


def build_target_base(session_result: pd.DataFrame) -> pd.DataFrame:
    """One row per driver-race with target and diagnostic outcome columns."""
    if session_result.empty:
        raise ValueError("session_result is empty; cannot build Gold base table.")

    required = ["session_key", "meeting_key", "driver_number", "position", "points"]
    missing = [c for c in required if c not in session_result.columns]
    if missing:
        raise ValueError(f"session_result missing columns: {missing}")

    base = session_result.copy()
    base["driver_number"] = pd.to_numeric(base["driver_number"], errors="coerce")
    base["session_key"] = pd.to_numeric(base["session_key"], errors="coerce")
    base["meeting_key"] = pd.to_numeric(base["meeting_key"], errors="coerce")
    points = pd.to_numeric(base["points"], errors="coerce").fillna(0)
    base["points_finish"] = (points > 0).astype(int)
    base["final_position"] = pd.to_numeric(base["position"], errors="coerce")
    base["result_points"] = points
    for src, dst in [
        ("dnf", "result_dnf"),
        ("dns", "result_dns"),
        ("dsq", "result_dsq"),
    ]:
        if src in base.columns:
            base[dst] = base[src]
        else:
            base[dst] = False

    keep = list(DRIVER_KEYS) + [
        "points_finish",
        "final_position",
        "result_points",
        "result_dnf",
        "result_dns",
        "result_dsq",
    ]
    dupes = base.duplicated(subset=DRIVER_KEYS, keep=False).sum()
    if dupes:
        logger.warning("Duplicate driver-race keys in session_result: %s rows", dupes)
    return base[keep].drop_duplicates(subset=DRIVER_KEYS).reset_index(drop=True)


def _first_n_lap_mask(laps: pd.DataFrame, n: int = 5) -> pd.Series:
    if "lap_number" not in laps.columns:
        return pd.Series(False, index=laps.index)
    return pd.to_numeric(laps["lap_number"], errors="coerce") <= n


def build_lap_features(laps: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Driver-session lap pace and sector aggregates (early laps only for first-five stats)."""
    if laps.empty:
        return pd.DataFrame(columns=DRIVER_KEYS)

    work = attach_meeting_key(laps.copy(), sessions)
    work["lap_number"] = pd.to_numeric(work.get("lap_number"), errors="coerce")
    if "lap_duration" not in work.columns and "duration" in work.columns:
        work["lap_duration"] = pd.to_numeric(work["duration"], errors="coerce")

    sector_map = {
        "avg_sector_1": "duration_sector_1",
        "avg_sector_2": "duration_sector_2",
        "avg_sector_3": "duration_sector_3",
    }
    for feat, src in sector_map.items():
        if src in work.columns:
            work[feat] = pd.to_numeric(work[src], errors="coerce")
        else:
            logger.info("Lap feature skipped; missing column %s for %s", src, feat)

    speed_cols = {"avg_i1_speed": "i1_speed", "avg_i2_speed": "i2_speed", "avg_st_speed": "st_speed"}
    for feat, src in speed_cols.items():
        if src in work.columns:
            work[feat] = pd.to_numeric(work[src], errors="coerce")
        else:
            logger.info("Lap feature skipped; missing column %s", src)

    if "is_pit_out_lap" in work.columns:
        work["_pit_out"] = work["is_pit_out_lap"].fillna(False).astype(bool)
    else:
        work["_pit_out"] = False
        logger.info("Lap feature skipped; missing is_pit_out_lap")

    early = work[_first_n_lap_mask(work)]
    early = early[early["lap_duration"].notna()] if "lap_duration" in early.columns else early

    agg_main: dict[str, Any] = {}
    if "lap_duration" in work.columns:
        agg_main["lap_count"] = ("lap_number", "count")
        agg_main["avg_lap_duration"] = ("lap_duration", "mean")
        agg_main["median_lap_duration"] = ("lap_duration", "median")
        agg_main["best_lap_duration"] = ("lap_duration", "min")
        agg_main["std_lap_duration"] = ("lap_duration", "std")
    for feat in sector_map:
        if feat in work.columns:
            agg_main[feat] = (feat, "mean")
    for feat in speed_cols:
        if feat in work.columns:
            agg_main[feat] = (feat, "mean")
    if "_pit_out" in work.columns:
        agg_main["pit_out_lap_count"] = ("_pit_out", "sum")

    if not agg_main:
        return pd.DataFrame(columns=DRIVER_KEYS)

    main = work.groupby(DRIVER_KEYS, as_index=False).agg(**agg_main)

    if not early.empty and "lap_duration" in early.columns:
        early_agg = (
            early.groupby(DRIVER_KEYS, as_index=False)["lap_duration"]
            .agg(
                avg_first5_lap_duration="mean",
                best_first5_lap_duration="min",
                std_first5_lap_duration="std",
            )
        )
        main = main.merge(early_agg, on=DRIVER_KEYS, how="left")
    else:
        logger.info("First-five lap duration features skipped (no early lap_duration rows).")

    return main


def build_pit_features(pit: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Driver-session pit stop aggregates."""
    if pit.empty:
        return pd.DataFrame(columns=DRIVER_KEYS)

    work = attach_meeting_key(pit.copy(), sessions)
    work["lap_number"] = pd.to_numeric(work.get("lap_number"), errors="coerce")
    if "pit_duration" in work.columns:
        work["_pit_dur"] = pd.to_numeric(work["pit_duration"], errors="coerce")
    elif "lane_duration" in work.columns:
        work["_pit_dur"] = pd.to_numeric(work["lane_duration"], errors="coerce")
        logger.info("Using lane_duration as pit duration fallback.")
    else:
        work["_pit_dur"] = np.nan
        logger.info("Pit duration column missing; duration aggregates will be null.")

    agg: dict[str, Any] = {"pit_stop_count": ("session_key", "count")}
    if work["_pit_dur"].notna().any():
        agg["avg_pit_duration"] = ("_pit_dur", "mean")
        agg["min_pit_duration"] = ("_pit_dur", "min")
        agg["max_pit_duration"] = ("_pit_dur", "max")
    if "lap_number" in work.columns:
        agg["first_pit_lap"] = ("lap_number", "min")

    features = work.groupby(DRIVER_KEYS, as_index=False).agg(**agg)
    if "first_pit_lap" in features.columns:
        features["early_pit_stop_flag"] = (features["first_pit_lap"] <= 10).astype(int)
    else:
        features["early_pit_stop_flag"] = 0
    return features


def build_position_features(position: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Early-race position features (no final-position leakage)."""
    if position.empty:
        return pd.DataFrame(columns=DRIVER_KEYS)

    work = attach_meeting_key(position.copy(), sessions)
    work["position"] = pd.to_numeric(work.get("position"), errors="coerce")
    if "date" in work.columns:
        work["_sort_time"] = pd.to_datetime(work["date"], errors="coerce")
    else:
        work["_sort_time"] = np.arange(len(work))
    work = work.sort_values(DRIVER_KEYS + ["_sort_time"])

    rows: list[dict[str, Any]] = []
    for keys, group in work.groupby(DRIVER_KEYS):
        g = group.reset_index(drop=True)
        early = g.head(5)
        pos = early["position"].dropna()
        row = dict(zip(DRIVER_KEYS, keys))
        row["position_observation_count"] = len(g)
        row["diagnostic_final_observed_position"] = (
            float(g["position"].dropna().iloc[-1]) if g["position"].notna().any() else np.nan
        )
        if len(pos):
            row["first_observed_position"] = float(pos.iloc[0])
            row["early_avg_position"] = float(pos.mean())
            row["early_min_position"] = float(pos.min())
            row["early_max_position"] = float(pos.max())
        else:
            row["first_observed_position"] = np.nan
            row["early_avg_position"] = np.nan
            row["early_min_position"] = np.nan
            row["early_max_position"] = np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def build_weather_features(weather: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Session-level weather joined to all drivers."""
    if weather.empty:
        return pd.DataFrame(columns=SESSION_KEYS)

    work = attach_meeting_key(weather.copy(), sessions)
    agg_cols = {
        "air_temperature": "avg_air_temperature",
        "track_temperature": "avg_track_temperature",
        "humidity": "avg_humidity",
        "pressure": "avg_pressure",
        "wind_speed": "avg_wind_speed",
        "rainfall": "rainfall_mean",
    }
    spec: dict[str, tuple[str, str]] = {}
    for src, dst in agg_cols.items():
        if src in work.columns:
            work[src] = pd.to_numeric(work[src], errors="coerce")
            spec[dst] = (src, "mean")

    if not spec:
        return pd.DataFrame(columns=SESSION_KEYS)

    out = work.groupby(SESSION_KEYS, as_index=False).agg(**spec)
    if "rainfall" in work.columns:
        rain = work.groupby(SESSION_KEYS, as_index=False)["rainfall"].max()
        out = out.merge(rain.rename(columns={"rainfall": "_rain_max"}), on=SESSION_KEYS, how="left")
        out["rainfall_flag"] = (out["_rain_max"].fillna(0) > 0).astype(int)
        out = out.drop(columns=["_rain_max"], errors="ignore")
    elif "rainfall_mean" in out.columns:
        out["rainfall_flag"] = (out["rainfall_mean"].fillna(0) > 0).astype(int)
    return out


def _count_messages(series: pd.Series, needle: str) -> int:
    if series.empty:
        return 0
    text = series.fillna("").astype(str).str.upper()
    return int(text.str.contains(needle, regex=False).sum())


def build_race_control_features(
    race_control: pd.DataFrame, sessions: pd.DataFrame
) -> pd.DataFrame:
    """Session-level race control message counts.

    ``qualifying_phase`` is intentionally excluded — it is 100% null in current
    OpenF1 pulls (see Silver data-quality notes). Features use ``message`` and
    ``flag`` only and are count-aware so legitimate event-stream re-broadcasts
    contribute to the totals.
    """
    if race_control.empty:
        return pd.DataFrame(columns=SESSION_KEYS)

    work = attach_meeting_key(race_control.copy(), sessions)
    rows: list[dict[str, Any]] = []
    for keys, group in work.groupby(SESSION_KEYS):
        row = dict(zip(SESSION_KEYS, keys))
        messages = group.get("message", pd.Series(dtype=object))
        flags = group.get("flag", pd.Series(dtype=object))
        combined = messages.fillna("").astype(str) + " " + flags.fillna("").astype(str)
        row["race_control_message_count"] = len(group)
        row["flag_message_count"] = int(flags.notna().sum()) if "flag" in group.columns else 0
        row["yellow_flag_count"] = _count_messages(combined, "YELLOW")
        row["red_flag_count"] = _count_messages(combined, "RED")
        row["green_flag_count"] = _count_messages(combined, "GREEN")
        row["safety_car_message_count"] = _count_messages(combined, "SAFETY CAR") + _count_messages(
            combined, "VIRTUAL SAFETY CAR"
        )
        row["pit_exit_message_count"] = _count_messages(combined, "PIT EXIT")
        rows.append(row)
    return pd.DataFrame(rows)


def build_metadata_features(
    drivers: pd.DataFrame,
    sessions: pd.DataFrame,
    meetings: pd.DataFrame,
) -> pd.DataFrame:
    """Driver-session metadata from drivers, sessions, and meetings."""
    if drivers.empty:
        return pd.DataFrame(columns=DRIVER_KEYS)

    meta = drivers.copy()
    meta = attach_meeting_key(meta, sessions)

    driver_cols = {
        "full_name": "full_name",
        "name_acronym": "name_acronym",
        "team_name": "team_name",
        "country_code": "driver_country_code",
    }
    keep_d = [c for c in DRIVER_KEYS if c in meta.columns]
    for src in driver_cols:
        if src in meta.columns:
            keep_d.append(src)

    meta = meta[keep_d].drop_duplicates(subset=DRIVER_KEYS)

    if not sessions.empty:
        sess_cols = ["session_key"]
        rename = {}
        if "session_type" in sessions.columns:
            sess_cols.append("session_type")
        if "session_name" in sessions.columns:
            sess_cols.append("session_name")
        if "year" in sessions.columns:
            rename["year"] = "session_year"
            sess_cols.append("year")
        if "circuit_short_name" in sessions.columns:
            sess_cols.append("circuit_short_name")
        if "country_name" in sessions.columns:
            rename["country_name"] = "session_country_name"
            sess_cols.append("country_name")
        if "location" in sessions.columns:
            sess_cols.append("location")
        sess = sessions[sess_cols].drop_duplicates(subset=["session_key"]).rename(columns=rename)
        meta = meta.merge(sess, on="session_key", how="left")

    if not meetings.empty:
        meet_cols = ["meeting_key"]
        if "meeting_name" in meetings.columns:
            meet_cols.append("meeting_name")
        if "circuit_key" in meetings.columns:
            meet_cols.append("circuit_key")
        meet = meetings[meet_cols].drop_duplicates(subset=["meeting_key"])
        meta = meta.merge(meet, on="meeting_key", how="left", suffixes=("", "_meet"))
        if "circuit_short_name_meet" in meta.columns:
            meta["circuit_short_name"] = meta["circuit_short_name"].fillna(
                meta["circuit_short_name_meet"]
            )
            meta = meta.drop(columns=["circuit_short_name_meet"])
        if "country_name_meet" in meta.columns and "session_country_name" in meta.columns:
            meta["session_country_name"] = meta["session_country_name"].fillna(meta["country_name_meet"])

    return meta.drop_duplicates(subset=DRIVER_KEYS)


def _join_quality_row(
    feature_group: str,
    rows_before: int,
    merged: pd.DataFrame,
    base_keys: pd.DataFrame,
    notes: str = "",
) -> dict[str, Any]:
    rows_after = len(merged)
    base_set = set(map(tuple, base_keys[DRIVER_KEYS].dropna().values.tolist()))
    merged_set = set(map(tuple, merged[DRIVER_KEYS].dropna().values.tolist()))
    unmatched = len(base_set - merged_set)
    unmatched_pct = round(unmatched / len(base_set) * 100, 4) if base_set else 0.0
    return {
        "feature_group": feature_group,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "unmatched_base_rows": unmatched,
        "unmatched_pct": unmatched_pct,
        "notes": notes,
    }


def _left_join_features(
    base: pd.DataFrame,
    features: pd.DataFrame,
    on: list[str],
    feature_group: str,
    join_logs: list[dict[str, Any]],
) -> pd.DataFrame:
    rows_before = len(base)
    if features.empty or not set(on).issubset(features.columns):
        join_logs.append(
            _join_quality_row(
                feature_group,
                rows_before,
                base,
                base[DRIVER_KEYS],
                notes="empty or missing join keys; left join adds no rows",
            )
        )
        return base
    merged = base.merge(features, on=on, how="left", suffixes=("", f"_{feature_group}"))
    join_logs.append(_join_quality_row(feature_group, rows_before, merged, base[DRIVER_KEYS]))
    return merged


def build_leakage_guard_report(gold_df: pd.DataFrame) -> pd.DataFrame:
    """Flag columns that must not be used as predictive features."""
    rows: list[dict[str, Any]] = []
    for col in gold_df.columns:
        if col in IDENTIFIER_COLUMNS:
            rows.append(
                {
                    "column_name": col,
                    "leakage_risk": "none",
                    "allowed_for_modeling": False,
                    "reason": "identifier key",
                }
            )
        elif col == TARGET_COLUMN:
            rows.append(
                {
                    "column_name": col,
                    "leakage_risk": "target",
                    "allowed_for_modeling": False,
                    "reason": "model target",
                }
            )
        elif col.startswith("diagnostic_") or col in LEAKAGE_FORBIDDEN_COLUMNS:
            rows.append(
                {
                    "column_name": col,
                    "leakage_risk": "high",
                    "allowed_for_modeling": False,
                    "reason": "post-race outcome or diagnostic field",
                }
            )
        elif col in METADATA_COLUMNS:
            rows.append(
                {
                    "column_name": col,
                    "leakage_risk": "low",
                    "allowed_for_modeling": False,
                    "reason": "metadata; encode explicitly if used in modeling",
                }
            )
        else:
            rows.append(
                {
                    "column_name": col,
                    "leakage_risk": "low",
                    "allowed_for_modeling": True,
                    "reason": "engineered session feature; see feature tier in model_feature_plan",
                }
            )
    return pd.DataFrame(rows)


def get_model_feature_columns(gold_df: pd.DataFrame) -> list[str]:
    """Feature columns safe for modeling (excludes identifiers, target, leakage)."""
    guard = build_leakage_guard_report(gold_df)
    return guard.loc[guard["allowed_for_modeling"], "column_name"].tolist()


def build_gold_target_distribution(gold_df: pd.DataFrame) -> pd.DataFrame:
    if TARGET_COLUMN not in gold_df.columns:
        return pd.DataFrame(columns=[TARGET_COLUMN, "count", "pct"])
    counts = gold_df[TARGET_COLUMN].value_counts(dropna=False).sort_index()
    total = counts.sum()
    return pd.DataFrame(
        {
            TARGET_COLUMN: counts.index,
            "count": counts.values,
            "pct": (counts.values / total * 100).round(4) if total else 0,
        }
    )


def build_gold_feature_mart_pandas(
    silver_dir: Path,
    gold_dir: Path,
    data_quality_reports_dir: Path,
    feature_definitions_dir: Path,
) -> dict[str, Any]:
    """Build Gold mart with pandas (fallback)."""
    from openf1_pipeline.features.feature_dictionary import (
        build_feature_dictionary,
        write_feature_dictionary,
    )

    silver_dir = Path(silver_dir)
    gold_dir = Path(gold_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    feature_definitions_dir = Path(feature_definitions_dir)
    ensure_dir(gold_dir)
    ensure_dir(data_quality_reports_dir)
    ensure_dir(feature_definitions_dir)

    tables = load_silver_tables(silver_dir)
    sessions = tables.get("sessions", pd.DataFrame())
    meetings = tables.get("meetings", pd.DataFrame())

    base = build_target_base(tables["session_result"])
    join_logs: list[dict[str, Any]] = []

    gold = base.copy()
    meta = build_metadata_features(tables.get("drivers", pd.DataFrame()), sessions, meetings)
    meta_cols = [c for c in meta.columns if c not in DRIVER_KEYS]
    gold = _left_join_features(gold, meta, DRIVER_KEYS, "metadata", join_logs)

    for builder, name, on in [
        (lambda: build_lap_features(tables.get("laps", pd.DataFrame()), sessions), "laps", DRIVER_KEYS),
        (lambda: build_pit_features(tables.get("pit", pd.DataFrame()), sessions), "pit", DRIVER_KEYS),
        (
            lambda: build_position_features(tables.get("position", pd.DataFrame()), sessions),
            "position",
            DRIVER_KEYS,
        ),
        (
            lambda: build_weather_features(tables.get("weather", pd.DataFrame()), sessions),
            "weather",
            SESSION_KEYS,
        ),
        (
            lambda: build_race_control_features(
                tables.get("race_control", pd.DataFrame()), sessions
            ),
            "race_control",
            SESSION_KEYS,
        ),
    ]:
        feats = builder()
        gold = _left_join_features(gold, feats, on, name, join_logs)

    dupes = gold.duplicated(subset=DRIVER_KEYS).sum()
    if dupes:
        raise ValueError(f"Gold mart has {dupes} duplicate rows at driver-race grain.")

    for col in EVENT_ABSENCE_ZERO_COLS:
        if col in gold.columns:
            gold[col] = gold[col].fillna(0)

    mart_path = gold_dir / GOLD_MART_FILENAME
    save_dataframe_parquet(gold, mart_path)

    summary_path = data_quality_reports_dir / "gold_feature_summary_stats.csv"
    missing_path = data_quality_reports_dir / "gold_feature_missingness.csv"
    target_path = data_quality_reports_dir / "gold_target_distribution.csv"
    join_path = data_quality_reports_dir / "gold_join_quality_report.csv"
    leakage_path = data_quality_reports_dir / "gold_leakage_guard_report.csv"
    dict_path = feature_definitions_dir / "feature_dictionary.csv"

    save_dataframe_csv(summarize_dataframe(gold, "driver_race_feature_mart"), summary_path)
    save_dataframe_csv(compute_missingness(gold, "driver_race_feature_mart"), missing_path)
    save_dataframe_csv(build_gold_target_distribution(gold), target_path)
    save_dataframe_csv(pd.DataFrame(join_logs), join_path)
    leakage = build_leakage_guard_report(gold)
    save_dataframe_csv(leakage, leakage_path)
    write_feature_dictionary(build_feature_dictionary(gold), dict_path)

    model_features = get_model_feature_columns(gold)
    summary = {
        "engine": "pandas",
        "row_count": len(gold),
        "column_count": len(gold.columns),
        "model_feature_count": len(model_features),
        "duplicate_grain_rows": int(dupes),
        "points_finish_positive_pct": round(
            gold[TARGET_COLUMN].mean() * 100, 4
        )
        if TARGET_COLUMN in gold.columns
        else None,
    }

    return {
        "paths": {
            "driver_race_feature_mart": mart_path,
            "gold_feature_summary_stats": summary_path,
            "gold_feature_missingness": missing_path,
            "gold_target_distribution": target_path,
            "gold_join_quality_report": join_path,
            "gold_leakage_guard_report": leakage_path,
            "feature_dictionary": dict_path,
        },
        "summary": summary,
        "model_feature_columns": model_features,
    }


def build_gold_feature_mart(
    silver_dir: Path,
    gold_dir: Path,
    data_quality_reports_dir: Path,
    feature_definitions_dir: Path,
    engine: str = "spark",
    spark=None,
    allow_fallback: bool = False,
) -> dict[str, Any]:
    """
    Build Gold mart (Spark by default).

    When ``allow_fallback=False`` (recommended in Colab), Spark failures are raised
    immediately instead of partially writing Spark Parquet dirs then colliding with pandas.
    """
    if engine == "spark":
        try:
            from openf1_pipeline.gold.build_feature_mart_spark import build_gold_feature_mart_spark
            from openf1_pipeline.utils.spark import get_spark

            spark = spark or get_spark()
            return build_gold_feature_mart_spark(
                spark,
                silver_dir,
                gold_dir,
                data_quality_reports_dir,
                feature_definitions_dir,
            )
        except Exception as exc:
            if not allow_fallback:
                raise RuntimeError(
                    "Gold Spark engine failed with allow_fallback=False. "
                    f"Clean {gold_dir} and fix the Spark error before retrying. "
                    f"Original error: {exc}"
                ) from exc
            logger.warning(
                "Gold Spark engine failed; allow_fallback=True — cleaning Gold layer then pandas: %s",
                exc,
            )
            from openf1_pipeline.utils.cleanup import clean_gold_layer_outputs

            clean_gold_layer_outputs(
                gold_dir=gold_dir,
                data_quality_reports_dir=data_quality_reports_dir,
                feature_definitions_dir=feature_definitions_dir,
            )
    return build_gold_feature_mart_pandas(
        silver_dir, gold_dir, data_quality_reports_dir, feature_definitions_dir
    )
