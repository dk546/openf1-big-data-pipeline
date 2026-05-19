"""
Spark-first Gold driver-race feature mart.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from openf1_pipeline.features.feature_dictionary import (
    build_feature_dictionary,
    write_feature_dictionary,
)
from openf1_pipeline.gold.build_feature_mart import (
    DRIVER_KEYS,
    EVENT_ABSENCE_ZERO_COLS,
    GOLD_MART_FILENAME,
    SESSION_KEYS,
    TARGET_COLUMN,
    build_gold_target_distribution,
    build_leakage_guard_report,
    get_model_feature_columns,
)
from openf1_pipeline.quality.profiling import compute_missingness, summarize_dataframe
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv
from openf1_pipeline.utils.logging import get_logger
from openf1_pipeline.utils.spark import (
    read_spark_parquet_if_exists,
    safe_to_pandas,
    spark_path,
    write_spark_dataframe,
)

logger = get_logger(__name__)


def _load_silver(spark: SparkSession, silver_dir: Path, name: str) -> DataFrame | None:
    return read_spark_parquet_if_exists(spark, silver_dir / f"{name}_clean.parquet")


def _attach_meeting_key_spark(sdf: DataFrame, sessions: DataFrame | None) -> DataFrame:
    if sdf is None:
        return sdf
    if "meeting_key" in sdf.columns and sdf.filter(F.col("meeting_key").isNotNull()).count() > 0:
        return sdf
    if sessions is None or "meeting_key" not in sessions.columns:
        return sdf
    sk_map = sessions.select("session_key", "meeting_key").dropDuplicates(["session_key"])
    return sdf.join(sk_map, on="session_key", how="left")


def build_target_base_spark(session_result: DataFrame) -> DataFrame:
    if session_result is None or session_result.rdd.isEmpty():
        raise ValueError("session_result is empty; cannot build Gold base.")
    select_cols = [
        F.col("session_key").cast("long"),
        F.col("meeting_key").cast("long"),
        F.col("driver_number").cast("long"),
        F.col("position").cast("double"),
        F.col("points").cast("double"),
    ]
    for src, dst in [("dnf", "result_dnf"), ("dns", "result_dns"), ("dsq", "result_dsq")]:
        if src in session_result.columns:
            select_cols.append(F.col(src).alias(dst))
        else:
            select_cols.append(F.lit(False).alias(dst))
    base = session_result.select(*select_cols)
    base = base.withColumn("points_finish", F.when(F.col("points") > 0, 1).otherwise(0))
    base = base.withColumn("final_position", F.col("position"))
    base = base.withColumn("result_points", F.col("points"))
    drop_cols = [c for c in ("position", "points") if c in base.columns]
    if drop_cols:
        base = base.drop(*drop_cols)
    return base.dropDuplicates(DRIVER_KEYS)


def build_lap_features_spark(laps: DataFrame | None, sessions: DataFrame | None) -> DataFrame:
    if laps is None or laps.rdd.isEmpty():
        return None
    work = _attach_meeting_key_spark(laps, sessions)
    if "lap_duration" not in work.columns and "duration" in work.columns:
        work = work.withColumn("lap_duration", F.col("duration").cast("double"))
    early = work.filter(F.col("lap_number") <= 5) if "lap_number" in work.columns else work.limit(0)

    agg_exprs = [
        F.count("lap_number").alias("lap_count"),
        F.avg("lap_duration").alias("avg_lap_duration"),
        F.expr("percentile_approx(lap_duration, 0.5)").alias("median_lap_duration"),
        F.min("lap_duration").alias("best_lap_duration"),
        F.stddev("lap_duration").alias("std_lap_duration"),
    ]
    for src, alias in [
        ("duration_sector_1", "avg_sector_1"),
        ("duration_sector_2", "avg_sector_2"),
        ("duration_sector_3", "avg_sector_3"),
        ("i1_speed", "avg_i1_speed"),
        ("i2_speed", "avg_i2_speed"),
        ("st_speed", "avg_st_speed"),
    ]:
        if src in work.columns:
            agg_exprs.append(F.avg(src).alias(alias))
    if "is_pit_out_lap" in work.columns:
        agg_exprs.append(
            F.sum(F.when(F.col("is_pit_out_lap") == True, 1).otherwise(0)).alias("pit_out_lap_count")  # noqa: E712
        )

    main = work.groupBy(*DRIVER_KEYS).agg(*agg_exprs)
    if "lap_duration" in early.columns:
        early_agg = early.groupBy(*DRIVER_KEYS).agg(
            F.avg("lap_duration").alias("avg_first5_lap_duration"),
            F.min("lap_duration").alias("best_first5_lap_duration"),
            F.stddev("lap_duration").alias("std_first5_lap_duration"),
        )
        main = main.join(early_agg, on=DRIVER_KEYS, how="left")
    return main


def build_pit_features_spark(pit: DataFrame | None, sessions: DataFrame | None) -> DataFrame:
    if pit is None or pit.rdd.isEmpty():
        return None
    work = _attach_meeting_key_spark(pit, sessions)
    dur_col = (
        "pit_duration"
        if "pit_duration" in work.columns
        else ("lane_duration" if "lane_duration" in work.columns else None)
    )
    agg = [F.count("*").alias("pit_stop_count")]
    if dur_col:
        agg.extend(
            [
                F.avg(dur_col).alias("avg_pit_duration"),
                F.min(dur_col).alias("min_pit_duration"),
                F.max(dur_col).alias("max_pit_duration"),
            ]
        )
    if "lap_number" in work.columns:
        agg.append(F.min("lap_number").alias("first_pit_lap"))
    feats = work.groupBy(*DRIVER_KEYS).agg(*agg)
    if "first_pit_lap" in feats.columns:
        feats = feats.withColumn(
            "early_pit_stop_flag", F.when(F.col("first_pit_lap") <= 10, 1).otherwise(0)
        )
    else:
        feats = feats.withColumn("early_pit_stop_flag", F.lit(0))
    return feats


def build_position_features_spark(position: DataFrame | None, sessions: DataFrame | None) -> DataFrame:
    if position is None or position.rdd.isEmpty():
        return None
    work = _attach_meeting_key_spark(position, sessions)
    sort_col = "date" if "date" in work.columns else "session_key"
    w = Window.partitionBy(*DRIVER_KEYS).orderBy(F.col(sort_col))
    ranked = work.withColumn("_rn", F.row_number().over(w))
    early = ranked.filter(F.col("_rn") <= 5)
    early_agg = early.groupBy(*DRIVER_KEYS).agg(
        F.first("position").alias("first_observed_position"),
        F.avg("position").alias("early_avg_position"),
        F.min("position").alias("early_min_position"),
        F.max("position").alias("early_max_position"),
        F.count("*").alias("position_observation_count"),
    )
    final = ranked.groupBy(*DRIVER_KEYS).agg(
        F.last("position").alias("diagnostic_final_observed_position")
    )
    return early_agg.join(final, on=DRIVER_KEYS, how="left")


def build_weather_features_spark(weather: DataFrame | None, sessions: DataFrame | None) -> DataFrame:
    if weather is None or weather.rdd.isEmpty():
        return None
    work = _attach_meeting_key_spark(weather, sessions)
    agg = []
    for src, alias in [
        ("air_temperature", "avg_air_temperature"),
        ("track_temperature", "avg_track_temperature"),
        ("humidity", "avg_humidity"),
        ("pressure", "avg_pressure"),
        ("wind_speed", "avg_wind_speed"),
        ("rainfall", "rainfall_mean"),
    ]:
        if src in work.columns:
            agg.append(F.avg(src).alias(alias))
    if not agg:
        return None
    out = work.groupBy(*SESSION_KEYS).agg(*agg)
    if "rainfall" in work.columns:
        rain = work.groupBy(*SESSION_KEYS).agg(F.max("rainfall").alias("_rain_max"))
        out = out.join(rain, on=SESSION_KEYS, how="left").withColumn(
            "rainfall_flag", F.when(F.col("_rain_max") > 0, 1).otherwise(0)
        ).drop("_rain_max")
    return out


def build_race_control_features_spark(
    race_control: DataFrame | None, sessions: DataFrame | None
) -> DataFrame:
    # Race-control features use ``message`` and ``flag`` only.
    # ``qualifying_phase`` is known-empty (100% null in current OpenF1 pulls) per the
    # Silver data-quality notes, and is intentionally excluded from feature aggregation.
    if race_control is None or race_control.rdd.isEmpty():
        return None
    work = _attach_meeting_key_spark(race_control, sessions)
    msg = F.coalesce(F.col("message"), F.lit("")).cast("string")
    flag = F.coalesce(F.col("flag"), F.lit("")).cast("string")
    combined = F.upper(F.concat_ws(" ", msg, flag))
    return work.groupBy(*SESSION_KEYS).agg(
        F.count("*").alias("race_control_message_count"),
        F.sum(F.when(flag != "", 1).otherwise(0)).alias("flag_message_count"),
        F.sum(F.when(combined.contains("YELLOW"), 1).otherwise(0)).alias("yellow_flag_count"),
        F.sum(F.when(combined.contains("RED"), 1).otherwise(0)).alias("red_flag_count"),
        F.sum(F.when(combined.contains("GREEN"), 1).otherwise(0)).alias("green_flag_count"),
        (
            F.sum(F.when(combined.contains("SAFETY CAR"), 1).otherwise(0))
            + F.sum(F.when(combined.contains("VIRTUAL SAFETY CAR"), 1).otherwise(0))
        ).alias("safety_car_message_count"),
        F.sum(F.when(combined.contains("PIT EXIT"), 1).otherwise(0)).alias("pit_exit_message_count"),
    )


def build_metadata_features_spark(
    drivers: DataFrame | None,
    sessions: DataFrame | None,
    meetings: DataFrame | None,
) -> DataFrame:
    if drivers is None or drivers.rdd.isEmpty():
        return None
    meta = drivers
    if "country_code" in meta.columns:
        meta = meta.withColumnRenamed("country_code", "driver_country_code")
    keep = [c for c in DRIVER_KEYS + ["full_name", "name_acronym", "team_name", "driver_country_code"] if c in meta.columns]
    meta = meta.select(*keep).dropDuplicates(DRIVER_KEYS)
    if sessions is not None and not sessions.rdd.isEmpty():
        sess_cols = ["session_key"]
        for c in ("session_type", "session_name", "circuit_short_name", "location"):
            if c in sessions.columns:
                sess_cols.append(c)
        sess = sessions.select(
            *[c for c in sess_cols if c in sessions.columns],
            F.col("year").alias("session_year") if "year" in sessions.columns else F.lit(None).alias("session_year"),
            F.col("country_name").alias("session_country_name")
            if "country_name" in sessions.columns
            else F.lit(None).alias("session_country_name"),
        ).dropDuplicates(["session_key"])
        meta = meta.join(sess, on="session_key", how="left")
    if meetings is not None and not meetings.rdd.isEmpty():
        mcols = [c for c in ("meeting_key", "meeting_name", "circuit_key") if c in meetings.columns]
        meta = meta.join(meetings.select(*mcols).dropDuplicates(["meeting_key"]), on="meeting_key", how="left")
    return meta


def _join_quality_spark(
    name: str, before: int, after: int, base: DataFrame, merged: DataFrame, notes: str = ""
) -> dict[str, Any]:
    bkeys = safe_to_pandas(base.select(*DRIVER_KEYS).distinct(), 500_000)
    mkeys = safe_to_pandas(merged.select(*DRIVER_KEYS).distinct(), 500_000)
    base_set = set(map(tuple, bkeys[DRIVER_KEYS].dropna().values.tolist()))
    merged_set = set(map(tuple, mkeys[DRIVER_KEYS].dropna().values.tolist()))
    unmatched = len(base_set - merged_set)
    unmatched_pct = round(unmatched / len(base_set) * 100, 4) if base_set else 0.0
    return {
        "feature_group": name,
        "rows_before": before,
        "rows_after": after,
        "unmatched_base_rows": unmatched,
        "unmatched_pct": unmatched_pct,
        "notes": notes,
    }


def _left_join_spark(base: DataFrame, right: DataFrame | None, on: list[str], name: str, logs: list) -> DataFrame:
    before = base.count()
    if right is None or right.rdd.isEmpty() or not set(on).issubset(set(right.columns)):
        logs.append(
            _join_quality_spark(name, before, before, base, base, notes="empty or missing keys")
        )
        return base
    merged = base.join(right, on=on, how="left")
    after = merged.count()
    logs.append(_join_quality_spark(name, before, after, base, merged))
    return merged


def build_gold_feature_mart_spark(
    spark: SparkSession,
    silver_dir: Path,
    gold_dir: Path,
    data_quality_reports_dir: Path,
    feature_definitions_dir: Path,
) -> dict[str, Any]:
    """Build Gold mart with Spark; export small CSV reports via pandas."""
    silver_dir = Path(silver_dir)
    gold_dir = Path(gold_dir)
    data_quality_reports_dir = Path(data_quality_reports_dir)
    feature_definitions_dir = Path(feature_definitions_dir)
    ensure_dir(gold_dir)
    ensure_dir(data_quality_reports_dir)
    ensure_dir(feature_definitions_dir)

    sessions = _load_silver(spark, silver_dir, "sessions")
    meetings = _load_silver(spark, silver_dir, "meetings")
    session_result = _load_silver(spark, silver_dir, "session_result")
    if session_result is None:
        raise ValueError(
            "session_result_clean.parquet missing — required for Gold target (points_finish). "
            "Run 02_silver_cleaning_quality.ipynb first."
        )
    if session_result.rdd.isEmpty():
        raise ValueError(
            "session_result_clean.parquet is empty — cannot build Gold base table. "
            "Re-run Bronze ingestion and Silver cleaning."
        )

    base = build_target_base_spark(session_result)
    gold = base
    join_logs: list[dict[str, Any]] = []

    meta = build_metadata_features_spark(
        _load_silver(spark, silver_dir, "drivers"), sessions, meetings
    )
    gold = _left_join_spark(gold, meta, DRIVER_KEYS, "metadata", join_logs)
    gold = _left_join_spark(
        gold, build_lap_features_spark(_load_silver(spark, silver_dir, "laps"), sessions), DRIVER_KEYS, "laps", join_logs
    )
    gold = _left_join_spark(
        gold, build_pit_features_spark(_load_silver(spark, silver_dir, "pit"), sessions), DRIVER_KEYS, "pit", join_logs
    )
    gold = _left_join_spark(
        gold,
        build_position_features_spark(_load_silver(spark, silver_dir, "position"), sessions),
        DRIVER_KEYS,
        "position",
        join_logs,
    )
    gold = _left_join_spark(
        gold,
        build_weather_features_spark(_load_silver(spark, silver_dir, "weather"), sessions),
        SESSION_KEYS,
        "weather",
        join_logs,
    )
    gold = _left_join_spark(
        gold,
        build_race_control_features_spark(_load_silver(spark, silver_dir, "race_control"), sessions),
        SESSION_KEYS,
        "race_control",
        join_logs,
    )

    dupes = gold.groupBy(*DRIVER_KEYS).count().filter(F.col("count") > 1).count()
    if dupes:
        raise ValueError(f"Gold mart has {dupes} duplicate driver-race keys.")

    for col in EVENT_ABSENCE_ZERO_COLS:
        if col in gold.columns:
            gold = gold.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

    mart_path = gold_dir / GOLD_MART_FILENAME
    write_spark_dataframe(gold, mart_path)

    gold_pdf = safe_to_pandas(gold, max_rows=500_000)
    summary_path = data_quality_reports_dir / "gold_feature_summary_stats.csv"
    missing_path = data_quality_reports_dir / "gold_feature_missingness.csv"
    target_path = data_quality_reports_dir / "gold_target_distribution.csv"
    join_path = data_quality_reports_dir / "gold_join_quality_report.csv"
    leakage_path = data_quality_reports_dir / "gold_leakage_guard_report.csv"
    dict_path = feature_definitions_dir / "feature_dictionary.csv"

    save_dataframe_csv(summarize_dataframe(gold_pdf, "driver_race_feature_mart"), summary_path)
    save_dataframe_csv(compute_missingness(gold_pdf, "driver_race_feature_mart"), missing_path)
    save_dataframe_csv(build_gold_target_distribution(gold_pdf), target_path)
    save_dataframe_csv(pd.DataFrame(join_logs), join_path)
    leakage = build_leakage_guard_report(gold_pdf)
    save_dataframe_csv(leakage, leakage_path)
    write_feature_dictionary(build_feature_dictionary(gold_pdf), dict_path)

    model_features = get_model_feature_columns(gold_pdf)
    summary = {
        "engine": "spark",
        "row_count": len(gold_pdf),
        "column_count": len(gold_pdf.columns),
        "model_feature_count": len(model_features),
        "duplicate_grain_rows": int(dupes),
        "points_finish_positive_pct": round(gold_pdf[TARGET_COLUMN].mean() * 100, 4)
        if TARGET_COLUMN in gold_pdf.columns
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
