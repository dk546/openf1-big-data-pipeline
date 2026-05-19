"""
Build consolidated report tables from existing pipeline and model CSV artifacts.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def _read_csv_if_exists(path: Path) -> pd.DataFrame | None:
    path = Path(path)
    if not path.is_file():
        logger.warning("Report input missing: %s", path)
        return None
    return pd.read_csv(path)


def build_data_volume_by_layer(
    bronze_row_counts_path: Path,
    silver_inventory_path: Path,
    gold_row_count: int | None = None,
) -> pd.DataFrame:
    """Aggregate row counts by pipeline layer."""
    rows: list[dict] = []
    bronze = _read_csv_if_exists(bronze_row_counts_path)
    if bronze is not None and "row_count" in bronze.columns:
        rows.append(
            {
                "layer": "bronze",
                "entity": "all_endpoints",
                "row_count": int(bronze["row_count"].sum()),
                "file_count": len(bronze),
            }
        )
        for ep, grp in bronze.groupby("endpoint"):
            rows.append(
                {
                    "layer": "bronze",
                    "entity": ep,
                    "row_count": int(grp["row_count"].sum()),
                    "file_count": len(grp),
                }
            )

    silver = _read_csv_if_exists(silver_inventory_path)
    if silver is not None and "row_count" in silver.columns:
        for _, r in silver.iterrows():
            rows.append(
                {
                    "layer": "silver",
                    "entity": r.get("table_name", ""),
                    "row_count": int(r["row_count"]),
                    "file_count": 1,
                }
            )

    if gold_row_count is not None:
        rows.append(
            {
                "layer": "gold",
                "entity": "driver_race_feature_mart",
                "row_count": int(gold_row_count),
                "file_count": 1,
            }
        )

    return pd.DataFrame(rows)


def build_bronze_endpoint_row_counts(bronze_row_counts_path: Path) -> pd.DataFrame:
    bronze = _read_csv_if_exists(bronze_row_counts_path)
    if bronze is None:
        return pd.DataFrame(columns=["endpoint", "total_rows", "file_count"])
    out = (
        bronze.groupby("endpoint", as_index=False)["row_count"]
        .agg(total_rows="sum", file_count="count")
        .sort_values("endpoint")
    )
    return out


def build_silver_cleaning_impact_table(silver_impact_path: Path) -> pd.DataFrame:
    df = _read_csv_if_exists(silver_impact_path)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def build_silver_error_taxonomy_table(
    duplicate_path: Path,
    outlier_path: Path,
    temporal_path: Path,
    ri_path: Path,
) -> pd.DataFrame:
    """Summarize Silver DQ issue counts by report type."""
    rows: list[dict] = []
    for label, path in [
        ("duplicate", duplicate_path),
        ("outlier", outlier_path),
        ("temporal", temporal_path),
        ("referential_integrity", ri_path),
    ]:
        df = _read_csv_if_exists(path)
        if df is None:
            continue
        rows.append({"report_type": label, "issue_rows": len(df), "source_file": str(path.name)})
    return pd.DataFrame(rows)


def build_gold_feature_group_summary(feature_dictionary_path: Path) -> pd.DataFrame:
    fd = _read_csv_if_exists(feature_dictionary_path)
    if fd is None:
        return pd.DataFrame()
    agg = (
        fd.groupby("feature_group", as_index=False)
        .agg(
            feature_count=("feature_name", "count"),
            allowed_for_modeling_count=("allowed_for_modeling", lambda s: int(s.sum())),
            avg_missing_pct=("missing_pct", "mean"),
        )
        .sort_values("feature_group")
    )
    return agg


def build_gold_target_distribution_table(gold_target_path: Path) -> pd.DataFrame:
    df = _read_csv_if_exists(gold_target_path)
    return df.copy() if df is not None else pd.DataFrame()


def build_model_baseline_comparison_table(baseline_metrics_path: Path) -> pd.DataFrame:
    df = _read_csv_if_exists(baseline_metrics_path)
    return df.copy() if df is not None else pd.DataFrame()


def build_model_validation_test_metrics_table(
    validation_metrics_path: Path,
    test_metrics_path: Path,
) -> pd.DataFrame:
    parts = []
    for split, path in [("validation", validation_metrics_path), ("test", test_metrics_path)]:
        df = _read_csv_if_exists(path)
        if df is not None:
            parts.append(df.assign(split=split) if "split" not in df.columns else df)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def build_confusion_matrix_table(confusion_path: Path) -> pd.DataFrame:
    df = _read_csv_if_exists(confusion_path)
    return df.copy() if df is not None else pd.DataFrame()


def build_error_analysis_summary_table(error_analysis_path: Path) -> pd.DataFrame:
    df = _read_csv_if_exists(error_analysis_path)
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return df
    return (
        df.groupby(["model_name", "split", "error_type"], as_index=False)["count"]
        .sum()
        .sort_values(["model_name", "split", "error_type"])
    )


def build_reproducibility_artifacts_table(
    manifests_dir: Path,
    artifacts_dir: Path,
    reports_dir: Path,
) -> pd.DataFrame:
    """List key artifact paths for MBA reproducibility section."""
    manifests_dir = Path(manifests_dir)
    artifacts_dir = Path(artifacts_dir)
    reports_dir = Path(reports_dir)
    candidates = [
        ("ingestion_manifest", manifests_dir / "ingestion_manifest.csv"),
        ("model_run_manifest", manifests_dir / "model_run_manifest.json"),
        ("run_manifest", manifests_dir / "run_manifest.json"),
        ("feature_dictionary", artifacts_dir / "feature_definitions/feature_dictionary.csv"),
        ("gold_leakage_guard", reports_dir / "data_quality/gold_leakage_guard_report.csv"),
        ("baseline_metrics", reports_dir / "model_results/baseline_metrics.csv"),
        ("validation_metrics", reports_dir / "model_results/validation_metrics.csv"),
        ("test_metrics", reports_dir / "model_results/test_metrics.csv"),
    ]
    rows = []
    for name, path in candidates:
        rows.append({"artifact_name": name, "path": str(path), "exists": path.exists()})
    return pd.DataFrame(rows)


def write_report_tables(
    tables: dict[str, pd.DataFrame],
    tables_dir: Path,
) -> dict[str, Path]:
    """Write named report tables to reports/tables/."""
    tables_dir = Path(tables_dir)
    ensure_dir(tables_dir)
    paths: dict[str, Path] = {}
    for name, df in tables.items():
        if df is None or df.empty:
            logger.warning("Skipping empty report table: %s", name)
            continue
        out = tables_dir / f"{name}.csv"
        save_dataframe_csv(df, out)
        paths[name] = out
    return paths
