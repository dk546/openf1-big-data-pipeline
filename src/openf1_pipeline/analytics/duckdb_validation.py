"""
DuckDB SQL validation over pipeline Parquet/CSV artifacts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def create_duckdb_connection(database_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection (optional on-disk path)."""
    if database_path:
        return duckdb.connect(database_path)
    return duckdb.connect()


def _glob_parquet(path: Path) -> str:
    p = path.resolve()
    if p.is_file():
        return str(p).replace("\\", "/")
    return str(p / "**" / "*.parquet").replace("\\", "/")


def save_duckdb_validation_reports(
    reports: dict[str, pd.DataFrame],
    output_dir: Path,
    prefix: str,
) -> dict[str, str]:
    """Write DuckDB validation DataFrames to CSV."""
    output_dir = Path(output_dir)
    ensure_dir(output_dir)
    paths: dict[str, str] = {}
    for name, df in reports.items():
        out = output_dir / f"duckdb_{prefix}_{name}.csv"
        save_dataframe_csv(df, out)
        paths[name] = str(out)
        logger.info("DuckDB report %s -> %s (%s rows)", name, out, len(df))
    return paths


def validate_bronze_with_duckdb(
    data_quality_reports_dir: Path,
) -> dict[str, pd.DataFrame]:
    """Validate Bronze CSV evidence with DuckDB SQL."""
    dq = Path(data_quality_reports_dir)
    con = create_duckdb_connection()
    reports: dict[str, pd.DataFrame] = {}

    row_counts_path = dq / "bronze_row_counts.csv"
    if row_counts_path.exists():
        con.execute(
            f"CREATE OR REPLACE VIEW bronze_row_counts AS "
            f"SELECT * FROM read_csv_auto('{row_counts_path.as_posix()}')"
        )
        reports["bronze_rows_by_endpoint"] = con.execute(
            """
            SELECT endpoint, SUM(row_count) AS total_rows, COUNT(*) AS file_count
            FROM bronze_row_counts
            GROUP BY endpoint
            ORDER BY endpoint
            """
        ).df()
        reports["bronze_total_rows"] = con.execute(
            "SELECT SUM(row_count) AS total_rows, COUNT(*) AS file_count FROM bronze_row_counts"
        ).df()

    drift_path = dq / "bronze_schema_drift.csv"
    if drift_path.exists():
        con.execute(
            f"CREATE OR REPLACE VIEW bronze_schema_drift AS "
            f"SELECT * FROM read_csv_auto('{drift_path.as_posix()}')"
        )
        if "possible_schema_drift_flag" in con.execute(
            "DESCRIBE bronze_schema_drift"
        ).df()["column_name"].tolist():
            reports["bronze_schema_drift_flags"] = con.execute(
                """
                SELECT *
                FROM bronze_schema_drift
                WHERE possible_schema_drift_flag = TRUE
                """
            ).df()
        else:
            reports["bronze_schema_drift_flags"] = pd.DataFrame()

    inventory_path = dq / "bronze_file_inventory.csv"
    if inventory_path.exists():
        con.execute(
            f"CREATE OR REPLACE VIEW bronze_inventory AS "
            f"SELECT * FROM read_csv_auto('{inventory_path.as_posix()}')"
        )
        reports["bronze_endpoint_coverage"] = con.execute(
            """
            SELECT endpoint, COUNT(*) AS files, SUM(file_size_bytes) AS total_bytes
            FROM bronze_inventory
            GROUP BY endpoint
            ORDER BY endpoint
            """
        ).df()

    con.close()
    return reports


def validate_silver_with_duckdb(silver_dir: Path) -> dict[str, pd.DataFrame]:
    """Run DuckDB SQL checks over Silver Parquet tables."""
    silver_dir = Path(silver_dir)
    con = create_duckdb_connection()
    reports: dict[str, pd.DataFrame] = {}

    tables = [
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
    inventory_rows: list[dict[str, Any]] = []
    for name in tables:
        pq = silver_dir / f"{name}_clean.parquet"
        if not pq.exists():
            inventory_rows.append({"table_name": name, "row_count": 0, "status": "missing"})
            continue
        glob = _glob_parquet(pq)
        try:
            n = con.execute(
                f"SELECT COUNT(*) AS row_count FROM read_parquet('{glob}')"
            ).fetchone()[0]
            inventory_rows.append({"table_name": name, "row_count": n, "status": "ok"})
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{glob}')"
            )
        except Exception as exc:
            inventory_rows.append(
                {"table_name": name, "row_count": 0, "status": f"error: {exc}"}
            )

    reports["silver_row_counts"] = pd.DataFrame(inventory_rows)
    ok_tables = {r["table_name"] for r in inventory_rows if r.get("status") == "ok"}

    if "session_result" in ok_tables:
        reports["session_result_target_support"] = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) AS points_positive_rows,
                ROUND(100.0 * SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4)
                    AS points_positive_pct
            FROM session_result
            """
        ).df()
        reports["session_result_duplicate_keys"] = con.execute(
            """
            SELECT session_key, driver_number, COUNT(*) AS cnt
            FROM session_result
            GROUP BY session_key, driver_number
            HAVING COUNT(*) > 1
            """
        ).df()

    for tbl, key_cols in [
        ("laps", "session_key"),
        ("pit", "session_key"),
        ("weather", "session_key"),
    ]:
        if tbl in ok_tables:
            cols = ", ".join(key_cols)
            reports[f"{tbl}_rows_by_session_key"] = con.execute(
                f"""
                SELECT {cols}, COUNT(*) AS row_count
                FROM {tbl}
                GROUP BY {cols}
                ORDER BY row_count DESC
                LIMIT 20
                """
            ).df()

    con.close()
    return reports


def validate_gold_with_duckdb(gold_path: Path) -> dict[str, pd.DataFrame]:
    """Validate Gold feature mart Parquet with DuckDB."""
    gold_path = Path(gold_path)
    con = create_duckdb_connection()
    reports: dict[str, pd.DataFrame] = {}

    if not gold_path.exists():
        con.close()
        return {"gold_status": pd.DataFrame([{"status": "missing", "path": str(gold_path)}])}

    glob = _glob_parquet(gold_path)
    con.execute(f"CREATE OR REPLACE VIEW gold AS SELECT * FROM read_parquet('{glob}')")

    reports["gold_row_count"] = con.execute("SELECT COUNT(*) AS total_rows FROM gold").df()
    reports["gold_duplicate_keys"] = con.execute(
        """
        SELECT session_key, meeting_key, driver_number, COUNT(*) AS cnt
        FROM gold
        GROUP BY session_key, meeting_key, driver_number
        HAVING COUNT(*) > 1
        """
    ).df()
    reports["gold_target_distribution"] = con.execute(
        """
        SELECT points_finish, COUNT(*) AS cnt,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
        FROM gold
        GROUP BY points_finish
        ORDER BY points_finish
        """
    ).df()

    if "team_name" in con.execute("DESCRIBE gold").df()["column_name"].tolist():
        reports["points_finish_by_team"] = con.execute(
            """
            SELECT team_name, points_finish, COUNT(*) AS cnt
            FROM gold
            WHERE team_name IS NOT NULL
            GROUP BY team_name, points_finish
            ORDER BY team_name, points_finish
            """
        ).df()

    if "circuit_short_name" in con.execute("DESCRIBE gold").df()["column_name"].tolist():
        reports["points_finish_by_circuit"] = con.execute(
            """
            SELECT circuit_short_name, points_finish, COUNT(*) AS cnt
            FROM gold
            WHERE circuit_short_name IS NOT NULL
            GROUP BY circuit_short_name, points_finish
            ORDER BY circuit_short_name, points_finish
            """
        ).df()

    # Missingness on a sample of columns (practical limit)
    cols = con.execute("DESCRIBE gold").df()["column_name"].tolist()[:40]
    miss_rows = []
    total = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]
    for col in cols:
        nulls = con.execute(
            f"SELECT COUNT(*) FROM gold WHERE \"{col}\" IS NULL"
        ).fetchone()[0]
        miss_rows.append(
            {
                "column_name": col,
                "missing_count": nulls,
                "missing_pct": round(nulls / total * 100, 4) if total else 0.0,
            }
        )
    reports["gold_missingness_summary"] = pd.DataFrame(miss_rows)

    con.close()
    return reports
