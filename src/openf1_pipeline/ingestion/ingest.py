"""
Orchestrate OpenF1 ingestion into Bronze JSONL storage.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from openf1_pipeline.config import (
    ENDPOINTS,
    GLOBAL_ENDPOINTS,
    RACE_SESSION_NAMES,
    SEASONS,
    SESSION_ENDPOINTS,
)
from openf1_pipeline.ingestion.openf1_client import OpenF1Client
from openf1_pipeline.utils.io import ensure_dir, save_dataframe_csv, save_jsonl
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

MANIFEST_COLUMNS = [
    "endpoint",
    "year",
    "session_key",
    "output_path",
    "record_count",
    "status",
    "error_message",
    "ingestion_timestamp_utc",
]

# Endpoints that may be missing for some sessions; failures are logged but non-fatal.
OPTIONAL_SESSION_ENDPOINTS = frozenset({"starting_grid"})

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _empty_manifest() -> pd.DataFrame:
    return pd.DataFrame(columns=MANIFEST_COLUMNS)


def _manifest_row(
    *,
    endpoint: str,
    year: int | None,
    session_key: int | str | None,
    output_path: Path | None,
    record_count: int,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "year": year,
        "session_key": session_key,
        "output_path": str(output_path) if output_path else None,
        "record_count": record_count,
        "status": status,
        "error_message": error_message,
        "ingestion_timestamp_utc": _utc_now_iso(),
    }


def get_race_sessions(client: OpenF1Client, seasons: list[int]) -> pd.DataFrame:
    """
    Fetch sessions per season and filter to main race sessions.

    Returns a DataFrame with year, meeting_key, session_key, session_name,
    and available circuit/country/date fields.
    """
    frames: list[pd.DataFrame] = []
    race_names_lower = {name.lower() for name in RACE_SESSION_NAMES}

    for year in seasons:
        records, error = client.fetch_endpoint("sessions", params={"year": year})
        if error is not None:
            logger.error("Failed to fetch sessions for year=%s: %s", year, error)
            continue

        if not records:
            logger.warning("No sessions returned for year=%s", year)
            continue

        df = pd.DataFrame(records)
        df["year"] = year

        name_col = "session_name" if "session_name" in df.columns else None
        type_col = "session_type" if "session_type" in df.columns else None

        mask = pd.Series(False, index=df.index)
        if name_col:
            mask = mask | df[name_col].astype(str).str.lower().isin(race_names_lower)
        if type_col:
            mask = mask | df[type_col].astype(str).str.lower().isin(race_names_lower)

        race_df = df.loc[mask].copy()
        logger.info("Year %s: %s race sessions of %s total", year, len(race_df), len(df))
        frames.append(race_df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "year",
                "meeting_key",
                "session_key",
                "session_name",
                "country_name",
                "circuit_short_name",
                "date_start",
                "date_end",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    keep_cols = [
        "year",
        "meeting_key",
        "session_key",
        "session_name",
        "country_name",
        "circuit_short_name",
        "date_start",
        "date_end",
    ]
    for col in keep_cols:
        if col not in combined.columns:
            combined[col] = None
    combined = combined.drop_duplicates(subset=["session_key"], keep="last")
    combined = combined.sort_values(["year", "session_key"]).reset_index(drop=True)
    return combined[keep_cols]


def ingest_global_endpoint(
    client: OpenF1Client,
    endpoint: str,
    seasons: list[int],
    output_base_dir: Path,
) -> pd.DataFrame:
    """
    Ingest meetings or sessions per season to:
    data/bronze/{endpoint}/year={year}/{endpoint}.jsonl
    """
    rows: list[dict[str, Any]] = []

    for year in seasons:
        output_path = (
            Path(output_base_dir)
            / endpoint
            / f"year={year}"
            / f"{endpoint}.jsonl"
        )
        params: dict[str, Any] = {"year": year}
        records, error = client.fetch_endpoint(endpoint, params=params)

        if error is not None:
            logger.error("Global ingest failed endpoint=%s year=%s: %s", endpoint, year, error)
            rows.append(
                _manifest_row(
                    endpoint=endpoint,
                    year=year,
                    session_key=None,
                    output_path=output_path,
                    record_count=0,
                    status=STATUS_FAILED,
                    error_message=error,
                )
            )
            continue

        ensure_dir(output_path.parent)
        count = save_jsonl(records, output_path)
        rows.append(
            _manifest_row(
                endpoint=endpoint,
                year=year,
                session_key=None,
                output_path=output_path,
                record_count=count,
                status=STATUS_SUCCESS,
            )
        )
        if count == 0:
            logger.warning("Global endpoint %s year=%s returned 0 rows (success)", endpoint, year)

    return pd.DataFrame(rows)


def ingest_endpoint_for_sessions(
    client: OpenF1Client,
    endpoint: str,
    sessions_df: pd.DataFrame,
    output_base_dir: Path,
) -> pd.DataFrame:
    """
    For each session_key, download endpoint data and save JSONL under Bronze.

    Zero rows after a successful API call is ``success``. Request failures are
    ``failed`` and recorded in the manifest without stopping other endpoints.
    """
    rows: list[dict[str, Any]] = []

    if sessions_df.empty:
        logger.warning("No sessions to ingest for endpoint=%s", endpoint)
        return _empty_manifest()

    for _, session in sessions_df.iterrows():
        session_key = session["session_key"]
        year = int(session["year"])
        output_path = (
            Path(output_base_dir)
            / endpoint
            / f"year={year}"
            / f"session_key={session_key}.jsonl"
        )

        records, error = client.fetch_endpoint_for_session(endpoint, session_key)

        if error is not None:
            logger.warning(
                "Session ingest failed endpoint=%s session_key=%s: %s",
                endpoint,
                session_key,
                error,
            )
            rows.append(
                _manifest_row(
                    endpoint=endpoint,
                    year=year,
                    session_key=session_key,
                    output_path=output_path,
                    record_count=0,
                    status=STATUS_FAILED,
                    error_message=error,
                )
            )
            if endpoint in OPTIONAL_SESSION_ENDPOINTS:
                logger.info(
                    "Optional endpoint %s failed for session %s; continuing.",
                    endpoint,
                    session_key,
                )
            continue

        ensure_dir(output_path.parent)
        count = save_jsonl(records, output_path)
        rows.append(
            _manifest_row(
                endpoint=endpoint,
                year=year,
                session_key=session_key,
                output_path=output_path,
                record_count=count,
                status=STATUS_SUCCESS,
            )
        )
        if count == 0:
            logger.info(
                "Endpoint %s session_key=%s returned 0 rows (success).",
                endpoint,
                session_key,
            )

    return pd.DataFrame(rows)


def run_bronze_ingestion(
    seasons: list[int] | None = None,
    endpoints: list[str] | None = None,
    bronze_dir: Path | None = None,
    manifests_dir: Path | None = None,
    max_sessions: int | None = None,
    endpoints_override: list[str] | None = None,
) -> pd.DataFrame:
    """
    Run full Bronze ingestion: global endpoints, race sessions, session-level data.

    Saves combined manifest to artifacts/manifests/ingestion_manifest.csv.
    """
    from openf1_pipeline.config import get_bronze_dir, get_manifests_dir

    seasons = seasons or SEASONS
    bronze_dir = Path(bronze_dir or get_bronze_dir())
    manifests_dir = Path(manifests_dir or get_manifests_dir())
    ensure_dir(bronze_dir)
    ensure_dir(manifests_dir)

    active_endpoints = set(endpoints_override or endpoints or ENDPOINTS)
    client = OpenF1Client()

    manifest_parts: list[pd.DataFrame] = []

    for endpoint in GLOBAL_ENDPOINTS:
        if endpoint not in active_endpoints:
            continue
        logger.info("Ingesting global endpoint: %s", endpoint)
        part = ingest_global_endpoint(client, endpoint, seasons, bronze_dir)
        manifest_parts.append(part)

    race_sessions = get_race_sessions(client, seasons)
    if max_sessions is not None and not race_sessions.empty:
        race_sessions = race_sessions.head(max_sessions).copy()
        logger.info("Limited to max_sessions=%s", max_sessions)

    for endpoint in SESSION_ENDPOINTS:
        if endpoint not in active_endpoints:
            continue
        logger.info(
            "Ingesting session endpoint: %s (%s sessions)",
            endpoint,
            len(race_sessions),
        )
        part = ingest_endpoint_for_sessions(
            client, endpoint, race_sessions, bronze_dir
        )
        manifest_parts.append(part)

    if manifest_parts:
        manifest_df = pd.concat(manifest_parts, ignore_index=True)
    else:
        manifest_df = _empty_manifest()

    manifest_path = manifests_dir / "ingestion_manifest.csv"
    save_dataframe_csv(manifest_df, manifest_path)
    logger.info(
        "Ingestion complete: %s manifest rows -> %s",
        len(manifest_df),
        manifest_path,
    )
    if not manifest_df.empty:
        logger.info(
            "Manifest summary:\n%s",
            manifest_df.groupby(["endpoint", "status"]).size().to_string(),
        )

    return manifest_df


def summarize_manifest(manifest_df: pd.DataFrame) -> dict[str, Any]:
    """Build a small summary dict for notebooks and run manifests."""
    if manifest_df.empty:
        return {
            "status_counts": {},
            "failed_endpoints": [],
            "success_endpoints": [],
            "row_counts_by_endpoint": {},
            "session_result_total_rows": 0,
            "starting_grid_total_rows": 0,
        }

    status_counts = manifest_df["status"].value_counts().to_dict()
    failed = (
        manifest_df.loc[manifest_df["status"] == STATUS_FAILED, "endpoint"]
        .drop_duplicates()
        .tolist()
    )
    success = (
        manifest_df.loc[manifest_df["status"] == STATUS_SUCCESS, "endpoint"]
        .drop_duplicates()
        .tolist()
    )
    row_counts = (
        manifest_df.groupby("endpoint")["record_count"].sum().astype(int).to_dict()
    )

    def _endpoint_rows(name: str) -> int:
        subset = manifest_df.loc[manifest_df["endpoint"] == name]
        return int(subset.loc[subset["status"] == STATUS_SUCCESS, "record_count"].sum())

    return {
        "status_counts": status_counts,
        "failed_endpoints": failed,
        "success_endpoints": success,
        "row_counts_by_endpoint": row_counts,
        "session_result_total_rows": _endpoint_rows("session_result"),
        "starting_grid_total_rows": _endpoint_rows("starting_grid"),
    }
