"""
Orchestrate OpenF1 ingestion into Bronze JSONL storage.
"""

from __future__ import annotations

import time
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

RETRY_MANIFEST_COLUMNS = MANIFEST_COLUMNS + [
    "previous_status",
    "previous_error_message",
    "retry_attempt",
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


def _resolve_retry_endpoint_set(
    endpoints_to_retry: list[str] | None,
    include_optional: bool,
) -> set[str]:
    """Default to session endpoints; drop optional ones unless explicitly included."""
    if endpoints_to_retry is None:
        candidates = set(SESSION_ENDPOINTS)
    else:
        candidates = {ep for ep in endpoints_to_retry if ep in SESSION_ENDPOINTS}
    if not include_optional:
        candidates -= OPTIONAL_SESSION_ENDPOINTS
    return candidates


def _coerce_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def retry_failed_session_endpoints(
    manifest_path: Path | None = None,
    bronze_dir: Path | None = None,
    manifests_dir: Path | None = None,
    endpoints_to_retry: list[str] | None = None,
    include_optional: bool = False,
    sleep_seconds: float = 3.0,
    max_retries_per_request: int = 5,
    timeout: int = 60,
    retry_manifest_filename: str = "ingestion_retry_manifest.csv",
) -> pd.DataFrame:
    """
    Targeted re-ingestion of failed session-level Bronze endpoints.

    Reads the existing ``ingestion_manifest.csv``, picks rows where
    ``status != success`` and ``endpoint`` is a session-level endpoint, throttles
    a fresh ``OpenF1Client`` (default 3 s baseline sleep) so the retry does not
    re-trigger a 429 storm, writes only those endpoint/session JSONL files, and
    saves a separate ``ingestion_retry_manifest.csv``. The original manifest is
    **not** modified.

    Global endpoints (``meetings``, ``sessions``) are never retried by this
    function. ``starting_grid`` (and any other endpoint in
    ``OPTIONAL_SESSION_ENDPOINTS``) is excluded unless ``include_optional=True``.

    Parameters
    ----------
    manifest_path : Path | None
        Override the manifest CSV path. Defaults to
        ``artifacts/manifests/ingestion_manifest.csv``.
    bronze_dir : Path | None
        Override the Bronze root. Defaults to ``get_bronze_dir()``.
    manifests_dir : Path | None
        Override the manifests dir for the retry CSV. Defaults to
        ``get_manifests_dir()``.
    endpoints_to_retry : list[str] | None
        Restrict retry to this endpoint subset. Defaults to all session
        endpoints (minus optional ones when ``include_optional=False``).
    include_optional : bool
        If ``False`` (default), exclude ``starting_grid`` from retry. The full
        run already records 100% ``starting_grid`` failure as expected/optional.
    sleep_seconds : float
        Base inter-request sleep used by the throttled ``OpenF1Client``.
    max_retries_per_request : int
        Per-request retry budget inside ``OpenF1Client._request_with_retries``.
    timeout : int
        Per-request HTTP timeout in seconds.
    retry_manifest_filename : str
        Output filename for the retry manifest CSV. Saved next to the original
        manifest. The original ``ingestion_manifest.csv`` is preserved.

    Returns
    -------
    pd.DataFrame
        The retry manifest. Empty DataFrame (with full columns) if there is
        nothing eligible to retry.
    """
    from openf1_pipeline.config import get_bronze_dir, get_manifests_dir

    bronze_dir = Path(bronze_dir or get_bronze_dir())
    manifests_dir = Path(manifests_dir or get_manifests_dir())
    manifest_path = Path(manifest_path or (manifests_dir / "ingestion_manifest.csv"))

    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Ingestion manifest not found at {manifest_path}. "
            "Run notebook 01 ingestion first."
        )

    manifest_df = pd.read_csv(manifest_path)
    if manifest_df.empty:
        logger.warning("Manifest is empty; nothing to retry.")
        return pd.DataFrame(columns=RETRY_MANIFEST_COLUMNS)

    eligible_endpoints = _resolve_retry_endpoint_set(endpoints_to_retry, include_optional)
    logger.info(
        "Retry eligible endpoints (include_optional=%s): %s",
        include_optional,
        sorted(eligible_endpoints),
    )

    failed_mask = manifest_df["status"] != STATUS_SUCCESS
    endpoint_mask = manifest_df["endpoint"].isin(eligible_endpoints)
    session_mask = manifest_df["session_key"].notna()
    targets = manifest_df.loc[failed_mask & endpoint_mask & session_mask].copy()

    if targets.empty:
        logger.info(
            "No failed session-level rows to retry under current filters "
            "(eligible=%s, include_optional=%s).",
            sorted(eligible_endpoints),
            include_optional,
        )
        ensure_dir(manifests_dir)
        empty = pd.DataFrame(columns=RETRY_MANIFEST_COLUMNS)
        save_dataframe_csv(empty, manifests_dir / retry_manifest_filename)
        return empty

    # Deduplicate to (endpoint, session_key) pairs and keep the latest failure
    # row for context (previous_status / previous_error_message).
    targets = (
        targets.sort_values(["endpoint", "session_key", "ingestion_timestamp_utc"])
        .drop_duplicates(subset=["endpoint", "session_key"], keep="last")
        .reset_index(drop=True)
    )

    logger.info(
        "Retry plan: %s (endpoint, session_key) targets at sleep_seconds=%s",
        len(targets),
        sleep_seconds,
    )

    client = OpenF1Client(
        timeout=timeout,
        max_retries=max_retries_per_request,
        sleep_seconds=sleep_seconds,
    )

    retry_rows: list[dict[str, Any]] = []

    for idx, row in targets.iterrows():
        endpoint = str(row["endpoint"])
        session_key = _coerce_int_or_none(row["session_key"])
        year = _coerce_int_or_none(row["year"])
        prev_status = row.get("status")
        prev_error = row.get("error_message")

        if session_key is None or year is None:
            logger.warning(
                "Skipping retry row with missing session_key/year: %s", row.to_dict()
            )
            continue

        output_path = (
            bronze_dir
            / endpoint
            / f"year={year}"
            / f"session_key={session_key}.jsonl"
        )

        logger.info(
            "[retry %s/%s] %s session_key=%s (sleep=%ss)",
            idx + 1,
            len(targets),
            endpoint,
            session_key,
            sleep_seconds,
        )

        records, error = client.fetch_endpoint_for_session(endpoint, session_key)

        base = _manifest_row(
            endpoint=endpoint,
            year=year,
            session_key=session_key,
            output_path=output_path,
            record_count=0,
            status=STATUS_FAILED,
            error_message=error,
        )
        base.update(
            {
                "previous_status": prev_status,
                "previous_error_message": prev_error,
                "retry_attempt": 1,
            }
        )

        if error is not None:
            retry_rows.append(base)
            # Extra inter-request pause after a failure to ease pressure on the API.
            time.sleep(sleep_seconds)
            continue

        ensure_dir(output_path.parent)
        count = save_jsonl(records, output_path)
        base["record_count"] = count
        base["status"] = STATUS_SUCCESS
        base["error_message"] = None
        retry_rows.append(base)

        # Inter-request throttling between successful calls as well.
        time.sleep(sleep_seconds)

    retry_df = pd.DataFrame(retry_rows, columns=RETRY_MANIFEST_COLUMNS)
    ensure_dir(manifests_dir)
    retry_path = manifests_dir / retry_manifest_filename
    save_dataframe_csv(retry_df, retry_path)

    if not retry_df.empty:
        summary = (
            retry_df.groupby(["endpoint", "status"]).size().unstack(fill_value=0)
        )
        logger.info(
            "Retry complete: %s rows -> %s\n%s",
            len(retry_df),
            retry_path,
            summary.to_string(),
        )
    else:
        logger.info("Retry complete: 0 rows -> %s", retry_path)

    return retry_df


def merge_retry_into_manifest(
    manifest_df: pd.DataFrame,
    retry_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return a merged manifest where retry rows supersede their original failures.

    Strategy: drop original rows whose ``(endpoint, session_key)`` was attempted
    in the retry (regardless of retry outcome), then concatenate retry rows
    truncated to the original ``MANIFEST_COLUMNS`` schema. The original manifest
    DataFrame and CSV are not mutated. The caller decides whether to persist the
    result.
    """
    if manifest_df.empty:
        return retry_df[MANIFEST_COLUMNS].copy() if not retry_df.empty else manifest_df.copy()
    if retry_df.empty:
        return manifest_df.copy()

    retry_keys = set(
        zip(
            retry_df["endpoint"].astype(str).tolist(),
            retry_df["session_key"].apply(_coerce_int_or_none).tolist(),
        )
    )

    def _drop_key(row: pd.Series) -> bool:
        sk = _coerce_int_or_none(row.get("session_key"))
        if sk is None:
            return False
        return (str(row["endpoint"]), sk) in retry_keys

    keep_mask = ~manifest_df.apply(_drop_key, axis=1)
    kept = manifest_df.loc[keep_mask, MANIFEST_COLUMNS].copy()
    retry_slim = retry_df[MANIFEST_COLUMNS].copy()
    return pd.concat([kept, retry_slim], ignore_index=True)


def summarize_retry_manifest(retry_df: pd.DataFrame) -> dict[str, Any]:
    """Compact summary of a retry manifest for notebooks and ChatGPT updates."""
    if retry_df.empty:
        return {
            "retry_attempts": 0,
            "newly_successful": 0,
            "still_failed": 0,
            "by_endpoint": {},
            "newly_successful_session_result": 0,
        }
    status_counts = retry_df["status"].value_counts().to_dict()
    by_endpoint = (
        retry_df.groupby(["endpoint", "status"]).size().unstack(fill_value=0).to_dict("index")
    )
    sr = retry_df.loc[retry_df["endpoint"] == "session_result"]
    sr_success = int((sr["status"] == STATUS_SUCCESS).sum()) if not sr.empty else 0
    return {
        "retry_attempts": int(len(retry_df)),
        "newly_successful": int(status_counts.get(STATUS_SUCCESS, 0)),
        "still_failed": int(status_counts.get(STATUS_FAILED, 0)),
        "by_endpoint": by_endpoint,
        "newly_successful_session_result": sr_success,
    }


def delete_stale_bronze_files(
    bronze_dir: Path | None = None,
    paths: list[str] | None = None,
) -> list[Path]:
    """
    Delete a specific list of stale Bronze JSONL files relative to ``bronze_dir``.

    Use this only after a successful retry has replaced the data (or if you
    explicitly want a manifest-clean Drive state). Returns the list of files
    actually deleted.
    """
    from openf1_pipeline.config import get_bronze_dir

    bronze_dir = Path(bronze_dir or get_bronze_dir())
    if not paths:
        return []

    deleted: list[Path] = []
    for rel in paths:
        candidate = (bronze_dir / rel) if not Path(rel).is_absolute() else Path(rel)
        if candidate.is_file():
            candidate.unlink()
            deleted.append(candidate)
            logger.info("Deleted stale Bronze file: %s", candidate)
        else:
            logger.info("Stale file not present (skipped): %s", candidate)
    return deleted
