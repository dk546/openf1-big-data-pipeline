"""
HTTP client for the OpenF1 API (retries, raw JSON list responses).
"""

from __future__ import annotations

import time
from typing import Any

import requests

from openf1_pipeline.config import OPENF1_BASE_URL
from openf1_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

# OpenF1 does not document cursor/page pagination on v1 endpoints. Responses are
# returned as a single JSON array for the supplied filters (e.g. session_key).
# This project controls payload size by querying session-level endpoints rather
# than unscoped global pulls for high-volume tables (laps, position).


class OpenF1Client:
    """Thin wrapper around OpenF1 REST endpoints."""

    def __init__(
        self,
        base_url: str = OPENF1_BASE_URL,
        timeout: int = 60,
        max_retries: int = 3,
        sleep_seconds: float = 0.25,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self._session = requests.Session()

    def build_url(self, endpoint: str) -> str:
        """Return full URL for an endpoint name."""
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def _normalize_response(self, payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            return [payload]
        raise TypeError(f"Unexpected API payload type: {type(payload)}")

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """
        GET an endpoint with optional query parameters.

        Retries transient network/server errors. Raises on persistent HTTP or
        connection failures. See ``fetch_endpoint`` for non-raising ingestion use.
        """
        records, error = self._request_with_retries(endpoint, params)
        if error is not None:
            raise RuntimeError(error)
        return records

    def get_endpoint(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Alias for ``get`` — fetch an OpenF1 endpoint as a list of dicts."""
        return self.get(endpoint, params=params)

    def fetch_endpoint(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch endpoint data without raising on HTTP 4xx/5xx.

        Returns ``(records, error_message)``. ``error_message`` is ``None`` on
        success (including successful calls that return zero rows).
        """
        return self._request_with_retries(endpoint, params)

    def _request_with_retries(
        self,
        endpoint: str,
        params: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        url = self.build_url(endpoint)
        params = params or {}
        last_error: str | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                if attempt > 1:
                    time.sleep(self.sleep_seconds * attempt)
                else:
                    time.sleep(self.sleep_seconds)

                response = self._session.get(url, params=params, timeout=self.timeout)

                if response.status_code == 429:
                    last_error = (
                        f"HTTP 429 rate limit for {endpoint} params={params}"
                    )
                    logger.warning(
                        "%s (attempt %s/%s)",
                        last_error,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(self.sleep_seconds * (2**attempt))
                    continue

                if response.status_code in (400, 404):
                    detail = response.text[:500]
                    msg = (
                        f"HTTP {response.status_code} for {endpoint} "
                        f"params={params}: {detail}"
                    )
                    logger.warning(msg)
                    return [], msg

                response.raise_for_status()
                records = self._normalize_response(response.json())
                logger.info(
                    "OpenF1 GET %s params=%s -> %s records",
                    endpoint,
                    params,
                    len(records),
                )
                return records, None

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                last_error = f"HTTP {status} for {endpoint} params={params}: {exc}"
                logger.warning(
                    "OpenF1 GET %s attempt %s/%s HTTP error: %s",
                    endpoint,
                    attempt,
                    self.max_retries,
                    last_error,
                )
            except requests.RequestException as exc:
                last_error = f"Request failed for {endpoint} params={params}: {exc}"
                logger.warning(
                    "OpenF1 GET %s attempt %s/%s failed: %s",
                    endpoint,
                    attempt,
                    self.max_retries,
                    last_error,
                )

        return [], last_error or f"OpenF1 request failed: {url} params={params}"

    def get_meetings(self, year: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if year is not None:
            params["year"] = year
        return self.get("meetings", params=params or None)

    def get_sessions(
        self,
        year: int | None = None,
        session_name: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if year is not None:
            params["year"] = year
        if session_name is not None:
            params["session_name"] = session_name
        return self.get("sessions", params=params or None)

    def get_endpoint_for_session(
        self,
        endpoint: str,
        session_key: int | str,
    ) -> list[dict[str, Any]]:
        return self.get_endpoint(endpoint, params={"session_key": session_key})

    def fetch_endpoint_for_session(
        self,
        endpoint: str,
        session_key: int | str,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Non-raising session-level fetch for Bronze ingestion manifests."""
        return self.fetch_endpoint(endpoint, params={"session_key": session_key})
