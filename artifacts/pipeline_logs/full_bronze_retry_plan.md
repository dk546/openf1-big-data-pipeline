# Full Bronze targeted retry plan

**Date:** 2026-05-19 (UTC)
**Scope:** OpenF1 2023–2025 full Bronze ingestion — recover failed session-level endpoint rows without re-running full ingestion.
**Pipeline phase:** Bronze (notebook `01_ingestion_bronze.ipynb`).

---

## 1. Why this utility exists

The first full Bronze run for 2023–2025 produced an `ingestion_manifest.csv` with a meaningful number of `failed` rows for **session-level** endpoints, almost all due to HTTP 429 (rate limit) bursts from the OpenF1 API. Observed pattern from the prior audit:

- 89 race sessions covered across 2023–2025.
- `session_result` succeeded for **70/89** sessions → **19 sessions missing** for the Gold modeling target.
- `starting_grid` failed for all 89 sessions (mostly HTTP 404). This is an **optional** endpoint.
- Various other failures spread across `pit`, `laps`, `position`, `race_control`, `weather`, `drivers` — clustered in time, consistent with rate-limit storms rather than data issues.

Re-running the full Bronze ingestion would be wasteful (hours of API calls, additional rate-limit risk, possible Drive churn) and would also wipe the successful Bronze JSONL files we already have. Instead, this plan adds a **targeted retry** that:

- Only retries the specific `(endpoint, session_key)` pairs that previously failed.
- Throttles the OpenF1 client more aggressively (default 3 s base sleep) so it does not re-trigger 429s.
- Leaves the original manifest and successful Bronze files untouched.
- Writes a separate retry manifest and refreshes Bronze evidence reports.

---

## 2. Design constraints (locked)

1. **Do not rerun full Bronze ingestion.**
2. **Do not delete successful Bronze JSONL files.**
3. **Do not modify the original `ingestion_manifest.csv`.** Add a separate `ingestion_retry_manifest.csv` instead.
4. **Do not retry global endpoints** (`meetings`, `sessions`) — they are season-scoped and not session-scoped.
5. **Do not retry `starting_grid` by default.** It is optional and consistently 404 for this dataset.
6. **Default to required session endpoints only:** `drivers`, `laps`, `pit`, `weather`, `position`, `race_control`, `session_result`.
7. **Throttle requests.** Default `sleep_seconds=3.0` between requests, with the per-request retry budget in `OpenF1Client` still applying inside each call.
8. **Idempotent file writes.** A retry overwrites only the specific JSONL it targets; nothing else is touched.
9. **Notebook safety.** The retry **does not run** unless `RUN_TARGETED_RETRY=True` is set explicitly in the notebook.
10. **Stale-file cleanup is opt-in.** The three known stale smoke-run files are deleted only when the user explicitly sets `DELETE_STALE_SMOKE_FILES=True`.

---

## 3. Files changed

| File | Change |
|------|--------|
| `src/openf1_pipeline/ingestion/ingest.py` | Added `retry_failed_session_endpoints(...)`, `merge_retry_into_manifest(...)`, `summarize_retry_manifest(...)`, `delete_stale_bronze_files(...)`, the `RETRY_MANIFEST_COLUMNS` schema, and helpers (`_resolve_retry_endpoint_set`, `_coerce_int_or_none`). |
| `notebooks/01_ingestion_bronze.ipynb` | Appended four cells: (1) markdown intro, (2) retry config (`RUN_TARGETED_RETRY=False`, `RETRY_SLEEP_SECONDS=3.0`, stale-file list), (3) retry execution + optional stale-file deletion, (4) Bronze report + DuckDB refresh, (5) session_result coverage verification. |
| `scripts/rebuild_colab_notebooks.py` | Updated `build_01()` to emit the new retry section. Also fixed an unrelated triple-quote bug in `build_05()` so the notebook regenerator no longer reintroduces the `notebooks/05_report_artifacts.ipynb` `SyntaxError`. |
| `README.md` | Added a short subsection under Bronze layer evidence describing the optional targeted retry. |
| `artifacts/pipeline_logs/full_bronze_retry_plan.md` | This plan document. |

No other modules, notebooks, or pipeline layers are touched.

---

## 4. New programmatic API

`openf1_pipeline.ingestion.ingest`:

```python
retry_failed_session_endpoints(
    manifest_path: Path | None = None,         # defaults to artifacts/manifests/ingestion_manifest.csv
    bronze_dir: Path | None = None,            # defaults to get_bronze_dir()
    manifests_dir: Path | None = None,         # defaults to get_manifests_dir()
    endpoints_to_retry: list[str] | None = None,  # default: required session endpoints
    include_optional: bool = False,            # set True to also retry starting_grid
    sleep_seconds: float = 3.0,                # base inter-request sleep
    max_retries_per_request: int = 5,          # per-request OpenF1Client retry budget
    timeout: int = 60,
    retry_manifest_filename: str = "ingestion_retry_manifest.csv",
) -> pd.DataFrame
```

Returns a retry-only manifest. Always writes `artifacts/manifests/<retry_manifest_filename>` (empty when nothing is eligible).

Supporting helpers:

- `summarize_retry_manifest(retry_df)` → `{retry_attempts, newly_successful, still_failed, by_endpoint, newly_successful_session_result}`.
- `merge_retry_into_manifest(manifest_df, retry_df)` → returns a merged DataFrame; **does not write to disk**. Caller decides whether to persist a combined manifest.
- `delete_stale_bronze_files(bronze_dir=None, paths=[...])` → deletes specific JSONL files under the Bronze root and returns the list of paths actually deleted. Used by the notebook for the three known stale smoke files.

The retry-only manifest extends `MANIFEST_COLUMNS` with:

- `previous_status` — status from the original manifest row.
- `previous_error_message` — original error message text.
- `retry_attempt` — currently always `1`; reserved for future multi-attempt orchestration.

---

## 5. Retry endpoint policy

| Endpoint | Default retry? | Notes |
|----------|----------------|-------|
| `session_result` | **Yes (required)** | Drives Gold `points_finish` target. Highest priority. |
| `drivers` | Yes (required) | Used for joins; small payload. |
| `laps` | Yes (required) | Large payload; throttled retry is important. |
| `pit` | Yes (required) | Used in Tier-2 features. |
| `weather` | Yes (required) | Used in environment features. |
| `position` | Yes (required) | Used in Tier-1 / leakage analysis. |
| `race_control` | Yes (required) | Used in Tier-2 context features. |
| `starting_grid` | **No (optional)** | 100% 404 in current dataset. Only retried when `include_optional=True`. |
| `meetings`, `sessions` | **No (global)** | Out of scope for this utility. |

Global endpoints (`meetings`, `sessions`) are silently ignored even if listed in `endpoints_to_retry`, because the resolver intersects against `SESSION_ENDPOINTS`.

---

## 6. Throttling strategy

`OpenF1Client` (existing) already implements per-request retries and HTTP-429-aware backoff. The retry utility composes those defenses with **caller-level inter-request sleeps**:

- The client is instantiated with `sleep_seconds=3.0` and `max_retries=5` (configurable).
- Before each request, `OpenF1Client._request_with_retries` sleeps `sleep_seconds * attempt`.
- After every retry-call (success or failure), the utility itself sleeps an additional `sleep_seconds`.
- This yields an effective minimum gap of roughly 3 seconds between successive sessions, with exponential backoff if any 429 still occurs inside a single request.

For 89 sessions × ~7 endpoints, the worst-case retry budget is bounded by the number of `failed` manifest rows, which is much smaller than the original ingestion volume. The 2023–2025 retry should comfortably fit inside a single Colab Pro Plus runtime.

---

## 7. Output contract

After a successful retry:

| Path | Behaviour |
|------|-----------|
| `data/bronze/{endpoint}/year={year}/session_key={session_key}.jsonl` | Overwritten for each successfully retried `(endpoint, session_key)` only. |
| `artifacts/manifests/ingestion_manifest.csv` | **Unchanged.** |
| `artifacts/manifests/ingestion_retry_manifest.csv` | New file (or overwritten). Contains exactly one row per retry attempt with full result detail. |
| `reports/data_quality/bronze_file_inventory.csv` | Regenerated by the notebook after retry. |
| `reports/data_quality/bronze_row_counts.csv` | Regenerated. |
| `reports/data_quality/bronze_schema_report.csv` | Regenerated. |
| `reports/data_quality/bronze_schema_drift.csv` | Regenerated. |
| `artifacts/schemas/bronze_schema_report.csv` | Regenerated (snapshot copy written by `generate_bronze_reports`). |
| `reports/data_quality/duckdb_bronze_*.csv` | Regenerated via `validate_bronze_with_duckdb` + `save_duckdb_validation_reports`. |

The retry manifest is always written, even when there is nothing to retry (in which case it is an empty CSV with the full column header — useful for downstream tooling and audits).

---

## 8. Stale smoke-file handling

Three Bronze files from a prior smoke run survived the full ingestion because the user did not clear Drive beforehand:

```
data/bronze/session_result/year=2024/session_key=9472.jsonl
data/bronze/session_result/year=2024/session_key=9480.jsonl
data/bronze/pit/year=2024/session_key=9480.jsonl
```

These sessions also failed in the full run, so the manifest currently shows them as `failed` while the JSONL files (from smoke) still exist on Drive. Two safe options:

1. **Default (`DELETE_STALE_SMOKE_FILES=False`):** leave the files in place. The retry will overwrite them if the targeted retry of those `(endpoint, session_key)` pairs succeeds. If retry also fails, the smoke data remains — the manifest will note failure but Bronze reports will continue to count rows for those files.
2. **Clean-manifest mode (`DELETE_STALE_SMOKE_FILES=True`):** delete the three files before retry. Used when the user wants strict manifest-to-disk consistency, accepting the possibility that retry leaves those sessions with **no** Bronze data.

The notebook prints the deleted paths and never deletes anything beyond the explicit list.

---

## 9. How to run in Colab

1. Open `notebooks/01_ingestion_bronze.ipynb` in Colab Pro Plus. Run the standard setup cell.
2. Skip the main ingestion section (already executed for 2023–2025). Scroll to **"Optional: targeted retry for failed session endpoints"** near the end.
3. Edit the config cell:
   - Set `RUN_TARGETED_RETRY = True`.
   - Optionally adjust `RETRY_SLEEP_SECONDS` (default `3.0`), `RETRY_ENDPOINTS`, `RETRY_INCLUDE_OPTIONAL`, `DELETE_STALE_SMOKE_FILES`.
4. Run the next three cells in order: retry execution → Bronze report refresh → coverage verification.
5. The retry manifest is written to `artifacts/manifests/ingestion_retry_manifest.csv`. Bronze reports are refreshed in place.

The retry section is a no-op when `RUN_TARGETED_RETRY=False`, so it is safe to re-run the whole notebook for documentation purposes without re-fetching anything.

---

## 10. How to verify session_result coverage after retry

The notebook's final cell prints these counts when retry has run:

- `session_result` manifest success (original run).
- `session_result` manifest success (retry only).
- `session_result` JSONL files on Drive (after retry-driven refresh of `bronze_file_inventory.csv`).
- Effective coverage = original successes + retry successes.

Manual cross-check (in the notebook or locally):

```python
import pandas as pd
orig = pd.read_csv("artifacts/manifests/ingestion_manifest.csv")
retry = pd.read_csv("artifacts/manifests/ingestion_retry_manifest.csv")

print("session_result success (original):",
      ((orig["endpoint"]=="session_result") & (orig["status"]=="success")).sum())
print("session_result success (retry):",
      ((retry["endpoint"]=="session_result") & (retry["status"]=="success")).sum())
print("session_result failed (retry):",
      ((retry["endpoint"]=="session_result") & (retry["status"]=="failed")).sum())

inv = pd.read_csv("reports/data_quality/bronze_file_inventory.csv")
print("session_result files on Drive:", (inv["endpoint"]=="session_result").sum())
```

Target: after retry, `session_result` coverage should approach 89/89 race sessions. Any session still missing after retry should be examined individually before proceeding to Silver.

---

## 11. Acceptance criteria

- [x] `retry_failed_session_endpoints` exists in `src/openf1_pipeline/ingestion/ingest.py` with the documented signature.
- [x] Notebook 01 contains a `RUN_TARGETED_RETRY` flag (default `False`) and `RETRY_SLEEP_SECONDS` (default `3.0`).
- [x] Original `ingestion_manifest.csv` is never modified by this utility.
- [x] Retry results are written to `artifacts/manifests/ingestion_retry_manifest.csv`.
- [x] `starting_grid` is excluded by default.
- [x] Global endpoints (`meetings`, `sessions`) cannot be retried by this function.
- [x] Bronze Spark reports and DuckDB validation are regenerated after a non-empty retry.
- [x] The three known stale smoke files are only deleted when the user explicitly opts in.
- [x] No notebooks were executed and no fake outputs were created while implementing the plan.

---

## 12. Out of scope

- Persisting a merged manifest CSV. `merge_retry_into_manifest` is available for downstream code, but the canonical files remain the original `ingestion_manifest.csv` and the new `ingestion_retry_manifest.csv`.
- Multi-attempt orchestration (e.g. re-retrying failures of the retry). The current utility runs a single pass; rerunning the notebook cell with `RUN_TARGETED_RETRY=True` is a manual second attempt.
- Backfilling the `starting_grid` 404s. Not feasible from the current OpenF1 endpoint.
- Any change to Silver / Gold / Modeling / Reporting layers.
