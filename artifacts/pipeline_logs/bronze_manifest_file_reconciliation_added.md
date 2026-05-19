# Bronze manifest ↔ files reconciliation (added)

**Date:** 2026-05-19 (UTC)
**Scope:** Bronze layer (notebook `01_ingestion_bronze.ipynb`) — reporting/validation only.
**Status:** Implemented. No notebooks were executed and no fake outputs were created.

---

## 1. Why this was added

The full 2023–2025 Bronze ingestion completed without exceptions, but the post-run audit (`artifacts/pipeline_logs/full_bronze_output_review.md`) surfaced a real inconsistency:

- Drive was not cleared before notebook 01 ran (`CLEAR_BRONZE_OUTPUTS=False`).
- The 429 storm during ingestion caused some sessions to fail their API calls in the **new** full run.
- However, three JSONL files left over from a previous smoke run were still on Drive **for sessions that the new manifest reported as failed**:
  - `data/bronze/session_result/year=2024/session_key=9472.jsonl`
  - `data/bronze/session_result/year=2024/session_key=9480.jsonl`
  - `data/bronze/pit/year=2024/session_key=9480.jsonl`
- As a result, `bronze_row_counts.csv` (counted from disk) drifted from manifest-reported successful rows:
  - `session_result`: 1,397 manifest-success rows vs **1,437** rows on Drive.
  - `pit`: 1,916 manifest-success rows vs **1,935** rows on Drive.

The existing Bronze evidence reports (`bronze_file_inventory.csv`, `bronze_row_counts.csv`, `bronze_schema_report.csv`, `bronze_schema_drift.csv`) describe **the manifest** and **the files** independently, but never compare them. This new reconciliation report does exactly that and produces an actionable per-row classification so the issue can be detected and resolved before Silver.

The reconciliation utility is purely additive — no existing report, no manifest, no Bronze data is modified or deleted.

---

## 2. What was added

### Functions in `src/openf1_pipeline/bronze/build_bronze.py`

| Function | Purpose |
|---|---|
| `reconcile_manifest_to_bronze_files(manifest_path, bronze_dir, row_counts_df=None)` | Join `ingestion_manifest.csv` and the JSONL inventory on `(endpoint, year, session_key)` and classify every row. Optional `row_counts_df` lets the caller reuse Spark-counted row counts so we don't re-read JSONL twice. |
| `summarize_bronze_reconciliation(df)` | Returns `{by_status, by_endpoint, by_issue_type, totals}` with totals for matched / stale / missing / row-mismatch / failed-but-file-present / optional-missing counts. |
| `generate_bronze_reconciliation_reports(manifest_path, bronze_dir, data_quality_reports_dir, row_counts_df=None)` | Calls the two above and writes the two evidence CSVs. Returns `{paths, summary, df}`. |
| Constants `RECONCILIATION_COLUMNS`, `RECONCILIATION_OPTIONAL_ENDPOINTS` | Public schema + optional-endpoint set, kept in sync with `ingest.OPTIONAL_SESSION_ENDPOINTS`. |

### DuckDB validation extension

`src/openf1_pipeline/analytics/duckdb_validation.py::validate_bronze_with_duckdb` now also reads `bronze_manifest_file_reconciliation.csv` when present and emits:

- `duckdb_bronze_manifest_file_reconciliation_summary.csv` — counts by `reconciliation_status`.
- `duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv` — counts by `endpoint × reconciliation_status`.
- `duckdb_bronze_manifest_file_reconciliation_issues.csv` — full rows for any status other than `matched` and `optional_missing` (for fast triage).

These names follow the existing `save_duckdb_validation_reports(..., prefix="bronze")` convention.

### Notebook 01 (`notebooks/01_ingestion_bronze.ipynb`)

A new section **"Manifest ↔ Bronze files reconciliation"** is inserted between the Bronze evidence reports cell and the DuckDB validation cell. It:

- Calls `generate_bronze_reconciliation_reports(...)`, passing the Spark-counted `bronze_row_counts.csv` so reconciliation uses the same counts as the Bronze report.
- Displays:
  - `reconciliation_totals` (named summary dict).
  - A `groupby("reconciliation_status")` table.
  - Non-matched rows (head 40).
  - Stale files, failed-but-file-present rows, row-count mismatches, and manifest-success-but-missing rows, each as their own table.
- Prints a warning:
  > WARNING: Bronze reconciliation found inconsistencies. If stale files exist, do not proceed to Silver until choosing a cleanup or retry policy. Use the targeted retry section below for 429-related failures, or surgically delete stale Bronze JSONL files before Silver.

If everything reconciles, the cell prints `OK: Bronze reconciliation is clean — safe to proceed to Silver.`

The targeted-retry refresh cell at the bottom of the notebook (introduced in `full_bronze_retry_plan.md`) now also re-runs reconciliation between `generate_bronze_reports(...)` and `validate_bronze_with_duckdb(...)` so the on-disk Bronze evidence stays consistent after retry.

Notebook regenerator (`scripts/rebuild_colab_notebooks.py::build_01()`) was updated to emit the new cells; the notebook itself was regenerated from the script. Cell count went from 28 → 30.

### Documentation

- `README.md`: added the two new artifacts to the Bronze artifact table and a short note explaining the reconciliation contract.
- This audit log: `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md`.

---

## 3. Issue found in the full run, expressed via this report

When notebook 01 is rerun against the current Drive state with this reconciliation in place, the following are expected (based on the full Bronze audit numbers):

| reconciliation_status | Expected count (approx.) | Reason |
|---|---:|---|
| `matched` | Most session-level `(endpoint, session_key)` pairs | Healthy core Bronze data. |
| `failed_manifest_file_exists` | 3 | The three known stale smoke files (`session_result` 9472, `session_result` 9480, `pit` 9480) — manifest marks these sessions as failed, but pre-existing smoke JSONL files survived. |
| `stale_file_not_in_success_manifest` | 0 (expected) | Would surface any orphaned JSONL with no manifest row at all. |
| `manifest_success_missing_file` | 0 (expected) | A manifest success without an on-disk file would be a bug; this surfaces it loudly. |
| `row_count_mismatch` | 0 (after retry) | Today the stale smoke files inflate `session_result`/`pit` row counts by a known delta; reconciliation reports the delta per row so it is visible. |
| `optional_missing` | 89 | `starting_grid` failed for every session and no file exists — recorded as expected/optional. |
| `manifest_failed_no_file` | The 19 failed `session_result` sessions + any other required-endpoint failures | Required session endpoints that failed and have no JSONL on Drive. These are the natural retry targets. |

After the targeted retry (`RUN_TARGETED_RETRY=True` with default settings), `manifest_failed_no_file` for required endpoints should drop close to zero. The three known stale files will either be overwritten (manifest entry flips to `success`, reconciliation flips to `matched`) or remain (`failed_manifest_file_exists`) — at which point the user can decide between accepting the holdover or running the opt-in deletion of the three known stale paths.

---

## 4. Status categories — interpretation table

| reconciliation_status | issue_type | Meaning | Suggested action |
|---|---|---|---|
| `matched` | `none` | Manifest says success, JSONL exists, row counts agree. | Nothing to do. |
| `row_count_mismatch` | `row_mismatch` | Manifest success and file exist but row counts differ. `row_count_delta = file − manifest`. | Investigate the specific JSONL; usually a stale file or an interrupted write. |
| `manifest_success_missing_file` | `missing_file` | Manifest claims success but the JSONL is not on disk. | Re-ingest that specific `(endpoint, session_key)` — likely Drive sync or storage issue. |
| `failed_manifest_file_exists` | `failed_but_file_present` | Manifest says failed but JSONL is present. Classic stale-file pattern. | Retry the request; on success the file is overwritten. If retry still fails and the file came from a prior good run, decide whether to keep or delete. |
| `stale_file_not_in_success_manifest` | `stale_file` | File exists with no manifest row at all. Orphaned data. | Either run a targeted retry to add a manifest row or delete the file. |
| `optional_missing` | `optional_endpoint` | Manifest failed, no file, and endpoint is optional (`starting_grid`). | Accept. Not a blocker. |
| `manifest_failed_no_file` | `manifest_only` | Required endpoint failed and no file. | Use the targeted retry workflow (`retry_failed_session_endpoints`). |
| `unknown` | `none` | Edge case not matching any rule. | Investigate; should be zero in practice. |

Per-row notes are also written into the `notes` column (e.g. `manifest=1397 file=1437 delta=40`).

---

## 5. How this supports reproducibility and data quality

- **Reproducibility:** the manifest is the canonical Bronze provenance record; reconciliation makes the manifest's claims testable against the actual on-disk state. Future Bronze runs (smoke or full) will produce the same evidence CSVs in the same locations, including the new reconciliation pair.
- **Stop-the-line before Silver:** the notebook prints a clear warning when any blocking status is non-zero, so Silver cannot be started accidentally on an inconsistent Bronze layer.
- **Targeted recovery loop:** the reconciliation report identifies the exact `(endpoint, year, session_key)` triples that need attention. The retry utility consumes the manifest and re-fetches only those triples; the refresh cell re-runs reconciliation so the user can confirm the loop is closed.
- **Independent validation:** DuckDB now reads the same reconciliation CSV and emits its own per-status / per-endpoint summaries, providing a second independent view of the same data alongside the Spark-produced reports.
- **No data destruction:** the utility never deletes JSONL files, never overwrites the original manifest, and never mutates the row-count or inventory CSVs in place. All side effects are new CSVs.

---

## 6. Operational checklist

Before Silver:

1. Confirm `reports/data_quality/bronze_manifest_file_reconciliation.csv` exists and is recent (modified time ≥ the latest Bronze report run).
2. Open `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv` (or the DuckDB CSVs) and verify the totals block:
   - `matched` is large.
   - `stale_files = 0`, `missing_files = 0`, `row_mismatches = 0`.
   - `failed_but_file_present` is either 0 or matches exactly the documented stale files.
   - `optional_missing` only contains `starting_grid` rows.
   - `manifest_failed_no_file = 0` for required endpoints (after retry).
3. If any of the above is not satisfied, run the targeted retry workflow with `RUN_TARGETED_RETRY=True`, then re-check the reconciliation summary.
4. Only proceed to `02_silver_cleaning_quality.ipynb` once the notebook prints `OK: Bronze reconciliation is clean — safe to proceed to Silver.`

---

## 7. Out of scope

- Reconciliation does **not** modify Bronze JSONL data, the original manifest, or the retry manifest.
- It does **not** trigger any API calls. Re-ingestion goes through `retry_failed_session_endpoints` separately.
- Silver / Gold / Modeling / Reporting are unchanged.
