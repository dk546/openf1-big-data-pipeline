# Bronze effective post-retry manifest — fix for `failed_but_file_present` after retry

**Date:** 2026-05-19 (UTC)
**Scope:** New helper + notebook integration so the post-retry Bronze reconciliation reports the truth on disk. No Bronze ingestion logic, Silver, Gold, modeling, or endpoint selection was changed.

---

## 1. Problem (observed in the user's full run)

After running the targeted retry against the full 2023–2025 Bronze run:

| Retry summary | Value |
|---|---|
| `retry_attempts` | 154 |
| `newly_successful` | 134 |
| `still_failed` | 20 |

Refreshed Bronze Spark reports looked healthy: 609 Bronze files, 148,184 total rows, zero schema-drift flags, all expected endpoints present.

But the refreshed reconciliation summary showed:

| Status | Count |
|---|---|
| `matched` | 475 |
| `failed_but_file_present` | **134** |
| `optional_missing` | 89 |
| `manifest_failed_no_file` | 20 |
| `stale_files` | 0 |
| `row_mismatches` | 0 |
| `missing_files` | 0 |
| `unknown` | 0 |
| **total_rows_reconciled** | **718** |

The 134 `failed_but_file_present` rows exactly equal the retry's `newly_successful`. Root cause: the post-retry reconciliation cell was reading the **original** `artifacts/manifests/ingestion_manifest.csv`, which still marks those 134 `(endpoint, session_key)` pairs as `failed`. The retry-recovered JSONL files are on Drive, so reconciliation correctly observed `manifest_status=failed AND file_exists=True` and classified them as `failed_but_file_present`. The retry rows live only in `ingestion_retry_manifest.csv`.

This is a reporting/contract bug, not a data bug. The fix is to reconcile against an **effective** manifest that overlays retry rows on the original manifest.

---

## 2. Solution — `ingestion_manifest_effective.csv`

### 2.1 Contract

- **Path:** `artifacts/manifests/ingestion_manifest_effective.csv`
- **Schema:** original `MANIFEST_COLUMNS` plus a new `manifest_source` column tagging each row's provenance as `"original"` or `"retry"`.
- **Merge rules** (implemented in `merge_retry_into_manifest` and persisted by `write_effective_manifest_after_retry`):
  - Successful original rows pass through unchanged (`manifest_source="original"`).
  - For every `(endpoint, session_key)` attempted by retry, the **retry row supersedes the original** — successful retries become `success`, still-failed retries remain `failed`, all tagged `manifest_source="retry"`.
  - Optional endpoint failures the retry intentionally skipped (default: `starting_grid`) pass through with their original `failed` status. Reconciliation continues to accept them as `optional_missing`.
  - Global endpoint rows (`meetings`, `sessions`) pass through unchanged.
  - Duplicate `(endpoint, session_key)` combinations are not produced.
- **Invariance:** the original `ingestion_manifest.csv` is **never** modified. The new CSV is created/overwritten on each notebook re-run.

### 2.2 Files changed

| File | Change |
|------|--------|
| `src/openf1_pipeline/ingestion/ingest.py` | Added `EFFECTIVE_MANIFEST_COLUMNS`, `EFFECTIVE_MANIFEST_FILENAME`, `MANIFEST_SOURCE_ORIGINAL`, `MANIFEST_SOURCE_RETRY`. Extended `merge_retry_into_manifest(...)` to emit a `manifest_source` column (covers empty-manifest, empty-retry, and full-merge cases). Added `write_effective_manifest_after_retry(...)` that reads original + retry CSVs, calls the merge, persists `ingestion_manifest_effective.csv`, and returns `{path, df, counts, manifest_path, retry_manifest_path}`. |
| `scripts/rebuild_colab_notebooks.py` | `build_01()` updated: (1) the initial reconciliation cell now auto-detects an existing retry manifest with newly successful rows and reconciles against the effective manifest in that case (defensive check + clear log line); (2) the post-retry refresh cell now builds the effective manifest first and reconciles against it instead of the original; (3) the coverage-verification cell now reads `ingestion_manifest_effective.csv` as the authoritative post-retry view. |
| `notebooks/01_ingestion_bronze.ipynb` | Regenerated from the rebuild script. 30 cells, 15 code cells, all parse-clean. |
| `README.md` | Bronze evidence table now lists `ingestion_manifest_effective.csv`. The "Targeted retry and manifest-file reconciliation" subsection describes the effective-manifest contract and clarifies that the original manifest is never modified. |
| `reports/report_draft/report_structure.md` | §3.2 Bronze, §4.3 Remediation rules, §4.4 Before/after validation, §6.6 Reproducibility now cite the effective manifest and its audit log. |
| `reports/report_draft/report_artifact_map.md` | New artifact row for `ingestion_manifest_effective.csv` with purpose / generator / source / status / interpretation. Quick-reference table extended. Reconciliation row notes "built against the effective manifest when a retry manifest is present, otherwise against the original manifest". |
| `reports/report_draft/table_figure_register.md` | Tables 16 and 17 sources extended to include `ingestion_manifest_effective.csv`. Table 17 gains a `session_result_effective` column for the authoritative post-retry coverage number. |
| `reports/report_draft/narrative_guardrails.md` | New guardrail H.8 — "Always reconcile against the effective post-retry manifest" — with prescribed wording and an instruction to label reconciliation totals with the manifest they were computed against. |
| `implementation_checklist.md` | §5a updated: implementation items checked for the effective manifest + defensive reconciliation; new run-dependent items for verifying the post-retry reconciliation totals against the effective manifest. |
| `artifacts/pipeline_logs/bronze_effective_manifest_post_retry.md` | This audit log (new). |

### 2.3 Defensive behaviour in `01_ingestion_bronze.ipynb`

Two cells in the notebook now make the right choice automatically:

1. **Initial reconciliation cell** (after Bronze Spark reports, before the optional retry section). If `artifacts/manifests/ingestion_retry_manifest.csv` is present **and** `summarize_retry_manifest(...).newly_successful > 0`, the cell calls `write_effective_manifest_after_retry(...)` and reconciles against the effective manifest. Otherwise it reconciles against the original manifest. Either way it prints the exact manifest path used.

2. **Post-retry refresh cell** (runs only when `RUN_TARGETED_RETRY=True` and the retry attempted rows). Always builds the effective manifest first and reconciles against it. Prints `Reconciliation manifest used: …ingestion_manifest_effective.csv (effective merged manifest)`.

The coverage-verification cell now reads `ingestion_manifest_effective.csv` and reports `session_result_effective` alongside the original-only and retry-only counts.

---

## 3. Exact manifest path used for final reconciliation

After the user reruns notebook 01 from the optional retry section (or even just the initial reconciliation cell — the defensive check will catch it):

```
artifacts/manifests/ingestion_manifest_effective.csv
```

The notebook prints both of:

- `[reconciliation] Using effective merged manifest …`
- `Reconciliation manifest used: …/ingestion_manifest_effective.csv (effective merged manifest)`

---

## 4. Expected reconciliation totals after rerun

With the same Drive state as the user's current run (609 Bronze files, retry recovered 134 sessions, 20 sessions still failed, 89 `starting_grid` optional failures), reconciling against the effective manifest should yield:

| Status | Before fix | After fix (expected) |
|---|---|---|
| `matched` | 475 | **609** (475 + 134) |
| `stale_files` | 0 | 0 |
| `missing_files` | 0 | 0 |
| `row_mismatches` | 0 | 0 |
| `failed_but_file_present` | 134 | **0** |
| `optional_missing` | 89 | 89 |
| `manifest_failed_no_file` | 20 | 20 |
| `unknown` | 0 | 0 |
| **total_rows_reconciled** | 718 | **718** |

This matches the actual on-disk state: 609 Bronze JSONL files (all `matched`), 89 optional `starting_grid` rows with no file, 20 required-endpoint rows that retry could not recover.

The `blocking_after_retry` counter in the notebook (sum of `stale_files + failed_but_file_present + row_mismatches + missing_files`) should drop from **134** to **0**, switching the printed verdict from `WARNING` to `OK: post-retry reconciliation is clean — safe to proceed to Silver.`

The remaining 20 `manifest_failed_no_file` rows are genuine source-availability limitations and should be acknowledged per narrative guardrail H.6. They do not block Silver; they are documented and reproducible.

---

## 5. Can notebook 01 be safely rerun from the retry / reconciliation section only?

**Yes.** The relevant cells are self-contained for re-run:

- The optional-retry execution cell imports `retry_failed_session_endpoints`, `summarize_retry_manifest`, and `delete_stale_bronze_files` directly.
- The post-retry refresh cell imports `write_effective_manifest_after_retry` directly and uses the `retry_df` produced by the cell above it.
- The initial reconciliation cell imports `summarize_retry_manifest` and `write_effective_manifest_after_retry` directly and works whether retry has been run or not.
- The coverage-verification cell reads CSVs from disk; it has no in-memory dependencies beyond `retry_df`.

Practical re-run strategies after a fresh runtime restart:

| Goal | Run these cells |
|------|-----------------|
| Just re-verify reconciliation with the current Drive state | Setup → Spark → Bronze config (`SMOKE_TEST=False`, `RUN_TARGETED_RETRY=False`) → "Run Bronze ingestion" *(no-op because all required files exist and manifest is present)* → Bronze reports → reconciliation cell. The defensive check uses the existing retry manifest automatically. |
| Re-do the retry and refresh | Same as above, then set `RUN_TARGETED_RETRY=True` and run the retry execution cell + post-retry refresh cell + coverage-verification cell. |
| Quickest: skip ingestion, only refresh | Setup → Spark → Bronze config → Bronze reports → reconciliation cell. *(Skip the ingestion cell. The reconciliation cell will detect the retry manifest and reconcile against the effective manifest.)* |

The full Bronze ingestion cell is idempotent at the manifest layer; running it again with the same seasons would attempt to re-fetch everything — that is **not** what is wanted here. The recommended path is to skip the ingestion cell and re-run just the reports + reconciliation cells.

---

## 6. What was not changed

- Bronze ingestion logic (`run_bronze_ingestion`, `ingest_global_endpoint`, `ingest_endpoint_for_sessions`): unchanged.
- Endpoint selection (`ENDPOINTS`, `GLOBAL_ENDPOINTS`, `SESSION_ENDPOINTS`, `OPTIONAL_SESSION_ENDPOINTS`): unchanged.
- Retry endpoint defaults (excludes `starting_grid`, never touches global endpoints): unchanged.
- Reconciliation classification function (`reconcile_manifest_to_bronze_files`, `summarize_bronze_reconciliation`, `generate_bronze_reconciliation_reports`): unchanged — they already accept an arbitrary `manifest_path`. We just now pass the right one.
- DuckDB reconciliation validation: unchanged.
- Silver, Gold, modeling, and reporting notebooks: not touched in this turn.

---

## 7. Verification performed in this turn

- Smoke-tested `merge_retry_into_manifest` and `write_effective_manifest_after_retry` locally with synthetic data covering: original success pass-through, retry recovery, retry-still-failed, optional `starting_grid` pass-through, global `meetings` row pass-through, retry-CSV-absent fallback. All assertions passed.
- Verified `notebooks/01_ingestion_bronze.ipynb` re-generated cleanly: 30 cells, 15 code cells, all parse as valid Python; expected keywords (`write_effective_manifest_after_retry`, `ingestion_manifest_effective.csv`, `summarize_retry_manifest`, `effective merged manifest`) all present.
- No notebooks were executed against live OpenF1 / live Drive.
- No fake outputs were created.
- The on-disk `ingestion_manifest.csv` from the user's run is preserved (the new code only reads it).
