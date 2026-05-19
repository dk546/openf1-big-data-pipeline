# Full Bronze Output Review — 2023–2025 Colab Run

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T18:30:00Z |
| **Run profile** | `SMOKE_TEST=False`, `INGEST_SEASONS=[2023,2024,2025]`, `MAX_SESSIONS=None`, `BRONZE_REPORT_ENGINE="spark"`, `ALLOW_FALLBACK=False`, `CLEAR_BRONZE_OUTPUTS=False`, `USE_GOOGLE_DRIVE=True` |
| **Drive output root** | `/content/drive/MyDrive/openf1_big_data_pipeline` |
| **Bronze run window (UTC)** | `2026-05-19T17:41:32Z` → `2026-05-19T17:59:09Z` (≈18 min) |
| **Notebook outputs source** | `notebooks/00_colab_setup.ipynb`, `notebooks/01_ingestion_bronze.ipynb` (cell outputs persisted) |
| **Drive artifact snapshot** | `evidence/full_2023_2025/` (locally pulled) |
| **Notebooks executed during this audit** | **None** |
| **Synthetic outputs created** | **None** |

---

## Files reviewed

### Drive snapshot pulled to repo
- `evidence/full_2023_2025/artifacts/manifests/ingestion_manifest.csv`
- `evidence/full_2023_2025/artifacts/schemas/bronze_schema_report.csv`
- `evidence/full_2023_2025/reports/data_quality/bronze_file_inventory.csv`
- `evidence/full_2023_2025/reports/data_quality/bronze_row_counts.csv`
- `evidence/full_2023_2025/reports/data_quality/bronze_schema_report.csv`
- `evidence/full_2023_2025/reports/data_quality/bronze_schema_drift.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_bronze_bronze_rows_by_endpoint.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_bronze_bronze_total_rows.csv`
- `evidence/full_2023_2025/reports/data_quality/duckdb_bronze_bronze_schema_drift_flags.csv`

### Inline notebook cell outputs reviewed
- `notebooks/00_colab_setup.ipynb` — cells 3 and 5
- `notebooks/01_ingestion_bronze.ipynb` — cells 2, 4, 6, 7, 9, 11, 13, 15, 17, 19

### Pipeline code re-confirmed
- `src/openf1_pipeline/config.py`
- `src/openf1_pipeline/ingestion/ingest.py`
- `src/openf1_pipeline/bronze/build_bronze.py`
- `src/openf1_pipeline/analytics/duckdb_validation.py`

### Reference
- `artifacts/pipeline_logs/full_run_readiness_audit.md`
- `README.md`

> **Note (workspace-root staleness):** The repo root contains an older smoke `artifacts/manifests/ingestion_manifest.csv` (18 rows, timestamps `2026-05-19T09:39:46Z`, 8-row `reports/data_quality/bronze_row_counts.csv`). These are leftover **local-only** smoke artifacts; they are **not** the Colab full-run outputs. Colab full-run outputs are on Drive and snapshotted to `evidence/full_2023_2025/`. Disregard the root copies for full-run review.

---

## 1. Notebook 00 setup — PASS

| Check | Cell evidence | Result |
|-------|---------------|--------|
| Drive mounted | `Mounted at /content/drive` | PASS |
| `OPENF1_DATA_ROOT` set | `OPENF1_DATA_ROOT: /content/drive/MyDrive/openf1_big_data_pipeline` | PASS |
| GitHub clone | `Cloning repository...` (fresh clone, not pull) | PASS |
| `pip install -r requirements.txt` | Implicit success (no exception thrown; subsequent imports work) | PASS |
| `pip install -e .` | Implicit success (`openf1_pipeline` imports below succeed) | PASS |
| `import openf1_pipeline` | `SEASONS: [2023, 2024, 2025]`, `ENDPOINTS:[10]`, `OpenF1Client: https://api.openf1.org/v1` printed | PASS |
| `PROJECT_ROOT` | `/content/openf1-big-data-pipeline` | Correct |
| `OUTPUT_ROOT` | `/content/drive/MyDrive/openf1_big_data_pipeline` | Correct |
| All Bronze/Silver/Gold/Reports/Artifacts dirs | Resolved on Drive | PASS |
| PySpark | Spark **4.0.2** logged in notebook 01 cell 4 (`Spark session started: openf1-big-data-pipeline`) | PASS |

**Notebook 00 verdict:** Setup correct. Colab is on the latest pushed GitHub code at run time. Drive paths correct.

---

## 2. Notebook 01 configuration — PASS (full mode confirmed)

| Required value | Notebook output | Result |
|----------------|-----------------|--------|
| `SMOKE_TEST=False` | `SMOKE_TEST=False, seasons=[2023, 2024, 2025], max_sessions=None` | PASS |
| `INGEST_SEASONS=[2023,2024,2025]` | same as above | PASS |
| `MAX_SESSIONS=None` | same as above | PASS |
| `BRONZE_REPORT_ENGINE="spark"` | Cell 15 log: `Bronze Spark reports generated: {'engine': 'spark', ...}` | PASS |
| `ALLOW_FALLBACK=False` | `CLEAR_BRONZE_OUTPUTS=False, ALLOW_FALLBACK=False` | PASS |
| `CLEAR_BRONZE_OUTPUTS=False` | Cell 9: `Skipping Bronze cleanup (CLEAR_BRONZE_OUTPUTS=False).` | PASS (but see §7 — this is the root cause of stale Drive files) |
| `session_result` in SESSION_ENDPOINTS | Cell 6: `SESSION_ENDPOINTS: ['drivers', 'laps', 'pit', 'weather', 'position', 'race_control', 'session_result', 'starting_grid']` | PASS |
| `starting_grid` optional | Cell 6: `starting_grid: OPTIONAL — failures are logged and ingestion continues` | PASS |

**Race-session filter applied (cell 11 ingest log):**
- 2023: **29 race sessions** of 118 total
- 2024: **30 race sessions** of 123 total
- 2025: **30 race sessions** of 123 total
- **Total: 89 race sessions** × 8 per-session endpoints = 712 calls + 6 global = 718 manifest rows. Confirmed.

**Notebook 01 verdict:** Ran in full mode, all three seasons, Spark-first reporting.

---

## 3. Manifest review — Healthy with ingestion-time 429 losses

### Totals (from `evidence/full_2023_2025/artifacts/manifests/ingestion_manifest.csv`)

| Metric | Value |
|--------|-------|
| Total manifest rows | **718** |
| Success | **475** |
| Failed | **243** |
| Run window | 18 minutes (single contiguous window) |

### Success vs failure by endpoint

| endpoint | success | failed | comment |
|----------|---------|--------|---------|
| meetings | 3 | 0 | all 3 seasons |
| sessions | 3 | 0 | all 3 seasons |
| drivers | 76 | 13 | 12× HTTP 429, 1× HTTP 404 |
| laps | 64 | 25 | 25× HTTP 429 |
| pit | 58 | 31 | 18× HTTP 429, 13× HTTP 404 |
| position | 67 | 22 | 21× HTTP 429, 1× HTTP 404 |
| race_control | 64 | 25 | 24× HTTP 429, 1× HTTP 404 |
| **session_result** | **70** | **19** | **18× HTTP 429, 1× HTTP 404 (session 9086, 2023)** |
| starting_grid | 0 | 89 | 71× HTTP 404, 18× HTTP 429 — **optional** |
| weather | 70 | 19 | 18× HTTP 429, 1× HTTP 404 |

### Failed `session_result` sessions (target-impacting)

| year | session_keys failed | cause |
|------|---------------------|-------|
| 2023 | 9086, 9212, 9213, 9220, 9221 | 9086 = 404; rest = 429 |
| 2024 | 9472, 9480, 9488, 9496, 9506 | all 429 |
| 2025 | 9864, 9869, 9877, 9883, 9888, 9896, 9904, 9912, 9920 | all 429 |

Race-session coverage **with target after this run**:

| year | race sessions filtered | session_result success | coverage |
|------|------------------------|------------------------|----------|
| 2023 | 29 | 24 | 82.8% |
| 2024 | 30 | 25 | 83.3% |
| 2025 | 30 | 21 | 70.0% |
| **Total** | **89** | **70** | **78.7%** |

### Per-session failure clusters
- Sessions 9070, 9078, 9086, 9204, 9212, 9616, 9472, 9480 had multiple endpoint failures (clustered 429 storms during the ingest)
- Session 9086 (2023) is the only HTTP 404 cluster — likely a non-race session (sprint or shootout) that slipped through the race filter and has no result data

### Manifest verdict
- Run was full mode (all 3 seasons, all race sessions filtered).
- `session_result` reachable for **70/89 race sessions (78.7%)** — usable but lossy.
- All non-`starting_grid` failures are dominated by HTTP 429 rate limits — recoverable on retry.
- `starting_grid` 100% failure is **expected and optional** per design.

---

## 4. Bronze row count review

### From `bronze_row_counts.csv` (478 file rows aggregated)

| endpoint | rows on Drive | files on Drive | rows per year (2023 / 2024 / 2025) |
|----------|---------------|----------------|------------------------------------|
| laps | **60,357** | 64 | 17,603 / 20,966 / 21,788 |
| position | **29,023** | 67 | 8,652 / 8,687 / 11,684 |
| weather | **10,142** | 70 | 2,955 / 3,996 / 3,191 |
| race_control | **5,088** | 64 | 1,807 / 1,581 / 1,700 |
| pit | **1,935** | 59 | 717 / 576 / 642 |
| drivers | **1,516** | 76 | 498 / 499 / 519 |
| **session_result** | **1,437** | **72** | 478 / 540 / 419 |
| sessions | 364 | 3 | 118 / 123 / 123 |
| meetings | 74 | 3 | 24 / 25 / 25 |
| **TOTAL** | **109,936** | **478** | 32,852 / 36,934 / 40,091 |

### Row counts vs manifest (CRITICAL CROSS-CHECK)

| endpoint | manifest success sum | bronze_row_counts sum | Δ | Δ files |
|----------|---------------------|----------------------|---|---------|
| meetings | 74 | 74 | 0 | 0 |
| sessions | 364 | 364 | 0 | 0 |
| drivers | 1,516 | 1,516 | 0 | 0 |
| laps | 60,357 | 60,357 | 0 | 0 |
| **pit** | **1,916** | **1,935** | **+19** | **+1 stale file** |
| position | 29,023 | 29,023 | 0 | 0 |
| race_control | 5,088 | 5,088 | 0 | 0 |
| **session_result** | **1,397** | **1,437** | **+40** | **+2 stale files** |
| weather | 10,142 | 10,142 | 0 | 0 |

**Stale files identified (confirmed by set-diff of inventory keys minus manifest success keys):**

| stale Drive file | rows | origin |
|------------------|------|--------|
| `data/bronze/session_result/year=2024/session_key=9472.jsonl` | 20 | smoke `evidence/smoke_2024_maxsessions2_spark/` run, not deleted because `CLEAR_BRONZE_OUTPUTS=False` |
| `data/bronze/session_result/year=2024/session_key=9480.jsonl` | 20 | same — smoke holdover |
| `data/bronze/pit/year=2024/session_key=9480.jsonl` | 19 | same — smoke holdover |

These three sessions (9472, 9480) **also failed in this run** (HTTP 429), so the manifest shows them as failed but the JSONL files from the prior smoke remain. Their content is **real OpenF1 data for 2024 races** — not corrupt — but it was ingested in a previous run.

### Required endpoints — none unexpectedly empty
- `session_result`: 1,437 rows on Drive (1,397 from this run + 40 stale smoke), 72 files — **target usable**
- `laps`: 60,357 rows / 64 files — **substantial**
- `position`: 29,023 rows / 67 files — **substantial**
- `weather`: 10,142 rows / 70 files — full coverage
- `race_control`: 5,088 rows / 64 files
- `pit`: 1,935 rows / 59 files
- `drivers`: 1,516 rows / 76 files
- `meetings`: 74 / 3 — all seasons
- `sessions`: 364 / 3 — all seasons
- `starting_grid`: 0 rows / 0 files — **optional, correctly absent**

### Row count verdict
- Spark and DuckDB agree exactly on every endpoint total.
- **Three stale smoke JSONL files inflate `session_result` by 40 rows and `pit` by 19 rows** vs the manifest. Real data, but inconsistent with the manifest's success/failure record.

---

## 5. Schema report review

### From `bronze_schema_report.csv` (106 column rows, 9 endpoints)

| endpoint | column count | observed type mix |
|----------|--------------|-------------------|
| drivers | 12 | string / bigint |
| laps | 16 | mix incl. `array<bigint>` for segment fields |
| meetings | 18 | string / bigint / double |
| pit | 8 | bigint / double |
| position | 5 | string / bigint |
| race_control | 11 | string / bigint |
| session_result | 11 | bigint / double / boolean |
| sessions | 15 | string / bigint |
| weather | 10 | string / bigint / double / boolean |
| **starting_grid** | **0** | **not in schema (no files exist — expected for 100% optional failure)** |

### Observed dtype distribution (Spark report)
- `string`: 42 columns
- `bigint`: 41 columns
- `double`: 14 columns
- `boolean`: 6 columns
- `array<bigint>`: 3 columns (lap segment fields)

### Schema drift

| Source | Result |
|--------|--------|
| `bronze_schema_drift.csv` (Spark) | 106 rows, **all `possible_schema_drift_flag=False`** |
| `duckdb_bronze_bronze_schema_drift_flags.csv` (DuckDB) | **0 rows (empty)** — confirms no flagged drift |

### Schema verdict
- All required endpoints present (`session_result` included).
- `starting_grid` absent from schema by design (no files; 404 fallback handled in Silver via `SILVER_EMPTY_SCHEMAS`).
- Zero schema drift across all 9 ingested endpoints.
- `array<bigint>` lap segment columns previously caused the duplicate-report bug — already fixed in `quality/profiling.py` (`make_dataframe_hashable_for_duplicates`).

---

## 6. DuckDB Bronze validation review

### Files present in evidence snapshot

| DuckDB report | Rows | Status |
|---------------|------|--------|
| `duckdb_bronze_bronze_rows_by_endpoint.csv` | 9 | OK — matches Spark exactly |
| `duckdb_bronze_bronze_total_rows.csv` | 1 | OK — `total_rows=109,936`, `file_count=478` |
| `duckdb_bronze_bronze_schema_drift_flags.csv` | 0 | OK — header-only (no drift) |
| `duckdb_bronze_bronze_endpoint_coverage.csv` | **Not in evidence snapshot, but was written on Drive** (per notebook 01 cell 17 log: `9 rows`) — local sync gap, not a pipeline gap |

### Cross-validation: DuckDB vs Spark

| Metric | Spark | DuckDB | Match |
|--------|-------|--------|-------|
| Total Bronze rows | 109,936 | 109,936 | YES |
| File count | 478 | 478 | YES |
| Rows by endpoint (all 9) | identical | identical | YES |
| Schema drift flags | 0 | 0 (empty) | YES |

### DuckDB verdict
- DuckDB independently confirms Spark row totals and endpoint coverage.
- **Both reports include the 59 stale smoke rows** because both read directly from Drive `data/bronze/*` — they cannot distinguish a stale JSONL from a fresh one. This is not a pipeline bug; it is an artifact of running without `CLEAR_BRONZE_OUTPUTS=True`.
- The 4th DuckDB CSV (`bronze_endpoint_coverage`) is on Drive but missing from the local `evidence/full_2023_2025/` snapshot — copy it before submitting evidence.

---

## 7. Staleness / mixed-output risk

### Verdict: **CONTAMINATION CONFIRMED — but limited and contained**

| Stale file | Rows | Year | Cause |
|------------|------|------|-------|
| `data/bronze/session_result/year=2024/session_key=9472.jsonl` | 20 | 2024 | Prior smoke ingest (`evidence/smoke_2024_maxsessions2_spark/`) was not cleared from Drive before the full run; full run also failed (429) for this session, so the smoke file persists |
| `data/bronze/session_result/year=2024/session_key=9480.jsonl` | 20 | 2024 | Same as above |
| `data/bronze/pit/year=2024/session_key=9480.jsonl` | 19 | 2024 | Same as above |

**No other staleness:**
- `laps`, `position`, `weather`, `race_control`, `drivers`, `meetings`, `sessions` — manifest success keys ⊇ file inventory keys for every other endpoint. File inventory contains zero orphan/stale files outside the three listed above.
- All manifest timestamps are inside a single 18-minute window on 2026-05-19 — there is **no mixed-window manifest**.
- Schema drift = 0 — types and column shapes are consistent across all 478 files.

### Is the stale data corrupt?
**No.** The three smoke JSONL files contain real OpenF1 records collected during the earlier smoke run (Phase 1–4 audits validated them). The only inconsistency is that the **manifest reports these sessions as `failed`** while the **JSONL files still exist on Drive**. Silver reads JSONL from Drive, so it will treat them as valid Bronze input — effectively adding 2 race sessions (9472, 9480 — 2024) and one pit-stop record set worth of clean data that the current run failed to re-fetch.

### Downstream consequences if you proceed as-is
- Silver will load 1,437 `session_result` rows (1,397 fresh + 40 smoke holdover) and 1,935 `pit` rows.
- Gold target will be computable for sessions 9472 and 9480 (2024 races) using the smoke records.
- Manifest will under-report what is in `data/bronze/`. Any narrative table that uses the manifest as the source of truth will show 70 race sessions; any table that uses `bronze_row_counts.csv` (or DuckDB) will show 72. Document this gap explicitly in the report.

---

## 8. Proceed / stop decision

### **PROCEED AFTER MINOR CHECK**

Pick one of the two options below before launching notebook 02:

| Option | Action | Effect | Recommended for |
|--------|--------|--------|-----------------|
| **A. Accept smoke holdover (no rework)** | Do nothing on Drive. Note the manifest-vs-files discrepancy in Silver audit. Run notebook 02 as-is. | Silver loads 72 race sessions worth of `session_result` (24/27/21 for 2023/2024/2025). Gold target computable for those 72. Smoke data treated as legitimate 2024 races. | Speed-first; no API rework |
| **B. Surgical cleanup (cleanest manifest)** | In a Colab cell, manually delete the three stale Drive files, then re-run notebook 01 cells 15–19 only (Bronze reports + DuckDB) so row_counts/file_inventory align with the manifest. | Silver loads 70 race sessions cleanly. Loses 9472/9480 unless re-ingested. Manifest matches Drive byte-for-byte. | Cleanest provenance |

If you also want to recover the 19 missing `session_result` races (and matching `pit`/`laps`/etc.) — they all failed with HTTP 429 except session 9086 (2023, 404):

| Option | Action |
|--------|--------|
| **C. Retry failed sessions (highest data quality)** | After A or B, run a one-cell retry loop using `OpenF1Client` with a 2–3 s sleep between calls for just the 19 failed `session_result` session_keys (and any other endpoints failed for those keys). Re-run notebook 01 cells 15–19 to refresh reports. Net: ~85+ races. |

**Path C is optional but ideal**, especially for the 2025 season where 9 of 30 race sessions currently lack target.

**Do not proceed to notebook 02 with a Drive full re-ingest** — it would lose the successfully-ingested ~75% of records and re-incur the same 429 storm.

---

## 9. What to send ChatGPT before launching notebook 02

### Minimum required summary (paste these)

1. **Manifest status counts**
   ```
   total=718, success=475, failed=243 (HTTP 429 dominant; HTTP 404 minority)
   ```
2. **Race sessions with target by year**
   ```
   2023: 24 / 29 (82.8%)
   2024: 25 / 30 (83.3%)   [+2 smoke holdover if Option A]
   2025: 21 / 30 (70.0%)
   ```
3. **Row counts by endpoint** (full table from §4 — manifest vs file inventory side-by-side)
4. **session_result rows**: 1,397 manifest / 1,437 on Drive (Δ=+40 from smoke holdover)
5. **Stale file list**: 3 JSONL files under `data/bronze/session_result/year=2024/` and `data/bronze/pit/year=2024/`
6. **Failed `session_result` session_keys**: list from §3
7. **starting_grid status**: 0 rows / 0 files — **optional, expected**
8. **Schema drift**: 0 (Spark and DuckDB agree)
9. **Chosen path**: A / B / C

### Optional detailed tables
- Bronze rows by endpoint × year (§4)
- Per-session failure clusters (e.g. session 9086 with 7 endpoint failures)
- DuckDB vs Spark cross-check table (§6)

### Warnings / errors to surface
- **Manifest vs `data/bronze/` row-count discrepancy** for `session_result` (+40) and `pit` (+19) caused by un-cleared smoke files.
- **78.7% race-session target coverage** — 19 sessions lack `session_result` due to 429.
- **`duckdb_bronze_bronze_endpoint_coverage.csv` exists on Drive but not in the local `evidence/full_2023_2025/` snapshot** — copy it before final evidence submission.

---

## 10. Recommended next action

1. **Decide path A, B, or C** (see §8).
2. If A: proceed immediately to notebook 02 (`02_silver_cleaning_quality.ipynb`) with defaults (`SILVER_ENGINE="spark"`, `ALLOW_FALLBACK=False`, `CLEAR_SILVER_OUTPUTS=True`).
3. If B: in a one-off Colab cell, run
   ```python
   from pathlib import Path
   drive = Path("/content/drive/MyDrive/openf1_big_data_pipeline/data/bronze")
   for p in [
       drive / "session_result/year=2024/session_key=9472.jsonl",
       drive / "session_result/year=2024/session_key=9480.jsonl",
       drive / "pit/year=2024/session_key=9480.jsonl",
   ]:
       if p.exists():
           p.unlink()
           print("deleted", p)
   ```
   Then re-run notebook 01 cells 15 onward (Bronze reports + DuckDB) only — do **not** re-run cell 11 ingestion.
4. If C: same as B, then run a retry loop with throttling on the 19 failed `session_result` keys (and any other endpoints flagged for those keys). Refresh Bronze reports.
5. After A / B / C, proceed to notebook 02.

---

## Final verdict (full Bronze)

**PROCEED AFTER MINOR CHECK** — Bronze is structurally healthy (Spark and DuckDB agree, zero schema drift, all required endpoints non-empty, all three seasons substantial). The only issues are documented and bounded:

- 3 stale smoke JSONL files (59 rows) on Drive — content valid, manifest discrepancy noted.
- 19 race sessions (across all three seasons) lack `session_result` due to HTTP 429 storms during ingest — recoverable via targeted retry (Path C).
- `starting_grid` 100% absent — optional, expected.

Silver will function correctly on this Bronze whether you choose path A, B, or C. **Do not** run a full Bronze re-ingest.

---

*Audit performed from frozen notebook outputs and Drive snapshot CSVs only. No code changes, no notebooks executed, no synthetic outputs created.*
