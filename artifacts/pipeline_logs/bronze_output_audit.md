# Bronze Output Audit Report

**Audit date (UTC):** 2026-05-19  
**Scope:** Bronze smoke test (`SMOKE_TEST=True`, `MAX_SESSIONS=2`, `INGEST_SEASONS=[2024]`)  
**Primary evidence:** `artifacts/manifests/ingestion_manifest.csv` (18 rows, aligns with Colab smoke summary)

---

## Files reviewed

| File | Status |
|------|--------|
| `artifacts/manifests/ingestion_manifest.csv` | **Complete** — matches smoke summary (3,690 success rows) |
| `reports/data_quality/bronze_row_counts.csv` | **Stale** — 8 files / 2,266 rows (single-session local subset) |
| `reports/data_quality/bronze_file_inventory.csv` | **Stale** — 8 files only |
| `reports/data_quality/bronze_schema_report.csv` | **Stale** — 95 columns, 8 endpoints (no `session_result`) |
| `reports/data_quality/bronze_schema_drift.csv` | **Stale** — derived from partial schema report |
| `artifacts/schemas/bronze_schema_report.csv` | Not present locally (copy may exist on Drive) |
| `data/bronze/**/*.jsonl` | Not in workspace (gitignored; expected on Drive after Colab) |
| `src/openf1_pipeline/ingestion/ingest.py` | Optional `starting_grid`, manifest design |
| `src/openf1_pipeline/bronze/build_bronze.py` | Evidence report generation |
| `src/openf1_pipeline/config.py` | `OPENF1_DATA_ROOT`, path layout |
| Silver DQ CSVs (reference) | `session_result` 40 rows loaded in prior Silver dev run |

---

## Bronze smoke summary (from manifest)

| Metric | Value |
|--------|------|
| Manifest rows | 18 |
| Status `success` | 16 |
| Status `failed` | 2 (`starting_grid` only) |
| Total rows ingested (success) | **3,690** |
| Race sessions | 2 (`session_key` 9472, 9480) |
| Season | 2024 |
| `session_result` rows | **40** (20 per session) |
| `starting_grid` rows | **0** (404, failed) |

**Note:** Manifest `output_path` values in the committed CSV use **Windows local paths** (`D:\Udemy\...`). Colab/Drive runs will show `/content/drive/MyDrive/...` paths. Row counts and file paths in manifest are still valid audit evidence; path strings are environment-specific.

---

## 1. Endpoint status

| Endpoint | Type | Status | Files (success) | Total rows | Notes |
|----------|------|--------|-----------------|------------|-------|
| `meetings` | Global | success | 1 | 25 | Calendar metadata |
| `sessions` | Global | success | 1 | 123 | Session discovery |
| `drivers` | Session | success | 2 | 40 | 20 drivers × 2 races |
| `laps` | Session | success | 2 | 2,031 | Largest volume (~1k/race) |
| `pit` | Session | success | 2 | 62 | Non-zero |
| `weather` | Session | success | 2 | 303 | Non-zero |
| `position` | Session | success | 2 | 927 | High frequency |
| `race_control` | Session | success | 2 | 139 | Non-zero |
| `session_result` | Session | success | 2 | **40** | **Required for Gold** |
| `starting_grid` | Session | **failed** | 0 | 0 | HTTP 404 — **optional** |

### `starting_grid` — optional and non-blocking

- Declared in `OPTIONAL_SESSION_ENDPOINTS` in `ingest.py`.
- Failures logged as `status=failed` with `error_message`; ingestion **continues**.
- Error: `HTTP 404 ... No results found` for both sessions — expected for some OpenF1 sessions.
- Gold can use grid heuristic only when data exists; not required for `points_finish`.

### `session_result` — usable for `points_finish`

- 40 rows = 20 classified results per race × 2 sessions (consistent with full F1 grid).
- Silver dev inventory shows columns include **`position`**, `driver_number`, `session_key`, `dnf`/`dns`/`dsq`, `points`.
- **Top-10 classified** target can be derived from `position` (and status flags) in Gold.
- No manifest failures for `session_result`.

---

## 2. Ingestion manifest audit

### Required columns

| Column | Present |
|--------|---------|
| `endpoint` | Yes |
| `year` | Yes |
| `session_key` | Yes (null for global rows) |
| `output_path` | Yes |
| `record_count` | Yes |
| `status` | Yes (`success` / `failed`) |
| `error_message` | Yes (populated on failure) |
| `ingestion_timestamp_utc` | Yes |

### Strengths

- One row per ingest attempt (audit trail).
- Distinguishes success vs failed without stopping the pipeline.
- Row counts per file support reconciliation (3,690 = sum of success `record_count`).
- Timestamps in UTC ISO format.

### Weaknesses

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| Absolute `output_path` | Harder to compare across Colab vs local | Accept for MBA; optional later: store relative paths |
| Failed rows still list `output_path` | File may be empty/missing | OK; `record_count=0` clarifies |
| Committed manifest paths ≠ Drive | Confusing if mixed environments | After Colab smoke, keep Drive manifest as source of truth; copy CSV to repo for evidence |

---

## 3. Row counts (manifest-derived)

Authoritative for this audit (committed `bronze_row_counts.csv` is **out of date**).

| Endpoint | Files | Total rows | Avg rows/file | Reasonable for 2-race smoke? |
|----------|-------|------------|---------------|------------------------------|
| meetings | 1 | 25 | 25 | Yes (2024 calendar) |
| sessions | 1 | 123 | 123 | Yes |
| drivers | 2 | 40 | 20 | Yes |
| laps | 2 | 2,031 | ~1,016 | Yes (telemetry-heavy) |
| pit | 2 | 62 | 31 | Yes |
| weather | 2 | 303 | ~152 | Yes |
| position | 2 | 927 | ~464 | Yes |
| race_control | 2 | 139 | ~70 | Yes |
| session_result | 2 | 40 | 20 | Yes |
| starting_grid | 0 | 0 | — | N/A (failed) |
| **Total (success)** | **16** | **3,690** | — | **Matches reported smoke total** |

**Action:** Re-run `generate_bronze_reports()` in Colab after smoke (or full) ingestion so `bronze_row_counts.csv` and `bronze_file_inventory.csv` match the manifest.

---

## 4. Schema report audit

### Committed repo state (stale)

- 95 schema rows across 8 endpoints — **missing `session_result` and `starting_grid`**.
- Built from only **8 JSONL files** (one race session), not the full 16-file smoke.

### Expected after regenerating reports on full smoke (Colab)

- Should include `session_result` (~10–15 columns: `position`, `driver_number`, `session_key`, etc.).
- `starting_grid` may still be absent if no successful files.
- User-reported **106 schema columns** on Colab is plausible with `session_result` included.

### Usefulness for MBA report

| Aspect | Assessment |
|--------|------------|
| Column names + observed types | Strong evidence of raw API shape |
| `non_null_sample_count` | Supports nullability discussion |
| `files_seen` | Supports cross-file consistency |
| Per-endpoint coverage | Good for Bronze DQ section |

### Columns to watch in Gold

- `session_result.position` — **critical** for `points_finish`
- `laps.lap_duration`, `position.position` — feature engineering
- Nullable speed sectors on laps — document in Silver/Gold, not Bronze issue

---

## 5. Schema drift

| Metric | Value |
|--------|-------|
| Drift flags (committed report) | **0** |
| Reason | Only **1 file per endpoint** in stale schema sample → `files_seen == max_files` → 100% coverage |

### Expected on full 2023–2025 ingestion

- Drift flags may appear when the same endpoint has **different columns across sessions/years** (API evolution, optional fields).
- Review any row with `possible_schema_drift_flag=True` and `coverage_pct < 100`.
- Zero flags on smoke is **expected**, not proof of zero drift in production.

### Watch list for full run

- `laps` (many optional telemetry fields)
- `position` (high file count)
- `session_result` (should remain stable for target building)

---

## 6. Bronze storage design

### Confirmed layout (from manifest paths)

| Scope | Pattern |
|-------|---------|
| Global | `data/bronze/{endpoint}/year={year}/{endpoint}.jsonl` |
| Session | `data/bronze/{endpoint}/year={year}/session_key={session_key}.jsonl` |

### Medallion fit

| Criterion | Assessment |
|-----------|------------|
| Raw JSONL, minimal transform | Yes (`save_jsonl` per API record) |
| Partitioned by year + session | Yes |
| Provenance via manifest | Yes |
| Separates global vs session API calls | Yes |

**Appropriate for Bronze** in a Colab + Drive MBA pipeline.

---

## 7. Reproducibility assessment

| Mechanism | Smoke status |
|-----------|--------------|
| Raw JSONL preservation | Yes (16 files on success) |
| Ingestion manifest | Yes — **strong** |
| Row counts CSV | **Out of sync** — regenerate |
| Schema report | **Out of sync** — regenerate |
| Schema drift report | **Out of sync** — regenerate |
| Drive-backed `OPENF1_DATA_ROOT` | Configured in notebooks (Colab) |
| API source documented | `https://api.openf1.org/v1` |

**Verdict:** Reproducibility design is sound; **regenerate DQ CSVs on Drive** after each official run before citing them in the report.

---

## 8. Risks before full ingestion

| Risk | Severity | Notes |
|------|----------|-------|
| API rate limits (429) | Medium | Retries in `openf1_client`; allow hours for full run |
| `starting_grid` 404s | Low | Optional; many sessions may fail |
| Overwrite smoke files | Low | Same paths for 2024 races; full run adds years/sessions |
| Manifest absolute paths | Low | Document environment in report |
| 2023 / 2025 coverage | Medium | 2025 calendar may be incomplete at run time |
| `session_result` availability | Low–medium | 100% success on smoke; monitor manifest on full run |
| `laps` + `position` volume | **High** | Largest Bronze tables; Drive space and runtime |
| Stale DQ reports in repo | Medium | Regenerate and copy summaries after Colab |
| Colab disconnect | Medium | Drive persistence + rerun idempotent paths |

---

## 9. Recommended minimal fixes (before full Bronze)

| Priority | Action | Code change? |
|----------|--------|--------------|
| **Required** | Re-run `generate_bronze_reports()` in Colab after smoke/full ingestion | No — run notebook cell |
| **Required** | Copy updated manifest + DQ CSVs from Drive to repo for GitHub evidence (optional) | No |
| Optional | Store relative paths in manifest | Small `ingest.py` change — defer unless needed |
| Optional | Add notebook reminder: run report cell before Silver | Markdown only |

**No Gold or modeling changes recommended.**

---

## 10. Final verdict

### **Bronze smoke passed**

The ingestion manifest proves a successful 2-session 2024 smoke: all required endpoints succeeded, `session_result` has 40 rows, and only optional `starting_grid` failed with documented 404 errors.

### Minor housekeeping before full run

- Regenerate `bronze_row_counts.csv`, `bronze_file_inventory.csv`, and schema reports from **Drive Bronze** so evidence files match the manifest.
- Proceed with full ingestion using `USE_GOOGLE_DRIVE=True`, `SMOKE_TEST=False`, seasons 2023–2025.

### Not blocking

- `starting_grid` failures
- Zero schema drift flags on smoke
- Windows paths in committed manifest (replace with Drive paths in report narrative)

---

## Appendix: Manifest failure detail

```
starting_grid | 2024 | 9472 | failed | HTTP 404: No results found
starting_grid | 2024 | 9480 | failed | HTTP 404: No results found
```
