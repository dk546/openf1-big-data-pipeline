# Bronze Ingestion Audit Report

**Audit date (UTC):** 2026-05-19  
**Auditor:** Cursor agent (pre–Colab full-run review)  
**Scope:** OpenF1 Bronze ingestion only (no Silver/Gold/modeling)

---

## Files reviewed

| File | Role |
|------|------|
| `README.md` | Stated data source and Colab workflow |
| `src/openf1_pipeline/config.py` | Base URL, seasons, endpoints, paths |
| `src/openf1_pipeline/ingestion/openf1_client.py` | HTTP client |
| `src/openf1_pipeline/ingestion/ingest.py` | Ingestion orchestration |
| `src/openf1_pipeline/bronze/build_bronze.py` | Bronze evidence reports |
| `src/openf1_pipeline/utils/io.py` | JSONL write (raw records) |
| `notebooks/01_ingestion_bronze.ipynb` | Colab entry point |
| `artifacts/manifests/ingestion_manifest.csv` | Local dev smoke manifest (reference) |
| `reports/data_quality/bronze_row_counts.csv` | Local dev smoke row counts (partial) |
| `.gitignore` | Data vs reports tracking |

---

## 1. Data source confirmation

### Verdict: **Ingestion uses the OpenF1 API, not the GitHub repository**

| Question | Finding |
|----------|---------|
| Base URL in code | `OPENF1_BASE_URL = "https://api.openf1.org/v1"` in `config.py` |
| HTTP library | `requests` in `OpenF1Client._request_with_retries()` |
| Example request URL | `https://api.openf1.org/v1/laps?session_key=9472` (built via `build_url(endpoint)`) |
| GitHub as data source | **No** — repository search shows no `github.com/br-g/openf1` URLs in Python ingestion code |
| GitHub references | Only in README/docs/notebook comments for **cloning the project repo**, not for F1 data |

The [br-g/openf1](https://github.com/br-g/openf1) repository is the upstream OpenF1 project documentation/source reference. This pipeline does **not** download race data from GitHub releases or raw files in that repo.

---

## 2. Ingestion flow (step by step)

```
run_bronze_ingestion()
    │
    ├─► OpenF1Client()  →  base URL https://api.openf1.org/v1
    │
    ├─► GLOBAL_ENDPOINTS (per season in `seasons` argument)
    │       meetings  → GET /v1/meetings?year={year}
    │       sessions  → GET /v1/sessions?year={year}
    │       Save: data/bronze/{endpoint}/year={year}/{endpoint}.jsonl
    │
    ├─► get_race_sessions(client, seasons)
    │       For each year: GET /v1/sessions?year={year}
    │       Filter rows where session_name or session_type matches "Race" (case-insensitive)
    │       Optional: max_sessions → .head(max_sessions) on race session list
    │
    └─► SESSION_ENDPOINTS (for each race session_key)
            GET /v1/{endpoint}?session_key={session_key}
            Save: data/bronze/{endpoint}/year={year}/session_key={session_key}.jsonl
            Append manifest row per call
    │
    └─► Write artifacts/manifests/ingestion_manifest.csv
```

### Season selection

- Default: `SEASONS = [2023, 2024, 2025]` from `config.py`
- Notebook smoke: `INGEST_SEASONS = [2024]` when `SMOKE_TEST = True`
- Notebook full: `INGEST_SEASONS = SEASONS` when `SMOKE_TEST = False`

### Raw record persistence

- `save_jsonl()` writes each API record dict as one JSON line (`json.dumps`, UTF-8)
- No column dropping or schema coercion at Bronze
- Fields preserved as returned by the API (minimal transformation)

---

## 3. Endpoint classification

| Endpoint | Type | Required / optional | Notes |
|----------|------|---------------------|-------|
| `meetings` | Global | Required | Calendar metadata per season |
| `sessions` | Global | Required | Session discovery; also used to derive race list |
| `drivers` | Session-level | Required | Per `session_key` |
| `laps` | Session-level | Required | High volume per session |
| `pit` | Session-level | Required | |
| `weather` | Session-level | Required | |
| `position` | Session-level | Required | High volume per session |
| `race_control` | Session-level | Required | |
| `session_result` | Session-level | **Required for Gold** | Final classification → `points_finish` |
| `starting_grid` | Session-level | **Optional** | `OPTIONAL_SESSION_ENDPOINTS`; 404 logged as `failed`, run continues |

Configured in `config.ENDPOINTS`, split into `GLOBAL_ENDPOINTS` and `SESSION_ENDPOINTS` in `ingest.py`.

---

## 4. Bronze storage design

### Confirmed path patterns

| Scope | Pattern |
|-------|---------|
| Global | `data/bronze/{endpoint}/year={year}/{endpoint}.jsonl` |
| Session | `data/bronze/{endpoint}/year={year}/session_key={session_key}.jsonl` |

`DATA_ROOT` env var redirects `data/` to Google Drive when set in Colab (`get_data_root()`).

### Raw preservation

- Bronze stores **API JSON objects line-by-line** in JSONL
- No Parquet conversion at Bronze
- No field filtering in ingestion layer

**Local smoke sample** (`bronze_row_counts.csv`): partial 2024 data only (dev run with `max_sessions=2`); not official evidence.

---

## 5. Ingestion manifest design

### Confirmed columns (`MANIFEST_COLUMNS`)

| Column | Present |
|--------|---------|
| `endpoint` | Yes |
| `year` | Yes |
| `session_key` | Yes (null for global rows) |
| `output_path` | Yes |
| `record_count` | Yes |
| `status` | Yes (`success` / `failed`) |
| `error_message` | Yes |
| `ingestion_timestamp_utc` | Yes |

Output file: `artifacts/manifests/ingestion_manifest.csv`

### Auditability

- One row per ingest attempt (global year or session+endpoint)
- Failed calls retain `error_message` and `record_count=0`
- Supports recomputing coverage: which sessions/endpoints succeeded
- Supports MBA report tables without narrative-only claims

**Note:** `output_path` currently stores **absolute paths** from the machine that ran ingestion (e.g. Windows paths in local smoke). On Colab, paths will be under `/content/...` or Drive. This does not affect JSONL data, only manifest path strings.

---

## 6. Failure handling

| Scenario | Behavior | Run continues? |
|----------|----------|----------------|
| HTTP **400** | `fetch_endpoint` returns `([], error_msg)`; manifest `status=failed` | Yes |
| HTTP **404** | Same (e.g. `starting_grid` missing for session) | Yes |
| HTTP **429** | Retry up to 3 times with backoff (`sleep_seconds * 2**attempt`); then failed if exhausted | Yes |
| Other HTTP / network | Retry; then `failed` | Yes |
| **Empty JSON array** `[]` | `status=success`, `record_count=0` | Yes |
| **starting_grid** unavailable | Logged as `failed`; optional endpoint; explicit log "continuing" | Yes |
| One endpoint fails for one session | Manifest row `failed`; other endpoints/sessions proceed | Yes |
| Global endpoint fails for one year | That year `failed`; other years proceed | Yes |

**Status semantics:** `success` = HTTP OK (including 0 rows). `failed` = request error (including 404).

---

## 7. Smoke test behavior (notebook)

| Setting | Smoke (`SMOKE_TEST = True`) |
|---------|------------------------------|
| `INGEST_SEASONS` | `[2024]` only |
| `MAX_SESSIONS` | `2` race sessions |
| Purpose | Validate API, paths, manifest, reports quickly before full run |

Protects against: long runtime, rate limits, and wasted Colab time before confirming imports and `session_result` availability.

---

## 8. Full ingestion behavior (`SMOKE_TEST = False`)

| Setting | Full run |
|---------|----------|
| `INGEST_SEASONS` | `[2023, 2024, 2025]` |
| `MAX_SESSIONS` | `None` (all race sessions per year) |

### Estimated risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Long runtime** | High | ~24 races/year × 3 years × 8 session endpoints ≈ hundreds of API calls + 0.25s pacing; allow 1–3+ hours in Colab |
| **API rate limits (429)** | Medium | Retries exist; increase `sleep_seconds` in client if needed |
| **Large `position` / `laps` files** | Medium | Session-scoped queries; use `DATA_ROOT` on Drive; monitor disk |
| **Colab disconnect** | Medium | Colab Pro Plus; save to Drive; rerun is idempotent per file (overwrites same paths) |
| **Memory** | Low–medium | Ingestion streams to JSONL per request, not one giant DataFrame |
| **`starting_grid` gaps** | Low | Expected; optional; document in report |
| **`session_result` missing** | Medium | Usually available; notebook warns if 0 rows on full run |
| **Overwrite smoke outputs** | Low | Full run overwrites same `year`/`session_key` paths; may leave orphan files only if smoke used different seasons only |

### 2025 season note

OpenF1 historical coverage starts 2023; 2025 races depend on calendar progress at run time. Document actual race count in manifest after Colab run.

---

## 9. Bronze evidence reports

Generated by `generate_bronze_reports()` after ingestion:

| Artifact | Path | Confirmed in code |
|----------|------|------------------|
| Ingestion manifest | `artifacts/manifests/ingestion_manifest.csv` | `ingest.run_bronze_ingestion` |
| File inventory | `reports/data_quality/bronze_file_inventory.csv` | `build_bronze.discover_bronze_files` |
| Row counts | `reports/data_quality/bronze_row_counts.csv` | `compute_bronze_row_counts` |
| Schema report | `reports/data_quality/bronze_schema_report.csv` | `build_bronze_schema_report` |
| Schema drift | `reports/data_quality/bronze_schema_drift.csv` | `detect_schema_drift` |
| Schema copy | `artifacts/schemas/bronze_schema_report.csv` | duplicate save |

---

## 10. Risks and weaknesses

### Colab / environment

| Issue | Detail |
|-------|--------|
| **`sys.path`** | Notebook adds `PROJECT_ROOT / "src"` — correct if `%cd` to repo root |
| **Project root detection** | `config.get_project_root()` uses `README.md` or `OPENF1_PROJECT_ROOT`; notebook previously checked `project_context.md` (gitignored on public repo) — **fixed** to use `README.md` |
| **`00_colab_setup.ipynb`** | Still mostly TODO placeholders; user must `%cd`, `pip install`, set `DATA_ROOT` manually |
| **Drive paths** | Supported via `DATA_ROOT`; not set automatically |

### Git / artifacts

| Issue | Detail |
|-------|--------|
| **`data/` gitignored** | Correct — Bronze bulk stays off GitHub |
| **`reports/` and `artifacts/`** | Not blanket-ignored; lightweight CSVs can be committed |
| **Planning `.md` gitignored** | `project_context.md` not on public repo; Cursor users keep locally |

### Design / operations

| Issue | Detail |
|-------|--------|
| **Duplicate sessions API calls** | `sessions` fetched in global ingest and again in `get_race_sessions` per year (minor inefficiency) |
| **Manifest absolute paths** | Cross-environment paths differ; consider relative paths in future |
| **No pagination** | OpenF1 returns full filtered array; session-scoped queries mitigate size |
| **`starting_grid` 404** | Recorded as `failed`, not `success` with 0 rows — acceptable for optional endpoint |
| **Local smoke ≠ evidence** | Existing CSVs are partial 2024 dev run only |

---

## 11. Recommended minimal fixes

| # | Fix | Status |
|---|-----|--------|
| 1 | Notebook `01`: detect repo via `README.md` not `project_context.md` | **Applied** |
| 2 | Before Colab: complete `00_colab_setup.ipynb` cells (`%cd`, `pip install`, `DATA_ROOT`) | User action |
| 3 | Colab: set `os.environ["DATA_ROOT"]` to Drive before ingestion | User action |
| 4 | Full run: monitor `session_result` row count in manifest summary | User action |
| 5 | Optional later: store relative paths in manifest | Deferred |

No Silver/Gold/modeling changes recommended in this audit.

---

## 12. Final verdict

### **Ready for Colab smoke run** (after completing setup notebook)

The ingestion design is **sound** for an evidence-driven MBA pipeline:

- Correct API source (`https://api.openf1.org/v1`)
- Raw JSONL Bronze with manifest and DQ reports
- Resilient per-endpoint failure handling
- Smoke/full controls in notebook

**Not yet ready for official full-run evidence** until:

1. Colab smoke run succeeds with `session_result` > 0  
2. Colab full run completes for 2023–2025 with manifest and Bronze reports saved to Drive  
3. Artifacts reviewed for report citations  

---

## Appendix: Local smoke manifest snapshot (reference only)

From `artifacts/manifests/ingestion_manifest.csv` (2024, 2 race sessions):

- 16 `success`, 2 `failed` (`starting_grid` HTTP 404)
- `session_result`: 40 rows total (20 per session)
- `starting_grid`: 0 rows (failed)

This confirms API ingestion and optional grid handling; **not** a substitute for Colab official runs.
