# Full-Run Readiness Audit — OpenF1 2023–2025 Pipeline

| Field | Value |
|-------|-------|
| **Audit date/time (UTC)** | 2026-05-19T17:12:00Z |
| **Audit scope** | Final readiness check before executing the full 2023–2025 Bronze → Silver → Gold → Modeling → Report Artifacts pipeline in Colab Pro Plus |
| **Architecture** | Python `requests` ingestion → PySpark Bronze reports → PySpark Silver cleaning → PySpark Gold feature mart → DuckDB validation throughout → scikit-learn / LightGBM consumption (Databricks out of scope) |
| **ML task** | Points-finish classification from integrated race-session features (Tier 1 early-session + Tier 2 full-session analytical). Not strict pre-race prediction. |
| **Final verdict** | **GO for full run** after applying the cell-10 fix below and the two manual notebook flips |
| **Notebooks executed during audit** | **None** |
| **Fake outputs created during audit** | **None** |

---

## 1. Files reviewed

### Documentation
- `README.md` (modified — pending commit)
- `project_context.md`, `project_plan.md`, `implementation_checklist.md` (root-level, gitignored by design)
- `reports/report_draft/report_structure.md`
- `reports/report_draft/report_artifact_map.md`
- `reports/report_draft/table_figure_register.md`
- `reports/report_draft/narrative_guardrails.md`

### Pipeline code (Spark-first official architecture)
- `src/openf1_pipeline/config.py`
- `src/openf1_pipeline/utils/io.py`, `utils/cleanup.py`, `utils/spark.py`
- `src/openf1_pipeline/analytics/duckdb_validation.py`
- `src/openf1_pipeline/ingestion/openf1_client.py`, `ingestion/ingest.py`
- `src/openf1_pipeline/bronze/build_bronze.py`
- `src/openf1_pipeline/silver/build_silver.py`, `silver/build_silver_spark.py`
- `src/openf1_pipeline/gold/build_feature_mart.py`, `gold/build_feature_mart_spark.py`
- `src/openf1_pipeline/features/feature_dictionary.py`
- `src/openf1_pipeline/modeling/feature_selection.py`, `splits.py`, `baselines.py`, `train.py`, `evaluate.py`
- `src/openf1_pipeline/reporting/report_tables.py`

### Notebooks
- `notebooks/00_colab_setup.ipynb`
- `notebooks/01_ingestion_bronze.ipynb`
- `notebooks/02_silver_cleaning_quality.ipynb`
- `notebooks/03_gold_feature_engineering.ipynb`
- `notebooks/04_modeling_evaluation.ipynb`
- `notebooks/05_report_artifacts.ipynb` (**one syntax fix applied to cell 10**)

### Audit logs (read-only inputs to this audit)
- `phase1_evidence_inventory_audit.md`
- `phase2_silver_quality_deep_dive.md`
- `phase3_gold_join_integration_audit.md`
- `phase4_feature_leakage_ml_readiness_audit.md`
- `phase4_fixes_applied.md`
- `report_structure_artifact_mapping.md`
- `downstream_notebook_safety_audit.md`
- `spark_silver_duplicate_report_fix.md`
- `spark_silver_starting_grid_fix.md`

### Frozen smoke evidence
- `evidence/smoke_2024_maxsessions2_spark/` — 37 CSV artifacts + `feature_dictionary.csv` (all confirmed present)

---

## 2. Overall readiness verdict

### **GO for full run** (after minor fixes)

| Component | Verdict | Blocker? |
|-----------|---------|----------|
| GitHub / Colab readiness | GO after commit | Untracked report-draft markdowns + modified README must be committed before `git pull` in Colab |
| Drive cleanup policy | GO | Frozen smoke evidence is in repo `evidence/`, not on Drive — safe to clear `data/`, `reports/`, `artifacts/` on Drive |
| Notebook settings | GO with 2 manual flips | `01: SMOKE_TEST=False`, `04: MODELING_MODE="full"` |
| Bronze | GO | No blockers |
| Silver | GO | No blockers (Phase 2 fixes already merged) |
| Gold | GO | No blockers (Phase 3 fixes already merged) |
| Modeling | GO | No blockers (Phase 4 fixes already merged; full-mode raises on empty splits) |
| Report artifacts | GO after cell-10 fix | **Was a hard blocker**; fixed in this audit |
| Runtime/resource risks | Acceptable | All risks medium or below with documented mitigations |

---

## 3. GitHub and Colab readiness

### Repository structure check

| Check | Result |
|-------|--------|
| `pyproject.toml` present | Yes (`src/`-layout package `openf1-big-data-pipeline` 0.1.0, requires Python ≥3.10) |
| `requirements.txt` includes PySpark / DuckDB / LightGBM / scikit-learn / pyarrow | Yes — `pyspark>=3.5.0`, `duckdb>=0.9.0`, `lightgbm>=4.1.0`, `scikit-learn>=1.3.0`, `pyarrow>=14.0.0` |
| Editable install supported (`pip install -e .`) | Yes (used by every notebook 00–05 setup cell) |
| `src/openf1_pipeline/` package layout | Yes (consistent across all notebooks' import paths) |

### `.gitignore` policy

| Pattern | Behavior | Correct? |
|---------|----------|---------|
| `/*.md` + `!/README.md` | Ignore ROOT markdowns except README; subdirectory `.md` tracked | Yes (project_context/plan/checklist intentionally local-only) |
| `data/bronze\|silver\|gold/**/*` + `.gitkeep` | Bulk data files ignored; folder structure preserved | Yes |
| `*.parquet`, `*.jsonl`, `data/**/*.json`, `data/**/*.csv` | Bulk data ignored | Yes |
| `evidence/` | Not in `.gitignore` → tracked | Yes (smoke evidence CSVs are safely committed) |
| `*.pkl`, `*.pickle`, `*.h5`, `*.pt`, `*.onnx`, `models/` | Model binaries ignored | Yes |
| `.env`, `.env.*` (except `.env.example`) | Secrets ignored | Yes |
| `spark-warehouse/`, `metastore_db/`, `derby.log` | Spark scratch ignored | Yes |

### Current git state (read-only check)

```
M  README.md
?? artifacts/pipeline_logs/report_structure_artifact_mapping.md
?? reports/report_draft/narrative_guardrails.md
?? reports/report_draft/report_artifact_map.md
?? reports/report_draft/report_structure.md
?? reports/report_draft/table_figure_register.md
```

### Action checklist before pushing to Colab

| Action | Files |
|--------|-------|
| **Commit (required)** | `README.md`, `reports/report_draft/*.md` (4 files), `artifacts/pipeline_logs/report_structure_artifact_mapping.md`, `notebooks/05_report_artifacts.ipynb` (cell 10 fix from this audit), `artifacts/pipeline_logs/full_run_readiness_audit.md` (this report) |
| **Do NOT commit** | `project_context.md`, `project_plan.md`, `implementation_checklist.md` (root, gitignored), any `.env`, any `*.parquet`/`*.jsonl` |
| **Already tracked** | `pyproject.toml`, `requirements.txt`, all `src/openf1_pipeline/**/*.py`, all `notebooks/*.ipynb`, `evidence/smoke_2024_maxsessions2_spark/` |

### Notebook setup cell consistency

| Notebook | Cell | Setup pattern | Result |
|----------|------|---------------|--------|
| 00–05 | Setup cell | `git clone/pull`, `pip install -r requirements.txt`, `pip install -e .`, Drive mount when `USE_GOOGLE_DRIVE=True`, set `OPENF1_DATA_ROOT` **before** importing `config` | Consistent and correct |

### Notebook compile check (read-only)

All notebooks' code cells parse cleanly under Python AST after the cell-10 fix.

---

## 4. Drive output policy and cleanup guidance

### Drive layout

Active root: `/content/drive/MyDrive/openf1_big_data_pipeline`

```
openf1_big_data_pipeline/
├── data/{bronze,silver,gold}/         <-- bulk pipeline outputs
├── reports/{data_quality,tables,figures,model_results}/
└── artifacts/{manifests,schemas,feature_definitions,pipeline_logs}/
```

### Smoke evidence safety

| Evidence | Location | Safe during Drive cleanup? |
|----------|----------|----------------------------|
| `evidence/smoke_2024_maxsessions2_spark/` (37 CSVs + feature_dictionary.csv) | **Repo root** (`/content/openf1-big-data-pipeline/evidence/...`) | **YES — completely separate from Drive output root**, untouched by Drive deletions |

### Recommended Drive cleanup (executed manually in Colab — do not run any notebook for this)

```python
# In a Colab cell, AFTER mounting Drive:
import shutil
from pathlib import Path
root = Path("/content/drive/MyDrive/openf1_big_data_pipeline")
for sub in ["data", "reports", "artifacts"]:
    p = root / sub
    if p.exists():
        shutil.rmtree(p)
        print("Deleted", p)
    p.mkdir(parents=True, exist_ok=True)
```

### What to delete vs preserve on Drive

| Path | Action |
|------|--------|
| `data/bronze/`, `data/silver/`, `data/gold/` | Delete (regenerated by notebooks 01–03) |
| `reports/data_quality/`, `reports/tables/`, `reports/figures/`, `reports/model_results/` | Delete (regenerated by 01–05) |
| `artifacts/manifests/`, `artifacts/schemas/`, `artifacts/feature_definitions/` | Delete (regenerated by 01–05) |
| `artifacts/pipeline_logs/` | Delete (logs from prior runs only; in-repo audit logs are unaffected) |

### Notebook cleanup flag behavior after manual Drive wipe

Each layer notebook owns its layer only — already audited in `downstream_notebook_safety_audit.md`:

| Notebook | Cleanup flag | Cleans | Preserves |
|----------|--------------|--------|-----------|
| 01 | `CLEAR_BRONZE_OUTPUTS=False` (recommended after manual wipe) | Bronze JSONL + bronze DQ + ingestion manifest | Everything else |
| 02 | `CLEAR_SILVER_OUTPUTS=True` | Silver Parquet + silver DQ + duckdb_silver CSVs | Bronze, Gold |
| 03 | `CLEAR_GOLD_OUTPUTS=True` | Gold Parquet + gold DQ + duckdb_gold + feature_dictionary | Silver, models |
| 04 | `CLEAR_MODEL_OUTPUTS=True` | model_results CSVs + model_run_manifest.json | Gold, DQ |
| 05 | `CLEAR_REPORT_ARTIFACTS=True` | reports/tables + reports/figures | DQ, model_results |

Notebook 01 will **not** delete Bronze data unless explicitly configured — safe.

---

## 5. Notebook settings audit for full run

### Required settings vs current defaults

| Notebook | Setting | Required for full run | Current default | Action |
|----------|---------|----------------------|-----------------|--------|
| 01 | `USE_GOOGLE_DRIVE` | `True` | `True` | OK |
| 01 | `SMOKE_TEST` | `False` | **`True`** | **Manual flip required** |
| 01 | `MAX_SESSIONS` | `None` (auto when `SMOKE_TEST=False`) | `2 if SMOKE_TEST else None` | OK (driven by `SMOKE_TEST`) |
| 01 | `INGEST_SEASONS` | `SEASONS = [2023, 2024, 2025]` | `[2024] if SMOKE_TEST else SEASONS` | OK (driven by `SMOKE_TEST`) |
| 01 | `BRONZE_REPORT_ENGINE` | `"spark"` | `"spark"` | OK |
| 01 | `ALLOW_FALLBACK` | `False` | `False` | OK |
| 01 | `CLEAR_BRONZE_OUTPUTS` | `False` (after manual Drive wipe) or `True` (force re-ingest) | `False` | OK |
| 02 | `USE_GOOGLE_DRIVE` | `True` | `True` | OK |
| 02 | `SILVER_ENGINE` | `"spark"` | `"spark"` | OK |
| 02 | `ALLOW_FALLBACK` | `False` | `False` | OK |
| 02 | `CLEAR_SILVER_OUTPUTS` | `True` | `True` | OK |
| 03 | `USE_GOOGLE_DRIVE` | `True` | `True` | OK |
| 03 | `GOLD_ENGINE` | `"spark"` | `"spark"` | OK |
| 03 | `ALLOW_FALLBACK` | `False` | `False` | OK |
| 03 | `CLEAR_GOLD_OUTPUTS` | `True` | `True` | OK |
| 04 | `USE_GOOGLE_DRIVE` | `True` | `True` | OK |
| 04 | `MODELING_MODE` | `"full"` | **`"smoke"`** | **Manual flip required** |
| 04 | `CLEAR_MODEL_OUTPUTS` | `True` | `True` | OK |
| 05 | `USE_GOOGLE_DRIVE` | `True` | `True` | OK |
| 05 | `CLEAR_REPORT_ARTIFACTS` | `True` | `True` | OK |

### Required manual changes summary

```python
# notebooks/01_ingestion_bronze.ipynb (cell 7, the configuration cell)
SMOKE_TEST = False     # was True

# notebooks/04_modeling_evaluation.ipynb (cell 5, the configuration cell)
MODELING_MODE = "full" # was "smoke"
```

No other manual changes required.

---

## 6. Bronze full-run readiness

| Check | Status | Detail |
|-------|--------|--------|
| API source `https://api.openf1.org/v1` | OK | `OPENF1_BASE_URL` in `config.py` |
| Seasons 2023, 2024, 2025 | OK | `SEASONS = [2023, 2024, 2025]` in `config.py` |
| Endpoints: meetings, sessions, drivers, laps, pit, weather, position, race_control, session_result, starting_grid | OK | All 10 in `ENDPOINTS` dict |
| `GLOBAL_ENDPOINTS` vs `SESSION_ENDPOINTS` | OK | `meetings`, `sessions`, `drivers` global; lap/pit/weather/position/race_control/session_result/starting_grid per-session |
| `session_result` required for target | OK | Ingested as a session endpoint; manifest enforces presence in `01` (warns if total rows = 0 and not smoke) |
| `starting_grid` optional | OK | 404/empty handled in `openf1_client.py`; Silver writes empty schema (`SILVER_EMPTY_SCHEMAS["starting_grid"]`); Gold builder does not depend on it |
| Retry/backoff for 429 | OK | `openf1_client.py` handles HTTP 429 with exponential backoff and recorded retry counts |
| Manifest records failures without stopping run | OK | `ingest.py` writes `ingestion_manifest.csv` with `status` per endpoint, never aborts on non-2xx |
| Spark Bronze reports default | OK | `BRONZE_REPORT_ENGINE="spark"`, `ALLOW_FALLBACK=False` in notebook 01 |
| DuckDB Bronze validation | OK | `duckdb_bronze_*.csv` written in notebook 01 |
| Full run can be long (laps + position) | Documented risk | See section 11 |

### Bronze verdict: **GO**

### What to verify after notebook 01
- `artifacts/manifests/ingestion_manifest.csv` — count `status=success` vs `status=failed`
- `reports/data_quality/bronze_row_counts.csv` — per-endpoint totals; `session_result` should be ≥ ~40 × (race sessions per season × 3 seasons)
- `reports/data_quality/duckdb_bronze_bronze_total_rows.csv` — must equal sum of `bronze_row_counts`
- `reports/data_quality/bronze_schema_drift.csv` — flag any unexpected column changes
- Failed endpoints expected: occasional `starting_grid` (optional)

---

## 7. Silver full-run readiness

| Check | Status | Detail |
|-------|--------|--------|
| Spark Silver default | OK | `SILVER_ENGINE="spark"` in notebook 02 |
| Pandas fallback disabled by default | OK | `ALLOW_FALLBACK=False` (no silent fallback after partial Spark writes) |
| `starting_grid` empty schema handled | OK | `SILVER_EMPTY_SCHEMAS` provides explicit columns; empty Parquet written with valid schema; rule `SIL_OPTIONAL_MISSING` |
| Duplicate reporting handles ndarray/list/dict columns | OK | `quality/profiling.py` `make_dataframe_hashable_for_duplicates` (fix in `spark_silver_duplicate_report_fix.md`) |
| Silver output cleanup avoids Spark-dir vs pandas-file collisions | OK | `clean_silver_layer_outputs()` and `CLEAR_SILVER_OUTPUTS=True` before each run |
| Expected reports produced | OK | 10-table inventory, missingness before/after, duplicate, outlier, temporal anomaly, RI, cleaning rules, cleaning impact, rejected records, DuckDB row counts |
| Structural missingness documented | OK | `pit.stop_duration` 100% null and `race_control.qualifying_phase` documented in Phase 2 audit; not imputed in Silver |
| Memory acceptable on full run | Acceptable | Spark processes JSONL by partition; Parquet writes are directory-style; report CSVs are small |
| Silver does NOT construct target | OK | Confirmed — target built in Gold only |
| Silver does NOT blindly impute modeling features | OK | Confirmed — only type casting, null-key drops, deduplication |

### Silver verdict: **GO**

### What to verify after notebook 02
- `reports/data_quality/silver_table_inventory.csv` — 10 tables present, row counts ≥ Bronze
- `reports/data_quality/silver_cleaning_impact_summary.csv` — `rows_before` vs `rows_after` per table
- `reports/data_quality/silver_referential_integrity_report.csv` — 0 unmatched on required parents
- `reports/data_quality/duckdb_silver_silver_row_counts.csv` — must agree with Spark Silver inventory
- `silver_duplicate_report.csv` must complete without error (was previously failing on ndarray columns — now fixed)
- `starting_grid_clean.parquet/` exists with valid schema even if empty

---

## 8. Gold full-run readiness

| Check | Status | Detail |
|-------|--------|--------|
| Spark Gold default | OK | `GOLD_ENGINE="spark"` in notebook 03 |
| Base table `session_result_clean` | OK | `build_feature_mart_spark.py` reads Silver `session_result_clean.parquet` |
| Grain = `(session_key, meeting_key, driver_number)` | OK | One row per driver-session; duplicate check in `gold_duplicate_keys` |
| Target `points_finish = 1 if points > 0 else 0` | OK | Computed in Gold; `TARGET_COLUMN="points_finish"` |
| Feature groups align with locked plan (laps, pit, position, weather, race_control, metadata) | OK | Phase 3 confirmed; `feature_dictionary.csv` 63 rows with 40 `allowed_for_modeling=True` |
| Leakage guard excludes outcome columns | OK | `LEAKAGE_FORBIDDEN_COLUMNS` includes `position`, `points`, `result_*`, `duration`, `gap_to_leader`, `number_of_laps`; `diagnostic_*` blocked by pattern |
| `feature_dictionary.csv` includes `feature_tier` | OK | Phase 4 fix in `features/feature_dictionary.py` |
| `model_feature_plan.csv` exists with 40 default features | OK | Notebook 03 creates via `save_model_feature_plan()` if missing; tier1_early=8, tier2_full_session=32 |
| DuckDB Gold validation included | OK | `duckdb_gold_*.csv` 5 reports + by-team / by-circuit slices |
| Gold output cleanup prevents stale outputs | OK | `clean_gold_layer_outputs()` and `CLEAR_GOLD_OUTPUTS=True` before each run |
| Empty `session_result` raises clear error | OK | Spark Gold builder fails fast (downstream notebook safety audit) |

### Gold verdict: **GO**

### What to verify after notebook 03
- `data/gold/driver_race_feature_mart.parquet/` exists; row count matches Silver `session_result_clean`
- `reports/data_quality/gold_target_distribution.csv` — both classes present per season
- `reports/data_quality/gold_join_quality_report.csv` — all feature groups 100% matched (`unmatched_pct=0`)
- `reports/data_quality/gold_leakage_guard_report.csv` — every blocked column has `allowed_for_modeling=False`
- `artifacts/feature_definitions/feature_dictionary.csv` — `feature_tier` column populated
- `artifacts/feature_definitions/model_feature_plan.csv` — 40 rows with `default_include=True`
- `duckdb_gold_gold_row_count.csv` must equal Spark Gold row count

---

## 9. Modeling full-run readiness

| Check | Status | Detail |
|-------|--------|--------|
| `MODELING_MODE="full"` requires season splits | OK | `resolve_modeling_splits(mode="full")` raises if any of train/val/test is empty or season-set mismatch |
| Train = 2023, validation = 2024, test = 2025 | OK | Enforced by notebook 04 cell 11 (`expected = {"train": {2023}, "validation": {2024}, "test": {2025}}`) |
| Empty split → notebook fails clearly | OK | `ValueError` raised with remediation message |
| Baselines: random, majority, heuristic | OK | All three wired in notebook 04 cell 13; `majority_class_baseline` added in Phase 4 fixes |
| Models: Logistic Regression, Random Forest, LightGBM | OK | `train.py` `build_logistic_regression_pipeline`, `build_random_forest_pipeline`, `build_lightgbm_pipeline`; all use `class_weight="balanced"` |
| Feature list uses `model_feature_plan.csv` first | OK | `resolve_model_feature_columns()` priority: plan → dictionary → raise |
| Leakage validation before training | OK | `validate_no_leakage(gold_df, feature_dict)` runs in notebook 04 cell 9 |
| Metrics: accuracy, precision, recall, F1, ROC-AUC | OK | `compute_classification_metrics()` returns all five; ROC-AUC null-safe for single-class splits |
| Confusion matrix written | OK | `compute_confusion_matrix_table()` per (model, split) |
| Error analysis written | OK | `build_error_analysis()` joins back to session/team/circuit |
| Feature importance written | OK | `extract_feature_importance()` per model |
| Model manifest written | OK | `save_modeling_outputs()` writes `model_run_manifest.json` with mode, split method, evidence tier, seed, feature counts, tier counts |
| No smoke metrics as final | OK | `evidence_tier=smoke_wiring_only` set automatically; full mode requires verified season splits |

### Modeling verdict: **GO after `MODELING_MODE="full"`**

### What to verify after notebook 04
- Split metadata: `split_method`, `evidence_tier` (must be `season_split` and **not** `smoke_wiring_only`)
- Split sizes: train/validation/test all non-zero; target rate per split prints
- `reports/model_results/baseline_metrics.csv` — 3 baselines × 2 splits = 6 rows
- `reports/model_results/validation_metrics.csv` — 3 models × validation
- `reports/model_results/test_metrics.csv` — 3 models × test
- `reports/model_results/confusion_matrix.csv` — per (model, split)
- `reports/model_results/feature_importance.csv` — at least RF and LightGBM populated
- `artifacts/manifests/model_run_manifest.json` — `modeling_mode="full"`, `feature_count=40`, `feature_tier_counts={tier1_early:8, tier2_full_session:32, total_default_numeric:40}`

---

## 10. Report artifact readiness

| Check | Status | Detail |
|-------|--------|--------|
| Report structure locked | OK | `reports/report_draft/report_structure.md` — 7 chapters, 26 subsections |
| Artifact map exists | OK | `reports/report_draft/report_artifact_map.md` |
| Table/figure register exists | OK | `reports/report_draft/table_figure_register.md` — 14 tables, 8 figures |
| Narrative guardrails exist | OK | `reports/report_draft/narrative_guardrails.md` — A–G locked statements |
| Notebook 05 does not fake missing outputs | OK | Each `build_*` function returns empty DataFrame when input CSV missing; `write_report_tables` skips empty |
| Tables generated only from real CSVs | OK | All inputs are concrete file paths (`DQ_DIR / "..."`, `MODEL_DIR / "..."`) |
| Figures only from existing CSVs | OK | `if cm_path.is_file(): ...` pattern throughout cell 12 |
| Missing inputs print WARNING, not crash | OK | Each missing-input branch prints `WARNING: ... missing — skipped figure` |
| Final report writable section-by-section | OK | Artifact map links each subsection to a discrete CSV/figure |
| Cell 10 SyntaxError (placeholder write) | **Fixed in this audit** | Changed opening `'''` to `'` so adjacent string literals concatenate |

### Report artifact verdict: **GO** (after cell-10 fix applied)

### What to verify after notebook 05
- `reports/tables/*.csv` — list of generated tables (data_volume_by_layer, bronze_endpoint_row_counts, silver_cleaning_impact, silver_error_taxonomy, gold_feature_group_summary, gold_target_distribution, model_baseline_comparison, model_validation_test_metrics, confusion_matrix, error_analysis_summary, reproducibility_artifacts)
- `reports/figures/*.png` — target_distribution.png, model_metric_comparison.png, confusion_matrix.png, feature_importance_top20.png, missingness_before_after.png
- `reports/figures/architecture_diagram_placeholder.md` — written by the fixed cell 10
- `artifacts/manifests/run_manifest.json` — paths populated; includes `model_run_manifest` link
- No `WARNING: ... missing` for any model_results inputs (would indicate an earlier notebook failed)

---

## 11. Runtime and resource risk assessment

| # | Risk | Severity | Likelihood | Mitigation | Proceed? |
|---|------|----------|------------|------------|----------|
| 1 | OpenF1 API rate limits / 429 | Medium | Medium | `openf1_client.py` has retry/backoff with exponential delay; manifest records retries; idempotent re-runs | **Yes** |
| 2 | Colab runtime disconnection during multi-hour Bronze ingestion | High | Medium | Outputs persisted to Drive each step; layer notebooks are independent; can resume per layer; bronze JSONL written incrementally | **Yes** — keep Colab tab active; consider running 01 first in a dedicated session |
| 3 | Google Drive write latency | Low | High | Accept; small CSV writes batched; bulk writes are Parquet | **Yes** |
| 4 | Spark driver memory on full 2023–2025 laps/position | Medium | Medium | Spark writes Parquet as partitioned directories; Silver/Gold use Spark DataFrame ops without `collect()`; pandas conversion via `safe_to_pandas(max_rows=500_000)` only at reporting boundary | **Yes** — monitor Colab memory in 02/03 |
| 5 | Spark reading/writing many JSONL/Parquet files | Low | Medium | DuckDB validation uses `_glob_parquet` to handle directories; Spark handles JSONL glob natively | **Yes** |
| 6 | DuckDB reading large Parquet directories | Low | Medium | DuckDB `_parquet_readable()` defensive check; status=error reported if unreadable | **Yes** |
| 7 | Full position/laps table size (millions of rows) | Medium | Medium | Spark lazy plan; aggregation to driver-session grain in Gold reduces to ~3000 rows; report CSVs are aggregates only | **Yes** |
| 8 | 2025 season availability / incompleteness | Medium | High | `MODELING_MODE="full"` **raises** on empty test split, so this is loud-fail not silent-wrong. If 2025 is partial but non-empty, document gap in report. If 2025 has zero race sessions, fall back to 2-fold (2023 train / 2024 test) and document. | **Yes** — verify after notebook 01 that 2025 `session_result` rows > 0 |
| 9 | Class imbalance (typically ~50% points-finish across full F1 grid is too optimistic; smoke 50/50 is artifact) | Medium | Medium | `class_weight="balanced"` already applied in all three models; report precision/recall/F1 alongside accuracy | **Yes** |
| 10 | Single-class split / model convergence | Medium | Low | ROC-AUC is null-safe in `evaluate.py`; LogisticRegression `max_iter` already increased; LightGBM and RF tolerate any class balance | **Yes** |
| 11 | Gold Spark→pandas CSV truncation at 500k rows in reporting | Low | Medium | Mart itself is written in Spark Parquet (not truncated); only audit CSV sampling capped — documented in `downstream_notebook_safety_audit.md` | **Yes** |

**Aggregate proceed decision: YES — proceed with full run.** No high-severity blockers remain.

---

## 12. Full-run monitoring checklist

### Notebook 00 — Colab setup

| Confirm | Verify | Stop if |
|---------|--------|---------|
| `USE_GOOGLE_DRIVE=True` | Drive mount succeeded; `OPENF1_DATA_ROOT` printed = `/content/drive/MyDrive/openf1_big_data_pipeline`; `pip install -e .` exited 0 | Drive mount fails or `openf1_pipeline` import fails |

### Notebook 01 — Bronze ingestion (**flip `SMOKE_TEST=False` first**)

| Confirm before run | `SMOKE_TEST=False`, `INGEST_SEASONS=[2023,2024,2025]`, `MAX_SESSIONS=None`, `BRONZE_REPORT_ENGINE="spark"`, `ALLOW_FALLBACK=False`, `CLEAR_BRONZE_OUTPUTS=False` (Drive was pre-cleaned) |
|---|---|
| **Outputs to check** | `artifacts/manifests/ingestion_manifest.csv`; `reports/data_quality/bronze_row_counts.csv`; `reports/data_quality/bronze_schema_drift.csv`; `duckdb_bronze_bronze_total_rows.csv`; per-endpoint bronze files in `data/bronze/{endpoint}/season=YYYY/...` |
| **Stop if** | `session_result_total_rows == 0`; any required endpoint (not `starting_grid`) shows 0 successful files for all sessions; schema drift flagged True for any endpoint |
| **Send ChatGPT** | (1) manifest status counts (success vs failed by endpoint); (2) row counts by endpoint from `bronze_row_counts.csv`; (3) `session_result` total rows; (4) failed endpoints list |

### Notebook 02 — Silver cleaning & quality

| Confirm before run | `SILVER_ENGINE="spark"`, `ALLOW_FALLBACK=False`, `CLEAR_SILVER_OUTPUTS=True`, same `USE_GOOGLE_DRIVE` as 01 |
|---|---|
| **Outputs to check** | `data/silver/*_clean.parquet/` (10 directories); `reports/data_quality/silver_table_inventory.csv`; `silver_cleaning_impact_summary.csv`; `silver_referential_integrity_report.csv`; `silver_duplicate_report.csv` (must not error on ndarray columns); `duckdb_silver_silver_row_counts.csv` |
| **Stop if** | Any required Silver table empty (except `starting_grid`); RI report shows unexpected unmatched parent rows; duplicate report fails to write |
| **Send ChatGPT** | (1) `silver_table_inventory` rows; (2) `silver_cleaning_impact_summary`; (3) RI report; (4) `duckdb_silver_silver_row_counts` summary |

### Notebook 03 — Gold feature engineering

| Confirm before run | `GOLD_ENGINE="spark"`, `ALLOW_FALLBACK=False`, `CLEAR_GOLD_OUTPUTS=True` |
|---|---|
| **Outputs to check** | `data/gold/driver_race_feature_mart.parquet/`; `gold_target_distribution.csv`; `gold_join_quality_report.csv`; `gold_leakage_guard_report.csv`; `feature_dictionary.csv` (with `feature_tier`); `model_feature_plan.csv` (40 default rows); `duckdb_gold_gold_row_count.csv` |
| **Stop if** | Gold row count != Silver `session_result_clean` row count; any join group has unmatched_pct > 0; both classes not present in target; `validate_no_leakage()` raises |
| **Send ChatGPT** | (1) Gold row count; (2) `gold_target_distribution` (per-season ideal); (3) `gold_join_quality_report`; (4) `gold_leakage_guard_report` summary (40 allowed); (5) feature tier counts |

### Notebook 04 — Modeling & evaluation (**flip `MODELING_MODE="full"` first**)

| Confirm before run | `MODELING_MODE="full"`, `CLEAR_MODEL_OUTPUTS=True` |
|---|---|
| **Outputs to check** | Split metadata prints (`split_method="season_split"`, `evidence_tier` is NOT `smoke_wiring_only`); per-split sizes and target rates; baseline + validation + test metrics tables; `reports/model_results/confusion_matrix.csv`; `error_analysis.csv`; `feature_importance.csv`; `artifacts/manifests/model_run_manifest.json` |
| **Stop if** | Any of train/val/test empty (raises in full mode by design); `validate_no_leakage()` raises; metric DataFrame empty after training |
| **Send ChatGPT** | (1) split sizes; (2) target distribution by split; (3) validation + test metrics tables; (4) confusion matrix; (5) feature importance top 20 |

### Notebook 05 — Report artifacts

| Confirm before run | `CLEAR_REPORT_ARTIFACTS=True`; **cell 10 fix is in place** (from this audit) |
|---|---|
| **Outputs to check** | `reports/tables/*.csv` (~11 tables); `reports/figures/*.png` (~5 figures); `reports/figures/architecture_diagram_placeholder.md`; `artifacts/manifests/run_manifest.json` |
| **Stop if** | More than one `WARNING: ... missing — skipped figure` for model_results inputs (suggests notebook 04 produced incomplete outputs) |
| **Send ChatGPT** | (1) list of generated tables; (2) list of generated figures; (3) any "WARNING: ... missing" lines |

---

## 13. Minimal fixes applied during this audit

| # | File | Cell / Location | Change | Reason |
|---|------|-----------------|--------|--------|
| 1 | `notebooks/05_report_artifacts.ipynb` | Cell 10 (`## Architecture diagram placeholder` write) | Opening `'''` → `'` (one character) so adjacent string literals concatenate | Unterminated triple-quoted string `SyntaxError`; would have blocked notebook 05 execution after cell 10 |

No other code, configuration, or notebook changes were made. No data files were created. No notebooks were executed.

---

## 14. Final go/no-go decision

### **GO for full run** (after the cell-10 fix applied above and the two manual flips before execution)

| Requirement | Status |
|-------------|--------|
| Code architecture matches official Spark-first plan | YES |
| All notebooks parse cleanly | YES (after this audit's fix) |
| Smoke audit issues addressed (P1–P4 fixes merged) | YES |
| Manual settings clearly identified | YES (`SMOKE_TEST=False`, `MODELING_MODE="full"`) |
| Drive cleanup plan defined and safe for frozen smoke evidence | YES |
| Runtime risks assessed, no high-severity blockers | YES |
| Monitoring checklist defined per notebook | YES |
| Required git commit set before Colab clone identified | YES |

**Run order:** `00 → 01 → 02 → 03 → 04 → 05` with the same `USE_GOOGLE_DRIVE=True` and same `OPENF1_DATA_ROOT` in every notebook.

**Stop and re-audit if:** any layer notebook raises an exception that is not in the documented stop-conditions above, or if any DuckDB validation report disagrees with its Spark counterpart by more than rounding.

---

*Audit performed by static review only. No notebooks executed, no API calls made, no synthetic outputs created.*
