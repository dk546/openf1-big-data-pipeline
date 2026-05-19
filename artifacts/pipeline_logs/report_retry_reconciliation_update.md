# Report-draft update — Bronze retry and manifest-file reconciliation

**Date:** 2026-05-19 (UTC)
**Scope:** Report planning documents only. No pipeline code changed in this pass. No notebooks were executed. No fake outputs were created.
**Trigger:** Two recent Bronze improvements need to be reflected in the final-report scaffolding:

1. Targeted retry utility (`retry_failed_session_endpoints`, `merge_retry_into_manifest`, `summarize_retry_manifest`, `delete_stale_bronze_files`) writing `artifacts/manifests/ingestion_retry_manifest.csv`.
2. Manifest-vs-file reconciliation (`reconcile_manifest_to_bronze_files`, `summarize_bronze_reconciliation`, `generate_bronze_reconciliation_reports`) writing `bronze_manifest_file_reconciliation.csv`, `bronze_manifest_file_reconciliation_summary.csv`, and three DuckDB cross-checks (`duckdb_bronze_manifest_file_reconciliation_summary.csv`, `..._by_endpoint.csv`, `..._issues.csv`).

Both were added because the first full Bronze 2023–2025 run completed with stale smoke files on Drive (`session_result` 9472/9480 and `pit` 9480 for 2024) and 19 `session_result` failures from an HTTP 429 storm, producing real manifest-vs-disk drift (`session_result` 1,397 manifest success rows vs 1,437 rows on Drive; `pit` 1,916 vs 1,935).

---

## 1. Files reviewed

- `reports/report_draft/report_structure.md`
- `reports/report_draft/report_artifact_map.md`
- `reports/report_draft/table_figure_register.md`
- `reports/report_draft/narrative_guardrails.md`
- `README.md`
- `project_plan.md` *(present locally; not modified — no retry/reconciliation references to insert without risking inconsistency with the more authoritative report_draft files)*
- `implementation_checklist.md` *(present locally)*
- `artifacts/pipeline_logs/full_bronze_output_review.md`
- `artifacts/pipeline_logs/full_bronze_retry_plan.md`
- `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md`
- `src/openf1_pipeline/ingestion/ingest.py`
- `src/openf1_pipeline/bronze/build_bronze.py`
- `notebooks/01_ingestion_bronze.ipynb`

---

## 2. Files updated

| File | Nature of update |
|------|------------------|
| `reports/report_draft/report_structure.md` | §3.2, §4.2, §4.3, §4.4, §4.5, §6.6 expanded with explicit retry + reconciliation evidence and narrative anchors. |
| `reports/report_draft/report_artifact_map.md` | New status legend value `pending until retry completes`; §2.2, §2.3, §2.4, §3.2, §4.1, §4.2, §4.3, §4.4, §4.5, §6.6 rows expanded; new dedicated subsection "Bronze retry + reconciliation artifacts" with purpose / generator / source / full-run status / interpretation per artifact; notebook-order and path-pattern tables updated. |
| `reports/report_draft/table_figure_register.md` | Added Table 15 (Bronze ingestion retry summary), Table 16 (Bronze manifest-file reconciliation summary), Table 17 (Target coverage before/after retry), optional Figure 9 (Target coverage before vs after retry); status table extended; `05` consolidation table updated to note retry-section direct outputs. |
| `reports/report_draft/narrative_guardrails.md` | §G placeholders extended; new §H "Bronze retry and reconciliation language" with seven sub-statements (H.1–H.7). |
| `README.md` | Bronze layer evidence section unified into "Targeted retry and manifest-file reconciliation" with retry + reconciliation roles, Silver-gating rules, and full artifact table (already included from prior turns). |
| `implementation_checklist.md` | New §5a "Bronze targeted retry and manifest-file reconciliation" and §5b "Report-draft updates for retry and reconciliation"; implementation-side items checked, run-dependent items intentionally unchecked. |
| `artifacts/pipeline_logs/report_retry_reconciliation_update.md` | This audit log (new). |

`project_plan.md` was left unchanged. It is a planning doc, not a status doc, and the report_draft files plus README now cover the user-facing narrative. Updating `project_plan.md` would risk drift unless a separate planning revision is requested.

---

## 3. Where retry/reconciliation is discussed in the report structure

| Report section | Retry / reconciliation coverage |
|----------------|-------------------------------|
| 2.2 Scope & endpoints | Cites `ingestion_manifest.csv`, `ingestion_retry_manifest.csv`, `bronze_manifest_file_reconciliation.csv`. |
| 2.3 Volume & characteristics | Cites `bronze_manifest_file_reconciliation_summary.csv` for reconciled totals. |
| 2.4 Quality risks | Documents HTTP 429 cluster + stale-file risk; cites reconciliation issues CSV and the retry plan + reconciliation audit logs. |
| 3.2 Bronze layer | Explicitly lists the retry function, the reconciliation function, the new evidence CSVs, and links to the two audit logs. |
| 4.2 Detection strategy | Adds Bronze-layer detection block listing the reconciliation report and its DuckDB CSVs. |
| 4.3 Remediation rules | Adds Bronze-layer remediation block describing the targeted retry, throttling default (3 s), `starting_grid` exclusion, and the opt-in stale-file deletion. |
| 4.4 Before/after validation | Adds Bronze before/after block: original manifest vs retry manifest vs refreshed Bronze reports + reconciliation summary. Marked `[PENDING: targeted retry not yet executed]`. |
| 4.5 Cleaning impact on modeling | Adds explicit `session_result` coverage narrative tied to Bronze artifacts; marks final coverage `[PENDING: target coverage after retry]`. |
| 6.6 Reproducibility statement | Cites the full provenance chain — original manifest → retry manifest → reconciliation reports → refreshed Bronze reports → audit logs. |

---

## 4. New artifacts added to the artifact map

Added to `report_artifact_map.md` (Chapter rows 2.2, 2.3, 2.4, 3.2, 4.1, 4.2, 4.3, 4.4, 4.5, 6.6) and the dedicated **"Bronze retry + reconciliation artifacts"** subsection:

- `artifacts/manifests/ingestion_retry_manifest.csv`
- `reports/data_quality/bronze_manifest_file_reconciliation.csv`
- `reports/data_quality/bronze_manifest_file_reconciliation_summary.csv`
- `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_summary.csv`
- `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_by_endpoint.csv`
- `reports/data_quality/duckdb_bronze_manifest_file_reconciliation_issues.csv`
- `artifacts/pipeline_logs/full_bronze_retry_plan.md`
- `artifacts/pipeline_logs/bronze_manifest_file_reconciliation_added.md`

Each entry carries purpose, generator notebook, source path, full-run status (`pending until retry completes` for retry-only outputs), and interpretation notes.

The bottom path-pattern table now lists `artifacts/manifests/ingestion_retry_manifest.csv` and the reconciliation CSV patterns mapped to Chapters 2, 3, 4, 6. The notebook-order table for notebook `01` now mentions the retry manifest and reconciliation outputs.

---

## 5. New planned tables and figures

Added to `table_figure_register.md`:

| ID | Title | Section | Status |
|----|-------|---------|--------|
| Table 15 | Bronze ingestion retry summary | §3.2, §4.3 | pending until retry completes |
| Table 16 | Bronze manifest-file reconciliation summary | §3.2, §4.2 | full-run evidence available; post-retry version pending |
| Table 17 | Target coverage before/after retry | §4.4, §4.5 | pending until retry completes |
| Figure 9 (optional) | Target coverage before vs after retry | §4.4, §4.5 | pending Table 17 |

Suggested columns and source artifacts are documented in the register. Table 14 (Reproducibility artifacts) note was extended to remind authors to include the retry manifest and reconciliation reports.

---

## 6. Narrative guardrails added

A new section **H — Bronze retry and reconciliation language** was appended to `narrative_guardrails.md` with seven sub-statements:

- H.1 Do not hide API failures — use the prescribed sentence describing the HTTP 429 event and manifest logging.
- H.2 Do not call stale files corruption — describe them as out-of-manifest, not invalid.
- H.3 Explain why retry is better than a full rerun — emphasize "targeted" and "required session endpoints".
- H.4 Explain `starting_grid` correctly — optional endpoint, mapped to `optional_missing`.
- H.5 Explain target coverage — `session_result` is the modeling-completeness metric, 89 race sessions in scope.
- H.6 Avoid overclaiming — retry recovers transient failures only; persistent failures are source-availability limitations.
- H.7 Cite reconciliation as a data-quality control — quote the seven `reconciliation_status` categories verbatim when defining terms in Chapter 4.

Existing §G placeholder conventions were extended with `[PENDING: targeted retry results — RUN_TARGETED_RETRY=True]` and `[PENDING: post-retry target coverage]`.

---

## 7. Remaining pending items after retry completes

Until the user runs the retry section of `notebooks/01_ingestion_bronze.ipynb` (with `RUN_TARGETED_RETRY=True`) and produces refreshed Bronze evidence, the following items in the report-draft scaffolding remain as placeholders:

- Population of Table 15 (retry summary) from `ingestion_retry_manifest.csv`.
- Refreshed values for Table 16 (reconciliation summary) reflecting the post-retry state.
- Population of Table 17 (target coverage before/after retry) and the optional Figure 9.
- §4.4 Bronze before/after numbers (manifest success rows pre- vs post-retry vs on-disk row counts).
- §4.5 effective `session_result` coverage value (`x / 89`).
- §6.6 reproducibility statement entries citing retry manifest checksums / timestamps once the file exists.
- `implementation_checklist.md` §5a items: `Retry manifest generated`, `Reconciliation summary generated on full-run Drive evidence`, `DuckDB reconciliation CSVs generated on full-run Drive evidence`, `Retry results reviewed before Silver`, `Target coverage after retry reviewed`, `Full-run Bronze ready for Silver after reconciliation clean/pass`.

These items are explicitly tagged with the `[PENDING: ...]` convention from `narrative_guardrails.md §G` or the status value `pending until retry completes` in `report_artifact_map.md`.

---

## 8. Constraints honored

- No notebooks were executed.
- No fake outputs were created.
- No invented retry results were inserted into tables; all retry-derived numbers are explicit placeholders.
- No pipeline code was modified in this pass — only the four report-draft files, README, the implementation checklist, and this audit log.
- `project_plan.md` left untouched (no instruction to update it; the report_draft files are the authoritative narrative source).
