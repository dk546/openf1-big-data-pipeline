# Report Structure and Artifact Mapping Audit

| Field | Value |
|-------|-------|
| **Date/time (UTC)** | 2026-05-19T19:00:00Z (approx.) |
| **Scope** | Lock final report structure; map sections to pipeline artifacts |
| **Notebooks executed** | **None** |
| **Fake results created** | **None** — placeholders only for full-run pending items |

---

## Files created

| File | Action |
|------|--------|
| `reports/report_draft/report_structure.md` | Created — locked chapters 1–7 with purpose and evidence per section |
| `reports/report_draft/report_artifact_map.md` | Created — subsection → artifact, notebook, path, status |
| `reports/report_draft/table_figure_register.md` | Created — Tables 1–14, Figures 1–8 |
| `reports/report_draft/narrative_guardrails.md` | Created — locked narrative statements A–G |
| `artifacts/pipeline_logs/report_structure_artifact_mapping.md` | Created — this audit log |
| `README.md` | Updated — §15 Report structure and artifact traceability |
| `project_plan.md` | Updated — §9.2 expanded with report_draft references |
| `implementation_checklist.md` | Updated — report traceability checklist items |

---

## Report structure (locked)

Seven chapters, 26 subsections:

1. Executive Summary  
2. Data Landscape (2.1–2.4)  
3. Pipeline Architecture and Technology Rationale (3.1–3.6)  
4. Data Quality and Cleaning (4.1–4.5)  
5. Feature Engineering and Integration (5.1–5.5)  
6. Experimental Results and Analysis (6.1–6.6)  
7. Conclusion and Reflection (7.1–7.3)  

Full outline: [`reports/report_draft/report_structure.md`](../../reports/report_draft/report_structure.md).

---

## Artifact mapping summary

| Chapter | Primary artifact locations | Smoke status | Full-run status |
|---------|---------------------------|--------------|-----------------|
| 2 | `artifacts/manifests/`; `reports/data_quality/bronze_*`; `duckdb_bronze_*` | Available in `evidence/smoke_2024_maxsessions2_spark/` | Volume and 2023–2025 scope pending |
| 3 | README; `data/*`; pipeline logs; `duckdb_*` | Available | `run_manifest.json` after full `05` pending |
| 4 | `reports/data_quality/silver_*`; `duckdb_silver_*` | Available (37+ CSVs in smoke bundle) | Full-season missingness pending |
| 5 | `gold_*`; `feature_dictionary.csv`; `model_feature_plan.csv` | Available (smoke mart ~40 rows) | Full mart scale pending |
| 6 | `reports/model_results/`; `reports/tables/` | Code ready; smoke metrics **not** final | `MODELING_MODE=full` pending |
| 7 | `artifacts/pipeline_logs/phase*_*.md` | Audits available | Author reflection pending |

Cross-reference: [`report_artifact_map.md`](../../reports/report_draft/report_artifact_map.md).

---

## Table and figure register summary

| Type | Count | Smoke-ready | Final pending |
|------|-------|-------------|---------------|
| Tables | 14 | 1–10 (with caveats) | 11–14 (modeling) |
| Figures | 8 | 3–6 partial via `05` | 1–2 manual; 7–8 modeling |

Register: [`table_figure_register.md`](../../reports/report_draft/table_figure_register.md).

---

## Narrative guardrails summary

Locked topics in [`narrative_guardrails.md`](../../reports/report_draft/narrative_guardrails.md):

- **A.** Integrated race-session task framing (not strict pre-race)  
- **B.** Tier 1 early-session vs Tier 2 full-session analytical features  
- **C.** pandas at boundaries; PySpark transforms; DuckDB validation  
- **D.** Structural missingness and outlier flagging vs removal  
- **E.** Smoke vs full evidence policy  
- **F.** Always-forbidden model columns  
- **G.** `[PENDING]` placeholder convention  

---

## Remaining report risks before full run

| Risk | Impact | Mitigation |
|------|--------|------------|
| No full 2023–2025 Gold mart on Drive | Cannot publish official target rates or volumes | Colab `01`–`03` full path |
| Smoke model metrics cited as final | Invalid performance claims | Use guardrails §E; Table 11 placeholder |
| `feature_dictionary.csv` without `feature_tier` on Drive | Stale dictionary until Gold rerun | `model_feature_plan.csv` is modeling SoT |
| Figure 1 still placeholder | Architecture diagram missing in submission | Replace before final PDF |
| Notebook `05` not run on full inputs | No consolidated `reports/tables/` | Run `05` after `04` full |

---

## Phase audit inputs reviewed

- `artifacts/pipeline_logs/phase1_evidence_inventory_audit.md`  
- `artifacts/pipeline_logs/phase2_silver_quality_deep_dive.md`  
- `artifacts/pipeline_logs/phase3_gold_join_integration_audit.md`  
- `artifacts/pipeline_logs/phase4_feature_leakage_ml_readiness_audit.md`  
- `artifacts/pipeline_logs/phase4_fixes_applied.md`  
- `README.md`, `project_context.md`, `project_plan.md`, `implementation_checklist.md`
