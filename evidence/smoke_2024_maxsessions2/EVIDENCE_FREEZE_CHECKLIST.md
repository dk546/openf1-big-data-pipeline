# Evidence Freeze Checklist — `smoke_2024_maxsessions2`

Use this checklist when copying **official Colab smoke** outputs from Google Drive into this folder (or when creating `smoke_2024_full` after full runs).

**Bronze context:** `SMOKE_TEST=True`, `MAX_SESSIONS=2`, `INGEST_SEASONS=[2024]`, `USE_GOOGLE_DRIVE=True`

**Drive output root:** `/content/drive/MyDrive/openf1_big_data_pipeline`

---

## Bronze — required

- [ ] `artifacts/manifests/ingestion_manifest.csv` (18 rows for smoke: 16 success, 2 `starting_grid` failed)
- [ ] `reports/data_quality/bronze_row_counts.csv` (**16** JSONL files, **~3,690** total rows — not 8 files / 2,266)
- [ ] `reports/data_quality/bronze_file_inventory.csv` (16 files)
- [ ] `reports/data_quality/bronze_schema_report.csv`
- [ ] `reports/data_quality/bronze_schema_drift.csv`
- [ ] `artifacts/schemas/bronze_schema_report.csv` (copy of schema report)

## Bronze — optional but useful

- [ ] Sample raw JSONL: `data/bronze/session_result/year=2024/session_key=*.jsonl` (one file)
- [ ] `artifacts/pipeline_logs/` audit notes if generated on Colab

## Silver — required (all under `reports/data_quality/`)

- [ ] `silver_table_inventory.csv`
- [ ] `silver_missingness_before.csv`
- [ ] `silver_missingness_after.csv`
- [ ] `silver_duplicate_report.csv`
- [ ] `silver_outlier_report.csv`
- [ ] `silver_temporal_anomaly_report.csv`
- [ ] `silver_referential_integrity_report.csv`
- [ ] `silver_cleaning_rules.csv`
- [ ] `silver_cleaning_impact_summary.csv`
- [ ] `silver_rejected_records_summary.csv` (may be empty if no rows removed)

## Silver — optional

- [ ] `data/silver/session_result_clean.parquet` (verify 40 rows on smoke)
- [ ] Other `data/silver/*_clean.parquet` if repo size allows (gitignored by default)

---

## Validation after copy

1. **Manifest:** `session_result` total `record_count` = 40; sum of success rows = 3,690.
2. **Row counts:** File count = 16; matches manifest success files.
3. **Silver inventory:** `session_result` row_count = 40; `laps` ≈ 2,031.
4. **Do not** commit bulk `data/bronze/*.jsonl` or all parquets to GitHub unless course requires it.

---

## Freeze status (updated 2026-05-19)

| File | Status |
|------|--------|
| `ingestion_manifest.csv` | **Done** — 18 rows, Drive paths |
| `bronze_row_counts.csv` | **Done** — 16 files, 3,690 rows |
| `bronze_file_inventory.csv` | **Done** — 16 files |
| `bronze_schema_report.csv` / drift | **Done** — 106 columns, 0 drift flags |
| Silver CSVs (8 types) | **Done** — match Colab run |
| `silver_temporal_anomaly_report.csv` | **Done** |
| Silver Parquet | Optional — on Drive only |

**Note:** Root `reports/data_quality/bronze_*.csv` may still be stale; always cite this evidence folder.

See `artifacts/pipeline_logs/silver_output_audit.md` for full audit.
