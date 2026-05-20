# Figure 1 — Medallion architecture diagram (placeholder)

This file is a placeholder for the **Medallion architecture diagram** referenced as Figure 1 in §3.1 of the final report. The Mermaid diagram below is rendered inline on GitHub and is sufficient for the report draft and the in-repo evidence bundle. Replace this file with a polished PNG (FigJam / draw.io / Lucidchart) before the final PDF submission if a designed diagram is preferred.

## Pipeline layers

```mermaid
flowchart LR
    api["OpenF1 API\n(https://api.openf1.org/v1)"] -->|requests + retry| bronze
    bronze["Bronze\nimmutable JSONL\nper (endpoint, year, session_key)"] -->|PySpark + DuckDB validation| silver
    silver["Silver\ntyped, cleaned Parquet\n(non-destructive)"] -->|PySpark + DuckDB validation| gold
    gold["Gold\ndriver-race feature mart\n1,756 rows x 62 cols\n40 model features"] -->|pandas + sklearn / LightGBM| modeling
    modeling["Modeling\n3 baselines + 3 ML models\nseason splits 2023/2024/2025"] -->|matplotlib + pandas| reports
    reports["Reports\nreports/tables/, reports/figures/\nrun_manifest.json"]
```

## Engine and provenance overlay

```mermaid
flowchart TB
    subgraph ingestion [01_ingestion_bronze]
        bronze_layer["Bronze layer\n(Python + requests)"]
        manifest["ingestion_manifest.csv +\nretry manifest +\neffective manifest"]
        reconciliation["bronze_manifest_file_reconciliation*.csv\n(gate before Silver)"]
    end
    subgraph silver_nb [02_silver_cleaning_quality]
        silver_layer["Silver layer\n(PySpark cleaning,\nDuckDB validation)"]
        silver_dq["silver_*.csv\n(missingness, duplicates,\nreferential integrity)"]
    end
    subgraph gold_nb [03_gold_feature_engineering]
        gold_layer["Gold layer\n(PySpark joins, feature tiers,\nDuckDB validation)"]
        gold_dq["gold_*.csv\n+ leakage_guard +\nfeature_dictionary +\nmodel_feature_plan"]
    end
    subgraph modeling_nb [04_modeling_evaluation]
        modeling_layer["Baselines + LR + RF + LightGBM\nseason splits 2023/2024/2025"]
        model_results["reports/model_results/*.csv\n+ model_run_manifest.json"]
    end
    subgraph reports_nb [05_report_artifacts]
        consolidation["Report consolidation"]
        tables["reports/tables/ (11 CSVs)"]
        figures["reports/figures/ (5 PNGs)"]
        run_manifest["artifacts/manifests/run_manifest.json"]
    end
    bronze_layer --> silver_layer
    silver_layer --> gold_layer
    gold_layer --> modeling_layer
    modeling_layer --> consolidation
    consolidation --> tables
    consolidation --> figures
    consolidation --> run_manifest
```

## Technology rationale (one-glance)

| Layer | Primary engine | Validation engine | Reporting / handoff |
|-------|----------------|-------------------|---------------------|
| Bronze ingestion | Python + `requests` | PySpark Bronze reports + DuckDB SQL | pandas CSV summaries |
| Silver cleaning | **PySpark** (`SILVER_ENGINE=spark`) | DuckDB SQL | pandas CSV summaries |
| Gold feature mart | **PySpark** (`GOLD_ENGINE=spark`) | DuckDB SQL | pandas CSV summaries |
| Modeling | pandas + scikit-learn + LightGBM | — | pandas CSV + matplotlib PNG |
| Report artifacts | pandas + matplotlib | — | CSV + PNG |

Databricks is **out of scope**; PySpark runs locally inside Google Colab via `get_spark()`. DuckDB provides independent SQL cross-checks of Spark and pandas outputs at every layer.

## Replacement guidance

When producing the final PDF, replace this file with a rendered PNG (e.g. `architecture_diagram.png`) and update the Figure 1 reference in [`reports/report_draft/table_figure_register.md`](../../../../reports/report_draft/table_figure_register.md) and [`reports/report_draft/report_structure.md`](../../../../reports/report_draft/report_structure.md) §3.1 accordingly. The Mermaid source above can be exported via the GitHub renderer or pasted into [https://mermaid.live](https://mermaid.live) for a quick PNG export.
