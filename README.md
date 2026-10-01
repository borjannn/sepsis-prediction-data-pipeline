# Sepsis Data Warehouse — ETL Pipeline for PhysioNet/CinC 2019

An end-to-end **ETL pipeline and data warehouse** built on the [PhysioNet/Computing in Cardiology Challenge 2019](https://physionet.org/content/challenge-2019/1.0.0/) dataset: hourly ICU records of ~40,000 patients from two hospitals, labelled for **sepsis**.

The project has two parts:
- **Data Warehouses project (this repository's focus):** extract the raw data, clean and transform it, and load it into a PostgreSQL warehouse with a star schema, data-quality checks and analytical (OLAP) views.
- **Data Mining project (later):** uses the warehouse's **leakage-safe, ML-ready feature mart** to build models for **early sepsis prediction**.

> **Status:** planning. See [`docs/PLAN.md`](docs/PLAN.md) for the detailed implementation plan and [`docs/SETUP.md`](docs/SETUP.md) for environment setup.

---

## Architecture at a glance

```
PhysioNet S3 ─► Extract ─► Bronze (raw) ─► Silver (clean + features) ─► Gold (star schema) ─► Marts ─► Parquet export ─► Data Mining
                  Python       Python/Polars         dbt / SQL                 dbt / SQL           dbt        Python
                                                                                   └─► TimescaleDB add-on (time-series showcase)
```

**Stack:** Python 3.14 (Polars, httpx, psycopg, pandera, Typer, uv) · PostgreSQL 18 + TimescaleDB (Docker) · dbt-core / dbt-postgres

---

## Pipeline stages

| # | Stage | What it does |
|---|---|---|
| 0 | **Foundation** | Docker'd PostgreSQL + TimescaleDB, database schemas per layer, config, a `sepsis` CLI, and audit tables that log every run. |
| 1 | **Extract** | Downloads ~40,336 patient files (`.psv`) from PhysioNet's public S3 mirror. Downloads run in parallel, can be resumed, and each file is verified by MD5. A manifest makes reruns incremental. |
| 2 | **Bronze load, split, profiling** | Parses all files with Polars, adds lineage (patient ID, hospital, source file), validates the schema, and loads everything into `raw.*` and Parquet. Then fixes the patient-level split, **before** any data-driven decision. Finally profiles the data (missingness, distributions, assumption checks), so the cleaning rules can be checked against the data. |
| 3 | **Silver: staging and validation** | dbt models rename and type the columns, unpivot to one row per observation, and flag physiologically implausible values (ranges kept in one seed file) and harmonise units. Data-quality tests run on every build. |
| 4 | **Silver: imputation, features, labels** | Capped forward-fill (LOCF), "was measured" / "hours since measured" indicators, clinical scores (SIRS, partial qSOFA/SOFA, shock index), rolling 6 h/24 h vital-sign statistics, and sepsis onset/label engineering. Every feature uses **past data only**. |
| 5 | **Gold: star schema** | Kimball dimensional model: `fact_patient_hour`, `fact_measurement`, `fact_icu_stay`, plus dimensions for patient, hospital, ICU unit, stay-hour and clinical variable. |
| 6 | **Marts** | An ML feature table that follows a strict leakage contract (`feat_` / `meta_` / `label_` / `outcome_` columns). Includes patient-level splits (random 70/15/15, 5 CV folds, and a hospital A → B holdout) and reference statistics computed on the training set only. OLAP views (`CUBE`, `ROLLUP`, `GROUPING SETS`) for sepsis rates, vital-sign trends and pre-onset trajectories. |
| 7 | **Export and hand-off** | Versioned Parquet exports with a data dictionary and manifest, so the Data Mining team can work without running the warehouse. |
| 8 | **TimescaleDB add-on** | An isolated stage: hypertable, compression, continuous aggregates and gap-filling, plus an honest benchmark against plain PostgreSQL. |
| 9 | **Run-all and polish** | A one-command `sepsis run-all`, dbt docs/lineage, end-to-end tests (including leakage tests), and material for the course report. |

---

## Repository layout (target)

```
├── README.md
├── docs/                 # PLAN.md, SETUP.md, generated profile & benchmark reports
├── docker-compose.yml    # PostgreSQL 18 + TimescaleDB
├── src/sepsis_pipeline/  # Python: extract, bronze load, profiling, export, CLI
├── dbt/sepsis_dw/        # dbt: staging → silver → gold → marts, seeds, tests
├── sql/                  # DB init scripts and the TimescaleDB add-on
├── tests/                # pytest
├── data/                 # (gitignored) raw files and bronze Parquet
└── exports/              # (gitignored) versioned outputs for Data Mining
```

---

## Data source and license

Reyna, M. A., Josef, C. S., Jeter, R., Shashikumar, S. P., Westover, M. B., Nemati, S., Clifford, G. D., & Sharma, A. (2020). *Early Prediction of Sepsis From Clinical Data: The PhysioNet/Computing in Cardiology Challenge 2019.* Critical Care Medicine, 48(2), 210–217.

Dataset: PhysioNet, DOI [10.13026/v64v-d857](https://doi.org/10.13026/v64v-d857). The project page lists **CC BY 4.0**, and the bundled `LICENSE.txt` is the **Open Database License (ODbL) v1.0**. This project gives attribution under both, and shares derived databases under the same terms.

PhysioNet: Goldberger, A., et al. (2000). *PhysioBank, PhysioToolkit, and PhysioNet.* Circulation, 101(23), e215–e220.

Raw data and exports are not committed to this repository.
