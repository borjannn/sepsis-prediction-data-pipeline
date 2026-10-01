# Implementation Plan — Sepsis Data Warehouse & ETL Pipeline

**Project:** ETL pipeline and data warehouse for the PhysioNet/CinC Challenge 2019 sepsis dataset
**Scope of this document:** the Data Warehouses (DW) project only. The Data Mining (DM) project consumes this pipeline's outputs and is out of scope here, except for the hand-off contract (Stage 7).
**Owner:** 1 person (DW). Consumers: a team of 3 (DM), working on **early sepsis prediction**.
**Status:** Implemented (2026-10-01): all stages built and tested on the full dataset. Plan revision 3 (agreed with the independent reviewer); implementation deviations are listed in [§10 Implementation log](#10-implementation-log).

---

## 1. Goals and non-goals

### Goals
1. A **reproducible, idempotent** pipeline. A single command takes you from "nothing" to a populated warehouse.
2. A **layered architecture** (bronze → silver → gold → marts) with clear contracts between layers.
3. A **Kimball-style star schema** that supports analytical (OLAP) queries over patients, hospitals, ICU units and time-in-stay.
4. An **ML-ready, leakage-safe feature mart** for early sepsis prediction. This is the foundation of the DM project.
5. **Data quality and lineage** that are visible and testable: tests, audit tables, run logs.
6. A **TimescaleDB add-on stage** that demonstrates time-series features with an honest benchmark. It is isolated, so dropping it never breaks the core pipeline.

### Non-goals
- Training models or choosing algorithms (DM project).
- Real-time, streaming or incremental-CDC ingestion. The dataset is a static, versioned snapshot (v1.0.0).
- A BI dashboard, workflow orchestrators (Airflow/Prefect), or production-grade ops. See §8 *Possible extensions*.

---

## 2. The source data

### 2.1 Verified facts

| Fact | Value | How verified |
|---|---|---|
| Source | PhysioNet Challenge 2019 v1.0.0, DOI [10.13026/v64v-d857](https://doi.org/10.13026/v64v-d857). Public S3 mirror: `s3://physionet-open/challenge-2019/1.0.0/` (HTTPS: `https://physionet-open.s3.amazonaws.com/...`) | PhysioNet page |
| License | The project page lists **CC BY 4.0**. The bundled `LICENSE.txt` is **ODbL v1.0**. We comply with both: attribution, plus share-alike for derived databases we share. | Page + `LICENSE.txt` |
| Layout | `training/training_setA/` (hospital A, 20,336 files), `training/training_setB/` (hospital B, 20,000 files) | S3 listing, PhysioNet page |
| File format | One `.psv` per patient (pipe-separated, header row, missing values = `NaN`), one row per ICU hour | Sample file |
| Size | 40,336 patients, ~1.55M patient-hours | PhysioNet page |
| Integrity | There is **no ZIP**, and `SHA256SUMS.txt` does **not** cover the training files. The S3 `ETag` equals the file's **MD5** (checked on `p000001.psv`). physionet.org also sends an ETag, but it is nginx's mtime-size value, which is not comparable to an MD5. | Manual check |
| Columns (41) | 8 vitals: `HR, O2Sat, Temp, SBP, MAP, DBP, Resp, EtCO2` · 26 labs: `BaseExcess, HCO3, FiO2, pH, PaCO2, SaO2, AST, BUN, Alkalinephos, Calcium, Chloride, Creatinine, Bilirubin_direct, Glucose, Lactate, Magnesium, Phosphate, Potassium, Bilirubin_total, TroponinI, Hct, Hgb, PTT, WBC, Fibrinogen, Platelets` · 6 demographics/admin: `Age, Gender, Unit1, Unit2, HospAdmTime, ICULOS` · label: `SepsisLabel` | Sample file |
| Label semantics | For sepsis patients, `SepsisLabel = 1` for hours `t ≥ t_sepsis − 6`. For non-sepsis patients it is always 0. | PhysioNet page |
| Age coding | `Age` in years, **100 for patients aged 90 or above** (top-coded) | PhysioNet page |
| Scoring (utility) | Official `evaluate_sepsis_score.py`:<ul><li>`dt_early=-12, dt_optimal=-6, dt_late=3.0, max_u_tp=1, min_u_fn=-2, u_fp=-0.05, u_tn=0`</li><li>`t_sepsis = argmax(labels) − dt_optimal`, which is a **row index**</li><li>best predictions = 1 on `[max(0, t_sepsis−12), min(t_sepsis+3+1, n))`</li><li>normalised score = `(observed − inaction)/(best − inaction)`</li></ul> | Script source (GitHub `physionetchallenges/evaluation-2019`) |
| Hospitals | Data comes from three hospital systems. Two are public (A, B). The hidden test set was drawn from A, B and a censored hospital C. | PhysioNet page + Reyna et al. 2020 |
| What's missing | No patient IDs inside the files (only the filename), no absolute timestamps (only `ICULOS` = hours since ICU admission), heavy missingness (labs often > 90% NULL) | Sample file |

### 2.2 Dataset construction rules (Reyna et al. 2020, *Crit Care Med* 48(2)), confirmed again in profiling
- Stays with fewer than 8 hourly records were excluded, and records were truncated at **2 weeks (336 h)**. This means **record length ≈ ICU length of stay, which is an outcome** (see §4.2).
- Patients with `t_sepsis < 4 h` after ICU admission were excluded.
- `HospAdmTime` is usually negative (hospital admission before ICU admission).

### 2.3 Assumptions to verify (each becomes a test)
- `(patient_id, ICULOS)` is unique, and `ICULOS` is contiguous (+1 per row) per patient. Check whether it always starts at 1.
- `Age`, `Gender`, `Unit1`, `Unit2` and `HospAdmTime` are constant within a patient.
- `SepsisLabel` is monotonic non-decreasing per patient, and stays 1 until the last row once positive.
- Units are consistent. In particular, check whether `FiO2` mixes fractions (0.21–1.0) with percentages (21–100), and whether `Temp` is always °C.
- `Unit1`/`Unit2` are MICU/SICU one-hot flags, or both NULL. Check whether both are ever 1.
- Minimum age (to define the age bands).

### 2.4 Profiling outcomes (resolved 2026-10-01, see `docs/data_profile.md`)

| Item | Result | Handling |
|---|---|---|
| `(patient_id, ICULOS)` unique, contiguous | Confirmed: 0 duplicates, 0 gaps | Uniqueness tested as an error; contiguity as a warning |
| `ICULOS` starts at 1 | **Violated** for 8,793 patients (21.8%) | `row_idx` (0-based) is used for the challenge utility, `iculos` for time-in-stay. Because hours are contiguous, `row_idx = iculos − min(iculos)` |
| Static attributes constant | Confirmed for all five | First row wins; `static_attr_conflict` stays as a guard |
| Label monotonic | Confirmed | Tested |
| Stays ≥ 8 rows, ≤ 336 h | Confirmed | — |
| FiO2 percent values | Only 16 values in (1, 100] (train), plus values in (0, 0.2), ≤ 0 and up to 4,000 | Rescale (1, 100] to a fraction; everything else outside 0.2–1.0 is flagged out of range |
| **Calcium unit mix** | ~11% of calcium values are < 2: ionized calcium (mmol/L) recorded under total calcium (mg/dL) | `is_unit_mismatch` flag: kept in long/`fact_measurement`, excluded from the clean wide table |
| Lactate / magnesium units | Distributions match mmol/L and mg/dL respectively (the PhysioNet page lists mg/dL and mmol/dL) | Seed uses the observed units; lactate `valid_max` widened to 35 (max observed 31) |
| Temp always °C | Yes (median 37). A few implausible values (< 25 or > 45) | Flagged out of range |
| Unit1/Unit2 | MICU or SICU one-hot, or both NULL (UNKNOWN: 47% at A, 30% at B); never both | `unit_code` MICU / SICU / UNKNOWN (BOTH kept in the seed but unused) |
| Minimum age | 14; 22 patients < 18; 392 patients top-coded at 100 | Age band `<18` added |
| Hospital-dependent missingness | EtCO2 never recorded at A; BaseExcess/HCO3/Chloride ~99.8% missing at B; DBP 48% missing at A vs 14% at B | Flagged as **shortcut risk** in the data dictionary (Stage 7) |
| Sepsis prevalence | 7.27% of patients (A 8.80%, B 5.71%); 426 positive from row 0; 15 with `t_sepsis` after the record ends | `onset_left_censored` / `onset_after_record_end` flags |
| Rows over the stay | 57% of rows in hours 1–24, 91% in hours 1–48 | Expect chunk skew in Stage 8 |

---

## 3. Architecture

```
                ┌──────────── Python (Polars) ─────────────┐   ┌──────────────────────────── dbt (SQL in Postgres) ─────────────────────────────┐
 PhysioNet S3 ──► [1] EXTRACT ──► data/raw/*.psv ──► [2] BRONZE ──► raw.patient_hour ──► staging ──► patient_split ──► [3] SILVER ──► [4] SILVER ──► [5] GOLD ──► [6] MARTS ──► [7] EXPORT ──► Parquet for DM
                  manifest +                         + data/bronze/          (view)    (Stage 2,       cleaning      imputation,     star        ML features
                  MD5 checks                           *.parquet                         before any    (long ↔ wide)  features,      schema      + OLAP views
                                                     + profile (train-only stats)        data-driven                  labels
                                                                                         decision)                                    │
                                                                                                                                      └──► [8] TIMESCALE ADD-ON (ts.*)
 audit.* (etl_run, dbt_model_run, dq_results) ◄── written by every stage
```

### 3.1 Technology choices

| Concern | Choice | Why |
|---|---|---|
| Warehouse DBMS | **PostgreSQL 18 + TimescaleDB 2.30**, Docker image `timescale/timescaledb:2.30.2-pg18` (the non-`-oss` tag, so columnstore and continuous aggregates are included) | A real DBMS with `CUBE`/`ROLLUP`/`GROUPING SETS`, window functions and materialized views. The Timescale image is still plain Postgres underneath, so the core pipeline stays portable. |
| Extract / bronze | **Python 3.14** + **Polars** + **httpx** | Fast, parallel file I/O and parsing of 40k small files. |
| Transformations | **dbt-core + dbt-postgres** | SQL-first, versioned models. Built-in lineage graph, documentation, data tests and grants. |
| Python-side validation | **pandera** (Polars backend) | Schema checks on bronze. (Great Expectations doesn't support Python 3.14 yet.) |
| DB driver / bulk I/O | **psycopg 3**: `COPY FROM STDIN` for loading, `COPY (query) TO STDOUT (FORMAT csv)` for exports | One driver and one fast path in both directions. No ADBC. |
| CLI | **Typer** | One entry point: `sepsis <stage>` |
| Config | **pydantic-settings** + `.env` | No hard-coded credentials or paths. |
| Environment | **uv** (`uv lock` → `uv.lock`, installs into `.venv`) | Reproducible pins across 3 teammates. Python 3.13 fallback documented. |
| Tests | **pytest** (Python and end-to-end) + **dbt tests** (SQL) | |
| Lint/format | **ruff** | |

### 3.2 Database schemas (layers)

| Schema | Layer | Built by | Contents |
|---|---|---|---|
| `raw` | Bronze | Python | Exact copy of the source plus lineage columns, file manifest, quarantine |
| `staging` | Silver (views) | dbt | Renamed, typed, 1:1 with raw |
| `silver` | Silver | dbt | Patient split, validated (long view and wide table), imputed, featurized, labels |
| `gold` | Gold | dbt | Star schema: dimensions and facts |
| `mart` | Marts | dbt | ML feature table, reference stats, utility normalisers, OLAP materialized views |
| `audit` | Cross-cutting | Python + dbt hooks | ETL runs, dbt model runs, data-quality results |
| `ts` | Add-on | SQL scripts | TimescaleDB hypertable, columnstore, continuous aggregates |

dbt's `generate_schema_name` macro is overridden so models land in exactly these schema names (no `<target>_` prefix).

### 3.3 Repository layout (target)

```
PythonProject4/
├── README.md
├── pyproject.toml / uv.lock        # package + pinned deps + ruff/pytest config
├── docker-compose.yml              # warehouse service only
├── .env.example                    # DB creds, paths, worker counts, seeds
├── .gitignore / .gitattributes     # data/, exports/, .env, dbt target/; eol=lf for *.sql, *.sh
├── docs/
│   ├── PLAN.md                     # this file
│   ├── SETUP.md                    # environment setup guide
│   ├── data_profile.md             # generated in Stage 2
│   └── timescale_benchmark.md      # generated in Stage 8
├── src/sepsis_pipeline/
│   ├── cli.py                      # Typer: doctor, extract, load-bronze, profile, transform, export, timescale, run-all
│   ├── config.py · db.py · audit.py
│   ├── extract/  (s3_listing.py, download.py)
│   ├── bronze/   (schema.py [pandera], load.py)
│   ├── profiling/profile.py
│   ├── transform/dbt_runner.py     # dbtRunner + run_results → audit.dbt_model_run
│   ├── export/parquet.py
│   └── timescale/addon.py
├── dbt/sepsis_dw/
│   ├── dbt_project.yml · profiles.yml · packages.yml (dbt_utils)
│   ├── seeds/      variable_catalog.csv, icu_unit.csv
│   ├── macros/     generate_schema_name, clinical_variables, locf, rolling_stats, sepsis_utility
│   ├── models/     staging/  silver/  gold/  marts/
│   └── tests/      singular SQL tests
├── sql/
│   ├── init/00_schemas.sql         # schemas, roles, audit tables, extension (container init)
│   └── timescale/                  # 01_hypertable.sql … 04_benchmark.sql
├── tests/                          # pytest: unit, LOCF reference, leakage, lineage, utility, e2e on sample
├── data/                           # gitignored: raw/, bronze/
└── exports/                        # gitignored: versioned Parquet outputs
```

---

## 4. Cross-cutting conventions

### 4.1 Naming and keys
- Source columns are converted to snake_case: `HR → hr`, `O2Sat → o2sat`, `TroponinI → troponin_i`, `HospAdmTime → hosp_adm_time`, `ICULOS → iculos`, `SepsisLabel → sepsis_label`.
- `patient_id` is the filename stem (e.g., `p000001`).
- **Grain key:** `(patient_id, iculos)` is the natural key of every patient-hour table. Ordering is always by `iculos`, never by file row order.
- `row_idx` = `row_number() over (partition by patient_id order by iculos) − 1`. This is the 0-based row index the official challenge scoring uses.

### 4.2 Leakage contract (applies to every layer and every export)

Every column delivered to the DM team belongs to exactly one group:

| Prefix | Meaning | May be a model input? |
|---|---|---|
| keys (`patient_id`, `iculos`) | Identity | `iculos` only as `feat_iculos` |
| `feat_*` | Uses only the **same patient's current and past hours**, plus fixed constants | Yes |
| `meta_*` | Hospital, unit, split assignments | Not by default (DM decides; hospital is a likely shortcut) |
| `label_*` | Targets derived from `SepsisLabel` | **Never** |
| `outcome_*` | Whole-stay facts that are only known at the end: LOS, `is_sepsis_patient`, worst values, onset hour | **Never** |

How the contract is enforced:
1. **Structural separation.** Feature models (`int_patient_hour_clean` → `_imputed` → `_features`) never select `sepsis_label`. Labels and outcomes are built in separate models tagged `label`/`outcome` and joined only in the final mart. An `information_schema` test asserts that **none of the three feature models** has a column named `sepsis_label`, `label_%` or `outcome_%`.
2. **Lineage test (pytest on `target/manifest.json`).** It asserts the **exact parent sets**, so a feature model cannot quietly join staging (which contains `sepsis_label`):
   - `int_patient_hour_clean` ⊆ {`int_measurement_long`, `stg_patient_hour`, seeds}
   - `int_patient_hour_imputed` = {`int_patient_hour_clean`}
   - `int_patient_hour_features` ⊆ {`int_patient_hour_imputed`, `int_patient_hour_clean`}

   It also asserts that their upstream closure has no `label`/`outcome`-tagged node, and that their **compiled SQL** never mentions `sepsis_label`. `int_patient_hour_clean` may read staging only for the hour spine and static attributes; the compiled-SQL check covers it.
3. **Truncation test.** Rebuilding with a patient's history cut at hour *k* must give identical `feat_*` values for hours ≤ *k* (Stage 4).
4. **No population statistics in features.** Anything fitted across patients (scaling, median imputation) is the DM team's job, using `mart.train_reference_stats` for the matching split scheme (including per-CV-fold rows).
5. **Cleaning rules are fixed constants**, not fitted values. Valid ranges, unit rescaling and LOCF caps come from clinical references and data-format facts. The profile is used only as a **sanity check**, and wherever a profile statistic informs a decision, it is computed on **train-split patients only** (the split exists before profiling; see Stage 2).

### 4.3 Operations
- **Idempotency:** every stage can be re-run.
  - Extract is incremental: it uses the manifest diff.
  - Bronze is **truncate and reload**: it takes under a minute, and the source is a static snapshot.
  - dbt models are full-refresh.
- **Audit:**
  - Every CLI stage runs inside `audit.etl_run(run_id, stage, started_at, finished_at, status, rows_in, rows_out, error, git_sha, params jsonb)`.
  - dbt `run_results.json` is parsed into `audit.dbt_model_run`.
  - Profiling and DQ metrics go to `audit.dq_results(run_id, layer, check_name, variable, metric, value)`.
- **Sample mode:**
  - every stage accepts `--sample N`, which takes a subset **stratified by (hospital, is_sepsis_patient)**
  - the default is 400 patients, giving ≈ 28 septic patients
  - it's used for the dev loop and for the pytest e2e test

---

## 5. Stages

Each stage lists its **goal**, **inputs → outputs**, **steps**, **design decisions**, **quality checks**, and **done when**.

---

### Stage 0 — Foundation

**Goal:** an empty but working skeleton: DB up, package installable, CLI runs, audit tables exist.

**Steps**
1. `git init`, `.gitignore`, `.gitattributes`, `pyproject.toml` (package `sepsis_pipeline`, console script `sepsis`).
2. `uv lock` on Python 3.14. Confirm Windows wheels exist for the **whole** transitive tree (dbt brings in many). If anything fails, switch to 3.13 and record why.
3. `docker-compose.yml`:
   - service `warehouse` using `timescale/timescaledb:2.30.2-pg18`, with a named volume and port from `.env`
   - **`shm_size: 1g`** (the container default of 64 MB breaks parallel queries)
   - `sql/init/` mounted into `/docker-entrypoint-initdb.d/`
   - `command:` tuning: `shared_buffers=1GB`, `work_mem=64MB`, `maintenance_work_mem=512MB`, `max_parallel_workers_per_gather=2`
   - `work_mem` is allocated per sort/WindowAgg node, and window aggregation runs single-threaded, so we don't over-provision either setting
   - healthcheck with `pg_isready`
   - SETUP.md documents sizing WSL2 memory via `.wslconfig`
4. `sql/init/00_schemas.sql`:
   - schemas `raw, staging, silver, gold, mart, audit, ts`
   - `CREATE EXTENSION IF NOT EXISTS timescaledb;`
   - audit tables
   - roles `etl_writer` (owner) and `dm_reader` (login, `USAGE` on `gold`, `mart`)
   - table-level `SELECT` grants are issued by dbt (`+grants`), because dbt recreates tables on every run
5. `config.py`, `db.py`, `audit.py` (a context manager that writes an `etl_run` row on enter and exit, including failures).
6. dbt scaffold:
   - `profiles.yml` reads `env_var(...)`
   - `dbt_utils`
   - `generate_schema_name` override
   - `+grants: {select: ['dm_reader']}` for gold/mart
7. Typer CLI with stub commands.

**Done when:** `docker compose up -d` → `sepsis doctor` (checks: DB connection, timescaledb extension, `dbt debug`, writable folders) is all green.

---

### Stage 1 — Extract

**Goal:** download all 40,336 `.psv` files reliably, verifiably and resumably.

**Inputs → outputs:** PhysioNet S3 → `data/raw/training_setA|B/*.psv`, `data/raw/manifest.parquet`, `raw.file_manifest`.

**Steps**
1. **List:** paginate `GET https://physionet-open.s3.amazonaws.com/?list-type=2&prefix=challenge-2019/1.0.0/training/` with continuation tokens (~41 pages). Parse `Key`, `ETag`, `Size`, `LastModified`. Keep only `*.psv`.
2. **Diff:** compare the remote listing with the local manifest. A file needs downloading if it is missing, or if its size or MD5 differs from the remote values.
3. **Download:**
   - `httpx` with a bounded thread pool (default 16 workers)
   - stream each file to a temp name, compute its MD5, compare with the ETag, then atomically rename
   - retries with exponential backoff and jitter
   - `rich` progress bar
   - a multipart ETag (containing `-`) falls back to a size check and logs a warning
4. **Manifest:** one row per file: `patient_id, source_set, s3_key, etag, size_bytes, local_path, md5, downloaded_at, status`. Write it to Parquet and replace `raw.file_manifest` with it.

**Design decisions:** download per file in pure Python. That needs no extra tools on Windows, and gives us MD5 verification and the manifest for free. If S3 is unreachable, the stage fails clearly. There is no second mirror to maintain.

**Quality checks:** counts per set match the listing (20,336 / 20,000). 0 MD5 mismatches after retries.

**Done when:** a second run downloads 0 files and finishes in seconds. A deleted or corrupted file gets re-downloaded.

---

### Stage 2 — Bronze load, patient split, profiling

**Goal:** land all raw rows untouched. Fix the patient split **before** any data-driven decision. Then profile the data, so that the cleaning rules are checked against evidence.

**Inputs → outputs:** `data/raw/**/*.psv` → `data/bronze/patient_hour/source_set=A|B/*.parquet`, `raw.patient_hour`, `raw.quarantine`, `staging.stg_patient_hour`, `silver.patient_split`, `docs/data_profile.md`, `audit.dq_results`.

**Steps**
1. **Parse:**
   - `pl.scan_csv("data/raw/training_set*/*.psv", separator="|", null_values=["NaN"], schema=<41 × Float64>, include_file_paths="source_file")`, so a single lazy scan reads all 40k files
   - add the columns `patient_id`, `source_set`, `source_row_number` (`pl.int_range(pl.len()).over("source_file") + 1`, which is per file; `with_row_index` would be global), `file_md5` (joined from the manifest), `load_run_id`, `loaded_at`
2. **Validate (pandera, once on the concatenated frame):**
   - the 41 expected columns
   - `SepsisLabel ∈ {0,1}`, `Gender ∈ {0,1}`
   - `ICULOS ≥ 1` and non-null
   - **no float NaN left** (only NULLs). Postgres sorts NaN above every number, so `max()` would return NaN.
   - failures are mapped back to files via `source_file`. Failing files go to `raw.quarantine` (file, reason) and are excluded, never silently fixed.
3. **Write bronze Parquet** (partitioned by `source_set`, zstd).
4. **Load into Postgres:**
   - `TRUNCATE raw.patient_hour`, then `COPY FROM STDIN` (binary)
   - indexes after the load: **non-unique** `(patient_id, iculos)`, plus `source_set`
   - uniqueness is a silver test, so `COPY` never aborts on bad data
5. **Staging + split (dbt):**
   - `sepsis transform --select stg_patient_hour patient_split` builds the staging view and **`silver.patient_split`** (spec in Stage 6a)
   - the split only needs `patient_id`, `source_set` and `max(sepsis_label)`
   - it reads **`raw.patient_hour` directly** (as a dbt source), so it ignores `leakage_cutoff` and stays frozen in every build
6. **Profile (`sepsis profile`):**
   - per variable, overall / per hospital / **train-only**: null %, min/p0.1/p1/p5/median/p95/p99/p99.9/max
   - per patient: rows, `ICULOS` range, contiguity gaps, duplicates
   - label stats: septic patients, positive rows, patients positive from row 0, distribution of `first_positive_row_idx`
   - the checks from §2.2–2.3; distribution of rows by `iculos` (needed for Stage 8)
   - output: `docs/data_profile.md` plus metrics in `audit.dq_results`

**Design decisions**
- Bronze changes representation only (`NaN` → NULL). Values are never clipped or renamed.
- Bronze lives both as Parquet (portable) and as a DB table (for dbt).
- The split is created here, not in Stage 6, so every later decision can be made "on train only".

**Quality checks:** distinct patients = manifest minus quarantine. Row count = total data lines across files.

**Done when:** bronze is loaded, the split exists, `docs/data_profile.md` exists, and every item in §2.2–2.3 is marked **confirmed** or **violated → handling decided**.

---

### Stage 3 — Silver: staging and validation (dbt)

**Goal:** typed, renamed, clinically validated data in two shapes: **long** (one row per observation) and **wide** (one row per patient-hour).

**Models and seeds**

1. **`staging.stg_patient_hour`** (view, built in Stage 2)
   - 1:1 with raw
   - snake_case
   - casts: `gender` → smallint, `iculos` → int, `sepsis_label` → smallint
   - adds `row_idx`
   - honours the dbt var `leakage_cutoff` (see Stage 4 tests): when set, it drops hours `> k` for a fixed list of patients. In normal runs it is a no-op.
   - `patient_split` does not read this view (see Stage 2)

2. **Seed `variable_catalog.csv`**, one row per clinical variable (34). Example rows:

   | variable | source_column | category | subcategory | unit | valid_min | valid_max | normal_low | normal_high | locf_max_hours |
   |---|---|---|---|---|---|---|---|---|---|
   | hr | HR | vital | hemodynamic | bpm | 20 | 300 | 60 | 100 | 6 |
   | temp | Temp | vital | — | °C | 25 | 45 | 36 | 38 | 12 |
   | map | MAP | vital | hemodynamic | mmHg | 20 | 250 | 70 | 105 | 6 |
   | lactate | Lactate | lab | blood gas | mmol/L | 0.1 | 30 | 0.5 | 2.0 | 24 |
   | wbc | WBC | lab | hematology | 10³/µL | 0.1 | 300 | 4 | 12 | 48 |
   | platelets | Platelets | lab | hematology | 10³/µL | 1 | 2000 | 150 | 400 | 48 |

   - Ranges and caps are **clinical constants**, with a short justification and reference per row in `seeds.yml`. They are sanity-checked against the train-only profile (p0.1/p99.9) so we don't cut plausible tails.
   - **LOCF caps:** vitals 4–12 h, routine labs 24–48 h, rare labs up to 72 h.

3. **Macro `clinical_variables()`:** a static Jinja list of the 34 variable names (with `source_column`). Wide models are generated from it, so the dbt parser never needs DB access. A test checks that the macro list equals the seed's `variable` column.

4. **`silver.int_measurement_long`** (**view**)
   - unpivots `stg_patient_hour` via `CROSS JOIN LATERAL (VALUES ...)` (generated from `clinical_variables()`), keeping only non-null observations
   - joins the seed in SQL
   - adds `is_out_of_range`, `is_abnormal`, and the unit harmonisation that is justified as a data-format fact (e.g., `FiO2 > 1 AND ≤ 100` → divide by 100, with `was_rescaled = true`; only if profiling shows mixed units)
   - it's a view, so the ~10M rows are stored only once, in `gold.fact_measurement`

5. **`silver.int_patient_hour_clean`** (table, the wide patient-hour table)
   - built by **pivoting the long view**: `max(value) filter (where variable = 'hr' and not is_out_of_range) as hr`, … (generated from `clinical_variables()`)
   - left-joined onto the hour spine of `stg_patient_hour`, so every patient-hour exists even with no observations
   - also carries the static attributes (age, gender, unit, hosp_adm_time) and `row_idx`
   - **`sepsis_label` is excluded** by contract (§4.2)
   - index `(patient_id, iculos)`

6. **`silver.int_dq_rejections`** (view): every out-of-range value, with patient, hour, variable, value and rule. It's for reporting, and nothing is deleted.

**Design decisions**
- Outliers are **flagged in long format** (kept in `fact_measurement`) and **nulled in wide format** (the feature path).
- Ranges live in one place (the seed) and are applied in SQL joins. That avoids `run_query` in macros, which fails at parse time and on a fresh DB.
- Static attributes: take them from the first row. If profiling shows they vary within a patient, flag that patient (`static_attr_conflict`) and report it.

**Quality checks (dbt tests)**
- `dbt_utils.unique_combination_of_columns (patient_id, iculos)` on staging (**error**)
- `accepted_values` on `sepsis_label`, `gender`
- macro list == seed list
- `sepsis_label` absent from `int_patient_hour_clean` (`information_schema`)
- singular tests:
  - `iculos` contiguity per patient (warn, or error if Stage 2 decided so)
  - static attributes constant
  - out-of-range share per variable (warn > 1%, error > 5%)

**Done when:** `dbt build --select staging silver` is green, and the out-of-range report is reviewed.

---

### Stage 4 — Silver: imputation, features, labels (dbt)

**Goal:** causal, model-ready features and correctly defined labels, built in **separate** models.

#### 4a. Features (lineage: `int_patient_hour_clean` only)

1. **`silver.int_patient_hour_imputed`** (table). For each of the 34 variables, computed on the **cleaned** values:
   - `<var>_measured` (bool): observed this hour
   - `<var>_hours_since` (int, **uncapped**, NULL if never observed): staleness is itself informative
   - `<var>_last` (double): **capped LOCF**, the last observed value if `hours_since ≤ locf_max_hours`, else NULL
   - **Implementation: one shared window for all 34 variables**, so there's a single sort:
     ```sql
     -- window w as (partition by patient_id order by iculos rows between unbounded preceding and current row)
     max(array[iculos::float8, hr]) filter (where hr is not null) over w  as hr_obs   -- {last_seen_iculos, last_value}
     -- outer select:
     iculos - hr_obs[1]::int                                           as hr_hours_since,
     case when iculos - hr_obs[1] <= {{ cap('hr') }} then hr_obs[2] end as hr_last
     ```
     Arrays compare element by element, and `iculos` is unique per patient, so the max array is always the latest observation. The frame is a running one (from unbounded preceding), so `max` is computed incrementally.
   - Caps are inlined from the macro list at compile time (they are constants). A test checks they match the seed.
   - **No population-level imputation** in the warehouse.

2. **`silver.int_patient_hour_features`** (table). All features use the current and past hours only:
   - **Hemodynamic** (on `_last` values):
     - shock index `hr/sbp`
     - pulse pressure `sbp − dbp`
     - derived MAP `(sbp + 2·dbp)/3` when MAP is missing, with a `map_is_derived` flag
   - **Ratios:** `bun/creatinine`; SpO2/FiO2 ratio `o2sat / fio2` (Rice et al. 2007), used instead of SaO2, which is almost always missing
   - **SIRS count (0–4)** from `_last` values:
     - temp > 38 or < 36
     - hr > 90
     - resp > 20 or paco2 < 32
     - wbc > 12 or < 4
     - plus `sirs_components_available`
   - **qSOFA-partial (0–2):** resp ≥ 22, sbp ≤ 100. Mentation isn't in the data, which is documented.
   - **SOFA-partial:**
     - components: coagulation (platelets), liver (bilirubin_total), cardiovascular (MAP < 70 only; no vasopressor data), renal (creatinine), respiratory via SpO2/FiO2 thresholds
     - each component as a column, plus their sum and `sofa_components_available`
     - `delta_sofa_partial_24h` = current − **min over the prior 24 h**, a proxy for the Sepsis-3 ≥ 2 rise. Using the minimum is robust to LOCF NULLs.
   - **Rolling windows** over 7 vitals (`hr, o2sat, temp, sbp, map, resp, dbp`), computed on **observed** values:
     - named windows `w6` / `w24` = `(partition by patient_id order by iculos range between 5|23 preceding and current row)`
     - stats: `avg`, `min`, `max`, `stddev_samp`, `count`
     - `regr_slope(value, iculos)` **only on `w24`**: moving-frame float aggregates have no inverse transition, so each one costs O(frame) per row
   - **Deltas:** `<v>_last − lag(<v>_last, 1)` and `<v>_last − lag(<v>_last, 6)`
   - **Measurement intensity:** lab draws in the last 24 h, hours since any lab. The data dictionary flags these as **potential hospital/practice shortcuts**.
   - **Cumulative:** running count of abnormal values, running `max(lactate)`

#### 4b. Labels and outcomes (tagged `label` / `outcome`, never upstream of features)

3. **`silver.int_patient_sepsis`** (one row per patient, tag `outcome`):
   - `is_sepsis_patient`
   - `first_positive_row_idx`, `first_positive_iculos`
   - `t_sepsis_row_idx` = `first_positive_row_idx + 6` (the official `argmax(labels) − dt_optimal`)
   - `sepsis_onset_iculos`
   - `onset_left_censored` = positive at row 0. Given the "t_sepsis ≥ 4 h" exclusion, the onset estimate for these patients may be off by up to ~3 h. This bound is documented.
   - `onset_after_record_end` = `t_sepsis_row_idx > last_row_idx`
   - `last_iculos`, `icu_los_hours`

4. **`silver.int_patient_hour_labels`** (patient-hour, tag `label`):
   - `label_sepsis` (= `SepsisLabel`, the primary target, already 6 h early)
   - `label_hours_to_onset` = `t_sepsis_row_idx − row_idx` (NULL for non-septic patients), which supports other horizons
   - `label_is_sepsis_patient`
   - **Challenge utility** (macro `sepsis_utility`, ported from the official `evaluate_sepsis_score.py`):
     - constants verified against the script source (§2.1): `dt_early=−12, dt_optimal=−6, dt_late=3, max_u_tp=1, min_u_fn=−2, u_fp=−0.05, u_tn=0`
     - utility is 0 for septic rows after `t_sepsis + dt_late`
     - computed on **`row_idx`**, not `iculos`
     - output columns: `label_utility_if_positive`, `label_utility_if_negative`

**Quality checks**
- **LOCF reference test (pytest):** on hand-picked sample patients, the dbt `_last` / `_hours_since` columns equal a Polars reference implementation.
- **Cap test:** `<var>_last is not null ⇒ <var>_hours_since ≤ cap`.
- **Truncation leakage test (pytest):**
  - the cutoff patients are a **fixed list that is always included in the sample**, including septic patients cut **before** their onset
  - build the sample DB normally, then **snapshot** the cutoff patients' `feat_*` rows into a Polars frame
  - rebuild with `--vars '{leakage_cutoff: k}'`
  - assert that all `feat_*` columns are equal for hours ≤ k, within a float tolerance of 1e-9
  - `patient_split` is unaffected, because it reads raw
- **Lineage test (pytest):** the manifest closure of `int_patient_hour_features` has no `label`/`outcome` nodes.
- **Label tests:**
  - once positive, the label stays 1 until the last row
  - `row_idx = iculos − min(iculos)` holds (if not, the difference is documented and `row_idx` stays authoritative)
  - count and report `onset_after_record_end` and `onset_left_censored`
- **Utility test (pytest):** per-patient utility sums from our macro equal the official script's `compute_prediction_utility` on ~20 sample patients, for all-ones and all-zeros predictions.
- **Performance:** time the feature models on the sample, extrapolate, and record the result. The target is < 15 min on the full data. If it's over, materialize `int_patient_hour_imputed` with an index and split the rolling stats into their own model.

**Done when:** all of the above pass on the sample, then on the full data.

---

### Stage 5 — Gold: star schema (dbt)

**Goal:** a dimensional model for analysis (OLAP) and a stable, documented interface.

**Dimensions** (descriptive attributes only; measures live in facts)

| Table | Grain | Attributes |
|---|---|---|
| `gold.dim_patient` | patient | `patient_key` (int surrogate, `row_number()` over `patient_id`, deterministic under full refresh), `patient_id` (NK), `age`, `age_band` (bands fixed after profiling: `<18` if present, 18–44, 45–64, 65–79, 80+), `gender_label`, `hosp_adm_time_hours`, `static_attr_conflict` |
| `gold.dim_hospital` | hospital | `hospital_key`, `hospital_code` (A/B), `source_set`, description |
| `gold.dim_icu_unit` | unit | `unit_key`, `unit_code` (MICU / SICU / UNKNOWN; BOTH only if profiling finds it), from seed `icu_unit.csv` |
| `gold.dim_stay_hour` | hour of ICU stay | `stay_hour_key` = `iculos` (1–336), `day_of_stay`, `hour_of_stay_day` (0–23, *not* clock time), `stay_phase` (0–6 h, 6–24 h, 1–3 d, 3–7 d, 7 d+), `is_first_24h` |
| `gold.dim_variable` | clinical variable | `variable_key`, `variable`, `display_name`, `category`, `subcategory`, `unit`, ranges, `locf_max_hours` (from the seed) |

`dim_stay_hour` replaces a calendar date dimension, because the source has no absolute timestamps. The report explains this as a deliberate modelling choice. Split assignments are **not** in `dim_patient`; they stay in `silver.patient_split` / the mart, to keep the dimension free of ML concerns.

**Facts**

| Table | Grain | Type | Content |
|---|---|---|---|
| `gold.fact_patient_hour` | patient × ICU hour | **Periodic snapshot** | FKs: `patient_key`, `hospital_key`, `unit_key`, `stay_hour_key`. The 34 cleaned values, `n_vitals_measured`, `n_labs_measured`, `sirs_count`, `sofa_partial`, `label_sepsis`, `label_hours_to_onset` |
| `gold.fact_measurement` | single observation | **Transaction** (sparse, EAV-style) | FKs: `patient_key`, `stay_hour_key`, `variable_key`, `hospital_key`, `unit_key`. Plus `value`, `is_out_of_range`, `is_abnormal`, `was_rescaled` (all observations, outliers flagged) |
| `gold.fact_icu_stay` | patient stay | **Accumulating snapshot** (tag `outcome`) | FKs: `patient_key`, `hospital_key`, `unit_key`, plus **role-playing** `stay_hour` FKs as milestones: `admit_stay_hour_key`, `first_positive_stay_hour_key`, `onset_stay_hour_key`, `last_stay_hour_key`. Measures: `icu_los_hours`, `is_sepsis_patient`, `hours_admission_to_onset`, observation counts per category, worst values (`max_lactate`, `min_map`, `max_hr`, `max_sofa_partial`), `pct_hours_with_full_vitals` |

**Design decisions**
- **SCD:** a static snapshot means Type 1 everywhere. The report explains when Type 2 would apply, e.g. a patient's unit changing if data arrived incrementally.
- **Conformed dimensions:** `dim_patient`, `dim_hospital` and `dim_icu_unit` are shared by all three facts. `dim_stay_hour` is shared by the two hourly facts, and role-played by `fact_icu_stay`. That enables drill-across from stay outcomes to hourly trajectories.
- **Physical design:**
  - B-tree indexes on FK columns
  - composite `(patient_key, stay_hour_key)` on both hourly facts
  - no BRIN: rows are physically ordered by patient, so `stay_hour_key` cycles inside every block and a BRIN index would never prune. The report may include this as a short `EXPLAIN` lesson.
- **Constraints:**
  - PK/unique constraints on dimension keys (dbt `constraints` with contracts, or post-hooks), then FK post-hooks on facts
  - dbt rebuilds use `DROP … CASCADE`, so the docs state that **FKs are guaranteed only after a full `dbt build`**. `relationships` tests guarantee integrity regardless.

**Quality checks:** `relationships` tests on every FK. `unique`/`not_null` on surrogate keys. Reconciliation: `fact_patient_hour` rows = `int_patient_hour_clean` rows, and `fact_measurement` rows = `int_measurement_long` rows.

**Done when:** the star schema is built, the dbt docs lineage graph renders, and the reconciliation tests pass.

---

### Stage 6 — Marts: ML features and OLAP (dbt)

#### 6a. Data-mining mart

**`silver.patient_split`** (built in Stage 2, documented here). Deterministic and **patient-level**:
```sql
-- reads the raw source, not staging, so leakage_cutoff never changes the split
with p as (select patient_id, source_set, max("SepsisLabel") as is_sepsis from {{ source('raw', 'patient_hour') }} group by 1, 2),
r as (select p.*,
             row_number() over (partition by source_set, is_sepsis order by md5(patient_id || '{{ var("split_seed") }}')) as rn,
             count(*)     over (partition by source_set, is_sepsis) as n
      from p)
select patient_id, source_set,
       case when (rn - 1)::float8 / n < 0.70 then 'train'
            when (rn - 1)::float8 / n < 0.85 then 'validation'
            else 'test' end                                          as split_random,
       case when (rn - 1)::float8 / n < 0.70 then 1 + (rn - 1) % 5 end as cv_fold,      -- train patients only, stratified
       case when source_set = 'B' then 'test'
            when (rn - 1)::float8 / n < 0.70 then 'train'
            else 'validation' end                                      as split_hospital_holdout,  -- A: 70/30 train/val, B: test
       '{{ var("split_version") }}' as split_version, '{{ var("split_seed") }}' as split_seed
from r
```
- **`split_random` (70/15/15):** stratified by (hospital, septic).
- **`cv_fold` (1–5):** patient-level folds on train patients. This stops the DM team from using row-level K-fold, which leaks heavily because a patient's hourly rows are near-duplicates.
- **`split_hospital_holdout`:** train and validation on A, with 30% of A held out for tuning so nobody tunes on B. Test on B. A cross-hospital generalisation check. The challenge's hidden test sets included patients from A, B and an unseen hospital C, so this approximates the "new hospital" part.

**`mart.ml_features_patient_hour`**, one row per patient-hour:
- keys: `patient_id`, `iculos`
- `feat_*`: Stage 4a features plus static demographics (`feat_age`, `feat_gender`, `feat_iculos`, `feat_hosp_adm_time`)
- `meta_*`: `meta_hospital`, `meta_icu_unit`, `meta_split_random`, `meta_cv_fold`, `meta_split_hospital_holdout`
- `label_*`: from Stage 4b
- no `outcome_*` columns

**`mart.train_reference_stats`**, keyed by **`(split_scheme, feature)`**:
- `split_scheme = 'random'` uses train patients only
- `split_scheme = 'hospital_holdout'` uses hospital A train patients only
- `split_scheme = 'cv_fold_k'` (k = 1–5) uses train patients outside fold k, so cross-validation doesn't leak through scaling or imputation
- stats: mean, std, median, p1, p99, missing %, n
- **patient-hour weighted**, which is documented: long stays dominate
- one test per scheme proves that no held-out patient contributes

**`mart.utility_normalisers`**, keyed by **`(split_scheme, split)`**:
- `best_utility_total` and `inaction_utility_total`, computed as in the official script
- lets the DM team compute the normalised challenge score `(observed − inaction)/(best − inaction)`

#### 6b. OLAP / analytical views (materialized views in `mart`)

| View | Construct | Question it answers |
|---|---|---|
| `mv_sepsis_rate_cube` | `GROUP BY CUBE (hospital_code, unit_code, age_band, gender_label)` + `GROUPING()` labels | Where is sepsis most prevalent? Includes patients, septic patients, rate and median LOS. |
| `mv_vitals_by_stay_phase` | `GROUP BY ROLLUP (stay_phase, day_of_stay)` × septic flag | How do vitals evolve over the stay? |
| `mv_measurement_frequency` | `GROUPING SETS ((category), (category, variable), (hospital_code, category))` | How differently do the hospitals measure? |
| `mv_pre_onset_trajectory` | Vitals/labs by `label_hours_to_onset` bucket (−48 … +6) vs. a matched-`iculos` non-septic baseline | What does deterioration look like? This is the "story" chart for the report. |

**Done when:**
- the split proportions are within ±0.5% of target per stratum, and the folds are balanced
- the reference-stats and normaliser tests pass
- each OLAP view answers its documented question

---

### Stage 7 — Export and DM hand-off

**Goal:** a versioned, self-describing dataset the DM team can use without running the warehouse.

**Outputs:** `exports/v{split_version}_{YYYYMMDD}/`
- `ml_features_patient_hour/meta_split_random={train,validation,test}/part-*.parquet` (zstd)
- `train_reference_stats.parquet`, `utility_normalisers.parquet`, `patient_split.parquet`
- `icu_stay_outcomes.parquet`: `fact_icu_stay` with every measure renamed `outcome_*`, for analysis only
- `data_dictionary.csv` / `.md`, generated from the dbt `manifest.json` plus the seed, with:
  - column, dtype, **group** (key/feat/meta/label/outcome), description, source variable, derivation, LOCF cap
  - a `shortcut_risk` note on `feat_iculos`, measurement-intensity features and hospital-dependent missingness
- `manifest.json`: pipeline version, git SHA, dbt invocation id, row/patient counts per split and stratum, column list with dtypes, file checksums
- `LICENSE-DATA.md`: CC BY 4.0 (project page) + ODbL v1.0 (`LICENSE.txt`), DOI 10.13026/v64v-d857, and the Reyna et al. 2020 citation

**Steps:**
1. Stream psycopg `COPY (select …) TO STDOUT (FORMAT csv, HEADER)` to a temp CSV per split. This avoids Python row objects, which would make 1.55M × ~250 columns slow.
2. `pl.scan_csv(..., schema=<from information_schema>)` → `sink_parquet` (zstd).
3. Validate the round-trip (row counts, schema).
4. Write the manifest.

**Hand-off contract:**
- The prefixes from §4.2 are stable. Adding columns is non-breaking. Renaming or removing columns bumps the export version.
- **Access:**
  - (a) Parquet exports on a shared drive (primary)
  - (b) read-only `dm_reader` on the same network
  - (c) a local rebuild with `docker compose up -d && sepsis run-all`

**Done when:** a teammate loads `train` with `pl.read_parquet(...)` and gets the row count stated in the manifest.

---

### Stage 8 — TimescaleDB add-on (isolated)

**Goal:** show what TimescaleDB adds to clinical time-series data, with honest measurements. Nothing in Stages 0–7 depends on this stage.

**Steps (`sql/timescale/*.sql`, run by `sepsis timescale`)**
1. **Hypertable** `ts.measurement`
   - columns: `patient_key, variable_key, iculos, value, is_out_of_range`
   - time column named `iculos`
   - `SELECT create_hypertable('ts.measurement', by_range('iculos', 24));` uses an integer time dimension in hours
   - any unique index must include `iculos`
   - load with `INSERT … SELECT` from `gold.fact_measurement`
   - `set_integer_now_func('ts.measurement', 'ts.integer_now')`, where `ts.integer_now()` is a `STABLE` SQL function returning the constant **336** (the dataset's maximum stay)
2. **Columnstore** (2.x syntax):
   - `ALTER TABLE ts.measurement SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = 'variable_key', timescaledb.orderby = 'patient_key, iculos');`
   - then convert every chunk: `DO $$ DECLARE c regclass; BEGIN FOR c IN SELECT show_chunks('ts.measurement') LOOP CALL convert_to_columnstore(c); END LOOP; END $$;`
   - comparison: a second hypertable `ts.measurement_by_patient` with `segmentby = 'patient_key'`. Chunks that are already converted can't change segmentby. Explain why it compresses worse (tiny segments).
3. **Continuous aggregate** `ts.cagg_patient_6h`: `time_bucket(6, iculos)`, grouped by patient and variable, with `avg/min/max/count`. One full refresh.
4. **Gap-filling cross-check:**
   - `time_bucket_gapfill(1, iculos, start, finish)` + `locf(...)` per sample patient, with **per-patient bounds** (each patient's min..max `iculos`), so no rows are generated outside the stay
   - compare with dbt's `<var>_last` on rows where `<var>_hours_since ≤ cap`, and check that gapfill is non-null wherever dbt nulled a value for exceeding the cap. The two implementations validate each other.
5. **Benchmark** (`04_benchmark.sql` + Python runner; median of 5 runs of `EXPLAIN (ANALYZE, BUFFERS)`):
   - storage: `gold.fact_measurement` vs. hypertable, before and after columnstore; **chunk size skew** (most rows sit in the first 24–48 h)
   - Q1: one patient's full trajectory
   - Q2: early window, `iculos` 1–24, which scans most of the data, so little gain is expected
   - Q3: **late window**, `iculos` 168–336, where chunk exclusion should shine
   - Q4: 6-h bucket aggregates for all patients, raw GROUP BY vs. continuous aggregate
   - Q5: one variable across all patients (segment pruning)
   - output: `docs/timescale_benchmark.md`, which states plainly where Timescale helps and where it doesn't

**Done when:** the benchmark report is generated, and the gapfill cross-check matches.

---

### Stage 9 — Run-all, docs, tests, report material

1. **`sepsis run-all`:** extract → load-bronze (+ staging and split) → profile → transform (`dbt build`) → export. `--with-timescale` also runs Stage 8. It stops on the first failure, and the error is recorded in `audit.etl_run`.
2. **dbt docs:** descriptions for models and columns, plus an **exposure** declaring the DM team's dependency on `mart.ml_features_patient_hour`.
3. **pytest e2e:** the full pipeline on the 400-patient stratified sample, in a throwaway `sepsis_test` database. This includes the LOCF reference, truncation leakage, lineage and utility tests.
4. **Report material:**
   - architecture diagram
   - star-schema ERD
   - dbt lineage graph
   - DQ summary from `audit.dq_results`
   - OLAP queries with outputs
   - Timescale benchmark
   - a "leakage-safety by design" section (§4.2)

---

## 6. Milestones

| # | Milestone | Stages | Exit criterion |
|---|---|---|---|
| M0 | Skeleton | 0 | `sepsis doctor` green, `uv.lock` committed |
| M1 | Data on disk | 1 | 40,336 verified files and manifest |
| M2 | Bronze + split + profile | 2 | `raw.patient_hour` loaded, split exists, `data_profile.md` reviewed, §2.2–2.3 resolved |
| M3 | Clean silver | 3 | Seed finalised, staging/long/clean models green |
| M4 | Features + labels | 4 | All Stage 4 tests green; performance measured |
| M5 | Star schema | 5 | Gold built, reconciliation tests pass |
| M6 | Marts | 6 | ML mart, reference stats, normalisers, OLAP views |
| M7 | Hand-off | 7 | First export delivered to the DM team |
| M8 | Timescale | 8 | Benchmark report |
| M9 | Polish | 9 | `run-all`, e2e tests, docs, report material |

Develop on `--sample 400` during M2–M4. Switch to the full data at each milestone's exit.

---

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| A source assumption is wrong (non-unique or non-contiguous hours, varying static attributes, mixed units) | Stage 2 profiling plus explicit tests. Handling decisions are documented in `data_profile.md` before writing rules. |
| Valid ranges remove real extreme values | Clinical ranges, sanity-checked against train p0.1/p99.9. Outliers are flagged and kept in `fact_measurement`. Reject share is tested. |
| Leakage: future info, whole-stay outcomes, test statistics, row-level CV | §4.2 contract: prefixes, structural separation, lineage, truncation and reference-stat tests. Split created before profiling. Patient-level CV folds provided. |
| Shortcut features (`iculos`, measurement intensity, hospital) inflate DM results | Flagged in the data dictionary. The hospital-holdout split exposes them. |
| Window-heavy models are slow | One shared sort for LOCF. `regr_slope` only on 24 h. Timed on the sample first. Split models if needed. |
| A Windows wheel is missing on Python 3.14 | `uv lock` in Stage 0 checks the whole tree. Fall back to 3.13. |
| The DM team needs a different grain or horizon | `label_hours_to_onset` supports any horizon. Outcome exports support patient-level analysis. Adding a mart is cheap. |
| Timescale adds complexity | Isolated schema and stage. `run-all` doesn't need it. |

---

## 8. Open questions and possible extensions

**Open questions**
1. Final valid/normal ranges and LOCF caps per variable. Drafted in Stage 3 from clinical references, sanity-checked on the train profile.
2. Split ratio 70/15/15 + 5 CV folds: **confirm with the DM team before M2**, because the split is created early.

**Possible extensions (not planned)**
- Prefect/Dagster orchestration, a BI dashboard (e.g. Metabase on the `mart` views), incremental dbt models, a sqlfluff linter.

---

## 9. Review log

**Revision 2 (after independent review of revision 1).** Changes adopted:
- **Leakage fixes:**
  - outcome columns (LOS, `is_sepsis_patient`, worst values, onset) moved out of `dim_patient`, marked `outcome_*`, and excluded from the ML mart
  - the leakage contract extended to all layers and exports, and enforced by structural separation plus lineage and truncation tests
  - reference stats keyed by split scheme
  - the split moved before profiling; cleaning rules declared as clinical constants
- **Technical fixes:**
  - parse-safe range validation (pivot from the long view plus a static variable macro) instead of `run_query`
  - single-sort LOCF via the array-max trick
  - `shm_size` and memory sizing
  - BRIN index dropped
  - non-unique raw index
  - utility computed on row index
  - working designs for the leakage and label tests
  - `cv_fold` added
  - explicit split bucket formula
  - stratified `--sample`
- **Modelling fixes:**
  - role-playing stay-hour milestones on `fact_icu_stay` (accumulating snapshot)
  - `fact_patient_hour` reclassified as a periodic snapshot
  - `hour_of_stay_day` renamed
  - unit codes corrected
  - `dbt +grants`
  - FK caveat documented
- **Timescale fixes:**
  - columnstore syntax
  - per-patient gapfill bounds, compared against uncapped LOCF
  - late-window query and chunk-skew reporting
  - constant `STABLE` `integer_now`
- **Cuts:** incremental bronze, Prefect, fallback mirror, ADBC, pgAdmin, sqlfluff, crosstab, and the second physical copy of the long table.
- **Facts:** dual license (CC BY 4.0 / ODbL) and DOI verified. Hidden test hospitals (A, B, C) and dataset exclusion rules recorded as "confirm in profiling".

**Revision 3 (round-2 review: VERDICT AGREE).** Refinements adopted:
- Leakage enforcement tightened:
  - `information_schema` check on all three feature models
  - exact parent sets plus a compiled-SQL check in the lineage test
- Truncation test fixed: snapshot before the rebuild, fixed cutoff patients pinned in the sample, `patient_split` reads raw so it stays frozen
- Hospital-holdout scheme gets an A-only validation set
- `cv_fold_k` reference stats
- `split_version` quoted
- per-file `source_row_number`
- `unit_key` added to `fact_measurement` (so the conformed-dimension claim holds)
- CSV `COPY` export path
- explicit columnstore SQL, and a separate hypertable for the segmentby comparison
- gapfill compared on rows within the cap
- §2 facts verified against the PhysioNet page, the official scoring script, and Reyna et al. 2020: age coding, utility constants, hospitals

**Not adopted, or adopted differently:**
- *"A dbt test that no `feat_` column's lineage reaches label models"*: dbt core has no column-level lineage. It's implemented as a model-level lineage test on `manifest.json`, plus an `information_schema` check that `sepsis_label` is absent from the feature base table.

---

## 10. Implementation log

What changed while building, and why. Measurements are from the full dataset on the
development laptop (14 GB RAM, Windows 11, Docker/WSL 2).

| Area | Plan | As built | Reason |
|---|---|---|---|
| Bronze load | One lazy scan over all files | Streaming batches of 2,000 files, each validated, written as a Parquet part and COPYed inside one transaction | One scan over 40k files exhausted memory (and crashed the IDE). Batched: 59 s, 0.9 GB peak |
| Profiler | Load the bronze frame | Lazy, column-pruned Polars scans | 0.6 GB peak |
| Calcium | Range validation only | `is_unit_mismatch` flag for Calcium < 2 (ionized calcium in mmol/L, ~11% of values), excluded from the clean table | Found by the profile; would otherwise fail the out-of-range test (§2.4) |
| Feature table | Full wide table | `int_patient_hour_features` holds keys + derived columns only; the mart joins it with `int_patient_hour_imputed` | Avoids a second ~140-column copy of 1.55M rows |
| ML mart | Table | View (`mart.ml_features_patient_hour`), materialised once as Parquet by the export | Avoids a ~3 GB duplicate; read cost is a 4-way key join |
| Reference statistics | dbt model in SQL | Polars, in `sepsis export`, from the train Parquet, loaded into `mart.train_reference_stats` (a documented dbt source). Membership stays in dbt (`split_scheme_membership`) and is tested there | Postgres evaluates ~200 percentile aggregates of one GROUP BY concurrently, each with its own sort: the query drove the host to 0.4 GB free and was cancelled. Polars: 23 s, 0.8 GB |
| Export | One file per split | Batches of 2,500 patients, one Parquet part each; `meta_split_random` kept inside the files as well as in the folder name | One-shot CSV→Parquet of the ~280-column train split peaked at 3.7 GB; batched 1.2 GB |
| Postgres memory | `shared_buffers` 1 GB, `work_mem` 64 MB | 512 MB / 32 MB | Shares a 14 GB laptop with the IDE and browser |
| Constraints | Post-hook PKs/FKs | PK/unique names carry the dbt invocation id | dbt's rename-and-swap leaves the old copy's index names in place; fixed names collide on rebuild |
| Leakage-test patients | Chosen later | 9 fixed patients in `sepsis_pipeline/leakage.py` (both hospitals and outcomes, septic onset after the 30 h cut, one record starting at hour 2) | Needed by the truncation test and always included in `--sample` |
| TimescaleDB conversion | One `DO` block converting all chunks | `02_columnstore.sql` only enables the columnstore; chunks are converted one per transaction (`if_not_columnstore => true`), and `sepsis timescale --resume` continues from the current `ts.*` state | Two runs were stopped by host memory pressure (the WSL VM keeps page cache after writing 10.5M rows twice); resuming avoids redoing finished work |
| Multi-statement scripts | One `execute` per file | Scripts split on `-- @@ statement break` lines | Postgres runs a multi-statement string as one implicit transaction, which `refresh_continuous_aggregate` refuses |
| Split test | ±0.5 percentage points per stratum | Within one patient of 70/15/15 per stratum | ±0.5% is impossible in small sample strata (e2e test); on the full data one patient is < 0.1% |
| Windows launchers | `sepsis` console script | Also `python -m sepsis_pipeline`; dbt is started through `python.exe` | Windows Application Control blocks unsigned venv launchers |

Runtimes (full data): extract 8.4 min first run / 30 s re-verify; bronze 59 s; profile 23 s;
dbt staging+silver ≈ 70 s, imputation+features+labels ≈ 4.5 min, gold ≈ 2 min, marts ≈ 40 s;
export ≈ 2.2 min; TimescaleDB add-on ≈ 4 min (full rebuild). End-to-end test on a 409-patient sample: 70 s.

