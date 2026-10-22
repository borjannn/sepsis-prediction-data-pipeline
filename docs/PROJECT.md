# How the project works

This document explains what the pipeline does with the data, step by step, and why. It ends with what the data mining work can do now that the data is clean.

Medical terms and abbreviations link to the [reference section](#reference) at the end.

**Contents**

1. [The problem](#1-the-problem)
2. [The data](#2-the-data)
3. [From raw files to a mining-ready dataset](#3-from-raw-files-to-a-mining-ready-dataset)
4. [Collecting the data](#4-collecting-the-data)
5. [Profiling: what the data really looks like](#5-profiling-what-the-data-really-looks-like)
6. [Cleaning](#6-cleaning)
7. [Missing values and feature engineering](#7-missing-values-and-feature-engineering)
8. [Labels and the evaluation metric](#8-labels-and-the-evaluation-metric)
9. [Avoiding data leakage](#9-avoiding-data-leakage)
10. [Splits and reference statistics](#10-splits-and-reference-statistics)
11. [The final dataset](#11-the-final-dataset)
12. [Supporting pieces](#12-supporting-pieces)
13. [Tools used](#13-tools-used)
14. [Next steps: data mining on the clean data](#14-next-steps-data-mining-on-the-clean-data)
15. [Reference](#reference)

---

<a id="1-the-problem"></a>

## 1. The problem

[Sepsis](#sepsis) is the body's extreme reaction to an infection. It is a leading cause of death in hospitals, and every hour of delayed treatment raises mortality. A model that flags sepsis a few hours before it is clinically recognised would give doctors time to act.

As a data mining task, this is **binary classification on time series**: for every hour a patient spends in the [ICU](#icu), predict whether they will develop sepsis soon. A few properties make it harder than a textbook classification problem:

- **It's temporal.** A prediction at hour *t* may only use what was known up to hour *t*.
- **It's very imbalanced.** 7.3% of patients become septic, and only 1.8% of patient-hours carry a positive label.
- **Rows aren't independent.** One patient contributes dozens of nearly identical hourly rows.
- **Accuracy is the wrong metric.** The [challenge utility score](#challenge-utility-score) rewards early warnings and penalises late or missing ones.

The work is split in two:

| Part | What it does |
|---|---|
| **Data preparation (this repository)** | Turns raw hospital files into a clean, tested, leakage-safe dataset ready for modelling |
| **Data mining (next)** | Explores the dataset and builds models that predict sepsis early (see [§14](#14-next-steps-data-mining-on-the-clean-data)) |

The preparation is a reproducible pipeline: one command goes from nothing to the final dataset. Most of the effort went into making sure the dataset can't let a model **cheat**, for example by seeing the future (see [§9](#9-avoiding-data-leakage)).

---

<a id="2-the-data"></a>

## 2. The data

The input is the public [PhysioNet/CinC](#physionet--cinc) Challenge 2019 dataset (v1.0.0): **40,336 patients** from **2 hospitals** (A: 20,336, B: 20,000).

- **One file per patient** (`p000001.psv`, pipe-separated text, missing values written as `NaN`).
- **One row per hour** the patient spent in the ICU, 1,552,210 rows in total.
- **41 columns** ([full list](#the-41-columns-in-the-data)):
  - 8 [vital signs](#vital-signs): heart rate, blood pressure, temperature, …
  - 26 [lab tests](#lab-tests): blood results such as [lactate](#lactate) and white blood cell count
  - 6 demographic/admin columns: age, gender, ICU type, [ICULOS](#iculos) (hours since ICU admission)
  - 1 label: [SepsisLabel](#sepsislabel) (0/1 per hour)

```
HR|O2Sat|Temp|SBP|MAP|DBP|Resp|...|Age  |Gender|Unit1|Unit2|HospAdmTime|ICULOS|SepsisLabel
NaN|NaN  |NaN |NaN|NaN|NaN|NaN |...|63.71|0     |NaN  |NaN  |-15.65     |1     |0
73 |99   |NaN |109|79 |60 |16  |...|63.71|0     |NaN  |NaN  |-15.65     |2     |0
```

The dataset's creators applied a few rules (Reyna et al. 2020) that matter later:

- Stays with fewer than 8 hourly records were excluded, and records were cut at **2 weeks (336 h)**. So the length of a record is roughly the ICU [length of stay](#los), which is an outcome and must not be used as a feature.
- Patients whose sepsis began less than 4 h after ICU admission were excluded.
- The data comes from three hospital systems. Only A and B are public. The challenge's hidden test set also drew on a third, unseen hospital C.

**What makes it messy:**

- **Lots of missing data.** Vitals are measured roughly hourly, labs maybe once a day, so many lab columns are over 90% empty.
- **No patient ID inside the files** (only the filename), and **no real timestamps**, only "hour N of the ICU stay".
- **Wrong or mixed units**, found by profiling (see [§5](#5-profiling-what-the-data-really-looks-like)).
- **Hospitals measure differently.** For example, hospital A never records EtCO2.
- **No usable integrity file.** There's no ZIP, and the published checksums don't cover the training files. The S3 mirror's ETag does equal each file's MD5, so that's what downloads are verified against.

---

<a id="3-from-raw-files-to-a-mining-ready-dataset"></a>

## 3. From raw files to a mining-ready dataset

The pipeline follows the usual knowledge discovery (KDD) process. This repository covers the first three steps; the last two are the data mining work.

| KDD step | What happens here | Section |
|---|---|---|
| **Selection** | Download every patient file, verify it, load it unchanged into a database | [§4](#4-collecting-the-data) |
| **Preprocessing** | Profile the data, fix units, flag impossible values, handle missing data | [§5](#5-profiling-what-the-data-really-looks-like)–[§7](#7-missing-values-and-feature-engineering) |
| **Transformation** | Build 262 time-aware features, the labels, the splits and the final dataset | [§7](#7-missing-values-and-feature-engineering)–[§11](#11-the-final-dataset) |
| **Data mining** | Exploration, clustering, classification | [§14](#14-next-steps-data-mining-on-the-clean-data) |
| **Evaluation** | Patient-level validation with the challenge utility score | [§8](#8-labels-and-the-evaluation-metric), [§14](#14-next-steps-data-mining-on-the-clean-data) |

Inside the database the data moves through layers, each cleaner than the one before (a [medallion architecture](#medallion-architecture)). Each layer has its own PostgreSQL schema:

```
PhysioNet S3
    │
    ▼
EXTRACT ──► raw .psv files, MD5-verified             (Python, httpx)
    │
    ▼
raw      ──► bronze: exact copy + lineage columns     (Python, Polars, pandera)
    │
    ▼
staging  ──► renamed, typed                           (dbt / SQL)
silver   ──► split, validated, imputed, features, labels
    │
    ├──► gold   ──► star schema for analysis          (dbt / SQL)
    ▼
mart     ──► ML feature table, reference stats, OLAP views
    │
    ▼
EXPORT   ──► versioned Parquet files ──► data mining

ts     TimescaleDB add-on (optional, isolated)
audit  logs every run of every stage
```

Everything runs with one command: `python -m sepsis_pipeline run-all`. See [SETUP.md](SETUP.md).

---

<a id="4-collecting-the-data"></a>

## 4. Collecting the data

**Download.** The pipeline lists every file in PhysioNet's public S3 bucket and downloads all 40,336 of them in parallel (16 threads, with retries). Each file's [MD5 checksum](#md5--etag) is checked against the one S3 reports, so corrupted downloads are caught. A **manifest** records every file (path, size, checksum, status), which makes the step incremental: a second run downloads only what's missing or changed. The first run takes 8.4 minutes; a re-run takes 30 seconds and downloads nothing.

**Load.** All files are parsed with Polars in batches of 2,000. The only changes are `NaN` → `NULL` and three lineage columns (`patient_id` from the filename, hospital, source file). Each file is then validated with pandera (41 columns, label ∈ {0,1}, hour ≥ 1, …). A file that fails goes to a **quarantine** table and is never silently fixed; on the full data there were none. The rows are loaded into Postgres (`raw.patient_hour`) with `COPY` and also saved as Parquet. 1.55M rows load in 59 seconds.

---

<a id="5-profiling-what-the-data-really-looks-like"></a>

## 5. Profiling: what the data really looks like

Before any cleaning rule was written, the pipeline **fixed the train/validation/test split** (by patient) and then profiled the data: missingness, value distributions, and checks of every assumption the later steps rely on. The full report is [data_profile.md](data_profile.md).

The split comes first for a reason. Choosing cleaning rules by looking at *all* the data would let test-set information leak into the pipeline. So every statistic that informs a decision is computed on **training patients only**.

**What the profile confirmed:**

- `(patient_id, ICULOS)` is unique and the hours are contiguous: 0 duplicates, 0 gaps.
- Age, gender, ICU unit and `HospAdmTime` are constant within a patient.
- `SepsisLabel` never goes from 1 back to 0.
- Every stay has between 8 and 336 hourly rows.
- `Temp` is always in °C (median 37). The few implausible values (below 25 or above 45) are flagged.

**What it found, and how each finding is handled:**

| Finding | Handling |
|---|---|
| 21.8% of patients don't start at hour 1 | A 0-based `row_idx` is added for the challenge scoring; `iculos` stays the time-in-stay column |
| ~11% of calcium values are in a different unit ([ionized vs total](#ionized-vs-total-calcium)) | Flagged as a unit mismatch and excluded from the clean table |
| [FiO2](#fio2) sometimes stored as % (21–100) instead of a fraction (0.21–1.0) | Values in (1, 100] are rescaled; anything else outside 0.2–1.0 is flagged as out of range |
| Lactate and magnesium units differ from what the PhysioNet page lists | The observed units (mmol/L, mg/dL) are used; lactate's valid maximum is widened to 35 |
| `Unit1`/`Unit2` are MICU or SICU, or both missing (47% at A, 30% at B) | ICU unit becomes MICU / SICU / UNKNOWN |
| Youngest patient is 14; 22 are under 18; 392 are top-coded at 100 (age 90+) | An age band `<18` is added |
| Hospital A never measures EtCO2; BaseExcess, HCO3 and Chloride are ~99.8% missing at B; DBP is 48% missing at A vs 14% at B | Marked as a [shortcut risk](#shortcut-risk) in the data dictionary |
| 426 septic patients are positive from their first row; 15 have onset after their record ends | Flagged per stay (`onset_left_censored`, `onset_after_record_end`) |
| 57% of all rows fall in the first 24 hours of a stay, 91% in the first 48 | Shapes the TimescaleDB chunking ([§12](#12-supporting-pieces)) |

---

<a id="6-cleaning"></a>

## 6. Cleaning

Cleaning happens in SQL, with dbt:

1. **Staging.** Columns are renamed to `snake_case` (`HR → hr`, `TroponinI → troponin_i`, `ICULOS → iculos`) and given proper types.
2. **Wide → long.** The data is unpivoted to one row per actual measurement, giving **10.5M observations**. In this shape, a per-variable rule is a simple join.
3. **Validation.** Each observation is checked against a seed file, `variable_catalog.csv`, which holds clinical ranges for each of the 34 variables:

   | variable | unit | valid range | normal range | max forward-fill |
   |---|---|---|---|---|
   | hr | bpm | 20–300 | 60–100 | 6 h |
   | lactate | mmol/L | 0.1–35 | 0.5–2.0 | 24 h |

   Values outside the valid range are [physiologically implausible](#physiologically-implausible) (a heart rate of 0 or 400, for example) and are usually sensor or typing errors. Units are harmonised at the same time (the FiO2 rescaling, the calcium flag).
4. **Long → wide.** The data is pivoted back to one row per patient-hour, with the bad values set to NULL.

**Nothing is deleted.** The long table keeps every value with an `is_out_of_range` flag; only the clean wide table, which feeds the features, drops it. Data tests run on every build.

---

<a id="7-missing-values-and-feature-engineering"></a>

## 7. Missing values and feature engineering

Every feature is built from the patient's **current and past hours only**:

| Feature type | Example |
|---|---|
| **Capped forward-fill ([LOCF](#locf))** | Last heart rate seen, but only if it's at most 6 h old; otherwise NULL |
| **Missingness indicators** | `hr_measured`, `hr_hours_since`. *When* a test is ordered carries information too |
| **Clinical scores** | [SIRS](#sirs), partial [qSOFA](#qsofa) and [SOFA](#sofa), [shock index](#shock-index), [pulse pressure](#pulse-pressure), [SpO2/FiO2 ratio](#spo2fio2-ratio) |
| **Rolling windows** | Mean / min / max / std / count / slope of vitals over the last 6 h and 24 h |
| **Deltas** | Change since 1 h ago and since 6 h ago |
| **Cumulative** | Highest lactate so far, number of abnormal values so far, lab draws in the last 24 h |

Missing values are deliberately **not** filled with population statistics such as the training median. That kind of [imputation](#imputation) depends on which patients are in the training set, so it belongs in the modelling step (see [§10](#10-splits-and-reference-statistics)). The pipeline only does capped LOCF, which uses the patient's own history.

All of this is SQL window functions, for example `... OVER (PARTITION BY patient_id ORDER BY iculos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. All 34 variables share one window, so Postgres sorts the 1.55M rows only once.

The result is **262 feature columns** per patient-hour.

---

<a id="8-labels-and-the-evaluation-metric"></a>

## 8. Labels and the evaluation metric

Labels are built in **separate models** that the feature models never read:

| Column | Meaning |
|---|---|
| `label_sepsis` | **The target.** The official `SepsisLabel`: 1 from 6 h before [sepsis onset](#sepsis-onset) onwards |
| `label_is_sepsis_patient` | The patient becomes septic at some point during the stay |
| `label_hours_to_onset` | Hours until onset (NULL for non-septic patients) |
| `label_utility_if_positive` | Challenge utility earned by predicting 1 at this hour |
| `label_utility_if_negative` | Challenge utility earned by predicting 0 at this hour |

The two utility columns come from the official [challenge utility score](#challenge-utility-score). The organisers' Python scoring script was ported to SQL and checked against the original. Because the per-hour reward is already in the data, scoring a model is a sum (see [§14](#14-next-steps-data-mining-on-the-clean-data)). The normalising totals per split are in `utility_normalisers.parquet`.

---

<a id="9-avoiding-data-leakage"></a>

## 9. Avoiding data leakage

[Data leakage](#data-leakage) means a model gets information it wouldn't have in real life. Its scores look great and its real-world performance is poor. There are three ways it could happen here:

1. **Time leakage:** a feature at hour 10 uses data from hour 12.
2. **Patient leakage:** the same patient appears in both training and test data.
3. **Statistics leakage:** test data influences scaling or imputation.

**The column contract.** Every column in the final dataset belongs to exactly one group, marked by its prefix:

| Prefix | Meaning | Can be a model input? |
|---|---|---|
| keys (`patient_id`, `iculos`) | Identity | `iculos` only as `feat_iculos` |
| `feat_*` | Uses only the same patient's current and past hours, plus fixed constants | ✅ Yes |
| `meta_*` | Hospital, ICU type, split assignments | ⚠️ Not by default; hospital is a likely shortcut |
| `label_*` | The targets | ❌ Never |
| `outcome_*` | Facts known only at the end of the stay (length of stay, worst values, onset hour) | ❌ Never |

**How the contract is enforced:**

1. **Structural separation.** The feature models (`int_patient_hour_clean` → `_imputed` → `_features`) never select `sepsis_label`. Labels and outcomes are built in separate models tagged `label`/`outcome` and joined only in the final mart. A test asserts that none of the three feature models has a column named `sepsis_label`, `label_%` or `outcome_%`.
2. **Lineage test.** A pytest reads dbt's dependency graph (`target/manifest.json`) and checks the exact parents of each feature model, so a feature model can't quietly join staging, which contains `sepsis_label`. It also checks that no `label`/`outcome` model is upstream of a feature model and that the compiled SQL never mentions `sepsis_label`.
3. **Truncation test.** A fixed set of patients is rebuilt with their history cut at hour *k*. Features for hours ≤ *k* must be **identical** to the full build. If anything changed, a feature was looking into the future.
4. **No population statistics in features.** Anything fitted across patients (scaling, median imputation) is left to modelling, using the train-only reference statistics ([§10](#10-splits-and-reference-statistics)).
5. **Cleaning rules are fixed constants**, not fitted values. Valid ranges, unit rescaling and LOCF caps come from clinical references and data-format facts. The profile is only a sanity check, computed on training patients.

Split tests also check the split proportions and that reference statistics use training patients only.

---

<a id="10-splits-and-reference-statistics"></a>

## 10. Splits and reference statistics

**Splits are by patient.** All of a patient's hours go to the same split, because consecutive hours are near-duplicates: a row-level split would put almost the same row in training and test. The split is deterministic. Patients are ordered by `md5(patient_id || seed)` within each (hospital, septic) group and cut at fixed fractions, so the same seed always gives the same split.

Three split schemes are provided, all in `patient_split.parquet` and as `meta_*` columns:

| Scheme | Column | What it is for |
|---|---|---|
| Random 70 / 15 / 15 | `meta_split_random` | The main train / validation / test split, [stratified](#stratified-split) by hospital and sepsis status |
| 5 CV folds | `meta_cv_fold` | Patient-level [cross-validation](#cv) on the training patients (NULL for the others) |
| Hospital holdout | `meta_split_hospital_holdout` | Train and validate on hospital A (70/30), test on hospital B. Does a model generalise to a hospital it never saw? |

Patients per split:

| Hospital | Septic | Train | Validation | Test |
|---|---|---|---|---|
| A | no | 12,983 | 2,782 | 2,781 |
| A | yes | 1,253 | 269 | 268 |
| B | no | 13,201 | 2,829 | 2,828 |
| B | yes | 800 | 171 | 171 |

**Reference statistics** (`train_reference_stats.parquet`) hold the mean, std, median, 1st/99th percentile and missing fraction of every feature, computed on training patients only. There is one set per split scheme: `random`, `hospital_holdout` (hospital A training patients only) and `cv_fold_1`…`cv_fold_5` (training patients outside that fold). Use the matching set to scale or impute, and cross-validation stays leak-free. The statistics are weighted by patient-hour, so long stays count more.

---

<a id="11-the-final-dataset"></a>

## 11. The final dataset

The pipeline writes a versioned, self-describing folder, `exports/v1_20261001/`. Using it needs no Docker and no database.

| File | Contents |
|---|---|
| `ml_features_patient_hour/` | The modelling table, one Parquet folder per split: train 1,083,869 / validation 233,663 / test 234,678 rows |
| `train_reference_stats.parquet` | Train-only statistics per split scheme ([§10](#10-splits-and-reference-statistics)) |
| `utility_normalisers.parquet` | Best and do-nothing utility totals per split, for the normalised challenge score |
| `patient_split.parquet` | Split membership per patient |
| `icu_stay_outcomes.parquet` | One row per stay with `outcome_*` columns (length of stay, worst values, onset hour). For analysis only, never as features |
| `data_dictionary.md` / `.csv` | Every column: group, type, description, derivation, LOCF cap, shortcut-risk note |
| `manifest.json` | Row and patient counts per split, column list, git commit, file checksums |
| `LICENSE-DATA.md` | Data license and citation |

Loading the training split:

```python
import polars as pl

train = pl.read_parquet("exports/v1_20261001/ml_features_patient_hour/meta_split_random=train/")
```

The export streams rows out of Postgres with psycopg's `COPY ... TO STDOUT`, and Polars converts them to Parquet in batches to keep memory low.

---

<a id="12-supporting-pieces"></a>

## 12. Supporting pieces

These parts aren't needed to use the dataset, but they're useful for exploration and for trusting the result.

### The warehouse (star schema)

The gold layer is a [star schema](#star-schema) of the same data, designed for analytical queries:

```
                    dim_patient
                        │
  dim_hospital ── fact_patient_hour ── dim_stay_hour
                        │
                    dim_icu_unit           (+ dim_variable for fact_measurement)
```

| Fact table | One row per | Use |
|---|---|---|
| `fact_patient_hour` | patient × ICU hour | Hourly trajectories |
| `fact_measurement` | single measurement (10.5M, an [EAV](#eav) layout) | "How often is X measured?", outlier analysis |
| `fact_icu_stay` | patient stay | Outcomes: [length of stay](#los), septic or not, worst values |

There's no date dimension because the data has no real timestamps. `dim_stay_hour` (hour 1–336 of the stay, day of stay, stay phase) takes its place. Primary and foreign keys are enforced, and tests check that the facts reconcile with silver.

### Exploration views (OLAP)

Four [materialized views](#materialized-view) answer common exploratory questions quickly, using [OLAP](#olap) SQL:

| View | SQL construct | Question |
|---|---|---|
| `mv_sepsis_rate_cube` | `CUBE` | Sepsis rate by hospital × ICU type × age band × gender |
| `mv_vitals_by_stay_phase` | `ROLLUP` | How do vitals change over the stay? |
| `mv_measurement_frequency` | `GROUPING SETS` | Do the two hospitals measure differently? |
| `mv_pre_onset_trajectory` | buckets | What do the 48 h before sepsis look like, compared with non-septic patients at the same point in their stay? |

### TimescaleDB add-on

An optional, isolated stage that tests what a time-series database adds. Nothing else depends on it. The 10.5M measurements are copied into a [hypertable](#hypertable), split into 24-hour chunks of ICU time, and compressed into a columnstore. Compared with plain Postgres:

| Result | Number |
|---|---|
| Storage | 938 MB → **55 MB** (17× smaller) |
| Query on late hours (168–336) | **76× faster** (skips whole chunks) |
| Query on one variable, all patients | **12× faster** |
| One patient's full history | **46× slower** (the data is spread over many chunks) |

It's good for "time window" and "one variable" queries and worse for "one patient" queries, which is why it stays an add-on. A gap-filling cross-check against the pipeline's capped LOCF found 0 mismatches.

### Quality and operations

- **Tests:** dbt data tests (uniqueness, ranges, foreign keys, label rules) plus pytest (unit, leakage, lineage, end-to-end on a 400-patient sample).
- **Audit tables:** every run logs start and end time, status, row counts and git commit to `audit.etl_run`.
- **Re-runnable:** any stage can be run again safely.
- **Sample mode:** `--sample 400` runs the whole pipeline in about 70 seconds during development.

Full-data runtimes: extract 8.4 min (first time) · load 59 s · profile 23 s · dbt ≈ 8 min · export ≈ 2 min.

---

<a id="13-tools-used"></a>

## 13. Tools used

| Tool | Used for | Why this one |
|---|---|---|
| **Python 3.14** | Download, load, profiling, export, CLI | Glue for everything outside the database |
| **Polars** | Parsing 40k files, writing Parquet, computing statistics | Much faster and leaner than pandas at this volume |
| **httpx** | Parallel downloads from PhysioNet's S3 bucket | Simple HTTP client with connection pooling |
| **pandera** | Schema validation of the raw data | Catches bad files before they enter the database |
| **PostgreSQL 18** (in **Docker**) | Where the cleaning and feature engineering run | Window functions, `CUBE`/`ROLLUP`, materialized views |
| **dbt** | All SQL transformations, data tests, lineage graph | Versioned SQL models with built-in tests and docs |
| **psycopg 3** | Database driver; bulk `COPY` in and out | Fastest way to move millions of rows |
| **TimescaleDB** | Optional time-series add-on | Postgres extension for time-series data |
| **Typer** | The `sepsis` command-line tool | One entry point per stage |
| **uv**, **pytest**, **ruff** | Dependencies, tests, linting | Reproducible environment for the team |

---

<a id="14-next-steps-data-mining-on-the-clean-data"></a>

## 14. Next steps: data mining on the clean data

The data is now clean, validated, split by patient and turned into features. What follows is the data mining itself.

### Explore before modelling

- **Check the export.** Load each split and compare the row counts with `manifest.json`. Read `data_dictionary.md`, especially the shortcut-risk notes.
- **Describe the classes.** Compare septic and non-septic patients on the features: distributions, missingness, correlation and mutual information with `label_sepsis`. `mv_pre_onset_trajectory` already shows how vitals drift in the 48 h before onset.
- **Cluster the patients.** Aggregate each stay (for example the first 24 h of `feat_*_last` values and scores) and run k-means or hierarchical clustering. Do septic patients concentrate in particular clusters? Clusters like these can point to patient subgroups.
- **Look for patterns.** Discretise vitals and labs into normal / abnormal using the normal ranges in `variable_catalog.csv`, then mine association rules (Apriori, FP-growth) or frequent sequences in the hours before onset.

### Build predictive models

- **Start with baselines.** A rule baseline (SIRS ≥ 2, or partial qSOFA ≥ 2) and a logistic regression on a small set of features. Every later model should beat these.
- **Gradient-boosted trees** (LightGBM, XGBoost) are the natural next step: they handle NULLs natively, cope with hundreds of features and were among the strongest approaches in the original challenge. Random forests are a good comparison.
- **Sequence models** (GRU/LSTM, temporal convolutions) can learn from each patient's raw hourly sequence instead of hand-built rolling features.
- **Handle the imbalance** (1.8% positive hours) with class weights or by tuning the decision threshold on validation. If you resample, resample whole patients, never individual rows.
- **Other framings** of the same data: predict per patient from the first *N* hours (`label_is_sepsis_patient`), or predict the time to onset (`label_hours_to_onset`) as regression or survival analysis.

### Evaluate properly

- **Cross-validate by patient** with `meta_cv_fold`, never row-level K-fold. Scale and impute each fold with its `cv_fold_k` reference statistics.
- **Report several metrics:** AUROC, AUPRC (more informative with this imbalance) and the normalised challenge utility. Utility is a sum over hours:

  ```python
  import numpy as np
  import polars as pl

  def normalised_utility(df: pl.DataFrame, pred: np.ndarray, norms: pl.DataFrame,
                         scheme: str = "random", split: str = "validation") -> float:
      observed = np.where(pred == 1, df["label_utility_if_positive"], df["label_utility_if_negative"]).sum()
      n = norms.filter((pl.col("split_scheme") == scheme) & (pl.col("split") == split)).row(0, named=True)
      return (observed - n["inaction_utility_total"]) / (n["best_utility_total"] - n["inaction_utility_total"])
  ```

  1 means perfect, 0 means the same as never predicting sepsis.
- **Tune on validation, test once.** Choose features, hyperparameters and the threshold on validation (or CV). Touch the test split only for the final number.
- **Test generalisation** with the hospital holdout: train on A, test on B.

### Check what the model learned

- **Shortcut features.** Train with and without the measurement-intensity features (`_measured`, `_hours_since`, lab-draw counts), `feat_iculos` and `meta_hospital`. A big drop means the model was partly reading clinician behaviour or hospital habits rather than the patient's physiology.
- **Feature importance.** Permutation importance or SHAP values show which signals drive predictions, and whether they make clinical sense (lactate, blood pressure, heart rate, scores).

### Rules that keep results honest

- Only `feat_*` columns are model inputs. Never use `label_*` or `outcome_*` as inputs; `meta_*` only on purpose.
- Fit every scaler, imputer and encoder on training data, or use the matching reference statistics.
- Keep all of a patient's hours together in every split, fold and resample.

---

<a id="reference"></a>

## Reference

Short, non-clinical explanations, just enough to follow the project.

**Contents:** [Medical concepts](#medical-concepts) · [The 41 columns in the data](#the-41-columns-in-the-data) · [Clinical scores](#clinical-scores) · [Units](#units) · [Data terms](#data-terms)

<a id="medical-concepts"></a>

### Medical concepts

<a id="sepsis"></a>

#### Sepsis
A life-threatening condition where the body's response to an infection damages its own organs. It can quickly progress to septic shock (dangerously low blood pressure) and death. Treatment (antibiotics, fluids) works best when it starts early, which is why early prediction matters.

<a id="sepsis-onset"></a>

#### Sepsis onset
The hour at which a patient is considered to have developed sepsis (`t_sepsis`). The dataset's creators determined it with the [Sepsis-3](#sepsis-3) criteria: suspected infection plus a [SOFA](#sofa) rise of 2 or more points.

<a id="sepsislabel"></a>

#### SepsisLabel
The dataset's target column. For septic patients it is `1` from **6 hours before** [onset](#sepsis-onset) onwards, so a model that predicts the label is predicting sepsis 6 h early. For non-septic patients it is always `0`. About 7.3% of patients are septic.

<a id="icu"></a>

#### ICU
**Intensive Care Unit.** The hospital ward for the most critically ill patients, who are monitored continuously.

<a id="micu--sicu"></a>

#### MICU / SICU
**Medical ICU** (patients with medical illnesses, e.g. pneumonia) and **Surgical ICU** (patients after surgery). The `Unit1` and `Unit2` columns are one-hot flags for these two; for many patients both are missing.

<a id="iculos"></a>

#### ICULOS
**ICU Length Of Stay:** the number of hours since the patient was admitted to the ICU. The data has no real dates or clock times, so this is the only "time" column.

<a id="los"></a>

#### LOS
**Length Of Stay:** how long a patient stayed (in the ICU, in our case). It's only known **at the end** of the stay, so it must never be used as a model input.

<a id="hospadmtime"></a>

#### HospAdmTime
Hours between **hospital** admission and **ICU** admission. It's usually negative, because the patient was in the hospital before moving to the ICU.

<a id="vital-signs"></a>

#### Vital signs
Basic body measurements taken frequently, often automatically by bedside monitors: heart rate, blood pressure, temperature, breathing rate, oxygen level. In this data they are recorded roughly every hour.

<a id="lab-tests"></a>

#### Lab tests
Results of blood tests analysed in a laboratory (e.g. lactate, white blood cell count). They're taken much less often than vitals, maybe once or twice a day, so most hourly rows have no lab values.

<a id="lactate"></a>

#### Lactate
A substance that builds up in the blood when tissues don't get enough oxygen. A high lactate level is one of the strongest warning signs of sepsis and septic shock.

<a id="fio2"></a>

#### FiO2
**Fraction of inspired Oxygen:** the share of oxygen in the air the patient breathes. Room air is 0.21 (21%); a ventilator can give up to 1.0 (100%). Some rows stored it as a percentage (e.g. `40`) instead of a fraction (`0.40`), so the pipeline rescales those values.

<a id="spo2-and-sao2"></a>

#### SpO2 and SaO2
Two ways to measure how saturated the blood is with oxygen (normal is about 95–100%):
- **SpO2** comes from a **p**ulse oximeter, the clip on the finger. It's non-invasive and measured often. This is the `O2Sat` column.
- **SaO2** comes from an **a**rterial blood sample. It's more accurate, but rarely available in this data.

<a id="ionized-vs-total-calcium"></a>

#### Ionized vs total calcium
Calcium in the blood can be reported two ways:
- **total calcium**, in mg/dL (normal ≈ 8.5–10.5)
- **ionized (free) calcium**, in mmol/L (normal ≈ 1.1–1.3)

About 11% of the `Calcium` values are below 2. That means ionized calcium was recorded in the total-calcium column. Mixing them would corrupt the feature, so the pipeline flags these values.

<a id="physiologically-implausible"></a>

#### Physiologically implausible
A value that can't occur in a living patient, such as a heart rate of 0 or 400, or a temperature of 15 °C. These are usually sensor glitches or typing errors. The valid ranges per variable live in `dbt/sepsis_dw/seeds/variable_catalog.csv`.

<a id="sepsis-3"></a>

#### Sepsis-3
The current (2016) international definition of sepsis: a suspected infection together with an acute rise in the [SOFA](#sofa) score of 2 or more points.

<a id="physionet--cinc"></a>

#### PhysioNet / CinC
**PhysioNet** is a free, public repository of medical research data (run from MIT). **CinC** (Computing in Cardiology) is a conference that hosts a yearly data challenge with PhysioNet. The 2019 challenge was early sepsis prediction, and its dataset is our input.

<a id="the-41-columns-in-the-data"></a>

### The 41 columns in the data

<a id="vital-signs-8"></a>

#### Vital signs (8)

| Column | Full name | What it measures | Unit |
|---|---|---|---|
| `HR` | Heart rate | Heartbeats per minute | bpm |
| `O2Sat` | Oxygen saturation ([SpO2](#spo2-and-sao2)) | % of blood oxygen capacity in use, from a finger clip | % |
| `Temp` | Temperature | Body temperature | °C |
| `SBP` | Systolic blood pressure | Pressure in the arteries while the heart beats (the "top" number) | mmHg |
| `DBP` | Diastolic blood pressure | Pressure between heartbeats (the "bottom" number) | mmHg |
| `MAP` | Mean arterial pressure | Average pressure over one heartbeat, ≈ (SBP + 2·DBP) / 3. Below 70 suggests the organs aren't getting enough blood | mmHg |
| `Resp` | Respiratory rate | Breaths per minute | breaths/min |
| `EtCO2` | End-tidal CO2 | CO2 in the air at the end of a breath out. Hospital A never records it | mmHg |

<a id="lab-tests-26"></a>

#### Lab tests (26)

| Column | Full name | What it tells you |
|---|---|---|
| `BaseExcess` | Base excess | Acid-base balance of the blood |
| `HCO3` | Bicarbonate | Acid-base balance (the body's buffer) |
| `FiO2` | [Fraction of inspired oxygen](#fio2) | How much oxygen the patient is given |
| `pH` | pH | Blood acidity (normal ≈ 7.35–7.45) |
| `PaCO2` | Partial pressure of CO2 (arterial) | How well the lungs remove CO2 |
| `SaO2` | Arterial oxygen saturation ([SaO2](#spo2-and-sao2)) | Oxygen saturation from a blood sample |
| `AST` | Aspartate aminotransferase | Liver enzyme; high means liver damage |
| `BUN` | Blood urea nitrogen | Kidney function |
| `Alkalinephos` | Alkaline phosphatase | Liver/bone enzyme |
| `Calcium` | Calcium | Electrolyte (see [ionized vs total](#ionized-vs-total-calcium)) |
| `Chloride` | Chloride | Electrolyte |
| `Creatinine` | Creatinine | Kidney function; high means the kidneys are failing |
| `Bilirubin_direct` | Direct bilirubin | Liver function |
| `Bilirubin_total` | Total bilirubin | Liver function |
| `Glucose` | Glucose | Blood sugar |
| `Lactate` | [Lactate](#lactate) | Tissue oxygen shortage; key sepsis marker |
| `Magnesium` | Magnesium | Electrolyte |
| `Phosphate` | Phosphate | Electrolyte |
| `Potassium` | Potassium | Electrolyte (heart rhythm) |
| `TroponinI` | Troponin I | Heart muscle damage |
| `Hct` | Hematocrit | % of blood volume made of red blood cells |
| `Hgb` | Hemoglobin | Oxygen-carrying protein in red blood cells |
| `PTT` | Partial thromboplastin time | How long blood takes to clot |
| `WBC` | White blood cell count | Immune cells; too high or too low suggests infection |
| `Fibrinogen` | Fibrinogen | Clotting protein |
| `Platelets` | Platelets | Clotting cells; low means clotting is failing |

<a id="demographics-and-admin-6--label-1"></a>

#### Demographics and admin (6) + label (1)

| Column | Meaning |
|---|---|
| `Age` | Age in years; patients aged 90+ are recorded as **100** (for privacy) |
| `Gender` | 0 = female, 1 = male |
| `Unit1` | 1 if the patient is in the [MICU](#micu--sicu) |
| `Unit2` | 1 if the patient is in the [SICU](#micu--sicu) |
| `HospAdmTime` | [Hours from hospital admission to ICU admission](#hospadmtime) |
| `ICULOS` | [Hours since ICU admission](#iculos) |
| `SepsisLabel` | [The target](#sepsislabel) |

<a id="clinical-scores"></a>

### Clinical scores

These are standard bedside checklists that doctors use to judge how sick a patient is. The pipeline computes them as features.

<a id="sirs"></a>

#### SIRS
**Systemic Inflammatory Response Syndrome** criteria: an older sepsis screening rule. It counts how many of these 4 are true (0–4); 2 or more is a warning sign:
- temperature > 38 °C or < 36 °C
- heart rate > 90
- breathing rate > 20, or PaCO2 < 32
- white blood cell count > 12 or < 4

<a id="sofa"></a>

#### SOFA
**Sequential Organ Failure Assessment.** It scores 6 organ systems from 0 (normal) to 4 (failing): lungs, blood clotting, liver, circulation, brain and kidneys. The total is 0–24.
- Our version is **partial**, because the data has no brain assessment and no vasopressor (blood-pressure drug) data.
- It uses the [SpO2/FiO2 ratio](#spo2fio2-ratio) for the lungs.

<a id="qsofa"></a>

#### qSOFA
**quick SOFA:** a 3-question bedside check (0–3):
- breathing rate ≥ 22
- systolic BP ≤ 100
- altered mental state

Our version is **partial (0–2)**, because the data has no mental-state column.

<a id="shock-index"></a>

#### Shock index
`HR / SBP`. A value above about 1.0 suggests shock, meaning the heart is racing to make up for low blood pressure.

<a id="pulse-pressure"></a>

#### Pulse pressure
`SBP − DBP`: the difference between the top and bottom blood pressure numbers.

<a id="spo2fio2-ratio"></a>

#### SpO2/FiO2 ratio
`O2Sat / FiO2`: how well the lungs get oxygen into the blood, relative to how much oxygen the patient is given. Lower is worse. It's a well-studied substitute for the PaO2/FiO2 ratio, which needs blood samples we rarely have.

<a id="challenge-utility-score"></a>

#### Challenge utility score
The official scoring function of the PhysioNet 2019 challenge. Instead of plain accuracy, it scores every hourly prediction:
- **Positive reward** for predicting sepsis 12 to 3 hours before onset; the maximum is at 6 h before.
- **Penalty** for missing sepsis (up to −2).
- **Small penalty** (−0.05) for each false alarm.

The final score is normalised so that 1 = perfect and 0 = never predicting sepsis: `(observed − inaction) / (best − inaction)`.

<a id="units"></a>

### Units

| Unit | Meaning |
|---|---|
| bpm | beats per minute |
| mmHg | millimetres of mercury (pressure) |
| °C | degrees Celsius |
| mg/dL | milligrams per decilitre (concentration by mass) |
| mmol/L | millimoles per litre (concentration by number of molecules) |
| 10³/µL | thousands of cells per microlitre of blood |

<a id="data-terms"></a>

### Data terms

<a id="data-leakage"></a>

#### Data leakage
When training data contains information the model wouldn't have at prediction time (future values, test-set statistics, the same patient in train and test). It makes results look better than they really are.

<a id="shortcut-risk"></a>

#### Shortcut risk
A feature that predicts the label for the "wrong" reason. For example, doctors order more lab tests when they already suspect sepsis, so *how often* a patient is tested can reveal the label. Hospital-specific habits (hospital A never records EtCO2) are another example.

<a id="imputation"></a>

#### Imputation
Filling in missing values. LOCF is one method; filling with the training median is another.

<a id="locf"></a>

#### LOCF
**Last Observation Carried Forward:** filling a missing value with the most recent known one. "**Capped**" LOCF only carries a value forward for a limited time (e.g. 6 h for heart rate, 24 h for lactate), because an old measurement no longer reflects the patient's state.

<a id="stratified-split"></a>

#### Stratified split
Splitting data so each part keeps the same proportions of key groups (here: hospital × septic / non-septic).

<a id="cv"></a>

#### CV
**Cross-Validation:** training and evaluating a model on several different train/validation partitions (here, 5 patient-level folds).

<a id="etl"></a>

#### ETL
**Extract, Transform, Load:** pulling data from a source, cleaning and reshaping it, and loading it into a database.

<a id="medallion-architecture"></a>

#### Medallion architecture
Data layers that get progressively cleaner: **bronze** (raw copy), **silver** (cleaned and enriched), **gold** (modelled for analysis). **Marts** are purpose-built tables for one consumer.

<a id="star-schema"></a>

#### Star schema
A database design with one central **fact** table (measurable events, e.g. a patient-hour) linked to **dimension** tables (descriptive context, e.g. patient, hospital).

<a id="olap"></a>

#### OLAP
**Online Analytical Processing:** queries that summarise data across many dimensions. In SQL these use `CUBE`, `ROLLUP` and `GROUPING SETS` to compute many group-by combinations at once.

<a id="materialized-view"></a>

#### Materialized view
A saved query whose result is stored like a table, so reading it is fast.

<a id="eav"></a>

#### EAV
**Entity-Attribute-Value:** a "long" table layout with one row per (patient, variable, value) instead of one column per variable. `fact_measurement` uses it.

<a id="md5--etag"></a>

#### MD5 / ETag
MD5 is a checksum (a file fingerprint). S3 reports each file's MD5 as its **ETag**, so we can verify that a download wasn't corrupted.

<a id="psv"></a>

#### PSV
**Pipe-Separated Values:** like CSV, but columns are separated by `|`.

<a id="parquet"></a>

#### Parquet
A compressed, column-oriented file format for analytics, readable by Polars, pandas, Spark, etc.

<a id="dbt"></a>

#### dbt
**data build tool:** you write SQL `SELECT` statements as "models", and dbt builds them as tables or views in the right order, runs data tests, and draws a lineage graph.

<a id="hypertable"></a>

#### Hypertable
A TimescaleDB table that's automatically split into time-based chunks (here, 24 h of ICU time each), so queries on a time range can skip irrelevant chunks.
