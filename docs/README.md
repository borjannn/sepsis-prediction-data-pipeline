# Sepsis early prediction: data preparation

The goal is to predict **sepsis** in ICU patients a few hours before it is recognised, using the public [PhysioNet/CinC Challenge 2019](https://physionet.org/content/challenge-2019/1.0.0/) dataset: hourly records of 40,336 patients from two hospitals.

This repository does the data preparation for that data mining task. It downloads the raw patient files, profiles and cleans them, engineers 262 time-aware features and builds the labels. The output is a **versioned Parquet dataset** with patient-level train / validation / test splits, ready for modelling. The dataset is built so that a model can't cheat: no feature looks into the future, no patient appears in two splits, and no statistic is fitted on test data.

## Documents

| File | What it covers |
|---|---|
| [PROJECT.md](PROJECT.md) | How the project works: the data, each preparation step, the leakage rules, the final dataset, **next steps for data mining**, and a glossary of medical terms |
| [SETUP.md](SETUP.md) | Installing and running the pipeline, plus how to use the dataset without running it |
| [data_profile.md](data_profile.md) | Generated profile of the raw data: assumption checks, missingness and value distributions |

## Using the data

You don't need to run the pipeline to start modelling. Get the latest `exports/v*/` folder, read its `data_dictionary.md`, then:

```python
import polars as pl

train = pl.read_parquet("exports/v1_20261001/ml_features_patient_hour/meta_split_random=train/")
```

Use only `feat_*` columns as inputs, `label_sepsis` as the target, and `meta_cv_fold` for patient-level cross-validation. The rules are in [PROJECT.md §14](PROJECT.md#14-next-steps-data-mining-on-the-clean-data).

## Running the pipeline

```powershell
uv sync --extra dev
Copy-Item .env.example .env         # then set the two passwords
docker compose up -d
python -m sepsis_pipeline run-all   # download -> load -> profile -> clean + features -> export
```

Details and troubleshooting are in [SETUP.md](SETUP.md).
