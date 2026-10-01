"""End-to-end pipeline test on a stratified sample, in a throwaway database `sepsis_test`.

Includes the truncation leakage test (docs/PLAN.md, Stage 4): rebuilding with the fixed
leakage patients' histories cut at hour k must leave every feat_* value at hours <= k
unchanged, proving that no feature looks into the future.

Slow (a full dbt build on ~400 patients, twice): run with `pytest -m e2e`.
Needs the warehouse container and the downloaded raw files (`sepsis extract`).
"""

import json
import os

import polars as pl
import psycopg
import pytest
from psycopg import sql

from sepsis_pipeline.bronze.stage import run_load_bronze
from sepsis_pipeline.config import PROJECT_ROOT, Settings, get_settings
from sepsis_pipeline.export.stage import run_export
from sepsis_pipeline.leakage import LEAKAGE_CUTOFF_HOUR, LEAKAGE_TEST_PATIENTS
from sepsis_pipeline.transform.dbt_runner import run_dbt

pytestmark = pytest.mark.e2e

TEST_DB = "sepsis_test"
SAMPLE = 400


def _admin_connection(settings: Settings) -> psycopg.Connection:
    admin = settings.model_copy(update={"postgres_db": "postgres"})
    return psycopg.connect(admin.conninfo, autocommit=True)


@pytest.fixture(scope="module")
def e2e_settings(tmp_path_factory):
    base = get_settings()
    if not (base.raw_dir / "manifest.parquet").exists():
        pytest.skip("raw files not downloaded (run `sepsis extract`)")
    try:
        admin = _admin_connection(base)
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse not reachable: {exc}")

    tmp = tmp_path_factory.mktemp("e2e")
    settings = base.model_copy(
        update={
            "postgres_db": TEST_DB,
            "bronze_dir_override": tmp / "bronze",
            "raw_dir_override": base.raw_dir,
            "data_dir": tmp,
            "export_dir": tmp / "exports",
        }
    )
    with admin:
        admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB))
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    with psycopg.connect(settings.conninfo, autocommit=True) as conn:
        conn.execute((PROJECT_ROOT / "sql" / "init" / "10_schemas.sql").read_text(encoding="utf-8"))
        conn.execute("GRANT USAGE ON SCHEMA gold, mart TO dm_reader")

    # dbt reads credentials from the environment; point it at the test database.
    old_db = os.environ.get("POSTGRES_DB")
    os.environ["POSTGRES_DB"] = TEST_DB
    try:
        yield settings
    finally:
        if old_db is None:
            os.environ.pop("POSTGRES_DB", None)
        else:
            os.environ["POSTGRES_DB"] = old_db
        with _admin_connection(base) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB))
            )


def _dbt(settings: Settings, *args: str) -> None:
    result = run_dbt(list(args), settings, capture=True)
    assert result.returncode == 0, result.stdout[-4000:]


def _leakage_features(settings: Settings) -> pl.DataFrame:
    with psycopg.connect(settings.conninfo) as conn:
        cur = conn.execute(
            "SELECT * FROM mart.ml_features_patient_hour"
            " WHERE patient_id = ANY(%s) AND iculos <= %s ORDER BY patient_id, iculos",
            (list(LEAKAGE_TEST_PATIENTS), LEAKAGE_CUTOFF_HOUR),
        )
        columns = [d.name for d in cur.description]
        frame = pl.DataFrame(cur.fetchall(), schema=columns, orient="row", infer_schema_length=None)
    feature_columns = [c for c in columns if c.startswith("feat_")]
    return frame.select("patient_id", "iculos", *feature_columns)


def test_pipeline_end_to_end_with_truncation_leakage_check(e2e_settings):
    settings = e2e_settings

    # Bronze: stratified sample plus the pinned leakage-test patients.
    summary = run_load_bronze(settings, sample=SAMPLE)
    assert SAMPLE <= summary.patients <= SAMPLE + len(LEAKAGE_TEST_PATIENTS) + 5
    assert summary.quarantined.height == 0

    # Full dbt build (all models and data tests) on the sample.
    _dbt(settings, "build")

    # Truncation leakage test.
    before = _leakage_features(settings)
    assert set(before["patient_id"]) == set(LEAKAGE_TEST_PATIENTS)
    vars_json = json.dumps(
        {"leakage_cutoff": LEAKAGE_CUTOFF_HOUR, "leakage_patients": list(LEAKAGE_TEST_PATIENTS)}
    )
    _dbt(settings, "run", "--select", "stg_patient_hour+", "--vars", vars_json)
    after = _leakage_features(settings)
    assert before.shape == after.shape
    for column in before.columns:
        left, right = before[column], after[column]
        if left.dtype.is_float():
            diff = (left - right).abs()
            same = ((diff <= 1e-9) | (left.is_null() & right.is_null())).all()
        else:
            same = left.equals(right)
        assert same, f"{column} changed when future hours were removed (leakage)"

    # Restore the untruncated build, then export and check the hand-off package.
    _dbt(settings, "run", "--select", "stg_patient_hour+")
    export = run_export(settings)
    manifest = json.loads((export.out_dir / "manifest.json").read_text(encoding="utf-8"))
    train = pl.read_parquet(export.out_dir / "ml_features_patient_hour" / "meta_split_random=train")
    assert train.height == manifest["rows_per_split"]["train"] > 0
    assert (train["meta_split_random"] == "train").all()
    assert not [c for c in train.columns if c.startswith("outcome_")]
    for name in (
        "train_reference_stats",
        "utility_normalisers",
        "patient_split",
        "icu_stay_outcomes",
    ):
        assert (export.out_dir / f"{name}.parquet").exists()
    assert (export.out_dir / "data_dictionary.md").exists()
    assert set(manifest["files"]) >= {"data_dictionary.csv", "LICENSE-DATA.md"}
