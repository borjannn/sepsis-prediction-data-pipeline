from datetime import UTC, datetime

import polars as pl
import pytest
from psycopg import sql

from sepsis_pipeline.export.dictionary import build_dictionary, dictionary_markdown
from sepsis_pipeline.export.parquet import export_query, relation_columns
from sepsis_pipeline.timescale.addon import QUERIES, TimescaleReport, render_report

CATALOG = pl.DataFrame(
    {
        "variable": ["hr", "etco2"],
        "display_name": ["Heart rate", "End-tidal carbon dioxide"],
        "unit": ["beats/min", "mmHg"],
        "locf_max_hours": [6, 6],
    }
)


def test_dictionary_groups_describes_and_flags_columns():
    columns = [
        ("patient_id", "text"), ("iculos", "integer"), ("meta_hospital", "text"),
        ("feat_iculos", "integer"), ("feat_hr", "double precision"),
        ("feat_hr_measured", "boolean"), ("feat_hr_last", "double precision"),
        ("feat_etco2_last", "double precision"), ("feat_hr_mean_6h", "double precision"),
        ("feat_hr_slope_24h", "double precision"), ("feat_hr_delta_1h", "double precision"),
        ("feat_shock_index", "double precision"), ("label_sepsis", "smallint"),
    ]  # fmt: skip
    risk = {"etco2": "missing 100% at hospital A vs 93% at B"}

    d = {r["column"]: r for r in build_dictionary(columns, CATALOG, risk).iter_rows(named=True)}

    assert [d[c]["group"] for c in ("patient_id", "meta_hospital", "feat_hr", "label_sepsis")] == [
        "key", "meta", "feat", "label",
    ]  # fmt: skip
    assert d["feat_hr_last"]["locf_cap_hours"] == 6
    assert "LOCF" in d["feat_hr_last"]["derivation"]
    assert d["feat_hr_mean_6h"]["source_variable"] == "hr"
    assert "6h" in d["feat_hr_mean_6h"]["description"]
    assert d["feat_hr_slope_24h"]["description"].startswith("Linear trend")
    assert "measurement intensity" in d["feat_hr_measured"]["shortcut_risk"]
    assert "hospital A" in d["feat_etco2_last"]["shortcut_risk"]
    assert "time-in-stay" in d["feat_iculos"]["shortcut_risk"]
    assert all(r["description"] for r in d.values()), "every column must be described"
    assert "`feat_shock_index`" in dictionary_markdown(build_dictionary(columns, CATALOG, risk))


def test_benchmark_report_is_generated_from_measurements():
    report = TimescaleReport(generated_at=datetime.now(UTC))
    report.sizes = {
        "gold.fact_measurement (plain table)": 800 * 2**20,
        "ts.measurement (columnstore, segmentby variable)": 100 * 2**20,
        "ts.measurement_by_patient (columnstore, segmentby patient)": 300 * 2**20,
    }
    report.chunks = [(1, 900), (25, 100)]
    for qid, _, _ in QUERIES:
        report.timings[(qid, "plain table")] = 100.0
        report.timings[(qid, "hypertable (rowstore)")] = 100.0
        report.timings[(qid, "hypertable (columnstore)")] = 25.0
    report.timings[("Q4", "continuous aggregate")] = 5.0

    text = render_report(report)

    assert "8.0x smaller" in text
    assert "segmentby patient_key compresses worse" in text
    assert "columnstore is 4.0x faster than the plain table" in text
    assert "about the same as the plain table" in text
    assert "90.0%" in text  # chunk skew share


@pytest.mark.db
def test_export_query_round_trips_types(db_conn, tmp_path):
    db_conn.execute(
        "DROP SCHEMA IF EXISTS pytest_export CASCADE; CREATE SCHEMA pytest_export;"
        " CREATE TABLE pytest_export.t (id integer, x double precision, flag boolean, name text);"
        " INSERT INTO pytest_export.t VALUES (1, 1.5, true, 'a,b'), (2, NULL, false, NULL),"
        " (3, 2.25, NULL, 'c')"
    )
    try:
        columns = relation_columns(db_conn, "pytest_export", "t")
        query = sql.SQL("SELECT * FROM pytest_export.t ORDER BY id")
        rows = export_query(db_conn, query, columns, tmp_path / "t.parquet", tmp_path / "tmp")

        frame = pl.read_parquet(tmp_path / "t.parquet")
        assert rows == 3
        assert frame.schema == {
            "id": pl.Int32,
            "x": pl.Float64,
            "flag": pl.Boolean,
            "name": pl.String,
        }
        assert frame["flag"].to_list() == [True, False, None]
        assert frame["name"].to_list() == ["a,b", None, "c"]
        assert not list((tmp_path / "tmp").glob("*.csv")), "temp CSV must be removed"
    finally:
        db_conn.execute("DROP SCHEMA pytest_export CASCADE")


def test_reference_stats_use_only_scheme_members():
    from sepsis_pipeline.export.reference_stats import compute_reference_stats

    train = pl.DataFrame(
        {
            "patient_id": ["a", "a", "b", "c", "c"],
            "feat_x": [1.0, 3.0, 5.0, 1000.0, None],
            "feat_n": [1, 2, 3, 4, 5],
        }
    ).lazy()
    membership = pl.DataFrame(
        {"patient_id": ["a", "b", "a"], "split_scheme": ["random", "random", "cv_fold_1"]}
    )

    stats = compute_reference_stats(train, membership, ["feat_x", "feat_n"])
    row = stats.filter((pl.col("split_scheme") == "random") & (pl.col("feature") == "feat_x"))
    assert row["mean"].item() == pytest.approx(3.0)  # patient c (held out) excluded
    assert row["median"].item() == pytest.approx(3.0)
    assert row["std"].item() == pytest.approx(2.0)  # sample std of 1, 3, 5
    assert row["n_rows"].item() == 3 and row["n_patients"].item() == 2
    fold = stats.filter((pl.col("split_scheme") == "cv_fold_1") & (pl.col("feature") == "feat_x"))
    assert fold["mean"].item() == pytest.approx(2.0)
    assert stats.height == 4
