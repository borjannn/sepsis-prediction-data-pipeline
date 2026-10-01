from pathlib import Path

import polars as pl
import pytest

from sepsis_pipeline.bronze.db import BronzeWriter, write_bronze
from sepsis_pipeline.bronze.load import (
    iter_parsed_batches,
    parse_files,
    patient_label_status,
    select_sample_patients,
    stratified_sample,
)
from sepsis_pipeline.bronze.schema import EXPECTED_HEADER, SOURCE_COLUMNS


def _row(iculos: int, label: int = 0, hr: str = "NaN", gender: str = "1") -> str:
    values = dict.fromkeys(SOURCE_COLUMNS, "NaN")
    values.update(HR=hr, Age="61.5", Gender=gender, ICULOS=str(iculos), SepsisLabel=str(label))
    values["HospAdmTime"] = "-0.03"
    return "|".join(values[c] for c in SOURCE_COLUMNS)


def _write(raw: Path, rel: str, lines: list[str], header: str = EXPECTED_HEADER) -> None:
    path = raw / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([header, *lines]) + "\n", encoding="utf-8")


def _manifest(rels: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "patient_id": [Path(r).stem for r in rels],
            "source_set": ["A" if "setA" in r else "B" for r in rels],
            "local_path": rels,
            "md5": ["0" * 32] * len(rels),
            "status": ["downloaded"] * len(rels),
        }
    )


@pytest.fixture
def raw_dir(tmp_path):
    raw = tmp_path / "raw"
    _write(raw, "training_setA/p000001.psv", [_row(1, hr="80"), _row(2, hr="82.5")])
    _write(raw, "training_setA/p000002.psv", [_row(1), _row(2, label=1)])
    _write(raw, "training_setB/p100001.psv", [_row(1, hr="70")])
    # Invalid files, each for a different reason:
    _write(raw, "training_setB/p100002.psv", [_row(1)], header="HR|Wrong")
    _write(raw, "training_setB/p100003.psv", [_row(1, label=2)])
    _write(raw, "training_setB/p100004.psv", [_row(1, gender="5"), _row(2)])
    _write(raw, "training_setB/p100005.psv", [_row(1, hr="nan")])
    _write(raw, "training_setB/p100006.psv", [])  # header only
    return raw


def test_parse_accepts_valid_files_and_adds_lineage(raw_dir):
    rels = ["training_setA/p000001.psv", "training_setA/p000002.psv", "training_setB/p100001.psv"]
    parsed = parse_files(
        _manifest(rels), raw_dir, load_run_id="00000000-0000-0000-0000-000000000001"
    )

    frame = parsed.frame
    assert parsed.quarantine.height == 0
    assert frame.height == 5
    assert frame.columns[:41] == SOURCE_COLUMNS
    p1 = frame.filter(pl.col("patient_id") == "p000001")
    assert p1["HR"].to_list() == [80.0, 82.5]
    assert p1["source_row_number"].to_list() == [1, 2]
    assert p1["source_file"].to_list() == ["training_setA/p000001.psv"] * 2
    assert frame.filter(pl.col("patient_id") == "p000002")["HR"].null_count() == 2  # NaN -> NULL
    assert set(frame["source_set"]) == {"A", "B"}


def test_invalid_files_are_quarantined_not_fixed(raw_dir):
    rels = sorted(str(p.relative_to(raw_dir).as_posix()) for p in raw_dir.rglob("*.psv"))
    parsed = parse_files(
        _manifest(rels), raw_dir, load_run_id="00000000-0000-0000-0000-000000000001"
    )

    reasons = dict(parsed.quarantine.select("patient_id", "reason").iter_rows())
    assert set(reasons) == {"p100002", "p100003", "p100004", "p100005", "p100006"}
    assert reasons["p100002"].startswith("unexpected header")
    assert "SepsisLabel" in reasons["p100003"]
    assert "Gender" in reasons["p100004"]
    assert "NaN" in reasons["p100005"]
    assert reasons["p100006"] == "no data rows"
    assert set(parsed.frame["patient_id"]) == {"p000001", "p000002", "p100001"}


def test_failed_downloads_are_skipped(raw_dir):
    manifest = _manifest(["training_setA/p000001.psv", "training_setA/p000002.psv"])
    manifest = manifest.with_columns(status=pl.Series(["downloaded", "failed"]))

    parsed = parse_files(manifest, raw_dir, load_run_id="00000000-0000-0000-0000-000000000001")

    assert set(parsed.frame["patient_id"]) == {"p000001"}


def test_stratified_sample_is_deterministic_and_keeps_pinned_patients():
    rows = []
    for i in range(200):
        pid = f"p{i:06d}"
        septic = i % 10 == 0
        for hour in (1, 2):
            rows.append({"patient_id": pid, "source_set": "A" if i < 120 else "B",
                         "SepsisLabel": float(septic and hour == 2)})  # fmt: skip
    frame = pl.DataFrame(rows)

    a = stratified_sample(frame, 50, seed=42, always_include=("p000199",))
    b = stratified_sample(frame, 50, seed=42, always_include=("p000199",))

    patients = a.group_by("patient_id", "source_set").agg(s=pl.col("SepsisLabel").max())
    assert a.equals(b)
    assert "p000199" in set(patients["patient_id"])
    assert 49 <= patients.height <= 52
    assert patients["s"].sum() == pytest.approx(5, abs=1)  # ~10% septic, as in the population
    assert a.group_by("patient_id").len()["len"].unique().to_list() == [2]  # whole patients


@pytest.mark.db
def test_write_bronze_reloads_tables(db_conn, raw_dir):
    rels = sorted(str(p.relative_to(raw_dir).as_posix()) for p in raw_dir.rglob("*.psv"))
    run_id = "00000000-0000-0000-0000-000000000001"
    parsed = parse_files(_manifest(rels), raw_dir, load_run_id=run_id)

    db_conn.execute("DROP SCHEMA IF EXISTS pytest_raw CASCADE; CREATE SCHEMA pytest_raw")
    try:
        for _ in range(2):  # idempotent truncate-and-reload
            write_bronze(db_conn, parsed.frame, parsed.quarantine, run_id, schema="pytest_raw")
        rows, patients, hr = db_conn.execute(
            'SELECT count(*), count(DISTINCT patient_id), sum("HR") FROM pytest_raw.patient_hour'
        ).fetchone()
        assert (rows, patients, hr) == (5, 3, 80 + 82.5 + 70)
        assert db_conn.execute("SELECT count(*) FROM pytest_raw.quarantine").fetchone()[0] == 5
    finally:
        db_conn.execute("DROP SCHEMA pytest_raw CASCADE")


def _all_rels(raw_dir):
    return sorted(str(p.relative_to(raw_dir).as_posix()) for p in raw_dir.rglob("*.psv"))


def test_batched_parsing_matches_single_pass(raw_dir):
    manifest = _manifest(_all_rels(raw_dir))
    run_id = "00000000-0000-0000-0000-000000000001"

    single = parse_files(manifest, raw_dir, run_id)
    batches = list(iter_parsed_batches(manifest, raw_dir, run_id, batch_files=2))

    assert len(batches) == 4
    merged = pl.concat([b.frame for b in batches]).drop("loaded_at")
    assert merged.sort("patient_id", "source_row_number").equals(
        single.frame.drop("loaded_at").sort("patient_id", "source_row_number")
    )
    assert sum(b.quarantine.height for b in batches) == single.quarantine.height


def test_label_status_and_sample_selection(raw_dir):
    status = patient_label_status(_manifest(_all_rels(raw_dir)), raw_dir, batch_files=3)

    by_patient = dict(status.select("patient_id", "is_sepsis").iter_rows())
    assert by_patient["p000001"] == 0 and by_patient["p000002"] == 1
    assert select_sample_patients(status, 100, seed=1) == set(status["patient_id"])
    chosen = select_sample_patients(status, 2, seed=1, always_include=("p100001",))
    assert "p100001" in chosen


@pytest.mark.db
def test_failed_load_rolls_back_previous_bronze(db_conn, raw_dir):
    run_id = "00000000-0000-0000-0000-000000000001"
    parsed = parse_files(_manifest(_all_rels(raw_dir)), raw_dir, load_run_id=run_id)

    db_conn.execute("DROP SCHEMA IF EXISTS pytest_raw CASCADE; CREATE SCHEMA pytest_raw")
    try:
        write_bronze(db_conn, parsed.frame, parsed.quarantine, run_id, schema="pytest_raw")
        with (
            pytest.raises(RuntimeError, match="mid-load failure"),
            BronzeWriter(db_conn, run_id, schema="pytest_raw") as writer,
        ):
            writer.add_batch(parsed.frame.head(1), parsed.quarantine)
            raise RuntimeError("mid-load failure")
        rows = db_conn.execute("SELECT count(*) FROM pytest_raw.patient_hour").fetchone()[0]
        assert rows == parsed.frame.height  # the previous load survived intact
    finally:
        db_conn.execute("DROP SCHEMA pytest_raw CASCADE")
