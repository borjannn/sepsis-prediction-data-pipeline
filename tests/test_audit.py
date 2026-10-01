import pytest

from sepsis_pipeline.audit import etl_run

pytestmark = pytest.mark.db


def _fetch_run(conn, run_id):
    return conn.execute(
        "SELECT stage, status, rows_in, rows_out, error, finished_at, params"
        " FROM audit.etl_run WHERE run_id = %s",
        (run_id,),
    ).fetchone()


def test_successful_stage_is_recorded(db_conn):
    with etl_run("pytest_success", params={"sample": 5}) as run:
        run.rows_in, run.rows_out = 10, 7

    try:
        stage, status, rows_in, rows_out, error, finished_at, params = _fetch_run(
            db_conn, run.run_id
        )
        assert (stage, status, rows_in, rows_out, error) == (
            "pytest_success",
            "success",
            10,
            7,
            None,
        )
        assert finished_at is not None
        assert params == {"sample": 5}
    finally:
        db_conn.execute("DELETE FROM audit.etl_run WHERE run_id = %s", (run.run_id,))


def test_failed_stage_is_recorded_and_reraised(db_conn):
    with pytest.raises(RuntimeError, match="boom"), etl_run("pytest_failure") as run:
        raise RuntimeError("boom")

    try:
        _, status, _, _, error, finished_at, _ = _fetch_run(db_conn, run.run_id)
        assert status == "failed"
        assert error == "RuntimeError: boom"
        assert finished_at is not None
    finally:
        db_conn.execute("DELETE FROM audit.etl_run WHERE run_id = %s", (run.run_id,))
