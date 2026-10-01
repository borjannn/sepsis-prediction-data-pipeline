"""Run dbt as a subprocess with the warehouse credentials injected from Settings.

A subprocess (instead of dbt's in-process dbtRunner) keeps dbt's global state out of the
pipeline process; run results are read back from target/run_results.json.

dbt is started through the current Python interpreter rather than the dbt.exe launcher,
so it works where Windows Application Control blocks unsigned venv launchers.
"""

import json
import os
import subprocess
import sys
from collections.abc import Sequence

import psycopg

from sepsis_pipeline.config import Settings, get_settings

_DBT_ENTRY = "import sys; from dbt.cli.main import cli; sys.exit(cli())"


def run_dbt(
    args: Sequence[str],
    settings: Settings | None = None,
    *,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    settings = settings or get_settings()
    project_dir = str(settings.dbt_project_dir)
    command = [
        sys.executable,
        "-c",
        _DBT_ENTRY,
        *args,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        project_dir,
    ]
    return subprocess.run(
        command,
        env={**os.environ, **settings.dbt_env()},
        cwd=project_dir,
        capture_output=capture,
        text=True,
        check=False,
    )


def record_run_results(conn: psycopg.Connection, run_id, settings: Settings | None = None) -> int:
    """Load target/run_results.json of the last dbt invocation into audit.dbt_model_run."""
    settings = settings or get_settings()
    path = settings.dbt_project_dir / "target" / "run_results.json"
    results = json.loads(path.read_text(encoding="utf-8"))["results"]
    rows = [
        (
            run_id,
            r["unique_id"],
            r["status"],
            r.get("execution_time"),
            (r.get("adapter_response") or {}).get("rows_affected"),
            (r.get("message") or "")[:2000] or None,
        )
        for r in results
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO audit.dbt_model_run"
            " (run_id, unique_id, status, execution_time_s, rows_affected, message)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            rows,
        )
    return len(rows)
