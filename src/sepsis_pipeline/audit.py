"""Run auditing: every pipeline stage is recorded in audit.etl_run."""

import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from psycopg.types.json import Jsonb

from sepsis_pipeline.config import PROJECT_ROOT, Settings
from sepsis_pipeline.db import connect


@dataclass
class EtlRun:
    run_id: uuid.UUID
    stage: str
    rows_in: int | None = None
    rows_out: int | None = None
    params: dict[str, Any] = field(default_factory=dict)


def current_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


@contextmanager
def etl_run(
    stage: str,
    params: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> Iterator[EtlRun]:
    """Record a pipeline stage in audit.etl_run.

    A 'running' row is written on entry and updated to 'success' or 'failed' on exit.
    The audit connection is autocommit and separate from the stage's own connections,
    so the audit trail survives a failed stage.
    """
    params = params or {}
    with connect(settings, autocommit=True) as conn:
        run_id = conn.execute(
            """
            INSERT INTO audit.etl_run (stage, status, git_sha, params)
            VALUES (%s, 'running', %s, %s)
            RETURNING run_id
            """,
            (stage, current_git_sha(), Jsonb(params)),
        ).fetchone()[0]
        run = EtlRun(run_id=run_id, stage=stage, params=params)
        try:
            yield run
        except BaseException as exc:
            conn.execute(
                """
                UPDATE audit.etl_run
                SET status = 'failed', finished_at = now(), rows_in = %s, rows_out = %s,
                    error = %s
                WHERE run_id = %s
                """,
                (run.rows_in, run.rows_out, f"{type(exc).__name__}: {exc}", run_id),
            )
            raise
        conn.execute(
            """
            UPDATE audit.etl_run
            SET status = 'success', finished_at = now(), rows_in = %s, rows_out = %s
            WHERE run_id = %s
            """,
            (run.rows_in, run.rows_out, run_id),
        )
