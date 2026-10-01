"""Run dbt as a subprocess with the warehouse credentials injected from Settings.

A subprocess (instead of dbt's in-process dbtRunner) keeps dbt's global state out of the
pipeline process; run results are read back from target/run_results.json.

dbt is started through the current Python interpreter rather than the dbt.exe launcher,
so it works where Windows Application Control blocks unsigned venv launchers.
"""

import os
import subprocess
import sys
from collections.abc import Sequence

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
