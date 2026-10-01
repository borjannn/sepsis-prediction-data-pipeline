"""Command-line entry point: `sepsis <stage>`."""

from dataclasses import dataclass

import typer
from rich.console import Console
from rich.table import Table

from sepsis_pipeline.config import PROJECT_ROOT, get_settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.transform.dbt_runner import run_dbt

app = typer.Typer(
    help="ETL pipeline for the PhysioNet/CinC 2019 sepsis data warehouse.",
    no_args_is_help=True,
)
console = Console()

EXPECTED_SCHEMAS = ("raw", "staging", "silver", "gold", "mart", "audit", "ts")
EXPECTED_AUDIT_TABLES = ("etl_run", "dbt_model_run", "dq_results")


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def _database_checks() -> list[Check]:
    settings = get_settings()
    try:
        conn = connect(settings)
    except Exception as exc:  # noqa: BLE001 - report any connection failure
        return [Check("Database connection", False, f"{type(exc).__name__}: {exc}".strip())]

    checks = []
    with conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        checks.append(
            Check(
                "Database connection",
                True,
                f"PostgreSQL {version} at {settings.postgres_host}:{settings.postgres_port}"
                f"/{settings.postgres_db}",
            )
        )

        row = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone()
        checks.append(
            Check("TimescaleDB extension", row is not None, row[0] if row else "not installed")
        )

        present = {
            r[0]
            for r in conn.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)",
                (list(EXPECTED_SCHEMAS),),
            )
        }
        missing = [s for s in EXPECTED_SCHEMAS if s not in present]
        checks.append(
            Check(
                "Layer schemas",
                not missing,
                ", ".join(EXPECTED_SCHEMAS) if not missing else f"missing: {', '.join(missing)}",
            )
        )

        present = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'audit'"
            )
        }
        missing = [t for t in EXPECTED_AUDIT_TABLES if t not in present]
        checks.append(
            Check(
                "Audit tables",
                not missing,
                ", ".join(EXPECTED_AUDIT_TABLES)
                if not missing
                else f"missing: {', '.join(missing)}",
            )
        )

        row = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = 'dm_reader'").fetchone()
        checks.append(
            Check("Read-only role dm_reader", row is not None, "exists" if row else "missing")
        )
    return checks


def _dbt_check() -> Check:
    try:
        result = run_dbt(["debug"], capture=True)
    except FileNotFoundError as exc:
        return Check("dbt debug", False, str(exc))
    if result.returncode == 0:
        return Check("dbt debug", True, "all checks passed")
    errors = [line for line in result.stdout.splitlines() if "ERROR" in line or "FAIL" in line]
    return Check("dbt debug", False, "; ".join(errors[-3:]) or "failed (run `sepsis dbt debug`)")


def _directory_checks() -> list[Check]:
    settings = get_settings()
    checks = []
    for label, path in (
        ("Data directory", settings.data_dir),
        ("Export directory", settings.export_dir),
    ):
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write_test"
            probe.write_text("ok")
            probe.unlink()
            checks.append(Check(label, True, str(path)))
        except OSError as exc:
            checks.append(Check(label, False, f"{path}: {exc}"))
    return checks


@app.command()
def doctor() -> None:
    """Check that the environment is ready: database, extension, schemas, dbt, folders."""
    env_file = PROJECT_ROOT / ".env"
    checks = [
        Check(
            ".env file",
            env_file.exists(),
            str(env_file) if env_file.exists() else "missing; copy .env.example",
        )
    ]
    if checks[0].ok:
        checks += _database_checks()
        checks.append(_dbt_check())
        checks += _directory_checks()

    table = Table(title="sepsis doctor", show_lines=False)
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail", overflow="fold")
    for check in checks:
        table.add_row(
            check.name, "[green]OK[/green]" if check.ok else "[red]FAIL[/red]", check.detail
        )
    console.print(table)

    if not all(c.ok for c in checks):
        raise typer.Exit(code=1)


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def dbt(ctx: typer.Context) -> None:
    """Run any dbt command against the warehouse, e.g. `sepsis dbt build --select staging`."""
    result = run_dbt(ctx.args)
    raise typer.Exit(code=result.returncode)


def _not_implemented(stage: int) -> None:
    console.print(f"[yellow]Not implemented yet[/yellow] (Stage {stage} in docs/PLAN.md).")
    raise typer.Exit(code=2)


@app.command()
def extract() -> None:
    """Download and verify the ~40k patient files from PhysioNet (Stage 1)."""
    _not_implemented(1)


@app.command("load-bronze")
def load_bronze() -> None:
    """Parse, validate and load the raw files into raw.* (Stage 2)."""
    _not_implemented(2)


@app.command()
def profile() -> None:
    """Profile the bronze data and write docs/data_profile.md (Stage 2)."""
    _not_implemented(2)


@app.command()
def transform() -> None:
    """Build and test the dbt models: staging → silver → gold → marts (Stages 3–6)."""
    _not_implemented(3)


@app.command()
def export() -> None:
    """Export the ML mart to versioned Parquet for the Data Mining team (Stage 7)."""
    _not_implemented(7)


@app.command()
def timescale() -> None:
    """Run the TimescaleDB add-on and its benchmark (Stage 8)."""
    _not_implemented(8)


@app.command("run-all")
def run_all() -> None:
    """Run the whole pipeline end to end (Stage 9)."""
    _not_implemented(9)


if __name__ == "__main__":
    app()
