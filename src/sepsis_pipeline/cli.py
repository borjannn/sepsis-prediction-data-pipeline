"""Command-line entry point: `sepsis <stage>`."""

from dataclasses import dataclass
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table

from sepsis_pipeline.audit import etl_run
from sepsis_pipeline.bronze.stage import run_load_bronze
from sepsis_pipeline.config import PROJECT_ROOT, get_settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.export.stage import run_export
from sepsis_pipeline.extract.stage import run_extract
from sepsis_pipeline.profiling.profile import (
    build_profile,
    load_inputs,
    render_markdown,
    write_dq_results,
)
from sepsis_pipeline.timescale.addon import run_timescale, write_report
from sepsis_pipeline.transform.dbt_runner import record_run_results, run_dbt

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


@app.command()
def extract(
    workers: int | None = typer.Option(None, help="Parallel downloads (default: EXTRACT_WORKERS)."),
) -> None:
    """Download and verify the ~40k patient files from PhysioNet (Stage 1)."""
    settings = get_settings()
    if workers is not None:
        settings = settings.model_copy(update={"extract_workers": workers})

    with (
        etl_run("extract", params={"workers": settings.extract_workers}) as run,
        Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress,
    ):
        task = progress.add_task("Listing PhysioNet S3 mirror", total=None)

        def on_listed(remote):
            progress.update(task, description="Verifying / downloading", total=len(remote))

        summary = run_extract(
            settings, on_listed=on_listed, on_result=lambda _: progress.advance(task)
        )
        run.rows_in = summary.remote_count
        run.rows_out = summary.remote_count - len(summary.failures)
        if not summary.ok:
            raise RuntimeError(f"{len(summary.failures)} files failed to download")

    counts = ", ".join(f"{k}: {v}" for k, v in sorted(summary.status_counts.items()))
    sets = ", ".join(f"{k}: {v}" for k, v in summary.per_set.items())
    console.print(
        f"[green]Extract complete[/green]: {summary.remote_count} files ({sets}); {counts}"
    )
    for warning in summary.warnings:
        console.print(f"[yellow]warning[/yellow]: {warning}")


SAMPLE_HELP = "Load only a stratified sample of N patients (dev loop). Default: all patients."


@app.command("load-bronze")
def load_bronze(
    sample: int | None = typer.Option(None, "--sample", help=SAMPLE_HELP),
) -> None:
    """Parse, validate and load the raw files into raw.* and bronze Parquet (Stage 2)."""
    settings = get_settings()
    with etl_run("load_bronze", params={"sample": sample}) as run, console.status("Loading bronze"):
        summary = run_load_bronze(settings, sample=sample)
        run.rows_in, run.rows_out = summary.files_in, summary.rows

    scope = (
        f"sample of {summary.patients} patients"
        if summary.sampled
        else f"{summary.patients} patients"
    )
    console.print(f"[green]Bronze loaded[/green]: {summary.rows:,} patient-hours, {scope}")
    if summary.quarantined.height:
        console.print(
            f"[yellow]{summary.quarantined.height} files quarantined[/yellow] (raw.quarantine):"
        )
        for row in summary.quarantined.head(10).iter_rows(named=True):
            console.print(f"  {row['source_file']}: {row['reason']}")
    else:
        console.print("No files quarantined.")


@app.command()
def profile(
    output: Path | None = typer.Option(None, help="Report path (default docs/data_profile.md)."),
) -> None:
    """Build staging + patient split, then profile bronze into docs/data_profile.md (Stage 2)."""
    settings = get_settings()
    with etl_run("profile") as run:
        console.print("Building staging view and patient split (dbt)...")
        result = run_dbt(["build", "--select", "stg_patient_hour", "patient_split"], settings)
        if result.returncode != 0:
            raise RuntimeError("dbt build of stg_patient_hour/patient_split failed")

        with console.status("Profiling bronze"), connect(settings) as conn:
            frame, split, quarantined = load_inputs(conn, settings.bronze_dir)
            prof = build_profile(frame, split, quarantined)
            write_dq_results(conn, run.run_id, prof)
        run.rows_in = prof.overview["patient_hours"]

    out = output or PROJECT_ROOT / "docs" / "data_profile.md"
    out.write_text(render_markdown(prof), encoding="utf-8")
    console.print(f"[green]Profile written[/green]: {out}")
    for check in prof.checks:
        mark = "[green]holds[/green]" if check.holds else "[yellow]violated[/yellow]"
        console.print(f"  {check.name}: {mark} ({check.observed})")
    for note in prof.notes:
        console.print(f"[yellow]note[/yellow]: {note}")


@app.command()
def transform(
    select: str | None = typer.Option(None, help="dbt --select expression (default: everything)."),
) -> None:
    """Build and test the dbt models: staging → silver → gold → marts (Stages 3–6)."""
    settings = get_settings()
    args = ["build"] + (["--select", select] if select else [])
    with etl_run("transform", params={"select": select}) as run:
        result = run_dbt(args, settings)
        with connect(settings) as conn:
            run.rows_out = record_run_results(conn, run.run_id, settings)
        if result.returncode != 0:
            raise RuntimeError("dbt build failed (details: audit.dbt_model_run, dbt logs)")
    console.print(f"[green]dbt build passed[/green]: {run.rows_out} nodes")


@app.command()
def export() -> None:
    """Export the ML mart to versioned Parquet for the Data Mining team (Stage 7)."""
    settings = get_settings()
    with etl_run("export") as run, console.status("Exporting Parquet"):
        summary = run_export(settings)
        run.rows_out = sum(summary.rows_per_split.values())
    rows = ", ".join(f"{k}: {v:,}" for k, v in summary.rows_per_split.items())
    console.print(f"[green]Export written[/green]: {summary.out_dir} ({summary.files} files)")
    console.print(f"  ml_features_patient_hour rows: {rows}")


@app.command()
def timescale() -> None:
    """Run the TimescaleDB add-on and its benchmark into docs/timescale_benchmark.md (Stage 8)."""
    settings = get_settings()
    with etl_run("timescale") as run, console.status("Running TimescaleDB add-on and benchmark"):
        report = run_timescale(settings)
        run.rows_out = sum(n for _, n in report.chunks)
    path = write_report(report)
    console.print(f"[green]Benchmark written[/green]: {path}")
    console.print(
        f"  gap-filling cross-check: {report.gapfill_rows_compared} hours compared, "
        f"{report.gapfill_mismatches} mismatches"
    )
    if report.gapfill_mismatches:
        raise typer.Exit(code=1)


@app.command("run-all")
def run_all(
    sample: int | None = typer.Option(None, "--sample", help=SAMPLE_HELP),
    with_timescale: bool = typer.Option(False, help="Also run the TimescaleDB add-on (Stage 8)."),
) -> None:
    """Run the whole pipeline end to end: extract → bronze → profile → dbt → export (Stage 9).

    Stops at the first failing stage; every stage is recorded in audit.etl_run.
    """
    profile_path = None
    if sample is not None:  # never overwrite the full-data profile with a sample profile
        profile_path = get_settings().data_dir / "data_profile_sample.md"
    steps = [
        ("extract", lambda: extract(workers=None)),
        ("load-bronze", lambda: load_bronze(sample=sample)),
        ("profile", lambda: profile(output=profile_path)),
        ("transform", lambda: transform(select=None)),
        ("export", export),
    ]
    if with_timescale:
        steps.append(("timescale", timescale))
    for name, step in steps:
        console.rule(f"[bold]{name}")
        step()
    console.rule("[green]pipeline complete")


if __name__ == "__main__":
    app()
