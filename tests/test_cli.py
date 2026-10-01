from typer.testing import CliRunner

from sepsis_pipeline.cli import app

runner = CliRunner()


def test_help_lists_all_stages():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in (
        "doctor",
        "extract",
        "load-bronze",
        "profile",
        "transform",
        "export",
        "timescale",
        "run-all",
        "dbt",
    ):
        assert command in result.output


def test_run_all_offers_sample_and_timescale_options():
    result = runner.invoke(app, ["run-all", "--help"])

    assert result.exit_code == 0
    assert "--sample" in result.output
    assert "--with-timescale" in result.output
