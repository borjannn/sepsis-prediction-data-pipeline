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


def test_unimplemented_stage_exits_with_code_2():
    result = runner.invoke(app, ["run-all"])

    assert result.exit_code == 2
    assert "Stage 9" in result.output
