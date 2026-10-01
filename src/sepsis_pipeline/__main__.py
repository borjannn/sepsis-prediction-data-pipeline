"""Allow `python -m sepsis_pipeline`, e.g. where Windows Application Control blocks sepsis.exe."""

from sepsis_pipeline.cli import app

app(prog_name="sepsis")
