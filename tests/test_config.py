from pathlib import Path

from sepsis_pipeline.config import PROJECT_ROOT, Settings


def test_relative_paths_resolve_against_project_root(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DATA_DIR", "./somewhere/data")

    settings = Settings(_env_file=None)

    assert settings.data_dir == (PROJECT_ROOT / "somewhere" / "data").resolve()
    assert settings.dbt_project_dir == (PROJECT_ROOT / "dbt" / "sepsis_dw").resolve()


def test_absolute_paths_are_kept(monkeypatch, tmp_path):
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("EXPORT_DIR", str(tmp_path))

    assert Settings(_env_file=None).export_dir == Path(tmp_path)


def test_password_is_not_leaked_in_repr(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "super-secret")

    settings = Settings(_env_file=None)

    assert "super-secret" not in repr(settings)
    assert "password=super-secret" in settings.conninfo


def test_dbt_env_exposes_profile_variables(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_PORT", "5544")

    env = Settings(_env_file=None).dbt_env()

    assert env["POSTGRES_PORT"] == "5544"
    assert env["POSTGRES_PASSWORD"] == "secret"
    assert set(env) >= {"POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "DBT_PROFILES_DIR"}
