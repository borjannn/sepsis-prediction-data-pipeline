"""Pipeline settings, read from environment variables and the project's .env file."""

from functools import lru_cache
from pathlib import Path

from psycopg.conninfo import make_conninfo
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# src/sepsis_pipeline/config.py -> project root (the package is installed in editable mode).
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "sepsis_dw"
    postgres_user: str = "etl_writer"
    postgres_password: SecretStr

    data_dir: Path = Path("data")
    export_dir: Path = Path("exports")
    extract_workers: int = 16
    split_seed: int = 42
    split_version: int = 1
    sample_size: int = 400

    dbt_project_dir: Path = Path("dbt/sepsis_dw")

    @field_validator("data_dir", "export_dir", "dbt_project_dir")
    @classmethod
    def _resolve_against_project_root(cls, path: Path) -> Path:
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()

    @property
    def conninfo(self) -> str:
        return make_conninfo(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            application_name="sepsis_pipeline",
        )

    def dbt_env(self) -> dict[str, str]:
        """Environment variables read by dbt/sepsis_dw/profiles.yml."""
        return {
            "POSTGRES_HOST": self.postgres_host,
            "POSTGRES_PORT": str(self.postgres_port),
            "POSTGRES_DB": self.postgres_db,
            "POSTGRES_USER": self.postgres_user,
            "POSTGRES_PASSWORD": self.postgres_password.get_secret_value(),
            "DBT_PROFILES_DIR": str(self.dbt_project_dir),
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
