import pytest

from sepsis_pipeline.config import get_settings
from sepsis_pipeline.db import connect


@pytest.fixture(autouse=True)
def _fresh_settings():
    """Settings are cached; clear the cache so tests that change env vars see their changes."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def db_conn():
    """Autocommit connection to the warehouse; skips the test if the database is not running."""
    try:
        conn = connect(autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"warehouse database not reachable: {exc}")
    with conn:
        yield conn
