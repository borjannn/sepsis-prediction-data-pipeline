"""HTTP client and retry helpers for talking to the PhysioNet S3 mirror."""

import random
import time
from collections.abc import Callable

import httpx

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryableError(Exception):
    """A transient failure (throttling, 5xx, truncated or corrupted download)."""


def make_client(max_connections: int) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections,
        ),
        follow_redirects=True,
        headers={"User-Agent": "sepsis-pipeline (PhysioNet Challenge 2019 ETL)"},
    )


def raise_for_retryable_status(response: httpx.Response) -> None:
    if response.status_code in RETRY_STATUSES:
        raise RetryableError(f"HTTP {response.status_code} for {response.url}")
    response.raise_for_status()


def with_retries[T](
    fn: Callable[[], T],
    *,
    attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 20.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call fn, retrying transient failures with exponential backoff and full jitter."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (RetryableError, httpx.TransportError):
            if attempt == attempts:
                raise
            sleep(random.uniform(0, min(max_delay, base_delay * 2 ** (attempt - 1))))
    raise AssertionError("unreachable")
