"""Concurrent, resumable, MD5-verified download of the patient files."""

import hashlib
import os
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import httpx

from sepsis_pipeline.extract.http import RetryableError, raise_for_retryable_status, with_retries
from sepsis_pipeline.extract.s3_listing import RemoteFile


class FileStatus(StrEnum):
    VERIFIED = "verified"  # already present locally with the expected size and MD5
    DOWNLOADED = "downloaded"
    FAILED = "failed"


@dataclass(frozen=True)
class FileResult:
    remote: RemoteFile
    local_path: Path
    status: FileStatus
    md5: str | None
    downloaded_at: datetime | None
    error: str | None = None


def file_md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_local(path: Path, remote: RemoteFile) -> str | None:
    """Return the local file's MD5 if it matches the remote object, else None."""
    if not path.is_file() or path.stat().st_size != remote.size:
        return None
    md5 = file_md5(path)
    if remote.is_multipart:  # ETag is not an MD5: the size check is all we can do
        return md5
    return md5 if md5 == remote.etag else None


def download_file(client: httpx.Client, url: str, remote: RemoteFile, dest: Path) -> str:
    """Stream url to dest atomically; raise RetryableError if the content does not verify."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    digest = hashlib.md5(usedforsecurity=False)
    try:
        with client.stream("GET", url) as response:
            raise_for_retryable_status(response)
            with tmp.open("wb") as f:
                for chunk in response.iter_bytes():
                    digest.update(chunk)
                    f.write(chunk)
        md5 = digest.hexdigest()
        size = tmp.stat().st_size
        if size != remote.size:
            raise RetryableError(f"size mismatch for {remote.key}: got {size}, want {remote.size}")
        if not remote.is_multipart and md5 != remote.etag:
            raise RetryableError(f"MD5 mismatch for {remote.key}: got {md5}, want {remote.etag}")
        os.replace(tmp, dest)
        return md5
    finally:
        tmp.unlink(missing_ok=True)


def _sync_one(client: httpx.Client, base_url: str, remote: RemoteFile, raw_dir: Path) -> FileResult:
    dest = raw_dir / remote.relative_path
    md5 = verify_local(dest, remote)
    if md5 is not None:
        mtime = datetime.fromtimestamp(dest.stat().st_mtime, tz=UTC)
        return FileResult(remote, dest, FileStatus.VERIFIED, md5, mtime)
    try:
        md5 = with_retries(lambda: download_file(client, f"{base_url}/{remote.key}", remote, dest))
    except Exception as exc:  # noqa: BLE001 - one bad file must not stop the other 40k
        return FileResult(
            remote, dest, FileStatus.FAILED, None, None, f"{type(exc).__name__}: {exc}"
        )
    return FileResult(remote, dest, FileStatus.DOWNLOADED, md5, datetime.now(UTC))


def sync_files(
    client: httpx.Client,
    base_url: str,
    remote_files: Iterable[RemoteFile],
    raw_dir: Path,
    workers: int,
    on_result: Callable[[FileResult], None] | None = None,
) -> list[FileResult]:
    """Make raw_dir match the remote listing: verify what exists, download what is missing."""
    for leftover in raw_dir.rglob("*.part"):
        leftover.unlink()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_sync_one, client, base_url, r, raw_dir) for r in remote_files]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if on_result is not None:
                on_result(result)
    return sorted(results, key=lambda r: r.remote.key)
