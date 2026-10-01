import hashlib
from datetime import UTC, datetime

import httpx
import pytest

from sepsis_pipeline.extract import http as http_mod
from sepsis_pipeline.extract.download import FileStatus, sync_files, verify_local
from sepsis_pipeline.extract.manifest import build_manifest, write_manifest_table
from sepsis_pipeline.extract.s3_listing import RemoteFile, list_remote_files

BASE = "https://mirror.test"
PREFIX = "challenge-2019/1.0.0/training/"

FILES = {
    f"{PREFIX}training_setA/p000001.psv": b"HR|SepsisLabel\n80|0\n",
    f"{PREFIX}training_setA/p000002.psv": b"HR|SepsisLabel\n90|0\n91|1\n",
    f"{PREFIX}training_setB/p100001.psv": b"HR|SepsisLabel\nNaN|0\n",
}


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _listing_page(keys, token=None):
    contents = "".join(
        f"<Contents><Key>{k}</Key><LastModified>2024-03-04T04:58:21.000Z</LastModified>"
        f"<ETag>&quot;{_md5(FILES[k]) if k in FILES else 'x'}&quot;</ETag>"
        f"<Size>{len(FILES.get(k, b''))}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        for k in keys
    )
    trunc = "true" if token else "false"
    nxt = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<IsTruncated>{trunc}</IsTruncated>{nxt}{contents}</ListBucketResult>"
    ).encode()


class FakeMirror:
    """Two listing pages (with a non-.psv key to be ignored) and the file bodies."""

    def __init__(self, corrupt_first: set[str] | None = None, always_fail: set[str] | None = None):
        self.corrupt_first = set(corrupt_first or ())
        self.always_fail = set(always_fail or ())
        self.downloads: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip("/")
        if path == "":
            keys = sorted(FILES)
            if request.url.params.get("continuation-token") == "page2":
                return httpx.Response(200, content=_listing_page(keys[2:]))
            return httpx.Response(
                200, content=_listing_page([*keys[:2], f"{PREFIX}README.txt"], token="page2")
            )
        self.downloads.append(path)
        if path in self.always_fail:
            return httpx.Response(503)
        if path in self.corrupt_first:
            self.corrupt_first.discard(path)
            return httpx.Response(200, content=b"X" * len(FILES[path]))
        return httpx.Response(200, content=FILES[path])


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(http_mod.time, "sleep", lambda _: None)


def _client(mirror):
    return httpx.Client(transport=httpx.MockTransport(mirror))


def test_listing_follows_continuation_tokens_and_keeps_only_patient_files():
    remote = list_remote_files(_client(FakeMirror()), BASE, PREFIX)

    assert [r.key for r in remote] == sorted(FILES)
    first = remote[0]
    assert (first.patient_id, first.source_set) == ("p000001", "A")
    assert str(first.relative_path) == "training_setA/p000001.psv"
    assert first.etag == _md5(FILES[first.key])
    assert first.last_modified == datetime(2024, 3, 4, 4, 58, 21, tzinfo=UTC)


def test_sync_downloads_then_second_run_only_verifies(tmp_path):
    mirror = FakeMirror()
    client = _client(mirror)
    remote = list_remote_files(client, BASE, PREFIX)

    first = sync_files(client, BASE, remote, tmp_path, workers=2)
    assert {r.status for r in first} == {FileStatus.DOWNLOADED}
    assert (tmp_path / "training_setB" / "p100001.psv").read_bytes() == FILES[remote[2].key]

    mirror.downloads.clear()
    second = sync_files(client, BASE, remote, tmp_path, workers=2)
    assert {r.status for r in second} == {FileStatus.VERIFIED}
    assert mirror.downloads == []


def test_corrupted_local_file_is_redownloaded(tmp_path):
    client = _client(FakeMirror())
    remote = list_remote_files(client, BASE, PREFIX)
    sync_files(client, BASE, remote, tmp_path, workers=2)

    target = tmp_path / remote[0].relative_path
    target.write_bytes(b"Y" * len(FILES[remote[0].key]))  # same size, wrong content
    assert verify_local(target, remote[0]) is None

    results = {r.remote.key: r.status for r in sync_files(client, BASE, remote, tmp_path, 2)}
    assert results[remote[0].key] is FileStatus.DOWNLOADED
    assert target.read_bytes() == FILES[remote[0].key]


def test_md5_mismatch_is_retried(tmp_path):
    bad = sorted(FILES)[1]
    mirror = FakeMirror(corrupt_first={bad})
    client = _client(mirror)
    remote = list_remote_files(client, BASE, PREFIX)

    results = sync_files(client, BASE, remote, tmp_path, workers=1)

    assert {r.status for r in results} == {FileStatus.DOWNLOADED}
    assert mirror.downloads.count(bad) == 2


def test_persistent_failure_is_reported_without_partial_files(tmp_path):
    bad = sorted(FILES)[0]
    client = _client(FakeMirror(always_fail={bad}))
    remote = list_remote_files(client, BASE, PREFIX)

    results = {r.remote.key: r for r in sync_files(client, BASE, remote, tmp_path, workers=2)}

    assert results[bad].status is FileStatus.FAILED
    assert "503" in results[bad].error
    assert not (tmp_path / remote[0].relative_path).exists()
    assert list(tmp_path.rglob("*.part")) == []


def test_multipart_etag_is_size_checked_only(tmp_path):
    data = b"abc\n"
    path = tmp_path / "f.psv"
    path.write_bytes(data)
    remote = RemoteFile("k/training_setA/f.psv", "deadbeef-2", len(data), datetime.now(UTC))

    assert verify_local(path, remote) == _md5(data)


def test_manifest_has_relative_posix_paths(tmp_path):
    client = _client(FakeMirror())
    remote = list_remote_files(client, BASE, PREFIX)
    manifest = build_manifest(sync_files(client, BASE, remote, tmp_path, 2), tmp_path)

    assert manifest.height == 3
    assert manifest["local_path"].to_list()[0] == "training_setA/p000001.psv"
    assert set(manifest["status"]) == {"downloaded"}


@pytest.mark.db
def test_manifest_table_is_replaced(db_conn, tmp_path):
    client = _client(FakeMirror())
    remote = list_remote_files(client, BASE, PREFIX)
    manifest = build_manifest(sync_files(client, BASE, remote, tmp_path, 2), tmp_path)

    db_conn.execute("DROP SCHEMA IF EXISTS pytest_raw CASCADE; CREATE SCHEMA pytest_raw")
    try:
        write_manifest_table(db_conn, manifest, schema="pytest_raw")
        write_manifest_table(db_conn, manifest, schema="pytest_raw")  # idempotent
        count, sets = db_conn.execute(
            "SELECT count(*), string_agg(DISTINCT source_set, ',' ORDER BY source_set)"
            " FROM pytest_raw.file_manifest"
        ).fetchone()
        assert (count, sets) == (3, "A,B")
    finally:
        db_conn.execute("DROP SCHEMA pytest_raw CASCADE")
