"""List the PhysioNet Challenge 2019 training files on the public S3 mirror."""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

import httpx

from sepsis_pipeline.extract.http import raise_for_retryable_status, with_retries

S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# Folder name on the mirror -> hospital code used throughout the warehouse.
SOURCE_SETS = {"training_setA": "A", "training_setB": "B"}

# Published file counts (PhysioNet page); a mismatch is reported, the listing stays authoritative.
EXPECTED_COUNTS = {"A": 20_336, "B": 20_000}


@dataclass(frozen=True)
class RemoteFile:
    key: str
    etag: str
    size: int
    last_modified: datetime

    @property
    def folder(self) -> str:
        return PurePosixPath(self.key).parent.name

    @property
    def source_set(self) -> str:
        return SOURCE_SETS[self.folder]

    @property
    def patient_id(self) -> str:
        return PurePosixPath(self.key).stem

    @property
    def relative_path(self) -> PurePosixPath:
        """Path below the raw data directory, e.g. training_setA/p000001.psv."""
        return PurePosixPath(self.folder, PurePosixPath(self.key).name)

    @property
    def is_multipart(self) -> bool:
        """Multipart-upload ETags (containing '-') are not the file's MD5."""
        return "-" in self.etag


def _parse_page(content: bytes) -> tuple[list[RemoteFile], str | None]:
    root = ET.fromstring(content)
    files = []
    for item in root.findall("s3:Contents", S3_NS):
        key = item.findtext("s3:Key", namespaces=S3_NS)
        if not key.endswith(".psv") or PurePosixPath(key).parent.name not in SOURCE_SETS:
            continue
        files.append(
            RemoteFile(
                key=key,
                etag=item.findtext("s3:ETag", namespaces=S3_NS).strip('"'),
                size=int(item.findtext("s3:Size", namespaces=S3_NS)),
                last_modified=datetime.fromisoformat(
                    item.findtext("s3:LastModified", namespaces=S3_NS)
                ),
            )
        )
    truncated = root.findtext("s3:IsTruncated", namespaces=S3_NS) == "true"
    next_token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS) if truncated else None
    return files, next_token


def list_remote_files(client: httpx.Client, base_url: str, prefix: str) -> list[RemoteFile]:
    """All patient files below prefix, following ListObjectsV2 continuation tokens."""
    files: list[RemoteFile] = []
    token: str | None = None
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if token:
            params["continuation-token"] = token

        def fetch_page(params=params) -> httpx.Response:
            response = client.get(f"{base_url}/", params=params)
            raise_for_retryable_status(response)
            return response

        page, token = _parse_page(with_retries(fetch_page).content)
        files.extend(page)
        if token is None:
            return sorted(files, key=lambda f: f.key)
