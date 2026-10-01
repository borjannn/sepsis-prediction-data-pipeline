"""Stage 1 — Extract: list, verify and download the patient files, then write the manifest."""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from sepsis_pipeline.config import Settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.extract.download import FileResult, FileStatus, sync_files
from sepsis_pipeline.extract.http import make_client
from sepsis_pipeline.extract.manifest import (
    build_manifest,
    write_manifest_parquet,
    write_manifest_table,
)
from sepsis_pipeline.extract.s3_listing import EXPECTED_COUNTS, RemoteFile, list_remote_files


@dataclass
class ExtractSummary:
    remote_count: int
    status_counts: Counter
    per_set: dict[str, int]
    warnings: list[str] = field(default_factory=list)
    failures: list[FileResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def run_extract(
    settings: Settings,
    *,
    on_listed: Callable[[list[RemoteFile]], None] | None = None,
    on_result: Callable[[FileResult], None] | None = None,
) -> ExtractSummary:
    raw_dir = settings.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    with make_client(settings.extract_workers) as client:
        remote = list_remote_files(client, settings.physionet_base_url, settings.physionet_prefix)
        if on_listed is not None:
            on_listed(remote)
        results = sync_files(
            client,
            settings.physionet_base_url,
            remote,
            raw_dir,
            settings.extract_workers,
            on_result,
        )

    manifest = build_manifest(results, raw_dir)
    write_manifest_parquet(manifest, raw_dir / "manifest.parquet")
    with connect(settings) as conn:
        write_manifest_table(conn, manifest)

    per_set = Counter(r.source_set for r in remote)
    summary = ExtractSummary(
        remote_count=len(remote),
        status_counts=Counter(r.status for r in results),
        per_set=dict(sorted(per_set.items())),
        failures=[r for r in results if r.status is FileStatus.FAILED],
    )
    for source_set, expected in EXPECTED_COUNTS.items():
        if per_set.get(source_set, 0) != expected:
            summary.warnings.append(
                f"set {source_set}: listing has {per_set.get(source_set, 0)} files, "
                f"PhysioNet publishes {expected}"
            )
    multipart = sum(r.is_multipart for r in remote)
    if multipart:
        summary.warnings.append(f"{multipart} files have multipart ETags (size-checked only)")
    listed = {raw_dir / r.relative_path for r in remote}
    strays = [p for p in raw_dir.rglob("*.psv") if p not in listed]
    if strays:
        summary.warnings.append(f"{len(strays)} local .psv files are not in the remote listing")
    return summary
