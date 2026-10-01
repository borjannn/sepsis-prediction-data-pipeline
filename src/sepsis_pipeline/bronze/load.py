"""Parse the raw .psv files, quarantine invalid ones, and add lineage columns.

Files are processed in bounded batches (READ_BATCH_FILES): one scan over all ~40k files makes
Polars allocate read buffers for the whole scan and exhausts memory, and holding the full
1.55M-row frame plus its intermediate copies peaks at several GB. Streaming batches keeps the
load at a few hundred MB regardless of dataset size.
"""

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from sepsis_pipeline.bronze.schema import (
    BRONZE_COLUMNS,
    BRONZE_SCHEMA,
    EXPECTED_HEADER,
    FILE_RULES,
    LABEL,
    SOURCE_SCHEMA,
)

QUARANTINE_SCHEMA = {"source_file": pl.String, "patient_id": pl.String, "reason": pl.String}
READ_BATCH_FILES = 2_000


@dataclass
class ParsedBronze:
    frame: pl.DataFrame  # validated rows of the accepted files, BRONZE_COLUMNS order
    quarantine: pl.DataFrame  # one row per rejected file (QUARANTINE_SCHEMA)


def _first_line(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as f:
        return f.readline().rstrip("\r\n")


def _read_csv_kwargs() -> dict:
    return {"separator": "|", "null_values": ["NaN"], "schema": SOURCE_SCHEMA, "has_header": True}


def _empty_source_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={**SOURCE_SCHEMA, "source_file": pl.String})


def _read_files(paths: list[Path]) -> tuple[pl.DataFrame, list[dict]]:
    """One scan over the files; if any file breaks parsing, isolate it file by file."""
    if not paths:
        return _empty_source_frame(), []
    try:
        frame = pl.scan_csv(
            [str(p) for p in paths], include_file_paths="source_file", **_read_csv_kwargs()
        ).collect()
        rejected: list[dict] = []
    except pl.exceptions.PolarsError:
        frames, rejected = [_empty_source_frame()], []
        for path in paths:
            try:
                frames.append(
                    pl.read_csv(path, **_read_csv_kwargs()).with_columns(
                        source_file=pl.lit(str(path))
                    )
                )
            except pl.exceptions.PolarsError as exc:
                rejected.append({"path": path, "reason": f"parse error: {exc}".splitlines()[0]})
        frame = pl.concat(frames)

    # A header-only file parses to zero rows and so never reaches the per-file rules.
    seen = set(frame["source_file"].str.replace_all("\\", "/", literal=True).unique().to_list())
    rejected_paths = {r["path"] for r in rejected}
    for path in paths:
        if path not in rejected_paths and path.as_posix() not in seen:
            rejected.append({"path": path, "reason": "no data rows"})
    return frame, rejected


def _accepted_files(manifest: pl.DataFrame) -> pl.DataFrame:
    return manifest.filter(pl.col("status") != "failed").select(
        "patient_id", "source_set", "local_path", pl.col("md5").alias("file_md5")
    )


def parse_files(
    manifest: pl.DataFrame, raw_dir: Path, load_run_id: str, loaded_at: datetime | None = None
) -> ParsedBronze:
    """Parse the manifest's successfully downloaded files (one batch; see iter_parsed_batches).

    manifest needs columns patient_id, source_set, local_path (relative to raw_dir), md5, status.
    """
    loaded_at = loaded_at or datetime.now(UTC)
    files = _accepted_files(manifest)
    rejected: list[dict] = []

    # Absolute POSIX path -> manifest's relative path. Polars reports include_file_paths with
    # forward slashes even on Windows, so both sides are normalised to POSIX form.
    to_relative = {(raw_dir / lp).as_posix(): lp for lp in files["local_path"]}

    paths = []
    for local_path in files["local_path"]:
        path = raw_dir / local_path
        header = _first_line(path)
        if header != EXPECTED_HEADER:
            rejected.append({"path": path, "reason": f"unexpected header: {header[:80]}"})
        else:
            paths.append(path)

    frame, parse_rejected = _read_files(paths)
    rejected += parse_rejected
    frame = frame.with_columns(
        pl.col("source_file").str.replace_all("\\", "/", literal=True).replace_strict(to_relative)
    )

    rule_results = frame.group_by("source_file").agg(
        **{name: expr for name, expr in FILE_RULES.items()}
    )
    failing = rule_results.unpivot(index="source_file", variable_name="reason").filter(
        ~pl.col("value")
    )
    quarantine_rows = [
        {"source_file": to_relative[r["path"].as_posix()], "reason": r["reason"]} for r in rejected
    ] + failing.select("source_file", "reason").to_dicts()
    quarantine = (
        pl.DataFrame(quarantine_rows, schema={"source_file": pl.String, "reason": pl.String})
        .group_by("source_file")
        .agg(pl.col("reason").sort().str.join("; "))
        .join(
            files.select("local_path", "patient_id"), left_on="source_file", right_on="local_path"
        )
        .select(list(QUARANTINE_SCHEMA))
        .sort("source_file")
    )

    accepted = (
        frame.join(quarantine.select("source_file"), on="source_file", how="anti")
        .join(files, left_on="source_file", right_on="local_path", how="inner")
        .with_columns(
            source_row_number=(pl.int_range(pl.len()).over("source_file") + 1).cast(pl.Int32),
            load_run_id=pl.lit(load_run_id),
            loaded_at=pl.lit(loaded_at, dtype=pl.Datetime("us", "UTC")),
        )
        .select(BRONZE_COLUMNS)
        .sort("source_set", "patient_id", "source_row_number")
    )
    return ParsedBronze(frame=BRONZE_SCHEMA.validate(accepted), quarantine=quarantine)


def iter_parsed_batches(
    manifest: pl.DataFrame,
    raw_dir: Path,
    load_run_id: str,
    batch_files: int = READ_BATCH_FILES,
) -> Iterator[ParsedBronze]:
    """parse_files over bounded batches of the manifest (constant memory)."""
    loaded_at = datetime.now(UTC)
    manifest = manifest.sort("local_path")
    for start in range(0, manifest.height, batch_files):
        yield parse_files(
            manifest.slice(start, batch_files), raw_dir, load_run_id, loaded_at=loaded_at
        )


def patient_label_status(
    manifest: pl.DataFrame, raw_dir: Path, batch_files: int = READ_BATCH_FILES
) -> pl.DataFrame:
    """(patient_id, source_set, is_sepsis) for every readable file, reading only the label.

    Cheap first pass used to choose a stratified --sample before the real load.
    """
    files = _accepted_files(manifest).sort("local_path")
    parts = []
    for start in range(0, files.height, batch_files):
        batch = files.slice(start, batch_files)
        to_patient = {
            (raw_dir / lp).as_posix(): pid
            for lp, pid in batch.select("local_path", "patient_id").iter_rows()
        }
        try:
            labels = (
                pl.scan_csv(
                    [str(raw_dir / lp) for lp in batch["local_path"]],
                    include_file_paths="source_file",
                    **_read_csv_kwargs(),
                )
                .select("source_file", LABEL)
                .collect()
            )
        except pl.exceptions.PolarsError:
            continue  # unreadable files are quarantined by the real load anyway
        parts.append(
            labels.with_columns(
                patient_id=pl.col("source_file")
                .str.replace_all("\\", "/", literal=True)
                .replace_strict(to_patient)
            )
            .group_by("patient_id")
            .agg(is_sepsis=pl.col(LABEL).max())
        )
    status = (
        pl.concat(parts)
        if parts
        else pl.DataFrame(schema={"patient_id": pl.String, "is_sepsis": pl.Float64})
    )
    return status.join(files.select("patient_id", "source_set"), on="patient_id")


def select_sample_patients(
    patients: pl.DataFrame,
    size: int,
    seed: int | str,
    always_include: tuple[str, ...] = (),
) -> set[str]:
    """Deterministic patient sample stratified by (hospital, sepsis patient).

    patients has columns patient_id, source_set, is_sepsis. Each stratum contributes in
    proportion to its share of patients; within a stratum the patients with the smallest
    md5(patient_id || seed) are taken. always_include patients are added on top (e.g. the fixed
    patients of the truncation leakage test).
    """
    total = patients.height
    if size >= total:
        return set(patients["patient_id"].to_list())

    def rank_key(pid: str) -> str:
        return hashlib.md5(f"{pid}{seed}".encode(), usedforsecurity=False).hexdigest()

    ranked = patients.with_columns(
        rank_key=pl.col("patient_id").map_elements(rank_key, return_dtype=pl.String)
    )
    chosen: set[str] = set(always_include)
    for _, stratum in ranked.group_by("source_set", "is_sepsis", maintain_order=True):
        quota = max(1, round(size * stratum.height / total))
        chosen.update(stratum.sort("rank_key").head(quota)["patient_id"].to_list())
    return chosen


def stratified_sample(
    frame: pl.DataFrame,
    size: int,
    seed: int | str,
    always_include: tuple[str, ...] = (),
) -> pl.DataFrame:
    """select_sample_patients applied to an in-memory patient-hour frame."""
    patients = frame.group_by("patient_id", "source_set").agg(is_sepsis=pl.col(LABEL).max())
    keep = select_sample_patients(patients, size, seed, always_include)
    return frame.filter(pl.col("patient_id").is_in(sorted(keep)))
