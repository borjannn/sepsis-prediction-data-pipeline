"""Stage 2 — Bronze load: parse + validate the raw files, write Parquet, reload raw.*.

Streams bounded batches of files (see bronze.load): each batch is validated, written as a
Parquet part and COPYed into Postgres, all inside one transaction.
"""

import shutil
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from sepsis_pipeline.bronze.db import BronzeWriter
from sepsis_pipeline.bronze.load import (
    QUARANTINE_SCHEMA,
    iter_parsed_batches,
    patient_label_status,
    select_sample_patients,
)
from sepsis_pipeline.config import Settings
from sepsis_pipeline.db import connect
from sepsis_pipeline.leakage import LEAKAGE_TEST_PATIENTS


@dataclass
class BronzeSummary:
    files_in: int
    patients: int
    rows: int
    quarantined: pl.DataFrame
    sampled: bool


def run_load_bronze(
    settings: Settings,
    sample: int | None = None,
    on_batch: Callable[[int, int], None] | None = None,
) -> BronzeSummary:
    manifest_path = settings.raw_dir / "manifest.parquet"
    if not manifest_path.exists():
        raise FileNotFoundError(f"{manifest_path} not found; run `sepsis extract` first")
    manifest = pl.read_parquet(manifest_path).filter(pl.col("status") != "failed")
    files_in = manifest.height

    if sample is not None:
        status = patient_label_status(manifest, settings.raw_dir)
        keep = select_sample_patients(
            status, sample, settings.split_seed, always_include=LEAKAGE_TEST_PATIENTS
        )
        manifest = manifest.filter(pl.col("patient_id").is_in(sorted(keep)))

    out_dir = settings.bronze_dir / "patient_hour"
    shutil.rmtree(out_dir, ignore_errors=True)
    load_run_id = str(uuid.uuid4())
    quarantined: list[pl.DataFrame] = [pl.DataFrame(schema=QUARANTINE_SCHEMA)]
    patients = 0

    with connect(settings) as conn, BronzeWriter(conn, load_run_id) as writer:
        for i, batch in enumerate(iter_parsed_batches(manifest, settings.raw_dir, load_run_id)):
            for (source_set,), part in batch.frame.group_by("source_set"):
                part_dir = out_dir / f"source_set={source_set}"
                part_dir.mkdir(parents=True, exist_ok=True)
                part.drop("source_set").write_parquet(
                    part_dir / f"part-{i:04d}.parquet", compression="zstd"
                )
            writer.add_batch(batch.frame, batch.quarantine)
            quarantined.append(batch.quarantine)
            patients += batch.frame["patient_id"].n_unique()
            if on_batch is not None:
                on_batch(writer.rows, manifest.height)
        rows = writer.rows

    return BronzeSummary(
        files_in=files_in,
        patients=patients,
        rows=rows,
        quarantined=pl.concat(quarantined),
        sampled=sample is not None,
    )
