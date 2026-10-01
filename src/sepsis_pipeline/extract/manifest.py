"""The file manifest: one row per patient file, written to Parquet and raw.file_manifest."""

from pathlib import Path

import polars as pl
import psycopg
from psycopg import sql

from sepsis_pipeline.extract.download import FileResult

MANIFEST_SCHEMA = {
    "patient_id": pl.String,
    "source_set": pl.String,
    "s3_key": pl.String,
    "etag": pl.String,
    "size_bytes": pl.Int64,
    "last_modified": pl.Datetime("us", "UTC"),
    "local_path": pl.String,
    "md5": pl.String,
    "downloaded_at": pl.Datetime("us", "UTC"),
    "status": pl.String,
    "error": pl.String,
}

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    patient_id    text        PRIMARY KEY,
    source_set    char(1)     NOT NULL CHECK (source_set IN ('A', 'B')),
    s3_key        text        NOT NULL,
    etag          text        NOT NULL,
    size_bytes    bigint      NOT NULL,
    last_modified timestamptz NOT NULL,
    local_path    text        NOT NULL,
    md5           text,
    downloaded_at timestamptz,
    status        text        NOT NULL CHECK (status IN ('verified', 'downloaded', 'failed')),
    error         text
)
"""


def build_manifest(results: list[FileResult], raw_dir: Path) -> pl.DataFrame:
    """local_path is stored relative to raw_dir (POSIX style) so the manifest is portable."""
    rows = [
        {
            "patient_id": r.remote.patient_id,
            "source_set": r.remote.source_set,
            "s3_key": r.remote.key,
            "etag": r.remote.etag,
            "size_bytes": r.remote.size,
            "last_modified": r.remote.last_modified,
            "local_path": r.local_path.relative_to(raw_dir).as_posix(),
            "md5": r.md5,
            "downloaded_at": r.downloaded_at,
            "status": str(r.status),
            "error": r.error,
        }
        for r in results
    ]
    return pl.DataFrame(rows, schema=MANIFEST_SCHEMA)


def write_manifest_parquet(manifest: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    manifest.write_parquet(tmp, compression="zstd")
    tmp.replace(path)


def write_manifest_table(
    conn: psycopg.Connection, manifest: pl.DataFrame, schema: str = "raw"
) -> None:
    """Replace the manifest table's contents in a single transaction."""
    table = sql.Identifier(schema, "file_manifest")
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in MANIFEST_SCHEMA)
    with conn.transaction():
        conn.execute(sql.SQL(_TABLE_DDL).format(table=table))
        conn.execute(sql.SQL("TRUNCATE {table}").format(table=table))
        with conn.cursor().copy(
            sql.SQL("COPY {table} ({columns}) FROM STDIN").format(table=table, columns=columns)
        ) as copy:
            for row in manifest.iter_rows():
                copy.write_row(row)
