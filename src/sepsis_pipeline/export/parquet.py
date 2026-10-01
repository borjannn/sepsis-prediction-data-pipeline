"""Stream query results from Postgres into Parquet with bounded memory.

COPY (query) TO STDOUT (FORMAT csv) is streamed to a temporary CSV, then Polars streams that
CSV into Parquet (zstd) with an explicit schema derived from the Postgres column types, so
memory use does not depend on the result size and no Python row objects are created.
"""

import hashlib
from pathlib import Path

import polars as pl
import psycopg
from psycopg import sql

_PG_TO_POLARS = {
    "double precision": pl.Float64,
    "real": pl.Float32,
    "numeric": pl.Float64,
    "bigint": pl.Int64,
    "integer": pl.Int32,
    "smallint": pl.Int16,
    "boolean": pl.Boolean,
    "text": pl.String,
    "character": pl.String,
    "character varying": pl.String,
    "uuid": pl.String,
    "timestamp with time zone": pl.Datetime("us", "UTC"),
}


def relation_columns(conn: psycopg.Connection, schema: str, relation: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, relation),
    ).fetchall()
    if not rows:
        raise LookupError(f"{schema}.{relation} has no columns (does it exist?)")
    return rows


def polars_schema(columns: list[tuple[str, str]]) -> dict[str, pl.DataType]:
    return {name: _PG_TO_POLARS.get(pg_type, pl.String) for name, pg_type in columns}


def export_query(
    conn: psycopg.Connection,
    query: sql.Composable,
    columns: list[tuple[str, str]],
    out_path: Path,
    tmp_dir: Path,
) -> int:
    """Write the query result to out_path (Parquet). Returns the number of rows written."""
    schema = polars_schema(columns)
    # Booleans arrive as t/f in Postgres CSV: read them as strings and convert.
    read_schema = {n: (pl.String if t == pl.Boolean else t) for n, t in schema.items()}
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_csv = tmp_dir / (out_path.stem + ".csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with (
            tmp_csv.open("wb") as f,
            conn.cursor().copy(
                sql.SQL("COPY ({}) TO STDOUT (FORMAT csv, HEADER)").format(query)
            ) as copy,
        ):
            for chunk in copy:
                f.write(chunk)
        lazy = pl.scan_csv(tmp_csv, schema=read_schema, has_header=True).with_columns(
            pl.col(n).replace_strict({"t": True, "f": False}, return_dtype=pl.Boolean)
            for n, t in schema.items()
            if t == pl.Boolean
        )
        lazy.sink_parquet(out_path, compression="zstd")
    finally:
        tmp_csv.unlink(missing_ok=True)
    return pl.scan_parquet(out_path).select(pl.len()).collect().item()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
