"""Write bronze to Postgres: truncate and reload raw.patient_hour and raw.quarantine."""

import io

import polars as pl
import psycopg
from psycopg import sql

from sepsis_pipeline.bronze.load import QUARANTINE_SCHEMA
from sepsis_pipeline.bronze.schema import LINEAGE_SCHEMA, SOURCE_COLUMNS

_LINEAGE_SQL_TYPES = {
    "patient_id": "text NOT NULL",
    "source_set": "char(1) NOT NULL",
    "source_file": "text NOT NULL",
    "source_row_number": "integer NOT NULL",
    "file_md5": "text NOT NULL",
    "load_run_id": "uuid NOT NULL",
    "loaded_at": "timestamptz NOT NULL",
}
assert set(_LINEAGE_SQL_TYPES) == set(LINEAGE_SCHEMA)

COPY_BATCH_ROWS = 200_000


def _patient_hour_ddl(table: sql.Composable) -> sql.Composed:
    """Source columns keep their original names (quoted) and become double precision."""
    columns = [sql.SQL("{} double precision").format(sql.Identifier(c)) for c in SOURCE_COLUMNS] + [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
        for c, t in _LINEAGE_SQL_TYPES.items()
    ]
    return sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, sql.SQL(", ").join(columns))


def _copy_frame(conn: psycopg.Connection, table: sql.Composable, frame: pl.DataFrame) -> None:
    """COPY a frame as CSV in batches (empty unquoted field = NULL)."""
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in frame.columns)
    statement = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(table, columns)
    with conn.cursor().copy(statement) as copy:
        for batch in frame.iter_slices(COPY_BATCH_ROWS):
            buffer = io.BytesIO()
            batch.write_csv(buffer, include_header=False, float_precision=None)
            copy.write(buffer.getvalue())


class BronzeWriter:
    """Replace bronze contents in one transaction, fed batch by batch.

    Use as a context manager inside an open connection: entering truncates the tables and drops
    the indexes, add_batch() COPYs each batch, and a clean exit loads the quarantine, rebuilds
    the indexes and commits. Any exception rolls everything back, so a failed load leaves the
    previous bronze contents untouched.

    TRUNCATE (not DROP) keeps the dbt staging views that depend on raw.patient_hour. The
    (patient_id, ICULOS) index is deliberately non-unique: uniqueness is enforced by a silver
    test, so COPY never aborts on bad data.
    """

    def __init__(self, conn: psycopg.Connection, load_run_id: str, schema: str = "raw") -> None:
        self.conn = conn
        self.load_run_id = load_run_id
        self.schema = schema
        self.patient_hour = sql.Identifier(schema, "patient_hour")
        self.quarantine_table = sql.Identifier(schema, "quarantine")
        self.rows = 0
        self._quarantine: list[pl.DataFrame] = []
        self._transaction = None

    def __enter__(self) -> "BronzeWriter":
        self._transaction = self.conn.transaction()
        self._transaction.__enter__()
        conn, schema = self.conn, self.schema
        conn.execute(_patient_hour_ddl(self.patient_hour))
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (source_file text PRIMARY KEY, patient_id text,"
                " reason text NOT NULL, load_run_id uuid NOT NULL,"
                " quarantined_at timestamptz NOT NULL DEFAULT now())"
            ).format(self.quarantine_table)
        )
        for index in ("patient_hour_patient_iculos_idx", "patient_hour_source_set_idx"):
            conn.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(schema, index)))
        conn.execute(sql.SQL("TRUNCATE {}, {}").format(self.patient_hour, self.quarantine_table))
        return self

    def add_batch(self, frame: pl.DataFrame, quarantine: pl.DataFrame) -> None:
        _copy_frame(self.conn, self.patient_hour, frame)
        self.rows += frame.height
        if quarantine.height:
            self._quarantine.append(quarantine.select(list(QUARANTINE_SCHEMA)))

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            if self._quarantine:
                _copy_frame(
                    self.conn,
                    self.quarantine_table,
                    pl.concat(self._quarantine).with_columns(load_run_id=pl.lit(self.load_run_id)),
                )
            self.conn.execute(
                sql.SQL(
                    'CREATE INDEX patient_hour_patient_iculos_idx ON {} (patient_id, "ICULOS")'
                ).format(self.patient_hour)
            )
            self.conn.execute(
                sql.SQL("CREATE INDEX patient_hour_source_set_idx ON {} (source_set)").format(
                    self.patient_hour
                )
            )
        self._transaction.__exit__(exc_type, exc, tb)
        if exc_type is None:
            self.conn.execute(sql.SQL("ANALYZE {}").format(self.patient_hour))
            self.conn.commit()


def write_bronze(
    conn: psycopg.Connection,
    frame: pl.DataFrame,
    quarantine: pl.DataFrame,
    load_run_id: str,
    schema: str = "raw",
) -> None:
    """Replace bronze contents with one in-memory frame (convenience for small loads/tests)."""
    with BronzeWriter(conn, load_run_id, schema) as writer:
        writer.add_batch(frame, quarantine)
