"""Train-only reference statistics per (split_scheme, feature), computed with Polars.

Computed here rather than in SQL: Postgres evaluates the ~200 percentile aggregates of one
GROUP BY concurrently, each holding its own sort, which exhausted memory on a 14 GB laptop.
Polars reads the exported train Parquet a few columns at a time, so memory stays at a few
hundred MB. Which patients belong to each scheme still comes from the dbt model
mart.split_scheme_membership (single source of truth, tested there).

Statistics are patient-hour weighted and match Postgres semantics: sample standard deviation,
linear-interpolated percentiles (percentile_cont).
"""

from pathlib import Path

import polars as pl
import psycopg
from psycopg import sql

STATS_SCHEMA = {
    "split_scheme": pl.String,
    "feature": pl.String,
    "mean": pl.Float64,
    "std": pl.Float64,
    "median": pl.Float64,
    "p01": pl.Float64,
    "p99": pl.Float64,
    "missing_fraction": pl.Float64,
    "n_observed": pl.Int64,
    "n_rows": pl.Int64,
    "n_patients": pl.Int64,
}
COLUMNS_PER_SCAN = 10


def numeric_feature_columns(schema: pl.Schema) -> list[str]:
    return [n for n, t in schema.items() if n.startswith("feat_") and t.is_numeric()]


def compute_reference_stats(
    train: pl.LazyFrame, membership: pl.DataFrame, feature_columns: list[str]
) -> pl.DataFrame:
    """train: lazy frame of training rows with patient_id + features.
    membership: (patient_id, split_scheme) — the patients allowed to contribute per scheme."""
    rows = []
    for scheme in sorted(membership["split_scheme"].unique().to_list()):
        ids = membership.filter(pl.col("split_scheme") == scheme)["patient_id"]
        subset = train.filter(pl.col("patient_id").is_in(ids.implode()))
        n_rows, n_patients = (
            subset.select(pl.len(), pl.col("patient_id").n_unique()).collect().row(0)
        )
        for start in range(0, len(feature_columns), COLUMNS_PER_SCAN):
            batch = feature_columns[start : start + COLUMNS_PER_SCAN]
            frame = subset.select([pl.col(c).cast(pl.Float64) for c in batch]).collect()
            for column in batch:
                values = frame[column].drop_nulls()
                rows.append(
                    {
                        "split_scheme": scheme,
                        "feature": column,
                        "mean": values.mean(),
                        "std": values.std(ddof=1),
                        "median": values.quantile(0.5, interpolation="linear"),
                        "p01": values.quantile(0.01, interpolation="linear"),
                        "p99": values.quantile(0.99, interpolation="linear"),
                        "missing_fraction": 1 - values.len() / n_rows if n_rows else None,
                        "n_observed": values.len(),
                        "n_rows": n_rows,
                        "n_patients": n_patients,
                    }
                )
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def load_membership(conn: psycopg.Connection) -> pl.DataFrame:
    return pl.DataFrame(
        conn.execute(
            "SELECT patient_id, split_scheme FROM mart.split_scheme_membership"
        ).fetchall(),
        schema={"patient_id": pl.String, "split_scheme": pl.String},
        orient="row",
    )


def write_stats_table(conn: psycopg.Connection, stats: pl.DataFrame) -> None:
    """Replace mart.train_reference_stats (owned by `sepsis export`, documented as a dbt source)."""
    table = sql.Identifier("mart", "train_reference_stats")
    types = {pl.String: "text", pl.Float64: "double precision", pl.Int64: "bigint"}
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(n), sql.SQL(types[t]))
        for n, t in STATS_SCHEMA.items()
    )
    with conn.transaction():
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
        conn.execute(
            sql.SQL("CREATE TABLE {} ({}, PRIMARY KEY (split_scheme, feature))").format(
                table, columns
            )
        )
        with conn.cursor().copy(
            sql.SQL("COPY {} ({}) FROM STDIN").format(
                table, sql.SQL(", ").join(sql.Identifier(n) for n in STATS_SCHEMA)
            )
        ) as copy:
            for row in stats.iter_rows():
                copy.write_row(row)
        conn.execute(sql.SQL("GRANT SELECT ON {} TO dm_reader").format(table))


def build_reference_stats(conn: psycopg.Connection, train_dir: Path) -> pl.DataFrame:
    train = pl.scan_parquet(train_dir)
    membership = load_membership(conn)
    train_patients = set(train.select("patient_id").unique().collect()["patient_id"].to_list())
    outside = set(membership["patient_id"].to_list()) - train_patients
    if outside:  # every scheme's training patients must be random-split training patients
        raise RuntimeError(f"{len(outside)} scheme members are not in the train export")
    stats = compute_reference_stats(
        train, membership, numeric_feature_columns(train.collect_schema())
    )
    write_stats_table(conn, stats)
    return stats
